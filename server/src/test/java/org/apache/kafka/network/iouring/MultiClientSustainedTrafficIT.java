/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.network.iouring;

import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.network.ByteBufferSend;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.channel.Channel;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Multi-client regression test: io_uring selector must keep echoing under
 * sustained loopback traffic from {@link #NUM_CLIENTS} concurrent client
 * sockets. {@link SustainedTrafficIT} (single client) passes 20k round-trips
 * cleanly, but the SelectorThroughputBenchmark exposed a hang that only
 * surfaces with multiple concurrent sockets driving the same selector. This
 * test reproduces that scenario in-package so the freeze can be diagnosed
 * with direct access to channel state.
 */
class MultiClientSustainedTrafficIT {

    private static final ListenerName LISTENER = ListenerName.normalised("PLAINTEXT");
    private static final int MAX_RECEIVE = 1 << 20;
    private static final long IDLE_NANOS_NEVER = TimeUnit.HOURS.toNanos(1);
    private static final long CONNECT_DEADLINE_MS = 30_000;
    private static final long TEST_DEADLINE_MS = 60_000;
    private static final int NUM_CLIENTS = 4;
    private static final int ROUND_TRIPS_PER_CLIENT = 5_000;

    @Test
    void manyConcurrentClientsEachComplete5kRoundTrips() throws Exception {
        assumeTrue(IoUringSupport.isAvailable(),
            "io_uring not available (" + IoUringSupport.unavailabilityReason() + "); skipping");

        try (IoUringSelector selector = new IoUringSelector(
                LISTENER, MAX_RECEIVE, MemoryPool.NONE, IDLE_NANOS_NEVER, Time.SYSTEM);
             IoUringServerListener listener = new IoUringServerListener(
                 new InetSocketAddress("127.0.0.1", 0), selector)) {
            listener.start();
            int port = listener.boundPort();

            AtomicBoolean stop = new AtomicBoolean(false);
            Thread server = new Thread(() -> echoLoop(selector, stop), "iouring-echo");
            server.setDaemon(true);
            server.start();

            List<Socket> sockets = new ArrayList<>();
            ClientState[] state = new ClientState[NUM_CLIENTS];
            try {
                CountDownLatch startGate = new CountDownLatch(1);
                for (int i = 0; i < NUM_CLIENTS; i++) {
                    state[i] = startClient(i, port, sockets, startGate);
                }
                startGate.countDown();
                awaitOrDumpOnStall(selector, state);
                joinClients(state);
                assertAllClientsCompleted(state);
            } finally {
                stop.set(true);
                selector.wakeup();
                server.join(2_000);
                for (Socket s : sockets) {
                    try {
                        s.close();
                    } catch (Exception ignored) {
                        // best-effort
                    }
                }
            }
        }
    }

    private static ClientState startClient(int idx, int port, List<Socket> sockets,
                                           CountDownLatch startGate) throws Exception {
        Socket s = new Socket();
        s.connect(new InetSocketAddress("127.0.0.1", port), (int) CONNECT_DEADLINE_MS);
        s.setTcpNoDelay(true);
        s.setSoTimeout(15_000);
        sockets.add(s);
        ClientState st = new ClientState(idx);
        st.thread = new Thread(() -> runClient(idx, s, startGate, st), "client-" + idx);
        st.thread.setDaemon(true);
        st.thread.start();
        return st;
    }

    private static void runClient(int idx, Socket s, CountDownLatch startGate, ClientState st) {
        try {
            DataInputStream in = new DataInputStream(s.getInputStream());
            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            byte[] payload = new byte[64];
            for (int b = 0; b < payload.length; b++) payload[b] = (byte) ((idx + b) & 0xff);
            byte[] recv = new byte[payload.length];
            startGate.await();
            for (int n = 0; n < ROUND_TRIPS_PER_CLIENT; n++) {
                out.writeInt(payload.length);
                out.write(payload);
                out.flush();
                int respLen = in.readInt();
                assertEquals(payload.length, respLen, "client " + idx + " frame " + n + " wrong size");
                in.readFully(recv);
                // TEST-1: assert byte-level content, not just length. Each client builds a
                // distinct payload (idx + b mod 256). A cross-channel routing bug would
                // surface here — without this check, a response intended for client B
                // delivered to client A's socket would still pass the length-only test
                // (all payloads are 64 bytes). Same goes for partial-write drops where
                // a byte is dropped and re-filled from another buffer.
                assertArrayEquals(payload, recv,
                    "client " + idx + " frame " + n + " content mismatch — possible cross-channel routing or wire-ordering regression");
                st.progress.lazySet(n + 1);
            }
        } catch (Throwable t) {
            st.failure = t;
        }
    }

    private static void awaitOrDumpOnStall(IoUringSelector selector, ClientState[] state) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TEST_DEADLINE_MS;
        long lastDump = System.nanoTime();
        long[] lastProgress = new long[state.length];
        long stallStartedAt = 0;
        while (System.currentTimeMillis() < deadline) {
            long sum = 0;
            boolean allDone = true;
            boolean progressing = false;
            for (int i = 0; i < state.length; i++) {
                long p = state[i].progress.get();
                sum += p;
                if (p < ROUND_TRIPS_PER_CLIENT) allDone = false;
                if (p != lastProgress[i]) progressing = true;
                lastProgress[i] = p;
            }
            if (allDone) return;
            if (System.nanoTime() - lastDump > TimeUnit.SECONDS.toNanos(1)) {
                System.err.printf("[multi-client] progress sum=%d/%d  per-client=%s%n",
                    sum, state.length * ROUND_TRIPS_PER_CLIENT, arrSnapshot(state));
                dumpSelectorState(selector);
                lastDump = System.nanoTime();
            }
            stallStartedAt = trackStall(selector, progressing, stallStartedAt);
            // A detected stall must fail the test loudly. Previously trackStall returned -1
            // and we returned silently, so the only signal was the trip-count assertion that
            // followed — a stall would surface as "client N did not complete its round-trip
            // budget" with no indication it was a selector hang vs. a client failure, and no
            // stack-trace context attached to the assertion that fails. With io_uring's
            // multi-client hang as the motivating regression, the diagnostic must be the
            // assertion message itself.
            if (stallStartedAt < 0) {
                fail("[multi-client] selector stalled — 10s with no round-trip progress on any client; "
                    + "see stderr above for selector state + thread dump. per-client=" + arrSnapshot(state));
            }
            Thread.sleep(50);
        }
        // Deadline expired without all clients completing. This is also a failure — the
        // earlier loop only returned early on allDone or detected-stall; falling through to
        // here means the budget ran out mid-stream, which the round-trip assertion would
        // otherwise mask as "client N did 4923/5000".
        fail("[multi-client] test deadline " + TEST_DEADLINE_MS + "ms expired with progress="
            + arrSnapshot(state) + "/" + ROUND_TRIPS_PER_CLIENT);
    }

    private static long trackStall(IoUringSelector selector, boolean progressing, long stallStartedAt) {
        if (progressing) return 0;
        long now = System.currentTimeMillis();
        if (stallStartedAt == 0) return now;
        if (now - stallStartedAt > 10_000) {
            System.err.println("[multi-client] STALL detected — 10s without progress");
            dumpSelectorState(selector);
            dumpAllStacks();
            return -1;
        }
        return stallStartedAt;
    }

    private static void dumpAllStacks() {
        Map<Thread, StackTraceElement[]> all = Thread.getAllStackTraces();
        System.err.println("[multi-client] ===== ALL THREAD NAMES =====");
        for (Thread t : all.keySet()) {
            System.err.printf("[thread] '%s' state=%s daemon=%b%n", t.getName(), t.getState(), t.isDaemon());
        }
        System.err.println("[multi-client] ===== THREAD STACKS =====");
        for (Map.Entry<Thread, StackTraceElement[]> e : all.entrySet()) {
            Thread t = e.getKey();
            System.err.printf("[stack] %s state=%s%n", t.getName(), t.getState());
            for (StackTraceElement frame : e.getValue()) {
                System.err.printf("    at %s%n", frame);
            }
        }
        System.err.println("[multi-client] ===== END THREAD DUMP =====");
    }

    private static void joinClients(ClientState[] state) throws InterruptedException {
        for (ClientState st : state) st.thread.join(5_000);
    }

    private static void assertAllClientsCompleted(ClientState[] state) {
        for (ClientState st : state) {
            if (st.failure != null) {
                throw new AssertionError("client " + st.idx + " failed", st.failure);
            }
            assertEquals(ROUND_TRIPS_PER_CLIENT, st.progress.get(),
                "client " + st.idx + " did not complete its round-trip budget");
        }
    }

    private static final class ClientState {
        final int idx;
        final AtomicLong progress = new AtomicLong();
        Thread thread;
        volatile Throwable failure;

        ClientState(int idx) {
            this.idx = idx;
        }
    }

    private static String arrSnapshot(ClientState[] state) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < state.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(state[i].progress.get());
        }
        return sb.append("]").toString();
    }

    private static void echoLoop(IoUringSelector selector, AtomicBoolean stop) {
        // Mirror the production Processor mute/unmute pair: a request mutes its channel
        // until the response is flushed, then completedSends unmutes it. Without this the
        // bench drives selector.send() back-to-back while the channel state machine still
        // thinks it's READY-for-receive — masking any selector bug that only surfaces in
        // the muted/unmuted transition (the io_uring sustained-traffic hang lived in that
        // gap). See SocketServer.Processor.processCompletedReceives/processCompletedSends.
        try {
            while (!stop.get()) {
                selector.poll(50);
                for (NetworkReceive recv : selector.completedReceives()) {
                    String id = recv.source();
                    selector.mute(id);
                    ByteBuffer payload = recv.payload();
                    ByteBuffer copy = ByteBuffer.allocate(payload.remaining());
                    copy.put(payload);
                    copy.flip();
                    selector.send(new NetworkSend(id, ByteBufferSend.sizePrefixed(copy)));
                }
                for (NetworkSend sent : selector.completedSends()) {
                    selector.unmute(sent.destinationId());
                }
                selector.clearCompletedReceives();
                selector.clearCompletedSends();
            }
        } catch (Throwable t) {
            t.printStackTrace(System.err);
        }
    }

    private static void dumpSelectorState(IoUringSelector selector) {
        for (org.apache.kafka.common.network.KafkaChannel kc : selector.channels()) {
            dumpChannelState(selector, kc);
        }
    }

    private static void dumpChannelState(IoUringSelector selector, org.apache.kafka.common.network.KafkaChannel kc) {
        Channel netty = selector.nettyChannelFor(kc.id());
        IoUringTransportLayer transport = selector.transportFor(kc.id());
        String autoRead = nettyFlag(netty, n -> Boolean.toString(n.config().isAutoRead()));
        String writable = nettyFlag(netty, n -> Boolean.toString(n.isWritable()));
        String active = nettyFlag(netty, n -> Boolean.toString(n.isActive()));
        long bbu = netty == null ? -1 : netty.bytesBeforeUnwritable();
        long inboundB = transport == null ? -1 : transport.inboundBytesSnapshot();
        int inboundQ = transport == null ? -1 : transport.inboundQueueDepth();
        long pendingOut = transport == null ? -1 : transport.pendingWriteBytesSnapshot();
        long totalIn = transport == null ? -1 : transport.totalInboundBytesSnapshot();
        long offerCalls = transport == null ? -1 : transport.offerInboundCallsSnapshot();
        System.err.printf("  [chan=%s] autoRead=%s writable=%s active=%s bbu=%d "
                + "muted=%s hasSend=%s inboundB=%d inboundQ=%d pendingOut=%d "
                + "totalIn=%d offers=%d%n",
            kc.id(), autoRead, writable, active, bbu, kc.isMuted(), kc.hasSend(),
            inboundB, inboundQ, pendingOut, totalIn, offerCalls);
    }

    private static String nettyFlag(Channel netty, java.util.function.Function<Channel, String> reader) {
        return netty == null ? "null" : reader.apply(netty);
    }
}
