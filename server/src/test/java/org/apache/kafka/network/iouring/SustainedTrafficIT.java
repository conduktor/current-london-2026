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
import java.util.concurrent.TimeUnit;

import io.netty.channel.Channel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Regression test: io_uring selector must keep echoing under sustained loopback
 * traffic for many round-trips, not just one. SelectorThroughputBenchmark
 * exposed a hang where ~thousands of receives are processed and then the
 * selector silently stops surfacing inbound bytes. The single-round-trip
 * {@link IoUringServerListenerIT} did not catch it.
 */
class SustainedTrafficIT {

    private static final ListenerName LISTENER = ListenerName.normalised("PLAINTEXT");
    private static final int MAX_RECEIVE = 1 << 20;
    private static final long IDLE_NANOS_NEVER = TimeUnit.HOURS.toNanos(1);
    private static final long DEADLINE_MS = 30_000;
    private static final int ROUND_TRIPS = 20_000;

    @Test
    void roundTripsManyEchoesWithoutStalling() throws Exception {
        assumeTrue(IoUringSupport.isAvailable(),
            "io_uring not available (" + IoUringSupport.unavailabilityReason() + "); skipping");

        try (IoUringSelector selector = new IoUringSelector(
                LISTENER, MAX_RECEIVE, MemoryPool.NONE, IDLE_NANOS_NEVER, Time.SYSTEM);
             IoUringServerListener listener = new IoUringServerListener(
                 new InetSocketAddress("127.0.0.1", 0), selector)) {
            listener.start();
            int port = listener.boundPort();

            try (Socket client = new Socket()) {
                client.connect(new InetSocketAddress("127.0.0.1", port), (int) DEADLINE_MS);
                client.setTcpNoDelay(true);
                client.setSoTimeout(5_000);
                DataInputStream in = new DataInputStream(client.getInputStream());
                DataOutputStream out = new DataOutputStream(client.getOutputStream());

                // Need a server thread to drive the selector while we drive the client.
                Thread server = new Thread(() -> echoLoop(selector), "iouring-echo");
                server.setDaemon(true);
                server.start();
                try {
                    byte[] payload = new byte[64];
                    for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i & 0xff);
                    byte[] recv = new byte[payload.length];
                    long started = System.nanoTime();
                    int lastReported = 0;
                    for (int n = 0; n < ROUND_TRIPS; n++) {
                        out.writeInt(payload.length);
                        out.write(payload);
                        out.flush();
                        int respLen = in.readInt();
                        assertEquals(payload.length, respLen, "frame " + n + " wrong size");
                        in.readFully(recv);
                        if ((n & 0x3FF) == 0 && n != lastReported) {
                            long elapsedNs = System.nanoTime() - started;
                            System.err.printf("[client] round-trip %d ok at %dms%n",
                                n, elapsedNs / 1_000_000);
                            dumpSelectorState(selector);
                            lastReported = n;
                        }
                    }
                    System.err.printf("[client] completed %d round-trips%n", ROUND_TRIPS);
                } finally {
                    server.interrupt();
                    server.join(2_000);
                }
            }
        }
    }

    private static void echoLoop(IoUringSelector selector) {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                selector.poll(50);
                for (NetworkReceive recv : selector.completedReceives()) {
                    String id = recv.source();
                    ByteBuffer payload = recv.payload();
                    ByteBuffer copy = ByteBuffer.allocate(payload.remaining());
                    copy.put(payload);
                    copy.flip();
                    selector.send(new NetworkSend(id, ByteBufferSend.sizePrefixed(copy)));
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
            Channel netty = selector.nettyChannelFor(kc.id());
            String autoRead = netty == null ? "null" : Boolean.toString(netty.config().isAutoRead());
            String writable = netty == null ? "null" : Boolean.toString(netty.isWritable());
            String active = netty == null ? "null" : Boolean.toString(netty.isActive());
            long bbu = netty == null ? -1 : netty.bytesBeforeUnwritable();
            System.err.printf("  [chan=%s] autoRead=%s writable=%s active=%s bytesBeforeUnwritable=%d "
                    + "muted=%s hasSend=%s%n",
                kc.id(), autoRead, writable, active, bbu, kc.isMuted(), kc.hasSend());
        }
    }
}
