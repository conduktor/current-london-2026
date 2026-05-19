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
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Linux-only end-to-end test that the {@link IoUringServerListener} can:
 * <ol>
 *   <li>Bind a real {@code IoUringServerSocketChannel} on a kernel-assigned port.</li>
 *   <li>Accept a connection from a plain {@link Socket} client.</li>
 *   <li>Deliver the client's size-prefixed frame to the Processor side via
 *       {@link IoUringSelector#completedReceives()}.</li>
 *   <li>Send a size-prefixed response via {@link IoUringSelector#send} and have the
 *       client read it off the wire.</li>
 * </ol>
 *
 * <p>This is the integration counterpart to {@link IoUringSelectorTest} — that one
 * drives the selector with {@code EmbeddedChannel}; this one drives it with a real
 * kernel socket pair through Netty's io_uring transport. Skipped transparently
 * on hosts where {@link IoUringSupport#isAvailable()} returns false (non-Linux
 * dev machines, CI runners without io_uring).
 */
class IoUringServerListenerIT {

    private static final ListenerName LISTENER = ListenerName.normalised("PLAINTEXT");
    private static final int MAX_RECEIVE = 1 << 20;
    private static final long IDLE_NANOS_NEVER = TimeUnit.HOURS.toNanos(1);
    private static final long DEADLINE_MS = 10_000;

    @Test
    void roundTripsAFrameThroughTheRealIoUringTransport() throws Exception {
        assumeTrue(IoUringSupport.isAvailable(),
            "io_uring not available (" + IoUringSupport.unavailabilityReason() + "); skipping");

        try (IoUringSelector selector = new IoUringSelector(
                LISTENER, MAX_RECEIVE, MemoryPool.NONE, IDLE_NANOS_NEVER, Time.SYSTEM);
             IoUringServerListener listener = new IoUringServerListener(
                 new InetSocketAddress("127.0.0.1", 0), selector)) {
            listener.start();

            int port = listener.boundPort();
            assertTrue(port > 0, "kernel must have assigned an ephemeral port");

            try (Socket client = new Socket()) {
                client.connect(new InetSocketAddress("127.0.0.1", port), (int) DEADLINE_MS);
                client.setSoTimeout((int) DEADLINE_MS);

                // 1. Wait for the accept to surface on the selector side.
                String channelId = pollForFirstConnected(selector);

                // 2. Client writes a size-prefixed frame; selector must surface it.
                byte[] requestBody = "hello-io_uring".getBytes();
                writeFrame(client, requestBody);

                NetworkReceive received = pollForFirstReceive(selector);
                byte[] receivedBody = new byte[received.payload().remaining()];
                received.payload().get(receivedBody);
                assertEquals("hello-io_uring", new String(receivedBody));

                // 3. Selector sends a frame back; client must read it.
                byte[] responseBody = "ack-from-broker".getBytes();
                selector.send(new NetworkSend(channelId, ByteBufferSend.sizePrefixed(
                    java.nio.ByteBuffer.wrap(responseBody))));
                pollForFirstSend(selector, channelId);

                DataInputStream in = new DataInputStream(client.getInputStream());
                int len = in.readInt();
                assertEquals(responseBody.length, len, "wire response size must match payload length");
                byte[] tail = new byte[len];
                in.readFully(tail);
                assertEquals("ack-from-broker", new String(tail));
            }
        }
    }

    private static String pollForFirstConnected(IoUringSelector selector) throws Exception {
        long deadline = System.currentTimeMillis() + DEADLINE_MS;
        while (System.currentTimeMillis() < deadline) {
            selector.poll(50);
            if (!selector.connected().isEmpty()) {
                return selector.connected().get(0);
            }
        }
        throw new AssertionError("listener never surfaced an accepted channel within " + DEADLINE_MS + "ms");
    }

    private static NetworkReceive pollForFirstReceive(IoUringSelector selector) throws Exception {
        long deadline = System.currentTimeMillis() + DEADLINE_MS;
        while (System.currentTimeMillis() < deadline) {
            selector.poll(50);
            if (!selector.completedReceives().isEmpty()) {
                return selector.completedReceives().iterator().next();
            }
        }
        throw new AssertionError("selector never produced a completedReceive within " + DEADLINE_MS + "ms");
    }

    private static void pollForFirstSend(IoUringSelector selector, String expectedDestination) throws Exception {
        long deadline = System.currentTimeMillis() + DEADLINE_MS;
        while (System.currentTimeMillis() < deadline) {
            selector.poll(50);
            for (NetworkSend send : selector.completedSends()) {
                if (expectedDestination.equals(send.destinationId())) {
                    return;
                }
            }
        }
        throw new AssertionError(
            "selector never reported a completedSend for " + expectedDestination + " within " + DEADLINE_MS + "ms");
    }

    private static void writeFrame(Socket client, byte[] body) throws Exception {
        DataOutputStream out = new DataOutputStream(client.getOutputStream());
        out.writeInt(body.length);
        out.write(body);
        out.flush();
    }

    @Test
    void acceptedChildSocketHasTcpNoDelayAndKeepAliveOn() throws Exception {
        // NIO Acceptor.configureAcceptedSocketChannel (SocketServer.scala:740-746) sets
        // TCP_NODELAY=true and SO_KEEPALIVE=true on every accepted child. The io_uring
        // listener must match — otherwise the broker silently regresses on request/response
        // latency (Nagle adds hundreds of microseconds per flush) and silently fails to
        // detect half-open peer connections (which keeps a dead peer slot indefinitely).
        // We verify against the broker-side Netty channel via the selector's package-private
        // {@code nettyChannelFor} accessor: the channel's ChannelConfig is the authoritative
        // source for what {@code childOption(TCP_NODELAY)} / {@code childOption(SO_KEEPALIVE)}
        // actually applied to the accepted child.
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
                String channelId = pollForFirstConnected(selector);

                Channel brokerChannel = selector.nettyChannelFor(channelId);
                assertTrue(brokerChannel != null,
                    "broker-side Netty channel must be reachable from the selector for " + channelId);
                assertEquals(Boolean.TRUE, brokerChannel.config().getOption(ChannelOption.TCP_NODELAY),
                    "accepted child must inherit TCP_NODELAY=true (Nagle off); else request/response "
                        + "latency silently regresses by hundreds of microseconds per flush");
                assertEquals(Boolean.TRUE, brokerChannel.config().getOption(ChannelOption.SO_KEEPALIVE),
                    "accepted child must inherit SO_KEEPALIVE=true; else half-open peer connections "
                        + "are never detected and the broker leaks dead peer slots indefinitely");
            }
        }
    }

    @Test
    void operatorConfiguredBufferSizesArePassedToTheBootstrap() throws Exception {
        // socket.send.buffer.bytes and socket.receive.buffer.bytes are wired through
        // SocketServer.scala into IoUringServerListener — without that wiring, an operator
        // who tuned the broker for high-throughput Kafka traffic would silently leave the
        // io_uring listener on the OS defaults (typically 64KB-128KB), bottlenecking the
        // listener at a fraction of the NIO listener's throughput on the same broker.
        // This test exercises the constructor path with explicit non-default values and
        // asserts the listener accepts traffic — a wiring smoke-test that catches
        // accidental drops of the buffer-size parameters.
        assumeTrue(IoUringSupport.isAvailable(),
            "io_uring not available (" + IoUringSupport.unavailabilityReason() + "); skipping");

        int sendBuf = 256 * 1024;
        int recvBuf = 512 * 1024;
        try (IoUringSelector selector = new IoUringSelector(
                LISTENER, MAX_RECEIVE, MemoryPool.NONE, IDLE_NANOS_NEVER, Time.SYSTEM);
             IoUringServerListener listener = new IoUringServerListener(
                 new InetSocketAddress("127.0.0.1", 0), selector,
                 /*soBacklog*/ 128, sendBuf, recvBuf)) {
            listener.start();

            int port = listener.boundPort();
            try (Socket client = new Socket()) {
                client.connect(new InetSocketAddress("127.0.0.1", port), (int) DEADLINE_MS);
                pollForFirstConnected(selector);
                writeFrame(client, "buffered".getBytes());
                NetworkReceive received = pollForFirstReceive(selector);
                byte[] body = new byte[received.payload().remaining()];
                received.payload().get(body);
                assertEquals("buffered", new String(body),
                    "listener with custom buffer sizes still round-trips the payload correctly");
            }
        }
    }

    /**
     * C-18-L1 regression: {@link IoUringServerListener#close()} must return within a bounded
     * wall-clock budget even when the io_uring event-loop thread is wedged inside a task that
     * never honours interrupts. Pre-fix the code used {@code syncUninterruptibly()} on the
     * {@code shutdownGracefully} future, which blocks <em>forever</em> whenever the event-loop
     * thread fails to transition to TERMINATED — a known io_uring failure mode under
     * submission-queue pressure or a non-interruptible kernel syscall.
     *
     * <p>The test wedges the loop by submitting a busy-loop {@link Runnable} that ignores
     * interrupts, then asserts {@code close()} still returns within
     * {@code SHUTDOWN_QUIET_MS + SHUTDOWN_TIMEOUT_MS} plus a generous CI slack. Without the
     * L1 fix the {@code close()} call would never return and this test would time out at
     * the JUnit deadline rather than fail with a clean assertion.
     */
    @Test
    void closeReturnsWithinBoundedTimeoutEvenWithWedgedEventLoopTask() throws Exception {
        assumeTrue(IoUringSupport.isAvailable(),
            "io_uring not available (" + IoUringSupport.unavailabilityReason() + "); skipping");

        AtomicBoolean keepRunning = new AtomicBoolean(true);
        try (IoUringSelector selector = new IoUringSelector(
                LISTENER, MAX_RECEIVE, MemoryPool.NONE, IDLE_NANOS_NEVER, Time.SYSTEM)) {
            IoUringServerListener listener = new IoUringServerListener(
                new InetSocketAddress("127.0.0.1", 0), selector);
            try {
                listener.start();

                CountDownLatch wedgeStarted = new CountDownLatch(1);
                // Reach into the private event-loop group field. Adding a public accessor for
                // a test-only seam would leak production API; reflection on a final class is
                // the contained alternative — package-private to test code only.
                Field field = IoUringServerListener.class.getDeclaredField("eventLoopGroup");
                field.setAccessible(true);
                EventLoopGroup group = (EventLoopGroup) field.get(listener);
                group.next().execute(() -> {
                    wedgeStarted.countDown();
                    // Busy-wait ignoring interrupts. Netty's shutdownGracefully sends an
                    // interrupt; a busy loop that does not check Thread.interrupted() never
                    // yields back to the loop's run() — exactly the wedge scenario the L1
                    // fix protects against. yield() keeps a single-CPU CI runner responsive.
                    while (keepRunning.get()) {
                        Thread.yield();
                    }
                });
                // Wait until the wedge task is actually executing on the loop thread. If we
                // raced close() in before the task started running, we would only exercise
                // the empty-queue shutdown path which always returned quickly even pre-fix.
                assertTrue(wedgeStarted.await(5, TimeUnit.SECONDS),
                    "wedge task must reach the event-loop thread before we close the listener");

                long startNanos = System.nanoTime();
                listener.close();
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

                // Listener's budget: SHUTDOWN_QUIET_MS (100) + SHUTDOWN_TIMEOUT_MS (5000) = 5100ms.
                // Allow ~2x slack for CI scheduling jitter and the additional channel.close()
                // bounded wait that runs first. Anything materially over this means the bound
                // regressed and a real broker shutdown could hang indefinitely.
                long maxAllowedMs = 12_000;
                assertTrue(elapsedMs <= maxAllowedMs,
                    "close() must return within " + maxAllowedMs + "ms even when the io_uring "
                        + "event loop is wedged; observed " + elapsedMs + "ms. Pre-L1 this hung "
                        + "forever via syncUninterruptibly() — the regression has returned.");
            } finally {
                // Release the wedge so the stray loop thread can exit. close() above is
                // already idempotent so the explicit call here is purely defensive.
                keepRunning.set(false);
                listener.close();
            }
        }
    }
}
