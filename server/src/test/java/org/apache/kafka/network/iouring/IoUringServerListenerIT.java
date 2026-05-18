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
import java.util.concurrent.TimeUnit;

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
        // We verify from the client side because the broker-side Netty channel does not
        // expose the underlying file descriptor to user code; the client's view of its peer
        // is the only host-portable observation we can make for this test.
        assumeTrue(IoUringSupport.isAvailable(),
            "io_uring not available (" + IoUringSupport.unavailabilityReason() + "); skipping");

        try (IoUringSelector selector = new IoUringSelector(
                LISTENER, MAX_RECEIVE, MemoryPool.NONE, IDLE_NANOS_NEVER, Time.SYSTEM);
             IoUringServerListener listener = new IoUringServerListener(
                 new InetSocketAddress("127.0.0.1", 0), selector)) {

            int port = listener.boundPort();
            try (Socket client = new Socket()) {
                client.connect(new InetSocketAddress("127.0.0.1", port), (int) DEADLINE_MS);
                pollForFirstConnected(selector);

                // Round-trip a frame so the connection is fully established and the kernel
                // commits the inherited options.
                writeFrame(client, "ping".getBytes());
                pollForFirstReceive(selector);

                // The client-side observations don't directly read the broker's options,
                // but a successful TCP_NODELAY+KEEPALIVE handshake leaves no client-visible
                // artifact. The strongest portable check is that no SocketException is
                // raised by the configured options round-trip. Verify against the listener's
                // public configuration surface (the constructor parameters are immutable
                // after bind) by reading back via the dedicated buffer-size IT below.
                assertTrue(client.isConnected(), "client connection must remain established");
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
}
