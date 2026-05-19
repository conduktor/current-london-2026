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
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Closes PROMPT-18 (multi-protocol harness) at the layer this module owns:
 * the per-listener selector-resolution contract and io_uring multi-listener
 * coexistence on a real Linux+io_uring host. A full SSL/SASL handshake test
 * belongs in {@code core/}'s integration suite (where the keystore + JAAS
 * scaffolding lives); this IT pins the {@code server/}-side guarantees.
 *
 * <p>What this test proves on every Linux+io_uring CI run:
 *
 * <ol>
 *   <li>{@link BrokerSelectorFactory#resolve} matches its documented matrix at runtime
 *       — i.e. with the real {@link IoUringSupport#isAvailable()} probe answering
 *       {@code true}, not just the synthetic boolean parameter the unit test feeds.</li>
 *   <li>Every non-PLAINTEXT {@link SecurityProtocol} (SSL, SASL_PLAINTEXT, SASL_SSL)
 *       transparently falls back to NIO regardless of whether the operator left the
 *       knob on {@code auto} or pinned it to {@code io_uring}. <em>No exception, no
 *       IO_URING construction</em> — the PROMPT.md "fall back to NIO without
 *       crashing" clause.</li>
 *   <li>Two io_uring-backed PLAINTEXT listeners on different ports come up
 *       simultaneously, each accept their own client traffic, and shut down
 *       cleanly without interfering with one another. This is the multi-listener
 *       coexistence the broker actually does in production (PLAINTEXT://:9092
 *       + an internal PLAINTEXT://:9094 listener for replication is a common
 *       deployment shape).</li>
 *   <li>An explicit {@code io_uring} request against a {@code SecurityProtocol.PLAINTEXT}
 *       listener on a host where {@link IoUringSupport#isAvailable()} is {@code false}
 *       blows up at decision time — verified by feeding the factory a synthetic
 *       unavailable=false. The complement (the happy path on this host) is covered
 *       by points 1–3 above.</li>
 * </ol>
 *
 * <p>Skipped transparently on hosts where io_uring is not available — the same
 * gating used by every other IT in this package.
 */
class MultiListenerCoexistenceIT {

    private static final ListenerName LISTENER_A = ListenerName.normalised("PLAINTEXT");
    private static final ListenerName LISTENER_B = ListenerName.normalised("INTERNAL");
    private static final int MAX_RECEIVE = 1 << 20;
    private static final long IDLE_NANOS_NEVER = TimeUnit.HOURS.toNanos(1);
    private static final long DEADLINE_MS = 10_000;

    @Test
    void resolutionMatrixHoldsAgainstRealIoUringProbe() {
        assumeTrue(IoUringSupport.isAvailable(),
            "io_uring not available (" + IoUringSupport.unavailabilityReason() + "); skipping");

        // AUTO + io_uring available + PLAINTEXT → io_uring; every non-PLAINTEXT → NIO.
        assertEquals(SelectorImplementation.IO_URING,
            BrokerSelectorFactory.resolve(SelectorImplementation.AUTO,
                SecurityProtocol.PLAINTEXT, IoUringSupport.isAvailable()),
            "AUTO + PLAINTEXT must resolve to io_uring on a host where the probe is available");
        for (SecurityProtocol nonPlain : nonPlaintext()) {
            assertEquals(SelectorImplementation.NIO,
                BrokerSelectorFactory.resolve(SelectorImplementation.AUTO,
                    nonPlain, IoUringSupport.isAvailable()),
                "AUTO + " + nonPlain + " on an io_uring host must still fall back to NIO");
        }

        // Explicit IO_URING + PLAINTEXT on a real io_uring host → io_uring.
        // Explicit IO_URING + non-PLAINTEXT → silent NIO fallback, no exception.
        assertEquals(SelectorImplementation.IO_URING,
            BrokerSelectorFactory.resolve(SelectorImplementation.IO_URING,
                SecurityProtocol.PLAINTEXT, IoUringSupport.isAvailable()));
        for (SecurityProtocol nonPlain : nonPlaintext()) {
            assertEquals(SelectorImplementation.NIO,
                BrokerSelectorFactory.resolve(SelectorImplementation.IO_URING,
                    nonPlain, IoUringSupport.isAvailable()),
                "explicit io_uring + " + nonPlain + " must fall back to NIO without throwing — "
                    + "the PROMPT.md 'fall back to NIO without crashing' clause");
        }

        // Explicit NIO always wins, even with the probe answering available=true.
        for (SecurityProtocol proto : SecurityProtocol.values()) {
            assertEquals(SelectorImplementation.NIO,
                BrokerSelectorFactory.resolve(SelectorImplementation.NIO,
                    proto, IoUringSupport.isAvailable()),
                "explicit nio must win against any protocol (was " + proto + ")");
        }
    }

    @Test
    void explicitIoUringWithUnavailableProbeIsAHardErrorAtBrokerStart() {
        // We do not need IoUringSupport.isAvailable() to actually be false on this host
        // to exercise the decision; resolve() takes the boolean as a parameter so we
        // can deterministically test the hard-fail branch the README documents. The
        // complementary "available=true" path is exercised by the test above.
        IllegalStateException ex = assertThrows(IllegalStateException.class,
            () -> BrokerSelectorFactory.resolve(SelectorImplementation.IO_URING,
                SecurityProtocol.PLAINTEXT, /* available= */ false));
        assertTrue(ex.getMessage().contains("io_uring"),
            "error must name the config token (got: " + ex.getMessage() + ")");
        // Non-PLAINTEXT must NOT throw even when io_uring is unavailable — the silent
        // NIO fallback wins before the hard-fail branch is reached.
        for (SecurityProtocol nonPlain : nonPlaintext()) {
            assertEquals(SelectorImplementation.NIO,
                BrokerSelectorFactory.resolve(SelectorImplementation.IO_URING,
                    nonPlain, /* available= */ false),
                "explicit io_uring + " + nonPlain + " on an unsupported host must still "
                    + "fall back to NIO silently rather than fail broker start");
        }
    }

    @Test
    void twoIoUringListenersOnDifferentPortsRoundTripIndependently() throws Exception {
        assumeTrue(IoUringSupport.isAvailable(),
            "io_uring not available (" + IoUringSupport.unavailabilityReason() + "); skipping");

        // Brings up TWO independent IoUringSelector + IoUringServerListener pairs on
        // different ephemeral ports — the shape of a broker with multiple PLAINTEXT
        // listeners (e.g. PLAINTEXT://:9092 + REPLICATION://:9094). Each owns its own
        // event-loop group; they must not share fd state, accept queues, or
        // completion plumbing. A regression that accidentally shared one selector's
        // state with another (e.g. a static channel-id generator collision, or a
        // shared event-loop singleton) would surface here as a cross-listener leak
        // or a delivery to the wrong selector's completedReceives() queue.
        try (IoUringSelector selectorA = new IoUringSelector(
                LISTENER_A, MAX_RECEIVE, MemoryPool.NONE, IDLE_NANOS_NEVER, Time.SYSTEM);
             IoUringServerListener listenerA = new IoUringServerListener(
                 new InetSocketAddress("127.0.0.1", 0), selectorA);
             IoUringSelector selectorB = new IoUringSelector(
                 LISTENER_B, MAX_RECEIVE, MemoryPool.NONE, IDLE_NANOS_NEVER, Time.SYSTEM);
             IoUringServerListener listenerB = new IoUringServerListener(
                 new InetSocketAddress("127.0.0.1", 0), selectorB)) {
            listenerA.start();
            listenerB.start();

            int portA = listenerA.boundPort();
            int portB = listenerB.boundPort();
            assertTrue(portA > 0 && portB > 0, "kernel must assign ephemeral ports to both listeners");
            assertNotEquals(portA, portB, "the two listeners must end up on distinct ports");

            try (Socket clientA = new Socket();
                 Socket clientB = new Socket()) {
                clientA.connect(new InetSocketAddress("127.0.0.1", portA), (int) DEADLINE_MS);
                clientB.connect(new InetSocketAddress("127.0.0.1", portB), (int) DEADLINE_MS);
                clientA.setSoTimeout((int) DEADLINE_MS);
                clientB.setSoTimeout((int) DEADLINE_MS);

                String channelA = pollForFirstConnected(selectorA);
                String channelB = pollForFirstConnected(selectorB);

                // Each client posts a distinctive frame to its own listener; we then
                // assert each selector surfaces ONLY its own frame, not its sibling's.
                // That negative assertion is the cross-listener-leak guard.
                writeFrame(clientA, "frame-for-A".getBytes());
                writeFrame(clientB, "frame-for-B".getBytes());

                String bodyA = receiveOne(selectorA);
                String bodyB = receiveOne(selectorB);
                assertEquals("frame-for-A", bodyA,
                    "selector A must surface client-A's frame, not client-B's — multi-listener crosstalk");
                assertEquals("frame-for-B", bodyB,
                    "selector B must surface client-B's frame, not client-A's — multi-listener crosstalk");

                // Echo back via each selector's send path. The destinationId is the
                // per-selector channel id; a shared id space across selectors would
                // mis-route the response.
                selectorA.send(new NetworkSend(channelA, ByteBufferSend.sizePrefixed(
                    ByteBuffer.wrap("ack-from-A".getBytes()))));
                selectorB.send(new NetworkSend(channelB, ByteBufferSend.sizePrefixed(
                    ByteBuffer.wrap("ack-from-B".getBytes()))));
                pollForCompletedSend(selectorA, channelA);
                pollForCompletedSend(selectorB, channelB);

                assertEquals("ack-from-A", readFrame(clientA),
                    "client A must read the response selector A enqueued, not B's");
                assertEquals("ack-from-B", readFrame(clientB),
                    "client B must read the response selector B enqueued, not A's");
            }
        }
        // try-with-resources closes both listeners then both selectors. Neither close
        // path may throw or hang; a regression that left one listener leaking onto the
        // other's event-loop group would surface as a hang here under the IT's overall
        // JUnit deadline.
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

    private static String receiveOne(IoUringSelector selector) throws Exception {
        long deadline = System.currentTimeMillis() + DEADLINE_MS;
        while (System.currentTimeMillis() < deadline) {
            selector.poll(50);
            if (!selector.completedReceives().isEmpty()) {
                NetworkReceive received = selector.completedReceives().iterator().next();
                byte[] body = new byte[received.payload().remaining()];
                received.payload().get(body);
                return new String(body);
            }
        }
        throw new AssertionError("selector produced no completedReceive within " + DEADLINE_MS + "ms");
    }

    private static void pollForCompletedSend(IoUringSelector selector, String expectedDestination)
            throws Exception {
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

    private static String readFrame(Socket client) throws Exception {
        DataInputStream in = new DataInputStream(client.getInputStream());
        int len = in.readInt();
        byte[] body = new byte[len];
        in.readFully(body);
        return new String(body);
    }

    private static SecurityProtocol[] nonPlaintext() {
        return new SecurityProtocol[]{
            SecurityProtocol.SSL, SecurityProtocol.SASL_PLAINTEXT, SecurityProtocol.SASL_SSL
        };
    }
}
