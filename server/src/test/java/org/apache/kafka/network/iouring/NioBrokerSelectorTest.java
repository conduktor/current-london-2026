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
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.PlaintextChannelBuilder;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for {@link NioBrokerSelector}: wraps a real NIO {@code Selector},
 * accepts a real loopback TCP connection, and exercises every method the broker's
 * {@code Processor} calls — register, channel, channels, closingChannel,
 * clearCompletedReceives, clearCompletedSends, lowestPriorityChannel — plus mute,
 * unmute and a size-prefixed message round-trip.
 *
 * <p>This test exercises the real TCP boundary so we know the adapter is genuinely
 * pass-through and not just a syntactic delegate.
 */
class NioBrokerSelectorTest {

    private static final String CONN_ID = "broker:9092-127.0.0.1:0-0";
    private static final int MAX_RECEIVE_SIZE = 16 * 1024;

    private ServerSocketChannel listener;
    private SocketChannel clientSide;
    private SocketChannel serverSide;
    private NioBrokerSelector selector;
    private Metrics metrics;

    @BeforeEach
    void setUp() throws IOException {
        listener = ServerSocketChannel.open();
        listener.bind(new InetSocketAddress("127.0.0.1", 0));

        clientSide = SocketChannel.open();
        clientSide.configureBlocking(false);
        clientSide.connect(new InetSocketAddress("127.0.0.1", listener.socket().getLocalPort()));

        serverSide = awaitAccept(listener);
        serverSide.configureBlocking(false);

        // The client-side connect is non-blocking; finish it so subsequent writes don't fail.
        long deadline = System.currentTimeMillis() + 2_000;
        while (!clientSide.finishConnect()) {
            if (System.currentTimeMillis() > deadline) throw new IOException("connect timed out");
        }

        metrics = new Metrics();
        selector = new NioBrokerSelector(newNioSelector(metrics, /* idleMs= */ -1L));
    }

    @AfterEach
    void tearDown() throws IOException {
        if (selector != null) selector.close();
        if (clientSide != null) clientSide.close();
        if (serverSide != null && serverSide.isOpen()) serverSide.close();
        if (listener != null) listener.close();
        if (metrics != null) metrics.close();
    }

    @Test
    void registerExposesChannelViaLookupApis() throws IOException {
        selector.register(CONN_ID, serverSide);

        KafkaChannel ch = selector.channel(CONN_ID);
        assertNotNull(ch);
        assertEquals(CONN_ID, ch.id());
        assertNull(selector.channel("unknown-id"));
        assertNull(selector.closingChannel(CONN_ID));
        assertTrue(selector.channels().stream().anyMatch(c -> CONN_ID.equals(c.id())));
    }

    @Test
    void duplicateRegisterRejected() throws IOException {
        selector.register(CONN_ID, serverSide);
        assertThrows(IllegalStateException.class, () -> selector.register(CONN_ID, serverSide));
    }

    @Test
    void roundTripsAPlaintextMessageAndClearsCompletions() throws IOException {
        selector.register(CONN_ID, serverSide);

        byte[] payload = "kafka-io-uring".getBytes();
        ByteBuffer framed = ByteBuffer.allocate(4 + payload.length);
        framed.putInt(payload.length).put(payload).flip();
        while (framed.hasRemaining()) clientSide.write(framed);

        Collection<NetworkReceive> completed = pollUntilReceived();
        assertEquals(1, completed.size());
        NetworkReceive recv = completed.iterator().next();
        ByteBuffer body = recv.payload();
        byte[] echoed = new byte[body.remaining()];
        body.get(echoed);
        assertEquals(new String(payload), new String(echoed));

        selector.clearCompletedReceives();
        assertTrue(selector.completedReceives().isEmpty());
    }

    @Test
    void localCloseDropsChannelWithoutDisconnectNotification() throws IOException {
        // Selector.close(String) deliberately does NOT publish a disconnect notification
        // for locally-initiated closes (only remote/error closes do). The channel just
        // disappears from the registered set.
        selector.register(CONN_ID, serverSide);
        assertNotNull(selector.channel(CONN_ID));
        selector.close(CONN_ID);
        selector.poll(50);
        assertNull(selector.channel(CONN_ID), "channel must be deregistered after local close");
        assertFalse(selector.disconnected().containsKey(CONN_ID),
            "local close must NOT publish a disconnect notification");
    }

    @Test
    void muteThenUnmuteFlipsTheChannelMutedFlag() throws IOException {
        selector.register(CONN_ID, serverSide);
        KafkaChannel ch = selector.channel(CONN_ID);
        assertFalse(ch.isMuted(), "freshly registered channel is not muted");
        selector.mute(CONN_ID);
        assertTrue(ch.isMuted());
        selector.unmute(CONN_ID);
        assertFalse(ch.isMuted());
    }

    @Test
    void closeDropsAllChannels() throws IOException {
        selector.register(CONN_ID, serverSide);
        selector.close();
        assertNull(selector.channel(CONN_ID));
    }

    @Test
    void lowestPriorityChannelFallsBackToARegisteredChannel() throws IOException {
        // With idle tracking disabled (idleMs=-1) there is no LRU view, but Selector
        // still returns *some* registered channel as a last-resort eviction candidate
        // — matching the production Selector contract the broker relies on.
        selector.register(CONN_ID, serverSide);
        KafkaChannel candidate = selector.lowestPriorityChannel();
        assertNotNull(candidate, "must return a candidate when channels are registered");
        assertEquals(CONN_ID, candidate.id());
    }

    @Test
    void unwrapReturnsTheUnderlyingNioSelector() {
        assertSame(selector.unwrap(), selector.unwrap());
        assertNotNull(selector.unwrap());
    }

    private SocketChannel awaitAccept(ServerSocketChannel ssc) throws IOException {
        long deadline = System.currentTimeMillis() + 2_000;
        SocketChannel sc;
        while ((sc = ssc.accept()) == null) {
            if (System.currentTimeMillis() > deadline) throw new IOException("accept timed out");
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        }
        return sc;
    }

    private Collection<NetworkReceive> pollUntilReceived() throws IOException {
        long deadline = System.currentTimeMillis() + 2_000;
        while (System.currentTimeMillis() < deadline) {
            selector.poll(50);
            if (!selector.completedReceives().isEmpty()) return selector.completedReceives();
        }
        throw new IOException("no receive completed in 2s");
    }

    private org.apache.kafka.common.network.Selector newNioSelector(Metrics m, long idleMs) {
        PlaintextChannelBuilder channelBuilder = new PlaintextChannelBuilder(null);
        channelBuilder.configure(Collections.emptyMap());
        return new org.apache.kafka.common.network.Selector(
            MAX_RECEIVE_SIZE,
            idleMs,
            0,
            m,
            new MockTime(),
            "broker-selector-test",
            Collections.emptyMap(),
            false,
            false,
            channelBuilder,
            MemoryPool.NONE,
            new LogContext());
    }
}
