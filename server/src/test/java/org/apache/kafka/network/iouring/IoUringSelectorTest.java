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
import org.apache.kafka.common.network.ChannelState;
import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link IoUringSelector}.
 *
 * <p>These tests drive the selector with {@link EmbeddedChannel}s and the package-private
 * event-loop callbacks ({@code onAccept}, {@code onRead}, {@code onDisconnect},
 * {@code onWritabilityChanged}). Real Netty bind / accept is exercised separately in
 * {@code IoUringSelectorIT} — keeping the unit surface synchronous and deterministic.
 *
 * <p>Time is controlled with {@link MockTime} so idle-expiry assertions don't race.
 */
class IoUringSelectorTest {

    private static final ListenerName LISTENER = ListenerName.normalised("PLAINTEXT");
    private static final InetSocketAddress REMOTE_A = new InetSocketAddress("198.51.100.7", 51234);
    private static final InetSocketAddress REMOTE_B = new InetSocketAddress("198.51.100.8", 51235);
    private static final InetSocketAddress LOCAL = new InetSocketAddress("203.0.113.1", 9092);
    private static final int MAX_RECEIVE = 1 << 20;
    private static final long IDLE_NANOS_NEVER = TimeUnit.HOURS.toNanos(1);

    private final MockTime time = new MockTime();
    private final List<EmbeddedChannel> channels = new ArrayList<>();
    private IoUringSelector selector;

    @AfterEach
    void tearDown() {
        if (selector != null) selector.close();
        for (EmbeddedChannel ch : channels) {
            if (ch.isOpen()) ch.close();
        }
    }

    private IoUringSelector newSelector(long maxIdleNanos) {
        selector = new IoUringSelector(LISTENER, MAX_RECEIVE, MemoryPool.NONE, maxIdleNanos, time);
        return selector;
    }

    private EmbeddedChannel acceptNew(IoUringSelector s, InetSocketAddress remote) {
        EmbeddedChannel ch = new EmbeddedChannel();
        channels.add(ch);
        s.onAccept(ch, remote, LOCAL);
        return ch;
    }

    private static ByteBuf framed(String payload) {
        // Kafka wire framing: 4-byte big-endian size + payload bytes.
        byte[] body = payload.getBytes();
        ByteBuffer buf = ByteBuffer.allocate(4 + body.length);
        buf.putInt(body.length);
        buf.put(body);
        buf.flip();
        return Unpooled.wrappedBuffer(buf);
    }

    @Test
    void pollWithNoChannelsReturnsEmptyOutputs() throws Exception {
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);

        s.poll(0);

        assertTrue(s.channels().isEmpty());
        assertTrue(s.connected().isEmpty());
        assertTrue(s.completedReceives().isEmpty());
        assertTrue(s.completedSends().isEmpty());
        assertTrue(s.disconnected().isEmpty());
    }

    @Test
    void acceptedChannelSurfacesInConnectedListAndChannelsMap() throws Exception {
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        EmbeddedChannel netty = acceptNew(s, REMOTE_A);
        assertTrue(netty.isOpen(), "sanity: EmbeddedChannel is open after accept");

        s.poll(0);

        assertEquals(1, s.connected().size(), "newly accepted channel must appear in connected()");
        String id = s.connected().get(0);
        KafkaChannel channel = s.channel(id);
        assertNotNull(channel);
        assertEquals(1, s.channels().size());
        // Channel is PLAINTEXT, no handshake: must be ready immediately.
        assertTrue(channel.ready(), "PLAINTEXT channel is ready after accept");
        assertTrue(s.isChannelReady(id));

        // connected() is per-poll output: a second poll with no new accepts must clear it.
        s.poll(0);
        assertTrue(s.connected().isEmpty(), "connected() must clear between polls");
        assertEquals(1, s.channels().size(), "channels() persists across polls");
    }

    @Test
    void inboundBytesProduceACompletedReceive() throws Exception {
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        EmbeddedChannel netty = acceptNew(s, REMOTE_A);
        s.poll(0); // accept

        s.onRead(netty, framed("hello-io_uring"));
        s.poll(0);

        assertEquals(1, s.completedReceives().size(), "one full size-prefixed frame -> one completedReceive");
        NetworkReceive r = s.completedReceives().iterator().next();
        byte[] body = new byte[r.payload().remaining()];
        r.payload().get(body);
        assertEquals("hello-io_uring", new String(body));

        // Per Selector contract: completedReceives drains across polls (caller clears, or poll resets).
        s.poll(0);
        assertTrue(s.completedReceives().isEmpty());
    }

    @Test
    void atMostOneCompletedReceivePerChannelPerPoll() throws Exception {
        // Mirrors org.apache.kafka.common.network.Selector's "one completed receive per channel
        // per poll" invariant — the broker's request-handling expects that fairness guarantee.
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        EmbeddedChannel netty = acceptNew(s, REMOTE_A);
        s.poll(0);

        s.onRead(netty, framed("one"));
        s.onRead(netty, framed("two"));

        s.poll(0);
        assertEquals(1, s.completedReceives().size(), "first poll: only one receive surfaces");

        s.poll(0);
        assertEquals(1, s.completedReceives().size(), "second poll: the next receive surfaces");
    }

    @Test
    void sendIsWrittenToTheNettyOutboundAndCompletes() throws Exception {
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        EmbeddedChannel netty = acceptNew(s, REMOTE_A);
        s.poll(0);

        String id = s.connected().get(0);
        ByteBuffer body = ByteBuffer.wrap("ack".getBytes());
        s.send(new NetworkSend(id, ByteBufferSend.sizePrefixed(body)));

        s.poll(0);

        assertEquals(1, s.completedSends().size(), "one ByteBufferSend -> one completedSend");
        assertEquals(id, s.completedSends().get(0).destinationId());

        // The send must have hit the Netty channel's outbound buffer.
        ByteBuf wire = collectAllOutbound(netty);
        assertEquals(4 + 3, wire.readableBytes(), "wire payload = 4-byte size prefix + 'ack'");
        assertEquals(3, wire.readInt());
        byte[] tail = new byte[3];
        wire.readBytes(tail);
        assertEquals("ack", new String(tail));
        wire.release();
    }

    @Test
    void muteSuppressesReadsUntilUnmute() throws Exception {
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        EmbeddedChannel netty = acceptNew(s, REMOTE_A);
        s.poll(0);
        String id = s.connected().get(0);

        s.mute(id);
        s.onRead(netty, framed("muted"));
        s.poll(0);
        assertTrue(s.completedReceives().isEmpty(), "muted channel must not surface receives");

        // autoRead on the Netty side must reflect mute, so the kernel stops pulling bytes.
        assertFalse(netty.config().isAutoRead(), "muting flips Netty autoRead off");

        s.unmute(id);
        s.poll(0);
        assertEquals(1, s.completedReceives().size(), "unmute drains buffered bytes on next poll");
        assertTrue(netty.config().isAutoRead(), "unmute restores autoRead");
    }

    @Test
    void muteAllAndUnmuteAllAffectEveryChannel() throws Exception {
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        EmbeddedChannel a = acceptNew(s, REMOTE_A);
        EmbeddedChannel b = acceptNew(s, REMOTE_B);
        s.poll(0);

        s.muteAll();
        assertFalse(a.config().isAutoRead());
        assertFalse(b.config().isAutoRead());

        s.unmuteAll();
        assertTrue(a.config().isAutoRead());
        assertTrue(b.config().isAutoRead());
    }

    @Test
    void disconnectFromEventLoopSurfacesAsLocalCloseAfterDrainingFinalBytes() throws Exception {
        // A peer-closed connection with bytes still in the queue must deliver those bytes
        // BEFORE we declare the channel disconnected. Mirrors NIO Selector's closingChannels
        // semantics — otherwise short-lived clients lose their final request.
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        EmbeddedChannel netty = acceptNew(s, REMOTE_A);
        s.poll(0);
        String id = s.connected().get(0);

        s.onRead(netty, framed("final-frame"));
        s.onDisconnect(netty);
        s.poll(0);

        assertEquals(1, s.completedReceives().size(),
            "buffered bytes must be delivered before the disconnect is reported");
        assertTrue(s.disconnected().containsKey(id), "disconnect surfaces on the same poll");
        assertNull(s.channel(id), "channel is gone from the active map");
    }

    @Test
    void idleConnectionsExpireOnlyAfterMaxIdleNanos() throws Exception {
        long idle = TimeUnit.MILLISECONDS.toNanos(100);
        IoUringSelector s = newSelector(idle);
        acceptNew(s, REMOTE_A);
        s.poll(0); // accept; lastActive = now
        String id = s.connected().get(0);
        assertTrue(s.disconnected().isEmpty(), "fresh channel must not be expired");

        time.sleep(50);
        s.poll(0);
        assertTrue(s.disconnected().isEmpty(), "channel within idle window stays connected");

        time.sleep(200);
        s.poll(0);
        assertTrue(s.disconnected().containsKey(id), "channel past idle window must expire");
        assertEquals(ChannelState.State.EXPIRED, s.disconnected().get(id).state());
    }

    @Test
    void readActivityKeepsAChannelAliveAgainstIdleExpiry() throws Exception {
        long idle = TimeUnit.MILLISECONDS.toNanos(100);
        IoUringSelector s = newSelector(idle);
        EmbeddedChannel netty = acceptNew(s, REMOTE_A);
        s.poll(0);
        String id = s.connected().get(0);

        // Tick under idle, feed a frame -> lastActive bumps on the Processor's read.
        time.sleep(80);
        s.onRead(netty, framed("alive"));
        s.poll(0);
        assertEquals(1, s.completedReceives().size());

        // Another 80ms — total elapsed since accept is 160ms > 100ms, but last read was 80ms ago.
        time.sleep(80);
        s.poll(0);
        assertTrue(s.disconnected().isEmpty(), "channel that read recently must not be expired");
        assertNotNull(s.channel(id));
    }

    @Test
    void closeUnregistersAllChannelsAndIsIdempotent() {
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        acceptNew(s, REMOTE_A);
        acceptNew(s, REMOTE_B);

        s.close();
        assertTrue(s.channels().isEmpty(), "close releases all channels");
        s.close(); // idempotent
    }

    @Test
    void closeByIdRemovesTheChannel() throws Exception {
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        acceptNew(s, REMOTE_A);
        s.poll(0);
        String id = s.connected().get(0);
        assertNotNull(s.channel(id));

        s.close(id);
        assertNull(s.channel(id));
    }

    @Test
    void registerThrowsForIoUringListener() {
        // io_uring listener accepts directly via SO_REUSEPORT; the Acceptor path that hands
        // a SocketChannel to register() is not used. Throwing makes the wiring mismatch loud.
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        assertThrows(UnsupportedOperationException.class,
            () -> s.register("ignored", null));
    }

    @Test
    void connectThrowsForBrokerSideSelector() {
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        assertThrows(UnsupportedOperationException.class,
            () -> s.connect("ignored", REMOTE_A, 0, 0));
    }

    @Test
    void wakeupUnblocksALongPoll() throws Exception {
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        AtomicReference<Exception> err = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                s.poll(TimeUnit.SECONDS.toMillis(30));
            } catch (Exception e) {
                err.set(e);
            }
        }, "test-poller");
        t.start();
        // Give the poll a moment to start waiting on the semaphore.
        Thread.sleep(50);
        s.wakeup();
        t.join(TimeUnit.SECONDS.toMillis(5));
        assertFalse(t.isAlive(), "wakeup() must unblock poll() promptly");
        assertNull(err.get(), () -> "poll threw: " + err.get());
    }

    @Test
    void onWritabilityChangedSignalsThePoller() throws Exception {
        // If a downstream send filled Netty's outbound buffer (isWritable=false), the
        // Processor's poll could be stuck waiting. When writability flips back to true,
        // poll must wake so kafkaChannel.write() retries.
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        EmbeddedChannel netty = acceptNew(s, REMOTE_A);
        s.poll(0);

        AtomicReference<Boolean> returned = new AtomicReference<>(false);
        Thread t = new Thread(() -> {
            try {
                s.poll(TimeUnit.SECONDS.toMillis(30));
                returned.set(true);
            } catch (Exception ignored) { /* noop */ }
        }, "test-poller-2");
        t.start();
        Thread.sleep(50);
        s.onWritabilityChanged(netty);
        t.join(TimeUnit.SECONDS.toMillis(5));
        assertTrue(returned.get(), "onWritabilityChanged must wake a blocking poll");
    }

    private static ByteBuf collectAllOutbound(EmbeddedChannel ch) {
        ByteBuf merged = Unpooled.buffer();
        Object o;
        while ((o = ch.readOutbound()) != null) {
            if (o instanceof ByteBuf) {
                ByteBuf buf = (ByteBuf) o;
                merged.writeBytes(buf);
                buf.release();
            }
        }
        return merged;
    }
}
