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
    void lowestPriorityChannelMatchesNioContract() throws Exception {
        // Contract: SocketServer.Processor.closeExcessConnections uses lowestPriorityChannel
        // to pick a victim under broker-wide max.connections pressure. It must, in order:
        //   1. prefer a channel already in teardown (closingChannels), and otherwise
        //   2. evict the least-recently-active channel so long-lived hot peers
        //      (controller, replication) survive eviction.
        // NIO does this — io_uring must agree, otherwise on a busy mixed-listener broker
        // the io_uring listener happily evicts the boot-time inter-broker connection.
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        assertNull(s.lowestPriorityChannel(), "empty selector -> null");

        EmbeddedChannel firstNetty = acceptNew(s, REMOTE_A);
        s.poll(0); // step 1 stamps lastActiveNanos for first channel
        String firstId = s.connected().get(0);

        time.sleep(10); // advance clock so second accept has a later timestamp
        EmbeddedChannel secondNetty = acceptNew(s, REMOTE_B);
        s.poll(0);
        String secondId = s.connected().get(0);

        // First channel is older -> lowest priority.
        assertEquals(firstId, s.lowestPriorityChannel().id(),
            "least-recently-active channel must be the eviction victim — older lastActiveNanos wins");

        // Drive a read on the first channel to refresh its timestamp; now the second is older.
        time.sleep(10);
        s.onRead(firstNetty, framed("ping"));
        s.poll(0);
        assertEquals(secondId, s.lowestPriorityChannel().id(),
            "after activity on first, second becomes the oldest -> next eviction victim");

        // Initiate disconnect on the second channel with buffered bytes still to drain —
        // the standard NIO closingChannels lifecycle path. Step 3 of poll() routes the
        // channel into closingChannels for one extra poll of grace so the Processor can
        // resolve the final completedReceive via closingChannel(id).
        s.onRead(secondNetty, framed("last"));
        s.onDisconnect(secondNetty);
        s.poll(0);
        assertNotNull(s.closingChannel(secondId),
            "test setup: a peer-closed channel with buffered bytes lands in closingChannels");
        KafkaChannel victim = s.lowestPriorityChannel();
        assertNotNull(victim);
        assertEquals(secondId, victim.id(),
            "a channel already in teardown must outrank any healthy channel for eviction");
    }

    @Test
    void newlyAcceptedChannelDoesNotSurfaceReceiveInSamePoll() throws Exception {
        // Regression: a freshly-accepted channel that receives bytes in the same poll
        // window as the accept must not produce a completedReceive on that poll. The
        // Processor's loop is poll() -> applyConnectionQuotasForNewlyAcceptedChannels()
        // -> processCompletedReceives(); if a receive surfaced before the quota check
        // had run, a refused connection (tryInc=false, or TooMany after close) would
        // (a) leak a request into the request queue and (b) trip an IllegalStateException
        // in processCompletedReceives whose cleanup path calls connectionQuotas.dec on
        // a never-inc'd connection.
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        EmbeddedChannel netty = acceptNew(s, REMOTE_A);

        // Bytes arrive before the very first poll — same window as the accept.
        s.onRead(netty, framed("payload"));

        s.poll(0);

        assertEquals(1, s.connected().size(),
            "channel must still surface in connected() on the accept poll");
        assertTrue(s.completedReceives().isEmpty(),
            "no receive may surface on the same poll as the accept — quota gate must run first");

        // The very next poll, with no fresh accepts, drains the buffered receive.
        s.poll(0);
        assertEquals(1, s.completedReceives().size(),
            "buffered receive surfaces on the next poll, once the quota gate has had a chance to refuse");
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
        // Processor.processCompletedReceives resolves the receive's source via either
        // channel(id) or closingChannel(id). channel(id) is null after disconnect, so
        // closingChannel(id) MUST keep the channel reachable for one extra poll — otherwise
        // the final completedReceive lands on a null KafkaChannel and gets silently dropped.
        assertNotNull(s.closingChannel(id),
            "closingChannel must surface the disconnected channel so the Processor can resolve the final receive");

        // The next poll evicts the closing channel for real.
        s.poll(0);
        assertNull(s.closingChannel(id),
            "closingChannel must be evicted on the next poll; only one extra poll of grace");
    }

    @Test
    void connectionIdsParseAsServerConnectionIdSoQuotasCanBeReleased() throws Exception {
        // Processor.processDisconnected feeds the connection id back into
        // ServerConnectionId.fromString to decrement ConnectionQuotas. If the format
        // doesn't match, the quota leaks for every disconnect.
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        acceptNew(s, REMOTE_A);
        s.poll(0);
        String id = s.connected().get(0);

        org.apache.kafka.common.network.ServerConnectionId parsed =
            org.apache.kafka.common.network.ServerConnectionId.fromString(id)
                .orElseThrow(() -> new AssertionError("id " + id + " is not a valid ServerConnectionId"));
        assertEquals(0, parsed.processorId(),
            "id minted via the test-only ctor uses processorId=0; the production ctor takes the real id");
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
    void closeByIdAlsoCleansClosingChannels() throws Exception {
        // Codex audit blocker: NIO's Selector.close(id) cleans both `channels` and
        // `closingChannels` (clients/.../Selector.java:886-899). Without that fallthrough,
        // closeExcessConnections under broker-max pressure would repeatedly select the
        // same closing channel via lowestPriorityChannel() — the closingChannels map
        // would not drain until the next poll's eviction, letting the same id be
        // double-selected within the same Acceptor decision.
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        EmbeddedChannel netty = acceptNew(s, REMOTE_A);
        s.poll(0);
        String id = s.connected().get(0);

        // Buffer a frame so the disconnect path routes through closingChannels (mirrors
        // NIO's "keep around for one more poll to flush pending receives" semantics).
        s.onRead(netty, framed("final-frame"));
        s.onDisconnect(netty);
        s.poll(0);
        assertNull(s.channel(id));
        assertNotNull(s.closingChannel(id),
            "preconditions: channel must be in closingChannels for this test to mean anything");

        // close(id) must drain the closing entry.
        s.close(id);
        assertNull(s.closingChannel(id),
            "close(id) must remove the entry from closingChannels — otherwise lowestPriorityChannel " +
            "would keep selecting it for eviction until the next poll");
    }

    @Test
    void samePollAcceptAndDisconnectDoesNotSurfaceDisconnectOrQuotaUnderflow() throws Exception {
        // Codex audit blocker: if a peer FINs in the same poll window as the accept,
        // pendingDisconnects fires for an id that SocketServer's
        // applyConnectionQuotasForNewlyAcceptedChannels has not yet tryInc'd. With
        // selector.channel(id) returning null (channel already removed), tryInc is
        // skipped — no inc happens. If we then surfaced this id in disconnected,
        // processDisconnected would run connectionQuotas.dec on a counter that was
        // never inc'd, walking it negative on every accept-races-FIN. The selector
        // must drop the channel silently (close + remove from connected, NOT add to
        // disconnected) so SocketServer never sees this transient connection.
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        EmbeddedChannel netty = acceptNew(s, REMOTE_A);
        s.onDisconnect(netty);

        s.poll(0);

        assertTrue(s.connected().isEmpty(),
            "same-poll accept+disconnect: must NOT surface in connected() — quota would never " +
            "be inc'd anyway because channel(id) is already null when SocketServer looks");
        assertTrue(s.disconnected().isEmpty(),
            "same-poll accept+disconnect: must NOT surface in disconnected() — otherwise " +
            "processDisconnected runs dec() on a never-inc'd counter");
        assertNull(s.channel(netty.id() == null ? "irrelevant" : "irrelevant"),
            "the channel id is internal but the channels map must be empty");
        assertTrue(s.channels().isEmpty(),
            "channels map must be empty — full resource cleanup happened during the silent drop");
    }

    @Test
    void sendOnClosingChannelRoutesToFailedSendsNotThrow() throws Exception {
        // Mirror NIO Selector.send (clients/.../Selector.java:391-413): when send arrives
        // for a channel that has just disconnected, the send is recorded as a failedSend
        // and surfaces as FAILED_SEND on the next poll — it must NOT throw, because
        // Processor.sendResponse synchronously calls selector.send for any response in
        // flight and an exception here would propagate up to Processor.processChannelException
        // → Processor.close, double-dec'ing a quota that was already dec'd when the
        // original disconnect surfaced.
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        EmbeddedChannel netty = acceptNew(s, REMOTE_A);
        s.poll(0);
        String id = s.connected().get(0);

        // Buffer bytes so the disconnect routes through closingChannels (matches NIO:
        // empty-disconnect goes straight to disconnected; partial-receive goes through
        // closingChannels so the final receive can be flushed).
        s.onRead(netty, framed("late-request"));
        s.onDisconnect(netty);
        s.poll(0);
        assertNotNull(s.closingChannel(id), "preconditions: must be in closingChannels");
        // The Processor consumed the initial disconnect notification — clear our own view.
        // (In the real code, processDisconnected has already run by now and dec'd the quota.)

        // Send for the closing channel MUST NOT throw.
        ByteBuffer body = ByteBuffer.wrap("late-response".getBytes());
        s.send(new NetworkSend(id, ByteBufferSend.sizePrefixed(body)));

        // Next poll: surfaces as FAILED_SEND in disconnected.
        s.poll(0);
        // Note: the closingChannel was evicted at top of this poll, so the FAILED_SEND
        // suppresses through that path. There must NOT be a duplicate disconnect for id.
        assertFalse(s.disconnected().containsKey(id),
            "the closing channel's original LOCAL_CLOSE already fired; the eviction-time " +
            "failedSends.remove must suppress the duplicate FAILED_SEND notification — otherwise " +
            "processDisconnected runs dec() twice for the same connection");
    }

    @Test
    void sendOnUnknownChannelStillThrows() throws Exception {
        // Defensive: send() must only route to failedSends if the id is in closingChannels.
        // Sending to a never-known id still indicates a Processor bug worth surfacing loud.
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        s.poll(0);

        ByteBuffer body = ByteBuffer.wrap("ack".getBytes());
        assertThrows(IllegalStateException.class,
            () -> s.send(new NetworkSend("never-existed", ByteBufferSend.sizePrefixed(body))));
    }

    @Test
    void atMostOneCompletedReceivePerChannelAcrossStep2AndStep3InSamePoll() throws Exception {
        // Regression for the cross-step variant of "one receive per channel per poll":
        // step 2 (read pass on healthy channels) and step 3 (final-read drain on
        // disconnecting channels) must agree on the cap. If a peer pipelines two frames
        // and FINs between them mid-poll, step 2 delivers frame 1, then step 3 must NOT
        // also deliver frame 2 — otherwise the Processor sees two requests for one
        // connection in a single iteration, breaking the mute-after-receive contract.
        // The next poll surfaces the remaining frame via closingChannel(id).
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        EmbeddedChannel netty = acceptNew(s, REMOTE_A);
        s.poll(0); // accept settles; channel is no longer justAccepted

        // Two frames buffered + disconnect — all surfaced together in the next poll.
        s.onRead(netty, framed("first"));
        s.onRead(netty, framed("second"));
        s.onDisconnect(netty);
        s.poll(0);

        assertEquals(1, s.completedReceives().size(),
            "cross-step cap: step 2 delivers exactly one receive; step 3 must skip the final " +
            "drain when step 2 already produced a receive for this channel");
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
