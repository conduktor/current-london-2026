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
import org.apache.kafka.common.memory.SimpleMemoryPool;
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
    void malformedFrameSizeClosesTheChannelRatherThanEscapingPoll() throws Exception {
        // Codex v5 BLOCKER: KafkaChannel.read() declares throws IOException, but the
        // underlying NetworkReceive throws InvalidReceiveException (a KafkaException ->
        // RuntimeException) when the wire reports a negative size or a size greater than
        // socket.request.max.bytes. Before the fix this escaped poll() because we caught
        // only IOException; the Processor never saw the failure and the channel leaked.
        // Two malformed inputs to exercise both code paths in NetworkReceive:
        //   - a negative size prefix (rejected first)
        //   - a size that fits but exceeds maxSize (rejected second)
        // For each, the channel must route through ChannelState.LOCAL_CLOSE (queued via
        // enqueueClose) and surface in disconnected() on the next poll's eviction step.

        // Case 1: negative size prefix. After the accept poll completes, the channel
        // is no longer in justAccepted, so the next poll's read step actually runs
        // channel.read(), which trips InvalidReceiveException from NetworkReceive.
        IoUringSelector s1 = newSelector(IDLE_NANOS_NEVER);
        EmbeddedChannel netty1 = acceptNew(s1, REMOTE_A);
        s1.poll(0); // accept
        String id1 = s1.connected().get(0);
        ByteBuf bad = Unpooled.buffer(4).writeInt(-1);
        s1.onRead(netty1, bad);
        // poll() must NOT throw — the InvalidReceiveException must be absorbed inside
        // the catch (Exception) on the read path, and the channel must be routed
        // through enqueueClose (which populates disconnected in the same poll).
        s1.poll(0); // read step trips exception, enqueueClose -> disconnected[id1]
        assertTrue(s1.disconnected().containsKey(id1),
            "malformed negative size must surface as a disconnect, not escape poll(): id1=" + id1
                + " disconnected=" + s1.disconnected());
        assertEquals(ChannelState.LOCAL_CLOSE, s1.disconnected().get(id1));
        s1.close();

        // Case 2: positive size that exceeds the configured maxSize.
        IoUringSelector s2 = newSelector(IDLE_NANOS_NEVER);
        EmbeddedChannel netty2 = acceptNew(s2, REMOTE_B);
        s2.poll(0); // accept
        String id2 = s2.connected().get(0);
        ByteBuf oversized = Unpooled.buffer(4).writeInt(MAX_RECEIVE + 1);
        s2.onRead(netty2, oversized);
        s2.poll(0); // read step trips exception
        assertTrue(s2.disconnected().containsKey(id2),
            "oversized frame must surface as a disconnect, not escape poll(): id2=" + id2
                + " disconnected=" + s2.disconnected());
        assertEquals(ChannelState.LOCAL_CLOSE, s2.disconnected().get(id2));
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
        // BEFORE we declare the channel disconnected. Mirrors NIO Selector.clear() at
        // clients/.../Selector.java:842-865 — the channel stays in closingChannels while
        // there's pending work; only when nothing more is forthcoming does doClose
        // (notifyDisconnect=true) emit the LOCAL_CLOSE into `disconnected`. Deferring
        // the disconnect notification to the eviction poll closes the double-dec window
        // where the Processor would otherwise see `disconnected` for an id in the same
        // poll that closeExcessConnections could also call selector.close(id) on.
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        EmbeddedChannel netty = acceptNew(s, REMOTE_A);
        s.poll(0);
        String id = s.connected().get(0);

        s.onRead(netty, framed("final-frame"));
        s.onDisconnect(netty);

        // Poll 1: FIN poll. The buffered receive surfaces, the channel goes into
        // closingChannels for one more poll, but `disconnected` stays empty. The
        // Processor's processCompletedReceives resolves the receive via
        // closingChannel(id), then on the NEXT poll the eviction emits the disconnect.
        s.poll(0);

        assertEquals(1, s.completedReceives().size(),
            "buffered bytes must be delivered on the FIN poll, before the disconnect notification");
        assertFalse(s.disconnected().containsKey(id),
            "disconnect notification is deferred to the eviction poll, matching NIO Selector.clear: "
            + "emitting on the FIN poll would race processDisconnected against closeExcessConnections "
            + "for the same id in the same Processor turn, double-dec'ing the per-IP quota");
        assertNull(s.channel(id), "channel is gone from the active map on the FIN poll");
        // Processor.processCompletedReceives resolves the receive's source via either
        // channel(id) or closingChannel(id). channel(id) is null after disconnect, so
        // closingChannel(id) MUST keep the channel reachable so the final completedReceive
        // can be resolved — otherwise it lands on a null KafkaChannel and gets silently dropped.
        assertNotNull(s.closingChannel(id),
            "closingChannel must surface the disconnected channel so the Processor can resolve the final receive");

        // Poll 2: eviction poll. Nothing more is forthcoming (read returned nothing,
        // !muted, !sendFailed) so drainClosingChannels evicts and emits LOCAL_CLOSE.
        s.poll(0);
        assertTrue(s.disconnected().containsKey(id),
            "eviction poll must emit LOCAL_CLOSE — this is the SINGLE disconnect notification for the channel");
        assertEquals(ChannelState.LOCAL_CLOSE, s.disconnected().get(id),
            "evicted closing channel surfaces as LOCAL_CLOSE per NIO doClose(channel, true)");
        assertNull(s.closingChannel(id),
            "closingChannel must be evicted on the next poll; only one extra poll of grace");
        assertTrue(s.completedReceives().isEmpty(),
            "no extra receive on the eviction poll — the channel had only one buffered frame");
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
        // and surfaces on the NEXT poll — it must NOT throw, because Processor.sendResponse
        // synchronously calls selector.send for any response in flight and an exception here
        // would propagate up to Processor.processChannelException → Processor.close,
        // dec'ing a quota that another path is also dec'ing for the same disconnect.
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
        assertFalse(s.disconnected().containsKey(id),
            "preconditions: disconnect is deferred to the eviction poll, not emitted on FIN");

        // Send for the closing channel MUST NOT throw — must route to failedSends and
        // surface on the next poll as a single disconnect notification.
        ByteBuffer body = ByteBuffer.wrap("late-response".getBytes());
        s.send(new NetworkSend(id, ByteBufferSend.sizePrefixed(body)));

        // Poll 2 is the eviction poll: failedSends.remove(id) inside drainClosingChannels
        // makes sendFailed=true, the read attempt is skipped, the channel is evicted, and
        // exactly one entry lands in disconnected — LOCAL_CLOSE, matching NIO's
        // doClose(channel, notifyDisconnect=true) at clients/.../Selector.java:856-857
        // which writes channel.state() (the original close cause). The subsequent
        // `for (String id : failedSends)` loop sees nothing for this id because the
        // closingChannels drain already consumed it — that suppression is what prevents
        // the duplicate FAILED_SEND that would double-notify processDisconnected.
        s.poll(0);
        assertTrue(s.disconnected().containsKey(id),
            "eviction poll must emit exactly one disconnect for the channel");
        assertEquals(ChannelState.LOCAL_CLOSE, s.disconnected().get(id),
            "evicted closing channel surfaces as LOCAL_CLOSE (the original close cause); " +
            "the duplicate FAILED_SEND notification must be suppressed by failedSends.remove");
        assertNull(s.closingChannel(id),
            "the channel must be fully evicted on the eviction poll");

        // Poll 3: nothing left. Asserts there is no lingering failedSends entry that
        // could surface a phantom disconnect for an already-evicted id.
        s.poll(0);
        assertTrue(s.disconnected().isEmpty(),
            "no phantom disconnect on subsequent polls — the channel state is fully drained");
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

    @Test
    void pipelinedFramesSurfaceAcrossClosingChannelPolls() throws Exception {
        // Codex v4 BLOCKER: closingChannels must support pipelined final-read across polls.
        // A peer that pipelines three requests and then FINs must have all three surface
        // as completedReceives across successive polls — one per poll (the "one receive per
        // channel per poll" cap continues to apply across the FIN poll + eviction polls).
        // Without drainClosingChannels iterating across polls, R2 and R3 would be silently
        // dropped when the channel hits FIN — the broker would acknowledge the connection
        // as cleanly closed despite never serving the pipelined requests.
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        EmbeddedChannel netty = acceptNew(s, REMOTE_A);
        s.poll(0);
        String id = s.connected().get(0);

        s.onRead(netty, framed("r1"));
        s.onRead(netty, framed("r2"));
        s.onRead(netty, framed("r3"));
        s.onDisconnect(netty);

        // Poll 1 (FIN poll): step 2 read pass delivers R1. Step 3 sees a receive already
        // surfaced this poll, so it does NOT redrain. Channel goes into closingChannels.
        s.poll(0);
        assertEquals(1, s.completedReceives().size(), "FIN poll: step 2 delivers R1");
        assertContains(s.completedReceives(), "r1");
        assertFalse(s.disconnected().containsKey(id), "no disconnect on FIN poll");
        assertNotNull(s.closingChannel(id), "channel remains in closingChannels for pipelined drain");

        // Poll 2: drainClosingChannels reads from the closing channel, surfaces R2,
        // keeps the channel because more bytes are still readable.
        s.poll(0);
        assertEquals(1, s.completedReceives().size(), "drain poll 1: R2 surfaces");
        assertContains(s.completedReceives(), "r2");
        assertFalse(s.disconnected().containsKey(id), "channel still has work; no disconnect yet");
        assertNotNull(s.closingChannel(id), "still buffered work; closingChannel must persist");

        // Poll 3: surfaces R3, channel still has bytes? No — exhausted, but next poll's
        // drainClosingChannels will re-attempt the read and discover nothing else, then evict.
        s.poll(0);
        assertEquals(1, s.completedReceives().size(), "drain poll 2: R3 surfaces");
        assertContains(s.completedReceives(), "r3");

        // Poll 4: eviction — read produces no more bytes, channel is evicted with LOCAL_CLOSE.
        s.poll(0);
        assertTrue(s.disconnected().containsKey(id),
            "eviction poll: all pipelined work drained, channel evicted with LOCAL_CLOSE");
        assertEquals(ChannelState.LOCAL_CLOSE, s.disconnected().get(id));
        assertNull(s.closingChannel(id), "fully evicted");
    }

    @Test
    void muteDrivesTheKafkaChannelStateMachineNotJustAutoRead() throws Exception {
        // Codex v4 BLOCKER: selector.mute(id) must transition the KafkaChannel mute state
        // to MUTED so SocketServer.scala's subsequent handleChannelMuteEvent(REQUEST_RECEIVED)
        // can advance MUTED → MUTED_AND_RESPONSE_PENDING. KafkaChannel.handleChannelMuteEvent
        // (clients/.../KafkaChannel.java:274-312) throws IllegalStateException if the source
        // state is wrong — so if mute() only flipped autoRead without driving the state
        // machine, every first valid request on every connection would crash the Processor.
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        EmbeddedChannel netty = acceptNew(s, REMOTE_A);
        s.poll(0);
        String id = s.connected().get(0);
        KafkaChannel channel = s.channel(id);
        assertNotNull(channel);

        assertFalse(channel.isMuted(), "freshly accepted channel: NOT_MUTED");

        s.mute(id);
        assertTrue(channel.isMuted(),
            "selector.mute must transition KafkaChannel state to MUTED — otherwise the broker's " +
            "handleChannelMuteEvent(REQUEST_RECEIVED) throws and the first request crashes");
        assertFalse(netty.config().isAutoRead(),
            "mute must also flip Netty autoRead off (kernel-level backpressure) in addition to the state transition");

        // Verify the state is actually MUTED (not some other muted variant) — handleChannelMuteEvent
        // for REQUEST_RECEIVED expects the source state to be MUTED specifically. We exercise it
        // here: a successful call proves the state transition is correct.
        channel.handleChannelMuteEvent(KafkaChannel.ChannelMuteEvent.REQUEST_RECEIVED);
        // The transition succeeded if no IllegalStateException was thrown above.

        s.unmute(id);
        // After REQUEST_RECEIVED, the channel is MUTED_AND_RESPONSE_PENDING. maybeUnmute
        // requires RESPONSE_SENT first, so the channel stays muted.
        assertTrue(channel.isMuted(),
            "unmute on MUTED_AND_RESPONSE_PENDING: stays muted until RESPONSE_SENT — matches NIO");

        channel.handleChannelMuteEvent(KafkaChannel.ChannelMuteEvent.RESPONSE_SENT);
        s.unmute(id);
        assertFalse(channel.isMuted(),
            "after RESPONSE_SENT + unmute: channel returns to NOT_MUTED");
        assertTrue(netty.config().isAutoRead(),
            "unmute flips Netty autoRead back on — bytes flow again");
    }

    @Test
    void muteAllAndUnmuteAllAlsoDriveTheStateMachine() throws Exception {
        // Same invariant as the per-channel test but for the muteAll/unmuteAll path
        // (SocketServer uses these for back-pressure across all connections on a Processor).
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        EmbeddedChannel a = acceptNew(s, REMOTE_A);
        EmbeddedChannel b = acceptNew(s, REMOTE_B);
        s.poll(0);
        KafkaChannel ka = s.channel(s.connected().get(0));
        KafkaChannel kb = s.channel(s.connected().get(1));
        assertNotNull(ka);
        assertNotNull(kb);
        assertFalse(ka.isMuted());
        assertFalse(kb.isMuted());

        s.muteAll();
        assertTrue(ka.isMuted(), "muteAll must transition every channel to MUTED, not just autoRead");
        assertTrue(kb.isMuted());
        assertFalse(a.config().isAutoRead());
        assertFalse(b.config().isAutoRead());

        s.unmuteAll();
        assertFalse(ka.isMuted());
        assertFalse(kb.isMuted());
        assertTrue(a.config().isAutoRead());
        assertTrue(b.config().isAutoRead());
    }

    private static void assertContains(Iterable<NetworkReceive> receives, String expected) {
        for (NetworkReceive r : receives) {
            ByteBuffer payload = r.payload().duplicate();
            byte[] body = new byte[payload.remaining()];
            payload.get(body);
            if (new String(body).equals(expected)) return;
        }
        throw new AssertionError("did not find a NetworkReceive with payload '" + expected + "'");
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

    @Test
    void onAcceptDropsConnectionWhenPendingQueueIsFull() throws Exception {
        // Regression for the unbounded-pre-quota-accept blow-up: if the Processor stalls
        // (e.g. blocked on request-handler backpressure), Netty's event loop must not
        // accumulate KafkaChannel state indefinitely in pendingAccepts. After the cap
        // (DEFAULT_MAX_PENDING_ACCEPTS = 20) is reached, further onAccept calls must close
        // the freshly accepted netty channel immediately rather than enqueueing it.
        //
        // The cap is the io_uring analog of NIO's ArrayBlockingQueue(connectionQueueSize)
        // in SocketServer.Acceptor — NIO blocks the Acceptor on put() to apply kernel-level
        // backpressure, we close because the Netty event loop is shared across this
        // listener's child channels and blocking it would stall the world.
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        int cap = 20; // mirrors DEFAULT_MAX_PENDING_ACCEPTS in IoUringSelector

        // Saturate the queue: 20 accepts with NO intervening poll, so nothing drains.
        List<EmbeddedChannel> queued = new ArrayList<>();
        for (int i = 0; i < cap; i++) {
            queued.add(acceptNew(s, new InetSocketAddress("198.51.100.9", 40000 + i)));
        }
        for (EmbeddedChannel ch : queued) {
            assertTrue(ch.isOpen(), "channels up to the cap must stay open");
        }

        // 21st accept: queue is at the cap. Selector must close this one without queuing.
        EmbeddedChannel overflow = acceptNew(s, new InetSocketAddress("198.51.100.9", 40020));
        assertFalse(overflow.isOpen(),
            "accept past the cap must be refused (netty channel closed) — otherwise " +
            "a stalled Processor leaks KafkaChannel + direct-memory state per accept");

        // Poll drains the queued accepts so the next batch of accepts is admissible. This
        // also verifies the counter is properly decremented on drain: without that, the
        // cap would be permanently saturated after a single high-water event.
        s.poll(0);
        assertEquals(cap, s.connected().size(), "all queued accepts must surface this poll");

        // Verify the counter actually recovered: a fresh accept right after the drain
        // must be admitted.
        EmbeddedChannel freshAfterDrain = acceptNew(s, new InetSocketAddress("198.51.100.9", 41000));
        assertTrue(freshAfterDrain.isOpen(),
            "after draining the queue, the counter must reset so new accepts are admitted");
    }

    @Test
    void selfMutedChannelUnmutesOnceMemoryPoolRecovers() throws Exception {
        // Regression for v6 BLOCKER 5: KafkaChannel.read() self-mutes when
        // memoryPool.tryAllocate returns null (clients/.../KafkaChannel.java:414-417). NIO
        // Selector.poll() lines 457-466 walks every non-explicitly-muted channel and calls
        // maybeUnmute() once memoryPool.isOutOfMemory() reports false. Without that
        // recovery, channels stay MUTED until idle expiry (~10 min default) — under
        // queued.max.bytes pressure the broker wedges its listener.
        SimpleMemoryPool pool = new SimpleMemoryPool(64, 64, false, null);
        // Drain the pool so KafkaChannel.read's tryAllocate returns null on the next call.
        java.nio.ByteBuffer drain = pool.tryAllocate(64);
        assertNotNull(drain, "sanity: SimpleMemoryPool starts with capacity");
        assertTrue(pool.isOutOfMemory(), "sanity: pool is dry after draining");
        selector = new IoUringSelector(LISTENER, MAX_RECEIVE, pool, IDLE_NANOS_NEVER, time);

        EmbeddedChannel netty = acceptNew(selector, REMOTE_A);
        selector.poll(0); // surface accept (justAccepted defers reads to the next poll)
        String id = selector.connected().get(0);
        KafkaChannel channel = selector.channel(id);
        assertNotNull(channel);
        assertFalse(channel.isMuted(), "freshly accepted channel must start unmuted");

        // Drive a framed payload in. KafkaChannel.read() reads the 4-byte size header,
        // calls memoryPool.tryAllocate(payloadSize), gets null, and self-mutes via
        // KafkaChannel.mute(). The completedReceive does NOT surface yet.
        selector.onRead(netty, framed("payload"));
        selector.poll(0);
        assertTrue(channel.isMuted(),
            "channel must self-mute when memoryPool.tryAllocate returns null inside read()");
        assertTrue(selector.completedReceives().isEmpty(),
            "self-muted read must not surface a completedReceive");
        assertFalse(netty.config().isAutoRead(),
            "self-mute must also flip Netty autoRead off (kernel-level backpressure)");

        // Release the manual allocation — pool now reports memory available again. On the
        // next poll, recoverFromMemoryPressure() must walk channels, detect this one is
        // self-muted (not in explicitlyMutedChannels), and call maybeUnmute().
        pool.release(drain);
        assertFalse(pool.isOutOfMemory(), "sanity: pool is no longer dry");

        selector.poll(0);
        assertFalse(channel.isMuted(),
            "MemoryPool recovery must unmute the previously self-muted channel — without " +
            "this, the channel stays MUTED until idle expiry and the broker wedges");
        assertTrue(netty.config().isAutoRead(),
            "recovery must restore Netty autoRead so the kernel resumes pushing bytes");
        // The buffered payload now flows through to a completedReceive (size header was
        // already consumed; the unmute lets the retry tryAllocate succeed and drain the
        // remaining bytes from the inbound queue).
        assertEquals(1, selector.completedReceives().size(),
            "after recovery the previously-blocked frame must surface as a completedReceive");
    }

    @Test
    void recoveryDoesNotUnmuteOperatorMutedChannels() throws Exception {
        // The explicitlyMutedChannels Set keeps operator-driven mutes (RESPONSE_QUEUED,
        // throttling, KafkaChannel state machine handover) distinct from self-mutes. If the
        // recovery loop unmuted operator-muted channels, pipelined requests would slip past
        // the request-handling throttle. Mirrors NIO Selector.unmute()'s explicitlyMutedChannels
        // gate (clients/.../Selector.java:762-763).
        SimpleMemoryPool pool = new SimpleMemoryPool(64, 64, false, null);
        java.nio.ByteBuffer drain = pool.tryAllocate(64);
        assertNotNull(drain);
        selector = new IoUringSelector(LISTENER, MAX_RECEIVE, pool, IDLE_NANOS_NEVER, time);

        // Two channels: one will be operator-muted, the other will self-mute.
        EmbeddedChannel selfMutedNetty = acceptNew(selector, REMOTE_A);
        EmbeddedChannel operatorMutedNetty = acceptNew(selector, REMOTE_B);
        selector.poll(0); // surface both accepts
        List<String> ids = new ArrayList<>(selector.connected());
        String selfMutedId = ids.get(0);
        String operatorMutedId = ids.get(1);

        // Operator-mute one channel via the public mute() API — this lands it in
        // explicitlyMutedChannels. The other receives a frame that drives a self-mute.
        selector.mute(operatorMutedId);
        selector.onRead(selfMutedNetty, framed("payload"));
        selector.poll(0);
        assertTrue(selector.channel(selfMutedId).isMuted(), "memory-pressure path muted us");
        assertTrue(selector.channel(operatorMutedId).isMuted(), "operator mute applied");

        // Pool recovers. Recovery loop must unmute ONLY the self-muted channel; the
        // operator-muted channel must remain muted.
        pool.release(drain);
        selector.poll(0);
        assertFalse(selector.channel(selfMutedId).isMuted(),
            "self-muted channel must be unmuted by recovery");
        assertTrue(selector.channel(operatorMutedId).isMuted(),
            "operator-muted channel must NOT be unmuted by recovery — explicitlyMutedChannels " +
            "gate protects request-pipeline throttling from being bypassed by memory recovery");
    }

    @Test
    void selfMutedClosingChannelDrainsRatherThanStickingForever() throws Exception {
        // Regression for v7 BLOCKER 3: drainClosingChannels() at IoUringSelector.java:601
        // previously short-circuited on the broader channel.isMuted(), which captures BOTH
        // operator-driven mutes (RESPONSE_QUEUED, throttling) and self-mutes triggered by
        // memoryPool.tryAllocate returning null inside KafkaChannel.read.
        //
        // NIO's maybeReadFromClosingChannel (clients/.../Selector.java:702) only short-
        // circuits on explicitlyMutedChannels.contains(channel) — the narrower predicate.
        // Self-muted closing channels MUST still attempt to drain because
        // recoverFromMemoryPressure() only walks channels.values(), never closingChannels.
        // Without this fix, a peer FIN arriving while queued.max.bytes is saturated would
        // strand the channel here forever: drain skips it, recovery skips it, the
        // connection-quota slot leaks, and the queued ByteBufs / KafkaChannel memory are
        // retained until process death.
        SimpleMemoryPool pool = new SimpleMemoryPool(64, 64, false, null);
        java.nio.ByteBuffer drain = pool.tryAllocate(64);
        assertNotNull(drain);
        selector = new IoUringSelector(LISTENER, MAX_RECEIVE, pool, IDLE_NANOS_NEVER, time);

        EmbeddedChannel netty = acceptNew(selector, REMOTE_A);
        selector.poll(0);
        String id = selector.connected().get(0);
        KafkaChannel channel = selector.channel(id);

        // Drive a frame in. KafkaChannel.read() reads the size header, tryAllocate fails,
        // self-mutes via KafkaChannel.mute(). channel.isMuted() is now true; the channel is
        // NOT in explicitlyMutedChannels.
        selector.onRead(netty, framed("payload"));
        selector.poll(0);
        assertTrue(channel.isMuted(), "preconditions: channel must be self-muted");

        // Peer closes. Channel goes through onDisconnect → closingChannels with the
        // self-mute state still attached. The first post-disconnect poll runs
        // drainPendingDisconnects (step 3) and routes the channel into closingChannels —
        // drainClosingChannels (step 2) ran earlier in that same poll, when closingChannels
        // was still empty, so no eviction work happens yet. NIO behaves identically: a
        // FIN'd channel gets one extra poll of grace before drainClosingChannels evicts.
        selector.onDisconnect(netty);
        selector.poll(0);
        assertNotNull(selector.closingChannel(id),
            "preconditions: after one post-disconnect poll the channel must be in closingChannels");

        // Second post-disconnect poll: drainClosingChannels now sees the channel. With the
        // BUG, the broader isMuted() short-circuit would set keepClosing=true and the
        // channel would never be evicted. With the FIX, explicitlyMutedChannels.contains
        // returns false → channel.read() runs → tryAllocate still returns null (we have
        // NOT released memory) → no completedReceive → keepClosing stays false → the
        // channel is evicted with LOCAL_CLOSE, recovering the connection-quota slot and
        // releasing the queued ByteBufs.
        selector.poll(0);

        assertNull(selector.closingChannel(id),
            "self-muted closing channel must be evicted by drainClosingChannels even when " +
            "memory pressure persists — otherwise the connection-quota slot leaks forever");
        assertEquals(ChannelState.LOCAL_CLOSE, selector.disconnected().get(id),
            "evicted self-muted closing channel must surface as LOCAL_CLOSE (NIO contract)");
        pool.release(drain);
    }

    @Test
    void operatorMutedClosingChannelStaysUntilProcessorUnmutes() throws Exception {
        // Counterpart to selfMutedClosingChannelDrainsRatherThanStickingForever: the FIX
        // narrows the short-circuit from isMuted() to explicitlyMutedChannels.contains().
        // Operator-muted closing channels MUST still be preserved across polls — the
        // Processor calls KafkaChannel.maybeCompleteReceive only after the throttling /
        // RESPONSE_QUEUED handshake releases the mute. Mirrors NIO Selector.java:702
        // first half of the OR clause.
        selector = newSelector(IDLE_NANOS_NEVER);
        EmbeddedChannel netty = acceptNew(selector, REMOTE_A);
        selector.poll(0);
        String id = selector.connected().get(0);

        // Buffer a frame so the disconnect path routes through closingChannels.
        selector.onRead(netty, framed("payload"));
        selector.poll(0);
        // First receive surfaces immediately because MemoryPool.NONE always allocates.
        // Drain the completedReceives so the next poll's surface is clean.
        selector.completedReceives().clear();

        // Operator-mute — registers in explicitlyMutedChannels.
        selector.mute(id);

        // Buffer another frame and disconnect: channel routes to closingChannels with the
        // operator mute attached.
        selector.onRead(netty, framed("more"));
        selector.onDisconnect(netty);

        selector.poll(0);
        assertNotNull(selector.closingChannel(id),
            "operator-muted closing channel must NOT be evicted on the first drain poll — " +
            "the Processor still owes an explicit unmute before the buffered receive surfaces");
        assertFalse(selector.disconnected().containsKey(id),
            "operator-muted closing channel must NOT emit a disconnect yet — eviction is " +
            "deferred until the Processor unmutes and the final receive flushes");
    }

    @Test
    void connectionIdWrapsAtIntegerMaxValue() throws Exception {
        // Regression for v9 BLOCKER 2: connection-id index used to be AtomicLong, but
        // ServerConnectionId.fromString parses the index segment with Integer.parseInt.
        // A long-running broker that accepted > 2.1B connections would emit IDs that
        // parse to Optional.empty(), so SocketServer.processDisconnected could not
        // decrement quotas — leaking connection slots until restart. NIO wraps at
        // Int.MaxValue (SocketServer.scala line 1431-1432); io_uring must too.
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);

        // Pre-seed the internal counter to the wrap boundary via reflection. There is no
        // production setter for this — the only way to exercise the wrap branch is to put
        // the counter into the pre-wrap state directly.
        java.lang.reflect.Field idGenField = IoUringSelector.class.getDeclaredField("idGen");
        idGenField.setAccessible(true);
        java.util.concurrent.atomic.AtomicInteger idGen =
            (java.util.concurrent.atomic.AtomicInteger) idGenField.get(s);
        idGen.set(Integer.MAX_VALUE);

        // Accept one — index segment must be Integer.MAX_VALUE (still parses to int).
        EmbeddedChannel first = acceptNew(s, REMOTE_A);
        String firstId = first.attr(IoUringSelector.CHANNEL_ID_ATTR).get();
        java.util.Optional<org.apache.kafka.common.network.ServerConnectionId> firstParsed =
            org.apache.kafka.common.network.ServerConnectionId.fromString(firstId);
        assertTrue(firstParsed.isPresent(),
            "id at Integer.MAX_VALUE must still parse — under the old AtomicLong this " +
            "passed too, the bug only surfaced on the wrap call below");
        assertEquals(Integer.MAX_VALUE, firstParsed.get().index());

        // Accept another — the wrap must roll the counter back to 0, not to a value
        // outside int range.
        EmbeddedChannel second = acceptNew(s, REMOTE_B);
        String secondId = second.attr(IoUringSelector.CHANNEL_ID_ATTR).get();
        java.util.Optional<org.apache.kafka.common.network.ServerConnectionId> secondParsed =
            org.apache.kafka.common.network.ServerConnectionId.fromString(secondId);
        assertTrue(secondParsed.isPresent(),
            "after Integer.MAX_VALUE the next index MUST wrap to 0 — under AtomicLong it " +
            "would be Integer.MAX_VALUE+1L, which Integer.parseInt rejects, breaking " +
            "SocketServer.processDisconnected and leaking the quota slot");
        assertEquals(0, secondParsed.get().index(),
            "wrap target is 0 — matches NIO's SocketServer.scala 'if (... == Int.MaxValue) 0 else +1'");
    }

    @Test
    void acceptInitializesClientInformationToEmpty() throws Exception {
        // Regression for v12 BLOCKER: NIO's Selector.register (clients/Selector.java:316-318)
        // seeds ClientInformation.EMPTY so that any consumer dereferencing
        // channelMetadataRegistry.clientInformation() pre-ApiVersions sees a non-null value.
        // Without this seed on io_uring, the FIRST request on any connection — including
        // the ApiVersionsRequest itself, since SocketServer.scala:1201-1215 constructs the
        // RequestContext BEFORE the registry is populated — captures null. Subsequent
        // dereferences (request-DEBUG logging at RequestConvertToJson.java:747,
        // deprecated-request metrics at RequestMetrics.java:149-150) then NPE and close
        // otherwise-valid connections.
        IoUringSelector s = newSelector(IDLE_NANOS_NEVER);
        EmbeddedChannel netty = acceptNew(s, REMOTE_A);
        String id = netty.attr(IoUringSelector.CHANNEL_ID_ATTR).get();
        // The KafkaChannel only appears in s.channels() after poll() promotes it out of
        // pendingAccepts, so poll once to make the registry observable through the public
        // accessor used by the production code.
        s.poll(0);
        KafkaChannel channel = s.channel(id);
        assertNotNull(channel, "channel should be visible after the accept-draining poll");
        org.apache.kafka.common.network.ClientInformation seeded =
            channel.channelMetadataRegistry().clientInformation();
        assertNotNull(seeded,
            "ClientInformation must be seeded on accept — otherwise RequestContext on the " +
            "first request captures null and request-logging / deprecated-request metrics NPE");
        assertEquals(org.apache.kafka.common.network.ClientInformation.EMPTY, seeded,
            "io_uring must seed ClientInformation.EMPTY, matching NIO Selector.register");
    }
}
