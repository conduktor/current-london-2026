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

import org.apache.kafka.common.network.TransportLayer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link IoUringTransportLayer}.
 *
 * <p>These tests use Netty's {@link EmbeddedChannel} as the underlying transport so they
 * exercise the real outbound write path (the Channel's outbound buffer collects writes
 * we can pop afterwards) while staying purely in-JVM — no kernel io_uring required.
 *
 * <p>The read path is tested by injecting bytes directly into the layer's inbound queue
 * via a package-private hook ({@link IoUringTransportLayer#offerInbound(ByteBuf)}). In
 * production that hook is called from the Netty event-loop thread on every
 * {@code channelRead}; here we call it from the test thread to keep the test
 * deterministic. The thread-safety of that queue is guaranteed by
 * {@link java.util.concurrent.ConcurrentLinkedQueue} regardless of which thread feeds it.
 */
class IoUringTransportLayerTest {

    private static final InetSocketAddress REMOTE = new InetSocketAddress("198.51.100.7", 51234);
    private static final InetSocketAddress LOCAL = new InetSocketAddress("203.0.113.1", 9092);

    private EmbeddedChannel channel;
    private IoUringTransportLayer layer;

    private IoUringTransportLayer newLayer() {
        channel = new EmbeddedChannel();
        layer = new IoUringTransportLayer(channel, REMOTE, LOCAL);
        return layer;
    }

    @AfterEach
    void tearDown() {
        if (layer != null) {
            try {
                layer.close();
            } catch (Exception ignored) { /* noop */ }
        }
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }

    @Test
    void readyAndHandshakeAreNoOpsForPlaintext() throws Exception {
        IoUringTransportLayer l = newLayer();
        assertTrue(l.ready(), "PLAINTEXT is always ready — no handshake to wait for");
        l.handshake(); // no exception
        assertTrue(l.finishConnect(), "server-accepted channels are connected at construction");
        assertTrue(l.isConnected());
        assertTrue(l.isOpen());
    }

    @Test
    void peerPrincipalIsAnonymousForPlaintext() throws Exception {
        // SSL/SASL layers return real peer principals; PLAINTEXT must report ANONYMOUS so
        // the broker's authorizer rejects on the right code path.
        IoUringTransportLayer l = newLayer();
        assertEquals(org.apache.kafka.common.security.auth.KafkaPrincipal.ANONYMOUS, l.peerPrincipal());
    }

    @Test
    void socketChannelExposesCachedRemoteAndLocalAddresses() throws Exception {
        IoUringTransportLayer l = newLayer();
        assertEquals(REMOTE.getAddress(), l.socketChannel().socket().getInetAddress());
        assertEquals(REMOTE.getPort(), l.socketChannel().socket().getPort());
        assertEquals(LOCAL, l.socketChannel().getLocalAddress());
        assertEquals(REMOTE, l.socketChannel().getRemoteAddress());
    }

    @Test
    void selectionKeyIsAnInertHandle() {
        IoUringTransportLayer l = newLayer();
        SelectionKey key = l.selectionKey();
        assertNotNull(key);
        assertSame(key, l.selectionKey(), "selectionKey() must be stable — identity matters for set membership");
        assertTrue(key.isValid(), "fresh key is valid until disconnect/close");
    }

    @Test
    void interestOpsRoundTripThroughTheNoopKey() {
        IoUringTransportLayer l = newLayer();
        // Constructor pre-sets OP_READ so newly-accepted channels are not muted, matching
        // what Selector.register does for the NIO path.
        assertFalse(l.isMute(), "freshly constructed channel starts with OP_READ set, not muted");

        l.removeInterestOps(SelectionKey.OP_READ);
        assertTrue(l.isMute(), "with OP_READ cleared, the channel is muted (matches PlaintextTransportLayer.isMute)");

        l.addInterestOps(SelectionKey.OP_READ);
        assertFalse(l.isMute(), "re-adding OP_READ unmutes");
    }

    @Test
    void isMuteReturnsFalseAfterCloseEvenIfMutedAtCloseTime() {
        // ERR-2 (NIO parity): NIO PlaintextTransportLayer.isMute (clients/src/main/java/
        // org/apache/kafka/common/network/PlaintextTransportLayer.java:203-204) short-circuits
        // on key.isValid() so a channel whose key was cancelled by close() reports
        // isMute()==false. Without the same short-circuit in the io_uring path, a channel
        // that was muted right before close() would still report isMute()==true forever
        // (interestOps is a cached field that close()'s key.cancel() doesn't touch). Any
        // future introspection caller (metrics, channel-iteration debug, parity-sensitive
        // helpers) reading isMute() post-close would see io_uring lie where NIO doesn't.
        IoUringTransportLayer l = newLayer();
        l.removeInterestOps(SelectionKey.OP_READ);
        assertTrue(l.isMute(), "preconditions: channel is muted at close time");

        l.close();

        assertFalse(l.isMute(), "closed channel must not report itself muted (NIO parity)");
    }

    @Test
    void removingOpReadFlipsNettyAutoReadOffForKernelBackpressure() {
        // KafkaChannel.mute() (called internally when MemoryPool.tryAllocate returns null)
        // calls transport.removeInterestOps(OP_READ). On NIO that takes the SocketChannel out
        // of the JDK selector's ready set so the broker stops draining the kernel buffer. On
        // io_uring there is no SelectionKey — Netty's autoRead drives kernel reads. So the
        // transport layer has to flip autoRead off on OP_READ removal, or every byte the
        // kernel has pushed into Netty keeps draining into our inbound queue under memory
        // pressure and the broker OOMs instead of applying backpressure.
        EmbeddedChannel netty = new EmbeddedChannel();
        IoUringTransportLayer l = new IoUringTransportLayer(netty, REMOTE, LOCAL);
        assertTrue(netty.config().isAutoRead(), "EmbeddedChannel default is autoRead=true");

        l.removeInterestOps(SelectionKey.OP_READ);
        assertFalse(netty.config().isAutoRead(),
            "OP_READ removed -> autoRead flipped off so the kernel stops pushing bytes");

        l.addInterestOps(SelectionKey.OP_READ);
        assertTrue(netty.config().isAutoRead(),
            "OP_READ added back -> autoRead flipped on so the kernel resumes pushing bytes");

        // Idempotency: a redundant remove must not toggle autoRead from off→on by accident.
        l.removeInterestOps(SelectionKey.OP_READ);
        assertFalse(netty.config().isAutoRead());
        l.removeInterestOps(SelectionKey.OP_READ); // already cleared
        assertFalse(netty.config().isAutoRead(), "double-remove is a no-op");
    }

    @Test
    void readDrainsTheInboundQueueIntoTheDestinationByteBuffer() throws Exception {
        IoUringTransportLayer l = newLayer();
        byte[] payload = "hello-io_uring".getBytes();
        l.offerInbound(Unpooled.wrappedBuffer(payload));

        ByteBuffer dst = ByteBuffer.allocate(payload.length);
        int n = l.read(dst);
        assertEquals(payload.length, n);
        dst.flip();
        byte[] echoed = new byte[dst.remaining()];
        dst.get(echoed);
        assertEquals(new String(payload), new String(echoed));

        assertFalse(l.hasBytesBuffered(), "queue is empty after we drained it");
        assertEquals(0, l.read(ByteBuffer.allocate(4)), "subsequent read on empty queue returns 0, not -1");
    }

    @Test
    void readSpansMultipleByteBufsAndShortDestinationBuffers() throws Exception {
        // The Netty event loop may deliver one frame across multiple ByteBufs and the
        // Kafka NetworkReceive may pull only part of one ByteBuf at a time. Both paths
        // need to work without dropping or duplicating bytes.
        IoUringTransportLayer l = newLayer();
        l.offerInbound(Unpooled.wrappedBuffer("first-".getBytes()));
        l.offerInbound(Unpooled.wrappedBuffer("second".getBytes()));

        ByteBuffer dst = ByteBuffer.allocate(4); // pulls partial-first only
        assertEquals(4, l.read(dst));
        assertEquals("firs", new String(dst.array()));

        dst.clear();
        assertEquals(4, l.read(dst));
        assertEquals("t-se", new String(dst.array()));

        dst.clear();
        assertEquals(4, l.read(dst));
        assertEquals("cond", new String(dst.array()));

        assertFalse(l.hasBytesBuffered());
        assertEquals(0, l.read(dst));
    }

    @Test
    void readReturnsMinusOneOnEofAfterTheQueueIsDrained() throws Exception {
        // EOF must only surface after we've delivered all buffered bytes — Selector's
        // read loop expects the same ordering from Plaintext: drain first, then -1.
        IoUringTransportLayer l = newLayer();
        l.offerInbound(Unpooled.wrappedBuffer("tail".getBytes()));
        l.markEof();

        ByteBuffer dst = ByteBuffer.allocate(8);
        assertEquals(4, l.read(dst), "buffered bytes are still delivered after EOF marker");
        dst.clear();
        assertEquals(-1, l.read(dst), "only after the queue is drained does -1 surface");
    }

    @Test
    void writeFlushesIntoTheNettyChannelOutboundBuffer() throws Exception {
        IoUringTransportLayer l = newLayer();
        ByteBuffer src = ByteBuffer.wrap("ack".getBytes());
        int n = l.write(src);
        assertEquals(3, n);
        assertFalse(src.hasRemaining(), "after write, the source buffer's position has advanced to limit");

        // EmbeddedChannel buffers writes in an outbound queue — pop and inspect.
        ByteBuf written = channel.readOutbound();
        assertNotNull(written, "the write must have reached the Netty channel");
        byte[] bytes = new byte[written.readableBytes()];
        written.readBytes(bytes);
        written.release();
        assertEquals("ack", new String(bytes));
    }

    @Test
    void writeReturnsZeroWhenNettyChannelReportsNotWritable() throws Exception {
        // Codex v5 BLOCKER: a slow consumer can let Netty's outboundBuffer grow without
        // bound because write() unconditionally allocates a directBuffer and queues it.
        // The defense is the same shape as NIO's: when the underlying buffer is saturated,
        // write() must return 0 so ByteBufferSend.writeTo defers the send to the next
        // poll. The Selector then re-attempts after onWritabilityChanged() wakes it.
        //
        // EmbeddedChannel's writability state is controlled via the user-defined
        // writability bits on its ChannelOutboundBuffer — flipping bit 1 to false
        // forces isWritable() to return false regardless of how many bytes are queued.
        // This isolates the test from the water-mark configuration so we only verify
        // the gate logic, not the water-mark plumbing.
        EmbeddedChannel netty = new EmbeddedChannel();
        IoUringTransportLayer l = new IoUringTransportLayer(netty, REMOTE, LOCAL);

        // Sanity: a fresh channel is writable, so the first write succeeds.
        ByteBuffer src = ByteBuffer.wrap(new byte[]{1, 2, 3, 4});
        assertEquals(4, l.write(src), "fresh channel is writable; first write must succeed");
        ByteBuf first = netty.readOutbound();
        first.release();

        // Force the channel to report not writable.
        netty.unsafe().outboundBuffer().setUserDefinedWritability(1, false);
        assertFalse(netty.isWritable(), "user-defined writability must flip isWritable() to false");

        // Backpressure kicks in: write returns 0, no directBuffer is allocated.
        ByteBuffer src2 = ByteBuffer.wrap(new byte[]{5, 6, 7, 8});
        int n2 = l.write(src2);
        assertEquals(0, n2, "writes must return 0 once isWritable() is false");
        assertEquals(4, src2.remaining(),
            "source ByteBuffer must be left intact so the next poll's write retries the same bytes");
        assertNull(netty.readOutbound(),
            "no bytes must have been handed to Netty during the backpressure window");

        // Flip writability back: write resumes immediately.
        netty.unsafe().outboundBuffer().setUserDefinedWritability(1, true);
        assertTrue(netty.isWritable(), "restoring user-defined writability flips isWritable() back true");
        assertEquals(4, l.write(src2), "with writability restored, the same source drains in one call");
        ByteBuf second = netty.readOutbound();
        second.release();
    }

    @Test
    void hasPendingWritesReflectsNettyChannelWritability() {
        // EmbeddedChannel reports writable=true while its outbound buffer hasn't been
        // marked unwritable — this is the canonical signal Send.writeTo() will use to
        // know we're not done.
        IoUringTransportLayer l = newLayer();
        assertFalse(l.hasPendingWrites(), "fresh embedded channel is writable, no pending writes");
    }

    @Test
    void disconnectClosesTheNettyChannelAndDrainsTheQueue() {
        IoUringTransportLayer l = newLayer();
        l.offerInbound(Unpooled.wrappedBuffer(new byte[]{1, 2, 3}));
        assertTrue(l.hasBytesBuffered());
        assertTrue(channel.isOpen());

        l.disconnect();

        assertFalse(channel.isOpen(), "Netty channel must be closed");
        assertFalse(l.hasBytesBuffered(), "queued ByteBufs must be released so they don't leak");
        assertFalse(l.isOpen(), "isOpen reflects disconnected state");
    }

    @Test
    void closeIsIdempotent() {
        IoUringTransportLayer l = newLayer();
        l.close();
        l.close();
        assertFalse(l.isOpen());
    }

    @Test
    void implementsTheTransportLayerInterface() {
        // Compilation-level guarantee that KafkaChannel will accept this as its TransportLayer.
        TransportLayer iface = newLayer();
        assertNotNull(iface);
    }

    @Test
    void offerInboundAfterCloseReleasesTheBufImmediately() {
        // Once close() has set the closed flag, any Netty channelRead that races through
        // offerInbound must release its buf instead of leaking it into a queue nobody owns.
        IoUringTransportLayer l = newLayer();
        l.close();

        ByteBuf buf = Unpooled.buffer(4).writeBytes(new byte[]{1, 2, 3, 4});
        assertEquals(1, buf.refCnt(), "fresh buf starts with refCnt 1");
        l.offerInbound(buf);
        assertEquals(0, buf.refCnt(),
            "offer-after-close must release — otherwise pooled direct memory bleeds on every disconnect");
        assertFalse(l.hasBytesBuffered(), "the buf must not have landed in the queue");
    }

    @Test
    void asyncWriteFailureSurfacesAsIoExceptionOnNextWrite() throws Exception {
        // Codex v4 BLOCKER: a Netty writeAndFlush whose promise fires with !isSuccess()
        // (peer RST mid-response, kernel buffer pressure, channel closed under us) must
        // surface to the Processor as an IOException on the next write() call — that is
        // what routes the channel through ChannelState.FAILED_SEND in the Selector poll
        // loop. Without this, completedSends fires for bytes the kernel never delivered
        // and the request handling pipeline silently acks responses the client never saw.
        EmbeddedChannel netty = new EmbeddedChannel();
        IoUringTransportLayer l = new IoUringTransportLayer(netty, REMOTE, LOCAL);

        // Close the channel BEFORE the write — Netty's writeAndFlush completes the
        // promise with a ClosedChannelException synchronously on the calling thread,
        // exercising the listener path that stashes asyncWriteFailure.
        netty.close().syncUninterruptibly();
        assertFalse(netty.isOpen());

        // First write: bytes get queued, the listener fires synchronously with the
        // closed-channel failure, asyncWriteFailure is set. Return value here is the
        // number of bytes Netty accepted into the outbound queue — Netty's contract
        // doesn't reject a closed-channel write synchronously; the failure surfaces
        // through the promise.
        ByteBuffer src = ByteBuffer.wrap("late".getBytes());
        try {
            l.write(src);
        } catch (java.io.IOException firstThrow) {
            // Some platforms have the listener fire synchronously from writeAndFlush()
            // itself; that's fine — the first write already surfaced the failure.
            assertTrue(firstThrow.getMessage().contains("async write failed"));
            return;
        }

        // Listener may have fired synchronously inside writeAndFlush — pendingWriteBytes
        // is back to 0 but asyncWriteFailure must keep hasPendingWrites() true so
        // ByteBufferSend.completed() does NOT report success in the gap before the next
        // write() throws.
        assertTrue(l.hasPendingWrites(),
            "hasPendingWrites must stay true while asyncWriteFailure is pending, so " +
            "ByteBufferSend.completed returns false until the next write() throws");

        // Second write: the stashed failure must surface here, *before* we touch the
        // bytes — that's the synchronous throw the Selector's write step catches and
        // routes through enqueueClose(FAILED_SEND).
        ByteBuffer src2 = ByteBuffer.wrap("next".getBytes());
        java.io.IOException thrown = assertThrows(java.io.IOException.class, () -> l.write(src2));
        assertTrue(thrown.getMessage().contains("async write failed"),
            "the throw must clearly identify itself as an async failure relay");
        assertNotNull(thrown.getCause(),
            "the original Netty failure cause must be attached for diagnostics");

        // Once consumed, the failure must NOT re-fire on a third write — the field is
        // cleared after the throw so a subsequent recovered write (theoretical) wouldn't
        // throw forever. In practice the channel is doomed at this point.
        assertFalse(l.hasPendingWrites(),
            "after the throw, asyncWriteFailure is cleared and hasPendingWrites returns to false");
    }

    @Test
    void asyncWriteFailureSurfacesThroughVectoredWriteEvenWhenAllBuffersEmpty() throws Exception {
        // Critical edge case: ByteBufferSend.writeTo iterates a vectored write[] until
        // every ByteBuffer is fully drained. The LAST writeTo call passes an array of
        // already-empty buffers and the inner write(ByteBuffer) calls are all skipped
        // (no `if (!hasRemaining()) continue` lands in a real write). Without checking
        // asyncWriteFailure at the top of write(ByteBuffer[]), the failure from the
        // previous chunk would be silently swallowed and Send.completed() would falsely
        // report success.
        EmbeddedChannel netty = new EmbeddedChannel();
        IoUringTransportLayer l = new IoUringTransportLayer(netty, REMOTE, LOCAL);

        netty.close().syncUninterruptibly();

        // First vectored write: queues bytes, listener fires with closed-channel failure.
        ByteBuffer chunk = ByteBuffer.wrap("first".getBytes());
        try {
            l.write(new ByteBuffer[]{chunk});
        } catch (java.io.IOException firstThrow) {
            // First write already surfaced — same as the single-buffer test above.
            assertTrue(firstThrow.getMessage().contains("async write failed"));
            return;
        }

        // Second vectored write with ALL EMPTY buffers — the inner write loop has nothing
        // to do, so the failure MUST surface from the top-level guard in write(ByteBuffer[]).
        ByteBuffer empty1 = ByteBuffer.allocate(0);
        ByteBuffer empty2 = ByteBuffer.allocate(8);
        empty2.position(8); // hasRemaining()=false
        java.io.IOException thrown = assertThrows(java.io.IOException.class,
            () -> l.write(new ByteBuffer[]{empty1, empty2}));
        assertTrue(thrown.getMessage().contains("async write failed"));
    }

    @Test
    void concurrentOfferAndCloseNeverLeaksBufs() throws Exception {
        // Reproduces the race the double-check in offerInbound is there to close:
        // the Netty event-loop thread may be mid-channelRead (about to offer) while the
        // Processor thread runs close() (drains the queue). A single read of `closed`
        // before offer() lets a buf slip into the queue *after* the drain, leaking forever.
        // We run many iterations to exercise the window deterministically enough to catch
        // a regression that would re-open the gap.
        int iterations = 200;
        java.util.List<ByteBuf> witnesses = new java.util.ArrayList<>(iterations);
        for (int i = 0; i < iterations; i++) {
            IoUringTransportLayer l = new IoUringTransportLayer(new EmbeddedChannel(), REMOTE, LOCAL);
            ByteBuf buf = Unpooled.buffer(4).writeBytes(new byte[]{1, 2, 3, 4});
            witnesses.add(buf);

            // Two threads, one offering, one closing. Start gate to maximize contention.
            java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
            Thread offerer = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException ignored) {
                    return;
                }
                l.offerInbound(buf);
            });
            Thread closer = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException ignored) {
                    return;
                }
                l.close();
            });
            offerer.start();
            closer.start();
            start.countDown();
            offerer.join();
            closer.join();
        }

        for (int i = 0; i < witnesses.size(); i++) {
            assertEquals(0, witnesses.get(i).refCnt(),
                "iteration " + i + ": buf must be released regardless of offer/close interleaving");
        }
    }

    @Test
    void offerInboundFlipsAutoReadOffOnceHighWatermarkExceeded() {
        // Regression for v7 BLOCKER 1: a fast/abusive PLAINTEXT client (or simply a slow
        // Processor) lets ByteBufs accumulate unbounded in the transport's inbound queue
        // while Netty AUTO_READ stays on. Direct-memory OOM takes the broker down before
        // KafkaChannel's MemoryPool-backed queued.max.bytes throttle even fires — that
        // throttle gates the destination NetworkReceive buffer, not the raw bytes still
        // sitting in this queue.
        //
        // Defense: track inbound bytes; when they cross
        // {@link IoUringTransportLayer#INBOUND_HIGH_WATERMARK_BYTES} flip autoRead off
        // so the kernel applies TCP-level backpressure to the peer.
        IoUringTransportLayer l = newLayer();
        assertTrue(channel.config().isAutoRead(), "preconditions: fresh channel has autoRead on");

        // Push just under the high water mark — autoRead stays on.
        int chunk = 1 << 16; // 64 KiB chunks
        int chunksUnderHigh = (IoUringTransportLayer.INBOUND_HIGH_WATERMARK_BYTES / chunk) - 1;
        for (int i = 0; i < chunksUnderHigh; i++) {
            l.offerInbound(Unpooled.buffer(chunk).writeBytes(new byte[chunk]));
        }
        assertTrue(channel.config().isAutoRead(),
            "below the high water mark, autoRead must remain on — flapping kills throughput");

        // One more push crosses the watermark.
        l.offerInbound(Unpooled.buffer(chunk).writeBytes(new byte[chunk]));
        l.offerInbound(Unpooled.buffer(chunk).writeBytes(new byte[chunk]));
        assertFalse(channel.config().isAutoRead(),
            "crossing INBOUND_HIGH_WATERMARK_BYTES must flip Netty autoRead off so the kernel " +
            "applies TCP-level backpressure — otherwise the transport queue grows without bound " +
            "and direct memory OOMs before KafkaChannel's MemoryPool throttle fires");
    }

    @Test
    void readReEnablesAutoReadOnceBelowLowWatermark() throws Exception {
        // Counterpart to offerInboundFlipsAutoReadOffOnceHighWatermarkExceeded: once the
        // Processor drains the queue below INBOUND_LOW_WATERMARK_BYTES, autoRead must be
        // re-enabled so the kernel resumes pushing bytes. The HIGH-LOW gap prevents the
        // autoRead flag from flapping under a steady-state load near a single threshold.
        IoUringTransportLayer l = newLayer();
        // Fill above HIGH so autoRead gets flipped off.
        int chunk = 1 << 16;
        int chunks = (IoUringTransportLayer.INBOUND_HIGH_WATERMARK_BYTES / chunk) + 4;
        for (int i = 0; i < chunks; i++) {
            l.offerInbound(Unpooled.buffer(chunk).writeBytes(new byte[chunk]));
        }
        assertFalse(channel.config().isAutoRead(),
            "preconditions: queue must be past high water mark so autoRead is off");

        // Drain via read() until the queue is below LOW. We pass a buffer larger than
        // (HIGH - LOW) so a single read crosses the boundary cleanly.
        int drainSize = (chunks * chunk) - (IoUringTransportLayer.INBOUND_LOW_WATERMARK_BYTES / 2);
        ByteBuffer dst = ByteBuffer.allocate(drainSize);
        int read = l.read(dst);
        assertTrue(read > 0, "read must drain some bytes");

        assertTrue(channel.config().isAutoRead(),
            "after the drain crosses below INBOUND_LOW_WATERMARK_BYTES on an unmuted channel, " +
            "autoRead must be flipped back on so the kernel resumes pushing bytes");
    }

    @Test
    void readDoesNotReEnableAutoReadWhileMuted() throws Exception {
        // The autoRead re-enable path must respect the mute state. An operator mute
        // (RESPONSE_QUEUED throttling) or memory-pool self-mute keeps autoRead off
        // regardless of queue depth — otherwise the request-pipeline throttle is
        // bypassed and pipelined requests slip past the broker's flow control.
        IoUringTransportLayer l = newLayer();
        // Fill above HIGH.
        int chunk = 1 << 16;
        int chunks = (IoUringTransportLayer.INBOUND_HIGH_WATERMARK_BYTES / chunk) + 4;
        for (int i = 0; i < chunks; i++) {
            l.offerInbound(Unpooled.buffer(chunk).writeBytes(new byte[chunk]));
        }
        // Operator mute: removes OP_READ from interest mask AND flips autoRead off (already off).
        l.removeInterestOps(SelectionKey.OP_READ);
        assertTrue(l.isMute(), "preconditions: channel is muted");
        assertFalse(channel.config().isAutoRead(), "preconditions: autoRead is off");

        // Drain far below LOW.
        int drainSize = chunks * chunk;
        ByteBuffer dst = ByteBuffer.allocate(drainSize);
        l.read(dst);

        assertFalse(channel.config().isAutoRead(),
            "muted channels must NOT have autoRead re-enabled even after the queue drains — " +
            "doing so would let the kernel push bytes through a channel the Processor has " +
            "explicitly paused via the request-pipeline throttle");
    }

    @Test
    void unmuteRespectsInboundWatermarkGate() throws Exception {
        // Codex v13 BLOCKER 2 regression. Scenario: bytes accumulate past HIGH_WATERMARK so
        // offerInbound flipped autoRead off, then the KafkaChannel self-mutes due to MemoryPool
        // pressure (clearing OP_READ — autoRead was already off). When MemoryPool releases and
        // KafkaChannel.maybeUnmute calls addInterestOps(OP_READ), the previous implementation
        // unconditionally re-enabled autoRead — even though the inbound queue was still pressurized
        // above HIGH_WATERMARK. The kernel would then resume pushing bytes past the gate before
        // the Processor had a chance to drain anything, doubling the direct-memory headroom.
        IoUringTransportLayer l = newLayer();
        // Fill above HIGH so offerInbound has flipped autoRead off and the gate is engaged.
        int chunk = 1 << 16;
        int chunks = (IoUringTransportLayer.INBOUND_HIGH_WATERMARK_BYTES / chunk) + 4;
        for (int i = 0; i < chunks; i++) {
            l.offerInbound(Unpooled.buffer(chunk).writeBytes(new byte[chunk]));
        }
        assertFalse(channel.config().isAutoRead(),
            "preconditions: offerInbound past HIGH must have flipped autoRead off");

        // Mute the channel (mirrors KafkaChannel.mute() on MemoryPool self-mute).
        l.removeInterestOps(SelectionKey.OP_READ);
        assertTrue(l.isMute(), "preconditions: channel is muted");
        assertFalse(channel.config().isAutoRead(), "preconditions: autoRead remained off across mute");

        // Unmute. The queue is still above HIGH_WATERMARK so autoRead MUST stay off — the read
        // path will flip it back on once the Processor drains the queue below LOW_WATERMARK.
        l.addInterestOps(SelectionKey.OP_READ);
        assertFalse(l.isMute(), "unmute restored OP_READ");
        assertFalse(channel.config().isAutoRead(),
            "addInterestOps(OP_READ) must respect the inbound watermark gate when inboundBytes > LOW_WATERMARK; "
            + "re-enabling autoRead here would let the kernel push more bytes past HIGH before the queue drains, "
            + "doubling the direct-memory headroom under a slowloris / pipelined-write DoS");

        // Drain below LOW — the read-path gate is the legitimate place to re-enable autoRead.
        int drainSize = chunks * chunk;
        ByteBuffer dst = ByteBuffer.allocate(drainSize);
        l.read(dst);
        assertTrue(channel.config().isAutoRead(),
            "once the read path drains below LOW_WATERMARK on an unmuted channel, autoRead is re-enabled");
    }

    @Test
    void unmuteEnablesAutoReadWhenQueueAlreadyBelowLowWatermark() throws Exception {
        // Companion to unmuteRespectsInboundWatermarkGate: when inboundBytes is BELOW LOW at the
        // moment of unmute, autoRead must be re-enabled immediately — the read path is not going
        // to re-enable it for us because the gate condition (was above HIGH, dropped below LOW)
        // was never tripped. This is the steady-state happy path: mute, drain to zero while
        // muted, then unmute.
        IoUringTransportLayer l = newLayer();
        l.removeInterestOps(SelectionKey.OP_READ);
        assertFalse(channel.config().isAutoRead(), "preconditions: muted, autoRead off");
        // inboundBytes is 0 here (no offerInbound). Unmute must enable autoRead.
        l.addInterestOps(SelectionKey.OP_READ);
        assertTrue(channel.config().isAutoRead(),
            "with inboundBytes == 0 (well below LOW), addInterestOps(OP_READ) must re-enable autoRead");
    }

    @Test
    void writeChunksLargePayloadsToMaxWriteChunkBytes() throws Exception {
        // Regression for v7 BLOCKER 2: write(ByteBuffer) previously allocated a direct
        // ByteBuf of size = src.remaining() in one shot, BEFORE the isWritable()
        // backpressure gate flipped. A 100 MiB Fetch response on an empty outbound queue
        // bypassed every safeguard. Many slow consumers in flight at once each spiked
        // 100 MiB of direct memory and the broker OOMed.
        //
        // Defense: cap each write to MAX_WRITE_CHUNK_BYTES. ByteBufferSend.writeTo
        // re-enters on the next poll with the remainder — correctness is preserved,
        // only the per-call allocation footprint is bounded.
        EmbeddedChannel netty = new EmbeddedChannel();
        // Push Netty's water marks well above MAX_WRITE_CHUNK_BYTES so the chunking cap
        // is exercised in isolation. Without this, Netty's default (32 KiB low, 64 KiB
        // high) would dominate via bytesBeforeUnwritable and mask the MAX_WRITE_CHUNK
        // limit. Production sets these via the bootstrap; the test covers the cap.
        netty.config().setWriteBufferWaterMark(new io.netty.channel.WriteBufferWaterMark(
            IoUringTransportLayer.MAX_WRITE_CHUNK_BYTES * 2,
            IoUringTransportLayer.MAX_WRITE_CHUNK_BYTES * 4));
        IoUringTransportLayer l = new IoUringTransportLayer(netty, REMOTE, LOCAL);

        int payloadSize = IoUringTransportLayer.MAX_WRITE_CHUNK_BYTES * 4;
        byte[] payload = new byte[payloadSize];
        // Fill with a non-zero pattern so a corrupt-write test would catch it.
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i % 251);
        ByteBuffer src = ByteBuffer.wrap(payload);

        int written = l.write(src);
        assertEquals(IoUringTransportLayer.MAX_WRITE_CHUNK_BYTES, written,
            "a single write call must allocate at most MAX_WRITE_CHUNK_BYTES — otherwise a " +
            "single huge response can spike direct memory before backpressure kicks in");
        assertEquals(payloadSize - IoUringTransportLayer.MAX_WRITE_CHUNK_BYTES, src.remaining(),
            "the source ByteBuffer must keep the remainder so the next poll's write call " +
            "re-attempts the rest — mirrors how NIO returns partial progress");

        // Verify the bytes actually written are the FIRST chunk (not corrupted/skipped).
        ByteBuf flushed = netty.readOutbound();
        assertNotNull(flushed);
        assertEquals(IoUringTransportLayer.MAX_WRITE_CHUNK_BYTES, flushed.readableBytes());
        for (int i = 0; i < 32; i++) {
            assertEquals((byte) (i % 251), flushed.getByte(i),
                "byte " + i + " of the first chunk must match the original payload");
        }
        flushed.release();
        l.close();
        netty.close();
    }

    @Test
    void writeReturnsZeroOncePastHighWaterMarkRegardlessOfChunkSize() throws Exception {
        // Counterpart to writeChunksLargePayloadsToMaxWriteChunkBytes: even with a giant
        // source ByteBuffer, once Netty's outbound queue crosses the high water mark
        // bytesBeforeUnwritable returns 0 → write() returns 0 → backpressure latches in.
        EmbeddedChannel netty = new EmbeddedChannel();
        IoUringTransportLayer l = new IoUringTransportLayer(netty, REMOTE, LOCAL);

        netty.unsafe().outboundBuffer().setUserDefinedWritability(1, false);
        assertFalse(netty.isWritable(), "preconditions: channel is past high water mark");

        ByteBuffer big = ByteBuffer.wrap(new byte[IoUringTransportLayer.MAX_WRITE_CHUNK_BYTES * 2]);
        assertEquals(0, l.write(big),
            "once isWritable is false, write must return 0 — the chunking cap MUST NOT " +
            "force a 1 MiB allocation through a backpressured channel");
        assertEquals(IoUringTransportLayer.MAX_WRITE_CHUNK_BYTES * 2, big.remaining(),
            "source ByteBuffer must keep all its bytes when backpressure latches");
        l.close();
        netty.close();
    }

    @Test
    void vectoredWriteStopsAtFirstPartialBufferToPreserveOrdering() throws Exception {
        // Regression for v8 BLOCKER 1: write(ByteBuffer[]) used to keep iterating the source
        // array even when the previous buffer was only partially drained — e.g. when the
        // middle buffer hit MAX_WRITE_CHUNK_BYTES. That would emit bytes from the trailer
        // before the middle buffer was fully on the wire, corrupting any Send composed of
        // [header, payload, trailer] (notably Fetch responses). The contract of
        // GatheringByteChannel.write — and of every NIO socket — is "strictly in order".
        EmbeddedChannel netty = new EmbeddedChannel();
        netty.config().setWriteBufferWaterMark(new io.netty.channel.WriteBufferWaterMark(
            IoUringTransportLayer.MAX_WRITE_CHUNK_BYTES * 2,
            IoUringTransportLayer.MAX_WRITE_CHUNK_BYTES * 4));
        IoUringTransportLayer l = new IoUringTransportLayer(netty, REMOTE, LOCAL);

        byte[] headerBytes = new byte[8];
        for (int i = 0; i < headerBytes.length; i++) headerBytes[i] = (byte) ('H' + i);
        byte[] bigBytes = new byte[IoUringTransportLayer.MAX_WRITE_CHUNK_BYTES + 1024];
        for (int i = 0; i < bigBytes.length; i++) bigBytes[i] = (byte) (i % 251);
        byte[] trailerBytes = new byte[4];
        for (int i = 0; i < trailerBytes.length; i++) trailerBytes[i] = (byte) ('T' + i);

        ByteBuffer header  = ByteBuffer.wrap(headerBytes);
        ByteBuffer payload = ByteBuffer.wrap(bigBytes);
        ByteBuffer trailer = ByteBuffer.wrap(trailerBytes);
        ByteBuffer[] vec = {header, payload, trailer};

        long wrote = l.write(vec);

        // Header drains fully; payload's per-call write is capped at MAX_WRITE_CHUNK_BYTES,
        // leaving 1024 bytes behind; trailer MUST NOT be touched.
        assertEquals(0, header.remaining(), "header must drain fully (smaller than the cap)");
        assertEquals(bigBytes.length - IoUringTransportLayer.MAX_WRITE_CHUNK_BYTES,
            payload.remaining(),
            "payload must keep the bytes that didn't fit in this poll's chunk — the per-call " +
            "MAX_WRITE_CHUNK_BYTES cap applies to each write(ByteBuffer) individually");
        assertEquals(trailerBytes.length, trailer.remaining(),
            "trailer MUST remain untouched until the payload is fully drained — vectored " +
            "write loop must abort on the first partial buffer");
        assertEquals((long) headerBytes.length + IoUringTransportLayer.MAX_WRITE_CHUNK_BYTES, wrote,
            "total = header drained in full + first MAX_WRITE_CHUNK_BYTES of payload; " +
            "trailer contributes nothing");

        // Drain whatever Netty queued to keep the test hygienic.
        ByteBuf flushed;
        while ((flushed = netty.readOutbound()) != null) flushed.release();
        l.close();
        netty.close();
    }

    @Test
    void vectoredWriteStopsWhenInnerWriteReturnsZero() throws Exception {
        // Counterpart to vectoredWriteStopsAtFirstPartialBufferToPreserveOrdering: when the
        // first non-empty buffer hits backpressure (write returns 0), the loop must abort
        // even though that buffer hasn't been "partially" written — otherwise a backpressured
        // channel would still get bytes from a later buffer in the vector.
        EmbeddedChannel netty = new EmbeddedChannel();
        IoUringTransportLayer l = new IoUringTransportLayer(netty, REMOTE, LOCAL);
        netty.unsafe().outboundBuffer().setUserDefinedWritability(1, false);
        assertFalse(netty.isWritable(), "preconditions: channel is past high water mark");

        ByteBuffer first  = ByteBuffer.wrap(new byte[64]);
        ByteBuffer second = ByteBuffer.wrap(new byte[64]);
        ByteBuffer[] vec = {first, second};

        long wrote = l.write(vec);

        assertEquals(0, wrote, "must report no progress when the channel is backpressured");
        assertEquals(64, first.remaining(),  "first buffer untouched");
        assertEquals(64, second.remaining(), "second buffer MUST be untouched when the first " +
            "buffer reported 0 progress — loop must abort on a zero return");
        l.close();
        netty.close();
    }

    @Test
    void writeWakeCallbackFiresOnceForEachWriteAndFlushCompletion() throws Exception {
        // Regression for v10 BLOCKER: the writeAndFlush listener runs on Netty's event-loop
        // thread, NOT the Processor thread. Without an explicit wake of the Processor's
        // poll-side Semaphore from inside the listener, a small Send (one that never crosses
        // the outbound watermark, so onWritabilityChanged is not invoked) leaves poll() asleep
        // until the full timeoutMs elapses — typical Kafka small-request latency would grow
        // by ~300ms per round trip. EmbeddedChannel completes writeAndFlush synchronously, so
        // this test merely verifies the listener path INVOKES the callback; the real async
        // path is exercised by IoUringSelectorTest's send/poll integration.
        EmbeddedChannel netty = new EmbeddedChannel();
        java.util.concurrent.atomic.AtomicInteger wakeups = new java.util.concurrent.atomic.AtomicInteger();
        IoUringTransportLayer l = new IoUringTransportLayer(netty, REMOTE, LOCAL, wakeups::incrementAndGet);

        ByteBuffer src = ByteBuffer.wrap(new byte[128]);
        long wrote = l.write(src);
        assertEquals(128, wrote);

        assertEquals(1, wakeups.get(),
            "writeAndFlush listener must wake the Processor exactly once per completed write " +
            "— otherwise small responses incur a poll-timeout-sized latency penalty before " +
            "completedSends fires");

        // Drain so AfterEach's close doesn't trip on retained bufs.
        ByteBuf flushed;
        while ((flushed = netty.readOutbound()) != null) flushed.release();
        l.close();
        netty.close();
    }

    @Test
    void writeWakeCallbackFiresOnAsyncWriteFailure() throws Exception {
        // The wake-on-completion path must also fire when the promise completes
        // exceptionally — otherwise the Processor stays asleep with asyncWriteFailure
        // already stashed, and the FAILED_SEND routing is delayed by a full poll timeout.
        EmbeddedChannel netty = new EmbeddedChannel();
        java.util.concurrent.atomic.AtomicInteger wakeups = new java.util.concurrent.atomic.AtomicInteger();
        IoUringTransportLayer l = new IoUringTransportLayer(netty, REMOTE, LOCAL, wakeups::incrementAndGet);

        // Close the channel so writeAndFlush fails with ClosedChannelException.
        netty.close().syncUninterruptibly();

        ByteBuffer src = ByteBuffer.wrap(new byte[64]);
        long wrote = l.write(src);
        // After the channel is closed, write() bypasses the watermark gate and still hands
        // off the bytes to writeAndFlush; the promise then completes exceptionally.
        assertEquals(64, wrote, "write should return the bytes handed to writeAndFlush even when " +
            "the future then fails — the failure surfaces on the next call via asyncWriteFailure");

        assertEquals(1, wakeups.get(),
            "failure path of writeAndFlush listener must also fire the wake callback — " +
            "otherwise the Processor stays asleep with asyncWriteFailure stashed and " +
            "FAILED_SEND routing is delayed by the full poll timeout");
        l.close();
    }

    @Test
    void writeWakeCallbackFiresFromNettyEventLoopThreadNotCallerThread() throws Exception {
        // Round-16 audit BLOCKER B-T3. The two writeWakeCallback tests above use
        // EmbeddedChannel, which runs the writeAndFlush listener synchronously on the
        // caller thread. That shape verifies the listener PATH invokes the callback but
        // says nothing about the cross-thread hand-off the production wire actually
        // depends on: in production the listener runs on the Netty event-loop thread,
        // and the callback must wake a Processor sleeping on a Semaphore on a DIFFERENT
        // thread. If the EmbeddedChannel tests stay green while the cross-thread wake
        // silently regresses, the v10 hang re-enters with no test coverage to catch it.
        //
        // Use a real Netty channel pair on a DefaultEventLoopGroup so writeAndFlush
        // listeners fire on the event-loop thread, off the test thread. LocalChannel is
        // the lightest weight option that exercises a real EventLoop without requiring
        // a TCP socket or io_uring kernel support.
        DefaultEventLoopGroup serverGroup = new DefaultEventLoopGroup(1);
        DefaultEventLoopGroup clientGroup = new DefaultEventLoopGroup(1);
        Channel server = null;
        Channel client = null;
        IoUringTransportLayer layer = null;
        try {
            LocalAddress addr = new LocalAddress("iouring-wake-test-" + System.nanoTime());

            ServerBootstrap sb = new ServerBootstrap();
            sb.group(serverGroup);
            sb.channel(LocalServerChannel.class);
            sb.childHandler(new ChannelInboundHandlerAdapter() {
                @Override
                public void channelRead(io.netty.channel.ChannelHandlerContext ctx, Object msg) {
                    // Drop inbound — we only care about the outbound write completing.
                    io.netty.util.ReferenceCountUtil.release(msg);
                }
            });
            server = sb.bind(addr).syncUninterruptibly().channel();

            Bootstrap b = new Bootstrap();
            b.group(clientGroup);
            b.channel(LocalChannel.class);
            b.handler(new ChannelInboundHandlerAdapter());
            client = b.connect(addr).syncUninterruptibly().channel();

            Thread callerThread = Thread.currentThread();
            AtomicReference<Thread> wakeThread = new AtomicReference<>();
            CountDownLatch wokenUp = new CountDownLatch(1);
            layer = new IoUringTransportLayer(client, REMOTE, LOCAL, () -> {
                wakeThread.compareAndSet(null, Thread.currentThread());
                wokenUp.countDown();
            });

            ByteBuffer src = ByteBuffer.wrap(new byte[128]);
            long wrote = layer.write(src);
            assertEquals(128, wrote, "write must hand off all bytes to the Netty outbound");

            assertTrue(wokenUp.await(5, TimeUnit.SECONDS),
                "writeWakeCallback must fire across thread boundaries — the Netty event-loop " +
                "thread runs the writeAndFlush listener and must signal the Processor on a " +
                "different thread. If the callback never lands, poll() sleeps the full " +
                "timeout and small-request latency degrades by ~timeoutMs per round trip.");
            assertNotSame(callerThread, wakeThread.get(),
                "wake callback must execute on the Netty event-loop thread, NOT the caller " +
                "thread that invoked write() — EmbeddedChannel's synchronous behaviour was " +
                "masking the real cross-thread hand-off the production wire depends on. " +
                "Caller=" + callerThread.getName() + " wake=" + wakeThread.get().getName());
        } finally {
            if (layer != null) layer.close();
            if (client != null) client.close().syncUninterruptibly();
            if (server != null) server.close().syncUninterruptibly();
            serverGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
            clientGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }
}
