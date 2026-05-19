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
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.util.internal.OutOfDirectMemoryError;

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
@ExtendWith(IoUringLeakDetectorExtension.class)
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
    void readReconcilesAutoReadEvenWhenNoBytesAvailable() throws Exception {
        // Regression for the AUTOREAD-RACE silent stall.
        //
        // Adversarial interleaving between the event-loop offerInbound and the Processor read:
        //
        //   State: inboundBytes = HIGH-50, autoRead = true, queue has bytes.
        //   T1 (event loop): offerInbound(buf, size=100)
        //        - addAndGet(+100) → local `after = HIGH+50`
        //        - inbound.offer(buf)
        //        - [PAUSE before the autoRead-off gate]
        //   T2 (Processor): read(dst)
        //        - drains the queue completely → total = HIGH+50
        //        - addAndGet(-(HIGH+50)) → local `after = 0`
        //        - gate: after <= LOW, !isMute, open, BUT !isAutoRead() is FALSE
        //          (T1 has not yet flipped it). Gate does nothing.
        //   T1 (resumes): isAutoRead() still true, local `after` still HIGH+50 ≥ HIGH.
        //        - setAutoRead(false).
        //
        // Final state: queue=0, autoRead=false, not muted, channel open.
        // Nothing will recover this:
        //   - offerInbound won't fire (the kernel honored TCP backpressure and stopped pushing).
        //   - read() doesn't reconcile because the autoRead re-enable lives inside `if (total > 0)`.
        //   - addInterestOps(OP_READ) won't fire because OP_READ is already in the interest mask.
        //   - The channel goes silent until idle expiry (~10 min) closes it.
        //
        // Fix: in read(), the autoRead reconciliation MUST run on every Processor poll, regardless
        // of whether bytes were drained this call. The next Processor.poll cycle then heals the
        // race within microseconds.
        IoUringTransportLayer l = newLayer();

        // Simulate the post-race state directly. We can't run two threads against an
        // EmbeddedChannel deterministically (its pipeline is single-threaded), so we drive the
        // observable end state: autoRead=false, queue empty, inboundBytes=0, not muted.
        assertTrue(channel.config().isAutoRead(), "fresh channel: autoRead is on");
        assertFalse(l.isMute(), "fresh channel: not muted");
        channel.config().setAutoRead(false);
        assertFalse(channel.config().isAutoRead(),
            "preconditions: autoRead is off (simulates the stale-after race outcome)");

        // Processor calls read on every poll, even for channels with nothing buffered.
        ByteBuffer dst = ByteBuffer.allocate(4096);
        int n = l.read(dst);
        assertEquals(0, n, "queue is empty: read returns 0 (no bytes, not EOF)");

        assertTrue(channel.config().isAutoRead(),
            "read() MUST reconcile autoRead with the current queue depth on every call, not only "
            + "when bytes were drained. Without this, the offerInbound-vs-read race can latch the "
            + "channel into autoRead=false with an empty queue and a live peer, silently stalling "
            + "the connection until idle expiry.");
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

    @Test
    void multipleAsyncWriteFailuresPreserveFirstCauseAndSuppressTheRest() throws Exception {
        // DIAG-1: a peer RST against a channel with N in-flight writes causes Netty to
        // fail every queued writeAndFlush promise. Each listener firing reaches the
        // failure-recording branch with its own cause. The previous shape
        // `asyncWriteFailure = f.cause()` kept only the LAST cause — operators
        // investigating a multi-write disconnect would see one ClosedChannelException
        // and miss the original triggering exception. The fix preserves the first cause
        // as the primary and chains subsequent causes as suppressed exceptions under it.
        //
        // EmbeddedChannel completes writeAndFlush futures synchronously on the calling
        // thread, so the natural cadence with two real failing writes is "listener fires,
        // next write() consumes the cause via the top guard" — there's never a moment
        // where two unconsumed causes coexist. We bypass that by invoking the production
        // failure-recording path directly via {@link
        // IoUringTransportLayer#recordAsyncWriteFailureForTesting}, which is the same
        // code path the real listener calls. This deterministically stacks two unconsumed
        // causes, mirroring the production race where two listeners run between
        // Processor poll cycles.
        IoUringTransportLayer l = newLayer();

        RuntimeException firstCause = new RuntimeException("first-promise-failed");
        RuntimeException secondCause = new RuntimeException("second-promise-failed");
        l.recordAsyncWriteFailureForTesting(firstCause);
        l.recordAsyncWriteFailureForTesting(secondCause);

        ByteBuffer src = ByteBuffer.wrap("trigger-throw".getBytes());
        java.io.IOException thrown = assertThrows(java.io.IOException.class, () -> l.write(src));

        assertTrue(thrown.getMessage().contains("async write failed"),
            "the throw must clearly identify itself as an async failure relay");
        assertSame(firstCause, thrown.getCause(),
            "DIAG-1: the FIRST recorded cause must be the primary — operators investigating " +
            "a multi-write disconnect rely on the first cause being the actual triggering " +
            "exception (subsequent causes are typically reactions to it).");

        Throwable[] suppressed = thrown.getCause().getSuppressed();
        assertEquals(1, suppressed.length,
            "DIAG-1: the second recorded cause must be chained as a suppressed exception " +
            "under the primary, so operators see both causes in a single stack trace. " +
            "Pre-fix behaviour would have overwritten the first cause and surfaced only " +
            "the second — losing the original triggering exception.");
        assertSame(secondCause, suppressed[0],
            "the suppressed exception must be the second recorded cause, not the first");

        // Field returns to clean state after the throw (DIAG-2): hasPendingWrites must
        // report false, so a subsequent ByteBufferSend.completed() check returns true on
        // the (failed) channel and the Selector tears it down via FAILED_SEND.
        assertFalse(l.hasPendingWrites(),
            "after the throw consumes the failure via getAndSet(null), hasPendingWrites " +
            "must return false so the failed-send accounting completes cleanly");
    }

    @Test
    void closeClearsStashedAsyncWriteFailureSoHasPendingWritesReportsCleanState() throws Exception {
        // DIAG-3: a writeAndFlush promise that completes (failed) AFTER close() — possible
        // when Netty had the write in-flight at the moment of close — would leave
        // asyncWriteFailure set forever. hasPendingWrites() reads it and would return true
        // on a closed channel. Operators inspecting the channel state see a misleading
        // "pending writes" signal long after the layer was torn down. close() must clear
        // the stashed failure as part of its drain-all invariant.
        //
        // Determinism: we use the recordAsyncWriteFailureForTesting seam to plant the
        // failure synchronously. The earlier shape of this test relied on triggering an
        // async failure through EmbeddedChannel's writeAndFlush — but EmbeddedChannel
        // surfaces the listener synchronously, so the very next write() called from the
        // catch block consumed the stashed failure via getAndSet(null) and zeroed it
        // before close() ever ran. The original test passed even when close() did NOT
        // clear the field; the seam removes that loophole.
        EmbeddedChannel netty = new EmbeddedChannel();
        IoUringTransportLayer l = new IoUringTransportLayer(netty, REMOTE, LOCAL);

        IOException planted = new IOException("planted DIAG-3 async write failure");
        l.recordAsyncWriteFailureForTesting(planted);
        assertTrue(l.hasPendingWrites(),
            "precondition: a stashed asyncWriteFailure must make hasPendingWrites return true " +
            "(seam matches production recordAsyncWriteFailure path)");

        l.close();

        assertFalse(l.hasPendingWrites(),
            "DIAG-3: close() must clear asyncWriteFailure so hasPendingWrites() reports " +
            "false on a closed channel. A non-null stashed failure would otherwise persist " +
            "indefinitely and operators inspecting channel state would see a misleading " +
            "'pending writes' signal long after the layer was torn down.");
    }

    @Test
    void closeResetsPendingWriteBytesSoHasPendingWritesReportsCleanState() throws Exception {
        // DIAG-4: symmetric to DIAG-3 for pendingWriteBytes. write() addAndGets the chunk
        // size BEFORE addListener fires the decrement. If close() runs while a write is
        // in flight inside Netty's event loop (the listener has not yet decremented),
        // pendingWriteBytes stays positive forever and hasPendingWrites() reports true
        // on a closed channel — violating the same drain-all invariant DIAG-3 closed for
        // asyncWriteFailure. close() must reset pendingWriteBytes as part of its drain.
        //
        // Determinism: we use the addPendingWriteBytesForTesting seam to plant the bytes
        // synchronously, because EmbeddedChannel's writeAndFlush success listener fires
        // in-place and decrements the counter back to zero before close() runs — the
        // test would pass without the close-side reset.
        EmbeddedChannel netty = new EmbeddedChannel();
        IoUringTransportLayer l = new IoUringTransportLayer(netty, REMOTE, LOCAL);

        l.addPendingWriteBytesForTesting(128);
        assertEquals(128, l.pendingWriteBytesSnapshot(),
            "precondition: planted bytes must show up in the snapshot");
        assertTrue(l.hasPendingWrites(),
            "precondition: positive pendingWriteBytes must make hasPendingWrites return true");

        l.close();

        assertEquals(0, l.pendingWriteBytesSnapshot(),
            "DIAG-4: close() must reset pendingWriteBytes so the closed-resource invariant " +
            "(every accessor reports a clean drained state) holds for the write-side counter, " +
            "symmetric to the DIAG-3 reset of asyncWriteFailure.");
        assertFalse(l.hasPendingWrites(),
            "DIAG-4: hasPendingWrites must report false on a closed channel even when a write " +
            "was in-flight at the moment of close. Without the reset, the counter would stay " +
            "positive until a Netty listener fired post-close (potentially never).");
    }

    @Test
    void postCloseAsyncWriteListenerDoesNotRepolluteAsyncWriteFailure() throws Exception {
        // CONC-F1: Netty completes writeAndFlush futures asynchronously on the event-loop
        // thread, so a listener registered just before close() runs on the Processor can
        // fire AFTER close() has reset asyncWriteFailure (DIAG-3) and pendingWriteBytes
        // (DIAG-4). Without a closed-state short-circuit at the top of the listener,
        // recordAsyncWriteFailure's CAS would succeed (slot is null again after close)
        // and re-pollute asyncWriteFailure — silently breaking the closed-resource
        // invariant that DIAG-3 closed for the Processor-thread side.
        //
        // Determinism: we use the onAsyncWriteCompleteForTesting seam to drive the
        // listener body directly. EmbeddedChannel completes promises synchronously on
        // the caller, so a real-channel test cannot observe the post-close interleaving
        // without flaky timing primitives. The seam delegates to the production method
        // (not a duplicated body), so a regression in the lambda → onAsyncWriteComplete
        // refactor would surface here.
        EmbeddedChannel netty = new EmbeddedChannel();
        IoUringTransportLayer l = new IoUringTransportLayer(netty, REMOTE, LOCAL);

        l.close();
        assertFalse(l.hasPendingWrites(),
            "precondition: close() must leave the channel drained");

        // Fire the listener as if Netty completed a failed writeAndFlush AFTER close().
        IOException stale = new IOException("post-close async write failure");
        l.onAsyncWriteCompleteForTesting(false, stale, 256);

        assertFalse(l.hasPendingWrites(),
            "CONC-F1: post-close listener must not re-pollute asyncWriteFailure via " +
            "recordAsyncWriteFailure's CAS. A non-null stashed failure after close would " +
            "make hasPendingWrites() report true on a torn-down channel, violating the " +
            "closed-resource invariant DIAG-3 established on the Processor-thread side.");
        assertEquals(0, l.pendingWriteBytesSnapshot(),
            "CONC-F1: post-close listener must not decrement pendingWriteBytes. close() " +
            "set the counter to 0; addAndGet(-256) without the guard would drive it to -256, " +
            "leaking a negative value to the snapshot seam and any future observability hook.");
    }

    @Test
    void preCloseAsyncWriteListenerStillMutatesState() throws Exception {
        // Anti-tautology check for CONC-F1. The closed-state guard must not be so eager
        // that it suppresses normal (pre-close) listener firings. If a regression flipped
        // the guard sense or fired it too early, this test catches it before the
        // post-close test could mask a broken happy path.
        EmbeddedChannel netty = new EmbeddedChannel();
        IoUringTransportLayer l = new IoUringTransportLayer(netty, REMOTE, LOCAL);

        l.addPendingWriteBytesForTesting(256);
        assertEquals(256, l.pendingWriteBytesSnapshot(),
            "precondition: planted bytes must show up in the snapshot");

        IOException cause = new IOException("pre-close failure");
        l.onAsyncWriteCompleteForTesting(false, cause, 256);

        assertEquals(0, l.pendingWriteBytesSnapshot(),
            "pre-close listener must still decrement pendingWriteBytes by the chunk size");
        assertTrue(l.hasPendingWrites(),
            "pre-close failure must still pollute asyncWriteFailure so the next write() " +
            "call surfaces the IOException to the Processor's FAILED_SEND path");
    }

    @Test
    void writeSurfacesOutOfDirectMemoryErrorFromAllocatorAsIOException() throws Exception {
        // B-CHAOS-2: nettyChannel.alloc().directBuffer(safeChunk) can throw
        // OutOfDirectMemoryError (a subclass of OutOfMemoryError → Error) when Netty's
        // direct-memory pool ceiling is hit. Without the alloc-site try/catch, the Error
        // escapes write() entirely — the TransportLayer contract requires IOException so
        // the Selector's write step routes the channel through FAILED_SEND. An escaping
        // Error bypasses FAILED_SEND, leaves the in-flight Send silently abandoned, and
        // surfaces as an unhandled Error in the Processor's run loop.
        //
        // We inject the failure with a custom allocator whose directBuffer() always
        // throws OutOfDirectMemoryError. Running the bug naturally would require pushing
        // direct memory to the pool ceiling, which is environment-dependent and slow;
        // the seam is honest because the production code path goes through exactly this
        // call (nettyChannel.alloc().directBuffer(safeChunk)).
        EmbeddedChannel netty = new EmbeddedChannel();
        netty.config().setAllocator(ALLOC_THROWS_DIRECT_OOM);
        IoUringTransportLayer l = new IoUringTransportLayer(netty, REMOTE, LOCAL);

        ByteBuffer src = ByteBuffer.wrap(new byte[]{1, 2, 3, 4});
        IOException ex = assertThrows(IOException.class, () -> l.write(src),
            "B-CHAOS-2: OutOfDirectMemoryError from the allocator must be wrapped in " +
            "IOException so the Selector's write step routes the channel through FAILED_SEND. " +
            "Without the wrap, the Error escapes write() and bypasses FAILED_SEND entirely.");
        assertTrue(ex.getCause() instanceof OutOfDirectMemoryError,
            "the cause chain must preserve the original OutOfDirectMemoryError so operators " +
            "can attribute the failure to direct-memory pressure rather than a generic write " +
            "error; got cause: " + ex.getCause());
        // src must not have been consumed — the failure happened before we copied bytes
        // out of it (the alloc throws before buf.writeBytes(src)).
        assertEquals(4, src.remaining(),
            "src ByteBuffer must remain untouched on alloc failure: writeBytes never ran, " +
            "so position/limit must be unchanged. Otherwise a retry by ByteBufferSend.writeTo " +
            "would skip bytes the broker never actually sent.");
        l.close();
    }

    /**
     * Test fixture for {@link #writeSurfacesOutOfDirectMemoryErrorFromAllocatorAsIOException}.
     * Heap allocations delegate to {@link UnpooledByteBufAllocator} so unrelated code paths
     * (Netty's outbound queue accounting, etc.) keep working; only directBuffer() throws.
     */
    private static final ByteBufAllocator ALLOC_THROWS_DIRECT_OOM = new AbstractByteBufAllocator(false) {
        @Override
        protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
            return UnpooledByteBufAllocator.DEFAULT.heapBuffer(initialCapacity, maxCapacity);
        }
        @Override
        protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
            throw newOutOfDirectMemoryError("test injection: direct memory pool exhausted");
        }
        @Override
        public boolean isDirectBufferPooled() {
            return false;
        }
    };

    /**
     * Constructs {@link OutOfDirectMemoryError} via reflection because Netty's
     * {@code OutOfDirectMemoryError(String)} constructor is package-private and Netty exposes
     * no public factory. Netty 4.x is an unnamed module on the classpath, so
     * {@code setAccessible(true)} succeeds without {@code --add-opens} (no JPMS opens needed).
     * A same-package bridge class would also have worked, but it forces the bridge into
     * {@code io.netty.util.internal} — a top-level package outside this checkstyle
     * import-control's {@code org.apache.kafka} root, which triggers
     * "Import control file does not handle this package". Reflection keeps the test
     * self-contained without touching checkstyle config or extending the root.
     */
    private static OutOfDirectMemoryError newOutOfDirectMemoryError(String message) {
        try {
            java.lang.reflect.Constructor<OutOfDirectMemoryError> c =
                OutOfDirectMemoryError.class.getDeclaredConstructor(String.class);
            c.setAccessible(true);
            return c.newInstance(message);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(
                "could not construct OutOfDirectMemoryError via reflection — Netty's " +
                "constructor signature changed or the JVM rejected setAccessible; the " +
                "B-CHAOS-2 fault-injection contract is broken until the test is updated",
                e);
        }
    }

    @Test
    void recordAsyncWriteFailureNeverDropsACauseUnderConcurrentConsumerRetryRace() throws Exception {
        // DIAG-5: the retry-CAS path in recordAsyncWriteFailure has a window where a third
        // listener can win the slot between our primary.get() (which returned null because
        // the Processor's getAndSet(null) raced in) and our CAS-2(null, cause). In that
        // window, CAS-2 fails because slot is no longer null, and the pre-fix code falls
        // through silently — our cause is lost. The channel still fails (via whichever
        // cause won the slot or was just consumed), but the lost cause is debugging info
        // operators rely on to attribute the failure to a specific write.
        //
        // Pre-fix code:
        //     if (!asyncWriteFailure.compareAndSet(null, cause)) {        // CAS-1
        //         Throwable primary = asyncWriteFailure.get();             // load
        //         if (primary == null) {
        //             asyncWriteFailure.compareAndSet(null, cause);        // CAS-2 — drop on fail
        //         } else {
        //             primary.addSuppressed(cause);
        //         }
        //     }
        //
        // Race interleaving that drops causeB:
        //   1. listener A: CAS-1(null, A) succeeds → slot = A
        //   2. listener B: CAS-1(null, B) fails (slot = A)
        //   3. Processor: getAndSet(null) returns A → slot = null
        //   4. listener B: primary.get() reads null
        //   5. listener C: CAS-1(null, C) succeeds → slot = C
        //   6. listener B: CAS-2(null, B) fails (slot = C, not null) → B silently dropped
        //
        // We can't reliably reproduce this exact interleaving with bare Thread.sleep, so we
        // run a stress test: N writer threads each plant K causes, one consumer thread
        // drains the slot in a tight loop, and at the end we account for every cause —
        // either consumed (returned by getAndSet) or suppressed under some consumed primary.
        // Any cause not accounted for is a drop. The pre-fix code reproducibly drops a
        // handful per million iterations; the fix accounts for 100% across the same run.
        //
        // Determinism: we use the package-private recordAsyncWriteFailureForTesting seam
        // (which calls the production recordAsyncWriteFailure verbatim — see
        // {@link IoUringTransportLayer#recordAsyncWriteFailureForTesting}) so this test
        // exercises the exact same code path real Netty listeners hit.
        //
        // Test design note: the consumer thread collects PRIMARIES only; it does NOT
        // snapshot {@code primary.getSuppressed()} during the run. Reason: in the
        // production code, a listener that lost the CAS does {@code primary.addSuppressed(cause)}
        // potentially AFTER the Processor has already drained the primary via
        // {@code getAndSet(null)}. In production this is fine because the Processor wraps
        // the primary in an IOException whose stack trace reflects the LIVE primary state
        // at the moment any reader inspects it — addSuppressed is thread-safe and visible
        // through subsequent getSuppressed() calls. But if the test snapshots
        // primary.getSuppressed() *while writers are still running*, late addSuppressed
        // calls land into the live primary but miss our snapshot, producing false-positive
        // "drops". So we collect primaries during the run and only walk their suppressed
        // lists ONCE, after all writers finish — by which time every addSuppressed has
        // completed and the suppressed lists are stable.
        IoUringTransportLayer l = newLayer();

        final int writers = 4;
        final int causesPerWriter = 50_000;
        final int totalCauses = writers * causesPerWriter;

        // ConcurrentLinkedQueue so consumer-thread offers and final aggregation are safe;
        // IdentityHashMap for the final accounting because every cause is a distinct
        // RuntimeException with a unique message (we want identity comparison anyway).
        ConcurrentLinkedQueue<Throwable> consumedPrimaries = new ConcurrentLinkedQueue<>();

        Throwable[][] causes = new Throwable[writers][causesPerWriter];
        for (int w = 0; w < writers; w++) {
            for (int i = 0; i < causesPerWriter; i++) {
                causes[w][i] = new RuntimeException("plant-w" + w + "-i" + i);
            }
        }

        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch writersDone = new CountDownLatch(writers);
        AtomicInteger consumerStop = new AtomicInteger(0);

        ExecutorService pool = Executors.newFixedThreadPool(writers + 1);
        try {
            pool.submit(consumerTask(l, start, consumerStop, consumedPrimaries));
            for (int w = 0; w < writers; w++) {
                pool.submit(writerTask(l, start, writersDone, causes[w]));
            }

            start.countDown();
            assertTrue(writersDone.await(60, TimeUnit.SECONDS),
                "writers must finish within 60s");
            // Let consumer finish its final-drain sweep before stopping it.
            Thread.sleep(50);
            consumerStop.set(1);
        } finally {
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS),
                "executor must terminate within 10s");
        }

        // Final defensive drain in case anything is still in the slot.
        Throwable trailing = l.asyncWriteFailureForTestingGetAndClear();
        if (trailing != null) {
            consumedPrimaries.add(trailing);
        }

        // Walk all consumed primaries NOW (after writers stopped) — every addSuppressed
        // has completed, suppressed lists are stable.
        Set<Throwable> consumed = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
        Set<Throwable> suppressed = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable primary : consumedPrimaries) {
            consumed.add(primary);
            for (Throwable s : primary.getSuppressed()) {
                suppressed.add(s);
            }
        }

        int missing = countMissing(causes, consumed, suppressed);
        int accountedFor = totalCauses - missing;

        assertEquals(totalCauses, accountedFor,
            "DIAG-5: every planted cause must be either consumed as a primary or chained as " +
            "a suppressed exception. Pre-fix the retry-CAS at recordAsyncWriteFailure dropped " +
            "causes silently when a third listener won the slot between our null-read and our " +
            "CAS-2; missing count was " + missing + " out of " + totalCauses + ".");
        assertEquals(0, missing,
            "DIAG-5: no planted cause may be unaccounted for (no consumed, no suppressed). " +
            "missing=" + missing);
    }

    private static Runnable consumerTask(IoUringTransportLayer l,
                                         CountDownLatch start,
                                         AtomicInteger consumerStop,
                                         ConcurrentLinkedQueue<Throwable> consumedPrimaries) {
        return () -> {
            try {
                start.await();
                while (consumerStop.get() == 0) {
                    drainOnce(l, consumedPrimaries);
                }
                // Final drain sweep — under load a writer's last plant may land between
                // the consumer's last loop iteration and consumerStop flipping.
                for (int i = 0; i < 64; i++) {
                    drainOnce(l, consumedPrimaries);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
    }

    private static Runnable writerTask(IoUringTransportLayer l,
                                       CountDownLatch start,
                                       CountDownLatch writersDone,
                                       Throwable[] myCauses) {
        return () -> {
            try {
                start.await();
                for (Throwable c : myCauses) {
                    l.recordAsyncWriteFailureForTesting(c);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                writersDone.countDown();
            }
        };
    }

    private static void drainOnce(IoUringTransportLayer l,
                                  ConcurrentLinkedQueue<Throwable> consumedPrimaries) {
        Throwable primary = l.asyncWriteFailureForTestingGetAndClear();
        if (primary != null) {
            consumedPrimaries.add(primary);
        }
    }

    private static int countMissing(Throwable[][] causes,
                                    Set<Throwable> consumed,
                                    Set<Throwable> suppressed) {
        int missing = 0;
        for (Throwable[] row : causes) {
            for (Throwable c : row) {
                if (!consumed.contains(c) && !suppressed.contains(c)) {
                    missing++;
                }
            }
        }
        return missing;
    }
}
