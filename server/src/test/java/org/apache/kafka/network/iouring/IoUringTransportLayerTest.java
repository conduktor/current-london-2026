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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
}
