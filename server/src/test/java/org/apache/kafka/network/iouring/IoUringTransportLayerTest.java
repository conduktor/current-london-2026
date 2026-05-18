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
import static org.junit.jupiter.api.Assertions.assertSame;
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
}
