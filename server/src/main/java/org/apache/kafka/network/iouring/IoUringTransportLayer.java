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
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.Principal;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * {@link TransportLayer} that bridges Kafka's {@code KafkaChannel} to a Netty
 * {@code IoUringSocketChannel} (or, in tests, any Netty {@code Channel}).
 *
 * <p>Two threads touch this object:
 * <ul>
 *   <li>The <strong>Processor thread</strong> calls {@code read}, {@code write},
 *       {@code disconnect}, {@code close}, {@code hasBytesBuffered}, the interest-ops
 *       methods, and the various getters.</li>
 *   <li>The <strong>Netty event-loop thread</strong> calls {@link #offerInbound(ByteBuf)}
 *       on every {@code channelRead} and {@link #markEof()} on {@code channelInactive}.
 *       It must never call into the read/write/close methods.</li>
 * </ul>
 *
 * <p>Cross-thread state lives in a {@link ConcurrentLinkedQueue} of inbound
 * {@link ByteBuf}s plus two {@code volatile} flags ({@code eofSeen}, {@code closed}).
 * Outbound writes call {@link Channel#writeAndFlush(Object)}, which Netty documents as
 * safe to call from any thread — internally it schedules onto the channel's own event
 * loop.
 *
 * <p>The interest-ops methods are no-ops at the kernel layer. Kafka's {@code KafkaChannel}
 * updates the interest mask to signal "I want OP_READ" / "I want OP_WRITE" — under NIO
 * that registers with the JDK Selector; here io_uring drives reads autonomously
 * (via Netty's autoRead) and writes via {@link Channel#writeAndFlush(Object)}, so the
 * interest mask is purely advisory. {@link #isMute()} still derives from it, exactly as
 * {@code PlaintextTransportLayer} does, so {@code KafkaChannel.isMuted()} matches the
 * NIO baseline.
 */
final class IoUringTransportLayer implements TransportLayer {

    private final Channel nettyChannel;
    private final StubSocketChannel socketChannel;
    private final NoopSelectionKey selectionKey;

    private final Queue<ByteBuf> inbound = new ConcurrentLinkedQueue<>();
    private volatile boolean eofSeen;
    private volatile boolean closed;
    /**
     * Bytes still riding inside Netty's outbound buffer for this channel — incremented on
     * write(), decremented on writeAndFlush completion. {@link #hasPendingWrites()} reads
     * this so {@code KafkaChannel.maybeCompleteSend} only reports "send done" once the
     * bytes have actually been handed off to the kernel by Netty, not just queued.
     */
    private final AtomicLong pendingWriteBytes = new AtomicLong(0);

    IoUringTransportLayer(Channel nettyChannel, InetSocketAddress remote, InetSocketAddress local) {
        this.nettyChannel = nettyChannel;
        this.socketChannel = new StubSocketChannel(remote, local);
        this.selectionKey = new NoopSelectionKey();
        // Match Selector.register: freshly registered channels start with OP_READ set so
        // isMute() reports false. io_uring's autoRead=true is the equivalent of OP_READ.
        this.selectionKey.interestOps(SelectionKey.OP_READ);
    }

    /**
     * Called from the Netty event-loop thread on every {@code channelRead}. The buffer
     * is consumed by subsequent {@link #read(ByteBuffer)} calls on the Processor thread.
     *
     * <p>The closed-check is repeated <em>after</em> the offer to close a leak window
     * with the Processor's {@link #close()}: if Processor sets {@code closed=true} and
     * drains the queue in between our read of {@code closed} and our {@code offer()},
     * the buf we just queued would never be released. Re-checking and draining ourselves
     * keeps the contract that every {@code offerInbound} either delivers the buf or
     * releases it.
     */
    void offerInbound(ByteBuf buf) {
        if (closed) {
            buf.release();
            return;
        }
        inbound.offer(buf);
        if (closed) {
            ByteBuf b;
            while ((b = inbound.poll()) != null) {
                b.release();
            }
        }
    }

    /**
     * Called from the Netty event-loop thread on {@code channelInactive}. Subsequent
     * {@link #read(ByteBuffer)} calls drain whatever is still queued, then start
     * returning {@code -1}.
     */
    void markEof() {
        eofSeen = true;
    }

    @Override
    public boolean ready() {
        return true;
    }

    @Override
    public boolean finishConnect() {
        return true;
    }

    @Override
    public void disconnect() {
        close();
    }

    @Override
    public boolean isConnected() {
        return !closed && nettyChannel.isActive();
    }

    @Override
    public SocketChannel socketChannel() {
        return socketChannel;
    }

    @Override
    public SelectionKey selectionKey() {
        return selectionKey;
    }

    @Override
    public void handshake() {
        // PLAINTEXT: nothing to do.
    }

    @Override
    public Principal peerPrincipal() {
        return KafkaPrincipal.ANONYMOUS;
    }

    @Override
    public void addInterestOps(int ops) {
        int before = selectionKey.interestOps();
        int after = before | ops;
        selectionKey.interestOps(after);
        // Mirror the NIO contract: when OP_READ comes back in the interest mask the channel
        // is once again accepting reads. Flip Netty autoRead on so the kernel resumes pushing
        // bytes. This is what KafkaChannel.maybeUnmute() relies on after MemoryPool pressure
        // releases, and it is also the symmetric counterpart of removeInterestOps below.
        if ((after & SelectionKey.OP_READ) != 0 && (before & SelectionKey.OP_READ) == 0
                && nettyChannel.isOpen() && !nettyChannel.config().isAutoRead()) {
            nettyChannel.config().setAutoRead(true);
        }
    }

    @Override
    public void removeInterestOps(int ops) {
        int before = selectionKey.interestOps();
        int after = before & ~ops;
        selectionKey.interestOps(after);
        // On the NIO path, removing OP_READ takes the SocketChannel out of the JDK selector's
        // ready set so the broker stops draining the kernel buffer. On io_uring there is no
        // SelectionKey driving the kernel — Netty's autoRead does. Reflect the mute side-effect
        // here so KafkaChannel.mute() (called internally when MemoryPool.tryAllocate returns
        // null) actually applies kernel-level backpressure instead of letting bytes pile up
        // in the transport's inbound queue.
        if ((after & SelectionKey.OP_READ) == 0 && (before & SelectionKey.OP_READ) != 0
                && nettyChannel.isOpen() && nettyChannel.config().isAutoRead()) {
            nettyChannel.config().setAutoRead(false);
        }
    }

    @Override
    public boolean isMute() {
        return (selectionKey.interestOps() & SelectionKey.OP_READ) == 0;
    }

    @Override
    public boolean hasBytesBuffered() {
        return !inbound.isEmpty();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (closed) throw new IOException("transport layer is closed");
        int total = 0;
        while (dst.hasRemaining()) {
            ByteBuf head = inbound.peek();
            if (head == null) break;
            int toRead = Math.min(head.readableBytes(), dst.remaining());
            // Bound the destination so head.readBytes never over-reads — Netty's
            // readBytes(ByteBuffer) keeps going until dst is full.
            int savedLimit = dst.limit();
            dst.limit(dst.position() + toRead);
            head.readBytes(dst);
            dst.limit(savedLimit);
            total += toRead;
            if (!head.isReadable()) {
                inbound.poll();
                head.release();
            }
        }
        if (total == 0 && eofSeen && inbound.isEmpty()) return -1;
        return total;
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        long total = 0;
        boolean anyProgress = false;
        for (int i = 0; i < length; i++) {
            ByteBuffer dst = dsts[offset + i];
            if (!dst.hasRemaining()) continue;
            int n = read(dst);
            if (n == -1) {
                return anyProgress ? total : -1;
            }
            if (n == 0) break;
            total += n;
            anyProgress = true;
        }
        return total;
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return read(dsts, 0, dsts.length);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if (closed) throw new IOException("transport layer is closed");
        int remaining = src.remaining();
        if (remaining == 0) return 0;
        // Use a pooled direct buffer so io_uring can submit the bytes without a heap-to-
        // direct intermediate copy. The buffer is released by Netty after the channel has
        // flushed it; we only own the writeAndFlush completion listener.
        ByteBuf buf = nettyChannel.alloc().directBuffer(remaining);
        // Anything between allocation and writeAndFlush taking ownership must release the
        // buf on failure — otherwise the pooled allocator slowly bleeds direct memory.
        boolean handedOff = false;
        try {
            buf.writeBytes(src);
            // Install the dec listener BEFORE incrementing pendingWriteBytes. If the
            // listener registration itself throws synchronously (DefaultPromise can
            // throw when the executor is shut down), inc'ing first would strand the
            // counter positive — hasPendingWrites() then permanently reports true and
            // KafkaChannel.maybeCompleteSend never advances. The ByteBuf itself is
            // released by writeAndFlush's promise on either path (success or failure),
            // so the only leak we have to defend against here is the counter.
            io.netty.channel.ChannelFuture future = nettyChannel.writeAndFlush(buf);
            handedOff = true;
            future.addListener(f -> pendingWriteBytes.addAndGet(-remaining));
            pendingWriteBytes.addAndGet(remaining);
        } finally {
            if (!handedOff) {
                buf.release();
            }
        }
        return remaining;
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        long total = 0;
        for (int i = 0; i < length; i++) {
            ByteBuffer src = srcs[offset + i];
            if (!src.hasRemaining()) continue;
            total += write(src);
        }
        return total;
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return write(srcs, 0, srcs.length);
    }

    @Override
    public boolean hasPendingWrites() {
        // Use our own counter, not nettyChannel.isWritable(): the latter only flips when
        // the outbound queue crosses Netty's high water mark, so any send below the mark
        // would otherwise report "fully drained" the instant write() returned — even though
        // the bytes are still sitting in Netty's queue waiting for the event loop to flush.
        return pendingWriteBytes.get() > 0;
    }

    @Override
    public long transferFrom(FileChannel fileChannel, long position, long count) throws IOException {
        // Naive read-then-write. v1 does not chase the zero-copy fast path; the broker
        // serves PLAINTEXT log segments through the same code path as any other write,
        // so correctness comes first.
        int chunk = (int) Math.min(count, 64 * 1024);
        ByteBuffer buf = ByteBuffer.allocate(chunk);
        int n = fileChannel.read(buf, position);
        if (n <= 0) return 0;
        buf.flip();
        return write(buf);
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        try {
            if (nettyChannel.isOpen()) nettyChannel.close();
        } catch (Exception ignored) { /* close is best-effort */ }
        ByteBuf b;
        while ((b = inbound.poll()) != null) {
            b.release();
        }
        selectionKey.cancel();
    }
}
