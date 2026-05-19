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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.atomic.AtomicReference;

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

    /**
     * Hard upper bound on the number of bytes a single {@link #write(ByteBuffer)} call will
     * allocate in one direct {@link ByteBuf}. Two failure modes are bounded by this cap:
     *
     * <ol>
     *   <li><b>Direct-memory OOM under a single huge response.</b> A 100 MiB Fetch response
     *       reaching a channel whose Netty outbound buffer is empty would otherwise allocate
     *       a 100 MiB direct buffer in one shot — bypassing the high-water-mark gate, which
     *       only flips after the queue is already past the limit. Capping each call to 1 MiB
     *       keeps the per-call footprint bounded; the source ByteBuffer keeps its remainder
     *       and ByteBufferSend re-enters on the next poll, mirroring how NIO returns partial
     *       progress when {@code SO_SNDBUF} saturates.</li>
     *   <li><b>Direct-memory exhaustion across many slow consumers.</b> Without the cap,
     *       N concurrent in-flight big sends each allocate N * responseSize. Even with the
     *       Netty watermark gate, a burst of slow consumers can collectively exceed direct
     *       memory before any single channel trips the gate. The cap bounds the
     *       worst-case per-call allocation, regardless of how many channels are writing.</li>
     * </ol>
     *
     * <p>1 MiB is a deliberate balance: small enough to keep direct memory bounded under DoS,
     * large enough that typical Kafka response sizes (Produce ack, Fetch chunk) drain in a
     * couple of poll iterations rather than thousands.
     */
    static final int MAX_WRITE_CHUNK_BYTES = 1 << 20;

    /**
     * High-water mark for the per-channel inbound ByteBuf queue. When {@link #inboundBytes}
     * exceeds this, {@link #offerInbound(ByteBuf)} flips Netty {@code autoRead} off so the
     * kernel applies TCP-level backpressure to the peer. Without this gate, a fast malicious
     * PLAINTEXT client (or simply a slow Processor) lets ByteBufs accumulate unbounded in
     * the transport queue — direct memory exhaustion takes the broker down before the
     * KafkaChannel-level MemoryPool gate (queued.max.bytes) even kicks in, because the
     * MemoryPool only governs the destination NetworkReceive buffer, not the raw bytes
     * still riding in Netty's inbound side.
     */
    static final int INBOUND_HIGH_WATERMARK_BYTES = 1 << 20;

    /**
     * Low-water mark for the per-channel inbound ByteBuf queue. Once {@link #inboundBytes}
     * drops below this after a Processor drain, {@link #read} flips Netty {@code autoRead}
     * back on so kernel reads resume. The HIGH-LOW gap prevents oscillation: a single
     * fluctuation around the high water mark would otherwise trigger constant autoRead
     * flapping. Only re-enabled when the channel is not muted ({@link #isMute()}) — an
     * operator mute or memory-pool self-mute must keep autoRead off regardless of queue
     * depth, otherwise the request-pipeline throttle is bypassed.
     */
    static final int INBOUND_LOW_WATERMARK_BYTES = 1 << 18;

    private static final Logger log = LoggerFactory.getLogger(IoUringTransportLayer.class);

    private final Channel nettyChannel;
    private final StubSocketChannel socketChannel;
    private final NoopSelectionKey selectionKey;

    private final Queue<ByteBuf> inbound = new ConcurrentLinkedQueue<>();
    /**
     * Total readable bytes currently sitting in {@link #inbound}. Bumped from the event-loop
     * thread inside {@link #offerInbound}, drained from the Processor thread inside
     * {@link #read}. Drives the autoRead watermark gate so a slow Processor (or fast/abusive
     * peer) cannot accumulate unbounded direct memory in the transport queue.
     *
     * <p>The counter is also decremented in {@link #close()} when remaining ByteBufs are
     * released, so a teardown does not leave the counter falsely positive should the
     * channel be re-used (it is not, in v1, but the invariant is cheap to maintain).
     */
    private final AtomicLong inboundBytes = new AtomicLong(0);
    /** Cumulative bytes ever pushed into {@link #inbound} by Netty event loop. Test-only diagnostic. */
    private final AtomicLong totalInboundBytes = new AtomicLong(0);
    /** Number of offerInbound calls. Test-only diagnostic. */
    private final AtomicLong offerInboundCalls = new AtomicLong(0);
    private volatile boolean eofSeen;
    private volatile boolean closed;
    /**
     * Bytes still riding inside Netty's outbound buffer for this channel — incremented on
     * write(), decremented on writeAndFlush completion. {@link #hasPendingWrites()} reads
     * this so {@code KafkaChannel.maybeCompleteSend} only reports "send done" once the
     * bytes have actually been handed off to the kernel by Netty, not just queued.
     */
    private final AtomicLong pendingWriteBytes = new AtomicLong(0);
    /**
     * Async write failure surfaced by Netty's writeAndFlush promise. If the kernel rejected
     * the write (peer RST mid-response, kernel buffer pressure, channel closed under us),
     * Netty calls our listener with {@code !future.isSuccess()}; we cannot route the failure
     * back to the Processor inline because the listener runs on the event loop. Instead we
     * stash the cause here and re-throw it from the next {@link #write(ByteBuffer)} call.
     * That converts the silent async failure into a synchronous {@link IOException} that
     * {@link IoUringSelector#poll(long)}'s write step catches and routes through
     * {@code ChannelState.FAILED_SEND} — matching how NIO surfaces a peer-RST'd write
     * through its {@code Selector.poll} loop.
     *
     * <p>{@link AtomicReference} (not bare {@code volatile}) so the Processor's read-and-clear
     * is atomic and so multiple concurrent listener firings preserve all causes via
     * {@link Throwable#addSuppressed(Throwable)} rather than the last-one-wins overwrite a
     * raw {@code volatile} assignment produces. Concretely:
     *
     * <ol>
     *   <li><b>DIAG-1 (multiple listener stomp).</b> A peer RST against a channel with N
     *       in-flight writes causes Netty to fail every queued promise; each listener fires
     *       on the event-loop thread, sequentially. A raw {@code = f.cause()} keeps only the
     *       last one. Operators investigating a flapping client see the tail cause and miss
     *       the head — which is often the actual triggering exception (the rest are
     *       {@code ClosedChannelException}s reacting to the first one's tear-down).</li>
     *   <li><b>DIAG-2 (read-then-clear race).</b> The previous shape
     *       {@code Throwable f = field; if (f != null) field = null; throw …;} has a window
     *       between the load and the clear where a listener could store a new cause; the
     *       clear then wipes it and the next {@link #write(ByteBuffer)} returns normally,
     *       silently swallowing the failure. {@link AtomicReference#getAndSet(Object)}
     *       collapses the read and clear into one CAS so a concurrent set is either seen
     *       (and surfaced) or happens after the clear (and surfaces on the next write).</li>
     * </ol>
     *
     * <p>The listener thread is single-threaded per channel (Netty event loop), so listener-
     * vs-listener races are not possible — only listener vs Processor. Per analysis: with
     * the addSuppressed pattern below, every cause is either (a) the primary thrown
     * IOException's cause, (b) suppressed under that primary, or (c) the primary of a
     * subsequent {@link #write(ByteBuffer)} throw. None are dropped.
     */
    private final AtomicReference<Throwable> asyncWriteFailure = new AtomicReference<>();

    /**
     * Wakes the owning {@link IoUringSelector}'s blocking {@code poll(timeoutMs)} when the
     * Netty writeAndFlush promise completes async on the event-loop thread. Without this,
     * the listener decrements {@link #pendingWriteBytes} silently — the Processor thread
     * stays asleep on {@code wakeup.acquireWithTimeout} until the next event (300 ms by
     * default) and {@code completedSends}/{@code RESPONSE_SENT} are delayed by that whole
     * window for every small response. {@code onWritabilityChanged} only fires when the
     * outbound buffer crosses high/low water marks, so a typical sub-watermark response
     * (e.g. 1 KB) never triggers it. We therefore call this Runnable from inside the
     * writeAndFlush listener, after {@link #pendingWriteBytes} is decremented and after
     * any {@link #asyncWriteFailure} is recorded.
     *
     * <p>EmbeddedChannel completes writeAndFlush synchronously on the calling thread, so
     * unit tests using EmbeddedChannel never expose the stall this callback prevents;
     * see {@code asyncWriteCompletionWakesSelector} for the regression test that uses an
     * out-of-loop completion to actually exercise the async path.
     */
    private final Runnable writeWakeCallback;

    IoUringTransportLayer(Channel nettyChannel, InetSocketAddress remote, InetSocketAddress local) {
        this(nettyChannel, remote, local, () -> { });
    }

    IoUringTransportLayer(Channel nettyChannel, InetSocketAddress remote, InetSocketAddress local,
                          Runnable writeWakeCallback) {
        this.nettyChannel = nettyChannel;
        this.socketChannel = new StubSocketChannel(remote, local);
        this.selectionKey = new NoopSelectionKey();
        this.writeWakeCallback = writeWakeCallback;
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
        int size = buf.readableBytes();
        offerInboundCalls.incrementAndGet();
        totalInboundBytes.addAndGet(size);
        // Increment-before-offer ordering. If we offered first and then incremented, a
        // concurrent reader could peek the buf, drain it, and run inboundBytes.addAndGet(-total)
        // BEFORE our addAndGet(+size) lands — making inboundBytes transiently negative and
        // tripping the LOW watermark check on the read path. Incrementing first guarantees the
        // counter is at least as large as the queued bytes at every observable moment; the
        // counter can briefly OVER-estimate (we added size, but haven't queued yet) which is
        // safe — the watermark gate erring conservative protects against direct-memory OOM.
        long after = inboundBytes.addAndGet(size);
        inbound.offer(buf);
        if (closed) {
            // Race with Processor's close(): drain anything we just queued so we don't leak.
            ByteBuf b;
            while ((b = inbound.poll()) != null) {
                b.release();
            }
            inboundBytes.set(0);
            return;
        }
        // Inbound watermark gate. When the queue crosses the high water mark, flip Netty
        // autoRead off so the kernel stops pushing more bytes to us — TCP's own flow
        // control will throttle the peer. Without this gate, a fast/abusive PLAINTEXT
        // client (or simply a slow Processor) lets ByteBufs accumulate unbounded in the
        // inbound queue; direct-memory OOM takes the broker down before KafkaChannel's
        // MemoryPool-based queued.max.bytes throttle even fires (that throttle governs
        // the destination NetworkReceive buffer, not the bytes still sitting in this
        // queue waiting to be drained).
        //
        // setAutoRead is called inline on the caller's thread. offerInbound is contracted
        // to be invoked from the channel's event loop (the production caller is the Netty
        // pipeline's channelRead, and EmbeddedChannel in tests routes channelRead
        // synchronously on the test thread — same single-threaded discipline). The
        // companion gate in read() reconciles autoRead back on if this flip races with
        // a concurrent Processor drain (see AUTOREAD-RACE).
        if (after >= INBOUND_HIGH_WATERMARK_BYTES && nettyChannel.config().isAutoRead()) {
            nettyChannel.config().setAutoRead(false);
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
        // Mirror the NIO contract: when OP_READ comes back in the interest mask the channel is
        // once again accepting reads. Flip Netty autoRead on so the kernel resumes pushing bytes.
        // BUT we must still honor the inbound watermark gate — if inboundBytes is above LOW, the
        // queue is already pressurized and re-enabling reads here would let the kernel push more
        // bytes past HIGH_WATERMARK before the Processor drains. Leave autoRead off in that case;
        // the read path (above, around the inboundBytes.addAndGet(-total) decrement) will flip it
        // back on once the Processor has drained the queue below LOW_WATERMARK, matching the
        // identical condition there.
        boolean opReadFreshlyAdded = (after & SelectionKey.OP_READ) != 0
                && (before & SelectionKey.OP_READ) == 0;
        if (opReadFreshlyAdded && nettyChannel.isOpen() && !nettyChannel.config().isAutoRead()
                && inboundBytes.get() <= INBOUND_LOW_WATERMARK_BYTES) {
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
        // Mirror NIO PlaintextTransportLayer.isMute (clients/src/main/java/org/apache/kafka/
        // common/network/PlaintextTransportLayer.java:203-204): the key.isValid() short-circuit
        // is what makes isMute() return false after close() ran cancel() on the key. Without
        // this, the cached interestOps still reads "OP_READ cleared" if the transport happened
        // to be muted at close time, so isMute() reports true on a dead channel — NIO reports
        // false. Any caller treating isMute() as "channel is healthy and currently throttled"
        // would diverge: a future KafkaChannel introspection path that reads isMute() post-
        // close would observe the wrong value on io_uring vs NIO. Current production callers
        // (NetworkSend metric tagging, channel selector iteration) all gate on the channel
        // being live before reading isMute(), so this is currently latent — but the parity
        // landmine is one line away from being defused, so defuse it.
        return selectionKey.isValid() && (selectionKey.interestOps() & SelectionKey.OP_READ) == 0;
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
        if (total > 0) {
            inboundBytes.addAndGet(-total);
        }
        // Inbound watermark gate (read-side): once the queue has fallen below the LOW
        // water mark, flip autoRead back on so the kernel resumes pushing bytes. Only
        // re-enable on an unmuted channel — an operator mute (RESPONSE_QUEUED throttling)
        // or memory-pool self-mute (KafkaChannel.read failed to allocate) must keep
        // autoRead off regardless of queue depth, otherwise the request-pipeline throttle
        // is bypassed.
        //
        // CRITICAL: this reconciliation MUST run on EVERY read() call, not only when
        // total > 0. The Processor calls read() on every poll for every registered
        // channel; if we gated this on total > 0, the following silent-stall race
        // becomes terminal:
        //   1. queue near HIGH, autoRead=true. Event-loop offerInbound runs its
        //      addAndGet(+size) which crosses HIGH, but is preempted before the
        //      setAutoRead(false) call lands.
        //   2. Processor.read drains the entire queue. total > 0 path runs, addAndGet(-total)
        //      gives after=0 — but the gate condition !isAutoRead() is FALSE (autoRead is
        //      still true because the event loop's flip-off hasn't lapsed yet). Gate skips.
        //   3. Event loop resumes and runs setAutoRead(false) based on its stale local
        //      `after` value.
        //   4. End state: queue=0, autoRead=false, channel unmuted, peer still wants
        //      to send. The kernel honors TCP backpressure and stops pushing, so no
        //      further offerInbound runs. read() returns 0 every poll. Nothing recovers
        //      until idle expiry (~10 min) closes the connection.
        // Running this gate every read() means the next Processor poll cycle heals the
        // race within microseconds.
        if (inboundBytes.get() <= INBOUND_LOW_WATERMARK_BYTES
                && !isMute()
                && nettyChannel.isOpen()
                && !nettyChannel.config().isAutoRead()) {
            nettyChannel.config().setAutoRead(true);
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

    /**
     * Store an async writeAndFlush failure for the Processor's next write() call to surface.
     * Preserves the first cause as the primary; subsequent causes chain as suppressed.
     *
     * <p>Race with the Processor's {@code asyncWriteFailure.getAndSet(null)}:
     * <ul>
     *   <li>If our CAS-null-to-cause wins, the next Processor write surfaces our cause.</li>
     *   <li>If CAS fails because a primary is already set, we addSuppressed under it. The
     *       Processor may have just consumed that primary via getAndSet(null), in which case
     *       the IOException it's about to throw still carries the primary (and thus our
     *       suppressed cause), so nothing is lost.</li>
     *   <li>If CAS fails AND the primary read returns null (Processor's getAndSet(null) raced
     *       between our failed CAS and the load), the slot is empty; retry the CAS. Worst
     *       case the next listener firing addSuppresseds under us — never lost.</li>
     * </ul>
     */
    private void recordAsyncWriteFailure(Throwable cause) {
        if (!asyncWriteFailure.compareAndSet(null, cause)) {
            Throwable primary = asyncWriteFailure.get();
            if (primary == null) {
                asyncWriteFailure.compareAndSet(null, cause);
            } else {
                primary.addSuppressed(cause);
            }
        }
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if (closed) throw new IOException("transport layer is closed");
        // If a prior async write already failed, surface it now so the Processor's write
        // step routes the channel through FAILED_SEND rather than reporting completedSend
        // for bytes the kernel never delivered.
        //
        // DIAG-2: read-and-clear must be atomic. A naive load-then-store-null shape leaves
        // a window where a listener could store a fresh cause between our load (which sees
        // the prior cause) and our clear (which wipes the fresh one). The fresh cause is
        // silently dropped — the next write proceeds normally and Send.completed() returns
        // true on bytes the kernel never delivered. {@link AtomicReference#getAndSet}
        // collapses the two operations into one CAS: a concurrent set is either reflected
        // in our return value (and surfaces here) or happens strictly after our clear (and
        // surfaces on the next write call).
        Throwable failure = asyncWriteFailure.getAndSet(null);
        if (failure != null) {
            throw new IOException("async write failed", failure);
        }
        int remaining = src.remaining();
        if (remaining == 0) return 0;
        // Backpressure + bounded allocation. Two failure modes have to be defended here:
        //
        //   (a) The classic "Netty outbound buffer past high water mark" case: a slow peer
        //       isn't draining fast enough, isWritable() is false, queueing more bytes
        //       would bloat direct memory until the broker OOMs. Return 0; the next
        //       Selector poll re-attempts, and onWritabilityChanged() wakes the poll the
        //       moment the buffer drains below the low water mark.
        //
        //   (b) The "huge response in one shot" case: a 100MB Fetch response arrives at a
        //       channel whose outbound buffer is currently empty — isWritable() is true.
        //       The naive implementation allocates a 100MB direct ByteBuf in a single call
        //       BEFORE the high water mark gate kicks in. Many slow consumers in flight at
        //       once can each spike 100MB of direct memory, bypassing the gate entirely.
        //       The fix is to cap each write to {@code bytesBeforeUnwritable()} (Netty's
        //       remaining headroom under the high water mark) AND to a hard
        //       {@link #MAX_WRITE_CHUNK_BYTES} chunk so a single call cannot drown the
        //       pooled allocator. ByteBufferSend re-enters write() on the next poll and the
        //       source ByteBuffer keeps its remainder, so correctness for large responses
        //       is preserved — only the per-call allocation footprint changes.
        //
        // Restrict (a) to healthy channels (isActive() = open + connected). For a closed/
        // disconnected channel, isWritable() is false too — but we WANT those writes to
        // proceed so writeAndFlush's promise fails and surfaces the error through
        // asyncWriteFailure. Returning 0 on a dead channel would silently swallow the
        // error: ByteBufferSend.writeTo would loop forever waiting for writability that
        // never comes, while the Send is silently considered "in flight".
        long room;
        if (nettyChannel.isActive()) {
            room = nettyChannel.bytesBeforeUnwritable();
            if (room <= 0) return 0;
        } else {
            // Channel already torn down — bypass the watermark gate so the writeAndFlush
            // failure path surfaces. Cap by MAX_WRITE_CHUNK_BYTES regardless: even on a
            // dying channel we don't want to over-allocate before the failure surfaces.
            room = MAX_WRITE_CHUNK_BYTES;
        }
        int safeChunk = (int) Math.min((long) remaining, Math.min(room, (long) MAX_WRITE_CHUNK_BYTES));
        // Use a pooled direct buffer so io_uring can submit the bytes without a heap-to-
        // direct intermediate copy. The buffer is released by Netty after the channel has
        // flushed it; we only own the writeAndFlush completion listener.
        ByteBuf buf = nettyChannel.alloc().directBuffer(safeChunk);
        // Anything between allocation and writeAndFlush taking ownership must release the
        // buf on failure — otherwise the pooled allocator slowly bleeds direct memory.
        boolean handedOff = false;
        try {
            // Bound src so writeBytes only consumes safeChunk bytes. ByteBufferSend.writeTo
            // re-enters on the next poll with the remainder, mirroring how NIO's
            // SocketChannel.write returns partial progress when SO_SNDBUF saturates.
            int savedLimit = src.limit();
            src.limit(src.position() + safeChunk);
            buf.writeBytes(src);
            src.limit(savedLimit);
            // Register the decrement-listener call (line below) BEFORE the increment
            // statement (further below). Two reasons:
            // (1) If listener registration itself throws synchronously — DefaultPromise
            //     can RejectedExecutionException when the executor is shut down — inc'ing
            //     first would strand the counter positive forever: hasPendingWrites() would
            //     read true on every subsequent poll and KafkaChannel.maybeCompleteSend
            //     would never fire. The ByteBuf is released by writeAndFlush's promise on
            //     either path (success or failure), so the only leak this catches is the
            //     counter.
            // (2) This ordering does mean that if writeAndFlush completes synchronously
            //     (the buffer goes straight to the wire because the outbound queue is
            //     drained and the event-loop happens to flush inline), addListener fires
            //     the body inline — decrementing to -safeChunk briefly before the
            //     increment below restores it to 0. That transient negative is benign:
            //     the Processor is single-threaded for this channel, so no other observer
            //     reads pendingWriteBytes during the window between addListener returning
            //     and the addAndGet below executing.
            try {
                io.netty.channel.ChannelFuture future = nettyChannel.writeAndFlush(buf);
                handedOff = true;
                future.addListener(f -> {
                    if (!f.isSuccess()) {
                        // Record the cause so the next Processor write step can throw it
                        // synchronously and route the channel through FAILED_SEND. Without
                        // this, a peer RST mid-response leaves the broker thinking the send
                        // completed normally — completedSends fires, RESPONSE_SENT mute event
                        // succeeds, and the request handling pipeline silently advances on a
                        // request the client never saw. Set BEFORE decrementing pendingWriteBytes:
                        // a reader observing the listener mid-flight must not see
                        // (pendingWriteBytes == 0 && asyncWriteFailure == null) — that window is
                        // the exact false-success window where ByteBufferSend.completed() returns
                        // true and KafkaChannel.maybeCompleteSend() emits a Send the kernel rejected.
                        //
                        // DIAG-1: preserve all causes when multiple listeners fire (e.g. a
                        // peer RST against a channel with N in-flight writes). First cause
                        // wins as the IOException's cause; subsequent causes are chained as
                        // suppressed exceptions. See field Javadoc for the race analysis.
                        recordAsyncWriteFailure(f.cause());
                    }
                    pendingWriteBytes.addAndGet(-safeChunk);
                    // Wake the Processor's poll(). The listener runs on Netty's event loop
                    // thread (a separate thread from the Processor in production), so without
                    // this callback the Processor stays asleep on its wakeup Semaphore until
                    // the poll timeout expires — even though hasPendingWrites() now reads
                    // false and maybeCompleteSend() would return the completed Send on the
                    // very next poll iteration. For a small response that never crosses the
                    // outbound watermark, channelWritabilityChanged is not invoked, so no
                    // other wakeup source exists. Result without this line: every small
                    // request/response incurs an extra ~300 ms (the default poll timeout)
                    // before the broker emits RESPONSE_SENT. The bug is invisible under
                    // EmbeddedChannel because that channel completes writeAndFlush futures
                    // synchronously on the calling thread.
                    writeWakeCallback.run();
                });
                pendingWriteBytes.addAndGet(safeChunk);
            } catch (RuntimeException e) {
                // Netty's writeAndFlush / DefaultPromise.addListener can throw unchecked
                // when the event loop is shut down (RejectedExecutionException) or in other
                // listener-registration races. The TransportLayer contract is to throw
                // IOException so the Selector's write step routes the channel through
                // FAILED_SEND; an escaping RuntimeException bypasses that path and
                // surfaces as an unhandled error in the Processor's run loop, breaking
                // graceful shutdown ordering. If writeAndFlush threw before returning a
                // future, handedOff stays false and the finally below releases buf;
                // if addListener threw after writeAndFlush returned, Netty owns the buf
                // and will release it on flush completion (no decrementer installed, but
                // the inc on line 516 didn't run either — counter stays consistent).
                throw new IOException("write submission failed", e);
            }
        } finally {
            if (!handedOff) {
                buf.release();
            }
        }
        return safeChunk;
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        // Check the async failure FIRST, before scanning for buffers with bytes — a
        // failed prior write must surface even when the Send has fully drained its
        // ByteBuffers and the inner write(ByteBuffer) call would otherwise be skipped.
        // ByteBufferSend.writeTo calls into this overload, so this is the actual fault
        // path that converts the volatile failure into the synchronous IOException the
        // Selector's write step needs to route the channel through FAILED_SEND.
        if (closed) throw new IOException("transport layer is closed");
        // See DIAG-2 note on the scalar write() — same atomic getAndSet rationale.
        Throwable failure = asyncWriteFailure.getAndSet(null);
        if (failure != null) {
            throw new IOException("async write failed", failure);
        }
        long total = 0;
        for (int i = 0; i < length; i++) {
            ByteBuffer src = srcs[offset + i];
            if (!src.hasRemaining()) continue;
            // GatheringByteChannel ordering: a partial write of {@code src} (because of
            // {@link #MAX_WRITE_CHUNK_BYTES} or {@code bytesBeforeUnwritable} pressure) must
            // halt the loop here. Continuing to a later buffer would interleave its bytes
            // ahead of the remainder of {@code src}, scrambling on-wire framing for any
            // multi-buffer {@link org.apache.kafka.common.network.ByteBufferSend} —
            // notably Fetch responses whose Send is built as
            // {@code [header, recordSet, trailer]}: a partial write of {@code recordSet}
            // followed by writes from {@code trailer} would emit trailer bytes inside
            // the record set. {@link java.nio.channels.SocketChannel#write(ByteBuffer[])}
            // never reorders, and our impl must match. A zero return (backpressured)
            // similarly aborts the loop so the caller re-enters on the next poll.
            int wrote = write(src);
            total += wrote;
            if (wrote == 0 || src.hasRemaining()) {
                break;
            }
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
        //
        // Also report pending when a prior write failed asynchronously: ByteBufferSend
        // calls hasPendingWrites() inside writeTo() to decide if the Send is completed,
        // and a synchronous-listener failure could otherwise zero the counter before the
        // next Processor write call surfaces the throw. Holding "pending" until the next
        // write() throws keeps {@code Send.completed} false in that interleaving, so
        // {@code KafkaChannel.maybeCompleteSend} does NOT fire on the failed bytes.
        return pendingWriteBytes.get() > 0 || asyncWriteFailure.get() != null;
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

    /** Package-private diagnostic — bytes queued in {@link #inbound} (test/debug only). */
    long inboundBytesSnapshot() {
        return inboundBytes.get();
    }

    /** Package-private diagnostic — bytes still riding inside Netty's outbound queue (test/debug only). */
    long pendingWriteBytesSnapshot() {
        return pendingWriteBytes.get();
    }

    /** Package-private diagnostic — number of {@link ByteBuf} buffers queued for read (test/debug only). */
    int inboundQueueDepth() {
        return inbound.size();
    }

    /** Package-private diagnostic — cumulative bytes ever offered by Netty (test/debug only). */
    long totalInboundBytesSnapshot() {
        return totalInboundBytes.get();
    }

    /** Package-private diagnostic — cumulative {@link #offerInbound} calls (test/debug only). */
    long offerInboundCallsSnapshot() {
        return offerInboundCalls.get();
    }

    /**
     * Test-only entry point that invokes the same code path as the Netty writeAndFlush
     * failure listener — namely {@link #recordAsyncWriteFailure(Throwable)} — without
     * needing a real failed promise. Used by DIAG-1's regression test to drive multiple
     * listener firings without the intervening read-and-clear that EmbeddedChannel
     * forces when its synchronous future-completion races against the next write() call.
     * The production listener path is identical; this method exists solely so the test
     * can stack two unconsumed causes deterministically.
     */
    void recordAsyncWriteFailureForTesting(Throwable cause) {
        recordAsyncWriteFailure(cause);
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        try {
            if (nettyChannel.isOpen()) nettyChannel.close();
        } catch (Exception e) {
            // Caller chain (Utils.closeAllQuietly via KafkaChannel.close) will not see this
            // exception; NIO's PlaintextTransportLayer.close (clients/src/main/java/org/apache/
            // kafka/common/network/PlaintextTransportLayer.java:81-84) propagates IOException
            // and operators see it in the broker log. We keep best-effort semantics for the
            // io_uring path (Netty's close is async; the channel is dead either way), but
            // surface the failure at WARN so operators can correlate hung-disconnect tickets
            // with the underlying Netty error instead of staring at a silent stack frame.
            log.warn("Netty channel close raised an exception during transport-layer close (remote={})",
                socketChannel.getRemoteAddress(), e);
        }
        ByteBuf b;
        while ((b = inbound.poll()) != null) {
            b.release();
        }
        inboundBytes.set(0);
        // DIAG-3: clear any stashed async write failure. Without this, a writeAndFlush
        // promise that completes (failed) AFTER close() — possible when Netty had the
        // write in-flight at the moment of close — would leave asyncWriteFailure set
        // forever. {@link #hasPendingWrites()} reads it and would return true on a
        // closed channel: harmless in v1 because the Selector tears the channel down
        // and never polls it again, but it violates the closed-resource invariant
        // ("after close(), every accessor reports a clean drained state") and makes
        // {@link KafkaChannel#maybeCompleteSend} reasoning subtler than it needs to
        // be. {@link AtomicReference#set} is the right primitive here: we don't care
        // about the previous value (close() destroys all paths to surface it anyway).
        asyncWriteFailure.set(null);
        selectionKey.cancel();
    }
}
