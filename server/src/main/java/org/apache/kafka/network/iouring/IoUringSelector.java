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
import org.apache.kafka.common.network.Authenticator;
import org.apache.kafka.common.network.ChannelState;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.network.KafkaChannelMuteBridge;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

/**
 * Broker-side {@link BrokerSelector} backed by Netty's io_uring transport.
 *
 * <h3>What this class does</h3>
 * Owns the per-Processor channel state for an io_uring listener: tracks accepted
 * {@link KafkaChannel} instances, runs the read/write progress loop on {@link #poll(long)},
 * applies idle-connection expiry, and drives mute / unmute semantics. The actual kernel
 * I/O happens on a Netty event-loop thread; this class is driven from the Processor
 * thread and bridged to the event loop through {@link ConcurrentLinkedQueue}s — matching
 * the PROMPT.md acceptance criteria for cross-thread signalling.
 *
 * <h3>What this class does <em>not</em> do</h3>
 * Binding, accepting, and the Netty pipeline live in a separate class (the {@code
 * IoUringServerListener}, follow-up) so this class stays unit-testable without standing
 * up a real socket. The event loop calls into the four package-private hooks:
 * {@link #onAccept(Channel, InetSocketAddress, InetSocketAddress)},
 * {@link #onRead(Channel, ByteBuf)}, {@link #onDisconnect(Channel)}, and
 * {@link #onWritabilityChanged(Channel)}.
 *
 * <h3>Threading invariants</h3>
 * <ul>
 *   <li>Only the Processor thread mutates {@code channels}, {@code lastActiveNanos},
 *       and the four per-poll output collections.</li>
 *   <li>Only the event-loop thread calls the four {@code onX} hooks; those push into
 *       cross-thread queues and release {@link #wakeup}.</li>
 *   <li>{@link #wakeup()}, {@link #send(NetworkSend)}, {@link #mute(String)},
 *       {@link #unmute(String)} are documented as called from the Processor thread per the
 *       {@code Selectable} contract — they do not need to be re-entrant.</li>
 * </ul>
 *
 * <h3>Semantic alignment with NIO Selector</h3>
 * The order of operations inside {@link #poll(long)} is deliberate and mirrors
 * {@code org.apache.kafka.common.network.Selector}:
 * <ol>
 *   <li>Capture {@code nowNanos} once, before any I/O — so a channel that transferred bytes
 *       this tick can never be reaped by idle expiry on the same tick.</li>
 *   <li>Drain accepts first (so a fresh channel can immediately do read/write work).</li>
 *   <li>For each active channel: at most one {@link KafkaChannel#read()} progressing to one
 *       completed receive, then at most one {@link KafkaChannel#write()} progressing to one
 *       completed send. The "one completed receive per channel per poll" cap is a fairness
 *       invariant the request-handling path depends on.</li>
 *   <li>Drain disconnects last — but a channel disconnected mid-tick with buffered bytes
 *       gets one final read pass before it is removed, matching NIO's
 *       {@code closingChannels} semantics. Without this, short-lived clients silently lose
 *       their last request.</li>
 *   <li>Idle expiry sweeps using the {@code nowNanos} captured in step 1.</li>
 *   <li>If no work happened, wait on {@link #wakeup} for the remainder of the timeout.
 *       Permits are drained on acquisition so a burst of event-loop notifications does
 *       not produce N consecutive zero-work polls.</li>
 * </ol>
 */
public final class IoUringSelector implements BrokerSelector {

    private static final Logger log = LoggerFactory.getLogger(IoUringSelector.class);

    /** Attached on every Netty channel so the event-loop hooks can route to the right transport. */
    static final AttributeKey<IoUringTransportLayer> TRANSPORT_ATTR =
        AttributeKey.valueOf("kafka.iouring.transport");
    /** Channel id, so {@link #onDisconnect(Channel)} can enqueue it without touching the transport. */
    static final AttributeKey<String> CHANNEL_ID_ATTR =
        AttributeKey.valueOf("kafka.iouring.channelId");

    private final ListenerName listenerName;
    private final int maxReceiveSize;
    private final MemoryPool memoryPool;
    private final long connectionsMaxIdleNanos;
    private final Time time;
    /**
     * Broker configs threaded down to {@link IoUringPlaintextAuthenticator}'s
     * {@link org.apache.kafka.common.network.ChannelBuilders#createPrincipalBuilder}. Required
     * so a user-configured {@code principal.builder.class} is honored on the io_uring path —
     * passing an empty map would silently fall back to DefaultKafkaPrincipalBuilder, diverging
     * from the NIO PLAINTEXT path on the same broker.
     */
    private final Map<String, ?> configs;
    /**
     * Processor id baked into every connection id we mint. The Kafka request-handling path
     * decrements connection quotas by parsing the connection id with {@link
     * org.apache.kafka.common.network.ServerConnectionId}, so the id must follow the
     * {@code localHost:localPort-remoteHost:remotePort-processorId-index} format — otherwise
     * {@code Processor.processDisconnected} would silently fail to release the quota.
     */
    private final int processorId;

    // Processor-owned state.
    private final Map<String, KafkaChannel> channels = new LinkedHashMap<>();
    /**
     * Channels that finished serving on this poll but whose final {@code completedReceives}
     * still need a {@code channel(id)} or {@code closingChannel(id)} resolution from the
     * Processor. NIO's {@code KSelector} keeps closing channels alive for one extra poll for
     * exactly this reason — we evict the previous poll's entries at the start of the next
     * poll, after the Processor has had its chance to drain them.
     */
    private final Map<String, KafkaChannel> closingChannels = new HashMap<>();
    /** Crossed by both threads: event loop puts on accept, Processor reads on mute/unmute/close. */
    private final Map<String, Channel> nettyChannels = new ConcurrentHashMap<>();
    private final Map<String, Long> lastActiveNanos = new HashMap<>();
    /**
     * Tracks channels that the operator explicitly muted via {@link #mute(String)} or
     * {@link #muteAll()}, distinct from channels that {@link KafkaChannel#read()} self-muted
     * because {@link MemoryPool#tryAllocate(int)} returned null. Mirrors NIO's
     * {@code Selector.explicitlyMutedChannels} (clients/.../Selector.java:108).
     *
     * <p>The recovery loop at the top of {@link #poll(long)} must NOT unmute channels in this
     * set — they are still muted because the request-handling pipeline (RESPONSE_QUEUED →
     * RESPONSE_SENT mute events) asked us to hold them, not because of memory pressure.
     * Unmuting them would let pipelined requests slip past the throttle.
     *
     * <p>Identity-based set (not id-based) to match NIO's storage; the KafkaChannel instance
     * is stable for the channel's lifetime.
     */
    private final Set<KafkaChannel> explicitlyMutedChannels = new HashSet<>();
    /**
     * Set whenever a channel self-mutes because {@code memoryPool.tryAllocate} returned null
     * during {@link KafkaChannel#read()}. Cleared at the top of the next {@link #poll(long)}
     * when {@code memoryPool.isOutOfMemory()} reports false again — at that point the
     * recovery loop walks every non-explicitly-muted channel and calls {@code maybeUnmute()}.
     *
     * <p>Without this, channels that self-mute under {@code queued.max.bytes} pressure stay
     * MUTED forever (until idle expiry, ~10 min by default) — NIO {@code Selector.poll()}
     * lines 457-466 own this exact recovery path, and the io_uring selector must preserve it.
     */
    private boolean outOfMemory;

    // Cross-thread queues (event loop pushes, Processor pulls).
    private final Queue<KafkaChannel> pendingAccepts = new ConcurrentLinkedQueue<>();
    /**
     * Cheap O(1) view of {@link #pendingAccepts}'s depth — ConcurrentLinkedQueue.size() walks
     * the chain, so we maintain a counter alongside it. The size is read on every accept to
     * enforce {@link #maxPendingAccepts}; with a high accept rate the O(n) walk would itself
     * become the bottleneck (and would race the producer/consumer pointers).
     */
    private final AtomicInteger pendingAcceptCount = new AtomicInteger();
    /**
     * Bound for {@link #pendingAccepts}. Mirrors NIO's per-Processor
     * {@code ArrayBlockingQueue(connectionQueueSize)} in {@code SocketServer.Acceptor}:
     * NIO blocks the Acceptor thread on {@code put()} when full, providing kernel-level
     * backpressure via the SYN/ACCEPT backlog. We can't block — the Netty event loop is
     * shared across all child channels for this listener and freezing it would stall every
     * accepted connection — so when the cap is hit, {@link #onAccept(Channel, InetSocketAddress, InetSocketAddress)}
     * closes the freshly accepted netty channel instead. The client sees a RST and the OS
     * backlog drains.
     *
     * <p>The value matches the default for {@code socket.server.listen.backlog.size}'s
     * downstream queue (NIO uses 20 hardcoded per processor). Under sustained pressure the
     * total cap is N * 20 across N selectors, same as NIO.
     */
    private static final int DEFAULT_MAX_PENDING_ACCEPTS = 20;
    private final int maxPendingAccepts;
    private final Queue<String> pendingDisconnects = new ConcurrentLinkedQueue<>();

    // Wakeup signalling. Permits are drained on acquisition; see acquireWithTimeout below.
    private final Semaphore wakeup = new Semaphore(0);

    // Per-poll outputs.
    private final List<NetworkReceive> completedReceives = new ArrayList<>();
    private final List<NetworkSend> completedSends = new ArrayList<>();
    private final Map<String, ChannelState> disconnected = new HashMap<>();
    private final List<String> connected = new ArrayList<>();
    /**
     * Channels that just surfaced in {@link #connected} during this poll. The read step
     * (poll step 2) skips them so a newly-accepted channel cannot produce a
     * {@code completedReceive} in the same poll where it first appears to the Processor —
     * otherwise SocketServer.applyConnectionQuotasForNewlyAcceptedChannels (which runs
     * AFTER poll and may refuse the channel via {@code selector.close}) would leave a
     * receive in {@link #completedReceives} pointing at a channel already removed, and
     * Processor.processCompletedReceives would (a) emit the request to the request queue
     * before the refusal completes, and (b) trip an IllegalStateException whose cleanup
     * path calls {@code connectionQuotas.dec} on a connection that was never {@code inc}'d.
     */
    private final Set<String> justAccepted = new HashSet<>();
    /**
     * Channel ids that produced a {@link NetworkReceive} in this poll. The
     * "one completed receive per channel per poll" invariant — documented in the class
     * javadoc and enforced by NIO's {@code Selector.addToCompletedReceives} via an
     * {@code IllegalStateException} on duplicate inserts — needs to hold across the
     * step-2 read pass <em>and</em> the step-3 disconnect drain. If both fire for the
     * same channel id (peer FIN arrives mid-poll, with another pipelined frame still
     * buffered), the request channel would otherwise see two requests for one
     * connection and the KafkaChannel mute-after-receive contract breaks.
     */
    private final Set<String> receivesThisPoll = new HashSet<>();
    /**
     * Channels for which {@link #send(NetworkSend)} could not deliver because the peer
     * had already disconnected (id is in {@link #closingChannels}) or {@link
     * KafkaChannel#setSend} threw. Drained at the start of the next poll into
     * {@link #disconnected} with {@link ChannelState#FAILED_SEND}, mirroring NIO's
     * {@code Selector.failedSends} pipeline so the Processor sees a normal disconnect
     * notification rather than an {@code IllegalStateException} that would otherwise
     * propagate up through {@code Processor.processChannelException} and double-dec the
     * connection quota.
     */
    private final List<String> failedSends = new ArrayList<>();

    // 32-bit wrapping counter, NOT AtomicLong. ServerConnectionId.fromString parses
    // the index segment via Integer.parseInt, so a long that exceeds Integer.MAX_VALUE
    // (~2.1B accepts on a long-running broker — reachable under connection-churn) would
    // make fromString return Optional.empty, defeating SocketServer.processDisconnected
    // and leaking connection-quota slots until restart. NIO uses an int that wraps at
    // Int.MaxValue (SocketServer.scala line 1431-1432); we mirror that wrap exactly.
    private final AtomicInteger idGen = new AtomicInteger();
    private volatile boolean closed;

    /**
     * Test-only constructor: uses processor id 0 and an empty configs map (default principal builder).
     * Production code must use the 7-arg constructor so a user-configured {@code principal.builder.class}
     * is honored — empty configs silently falls back to {@code DefaultKafkaPrincipalBuilder} and diverges
     * from the NIO PLAINTEXT path on the same broker. Kept {@code public} because the throughput benchmark
     * lives in a sibling sub-package ({@code .bench}) and cannot reach a package-private constructor.
     */
    public IoUringSelector(ListenerName listenerName,
                           int maxReceiveSize,
                           MemoryPool memoryPool,
                           long connectionsMaxIdleNanos,
                           Time time) {
        this(listenerName, maxReceiveSize, memoryPool, connectionsMaxIdleNanos, time, 0, java.util.Collections.emptyMap());
    }

    /**
     * Test-only constructor: empty configs map (default principal builder). Same {@code public}
     * visibility caveat as the no-processor-id variant above.
     */
    public IoUringSelector(ListenerName listenerName,
                           int maxReceiveSize,
                           MemoryPool memoryPool,
                           long connectionsMaxIdleNanos,
                           Time time,
                           int processorId) {
        this(listenerName, maxReceiveSize, memoryPool, connectionsMaxIdleNanos, time, processorId, java.util.Collections.emptyMap());
    }

    public IoUringSelector(ListenerName listenerName,
                           int maxReceiveSize,
                           MemoryPool memoryPool,
                           long connectionsMaxIdleNanos,
                           Time time,
                           int processorId,
                           Map<String, ?> configs) {
        this.listenerName = Objects.requireNonNull(listenerName, "listenerName");
        this.maxReceiveSize = maxReceiveSize;
        this.memoryPool = Objects.requireNonNull(memoryPool, "memoryPool");
        this.connectionsMaxIdleNanos = connectionsMaxIdleNanos;
        this.time = Objects.requireNonNull(time, "time");
        this.processorId = processorId;
        this.configs = Objects.requireNonNull(configs, "configs");
        this.maxPendingAccepts = DEFAULT_MAX_PENDING_ACCEPTS;
    }

    // -------------------------------------------------------------------------
    // Event-loop hooks (package-private; called from the Netty event-loop thread)
    // -------------------------------------------------------------------------

    /**
     * Register a freshly accepted Netty channel. Builds the full {@link KafkaChannel}
     * stack and queues it for the next {@link #poll(long)}.
     */
    void onAccept(Channel nettyChannel, InetSocketAddress remote, InetSocketAddress local) {
        if (closed) {
            nettyChannel.close();
            return;
        }
        // Bound the accept queue depth: if the Processor is slow to drain pendingAccepts,
        // close the freshly accepted netty channel immediately rather than building the
        // full KafkaChannel stack (TransportLayer, Authenticator, MetadataRegistry, plus
        // a slot in nettyChannels) and stranding it in an unbounded queue. This mirrors
        // NIO's per-Processor bounded queue (ArrayBlockingQueue(connectionQueueSize) in
        // SocketServer.Acceptor) — NIO blocks on put() when full so the OS backlog
        // absorbs the pressure; we close because blocking the Netty event loop would
        // freeze every child channel served by this listener.
        //
        // Closing here sends a TCP RST (or FIN) to the client. Without the cap, a stuck
        // Processor would let the queue grow until the JVM OOMs — every entry pins a
        // Netty Channel with its own direct-memory recvByteBufAllocator state.
        if (pendingAcceptCount.get() >= maxPendingAccepts) {
            log.info("io_uring: dropping accept from {} — pending-accept queue at cap ({}); " +
                     "Processor likely overloaded", remote, maxPendingAccepts);
            nettyChannel.close();
            return;
        }
        // Format must match ServerConnectionId so Processor.processDisconnected can parse it
        // and decrement ConnectionQuotas correctly. Using the synthetic "iouring-N" form
        // would silently break quota release.
        int connectionIndex = idGen.getAndUpdate(i -> i == Integer.MAX_VALUE ? 0 : i + 1);
        String id = local.getAddress().getHostAddress() + ":" + local.getPort() + "-"
                  + remote.getAddress().getHostAddress() + ":" + remote.getPort() + "-"
                  + processorId + "-" + connectionIndex;
        // Pass wakeup::release as the write-completion callback. Netty fires writeAndFlush's
        // listener on the event-loop thread, NOT the Processor thread — so the listener must
        // re-arm the Processor's poll() semaphore, otherwise a small Send (no watermark
        // crossing → no onWritabilityChanged) leaves poll() asleep until timeoutMs while
        // completedSends/RESPONSE_SENT are silently pending.
        IoUringTransportLayer transport = new IoUringTransportLayer(nettyChannel, remote, local, wakeup::release);
        // C-17-Lifecycle-C2: if any subsequent constructor throws (the most likely culprit is
        // PrincipalBuilder.build() inside IoUringPlaintextAuthenticator, or KafkaChannel's
        // own constructor), we must close every partially-built object — otherwise the
        // authenticator's per-channel principal-builder resources and the transport's
        // direct-memory recvByteBufAllocator state are orphaned with no close() path.
        // Track each side-effect separately so the catch knows exactly what to undo.
        Authenticator authenticator = null;
        KafkaChannel channel;
        try {
            authenticator = new IoUringPlaintextAuthenticator(transport, listenerName, configs);
            IoUringChannelMetadataRegistry metadata = new IoUringChannelMetadataRegistry();
            // Mirror NIO Selector.register: seed ClientInformation.EMPTY so the first RequestContext
            // (built before any ApiVersionsRequest is parsed) never captures null.
            metadata.registerClientInformation(ClientInformation.EMPTY);
            final Authenticator authForLambda = authenticator;
            channel = new KafkaChannel(id, transport, () -> authForLambda, maxReceiveSize, memoryPool, metadata);
        } catch (Throwable t) {
            // C-18-F10: catch Throwable, not just RuntimeException. PrincipalBuilder.build()
            // and IoUringPlaintextAuthenticator's reflective principal-builder load can throw
            // Error subclasses (NoClassDefFoundError, ExceptionInInitializerError, LinkageError)
            // when a misconfigured principal.builder.class fails to load. Catching only
            // RuntimeException lets those Errors escape the event-loop thread, killing the
            // Netty IO handler and silently freezing the listener (no further accepts/reads
            // fire). Mirrors B-17-3 in IoUringSupport.computeProbe and NIO Selector.register's
            // pattern of treating channel-build failure as a per-channel close, not a
            // listener-fatal event. VM-fatal Errors (OOMError, StackOverflowError) are still
            // re-thrown below so JVM termination semantics are preserved.
            log.warn("io_uring: failed to build KafkaChannel for {} — releasing transport+authenticator", id, t);
            Utils.closeQuietly(authenticator, "authenticator on accept-build failure");
            Utils.closeQuietly(transport, "transport on accept-build failure");
            nettyChannel.close();
            if (t instanceof VirtualMachineError) {
                // OOMError, StackOverflowError, InternalError — the JVM is in an unrecoverable
                // state, propagating preserves crash-fast semantics.
                throw (VirtualMachineError) t;
            }
            return;
        }

        nettyChannel.attr(TRANSPORT_ATTR).set(transport);
        nettyChannel.attr(CHANNEL_ID_ATTR).set(id);

        // Stash the netty channel so mute/unmute can flip autoRead without chasing
        // references through the KafkaChannel's selectionKey.
        nettyChannels.put(id, nettyChannel);
        pendingAccepts.offer(channel);
        pendingAcceptCount.incrementAndGet();

        // B-17-1: re-check closed AFTER publishing. close() drains channels / closingChannels
        // / nettyChannels / pendingAccepts based on the snapshot taken when closed was set
        // true. If close() ran between our initial closed check at the top and the publish
        // we just performed, our entries are orphaned — the KafkaChannel sits in pendingAccepts
        // that nothing will drain, and the netty channel sits in nettyChannels with the same
        // fate, leaking direct memory + a file descriptor per racing accept. Re-checking and
        // self-cleaning closes that window: if close() saw our entries it cleaned them up
        // (our undo here is a no-op); if close() ran fully before our publish, we now own
        // the cleanup. KafkaChannel.close() is idempotent via Utils.closeAll so the worst
        // case is double-close, which is safe.
        if (closed) {
            log.debug("io_uring: onAccept raced selector close — cleaning up id {}", id);
            nettyChannels.remove(id);
            if (pendingAccepts.remove(channel)) {
                pendingAcceptCount.decrementAndGet();
            }
            Utils.closeQuietly(channel, "channel on accept-close race");
            return;
        }
        wakeup.release();
    }

    /** Forward bytes from the event loop into the transport's inbound queue. */
    void onRead(Channel nettyChannel, ByteBuf buf) {
        IoUringTransportLayer transport = nettyChannel.attr(TRANSPORT_ATTR).get();
        if (transport == null) {
            buf.release();
            return;
        }
        transport.offerInbound(buf);
        wakeup.release();
    }

    /** Mark EOF on the transport and enqueue the channel id for graceful removal. */
    void onDisconnect(Channel nettyChannel) {
        IoUringTransportLayer transport = nettyChannel.attr(TRANSPORT_ATTR).get();
        if (transport != null) transport.markEof();
        String id = nettyChannel.attr(CHANNEL_ID_ATTR).get();
        if (id != null) pendingDisconnects.offer(id);
        wakeup.release();
    }

    /**
     * Netty's outbound buffer flipped from unwritable to writable (or vice-versa). Wake
     * any blocking poll so {@link KafkaChannel#write()} retries.
     */
    void onWritabilityChanged(Channel nettyChannel) {
        wakeup.release();
    }

    // -------------------------------------------------------------------------
    // Selectable / BrokerSelector API (called from the Processor thread)
    // -------------------------------------------------------------------------

    @Override
    public void poll(long timeoutMs) throws IOException {
        if (closed) throw new IOException("selector is closed");

        // Reset per-poll outputs BEFORE evicting closingChannels: the eviction populates
        // disconnected with LOCAL_CLOSE for each evicted id, mirroring NIO's
        // {@code Selector.clear} which calls {@code addToDisconnected} during eviction.
        completedReceives.clear();
        completedSends.clear();
        disconnected.clear();
        connected.clear();
        justAccepted.clear();
        receivesThisPoll.clear();

        // MemoryPool recovery: re-admit channels that self-muted due to memory pressure
        // once the pool reports available again. Helper extracted to keep poll() under
        // checkstyle's MethodLength limit.
        recoverFromMemoryPressure();

        // Drain closingChannels left over from the previous poll. Helper extracted for
        // both readability and to keep poll() under checkstyle's MethodLength limit.
        drainClosingChannels();

        // Any failedSends not absorbed by the closingChannels eviction above are NEW
        // failures (e.g. setSend threw on a healthy channel) — surface them as the
        // disconnect notification the Processor expects.
        for (String id : failedSends) {
            disconnected.put(id, ChannelState.FAILED_SEND);
        }
        failedSends.clear();

        // Capture once, BEFORE I/O, so a channel that progresses this poll never expires this poll.
        long nowNanos = time.nanoseconds();

        boolean madeProgress = false;

        // 1. Drain accepts.
        KafkaChannel acceptedChannel;
        while ((acceptedChannel = pendingAccepts.poll()) != null) {
            pendingAcceptCount.decrementAndGet();
            channels.put(acceptedChannel.id(), acceptedChannel);
            lastActiveNanos.put(acceptedChannel.id(), nowNanos);
            // PLAINTEXT auth completes synchronously; one prepare() call drives the channel to READY.
            try {
                acceptedChannel.prepare();
            } catch (Exception e) {
                surfacePrepareFailureAsDisconnect(acceptedChannel, e);
                madeProgress = true;
                continue;
            }
            connected.add(acceptedChannel.id());
            justAccepted.add(acceptedChannel.id());
            madeProgress = true;
        }

        // 2. For each active channel, do at most one read and one write step.
        //    Channels in justAccepted (added this same poll's step 1) are deferred until
        //    the next poll — SocketServer must run applyConnectionQuotasForNewlyAcceptedChannels
        //    against the freshly-connected channel before we surface any receive from it.
        for (Iterator<Map.Entry<String, KafkaChannel>> it = channels.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, KafkaChannel> entry = it.next();
            KafkaChannel channel = entry.getValue();
            if (!channel.ready()) continue;
            if (justAccepted.contains(channel.id())) continue;

            // Read step. KafkaChannel.isMuted() is the source of truth: mute(id) above calls
            // through to KafkaChannel.mute() via KafkaChannelMuteBridge, which sets muteState
            // and removes OP_READ; the transport layer translates the OP_READ removal into
            // Netty autoRead=false. So a muted channel here has both kernel-level backpressure
            // (no more inbound from Netty) and a state-machine gate (so we never produce a
            // completedReceive while the Processor expects MUTED or MUTED_AND_RESPONSE_PENDING).
            if (!channel.isMuted()) {
                try {
                    long read = channel.read();
                    if (read > 0) {
                        lastActiveNanos.put(channel.id(), nowNanos);
                        madeProgress = true;
                    }
                    NetworkReceive completed = channel.maybeCompleteReceive();
                    if (completed != null) {
                        completedReceives.add(completed);
                        receivesThisPoll.add(channel.id());
                        madeProgress = true;
                    }
                    // Self-mute detection: KafkaChannel.read() flips muteState to MUTED when
                    // memoryPool.tryAllocate returns null. If now muted and NOT operator-muted,
                    // the read self-muted us. Flag outOfMemory so the next poll's
                    // recoverFromMemoryPressure() walks channels. Mirrors NIO Selector:691-692.
                    if (channel.isMuted() && !explicitlyMutedChannels.contains(channel)) {
                        outOfMemory = true;
                    }
                } catch (Exception e) {
                    // Catch Exception, not just IOException: KafkaChannel.read() declares
                    // throws IOException, but NetworkReceive throws InvalidReceiveException
                    // (a KafkaException -> RuntimeException) when the wire reports a negative
                    // size or a size > socket.request.max.bytes. Catching only IOException
                    // lets that escape poll() so Processor.poll() never sees the failure,
                    // the channel is never closed, the connection quota is never decremented,
                    // and the unread Netty ByteBufs accumulate until idle expiry or direct-
                    // memory failure. Matches NIO Selector.pollSelectionKeys's catch (Exception).
                    log.debug("Read failed on channel {}", channel.id(), e);
                    enqueueClose(channel.id(), ChannelState.LOCAL_CLOSE);
                    it.remove();
                    lastActiveNanos.remove(channel.id());
                    continue;
                }
            }

            // Write step. C-18-F1: pending send counts as activity even if write()==0
            // (Netty high-water). See idleExpiryDoesNotReapMutedChannelWithBackpressuredSend.
            if (channel.hasSend()) {
                lastActiveNanos.put(channel.id(), nowNanos);
                try {
                    long written = channel.write();
                    if (written > 0) {
                        madeProgress = true;
                    }
                    NetworkSend send = channel.maybeCompleteSend();
                    if (send != null) {
                        completedSends.add(send);
                        madeProgress = true;
                    }
                } catch (Exception e) {
                    // Same rationale as the read path: catch Exception, not just IOException,
                    // so a RuntimeException from inside the send chain (e.g. an outbound
                    // ByteBufferSend tripping over a malformed message) routes through
                    // FAILED_SEND instead of escaping poll() and stranding the channel.
                    log.debug("Write failed on channel {}", channel.id(), e);
                    enqueueClose(channel.id(), ChannelState.FAILED_SEND);
                    it.remove();
                    lastActiveNanos.remove(channel.id());
                }
            }
        }

        // 3. Drain disconnects — but give buffered bytes one last delivery pass first,
        //    mirroring NIO's closingChannels semantics. The channel goes into
        //    closingChannels so this poll's completedReceives still resolve via
        //    closingChannel(id) on the Processor side; we close the channel for real at the
        //    start of the next poll.
        if (drainPendingDisconnects()) {
            madeProgress = true;
        }

        // 4. Idle expiry — at most one channel per poll. Helper extracted to keep poll()
        //    under checkstyle's MethodLength limit and to mirror NIO semantics.
        if (maybeExpireOldestIdleChannel(nowNanos)) {
            madeProgress = true;
        }

        // 5. If nothing happened, wait. Drain permits on acquisition so a burst of
        //    event-loop notifications doesn't produce N consecutive zero-work polls.
        if (!madeProgress && timeoutMs > 0) {
            acquireWithTimeout(timeoutMs);
        } else {
            // We did work — but a wakeup may have queued more events. Don't sleep, but
            // also don't leave stale permits around for the next poll's wait condition.
            wakeup.drainPermits();
        }
    }

    /**
     * If the previous poll observed a channel self-mute because {@code memoryPool.tryAllocate}
     * returned null (KafkaChannel.read() flips muteState to MUTED when the pool is dry), and
     * the pool now reports available again, walk every channel and call {@code maybeUnmute()}
     * — but ONLY for channels NOT in {@link #explicitlyMutedChannels} (those stay muted
     * because the request pipeline is still throttling them, not because of memory pressure).
     *
     * <p>Mirrors NIO {@code Selector.poll()} at clients/.../Selector.java:457-466. Without
     * this recovery loop, channels that self-mute under {@code queued.max.bytes} pressure
     * stay MUTED until idle expiry (~10 min default) and the broker effectively wedges its
     * PLAINTEXT listener under sustained memory pressure.
     */
    private void recoverFromMemoryPressure() {
        if (!memoryPool.isOutOfMemory() && outOfMemory) {
            log.trace("io_uring selector recovering from memory pressure — unmuting self-muted channels");
            for (KafkaChannel channel : channels.values()) {
                if (channel.isInMutableState() && !explicitlyMutedChannels.contains(channel)) {
                    KafkaChannelMuteBridge.maybeUnmute(channel);
                }
            }
            outOfMemory = false;
        }
    }

    /**
     * Evict at most ONE oldest-idle channel per poll. Mirrors NIO
     * {@code Selector.maybeCloseOldestConnection} (clients/.../Selector.java:795-810):
     * NIO evicts a single LRU-head entry per poll so a backlog of idle channels does
     * not turn into a synchronous eviction storm on the Processor thread (each close
     * walks transport + authenticator + buffer cleanup; sweeping all of them in one
     * poll has stalled selector loops in production).
     *
     * <p>Round-20C F12: the previous implementation walked {@link #lastActiveNanos}
     * and reaped every expired entry in one pass — a 10k-idle-channel host sweeping
     * all 10k on the same poll spiked p99 poll latency by orders of magnitude. NIO
     * does not have this problem because it evicts the head of an insertion-ordered
     * map. We don't keep an LRU index here, so we pay an O(n) min-scan once per poll
     * to find the oldest — same cost as the existing {@code lowestPriorityChannel}
     * scan over the same map, and dramatically cheaper than evicting N channels in
     * one pass.
     *
     * @return true iff a channel was evicted this poll
     */
    private boolean maybeExpireOldestIdleChannel(long nowNanos) {
        if (connectionsMaxIdleNanos <= 0 || lastActiveNanos.isEmpty()) {
            return false;
        }
        String oldestId = null;
        long oldestNanos = Long.MAX_VALUE;
        for (Map.Entry<String, Long> entry : lastActiveNanos.entrySet()) {
            if (entry.getValue() < oldestNanos) {
                oldestNanos = entry.getValue();
                oldestId = entry.getKey();
            }
        }
        if (oldestId == null || nowNanos - oldestNanos <= connectionsMaxIdleNanos) {
            return false;
        }
        KafkaChannel channel = channels.remove(oldestId);
        nettyChannels.remove(oldestId);
        lastActiveNanos.remove(oldestId);
        if (channel != null) {
            explicitlyMutedChannels.remove(channel);
            disconnected.put(oldestId, ChannelState.EXPIRED);
            Utils.closeQuietly(channel, "expired channel");
        }
        return true;
    }

    /**
     * Mirror NIO {@code close(channel, CloseMode.GRACEFUL)} at
     * {@code clients/.../Selector.java#929-955} for the prepare-throws path: NIO routes a
     * prepare() failure through close(GRACEFUL) → doClose → {@code disconnected.put(id, channel.state())}
     * and {@code connected.remove(id)} (line 934). The Processor then runs
     * {@code processDisconnected} which decrements the connection quota.
     *
     * <p>Pre-fix, the io_uring path caught the exception, logged at DEBUG, and fell through
     * to add the channel to {@code connected} + {@code channels} anyway. Because
     * {@code channel.ready()} is false, step 2 (read) and step 3 (write) skip the channel
     * every subsequent poll; it lingers in {@code channels} until {@code connectionsMaxIdleNanos}
     * elapses (default ~10 minutes), at which point it surfaces as EXPIRED rather than the
     * authenticator's reported state — the per-listener connection quota is leaked for that
     * entire window and the metric attribution is wrong. For today's PLAINTEXT authenticator
     * {@code prepare()} cannot actually throw, but any future SASL/SSL path on this code
     * route would inherit the bug silently.
     */
    private void surfacePrepareFailureAsDisconnect(KafkaChannel acceptedChannel, Exception cause) {
        String id = acceptedChannel.id();
        // Mirror NIO at clients/.../Selector.java:973: disconnected.put(channel.id(), channel.state()).
        // The authenticator is responsible for transitioning state to AUTHENTICATION_FAILED
        // before throwing (SslAuthenticator / SaslServerAuthenticator do this). PLAINTEXT does
        // not throw in practice, but emitting whatever state the channel reports is the contract
        // the Processor's processDisconnected was written against — don't second-guess it.
        ChannelState reportedState = acceptedChannel.state();
        log.debug("prepare() failed for accepted channel {}, surfacing as {}", id, reportedState, cause);
        channels.remove(id);
        nettyChannels.remove(id);
        lastActiveNanos.remove(id);
        explicitlyMutedChannels.remove(acceptedChannel);
        disconnected.put(id, reportedState);
        Utils.closeQuietly(acceptedChannel, "channel after prepare() failure");
    }

    /**
     * Drain {@link #closingChannels} left over from the previous poll. NIO's
     * {@code Selector.clear} at {@code clients/.../Selector.java#842-863} is the model:
     * a channel stays in {@code closingChannels} as long as
     * <ul>
     *   <li>{@link #failedSends} did not fire for it this poll (the FAILED_SEND
     *       notification is the terminal signal — no further reads should be attempted),
     *       AND</li>
     *   <li>there is more buffered work we can still deliver: either the channel is
     *       muted (the Processor will read it after explicit unmute) OR one more read
     *       can produce a completedReceive.</li>
     * </ul>
     * <p>Evict only when nothing more is forthcoming, and at *that* point emit the
     * disconnect notification. This is what defers {@code disconnected} from the FIN
     * poll (where the channel still has buffered work) to the eviction poll, matching
     * NIO and closing the same-poll double-dec window
     * ({@code processDisconnected} + {@code closeExcessConnections}-&gt;{@code close}).
     * For pipelined requests followed by FIN this drain is what surfaces R2, R3, …
     * across successive polls — without it, only R1 reaches the request queue.
     */
    private void drainClosingChannels() {
        if (closingChannels.isEmpty()) return;
        Iterator<Map.Entry<String, KafkaChannel>> it = closingChannels.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, KafkaChannel> entry = it.next();
            String id = entry.getKey();
            KafkaChannel channel = entry.getValue();
            boolean sendFailed = failedSends.remove(id);
            boolean keepClosing = false;
            if (!sendFailed && channel.ready()) {
                if (explicitlyMutedChannels.contains(channel)) {
                    // Operator-muted closing channel: leave for next poll. The Processor must
                    // unmute it explicitly before the final buffered receives surface, exactly
                    // as NIO does at {@code Selector.java#702}. We intentionally do NOT short-
                    // circuit on the broader {@code channel.isMuted()}: a channel can also be
                    // self-muted (memory-pool pressure inside {@link KafkaChannel#read}). Self-
                    // muted closing channels must still attempt to drain, otherwise a peer FIN
                    // arriving while {@code queued.max.bytes} is saturated would strand the
                    // channel here forever — {@link #recoverFromMemoryPressure} only walks
                    // {@link #channels}, not {@link #closingChannels}, so the channel never
                    // gets unmuted, never has its connection quota decremented, and Netty's
                    // inbound {@code ByteBuf}s queued on the transport layer leak indefinitely.
                    keepClosing = true;
                } else {
                    try {
                        channel.read();
                        NetworkReceive completed = channel.maybeCompleteReceive();
                        if (completed != null) {
                            completedReceives.add(completed);
                            receivesThisPoll.add(id);
                            keepClosing = true;
                        }
                    } catch (Exception e) {
                        // Catch Exception, not just IOException: an InvalidReceiveException
                        // (RuntimeException) here means the final-read tripped on a malformed
                        // size prefix. We just want to evict the channel and let the next poll's
                        // closingChannel(id) path be a no-op — escaping would leak the channel.
                        log.trace("Read from closing channel {} failed, evicting", id, e);
                    }
                }
            }
            if (!keepClosing) {
                explicitlyMutedChannels.remove(channel);
                // Surface the channel's actual state, not a hardcoded LOCAL_CLOSE. NIO's
                // {@code Selector.java:973} does the same: a peer-FIN'd channel stays READY,
                // a write failure was already set to FAILED_SEND by the failedSends path,
                // idle expiry uses the idle-sweep path (already EXPIRED). Hardcoding
                // LOCAL_CLOSE here misclassified every remote disconnect — every metric,
                // log, and downstream gauge that distinguishes peer-initiated vs broker-
                // initiated disconnects was reading the wrong cause for io_uring listeners.
                disconnected.put(id, channel.state());
                Utils.closeQuietly(channel, "closing channel evicted");
                it.remove();
            }
        }
    }

    /**
     * Step 3 of {@link #poll(long)}: drain {@link #pendingDisconnects}, surface each id
     * in {@link #disconnected}, and stash the channel in {@link #closingChannels} so the
     * Processor can still resolve any final completedReceives via
     * {@code closingChannel(id)} before the next poll evicts it.
     *
     * <p>Two invariants are enforced here:
     * <ul>
     *   <li><b>same-poll accept+disconnect:</b> a channel in {@link #justAccepted} that
     *       FINs before the Processor has run {@code applyConnectionQuotasForNewlyAcceptedChannels}
     *       is dropped silently (no {@code disconnected} entry, removed from
     *       {@code connected}). Surfacing it would let {@code processDisconnected} call
     *       {@code connectionQuotas.dec} on a counter that was never {@code inc}'d.
     *   <li><b>one completedReceive per channel per poll:</b> the final-read drain is
     *       skipped if step 2 already produced a receive for this id. Remaining buffered
     *       bytes stay readable through {@code closingChannel(id)} for one more poll —
     *       same as NIO's {@code Selector.clear()}.
     *   <li><b>muted channels do not produce a final receive:</b> a channel that was
     *       muted (operator or MemoryPool) at FIN time must not have its inbound queue
     *       drained here. The Processor explicitly relies on the invariant that the
     *       request flow stays paused until {@code unmute()} is called, and muting
     *       only blocks new kernel pushes, not bytes already in the transport queue. NIO
     *       enforces this via {@code maybeReadFromClosingChannel} skipping muted channels
     *       at {@code Selector.java#698}. We defer the buffered bytes to the next poll's
     *       {@code closingChannel(id)} resolution path, where the channel will be
     *       drained only after the operator unmutes — matching NIO precisely.
     * </ul>
     *
     * @return {@code true} if any channel transitioned to disconnected (or any silent
     *         drop happened), so the caller can mark progress and skip the wait at the
     *         end of {@link #poll(long)}.
     */
    private boolean drainPendingDisconnects() {
        boolean madeProgress = false;
        String disconnectId;
        while ((disconnectId = pendingDisconnects.poll()) != null) {
            KafkaChannel channel = channels.remove(disconnectId);
            nettyChannels.remove(disconnectId);
            lastActiveNanos.remove(disconnectId);
            if (channel == null) continue;
            if (justAccepted.contains(disconnectId)) {
                connected.remove(disconnectId);
                Utils.closeQuietly(channel, "same-poll accept+disconnect, quota never inc'd");
                madeProgress = true;
                continue;
            }
            if (!receivesThisPoll.contains(disconnectId) && !channel.isMuted()) {
                try {
                    while (channel.ready()) {
                        long read = channel.read();
                        NetworkReceive completed = channel.maybeCompleteReceive();
                        if (completed != null) {
                            completedReceives.add(completed);
                            receivesThisPoll.add(disconnectId);
                            madeProgress = true;
                            break;
                        }
                        if (read <= 0) break;
                    }
                } catch (Exception e) {
                    // Catch Exception, not just IOException: InvalidReceiveException
                    // (RuntimeException) during the FIN-drain must not escape — the
                    // channel still needs to land in closingChannels for the eviction
                    // notification path; otherwise the disconnect notification is lost.
                    log.debug("Final read on disconnecting channel {} failed", disconnectId, e);
                }
            }
            closingChannels.put(disconnectId, channel);
            madeProgress = true;
        }
        return madeProgress;
    }

    private void acquireWithTimeout(long timeoutMs) {
        try {
            if (wakeup.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS)) {
                wakeup.drainPermits();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private void enqueueClose(String id, ChannelState state) {
        KafkaChannel channel = channels.get(id);
        if (channel != null) {
            nettyChannels.remove(id);
            explicitlyMutedChannels.remove(channel);
            disconnected.put(id, state);
            Utils.closeQuietly(channel, "channel after I/O error");
        }
    }

    @Override
    public void wakeup() {
        wakeup.release();
    }

    @Override
    public void send(NetworkSend send) {
        String destinationId = send.destinationId();
        KafkaChannel channel = channels.get(destinationId);
        if (channel == null) {
            if (closingChannels.containsKey(destinationId)) {
                // Peer FIN'd between the poll that produced this response and now. NIO
                // (clients/.../Selector.java:391-413) routes this to failedSends rather
                // than throwing: the original disconnect notification already fired in
                // the previous poll, so on the next poll the closingChannels eviction
                // will drop this id from failedSends and the duplicate is suppressed.
                // Throwing here would propagate up through Processor.processChannelException
                // → Processor.close → connectionQuotas.dec on a connection that was
                // already dec'd by processDisconnected, walking the per-IP counter
                // negative on every disconnect-races-response.
                failedSends.add(destinationId);
                return;
            }
            throw new IllegalStateException("Attempt to send on unknown channel " + destinationId);
        }
        try {
            channel.setSend(send);
        } catch (Exception e) {
            // setSend throws if a send is already in progress — a Processor invariant
            // violation rather than a network failure, but mirror NIO's behavior:
            // surface as FAILED_SEND via the standard disconnected pipeline, close the
            // channel, and re-throw so the bug is not silently swallowed. We are on the
            // Processor thread between polls, so dropping the channel here is safe —
            // there is no concurrent step-2 iterator to invalidate. We do NOT add to
            // `disconnected` directly: the failedSends drain at the top of the next
            // poll is the single source of truth for this notification, and the
            // channel is no longer in channels/closingChannels so Processor's
            // openOrClosingChannel(id) returns None and processChannelException's
            // close(id) branch is skipped — no double dec.
            log.error("Unexpected exception during setSend, closing connection {} and rethrowing", destinationId, e);
            nettyChannels.remove(destinationId);
            lastActiveNanos.remove(destinationId);
            channels.remove(destinationId);
            failedSends.add(destinationId);
            Utils.closeQuietly(channel, "channel after setSend exception");
            throw e;
        }
    }

    @Override
    public void mute(String id) {
        // Route through the same-package bridge so KafkaChannel's state machine actually
        // transitions to MUTED. SocketServer.scala calls selector.mute(id) immediately
        // followed by channel.handleChannelMuteEvent(REQUEST_RECEIVED), which transitions
        // MUTED → MUTED_AND_RESPONSE_PENDING — if we don't put the channel into MUTED
        // here, that handleChannelMuteEvent call throws IllegalStateException and the
        // first valid request on every connection crashes. KafkaChannel.mute() also
        // calls transport.removeInterestOps(OP_READ), which our IoUringTransportLayer
        // override translates into Netty autoRead=false, so kernel-level backpressure
        // happens atomically with the state transition.
        KafkaChannel channel = channels.get(id);
        if (channel == null) channel = closingChannels.get(id);
        if (channel == null) return;
        KafkaChannelMuteBridge.mute(channel);
        // Track operator-driven mutes separately from self-mutes due to memory pressure.
        // The recovery loop at the top of poll() uses this set to decide which channels
        // are safe to unmute when memory pressure clears.
        explicitlyMutedChannels.add(channel);
    }

    @Override
    public void unmute(String id) {
        KafkaChannel channel = channels.get(id);
        if (channel == null) channel = closingChannels.get(id);
        if (channel == null) return;
        if (KafkaChannelMuteBridge.maybeUnmute(channel)) {
            // Drop from the operator-muted set only on successful unmute. NIO does the
            // same (Selector.unmute lines 762-763): if maybeUnmute returns false (e.g.
            // the channel is in MUTED_AND_RESPONSE_PENDING and not yet ready to leave
            // MUTED), the set entry stays so the next unmute attempt still respects the
            // operator intent.
            explicitlyMutedChannels.remove(channel);
            // unmute may have flipped autoRead on; bytes may now flow into the transport
            // queue and the next read step needs to drain them, so wake any blocking poll.
            wakeup.release();
        }
    }

    @Override
    public void muteAll() {
        for (KafkaChannel channel : channels.values()) {
            KafkaChannelMuteBridge.mute(channel);
            explicitlyMutedChannels.add(channel);
        }
    }

    @Override
    public void unmuteAll() {
        for (KafkaChannel channel : channels.values()) {
            if (KafkaChannelMuteBridge.maybeUnmute(channel)) {
                explicitlyMutedChannels.remove(channel);
            }
        }
        wakeup.release();
    }

    @Override
    public boolean isChannelReady(String id) {
        KafkaChannel channel = channels.get(id);
        return channel != null && channel.ready();
    }

    @Override
    public List<NetworkSend> completedSends() {
        return completedSends;
    }

    @Override
    public Collection<NetworkReceive> completedReceives() {
        return completedReceives;
    }

    @Override
    public Map<String, ChannelState> disconnected() {
        return disconnected;
    }

    @Override
    public List<String> connected() {
        return connected;
    }

    @Override
    public void clearCompletedReceives() {
        completedReceives.clear();
    }

    @Override
    public void clearCompletedSends() {
        completedSends.clear();
    }

    @Override
    public List<KafkaChannel> channels() {
        return new ArrayList<>(channels.values());
    }

    @Override
    public KafkaChannel channel(String id) {
        return channels.get(id);
    }

    @Override
    public KafkaChannel closingChannel(String id) {
        return closingChannels.get(id);
    }

    /**
     * Package-private accessor used by integration tests that need to inspect the broker-side
     * Netty channel of an accepted connection (e.g. to verify that {@code childOption(TCP_NODELAY)}
     * and {@code childOption(SO_KEEPALIVE)} were actually applied by {@link IoUringServerListener}).
     * The standard {@link org.apache.kafka.common.network.Selectable} surface intentionally does
     * not expose the underlying transport's Netty channel, so a test-only accessor here is the
     * narrowest possible hook — it does not enlarge any public API and is unreachable from
     * production callers in other packages.
     *
     * @return the Netty channel for the given connection id, or {@code null} if no such channel
     *         exists on this selector
     */
    Channel nettyChannelFor(String id) {
        return nettyChannels.get(id);
    }

    /**
     * Package-private diagnostic accessor for tests to inspect transport-layer counters
     * (inbound queue depth, pending outbound bytes) during stall reproduction. Reaches the
     * {@link IoUringTransportLayer} attached to the channel's Netty attribute store. Not on
     * the {@link BrokerSelector} interface — purely a test hook.
     */
    IoUringTransportLayer transportFor(String id) {
        Channel nettyChannel = nettyChannels.get(id);
        return nettyChannel == null ? null : nettyChannel.attr(TRANSPORT_ATTR).get();
    }

    @Override
    public KafkaChannel lowestPriorityChannel() {
        // Mirrors org.apache.kafka.common.network.Selector.lowestPriorityChannel():
        //   1. A channel already in teardown — evicting one of those is "free" and never
        //      sacrifices a healthy connection.
        //   2. The least-recently-active channel — its peer hasn't done anything for the
        //      longest, so closing it loses the least useful state. Important so that
        //      the controller and replication peers (constantly active) survive eviction
        //      under broker-wide max.connections pressure on a PLAINTEXT listener.
        //   3. Any channel — degenerate fallback if neither table has an entry.
        // Previously this returned `channels.values().iterator().next()` which, given
        // the LinkedHashMap insertion order, would target the OLDEST connection (most
        // likely the inter-broker controller/replication peer that connected at boot).
        if (!closingChannels.isEmpty()) {
            return closingChannels.values().iterator().next();
        }
        if (!lastActiveNanos.isEmpty()) {
            // Find the channel with the smallest lastActiveNanos. The map is updated on
            // every read/write step in poll() so it tracks per-channel liveness accurately.
            String oldestId = null;
            long oldestNanos = Long.MAX_VALUE;
            for (Map.Entry<String, Long> entry : lastActiveNanos.entrySet()) {
                long nanos = entry.getValue();
                if (nanos < oldestNanos) {
                    oldestNanos = nanos;
                    oldestId = entry.getKey();
                }
            }
            if (oldestId != null) {
                KafkaChannel channel = channels.get(oldestId);
                if (channel != null) return channel;
            }
        }
        return channels.isEmpty() ? null : channels.values().iterator().next();
    }

    @Override
    public void register(String id, SocketChannel socketChannel) {
        throw new UnsupportedOperationException(
            "IoUringSelector accepts connections directly via the io_uring listener; " +
            "register(id, SocketChannel) is the Acceptor pathway that does not apply here. " +
            "Channel id was: " + id);
    }

    @Override
    public void connect(String id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) {
        throw new UnsupportedOperationException(
            "IoUringSelector is broker-side (server only); outgoing connect() is not supported. " +
            "Attempted destination: " + id + " -> " + address);
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        for (KafkaChannel channel : channels.values()) {
            Utils.closeQuietly(channel, "channel on selector close");
        }
        channels.clear();
        for (KafkaChannel channel : closingChannels.values()) {
            Utils.closeQuietly(channel, "closing channel on selector close");
        }
        closingChannels.clear();
        explicitlyMutedChannels.clear();
        nettyChannels.clear();
        lastActiveNanos.clear();
        // Drain queues so any in-flight ByteBufs are released.
        KafkaChannel pending;
        while ((pending = pendingAccepts.poll()) != null) {
            pendingAcceptCount.decrementAndGet();
            Utils.closeQuietly(pending, "pending-accept on close");
        }
        pendingDisconnects.clear();
        wakeup.release();
    }

    @Override
    public void close(String id) {
        KafkaChannel channel = channels.remove(id);
        nettyChannels.remove(id);
        lastActiveNanos.remove(id);
        if (channel != null) {
            // Mirror NIO Selector.close(id) (clients/.../Selector.java:891): stamp the
            // channel as LOCAL_CLOSE before tear-down. The caller of close(id) is the
            // Acceptor under broker-max pressure, not a peer disconnect — downstream
            // log/metric attribution (Processor.processChannelException via
            // openOrClosingChannel) reads channel.state() and would otherwise see the
            // pre-close state (typically READY) and report the eviction as if it were
            // an in-flight error. The closingChannels path below intentionally leaves
            // state alone (same as NIO) — those channels already carry the state under
            // which closing was first triggered.
            channel.state(ChannelState.LOCAL_CLOSE);
            explicitlyMutedChannels.remove(channel);
            Utils.closeQuietly(channel, "channel close(" + id + ")");
            return;
        }
        // Mirror NIO's Selector.close(id) (clients/.../Selector.java:886-899): if the
        // channel is not in the active map, look in closingChannels and clean it up
        // there. Without this, closeExcessConnections (called from the Acceptor under
        // broker-max pressure) can repeatedly select the same closing channel via
        // lowestPriorityChannel() — closingChannels stays populated until the NEXT
        // poll's eviction, and within the same poll there is no other way to drain it.
        // Equally important: the quota for closing channels was already `dec`'d when
        // they surfaced in `disconnected`. If close(id) re-routed through any path that
        // re-applies `dec`, the counter would underflow on every excess-eviction race.
        // Keeping the cleanup local to the selector and silent to the outside (no
        // entry in `disconnected`) preserves that one-dec-per-channel invariant.
        KafkaChannel closing = closingChannels.remove(id);
        if (closing != null) {
            explicitlyMutedChannels.remove(closing);
            failedSends.remove(id);
            Utils.closeQuietly(closing, "closing channel close(" + id + ")");
        }
    }
}
