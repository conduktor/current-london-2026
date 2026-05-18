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
import org.apache.kafka.common.network.KafkaChannel;
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
import java.util.concurrent.atomic.AtomicLong;

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
     * Channel ids the operator has muted. {@link KafkaChannel#mute()} is package-private in
     * {@code clients/}, so we track mute state here ourselves; {@link #poll(long)} gates
     * reads on this set, and the corresponding {@code setAutoRead(false)} call on the
     * Netty channel stops the kernel feeding more bytes.
     */
    private final Set<String> mutedChannelIds = new HashSet<>();

    // Cross-thread queues (event loop pushes, Processor pulls).
    private final Queue<KafkaChannel> pendingAccepts = new ConcurrentLinkedQueue<>();
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

    private final AtomicLong idGen = new AtomicLong();
    private volatile boolean closed;

    /** Test-only constructor: uses processor id 0 and an empty configs map (default principal builder). */
    public IoUringSelector(ListenerName listenerName,
                           int maxReceiveSize,
                           MemoryPool memoryPool,
                           long connectionsMaxIdleNanos,
                           Time time) {
        this(listenerName, maxReceiveSize, memoryPool, connectionsMaxIdleNanos, time, 0, java.util.Collections.emptyMap());
    }

    /** Test-only constructor: empty configs map (default principal builder). */
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
        // Format must match ServerConnectionId so Processor.processDisconnected can parse it
        // and decrement ConnectionQuotas correctly. Using the synthetic "iouring-N" form
        // would silently break quota release.
        String id = local.getAddress().getHostAddress() + ":" + local.getPort() + "-"
                  + remote.getAddress().getHostAddress() + ":" + remote.getPort() + "-"
                  + processorId + "-" + idGen.incrementAndGet();
        IoUringTransportLayer transport = new IoUringTransportLayer(nettyChannel, remote, local);
        Authenticator authenticator = new IoUringPlaintextAuthenticator(transport, listenerName, configs);
        IoUringChannelMetadataRegistry metadata = new IoUringChannelMetadataRegistry();
        KafkaChannel channel = new KafkaChannel(id, transport, () -> authenticator, maxReceiveSize, memoryPool, metadata);

        nettyChannel.attr(TRANSPORT_ATTR).set(transport);
        nettyChannel.attr(CHANNEL_ID_ATTR).set(id);

        // Stash the netty channel so mute/unmute can flip autoRead without chasing
        // references through the KafkaChannel's selectionKey.
        nettyChannels.put(id, nettyChannel);
        pendingAccepts.offer(channel);
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

        // Evict the previous poll's closing channels. The Processor has had its chance to
        // resolve any final completedReceives via closingChannel(id); now we can release
        // the KafkaChannel for real. This mirrors NIO's KSelector.closingChannels lifecycle.
        if (!closingChannels.isEmpty()) {
            for (KafkaChannel c : closingChannels.values()) {
                Utils.closeQuietly(c, "closing channel after Processor drained");
            }
            closingChannels.clear();
        }

        // Reset per-poll outputs.
        completedReceives.clear();
        completedSends.clear();
        disconnected.clear();
        connected.clear();
        justAccepted.clear();

        // Capture once, BEFORE I/O, so a channel that progresses this poll never expires this poll.
        long nowNanos = time.nanoseconds();

        boolean madeProgress = false;

        // 1. Drain accepts.
        KafkaChannel acceptedChannel;
        while ((acceptedChannel = pendingAccepts.poll()) != null) {
            channels.put(acceptedChannel.id(), acceptedChannel);
            lastActiveNanos.put(acceptedChannel.id(), nowNanos);
            // PLAINTEXT auth completes synchronously; one prepare() call drives the channel to READY.
            try {
                acceptedChannel.prepare();
            } catch (Exception e) {
                log.debug("PLAINTEXT prepare unexpectedly failed for {}", acceptedChannel.id(), e);
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

            // Read step.
            if (!mutedChannelIds.contains(channel.id())) {
                try {
                    long read = channel.read();
                    if (read > 0) {
                        lastActiveNanos.put(channel.id(), nowNanos);
                        madeProgress = true;
                    }
                    NetworkReceive completed = channel.maybeCompleteReceive();
                    if (completed != null) {
                        completedReceives.add(completed);
                        madeProgress = true;
                    }
                } catch (IOException e) {
                    log.debug("Read failed on channel {}", channel.id(), e);
                    enqueueClose(channel.id(), ChannelState.LOCAL_CLOSE);
                    it.remove();
                    lastActiveNanos.remove(channel.id());
                    continue;
                }
            }

            // Write step.
            if (channel.hasSend()) {
                try {
                    long written = channel.write();
                    if (written > 0) {
                        lastActiveNanos.put(channel.id(), nowNanos);
                        madeProgress = true;
                    }
                    NetworkSend send = channel.maybeCompleteSend();
                    if (send != null) {
                        completedSends.add(send);
                        madeProgress = true;
                    }
                } catch (IOException e) {
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
        String disconnectId;
        while ((disconnectId = pendingDisconnects.poll()) != null) {
            KafkaChannel channel = channels.remove(disconnectId);
            nettyChannels.remove(disconnectId);
            lastActiveNanos.remove(disconnectId);
            mutedChannelIds.remove(disconnectId);
            if (channel == null) continue;
            try {
                while (channel.ready()) {
                    long read = channel.read();
                    NetworkReceive completed = channel.maybeCompleteReceive();
                    if (completed != null) {
                        completedReceives.add(completed);
                        madeProgress = true;
                        // KSelector emits at most one completedReceive per channel per poll —
                        // matching that invariant keeps Processor.processCompletedReceives
                        // accounting (per-IP throttling, request-channel queue) consistent.
                        break;
                    }
                    if (read <= 0) break;
                }
            } catch (IOException e) {
                log.debug("Final read on disconnecting channel {} failed", disconnectId, e);
            }
            closingChannels.put(disconnectId, channel);
            disconnected.put(disconnectId, ChannelState.LOCAL_CLOSE);
            madeProgress = true;
        }

        // 4. Idle expiry — only against channels that haven't already been claimed by disconnect.
        if (connectionsMaxIdleNanos > 0) {
            Iterator<Map.Entry<String, Long>> idleIt = lastActiveNanos.entrySet().iterator();
            while (idleIt.hasNext()) {
                Map.Entry<String, Long> entry = idleIt.next();
                if (nowNanos - entry.getValue() > connectionsMaxIdleNanos) {
                    KafkaChannel channel = channels.remove(entry.getKey());
                    nettyChannels.remove(entry.getKey());
                    mutedChannelIds.remove(entry.getKey());
                    if (channel != null) {
                        disconnected.put(entry.getKey(), ChannelState.EXPIRED);
                        Utils.closeQuietly(channel, "expired channel");
                    }
                    idleIt.remove();
                    madeProgress = true;
                }
            }
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
            mutedChannelIds.remove(id);
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
        KafkaChannel channel = channels.get(send.destinationId());
        if (channel == null) {
            throw new IllegalStateException("Attempt to send on unknown channel " + send.destinationId());
        }
        channel.setSend(send);
    }

    @Override
    public void mute(String id) {
        if (!channels.containsKey(id)) return;
        if (mutedChannelIds.add(id)) {
            Channel netty = nettyChannels.get(id);
            if (netty != null) netty.config().setAutoRead(false);
        }
    }

    @Override
    public void unmute(String id) {
        if (!channels.containsKey(id)) return;
        if (mutedChannelIds.remove(id)) {
            Channel netty = nettyChannels.get(id);
            if (netty != null) netty.config().setAutoRead(true);
            wakeup.release(); // buffered bytes may now be drained
        }
    }

    @Override
    public void muteAll() {
        for (String id : channels.keySet()) {
            if (mutedChannelIds.add(id)) {
                Channel netty = nettyChannels.get(id);
                if (netty != null) netty.config().setAutoRead(false);
            }
        }
    }

    @Override
    public void unmuteAll() {
        for (String id : channels.keySet()) {
            if (mutedChannelIds.remove(id)) {
                Channel netty = nettyChannels.get(id);
                if (netty != null) netty.config().setAutoRead(true);
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

    @Override
    public KafkaChannel lowestPriorityChannel() {
        // v1: not used by the io_uring code path (the broker's quota-eviction logic targets
        // the listener pool, and the io_uring listener handles its own backpressure via
        // SO_BACKLOG / accept-rate). Return the first channel as a sensible default if any
        // caller wires this up later.
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
        nettyChannels.clear();
        lastActiveNanos.clear();
        mutedChannelIds.clear();
        // Drain queues so any in-flight ByteBufs are released.
        KafkaChannel pending;
        while ((pending = pendingAccepts.poll()) != null) {
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
        mutedChannelIds.remove(id);
        if (channel != null) {
            Utils.closeQuietly(channel, "channel close(" + id + ")");
        }
    }
}
