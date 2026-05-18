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
package org.apache.kafka.storage.internals.concentration;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Broker-facing facade for the concentration kernel. Owns the {@link LogicalTopicRegistry},
 * {@link LogicalOffsetTracker}, {@link BackingScanRecoverer}, and the per-(logicalTopic,
 * logicalPartition) {@link LogicalSidecarIndex} files. This is the single object the broker glue
 * (KafkaApis, ReplicaManager, UnifiedLog) holds a reference to.
 *
 * <p>The four hook points exercised by the broker, and where in this class they live:
 * <ul>
 *   <li><b>Produce – backing-topic rejection</b>: {@link #isBackingTopic(String)} — the broker
 *       inverts this and returns {@code INVALID_TOPIC_EXCEPTION} for direct produces to a backing
 *       physical name.</li>
 *   <li><b>Produce – logical→backing routing + offset assignment</b>:
 *       {@link #backingPartitionFor(String, int)} chooses the backing partition;
 *       {@link #reserveProduce(String, int)} / {@link #commitProduce(Reservation, long)} /
 *       {@link #rollbackProduce(Reservation)} drive the offset reservation against the tracker
 *       and persist the sidecar entry atomically.</li>
 *   <li><b>Fetch – translation</b>: {@link #resolveBackingOffset(String, int, long)} maps a
 *       logical offset to its backing offset via the sidecar.</li>
 *   <li><b>DeleteRecords</b>: {@link #advanceStartOffset(String, int, long)} moves the visible
 *       low-water for one logical partition. The backing log is not consulted.</li>
 *   <li><b>Restart</b>: {@link #recoverFromSidecars(Collection)} for cheap startup;
 *       {@link #recoverFromBackingScan(Iterator)} for full rebuild when sidecars are missing.</li>
 * </ul>
 *
 * <p>The kernel is thread-safe for concurrent produce/fetch across different partitions. The
 * sidecar map and the registry are concurrent collections; the tracker serialises reservations
 * per partition but does not block sibling partitions. {@link #close()} is idempotent and closes
 * every cached sidecar.
 */
public final class ConcentrationKernel implements AutoCloseable {

    private final LogicalTopicRegistry registry = new LogicalTopicRegistry();
    private final LogicalOffsetTracker tracker = new LogicalOffsetTracker();
    private final BackingScanRecoverer recoverer;
    private final ConcurrentHashMap<LogicalPartition, LogicalSidecarIndex> sidecars = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    public ConcentrationKernel(File sidecarDir) {
        this.recoverer = new BackingScanRecoverer(Objects.requireNonNull(sidecarDir, "sidecarDir"));
    }

    // ------------------ Declaration ------------------

    public void declare(LogicalTopicDescriptor descriptor) {
        ensureOpen();
        registry.declare(descriptor);
    }

    public Optional<LogicalTopicDescriptor> describe(String logicalName) {
        return registry.get(logicalName);
    }

    public boolean isBackingTopic(String physicalName) {
        return registry.isBackingTopic(physicalName);
    }

    /**
     * Returns the registry signal for broker fan-out: every logical topic whose backing is
     * {@code backingTopic}. The broker uses this on the fetch path to know which logical topics
     * may have records on a given backing partition.
     */
    public Collection<LogicalTopicDescriptor> descriptorsFor(String backingTopic) {
        return registry.descriptorsFor(backingTopic);
    }

    // ------------------ Routing ------------------

    /**
     * Logical → backing partition routing. Stable across calls. Throws
     * {@link NoSuchElementException} if the logical topic has not been declared, because the
     * broker should never call this without first resolving the topic.
     */
    public int backingPartitionFor(String logicalTopic, int logicalPartition) {
        LogicalTopicDescriptor d = registry.get(logicalTopic)
            .orElseThrow(() -> new NoSuchElementException("logical topic not declared: " + logicalTopic));
        return LogicalPartitionMapper.backingPartitionFor(d, logicalPartition);
    }

    // ------------------ Produce path ------------------

    /**
     * Reserve the next logical offset for (logicalTopic, logicalPartition). Caller must follow
     * with exactly one of {@link #commitProduce(Reservation, long)} or
     * {@link #rollbackProduce(Reservation)}. Blocks if another reservation is in flight for the
     * same partition (v1 serialises reservations per partition to honour the no-gaps invariant).
     */
    public Reservation reserveProduce(String logicalTopic, int logicalPartition) {
        ensureOpen();
        LogicalTopicDescriptor d = registry.get(logicalTopic)
            .orElseThrow(() -> new NoSuchElementException("logical topic not declared: " + logicalTopic));
        if (logicalPartition < 0 || logicalPartition >= d.numLogicalPartitions()) {
            throw new IllegalArgumentException(
                "logical partition " + logicalPartition + " out of range [0," + d.numLogicalPartitions() + ")");
        }
        return tracker.reserve(logicalTopic, logicalPartition);
    }

    /**
     * Persist the (logical → backing) sidecar entry and commit the reservation. If the sidecar
     * append throws (e.g., backing-offset non-monotonic — caller bug), the reservation is rolled
     * back automatically so the slot is free for the next reserve. The exception is propagated.
     */
    public void commitProduce(Reservation reservation, long backingOffset) throws IOException {
        ensureOpen();
        Objects.requireNonNull(reservation, "reservation");
        LogicalSidecarIndex sidecar = sidecarFor(reservation.logicalTopic(), reservation.logicalPartition());
        try {
            sidecar.append(backingOffset);
        } catch (IOException | RuntimeException e) {
            tracker.rollback(reservation);
            throw e;
        }
        tracker.commit(reservation);
    }

    public void rollbackProduce(Reservation reservation) {
        tracker.rollback(reservation);
    }

    // ------------------ Fetch path ------------------

    /**
     * Translate (logicalTopic, logicalPartition, logicalOffset) → backing offset. Used by the
     * fetch path to convert a stock-client logical fetch position into a backing read position.
     * Throws {@link IndexOutOfBoundsException} if the logical offset is outside the produced
     * range, and {@link IOException} if no sidecar exists yet (partition has had no produces).
     */
    public long resolveBackingOffset(String logicalTopic, int logicalPartition, long logicalOffset) throws IOException {
        ensureOpen();
        LogicalSidecarIndex sidecar = sidecarFor(logicalTopic, logicalPartition);
        return sidecar.lookup(logicalOffset);
    }

    public long nextLogicalOffset(String logicalTopic, int logicalPartition) {
        return tracker.nextLogicalOffset(logicalTopic, logicalPartition);
    }

    public long startLogicalOffset(String logicalTopic, int logicalPartition) {
        return tracker.startOffset(logicalTopic, logicalPartition);
    }

    // ------------------ DeleteRecords ------------------

    public void advanceStartOffset(String logicalTopic, int logicalPartition, long newStartOffset) {
        ensureOpen();
        tracker.advanceStartOffset(logicalTopic, logicalPartition, newStartOffset);
    }

    // ------------------ Recovery ------------------

    public void recoverFromSidecars(Collection<LogicalPartition> partitions) throws IOException {
        ensureOpen();
        recoverer.recoverFromSidecars(partitions, tracker);
    }

    public void recoverFromBackingScan(Iterator<RecoveryRecord> stream) throws IOException {
        ensureOpen();
        recoverer.recoverFromScan(stream, tracker);
    }

    // ------------------ Lifecycle ------------------

    @Override
    public synchronized void close() throws IOException {
        if (closed) return;
        closed = true;
        IOException firstError = null;
        for (LogicalSidecarIndex sidecar : sidecars.values()) {
            try {
                sidecar.close();
            } catch (IOException e) {
                if (firstError == null) firstError = e;
                // continue closing the rest — never let one bad handle keep the others open
            }
        }
        sidecars.clear();
        if (firstError != null) throw firstError;
    }

    // ------------------ internals ------------------

    private LogicalSidecarIndex sidecarFor(String logicalTopic, int logicalPartition) throws IOException {
        LogicalPartition key = new LogicalPartition(logicalTopic, logicalPartition);
        LogicalSidecarIndex existing = sidecars.get(key);
        if (existing != null) return existing;
        LogicalSidecarIndex fresh = recoverer.openSidecar(logicalTopic, logicalPartition);
        LogicalSidecarIndex previous = sidecars.putIfAbsent(key, fresh);
        if (previous != null) {
            // Lost the race; close the unused handle and return the winner.
            try {
                fresh.close();
            } catch (IOException ignored) {
                // best-effort
            }
            return previous;
        }
        return fresh;
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("ConcentrationKernel is closed");
    }
}
