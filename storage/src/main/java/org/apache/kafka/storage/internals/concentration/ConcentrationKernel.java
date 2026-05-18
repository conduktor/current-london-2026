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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
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

    /**
     * Directory name used by the broker wiring when nesting the sidecar directory inside a log
     * dir. Exposed here so {@code LogManager.loadLogs} can filter it from its topic-partition
     * scan, mirroring how {@code RemoteIndexCache.DIR_NAME} is filtered. Without this exclusion
     * LogManager rejects the directory because it does not match the {@code topic-partition}
     * naming convention, failing broker startup.
     */
    public static final String SIDECAR_DIR_NAME = "_concentration_sidecars";

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
     * True if {@code name} has been declared as a logical topic on this broker. Used by broker hot
     * paths (produce / fetch / DeleteRecords) to decide whether to route a request through the
     * kernel or treat it as a stock physical-topic request. Cheap concurrent read — the registry
     * is a {@link java.util.concurrent.ConcurrentHashMap} under the hood.
     */
    public boolean isLogicalTopic(String name) {
        return registry.contains(name);
    }

    /**
     * Returns the registry signal for broker fan-out: every logical topic whose backing is
     * {@code backingTopic}. The broker uses this on the fetch path to know which logical topics
     * may have records on a given backing partition. The returned list is an immutable snapshot
     * — safe to iterate without holding any kernel-internal lock.
     */
    public List<LogicalTopicDescriptor> descriptorsFor(String backingTopic) {
        return List.copyOf(registry.descriptorsFor(backingTopic));
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

    /**
     * Reserve a contiguous run of {@code count} logical offsets on one partition. Used by the
     * produce hot path when a stock client sends a multi-record batch: the broker reserves K
     * offsets, stamps them as headers on the K records, appends the batch, then commits or rolls
     * back via the matching {@link #commitProduceBatch} / {@link #rollbackProduceBatch}.
     *
     * <p>The per-partition tracker lock is held from reserve through commit/rollback, so K must
     * be small (one produce batch). The kernel does not impose an upper bound — that is the
     * broker's job, mirroring its existing per-request validation.
     */
    public Reservation[] reserveProduceBatch(String logicalTopic, int logicalPartition, int count) {
        ensureOpen();
        LogicalTopicDescriptor d = registry.get(logicalTopic)
            .orElseThrow(() -> new NoSuchElementException("logical topic not declared: " + logicalTopic));
        if (logicalPartition < 0 || logicalPartition >= d.numLogicalPartitions()) {
            throw new IllegalArgumentException(
                "logical partition " + logicalPartition + " out of range [0," + d.numLogicalPartitions() + ")");
        }
        return tracker.reserveBatch(logicalTopic, logicalPartition, count);
    }

    /**
     * Persist sidecar entries for every reservation in the batch in order
     * {@code [firstBackingOffset, firstBackingOffset+1, ..., firstBackingOffset + batch.length - 1]}
     * and commit the batch atomically. If any sidecar append throws, the whole batch is rolled
     * back so the slots are reusable; previously appended sidecar entries are left in place but
     * the tracker does not advance — recovery via backing-scan repairs the sidecar.
     */
    public void commitProduceBatch(Reservation[] batch, long firstBackingOffset) throws IOException {
        ensureOpen();
        Objects.requireNonNull(batch, "batch");
        if (batch.length == 0) {
            throw new IllegalArgumentException("commitProduceBatch requires a non-empty batch");
        }
        LogicalSidecarIndex sidecar = sidecarFor(batch[0].logicalTopic(), batch[0].logicalPartition());
        try {
            for (int i = 0; i < batch.length; i++) {
                sidecar.append(firstBackingOffset + i);
            }
        } catch (IOException | RuntimeException e) {
            tracker.rollbackBatch(batch);
            throw e;
        }
        tracker.commitBatch(batch);
    }

    public void rollbackProduceBatch(Reservation[] batch) {
        tracker.rollbackBatch(batch);
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

    // ------------------ Partition teardown ------------------

    /**
     * Drop all kernel state for one (logicalTopic, logicalPartition): close the sidecar handle,
     * remove the cached handle, remove the tracker state, and delete the on-disk sidecar file.
     * Used when the broker deletes a logical partition (or the whole logical topic, iterated
     * partition-by-partition). Without this API the kernel's caches grow linearly with the total
     * number of partitions ever produced to over the broker's lifetime — the production-readiness
     * audit flagged that as an unbounded-growth liability.
     *
     * <p>Caller invariant: no produce reservation is in flight against this partition. The
     * tracker raises {@link IllegalStateException} if one is, which the kernel re-throws after
     * releasing the sidecar handle (file is already closed; map entry already gone — partial
     * teardown is preferable to a leaked handle).
     *
     * @return {@code true} if any state existed and was removed; {@code false} if there was
     *     nothing to remove.
     */
    public synchronized boolean removeLogicalPartition(String logicalTopic, int logicalPartition) throws IOException {
        ensureOpen();
        Objects.requireNonNull(logicalTopic, "logicalTopic");
        LogicalPartition key = new LogicalPartition(logicalTopic, logicalPartition);
        boolean removedAny = false;

        // Order matters. Drop tracker state first because tracker.removePartition acquires the
        // partition lock — that's the lock held throughout reserve→commit, so by the time it
        // returns no in-flight commitProduce can still be using the sidecar handle. If we
        // closed the sidecar first we'd risk yanking it out from under a concurrent append.
        //
        // Caller contract: the broker must have stopped serving this partition before calling
        // here. tracker.removePartition surfaces a violation as IllegalStateException (an
        // outstanding reservation indicates a still-live produce) which propagates without
        // having touched anything yet.
        if (tracker.removePartition(logicalTopic, logicalPartition)) {
            removedAny = true;
        }
        LogicalSidecarIndex sidecar = sidecars.remove(key);
        if (sidecar != null) {
            sidecar.close();
            removedAny = true;
        }
        File sidecarFile = recoverer.sidecarFile(logicalTopic, logicalPartition);
        if (sidecarFile.exists()) {
            // Delete is best-effort: if it fails (e.g., a stray open handle the kernel does not
            // know about), surface as IOException so the caller knows the on-disk state diverged
            // from the in-memory state.
            if (!sidecarFile.delete()) {
                throw new IOException("failed to delete sidecar file " + sidecarFile);
            }
            removedAny = true;
        }
        return removedAny;
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

    /**
     * Partitions declared on this broker that have NO sidecar file on disk, in registry order.
     * Used by the full-scan recovery path to decide which backing partitions to scan and which
     * (logicalTopic, logicalPartition) tuples to filter in. A partition appears here iff:
     * <ul>
     *   <li>Its logical topic has been declared via {@link #declare}, AND</li>
     *   <li>The sidecar file {@code sidecarDir/<topic>/<partition>.sidecar} does not exist.</li>
     * </ul>
     * "Never produced to" and "sidecar lost / corrupt" both surface here — they are not
     * distinguishable from disk alone. The downstream backing-scan handles both correctly: a
     * never-produced partition has no matching records on the backing log so the iterator yields
     * nothing, leaving the tracker at the default {@code (0, 0)}; a lost-index partition has
     * records and is rebuilt from them.
     */
    public List<LogicalPartition> partitionsWithoutSidecar() {
        ensureOpen();
        List<LogicalPartition> out = new ArrayList<>();
        for (LogicalTopicDescriptor d : registry.all()) {
            for (int p = 0; p < d.numLogicalPartitions(); p++) {
                if (!recoverer.sidecarFile(d.logicalName(), p).exists()) {
                    out.add(new LogicalPartition(d.logicalName(), p));
                }
            }
        }
        return out;
    }

    /**
     * Cheap-path startup recovery. For every declared logical topic, enumerate the sidecar files
     * already on disk under {@code sidecarDir/<topic>/<partition>.sidecar} and seed the tracker
     * with their {@code (logicalStart=0, nextLogical=size)}.
     *
     * <p>This is the PROMPT's "restart with index → sub-second offset-tracker rebuild" path. The
     * broker calls this at startup after declaring every logical topic. Partitions that have
     * never been produced to have no sidecar file on disk and are left at the tracker's default
     * {@code (0, 0)} — they will be created on first produce.
     *
     * <p>Sidecar files whose {@code openSidecar} throws {@code CorruptIndexException} (tail-torn
     * write detected at open time) are NOT swallowed here: the exception propagates, surfacing
     * the corruption to the operator so the backing-log scan fallback can be triggered.
     *
     * <p>Idempotent: re-calling overwrites the tracker state for every partition with a sidecar,
     * which is correct because the on-disk sidecar is the source of truth in this path.
     */
    public void recoverFromDisk() throws IOException {
        ensureOpen();
        List<LogicalPartition> present = new ArrayList<>();
        for (LogicalTopicDescriptor d : registry.all()) {
            File topicDir = new File(recoverer.sidecarFile(d.logicalName(), 0).getParentFile().getPath());
            File[] files = topicDir.listFiles((dir, name) -> name.endsWith(".sidecar"));
            if (files == null) continue;
            for (File f : files) {
                String name = f.getName();
                int dot = name.lastIndexOf('.');
                if (dot <= 0) continue;
                int partition;
                try {
                    partition = Integer.parseInt(name.substring(0, dot));
                } catch (NumberFormatException nfe) {
                    continue; // not a numeric partition file — leave alone
                }
                if (partition < 0 || partition >= d.numLogicalPartitions()) {
                    // A sidecar file outside the declared partition range is a stale artefact
                    // (e.g., a topic that was previously declared with a larger N). Skip silently
                    // — the operator can clean it up; recovery should not abort because of it.
                    continue;
                }
                present.add(new LogicalPartition(d.logicalName(), partition));
            }
        }
        recoverer.recoverFromSidecars(present, tracker);
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

    /**
     * Double-checked: the fast path (sidecar already open) is a lock-free read of the concurrent
     * map. The slow path (first open per partition) takes the same monitor as {@link #close()},
     * so a sidecar opened after close() began running is impossible — close() either runs to
     * completion first and the subsequent open sees {@code closed == true} via ensureOpen(), or
     * close() blocks until this open finishes and then closes the new handle along with the
     * rest.
     */
    private LogicalSidecarIndex sidecarFor(String logicalTopic, int logicalPartition) throws IOException {
        LogicalPartition key = new LogicalPartition(logicalTopic, logicalPartition);
        LogicalSidecarIndex existing = sidecars.get(key);
        if (existing != null) return existing;
        synchronized (this) {
            ensureOpen();
            existing = sidecars.get(key);
            if (existing != null) return existing;
            LogicalSidecarIndex fresh = recoverer.openSidecar(logicalTopic, logicalPartition);
            sidecars.put(key, fresh);
            return fresh;
        }
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("ConcentrationKernel is closed");
    }
}
