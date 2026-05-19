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

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Per-(logicalTopic, logicalPartition) monotonic offset assignment with a serialised
 * reserve/commit/rollback protocol. v1 keeps the protocol simple — only one outstanding
 * reservation batch per partition at a time — which is enough to honour the acceptance criterion
 * "subsequent appends do not leave offset gaps" without the stretch cascade-rollback machinery.
 *
 * <p>A reservation may be a single offset (legacy reserve/commit/rollback) or a contiguous run of
 * K offsets ({@link #reserveBatch}/{@link #commitBatch}/{@link #rollbackBatch}). Batch semantics
 * are all-or-nothing: every member of a reserved batch must be committed or every member must be
 * rolled back. This matches what a stock Kafka producer needs — a multi-record produce batch is
 * atomic at the leader (LogValidator assigns contiguous offsets), so the kernel mirrors that.
 *
 * <p>Partitions are independent; concurrent traffic across many logical partitions is not
 * serialised against each other.
 */
public final class LogicalOffsetTracker {

    private record Key(String logicalTopic, int logicalPartition) { }

    private static final class PartitionState {
        final ReentrantLock lock = new ReentrantLock();
        // volatile so lockless readers (nextLogicalOffset / startOffset) see writes made by the
        // lock-holding writers (commit / advanceStartOffset / restorePartition). The lock alone
        // would create a happens-before for synchronised readers, but these accessors are
        // intentionally lock-free for the fetch path.
        volatile long startOffset = 0L;
        volatile long nextOffset = 0L;
        // The currently outstanding batch (length 1 for the legacy single-record API). null when
        // no reservation is in flight. Equality is by reference: a caller-provided batch must be
        // THE SAME array object the tracker handed out, not a copy.
        Reservation[] outstandingBatch = null;
    }

    private final ConcurrentHashMap<Key, PartitionState> states = new ConcurrentHashMap<>();

    private PartitionState stateFor(String logicalTopic, int logicalPartition) {
        Objects.requireNonNull(logicalTopic, "logicalTopic");
        return states.computeIfAbsent(new Key(logicalTopic, logicalPartition), k -> new PartitionState());
    }

    public Reservation reserve(String logicalTopic, int logicalPartition) {
        return reserveBatch(logicalTopic, logicalPartition, 1)[0];
    }

    public void commit(Reservation reservation) {
        Objects.requireNonNull(reservation, "reservation");
        commitBatchInternal(new Reservation[]{reservation}, /*expectSameRef*/ false);
    }

    public void rollback(Reservation reservation) {
        Objects.requireNonNull(reservation, "reservation");
        rollbackBatchInternal(new Reservation[]{reservation}, /*expectSameRef*/ false);
    }

    /**
     * Reserve a contiguous run of {@code count} logical offsets on one partition. The returned
     * array is the SAME object the tracker holds as its outstanding batch — pass it back verbatim
     * to {@link #commitBatch} or {@link #rollbackBatch}. Used by the broker's produce hot path to
     * stamp K records of a single produce batch with K contiguous logical offsets before the
     * backing append.
     *
     * <p>The per-partition lock is acquired in {@code reserveBatch} and released in the matching
     * {@code commitBatch} / {@code rollbackBatch}. Concurrent batches on the same partition are
     * serialised; sibling partitions are independent.
     *
     * @throws IllegalArgumentException if {@code count <= 0}.
     */
    public Reservation[] reserveBatch(String logicalTopic, int logicalPartition, int count) {
        if (count <= 0) {
            throw new IllegalArgumentException("batch reservation count must be > 0, was " + count);
        }
        PartitionState s = stateFor(logicalTopic, logicalPartition);
        s.lock.lock();
        // BLOCKER #179: the lock is acquired BEFORE any allocation; if either
        // {@code new Reservation[count]} or any per-element construction throws — most
        // realistically OutOfMemoryError on a hostile {@code count} like Integer.MAX_VALUE, or
        // any future Reservation-constructor validation — the lock leaks forever, deadlocking
        // every subsequent reserve/commit on this partition. Hold the lock on the happy path
        // (commit/rollback releases it) but release it on any failure path. Catching Throwable is
        // intentional: an Error path is exactly the realistic failure mode here, and we'd rather
        // surface the original Throwable than risk an unlock that propagates a second Throwable.
        boolean success = false;
        try {
            Reservation[] batch = new Reservation[count];
            long base = s.nextOffset;
            for (int i = 0; i < count; i++) {
                batch[i] = new Reservation(logicalTopic, logicalPartition, base + i);
            }
            s.outstandingBatch = batch;
            success = true;
            return batch;
        } finally {
            if (!success) {
                s.lock.unlock();
            }
        }
    }

    /**
     * Commit every reservation in the batch atomically. After return, the partition's
     * nextLogicalOffset advances by {@code batch.length} and every reservation is in committed
     * state. The same array reference returned by {@link #reserveBatch} must be passed back.
     */
    public void commitBatch(Reservation[] batch) {
        Objects.requireNonNull(batch, "batch");
        commitBatchInternal(batch, /*expectSameRef*/ true);
    }

    /**
     * Roll back every reservation in the batch atomically. No offsets are consumed.
     */
    public void rollbackBatch(Reservation[] batch) {
        Objects.requireNonNull(batch, "batch");
        rollbackBatchInternal(batch, /*expectSameRef*/ true);
    }

    private void commitBatchInternal(Reservation[] batch, boolean expectSameRef) {
        PartitionState s = expectOutstandingBatch(batch, expectSameRef);
        Reservation last = batch[batch.length - 1];
        s.nextOffset = last.logicalOffset() + 1;
        for (Reservation r : batch) {
            r.markCommitted();
        }
        s.outstandingBatch = null;
        s.lock.unlock();
    }

    private void rollbackBatchInternal(Reservation[] batch, boolean expectSameRef) {
        PartitionState s = expectOutstandingBatch(batch, expectSameRef);
        for (Reservation r : batch) {
            r.markRolledBack();
        }
        s.outstandingBatch = null;
        s.lock.unlock();
    }

    public long nextLogicalOffset(String logicalTopic, int logicalPartition) {
        Objects.requireNonNull(logicalTopic, "logicalTopic");
        PartitionState s = states.get(new Key(logicalTopic, logicalPartition));
        return s == null ? 0L : s.nextOffset;
    }

    public long startOffset(String logicalTopic, int logicalPartition) {
        Objects.requireNonNull(logicalTopic, "logicalTopic");
        PartitionState s = states.get(new Key(logicalTopic, logicalPartition));
        return s == null ? 0L : s.startOffset;
    }

    /**
     * Move the readable lower bound for this logical partition forward. The next-offset
     * high-water is untouched. Used by DeleteRecords on a logical topic to advance only that
     * logical partition's start offset; the backing log is not truncated.
     */
    public void advanceStartOffset(String logicalTopic, int logicalPartition, long newStartOffset) {
        PartitionState s = stateFor(logicalTopic, logicalPartition);
        s.lock.lock();
        try {
            if (newStartOffset < s.startOffset) {
                throw new IllegalArgumentException(
                    "newStartOffset (" + newStartOffset + ") < current startOffset (" + s.startOffset + ")");
            }
            if (newStartOffset > s.nextOffset) {
                throw new IllegalArgumentException(
                    "newStartOffset (" + newStartOffset + ") > nextLogicalOffset (" + s.nextOffset + ")");
            }
            s.startOffset = newStartOffset;
        } finally {
            s.lock.unlock();
        }
    }

    /**
     * Drop all bookkeeping for one (logicalTopic, logicalPartition). Caller invariant: there must
     * be no outstanding reservation — that would indicate a teardown racing an in-flight produce,
     * which is a broker bug. Used when a logical partition is being deleted from the broker;
     * lets the tracker's state map shrink rather than grow without bound over the broker's
     * lifetime.
     *
     * @return {@code true} if a state entry existed and was removed; {@code false} if there was
     *     nothing to remove (idempotent).
     * @throws IllegalStateException if a reservation is currently outstanding on this partition.
     */
    public boolean removePartition(String logicalTopic, int logicalPartition) {
        Objects.requireNonNull(logicalTopic, "logicalTopic");
        Key key = new Key(logicalTopic, logicalPartition);
        PartitionState s = states.get(key);
        if (s == null) return false;
        // Take the lock so we're synchronised against any in-flight reserve/commit. If the lock
        // is held by another thread, the reservation batch it holds is the outstanding one; we
        // will see it under our lock and refuse rather than silently dropping live state.
        s.lock.lock();
        try {
            if (s.outstandingBatch != null) {
                throw new IllegalStateException(
                    "cannot remove (" + logicalTopic + "," + logicalPartition
                        + "): a reservation is outstanding");
            }
            states.remove(key, s);
            return true;
        } finally {
            s.lock.unlock();
        }
    }

    /**
     * Direct seeding of partition state, used during recovery from a sidecar or backing-log scan
     * and by {@link ConcentrationKernel#advanceStartOffset} on its rollback path.
     *
     * <p>r22 BLOCKER #193: this used to skip locking on the documented assumption that recovery is
     * single-threaded "by contract." That contract is not enforced anywhere — and the
     * {@link ConcentrationKernel#advanceStartOffset} path on the DeleteRecords codepath does NOT
     * consult {@code isBackingReady}, so it can be in-flight against {@code restorePartition}
     * being called by the leader recoverer on the same partition. The PartitionState's
     * {@code startOffset} and {@code nextOffset} are individually volatile, but the pair is not
     * atomic — a reader (or this writer) without the lock can observe a torn snapshot where
     * {@code startOffset} from one writer pairs with {@code nextOffset} from another. Worse, the
     * unsynchronized writes to two volatile longs in this method can be reordered with the
     * locked writes in {@link #advanceStartOffset} or {@link #commitBatchInternal}, leaving
     * permanent inconsistency.
     *
     * <p>Taking the lock here closes both windows. The lock is reentrant, so the kernel's
     * rollback path ({@link ConcentrationKernel#advanceStartOffset} → catch → restorePartition)
     * remains correct — that thread has already RELEASED the lock by the time the catch runs.
     * Boot-time recovery sees no contention because produce paths haven't started; leader-
     * recovery sees no contention because the gate is closed (in-flight commits roll back at the
     * commitProduce second gen-check, releasing their locks before the recoverer touches the
     * tracker). The lock acquisition is defensive against any future caller that violates the
     * contract; it's free on the uncontended path.
     */
    void restorePartition(String logicalTopic, int logicalPartition, long startOffset, long nextOffset) {
        if (startOffset < 0 || nextOffset < startOffset) {
            throw new IllegalArgumentException(
                "invalid restore: startOffset=" + startOffset + ", nextOffset=" + nextOffset);
        }
        PartitionState s = stateFor(logicalTopic, logicalPartition);
        s.lock.lock();
        try {
            s.startOffset = startOffset;
            s.nextOffset = nextOffset;
        } finally {
            s.lock.unlock();
        }
    }

    /**
     * Resolve and validate the outstanding-batch state for a caller-provided batch. When
     * {@code expectSameRef} is true (the {@link #commitBatch}/{@link #rollbackBatch} path) we
     * require the caller to hand back the exact array reference {@link #reserveBatch} returned,
     * because that's the strictest check available without a separate batch-id. The legacy
     * single-record path wraps the lone reservation in a fresh 1-element array, so for that
     * codepath we relax the array-identity check and instead match on the single Reservation's
     * identity.
     */
    private PartitionState expectOutstandingBatch(Reservation[] batch, boolean expectSameRef) {
        if (batch.length == 0) {
            throw new IllegalStateException("batch is empty");
        }
        Reservation first = batch[0];
        Objects.requireNonNull(first, "reservation[0]");
        PartitionState s = states.get(new Key(first.logicalTopic(), first.logicalPartition()));
        if (s == null || s.outstandingBatch == null) {
            throw new IllegalStateException(
                "no outstanding reservation for ("
                    + first.logicalTopic() + "," + first.logicalPartition() + ")");
        }
        if (expectSameRef) {
            if (s.outstandingBatch != batch) {
                throw new IllegalStateException(
                    "batch is not the outstanding one for ("
                        + first.logicalTopic() + "," + first.logicalPartition() + ")");
            }
        } else {
            // Legacy single-record path: outstanding must be exactly the same one-element batch
            // *by Reservation identity*, since we just wrapped it.
            if (s.outstandingBatch.length != 1 || s.outstandingBatch[0] != first) {
                throw new IllegalStateException(
                    "reservation is not the outstanding one for ("
                        + first.logicalTopic() + "," + first.logicalPartition() + ")");
            }
        }
        for (Reservation r : batch) {
            if (r.state() != Reservation.State.OPEN) {
                throw new IllegalStateException(
                    "reservation already resolved for ("
                        + first.logicalTopic() + "," + first.logicalPartition() + ")");
            }
        }
        return s;
    }
}
