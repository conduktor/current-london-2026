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
 * reservation per partition at a time — which is enough to honour the acceptance criterion
 * "subsequent appends do not leave offset gaps" without the stretch cascade-rollback machinery.
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
        Reservation outstanding = null;
    }

    private final ConcurrentHashMap<Key, PartitionState> states = new ConcurrentHashMap<>();

    private PartitionState stateFor(String logicalTopic, int logicalPartition) {
        Objects.requireNonNull(logicalTopic, "logicalTopic");
        return states.computeIfAbsent(new Key(logicalTopic, logicalPartition), k -> new PartitionState());
    }

    public Reservation reserve(String logicalTopic, int logicalPartition) {
        PartitionState s = stateFor(logicalTopic, logicalPartition);
        s.lock.lock();
        Reservation r = new Reservation(logicalTopic, logicalPartition, s.nextOffset);
        s.outstanding = r;
        return r;
    }

    public void commit(Reservation reservation) {
        PartitionState s = expectOutstanding(reservation);
        s.nextOffset = reservation.logicalOffset() + 1;
        reservation.markCommitted();
        s.outstanding = null;
        s.lock.unlock();
    }

    public void rollback(Reservation reservation) {
        PartitionState s = expectOutstanding(reservation);
        reservation.markRolledBack();
        s.outstanding = null;
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
        // is held by another thread, the reservation it holds is the outstanding one; we will
        // see it under our lock and refuse rather than silently dropping live state.
        s.lock.lock();
        try {
            if (s.outstanding != null) {
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
     * Direct seeding of partition state, used during recovery from a sidecar or backing-log
     * scan. The reservation lock is not taken because recovery is single-threaded by contract.
     */
    void restorePartition(String logicalTopic, int logicalPartition, long startOffset, long nextOffset) {
        if (startOffset < 0 || nextOffset < startOffset) {
            throw new IllegalArgumentException(
                "invalid restore: startOffset=" + startOffset + ", nextOffset=" + nextOffset);
        }
        PartitionState s = stateFor(logicalTopic, logicalPartition);
        s.startOffset = startOffset;
        s.nextOffset = nextOffset;
    }

    private PartitionState expectOutstanding(Reservation reservation) {
        Objects.requireNonNull(reservation, "reservation");
        PartitionState s = states.get(new Key(reservation.logicalTopic(), reservation.logicalPartition()));
        if (s == null || s.outstanding != reservation || reservation.state() != Reservation.State.OPEN) {
            throw new IllegalStateException(
                "reservation is not the outstanding one for ("
                    + reservation.logicalTopic() + "," + reservation.logicalPartition() + ")");
        }
        return s;
    }
}
