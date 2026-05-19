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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behavioural contract of {@link LogicalOffsetTracker}.
 *
 * <p>The tracker assigns per-(logicalTopic, logicalPartition) monotonic offsets via a
 * reserve/commit/rollback protocol. It is the v1 source of truth for "what is the next logical
 * offset on this partition?" — the broker consults it at produce-time. The PROMPT's acceptance
 * criterion is: a rolled-back reservation must NOT leave a gap in the committed offset
 * sequence. The simplest correct way to honour that without cascading-rollback machinery (a
 * stretch goal) is to serialise reservations on the same partition: only one in flight at a
 * time.
 */
public class LogicalOffsetTrackerTest {

    @Test
    public void firstReservationAssignsOffsetZero() {
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        Reservation r = tracker.reserve("orders", 7);
        assertEquals(0L, r.logicalOffset());
    }

    @Test
    public void committedReservationsYieldContiguousOffsets() {
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        for (long expected = 0; expected < 10; expected++) {
            Reservation r = tracker.reserve("orders", 7);
            assertEquals(expected, r.logicalOffset());
            tracker.commit(r);
        }
        assertEquals(10L, tracker.nextLogicalOffset("orders", 7));
    }

    @Test
    public void rolledBackReservationDoesNotLeaveGap() {
        // PROMPT acceptance criterion: "Logical-offset reservation is rollbackable: if a
        // physical append fails, the reservation is not committed, and subsequent appends do
        // not leave offset gaps."
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        tracker.commit(tracker.reserve("orders", 7));      // -> 0
        tracker.commit(tracker.reserve("orders", 7));      // -> 1
        tracker.rollback(tracker.reserve("orders", 7));    // would-be 2, rolled back
        Reservation r = tracker.reserve("orders", 7);
        assertEquals(2L, r.logicalOffset(),
            "next reservation must reuse the offset of the rolled-back one");
        tracker.commit(r);
        assertEquals(3L, tracker.nextLogicalOffset("orders", 7));
    }

    @Test
    public void differentLogicalPartitionsAreIndependent() {
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        Reservation rA = tracker.reserve("orders", 1);
        Reservation rB = tracker.reserve("orders", 2);
        assertEquals(0L, rA.logicalOffset());
        assertEquals(0L, rB.logicalOffset(),
            "a reservation on partition 2 must not be affected by an outstanding reservation on partition 1");
    }

    @Test
    public void differentLogicalTopicsAreIndependent() {
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        tracker.commit(tracker.reserve("topicA", 0));   // A: 0
        tracker.commit(tracker.reserve("topicA", 0));   // A: 1
        Reservation rB = tracker.reserve("topicB", 0);
        assertEquals(0L, rB.logicalOffset(),
            "topicB's offsets must not be influenced by topicA's");
    }

    @Test
    public void initialOffsetsAreZero() {
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        assertEquals(0L, tracker.nextLogicalOffset("orders", 0));
        assertEquals(0L, tracker.startOffset("orders", 0));
    }

    @Test
    public void advanceStartOffsetMovesLowWaterButNotHighWater() {
        // PROMPT acceptance criterion: "DeleteRecords on a logical topic advances only that
        // logical partition's start offset."
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        for (int i = 0; i < 100; i++) tracker.commit(tracker.reserve("orders", 0));
        tracker.advanceStartOffset("orders", 0, 50);
        assertEquals(50L, tracker.startOffset("orders", 0));
        assertEquals(100L, tracker.nextLogicalOffset("orders", 0),
            "advancing the start offset must not change the next-to-be-assigned offset");
    }

    @Test
    public void advanceStartOffsetRejectsRegression() {
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        for (int i = 0; i < 10; i++) tracker.commit(tracker.reserve("orders", 0));
        tracker.advanceStartOffset("orders", 0, 5);
        assertThrows(IllegalArgumentException.class,
            () -> tracker.advanceStartOffset("orders", 0, 4),
            "start offset must be monotonic non-decreasing");
    }

    @Test
    public void advanceStartOffsetRejectsBeyondNext() {
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        for (int i = 0; i < 10; i++) tracker.commit(tracker.reserve("orders", 0));
        assertThrows(IllegalArgumentException.class,
            () -> tracker.advanceStartOffset("orders", 0, 11),
            "start offset must not exceed the next-to-be-assigned offset");
    }

    @Test
    public void commitOfUnknownReservationIsRejected() {
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        Reservation r = tracker.reserve("orders", 0);
        tracker.commit(r);
        // committing the same reservation twice is a programming error
        assertThrows(IllegalStateException.class, () -> tracker.commit(r));
    }

    @Test
    public void rollbackOfCommittedReservationIsRejected() {
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        Reservation r = tracker.reserve("orders", 0);
        tracker.commit(r);
        assertThrows(IllegalStateException.class, () -> tracker.rollback(r));
    }

    @Test
    public void reservationOnSamePartitionBlocksUntilOutstandingIsResolved() throws Exception {
        // v1 serialises reservations on the same partition to keep offsets contiguous under
        // rollback without needing the stretch "cascade rollback" machinery.
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        Reservation first = tracker.reserve("orders", 0);

        CountDownLatch secondStarted = new CountDownLatch(1);
        ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            Future<Reservation> secondFuture = exec.submit(() -> {
                secondStarted.countDown();
                return tracker.reserve("orders", 0);
            });
            assertTrue(secondStarted.await(1, TimeUnit.SECONDS));
            // give the worker a chance to actually call reserve()
            Thread.sleep(50);
            assertFalse(secondFuture.isDone(), "second reservation must block on first");
            tracker.commit(first);
            Reservation second = secondFuture.get(1, TimeUnit.SECONDS);
            assertEquals(1L, second.logicalOffset());
        } finally {
            exec.shutdownNow();
        }
    }

    @Test
    public void concurrentReservationsAcrossPartitionsRunInParallel() throws Exception {
        // Different partitions must not serialise — that would kill throughput. With 16 worker
        // threads each pinned to its own logical partition and tightly looping reserve+commit,
        // all 16 should make progress before any of them finishes its full quota.
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        final int partitions = 16;
        final int perPartition = 200;
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();

        ExecutorService exec = Executors.newFixedThreadPool(partitions);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int p = 0; p < partitions; p++) {
                final int partition = p;
                futures.add(exec.submit(() -> {
                    for (int i = 0; i < perPartition; i++) {
                        int now = inFlight.incrementAndGet();
                        peak.updateAndGet(prev -> Math.max(prev, now));
                        Reservation r = tracker.reserve("orders", partition);
                        tracker.commit(r);
                        inFlight.decrementAndGet();
                    }
                }));
            }
            for (Future<?> f : futures) f.get(30, TimeUnit.SECONDS);
        } finally {
            exec.shutdownNow();
        }
        for (int p = 0; p < partitions; p++) {
            assertEquals(perPartition, tracker.nextLogicalOffset("orders", p));
        }
        assertTrue(peak.get() >= 2, "at least two partition reservations should overlap in time");
    }

    @Test
    public void rejectsNullLogicalTopic() {
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        assertThrows(NullPointerException.class, () -> tracker.reserve(null, 0));
        assertThrows(NullPointerException.class, () -> tracker.nextLogicalOffset(null, 0));
    }

    @Test
    public void removePartitionDropsStateAndAllowsFreshStart() {
        // Pins the unbounded-growth audit fix: removing a partition wipes the state entry so the
        // tracker map shrinks. After removal, reserving on the same key starts at offset 0 again
        // (the tracker has no memory of past commits — that is the intended semantics for
        // partition deletion).
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        for (int i = 0; i < 5; i++) tracker.commit(tracker.reserve("orders", 0));
        assertEquals(5L, tracker.nextLogicalOffset("orders", 0));
        assertTrue(tracker.removePartition("orders", 0));
        assertEquals(0L, tracker.nextLogicalOffset("orders", 0),
            "after removal, the partition reads as never-seen");
        // Second remove on the same key is a no-op (idempotent).
        assertFalse(tracker.removePartition("orders", 0));
        // Fresh reservations start at 0.
        assertEquals(0L, tracker.reserve("orders", 0).logicalOffset());
    }

    @Test
    public void removePartitionRefusesWhileReservationOutstanding() {
        // If a produce is mid-flight, dropping the state would lose the reservation lock and
        // could let the partition's next-offset regress on the next reserve. Refuse loudly.
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        Reservation r = tracker.reserve("orders", 0);
        assertThrows(IllegalStateException.class,
            () -> tracker.removePartition("orders", 0));
        // Once committed, removal is allowed again.
        tracker.commit(r);
        assertTrue(tracker.removePartition("orders", 0));
    }

    @Test
    public void removePartitionDoesNotTouchSiblings() {
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        for (int i = 0; i < 3; i++) tracker.commit(tracker.reserve("orders", 0));
        for (int i = 0; i < 7; i++) tracker.commit(tracker.reserve("orders", 1));
        tracker.removePartition("orders", 0);
        assertEquals(0L, tracker.nextLogicalOffset("orders", 0));
        assertEquals(7L, tracker.nextLogicalOffset("orders", 1),
            "sibling partition state must survive removal of a neighbour");
    }

    @Test
    public void reservationCarriesItsOwnLogicalTopicAndPartition() {
        // The Reservation will be passed across to the sidecar-index writer; it must carry
        // enough identity to address the right index file.
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        Reservation r = tracker.reserve("orders", 9);
        assertEquals("orders", r.logicalTopic());
        assertEquals(9, r.logicalPartition());
        tracker.rollback(r);
    }

    // ---- Batch reservation API (used by broker produce hook #2 for K-record produce batches) ----

    @Test
    public void reserveBatchAssignsContiguousOffsetsFromZero() {
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        Reservation[] batch = tracker.reserveBatch("orders", 0, 5);
        assertEquals(5, batch.length);
        for (int i = 0; i < 5; i++) {
            assertEquals(i, batch[i].logicalOffset(),
                "batch member " + i + " must hold logical offset " + i);
            assertEquals("orders", batch[i].logicalTopic());
            assertEquals(0, batch[i].logicalPartition());
        }
        tracker.commitBatch(batch);
        assertEquals(5L, tracker.nextLogicalOffset("orders", 0));
    }

    @Test
    public void commitBatchAdvancesNextOffsetByBatchSize() {
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        tracker.commit(tracker.reserve("orders", 0));            // offset 0
        tracker.commitBatch(tracker.reserveBatch("orders", 0, 4)); // offsets 1..4
        assertEquals(5L, tracker.nextLogicalOffset("orders", 0),
            "after a 1-record commit + 4-record batch commit, next must be 5");
    }

    @Test
    public void rollbackBatchDoesNotConsumeOffsets() {
        // PROMPT acceptance criterion: a failed produce must not leave a gap. A rolled-back
        // batch must release every offset it reserved so the next reserve picks them up.
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        tracker.commit(tracker.reserve("orders", 0));    // offset 0
        Reservation[] batch = tracker.reserveBatch("orders", 0, 3); // would-be 1, 2, 3
        assertEquals(1L, batch[0].logicalOffset());
        assertEquals(3L, batch[2].logicalOffset());
        tracker.rollbackBatch(batch);
        // Next reserve picks up at the rolled-back range.
        Reservation retry = tracker.reserve("orders", 0);
        assertEquals(1L, retry.logicalOffset(),
            "rolled-back batch offsets must be reused — no gap");
        tracker.commit(retry);
        assertEquals(2L, tracker.nextLogicalOffset("orders", 0));
    }

    @Test
    public void reserveBatchOfOneEqualsSingleReserve() {
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        Reservation[] batch = tracker.reserveBatch("orders", 0, 1);
        assertEquals(1, batch.length);
        assertEquals(0L, batch[0].logicalOffset());
        // Single-record commit must also work via the legacy commit() API for a batch-of-one.
        tracker.commitBatch(batch);
        assertEquals(1L, tracker.nextLogicalOffset("orders", 0));
    }

    @Test
    public void reserveBatchWithNonPositiveCountThrows() {
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        assertThrows(IllegalArgumentException.class,
            () -> tracker.reserveBatch("orders", 0, 0));
        assertThrows(IllegalArgumentException.class,
            () -> tracker.reserveBatch("orders", 0, -1));
    }

    @Test
    public void reserveBatchReleasesLockOnAllocationThrow() throws Exception {
        // BLOCKER #179: the per-partition lock is acquired BEFORE the array allocation. A
        // hostile or pathological {@code count} (Integer.MAX_VALUE) triggers OutOfMemoryError
        // inside {@code new Reservation[count]} — before fix, that leaked the lock forever and
        // every subsequent reserve on the partition would block. Now the lock must be released
        // on any failure path so the partition is reusable.
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        // First, prime the partition state so the lock object exists.
        Reservation[] primer = tracker.reserveBatch("orders", 0, 1);
        tracker.commitBatch(primer);

        // Force allocation failure: Integer.MAX_VALUE reservations would need ~16 GB of heap
        // refs. JVM throws OutOfMemoryError before the for-loop runs.
        assertThrows(OutOfMemoryError.class,
            () -> tracker.reserveBatch("orders", 0, Integer.MAX_VALUE));

        // Lock must have been released. A subsequent reserve+commit from a SEPARATE thread must
        // succeed within a reasonable bound — if the lock leaked, the reserve would block
        // forever. ReentrantLock is per-thread, so the same worker thread must perform both
        // reserve and commit to round-trip the lock cleanly.
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<long[]> f = pool.submit(() -> {
                Reservation[] retry = tracker.reserveBatch("orders", 0, 2);
                long[] offsets = new long[] {retry[0].logicalOffset(), retry[1].logicalOffset()};
                tracker.commitBatch(retry);
                return offsets;
            });
            long[] offsets = f.get(2, TimeUnit.SECONDS);
            assertEquals(1L, offsets[0], "first reservation after failed batch reuses the next slot");
            assertEquals(2L, offsets[1]);
            assertEquals(3L, tracker.nextLogicalOffset("orders", 0));
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void commitBatchOnForeignArrayIsRejected() {
        // The tracker requires the SAME array reference returned by reserveBatch — a forged
        // copy with the same contents must not be accepted as the outstanding batch.
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        Reservation[] batch = tracker.reserveBatch("orders", 0, 3);
        Reservation[] copy = batch.clone();
        assertThrows(IllegalStateException.class, () -> tracker.commitBatch(copy));
        // Recovery: original batch reference still works.
        tracker.commitBatch(batch);
        assertEquals(3L, tracker.nextLogicalOffset("orders", 0));
    }

    @Test
    public void rollbackBatchOnForeignArrayIsRejected() {
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        Reservation[] batch = tracker.reserveBatch("orders", 0, 2);
        Reservation[] copy = batch.clone();
        assertThrows(IllegalStateException.class, () -> tracker.rollbackBatch(copy));
        tracker.rollbackBatch(batch);
        assertEquals(0L, tracker.nextLogicalOffset("orders", 0));
    }

    @Test
    public void reserveBatchSerialisesOnTheSamePartition() throws Exception {
        // Two threads racing reserveBatch on the same partition must serialise — never assign
        // overlapping logical-offset ranges. Pin this directly because hook #2 will rely on
        // serialised reservations to maintain "no gaps".
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        int batchesPerThread = 100;
        int threads = 4;
        int batchSize = 5;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<List<Long>>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    List<Long> seen = new ArrayList<>();
                    for (int b = 0; b < batchesPerThread; b++) {
                        Reservation[] batch = tracker.reserveBatch("orders", 0, batchSize);
                        for (Reservation r : batch) seen.add(r.logicalOffset());
                        tracker.commitBatch(batch);
                    }
                    return seen;
                }));
            }
            start.countDown();
            // Collect every offset reserved across every thread and assert the union covers
            // [0, threads * batchesPerThread * batchSize) with no duplicates.
            List<Long> all = new ArrayList<>();
            for (Future<List<Long>> f : futures) all.addAll(f.get(30, TimeUnit.SECONDS));
            assertEquals(threads * batchesPerThread * batchSize, all.size());
            all.sort(Long::compareTo);
            for (int i = 0; i < all.size(); i++) {
                assertEquals((long) i, all.get(i).longValue(),
                    "offset " + i + " must appear exactly once in the union");
            }
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void doubleCommitOfTheSameBatchIsRejected() {
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        Reservation[] batch = tracker.reserveBatch("orders", 0, 2);
        tracker.commitBatch(batch);
        assertThrows(IllegalStateException.class, () -> tracker.commitBatch(batch));
    }

    @Test
    public void restorePartitionMustWaitForOutstandingReservationLock() throws Exception {
        // r22 BLOCKER #193 regression discriminator. The pre-fix restorePartition skipped the
        // per-partition lock, so it could mutate startOffset/nextOffset concurrently with a
        // commitBatchInternal or advanceStartOffset on the same partition — silent torn writes
        // since the volatile-pair is not atomic. With the fix, restorePartition must serialise on
        // the same lock the reservation holds: a thread calling restorePartition while another
        // thread holds the lock (via reserveBatch) must BLOCK until the lock-holder commits or
        // rolls back. Pre-fix, restorePartition would return immediately, leaving the asserted
        // post-state unreliable.
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        Reservation[] batch = tracker.reserveBatch("orders", 0, 3);

        AtomicInteger restoreCompleted = new AtomicInteger(0);
        Thread restorer = new Thread(() -> {
            tracker.restorePartition("orders", 0, 0L, 999L);
            restoreCompleted.incrementAndGet();
        }, "restorer");
        restorer.start();
        // Give the restorer a chance to run. If it ignored the lock (pre-fix behaviour) it would
        // complete here; with the fix it must wait on s.lock that we still hold via reserveBatch.
        Thread.sleep(200);
        assertEquals(0, restoreCompleted.get(),
            "restorePartition must NOT complete while a reservation holds the partition lock");

        // Release the lock by committing the batch. restorePartition now wins the lock and
        // overwrites our committed nextOffset of 3 → 999.
        tracker.commitBatch(batch);
        restorer.join(2_000);
        assertEquals(1, restoreCompleted.get(),
            "restorePartition must complete once the reservation releases the lock");
        assertEquals(999L, tracker.nextLogicalOffset("orders", 0),
            "post-restore nextOffset must reflect the restored value (last-writer-wins under lock)");
    }
}
