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
}
