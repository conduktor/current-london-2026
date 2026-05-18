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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behavioural contract of {@link ConcentrationKernel}. The kernel is the single object the broker
 * glue interacts with — it composes {@link LogicalTopicRegistry}, {@link LogicalOffsetTracker},
 * {@link BackingScanRecoverer}, and per-partition {@link LogicalSidecarIndex} files into one
 * lifecycle-managed surface.
 *
 * <p>These tests focus on the composition seams — declaration → routing → produce reserve/commit/
 * rollback → fetch translation → DeleteRecords → close lifecycle — rather than re-asserting the
 * behaviours already pinned by the per-component tests.
 */
public class ConcentrationKernelTest {

    private File sidecarDir;
    private ConcentrationKernel kernel;

    @BeforeEach
    public void setup() throws IOException {
        sidecarDir = TestUtils.tempDirectory();
        kernel = new ConcentrationKernel(sidecarDir);
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (kernel != null) kernel.close();
    }

    private LogicalTopicDescriptor descriptor(String name, int n, String backing, int m) {
        return new LogicalTopicDescriptor(name, n, backing, m);
    }

    @Test
    public void declareMakesTopicVisible() {
        kernel.declare(descriptor("orders", 100, "shared", 4));
        assertTrue(kernel.describe("orders").isPresent());
        assertEquals(100, kernel.describe("orders").orElseThrow().numLogicalPartitions());
        assertTrue(kernel.isBackingTopic("shared"));
        assertFalse(kernel.isBackingTopic("orders"));
        assertFalse(kernel.describe("unknown").isPresent());
    }

    @Test
    public void isLogicalTopicTrueForDeclaredAndFalseForBackingAndUnknown() {
        kernel.declare(descriptor("orders", 100, "shared", 4));
        // Declared logical topic — true. Reused by every broker hot path (produce, fetch,
        // DeleteRecords) to decide whether to route through the kernel.
        assertTrue(kernel.isLogicalTopic("orders"));
        // Backing physical topic — false. The kernel keeps logical and backing namespaces
        // disjoint at declaration time; isLogicalTopic must respect that.
        assertFalse(kernel.isLogicalTopic("shared"));
        // Never-seen name — false.
        assertFalse(kernel.isLogicalTopic("never-declared"));
    }

    @Test
    public void backingPartitionForUsesModuloMapping() {
        kernel.declare(descriptor("orders", 100, "shared", 4));
        assertEquals(0, kernel.backingPartitionFor("orders", 0));
        assertEquals(1, kernel.backingPartitionFor("orders", 1));
        assertEquals(3, kernel.backingPartitionFor("orders", 3));
        assertEquals(0, kernel.backingPartitionFor("orders", 4));
        assertEquals(3, kernel.backingPartitionFor("orders", 99));
    }

    @Test
    public void backingPartitionForUnknownLogicalTopicThrows() {
        assertThrows(NoSuchElementException.class, () -> kernel.backingPartitionFor("ghost", 0));
    }

    @Test
    public void reserveAssignsContiguousOffsetsAcrossCommits() throws IOException {
        kernel.declare(descriptor("orders", 4, "shared", 1));
        AtomicLong backing = new AtomicLong(0);
        for (long expected = 0; expected < 10; expected++) {
            Reservation r = kernel.reserveProduce("orders", 0);
            assertEquals(expected, r.logicalOffset());
            kernel.commitProduce(r, backing.getAndIncrement());
        }
        assertEquals(10L, kernel.nextLogicalOffset("orders", 0));
    }

    @Test
    public void rollbackDoesNotLeaveOffsetGap() throws IOException {
        kernel.declare(descriptor("orders", 4, "shared", 1));
        AtomicLong backing = new AtomicLong(0);
        kernel.commitProduce(kernel.reserveProduce("orders", 0), backing.getAndIncrement());
        kernel.commitProduce(kernel.reserveProduce("orders", 0), backing.getAndIncrement());
        kernel.rollbackProduce(kernel.reserveProduce("orders", 0)); // would-be 2 — rolled back
        Reservation r = kernel.reserveProduce("orders", 0);
        assertEquals(2L, r.logicalOffset(),
            "the rolled-back offset must be reused — no gap");
        kernel.commitProduce(r, backing.getAndIncrement());
        assertEquals(3L, kernel.nextLogicalOffset("orders", 0));
    }

    @Test
    public void commitPersistsSidecarMappingForFetchPath() throws IOException {
        kernel.declare(descriptor("orders", 4, "shared", 1));
        kernel.commitProduce(kernel.reserveProduce("orders", 0), 100L);
        kernel.commitProduce(kernel.reserveProduce("orders", 0), 250L);
        kernel.commitProduce(kernel.reserveProduce("orders", 0), 999L);

        assertEquals(100L, kernel.resolveBackingOffset("orders", 0, 0L));
        assertEquals(250L, kernel.resolveBackingOffset("orders", 0, 1L));
        assertEquals(999L, kernel.resolveBackingOffset("orders", 0, 2L));
    }

    @Test
    public void resolveBackingOffsetOutOfRangeThrows() throws IOException {
        kernel.declare(descriptor("orders", 4, "shared", 1));
        kernel.commitProduce(kernel.reserveProduce("orders", 0), 100L);
        assertThrows(IndexOutOfBoundsException.class,
            () -> kernel.resolveBackingOffset("orders", 0, 1L));
        assertThrows(IndexOutOfBoundsException.class,
            () -> kernel.resolveBackingOffset("orders", 0, -1L));
    }

    @Test
    public void resolveBackingOffsetOnNeverProducedPartitionThrowsOutOfBounds() throws IOException {
        kernel.declare(descriptor("orders", 4, "shared", 1));
        // never produced — sidecar is empty; lookup at any offset is out of [0, 0).
        assertThrows(IndexOutOfBoundsException.class,
            () -> kernel.resolveBackingOffset("orders", 3, 0L));
    }

    @Test
    public void commitOnNonMonotonicBackingOffsetRollsBackReservation() throws IOException {
        // Sidecar enforces backing-offset monotonicity. If the broker passes a non-monotonic
        // value (which would be a caller bug — backing offsets come from the log's monotonic
        // append), the kernel must roll the reservation back so the next reserve reuses the slot
        // rather than leaving a gap.
        kernel.declare(descriptor("orders", 4, "shared", 1));
        kernel.commitProduce(kernel.reserveProduce("orders", 0), 100L);
        Reservation second = kernel.reserveProduce("orders", 0);
        assertEquals(1L, second.logicalOffset());
        assertThrows(IllegalArgumentException.class,
            () -> kernel.commitProduce(second, 50L)); // non-monotonic
        // Slot 1 is free again; next reserve takes it.
        Reservation retry = kernel.reserveProduce("orders", 0);
        assertEquals(1L, retry.logicalOffset(),
            "after commit-failure the slot must be free for re-reservation, no gap");
        kernel.commitProduce(retry, 200L);
        assertEquals(2L, kernel.nextLogicalOffset("orders", 0));
        assertEquals(200L, kernel.resolveBackingOffset("orders", 0, 1L));
    }

    @Test
    public void advanceStartOffsetMovesLowWaterOnlyForOneLogicalPartition() throws IOException {
        kernel.declare(descriptor("orders", 4, "shared", 1));
        AtomicLong backing = new AtomicLong(0);
        for (int i = 0; i < 100; i++) {
            kernel.commitProduce(kernel.reserveProduce("orders", 0), backing.getAndIncrement());
            kernel.commitProduce(kernel.reserveProduce("orders", 1), backing.getAndIncrement());
        }
        kernel.advanceStartOffset("orders", 0, 50L);
        assertEquals(50L, kernel.startLogicalOffset("orders", 0));
        assertEquals(0L, kernel.startLogicalOffset("orders", 1),
            "sibling partition's start must not move");
        assertEquals(100L, kernel.nextLogicalOffset("orders", 0));
    }

    @Test
    public void directProduceToBackingTopicNameIsSignalled() {
        kernel.declare(descriptor("orders", 100, "shared", 4));
        kernel.declare(descriptor("payments", 50, "shared", 4));
        // The kernel's signal — the broker hook inverts this and returns INVALID_TOPIC_EXCEPTION.
        assertTrue(kernel.isBackingTopic("shared"));
        assertFalse(kernel.isBackingTopic("orders"));
        assertFalse(kernel.isBackingTopic("payments"));
        assertFalse(kernel.isBackingTopic("unknown"));
    }

    @Test
    public void closeIsIdempotent() throws IOException {
        kernel.declare(descriptor("orders", 4, "shared", 1));
        kernel.commitProduce(kernel.reserveProduce("orders", 0), 0L);
        kernel.close();
        kernel.close(); // double close is a no-op
        kernel = null; // skip tearDown's close
    }

    @Test
    public void reservePartitionMustBelongToDeclaredLogicalTopic() {
        kernel.declare(descriptor("orders", 4, "shared", 1));
        assertThrows(IllegalArgumentException.class,
            () -> kernel.reserveProduce("orders", 4));   // N=4 → valid range [0,3]
        assertThrows(IllegalArgumentException.class,
            () -> kernel.reserveProduce("orders", -1));
        assertThrows(NoSuchElementException.class,
            () -> kernel.reserveProduce("ghost", 0));
    }

    @Test
    public void afterCloseFurtherOperationsAreRejected() throws IOException {
        kernel.declare(descriptor("orders", 4, "shared", 1));
        // Open a sidecar before close so we exercise both the cached-sidecar path and the
        // first-open path post-close.
        kernel.commitProduce(kernel.reserveProduce("orders", 0), 100L);
        kernel.close();

        // Every entry point that interacts with sidecars or the tracker must reject post-close.
        // This pins the audit-fix invariant: no operation can sneak a sidecar open past close().
        assertThrows(IllegalStateException.class,
            () -> kernel.reserveProduce("orders", 0));
        assertThrows(IllegalStateException.class,
            () -> kernel.resolveBackingOffset("orders", 0, 0L));
        assertThrows(IllegalStateException.class,
            () -> kernel.resolveBackingOffset("orders", 3, 0L));  // a never-opened partition
        assertThrows(IllegalStateException.class,
            () -> kernel.advanceStartOffset("orders", 0, 0L));
        assertThrows(IllegalStateException.class,
            () -> kernel.declare(descriptor("payments", 4, "other", 1)));
        kernel = null;
    }

    @Test
    public void descriptorsForReturnsImmutableSnapshot() {
        kernel.declare(descriptor("orders", 100, "shared", 4));
        kernel.declare(descriptor("payments", 50, "shared", 4));
        List<LogicalTopicDescriptor> snapshot = kernel.descriptorsFor("shared");
        assertEquals(2, snapshot.size());
        // Mutation must throw — the broker can iterate without defensive copying.
        assertThrows(UnsupportedOperationException.class,
            () -> snapshot.add(descriptor("more", 10, "shared", 4)));
        // A later declare must not appear in the captured snapshot.
        kernel.declare(descriptor("returns", 25, "shared", 4));
        assertEquals(2, snapshot.size(),
            "snapshot must be stable against subsequent declare() calls");
        assertEquals(3, kernel.descriptorsFor("shared").size(),
            "a fresh call returns the up-to-date count");
    }

    @Test
    public void recoveryFromSidecarsRebuildsTrackerStateAfterRestart() throws IOException {
        kernel.declare(descriptor("orders", 4, "shared", 1));
        for (long b = 0; b < 7; b++) kernel.commitProduce(kernel.reserveProduce("orders", 0), b);
        for (long b = 7; b < 10; b++) kernel.commitProduce(kernel.reserveProduce("orders", 1), b);
        kernel.close();

        ConcentrationKernel rebuilt = new ConcentrationKernel(sidecarDir);
        try {
            rebuilt.declare(descriptor("orders", 4, "shared", 1));
            rebuilt.recoverFromSidecars(List.of(
                new LogicalPartition("orders", 0),
                new LogicalPartition("orders", 1)
            ));
            assertEquals(7L, rebuilt.nextLogicalOffset("orders", 0));
            assertEquals(3L, rebuilt.nextLogicalOffset("orders", 1));
            // Subsequent reserves continue contiguously.
            assertEquals(7L, rebuilt.reserveProduce("orders", 0).logicalOffset());
        } finally {
            rebuilt.close();
        }
        kernel = null;
    }

    @Test
    public void recoveryFromBackingScanReplaysHeadersIntoSidecarsAndTracker() throws IOException {
        kernel.close();
        ConcentrationKernel fresh = new ConcentrationKernel(sidecarDir);
        try {
            fresh.declare(descriptor("topicA", 1, "shared", 1));
            fresh.declare(descriptor("topicB", 1, "shared", 1));
            fresh.recoverFromBackingScan(List.of(
                new RecoveryRecord("topicA", 0, 0L, 0L),
                new RecoveryRecord("topicB", 0, 0L, 1L),
                new RecoveryRecord("topicA", 0, 1L, 2L),
                new RecoveryRecord("topicB", 0, 1L, 3L)
            ).iterator());
            assertEquals(2L, fresh.nextLogicalOffset("topicA", 0));
            assertEquals(2L, fresh.nextLogicalOffset("topicB", 0));
            assertEquals(0L, fresh.resolveBackingOffset("topicA", 0, 0L));
            assertEquals(2L, fresh.resolveBackingOffset("topicA", 0, 1L));
            assertEquals(1L, fresh.resolveBackingOffset("topicB", 0, 0L));
            assertEquals(3L, fresh.resolveBackingOffset("topicB", 0, 1L));
        } finally {
            fresh.close();
        }
        kernel = null;
    }

    @Test
    public void recoverFromDiskSeedsTrackerForEveryPartitionWithASidecar() throws IOException {
        // PROMPT acceptance criterion: "Broker restart with intact durable index → sub-second
        // offset-tracker rebuild from the sidecar file." This pins the broker-startup wiring:
        // after declarations are made, a single recoverFromDisk() call seeds the tracker for
        // every partition whose sidecar file is intact on disk, with no further input from the
        // broker. Mirrors what BrokerServer.startup() will invoke.
        kernel.declare(descriptor("orders", 4, "shared", 1));
        kernel.declare(descriptor("events", 4, "shared", 1));
        // Produce a few records to "orders" partitions 0 and 2, and one to "events" partition 1.
        for (long b = 0; b < 5; b++) kernel.commitProduce(kernel.reserveProduce("orders", 0), b);
        for (long b = 5; b < 8; b++) kernel.commitProduce(kernel.reserveProduce("orders", 2), b);
        kernel.commitProduce(kernel.reserveProduce("events", 1), 100L);
        kernel.close();

        // Rebuild from disk: declarations are made again (mirrors broker startup re-reading
        // config), then a single recoverFromDisk() seeds every partition whose sidecar exists.
        ConcentrationKernel rebuilt = new ConcentrationKernel(sidecarDir);
        try {
            rebuilt.declare(descriptor("orders", 4, "shared", 1));
            rebuilt.declare(descriptor("events", 4, "shared", 1));
            rebuilt.recoverFromDisk();
            assertEquals(5L, rebuilt.nextLogicalOffset("orders", 0));
            assertEquals(0L, rebuilt.nextLogicalOffset("orders", 1), "no sidecar — stays at zero");
            assertEquals(3L, rebuilt.nextLogicalOffset("orders", 2));
            assertEquals(0L, rebuilt.nextLogicalOffset("orders", 3), "no sidecar — stays at zero");
            assertEquals(1L, rebuilt.nextLogicalOffset("events", 1));
            // Next reserve on a recovered partition continues contiguously from its end.
            assertEquals(5L, rebuilt.reserveProduce("orders", 0).logicalOffset());
            assertEquals(3L, rebuilt.reserveProduce("orders", 2).logicalOffset());
        } finally {
            rebuilt.close();
        }
        kernel = null;
    }

    @Test
    public void recoverFromDiskSkipsSidecarsForStalePartitionsOutsideDeclaredRange() throws IOException {
        // Defensive: if a previous incarnation of the topic was declared with a larger N (say
        // 100) and partition 80 had a sidecar, but the topic is now redeclared with N=4, the
        // stale partition-80 sidecar must NOT be loaded — it would be invalid against the
        // current descriptor's partition-range and would surprise the tracker. Skip silently
        // so recovery succeeds; the operator can clean the stale file up later.
        kernel.declare(descriptor("orders", 100, "shared", 1));
        kernel.commitProduce(kernel.reserveProduce("orders", 80), 0L);
        kernel.close();

        ConcentrationKernel rebuilt = new ConcentrationKernel(sidecarDir);
        try {
            rebuilt.declare(descriptor("orders", 4, "shared", 1)); // smaller N — partition 80 stale
            rebuilt.recoverFromDisk(); // must NOT throw
            for (int p = 0; p < 4; p++) {
                assertEquals(0L, rebuilt.nextLogicalOffset("orders", p),
                    "no in-range partition had a sidecar; all stay at zero");
            }
        } finally {
            rebuilt.close();
        }
        kernel = null;
    }

    @Test
    public void recoverFromDiskOnEmptyDirIsANoOp() throws IOException {
        // First-boot scenario: no sidecars on disk yet. recoverFromDisk() must succeed without
        // touching the tracker.
        kernel.declare(descriptor("orders", 4, "shared", 1));
        kernel.recoverFromDisk(); // no sidecars, no state change
        assertEquals(0L, kernel.nextLogicalOffset("orders", 0));
        assertEquals(0L, kernel.reserveProduce("orders", 0).logicalOffset(),
            "no recovery state — next reserve starts at 0");
    }

    @Test
    public void recoverFromDiskRejectsAfterClose() throws IOException {
        kernel.declare(descriptor("orders", 4, "shared", 1));
        kernel.close();
        assertThrows(IllegalStateException.class, kernel::recoverFromDisk);
        kernel = null;
    }

    @Test
    public void removeLogicalPartitionClosesSidecarAndDeletesFileAndTrackerState() throws IOException {
        // Pins the unbounded-growth audit fix at the broker-facing surface: removeLogicalPartition
        // must close the cached sidecar handle, delete the on-disk file, and drop the tracker
        // state so a subsequent reserve starts fresh at offset 0.
        kernel.declare(descriptor("orders", 4, "shared", 1));
        kernel.commitProduce(kernel.reserveProduce("orders", 0), 100L);
        kernel.commitProduce(kernel.reserveProduce("orders", 0), 200L);
        assertEquals(2L, kernel.nextLogicalOffset("orders", 0));

        // The sidecar file lives under <sidecarDir>/<logicalTopic>/<partition>.sidecar.
        File sidecarFile = new File(new File(sidecarDir, "orders"), "0.sidecar");
        assertTrue(sidecarFile.exists(), "sidecar file should have been created by produce");

        assertTrue(kernel.removeLogicalPartition("orders", 0));
        assertFalse(sidecarFile.exists(), "sidecar file must be deleted");
        assertEquals(0L, kernel.nextLogicalOffset("orders", 0),
            "tracker state must be wiped — partition reads as never-seen");

        // Second remove is a no-op.
        assertFalse(kernel.removeLogicalPartition("orders", 0));

        // After removal, the partition can be re-produced to and the on-disk file reappears.
        kernel.commitProduce(kernel.reserveProduce("orders", 0), 999L);
        assertEquals(999L, kernel.resolveBackingOffset("orders", 0, 0L));
        assertTrue(sidecarFile.exists());
    }

    @Test
    public void removeLogicalPartitionRefusesWhenReservationInFlight() throws IOException {
        kernel.declare(descriptor("orders", 4, "shared", 1));
        Reservation r = kernel.reserveProduce("orders", 0);
        // Sidecar was opened by reserveProduce indirectly? No — reserveProduce only touches the
        // tracker. But sidecarFile may or may not exist; what matters is the tracker lock is
        // held, so the remove must throw.
        assertThrows(IllegalStateException.class,
            () -> kernel.removeLogicalPartition("orders", 0));
        // After committing, removal proceeds normally.
        kernel.commitProduce(r, 50L);
        assertTrue(kernel.removeLogicalPartition("orders", 0));
    }

    @Test
    public void removeLogicalPartitionRejectsAfterClose() throws IOException {
        kernel.declare(descriptor("orders", 4, "shared", 1));
        kernel.commitProduce(kernel.reserveProduce("orders", 0), 0L);
        kernel.close();
        assertThrows(IllegalStateException.class,
            () -> kernel.removeLogicalPartition("orders", 0));
        kernel = null;
    }

    @Test
    public void reservationIdentityIsPreservedAcrossCommit() throws IOException {
        kernel.declare(descriptor("orders", 4, "shared", 1));
        Reservation r = kernel.reserveProduce("orders", 0);
        assertNotNull(r);
        assertEquals("orders", r.logicalTopic());
        assertEquals(0, r.logicalPartition());
        kernel.commitProduce(r, 100L);
    }

    // ---- Batch produce API (used by broker hook #2 for stock multi-record produce batches) ----

    @Test
    public void commitProduceBatchAppendsSidecarEntriesInOrderAndAdvancesTracker() throws IOException {
        // The hot path: stock producer sends a K-record batch, broker reserves K logical offsets,
        // backing append returns first backing offset B, broker commits the batch with
        // [B, B+1, ..., B+K-1] persisted as sidecar entries against logical offsets [0..K-1].
        kernel.declare(descriptor("orders", 4, "shared", 1));
        Reservation[] batch = kernel.reserveProduceBatch("orders", 0, 5);
        assertEquals(5, batch.length);
        kernel.commitProduceBatch(batch, 1000L);
        assertEquals(5L, kernel.nextLogicalOffset("orders", 0));
        for (int i = 0; i < 5; i++) {
            assertEquals(1000L + i, kernel.resolveBackingOffset("orders", 0, i),
                "logical offset " + i + " must map to backing 1000+" + i);
        }
    }

    @Test
    public void rollbackProduceBatchReleasesAllReservedOffsets() throws IOException {
        // PROMPT acceptance criterion mirror: a failed produce must not leave gaps. The whole
        // K-record range comes back into play after the rollback.
        kernel.declare(descriptor("orders", 4, "shared", 1));
        kernel.commitProduce(kernel.reserveProduce("orders", 0), 500L);  // offset 0
        Reservation[] batch = kernel.reserveProduceBatch("orders", 0, 3); // would-be 1..3
        assertEquals(1L, batch[0].logicalOffset());
        kernel.rollbackProduceBatch(batch);
        // Next reservation reuses the rolled-back range with no gap.
        Reservation retry = kernel.reserveProduce("orders", 0);
        assertEquals(1L, retry.logicalOffset(),
            "rolled-back batch offsets must be reused — no gap");
        kernel.commitProduce(retry, 600L);
        assertEquals(2L, kernel.nextLogicalOffset("orders", 0));
    }

    @Test
    public void reserveProduceBatchRejectsUnknownTopicAndOutOfRangePartition() {
        kernel.declare(descriptor("orders", 4, "shared", 1));
        assertThrows(NoSuchElementException.class,
            () -> kernel.reserveProduceBatch("ghost", 0, 3));
        assertThrows(IllegalArgumentException.class,
            () -> kernel.reserveProduceBatch("orders", 4, 3));   // N=4 → valid range [0,3]
        assertThrows(IllegalArgumentException.class,
            () -> kernel.reserveProduceBatch("orders", -1, 3));
    }

    @Test
    public void commitProduceBatchOnEmptyArrayIsRejected() {
        kernel.declare(descriptor("orders", 4, "shared", 1));
        assertThrows(IllegalArgumentException.class,
            () -> kernel.commitProduceBatch(new Reservation[0], 100L));
    }

    @Test
    public void commitProduceBatchSidecarFailureRollsBackEntireBatch() throws IOException {
        // If sidecar.append throws mid-batch (e.g., non-monotonic backing offset), the whole batch
        // reservation is released so the slots can be reused. Tracker.nextOffset does NOT advance.
        kernel.declare(descriptor("orders", 4, "shared", 1));
        kernel.commitProduce(kernel.reserveProduce("orders", 0), 100L); // sidecar has [100]
        Reservation[] batch = kernel.reserveProduceBatch("orders", 0, 3); // 1..3
        // Pass a firstBackingOffset that goes non-monotonic mid-batch: 200, 201, 99.
        // Actually the LogicalSidecarIndex enforces strict monotonicity on every append, so any
        // backing offset <= the previous one throws. Here we go 200, 201, 202 — monotonic, no
        // failure. We need a different way to force a failure. Easiest is to start the firstBacking
        // below the existing high-water (100). The very first append will throw because 50 < 100.
        assertThrows(RuntimeException.class,
            () -> kernel.commitProduceBatch(batch, 50L));
        // After the failure, no offsets were consumed by the batch — tracker still at 1.
        assertEquals(1L, kernel.nextLogicalOffset("orders", 0),
            "failed commit must roll back the batch so the high-water stays put");
        // And a fresh reservation reuses offset 1 (no gap).
        Reservation retry = kernel.reserveProduce("orders", 0);
        assertEquals(1L, retry.logicalOffset());
        kernel.commitProduce(retry, 200L);
    }

    @Test
    public void reserveProduceBatchAfterCloseIsRejected() throws IOException {
        kernel.declare(descriptor("orders", 4, "shared", 1));
        kernel.close();
        assertThrows(IllegalStateException.class,
            () -> kernel.reserveProduceBatch("orders", 0, 3));
        kernel = null;
    }

    // ---- Idempotent retry cache (v1 fix for PROMPT scenario 6) ----

    // All existing idempotent tests use a fixed test epoch (LEADER_EPOCH = 5) on both record and
    // lookup — leader-epoch behaviour is exercised in its own focused tests further below.
    private static final int LEADER_EPOCH = 5;

    @Test
    public void lookupIdempotentBatchReturnsEmptyBeforeAnyRecord() {
        // Cache miss on a never-seen producerId. The broker reads "empty" as "go down the
        // reserve+stamp+append path" — there's no shortcut to take.
        kernel.declare(descriptor("orders", 4, "shared", 1));
        IdempotentBatchKey key = new IdempotentBatchKey(42L, (short) 0, 0, 2);
        assertTrue(kernel.lookupIdempotentBatch("orders", 0, key, LEADER_EPOCH).isEmpty());
    }

    @Test
    public void recordThenLookupIdempotentBatchReturnsExactResult() {
        // After a successful commit, the broker records the (key, result) pair. A subsequent
        // retry sees a hit and returns the SAME logical offsets — never reserving new ones.
        kernel.declare(descriptor("orders", 4, "shared", 1));
        IdempotentBatchKey key = new IdempotentBatchKey(42L, (short) 0, 0, 2);
        IdempotentBatchResult result = new IdempotentBatchResult(500L, 502L, 0L, 1700000000000L, LEADER_EPOCH);
        kernel.recordIdempotentBatch("orders", 0, key, result);

        IdempotentBatchResult hit = kernel.lookupIdempotentBatch("orders", 0, key, LEADER_EPOCH).orElseThrow();
        assertEquals(500L, hit.logicalBaseOffset());
        assertEquals(502L, hit.logicalLastOffset());
        assertEquals(0L, hit.logStartOffset());
        assertEquals(1700000000000L, hit.logAppendTime());
        assertEquals(LEADER_EPOCH, hit.leaderEpoch());
    }

    @Test
    public void differentEpochIsACacheMiss() {
        // Idempotent epoch bump (Producer.flush + restart of producerId session) MUST reset the
        // dedup window — a producer with a new epoch is functionally a fresh producer and its
        // sequence numbers start over. Caching across epoch would wrongly short-circuit valid
        // appends from the new session.
        kernel.declare(descriptor("orders", 4, "shared", 1));
        IdempotentBatchKey original = new IdempotentBatchKey(42L, (short) 0, 0, 2);
        kernel.recordIdempotentBatch("orders", 0, original,
            new IdempotentBatchResult(500L, 502L, 0L, 1L, LEADER_EPOCH));

        IdempotentBatchKey newEpoch = new IdempotentBatchKey(42L, (short) 1, 0, 2);
        assertTrue(kernel.lookupIdempotentBatch("orders", 0, newEpoch, LEADER_EPOCH).isEmpty(),
            "epoch bump must invalidate the cache hit");
    }

    @Test
    public void differentSequenceRangeIsACacheMiss() {
        // The cache key includes BOTH baseSequence and lastSequence — a partial overlap (e.g.,
        // sequences 0..2 already cached, retry now sending 1..3) is a brand-new batch and must
        // not hit. The backing log treats it as new too.
        kernel.declare(descriptor("orders", 4, "shared", 1));
        kernel.recordIdempotentBatch("orders", 0,
            new IdempotentBatchKey(42L, (short) 0, 0, 2),
            new IdempotentBatchResult(500L, 502L, 0L, 1L, LEADER_EPOCH));

        assertTrue(kernel.lookupIdempotentBatch("orders", 0,
            new IdempotentBatchKey(42L, (short) 0, 1, 3), LEADER_EPOCH).isEmpty(),
            "different sequence range must be a cache miss");
    }

    @Test
    public void cacheCapacityIsCappedAtFivePerProducer() {
        // Matches UnifiedLog.ProducerStateEntry.NUM_BATCHES_TO_RETAIN. Caching deeper than the
        // backing log would risk a false-positive on producer.id wraparound after long idle —
        // we'd hit our cache for a batch the backing has long since forgotten.
        kernel.declare(descriptor("orders", 4, "shared", 1));
        for (int i = 0; i < 6; i++) {
            IdempotentBatchKey key = new IdempotentBatchKey(42L, (short) 0, i, i);
            kernel.recordIdempotentBatch("orders", 0, key,
                new IdempotentBatchResult(500L + i, 500L + i, 0L, 1L, LEADER_EPOCH));
        }
        // Oldest (seq=0) evicted; seq=1..5 retained.
        assertTrue(kernel.lookupIdempotentBatch("orders", 0,
            new IdempotentBatchKey(42L, (short) 0, 0, 0), LEADER_EPOCH).isEmpty(),
            "FIFO eviction must drop the oldest entry once the 6th lands");
        for (int i = 1; i <= 5; i++) {
            assertEquals(500L + i,
                kernel.lookupIdempotentBatch("orders", 0,
                    new IdempotentBatchKey(42L, (short) 0, i, i), LEADER_EPOCH)
                    .orElseThrow().logicalBaseOffset(),
                "seq=" + i + " must still be cached");
        }
    }

    @Test
    public void differentProducerIdsAreIsolated() {
        // Two producers sharing a logical partition must NOT cross-contaminate cache state — a
        // producer.id collision would otherwise resurface stale entries from a long-dead producer.
        kernel.declare(descriptor("orders", 4, "shared", 1));
        kernel.recordIdempotentBatch("orders", 0,
            new IdempotentBatchKey(42L, (short) 0, 0, 0),
            new IdempotentBatchResult(500L, 500L, 0L, 1L, LEADER_EPOCH));
        kernel.recordIdempotentBatch("orders", 0,
            new IdempotentBatchKey(99L, (short) 0, 0, 0),
            new IdempotentBatchResult(501L, 501L, 0L, 1L, LEADER_EPOCH));
        assertEquals(500L, kernel.lookupIdempotentBatch("orders", 0,
            new IdempotentBatchKey(42L, (short) 0, 0, 0), LEADER_EPOCH).orElseThrow().logicalBaseOffset());
        assertEquals(501L, kernel.lookupIdempotentBatch("orders", 0,
            new IdempotentBatchKey(99L, (short) 0, 0, 0), LEADER_EPOCH).orElseThrow().logicalBaseOffset());
    }

    @Test
    public void differentLogicalPartitionsAreIsolated() {
        // A retry against partition 1 must NOT hit the cache populated by partition 0 — the
        // logical-partition scoping is what lets two stock producers share a backing without
        // their idempotent state leaking across the demultiplexing boundary.
        kernel.declare(descriptor("orders", 4, "shared", 1));
        IdempotentBatchKey key = new IdempotentBatchKey(42L, (short) 0, 0, 0);
        kernel.recordIdempotentBatch("orders", 0, key,
            new IdempotentBatchResult(500L, 500L, 0L, 1L, LEADER_EPOCH));
        assertTrue(kernel.lookupIdempotentBatch("orders", 1, key, LEADER_EPOCH).isEmpty(),
            "partition scoping must isolate cache entries");
    }

    @Test
    public void removeLogicalPartitionPurgesIdempotentCache() throws IOException {
        // Without cache cleanup the kernel would leak entries proportional to total producer.ids
        // seen on a partition over the broker's lifetime — even after the partition is gone.
        kernel.declare(descriptor("orders", 4, "shared", 1));
        kernel.commitProduce(kernel.reserveProduce("orders", 0), 100L);
        IdempotentBatchKey key = new IdempotentBatchKey(42L, (short) 0, 0, 0);
        kernel.recordIdempotentBatch("orders", 0, key,
            new IdempotentBatchResult(0L, 0L, 0L, 1L, LEADER_EPOCH));
        assertTrue(kernel.lookupIdempotentBatch("orders", 0, key, LEADER_EPOCH).isPresent());
        assertTrue(kernel.removeLogicalPartition("orders", 0));
        assertTrue(kernel.lookupIdempotentBatch("orders", 0, key, LEADER_EPOCH).isEmpty(),
            "removeLogicalPartition must drop the idempotent cache for that partition");
    }

    @Test
    public void lookupAtNewerLeaderEpochEvictsStaleEntryAndMisses() {
        // Codex HIGH #7: broker was leader at epoch 5 and cached batch X. Leadership flapped
        // away and back; now the broker is leader at epoch 7. A retry of batch X under the new
        // epoch must NOT hit the stale cache entry, because the logical offsets the cache holds
        // belong to a tracker state that the intervening leader may have invalidated. Lookup
        // returns empty AND evicts the stale entry so the second lookup at the new epoch is
        // already O(deque depth) instead of paying the eviction cost again.
        kernel.declare(descriptor("orders", 4, "shared", 1));
        IdempotentBatchKey key = new IdempotentBatchKey(42L, (short) 0, 0, 0);
        kernel.recordIdempotentBatch("orders", 0, key,
            new IdempotentBatchResult(500L, 500L, 0L, 1L, /*leaderEpoch*/ 5));

        assertTrue(kernel.lookupIdempotentBatch("orders", 0, key, /*current*/ 7).isEmpty(),
            "newer-epoch lookup must miss to avoid false-positive duplicate ACK");
        // Stale entry has been evicted — even a lookup at the ORIGINAL recording epoch must now
        // miss. This is the strong eviction guarantee that prevents zombie hits from a broker
        // that briefly re-saw its old epoch.
        assertTrue(kernel.lookupIdempotentBatch("orders", 0, key, /*current*/ 5).isEmpty(),
            "evicted stale entry must stay evicted on subsequent lookups");
    }

    @Test
    public void lookupAtOlderLeaderEpochAlsoEvictsAsStale() {
        // Symmetric to the newer-epoch case: an OLDER epoch lookup against a NEWER cache entry
        // is equally untrustworthy. The lookup must not hit, and the eviction lets a subsequent
        // newer-epoch lookup take the cold path. This shouldn't happen under correct broker
        // behaviour (epochs only advance), but a defensive evict-on-any-mismatch is robust to
        // the broker passing -1 (NO_PARTITION_LEADER_EPOCH) on the lookup when the cache holds
        // a real epoch.
        kernel.declare(descriptor("orders", 4, "shared", 1));
        IdempotentBatchKey key = new IdempotentBatchKey(42L, (short) 0, 0, 0);
        kernel.recordIdempotentBatch("orders", 0, key,
            new IdempotentBatchResult(500L, 500L, 0L, 1L, /*leaderEpoch*/ 5));

        assertTrue(kernel.lookupIdempotentBatch("orders", 0, key, /*current*/ -1).isEmpty(),
            "mismatched epoch on lookup must evict regardless of direction");
    }

    @Test
    public void invalidateIdempotentCacheForBackingDropsEntriesAcrossSharedLogicalTopics() {
        // Two logical topics share one backing. A leadership change on the backing partition
        // means BOTH logical topics' idempotent caches need to be cleared — neither is safe to
        // trust until the new leader has rebuilt its state. The kernel method exists to give
        // a future Partition.makeLeader/makeFollower hook a single call to make.
        kernel.declare(descriptor("orders", 4, "shared", 1));
        kernel.declare(descriptor("payments", 4, "shared", 1));
        IdempotentBatchKey key = new IdempotentBatchKey(42L, (short) 0, 0, 0);
        kernel.recordIdempotentBatch("orders", 0, key,
            new IdempotentBatchResult(500L, 500L, 0L, 1L, LEADER_EPOCH));
        kernel.recordIdempotentBatch("payments", 0, key,
            new IdempotentBatchResult(700L, 700L, 0L, 1L, LEADER_EPOCH));

        kernel.invalidateIdempotentCacheForBacking("shared");

        assertTrue(kernel.lookupIdempotentBatch("orders", 0, key, LEADER_EPOCH).isEmpty(),
            "invalidation must drop entries on every logical topic backed by 'shared'");
        assertTrue(kernel.lookupIdempotentBatch("payments", 0, key, LEADER_EPOCH).isEmpty(),
            "invalidation must drop entries on every logical topic backed by 'shared'");
    }

    @Test
    public void invalidateIdempotentCacheForBackingDoesNotAffectOtherBackings() {
        // A leadership change on 'shared-A' must not flush cache state for logical topics whose
        // backing is 'shared-B' — the two backings are independent partitions and one's leader
        // transition says nothing about the other's correctness.
        kernel.declare(descriptor("orders", 4, "shared-A", 1));
        kernel.declare(descriptor("inventory", 4, "shared-B", 1));
        IdempotentBatchKey key = new IdempotentBatchKey(42L, (short) 0, 0, 0);
        kernel.recordIdempotentBatch("orders", 0, key,
            new IdempotentBatchResult(500L, 500L, 0L, 1L, LEADER_EPOCH));
        kernel.recordIdempotentBatch("inventory", 0, key,
            new IdempotentBatchResult(900L, 900L, 0L, 1L, LEADER_EPOCH));

        kernel.invalidateIdempotentCacheForBacking("shared-A");

        assertTrue(kernel.lookupIdempotentBatch("orders", 0, key, LEADER_EPOCH).isEmpty(),
            "shared-A invalidation must drop orders cache");
        assertEquals(900L, kernel.lookupIdempotentBatch("inventory", 0, key, LEADER_EPOCH)
            .orElseThrow().logicalBaseOffset(),
            "shared-A invalidation must NOT touch shared-B-backed logical topics");
    }

    @Test
    public void invalidateIdempotentCacheForBackingIsNoOpForUnknownBacking() {
        // Hooking this from a leadership-transition path is going to fire for backing topics
        // that have no logical topic declared on them (the broker hosts many partitions). The
        // call must be a cheap no-op in that case, not an exception.
        kernel.declare(descriptor("orders", 4, "shared", 1));
        kernel.invalidateIdempotentCacheForBacking("unrelated-physical-topic");
        // No assertion: the test passes if no exception is thrown.
    }

    @Test
    public void recordIdempotentBatchAfterCloseIsRejected() {
        // Closed kernel must refuse cache writes — the underlying maps may still be held by
        // sidebar code, but the kernel's contract is "no operations after close()".
        kernel.declare(descriptor("orders", 4, "shared", 1));
        try {
            kernel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        assertThrows(IllegalStateException.class,
            () -> kernel.recordIdempotentBatch("orders", 0,
                new IdempotentBatchKey(42L, (short) 0, 0, 0),
                new IdempotentBatchResult(0L, 0L, 0L, 1L, LEADER_EPOCH)));
        kernel = null;
    }

    // ------------------ assertBackingTopicNotCompacted ------------------

    @Test
    public void assertBackingTopicNotCompactedAcceptsDeletePolicy() {
        // The common case: a backing topic created with default or explicit cleanup.policy=delete.
        // The first call validates and caches; we don't observe behaviour directly here beyond
        // "does not throw", but the cache-hit test below relies on this completing successfully.
        kernel.assertBackingTopicNotCompacted("shared", "delete");
    }

    @Test
    public void assertBackingTopicNotCompactedAcceptsNullPolicyAsDelete() {
        // A broker that has not loaded any topic-level override for cleanup.policy may pass null
        // here; the kernel must treat that as the broker default ("delete") rather than NPE'ing
        // out and stalling the produce path.
        kernel.assertBackingTopicNotCompacted("shared", null);
    }

    @Test
    public void assertBackingTopicNotCompactedRejectsCompactPolicy() {
        // PROMPT.md v1: non-compacted backings only. Compaction on a shared backing would let
        // logical topic A's tombstone for key K silently delete logical topic B's record with the
        // same key. Surface this loudly so the operator fixes the topic config.
        IllegalStateException ex = assertThrows(IllegalStateException.class,
            () -> kernel.assertBackingTopicNotCompacted("shared", "compact"));
        assertTrue(ex.getMessage().contains("shared"),
            "error must name the offending backing topic so the operator can fix it");
        assertTrue(ex.getMessage().contains("compact"),
            "error must surface the offending cleanup.policy value");
    }

    @Test
    public void assertBackingTopicNotCompactedRejectsCompositeCompactDeletePolicy() {
        // "compact,delete" is a legitimate cleanup.policy value in stock Kafka, but for
        // concentration v1 it is still unsafe — the compactor runs and the cross-logical-topic
        // key collision applies just the same. Reject anything containing "compact".
        assertThrows(IllegalStateException.class,
            () -> kernel.assertBackingTopicNotCompacted("shared", "compact,delete"));
    }

    @Test
    public void assertBackingTopicNotCompactedShortCircuitsOnSamePolicyRepeated() {
        // Repeated calls with the SAME policy must short-circuit so the hot path pays only a
        // ConcurrentHashMap.get + String.equals. We assert no exception is thrown across many
        // calls; the absence of a re-evaluation can only be observed indirectly (any thrown
        // exception would surface here), but the post-condition we care about is correctness.
        kernel.assertBackingTopicNotCompacted("shared", "delete");
        for (int i = 0; i < 10; i++) {
            kernel.assertBackingTopicNotCompacted("shared", "delete");
        }
    }

    @Test
    public void assertBackingTopicNotCompactedRevalidatesAfterAlterConfigsFlipToCompact() {
        // The staleness vector: once a backing was validated as "delete", a subsequent
        // AlterConfigs flipping cleanup.policy to "compact" must be caught on the next produce.
        // The broker calls assertBackingTopicNotCompacted with the freshly-resolved policy string
        // every time, so a change in the argument has to invalidate the cached verdict instead of
        // being suppressed by it.
        kernel.assertBackingTopicNotCompacted("shared", "delete");
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> kernel.assertBackingTopicNotCompacted("shared", "compact"));
        assertTrue(e.getMessage().contains("shared"));
        assertTrue(e.getMessage().contains("compact"));
    }

    @Test
    public void assertBackingTopicNotCompactedRejectsOnEveryCallAfterCompactFlip() {
        // Once we have rejected "compact" for a backing, a subsequent call with the same
        // "compact" must STILL throw — the rejection must NOT have been cached as a "fine"
        // verdict, and the stale "delete" verdict must have been removed so it doesn't suppress
        // the second rejection either.
        kernel.assertBackingTopicNotCompacted("shared", "delete");
        assertThrows(IllegalStateException.class,
            () -> kernel.assertBackingTopicNotCompacted("shared", "compact"));
        assertThrows(IllegalStateException.class,
            () -> kernel.assertBackingTopicNotCompacted("shared", "compact"));
    }

    @Test
    public void assertBackingTopicNotCompactedRecoversWhenPolicyFlipsBackToDelete() {
        // After a transient compact verdict (operator typo, rolled back), flipping back to
        // delete must succeed on the next call — no permanent quarantine of a backing once
        // a stale verdict was observed.
        kernel.assertBackingTopicNotCompacted("shared", "delete");
        assertThrows(IllegalStateException.class,
            () -> kernel.assertBackingTopicNotCompacted("shared", "compact"));
        kernel.assertBackingTopicNotCompacted("shared", "delete");
    }

    @Test
    public void assertBackingTopicNotCompactedIsolatesBackingTopics() {
        // Each backing topic has its own validation slot — caching "shared-A" must not mask a
        // mis-configured "shared-B". Otherwise a single declare of a safe backing would unlock all
        // backings broker-wide.
        kernel.assertBackingTopicNotCompacted("shared-A", "delete");
        assertThrows(IllegalStateException.class,
            () -> kernel.assertBackingTopicNotCompacted("shared-B", "compact"));
    }

    @Test
    public void assertBackingTopicNotCompactedRejectsNullBackingTopic() {
        assertThrows(NullPointerException.class,
            () -> kernel.assertBackingTopicNotCompacted(null, "delete"));
    }

    // ------------------ Per-backing readiness gate (Commit B.1) ------------------

    @Test
    public void unseenBackingDefaultsToReady() {
        // Default for any never-observed backing is "ready" — the kernel trusts whatever tracker
        // state was loaded at broker startup until something explicitly invalidates it. This
        // matches the steady-state expectation that an idle broker holds a quiescent ready set
        // and pays nothing on the produce hot path.
        assertTrue(kernel.isBackingReady(new TopicPartition("shared", 0)));
        assertTrue(kernel.isBackingReady(new TopicPartition("never-declared", 42)));
    }

    @Test
    public void markBackingUnreadyClosesGate() {
        TopicPartition tp = new TopicPartition("shared", 3);
        kernel.markBackingUnready(tp);
        assertFalse(kernel.isBackingReady(tp));
        // Sibling partitions of the same backing topic remain ready — readiness is per-
        // (topic, partition), not per topic, because the broker may be leader of partition 3
        // and follower of partition 5 of the same backing.
        assertTrue(kernel.isBackingReady(new TopicPartition("shared", 4)));
        // Other backings are unaffected.
        assertTrue(kernel.isBackingReady(new TopicPartition("other-shared", 3)));
    }

    @Test
    public void markBackingReadyClearsGateAndReportsPriorState() {
        TopicPartition tp = new TopicPartition("shared", 1);
        // First clear on a never-marked partition returns false (nothing to clear).
        assertFalse(kernel.markBackingReady(tp));
        kernel.markBackingUnready(tp);
        // First clear after a mark returns true (the mark was removed).
        assertTrue(kernel.markBackingReady(tp));
        // Second clear is a no-op and returns false. Idempotent.
        assertFalse(kernel.markBackingReady(tp));
        assertTrue(kernel.isBackingReady(tp));
    }

    @Test
    public void markBackingUnreadyIsIdempotent() {
        TopicPartition tp = new TopicPartition("shared", 2);
        kernel.markBackingUnready(tp);
        kernel.markBackingUnready(tp);
        kernel.markBackingUnready(tp);
        assertFalse(kernel.isBackingReady(tp));
        // One clear undoes all the marks (set semantics, not a counter).
        assertTrue(kernel.markBackingReady(tp));
        assertTrue(kernel.isBackingReady(tp));
    }

    @Test
    public void readinessGateRejectsNullPartition() {
        assertThrows(NullPointerException.class, () -> kernel.markBackingUnready(null));
        assertThrows(NullPointerException.class, () -> kernel.markBackingReady(null));
        assertThrows(NullPointerException.class, () -> kernel.isBackingReady(null));
    }

    @Test
    public void readinessGateIsIndependentOfIdempotentCacheAndTracker() {
        // The gate is pure metadata — flipping it must not touch the tracker or the idempotent
        // cache. This is what lets B.2 sequence the operations: close the gate FIRST, then drop
        // the tracker; re-open AFTER the rehydrate publishes. Reversing those steps would leak
        // produces against a stale tracker; flipping the gate must therefore be cheap and
        // observable in isolation.
        kernel.declare(descriptor("orders", 4, "shared", 2));
        TopicPartition tp = new TopicPartition("shared", 1);
        long beforeNextOffset = kernel.nextLogicalOffset("orders", 1);
        long beforeStartOffset = kernel.startLogicalOffset("orders", 1);

        kernel.markBackingUnready(tp);
        kernel.markBackingReady(tp);

        assertEquals(beforeNextOffset, kernel.nextLogicalOffset("orders", 1));
        assertEquals(beforeStartOffset, kernel.startLogicalOffset("orders", 1));
    }
}
