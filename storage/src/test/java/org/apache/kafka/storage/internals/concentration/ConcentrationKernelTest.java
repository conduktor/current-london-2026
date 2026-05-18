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
        kernel.close();
        assertThrows(IllegalStateException.class,
            () -> kernel.reserveProduce("orders", 0));
        kernel = null;
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
    public void reservationIdentityIsPreservedAcrossCommit() throws IOException {
        kernel.declare(descriptor("orders", 4, "shared", 1));
        Reservation r = kernel.reserveProduce("orders", 0);
        assertNotNull(r);
        assertEquals("orders", r.logicalTopic());
        assertEquals(0, r.logicalPartition());
        kernel.commitProduce(r, 100L);
    }
}
