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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behavioural contract of {@link BackingScanRecoverer}.
 *
 * <p>Two recovery flavours, both required by the PROMPT acceptance criteria:
 * <ul>
 *   <li><b>Sidecar-present</b>: the sidecar file is intact; seed the tracker from it. Cheap
 *       startup.</li>
 *   <li><b>Sidecar-absent</b>: scan the backing log for records carrying logical headers and
 *       rebuild both the sidecar and the tracker. Slower but no data loss.</li>
 * </ul>
 */
public class BackingScanRecovererTest {

    private File sidecarDir;
    private BackingScanRecoverer recoverer;

    @BeforeEach
    public void setup() throws IOException {
        sidecarDir = TestUtils.tempDirectory();
        recoverer = new BackingScanRecoverer(sidecarDir);
    }

    @Test
    public void scanRebuildsSidecarFromInterleavedRecoveryRecords() throws IOException {
        // PROMPT functional scenario: two logical topics share a single-partition backing,
        // records interleaved round-robin. After scan, each logical topic must have its own
        // contiguous logical offset sequence and a sidecar mapping logical→backing for it.
        List<RecoveryRecord> stream = List.of(
            new RecoveryRecord("topicA", 0, 0L, 0L),
            new RecoveryRecord("topicB", 0, 0L, 1L),
            new RecoveryRecord("topicA", 0, 1L, 2L),
            new RecoveryRecord("topicB", 0, 1L, 3L),
            new RecoveryRecord("topicA", 0, 2L, 4L)
        );

        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        recoverer.recoverFromScan(stream.iterator(), tracker);

        assertEquals(3L, tracker.nextLogicalOffset("topicA", 0));
        assertEquals(2L, tracker.nextLogicalOffset("topicB", 0));

        try (LogicalSidecarIndex aSidecar = recoverer.openSidecar("topicA", 0)) {
            assertEquals(3L, aSidecar.size());
            assertEquals(0L, aSidecar.lookup(0L));
            assertEquals(2L, aSidecar.lookup(1L));
            assertEquals(4L, aSidecar.lookup(2L));
        }
        try (LogicalSidecarIndex bSidecar = recoverer.openSidecar("topicB", 0)) {
            assertEquals(2L, bSidecar.size());
            assertEquals(1L, bSidecar.lookup(0L));
            assertEquals(3L, bSidecar.lookup(1L));
        }
    }

    @Test
    public void scanRebuildsTrackerWhenLogicalTopicsCoverDifferentPartitions() throws IOException {
        List<RecoveryRecord> stream = List.of(
            new RecoveryRecord("topicA", 0, 0L, 0L),
            new RecoveryRecord("topicA", 0, 1L, 1L),
            new RecoveryRecord("topicA", 1, 0L, 2L),
            new RecoveryRecord("topicA", 1, 1L, 3L),
            new RecoveryRecord("topicA", 0, 2L, 4L)
        );
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        recoverer.recoverFromScan(stream.iterator(), tracker);

        assertEquals(3L, tracker.nextLogicalOffset("topicA", 0));
        assertEquals(2L, tracker.nextLogicalOffset("topicA", 1));
    }

    @Test
    public void scanRejectsLogicalOffsetGap() throws IOException {
        // If a logical-offset gap shows up during scan, the data is corrupted — surface it.
        List<RecoveryRecord> stream = List.of(
            new RecoveryRecord("topicA", 0, 0L, 0L),
            new RecoveryRecord("topicA", 0, 2L, 1L)   // gap: expected 1, got 2
        );
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        IllegalStateException thrown = assertThrows(IllegalStateException.class,
            () -> recoverer.recoverFromScan(stream.iterator(), tracker));
        // Pin that the message identifies *which* corruption was detected and on which partition,
        // so a future change that throws ISE for an unrelated reason does not fool this test.
        String msg = thrown.getMessage();
        assertTrue(msg.contains("topicA") && msg.contains("discontinuity")
                && msg.contains("expected 1") && msg.contains("got 2"),
            "exception message must identify the gap: " + msg);
    }

    @Test
    public void scanRejectsLogicalOffsetRegression() throws IOException {
        List<RecoveryRecord> stream = List.of(
            new RecoveryRecord("topicA", 0, 0L, 0L),
            new RecoveryRecord("topicA", 0, 1L, 1L),
            new RecoveryRecord("topicA", 0, 1L, 2L)   // duplicate logical offset for topicA[0]
        );
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        IllegalStateException thrown = assertThrows(IllegalStateException.class,
            () -> recoverer.recoverFromScan(stream.iterator(), tracker));
        String msg = thrown.getMessage();
        assertTrue(msg.contains("topicA") && msg.contains("expected 2") && msg.contains("got 1"),
            "exception message must identify the regression: " + msg);
    }

    @Test
    public void recoveryFromSidecarSeedsTrackerWithoutScan() throws IOException {
        // Persist a sidecar without touching the tracker, then recover.
        try (LogicalSidecarIndex idx = recoverer.openSidecar("topicA", 0)) {
            for (long i = 0; i < 5; i++) idx.append(i * 10);
        }
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        recoverer.recoverFromSidecars(List.of(new LogicalPartition("topicA", 0)), tracker);
        assertEquals(5L, tracker.nextLogicalOffset("topicA", 0));
    }

    @Test
    public void recoveryFromSidecarsAcrossMultiplePartitionsIsCheap() throws IOException {
        // Cheap startup: just sized reads.
        try (LogicalSidecarIndex a0 = recoverer.openSidecar("topicA", 0)) {
            for (long i = 0; i < 100; i++) a0.append(i);
        }
        try (LogicalSidecarIndex a1 = recoverer.openSidecar("topicA", 1)) {
            for (long i = 0; i < 50; i++) a1.append(i + 1000);
        }
        try (LogicalSidecarIndex b0 = recoverer.openSidecar("topicB", 0)) {
            for (long i = 0; i < 25; i++) b0.append(i + 2000);
        }

        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        long start = System.nanoTime();
        recoverer.recoverFromSidecars(List.of(
            new LogicalPartition("topicA", 0),
            new LogicalPartition("topicA", 1),
            new LogicalPartition("topicB", 0)
        ), tracker);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertEquals(100L, tracker.nextLogicalOffset("topicA", 0));
        assertEquals(50L, tracker.nextLogicalOffset("topicA", 1));
        assertEquals(25L, tracker.nextLogicalOffset("topicB", 0));
        // PROMPT acceptance criterion: sub-second offset-tracker rebuild from sidecars.
        // Allow a generous bound; this is purely a sanity bound, not a perf benchmark.
        if (elapsedMs > 1000) {
            throw new AssertionError("sidecar recovery took " + elapsedMs + "ms, expected < 1000ms");
        }
    }

    @Test
    public void recoveryFromEmptyScanLeavesTrackerUnchanged() throws IOException {
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        recoverer.recoverFromScan(List.<RecoveryRecord>of().iterator(), tracker);
        assertEquals(0L, tracker.nextLogicalOffset("topicA", 0));
    }

    @Test
    public void filterAwareScanResetsPartitionsAbsentFromStream() throws IOException {
        // BLOCKER 2 fix: when a partition is in the filter (i.e. mapped onto this backing on
        // the new leader) but yields zero records in the scan window, its sidecar must be
        // truncated to 0 and its tracker entry restored to (persistedStart, 0). Without this,
        // stale local state from a previous incarnation survives past the gate-open and
        // re-exposes ghost records to consumers/fetchers.

        // Pre-seed stale sidecar + tracker state for topicA/0 — simulates "this broker was
        // a leader for this backing in a past life and accumulated 10 logical offsets".
        try (LogicalSidecarIndex stale = recoverer.openSidecar("topicA", 0)) {
            for (long i = 0; i < 10; i++) {
                stale.append(100L + i);
            }
        }
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        tracker.restorePartition("topicA", 0, 0L, 10L);
        assertEquals(10L, tracker.nextLogicalOffset("topicA", 0));

        // Filter declares topicA/0 — and the stream is EMPTY (no records on the current
        // leader's log window). Pre-fix behavior: tracker remains at 10, sidecar at 10 entries.
        // Post-fix behavior: tracker reset to 0, sidecar truncated.
        Set<LogicalPartition> filter = Set.of(new LogicalPartition("topicA", 0));
        recoverer.recoverFromScan(List.<RecoveryRecord>of().iterator(), tracker, filter);

        assertEquals(0L, tracker.nextLogicalOffset("topicA", 0));
        try (LogicalSidecarIndex sidecar = recoverer.openSidecar("topicA", 0)) {
            assertEquals(0L, sidecar.size());
        }
    }

    @Test
    public void filterAwareScanPreservesZeroPersistedStartOnAbsentPartition() throws IOException {
        // When persistedStart=0 (no DeleteRecords history) and the partition is in the filter
        // but absent from the stream, the rebuild ends at tracker (0, 0) and an empty sidecar.
        // The non-zero-persistedStart case is the loud-failure variant covered by
        // filterAwareScanThrowsIfPersistedStartExceedsRebuiltSize.
        try (LogicalSidecarIndex stale = recoverer.openSidecar("topicA", 0)) {
            for (long i = 0; i < 5; i++) stale.append(50L + i);
        }
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        tracker.restorePartition("topicA", 0, 0L, 5L);

        Set<LogicalPartition> filter = Set.of(new LogicalPartition("topicA", 0));
        recoverer.recoverFromScan(List.<RecoveryRecord>of().iterator(), tracker, filter);

        assertEquals(0L, tracker.nextLogicalOffset("topicA", 0));
        try (LogicalSidecarIndex sidecar = recoverer.openSidecar("topicA", 0)) {
            assertEquals(0L, sidecar.size());
        }
    }

    @Test
    public void filterAwareScanThrowsIfPersistedStartExceedsRebuiltSize() throws IOException {
        // The persistedStart > sidecar.size() guard exists for a reason: silently regressing
        // start to 0 would re-expose "deleted" records. We want a loud failure instead.
        try (LogicalSidecarIndex stale = recoverer.openSidecar("topicA", 0)) {
            for (long i = 0; i < 10; i++) stale.append(100L + i);
        }
        recoverer.persistStartOffset("topicA", 0, 5L);
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        Set<LogicalPartition> filter = Set.of(new LogicalPartition("topicA", 0));

        IOException io = assertThrows(IOException.class,
            () -> recoverer.recoverFromScan(List.<RecoveryRecord>of().iterator(), tracker, filter));
        assertTrue(io.getMessage().contains("startOffset 5"));
        assertTrue(io.getMessage().contains("rebuilt sidecar size 0"));
    }

    @Test
    public void filterAwareScanAdvancesPartitionsPresentInStream() throws IOException {
        // Cross-check: when the stream DOES yield records for a filter partition, the rebuild
        // proceeds normally and the tracker reflects the rebuilt state.
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        Set<LogicalPartition> filter = Set.of(
            new LogicalPartition("topicA", 0),
            new LogicalPartition("topicA", 1)  // in filter but absent from stream
        );
        List<RecoveryRecord> stream = List.of(
            new RecoveryRecord("topicA", 0, 0L, 100L),
            new RecoveryRecord("topicA", 0, 1L, 101L),
            new RecoveryRecord("topicA", 0, 2L, 102L)
        );
        recoverer.recoverFromScan(stream.iterator(), tracker, filter);

        // Present partition: advanced to 3 logical offsets.
        assertEquals(3L, tracker.nextLogicalOffset("topicA", 0));
        // Absent partition: reset to 0.
        assertEquals(0L, tracker.nextLogicalOffset("topicA", 1));
    }

    @Test
    public void filterAwareScanWithEmptyFilterMatchesLegacyBehavior() throws IOException {
        // The 2-arg form delegates to the 3-arg form with Collections.emptySet(). Verify that
        // an empty filter doesn't pre-truncate ANYTHING — partitions outside the stream's view
        // are left alone (legacy "trust the stream" semantics).
        try (LogicalSidecarIndex pre = recoverer.openSidecar("topicA", 0)) {
            for (long i = 0; i < 7; i++) pre.append(200L + i);
        }
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        tracker.restorePartition("topicA", 0, 0L, 7L);

        recoverer.recoverFromScan(List.<RecoveryRecord>of().iterator(), tracker, Collections.emptySet());

        // No filter → no reset. Sidecar and tracker unchanged.
        assertEquals(7L, tracker.nextLogicalOffset("topicA", 0));
        try (LogicalSidecarIndex sidecar = recoverer.openSidecar("topicA", 0)) {
            assertEquals(7L, sidecar.size());
        }
    }
}
