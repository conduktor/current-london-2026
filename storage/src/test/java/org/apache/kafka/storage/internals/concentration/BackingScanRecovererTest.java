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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
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

    @Test
    public void scanRollsBackInlineCreatedSidecarsOnIteratorThrow() throws IOException {
        // BLOCKER #181: a scan that fails mid-stream must NOT leave a partial sidecar file on
        // disk. The startup path (BackingLogScanRecovery) calls the 2-arg form with empty
        // filter, has no readiness gate, and the next broker restart's recoverFromDisk() will
        // pick up any leftover sidecar file as authoritative — silently truncating the
        // partition's logical-offset sequence to whatever prefix the failed scan happened to
        // write. The fix must delete inline-created sidecars on exceptional exit so
        // partitionsWithoutSidecar() re-flags them for a fresh scan on the next attempt.
        //
        // Pre-state: no sidecar files on disk for topicA[0] or topicA[1].
        LogicalPartition p0 = new LogicalPartition("topicA", 0);
        LogicalPartition p1 = new LogicalPartition("topicA", 1);
        assertTrue(!recoverer.sidecarFile(p0.logicalTopic(), p0.logicalPartition()).exists(),
            "precondition: topicA[0] sidecar must not exist before scan");
        assertTrue(!recoverer.sidecarFile(p1.logicalTopic(), p1.logicalPartition()).exists(),
            "precondition: topicA[1] sidecar must not exist before scan");

        // Iterator yields two successful records for topicA[0], one for topicA[1], then throws
        // mid-stream. Without the rollback fix, topicA[0].sidecar would have 2 entries on disk
        // and topicA[1].sidecar would have 1 — both partial files would survive the broker's
        // exit and be silently trusted as complete on the next startup.
        Iterator<RecoveryRecord> failing = new Iterator<RecoveryRecord>() {
            private final List<RecoveryRecord> records = List.of(
                new RecoveryRecord("topicA", 0, 0L, 0L),
                new RecoveryRecord("topicA", 0, 1L, 1L),
                new RecoveryRecord("topicA", 1, 0L, 2L)
            );
            private int idx = 0;
            @Override public boolean hasNext() {
                // After yielding all records, simulate a backing-log read failure mid-scan.
                return true;
            }
            @Override public RecoveryRecord next() {
                if (idx < records.size()) {
                    return records.get(idx++);
                }
                throw new SimulatedScanFailure("backing-log read failed at offset " + idx);
            }
        };

        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        assertThrows(SimulatedScanFailure.class,
            () -> recoverer.recoverFromScan(failing, tracker));

        // POST-CONDITION (the new invariant): both partial sidecar files are gone from disk.
        // Without the fix, BOTH files would still exist with partial content, and
        // recoverFromDisk on the next restart would seed the tracker with those stale prefixes
        // — exactly the silent-data-loss mode BLOCKER #181 describes.
        assertTrue(!recoverer.sidecarFile(p0.logicalTopic(), p0.logicalPartition()).exists(),
            "topicA[0] sidecar must be deleted on scan failure (was partial)");
        assertTrue(!recoverer.sidecarFile(p1.logicalTopic(), p1.logicalPartition()).exists(),
            "topicA[1] sidecar must be deleted on scan failure (was partial)");
        // The tracker must NOT carry any partial state either — restorePartition is only
        // reached on success.
        assertEquals(0L, tracker.nextLogicalOffset("topicA", 0));
        assertEquals(0L, tracker.nextLogicalOffset("topicA", 1));
    }

    @Test
    public void scanRollsBackInlineCreatedSidecarsOnGapException() throws IOException {
        // Same invariant as the iterator-throw case, but driven through the kernel's own
        // gap-detection path. A logical-offset gap on record N+1 leaves the partial sidecar
        // for the affected partition on disk under the pre-fix code, where it would later
        // be silently picked up by recoverFromDisk and treated as authoritative.
        LogicalPartition p = new LogicalPartition("topicB", 0);
        assertTrue(!recoverer.sidecarFile(p.logicalTopic(), p.logicalPartition()).exists());

        List<RecoveryRecord> stream = List.of(
            new RecoveryRecord("topicB", 0, 0L, 10L),
            new RecoveryRecord("topicB", 0, 1L, 11L),
            new RecoveryRecord("topicB", 0, 3L, 12L)   // gap: expected 2, got 3
        );
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        assertThrows(IllegalStateException.class,
            () -> recoverer.recoverFromScan(stream.iterator(), tracker));

        // The sidecar file we created and partially populated MUST be gone.
        assertTrue(!recoverer.sidecarFile(p.logicalTopic(), p.logicalPartition()).exists(),
            "partial sidecar must not survive a gap-detection failure");
        assertEquals(0L, tracker.nextLogicalOffset("topicB", 0));
    }

    @Test
    public void scanDoesNotDeleteFilterSidecarsOnFailure() throws IOException {
        // The rollback must distinguish "we created it inline" (delete) from "caller passed
        // it in the filter set" (leave alone). The leader-acquisition path passes the filter,
        // closes the readiness gate around the call, and the file may pre-date this scan —
        // it's NOT ours to delete on failure. The gate keeps the partial state invisible until
        // the recoverer succeeds; the next attempt re-runs preTruncateFilter.
        LogicalPartition filterPart = new LogicalPartition("topicA", 0);
        LogicalPartition inlinePart = new LogicalPartition("topicA", 1);

        // Pre-seed the filter-partition sidecar — simulates state from a previous incarnation.
        try (LogicalSidecarIndex pre = recoverer.openSidecar(filterPart.logicalTopic(), filterPart.logicalPartition())) {
            for (long i = 0; i < 4; i++) pre.append(500L + i);
        }

        // Stream: one record for the filter partition (gets appended to the now-truncated
        // file), one record for an inline partition (creates a new file), then a gap that
        // forces the scan to throw.
        List<RecoveryRecord> stream = List.of(
            new RecoveryRecord("topicA", 0, 0L, 600L),
            new RecoveryRecord("topicA", 1, 0L, 601L),
            new RecoveryRecord("topicA", 1, 2L, 602L)   // gap: expected 1, got 2
        );
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        Set<LogicalPartition> filter = Set.of(filterPart);
        assertThrows(IllegalStateException.class,
            () -> recoverer.recoverFromScan(stream.iterator(), tracker, filter));

        // Filter partition's file MUST still exist (caller's gate keeps it invisible).
        assertTrue(recoverer.sidecarFile(filterPart.logicalTopic(), filterPart.logicalPartition()).exists(),
            "filter sidecar must survive scan failure (caller owns lifecycle under the gate)");
        // Inline-created partition's file MUST be gone.
        assertTrue(!recoverer.sidecarFile(inlinePart.logicalTopic(), inlinePart.logicalPartition()).exists(),
            "inline-created sidecar must be deleted on scan failure");
    }

    private static final class SimulatedScanFailure extends RuntimeException {
        SimulatedScanFailure(String msg) {
            super(msg);
        }
    }

    // ---------------- r24 BLOCKER #248: parent-dir fsync discriminator tests ----------------

    @Test
    public void openSidecarOnNewTopicFsyncsSidecarDirAndTopicDir() throws IOException {
        // Two fsyncs must happen end-to-end on a fresh topic's first openSidecar:
        //   1. sidecarDir, after ensureTopicDir created topic-dir (so the topic-dir's dirent
        //      survives a crash before the FS's next metadata writeback).
        //   2. topic-dir, after the constructor created the .sidecar file (so the sidecar's
        //      dirent survives that same window).
        // Both go through the flushDirSeam, so the counting subclass observes both. Removing
        // either fsync from the production code would drop the count and fail this assertion.
        CountingRecoverer rec = new CountingRecoverer(sidecarDir);
        try (LogicalSidecarIndex sidecar = rec.openSidecar("freshTopic", 0)) {
            sidecar.append(0L);   // sanity: the sidecar is usable
        }
        Path expectedSidecarDir = sidecarDir.toPath().toAbsolutePath().normalize();
        Path expectedTopicDir = new File(sidecarDir, "freshTopic").toPath().toAbsolutePath().normalize();
        // ORDER matters per POSIX: the topic-dir's dirent lives in sidecarDir; fsync'ing the
        // .sidecar dirent (inside topic-dir) before sidecarDir means a crash between the two
        // fsyncs would leave a durable .sidecar inode pointing through a NON-durable topic-dir
        // dirent — recovery would not see either file. Asserting on ordered List equality (not
        // set membership) pins the chain {sidecarDir -> topic-dir -> .sidecar} the audit
        // requires for r24 #248.
        assertEquals(List.of(expectedSidecarDir, expectedTopicDir), rec.flushedPaths,
            "fsync order must be sidecarDir-first then topic-dir; saw " + rec.flushedPaths);
    }

    @Test
    public void ensureTopicDirRollsBackTopicDirWhenSidecarDirFsyncFails() throws IOException {
        // r25 audit follow-up to BLOCKER #248: when mkdirs succeeds but the parent-dir fsync
        // fails, the just-created topic-dir must be removed so a retry re-runs the full
        // mkdirs + fsync chain. Without rollback, the next ensureTopicDir call observes
        // topicDir.isDirectory()==true, fast-paths past the missing fsync, and the broker
        // never re-flushes the dirent the OS reported as un-durable.
        CountingRecoverer rec = new CountingRecoverer(sidecarDir);
        rec.failOnFlushDir = new IOException("simulated metadata-fsync failure");
        File topicDir = new File(sidecarDir, "freshTopic");
        assertThrows(IOException.class, () -> rec.openSidecar("freshTopic", 0));
        assertTrue(!topicDir.exists(),
            "topic-dir must be rolled back so the retry re-fsyncs sidecarDir");

        // Retry succeeds and observably re-fsyncs sidecarDir AND topic-dir in order.
        rec.failOnFlushDir = null;
        rec.flushedPaths.clear();
        try (LogicalSidecarIndex sidecar = rec.openSidecar("freshTopic", 0)) {
            sidecar.append(0L);
        }
        Path expectedSidecarDir = sidecarDir.toPath().toAbsolutePath().normalize();
        Path expectedTopicDir = topicDir.toPath().toAbsolutePath().normalize();
        assertEquals(List.of(expectedSidecarDir, expectedTopicDir), rec.flushedPaths,
            "retry after rollback must observably fsync sidecarDir AND topic-dir");
    }

    @Test
    public void reopeningExistingSidecarFsyncsNothing() throws IOException {
        // First open (via the @BeforeEach recoverer) creates the dir + file and fsyncs both.
        try (LogicalSidecarIndex initial = recoverer.openSidecar("topicA", 0)) {
            initial.append(0L);
        }
        // Subsequent open observes the file already exists → no creation → no fsync. This is
        // the steady-state fast path: every produce after the first to a given (topic,
        // partition) must NOT pay an fsync per open. Otherwise the no-fsync-per-append PROMPT
        // invariant would be silently violated by the kernel reopening on every restart.
        CountingRecoverer rec = new CountingRecoverer(sidecarDir);
        try (LogicalSidecarIndex reopen = rec.openSidecar("topicA", 0)) {
            assertEquals(1L, reopen.size(), "sanity: file content survived");
        }
        assertEquals(0, rec.flushedPaths.size(),
            "re-opening an existing sidecar must not fsync anything; saw " + rec.flushedPaths);
    }

    @Test
    public void openSidecarOnNewPartitionInExistingTopicFsyncsOnlyTopicDir() throws IOException {
        // Topic-dir already exists from a prior partition's open → ensureTopicDir's fast path
        // (isDirectory true) skips both mkdirs and sidecarDir-fsync. Only the new .sidecar file
        // creation needs a topic-dir fsync.
        try (LogicalSidecarIndex p0 = recoverer.openSidecar("topicA", 0)) {
            p0.append(0L);
        }
        CountingRecoverer rec = new CountingRecoverer(sidecarDir);
        try (LogicalSidecarIndex p1 = rec.openSidecar("topicA", 1)) {
            p1.append(99L);
        }
        Path expectedTopicDir = new File(sidecarDir, "topicA").toPath().toAbsolutePath().normalize();
        assertEquals(1, rec.flushedPaths.size(),
            "new-partition-in-existing-topic must fsync exactly topic-dir; saw " + rec.flushedPaths);
        assertTrue(rec.flushedPaths.contains(expectedTopicDir));
    }

    @Test
    public void openSidecarPropagatesFlushDirFailure() {
        // If the OS reports the fsync failed (e.g., I/O error on the metadata journal), the
        // produce path MUST see the failure rather than continuing with a possibly-not-durable
        // dirent. CountingRecoverer makes flushDirSeam throw on first invocation; the production
        // code surfaces that as IOException to its caller.
        CountingRecoverer rec = new CountingRecoverer(sidecarDir);
        rec.failOnFlushDir = new IOException("simulated metadata-fsync failure");
        IOException thrown = assertThrows(IOException.class, () -> rec.openSidecar("topicA", 0));
        assertEquals("simulated metadata-fsync failure", thrown.getMessage());
    }

    @Test
    public void persistStartOffsetFsyncsSidecarDirOnFreshTopic() throws IOException {
        // persistStartOffset calls ensureTopicDir which fsyncs sidecarDir on first creation
        // (one observable seam call). The rename's parent (topic-dir) is fsync'd inside
        // atomicMoveWithFallback via Utils.flushDir directly — NOT through our seam — so the
        // counter sees exactly one fsync from this codepath.
        CountingRecoverer rec = new CountingRecoverer(sidecarDir);
        rec.persistStartOffset("freshTopic", 0, 42L);
        Path expectedSidecarDir = sidecarDir.toPath().toAbsolutePath().normalize();
        assertEquals(1, rec.flushedPaths.size(),
            "persistStartOffset on fresh topic must fsync sidecarDir; saw " + rec.flushedPaths);
        assertTrue(rec.flushedPaths.contains(expectedSidecarDir));
    }

    @Test
    public void sidecarFileNoLongerCreatesTopicDirAsSideEffect() {
        // Path-only contract for sidecarFile() (and startOffsetFile()): inspection callers
        // (partitionsWithoutSidecar's .exists() walk, removeLogicalPartition's cleanup .exists()
        // check) must NOT accidentally materialise an empty topic-dir as a side effect — pre-
        // r24 #248, sidecarFile() did exactly that, which both (a) leaked an empty directory
        // per inspected topic-name and (b) created a dirent in sidecarDir without fsync.
        File computed = recoverer.sidecarFile("freshTopic", 0);
        assertEquals("0.sidecar", computed.getName());
        File topicDir = computed.getParentFile();
        assertEquals("freshTopic", topicDir.getName());
        assertTrue(!topicDir.exists(), "sidecarFile() must not create the topic-dir as a side effect");
    }

    /**
     * Counts {@link BackingScanRecoverer#flushDirSeam(Path)} invocations and optionally throws
     * on the next call. The seam pattern lets us assert the production code ACTUALLY fsyncs the
     * directories it claims to — a no-op {@code ensureTopicDir} would otherwise pass every
     * other test in this file silently.
     */
    private static final class CountingRecoverer extends BackingScanRecoverer {
        final List<Path> flushedPaths = new ArrayList<>();
        volatile IOException failOnFlushDir;

        CountingRecoverer(File sidecarDir) {
            super(sidecarDir);
        }

        @Override
        void flushDirSeam(Path path) throws IOException {
            flushedPaths.add(path);
            if (failOnFlushDir != null) {
                throw failOnFlushDir;
            }
        }
    }
}
