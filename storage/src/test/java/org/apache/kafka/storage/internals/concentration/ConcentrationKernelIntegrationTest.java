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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Multi-component scenarios from PROMPT.md §"Functional test scenarios". These exercise the
 * concentration kernel as a whole — registry + tracker + sidecar + recoverer composed the way the
 * broker glue will compose them at produce / fetch / DeleteRecords / restart time.
 *
 * <p>Scenarios covered here (kernel-addressable):
 * <ol>
 *   <li>Two logical topics share a single-partition backing, interleaved produce, per-topic
 *       isolation on consume.</li>
 *   <li>Three logical topics produce concurrently, monotonic per-topic offsets, no overlap.</li>
 *   <li>DeleteRecords on one logical partition does not affect siblings on the same backing.</li>
 *   <li>Restart with intact sidecars rehydrates the tracker without a backing-log scan.</li>
 *   <li>Restart without sidecars reconstructs both sidecars and tracker from a backing-log
 *       scan.</li>
 *   <li>Direct produce to a backing-topic name is rejected by the registry signal.</li>
 * </ol>
 *
 * <p>One PROMPT scenario — idempotent producer retry with no duplicates — sits at the broker
 * level (producer-id / epoch / sequence enforcement) and is not addressable at the kernel layer.
 * It is intentionally not covered here.
 */
public class ConcentrationKernelIntegrationTest {

    private File sidecarDir;
    private BackingScanRecoverer recoverer;
    private final Map<LogicalPartition, LogicalSidecarIndex> openSidecars = new HashMap<>();

    @BeforeEach
    public void setup() throws IOException {
        sidecarDir = TestUtils.tempDirectory();
        recoverer = new BackingScanRecoverer(sidecarDir);
    }

    @AfterEach
    public void teardown() throws IOException {
        for (LogicalSidecarIndex s : openSidecars.values()) {
            s.close();
        }
        openSidecars.clear();
    }

    /**
     * Simulate one record's produce path: reserve a logical offset, take the next backing slot
     * (the broker would call Log.append() here; we just hand out monotonic backing offsets),
     * persist the sidecar mapping, commit the reservation. Returns the assigned logical offset.
     */
    private long produce(LogicalOffsetTracker tracker,
                         String logicalTopic,
                         int logicalPartition,
                         AtomicLong nextBackingOffset) throws IOException {
        Reservation r = tracker.reserve(logicalTopic, logicalPartition);
        try {
            long backingOffset = nextBackingOffset.getAndIncrement();
            sidecarFor(logicalTopic, logicalPartition).append(backingOffset);
            tracker.commit(r);
            return r.logicalOffset();
        } catch (RuntimeException | IOException e) {
            tracker.rollback(r);
            throw e;
        }
    }

    private LogicalSidecarIndex sidecarFor(String logicalTopic, int logicalPartition) throws IOException {
        LogicalPartition key = new LogicalPartition(logicalTopic, logicalPartition);
        LogicalSidecarIndex existing = openSidecars.get(key);
        if (existing != null) return existing;
        LogicalSidecarIndex fresh = recoverer.openSidecar(logicalTopic, logicalPartition);
        openSidecars.put(key, fresh);
        return fresh;
    }

    private void closeAllSidecars() throws IOException {
        for (LogicalSidecarIndex s : openSidecars.values()) {
            s.close();
        }
        openSidecars.clear();
    }

    /**
     * PROMPT scenario 1. Two logical topics ride a single backing partition, interleaved
     * round-robin. After produce, each topic's sidecar contains exactly its own backing offsets;
     * the two sidecars' backing-offset sets are disjoint; logical offsets are contiguous
     * 0..N-1 per topic. This is the "consumer of A never sees B" invariant verified at the
     * translation-table level.
     */
    @Test
    public void twoLogicalTopicsShareSingleBackingPartitionAndStayIsolated() throws IOException {
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        AtomicLong backing = new AtomicLong(0);
        final int perTopic = 1000;

        for (int i = 0; i < perTopic; i++) {
            long aOffset = produce(tracker, "topicA", 0, backing);
            long bOffset = produce(tracker, "topicB", 0, backing);
            assertEquals(i, aOffset);
            assertEquals(i, bOffset);
        }

        assertEquals(perTopic, tracker.nextLogicalOffset("topicA", 0));
        assertEquals(perTopic, tracker.nextLogicalOffset("topicB", 0));

        Set<Long> aBacking = new HashSet<>();
        Set<Long> bBacking = new HashSet<>();
        for (long i = 0; i < perTopic; i++) {
            aBacking.add(sidecarFor("topicA", 0).lookup(i));
            bBacking.add(sidecarFor("topicB", 0).lookup(i));
        }
        assertEquals(perTopic, aBacking.size(), "topicA backing offsets must be distinct");
        assertEquals(perTopic, bBacking.size(), "topicB backing offsets must be distinct");

        Set<Long> intersection = new HashSet<>(aBacking);
        intersection.retainAll(bBacking);
        assertTrue(intersection.isEmpty(),
            "topicA and topicB must never share a backing offset; overlap: " + intersection);

        // Round-robin pattern: topicA gets even-numbered backing offsets, topicB odd-numbered.
        for (long i = 0; i < perTopic; i++) {
            assertEquals(2 * i, sidecarFor("topicA", 0).lookup(i));
            assertEquals(2 * i + 1, sidecarFor("topicB", 0).lookup(i));
        }
    }

    /**
     * PROMPT scenario 2. Three logical topics produce concurrently on one backing partition.
     * Each tracker reaches its expected next offset; each sidecar has the right size; the
     * sidecar's backing offsets are strictly monotonic (enforced by LogicalSidecarIndex.append);
     * no two records across all three topics share a backing offset.
     */
    @Test
    public void threeLogicalTopicsConcurrentlyProduceAndPreserveMonotonicOffsets()
            throws IOException, InterruptedException {
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        AtomicLong backing = new AtomicLong(0);
        final int perTopic = 500;
        String[] topics = {"topicA", "topicB", "topicC"};

        ExecutorService pool = Executors.newFixedThreadPool(topics.length);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(topics.length);
        try {
            for (String t : topics) {
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < perTopic; i++) {
                            produce(tracker, t, 0, backing);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(30, TimeUnit.SECONDS), "produces did not finish in time");
        } finally {
            pool.shutdownNow();
        }

        // Per-topic invariants.
        Set<Long> seenBacking = new HashSet<>();
        for (String t : topics) {
            assertEquals(perTopic, tracker.nextLogicalOffset(t, 0),
                "tracker next offset for " + t);
            LogicalSidecarIndex s = sidecarFor(t, 0);
            assertEquals(perTopic, s.size());
            long prev = -1;
            for (long i = 0; i < perTopic; i++) {
                long b = s.lookup(i);
                assertTrue(b > prev, "sidecar entries for " + t + " must be monotonic");
                prev = b;
                assertTrue(seenBacking.add(b),
                    "backing offset " + b + " seen twice across topics");
            }
        }
        assertEquals(topics.length * perTopic, seenBacking.size());
    }

    /**
     * PROMPT scenario 3. DeleteRecords on logical topic A advances only A's startOffset. The
     * sibling logical topic B on the same backing partition retains its full readable range.
     * The shared backing log is not consulted by DeleteRecords — only the per-topic tracker
     * state changes.
     */
    @Test
    public void deleteRecordsAdvancesOnlyOneLogicalPartitionStart() throws IOException {
        LogicalOffsetTracker tracker = new LogicalOffsetTracker();
        AtomicLong backing = new AtomicLong(0);
        for (int i = 0; i < 100; i++) {
            produce(tracker, "topicA", 0, backing);
            produce(tracker, "topicB", 0, backing);
        }

        long backingSizeBefore = backing.get();
        tracker.advanceStartOffset("topicA", 0, 50);

        assertEquals(50L, tracker.startOffset("topicA", 0));
        assertEquals(100L, tracker.nextLogicalOffset("topicA", 0));
        assertEquals(0L, tracker.startOffset("topicB", 0));
        assertEquals(100L, tracker.nextLogicalOffset("topicB", 0));

        // Backing log untouched (nextBackingOffset unchanged), both sidecars retain all entries.
        assertEquals(backingSizeBefore, backing.get(), "backing log must not have been truncated");
        assertEquals(100L, sidecarFor("topicA", 0).size());
        assertEquals(100L, sidecarFor("topicB", 0).size());
        // Reads in A's "deleted" logical range still resolve to backing offsets — DeleteRecords
        // only advances the visible low-water, it does not remove sidecar entries. The broker is
        // responsible for refusing fetches below startOffset. With the produce pattern above
        // (interleaved A then B), topicA's logical offset i lands at backing offset 2*i.
        assertEquals(2L * 49, sidecarFor("topicA", 0).lookup(49));
        assertEquals(0L, sidecarFor("topicA", 0).lookup(0));
    }

    /**
     * PROMPT scenario 4a. Restart with intact sidecars: a fresh tracker is rehydrated from the
     * sidecar files alone (no backing-log scan). Tracker state matches what was persisted, and
     * the persisted sidecars are unchanged.
     */
    @Test
    public void restartWithIntactSidecarsRehydratesTracker() throws IOException {
        LogicalOffsetTracker original = new LogicalOffsetTracker();
        AtomicLong backing = new AtomicLong(0);
        for (int i = 0; i < 30; i++) {
            produce(original, "topicA", 0, backing);
            produce(original, "topicA", 1, backing);
            produce(original, "topicB", 0, backing);
        }
        closeAllSidecars();

        // Restart: throw away the in-memory tracker, build a new one from the sidecar files.
        LogicalOffsetTracker rebuilt = new LogicalOffsetTracker();
        BackingScanRecoverer freshRecoverer = new BackingScanRecoverer(sidecarDir);
        freshRecoverer.recoverFromSidecars(List.of(
            new LogicalPartition("topicA", 0),
            new LogicalPartition("topicA", 1),
            new LogicalPartition("topicB", 0)
        ), rebuilt);

        assertEquals(30L, rebuilt.nextLogicalOffset("topicA", 0));
        assertEquals(30L, rebuilt.nextLogicalOffset("topicA", 1));
        assertEquals(30L, rebuilt.nextLogicalOffset("topicB", 0));
        // Subsequent produces continue contiguously.
        long postRestartOffset = produce(rebuilt, "topicA", 0, backing);
        assertEquals(30L, postRestartOffset);
    }

    /**
     * PROMPT scenario 4b. Restart without sidecars: wipe the sidecar dir, replay the backing-log
     * scan as a RecoveryRecord stream (which is what the broker constructs by reading record
     * headers in backing-offset order during restart), and verify the rebuilt sidecars and
     * tracker reproduce the original state byte-for-byte.
     */
    @Test
    public void restartWithoutSidecarsReconstructsFromBackingScan() throws IOException {
        LogicalOffsetTracker original = new LogicalOffsetTracker();
        AtomicLong backing = new AtomicLong(0);
        List<RecoveryRecord> auditTrail = new ArrayList<>();

        // Produce some records and remember exactly what landed at each backing offset.
        for (int i = 0; i < 50; i++) {
            long aOff = produce(original, "topicA", 0, backing);
            auditTrail.add(new RecoveryRecord("topicA", 0, aOff, backing.get() - 1));
            long bOff = produce(original, "topicB", 0, backing);
            auditTrail.add(new RecoveryRecord("topicB", 0, bOff, backing.get() - 1));
        }

        // Simulate crash: close and delete every sidecar.
        closeAllSidecars();
        File topicADir = new File(sidecarDir, "topicA");
        File topicBDir = new File(sidecarDir, "topicB");
        for (File dir : new File[] {topicADir, topicBDir}) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File f : files) {
                    if (!f.delete()) throw new IOException("could not delete " + f);
                }
            }
        }

        // Rebuild from the scan.
        LogicalOffsetTracker rebuilt = new LogicalOffsetTracker();
        BackingScanRecoverer freshRecoverer = new BackingScanRecoverer(sidecarDir);
        freshRecoverer.recoverFromScan(auditTrail.iterator(), rebuilt);

        assertEquals(50L, rebuilt.nextLogicalOffset("topicA", 0));
        assertEquals(50L, rebuilt.nextLogicalOffset("topicB", 0));

        // The rebuilt sidecar lookups must match the audit trail exactly.
        try (LogicalSidecarIndex a = freshRecoverer.openSidecar("topicA", 0);
             LogicalSidecarIndex b = freshRecoverer.openSidecar("topicB", 0)) {
            for (RecoveryRecord r : auditTrail) {
                LogicalSidecarIndex s = r.logicalTopic().equals("topicA") ? a : b;
                assertEquals(r.backingOffset(), s.lookup(r.logicalOffset()),
                    "rebuilt sidecar entry for " + r);
            }
        }
    }

    /**
     * Regression test for the latent bug surfaced by the audit fleet (DeleteRecords + restart):
     * before persisted startOffset existed, {@link BackingScanRecoverer#recoverFromSidecars} and
     * {@link BackingScanRecoverer#recoverFromScan} both hard-coded {@code startOffset = 0L}. The
     * tracker's in-memory advance from DeleteRecords would silently regress on every broker
     * bounce — "deleted" records would re-appear to consumers, violating PROMPT scenario 3 ("the
     * sibling logical topic on the same backing still reads its full range; the backing log is
     * not truncated") AND the post-restart contract for acceptance criterion 3.
     *
     * <p>This test exercises both recovery paths. We deliberately advance startOffset on topicA
     * via the kernel facade (which now persists), then restart twice: once via sidecars (cheap
     * path), once via a backing-log scan (full rebuild). In both cases the startOffset must
     * survive. Without the fix, both assertions land at 0L.
     */
    @Test
    public void advanceStartOffsetSurvivesBothRecoveryPaths() throws IOException {
        // Drive everything through the kernel facade so that advanceStartOffset goes through the
        // new persistence path. The kernel needs declared topics before recoverFromDisk will
        // include them in the cheap-path scan.
        LogicalTopicDescriptor descA = new LogicalTopicDescriptor("topicA", 1, "shared", 1);
        LogicalTopicDescriptor descB = new LogicalTopicDescriptor("topicB", 1, "shared", 1);

        try (ConcentrationKernel kernel = new ConcentrationKernel(sidecarDir)) {
            kernel.declare(descA);
            kernel.declare(descB);
            // Produce 100 records to A and 100 to B, interleaved on a shared backing partition.
            // commitProduceBatch requires the exact same array reference returned by reserve —
            // wrap-then-pass is rejected because the tracker checks outstandingBatch == batch.
            AtomicLong backing = new AtomicLong(0);
            for (int i = 0; i < 100; i++) {
                Reservation[] batchA = kernel.reserveProduceBatch("topicA", 0, 1);
                kernel.commitProduceBatch(batchA, backing.getAndIncrement());
                Reservation[] batchB = kernel.reserveProduceBatch("topicB", 0, 1);
                kernel.commitProduceBatch(batchB, backing.getAndIncrement());
            }
            // Advance A's start to 50 — the bug: in-memory the tracker advances, but pre-fix this
            // did NOT touch disk.
            kernel.advanceStartOffset("topicA", 0, 50L);
            assertEquals(50L, kernel.startLogicalOffset("topicA", 0));
            assertEquals(0L, kernel.startLogicalOffset("topicB", 0));
        }

        // Cheap-path recovery: sidecars are intact, just rebuild from them. Before the fix this
        // would silently load startOffset=0 for topicA, resurrecting offsets 0..49 to any
        // consumer — exactly the regression we're guarding against.
        try (ConcentrationKernel rebuilt = new ConcentrationKernel(sidecarDir)) {
            rebuilt.declare(descA);
            rebuilt.declare(descB);
            rebuilt.recoverFromDisk();
            assertEquals(50L, rebuilt.startLogicalOffset("topicA", 0),
                "cheap-path recovery must load the persisted startOffset, not default to 0 — "
                    + "otherwise DeleteRecords silently regresses on every broker restart");
            assertEquals(100L, rebuilt.nextLogicalOffset("topicA", 0),
                "nextLogicalOffset must still match the pre-restart sidecar size");
            // Sibling untouched, as on the original tracker.
            assertEquals(0L, rebuilt.startLogicalOffset("topicB", 0));
            assertEquals(100L, rebuilt.nextLogicalOffset("topicB", 0));
        }

        // Full-rebuild recovery: simulate corrupt/missing sidecars by feeding a synthetic scan
        // stream. The .startoffset file must still be honoured even though the sidecar is being
        // reconstructed from scratch — losing it here would re-expose deleted records the moment
        // the broker decided to take the scan path (e.g. after detecting a torn sidecar tail).
        File topicAdir = new File(sidecarDir, "topicA");
        for (File f : topicAdir.listFiles((d, name) -> name.endsWith(".sidecar"))) {
            assertTrue(f.delete(), "could not delete sidecar to simulate full-scan recovery");
        }
        List<RecoveryRecord> scanStream = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            // Re-stage 100 records as if read off the backing log in backing-offset order. Only
            // topicA is in scope here (we kept topicB's sidecar) — the scan path is partition-by-
            // partition under real conditions too.
            scanStream.add(new RecoveryRecord("topicA", 0, i, 2L * i));
        }
        try (ConcentrationKernel rebuilt2 = new ConcentrationKernel(sidecarDir)) {
            rebuilt2.declare(descA);
            rebuilt2.declare(descB);
            rebuilt2.recoverFromBackingScan(scanStream.iterator());
            assertEquals(50L, rebuilt2.startLogicalOffset("topicA", 0),
                "full-scan recovery must also honour the persisted startOffset — the .startoffset "
                    + "file lives next to the sidecar but is independent of it, so it survives a "
                    + "sidecar wipe");
            assertEquals(100L, rebuilt2.nextLogicalOffset("topicA", 0));
        }
    }

    /**
     * PROMPT scenario 5. Produce to a backing-topic name (bypassing the logical-topic routing
     * front-door) is rejected. The kernel-level signal that the broker glue inverts on is
     * {@link LogicalTopicRegistry#isBackingTopic(String)}; verify it lights up for backings and
     * stays dark for logical names and for unknown names. The actual produce-path rejection
     * lives in the broker hook and cannot be exercised here.
     */
    @Test
    public void registrySignalsThatBackingTopicsCannotBeProducedDirectly() {
        LogicalTopicRegistry registry = new LogicalTopicRegistry();
        registry.declare(new LogicalTopicDescriptor("orders", 100, "shared-backing", 4));
        registry.declare(new LogicalTopicDescriptor("payments", 50, "shared-backing", 4));

        assertTrue(registry.isBackingTopic("shared-backing"));
        assertFalse(registry.isBackingTopic("orders"));
        assertFalse(registry.isBackingTopic("payments"));
        assertFalse(registry.isBackingTopic("unknown-topic"));

        // Both logical topics resolve to the same backing.
        assertEquals("shared-backing", registry.get("orders").orElseThrow().backingTopic());
        assertEquals("shared-backing", registry.get("payments").orElseThrow().backingTopic());

        // And the descriptorsFor index lists both — broker can fan out per backing-partition
        // events to every logical topic riding on it.
        assertEquals(2, registry.descriptorsFor("shared-backing").size());

        // Sanity: namespaces are disjoint — no single name can be both logical AND backing.
        for (String name : List.of("orders", "payments", "shared-backing", "unknown")) {
            assertTrue(!(registry.contains(name) && registry.isBackingTopic(name)),
                "namespace overlap on '" + name + "'");
        }
    }
}
