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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behavioural contract of {@link LogicalSidecarIndex}.
 *
 * <p>One file per (logicalTopic, logicalPartition). Each 8-byte entry stores the backing offset
 * for the next logical offset (positionally encoded — entry i is for logical offset
 * baseLogicalOffset + i). Lookup is O(1) — no binary search needed. PROMPT lesson: "Kafka's
 * existing .index and .timeindex files exist for very good reasons. The custom index should
 * follow their shape, not invent a new one." This implementation mirrors that shape (mmap'd,
 * fixed-size entries, no fsync per append) while exploiting the fact that logical offsets are
 * contiguous to skip the relative-offset / binary-search machinery.
 */
public class LogicalSidecarIndexTest {

    private File tempDir;
    private final List<LogicalSidecarIndex> openIndexes = new ArrayList<>();

    @BeforeEach
    public void setup() throws IOException {
        tempDir = TestUtils.tempDirectory();
    }

    @AfterEach
    public void tearDown() throws IOException {
        for (LogicalSidecarIndex idx : openIndexes) idx.close();
        // tempDir is auto-cleaned by TestUtils on JVM exit
    }

    private LogicalSidecarIndex newIndex(String logicalTopic, int partition) throws IOException {
        File file = new File(tempDir, logicalTopic + "-" + partition + ".sidecar");
        LogicalSidecarIndex idx = new LogicalSidecarIndex(file, logicalTopic, partition);
        openIndexes.add(idx);
        return idx;
    }

    @Test
    public void newIndexIsEmpty() throws IOException {
        LogicalSidecarIndex idx = newIndex("orders", 0);
        assertEquals(0L, idx.size());
        assertEquals(0L, idx.nextLogicalOffset());
    }

    @Test
    public void appendAdvancesSizeAndNextLogicalOffset() throws IOException {
        LogicalSidecarIndex idx = newIndex("orders", 0);
        idx.append(100L);
        idx.append(200L);
        idx.append(305L);
        assertEquals(3L, idx.size());
        assertEquals(3L, idx.nextLogicalOffset());
    }

    @Test
    public void lookupReturnsBackingOffsetForGivenLogicalOffset() throws IOException {
        LogicalSidecarIndex idx = newIndex("orders", 0);
        idx.append(100L);  // logical 0 -> backing 100
        idx.append(205L);  // logical 1 -> backing 205
        idx.append(310L);  // logical 2 -> backing 310
        assertEquals(100L, idx.lookup(0L));
        assertEquals(205L, idx.lookup(1L));
        assertEquals(310L, idx.lookup(2L));
    }

    @Test
    public void lookupOutOfRangeThrows() throws IOException {
        LogicalSidecarIndex idx = newIndex("orders", 0);
        idx.append(100L);
        assertThrows(IndexOutOfBoundsException.class, () -> idx.lookup(-1L));
        assertThrows(IndexOutOfBoundsException.class, () -> idx.lookup(1L));
    }

    @Test
    public void reopenedIndexSeesPreviousAppends() throws IOException {
        File file = new File(tempDir, "orders-0.sidecar");
        try (LogicalSidecarIndex first = new LogicalSidecarIndex(file, "orders", 0)) {
            for (long i = 0; i < 1000; i++) first.append(1000L + i * 7);
        }
        try (LogicalSidecarIndex reopened = new LogicalSidecarIndex(file, "orders", 0)) {
            assertEquals(1000L, reopened.size());
            assertEquals(1000L, reopened.nextLogicalOffset());
            for (long i = 0; i < 1000; i++) {
                assertEquals(1000L + i * 7, reopened.lookup(i));
            }
        }
    }

    @Test
    public void reopenedIndexCanContinueAppending() throws IOException {
        File file = new File(tempDir, "orders-0.sidecar");
        try (LogicalSidecarIndex first = new LogicalSidecarIndex(file, "orders", 0)) {
            first.append(10L);
            first.append(20L);
        }
        try (LogicalSidecarIndex reopened = new LogicalSidecarIndex(file, "orders", 0)) {
            reopened.append(30L);
            assertEquals(3L, reopened.size());
            assertEquals(10L, reopened.lookup(0L));
            assertEquals(20L, reopened.lookup(1L));
            assertEquals(30L, reopened.lookup(2L));
        }
    }

    @Test
    public void rejectsNonMonotonicBackingOffset() throws IOException {
        // Backing offsets must be strictly increasing within a single sidecar — the broker
        // appends them in append order on the backing log, so any non-monotonic value indicates
        // a caller bug.
        LogicalSidecarIndex idx = newIndex("orders", 0);
        idx.append(100L);
        idx.append(200L);
        assertThrows(IllegalArgumentException.class, () -> idx.append(150L));
        assertThrows(IllegalArgumentException.class, () -> idx.append(200L));
    }

    @Test
    public void carriesItsLogicalIdentity() throws IOException {
        LogicalSidecarIndex idx = newIndex("orders", 7);
        assertEquals("orders", idx.logicalTopic());
        assertEquals(7, idx.logicalPartition());
    }

    @Test
    public void growsBeyondInitialAllocation() throws IOException {
        // Sidecar files must grow as needed without losing data. Append a number of entries
        // large enough to force at least one remap if pre-allocation is small.
        LogicalSidecarIndex idx = newIndex("orders", 0);
        final int total = 100_000;
        for (long i = 0; i < total; i++) idx.append(i * 2);
        assertEquals(total, idx.size());
        for (long i = 0; i < total; i += 12345) {
            assertEquals(i * 2, idx.lookup(i));
        }
    }

    @Test
    public void concurrentReadersDoNotInterfereWithEachOther() throws Exception {
        LogicalSidecarIndex idx = newIndex("orders", 0);
        final int n = 10_000;
        for (long i = 0; i < n; i++) idx.append(i * 3 + 1);

        final int readers = 8;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService exec = Executors.newFixedThreadPool(readers);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < readers; t++) {
                futures.add(exec.submit(() -> {
                    start.await();
                    for (long i = 0; i < n; i++) {
                        long expected = i * 3 + 1;
                        if (idx.lookup(i) != expected) {
                            throw new AssertionError("mismatch at " + i);
                        }
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : futures) f.get(30, TimeUnit.SECONDS);
        } finally {
            exec.shutdownNow();
        }
    }

    @Test
    public void closeAndReopenAreIdempotent() throws IOException {
        File file = new File(tempDir, "orders-0.sidecar");
        LogicalSidecarIndex idx = new LogicalSidecarIndex(file, "orders", 0);
        idx.append(42L);
        idx.close();
        idx.close(); // double close is a no-op
        try (LogicalSidecarIndex reopened = new LogicalSidecarIndex(file, "orders", 0)) {
            assertEquals(1L, reopened.size());
            assertEquals(42L, reopened.lookup(0L));
        }
        assertTrue(Files.exists(file.toPath()));
    }

    @Test
    public void truncateRollsBackEntries() throws IOException {
        // Sidecar truncation is used during recovery when the tail of the sidecar is found to
        // be ahead of the backing log (e.g., the broker crashed between appending to the
        // backing log and appending to the sidecar — actually the sidecar lags the log, so the
        // reverse: sidecar must be trimmed to match the durable backing tail).
        LogicalSidecarIndex idx = newIndex("orders", 0);
        for (long i = 0; i < 10; i++) idx.append(i * 10);
        idx.truncateTo(5);
        assertEquals(5L, idx.size());
        assertEquals(5L, idx.nextLogicalOffset());
        for (long i = 0; i < 5; i++) assertEquals(i * 10, idx.lookup(i));
        assertThrows(IndexOutOfBoundsException.class, () -> idx.lookup(5L));

        // After truncation, appending continues from the truncated point.
        idx.append(999L);
        assertEquals(6L, idx.size());
        assertEquals(999L, idx.lookup(5L));
    }

    @Test
    public void openOnTailTornFileFailsLoudly() throws IOException {
        // Simulate a crash mid-append: 2 entries written cleanly, then a 3-byte partial entry.
        // The sidecar must refuse to open and signal that a backing-log rebuild is required,
        // rather than silently misaddress every entry past the torn point.
        File file = new File(tempDir, "torn-0.sidecar");
        try (LogicalSidecarIndex idx = newIndex("torn", 0)) {
            idx.append(10L);
            idx.append(20L);
        }
        // newIndex registered the index; drop the registration so the close in @AfterEach is a no-op.
        openIndexes.clear();

        byte[] existing = Files.readAllBytes(file.toPath());
        byte[] withPartial = new byte[existing.length + 3];
        System.arraycopy(existing, 0, withPartial, 0, existing.length);
        // The trailing 3 bytes are zeros — value doesn't matter, only the unaligned length does.
        Files.write(file.toPath(), withPartial);

        RuntimeException thrown = assertThrows(RuntimeException.class,
            () -> new LogicalSidecarIndex(file, "torn", 0).close());
        // Pin both the local idiom (CorruptIndexException — same family as OffsetIndex /
        // TimeIndex) and that the message identifies the offending file and length.
        assertEquals("org.apache.kafka.storage.internals.log.CorruptIndexException",
            thrown.getClass().getName(),
            "must throw CorruptIndexException (the local Kafka idiom), got " + thrown.getClass());
        assertTrue(thrown.getMessage().contains(file.getName())
                && thrown.getMessage().contains("19")
                && thrown.getMessage().contains("not a multiple"),
            "message must identify file and torn length: " + thrown.getMessage());
    }
}
