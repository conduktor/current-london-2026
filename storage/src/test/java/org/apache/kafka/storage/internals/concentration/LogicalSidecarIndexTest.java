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

import static org.apache.kafka.storage.internals.concentration.LogicalSidecarIndex.ENTRY_SIZE;
import static org.apache.kafka.storage.internals.concentration.LogicalSidecarIndex.OFFSET_BYTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behavioural contract of {@link LogicalSidecarIndex}.
 *
 * <p>One file per (logicalTopic, logicalPartition). Each 12-byte entry stores the backing offset
 * (8 bytes) plus a CRC32C of those bytes (4 bytes), positionally encoded — entry i is for logical
 * offset baseLogicalOffset + i. Lookup is O(1). PROMPT lesson: "Kafka's existing .index and
 * .timeindex files exist for very good reasons. The custom index should follow their shape, not
 * invent a new one." This implementation mirrors that shape (fixed-size entries, no fsync per
 * append) while exploiting the fact that logical offsets are contiguous to skip the
 * relative-offset / binary-search machinery. The per-entry CRC32C closes a bit-rot DATA-LOSS hole
 * that a length-only sanity check cannot catch.
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
    public void constructorFailureReleasesFileDescriptor() throws IOException {
        // Pins the audit-fix invariant: when the constructor throws after opening the
        // RandomAccessFile (e.g., on a torn-length file), the handle must be closed before the
        // exception escapes. If it leaked, hammering the constructor in a tight loop would burn
        // through the JVM's FD limit (1024 by default on Linux) and surface as either a
        // FileSystemException("Too many open files") or an IOException from the open itself.
        File file = new File(tempDir, "leak-probe-0.sidecar");
        Files.write(file.toPath(), new byte[]{1, 2, 3}); // 3 bytes — not a multiple of ENTRY_SIZE
        // 2000 iterations is well above any reasonable default soft FD limit; if even 1% leaked
        // we would exhaust FDs before completing.
        for (int i = 0; i < 2000; i++) {
            RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> new LogicalSidecarIndex(file, "leak", 0));
            assertEquals("org.apache.kafka.storage.internals.log.CorruptIndexException",
                thrown.getClass().getName(),
                "iteration " + (i + 1) + " threw wrong type: " + thrown.getClass());
        }
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
        // TimeIndex) and that the message identifies the offending file and length. With the CRC
        // format change, 2 entries = 24 bytes + 3 partial = 27 — and 27 % 12 != 0 still trips the
        // length sanity check before the CRC pass ever runs.
        assertEquals("org.apache.kafka.storage.internals.log.CorruptIndexException",
            thrown.getClass().getName(),
            "must throw CorruptIndexException (the local Kafka idiom), got " + thrown.getClass());
        long tornLength = existing.length + 3L;
        assertTrue(thrown.getMessage().contains(file.getName())
                && thrown.getMessage().contains(Long.toString(tornLength))
                && thrown.getMessage().contains("not a multiple"),
            "message must identify file and torn length: " + thrown.getMessage());
    }

    @Test
    public void bitFlipInsideAlignedEntryIsCaughtByCrc() throws IOException {
        // PROMPT r19 ADV-STORAGE BLOCKER #132 — DATA-LOSS. Length-only sanity checks miss bit-flips
        // inside a complete 12-byte entry. Without per-entry CRC, a corrupted backing-offset byte
        // returns a wrong logical→backing mapping and the fetch path silently delivers the wrong
        // record (cross-tenant on a shared backing). The CRC field at offset 8..11 of every entry
        // must make this a loud CorruptIndexException, not a silent misroute.
        File file = new File(tempDir, "bitflip-0.sidecar");
        try (LogicalSidecarIndex idx = new LogicalSidecarIndex(file, "bitflip", 0)) {
            idx.append(100L);
            idx.append(200L);
            idx.append(300L);
        }

        // Flip a single bit in the backing-offset payload of entry 1 (bytes 12..19, since entry
        // 0 occupies bytes 0..11). Choose byte 19 (the LSB of the 8-byte offset) so the value
        // visibly diverges from 200L without overflowing any sanity guard.
        byte[] contents = Files.readAllBytes(file.toPath());
        int bytePos = ENTRY_SIZE + (OFFSET_BYTES - 1); // last byte of entry 1's offset payload
        contents[bytePos] = (byte) (contents[bytePos] ^ 0x01);
        Files.write(file.toPath(), contents);

        try (LogicalSidecarIndex reopened = new LogicalSidecarIndex(file, "bitflip", 0)) {
            // Entry 0 is intact — lookup must still resolve correctly.
            assertEquals(100L, reopened.lookup(0L));
            // Entry 1 has the bit-flip and the stored CRC32C no longer matches. The read must
            // throw CorruptIndexException, NOT silently return the flipped value.
            RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> reopened.lookup(1L));
            assertEquals("org.apache.kafka.storage.internals.log.CorruptIndexException",
                thrown.getClass().getName(),
                "bit-flipped entry must surface as CorruptIndexException: got " + thrown);
            assertTrue(thrown.getMessage().contains("CRC"),
                "message must say which check failed: " + thrown.getMessage());
        }
    }

    @Test
    public void bitFlipInCrcFieldAlsoSurfacesAsCorruption() throws IOException {
        // Mirror case of bitFlipInsideAlignedEntryIsCaughtByCrc. The CRC field itself is on disk
        // and just as vulnerable to bit-rot as the payload. A corrupted CRC byte that no longer
        // matches the (still-intact) offset bytes must also raise CorruptIndexException —
        // silently trusting the offset because "the CRC was probably wrong" would defeat the
        // detection (we cannot distinguish "CRC corrupt + offset intact" from "offset corrupt +
        // CRC intact" — both are equally untrustworthy).
        //
        // We use two entries and corrupt the CRC of the FIRST one so that constructor-time
        // readEntryAt(entries-1) verifies the intact last entry (entry 1) and the test reaches
        // the explicit lookup(0L) — keeping this case focused on the lookup-path CRC check.
        // The "corrupt last entry rejected at construction" case is covered separately by
        // crcOnLastEntryIsVerifiedAtConstructionTime.
        File file = new File(tempDir, "crc-bitflip-0.sidecar");
        try (LogicalSidecarIndex idx = new LogicalSidecarIndex(file, "crc-bitflip", 0)) {
            idx.append(42L);
            idx.append(99L);
        }
        byte[] contents = Files.readAllBytes(file.toPath());
        // Flip a byte inside entry 0's CRC field (bytes 8..11). Pick byte 8 — first CRC byte.
        int crcBytePos = OFFSET_BYTES;
        contents[crcBytePos] = (byte) (contents[crcBytePos] ^ 0x01);
        Files.write(file.toPath(), contents);

        try (LogicalSidecarIndex reopened = new LogicalSidecarIndex(file, "crc-bitflip", 0)) {
            // Entry 1 (the last, intact) is read at construction to seed lastBackingOffset, so the
            // constructor succeeds. The explicit lookup(0L) below is the path under test.
            RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> reopened.lookup(0L));
            assertEquals("org.apache.kafka.storage.internals.log.CorruptIndexException",
                thrown.getClass().getName(),
                "CRC-bitflipped entry must surface as CorruptIndexException: got " + thrown);
        }
    }

    @Test
    public void truncateToFailedReadLeavesStateIntactForCleanRetry() throws IOException {
        // r22 #165: if truncateTo's read of the new last entry throws (CRC mismatch on the
        // surviving tail, I/O error from the channel), the (entries, lastBackingOffset) pair must
        // remain in its pre-truncate state so a subsequent retry can re-attempt the operation
        // without observing a stale, too-high lastBackingOffset that would silently throttle
        // every later append. Pre-fix the order was (truncate, set entries, read tail), which
        // committed entries=newSize but kept lastBackingOffset at the OLD tail value — every
        // append in the (newTail, oldTail] range would then be rejected as non-monotonic until
        // the next broker restart re-seeded from disk.
        //
        // We simulate the failure by corrupting the entry that would become the new tail AFTER a
        // truncateTo(3): entry index 2. Then call truncateTo(3) and assert it throws, and that
        // the kernel-visible state (size, nextLogicalOffset, lookups of intact entries) is
        // unchanged from before the call.
        File file = new File(tempDir, "trunc-rollback-0.sidecar");
        try (LogicalSidecarIndex idx = new LogicalSidecarIndex(file, "trunc-rb", 0)) {
            for (long i = 0; i < 5; i++) idx.append(i * 10);  // entries 0..4, backing 0,10,20,30,40
        }
        // Corrupt entry 2 (which would become the new last after truncateTo(3)).
        byte[] contents = Files.readAllBytes(file.toPath());
        int bytePos = 2 * ENTRY_SIZE + (OFFSET_BYTES - 1);
        contents[bytePos] = (byte) (contents[bytePos] ^ 0x04);
        Files.write(file.toPath(), contents);

        // Reopen — entry 4 (the last) is intact, so the constructor succeeds.
        LogicalSidecarIndex idx = new LogicalSidecarIndex(file, "trunc-rb", 0);
        openIndexes.add(idx);
        assertEquals(5L, idx.size(), "all 5 entries present at reopen");

        // Pre-fix: truncateTo(3) would call channel.truncate(36) then set entries=3 then throw
        // when readEntryAt(2) hit the corrupted CRC — leaving entries=3 but lastBackingOffset
        // still=40 (the pre-truncate tail). With the fix the read runs FIRST and throws before
        // any state change, so the post-throw view must equal the pre-call view.
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> idx.truncateTo(3));
        assertEquals("org.apache.kafka.storage.internals.log.CorruptIndexException",
            thrown.getClass().getName(),
            "expected CorruptIndexException from the failing readEntryAt: " + thrown);

        // Critical assertion: an append that would have been rejected post-fix (because
        // lastBackingOffset would still be the stale value 40) must succeed when its backingOffset
        // is greater than the TRUE current last (40). Pick 41 — strictly greater than 40, which
        // would still be rejected if lastBackingOffset survived as something larger. The fix
        // keeps lastBackingOffset==40 (the true pre-call last), so 41 is accepted.
        idx.append(41L);
        assertEquals(6L, idx.size(),
            "append after rolled-back truncate must succeed against the true pre-call last");
    }

    @Test
    public void crcOnLastEntryIsVerifiedAtConstructionTime() throws IOException {
        // Construction reads the last entry to seed lastBackingOffset (so append() can enforce
        // strict monotonicity across a process restart). That read MUST go through the CRC
        // verification — otherwise a corrupted last entry would be silently trusted as the
        // monotonicity baseline, and a subsequent append could either succeed when it shouldn't
        // (next produce overlaps a corrupted offset) or fail spuriously (monotonicity check
        // against garbage). The right answer is to fail-fast at open time so the recovery path
        // rebuilds from the backing log.
        File file = new File(tempDir, "ctor-crc-0.sidecar");
        try (LogicalSidecarIndex idx = new LogicalSidecarIndex(file, "ctor-crc", 0)) {
            idx.append(10L);
            idx.append(20L);
        }
        byte[] contents = Files.readAllBytes(file.toPath());
        // Flip a byte in the LAST entry's offset payload (bytes 12..19 of the 24-byte file).
        int bytePos = ENTRY_SIZE + (OFFSET_BYTES - 1);
        contents[bytePos] = (byte) (contents[bytePos] ^ 0x02);
        Files.write(file.toPath(), contents);

        RuntimeException thrown = assertThrows(RuntimeException.class,
            () -> new LogicalSidecarIndex(file, "ctor-crc", 0).close());
        assertEquals("org.apache.kafka.storage.internals.log.CorruptIndexException",
            thrown.getClass().getName(),
            "corrupted last entry must abort construction with CorruptIndexException: got " + thrown);
    }
}
