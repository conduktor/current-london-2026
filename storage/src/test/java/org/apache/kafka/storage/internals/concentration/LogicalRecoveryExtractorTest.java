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

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behavioural contract of {@link LogicalRecoveryExtractor}. The extractor consumes a backing-log
 * page and emits {@link RecoveryRecord}s for the (logicalTopic, logicalPartition) subset that
 * still needs recovery. Pins:
 *
 * <ul>
 *   <li>Round-trips with {@link LogicalProduceStamper} — the produce-time wire format and the
 *       recovery-time decode agree byte-for-byte.</li>
 *   <li>Skips records whose logical identity isn't in the filter set (so already-recovered
 *       sidecars are not clobbered).</li>
 *   <li>Skips malformed / unstamped records without aborting the scan.</li>
 *   <li>Preserves input record order so the backing-offset sequence stays monotonic.</li>
 * </ul>
 */
public class LogicalRecoveryExtractorTest {

    @Test
    public void extractRoundTripsThroughStamper() {
        MemoryRecords source = MemoryRecords.withRecords(0L, Compression.NONE,
            new SimpleRecord("k0".getBytes(), "v0".getBytes()),
            new SimpleRecord("k1".getBytes(), "v1".getBytes()),
            new SimpleRecord("k2".getBytes(), "v2".getBytes()));
        MemoryRecords stamped = LogicalProduceStamper.stamp(source, "orders", 5, new long[] {100L, 101L, 102L});

        List<RecoveryRecord> got = LogicalRecoveryExtractor.extract(stamped,
            Collections.singleton(new LogicalPartition("orders", 5)));

        assertEquals(3, got.size());
        for (int i = 0; i < 3; i++) {
            assertEquals("orders", got.get(i).logicalTopic());
            assertEquals(5, got.get(i).logicalPartition());
            assertEquals(100L + i, got.get(i).logicalOffset());
            assertEquals(i, got.get(i).backingOffset(),
                "backing offset must be the record offset on the backing log");
        }
    }

    @Test
    public void extractDropsPartitionsNotInRecoveryFilter() {
        // Two logical topics share a backing partition. Caller signals only "orders-3" needs
        // recovery; the events records must NOT appear in the output.
        MemoryRecords ordersBatch = LogicalProduceStamper.stamp(
            MemoryRecords.withRecords(0L, Compression.NONE,
                new SimpleRecord("o0".getBytes(), "o0v".getBytes())),
            "orders", 3, new long[] {0L});
        MemoryRecords eventsBatch = LogicalProduceStamper.stamp(
            MemoryRecords.withRecords(1L, Compression.NONE,
                new SimpleRecord("e0".getBytes(), "e0v".getBytes())),
            "events", 0, new long[] {0L});
        MemoryRecords joined = concat(ordersBatch, eventsBatch);

        List<RecoveryRecord> got = LogicalRecoveryExtractor.extract(joined,
            Collections.singleton(new LogicalPartition("orders", 3)));

        assertEquals(1, got.size());
        assertEquals("orders", got.get(0).logicalTopic());
        assertEquals(3, got.get(0).logicalPartition());
    }

    @Test
    public void extractSkipsRecordsMissingHeaders() {
        // A record on the backing log without concentration headers (e.g. produced before the
        // feature was enabled) must be skipped, not abort the scan or be emitted as garbage.
        MemoryRecords unstamped = MemoryRecords.withRecords(0L, Compression.NONE,
            new SimpleRecord("k".getBytes(), "v".getBytes()));
        MemoryRecords goodStamped = LogicalProduceStamper.stamp(
            MemoryRecords.withRecords(1L, Compression.NONE,
                new SimpleRecord("k2".getBytes(), "v2".getBytes())),
            "orders", 0, new long[] {0L});
        MemoryRecords mixed = concat(unstamped, goodStamped);

        List<RecoveryRecord> got = LogicalRecoveryExtractor.extract(mixed,
            Collections.singleton(new LogicalPartition("orders", 0)));

        assertEquals(1, got.size());
        assertEquals("orders", got.get(0).logicalTopic());
        assertEquals(1L, got.get(0).backingOffset(),
            "backing offset of the surviving record must reflect its position on the backing log");
    }

    @Test
    public void extractSkipsPartiallyStampedRecord() {
        // A record with only LOGICAL_TOPIC_HEADER (no partition, no offset) is malformed and must
        // be skipped silently. Mirrors what the fetch translator does for the same shape.
        Header topicOnly = new RecordHeader(
            ConcentrationHeaders.LOGICAL_TOPIC_HEADER,
            "orders".getBytes(StandardCharsets.UTF_8));
        MemoryRecords malformed = MemoryRecords.withRecords(0L, Compression.NONE,
            new SimpleRecord(0L, "k".getBytes(), "v".getBytes(), new Header[] {topicOnly}));

        List<RecoveryRecord> got = LogicalRecoveryExtractor.extract(malformed,
            Collections.singleton(new LogicalPartition("orders", 0)));

        assertTrue(got.isEmpty());
    }

    @Test
    public void extractIgnoresWrongSizedPartitionHeader() {
        // Defensive: a logical-partition header that isn't exactly 4 bytes is treated as missing
        // rather than misdecoded. Prevents a header-format drift from silently misassigning
        // records to the wrong logical partition.
        Header topic = new RecordHeader(
            ConcentrationHeaders.LOGICAL_TOPIC_HEADER,
            "orders".getBytes(StandardCharsets.UTF_8));
        Header partition = new RecordHeader(
            ConcentrationHeaders.LOGICAL_PARTITION_HEADER,
            new byte[] {0x00, 0x00}); // 2 bytes, not 4
        Header offset = new RecordHeader(
            ConcentrationHeaders.LOGICAL_OFFSET_HEADER,
            ByteBuffer.allocate(Long.BYTES).putLong(0L).array());
        MemoryRecords malformed = MemoryRecords.withRecords(0L, Compression.NONE,
            new SimpleRecord(0L, "k".getBytes(), "v".getBytes(),
                new Header[] {topic, partition, offset}));

        List<RecoveryRecord> got = LogicalRecoveryExtractor.extract(malformed,
            Collections.singleton(new LogicalPartition("orders", 0)));

        assertTrue(got.isEmpty());
    }

    @Test
    public void extractIgnoresWrongSizedOffsetHeader() {
        Header topic = new RecordHeader(
            ConcentrationHeaders.LOGICAL_TOPIC_HEADER,
            "orders".getBytes(StandardCharsets.UTF_8));
        Header partition = new RecordHeader(
            ConcentrationHeaders.LOGICAL_PARTITION_HEADER,
            ByteBuffer.allocate(Integer.BYTES).putInt(0).array());
        Header offset = new RecordHeader(
            ConcentrationHeaders.LOGICAL_OFFSET_HEADER,
            new byte[] {0x00, 0x01, 0x02, 0x03}); // 4 bytes, not 8
        MemoryRecords malformed = MemoryRecords.withRecords(0L, Compression.NONE,
            new SimpleRecord(0L, "k".getBytes(), "v".getBytes(),
                new Header[] {topic, partition, offset}));

        List<RecoveryRecord> got = LogicalRecoveryExtractor.extract(malformed,
            Collections.singleton(new LogicalPartition("orders", 0)));

        assertTrue(got.isEmpty());
    }

    @Test
    public void extractPreservesInputOrder() {
        // Backing-offset monotonicity is the invariant BackingScanRecoverer enforces; the
        // extractor must therefore not reorder. Build three batches with interleaved logical
        // partitions on the same backing topic and verify their backing offsets come out
        // strictly increasing.
        MemoryRecords b0 = LogicalProduceStamper.stamp(
            MemoryRecords.withRecords(0L, Compression.NONE,
                new SimpleRecord("a".getBytes(), "a".getBytes())),
            "orders", 0, new long[] {0L});
        MemoryRecords b1 = LogicalProduceStamper.stamp(
            MemoryRecords.withRecords(1L, Compression.NONE,
                new SimpleRecord("b".getBytes(), "b".getBytes())),
            "orders", 1, new long[] {0L});
        MemoryRecords b2 = LogicalProduceStamper.stamp(
            MemoryRecords.withRecords(2L, Compression.NONE,
                new SimpleRecord("c".getBytes(), "c".getBytes())),
            "orders", 0, new long[] {1L});
        MemoryRecords joined = concat(b0, b1, b2);

        List<RecoveryRecord> got = LogicalRecoveryExtractor.extract(joined,
            Set.of(new LogicalPartition("orders", 0), new LogicalPartition("orders", 1)));

        assertEquals(3, got.size());
        assertEquals(0L, got.get(0).backingOffset());
        assertEquals(1L, got.get(1).backingOffset());
        assertEquals(2L, got.get(2).backingOffset());
    }

    @Test
    public void extractEmptyFilterReturnsEmpty() {
        // Caller has nothing to recover — the extractor must short-circuit without iterating
        // records (avoid unnecessary I/O on a hot startup path).
        MemoryRecords stamped = LogicalProduceStamper.stamp(
            MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord("k".getBytes())),
            "orders", 0, new long[] {0L});
        assertTrue(LogicalRecoveryExtractor.extract(stamped, Collections.emptyList()).isEmpty());
    }

    @Test
    public void extractRejectsNullArguments() {
        MemoryRecords stamped = MemoryRecords.EMPTY;
        assertThrows(NullPointerException.class,
            () -> LogicalRecoveryExtractor.extract(null, Collections.emptyList()));
        assertThrows(NullPointerException.class,
            () -> LogicalRecoveryExtractor.extract(stamped, null));
    }

    private static MemoryRecords concat(MemoryRecords... batches) {
        int total = 0;
        for (MemoryRecords mr : batches) total += mr.sizeInBytes();
        ByteBuffer joined = ByteBuffer.allocate(total);
        for (MemoryRecords mr : batches) joined.put(mr.buffer().duplicate());
        joined.flip();
        return MemoryRecords.readableRecords(joined);
    }
}
