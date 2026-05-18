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
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behavioural contract of {@link LogicalFetchTranslator} — the consume-side mirror of
 * {@link LogicalProduceStamper}. The translator takes a {@link MemoryRecords} fetched from a
 * backing partition (where records from multiple logical topics may be interleaved) and returns
 * a {@link MemoryRecords} containing ONLY the records for the targeted logical topic, with
 * offsets rewritten to their logical values and the two concentration headers stripped.
 *
 * <p>What's pinned here:
 * <ul>
 *   <li>Per-logical-topic isolation: a consumer of A never sees records from B.</li>
 *   <li>Offset rewrite: the surviving records carry logical offsets, never backing offsets.</li>
 *   <li>Header strip: the consumer never sees the concentration metadata headers, but user
 *       headers, key, value, and timestamp pass through verbatim.</li>
 *   <li>Edge cases: empty input, no-match input, missing or corrupt concentration headers.</li>
 * </ul>
 */
public class LogicalFetchTranslatorTest {

    @Test
    public void translateFiltersOutOtherLogicalTopics() {
        // PROMPT acceptance criterion: "Records from two logical topics interleaved on the same
        // backing partition are addressable independently — a consumer of logical topic A never
        // receives records from logical topic B, regardless of physical interleaving."
        MemoryRecords mixed = backingRecordsFromInterleaved(
            // Records as: (logicalTopic, logicalOffset, key, value)
            new InterleavedRecord("orders", 10L, "k0", "order-0"),
            new InterleavedRecord("events", 5L,  "k1", "event-0"),
            new InterleavedRecord("orders", 11L, "k2", "order-1"),
            new InterleavedRecord("events", 6L,  "k3", "event-1"),
            new InterleavedRecord("orders", 12L, "k4", "order-2"));

        MemoryRecords ordersView = LogicalFetchTranslator.translate(mixed, "orders");

        List<Record> records = collectRecords(ordersView);
        assertEquals(3, records.size(), "consumer of 'orders' must see only orders' records");
        assertArrayEquals("order-0".getBytes(), bytes(records.get(0).value()));
        assertArrayEquals("order-1".getBytes(), bytes(records.get(1).value()));
        assertArrayEquals("order-2".getBytes(), bytes(records.get(2).value()));
        // And of course the events records are absent — pin this explicitly to guard against a
        // future regression that "leaks through" if a header check ever shifts.
        for (Record r : records) {
            byte[] value = bytes(r.value());
            assertFalse(new String(value, StandardCharsets.UTF_8).startsWith("event-"),
                "event-* records must not leak into the orders view");
        }
    }

    @Test
    public void translateRewritesOffsetsToLogicalValues() {
        // PROMPT acceptance criterion: "Producer callbacks and consumer records receive logical
        // offsets, never physical."
        MemoryRecords mixed = backingRecordsFromInterleaved(
            new InterleavedRecord("orders", 100L, "k0", "v0"),
            new InterleavedRecord("events", 999L, "k1", "vX"), // filtered out
            new InterleavedRecord("orders", 101L, "k2", "v1"),
            new InterleavedRecord("orders", 102L, "k3", "v2"));

        MemoryRecords ordersView = LogicalFetchTranslator.translate(mixed, "orders");

        List<Record> records = collectRecords(ordersView);
        assertEquals(3, records.size());
        assertEquals(100L, records.get(0).offset());
        assertEquals(101L, records.get(1).offset());
        assertEquals(102L, records.get(2).offset());
        // Output batch baseOffset == first surviving record's logical offset.
        RecordBatch batch = ordersView.batches().iterator().next();
        assertEquals(100L, batch.baseOffset());
    }

    @Test
    public void translateStripsConcentrationHeaders() {
        // Concentration metadata headers are internal — a stock consumer must not see them. User
        // headers must survive verbatim alongside the strip.
        Header userHeader = new RecordHeader("x-trace-id", "trace-99".getBytes(StandardCharsets.UTF_8));
        MemoryRecords mixed = backingRecordsFromInterleaved(
            new InterleavedRecord("orders", 0L, "k", "payload", new Header[] {userHeader}));

        MemoryRecords ordersView = LogicalFetchTranslator.translate(mixed, "orders");

        Record r = collectRecords(ordersView).get(0);
        Header[] hs = r.headers();
        assertEquals(1, hs.length, "concentration headers must be stripped; user header kept");
        assertEquals("x-trace-id", hs[0].key());
        assertArrayEquals("trace-99".getBytes(StandardCharsets.UTF_8), hs[0].value());
        // Defensive: explicitly check no concentration header survives.
        for (Header h : hs) {
            assertFalse(ConcentrationHeaders.LOGICAL_TOPIC_HEADER.equals(h.key()),
                "logical-topic header must not leak to consumer");
            assertFalse(ConcentrationHeaders.LOGICAL_PARTITION_HEADER.equals(h.key()),
                "logical-partition header must not leak to consumer");
            assertFalse(ConcentrationHeaders.LOGICAL_OFFSET_HEADER.equals(h.key()),
                "logical-offset header must not leak to consumer");
        }
    }

    @Test
    public void translatePreservesKeyValueAndTimestamp() {
        long ts = 1_700_000_000_000L;
        MemoryRecords mixed = backingRecordsFromInterleaved(
            new InterleavedRecord("orders", 42L, "the-key", "the-value", ts));

        MemoryRecords ordersView = LogicalFetchTranslator.translate(mixed, "orders");

        Record r = collectRecords(ordersView).get(0);
        assertEquals(ts, r.timestamp());
        assertArrayEquals("the-key".getBytes(), bytes(r.key()));
        assertArrayEquals("the-value".getBytes(), bytes(r.value()));
    }

    @Test
    public void translateOnEmptyInputReturnsEmpty() {
        MemoryRecords out = LogicalFetchTranslator.translate(MemoryRecords.EMPTY, "orders");
        assertEquals(0, out.sizeInBytes());
        assertFalse(out.batches().iterator().hasNext());
    }

    @Test
    public void translateOnNoMatchingTopicReturnsEmpty() {
        MemoryRecords mixed = backingRecordsFromInterleaved(
            new InterleavedRecord("events", 0L, "k0", "v0"),
            new InterleavedRecord("events", 1L, "k1", "v1"));

        MemoryRecords out = LogicalFetchTranslator.translate(mixed, "orders");
        assertEquals(0, collectRecords(out).size());
    }

    @Test
    public void translateSkipsRecordsWithMissingLogicalOffsetHeader() {
        // Defensive: a corrupt record (missing logical-offset header) must be skipped, not crash
        // the translator. This shouldn't happen in production since the produce stamper always
        // emits both headers, but a backing-scan recovery could in principle race a stamper
        // rewrite. Survive rather than abort.
        MemoryRecords good = backingRecordsFromInterleaved(
            new InterleavedRecord("orders", 7L, "k", "good"));
        MemoryRecords corrupt = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord(RecordBatch.NO_TIMESTAMP, "k".getBytes(), "corrupt".getBytes(),
                new Header[] {
                    new RecordHeader(ConcentrationHeaders.LOGICAL_TOPIC_HEADER,
                        "orders".getBytes(StandardCharsets.UTF_8))
                    // intentionally: NO logical-offset header
                }));

        // Concatenate the two batches.
        ByteBuffer joined = ByteBuffer.allocate(good.sizeInBytes() + corrupt.sizeInBytes());
        joined.put(good.buffer().duplicate());
        joined.put(corrupt.buffer().duplicate());
        joined.flip();
        MemoryRecords mixed = MemoryRecords.readableRecords(joined);

        MemoryRecords out = LogicalFetchTranslator.translate(mixed, "orders");

        List<Record> records = collectRecords(out);
        assertEquals(1, records.size(), "only the well-formed orders record should survive");
        assertArrayEquals("good".getBytes(), bytes(records.get(0).value()));
    }

    @Test
    public void translateHandlesNullKeyAndNullValue() {
        // Round-robin producers commonly send null-key records. Null-value (tombstone) records
        // are rare on non-compacted topics — v1 backings are non-compacted — but a defensive
        // pass-through costs us nothing and avoids surprising a producer that emits one.
        MemoryRecords mixed = backingRecordsFromInterleaved(
            new InterleavedRecord("orders", 0L, null, "value-only"),
            new InterleavedRecord("orders", 1L, "key-only", null));

        MemoryRecords out = LogicalFetchTranslator.translate(mixed, "orders");

        List<Record> records = collectRecords(out);
        assertEquals(2, records.size());
        assertNull(records.get(0).key());
        assertArrayEquals("value-only".getBytes(), bytes(records.get(0).value()));
        assertArrayEquals("key-only".getBytes(), bytes(records.get(1).key()));
        assertNull(records.get(1).value());
    }

    @Test
    public void translateRejectsNullArguments() {
        MemoryRecords mr = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("a".getBytes()));
        assertThrows(NullPointerException.class,
            () -> LogicalFetchTranslator.translate(null, "orders"));
        assertThrows(NullPointerException.class,
            () -> LogicalFetchTranslator.translate(mr, null));
    }

    @Test
    public void translateRoundTripsThroughStamper() {
        // Cross-check: a batch stamped by LogicalProduceStamper.stamp("orders", offsets) must
        // round-trip through LogicalFetchTranslator.translate(_, "orders") with offsets and
        // payloads intact. Pins that the two halves agree on the on-wire format — this guards
        // against a header-format drift between produce-time and consume-time.
        MemoryRecords original = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord("k0".getBytes(), "payload-0".getBytes()),
            new SimpleRecord("k1".getBytes(), "payload-1".getBytes()),
            new SimpleRecord("k2".getBytes(), "payload-2".getBytes()));

        MemoryRecords stamped = LogicalProduceStamper.stamp(original, "orders", 0,
            new long[] {100L, 101L, 102L});
        MemoryRecords translated = LogicalFetchTranslator.translate(stamped, "orders");

        List<Record> records = collectRecords(translated);
        assertEquals(3, records.size());
        for (int i = 0; i < 3; i++) {
            assertEquals(100L + i, records.get(i).offset());
            assertArrayEquals(("k" + i).getBytes(), bytes(records.get(i).key()));
            assertArrayEquals(("payload-" + i).getBytes(), bytes(records.get(i).value()));
            assertEquals(0, records.get(i).headers().length, "no headers should survive");
        }
    }

    @Test
    public void translateSeparatesThreeLogicalTopicsOnOneBackingPartition() {
        // PROMPT functional scenario: three logical topics share one backing partition. Each
        // consumer must see only its own topic with monotonic logical offsets.
        MemoryRecords mixed = backingRecordsFromInterleaved(
            new InterleavedRecord("A", 0L, "ka0", "a-0"),
            new InterleavedRecord("B", 0L, "kb0", "b-0"),
            new InterleavedRecord("C", 0L, "kc0", "c-0"),
            new InterleavedRecord("A", 1L, "ka1", "a-1"),
            new InterleavedRecord("B", 1L, "kb1", "b-1"),
            new InterleavedRecord("A", 2L, "ka2", "a-2"),
            new InterleavedRecord("C", 1L, "kc1", "c-1"));

        MemoryRecords viewA = LogicalFetchTranslator.translate(mixed, "A");
        MemoryRecords viewB = LogicalFetchTranslator.translate(mixed, "B");
        MemoryRecords viewC = LogicalFetchTranslator.translate(mixed, "C");

        List<Record> recordsA = collectRecords(viewA);
        List<Record> recordsB = collectRecords(viewB);
        List<Record> recordsC = collectRecords(viewC);

        assertEquals(3, recordsA.size());
        assertEquals(2, recordsB.size());
        assertEquals(2, recordsC.size());

        // Each consumer sees monotonic logical offsets starting at zero.
        for (int i = 0; i < recordsA.size(); i++) assertEquals(i, recordsA.get(i).offset());
        for (int i = 0; i < recordsB.size(); i++) assertEquals(i, recordsB.get(i).offset());
        for (int i = 0; i < recordsC.size(); i++) assertEquals(i, recordsC.get(i).offset());

        // No payload leakage across topic views.
        for (Record r : recordsA) assertTrue(new String(bytes(r.value())).startsWith("a-"));
        for (Record r : recordsB) assertTrue(new String(bytes(r.value())).startsWith("b-"));
        for (Record r : recordsC) assertTrue(new String(bytes(r.value())).startsWith("c-"));
    }

    // ---- helpers ----

    private static class InterleavedRecord {
        final String logicalTopic;
        final long logicalOffset;
        final byte[] key;
        final byte[] value;
        final long timestamp;
        final Header[] userHeaders;

        InterleavedRecord(String logicalTopic, long logicalOffset, String key, String value) {
            this(logicalTopic, logicalOffset, key, value, RecordBatch.NO_TIMESTAMP, new Header[0]);
        }

        InterleavedRecord(String logicalTopic, long logicalOffset, String key, String value, long timestamp) {
            this(logicalTopic, logicalOffset, key, value, timestamp, new Header[0]);
        }

        InterleavedRecord(String logicalTopic, long logicalOffset, String key, String value,
                          Header[] userHeaders) {
            this(logicalTopic, logicalOffset, key, value, RecordBatch.NO_TIMESTAMP, userHeaders);
        }

        InterleavedRecord(String logicalTopic, long logicalOffset, String key, String value,
                          long timestamp, Header[] userHeaders) {
            this.logicalTopic = logicalTopic;
            this.logicalOffset = logicalOffset;
            this.key = key == null ? null : key.getBytes(StandardCharsets.UTF_8);
            this.value = value == null ? null : value.getBytes(StandardCharsets.UTF_8);
            this.timestamp = timestamp;
            this.userHeaders = userHeaders;
        }
    }

    /**
     * Build a backing-style MemoryRecords by stamping each input as if it had been written by
     * a separate produce call to the named logical topic. We construct one stamped batch per
     * record and concatenate, so the result mirrors the wire shape of a backing partition that
     * has had multiple logical-topic produces interleaved on it.
     */
    private static MemoryRecords backingRecordsFromInterleaved(InterleavedRecord... records) {
        List<MemoryRecords> stampedBatches = new ArrayList<>(records.length);
        int total = 0;
        for (InterleavedRecord ir : records) {
            MemoryRecords single = MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(ir.timestamp, ir.key, ir.value, ir.userHeaders));
            MemoryRecords stamped = LogicalProduceStamper.stamp(single, ir.logicalTopic, 0, new long[] {ir.logicalOffset});
            stampedBatches.add(stamped);
            total += stamped.sizeInBytes();
        }
        ByteBuffer joined = ByteBuffer.allocate(total);
        for (MemoryRecords mr : stampedBatches) {
            joined.put(mr.buffer().duplicate());
        }
        joined.flip();
        return MemoryRecords.readableRecords(joined);
    }

    private static List<Record> collectRecords(MemoryRecords records) {
        List<Record> out = new ArrayList<>();
        for (RecordBatch batch : records.batches()) {
            Iterator<Record> it = batch.iterator();
            while (it.hasNext()) out.add(it.next());
        }
        return out;
    }

    private static byte[] bytes(ByteBuffer buf) {
        if (buf == null) return null;
        byte[] arr = new byte[buf.remaining()];
        buf.duplicate().get(arr);
        return arr;
    }
}
