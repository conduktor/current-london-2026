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
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;

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
 * Behavioural contract of {@link LogicalProduceStamper}. The stamper rebuilds a
 * {@link MemoryRecords} batch produced against a logical topic, adding two headers per record:
 * the logical topic name and the per-logical-topic offset assigned by
 * {@link LogicalOffsetTracker}. This is the produce-side glue that lets the fetch path and the
 * backing-scan recovery path identify which logical topic each record belongs to.
 *
 * <p>What's pinned here:
 * <ul>
 *   <li>Header values are correct and addressable.</li>
 *   <li>Original payload, key, timestamp, headers, and idempotent-producer fields survive
 *       intact.</li>
 *   <li>Misuse (mismatched count, empty input, multi-batch input) fails loudly rather than
 *       silently corrupting the batch.</li>
 * </ul>
 */
public class LogicalProduceStamperTest {

    @Test
    public void stampAddsAllThreeHeadersToEveryRecord() {
        MemoryRecords source = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord("k1".getBytes(), "v1".getBytes()),
            new SimpleRecord("k2".getBytes(), "v2".getBytes()),
            new SimpleRecord("k3".getBytes(), "v3".getBytes()));

        MemoryRecords stamped = LogicalProduceStamper.stamp(source, "orders", 7, new long[] {10L, 11L, 12L});

        List<Record> records = collectRecords(stamped);
        assertEquals(3, records.size());
        for (int i = 0; i < 3; i++) {
            Header[] hs = records.get(i).headers();
            // Stamped headers come after the source's original headers (here, none) in stamp
            // order: topic, partition, offset.
            assertEquals(ConcentrationHeaders.LOGICAL_TOPIC_HEADER, hs[hs.length - 3].key());
            assertEquals("orders", new String(hs[hs.length - 3].value(), StandardCharsets.UTF_8));
            assertEquals(ConcentrationHeaders.LOGICAL_PARTITION_HEADER, hs[hs.length - 2].key());
            assertEquals(7, ByteBuffer.wrap(hs[hs.length - 2].value()).getInt());
            assertEquals(ConcentrationHeaders.LOGICAL_OFFSET_HEADER, hs[hs.length - 1].key());
            assertEquals(10L + i, ByteBuffer.wrap(hs[hs.length - 1].value()).getLong());
        }
    }

    @Test
    public void stampPreservesOriginalHeadersInOrder() {
        // PROMPT acceptance: idempotent producer state (which lives partly in headers) must
        // survive the rewrite. Even non-idempotent user headers must come back unchanged.
        Header h1 = new RecordHeader("x-request-id", "abc-123".getBytes(StandardCharsets.UTF_8));
        Header h2 = new RecordHeader("x-trace", "trace-1".getBytes(StandardCharsets.UTF_8));
        MemoryRecords source = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord(System.currentTimeMillis(), "k".getBytes(), "v".getBytes(),
                new Header[] {h1, h2}));

        MemoryRecords stamped = LogicalProduceStamper.stamp(source, "orders", 3, new long[] {0L});

        Record r = collectRecords(stamped).get(0);
        Header[] hs = r.headers();
        assertEquals(5, hs.length, "expected 2 original + 3 concentration headers");
        assertEquals("x-request-id", hs[0].key());
        assertArrayEquals("abc-123".getBytes(StandardCharsets.UTF_8), hs[0].value());
        assertEquals("x-trace", hs[1].key());
        assertArrayEquals("trace-1".getBytes(StandardCharsets.UTF_8), hs[1].value());
        assertEquals(ConcentrationHeaders.LOGICAL_TOPIC_HEADER, hs[2].key());
        assertEquals(ConcentrationHeaders.LOGICAL_PARTITION_HEADER, hs[3].key());
        assertEquals(3, ByteBuffer.wrap(hs[3].value()).getInt());
        assertEquals(ConcentrationHeaders.LOGICAL_OFFSET_HEADER, hs[4].key());
    }

    @Test
    public void stampPreservesKeyValueAndTimestamp() {
        long ts = 1_700_000_000_000L;
        MemoryRecords source = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord(ts, "key-a".getBytes(), "value-a".getBytes()));

        MemoryRecords stamped = LogicalProduceStamper.stamp(source, "orders", 0, new long[] {42L});

        Record r = collectRecords(stamped).get(0);
        assertEquals(ts, r.timestamp());
        assertArrayEquals("key-a".getBytes(), bytes(r.key()));
        assertArrayEquals("value-a".getBytes(), bytes(r.value()));
    }

    @Test
    public void stampPreservesIdempotentProducerFields() {
        // PROMPT acceptance criterion: "Idempotent producer state (producer id, epoch, sequence)
        // is preserved across the logical/physical translation."
        long producerId = 4242L;
        short producerEpoch = 7;
        int baseSequence = 100;
        long baseOffset = 0L;
        boolean isTransactional = false;

        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder b = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE,
            Compression.NONE, TimestampType.CREATE_TIME, baseOffset, RecordBatch.NO_TIMESTAMP,
            producerId, producerEpoch, baseSequence, isTransactional, false,
            RecordBatch.NO_PARTITION_LEADER_EPOCH);
        b.appendWithOffset(0L, new SimpleRecord(1000L, "k1".getBytes(), "v1".getBytes()));
        b.appendWithOffset(1L, new SimpleRecord(1001L, "k2".getBytes(), "v2".getBytes()));
        MemoryRecords source = b.build();

        MemoryRecords stamped = LogicalProduceStamper.stamp(source, "orders", 1, new long[] {0L, 1L});

        RecordBatch outBatch = stamped.batches().iterator().next();
        assertEquals(producerId, outBatch.producerId());
        assertEquals(producerEpoch, outBatch.producerEpoch());
        assertEquals(baseSequence, outBatch.baseSequence());
        assertFalse(outBatch.isTransactional());
        assertEquals(baseOffset, outBatch.baseOffset());
    }

    @Test
    public void stampRejectsCountMismatch() {
        MemoryRecords source = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes()));

        // 3 offsets for 2 records — must reject loudly rather than silently drop one.
        assertThrows(IllegalArgumentException.class,
            () -> LogicalProduceStamper.stamp(source, "orders", 0, new long[] {0L, 1L, 2L}));
        // 1 offset for 2 records.
        assertThrows(IllegalArgumentException.class,
            () -> LogicalProduceStamper.stamp(source, "orders", 0, new long[] {0L}));
    }

    @Test
    public void stampRejectsNegativeLogicalPartition() {
        MemoryRecords source = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord("a".getBytes()));
        assertThrows(IllegalArgumentException.class,
            () -> LogicalProduceStamper.stamp(source, "orders", -1, new long[] {0L}));
    }

    @Test
    public void stampRejectsEmptyAndMultiBatchInput() {
        // Empty: synthesize a MemoryRecords with zero records (legal in the format but illegal
        // for the broker hook to forward).
        MemoryRecords empty = MemoryRecords.EMPTY;
        assertThrows(IllegalArgumentException.class,
            () -> LogicalProduceStamper.stamp(empty, "orders", 0, new long[] {}));

        // Multi-batch: synthesize a MemoryRecords with two batches by concatenating the buffers.
        MemoryRecords batchA = MemoryRecords.withRecords(0L, Compression.NONE,
            new SimpleRecord("a".getBytes()));
        MemoryRecords batchB = MemoryRecords.withRecords(1L, Compression.NONE,
            new SimpleRecord("b".getBytes()));
        ByteBuffer joined = ByteBuffer.allocate(batchA.sizeInBytes() + batchB.sizeInBytes());
        joined.put(batchA.buffer().duplicate());
        joined.put(batchB.buffer().duplicate());
        joined.flip();
        MemoryRecords multi = MemoryRecords.readableRecords(joined);

        assertThrows(IllegalArgumentException.class,
            () -> LogicalProduceStamper.stamp(multi, "orders", 0, new long[] {0L, 1L}));
    }

    @Test
    public void stampRejectsNullArguments() {
        MemoryRecords source = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord("a".getBytes()));
        assertThrows(NullPointerException.class,
            () -> LogicalProduceStamper.stamp(null, "orders", 0, new long[] {0L}));
        assertThrows(NullPointerException.class,
            () -> LogicalProduceStamper.stamp(source, null, 0, new long[] {0L}));
        assertThrows(NullPointerException.class,
            () -> LogicalProduceStamper.stamp(source, "orders", 0, null));
    }

    @Test
    public void countRecordsAcrossSingleBatch() {
        MemoryRecords source = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes()),
            new SimpleRecord("c".getBytes()),
            new SimpleRecord("d".getBytes()));
        assertEquals(4, LogicalProduceStamper.countRecords(source));
    }

    @Test
    public void countRecordsOnEmpty() {
        assertEquals(0, LogicalProduceStamper.countRecords(MemoryRecords.EMPTY));
    }

    @Test
    public void stampHeaderValueIsExactlyEightBytesBigEndian() {
        // The fetch path and BackingScanRecoverer read this as a big-endian long. Pin the exact
        // wire format so a recovery path written separately doesn't drift from the produce
        // stamping.
        MemoryRecords source = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord("payload".getBytes()));

        MemoryRecords stamped = LogicalProduceStamper.stamp(source, "t", 0, new long[] {0x0123_4567_89AB_CDEFL});

        Record r = collectRecords(stamped).get(0);
        Header offsetHeader = r.headers()[r.headers().length - 1];
        assertEquals(ConcentrationHeaders.LOGICAL_OFFSET_HEADER, offsetHeader.key());
        byte[] expected = new byte[] {
            (byte) 0x01, (byte) 0x23, (byte) 0x45, (byte) 0x67,
            (byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF};
        assertArrayEquals(expected, offsetHeader.value());
    }

    @Test
    public void stampPreservesCompressionType() {
        // gzip is the cheapest non-NONE codec available; pin compression survival rather than
        // exhaustively testing every codec (the underlying builder handles them all uniformly).
        MemoryRecords source = MemoryRecords.withRecords(Compression.gzip().build(),
            new SimpleRecord("k1".getBytes(), "v1".getBytes()),
            new SimpleRecord("k2".getBytes(), "v2".getBytes()));

        MemoryRecords stamped = LogicalProduceStamper.stamp(source, "orders", 0, new long[] {0L, 1L});

        RecordBatch outBatch = stamped.batches().iterator().next();
        assertEquals(source.batches().iterator().next().compressionType(),
            outBatch.compressionType());
        List<Record> records = collectRecords(stamped);
        assertEquals(2, records.size());
    }

    @Test
    public void stampOnNullKeyRecordIsHandled() {
        // Stock producers commonly write keyless records (round-robin partitioner). The rebuild
        // must not crash on a null key.
        MemoryRecords source = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord(null, "value-only".getBytes()));

        MemoryRecords stamped = LogicalProduceStamper.stamp(source, "orders", 0, new long[] {0L});

        Record r = collectRecords(stamped).get(0);
        assertNull(r.key());
        assertArrayEquals("value-only".getBytes(), bytes(r.value()));
        assertTrue(r.headers().length >= 3);
    }

    // ---- helpers ----

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
