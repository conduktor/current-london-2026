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
package org.apache.kafka.server.views;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.BufferSupplier;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ViewFilter}.
 *
 * The filter must:
 *  - keep records whose predicate evaluates to {@code true};
 *  - drop records whose predicate evaluates to {@code false} or is unevaluable (skip);
 *  - preserve source offsets exactly (sparse view — the source-sparse offset mode);
 *  - emit a header-only batch when 100% of records in a batch are filtered out, so the
 *    consumer's offset advances past the source range without re-querying;
 *  - preserve control batches (transaction markers) as-is so EOS guarantees survive;
 *  - never throw on malformed payloads — predicate skip turns into a drop.
 */
class ViewFilterTest {

    private final PredicateCompiler compiler = new PredicateCompiler(PredicateLimits.defaults());

    @Test
    void matchingRecordsArePreservedWithSourceOffsets() {
        CompiledPredicate p = compiler.compile("body.color == 'red'");
        MemoryRecords input = MemoryRecords.withRecords(100L, Compression.NONE,
                rec("{\"color\":\"red\"}"),
                rec("{\"color\":\"blue\"}"),
                rec("{\"color\":\"red\"}"),
                rec("{\"color\":\"green\"}"),
                rec("{\"color\":\"red\"}"));

        MemoryRecords output = ViewFilter.apply(p, input, 0);

        List<Long> kept = offsetsOf(output);
        assertEquals(List.of(100L, 102L, 104L), kept,
                "only red records (at their source offsets) must pass the predicate");
    }

    @Test
    void droppedRecordsDoNotShiftOffsetsOfKeptRecords() {
        // Three records, only the middle one matches. Its source offset must be preserved.
        CompiledPredicate p = compiler.compile("body.keep == true");
        MemoryRecords input = MemoryRecords.withRecords(50L, Compression.NONE,
                rec("{\"keep\":false}"),
                rec("{\"keep\":true}"),
                rec("{\"keep\":false}"));

        MemoryRecords output = ViewFilter.apply(p, input, 0);

        List<Long> kept = offsetsOf(output);
        assertEquals(List.of(51L), kept, "kept record retains source offset 51");
    }

    @Test
    void allRecordsFilteredEmitsHeaderOnlyBatch() {
        // None of the records match — the filter must still emit a header-only batch so the
        // consumer offset advances past lastOffset (else the consumer re-fetches the same range
        // forever). This is the acceptance criterion from PROMPT.md.
        CompiledPredicate p = compiler.compile("body.color == 'red'");
        MemoryRecords input = MemoryRecords.withRecords(200L, Compression.NONE,
                rec("{\"color\":\"blue\"}"),
                rec("{\"color\":\"green\"}"),
                rec("{\"color\":\"yellow\"}"));

        MemoryRecords output = ViewFilter.apply(p, input, 0);

        assertEquals(List.of(), offsetsOf(output), "no records survive the filter");
        long maxOffset = -1L;
        long baseOffset = Long.MAX_VALUE;
        int batchCount = 0;
        for (MutableRecordBatch batch : output.batches()) {
            batchCount++;
            maxOffset = Math.max(maxOffset, batch.lastOffset());
            baseOffset = Math.min(baseOffset, batch.baseOffset());
        }
        assertEquals(1, batchCount, "header-only batch must remain so consumer advances");
        assertEquals(200L, baseOffset, "header preserves the source baseOffset");
        assertEquals(202L, maxOffset, "header preserves the source lastOffset (sparse advance)");
    }

    @Test
    void unevaluablePredicateDropsRecord() {
        // Malformed JSON body: parser throws → predicate returns Optional.empty() → drop.
        CompiledPredicate p = compiler.compile("body.color == 'red'");
        MemoryRecords input = MemoryRecords.withRecords(0L, Compression.NONE,
                rec("{not json"),
                rec("{\"color\":\"red\"}"),
                rec("{\"color\":\"blue\"}"));

        MemoryRecords output = ViewFilter.apply(p, input, 0);

        assertEquals(List.of(1L), offsetsOf(output),
                "malformed body is treated as 'unknown' (skip), only valid red record survives");
    }

    @Test
    void predicateCanReadHeaders() {
        CompiledPredicate p = compiler.compile("headers['tenant'] == 'acme'");
        Header[] acmeHeader = {new RecordHeader("tenant", "acme".getBytes(StandardCharsets.UTF_8))};
        Header[] otherHeader = {new RecordHeader("tenant", "globex".getBytes(StandardCharsets.UTF_8))};

        MemoryRecords input = MemoryRecords.withRecords(10L, Compression.NONE,
                new SimpleRecord(0L, null, "{}".getBytes(StandardCharsets.UTF_8), acmeHeader),
                new SimpleRecord(0L, null, "{}".getBytes(StandardCharsets.UTF_8), otherHeader),
                new SimpleRecord(0L, null, "{}".getBytes(StandardCharsets.UTF_8), acmeHeader));

        MemoryRecords output = ViewFilter.apply(p, input, 0);

        assertEquals(List.of(10L, 12L), offsetsOf(output));
    }

    @Test
    void predicateCanReadOffsetAndPartition() {
        CompiledPredicate p = compiler.compile("offset >= 102 && partition == 7");
        MemoryRecords input = MemoryRecords.withRecords(100L, Compression.NONE,
                rec("{}"), rec("{}"), rec("{}"), rec("{}"), rec("{}"));

        MemoryRecords output = ViewFilter.apply(p, input, 7);
        assertEquals(List.of(102L, 103L, 104L), offsetsOf(output));

        // Same records but wrong partition → all dropped, header-only batch remains.
        MemoryRecords outputWrongPart = ViewFilter.apply(p, input, 0);
        assertEquals(List.of(), offsetsOf(outputWrongPart));
        assertTrue(outputWrongPart.batches().iterator().hasNext(),
                "header-only batch must remain even when partition mismatches");
    }

    @Test
    void nullBodyIsTreatedAsUnusable() {
        // A null value (tombstone) makes body.* unusable → predicate skips → drop.
        CompiledPredicate p = compiler.compile("body.color == 'red'");
        MemoryRecords input = MemoryRecords.withRecords(0L, Compression.NONE,
                new SimpleRecord(0L, (byte[]) null, (byte[]) null),
                rec("{\"color\":\"red\"}"));

        MemoryRecords output = ViewFilter.apply(p, input, 0);
        assertEquals(List.of(1L), offsetsOf(output));
    }

    @Test
    void multipleBatchesEachFilteredIndependently() {
        // Two source batches at different offset ranges. Predicate filters each separately;
        // a fully-filtered batch still leaves a header so offsets advance.
        CompiledPredicate p = compiler.compile("body.keep == true");

        MemoryRecords batch1 = MemoryRecords.withRecords(0L, Compression.NONE,
                rec("{\"keep\":true}"),
                rec("{\"keep\":false}"));
        MemoryRecords batch2 = MemoryRecords.withRecords(10L, Compression.NONE,
                rec("{\"keep\":false}"),
                rec("{\"keep\":false}"));

        // Concatenate the two batches into a single MemoryRecords by appending the underlying
        // buffers. We rely on the API contract that filterTo iterates over batches.
        java.nio.ByteBuffer combined = java.nio.ByteBuffer.allocate(
                batch1.sizeInBytes() + batch2.sizeInBytes());
        combined.put(batch1.buffer().duplicate());
        combined.put(batch2.buffer().duplicate());
        combined.flip();
        MemoryRecords input = MemoryRecords.readableRecords(combined);

        MemoryRecords output = ViewFilter.apply(p, input, 0);

        // Batch 1 keeps record at offset 0; batch 2 is fully filtered but its header survives.
        assertEquals(List.of(0L), offsetsOf(output));
        long lastBatchEnd = -1L;
        for (MutableRecordBatch batch : output.batches()) {
            lastBatchEnd = batch.lastOffset();
        }
        assertEquals(11L, lastBatchEnd,
                "header-only batch for the fully-filtered second batch must advance maxOffset");
    }

    @Test
    void filterIsStatelessAcrossInvocations() {
        // Reusing the same predicate across calls must be safe (no carry-over state).
        CompiledPredicate p = compiler.compile("body.color == 'red'");

        MemoryRecords first = ViewFilter.apply(p,
                MemoryRecords.withRecords(0L, Compression.NONE,
                        rec("{\"color\":\"red\"}"),
                        rec("{\"color\":\"blue\"}")),
                0);
        MemoryRecords second = ViewFilter.apply(p,
                MemoryRecords.withRecords(100L, Compression.NONE,
                        rec("{\"color\":\"blue\"}"),
                        rec("{\"color\":\"red\"}")),
                0);

        assertEquals(List.of(0L), offsetsOf(first));
        assertEquals(List.of(101L), offsetsOf(second));
    }

    @Test
    void resultIsNonNullEvenWhenAllRecordsDropped() {
        CompiledPredicate p = compiler.compile("body.color == 'red'");
        MemoryRecords output = ViewFilter.apply(p,
                MemoryRecords.withRecords(0L, Compression.NONE,
                        rec("{\"color\":\"blue\"}")),
                0);
        assertNotNull(output);
    }

    @Test
    void bufferSupplierOverloadProducesSameOutputAsNoCaching() {
        // The 5-arg overload only changes WHERE the decompression staging buffers come from
        // (a pooled supplier vs fresh allocations). The output records must be byte-for-byte
        // equivalent to the simpler overload. We assert offsets which is sufficient: filterTo
        // does not (and cannot) restructure batches when the supplier changes.
        CompiledPredicate p = compiler.compile("body.color == 'red'");
        MemoryRecords input = MemoryRecords.withRecords(50L, Compression.lz4().build(),
                rec("{\"color\":\"red\"}"),
                rec("{\"color\":\"blue\"}"),
                rec("{\"color\":\"red\"}"));

        BufferSupplier.GrowableBufferSupplier supplier = new BufferSupplier.GrowableBufferSupplier();
        try {
            MemoryRecords pooled = ViewFilter.apply(p, input, 0, ViewMetrics.NOOP, supplier);
            MemoryRecords noCaching = ViewFilter.apply(p, input, 0, ViewMetrics.NOOP);
            assertEquals(offsetsOf(noCaching), offsetsOf(pooled),
                    "BufferSupplier choice must not change the filtered output");
            assertEquals(List.of(50L, 52L), offsetsOf(pooled));
        } finally {
            supplier.close();
        }
    }

    @Test
    void commitAbortControlBatchesPassThroughIntactSoReadCommittedConsumersTrackTransactions() {
        // PROMPT.md acceptance criterion (line 40) and functional test scenario (line 48):
        // READ_COMMITTED view consumers must observe correct isolation. Control batches
        // (COMMIT/ABORT end-transaction markers) MUST propagate through the filter with their
        // marker RECORD intact — not just the batch header. The consumer's READ_COMMITTED logic
        // (CompletedFetch.containsAbortMarker) iterates the control batch's records and parses
        // the first record's key for ControlRecordType.ABORT to clear aborted-producer state.
        // Stripping the record (e.g. via RETAIN_EMPTY + shouldRetainRecord=false) leaves the
        // consumer unable to distinguish ABORT from "no terminator yet", which leaks aborted
        // records to applications. Test both COMMIT and ABORT to cover both isolation outcomes.
        CompiledPredicate p = compiler.compile("body.color == 'red'");
        for (ControlRecordType type : List.of(ControlRecordType.COMMIT, ControlRecordType.ABORT)) {
            MemoryRecords output = ViewFilter.apply(p, withDataAndControlMarker(type), 0);

            // The transactional data batch had one "red" record at offset 0 plus a "blue" at 1;
            // only the red one survives the predicate. The control batch at offset 2 carries a
            // single COMMIT/ABORT marker record that must ride through filtering unchanged.
            assertEquals(List.of(0L, 2L), offsetsOf(output),
                    "red data record + control marker record both survive (type=" + type + ")");

            long maxLastOffset = -1L;
            boolean sawControlBatch = false;
            boolean sawDataBatch = false;
            for (MutableRecordBatch batch : output.batches()) {
                maxLastOffset = Math.max(maxLastOffset, batch.lastOffset());
                if (batch.isControlBatch()) {
                    sawControlBatch = true;
                    // The marker record itself must survive so the consumer can read it. This
                    // mirrors the iteration in CompletedFetch.containsAbortMarker:
                    //   Iterator<Record> it = batch.iterator();
                    //   assert it.hasNext();
                    //   ControlRecordType.parse(it.next().key())  -> COMMIT or ABORT
                    Iterator<Record> it = batch.iterator();
                    assertTrue(it.hasNext(),
                            "control batch must retain its marker record so the consumer can "
                                    + "detect transaction outcome (type=" + type + ")");
                    Record marker = it.next();
                    assertEquals(type, ControlRecordType.parse(marker.key()),
                            "control record key must round-trip to original marker type (type="
                                    + type + ")");
                    assertFalse(it.hasNext(),
                            "control batch carries exactly one marker record (type=" + type + ")");
                    // Producer-id / producer-epoch / transactional flag must round-trip — the
                    // consumer's READ_COMMITTED state machine keys off these to scope ABORTs to
                    // the right producer. A filter that rewrites them would silently corrupt
                    // every consumer's aborted-producers set.
                    assertEquals(73L, batch.producerId(),
                            "control batch producerId must survive filter (type=" + type + ")");
                    assertEquals((short) 0, batch.producerEpoch(),
                            "control batch producerEpoch must survive filter (type=" + type + ")");
                    assertTrue(batch.isTransactional(),
                            "control batch transactional flag must survive (type=" + type + ")");
                } else {
                    sawDataBatch = true;
                }
            }
            assertTrue(sawDataBatch, "data batch must remain (type=" + type + ")");
            assertTrue(sawControlBatch,
                    "control batch must propagate through filter so LSO advances (type=" + type + ")");
            assertEquals(2L, maxLastOffset,
                    "lastOffset must match source so consumer's LSO advances past marker (type="
                            + type + ")");
        }
    }

    /** Build a MemoryRecords containing (i) a transactional data batch with two records at offsets
     *  0 and 1, and (ii) an end-transaction control batch at offset 2 carrying the supplied
     *  COMMIT/ABORT marker. Same shape a broker would serve when a producer commits/aborts a
     *  transaction over a topic that becomes the backing for a view. */
    private static MemoryRecords withDataAndControlMarker(ControlRecordType controlType) {
        long producerId = 73L;
        short producerEpoch = 0;
        int partitionLeaderEpoch = 0;
        int baseSequence = 0;
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(
                buffer,
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                0L,                  // baseOffset
                0L,                  // logAppendTime
                producerId,
                producerEpoch,
                baseSequence,
                true,                // isTransactional
                false,               // isControlBatch
                partitionLeaderEpoch,
                buffer.capacity());
        builder.append(0L, null, "{\"color\":\"red\"}".getBytes(StandardCharsets.UTF_8));
        builder.append(0L, null, "{\"color\":\"blue\"}".getBytes(StandardCharsets.UTF_8));
        builder.close();
        MemoryRecords.writeEndTransactionalMarker(buffer, 2L, 0L, partitionLeaderEpoch,
                producerId, producerEpoch, new EndTransactionMarker(controlType, 0));
        buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }

    @Test
    void bufferSupplierOverloadCanBeReusedAcrossCallsWithoutCorruption() {
        // Production code re-uses one supplier across every view partition in a single fetch
        // callback. If filterTo retained references to staging buffers between calls, the second
        // invocation could see corrupted data. Guard against that regression here.
        CompiledPredicate p = compiler.compile("body.color == 'red'");
        BufferSupplier.GrowableBufferSupplier supplier = new BufferSupplier.GrowableBufferSupplier();
        try {
            MemoryRecords first = ViewFilter.apply(p,
                    MemoryRecords.withRecords(0L, Compression.lz4().build(),
                            rec("{\"color\":\"red\"}"),
                            rec("{\"color\":\"blue\"}")),
                    0, ViewMetrics.NOOP, supplier);
            MemoryRecords second = ViewFilter.apply(p,
                    MemoryRecords.withRecords(100L, Compression.snappy().build(),
                            rec("{\"color\":\"blue\"}"),
                            rec("{\"color\":\"red\"}")),
                    0, ViewMetrics.NOOP, supplier);
            assertEquals(List.of(0L), offsetsOf(first));
            assertEquals(List.of(101L), offsetsOf(second));
        } finally {
            supplier.close();
        }
    }

    @Test
    void filteredBatchesAlwaysCarryNoPartitionLeaderEpochRegardlessOfSource() {
        // The view partition keeps its own (lower) leader-epoch ledger. If the filter forwards the
        // backing topic's partition_leader_epoch through filtered records, the consumer's
        // CompletedFetch pins position.offsetEpoch to the backing epoch; the next fetch tries to
        // validate that epoch against the view ledger via OffsetsForLeaderEpoch, which we reject
        // with INVALID_REQUEST. The consumer's handleResponse default branch then retries forever
        // while the partition is stuck in AWAIT_VALIDATION (non-fetchable). Strip the field on the
        // way out — pin -1 / NO_PARTITION_LEADER_EPOCH so the consumer never enters validation.
        CompiledPredicate p = compiler.compile("body.color == 'red'");
        // Build a batch that explicitly carries a non-trivial backing partition_leader_epoch
        // (simulating a backing topic that has gone through several leader elections). Use the
        // builder constructor so we can set partitionLeaderEpoch directly.
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        int sourceBackingEpoch = 1234;
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(
                buffer,
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                0L,
                0L,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                false,
                false,
                sourceBackingEpoch,
                buffer.capacity());
        builder.append(0L, null, "{\"color\":\"red\"}".getBytes(StandardCharsets.UTF_8));
        builder.append(0L, null, "{\"color\":\"blue\"}".getBytes(StandardCharsets.UTF_8));
        builder.append(0L, null, "{\"color\":\"red\"}".getBytes(StandardCharsets.UTF_8));
        builder.close();
        buffer.flip();
        MemoryRecords input = MemoryRecords.readableRecords(buffer);

        // Sanity check: the source batch really does carry the backing epoch.
        for (MutableRecordBatch sourceBatch : input.batches()) {
            assertEquals(sourceBackingEpoch, sourceBatch.partitionLeaderEpoch(),
                    "input batch must carry the backing epoch so the strip is observable");
        }

        MemoryRecords output = ViewFilter.apply(p, input, 0);

        boolean sawBatch = false;
        for (MutableRecordBatch batch : output.batches()) {
            sawBatch = true;
            assertEquals(RecordBatch.NO_PARTITION_LEADER_EPOCH, batch.partitionLeaderEpoch(),
                    "every filtered batch must have partition_leader_epoch == -1 so the consumer "
                            + "never pins position.offsetEpoch to the backing epoch");
            // The CRC must remain valid after the in-place setter. DefaultRecordBatch.isValid()
            // recomputes the checksum from ATTRIBUTES_OFFSET onwards and compares against the
            // stored CRC. If partition_leader_epoch were inside CRC coverage, this would fail —
            // catching any future change to the v2 batch layout that breaks the assumption.
            assertTrue(((org.apache.kafka.common.record.DefaultRecordBatch) batch).isValid(),
                    "filtered batch CRC must remain valid after stripping partition_leader_epoch");
        }
        assertTrue(sawBatch, "expected at least one filtered batch in the output");
    }

    private static SimpleRecord rec(String json) {
        byte[] body = json == null ? null : json.getBytes(StandardCharsets.UTF_8);
        return new SimpleRecord(0L, null, body);
    }

    private static List<Long> offsetsOf(MemoryRecords records) {
        List<Long> offsets = new ArrayList<>();
        for (MutableRecordBatch batch : records.batches()) {
            Iterator<Record> it = batch.iterator();
            while (it.hasNext()) {
                Record r = it.next();
                offsets.add(r.offset());
            }
        }
        return offsets;
    }
}
