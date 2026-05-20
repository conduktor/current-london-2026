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
    void fullyFilteredDataBatchesScrubProducerAndTimestampMetadata() {
        // PROMPT.md acceptance criterion: "Preserves source offsets" — empty batches in
        // source_sparse mode must keep baseOffset and lastOffset so the consumer advances. They
        // must NOT keep anything else from the backing batch. Kafka's MemoryRecords.writeEmptyHeader
        // unconditionally copies producer_id, producer_epoch, base_sequence, max_timestamp, and
        // the transactional flag from the source batch when RETAIN_EMPTY is requested. For a
        // fully-filtered DATA batch on a view, those fields describe records the predicate just
        // REJECTED — leaking them would broadcast the backing producer's identity, the timestamp
        // range, and whether those filtered records were inside a transaction.
        //
        // The filter must scrub these fields on empty data batches while leaving baseOffset and
        // lastOffset intact for the sparse-offset advance.
        CompiledPredicate p = compiler.compile("body.color == 'red'");
        // Build a single transactional data batch with three "blue" records — none match the
        // predicate, so RETAIN_EMPTY produces an empty header.
        long backingProducerId = 12345L;
        short backingProducerEpoch = 7;
        int backingBaseSequence = 42;
        long backingTimestamp = 1_700_000_000_000L;
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(
                buffer,
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                500L,                       // baseOffset
                0L,                         // logAppendTime
                backingProducerId,
                backingProducerEpoch,
                backingBaseSequence,
                true,                       // isTransactional
                false,                      // isControlBatch
                0,                          // partitionLeaderEpoch
                buffer.capacity());
        builder.append(backingTimestamp, null, "{\"color\":\"blue\"}".getBytes(StandardCharsets.UTF_8));
        builder.append(backingTimestamp + 1, null, "{\"color\":\"green\"}".getBytes(StandardCharsets.UTF_8));
        builder.append(backingTimestamp + 2, null, "{\"color\":\"yellow\"}".getBytes(StandardCharsets.UTF_8));
        builder.close();
        buffer.flip();
        MemoryRecords input = MemoryRecords.readableRecords(buffer);

        // Sanity: source batch carries the producer/timestamp metadata we expect.
        MutableRecordBatch source = input.batches().iterator().next();
        assertEquals(backingProducerId, source.producerId(),
                "source batch must carry the producer id we'll be checking for scrubbing");
        assertEquals(backingProducerEpoch, source.producerEpoch(),
                "source batch must carry the producer epoch we'll be checking for scrubbing");
        assertTrue(source.isTransactional(),
                "source batch must be transactional so the strip is observable");

        MemoryRecords output = ViewFilter.apply(p, input, 0);

        assertEquals(List.of(), offsetsOf(output), "no records survive the predicate");
        int batchCount = 0;
        for (MutableRecordBatch emptyBatch : output.batches()) {
            batchCount++;
            // Sparse-offset advance MUST still work: consumer's lastFetched advances past 502.
            assertEquals(500L, emptyBatch.baseOffset(),
                    "baseOffset must survive — source_sparse mode requires it for offset advance");
            assertEquals(502L, emptyBatch.lastOffset(),
                    "lastOffset must survive — source_sparse mode requires it for offset advance");
            // Producer identity must be scrubbed.
            assertEquals(RecordBatch.NO_PRODUCER_ID, emptyBatch.producerId(),
                    "backing producer_id must NOT leak through a fully-filtered data batch — the "
                            + "filtered records' producer identity is gated by the predicate");
            assertEquals(RecordBatch.NO_PRODUCER_EPOCH, emptyBatch.producerEpoch(),
                    "backing producer_epoch must NOT leak through a fully-filtered data batch");
            assertEquals(RecordBatch.NO_SEQUENCE, emptyBatch.baseSequence(),
                    "backing base_sequence must NOT leak through a fully-filtered data batch");
            // Transactional flag must be cleared. The records were filtered out, so the consumer
            // has no business knowing those records were inside a transaction.
            assertFalse(emptyBatch.isTransactional(),
                    "transactional flag must be cleared on a fully-filtered data batch — leaving "
                            + "it set tells the view consumer that filtered records were inside a "
                            + "transaction, which is metadata about records they cannot read");
            // maxTimestamp must be cleared — leaking it discloses the timestamp range of the
            // filtered records.
            assertEquals(RecordBatch.NO_TIMESTAMP, emptyBatch.maxTimestamp(),
                    "max_timestamp must NOT leak — exposes the wall-clock range of filtered records");
            // The control-batch bit on a data batch must remain false (we did not turn it on).
            assertFalse(emptyBatch.isControlBatch(),
                    "an empty data batch must not be promoted to a control batch by scrubbing");
            // CRC must validate after the rewrite.
            assertTrue(((org.apache.kafka.common.record.DefaultRecordBatch) emptyBatch).isValid(),
                    "scrubbed empty data batch must carry a valid CRC");
        }
        assertEquals(1, batchCount, "exactly one empty header-only batch must remain");
    }

    @Test
    void controlBatchProducerMetadataSurvivesScrubWhenPartiallyVisibleBecauseReadCommittedNeedsIt() {
        // Companion to fullyFilteredDataBatchesScrubProducerAndTimestampMetadata: when the
        // transaction is PARTIALLY visible — at least one non-control data record from the same
        // producerId survives the predicate — the COMMIT/ABORT marker must propagate unchanged.
        // READ_COMMITTED consumers match markers to the producing transaction via producer_id +
        // producer_epoch on the control batch. Scrubbing those fields here silently corrupts
        // aborted-producers tracking and leaks aborted records. The fully-hidden case is handled
        // by the companion test fullyHiddenAbortMarkerIsScrubbedToEmptyHeader.
        CompiledPredicate p = compiler.compile("body.color == 'red'");
        MemoryRecords output = ViewFilter.apply(p, withDataAndControlMarker(ControlRecordType.ABORT), 0);

        boolean sawControl = false;
        for (MutableRecordBatch batch : output.batches()) {
            if (batch.isControlBatch()) {
                sawControl = true;
                assertEquals(73L, batch.producerId(),
                        "control-batch producer_id must NOT be scrubbed when same-pid data is visible — READ_COMMITTED depends on it");
                assertEquals((short) 0, batch.producerEpoch(),
                        "control-batch producer_epoch must NOT be scrubbed when same-pid data is visible — READ_COMMITTED depends on it");
                assertTrue(batch.isTransactional(),
                        "control-batch transactional flag must NOT be scrubbed when same-pid data is visible — it identifies the marker");
            }
        }
        assertTrue(sawControl, "expected the control batch to propagate through the filter");
    }

    @Test
    void fullyHiddenAbortMarkerIsScrubbedToEmptyHeader() {
        fullyHiddenMarkerScrubAssertions(ControlRecordType.ABORT);
    }

    @Test
    void fullyHiddenCommitMarkerIsScrubbedToEmptyHeader() {
        fullyHiddenMarkerScrubAssertions(ControlRecordType.COMMIT);
    }

    @Test
    void multiPidFetchScrubsOnlyMarkersOfFullyHiddenProducers() {
        // Two producers (pid=50 / pid=60) in the same fetch. Predicate filters everything for
        // pid=50, leaves at least one record for pid=60. The pid=50 marker (fully hidden) MUST
        // be scrubbed; the pid=60 marker (partially visible) MUST stay intact because the view
        // consumer needs producer_id + producer_epoch on it to terminate READ_COMMITTED aborted-
        // producer-range tracking against the surviving records.
        CompiledPredicate p = compiler.compile("body.color == 'red'");
        ByteBuffer buffer = ByteBuffer.allocate(4096);

        // pid=50 — fully hidden by the predicate.
        long hiddenPid = 50L;
        short hiddenEpoch = 1;
        MemoryRecordsBuilder hiddenBuilder = new MemoryRecordsBuilder(
                buffer,
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                0L, 0L,
                hiddenPid, hiddenEpoch, 0,
                true, false, 0,
                buffer.capacity());
        hiddenBuilder.append(0L, null, "{\"color\":\"blue\"}".getBytes(StandardCharsets.UTF_8));
        hiddenBuilder.close();
        MemoryRecords.writeEndTransactionalMarker(buffer, 1L, 0L, 0,
                hiddenPid, hiddenEpoch,
                new EndTransactionMarker(ControlRecordType.ABORT, 0));

        // pid=60 — partially visible (red + blue).
        long visiblePid = 60L;
        short visibleEpoch = 2;
        MemoryRecordsBuilder visibleBuilder = new MemoryRecordsBuilder(
                buffer,
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                2L, 0L,
                visiblePid, visibleEpoch, 0,
                true, false, 0,
                buffer.capacity());
        visibleBuilder.append(0L, null, "{\"color\":\"red\"}".getBytes(StandardCharsets.UTF_8));
        visibleBuilder.append(0L, null, "{\"color\":\"blue\"}".getBytes(StandardCharsets.UTF_8));
        visibleBuilder.close();
        MemoryRecords.writeEndTransactionalMarker(buffer, 4L, 0L, 0,
                visiblePid, visibleEpoch,
                new EndTransactionMarker(ControlRecordType.ABORT, 0));

        buffer.flip();
        MemoryRecords input = MemoryRecords.readableRecords(buffer);

        MemoryRecords output = ViewFilter.apply(p, input, 0);

        boolean hiddenMarkerScrubbed = false;     // pid=50's ABORT marker at offset 1
        boolean visibleMarkerIntact = false;      // pid=60's ABORT marker at offset 4
        for (MutableRecordBatch batch : output.batches()) {
            if (batch.baseOffset() == 1L) {
                assertFalse(batch.isControlBatch(),
                        "pid=50 ABORT marker (offset 1) must be scrubbed to a non-control header — producer fully hidden");
                assertEquals(RecordBatch.NO_PRODUCER_ID, batch.producerId(),
                        "pid=50 marker must not leak producer_id");
                assertFalse(batch.isTransactional(),
                        "pid=50 marker must clear isTransactional");
                assertFalse(batch.iterator().hasNext(),
                        "pid=50 marker must produce a header-only batch with no records");
                hiddenMarkerScrubbed = true;
            } else if (batch.baseOffset() == 4L) {
                assertTrue(batch.isControlBatch(),
                        "pid=60 ABORT marker (offset 4) must remain a control batch — pid has surviving data");
                assertEquals(visiblePid, batch.producerId(),
                        "pid=60 marker producer_id must round-trip — surviving same-pid data depends on it");
                assertEquals(visibleEpoch, batch.producerEpoch(),
                        "pid=60 marker producer_epoch must round-trip");
                assertTrue(batch.isTransactional(),
                        "pid=60 marker transactional flag must round-trip");
                visibleMarkerIntact = true;
            }
        }
        assertTrue(hiddenMarkerScrubbed,
                "expected pid=50 ABORT marker at offset 1 to be scrubbed to a header-only batch");
        assertTrue(visibleMarkerIntact,
                "expected pid=60 ABORT marker at offset 4 to remain a control batch with intact producer metadata");
    }

    private void fullyHiddenMarkerScrubAssertions(ControlRecordType controlType) {
        // When ALL non-control data records in a transaction fail the predicate, the COMMIT/ABORT
        // marker is a pure metadata leak: it broadcasts the backing producer's id + epoch +
        // transaction outcome (COMMIT/ABORT) + coordinator epoch to view consumers who have no
        // other reference to that transaction. KafkaApis.filterAbortedTransactionsByVisibleProducers
        // strips the matching abortedTransactions entry in the same fully-hidden case, so the
        // marker is also functionally a no-op for READ_COMMITTED tracking. The filter must scrub
        // the control batch to a header-only empty placeholder (offset advance only), the same
        // way it scrubs a fully-filtered data batch.
        //
        // PROMPT.md line 29 (lessons-already-known): "COMMIT/ABORT control batches contain no
        // user data, so they have nothing to predicate against — but they must still be emitted
        // as empty batches or READ_COMMITTED isolation silently breaks". Our scrub honours that:
        // the offset placeholder remains so LSO advances; the producer-identity payload is gone.
        CompiledPredicate p = compiler.compile("body.color == 'red'");
        long backingProducerId = 99L;
        short backingProducerEpoch = 3;
        int partitionLeaderEpoch = 0;
        int baseSequence = 0;
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(
                buffer,
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                0L,                            // baseOffset
                0L,                            // logAppendTime
                backingProducerId,
                backingProducerEpoch,
                baseSequence,
                true,                          // isTransactional
                false,                         // isControlBatch
                partitionLeaderEpoch,
                buffer.capacity());
        builder.append(0L, null, "{\"color\":\"blue\"}".getBytes(StandardCharsets.UTF_8));
        builder.append(0L, null, "{\"color\":\"green\"}".getBytes(StandardCharsets.UTF_8));
        builder.close();
        MemoryRecords.writeEndTransactionalMarker(buffer, 2L, 0L, partitionLeaderEpoch,
                backingProducerId, backingProducerEpoch,
                new EndTransactionMarker(controlType, 0));
        buffer.flip();
        MemoryRecords input = MemoryRecords.readableRecords(buffer);

        MemoryRecords output = ViewFilter.apply(p, input, 0);

        assertEquals(List.of(), offsetsOf(output), "no user records survive — predicate filtered all");
        int totalBatches = 0;
        int controlBatchCount = 0;
        int dataBatchCount = 0;
        boolean dataOffsetsObserved = false;
        boolean controlOffsetsObserved = false;
        for (MutableRecordBatch batch : output.batches()) {
            totalBatches++;
            assertFalse(batch.isControlBatch(),
                    "control batch for fully-hidden transaction must be scrubbed to a non-control header (type="
                            + controlType + ")");
            assertEquals(RecordBatch.NO_PRODUCER_ID, batch.producerId(),
                    "fully-hidden " + controlType + " marker must not leak producer_id");
            assertEquals(RecordBatch.NO_PRODUCER_EPOCH, batch.producerEpoch(),
                    "fully-hidden " + controlType + " marker must not leak producer_epoch");
            assertEquals(RecordBatch.NO_SEQUENCE, batch.baseSequence(),
                    "fully-hidden " + controlType + " marker must not leak base_sequence");
            assertFalse(batch.isTransactional(),
                    "fully-hidden " + controlType + " marker must clear isTransactional");
            assertEquals(RecordBatch.NO_TIMESTAMP, batch.maxTimestamp(),
                    "fully-hidden " + controlType + " marker must clear max_timestamp");
            assertFalse(batch.iterator().hasNext(),
                    "fully-hidden " + controlType + " marker must produce a header-only batch with no records "
                            + "(no marker payload, no coordinator-epoch leakage)");
            assertTrue(((org.apache.kafka.common.record.DefaultRecordBatch) batch).isValid(),
                    "scrubbed batch must carry a valid CRC (type=" + controlType + ")");
            if (batch.baseOffset() == 0L && batch.lastOffset() == 1L) {
                dataBatchCount++;
                dataOffsetsObserved = true;
            } else if (batch.baseOffset() == 2L && batch.lastOffset() == 2L) {
                controlBatchCount++;
                controlOffsetsObserved = true;
            }
        }
        assertEquals(2, totalBatches,
                "expected two header-only batches (scrubbed data + scrubbed marker, type=" + controlType + ")");
        assertEquals(1, dataBatchCount,
                "expected one header-only batch carrying the data span [0..1] for offset advance (type="
                        + controlType + ")");
        assertEquals(1, controlBatchCount,
                "expected one header-only batch carrying the marker's offset 2 for LSO advance (type="
                        + controlType + ")");
        assertTrue(dataOffsetsObserved,
                "data batch offsets must survive scrub so consumer advances past [0..1] (type=" + controlType + ")");
        assertTrue(controlOffsetsObserved,
                "marker offset 2 must survive scrub so LSO advances past the marker (type=" + controlType + ")");
    }

    @Test
    void samePidHiddenTxBeforeVisibleTxScrubsTheHiddenMarker() {
        // R61 BLOCKER closure. R58 used a producer-id-wide rule: a marker survived iff its pid
        // had any surviving non-control data ANYWHERE in the fetch. That over-keeps when the
        // SAME pid produces TX1 fully hidden then TX2 fully visible — TX1's marker survives
        // because pid has surviving data SOMEWHERE (from TX2), leaking TX1's COMMIT/ABORT
        // outcome, coordinator epoch, and offset boundary. The fix narrows the rule to
        // per-(pid, tx-range): a marker is kept iff at least one surviving non-control batch
        // with the same pid appeared BETWEEN this pid's previous marker (or fetch start) and
        // the current marker. TX1's marker is scrubbed; TX2's marker survives.
        //
        // The fetch handler also needs the scrubbed offsets back, because the abortedTx
        // filter (KafkaApis.filterAbortedTransactionsByVisibleProducers) walks per-pid
        // batches and uses control batches as transaction terminators — a scrubbed marker
        // is invisible to that walk. applyAndCollect returns the per-pid scrubbed offsets;
        // this test verifies both the wire scrub AND the side-channel.
        CompiledPredicate p = compiler.compile("body.color == 'red'");
        long pid = 80L;
        short epoch = 0;
        ByteBuffer buffer = ByteBuffer.allocate(4096);

        // TX1: data fully hidden (only "blue"), then ABORT marker.
        MemoryRecordsBuilder tx1Data = new MemoryRecordsBuilder(
                buffer,
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                100L, 0L,
                pid, epoch, 0,
                true, false, 0,
                buffer.capacity());
        tx1Data.append(0L, null, "{\"color\":\"blue\"}".getBytes(StandardCharsets.UTF_8));
        tx1Data.close();
        MemoryRecords.writeEndTransactionalMarker(buffer, 101L, 0L, 0,
                pid, epoch,
                new EndTransactionMarker(ControlRecordType.ABORT, 0));

        // TX2: data partially visible (red + blue), then COMMIT marker. Same pid, same epoch.
        MemoryRecordsBuilder tx2Data = new MemoryRecordsBuilder(
                buffer,
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                102L, 0L,
                pid, epoch, 1,
                true, false, 0,
                buffer.capacity());
        tx2Data.append(0L, null, "{\"color\":\"red\"}".getBytes(StandardCharsets.UTF_8));
        tx2Data.append(0L, null, "{\"color\":\"blue\"}".getBytes(StandardCharsets.UTF_8));
        tx2Data.close();
        MemoryRecords.writeEndTransactionalMarker(buffer, 104L, 0L, 0,
                pid, epoch,
                new EndTransactionMarker(ControlRecordType.COMMIT, 0));

        buffer.flip();
        MemoryRecords input = MemoryRecords.readableRecords(buffer);

        ViewFilter.FilterResult result = ViewFilter.applyAndCollect(
                p, input, 0, ViewMetrics.NOOP, BufferSupplier.NO_CACHING);

        // (1) Wire-level assertions: TX1 marker (offset 101) scrubbed; TX2 marker (offset 104) intact.
        boolean hiddenMarkerScrubbed = false;
        boolean visibleMarkerIntact = false;
        for (MutableRecordBatch batch : result.records().batches()) {
            if (batch.baseOffset() == 101L) {
                assertFalse(batch.isControlBatch(),
                        "TX1 marker (offset 101) must be scrubbed to a non-control header — same-pid TX1 hidden");
                assertEquals(RecordBatch.NO_PRODUCER_ID, batch.producerId(),
                        "TX1 marker must not leak producer_id even though TX2 of the same pid is visible");
                assertEquals(RecordBatch.NO_PRODUCER_EPOCH, batch.producerEpoch(),
                        "TX1 marker must not leak producer_epoch");
                assertFalse(batch.isTransactional(),
                        "TX1 marker must clear isTransactional");
                assertFalse(batch.iterator().hasNext(),
                        "TX1 marker must produce a header-only batch — no coordinator-epoch, no COMMIT/ABORT byte");
                hiddenMarkerScrubbed = true;
            } else if (batch.baseOffset() == 104L) {
                assertTrue(batch.isControlBatch(),
                        "TX2 marker (offset 104) must remain a control batch — TX2 has surviving data");
                assertEquals(pid, batch.producerId(),
                        "TX2 marker producer_id must round-trip — READ_COMMITTED matches it to TX2's data");
                assertEquals(epoch, batch.producerEpoch(),
                        "TX2 marker producer_epoch must round-trip");
                assertTrue(batch.isTransactional(),
                        "TX2 marker isTransactional must round-trip");
                visibleMarkerIntact = true;
            }
        }
        assertTrue(hiddenMarkerScrubbed,
                "expected the TX1 ABORT marker at offset 101 to be scrubbed to a header-only batch");
        assertTrue(visibleMarkerIntact,
                "expected the TX2 COMMIT marker at offset 104 to remain a control batch");

        // (2) Side-channel assertions: scrubbed marker offset is reported so KafkaApis can
        //     keep its abortedTx filter accurate after the wire scrub erases producer_id.
        assertNotNull(result.scrubbedMarkerOffsetsByProducerId(),
                "side-channel map must be non-null (empty allowed) so callers can iterate without null guards");
        assertTrue(result.scrubbedMarkerOffsetsByProducerId().containsKey(pid),
                "side-channel must record the scrubbed pid so KafkaApis can rebuild transaction boundaries");
        assertEquals(java.util.Set.of(101L), result.scrubbedMarkerOffsetsByProducerId().get(pid),
                "side-channel must report exactly the offset(s) of scrubbed markers for this pid (101 for TX1; 104 stayed intact)");
    }

    @Test
    void nonTransactionalSamePidBatchDoesNotKeepFullyHiddenTransactionMarkerAlive() {
        // R62 BLOCKER closure. R61 set survivingSinceLastMarker for ANY non-control batch with
        // count > 0, not just transactional ones. If the backing log contains a visible
        // non-transactional/idempotent batch with producer id P, then a transactional batch with
        // the same P whose records the predicate fully hides, then COMMIT/ABORT — R61 kept the
        // marker because P already had "surviving data" from the non-tx batch. That marker
        // terminates a TRANSACTION, not a producer, so non-transactional data must NOT count.
        // The leak exposed producerId/producerEpoch/COMMIT-or-ABORT/coordinator_epoch for a
        // transaction with zero visible transactional data.
        //
        // Fix: scanForScrubTargets now requires batch.isTransactional() to mark the tracker.
        // Non-transactional batches are inert to marker scrub decisions.
        CompiledPredicate p = compiler.compile("body.color == 'red'");
        long pid = 90L;
        short epoch = 0;
        ByteBuffer buffer = ByteBuffer.allocate(4096);

        // Non-transactional/idempotent batch (same pid). One visible record so the batch
        // survives the predicate — this is the bait that R61's bug latched onto.
        MemoryRecordsBuilder nonTx = new MemoryRecordsBuilder(
                buffer,
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                200L, 0L,
                pid, epoch, 0,
                false,               // isTransactional = false (idempotent only)
                false, 0,
                buffer.capacity());
        nonTx.append(0L, null, "{\"color\":\"red\"}".getBytes(StandardCharsets.UTF_8));
        nonTx.close();

        // Transactional batch with the same pid. All records hidden by the predicate.
        MemoryRecordsBuilder txData = new MemoryRecordsBuilder(
                buffer,
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                201L, 0L,
                pid, epoch, 0,
                true,                // isTransactional = true
                false, 0,
                buffer.capacity());
        txData.append(0L, null, "{\"color\":\"blue\"}".getBytes(StandardCharsets.UTF_8));
        txData.close();

        // COMMIT marker for the hidden transaction.
        MemoryRecords.writeEndTransactionalMarker(buffer, 202L, 0L, 0,
                pid, epoch,
                new EndTransactionMarker(ControlRecordType.COMMIT, 0));

        buffer.flip();
        MemoryRecords input = MemoryRecords.readableRecords(buffer);

        ViewFilter.FilterResult result = ViewFilter.applyAndCollect(
                p, input, 0, ViewMetrics.NOOP, BufferSupplier.NO_CACHING);

        boolean markerScrubbed = false;
        boolean nonTxBatchSurvived = false;
        for (MutableRecordBatch batch : result.records().batches()) {
            if (batch.baseOffset() == 200L) {
                assertFalse(batch.isControlBatch(),
                        "non-transactional surviving batch must remain a data batch");
                assertEquals(pid, batch.producerId(),
                        "non-transactional surviving batch carries its own pid — that is fine");
                nonTxBatchSurvived = true;
            } else if (batch.baseOffset() == 202L) {
                assertFalse(batch.isControlBatch(),
                        "COMMIT marker (offset 202) must be scrubbed to a non-control header — "
                                + "the prior non-transactional same-pid batch must NOT keep it alive");
                assertEquals(RecordBatch.NO_PRODUCER_ID, batch.producerId(),
                        "marker must not leak producer_id");
                assertEquals(RecordBatch.NO_PRODUCER_EPOCH, batch.producerEpoch(),
                        "marker must not leak producer_epoch");
                assertFalse(batch.isTransactional(),
                        "marker must clear isTransactional");
                assertFalse(batch.iterator().hasNext(),
                        "marker must produce a header-only batch — no COMMIT/ABORT byte, no coordinator_epoch");
                markerScrubbed = true;
            }
        }
        assertTrue(nonTxBatchSurvived,
                "non-transactional batch should round-trip through the filter");
        assertTrue(markerScrubbed,
                "expected the fully-hidden transaction's COMMIT marker to be scrubbed even though "
                        + "a same-pid non-transactional batch is visible");

        assertEquals(java.util.Set.of(202L), result.scrubbedMarkerOffsetsByProducerId().get(pid),
                "side-channel must report the scrubbed marker offset so KafkaApis "
                        + "can still find the transaction terminator");
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

    @Test
    void legacyV1BatchAllRecordsHiddenIsDroppedWithoutThrowing() {
        // R63: backing topics on upgraded clusters can still hold pre-3.0 segments with v0/v1
        // batches. ViewFilter previously returned RETAIN_EMPTY for every batch; MemoryRecords.filterTo
        // throws IllegalStateException("Empty batches are only supported for magic v2 and above")
        // when retainedRecords is empty and batch.magic() < V2 (MemoryRecords.java:197-199), making
        // a view fetch over a legacy all-hidden range fail instead of advancing. With DELETE_EMPTY
        // for legacy batches the batch is dropped silently; the consumer advances via the response's
        // high-watermark / lastStableOffset.
        CompiledPredicate p = compiler.compile("body.color == 'red'");
        MemoryRecords input = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, Compression.NONE,
                rec("{\"color\":\"blue\"}"),
                rec("{\"color\":\"green\"}"));

        MemoryRecords output = ViewFilter.apply(p, input, 0);

        assertEquals(List.of(), offsetsOf(output), "no records survive the legacy v1 filter");
        // No header emitted for legacy batches (DELETE_EMPTY drops them).
        assertFalse(output.batches().iterator().hasNext(),
                "legacy v0/v1 all-hidden batch must be dropped silently (no v2 placeholder)");
    }

    @Test
    void legacyV1BatchMatchingRecordsUpConvertToV2() {
        // R63 follow-on: legacy batches that contain at least one matching record must still
        // surface those records to the consumer. MemoryRecords.filterTo's
        // buildRetainedRecordsInto path rebuilds matching records into a CURRENT_MAGIC_VALUE (v2)
        // batch via writeOriginalBatch=false (MemoryRecords.java:229), so source offsets are
        // preserved and the consumer sees v2-encoded records.
        CompiledPredicate p = compiler.compile("body.color == 'red'");
        MemoryRecords input = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, Compression.NONE,
                rec("{\"color\":\"red\"}"),
                rec("{\"color\":\"blue\"}"),
                rec("{\"color\":\"red\"}"));

        MemoryRecords output = ViewFilter.apply(p, input, 0);

        assertEquals(List.of(0L, 2L), offsetsOf(output),
                "matching legacy v1 records preserved at their source offsets");
        for (MutableRecordBatch batch : output.batches()) {
            assertEquals(RecordBatch.CURRENT_MAGIC_VALUE, batch.magic(),
                    "legacy batch with surviving records up-converts to CURRENT_MAGIC_VALUE");
        }
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
