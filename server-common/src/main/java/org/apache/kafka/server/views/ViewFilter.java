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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Applies a {@link CompiledPredicate} to a batch of records, emitting only the records that
 * evaluate to {@code true}. Records whose evaluation returns {@code false} or
 * {@link Optional#empty()} (predicate skip — malformed JSON, cost-cap hit, etc.) are dropped.
 *
 * Crucially, batches whose records are entirely filtered out are NOT removed from the output
 * — they are retained as header-only batches. This matters because Kafka consumers track the
 * last fetched offset; if a backing-topic batch covering offsets {@code [100..200]} produces
 * no matching records, the consumer must still see {@code lastOffset = 200} or it will
 * re-fetch the same range indefinitely. {@link MemoryRecords.RecordFilter.BatchRetention#RETAIN_EMPTY}
 * writes an empty batch header that carries {@code baseOffset} and {@code lastOffset} from the
 * source batch, satisfying the sparse-offset acceptance criterion from PROMPT.md.
 *
 * Control batches (transaction markers) are also retained as empty so EOS semantics on the
 * backing topic propagate transparently to the view consumer.
 *
 * Thread-safe: every {@link #apply} call instantiates a fresh {@link RecordFilterImpl}, and
 * {@link CompiledPredicate#evaluate} uses only stack-local state.
 */
public final class ViewFilter {

    private ViewFilter() {
    }

    /**
     * Backwards-compatible overload: no metrics. Delegates to the metrics-aware overload with
     * {@link ViewMetrics#NOOP} so existing unit-test call sites that do not care about
     * observability don't need to thread a metrics instance through.
     */
    public static MemoryRecords apply(CompiledPredicate predicate, MemoryRecords input, int partition) {
        return apply(predicate, input, partition, ViewMetrics.NOOP);
    }

    /**
     * Apply {@code predicate} to {@code input}, producing a new {@link MemoryRecords} that
     * contains only the matching records. Convenience overload that uses
     * {@link BufferSupplier#NO_CACHING} for decompression staging — fine for unit tests but
     * wasteful under sustained fetch load. Production callers (the fetch handler) should use the
     * overload that accepts a {@link BufferSupplier}.
     */
    public static MemoryRecords apply(CompiledPredicate predicate, MemoryRecords input,
                                       int partition, ViewMetrics metrics) {
        return apply(predicate, input, partition, metrics, BufferSupplier.NO_CACHING);
    }

    /**
     * Apply {@code predicate} to {@code input}, producing a new {@link MemoryRecords} that
     * contains only the matching records.
     *
     * @param predicate            the compiled predicate; must not be null.
     * @param input                the source records (typically the result of a fetch against the
     *                             backing topic). Must not be null.
     * @param partition            the partition id, exposed to the predicate via the
     *                             {@code partition} binding.
     * @param metrics              meters for evaluations/skips/bytes. Pass {@link ViewMetrics#NOOP}
     *                             if you don't have a real metrics wired in (unit tests).
     * @param decompressionBuffers staging buffers for compressed-batch iteration; threaded into
     *                             {@link MemoryRecords#filterTo}. Reuse one supplier across all
     *                             view partitions in a single fetch callback so decompression
     *                             buffers can be reused across batches. Pass
     *                             {@link BufferSupplier#NO_CACHING} when allocation churn does not
     *                             matter (unit tests). Must not be null.
     * @return a fresh {@link MemoryRecords} owning its own buffer.
     */
    public static MemoryRecords apply(CompiledPredicate predicate, MemoryRecords input,
                                       int partition, ViewMetrics metrics,
                                       BufferSupplier decompressionBuffers) {
        if (predicate == null) {
            throw new IllegalArgumentException("predicate must not be null");
        }
        if (input == null) {
            throw new IllegalArgumentException("input must not be null");
        }
        if (metrics == null) {
            throw new IllegalArgumentException("metrics must not be null (use ViewMetrics.NOOP for none)");
        }
        if (decompressionBuffers == null) {
            throw new IllegalArgumentException("decompressionBuffers must not be null "
                    + "(use BufferSupplier.NO_CACHING if you don't want pooling)");
        }
        int inputSize = input.sizeInBytes();
        // The destination buffer is the filtered output: its lifetime extends past this method
        // (it backs the MemoryRecords we hand back, which is then serialized into the FetchResponse
        // on the network thread). We cannot return it to the supplier here without coordinating
        // a release with the response-send path, so it stays as a fresh allocation. The supplier
        // still earns its keep on decompression staging inside filterTo and on the FileRecords
        // slurp buffer in the caller (KafkaApis.applyViewFilter).
        ByteBuffer destination = ByteBuffer.allocate(Math.max(inputSize, 1));
        MemoryRecords.FilterResult result = input.filterTo(
                new RecordFilterImpl(predicate, partition, metrics),
                destination,
                decompressionBuffers);
        ByteBuffer out = result.outputBuffer();
        out.flip();
        MemoryRecords filtered = MemoryRecords.readableRecords(out);
        // Strip the backing topic's partition_leader_epoch from every filtered batch header. The
        // view partition maintains its own (lower) leader-epoch ledger, so leaking the backing
        // epoch through the records wedges the consumer:
        //   CompletedFetch  -> lastEpoch = currentBatch.partitionLeaderEpoch()    (backing epoch)
        //   FetchCollector  -> position.offsetEpoch = lastEpoch
        //   SubscriptionState -> AWAIT_VALIDATION on the *view* partition with the *backing* epoch
        //   OffsetsForLeaderEpoch on the view -> currently rejected with INVALID_REQUEST
        //   handleResponse default branch -> partition stays in partitionsToRetry forever
        // so the consumer is stuck in non-fetchable AWAIT_VALIDATION + infinite OFLE retry.
        // Mutating partition_leader_epoch in place is safe: the DefaultRecordBatch byte layout
        // places PARTITION_LEADER_EPOCH_OFFSET (12) BEFORE ATTRIBUTES_OFFSET (21), and the v2
        // CRC32C covers only [ATTRIBUTES_OFFSET, end), so the 4-byte rewrite does NOT invalidate
        // the batch checksum. Legacy v0/v1 batches don't carry the field at all and throw on the
        // setter, so we guard on magic >= MAGIC_VALUE_V2 — those batches cannot leak a backing
        // epoch because the field doesn't exist on the wire.
        // Two scrubs happen here:
        //   (1) partition_leader_epoch on every retained batch (see comment above).
        //   (2) on FULLY-FILTERED data batches (RETAIN_EMPTY produced a header but no records),
        //       producer_id / producer_epoch / base_sequence / max_timestamp / transactional flag
        //       must also be cleared. Kafka's MemoryRecords.writeEmptyHeader copies these fields
        //       verbatim from the source batch, so an empty data batch in the view fetch otherwise
        //       broadcasts the BACKING producer's identity, sequence, timestamp range, and
        //       transactional status — metadata about records that were specifically filtered
        //       OUT, leaked through the view's "I filtered them" placeholder. PROMPT.md mandates
        //       only baseOffset and lastOffset survive on the empty batch ("Preserves source
        //       offsets"); everything else is leakage.
        //
        // Control batches (transaction markers) are scrubbed when their producerId has NO
        // surviving non-control data anywhere in the filtered output. That is the case Codex
        // (R58) called out: a producer writes only records that the predicate filters out,
        // commits or aborts the transaction, and the marker still reaches the view consumer —
        // exposing the backing producer's id, epoch, transaction outcome, coordinator epoch,
        // and source offset boundary for a transaction the predicate completely hid.
        // (KafkaApis.filterAbortedTransactionsByVisibleProducers strips the matching
        // abortedTransactions entry in this case, so the marker is also functionally a no-op
        // for the consumer's READ_COMMITTED state machine — pure leakage.)
        //
        // We use a PER-PRODUCER-ID rule, not per-transaction-range: a marker is kept iff some
        // surviving non-control data in the same fetch shares its producerId. The narrower
        // per-transaction-range rule (keep only when the same OPEN transaction had survivors)
        // would scrub the boundary marker of a hidden transaction whose producer also had a
        // visible transaction later — but the downstream
        // KafkaApis.filterAbortedTransactionsByVisibleProducers walks markers to bound the
        // search range for each abortedTransactions entry, and a scrubbed marker no longer
        // carries its producerId so it can no longer act as a boundary. Per-pid retention
        // keeps the marker's pid intact in that case, leaving the boundary visible. The
        // canonical multi-tenancy model (one producer per tenant) is fully covered by per-pid.
        //
        // Scrubbed markers become header-only batches: baseOffset/lastOffset survive so LSO
        // advances, NO_PRODUCER_ID + NO_PRODUCER_EPOCH + NO_SEQUENCE + isTransactional=false +
        // isControlRecord=false are written explicitly, and the marker record body is dropped
        // (no coordinator-epoch leakage, no ControlRecordType.COMMIT|ABORT leakage).
        //
        // The rewrite uses DefaultRecordBatch.writeEmptyHeader (public) into a fresh buffer of
        // the SAME size as the post-filter output — scrubbed batches are exactly
        // RECORD_BATCH_OVERHEAD bytes whether the source was an empty data batch or a
        // fully-hidden control batch, and non-empty data / partially-visible control batches
        // are copied via batch.writeTo unchanged. We skip the rebuild entirely (single pass,
        // in-place setPartitionLeaderEpoch only) when no scrub is required, which is the common
        // case under permissive predicates with no fully-hidden producers.
        filtered = scrubBackingMetadata(filtered);
        metrics.recordBytes(inputSize, filtered.sizeInBytes());
        return filtered;
    }

    /**
     * Walks the post-filter batches to perform the three scrubs described in the apply-method
     * comment block (partition_leader_epoch on every retained batch, empty-data-batch identity
     * fields, and fully-hidden control-batch identity + marker payload). Returns the resulting
     * {@link MemoryRecords} — either the input (rebuild skipped) or a fresh rebuild.
     */
    private static MemoryRecords scrubBackingMetadata(MemoryRecords filtered) {
        Set<Long> survivingDataPids = null;
        boolean hasEmptyDataBatch = false;
        boolean hasAnyControlBatch = false;
        for (MutableRecordBatch batch : filtered.batches()) {
            if (batch.magic() < RecordBatch.MAGIC_VALUE_V2) {
                continue;
            }
            batch.setPartitionLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH);
            if (batch.isControlBatch()) {
                hasAnyControlBatch = true;
            } else if (isEmptyDataBatch(batch)) {
                hasEmptyDataBatch = true;
            } else if (hasSurvivingRecords(batch)) {
                if (survivingDataPids == null) {
                    survivingDataPids = new HashSet<>();
                }
                survivingDataPids.add(batch.producerId());
            }
        }
        final Set<Long> survivingPids = survivingDataPids == null ? Set.of() : survivingDataPids;
        boolean hasFullyHiddenControlBatch = hasAnyControlBatch
                && hasFullyHiddenControlBatch(filtered, survivingPids);
        if (!hasEmptyDataBatch && !hasFullyHiddenControlBatch) {
            return filtered;
        }
        ByteBuffer rebuilt = ByteBuffer.allocate(filtered.sizeInBytes());
        for (MutableRecordBatch batch : filtered.batches()) {
            if (shouldScrubAsEmpty(batch, survivingPids)) {
                DefaultRecordBatch.writeEmptyHeader(
                        rebuilt,
                        RecordBatch.CURRENT_MAGIC_VALUE,
                        RecordBatch.NO_PRODUCER_ID,
                        RecordBatch.NO_PRODUCER_EPOCH,
                        RecordBatch.NO_SEQUENCE,
                        batch.baseOffset(),
                        batch.lastOffset(),
                        RecordBatch.NO_PARTITION_LEADER_EPOCH,
                        batch.timestampType(),
                        RecordBatch.NO_TIMESTAMP,
                        false,  // isTransactional — cleared
                        false   // isControlRecord — cleared (header-only offset placeholder)
                );
            } else {
                batch.writeTo(rebuilt);
            }
        }
        rebuilt.flip();
        return MemoryRecords.readableRecords(rebuilt);
    }

    private static boolean hasSurvivingRecords(MutableRecordBatch batch) {
        Integer count = batch.countOrNull();
        return count != null && count > 0;
    }

    private static boolean hasFullyHiddenControlBatch(MemoryRecords filtered, Set<Long> survivingPids) {
        for (MutableRecordBatch batch : filtered.batches()) {
            if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2
                    && batch.isControlBatch()
                    && !survivingPids.contains(batch.producerId())) {
                return true;
            }
        }
        return false;
    }

    private static boolean shouldScrubAsEmpty(MutableRecordBatch batch, Set<Long> survivingPids) {
        if (batch.magic() < RecordBatch.MAGIC_VALUE_V2) {
            return false;
        }
        if (isEmptyDataBatch(batch)) {
            return true;
        }
        return batch.isControlBatch() && !survivingPids.contains(batch.producerId());
    }

    private static boolean isEmptyDataBatch(MutableRecordBatch batch) {
        if (batch.isControlBatch()) {
            return false;
        }
        Integer count = batch.countOrNull();
        return count != null && count == 0;
    }

    /**
     * Adapter that bridges {@link CompiledPredicate#evaluate} into Kafka's
     * {@link MemoryRecords.RecordFilter}. {@code currentTime} and {@code deleteRetentionMs} are
     * passed as {@code 0L}: the view filter never participates in delete-horizon logic because
     * it always returns {@link MemoryRecords.RecordFilter.BatchRetention#RETAIN_EMPTY}, which
     * bypasses the tombstone-horizon path inside {@link MemoryRecords#filterTo}.
     */
    private static final class RecordFilterImpl extends MemoryRecords.RecordFilter {
        private final CompiledPredicate predicate;
        private final int partition;
        private final ViewMetrics metrics;

        RecordFilterImpl(CompiledPredicate predicate, int partition, ViewMetrics metrics) {
            super(0L, 0L);
            this.predicate = predicate;
            this.partition = partition;
            this.metrics = metrics;
        }

        @Override
        protected BatchRetentionResult checkBatchRetention(RecordBatch batch) {
            // RETAIN_EMPTY for every batch — data and control. For data batches, an empty header
            // carries the source (baseOffset, lastOffset) so the consumer advances even through a
            // fully-filtered span. For control batches we *also* retain the marker record inside
            // via shouldRetainRecord — see the comment there for why an empty control header
            // breaks READ_COMMITTED isolation.
            return new BatchRetentionResult(BatchRetention.RETAIN_EMPTY, false);
        }

        @Override
        protected boolean shouldRetainRecord(RecordBatch batch, Record record) {
            // Control batches (COMMIT/ABORT end-transaction markers) must propagate through the
            // filter with their record payload intact. The consumer's READ_COMMITTED logic walks
            // the first record of a control batch and parses its key for ControlRecordType.ABORT
            // (see CompletedFetch.containsAbortMarker); a stripped marker is indistinguishable
            // from "no transaction terminator", so aborted producer-ids leak as committed and
            // their records are surfaced to applications. Predicates are still never evaluated
            // against control records — they carry no user payload — but the record itself rides
            // through unchanged.
            if (batch.isControlBatch()) {
                return true;
            }
            RecordContext ctx = contextFor(batch, record, partition);
            Optional<Boolean> verdict = predicate.evaluate(ctx);
            metrics.recordEvaluation();
            if (verdict.isEmpty()) {
                // Predicate skipped this record (cost-cap, malformed JSON, non-boolean, etc.).
                // Surfaces under PredicateSkipRate so operators see anomalies before consumers
                // notice silently-dropped records.
                metrics.recordSkip();
                return false;
            }
            return verdict.get();
        }
    }

    /**
     * Builds a {@link RecordContext} from a single {@link Record} without copying the key/value
     * bytes. The buffers are stable for the duration of {@link #shouldRetainRecord} — Kafka does
     * not advance the underlying record stream until {@code shouldRetainRecord} returns — so the
     * context can hold ByteBuffer references directly. Materialising a byte[] is deferred to the
     * (rare) {@link RecordContext#rawKey()} / {@link RecordContext#rawBody()} call.
     *
     * <p>Headers are still copied into the context's per-record map (`b.header` clones the byte[]
     * reference into a list, then DefaultRecordContext copies into a HashMap). That overhead is
     * bounded by the small header count typical of Kafka records and is dwarfed by the body-copy
     * win.</p>
     */
    private static RecordContext contextFor(RecordBatch batch, Record record, int partition) {
        RecordContexts.Builder b = RecordContexts.builder()
                .keyBuffer(record.hasKey() ? record.key() : null)
                .bodyBuffer(record.hasValue() ? record.value() : null)
                .offset(record.offset())
                .partition(partition)
                .timestamp(record.timestamp() == RecordBatch.NO_TIMESTAMP
                        ? batch.maxTimestamp()
                        : record.timestamp());
        Header[] headers = record.headers();
        if (headers != null) {
            for (Header h : headers) {
                if (h == null) continue;
                b.header(h.key(), h.value());
            }
        }
        return b.build();
    }

    /**
     * Package-private accessor for tests/wiring code that need the same context shape produced
     * during evaluation (e.g. a debug endpoint that wants to show what a record looks like to
     * a predicate). Not part of the public API.
     */
    static RecordContext debugContext(RecordBatch batch, Record record, int partition) {
        return contextFor(batch, record, partition);
    }
}
