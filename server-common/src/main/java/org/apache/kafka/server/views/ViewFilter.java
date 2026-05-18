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
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Optional;

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
        metrics.recordBytes(inputSize, filtered.sizeInBytes());
        return filtered;
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
            // RETAIN_EMPTY for every batch — data and control. The header carries the source
            // (baseOffset, lastOffset) so the consumer advances even through a fully-filtered
            // span. Control batches are not record-evaluated; filterTo simply writes their
            // empty header through.
            return new BatchRetentionResult(BatchRetention.RETAIN_EMPTY, false);
        }

        @Override
        protected boolean shouldRetainRecord(RecordBatch batch, Record record) {
            // Never evaluate predicates against control batches (they carry no user payload).
            if (batch.isControlBatch()) {
                return false;
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
     * Builds a {@link RecordContext} from a single {@link Record}, copying out the key, value,
     * and header bytes. We materialize {@code byte[]} eagerly so the underlying record buffer
     * may be advanced safely by Kafka after evaluation; JSON parsing and UTF-8 decoding remain
     * lazy inside the context implementation.
     */
    private static RecordContext contextFor(RecordBatch batch, Record record, int partition) {
        byte[] key = bytesOrNull(record.hasKey() ? record.key() : null);
        byte[] body = bytesOrNull(record.hasValue() ? record.value() : null);
        RecordContexts.Builder b = RecordContexts.builder()
                .key(key)
                .body(body)
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

    private static byte[] bytesOrNull(ByteBuffer buf) {
        if (buf == null) {
            return null;
        }
        return Utils.toArray(buf);
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
