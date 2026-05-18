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
     * Apply {@code predicate} to {@code input}, producing a new {@link MemoryRecords} that
     * contains only the matching records.
     *
     * @param predicate the compiled predicate; must not be null.
     * @param input     the source records (typically the result of a fetch against the backing
     *                  topic). Must not be null.
     * @param partition the partition id, exposed to the predicate via the {@code partition}
     *                  binding.
     * @return a fresh {@link MemoryRecords} owning its own buffer.
     */
    public static MemoryRecords apply(CompiledPredicate predicate, MemoryRecords input, int partition) {
        if (predicate == null) {
            throw new IllegalArgumentException("predicate must not be null");
        }
        if (input == null) {
            throw new IllegalArgumentException("input must not be null");
        }
        int inputSize = input.sizeInBytes();
        ByteBuffer destination = ByteBuffer.allocate(Math.max(inputSize, 1));
        MemoryRecords.FilterResult result = input.filterTo(
                new RecordFilterImpl(predicate, partition),
                destination,
                BufferSupplier.NO_CACHING);
        ByteBuffer out = result.outputBuffer();
        out.flip();
        return MemoryRecords.readableRecords(out);
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

        RecordFilterImpl(CompiledPredicate predicate, int partition) {
            super(0L, 0L);
            this.predicate = predicate;
            this.partition = partition;
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
            // Skip (Optional.empty) and explicit false both drop the record. The distinction
            // is preserved inside CompiledPredicate for metrics, not for retention.
            return verdict.orElse(Boolean.FALSE);
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
