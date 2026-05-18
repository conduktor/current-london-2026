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
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Objects;

/**
 * Rebuilds a {@link MemoryRecords} batch produced against a logical topic, augmenting each record
 * with the three concentration headers ({@link ConcentrationHeaders#LOGICAL_TOPIC_HEADER},
 * {@link ConcentrationHeaders#LOGICAL_PARTITION_HEADER},
 * {@link ConcentrationHeaders#LOGICAL_OFFSET_HEADER}) so that:
 *
 * <ul>
 *   <li>The fetch path can demultiplex records that share a backing partition with another
 *       logical topic.</li>
 *   <li>{@link BackingScanRecoverer} can rebuild the sidecar entirely from a backing-log scan
 *       when the durable sidecar is missing — the partition header is required here because
 *       {@link LogicalPartitionMapper} is many-to-one and the backing partition alone is
 *       insufficient to recover the originating logical partition.</li>
 * </ul>
 *
 * <p>This is a pure function. The original {@code MemoryRecords} is read but never mutated; a
 * fresh {@code MemoryRecords} is returned. Producer-id, epoch, base-sequence,
 * isTransactional, isControlBatch, timestampType, baseOffset, and partitionLeaderEpoch are all
 * preserved verbatim so idempotent-producer state survives the rewrite.
 *
 * <p>v1 supports a single record batch per input — stock producers send exactly one batch per
 * {@code (topic, partition)} entry in a ProduceRequest, which is the path this utility is on.
 * Multi-batch input raises {@link IllegalArgumentException}; lifting that restriction is a
 * targeted follow-up if a producer that emits multi-batch requests ever surfaces.
 */
public final class LogicalProduceStamper {

    private LogicalProduceStamper() { }

    /**
     * Return a new {@link MemoryRecords} whose records are identical to {@code source}'s except
     * that each has three extra headers appended:
     * {@code __concentration_logical_topic = utf8(logicalTopic)},
     * {@code __concentration_logical_partition = bigEndian(logicalPartition)}, and
     * {@code __concentration_logical_offset = bigEndian(logicalOffsets[i])}.
     *
     * @throws IllegalArgumentException if {@code source} has zero or more-than-one batches, if
     *     the number of records does not equal {@code logicalOffsets.length}, or if
     *     {@code logicalPartition} is negative.
     */
    public static MemoryRecords stamp(
        MemoryRecords source,
        String logicalTopic,
        int logicalPartition,
        long[] logicalOffsets
    ) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(logicalTopic, "logicalTopic");
        Objects.requireNonNull(logicalOffsets, "logicalOffsets");
        if (logicalPartition < 0) {
            throw new IllegalArgumentException("logicalPartition must be non-negative: " + logicalPartition);
        }

        Iterator<? extends RecordBatch> batchIter = source.batches().iterator();
        if (!batchIter.hasNext()) {
            throw new IllegalArgumentException("source MemoryRecords has no batches");
        }
        RecordBatch batch = batchIter.next();
        if (batchIter.hasNext()) {
            throw new IllegalArgumentException(
                "LogicalProduceStamper.stamp currently supports single-batch MemoryRecords only");
        }

        int recordCount = 0;
        for (Record r : batch) {
            recordCount++;
            if (r == null) {
                // defensive — iterators on corrupt records can yield null; the broker upstream
                // would have rejected this batch, but we'd rather fail explicitly here than NPE
                // mid-rebuild
                throw new IllegalArgumentException("null record encountered in batch");
            }
        }
        if (recordCount != logicalOffsets.length) {
            throw new IllegalArgumentException(
                "logicalOffsets length (" + logicalOffsets.length
                    + ") does not match record count (" + recordCount + ")");
        }

        byte[] logicalTopicBytes = logicalTopic.getBytes(StandardCharsets.UTF_8);
        byte[] logicalPartitionBytes = ByteBuffer.allocate(Integer.BYTES).putInt(logicalPartition).array();

        // Estimate output size: original batch size + 3 extra headers per record. Headers are
        // small (key + small-int value + per-record overhead) but the per-record varint encoding
        // means even small additions accumulate. Allocate generously — the buffer is shrunk by
        // build() if it's bigger than needed. Mirror LogValidator's pattern of summing
        // AbstractRecords.estimateSizeInBytes for the records body and adding fixed margins.
        int headerOverheadPerRecord =
            ConcentrationHeaders.LOGICAL_TOPIC_HEADER.length() + logicalTopicBytes.length
            + ConcentrationHeaders.LOGICAL_PARTITION_HEADER.length() + Integer.BYTES
            + ConcentrationHeaders.LOGICAL_OFFSET_HEADER.length() + Long.BYTES
            + 48; // varint and header-count growth margin
        int estimatedSize = AbstractRecords.estimateSizeInBytes(
            batch.magic(),
            batch.baseOffset(),
            batch.compressionType(),
            batch)
            + recordCount * headerOverheadPerRecord;

        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize);
        MemoryRecordsBuilder builder = MemoryRecords.builder(
            buffer,
            batch.magic(),
            Compression.of(batch.compressionType()).build(),
            batch.timestampType(),
            batch.baseOffset(),
            batch.timestampType() == org.apache.kafka.common.record.TimestampType.LOG_APPEND_TIME
                ? batch.maxTimestamp() : RecordBatch.NO_TIMESTAMP,
            batch.producerId(),
            batch.producerEpoch(),
            batch.baseSequence(),
            batch.isTransactional(),
            batch.isControlBatch(),
            batch.partitionLeaderEpoch()
        );

        long offsetCursor = batch.baseOffset();
        int offsetIndex = 0;
        for (Record record : batch) {
            Header[] orig = record.headers();
            Header[] augmented = new Header[orig.length + 3];
            System.arraycopy(orig, 0, augmented, 0, orig.length);
            augmented[orig.length] = new RecordHeader(
                ConcentrationHeaders.LOGICAL_TOPIC_HEADER,
                logicalTopicBytes);
            augmented[orig.length + 1] = new RecordHeader(
                ConcentrationHeaders.LOGICAL_PARTITION_HEADER,
                logicalPartitionBytes);
            ByteBuffer offsetValue = ByteBuffer.allocate(Long.BYTES);
            offsetValue.putLong(logicalOffsets[offsetIndex++]).flip();
            augmented[orig.length + 2] = new RecordHeader(
                ConcentrationHeaders.LOGICAL_OFFSET_HEADER,
                offsetValue.array());
            builder.appendWithOffset(offsetCursor++, record.timestamp(),
                record.key(), record.value(), augmented);
        }

        return builder.build();
    }

    /**
     * Count the records in {@code source} across all its batches. Cheap iteration — used by the
     * broker hook to size the logical-offset reservation before calling {@link #stamp}.
     */
    public static int countRecords(MemoryRecords source) {
        Objects.requireNonNull(source, "source");
        int count = 0;
        for (RecordBatch batch : source.batches()) {
            for (Record r : batch) {
                if (r != null) count++;
            }
        }
        return count;
    }

}
