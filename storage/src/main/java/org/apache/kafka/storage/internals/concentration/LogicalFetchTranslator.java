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
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Demultiplex-and-rewrite pass on a {@link MemoryRecords} fetched from a backing partition.
 * Keeps only the records whose {@link ConcentrationHeaders#LOGICAL_TOPIC_HEADER} matches
 * {@code targetLogicalTopic} AND whose {@link ConcentrationHeaders#LOGICAL_PARTITION_HEADER}
 * matches {@code targetLogicalPartition}, and rewrites each kept record's offset to the value
 * carried by its {@link ConcentrationHeaders#LOGICAL_OFFSET_HEADER}. The three concentration
 * headers (topic / partition / offset) are stripped from the output; user-supplied headers, key,
 * value, and timestamp survive verbatim.
 *
 * <p>This is the consume-side mirror of {@link LogicalProduceStamper}: where the stamper adds
 * the headers so a backing record can be identified by its logical (topic, partition), this
 * translator removes them after using them to filter and rewrite offsets. A stock consumer of the
 * logical topic must never see them — they are internal metadata.
 *
 * <p>Why filtering by topic alone is insufficient: PROMPT.md's premise is N&gt;&gt;M, so multiple
 * logical partitions of the same topic regularly share a backing partition. Two partitions of
 * {@code orders} mapped to {@code shared-0} would cross-leak records to each other if the
 * translator only checked the topic header — exactly the kind of correctness regression that
 * makes per-partition offsets and ordering meaningless to the stock consumer.
 *
 * <p>Output baseOffset is the first surviving record's logical offset.
 *
 * <p>This is a pure function. Idempotent-producer state (producerId, baseSequence) is NOT
 * preserved in the translated output: filtering breaks the per-batch sequence invariant by
 * construction, and producer idempotence is validated at the produce path, not the fetch path.
 */
public final class LogicalFetchTranslator {

    private LogicalFetchTranslator() { }

    /**
     * Demultiplex {@code source} and return a {@link MemoryRecords} that contains only the
     * records belonging to {@code (targetLogicalTopic, targetLogicalPartition)}, with offsets
     * rewritten from each record's {@link ConcentrationHeaders#LOGICAL_OFFSET_HEADER}.
     *
     * <p>If no records match, returns {@link MemoryRecords#EMPTY}.
     *
     * @throws NullPointerException if {@code source} or {@code targetLogicalTopic} is null
     * @throws IllegalArgumentException if {@code targetLogicalPartition} is negative
     */
    public static MemoryRecords translate(Records source, String targetLogicalTopic, int targetLogicalPartition) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(targetLogicalTopic, "targetLogicalTopic");
        if (targetLogicalPartition < 0) {
            throw new IllegalArgumentException("targetLogicalPartition must be non-negative: " + targetLogicalPartition);
        }

        FilterResult filtered = collectSurvivors(source, targetLogicalTopic, targetLogicalPartition);
        if (filtered.survivors.isEmpty()) {
            return MemoryRecords.EMPTY;
        }
        return rebuildBatch(filtered);
    }

    private static FilterResult collectSurvivors(Records source, String targetLogicalTopic, int targetLogicalPartition) {
        List<SurvivingRecord> survivors = new ArrayList<>();
        byte magic = RecordBatch.CURRENT_MAGIC_VALUE;
        TimestampType timestampType = TimestampType.CREATE_TIME;
        for (RecordBatch batch : source.batches()) {
            // Snap to the first batch's magic + timestamp type. v1 always rebuilds at NONE
            // compression on the response path — the throughput cost is paid here, but the
            // alternative is re-encoding compressed batches per filter pass, which is worse.
            magic = batch.magic();
            timestampType = batch.timestampType();
            for (Record record : batch) {
                SurvivingRecord s = maybeKeepRecord(record, targetLogicalTopic, targetLogicalPartition);
                if (s != null) survivors.add(s);
            }
        }
        return new FilterResult(survivors, magic, timestampType);
    }

    private static SurvivingRecord maybeKeepRecord(Record record, String targetLogicalTopic, int targetLogicalPartition) {
        if (record == null) return null;
        ConcentrationMarkers markers = readMarkers(record.headers());
        if (!targetLogicalTopic.equals(markers.logicalTopic)) return null;
        // Drop records whose logicalPartition header doesn't match. A null partition header
        // here means the stamper that wrote this record predates the partition-header rollout
        // (or the record was produced by an unrelated path). Either way we cannot prove the
        // record belongs to the caller's logical partition, so drop it — the alternative is
        // leaking cross-partition records, which corrupts per-partition ordering and offsets.
        if (markers.logicalPartition == null || markers.logicalPartition != targetLogicalPartition) {
            return null;
        }
        if (markers.logicalOffset == null) return null; // corrupt — skip rather than fail
        return new SurvivingRecord(
            markers.logicalOffset,
            record.timestamp(),
            copyBuffer(record.key()),
            copyBuffer(record.value()),
            stripConcentrationHeaders(record.headers()));
    }

    private static ConcentrationMarkers readMarkers(Header[] headers) {
        String logicalTopic = null;
        Integer logicalPartition = null;
        Long logicalOffset = null;
        // Last-wins by design: {@link LogicalProduceStamper#stamp} appends the broker's three
        // concentration headers at the tail of each record's header array. If a malformed record
        // (e.g., one persisted by a pre-guard broker during a rolling upgrade) carries duplicate
        // entries, the broker-stamped values are always physically last and must override any
        // client-supplied values earlier in the array. Using first-wins here would let a client
        // forge the logical topic / partition / offset and route its record under another
        // tenant's identity. We also cannot {@code break} on first match for the same reason.
        for (Header h : headers) {
            if (h == null || h.key() == null) continue;
            if (ConcentrationHeaders.LOGICAL_TOPIC_HEADER.equals(h.key())) {
                if (h.value() != null) {
                    logicalTopic = new String(h.value(), StandardCharsets.UTF_8);
                }
            } else if (ConcentrationHeaders.LOGICAL_PARTITION_HEADER.equals(h.key())) {
                byte[] v = h.value();
                if (v != null && v.length == Integer.BYTES) {
                    logicalPartition = ByteBuffer.wrap(v).getInt();
                }
            } else if (ConcentrationHeaders.LOGICAL_OFFSET_HEADER.equals(h.key())) {
                byte[] v = h.value();
                if (v != null && v.length == Long.BYTES) {
                    logicalOffset = ByteBuffer.wrap(v).getLong();
                }
            }
        }
        return new ConcentrationMarkers(logicalTopic, logicalPartition, logicalOffset);
    }

    private static MemoryRecords rebuildBatch(FilterResult filtered) {
        List<SurvivingRecord> survivors = filtered.survivors;
        long baseOffset = survivors.get(0).logicalOffset;
        int estimated = estimateBufferSize(survivors);
        ByteBuffer buffer = ByteBuffer.allocate(estimated);
        long logAppendTime = filtered.timestampType == TimestampType.LOG_APPEND_TIME
            ? survivors.get(0).timestamp
            : RecordBatch.NO_TIMESTAMP;
        MemoryRecordsBuilder builder = MemoryRecords.builder(
            buffer,
            filtered.magic,
            Compression.NONE,
            filtered.timestampType,
            baseOffset,
            logAppendTime,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_EPOCH,
            RecordBatch.NO_SEQUENCE,
            false,
            false,
            RecordBatch.NO_PARTITION_LEADER_EPOCH);
        for (SurvivingRecord s : survivors) {
            ByteBuffer key = s.key == null ? null : ByteBuffer.wrap(s.key);
            ByteBuffer value = s.value == null ? null : ByteBuffer.wrap(s.value);
            builder.appendWithOffset(s.logicalOffset, s.timestamp, key, value, s.headers);
        }
        return builder.build();
    }

    private static int estimateBufferSize(List<SurvivingRecord> survivors) {
        int estimated = 128; // batch header
        for (SurvivingRecord s : survivors) {
            estimated += 24; // per-record overhead estimate (varints, etc.)
            estimated += s.key == null ? 0 : s.key.length;
            estimated += s.value == null ? 0 : s.value.length;
            for (Header h : s.headers) {
                estimated += 8 + h.key().length() + (h.value() == null ? 0 : h.value().length);
            }
        }
        return estimated;
    }

    private static byte[] copyBuffer(ByteBuffer buf) {
        if (buf == null) return null;
        byte[] out = new byte[buf.remaining()];
        buf.duplicate().get(out);
        return out;
    }

    private static Header[] stripConcentrationHeaders(Header[] in) {
        int retained = countRetained(in);
        Header[] out = new Header[retained];
        int j = 0;
        for (Header h : in) {
            if (!isConcentrationHeader(h)) {
                out[j++] = h;
            }
        }
        return out;
    }

    private static int countRetained(Header[] in) {
        int retained = 0;
        for (Header h : in) {
            if (!isConcentrationHeader(h)) retained++;
        }
        return retained;
    }

    private static boolean isConcentrationHeader(Header h) {
        return ConcentrationHeaders.LOGICAL_TOPIC_HEADER.equals(h.key())
            || ConcentrationHeaders.LOGICAL_PARTITION_HEADER.equals(h.key())
            || ConcentrationHeaders.LOGICAL_OFFSET_HEADER.equals(h.key());
    }

    private static final class SurvivingRecord {
        final long logicalOffset;
        final long timestamp;
        final byte[] key;
        final byte[] value;
        final Header[] headers;

        SurvivingRecord(long logicalOffset, long timestamp, byte[] key, byte[] value, Header[] headers) {
            this.logicalOffset = logicalOffset;
            this.timestamp = timestamp;
            this.key = key;
            this.value = value;
            this.headers = headers;
        }
    }

    private static final class FilterResult {
        final List<SurvivingRecord> survivors;
        final byte magic;
        final TimestampType timestampType;

        FilterResult(List<SurvivingRecord> survivors, byte magic, TimestampType timestampType) {
            this.survivors = survivors;
            this.magic = magic;
            this.timestampType = timestampType;
        }
    }

    private static final class ConcentrationMarkers {
        final String logicalTopic;
        final Integer logicalPartition;
        final Long logicalOffset;

        ConcentrationMarkers(String logicalTopic, Integer logicalPartition, Long logicalOffset) {
            this.logicalTopic = logicalTopic;
            this.logicalPartition = logicalPartition;
            this.logicalOffset = logicalOffset;
        }
    }
}
