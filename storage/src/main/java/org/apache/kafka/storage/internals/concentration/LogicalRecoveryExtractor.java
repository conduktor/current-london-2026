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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Pull {@link RecoveryRecord}s out of a {@link Records} block read from a backing log. Used by
 * the full-scan recovery path: the broker reads each backing partition page-by-page, hands the
 * page to this extractor, and streams the produced {@code RecoveryRecord}s through
 * {@link ConcentrationKernel#recoverFromBackingScan(java.util.Iterator)}.
 *
 * <p>This is a pure function. Its contract:
 * <ul>
 *   <li>Each input record must carry the three concentration headers stamped by
 *       {@link LogicalProduceStamper}: {@link ConcentrationHeaders#LOGICAL_TOPIC_HEADER},
 *       {@link ConcentrationHeaders#LOGICAL_PARTITION_HEADER}, and
 *       {@link ConcentrationHeaders#LOGICAL_OFFSET_HEADER}.</li>
 *   <li>Records whose {@code (logicalTopic, logicalPartition)} is not in {@code recoveryNeeded}
 *       are skipped silently — the caller decided those partitions are already recovered.</li>
 *   <li>Records missing any of the three headers, or with a non-decodable header value, are
 *       skipped silently rather than aborting the scan. A backing partition has many records;
 *       one malformed record (e.g., a record from before concentration was enabled) must not
 *       block recovery of the rest. {@link BackingScanRecoverer} is the authority on
 *       logical-offset gaps and will surface them.</li>
 *   <li>Records are emitted in the input order, so the backing-offset sequence is monotonic if
 *       and only if the caller fed input in offset order — which the broker's read-page loop
 *       does by construction.</li>
 * </ul>
 */
public final class LogicalRecoveryExtractor {

    private LogicalRecoveryExtractor() { }

    /**
     * Extract recovery records for the subset of partitions in {@code recoveryNeeded}. Records on
     * the backing log that belong to a logical partition already recovered (i.e., not in the
     * filter set) are intentionally dropped so that {@link BackingScanRecoverer#recoverFromScan}
     * does not clobber sidecars the cheap path already rebuilt.
     *
     * @param records a {@link Records} block from a backing partition (e.g., from
     *     {@code UnifiedLog.read(...)} on the broker side)
     * @param recoveryNeeded which (logicalTopic, logicalPartition) tuples to recover; other
     *     matches are skipped
     * @return one {@link RecoveryRecord} per matching input record, in input order
     */
    public static List<RecoveryRecord> extract(Records records, Collection<LogicalPartition> recoveryNeeded) {
        Objects.requireNonNull(records, "records");
        Objects.requireNonNull(recoveryNeeded, "recoveryNeeded");
        Set<LogicalPartition> filter = new HashSet<>(recoveryNeeded);
        List<RecoveryRecord> out = new ArrayList<>();
        if (filter.isEmpty()) return out;

        for (RecordBatch batch : records.batches()) {
            for (Record record : batch) {
                if (record == null) continue;
                RecoveryRecord rr = tryDecode(record);
                if (rr == null) continue;
                if (!filter.contains(new LogicalPartition(rr.logicalTopic(), rr.logicalPartition()))) continue;
                out.add(rr);
            }
        }
        return out;
    }

    private static RecoveryRecord tryDecode(Record record) {
        ParsedHeaders parsed = parseHeaders(record.headers());
        if (parsed.logicalTopic == null || parsed.logicalPartition == null || parsed.logicalOffset == null) {
            return null;
        }
        if (parsed.logicalPartition < 0 || parsed.logicalOffset < 0) return null;
        long backingOffset = record.offset();
        if (backingOffset < 0) return null;
        return new RecoveryRecord(parsed.logicalTopic, parsed.logicalPartition, parsed.logicalOffset, backingOffset);
    }

    private static ParsedHeaders parseHeaders(Header[] headers) {
        ParsedHeaders out = new ParsedHeaders();
        // Last-wins by design: see the matching comment in
        // {@link LogicalFetchTranslator#readMarkers}. Recovery reads records that have been
        // durably written, so the layer-1 stamper guard prevents this from mattering for records
        // produced after the guard ships — but during a rolling upgrade some backing pages may
        // contain records produced by a broker that lacked the guard. Last-wins ensures the
        // broker-stamped identity always overrides any client-supplied forgery, including
        // for the sidecar rebuild that recovery feeds.
        for (Header h : headers) {
            if (h == null || h.key() == null) continue;
            applyHeader(h, out);
        }
        return out;
    }

    private static void applyHeader(Header h, ParsedHeaders out) {
        if (ConcentrationHeaders.LOGICAL_TOPIC_HEADER.equals(h.key())) {
            if (h.value() != null) {
                out.logicalTopic = new String(h.value(), StandardCharsets.UTF_8);
            }
        } else if (ConcentrationHeaders.LOGICAL_PARTITION_HEADER.equals(h.key())) {
            if (h.value() != null && h.value().length == Integer.BYTES) {
                out.logicalPartition = ByteBuffer.wrap(h.value()).getInt();
            }
        } else if (ConcentrationHeaders.LOGICAL_OFFSET_HEADER.equals(h.key())) {
            if (h.value() != null && h.value().length == Long.BYTES) {
                out.logicalOffset = ByteBuffer.wrap(h.value()).getLong();
            }
        }
    }

    private static final class ParsedHeaders {
        String logicalTopic;
        Integer logicalPartition;
        Long logicalOffset;
    }
}
