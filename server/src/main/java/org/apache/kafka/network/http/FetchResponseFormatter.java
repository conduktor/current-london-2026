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
package org.apache.kafka.network.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.common.protocol.Errors;

import java.util.List;
import java.util.Objects;

/**
 * Translates a single-partition fetch result into the JSON body the HTTP bridge returns from
 * {@code GET /v1/topics/{topic}/records}.
 *
 * <p>The body shape is forward-compatible with multi-partition fetch (which v1 does not expose at the URL layer):
 * <pre>
 * {
 *   "topic": "...",
 *   "partitions": [
 *     {
 *       "partition": N,
 *       "logStartOffset": ...,
 *       "highWatermark": ...,
 *       "records": [
 *         { "offset": N, "timestamp": ms,
 *           "key":   &lt;value-envelope | null&gt;,
 *           "value": &lt;value-envelope&gt; },
 *         ...
 *       ],
 *       "_links": { "self": "&lt;cursor at requestedOffset&gt;" }
 *     }
 *   ],
 *   "_links": {
 *     "first":    "&lt;cursor at logStartOffset&gt;",
 *     "previous": "&lt;cursor at requestedOffset-1&gt;" or null,
 *     "next":     "&lt;cursor past the last returned record&gt;",
 *     "last":     "&lt;cursor at highWatermark&gt;"
 *   }
 * }
 * </pre>
 *
 * <p>HATEOAS rules:
 * <ul>
 *   <li>Per-partition {@code _links.self} points at the offset the client just requested — it's the cursor for
 *       "render this same page again".</li>
 *   <li>{@code _links.next} points one offset past the last record we actually returned. When the response is empty
 *       (caught up to the high watermark, or long-polling), {@code next} repeats the requested offset.</li>
 *   <li>{@code _links.previous} steps one offset back. {@code null} when already at {@code logStartOffset}. We do not
 *       try to compute a "previous page boundary"; clients drive backward navigation themselves.</li>
 *   <li>{@code _links.first} is the cursor at {@code logStartOffset}; {@code _links.last} the high watermark.</li>
 * </ul>
 *
 * <p>Status: success → {@code 200}; per-partition failure → {@link HttpStatusMapper#toHttpStatus(Errors)} with an
 * {@link ErrorEnvelope} body. A positive {@code throttleTimeMs} always sets {@code Retry-After} regardless of status,
 * matching the quota semantics from {@link ProduceResponseFormatter}.
 */
public final class FetchResponseFormatter {

    private final ObjectMapper mapper;

    public FetchResponseFormatter(ObjectMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper);
    }

    /**
     * Format the result of a single-partition fetch.
     *
     * @param topic the topic name carried in the body
     * @param partition the fetched partition data (success or per-partition failure)
     * @param throttleTimeMs broker-side throttle delay in milliseconds; non-positive means no throttle
     */
    public Formatted format(String topic, PartitionFetch partition, long throttleTimeMs) {
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(partition, "partition must not be null");

        int retryAfter = RetryAfterCalculator.shouldSet(throttleTimeMs)
            ? RetryAfterCalculator.seconds(throttleTimeMs)
            : 0;

        if (partition.error != Errors.NONE) {
            ObjectNode body = ErrorEnvelope.forError(mapper, partition.error, partition.errorMessage);
            body.put("topic", topic);
            body.put("partition", partition.partition);
            return new Formatted(HttpStatusMapper.toHttpStatus(partition.error), body, retryAfter);
        }

        ObjectNode body = mapper.createObjectNode();
        body.put("topic", topic);
        ArrayNode partitions = body.putArray("partitions");
        partitions.add(renderPartition(topic, partition));
        body.set("_links", renderRootLinks(topic, partition));
        return new Formatted(HttpStatusMapper.OK, body, retryAfter);
    }

    /**
     * Format a top-level error — bridge-side validation failure, malformed query string, undecodable cursor, etc. —
     * before any partition state was reached.
     */
    public Formatted topLevelError(String topic, Errors error, String overrideMessage, long throttleTimeMs) {
        Objects.requireNonNull(topic, "topic must not be null");
        ObjectNode body = ErrorEnvelope.forError(mapper, error, overrideMessage);
        body.put("topic", topic);
        int status = HttpStatusMapper.toHttpStatus(error);
        int retryAfter = RetryAfterCalculator.shouldSet(throttleTimeMs)
            ? RetryAfterCalculator.seconds(throttleTimeMs)
            : 0;
        return new Formatted(status, body, retryAfter);
    }

    private ObjectNode renderPartition(String topic, PartitionFetch p) {
        ObjectNode node = mapper.createObjectNode();
        node.put("partition", p.partition);
        node.put("logStartOffset", p.logStartOffset);
        node.put("highWatermark", p.highWatermark);

        ArrayNode records = node.putArray("records");
        for (FetchedRecord r : p.records) {
            records.add(renderRecord(r));
        }

        ObjectNode links = node.putObject("_links");
        links.put("self", CursorCodec.encode(topic, p.partition, p.requestedOffset));
        return node;
    }

    private ObjectNode renderRecord(FetchedRecord r) {
        ObjectNode node = mapper.createObjectNode();
        node.put("offset", r.offset);
        node.put("timestamp", r.timestamp);
        node.set("key", r.key == null
            ? mapper.nullNode()
            : ValueSerializer.encode(r.key, null).toJson(mapper));
        node.set("value", ValueSerializer.encode(r.value, r.contentType).toJson(mapper));
        return node;
    }

    private ObjectNode renderRootLinks(String topic, PartitionFetch p) {
        long nextOffset = p.records.isEmpty()
            ? p.requestedOffset
            : p.records.get(p.records.size() - 1).offset + 1;

        ObjectNode links = mapper.createObjectNode();
        links.put("first", CursorCodec.encode(topic, p.partition, p.logStartOffset));
        if (p.requestedOffset > p.logStartOffset) {
            links.put("previous", CursorCodec.encode(topic, p.partition, p.requestedOffset - 1));
        } else {
            links.putNull("previous");
        }
        links.put("next", CursorCodec.encode(topic, p.partition, nextOffset));
        links.put("last", CursorCodec.encode(topic, p.partition, p.highWatermark));
        return links;
    }

    /** A single record as it should appear in the JSON response. */
    public static final class FetchedRecord {
        private final long offset;
        private final byte[] key;
        private final byte[] value;
        private final String contentType;
        private final long timestamp;

        public FetchedRecord(long offset, byte[] key, byte[] value, String contentType, long timestamp) {
            this.offset = offset;
            this.key = key;
            this.value = value;
            this.contentType = contentType;
            this.timestamp = timestamp;
        }

        public long offset() {
            return offset;
        }

        public byte[] key() {
            return key;
        }

        public byte[] value() {
            return value;
        }

        public String contentType() {
            return contentType;
        }

        public long timestamp() {
            return timestamp;
        }
    }

    /** Per-partition fetch view. error == NONE for success; non-NONE collapses the response to an error envelope. */
    public static final class PartitionFetch {
        private final int partition;
        private final Errors error;
        private final String errorMessage;
        private final long requestedOffset;
        private final long logStartOffset;
        private final long highWatermark;
        private final List<FetchedRecord> records;

        public PartitionFetch(int partition, Errors error, String errorMessage,
                              long requestedOffset, long logStartOffset, long highWatermark,
                              List<FetchedRecord> records) {
            this.partition = partition;
            this.error = Objects.requireNonNull(error, "error must not be null");
            this.errorMessage = errorMessage;
            this.requestedOffset = requestedOffset;
            this.logStartOffset = logStartOffset;
            this.highWatermark = highWatermark;
            this.records = Objects.requireNonNull(records, "records must not be null");
        }

        public int partition() {
            return partition;
        }

        public Errors error() {
            return error;
        }

        public String errorMessage() {
            return errorMessage;
        }

        public long requestedOffset() {
            return requestedOffset;
        }

        public long logStartOffset() {
            return logStartOffset;
        }

        public long highWatermark() {
            return highWatermark;
        }

        public List<FetchedRecord> records() {
            return records;
        }
    }

    /** What the bridge needs to write the HTTP response: status, JSON body, optional Retry-After seconds. */
    public static final class Formatted {
        private final int status;
        private final JsonNode body;
        private final int retryAfterSeconds;

        Formatted(int status, JsonNode body, int retryAfterSeconds) {
            this.status = status;
            this.body = body;
            this.retryAfterSeconds = retryAfterSeconds;
        }

        public int status() {
            return status;
        }

        public JsonNode body() {
            return body;
        }

        public boolean hasRetryAfter() {
            return retryAfterSeconds > 0;
        }

        public int retryAfterSeconds() {
            return retryAfterSeconds;
        }
    }
}
