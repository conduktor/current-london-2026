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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;

/**
 * Translates the JSON body of {@code POST /v1/topics/{topic}/records} into a {@link ProduceCommand} — a plain Java
 * view of what the bridge needs to build a {@code ProduceRequest}.
 *
 * <p>Body shape:
 * <pre>
 * {
 *   "records": [
 *     { "partition": &lt;int&gt; (optional),
 *       "key":   &lt;value-envelope&gt; (optional),
 *       "value": &lt;value-envelope&gt; (required)
 *     }, ...
 *   ]
 * }
 * </pre>
 * Each {@code key} / {@code value} is the same four-shape envelope handled by {@link ValueSerializer} — keeping the
 * encoding rules in one place rather than re-implementing them here.
 *
 * <p>Validation errors raise {@link BadRequestException}, which the bridge maps to {@code 400 Bad Request} via the
 * shared {@link ErrorEnvelope}. The parser is the only authoritative source of "is this body well-formed?" — once we
 * return a {@code ProduceCommand}, downstream code can trust the fields.
 */
public final class ProduceRequestParser {

    private ProduceRequestParser() {
    }

    public static ProduceCommand parse(String topic, JsonNode body) {
        if (topic == null || topic.trim().isEmpty()) {
            throw new BadRequestException("topic must not be empty");
        }
        if (body == null || !body.isObject()) {
            throw new BadRequestException("request body must be a JSON object");
        }
        JsonNode recordsNode = body.get("records");
        if (recordsNode == null) {
            throw new BadRequestException("missing field: records");
        }
        if (!recordsNode.isArray()) {
            throw new BadRequestException("field 'records' must be an array");
        }
        if (recordsNode.size() == 0) {
            throw new BadRequestException("field 'records' must not be empty");
        }

        List<RecordEntry> entries = new ArrayList<>(recordsNode.size());
        for (int i = 0; i < recordsNode.size(); i++) {
            entries.add(parseEntry(i, recordsNode.get(i)));
        }
        return new ProduceCommand(topic, Collections.unmodifiableList(entries));
    }

    private static RecordEntry parseEntry(int index, JsonNode entry) {
        if (entry == null || !entry.isObject()) {
            throw new BadRequestException("records[" + index + "] must be a JSON object");
        }

        OptionalInt partition = readPartition(index, entry.get("partition"));
        byte[] key = decodeEnvelope(index, "key", entry.get("key"), false);
        byte[] value = decodeEnvelope(index, "value", entry.get("value"), true);
        String contentType = entry.has("value") ? ValueSerializer.contentTypeFor(entry.get("value")) : null;

        return new RecordEntry(partition, key, value, contentType);
    }

    private static OptionalInt readPartition(int index, JsonNode partitionNode) {
        if (partitionNode == null || partitionNode.isNull()) {
            return OptionalInt.empty();
        }
        if (!partitionNode.isInt()) {
            throw new BadRequestException("records[" + index + "].partition must be an integer");
        }
        int p = partitionNode.asInt();
        if (p < 0) {
            throw new BadRequestException(
                "records[" + index + "].partition must be non-negative (omit the field to let the broker decide)");
        }
        return OptionalInt.of(p);
    }

    private static byte[] decodeEnvelope(int index, String fieldName, JsonNode envelope, boolean required) {
        if (envelope == null || envelope.isNull()) {
            if (required) {
                throw new BadRequestException("records[" + index + "]." + fieldName + " is required");
            }
            return null;
        }
        try {
            return ValueSerializer.decode(envelope);
        } catch (IllegalArgumentException e) {
            throw new BadRequestException(
                "records[" + index + "]." + fieldName + ": " + e.getMessage(), e);
        }
    }

    /** A request body that survived validation. */
    public static final class ProduceCommand {
        private final String topic;
        private final List<RecordEntry> records;

        public ProduceCommand(String topic, List<RecordEntry> records) {
            this.topic = Objects.requireNonNull(topic, "topic must not be null");
            this.records = Objects.requireNonNull(records, "records must not be null");
        }

        public String topic() {
            return topic;
        }

        public List<RecordEntry> records() {
            return records;
        }
    }

    /** One record from the request body, decoded but not yet submitted to the broker. */
    public static final class RecordEntry {
        private final OptionalInt partition;
        private final byte[] key;
        private final byte[] value;
        private final String contentType;

        public RecordEntry(OptionalInt partition, byte[] key, byte[] value, String contentType) {
            this.partition = Objects.requireNonNull(partition);
            this.key = key;
            this.value = value;
            this.contentType = contentType;
        }

        public OptionalInt partition() {
            return partition;
        }

        public byte[] key() {
            return key;
        }

        public byte[] value() {
            return value;
        }

        /**
         * The {@code content-type} record header that should accompany this value, or {@code null} when none applies.
         * Today this is {@code application/json} for JSON-typed values and {@code null} for the rest — see
         * {@link ValueSerializer#contentTypeFor(JsonNode)}.
         */
        public String contentType() {
            return contentType;
        }
    }

    /** Thrown when the body fails validation. The bridge turns this into a {@code 400 Bad Request}. */
    public static final class BadRequestException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public BadRequestException(String message) {
            super(message);
        }

        public BadRequestException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
