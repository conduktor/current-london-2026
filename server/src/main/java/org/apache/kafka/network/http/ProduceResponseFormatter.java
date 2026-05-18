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
 * Translates a produce response (one topic, many partitions) into the HTTP-shaped result the bridge returns to the
 * caller. Encapsulates the spec's three-state status logic:
 * <ul>
 *   <li>every partition succeeded → {@code 200 OK}</li>
 *   <li>every partition failed with the same Kafka error code → that error's mapped HTTP status (one true uniform failure)</li>
 *   <li>anything else (mixed success/failure, or multiple distinct error codes) → {@code 207 Multi-Status}</li>
 * </ul>
 * {@code throttleTimeMs} is orthogonal: when positive, a {@code Retry-After} header is set regardless of status.
 * That includes the quota-exhaustion case where the produce succeeded ({@code 200}) but the caller should still slow down.
 */
public final class ProduceResponseFormatter {

    private final ObjectMapper mapper;

    public ProduceResponseFormatter(ObjectMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper);
    }

    /**
     * Format the result of a produce against a single topic across one or more partitions.
     *
     * @param topic the topic name carried in the body
     * @param results one entry per partition that was attempted
     * @param throttleTimeMs broker-side throttle delay in milliseconds; non-positive means no throttle
     */
    public Formatted format(String topic, List<PartitionResult> results, long throttleTimeMs) {
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(results, "results must not be null");

        ObjectNode body = mapper.createObjectNode();
        body.put("topic", topic);
        ArrayNode entries = body.putArray("results");
        for (PartitionResult r : results) {
            entries.add(renderEntry(r));
        }

        int status = resolveStatus(results);
        int retryAfter = RetryAfterCalculator.shouldSet(throttleTimeMs)
            ? RetryAfterCalculator.seconds(throttleTimeMs)
            : 0;
        return new Formatted(status, body, retryAfter);
    }

    /**
     * Format a request that failed before any partition-level result could be produced — a malformed body, an unknown
     * topic at the bridge layer, etc. Produces an {@link ErrorEnvelope}-shaped body rather than a partitioned results
     * array, because there is no partition state to report.
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

    private ObjectNode renderEntry(PartitionResult r) {
        ObjectNode node = mapper.createObjectNode();
        node.put("partition", r.partition());
        if (r.error() == Errors.NONE) {
            node.put("offset", r.offset());
            node.put("errorCode", 0);
            node.putNull("errorMessage");
        } else {
            node.putNull("offset");
            node.put("errorCode", r.error().code());
            String msg = r.errorMessage() != null ? r.errorMessage() : r.error().message();
            node.put("errorMessage", msg != null ? msg : "");
        }
        return node;
    }

    private static int resolveStatus(List<PartitionResult> results) {
        if (results.isEmpty()) {
            return HttpStatusMapper.INTERNAL_SERVER_ERROR;
        }

        boolean anySuccess = false;
        boolean anyFailure = false;
        Errors firstFailure = null;
        boolean uniformFailure = true;

        for (PartitionResult r : results) {
            if (r.error() == Errors.NONE) {
                anySuccess = true;
            } else {
                anyFailure = true;
                if (firstFailure == null) {
                    firstFailure = r.error();
                } else if (firstFailure != r.error()) {
                    uniformFailure = false;
                }
            }
        }

        if (anySuccess && !anyFailure) {
            return HttpStatusMapper.OK;
        }
        if (anyFailure && !anySuccess && uniformFailure) {
            return HttpStatusMapper.toHttpStatus(firstFailure);
        }
        return HttpStatusMapper.MULTI_STATUS;
    }

    /** Single-partition projection of a produce response. */
    public static final class PartitionResult {
        private final int partition;
        private final long offset;
        private final Errors error;
        private final String errorMessage;

        public PartitionResult(int partition, long offset, Errors error, String errorMessage) {
            this.partition = partition;
            this.offset = offset;
            this.error = Objects.requireNonNull(error, "error must not be null");
            this.errorMessage = errorMessage;
        }

        public int partition() {
            return partition;
        }

        public long offset() {
            return offset;
        }

        public Errors error() {
            return error;
        }

        public String errorMessage() {
            return errorMessage;
        }
    }

    /** What the bridge needs to write the HTTP response: status code, JSON body, optional Retry-After seconds. */
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
