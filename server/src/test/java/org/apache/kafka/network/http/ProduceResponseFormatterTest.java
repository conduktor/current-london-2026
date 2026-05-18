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

import org.apache.kafka.common.protocol.Errors;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProduceResponseFormatterTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final ProduceResponseFormatter formatter = new ProduceResponseFormatter(mapper);

    private static ProduceResponseFormatter.PartitionResult ok(int partition, long offset) {
        return new ProduceResponseFormatter.PartitionResult(partition, offset, Errors.NONE, null);
    }

    private static ProduceResponseFormatter.PartitionResult err(int partition, Errors error) {
        return new ProduceResponseFormatter.PartitionResult(partition, -1L, error, null);
    }

    @Test
    void pureSuccessReturns200() {
        List<ProduceResponseFormatter.PartitionResult> results = new ArrayList<>();
        results.add(ok(0, 100));
        results.add(ok(1, 200));
        results.add(ok(2, 300));

        ProduceResponseFormatter.Formatted f = formatter.format("orders", results, 0);

        assertEquals(200, f.status());
        assertEquals(false, f.hasRetryAfter());
    }

    @Test
    void uniformAuthFailureReturns403() {
        List<ProduceResponseFormatter.PartitionResult> results = new ArrayList<>();
        results.add(err(0, Errors.TOPIC_AUTHORIZATION_FAILED));
        results.add(err(1, Errors.TOPIC_AUTHORIZATION_FAILED));

        ProduceResponseFormatter.Formatted f = formatter.format("orders", results, 0);

        assertEquals(403, f.status());
    }

    @Test
    void uniformUnknownTopicReturns404() {
        List<ProduceResponseFormatter.PartitionResult> results = new ArrayList<>();
        results.add(err(0, Errors.UNKNOWN_TOPIC_OR_PARTITION));
        results.add(err(1, Errors.UNKNOWN_TOPIC_OR_PARTITION));

        ProduceResponseFormatter.Formatted f = formatter.format("orders", results, 0);

        assertEquals(404, f.status());
    }

    @Test
    void mixedSuccessAndFailureReturns207() {
        // Spec scenario: partition 0 succeeds, partition 1 offline, partition 2 succeeds → 207
        List<ProduceResponseFormatter.PartitionResult> results = new ArrayList<>();
        results.add(ok(0, 100));
        results.add(err(1, Errors.NOT_LEADER_OR_FOLLOWER));
        results.add(ok(2, 200));

        ProduceResponseFormatter.Formatted f = formatter.format("orders", results, 0);

        assertEquals(207, f.status());
    }

    @Test
    void mixedFailuresWithDifferentErrorCodesAlsoReturns207() {
        // "uniform" means same error code across all partitions; different failures → still 207
        List<ProduceResponseFormatter.PartitionResult> results = new ArrayList<>();
        results.add(err(0, Errors.NOT_LEADER_OR_FOLLOWER));
        results.add(err(1, Errors.UNKNOWN_TOPIC_OR_PARTITION));

        ProduceResponseFormatter.Formatted f = formatter.format("orders", results, 0);

        assertEquals(207, f.status());
    }

    @Test
    void singlePartitionSuccessReturns200() {
        List<ProduceResponseFormatter.PartitionResult> results = new ArrayList<>();
        results.add(ok(0, 100));

        ProduceResponseFormatter.Formatted f = formatter.format("orders", results, 0);

        assertEquals(200, f.status());
    }

    @Test
    void singlePartitionFailureReturnsMappedStatus() {
        List<ProduceResponseFormatter.PartitionResult> results = new ArrayList<>();
        results.add(err(0, Errors.TOPIC_AUTHORIZATION_FAILED));

        ProduceResponseFormatter.Formatted f = formatter.format("orders", results, 0);

        assertEquals(403, f.status());
    }

    // ----- body shape -----

    @Test
    void bodyIncludesTopicAndResultsArray() {
        List<ProduceResponseFormatter.PartitionResult> results = new ArrayList<>();
        results.add(ok(0, 100));
        results.add(err(1, Errors.NOT_LEADER_OR_FOLLOWER));

        ProduceResponseFormatter.Formatted f = formatter.format("orders", results, 0);
        JsonNode body = f.body();

        assertEquals("orders", body.get("topic").asText());
        assertTrue(body.get("results").isArray());
        assertEquals(2, body.get("results").size());
    }

    @Test
    void bodyEntryForSuccessExposesPartitionAndOffset() {
        List<ProduceResponseFormatter.PartitionResult> results = new ArrayList<>();
        results.add(ok(7, 12345));

        ProduceResponseFormatter.Formatted f = formatter.format("orders", results, 0);
        JsonNode entry = f.body().get("results").get(0);

        assertEquals(7, entry.get("partition").asInt());
        assertEquals(12345, entry.get("offset").asLong());
        assertEquals(0, entry.get("errorCode").asInt());
    }

    @Test
    void bodyEntryForFailureHasNullOffsetAndErrorCode() {
        List<ProduceResponseFormatter.PartitionResult> results = new ArrayList<>();
        results.add(err(3, Errors.NOT_LEADER_OR_FOLLOWER));

        ProduceResponseFormatter.Formatted f = formatter.format("orders", results, 0);
        JsonNode entry = f.body().get("results").get(0);

        assertEquals(3, entry.get("partition").asInt());
        assertTrue(entry.get("offset").isNull(), "offset must be JSON null on failure");
        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code(), entry.get("errorCode").asInt());
        assertFalse(entry.get("errorMessage").isNull());
    }

    // ----- Retry-After (quota path) -----

    @Test
    void positiveThrottleSetsRetryAfterOn200() {
        // Per spec: produce quota exhaustion → 200 with Retry-After
        List<ProduceResponseFormatter.PartitionResult> results = new ArrayList<>();
        results.add(ok(0, 100));

        ProduceResponseFormatter.Formatted f = formatter.format("orders", results, 1500);

        assertEquals(200, f.status());
        assertTrue(f.hasRetryAfter());
        assertEquals(2, f.retryAfterSeconds());
    }

    @Test
    void zeroThrottleHasNoRetryAfter() {
        List<ProduceResponseFormatter.PartitionResult> results = new ArrayList<>();
        results.add(ok(0, 100));

        ProduceResponseFormatter.Formatted f = formatter.format("orders", results, 0);

        assertFalse(f.hasRetryAfter());
    }

    @Test
    void retryAfterCarriedEvenOn207() {
        // If the response is mixed AND throttled, the header still goes through.
        List<ProduceResponseFormatter.PartitionResult> results = new ArrayList<>();
        results.add(ok(0, 100));
        results.add(err(1, Errors.NOT_LEADER_OR_FOLLOWER));

        ProduceResponseFormatter.Formatted f = formatter.format("orders", results, 999);

        assertEquals(207, f.status());
        assertTrue(f.hasRetryAfter());
        assertEquals(1, f.retryAfterSeconds());
    }

    // ----- empty / degenerate -----

    @Test
    void emptyResultsReturns500() {
        // A response with no partition results is a broker bug, but the bridge must still produce a valid HTTP shape.
        List<ProduceResponseFormatter.PartitionResult> results = new ArrayList<>();

        ProduceResponseFormatter.Formatted f = formatter.format("orders", results, 0);

        assertEquals(500, f.status());
        assertEquals(0, f.body().get("results").size());
    }

    // ----- error mode (top-level error, no per-partition results) -----

    @Test
    void topLevelErrorBypassesBody() {
        ProduceResponseFormatter.Formatted f =
            formatter.topLevelError("orders", Errors.INVALID_REQUEST, "topic must not be empty", 0);

        assertEquals(400, f.status());
        assertEquals(Errors.INVALID_REQUEST.code(), f.body().get("errorCode").asInt());
        assertEquals("topic must not be empty", f.body().get("errorMessage").asText());
        assertNull(f.body().get("results"));
    }
}
