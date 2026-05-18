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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaHttpBridgeTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private JsonNode json(String s) {
        try {
            return mapper.readTree(s);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Records what the bridge handed to the submitter so tests can assert on it. */
    private static class FakeSubmitter implements RequestSubmitter {
        ProduceRequestParser.ProduceCommand lastProduce;
        FetchRequestParser.FetchCommand lastFetch;
        RequestSubmitter.ProduceResult produceResult;
        RequestSubmitter.FetchResult fetchResult;
        RuntimeException produceFailure;

        @Override
        public CompletableFuture<RequestSubmitter.ProduceResult> submitProduce(
                ProduceRequestParser.ProduceCommand command) {
            this.lastProduce = command;
            if (produceFailure != null) {
                CompletableFuture<RequestSubmitter.ProduceResult> failed = new CompletableFuture<>();
                failed.completeExceptionally(produceFailure);
                return failed;
            }
            return CompletableFuture.completedFuture(produceResult);
        }

        @Override
        public CompletableFuture<RequestSubmitter.FetchResult> submitFetch(
                FetchRequestParser.FetchCommand command) {
            this.lastFetch = command;
            return CompletableFuture.completedFuture(fetchResult);
        }
    }

    // ----- produce: happy path -----

    @Test
    void producePassesParsedCommandToSubmitter() throws Exception {
        FakeSubmitter submitter = new FakeSubmitter();
        submitter.produceResult = new RequestSubmitter.ProduceResult(
            Collections.singletonList(
                new ProduceResponseFormatter.PartitionResult(0, 42L, Errors.NONE, null)),
            0L);

        KafkaHttpBridge bridge = new KafkaHttpBridge(mapper, submitter);
        HttpBridgeResponse r = bridge.produce("orders",
            json("{\"records\":[{\"partition\":0,\"value\":{\"type\":\"STRING\",\"data\":\"x\"}}]}")).get();

        assertEquals(200, r.status());
        assertEquals("orders", submitter.lastProduce.topic());
        assertEquals(1, submitter.lastProduce.records().size());
        assertEquals(0, submitter.lastProduce.records().get(0).partition().getAsInt());
    }

    @Test
    void produceReturns207OnMixedPartitionResults() throws Exception {
        // Spec scenario: partition 0 success, partition 1 offline, partition 2 success.
        FakeSubmitter submitter = new FakeSubmitter();
        List<ProduceResponseFormatter.PartitionResult> partitions = new ArrayList<>();
        partitions.add(new ProduceResponseFormatter.PartitionResult(0, 100, Errors.NONE, null));
        partitions.add(new ProduceResponseFormatter.PartitionResult(1, -1, Errors.NOT_LEADER_OR_FOLLOWER, null));
        partitions.add(new ProduceResponseFormatter.PartitionResult(2, 200, Errors.NONE, null));
        submitter.produceResult = new RequestSubmitter.ProduceResult(partitions, 0L);

        KafkaHttpBridge bridge = new KafkaHttpBridge(mapper, submitter);
        HttpBridgeResponse r = bridge.produce("orders",
            json("{\"records\":["
                + "{\"partition\":0,\"value\":{\"type\":\"STRING\",\"data\":\"a\"}},"
                + "{\"partition\":1,\"value\":{\"type\":\"STRING\",\"data\":\"b\"}},"
                + "{\"partition\":2,\"value\":{\"type\":\"STRING\",\"data\":\"c\"}}]}")).get();

        assertEquals(207, r.status());
        assertEquals(3, r.body().get("results").size());
    }

    @Test
    void produceReturns403OnUniformAuthFailure() throws Exception {
        FakeSubmitter submitter = new FakeSubmitter();
        List<ProduceResponseFormatter.PartitionResult> partitions = new ArrayList<>();
        partitions.add(new ProduceResponseFormatter.PartitionResult(0, -1, Errors.TOPIC_AUTHORIZATION_FAILED, null));
        partitions.add(new ProduceResponseFormatter.PartitionResult(1, -1, Errors.TOPIC_AUTHORIZATION_FAILED, null));
        submitter.produceResult = new RequestSubmitter.ProduceResult(partitions, 0L);

        KafkaHttpBridge bridge = new KafkaHttpBridge(mapper, submitter);
        HttpBridgeResponse r = bridge.produce("orders",
            json("{\"records\":["
                + "{\"partition\":0,\"value\":{\"type\":\"STRING\",\"data\":\"a\"}},"
                + "{\"partition\":1,\"value\":{\"type\":\"STRING\",\"data\":\"b\"}}]}")).get();

        assertEquals(403, r.status());
    }

    @Test
    void produceQuotaThrottleSetsRetryAfterOn200() throws Exception {
        FakeSubmitter submitter = new FakeSubmitter();
        submitter.produceResult = new RequestSubmitter.ProduceResult(
            Collections.singletonList(
                new ProduceResponseFormatter.PartitionResult(0, 42L, Errors.NONE, null)),
            1500L);

        KafkaHttpBridge bridge = new KafkaHttpBridge(mapper, submitter);
        HttpBridgeResponse r = bridge.produce("orders",
            json("{\"records\":[{\"value\":{\"type\":\"STRING\",\"data\":\"x\"}}]}")).get();

        assertEquals(200, r.status());
        assertTrue(r.hasRetryAfter());
        assertEquals(2, r.retryAfterSeconds());
    }

    // ----- produce: bridge-side validation -----

    @Test
    void produceReturns400WhenBodyIsMalformed() throws Exception {
        FakeSubmitter submitter = new FakeSubmitter();
        KafkaHttpBridge bridge = new KafkaHttpBridge(mapper, submitter);

        HttpBridgeResponse r = bridge.produce("orders", json("{}")).get();

        assertEquals(400, r.status());
        assertEquals(Errors.INVALID_REQUEST.code(), r.body().get("errorCode").asInt());
        // never reached the submitter
        assertEquals(null, submitter.lastProduce);
    }

    @Test
    void produceReturns400WhenTopicIsBlank() throws Exception {
        FakeSubmitter submitter = new FakeSubmitter();
        KafkaHttpBridge bridge = new KafkaHttpBridge(mapper, submitter);

        HttpBridgeResponse r = bridge.produce("   ",
            json("{\"records\":[{\"value\":{\"type\":\"STRING\",\"data\":\"x\"}}]}")).get();

        assertEquals(400, r.status());
    }

    @Test
    void produceReturns500WhenSubmitterFails() throws Exception {
        FakeSubmitter submitter = new FakeSubmitter();
        submitter.produceFailure = new RuntimeException("broker down");

        KafkaHttpBridge bridge = new KafkaHttpBridge(mapper, submitter);
        HttpBridgeResponse r = bridge.produce("orders",
            json("{\"records\":[{\"value\":{\"type\":\"STRING\",\"data\":\"x\"}}]}")).get();

        assertEquals(500, r.status());
        assertNotNull(r.body().get("errorMessage"));
    }

    // ----- fetch: happy path -----

    @Test
    void fetchPassesParsedQueryToSubmitter() throws Exception {
        FakeSubmitter submitter = new FakeSubmitter();
        submitter.fetchResult = new RequestSubmitter.FetchResult(
            new FetchResponseFormatter.PartitionFetch(
                0, Errors.NONE, null, 100, 0, 200,
                Collections.singletonList(new FetchResponseFormatter.FetchedRecord(
                    100, null, "hello".getBytes(StandardCharsets.UTF_8), null, 999L))),
            0L);

        KafkaHttpBridge bridge = new KafkaHttpBridge(mapper, submitter);
        HttpBridgeResponse r = bridge.fetch("orders", QueryParams.of("partition", "0", "offset", "100")).get();

        assertEquals(200, r.status());
        assertEquals(0, submitter.lastFetch.partition());
        assertEquals(100L, submitter.lastFetch.offset());

        // HATEOAS links present
        assertNotNull(r.body().get("_links"));
        assertNotNull(r.body().get("partitions").get(0).get("_links").get("self"));
    }

    @Test
    void fetchByCursorRoundTripsViaSubmitter() throws Exception {
        FakeSubmitter submitter = new FakeSubmitter();
        submitter.fetchResult = new RequestSubmitter.FetchResult(
            new FetchResponseFormatter.PartitionFetch(
                4, Errors.NONE, null, 77, 0, 100,
                Collections.singletonList(new FetchResponseFormatter.FetchedRecord(
                    77, null, "x".getBytes(StandardCharsets.UTF_8), null, 1L))),
            0L);

        String cursor = CursorCodec.encode("orders", 4, 77);
        KafkaHttpBridge bridge = new KafkaHttpBridge(mapper, submitter);
        bridge.fetch("orders", QueryParams.of("cursor", cursor)).get();

        assertEquals(4, submitter.lastFetch.partition());
        assertEquals(77L, submitter.lastFetch.offset());
    }

    @Test
    void fetchReturns403OnTopicAuthFailure() throws Exception {
        FakeSubmitter submitter = new FakeSubmitter();
        submitter.fetchResult = new RequestSubmitter.FetchResult(
            new FetchResponseFormatter.PartitionFetch(
                0, Errors.TOPIC_AUTHORIZATION_FAILED, null, 0, 0, 0, Collections.emptyList()),
            0L);

        KafkaHttpBridge bridge = new KafkaHttpBridge(mapper, submitter);
        HttpBridgeResponse r = bridge.fetch("orders", QueryParams.of("partition", "0", "offset", "0")).get();

        assertEquals(403, r.status());
    }

    @Test
    void fetchReturns400WhenQueryIsInvalid() throws Exception {
        FakeSubmitter submitter = new FakeSubmitter();
        KafkaHttpBridge bridge = new KafkaHttpBridge(mapper, submitter);

        HttpBridgeResponse r = bridge.fetch("orders", QueryParams.of("partition", "notAnInt", "offset", "0")).get();

        assertEquals(400, r.status());
        assertEquals(null, submitter.lastFetch);
    }

    @Test
    void fetchReturns400WhenCursorIsForDifferentTopic() throws Exception {
        FakeSubmitter submitter = new FakeSubmitter();
        KafkaHttpBridge bridge = new KafkaHttpBridge(mapper, submitter);

        String wrongCursor = CursorCodec.encode("payments", 0, 0);
        HttpBridgeResponse r = bridge.fetch("orders", QueryParams.of("cursor", wrongCursor)).get();

        assertEquals(400, r.status());
    }

    @Test
    void fetchReturns500WhenSubmitterFutureFails() throws Exception {
        FakeSubmitter submitter = new FakeSubmitter() {
            @Override
            public CompletableFuture<RequestSubmitter.FetchResult> submitFetch(
                    FetchRequestParser.FetchCommand command) {
                CompletableFuture<RequestSubmitter.FetchResult> f = new CompletableFuture<>();
                f.completeExceptionally(new RuntimeException("broker down"));
                return f;
            }
        };

        KafkaHttpBridge bridge = new KafkaHttpBridge(mapper, submitter);
        HttpBridgeResponse r = bridge.fetch("orders", QueryParams.of("partition", "0", "offset", "0")).get();

        assertEquals(500, r.status());
    }

    @Test
    void fetchThrottlePassesThroughRetryAfter() throws Exception {
        FakeSubmitter submitter = new FakeSubmitter();
        submitter.fetchResult = new RequestSubmitter.FetchResult(
            new FetchResponseFormatter.PartitionFetch(
                0, Errors.NONE, null, 0, 0, 0, Collections.emptyList()),
            2500L);

        KafkaHttpBridge bridge = new KafkaHttpBridge(mapper, submitter);
        HttpBridgeResponse r = bridge.fetch("orders", QueryParams.of("partition", "0", "offset", "0")).get();

        assertEquals(200, r.status());
        assertTrue(r.hasRetryAfter());
        assertEquals(3, r.retryAfterSeconds());
    }

    // ----- bridge sanity -----

    @Test
    void produceFutureIsAsync() throws Exception {
        // Smoke: bridge must complete the future from the submitter's future, not synchronously.
        CompletableFuture<RequestSubmitter.ProduceResult> deferred = new CompletableFuture<>();
        RequestSubmitter submitter = new RequestSubmitter() {
            @Override
            public CompletableFuture<ProduceResult> submitProduce(ProduceRequestParser.ProduceCommand c) {
                return deferred;
            }
            @Override
            public CompletableFuture<FetchResult> submitFetch(FetchRequestParser.FetchCommand c) {
                throw new UnsupportedOperationException();
            }
        };

        KafkaHttpBridge bridge = new KafkaHttpBridge(mapper, submitter);
        CompletableFuture<HttpBridgeResponse> response = bridge.produce("orders",
            json("{\"records\":[{\"value\":{\"type\":\"STRING\",\"data\":\"x\"}}]}"));

        assertFalse(response.isDone(), "response should not be completed until submitter completes");

        deferred.complete(new RequestSubmitter.ProduceResult(
            Collections.singletonList(
                new ProduceResponseFormatter.PartitionResult(0, 1L, Errors.NONE, null)),
            0L));

        assertEquals(200, response.get().status());
    }

    @Test
    void bridgeNeverThrowsToCaller() throws Exception {
        // The bridge is the boundary; it must yield an HttpBridgeResponse for every possible input, never propagate.
        FakeSubmitter submitter = new FakeSubmitter();
        KafkaHttpBridge bridge = new KafkaHttpBridge(mapper, submitter);

        // null body
        HttpBridgeResponse r1 = bridge.produce("orders", null).get();
        assertEquals(400, r1.status());

        // garbage body
        HttpBridgeResponse r2 = bridge.produce("orders", mapper.readTree("\"oops\"")).get();
        assertEquals(400, r2.status());

        // null query
        HttpBridgeResponse r3 = bridge.fetch("orders", QueryParams.of()).get();
        assertEquals(400, r3.status());
    }

    @Test
    void bridgeHonorsThrottleOnTopLevelError() throws Exception {
        // top-level 400 should NOT carry Retry-After (per spec: 403/404/400 do not).
        FakeSubmitter submitter = new FakeSubmitter();
        KafkaHttpBridge bridge = new KafkaHttpBridge(mapper, submitter);

        HttpBridgeResponse r = bridge.produce("orders", json("{}")).get();

        assertEquals(400, r.status());
        assertFalse(r.hasRetryAfter());
    }

    @Test
    void completedExceptionallyFuturePropagatesAsBridge500() {
        FakeSubmitter submitter = new FakeSubmitter();
        submitter.produceFailure = new IllegalStateException("boom");

        KafkaHttpBridge bridge = new KafkaHttpBridge(mapper, submitter);
        try {
            HttpBridgeResponse r = bridge.produce("orders",
                json("{\"records\":[{\"value\":{\"type\":\"STRING\",\"data\":\"x\"}}]}")).get();
            assertEquals(500, r.status());
        } catch (InterruptedException | ExecutionException e) {
            throw new AssertionError("bridge must not propagate exceptions", e);
        }
    }
}
