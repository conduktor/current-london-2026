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

import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.Request;
import org.eclipse.jetty.client.StringRequestContent;
import org.eclipse.jetty.http.HttpMethod;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaHttpServerIntegrationTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final ControllableSubmitter submitter = new ControllableSubmitter();
    private KafkaHttpServer server;
    private HttpClient client;

    @BeforeEach
    void setUp() throws Exception {
        server = new KafkaHttpServer("127.0.0.1", 0, new KafkaHttpBridge(mapper, submitter), mapper);
        server.start();
        client = new HttpClient();
        client.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) {
            client.stop();
        }
        if (server != null) {
            server.stop();
        }
    }

    private String url(String path) {
        return "http://127.0.0.1:" + server.boundPort() + path;
    }

    private JsonNode asJson(byte[] bytes) throws Exception {
        return mapper.readTree(bytes);
    }

    // ----- routing -----

    @Test
    void produceHits200OnHappyPath() throws Exception {
        submitter.produceResult = new RequestSubmitter.ProduceResult(
            Collections.singletonList(
                new ProduceResponseFormatter.PartitionResult(0, 99L, Errors.NONE, null)),
            0L);

        ContentResponse resp = client.newRequest(url("/v1/topics/orders/records"))
            .method(HttpMethod.POST)
            .body(new StringRequestContent("application/json",
                "{\"records\":[{\"partition\":0,\"value\":{\"type\":\"STRING\",\"data\":\"hi\"}}]}"))
            .send();

        assertEquals(200, resp.getStatus());
        JsonNode body = asJson(resp.getContent());
        assertEquals("orders", body.get("topic").asText());
        assertEquals(99L, body.get("results").get(0).get("offset").asLong());
    }

    @Test
    void produce207OnMixedPartitionResults() throws Exception {
        // Spec scenario from PROMPT.md: 0 OK, 1 offline, 2 OK -> 207.
        List<ProduceResponseFormatter.PartitionResult> parts = new ArrayList<>();
        parts.add(new ProduceResponseFormatter.PartitionResult(0, 100, Errors.NONE, null));
        parts.add(new ProduceResponseFormatter.PartitionResult(1, -1, Errors.NOT_LEADER_OR_FOLLOWER, null));
        parts.add(new ProduceResponseFormatter.PartitionResult(2, 200, Errors.NONE, null));
        submitter.produceResult = new RequestSubmitter.ProduceResult(parts, 0L);

        ContentResponse resp = client.newRequest(url("/v1/topics/orders/records"))
            .method(HttpMethod.POST)
            .body(new StringRequestContent("application/json",
                "{\"records\":["
                + "{\"partition\":0,\"value\":{\"type\":\"STRING\",\"data\":\"a\"}},"
                + "{\"partition\":1,\"value\":{\"type\":\"STRING\",\"data\":\"b\"}},"
                + "{\"partition\":2,\"value\":{\"type\":\"STRING\",\"data\":\"c\"}}]}"))
            .send();

        assertEquals(207, resp.getStatus());
        assertEquals(3, asJson(resp.getContent()).get("results").size());
    }

    @Test
    void produceRetryAfterOnQuotaThrottle() throws Exception {
        submitter.produceResult = new RequestSubmitter.ProduceResult(
            Collections.singletonList(
                new ProduceResponseFormatter.PartitionResult(0, 1L, Errors.NONE, null)),
            1500L);

        ContentResponse resp = client.newRequest(url("/v1/topics/orders/records"))
            .method(HttpMethod.POST)
            .body(new StringRequestContent("application/json",
                "{\"records\":[{\"value\":{\"type\":\"STRING\",\"data\":\"x\"}}]}"))
            .send();

        assertEquals(200, resp.getStatus());
        assertEquals("2", resp.getHeaders().get("Retry-After"));
    }

    @Test
    void produce403OnUniformAuthFailure() throws Exception {
        List<ProduceResponseFormatter.PartitionResult> parts = new ArrayList<>();
        parts.add(new ProduceResponseFormatter.PartitionResult(0, -1, Errors.TOPIC_AUTHORIZATION_FAILED, null));
        submitter.produceResult = new RequestSubmitter.ProduceResult(parts, 0L);

        ContentResponse resp = client.newRequest(url("/v1/topics/orders/records"))
            .method(HttpMethod.POST)
            .body(new StringRequestContent("application/json",
                "{\"records\":[{\"value\":{\"type\":\"STRING\",\"data\":\"x\"}}]}"))
            .send();

        assertEquals(403, resp.getStatus());
    }

    @Test
    void produce400OnInvalidJson() throws Exception {
        ContentResponse resp = client.newRequest(url("/v1/topics/orders/records"))
            .method(HttpMethod.POST)
            .body(new StringRequestContent("application/json", "{this is not json"))
            .send();

        assertEquals(400, resp.getStatus());
    }

    @Test
    void produce400OnMissingRecords() throws Exception {
        ContentResponse resp = client.newRequest(url("/v1/topics/orders/records"))
            .method(HttpMethod.POST)
            .body(new StringRequestContent("application/json", "{}"))
            .send();

        assertEquals(400, resp.getStatus());
    }

    // ----- fetch -----

    @Test
    void fetch200WithRecordsAndLinks() throws Exception {
        submitter.fetchResult = new RequestSubmitter.FetchResult(
            new FetchResponseFormatter.PartitionFetch(
                0, Errors.NONE, null, 100, 0, 300,
                Collections.singletonList(new FetchResponseFormatter.FetchedRecord(
                    100, null, "hi".getBytes(StandardCharsets.UTF_8), null, 1000L))),
            0L);

        ContentResponse resp = client.newRequest(url("/v1/topics/orders/records?partition=0&offset=100"))
            .method(HttpMethod.GET)
            .send();

        assertEquals(200, resp.getStatus());
        JsonNode body = asJson(resp.getContent());
        assertEquals("orders", body.get("topic").asText());
        assertEquals(1, body.get("partitions").size());
        assertNotNull(body.get("_links").get("first"));
        assertNotNull(body.get("_links").get("next"));
        assertNotNull(body.get("partitions").get(0).get("_links").get("self"));

        // Following the "next" link via cursor should hit the same endpoint
        String nextCursor = body.get("_links").get("next").asText();
        ContentResponse follow = client.newRequest(url("/v1/topics/orders/records?cursor=" + nextCursor))
            .method(HttpMethod.GET)
            .send();
        assertEquals(200, follow.getStatus());
        // submitter was invoked again -- this time with the cursor's offset (101)
        assertEquals(101L, submitter.lastFetch.offset());
    }

    @Test
    void fetch400OnBadQuery() throws Exception {
        ContentResponse resp = client.newRequest(url("/v1/topics/orders/records?partition=notAnInt&offset=0"))
            .method(HttpMethod.GET)
            .send();

        assertEquals(400, resp.getStatus());
    }

    @Test
    void fetch400OnMissingParams() throws Exception {
        ContentResponse resp = client.newRequest(url("/v1/topics/orders/records"))
            .method(HttpMethod.GET)
            .send();

        assertEquals(400, resp.getStatus());
    }

    // ----- routing edge cases -----

    @Test
    void unknownPathReturns404() throws Exception {
        ContentResponse resp = client.newRequest(url("/v1/topics/orders/something-else"))
            .method(HttpMethod.GET)
            .send();

        assertEquals(404, resp.getStatus());
    }

    @Test
    void rootPathReturns404() throws Exception {
        ContentResponse resp = client.newRequest(url("/v1/topics//records"))
            .method(HttpMethod.GET)
            .send();

        // empty topic must be rejected (we never want to submit a "" topic to the broker)
        assertTrue(resp.getStatus() == 404 || resp.getStatus() == 400);
    }

    // ----- async correctness -----

    @Test
    void deferredSubmitterCompletionStillSucceeds() throws Exception {
        // Bridge uses CompletableFuture; verify a delayed completion is correctly written back to the wire.
        CompletableFuture<RequestSubmitter.ProduceResult> deferred = new CompletableFuture<>();
        submitter.produceFutureOverride = deferred;

        Request req = client.newRequest(url("/v1/topics/orders/records"))
            .method(HttpMethod.POST)
            .body(new StringRequestContent("application/json",
                "{\"records\":[{\"value\":{\"type\":\"STRING\",\"data\":\"x\"}}]}"));

        // Complete in a different thread after a brief pause
        new Thread(() -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            deferred.complete(new RequestSubmitter.ProduceResult(
                Collections.singletonList(
                    new ProduceResponseFormatter.PartitionResult(0, 7L, Errors.NONE, null)),
                0L));
        }).start();

        ContentResponse resp = req.send();
        assertEquals(200, resp.getStatus());
        assertEquals(7L, asJson(resp.getContent()).get("results").get(0).get("offset").asLong());
    }

    @Test
    void submitterThrowableProduces500() throws Exception {
        submitter.produceFailure = new RuntimeException("broker fell over");

        ContentResponse resp = client.newRequest(url("/v1/topics/orders/records"))
            .method(HttpMethod.POST)
            .body(new StringRequestContent("application/json",
                "{\"records\":[{\"value\":{\"type\":\"STRING\",\"data\":\"x\"}}]}"))
            .send();

        assertEquals(500, resp.getStatus());
        assertNotNull(asJson(resp.getContent()).get("errorMessage"));
    }

    // ----- extractTopic unit -----

    @Test
    void extractTopicHandlesGoodAndBadPaths() {
        assertEquals("orders", KafkaHttpServlet.extractTopic("/topics/orders/records"));
        assertEquals("o-r-d-e-r-s", KafkaHttpServlet.extractTopic("/topics/o-r-d-e-r-s/records"));
        assertNull(KafkaHttpServlet.extractTopic(null));
        assertNull(KafkaHttpServlet.extractTopic("/topics/orders"));               // missing /records suffix
        assertNull(KafkaHttpServlet.extractTopic("/topics//records"));             // empty topic
        assertNull(KafkaHttpServlet.extractTopic("/topics/orders/extra/records")); // slash inside topic
        assertNull(KafkaHttpServlet.extractTopic("/other/orders/records"));        // wrong prefix
    }

    /** Submitter that yields whatever the test set up, with overrides for failure and deferred completion. */
    private static class ControllableSubmitter implements RequestSubmitter {
        RequestSubmitter.ProduceResult produceResult;
        RequestSubmitter.FetchResult fetchResult;
        RuntimeException produceFailure;
        CompletableFuture<RequestSubmitter.ProduceResult> produceFutureOverride;
        FetchRequestParser.FetchCommand lastFetch;

        @Override
        public CompletableFuture<ProduceResult> submitProduce(ProduceRequestParser.ProduceCommand command) {
            if (produceFutureOverride != null) {
                return produceFutureOverride;
            }
            if (produceFailure != null) {
                CompletableFuture<ProduceResult> f = new CompletableFuture<>();
                f.completeExceptionally(produceFailure);
                return f;
            }
            return CompletableFuture.completedFuture(produceResult);
        }

        @Override
        public CompletableFuture<FetchResult> submitFetch(FetchRequestParser.FetchCommand command) {
            this.lastFetch = command;
            return CompletableFuture.completedFuture(fetchResult);
        }
    }
}
