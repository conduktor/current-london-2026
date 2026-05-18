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
import org.eclipse.jetty.client.InputStreamResponseListener;
import org.eclipse.jetty.client.Request;
import org.eclipse.jetty.client.Response;
import org.eclipse.jetty.client.StringRequestContent;
import org.eclipse.jetty.http.HttpMethod;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaHttpServerIntegrationTest {

    // 1 MiB matches the production default in SocketServerConfigs — plenty of headroom for the small JSON bodies in
    // these tests, while still defending against the multi-GiB OOM scenario the cap exists to prevent.
    private static final int DEFAULT_TEST_MAX_BODY_BYTES = 1024 * 1024;

    private final ObjectMapper mapper = new ObjectMapper();
    private final ControllableSubmitter submitter = new ControllableSubmitter();
    private KafkaHttpServer server;
    private HttpClient client;

    @BeforeEach
    void setUp() throws Exception {
        startServer(DEFAULT_TEST_MAX_BODY_BYTES);
    }

    private void startServer(int maxRequestBodyBytes) throws Exception {
        server = new KafkaHttpServer("127.0.0.1", 0, new KafkaHttpBridge(mapper, submitter), submitter, mapper,
            maxRequestBodyBytes);
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

    @Test
    void produceWithOversizedBodyReturns413() throws Exception {
        // Restart the server with a tiny cap so the smallest plausible JSON body still trips it. The cap defends the
        // broker JVM from an unbounded inbound POST: Jackson's readTree() consumes the whole stream into memory before
        // it parses, so without this cap a multi-GiB upload can OOM the broker before Kafka admission control runs.
        tearDown();
        startServer(64);

        // The body below is 100+ bytes — well past the 64-byte cap. The exact body shape doesn't matter; the cap
        // trips before Jackson finishes building the JsonNode tree.
        String body = "{\"records\":[{\"partition\":0,\"value\":{\"type\":\"STRING\",\"data\":\""
            + "x".repeat(200) + "\"}}]}";

        ContentResponse resp = client.newRequest(url("/v1/topics/orders/records"))
            .method(HttpMethod.POST)
            .body(new StringRequestContent("application/json", body))
            .send();

        assertEquals(413, resp.getStatus());
        JsonNode envelope = asJson(resp.getContent());
        // The envelope quotes the configured limit back to the caller so a client otherwise has no way to know how
        // big "too large" actually is. This is what makes the 413 actionable rather than just a refusal.
        assertTrue(envelope.get("errorMessage").asText().contains("64"),
            "errorMessage must quote the configured byte cap, got: " + envelope.get("errorMessage").asText());
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
    void fetchHonoursHalJsonAcceptHeader() throws Exception {
        // PROMPT.md FS5: a GET with Accept: application/hal+json must return application/hal+json on the wire.
        // The body shape (with _links) is already HAL-compatible; this assertion locks the content-type plumbing in.
        submitter.fetchResult = new RequestSubmitter.FetchResult(
            new FetchResponseFormatter.PartitionFetch(
                0, Errors.NONE, null, 100, 0, 300,
                Collections.singletonList(new FetchResponseFormatter.FetchedRecord(
                    100, null, "hi".getBytes(StandardCharsets.UTF_8), null, 1000L))),
            0L);

        ContentResponse resp = client.newRequest(url("/v1/topics/orders/records?partition=0&offset=100"))
            .method(HttpMethod.GET)
            .headers(h -> h.put("Accept", "application/hal+json"))
            .send();

        assertEquals(200, resp.getStatus());
        // Jetty appends ;charset=utf-8 on text responses but not on application/* — assert the prefix to stay tolerant.
        assertTrue(resp.getMediaType().startsWith("application/hal+json"),
            "expected application/hal+json content-type, got: " + resp.getMediaType());
        // The body must still carry the HAL _links — the content-type is a contract, not a body change.
        JsonNode body = asJson(resp.getContent());
        assertNotNull(body.get("_links").get("first"));
        assertNotNull(body.get("_links").get("last"));
        assertNotNull(body.get("_links").get("next"));
    }

    @Test
    void fetchDefaultsToPlainJsonWithoutHalAccept() throws Exception {
        // Existing clients (no Accept header, */*, or Accept: application/json) must still see application/json
        // — backwards-compatible by default.
        submitter.fetchResult = new RequestSubmitter.FetchResult(
            new FetchResponseFormatter.PartitionFetch(
                0, Errors.NONE, null, 100, 0, 300, Collections.emptyList()),
            0L);

        ContentResponse resp = client.newRequest(url("/v1/topics/orders/records?partition=0&offset=100"))
            .method(HttpMethod.GET)
            .send();

        assertEquals(200, resp.getStatus());
        assertTrue(resp.getMediaType().startsWith("application/json"),
            "expected application/json content-type, got: " + resp.getMediaType());
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

    // ----- SSE -----

    @Test
    void sseStreamsRecordsAsDataFrames() throws Exception {
        // PROMPT.md AC6/FS6: GET ?from=earliest with Accept: text/event-stream streams the records as SSE events.
        // We seed the submitter with two batches: a non-empty first fetch, then an empty page so the streamer loops
        // into "live tail" mode. The test reads the first two events off the wire and asserts the framing, content,
        // and id-line; then closes the connection to terminate the streamer.
        ConcurrentLinkedQueue<RequestSubmitter.FetchResult> queue = new ConcurrentLinkedQueue<>();
        queue.add(new RequestSubmitter.FetchResult(
            new FetchResponseFormatter.PartitionFetch(
                0, Errors.NONE, null, 0, 0, 2,
                List.of(
                    new FetchResponseFormatter.FetchedRecord(
                        0, null, "alpha".getBytes(StandardCharsets.UTF_8), null, 1111L),
                    new FetchResponseFormatter.FetchedRecord(
                        1, "k".getBytes(StandardCharsets.UTF_8), "beta".getBytes(StandardCharsets.UTF_8), null, 2222L))),
            0L));
        // Second response is empty — streamer should then re-poll and the queue is exhausted, which holds the next
        // fetch open. That keeps the connection alive long enough for us to assert and then disconnect.
        queue.add(new RequestSubmitter.FetchResult(
            new FetchResponseFormatter.PartitionFetch(
                0, Errors.NONE, null, 2, 0, 2, Collections.emptyList()),
            0L));
        submitter.fetchResultQueue = queue;

        InputStreamResponseListener listener = new InputStreamResponseListener();
        Request request = client.newRequest(url("/v1/topics/orders/records?partition=0&from=earliest"))
            .method(HttpMethod.GET)
            .headers(h -> h.put("Accept", "text/event-stream"));
        request.send(listener);
        Response response = listener.get(5, TimeUnit.SECONDS);
        assertEquals(200, response.getStatus());
        // Jetty appends charset on text/* responses — accept either bare or charset-suffixed.
        assertTrue(response.getHeaders().get("Content-Type").startsWith("text/event-stream"),
            "expected text/event-stream content-type, got: " + response.getHeaders().get("Content-Type"));

        try (InputStream body = listener.getInputStream();
             BufferedReader reader = new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8))) {
            String firstId = null;
            String firstData = null;
            String secondId = null;
            String secondData = null;
            // Skip past the ": connected" comment and pick out the two data events.
            String line;
            int eventsRead = 0;
            String pendingId = null;
            while ((line = reader.readLine()) != null && eventsRead < 2) {
                if (line.startsWith(": ")) {
                    continue; // comment
                }
                if (line.startsWith("id: ")) {
                    pendingId = line.substring(4);
                    continue;
                }
                if (line.startsWith("data: ")) {
                    if (eventsRead == 0) {
                        firstId = pendingId;
                        firstData = line.substring(6);
                    } else {
                        secondId = pendingId;
                        secondData = line.substring(6);
                    }
                    eventsRead++;
                }
            }
            assertEquals("0", firstId, "first event must carry id: 0");
            assertEquals("1", secondId, "second event must carry id: 1");
            JsonNode first = asJson(firstData.getBytes(StandardCharsets.UTF_8));
            assertEquals(0L, first.get("offset").asLong());
            assertEquals("STRING", first.get("value").get("type").asText());
            assertEquals("alpha", first.get("value").get("data").asText());
            JsonNode second = asJson(secondData.getBytes(StandardCharsets.UTF_8));
            assertEquals(1L, second.get("offset").asLong());
            assertEquals("k", second.get("key").get("data").asText());
            assertEquals("beta", second.get("value").get("data").asText());
        }
        // Closing the response stream above causes the next servlet write to fail; the streamer detects the
        // disconnect and completes the AsyncContext. We don't need an explicit teardown here.
    }

    @Test
    void sseRejectsMissingPartitionWithBadRequest() throws Exception {
        // The SSE branch parses the fetch command up-front so a malformed query string falls out as a one-shot 400,
        // not a half-opened event-stream. Without partition= the parser refuses.
        ContentResponse resp = client.newRequest(url("/v1/topics/orders/records?from=earliest"))
            .method(HttpMethod.GET)
            .headers(h -> h.put("Accept", "text/event-stream"))
            .send();
        assertEquals(400, resp.getStatus());
    }

    @Test
    void sseSurfacesPartitionErrorAsErrorFrame() throws Exception {
        // If the first fetch reports a partition-level error, the streamer emits one `event: error` SSE frame and
        // closes the stream rather than retrying — a streaming consumer that lost its source partition should be
        // told, not silently spun on.
        ConcurrentLinkedQueue<RequestSubmitter.FetchResult> queue = new ConcurrentLinkedQueue<>();
        queue.add(new RequestSubmitter.FetchResult(
            new FetchResponseFormatter.PartitionFetch(
                0, Errors.NOT_LEADER_OR_FOLLOWER, "moved", 0, 0, 0, Collections.emptyList()),
            0L));
        submitter.fetchResultQueue = queue;

        InputStreamResponseListener listener = new InputStreamResponseListener();
        client.newRequest(url("/v1/topics/orders/records?partition=0&from=earliest"))
            .method(HttpMethod.GET)
            .headers(h -> h.put("Accept", "text/event-stream"))
            .send(listener);
        Response response = listener.get(5, TimeUnit.SECONDS);
        assertEquals(200, response.getStatus());

        try (InputStream body = listener.getInputStream();
             BufferedReader reader = new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8))) {
            boolean sawErrorEvent = false;
            String errorData = null;
            String line;
            while ((line = reader.readLine()) != null) {
                if ("event: error".equals(line)) {
                    sawErrorEvent = true;
                } else if (sawErrorEvent && line.startsWith("data: ")) {
                    errorData = line.substring(6);
                    break;
                }
            }
            assertTrue(sawErrorEvent, "stream must emit `event: error` on a partition-level fetch failure");
            assertNotNull(errorData);
            JsonNode payload = asJson(errorData.getBytes(StandardCharsets.UTF_8));
            assertEquals("NOT_LEADER_OR_FOLLOWER", payload.get("errorCode").asText());
            assertEquals("moved", payload.get("errorMessage").asText());
        }
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
        // SSE flow: each submitFetch returns the next item in this queue. After the queue is exhausted, we return a
        // never-completing future so the SSE loop blocks waiting — the test then closes the connection to tear it down.
        java.util.Queue<RequestSubmitter.FetchResult> fetchResultQueue;
        java.util.List<FetchRequestParser.FetchCommand> fetchCommandLog = new java.util.concurrent.CopyOnWriteArrayList<>();

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
            fetchCommandLog.add(command);
            if (fetchResultQueue != null) {
                RequestSubmitter.FetchResult next = fetchResultQueue.poll();
                if (next != null) {
                    return CompletableFuture.completedFuture(next);
                }
                // Queue drained — block the loop. The test closes the response stream to terminate the streamer.
                return new CompletableFuture<>();
            }
            return CompletableFuture.completedFuture(fetchResult);
        }
    }
}
