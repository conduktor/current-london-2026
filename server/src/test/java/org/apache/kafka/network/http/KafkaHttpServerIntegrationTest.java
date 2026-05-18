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
import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.exceptions.UpgradeException;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaHttpServerIntegrationTest {

    // 1 MiB matches the production default in SocketServerConfigs — plenty of headroom for the small JSON bodies in
    // these tests, while still defending against the multi-GiB OOM scenario the cap exists to prevent.
    private static final int DEFAULT_TEST_MAX_BODY_BYTES = 1024 * 1024;
    // 100 matches the production default. Tests that need to exercise the cap pass an explicit smaller value.
    private static final int DEFAULT_TEST_MAX_SSE_STREAMS = 100;
    private static final int DEFAULT_TEST_MAX_WS_SUBSCRIPTIONS = 100;

    private final ObjectMapper mapper = new ObjectMapper();
    private final ControllableSubmitter submitter = new ControllableSubmitter();
    private KafkaHttpServer server;
    private HttpClient client;

    @BeforeEach
    void setUp() throws Exception {
        startServer(DEFAULT_TEST_MAX_BODY_BYTES, DEFAULT_TEST_MAX_SSE_STREAMS, DEFAULT_TEST_MAX_WS_SUBSCRIPTIONS);
    }

    private void startServer(int maxRequestBodyBytes, int maxConcurrentSseStreams,
                             int maxConcurrentWsSubscriptions) throws Exception {
        server = new KafkaHttpServer("127.0.0.1", 0, new KafkaHttpBridge(mapper, submitter), submitter, mapper,
            maxRequestBodyBytes, maxConcurrentSseStreams, maxConcurrentWsSubscriptions);
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
        startServer(64, DEFAULT_TEST_MAX_SSE_STREAMS, DEFAULT_TEST_MAX_WS_SUBSCRIPTIONS);

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
    void sseReturns429WhenConcurrentStreamCapReached() throws Exception {
        // Restart with a cap of 1 so the first stream consumes all capacity. The second stream attempt must be
        // refused at the admission gate with HTTP 429 + Retry-After, NOT a half-opened event-stream that then
        // immediately errors. A runaway client otherwise exhausts the Jetty thread pool and stalls the bridge.
        // While we're here, also assert the SseStreamsOpened + RejectedAtSseCap meters increment as expected —
        // this is the end-to-end wiring proof for the SSE-specific metrics (which never land in ResponseCount).
        tearDown();
        startServer(DEFAULT_TEST_MAX_BODY_BYTES, 1, DEFAULT_TEST_MAX_WS_SUBSCRIPTIONS);

        com.yammer.metrics.core.Meter streamsOpened = (com.yammer.metrics.core.Meter)
            org.apache.kafka.server.metrics.KafkaYammerMetrics.defaultRegistry().allMetrics()
                .get(bridgeMetricName("SseStreamsOpened"));
        com.yammer.metrics.core.Meter rejectedAtSseCap = (com.yammer.metrics.core.Meter)
            org.apache.kafka.server.metrics.KafkaYammerMetrics.defaultRegistry().allMetrics()
                .get(bridgeMetricName("RejectedAtSseCap"));
        com.yammer.metrics.core.Meter fetch2xx = (com.yammer.metrics.core.Meter)
            org.apache.kafka.server.metrics.KafkaYammerMetrics.defaultRegistry().allMetrics()
                .get(bridgeMetricName("ResponseCount", "operation", "Fetch", "statusClass", "2xx"));
        com.yammer.metrics.core.Meter fetch4xx = (com.yammer.metrics.core.Meter)
            org.apache.kafka.server.metrics.KafkaYammerMetrics.defaultRegistry().allMetrics()
                .get(bridgeMetricName("ResponseCount", "operation", "Fetch", "statusClass", "4xx"));
        assertNotNull(streamsOpened, "SseStreamsOpened must be registered after a fresh server start");
        assertNotNull(rejectedAtSseCap, "RejectedAtSseCap must be registered after a fresh server start");
        assertNotNull(fetch2xx, "ResponseCount Fetch/2xx must be registered after a fresh server start");
        assertNotNull(fetch4xx, "ResponseCount Fetch/4xx must be registered after a fresh server start");
        long openedBefore = streamsOpened.count();
        long rejectedBefore = rejectedAtSseCap.count();
        long fetch2xxBefore = fetch2xx.count();
        long fetch4xxBefore = fetch4xx.count();

        // Seed an indefinite stream: first fetch yields one record, subsequent fetches block (CompletableFuture
        // that never completes). That keeps the first stream alive and the limiter at capacity for the duration
        // of the test.
        ConcurrentLinkedQueue<RequestSubmitter.FetchResult> queue = new ConcurrentLinkedQueue<>();
        queue.add(new RequestSubmitter.FetchResult(
            new FetchResponseFormatter.PartitionFetch(
                0, Errors.NONE, null, 0, 0, 1,
                List.of(new FetchResponseFormatter.FetchedRecord(
                    0, null, "x".getBytes(StandardCharsets.UTF_8), null, 1L))),
            0L));
        submitter.fetchResultQueue = queue;

        // First stream — opens, reads one event, holds the slot.
        InputStreamResponseListener firstListener = new InputStreamResponseListener();
        client.newRequest(url("/v1/topics/orders/records?partition=0&from=earliest"))
            .method(HttpMethod.GET)
            .headers(h -> h.put("Accept", "text/event-stream"))
            .send(firstListener);
        Response first = firstListener.get(5, TimeUnit.SECONDS);
        assertEquals(200, first.getStatus());
        // Wait until the first record has been written so we can be confident the limiter has counted the slot.
        try (InputStream body = firstListener.getInputStream();
             BufferedReader reader = new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8))) {
            String line;
            boolean sawData = false;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("data: ")) {
                    sawData = true;
                    break;
                }
            }
            assertTrue(sawData, "first stream must have received at least one event before we test the cap");

            // Second stream attempt — limiter is at capacity, must be refused with 429 + Retry-After.
            ContentResponse second = client.newRequest(url("/v1/topics/orders/records?partition=0&from=earliest"))
                .method(HttpMethod.GET)
                .headers(h -> h.put("Accept", "text/event-stream"))
                .send();
            assertEquals(429, second.getStatus());
            assertEquals("5", second.getHeaders().get("Retry-After"));
            JsonNode envelope = asJson(second.getContent());
            assertTrue(envelope.get("errorMessage").asText().contains("concurrent"),
                "errorMessage should mention concurrent SSE streams, got: "
                    + envelope.get("errorMessage").asText());
        }
        // First stream gets torn down by closing the response above; the limiter releases when the streamer
        // detects the disconnect on its next write attempt.

        // Metrics wiring assertions: the accepted stream must increment SseStreamsOpened exactly once and must
        // NOT pollute the ResponseCount Fetch/2xx bucket (its lifetime is not a synchronous response). The
        // refused stream must increment RejectedAtSseCap exactly once. Without these assertions, a future
        // refactor could disconnect the metric from its servlet call site and the gauge/dashboards would silently
        // go cold.
        assertEquals(openedBefore + 1L, streamsOpened.count(),
            "the accepted SSE stream must increment SseStreamsOpened exactly once");
        assertEquals(rejectedBefore + 1L, rejectedAtSseCap.count(),
            "the refused SSE stream must increment RejectedAtSseCap exactly once");
        assertEquals(fetch2xxBefore, fetch2xx.count(),
            "accepted SSE must NOT land in ResponseCount Fetch/2xx — the SSE meter is intentionally separate");
        assertEquals(fetch4xxBefore + 1L, fetch4xx.count(),
            "the 429 must also land in ResponseCount Fetch/4xx so the bucketed dashboard reflects the rejection");
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

    @Test
    void sseReleasesSlotWhenIdleClientDisconnects() throws Exception {
        // Regression for the idle-disconnect SSE leak: a client that opens SSE on a perpetually-quiet topic
        // (broker keeps returning empty fetch pages) and then drops its connection must not leak the
        // SseStreamLimiter slot. Without the empty-fetch heartbeat the streamer has no write attempt that
        // could surface the TCP close — it would spin on broker fetches forever. With the heartbeat, the
        // next iteration's `: \n\n` write throws IOException once the kernel observes the peer close, and
        // closeStream() releases the slot. We assert via the ActiveSseStreams gauge dropping back to zero
        // AND a second SSE attempt succeeding under a cap of 1 (the strongest end-to-end proof of release).
        tearDown();
        startServer(DEFAULT_TEST_MAX_BODY_BYTES, 1, DEFAULT_TEST_MAX_WS_SUBSCRIPTIONS);
        submitter.fetchAlwaysEmpty = true;

        com.yammer.metrics.core.Gauge<?> active = (com.yammer.metrics.core.Gauge<?>)
            org.apache.kafka.server.metrics.KafkaYammerMetrics.defaultRegistry().allMetrics()
                .get(bridgeMetricName("ActiveSseStreams"));
        assertNotNull(active, "ActiveSseStreams gauge must be registered after a fresh server start");

        InputStreamResponseListener listener = new InputStreamResponseListener();
        client.newRequest(url("/v1/topics/orders/records?partition=0&from=earliest"))
            .method(HttpMethod.GET)
            .headers(h -> h.put("Accept", "text/event-stream"))
            .send(listener);
        Response response = listener.get(5, TimeUnit.SECONDS);
        assertEquals(200, response.getStatus());
        // Read the priming `: connected` comment so we know the streamer is fully wired before we disconnect.
        InputStream body = listener.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8));
        String connectedLine = reader.readLine();
        assertNotNull(connectedLine, "stream must emit at least the `: connected` comment line");
        assertTrue(connectedLine.startsWith(":"), "first SSE line must be a comment, got: " + connectedLine);
        // Wait until the streamer has counted the slot — the gauge update happens before the priming bytes
        // are written, so this should already be true.
        assertEquals(1, ((Number) active.value()).intValue(),
            "ActiveSseStreams must report 1 while the SSE stream is in flight");

        // Abrupt client-side disconnect. The streamer's next heartbeat write must fail with IOException
        // once the kernel observes the peer close, regardless of timing of the next fetch result.
        reader.close();
        body.close();

        // Poll the gauge with a generous timeout — broker fetch max-wait in production is 500ms; here
        // empty fetches return immediately, so the heartbeat-write-fails path should kick in within a few
        // hundred ms of the disconnect. 10s is enough headroom that a slow CI host won't flake.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (((Number) active.value()).intValue() != 0 && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(0, ((Number) active.value()).intValue(),
            "ActiveSseStreams must return to 0 within 10s of client disconnect — otherwise the idle "
                + "stream leaked its limiter slot (the fix is the empty-fetch heartbeat write in SseStreamer)");

        // Strongest end-to-end proof: under maxSseStreams=1, a fresh SSE attempt must now succeed. If the
        // slot had leaked we'd get 429 here instead of 200.
        InputStreamResponseListener secondListener = new InputStreamResponseListener();
        client.newRequest(url("/v1/topics/orders/records?partition=0&from=earliest"))
            .method(HttpMethod.GET)
            .headers(h -> h.put("Accept", "text/event-stream"))
            .send(secondListener);
        Response secondResponse = secondListener.get(5, TimeUnit.SECONDS);
        assertEquals(200, secondResponse.getStatus(),
            "after the first stream's slot is released, a second SSE attempt must be admitted (200), not 429");
        // Close the second connection so @AfterEach can shut the server down cleanly.
        secondListener.getInputStream().close();
    }

    // ----- WebSocket -----

    @Test
    void wsSubscribeDeliversInitialCreditsThenFlowDeliversMore() throws Exception {
        // PROMPT.md FS2: subscribe with initial credits N, broker has more than N records buffered, server delivers
        // exactly N, then a `flow` grant of M delivers M more. Proves the credit-gated streamer threads through the
        // real WebSocket upgrade and that subscribe/flow frames are parsed off the wire. Also asserts the WS metrics
        // (WsSubscriptionsOpened, ActiveWsSubscriptions) increment through the real upgrade path — Codex flagged this
        // explicitly: a unit-test-only proof leaves the upgrade-time creator unverified.
        com.yammer.metrics.core.Meter opened = (com.yammer.metrics.core.Meter)
            org.apache.kafka.server.metrics.KafkaYammerMetrics.defaultRegistry().allMetrics()
                .get(bridgeMetricName("WsSubscriptionsOpened"));
        com.yammer.metrics.core.Gauge<?> activeGauge = (com.yammer.metrics.core.Gauge<?>)
            org.apache.kafka.server.metrics.KafkaYammerMetrics.defaultRegistry().allMetrics()
                .get(bridgeMetricName("ActiveWsSubscriptions"));
        assertNotNull(opened, "WsSubscriptionsOpened must be registered");
        assertNotNull(activeGauge, "ActiveWsSubscriptions must be registered");
        long openedBefore = opened.count();

        // One batch with 20 records starting at offset 5 — initialCredits=5 will deliver 5 and buffer 15; flow=10
        // will then deliver 10 more. The drain logic only kicks a new fetch when the buffer empties AND credits>0,
        // so a single seeded batch is sufficient — the streamer stays parked on the buffer thereafter.
        ConcurrentLinkedQueue<RequestSubmitter.FetchResult> queue = new ConcurrentLinkedQueue<>();
        List<FetchResponseFormatter.FetchedRecord> records = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            records.add(new FetchResponseFormatter.FetchedRecord(
                5 + i, null, ("v" + i).getBytes(StandardCharsets.UTF_8), null, 1000L + i));
        }
        queue.add(new RequestSubmitter.FetchResult(
            new FetchResponseFormatter.PartitionFetch(0, Errors.NONE, null, 5, 0, 25, records), 0L));
        submitter.fetchResultQueue = queue;

        WebSocketClient wsClient = new WebSocketClient();
        wsClient.start();
        try {
            CapturingWsListener listener = new CapturingWsListener();
            URI uri = URI.create(wsUrl("/v1/topics/orders/subscribe"));
            Session session = wsClient.connect(listener, uri).get(5, TimeUnit.SECONDS);
            try {
                listener.openLatch.await(5, TimeUnit.SECONDS);
                session.sendText(
                    "{\"type\":\"subscribe\",\"partition\":0,\"offset\":5,\"maxBytes\":200000,\"initialCredits\":5}",
                    Callback.NOOP);
                listener.awaitMessages(5, 5, TimeUnit.SECONDS);
                // Five records delivered, buffer is sitting on the remaining 15 — verify no over-delivery before the
                // flow frame goes out. A small wait is the only honest way to detect "stream is paused" because the
                // absence of further records is silent.
                Thread.sleep(150);
                assertEquals(5, listener.messages.size(),
                    "initialCredits=5 must cap delivery at 5 records until a flow frame extends credit");
                // Verify the ActiveWsSubscriptions gauge sees the open connection while it is in fact open.
                Number active = (Number) activeGauge.value();
                assertEquals(1, active.intValue(),
                    "ActiveWsSubscriptions gauge must report 1 while a subscription is in progress");

                session.sendText("{\"type\":\"flow\",\"credits\":10}", Callback.NOOP);
                listener.awaitMessages(15, 5, TimeUnit.SECONDS);
                assertEquals(15, listener.messages.size(),
                    "after flow=10 the total delivered must be 15 — 5 initial + 10 granted");
            } finally {
                session.close(StatusCode.NORMAL, "test done", Callback.NOOP);
                listener.closeLatch.await(5, TimeUnit.SECONDS);
            }
            // Verify the record envelopes are well-formed JSON with the expected discriminator + offsets.
            for (int i = 0; i < 15; i++) {
                JsonNode envelope = asJson(listener.messages.get(i).getBytes(StandardCharsets.UTF_8));
                assertEquals("record", envelope.get("type").asText());
                assertEquals(5L + i, envelope.get("offset").asLong());
            }
        } finally {
            wsClient.stop();
        }

        assertEquals(openedBefore + 1L, opened.count(),
            "WsSubscriptionsOpened must increment exactly once per accepted upgrade");
    }

    @Test
    void wsReturns429WhenSubscriptionCapReached() throws Exception {
        // Restart with a WS cap of 1 so the first subscription consumes all capacity. The second upgrade attempt must
        // be refused with HTTP 429 + Retry-After at the upgrade gate, NOT a half-opened WS that immediately errors —
        // a runaway client otherwise pins a Jetty I/O slot per attempt. PROMPT.md AC reserves 429 for the WebSocket
        // pre-flight throttle (the concurrency cap is exactly that), distinguishing it from the HTTP produce path
        // which uses 200+Retry-After for quota throttling. Also asserts RejectedAtWsCap increments, which is the
        // metric operators alert on for "WS bridge is saturated".
        tearDown();
        startServer(DEFAULT_TEST_MAX_BODY_BYTES, DEFAULT_TEST_MAX_SSE_STREAMS, 1);
        com.yammer.metrics.core.Meter opened = (com.yammer.metrics.core.Meter)
            org.apache.kafka.server.metrics.KafkaYammerMetrics.defaultRegistry().allMetrics()
                .get(bridgeMetricName("WsSubscriptionsOpened"));
        com.yammer.metrics.core.Meter rejected = (com.yammer.metrics.core.Meter)
            org.apache.kafka.server.metrics.KafkaYammerMetrics.defaultRegistry().allMetrics()
                .get(bridgeMetricName("RejectedAtWsCap"));
        assertNotNull(opened, "WsSubscriptionsOpened must be registered after restart");
        assertNotNull(rejected, "RejectedAtWsCap must be registered after restart");
        long openedBefore = opened.count();
        long rejectedBefore = rejected.count();

        // Seed a single record so the first subscription receives something — proves the slot is actually held, not
        // released mid-handshake. Subsequent fetches will block (never-completing future); that's fine because we
        // only need the slot to stay taken for the duration of the cap-rejection check.
        ConcurrentLinkedQueue<RequestSubmitter.FetchResult> queue = new ConcurrentLinkedQueue<>();
        queue.add(new RequestSubmitter.FetchResult(
            new FetchResponseFormatter.PartitionFetch(
                0, Errors.NONE, null, 5, 0, 6,
                List.of(new FetchResponseFormatter.FetchedRecord(
                    5, null, "v0".getBytes(StandardCharsets.UTF_8), null, 1L))),
            0L));
        submitter.fetchResultQueue = queue;

        WebSocketClient wsClient = new WebSocketClient();
        wsClient.start();
        try {
            CapturingWsListener firstListener = new CapturingWsListener();
            Session first = wsClient.connect(firstListener, URI.create(wsUrl("/v1/topics/orders/subscribe")))
                .get(5, TimeUnit.SECONDS);
            try {
                firstListener.openLatch.await(5, TimeUnit.SECONDS);
                first.sendText(
                    "{\"type\":\"subscribe\",\"partition\":0,\"offset\":5,\"maxBytes\":200000,\"initialCredits\":1}",
                    Callback.NOOP);
                firstListener.awaitMessages(1, 5, TimeUnit.SECONDS);

                // Second upgrade attempt — limiter is at capacity, must fail with 429.
                CapturingWsListener secondListener = new CapturingWsListener();
                ExecutionException ex = org.junit.jupiter.api.Assertions.assertThrows(
                    ExecutionException.class,
                    () -> wsClient.connect(secondListener, URI.create(wsUrl("/v1/topics/orders/subscribe")))
                        .get(5, TimeUnit.SECONDS));
                Throwable cause = ex.getCause();
                assertTrue(cause instanceof UpgradeException,
                    "second connect must surface as UpgradeException, got: " + cause);
                assertEquals(429, ((UpgradeException) cause).getResponseStatusCode(),
                    "second connect must fail with HTTP 429 — the upgrade-time admission gate fired");

                // UpgradeException exposes only the status; to verify the rejection body and Retry-After header we
                // issue a third upgrade-shaped request through the plain HttpClient — the WS filter still invokes the
                // creator (which still returns null with sendError 429), but HttpClient surfaces the full response
                // instead of throwing UpgradeException. Both 429s land while cap=1 is still held by `first`.
                ContentResponse rawResp = client.newRequest(URI.create(url("/v1/topics/orders/subscribe")))
                    .method(HttpMethod.GET)
                    .headers(headers -> {
                        headers.put("Connection", "Upgrade");
                        headers.put("Upgrade", "websocket");
                        // Constant Sec-WebSocket-Key (RFC 6455 b64-encoded 16-byte nonce) — server-side handshake
                        // accepts any value; we only need the headers to satisfy the upgrade filter so the creator
                        // runs and our cap-rejection path fires.
                        headers.put("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");
                        headers.put("Sec-WebSocket-Version", "13");
                    })
                    .send();
                assertEquals(429, rawResp.getStatus(),
                    "raw upgrade request at-cap must also surface 429");
                assertEquals("5", rawResp.getHeaders().get("Retry-After"),
                    "Retry-After must survive sendError → JsonErrorHandler — clients rely on it to back off");
                assertTrue(rawResp.getHeaders().get("Content-Type").startsWith("application/json"),
                    "429 body must be JSON, not the default Jetty HTML error page");
                JsonNode envelope = mapper.readTree(rawResp.getContent());
                assertEquals(429, envelope.get("errorCode").asInt(),
                    "cap-rejection envelope must match the bridge's {errorCode, errorMessage} contract");
                assertTrue(envelope.get("errorMessage").asText().toLowerCase(java.util.Locale.ROOT).contains("cap"),
                    "errorMessage must explain why the upgrade was refused, got: " + envelope.get("errorMessage"));
            } finally {
                first.close(StatusCode.NORMAL, "test done", Callback.NOOP);
                firstListener.closeLatch.await(5, TimeUnit.SECONDS);
            }
        } finally {
            wsClient.stop();
        }

        assertEquals(openedBefore + 1L, opened.count(),
            "exactly one upgrade was accepted; WsSubscriptionsOpened must increment by 1");
        assertEquals(rejectedBefore + 2L, rejected.count(),
            "two upgrades were refused at cap (WebSocketClient + raw HttpClient); RejectedAtWsCap must increment by 2");
    }

    @Test
    void wsClosesWith1003WhenFirstFrameIsNotSubscribe() throws Exception {
        // The endpoint's state machine requires the first text frame to be a `subscribe`. A client that opens the
        // socket and immediately sends `flow` (or any other shape) is in protocol violation; the server must respond
        // with an error envelope and close 1003 (Unsupported Data) rather than silently swallow the frame or attempt
        // to recover. This proves the close code makes it over the wire — unit tests assert the local close call.
        WebSocketClient wsClient = new WebSocketClient();
        wsClient.start();
        try {
            CapturingWsListener listener = new CapturingWsListener();
            Session session = wsClient.connect(listener, URI.create(wsUrl("/v1/topics/orders/subscribe")))
                .get(5, TimeUnit.SECONDS);
            listener.openLatch.await(5, TimeUnit.SECONDS);

            // First frame is `flow`, not `subscribe` — protocol violation.
            session.sendText("{\"type\":\"flow\",\"credits\":5}", Callback.NOOP);
            // The endpoint sends an error envelope first, then the close frame. Wait for both.
            listener.awaitMessages(1, 5, TimeUnit.SECONDS);
            assertTrue(listener.closeLatch.await(5, TimeUnit.SECONDS),
                "server must close the session after a protocol violation, not just swallow the bad frame");

            JsonNode envelope = asJson(listener.messages.get(0).getBytes(StandardCharsets.UTF_8));
            assertEquals("error", envelope.get("type").asText());
            assertEquals("BAD_MESSAGE", envelope.get("errorCode").asText());
            assertTrue(envelope.get("errorMessage").asText().contains("subscribe"),
                "error message should name the missing 'subscribe' frame, got: "
                    + envelope.get("errorMessage").asText());
            assertEquals(StatusCode.BAD_DATA, listener.closeStatus,
                "WebSocket close code must be 1003 (Unsupported Data) for protocol violations");
        } finally {
            wsClient.stop();
        }
    }

    @Test
    void extractSubscribeTopicHandlesGoodAndBadPaths() {
        // Defence-in-depth: the WS filter already path-matches WS_PATH_SPEC before this method is consulted, but a
        // future Jetty-version change to path-matching semantics (e.g. trailing slashes, double slashes) must not
        // silently route to an empty-topic subscription. Mirror the SSE-side unit tests for extractTopic.
        assertEquals("orders", KafkaHttpServer.extractSubscribeTopic("/v1/topics/orders/subscribe", "/v1"));
        assertEquals("o-r-d-e-r-s",
            KafkaHttpServer.extractSubscribeTopic("/v1/topics/o-r-d-e-r-s/subscribe", "/v1"));
        // No context-path stripping when the request path is already context-relative — the production code passes
        // contextPath="/v1" so this branch is defensive, not the hot path. Still want it to behave.
        assertEquals("orders", KafkaHttpServer.extractSubscribeTopic("/topics/orders/subscribe", ""));
        assertNull(KafkaHttpServer.extractSubscribeTopic(null, "/v1"));
        assertNull(KafkaHttpServer.extractSubscribeTopic("/v1/topics/orders", "/v1"));                // missing suffix
        assertNull(KafkaHttpServer.extractSubscribeTopic("/v1/topics//subscribe", "/v1"));            // empty topic
        // slash inside topic
        assertNull(KafkaHttpServer.extractSubscribeTopic("/v1/topics/orders/extra/subscribe", "/v1"));
        // wrong prefix
        assertNull(KafkaHttpServer.extractSubscribeTopic("/v1/other/orders/subscribe", "/v1"));
    }

    private String wsUrl(String path) {
        return "ws://127.0.0.1:" + server.boundPort() + path;
    }

    /**
     * Captures text frames and the close status for assertion. {@link CountDownLatch}-based gates let tests await
     * specific milestones (open, N messages, close) without sleeping on arbitrary deadlines.
     */
    public static final class CapturingWsListener implements Session.Listener.AutoDemanding {
        final CountDownLatch openLatch = new CountDownLatch(1);
        final CountDownLatch closeLatch = new CountDownLatch(1);
        final CopyOnWriteArrayList<String> messages = new CopyOnWriteArrayList<>();
        volatile int closeStatus = -1;
        volatile String closeReason;

        @Override
        public void onWebSocketOpen(Session session) {
            openLatch.countDown();
        }

        @Override
        public void onWebSocketText(String message) {
            messages.add(message);
        }

        @Override
        public void onWebSocketClose(int statusCode, String reason) {
            this.closeStatus = statusCode;
            this.closeReason = reason;
            closeLatch.countDown();
        }

        /** Wait until {@code messages.size() >= target} or the deadline expires. Asserts that the target was
         * actually reached — otherwise downstream assertions that read messages.size() would silently mask a
         * subscription that never delivered anything (the latch and the size check are independent signals). */
        void awaitMessages(int target, long timeout, TimeUnit unit) throws InterruptedException {
            long deadline = System.nanoTime() + unit.toNanos(timeout);
            while (messages.size() < target && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            org.junit.jupiter.api.Assertions.assertTrue(messages.size() >= target,
                "expected to receive at least " + target + " WS messages within " + timeout + " " + unit
                    + " — only received " + messages.size() + ": " + messages);
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

    // ----- lifecycle and metrics wiring -----

    @Test
    void metricsArePopulatedThroughTheServletWiring() throws Exception {
        // Smoke test that proves the metrics object the server constructs is actually the one the servlet writes to.
        // Without this end-to-end check, a wiring mistake (forgotten constructor argument, lost reference) would only
        // show up the first time an operator queried JMX in production. We assert a small but representative slice:
        // a 2xx Produce updates the Produce/2xx response meter exactly once, and the histogram captures one sample.
        com.yammer.metrics.core.MetricName twoXxName = bridgeMetricName("ResponseCount", "operation", "Produce", "statusClass", "2xx");
        com.yammer.metrics.core.MetricName latencyName = bridgeMetricName("RequestLatencyMs", "operation", "Produce");
        com.yammer.metrics.core.Meter produce2xx = (com.yammer.metrics.core.Meter)
            org.apache.kafka.server.metrics.KafkaYammerMetrics.defaultRegistry().allMetrics().get(twoXxName);
        com.yammer.metrics.core.Histogram produceLatency = (com.yammer.metrics.core.Histogram)
            org.apache.kafka.server.metrics.KafkaYammerMetrics.defaultRegistry().allMetrics().get(latencyName);
        assertNotNull(produce2xx, "ResponseCount Produce/2xx meter should be registered while server is running");
        assertNotNull(produceLatency, "RequestLatencyMs Produce histogram should be registered while server is running");
        long meterBefore = produce2xx.count();
        long histBefore = produceLatency.count();

        submitter.produceResult = new RequestSubmitter.ProduceResult(
            Collections.singletonList(
                new ProduceResponseFormatter.PartitionResult(0, 7L, Errors.NONE, null)),
            0L);
        ContentResponse ok = client.newRequest(url("/v1/topics/orders/records"))
            .method(HttpMethod.POST)
            .body(new StringRequestContent("application/json",
                "{\"records\":[{\"value\":{\"type\":\"STRING\",\"data\":\"hi\"}}]}"))
            .send();
        assertEquals(200, ok.getStatus());

        // The servlet records on the async-completion thread; give the executor a brief window for the recording
        // to land before sampling. In practice the recording completes well within a few millis on localhost; this
        // is a deflake safety net, not a logical delay.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (produce2xx.count() == meterBefore && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(meterBefore + 1L, produce2xx.count(),
            "Produce 2xx meter must increment exactly once after a successful POST");
        assertEquals(histBefore + 1L, produceLatency.count(),
            "Produce latency histogram must record exactly one sample for the successful POST");
    }

    @Test
    void oversizedBodyRejectionMetricIsWiredFromTheServlet() throws Exception {
        // Restart with a tight body cap, then verify the 413 path increments both RejectedOversizedBody and
        // the Produce/4xx response meter. This is the second half of the wiring proof: rejections at the bridge
        // boundary (rather than the broker boundary) must land on their dedicated counter so operators can alert
        // on bridge-side admission control without first having to read access logs.
        tearDown();
        startServer(64, DEFAULT_TEST_MAX_SSE_STREAMS, DEFAULT_TEST_MAX_WS_SUBSCRIPTIONS);
        com.yammer.metrics.core.MetricName oversizedName = bridgeMetricName("RejectedOversizedBody");
        com.yammer.metrics.core.MetricName fourXxName = bridgeMetricName("ResponseCount", "operation", "Produce", "statusClass", "4xx");
        com.yammer.metrics.core.Meter oversized = (com.yammer.metrics.core.Meter)
            org.apache.kafka.server.metrics.KafkaYammerMetrics.defaultRegistry().allMetrics().get(oversizedName);
        com.yammer.metrics.core.Meter produce4xx = (com.yammer.metrics.core.Meter)
            org.apache.kafka.server.metrics.KafkaYammerMetrics.defaultRegistry().allMetrics().get(fourXxName);
        assertNotNull(oversized, "RejectedOversizedBody must be registered after restart");
        assertNotNull(produce4xx, "ResponseCount Produce/4xx must be registered after restart");
        long oversizedBefore = oversized.count();
        long produce4xxBefore = produce4xx.count();

        ContentResponse tooBig = client.newRequest(url("/v1/topics/orders/records"))
            .method(HttpMethod.POST)
            .body(new StringRequestContent("application/json",
                "{\"records\":[{\"value\":{\"type\":\"STRING\",\"data\":\"" + "x".repeat(200) + "\"}}]}"))
            .send();
        assertEquals(413, tooBig.getStatus());

        assertEquals(oversizedBefore + 1L, oversized.count(),
            "413 must increment RejectedOversizedBody exactly once");
        assertEquals(produce4xxBefore + 1L, produce4xx.count(),
            "413 must also land in ResponseCount Produce/4xx so the bucketed dashboard reflects the rejection");
    }

    @Test
    void metricsAreUnregisteredAfterStopEvenIfStartNeverRan() throws Exception {
        // Constructor registers metrics so the ActiveSseStreams gauge exists from the moment a KafkaHttpServer
        // object exists (operators expect "metric present" the second the broker process is running). But that
        // means a never-started or start-failed server still has live entries — stop() must close them even when
        // the Jetty server itself was never started, or the next start() in the same JVM trips "duplicate metric"
        // on the global Yammer registry.
        // Re-use the main test's setup by tearing down the running server first.
        tearDown();
        KafkaHttpServer neverStarted = new KafkaHttpServer("127.0.0.1", 0,
            new KafkaHttpBridge(mapper, submitter), submitter, mapper,
            DEFAULT_TEST_MAX_BODY_BYTES, DEFAULT_TEST_MAX_SSE_STREAMS, DEFAULT_TEST_MAX_WS_SUBSCRIPTIONS);
        try {
            com.yammer.metrics.core.MetricName gauge = bridgeMetricName("ActiveSseStreams");
            assertNotNull(
                org.apache.kafka.server.metrics.KafkaYammerMetrics.defaultRegistry().allMetrics().get(gauge),
                "constructor must register the ActiveSseStreams gauge so JMX never shows a missing metric");
        } finally {
            neverStarted.stop();
        }
        com.yammer.metrics.core.MetricName gauge = bridgeMetricName("ActiveSseStreams");
        assertNull(
            org.apache.kafka.server.metrics.KafkaYammerMetrics.defaultRegistry().allMetrics().get(gauge),
            "stop() must unregister even when start() never ran, or a subsequent KafkaHttpServer cannot construct");
        // Build a second server in the same JVM to prove no stale entries leaked. Construction throws if the
        // registry refused a duplicate.
        KafkaHttpServer second = new KafkaHttpServer("127.0.0.1", 0,
            new KafkaHttpBridge(mapper, submitter), submitter, mapper,
            DEFAULT_TEST_MAX_BODY_BYTES, DEFAULT_TEST_MAX_SSE_STREAMS, DEFAULT_TEST_MAX_WS_SUBSCRIPTIONS);
        second.stop();
        // Restore the @BeforeEach-style state for any later tests that follow the alphabetical execution order;
        // tearDown() in @AfterEach handles whatever this method leaves behind.
        startServer(DEFAULT_TEST_MAX_BODY_BYTES, DEFAULT_TEST_MAX_SSE_STREAMS, DEFAULT_TEST_MAX_WS_SUBSCRIPTIONS);
    }

    @Test
    void startAfterStopFailsLoudly() throws Exception {
        // After stop() the metrics object is dead. The one-shot contract documented on KafkaHttpServer says callers
        // must construct a new instance; defending start() with an IllegalStateException turns silent-data-loss
        // (servlet wired to a closed metrics object that drops every recording) into a noisy boot failure.
        tearDown();
        KafkaHttpServer oneShot = new KafkaHttpServer("127.0.0.1", 0,
            new KafkaHttpBridge(mapper, submitter), submitter, mapper,
            DEFAULT_TEST_MAX_BODY_BYTES, DEFAULT_TEST_MAX_SSE_STREAMS, DEFAULT_TEST_MAX_WS_SUBSCRIPTIONS);
        oneShot.start();
        oneShot.stop();
        IllegalStateException ex = org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, oneShot::start);
        org.junit.jupiter.api.Assertions.assertTrue(ex.getMessage().contains("stopped"),
            "error message should make the one-shot constraint obvious: " + ex.getMessage());
        startServer(DEFAULT_TEST_MAX_BODY_BYTES, DEFAULT_TEST_MAX_SSE_STREAMS, DEFAULT_TEST_MAX_WS_SUBSCRIPTIONS);
    }

    private static com.yammer.metrics.core.MetricName bridgeMetricName(String name, String... tagPairs) {
        StringBuilder mbean = new StringBuilder("kafka.network.http:type=BridgeMetrics,name=").append(name);
        StringBuilder scope = new StringBuilder();
        for (int i = 0; i + 1 < tagPairs.length; i += 2) {
            mbean.append(",").append(tagPairs[i]).append("=").append(tagPairs[i + 1]);
            if (scope.length() > 0) scope.append(".");
            scope.append(tagPairs[i]).append(".").append(tagPairs[i + 1]);
        }
        return new com.yammer.metrics.core.MetricName(
            "kafka.network.http", "BridgeMetrics", name,
            scope.length() == 0 ? null : scope.toString(),
            mbean.toString());
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
        // When true, submitFetch returns an immediately-completed empty fetch result on every call. Models a
        // perpetually-quiet topic where the broker's purgatory returns empty pages back-to-back — used by the
        // idle-disconnect regression test to ensure the SSE streamer keeps writing heartbeats (and thus
        // surfaces a client disconnect) even when no records ever arrive.
        volatile boolean fetchAlwaysEmpty;
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
            if (fetchAlwaysEmpty) {
                return CompletableFuture.completedFuture(new RequestSubmitter.FetchResult(
                    new FetchResponseFormatter.PartitionFetch(
                        0, Errors.NONE, null, command.offset(), 0, command.offset(), Collections.emptyList()),
                    0L));
            }
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
