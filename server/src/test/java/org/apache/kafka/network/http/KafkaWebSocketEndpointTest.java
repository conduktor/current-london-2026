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

import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.eclipse.jetty.websocket.api.UpgradeResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link KafkaWebSocketEndpoint} — the Jetty {@code Session.Listener.AutoDemanding}
 * implementation that drives a single WebSocket subscription on {@code /v1/topics/{topic}/subscribe}.
 *
 * <p>The endpoint enforces a tight protocol state machine:
 * <ul>
 *   <li>first text frame on the session MUST be {@code {"type":"subscribe",...}} — anything else is a
 *       protocol violation and the session is closed with status 1003 (Unsupported Data);</li>
 *   <li>after a valid subscribe, every subsequent text frame MUST be {@code {"type":"flow",...}};</li>
 *   <li>binary / partial / ping/pong frames the streamer doesn't speak are ignored (default Listener
 *       behaviour);</li>
 *   <li>{@code onWebSocketClose} releases the limiter token whether or not a streamer was started;</li>
 *   <li>{@code onWebSocketError} also releases the limiter token — the close handler may or may not
 *       follow, depending on how badly the socket failed, so cleanup must be idempotent.</li>
 * </ul>
 *
 * <p>These tests use a {@link FakeSession} so the state machine can be exercised without a real
 * WebSocket connection. End-to-end behaviour against the real Jetty WS upgrade pipeline is covered
 * by {@code KafkaHttpServerIntegrationTest}.
 */
class KafkaWebSocketEndpointTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private WsStreamLimiter limiter;
    private FakeSubmitter submitter;
    private FakeSession session;

    @BeforeEach
    void setUp() {
        limiter = new WsStreamLimiter(10);
        submitter = new FakeSubmitter();
        session = new FakeSession();
    }

    @AfterEach
    void tearDown() {
        // No-op: each test asserts limiter state explicitly. Setup creates fresh state.
    }

    // ----- protocol state machine -----

    @Test
    void firstFrameSubscribeStartsStreamerAndDeliversInitialCredits() {
        // Happy path: client opens, sends subscribe with N initial credits, broker has records ->
        // exactly N record frames arrive, the streamer is alive after.
        WsStreamLimiter.Token token = limiter.tryAcquire();
        submitter.queueFetch(records(0, 20));

        KafkaWebSocketEndpoint endpoint = newEndpoint(token, "orders");
        endpoint.onWebSocketOpen(session);
        endpoint.onWebSocketText(
            "{\"type\":\"subscribe\",\"partition\":0,\"offset\":0,\"initialCredits\":5}");

        assertEquals(5, session.recordCount(), "exactly initialCredits delivered after subscribe");
        assertFalse(session.closed.get(), "session must remain open after delivering the initial budget");
    }

    @Test
    void flowFrameAfterSubscribeGrantsAdditionalCredits() {
        // After the initial budget is drained, a flow frame must release more — proving the endpoint
        // forwards grants to the streamer rather than restarting the stream.
        WsStreamLimiter.Token token = limiter.tryAcquire();
        submitter.queueFetch(records(0, 20));

        KafkaWebSocketEndpoint endpoint = newEndpoint(token, "orders");
        endpoint.onWebSocketOpen(session);
        endpoint.onWebSocketText(
            "{\"type\":\"subscribe\",\"partition\":0,\"offset\":0,\"initialCredits\":5}");
        assertEquals(5, session.recordCount());

        endpoint.onWebSocketText("{\"type\":\"flow\",\"credits\":7}");
        assertEquals(12, session.recordCount(), "flow must release the next 7 records");
    }

    @Test
    void firstFrameNotSubscribeClosesWith1003() {
        // A flow frame as the first message is a protocol violation — the client never declared a
        // partition/offset/budget, so there is no stream to grant against. Close 1003 ("Unsupported
        // Data") with an envelope describing why so the client log makes the failure obvious.
        WsStreamLimiter.Token token = limiter.tryAcquire();
        KafkaWebSocketEndpoint endpoint = newEndpoint(token, "orders");
        endpoint.onWebSocketOpen(session);

        endpoint.onWebSocketText("{\"type\":\"flow\",\"credits\":1}");

        assertEquals(1003, session.closeStatus.get(),
            "first frame not being subscribe must close with 1003 (Unsupported Data)");
        assertTrue(session.closed.get(), "session must be closed");
        assertEquals(0, limiter.inUse(), "limiter slot must be released even with no streamer started");
    }

    @Test
    void malformedJsonOnFirstFrameClosesWith1003() {
        // Invalid JSON in the first frame is the same protocol violation: there is nothing the
        // endpoint can recover from, and silently ignoring the frame would leave the connection
        // half-open. Close 1003 and release the slot.
        WsStreamLimiter.Token token = limiter.tryAcquire();
        KafkaWebSocketEndpoint endpoint = newEndpoint(token, "orders");
        endpoint.onWebSocketOpen(session);

        endpoint.onWebSocketText("not valid JSON {");

        assertEquals(1003, session.closeStatus.get());
        assertEquals(0, limiter.inUse());
    }

    @Test
    void subscribeMissingRequiredFieldClosesWith1003() {
        // A subscribe with no partition is unrecoverable — same close path as malformed JSON.
        WsStreamLimiter.Token token = limiter.tryAcquire();
        KafkaWebSocketEndpoint endpoint = newEndpoint(token, "orders");
        endpoint.onWebSocketOpen(session);

        endpoint.onWebSocketText("{\"type\":\"subscribe\",\"offset\":0,\"initialCredits\":5}");

        assertEquals(1003, session.closeStatus.get());
        assertEquals(0, limiter.inUse());
    }

    @Test
    void doubleSubscribeAfterValidSubscribeClosesWith1003() {
        // The second frame after a valid subscribe must be flow. A second subscribe means the client
        // is trying to multiplex two streams on one connection — not supported, this is a state-machine
        // violation, close 1003. Releasing the slot is essential because the first streamer also has
        // to be torn down.
        WsStreamLimiter.Token token = limiter.tryAcquire();
        submitter.queueFetch(records(0, 0));
        KafkaWebSocketEndpoint endpoint = newEndpoint(token, "orders");
        endpoint.onWebSocketOpen(session);
        endpoint.onWebSocketText(
            "{\"type\":\"subscribe\",\"partition\":0,\"offset\":0,\"initialCredits\":1}");

        endpoint.onWebSocketText(
            "{\"type\":\"subscribe\",\"partition\":1,\"offset\":0,\"initialCredits\":1}");

        assertEquals(1003, session.closeStatus.get());
        assertEquals(0, limiter.inUse());
    }

    @Test
    void flowBeforeSubscribeAfterFailedSubscribeIsIgnoredBecauseSessionIsClosed() {
        // If the first subscribe failed validation, the session is already closed — a follow-up frame
        // (which a real client wouldn't send, but a buggy or hostile one might) must not re-acquire
        // a limiter slot or re-open the stream.
        WsStreamLimiter.Token token = limiter.tryAcquire();
        KafkaWebSocketEndpoint endpoint = newEndpoint(token, "orders");
        endpoint.onWebSocketOpen(session);
        endpoint.onWebSocketText("{\"type\":\"subscribe\"}"); // missing required fields
        assertEquals(1003, session.closeStatus.get());
        assertEquals(0, limiter.inUse());

        endpoint.onWebSocketText("{\"type\":\"flow\",\"credits\":1}");

        assertEquals(0, limiter.inUse(), "follow-up frames after close must not re-acquire a slot");
    }

    // ----- lifecycle and error path -----

    @Test
    void openInstallsEveryDefensiveCapSoNoJettyDefaultRaisesThePerSessionHeapCeiling() {
        // Regression guard: every cap the endpoint installs at open time must remain set. Leaving any
        // of {maxText, maxBinary, maxFrame, maxOutgoing, idle} at Jetty's defaults (64 KiB for the
        // size caps, -1 for outgoing, 30 s for idle) raises the per-session heap ceiling against
        // hostile inbound frames or lets the outbound queue grow without bound. The text-message cap
        // alone is insufficient — a single 64 KiB text frame still buffers up to Jetty's frame default
        // before the message cap fires, and binary frames (which the bridge silently drops) buffer up
        // to the binary default ceiling before discard.
        WsStreamLimiter.Token token = limiter.tryAcquire();
        KafkaWebSocketEndpoint endpoint = newEndpoint(token, "orders");

        endpoint.onWebSocketOpen(session);

        assertEquals(Duration.ofMinutes(5), session.idleTimeout, "idle timeout must be 5 minutes");
        assertEquals(8 * 1024L, session.maxTextMessageSize, "text message cap must be 8 KiB");
        assertEquals(8 * 1024L, session.maxBinaryMessageSize,
            "binary message cap must match text — bridge has no binary sink");
        assertEquals(8 * 1024L, session.maxFrameSize,
            "frame cap must be 8 KiB so per-frame bound matches per-message");
        assertEquals(1024, session.maxOutgoingFrames, "outgoing-frame queue cap must be MAX_OUTGOING_FRAMES");
    }

    @Test
    void closeReleasesLimiterEvenWithoutSubscribe() {
        // Client opens the WS connection then disconnects before sending any frame. The endpoint
        // must still release the slot it was holding on the limiter — otherwise idle clients
        // permanently consume capacity.
        WsStreamLimiter.Token token = limiter.tryAcquire();
        KafkaWebSocketEndpoint endpoint = newEndpoint(token, "orders");
        endpoint.onWebSocketOpen(session);

        endpoint.onWebSocketClose(1000, "normal closure");

        assertEquals(0, limiter.inUse(), "close before subscribe must release the slot");
    }

    @Test
    void closeAfterSubscribeStopsStreamerAndReleasesLimiter() {
        // Client subscribed, has been receiving records, then disconnects. The streamer's close()
        // must run — its underlying token is the same slot, so the limiter goes back to 0.
        WsStreamLimiter.Token token = limiter.tryAcquire();
        submitter.queueFetch(records(0, 3));
        KafkaWebSocketEndpoint endpoint = newEndpoint(token, "orders");
        endpoint.onWebSocketOpen(session);
        endpoint.onWebSocketText(
            "{\"type\":\"subscribe\",\"partition\":0,\"offset\":0,\"initialCredits\":3}");
        assertEquals(1, limiter.inUse(), "subscribe consumes a limiter slot");

        endpoint.onWebSocketClose(1000, "client disconnect");

        assertEquals(0, limiter.inUse(), "close after subscribe must release the slot via the streamer");
    }

    @Test
    void errorReleasesLimiterIdempotentlyEvenIfCloseAlsoFires() {
        // A failed socket may surface as onWebSocketError + onWebSocketClose, or just onWebSocketError.
        // Both call paths must end with the slot released, and back-to-back invocations must not
        // double-release.
        WsStreamLimiter.Token token = limiter.tryAcquire();
        KafkaWebSocketEndpoint endpoint = newEndpoint(token, "orders");
        endpoint.onWebSocketOpen(session);

        endpoint.onWebSocketError(new RuntimeException("simulated socket failure"));
        endpoint.onWebSocketClose(1006, "abnormal");

        assertEquals(0, limiter.inUse(), "error + close together still must release exactly once");
    }

    @Test
    void partitionErrorFromBrokerProducesErrorFrameAndClosesSession() {
        // Broker reports a partition-level error mid-stream. The streamer sends a single error frame
        // and closes the sink — that close propagates to session.close() so the WS client sees the
        // disconnect.
        WsStreamLimiter.Token token = limiter.tryAcquire();
        submitter.queueFetchError(Errors.UNKNOWN_TOPIC_OR_PARTITION, "no such partition");

        KafkaWebSocketEndpoint endpoint = newEndpoint(token, "orders");
        endpoint.onWebSocketOpen(session);
        endpoint.onWebSocketText(
            "{\"type\":\"subscribe\",\"partition\":99,\"offset\":0,\"initialCredits\":10}");

        assertEquals(1, session.errorCount(), "exactly one error frame on broker partition error");
        JsonNode err = session.errorAt(0);
        assertEquals("UNKNOWN_TOPIC_OR_PARTITION", err.get("errorCode").asText());
        assertTrue(session.closed.get(), "session must close after a broker partition error");
        assertEquals(0, limiter.inUse());
    }

    // ----- helpers -----

    private KafkaWebSocketEndpoint newEndpoint(WsStreamLimiter.Token token, String topic) {
        // Direct executor so all dispatch settles synchronously inside the test thread.
        return new KafkaWebSocketEndpoint(topic, submitter, MAPPER, token, Runnable::run);
    }

    private static List<FetchResponseFormatter.FetchedRecord> records(long startOffset, int count) {
        List<FetchResponseFormatter.FetchedRecord> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            out.add(new FetchResponseFormatter.FetchedRecord(
                startOffset + i,
                ("k" + i).getBytes(),
                ("v" + i).getBytes(),
                "application/json",
                1700000000000L + i));
        }
        return out;
    }

    // ----- fakes -----

    private static final class FakeSubmitter implements RequestSubmitter {
        private final java.util.Deque<java.util.function.Supplier<CompletableFuture<FetchResult>>> queue =
            new java.util.ArrayDeque<>();

        void queueFetch(List<FetchResponseFormatter.FetchedRecord> records) {
            queue.add(() -> {
                FetchResponseFormatter.PartitionFetch view = new FetchResponseFormatter.PartitionFetch(
                    0, Errors.NONE, null, 0L, 0L, 0L, records);
                return CompletableFuture.completedFuture(new FetchResult(view, 0L));
            });
        }

        void queueFetchError(Errors error, String message) {
            queue.add(() -> {
                FetchResponseFormatter.PartitionFetch view = new FetchResponseFormatter.PartitionFetch(
                    0, error, message, 0L, 0L, 0L, Collections.emptyList());
                return CompletableFuture.completedFuture(new FetchResult(view, 0L));
            });
        }

        @Override
        public CompletableFuture<ProduceResult> submitProduce(ProduceRequestParser.ProduceCommand command) {
            throw new UnsupportedOperationException("produce not used in WS endpoint tests");
        }

        @Override
        public CompletableFuture<FetchResult> submitFetch(FetchRequestParser.FetchCommand command) {
            java.util.function.Supplier<CompletableFuture<FetchResult>> next = queue.poll();
            if (next == null) {
                // Mirror the broker holding an idle fetch open during live-tail — never completes,
                // so a streamer that issues a fetch with no records left simply parks.
                return new CompletableFuture<>();
            }
            return next.get();
        }
    }

    /**
     * A fake {@link Session} that captures sendText/close calls in-memory so the endpoint's state
     * machine can be asserted deterministically. Only the methods the endpoint actually invokes are
     * implemented; everything else throws UnsupportedOperationException so accidental use surfaces
     * loudly during test development.
     */
    private static final class FakeSession implements Session {
        final AtomicBoolean closed = new AtomicBoolean();
        final AtomicReference<String> closeReason = new AtomicReference<>();
        final AtomicInteger closeStatus = new AtomicInteger();
        private final ConcurrentLinkedQueue<JsonNode> records = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<JsonNode> errors = new ConcurrentLinkedQueue<>();

        // Cap values the endpoint installs on open. Captured so the test can prove every defensive
        // bound is applied — leaving any of these at Jetty's defaults raises the per-session heap
        // ceiling for hostile inbound frames.
        volatile Duration idleTimeout;
        volatile long maxTextMessageSize;
        volatile long maxBinaryMessageSize;
        volatile long maxFrameSize;
        volatile int maxOutgoingFrames;

        @Override
        public void sendText(String text, Callback callback) {
            try {
                JsonNode node = MAPPER.readTree(text);
                if (node.has("type") && "error".equals(node.get("type").asText())) {
                    errors.add(node);
                } else {
                    records.add(node);
                }
                if (callback != null) callback.succeed();
            } catch (Exception e) {
                if (callback != null) callback.fail(e);
                throw new RuntimeException("FakeSession failed to parse outgoing frame: " + text, e);
            }
        }

        @Override
        public void close() {
            closed.set(true);
        }

        @Override
        public void close(int statusCode, String reason, Callback callback) {
            closeStatus.set(statusCode);
            closeReason.set(reason);
            closed.set(true);
            if (callback != null) callback.succeed();
        }

        @Override
        public boolean isOpen() {
            return !closed.get();
        }

        int recordCount() {
            return records.size();
        }

        int errorCount() {
            return errors.size();
        }

        JsonNode errorAt(int i) {
            return new ArrayList<>(errors).get(i);
        }

        // --- unused Session surface; throw to make accidental dependence loud ---

        @Override
        public void demand() {
            // The endpoint declares Session.Listener.AutoDemanding so Jetty handles demand itself;
            // the streamer never calls demand() explicitly. Quietly tolerate it so production-side
            // changes that add demand() don't break this fake.
        }

        @Override public void sendBinary(ByteBuffer payload, Callback callback) {
            throw new UnsupportedOperationException("WS bridge never sends binary frames");
        }

        @Override public void sendPartialBinary(ByteBuffer payload, boolean last, Callback callback) {
            throw new UnsupportedOperationException();
        }

        @Override public void sendPartialText(String payload, boolean last, Callback callback) {
            throw new UnsupportedOperationException();
        }

        @Override public void sendPing(ByteBuffer applicationData, Callback callback) {
            throw new UnsupportedOperationException();
        }

        @Override public void sendPong(ByteBuffer applicationData, Callback callback) {
            throw new UnsupportedOperationException();
        }

        @Override public void disconnect() {
            closed.set(true);
        }

        @Override public SocketAddress getLocalSocketAddress() {
            return null;
        }

        @Override public SocketAddress getRemoteSocketAddress() {
            return null;
        }

        @Override public String getProtocolVersion() {
            return "13";
        }

        @Override public UpgradeRequest getUpgradeRequest() {
            return null;
        }

        @Override public UpgradeResponse getUpgradeResponse() {
            return null;
        }

        @Override public boolean isSecure() {
            return false;
        }

        // Configurable surface — Jetty's Session extends Configurable. Setters that the endpoint
        // invokes on open capture into volatile fields above so a regression test can assert every
        // defensive cap is applied; unused getters return zero (no test reads them).

        @Override public Duration getIdleTimeout() {
            return Duration.ZERO;
        }

        @Override public void setIdleTimeout(Duration duration) {
            this.idleTimeout = duration;
        }

        @Override public int getInputBufferSize() {
            return 0;
        }

        @Override public void setInputBufferSize(int size) { }

        @Override public int getOutputBufferSize() {
            return 0;
        }

        @Override public void setOutputBufferSize(int size) { }

        @Override public long getMaxBinaryMessageSize() {
            return 0;
        }

        @Override public void setMaxBinaryMessageSize(long size) {
            this.maxBinaryMessageSize = size;
        }

        @Override public long getMaxTextMessageSize() {
            return 0;
        }

        @Override public void setMaxTextMessageSize(long size) {
            this.maxTextMessageSize = size;
        }

        @Override public long getMaxFrameSize() {
            return 0;
        }

        @Override public void setMaxFrameSize(long size) {
            this.maxFrameSize = size;
        }

        @Override public boolean isAutoFragment() {
            return false;
        }

        @Override public void setAutoFragment(boolean autoFragment) { }

        @Override public int getMaxOutgoingFrames() {
            return 0;
        }

        @Override public void setMaxOutgoingFrames(int maxOutgoingFrames) {
            this.maxOutgoingFrames = maxOutgoingFrames;
        }
    }
}
