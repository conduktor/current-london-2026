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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for WsStreamer — the credit-gated streamer behind /v1/topics/{topic}/subscribe.
 *
 * The contract being exercised here is PROMPT.md AC5 ("WebSocket subscriptions enforce credit-based
 * backpressure: subscribe with N initial credits → exactly N messages delivered → client must grant
 * additional credits to continue. Zero credits → no delivery. A fast producer cannot cause unbounded
 * server buffering.") and the FS2 scenario ("subscribe with initialCredits=5 against a queue holding
 * 20 messages delivers exactly 5 messages then pauses; granting 10 more delivers the next 10").
 *
 * The streamer talks to a {@link WsStreamer.FrameSink} abstraction rather than a Jetty Session, so the
 * credit accounting can be tested deterministically without bringing up a real WebSocket. The
 * endpoint code wraps a Session in a sink adapter; the streamer doesn't care.
 *
 * Threading: the streamer dispatches all its work onto a caller-supplied Executor. We use a
 * direct-execute synchronous executor in most tests so each call settles fully before the next
 * assertion, and a real thread pool in the concurrency test.
 */
class WsStreamerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private CapturingSink sink;
    private FakeSubmitter submitter;
    private WsStreamLimiter limiter;
    private WsStreamLimiter.Token token;

    @BeforeEach
    void setUp() {
        sink = new CapturingSink();
        submitter = new FakeSubmitter();
        limiter = new WsStreamLimiter(10);
        token = limiter.tryAcquire();
        assertNotNull(token, "limiter token must be available for setup");
    }

    @AfterEach
    void tearDown() {
        // Don't double-close intentionally — Token is idempotent, but we want to surface accidental
        // double-release from the streamer itself by inspecting limiter.inUse() in each test.
    }

    // ----- AC5 happy path: initial credit budget caps delivery exactly -----

    @Test
    void deliversExactlyInitialCreditsThenPauses() {
        // FS2 verbatim: subscribe with initialCredits=5 against a queue holding 20 records — exactly 5
        // must be delivered, and nothing more until the client grants additional credit.
        submitter.queueFetch(records(0, 20));

        WsStreamer.start(sink, submitter, MAPPER, "t", 0, 0L, OptionalInt.empty(), 5, token, direct());

        assertEquals(5, sink.recordCount(), "exactly initialCredits must be delivered");
        for (int i = 0; i < 5; i++) {
            assertEquals(i, sink.recordOffsetAt(i), "delivered records must be in offset order");
        }
        assertFalse(sink.closed.get(), "stream must remain open after pausing");
    }

    @Test
    void zeroInitialCreditsDeliversNothing() {
        // AC5: "Zero credits → no delivery". The streamer must not even submit a fetch — there is no
        // capacity to deliver records, so spending broker fetch budget on speculative reads is wrong.
        submitter.queueFetch(records(0, 20));

        WsStreamer.start(sink, submitter, MAPPER, "t", 0, 0L, OptionalInt.empty(), 0, token, direct());

        assertEquals(0, sink.recordCount(), "zero initial credits must deliver zero records");
        assertEquals(0, submitter.fetchCallCount(), "zero credits must not trigger a speculative fetch");
        assertFalse(sink.closed.get());
    }

    // ----- AC5 grant path: flow frames unblock further delivery -----

    @Test
    void grantingFlowCreditsDeliversNextN() {
        // FS2 second half: after the initial 5, granting 10 more credits must deliver the next 10
        // — drawing from the buffered carry-over of the first fetch — and stop there.
        submitter.queueFetch(records(0, 20));

        WsStreamer streamer =
            WsStreamer.start(sink, submitter, MAPPER, "t", 0, 0L, OptionalInt.empty(), 5, token, direct());
        assertEquals(5, sink.recordCount());

        streamer.grantCredits(10);

        assertEquals(15, sink.recordCount(), "grant of 10 must release the next 10");
        for (int i = 0; i < 15; i++) {
            assertEquals(i, sink.recordOffsetAt(i));
        }
        assertFalse(sink.closed.get(), "stream must still be open: 5 records remain in the buffer");
    }

    @Test
    void multipleSmallGrantsAccumulateCorrectly() {
        // Drip-feed grants: client grants 1 at a time. The streamer must respect each grant exactly,
        // never anticipating credit and never falling behind.
        submitter.queueFetch(records(0, 5));

        WsStreamer streamer =
            WsStreamer.start(sink, submitter, MAPPER, "t", 0, 0L, OptionalInt.empty(), 0, token, direct());
        assertEquals(0, sink.recordCount());

        // Prime the buffer with a grant that triggers a fetch, but constrained by zero initial credits.
        // We need a way to get records into the buffer without consuming them: grant 0 is rejected by the
        // parser, so we grant 1 to fetch then drain it slowly.
        streamer.grantCredits(1);
        assertEquals(1, sink.recordCount(), "first grant delivers exactly 1");

        streamer.grantCredits(1);
        assertEquals(2, sink.recordCount(), "second grant delivers exactly 1 more");

        streamer.grantCredits(3);
        assertEquals(5, sink.recordCount(), "third grant of 3 delivers the remaining 3");
    }

    // ----- recursive long-poll: empty fetches loop back -----

    @Test
    void emptyFetchTriggersNextFetchWhenCreditsRemain() {
        // The broker-side long-poll has already held us for its max-wait; if it returns empty, we must
        // immediately submit again (otherwise live-tail SSE/WS behaviour breaks). Crucially this must
        // only happen while we still have credit — otherwise we burn fetch capacity on a paused stream.
        submitter.queueFetch(records(0, 0));     // first fetch returns empty
        submitter.queueFetch(records(0, 3));     // second fetch returns 3 records

        WsStreamer.start(sink, submitter, MAPPER, "t", 0, 0L, OptionalInt.empty(), 10, token, direct());

        assertEquals(3, sink.recordCount(), "empty fetch must roll into the next fetch while credits remain");
        // After delivering the 3 records the streamer still holds 7 credits, so it issues a 3rd fetch that
        // parks against the fake submitter's "no more queued" never-completing future — that's the live-tail
        // posture. The "rolled past empty" property we care about is that at least 2 fetches happened.
        assertTrue(submitter.fetchCallCount() >= 2,
            "streamer must roll past empty into the next fetch; saw " + submitter.fetchCallCount());
    }

    @Test
    void emptyFetchDoesNotLoopWhenCreditsExhausted() {
        // If we've already delivered all our credit and the broker returns an empty page, we must not
        // hold the broker open. A paused stream consumes broker resources only via at most one
        // in-flight fetch.
        submitter.queueFetch(records(0, 5));

        WsStreamer.start(sink, submitter, MAPPER, "t", 0, 0L, OptionalInt.empty(), 5, token, direct());

        assertEquals(5, sink.recordCount());
        // No further fetches must be issued without a grant.
        assertEquals(1, submitter.fetchCallCount(), "paused stream must not issue speculative fetches");
    }

    // ----- partition-level error -----

    @Test
    void partitionErrorTerminatesStreamWithErrorFrame() {
        // Broker reported a partition-level error (e.g. UNKNOWN_TOPIC_OR_PARTITION). The streamer
        // must send a single error frame and close. The error must carry the broker's error name so the
        // client gets a meaningful errorCode.
        submitter.queueFetchError(Errors.UNKNOWN_TOPIC_OR_PARTITION, "no such partition");

        WsStreamer.start(sink, submitter, MAPPER, "t", 0, 0L, OptionalInt.empty(), 10, token, direct());

        assertEquals(0, sink.recordCount(), "no records delivered on partition error");
        assertEquals(1, sink.errorCount(), "exactly one error frame must be sent");
        JsonNode err = sink.errorAt(0);
        assertEquals("UNKNOWN_TOPIC_OR_PARTITION", err.get("errorCode").asText());
        assertEquals("no such partition", err.get("errorMessage").asText());
        assertTrue(sink.closed.get(), "stream must close after a partition-level error");
    }

    @Test
    void submitterFailureTerminatesStreamWithInternalError() {
        // The submitter future completed exceptionally — broker pipeline failure. Translate to an
        // INTERNAL error frame, close the stream, and release the limiter slot.
        submitter.queueFetchFailure(new RuntimeException("broker went away"));

        WsStreamer.start(sink, submitter, MAPPER, "t", 0, 0L, OptionalInt.empty(), 10, token, direct());

        assertEquals(0, sink.recordCount());
        assertEquals(1, sink.errorCount());
        assertEquals("INTERNAL", sink.errorAt(0).get("errorCode").asText());
        assertTrue(sink.closed.get());
        assertEquals(0, limiter.inUse(), "limiter slot must be released on terminal error");
    }

    // ----- record framing -----

    @Test
    void recordFrameCarriesAllFieldsTaggedAsRecord() {
        // The frame must declare type=record so a client can distinguish records from error frames on
        // the wire. We don't want clients sniffing by field presence — explicit discriminator only.
        submitter.queueFetch(Collections.singletonList(
            new FetchResponseFormatter.FetchedRecord(42L, "k".getBytes(), "v".getBytes(), "application/json", 1700000000000L)));

        WsStreamer.start(sink, submitter, MAPPER, "topicX", 7, 42L, OptionalInt.empty(), 1, token, direct());

        assertEquals(1, sink.recordCount());
        JsonNode r = sink.recordAt(0);
        assertEquals("record", r.get("type").asText(), "record frames must be tagged type=record");
        assertEquals(42L, r.get("offset").asLong());
        assertEquals(1700000000000L, r.get("timestamp").asLong());
        assertNotNull(r.get("key"), "record must include key field");
        assertNotNull(r.get("value"), "record must include value field");
    }

    // ----- lifecycle -----

    @Test
    void closeReleasesLimiterSlotAndClosesSink() {
        WsStreamer streamer =
            WsStreamer.start(sink, submitter, MAPPER, "t", 0, 0L, OptionalInt.empty(), 0, token, direct());
        assertEquals(1, limiter.inUse());

        streamer.close();

        assertEquals(0, limiter.inUse(), "close must release the limiter slot");
        assertTrue(sink.closed.get(), "close must close the underlying sink");
    }

    @Test
    void closeIsIdempotent() {
        WsStreamer streamer =
            WsStreamer.start(sink, submitter, MAPPER, "t", 0, 0L, OptionalInt.empty(), 0, token, direct());
        streamer.close();
        streamer.close();
        streamer.close();
        assertEquals(0, limiter.inUse(), "redundant close calls must not over-release the limiter");
    }

    @Test
    void grantAfterCloseIsIgnored() {
        // After the client side terminates (or after a broker error closes the stream), any grant that
        // arrived in flight must be dropped silently rather than triggering a fetch on a torn-down
        // connection.
        WsStreamer streamer =
            WsStreamer.start(sink, submitter, MAPPER, "t", 0, 0L, OptionalInt.empty(), 0, token, direct());
        streamer.close();
        submitter.queueFetch(records(0, 5));

        streamer.grantCredits(5);

        assertEquals(0, sink.recordCount(), "no delivery after close");
        assertEquals(0, submitter.fetchCallCount(), "no fetch must be issued after close");
    }

    // ----- concurrency -----

    @Test
    void concurrentGrantsAndFetchesPreserveCreditAccounting() throws InterruptedException {
        // Race a real executor: enqueue many small fetch results and bombard the streamer with parallel
        // grants. Total delivered records must equal min(total queued, total granted). No over-delivery
        // is the load-bearing invariant — the test would fail if a credit-decrement race let a slot get
        // consumed twice.
        submitter.queueFetch(records(0, 200));
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            WsStreamer streamer = WsStreamer.start(sink, submitter, MAPPER, "t", 0, 0L, OptionalInt.empty(),
                0, token, pool);
            // 100 parallel grants of 1.
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                futures.add(CompletableFuture.runAsync(() -> streamer.grantCredits(1), pool));
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            // Wait for delivery to settle.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (sink.recordCount() < 100 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(100, sink.recordCount(), "exactly 100 records delivered for 100 grants");
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    // ----- helpers -----

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

    private static java.util.concurrent.Executor direct() {
        return Runnable::run;
    }

    private static final class FakeSubmitter implements RequestSubmitter {
        private final java.util.Deque<java.util.function.Supplier<CompletableFuture<FetchResult>>> queue =
            new java.util.ArrayDeque<>();
        private final AtomicInteger fetchCalls = new AtomicInteger();

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

        void queueFetchFailure(Throwable t) {
            queue.add(() -> CompletableFuture.failedFuture(t));
        }

        int fetchCallCount() {
            return fetchCalls.get();
        }

        @Override
        public CompletableFuture<ProduceResult> submitProduce(ProduceRequestParser.ProduceCommand command) {
            throw new UnsupportedOperationException("produce not used in WsStreamer tests");
        }

        @Override
        public CompletableFuture<FetchResult> submitFetch(FetchRequestParser.FetchCommand command) {
            fetchCalls.incrementAndGet();
            java.util.function.Supplier<CompletableFuture<FetchResult>> next = queue.poll();
            if (next == null) {
                // No more queued responses: return a future that never completes, mirroring a real broker
                // that holds an idle fetch open for its max-wait window. If we returned an empty page here
                // instead, the streamer would correctly enter live-tail mode (refetch on empty while credit
                // remains) and recurse forever — the test must let live-tail park naturally.
                return new CompletableFuture<>();
            }
            return next.get();
        }
    }

    private static final class CapturingSink implements WsStreamer.FrameSink {
        private final ConcurrentLinkedQueue<JsonNode> records = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<JsonNode> errors = new ConcurrentLinkedQueue<>();
        final AtomicBoolean closed = new AtomicBoolean();

        @Override
        public void sendText(String text) {
            try {
                JsonNode node = MAPPER.readTree(text);
                if (node.has("type") && "error".equals(node.get("type").asText())) {
                    errors.add(node);
                } else {
                    records.add(node);
                }
            } catch (Exception e) {
                throw new RuntimeException("test sink failed to parse frame: " + text, e);
            }
        }

        @Override
        public void close() {
            closed.set(true);
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

        JsonNode recordAt(int i) {
            return new ArrayList<>(records).get(i);
        }

        JsonNode errorAt(int i) {
            return new ArrayList<>(errors).get(i);
        }

        long recordOffsetAt(int i) {
            return recordAt(i).get("offset").asLong();
        }
    }
}
