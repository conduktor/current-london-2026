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
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
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

    // ----- broker fetch-quota throttle -----

    @Test
    void throttledFetchDoesNotImmediatelySubmitNextFetch() {
        // PROMPT.md AC3 extends to streaming: when the broker returns throttleTimeMs > 0 (consumer fetch
        // quota exhausted), the streamer must back off. Our synthetic RequestChannel.Request has no socket
        // for KafkaApis.requestHelper.throttle to mute, so throttleTimeMs is the only signal we get — if we
        // ignored it the subscription would tight-loop the broker under quota pressure.
        //
        // A large throttle (10s) ensures the delayed wake-up cannot fire during this synchronous assertion.
        submitter.queueFetchWithThrottle(records(0, 0), 10_000L);

        WsStreamer.start(sink, submitter, MAPPER, "t", 0, 0L, OptionalInt.empty(), 10, token, direct());

        assertEquals(1, submitter.fetchCallCount(),
            "throttled fetch must not be immediately followed by another submission");
        assertEquals(0, sink.recordCount(), "no records to deliver on an empty throttled response");
        assertFalse(sink.closed.get(), "stream stays open while throttled — it isn't an error");
    }

    @Test
    void throttledFetchDeliversBufferedRecordsButDefersNextFetch() {
        // Throttle does not block the drain — records the broker did manage to return must still flow up
        // to the credit budget. Only the *next fetch* is gated.
        submitter.queueFetchWithThrottle(records(0, 3), 10_000L);

        WsStreamer.start(sink, submitter, MAPPER, "t", 0, 0L, OptionalInt.empty(), 10, token, direct());

        assertEquals(3, sink.recordCount(), "records returned alongside throttle must still be delivered");
        assertEquals(1, submitter.fetchCallCount(),
            "throttle window must defer the live-tail next fetch even when credits remain");
    }

    @Test
    void fetchResumesAfterThrottleDeadlinePasses() throws InterruptedException {
        // End-to-end of the throttle cycle: first fetch reports a short throttle, second fetch (after the
        // delay) returns records and drains. Tight bound — 60ms throttle, 800ms wait — keeps the test
        // fast while leaving ample headroom for scheduler jitter on a busy CI runner.
        submitter.queueFetchWithThrottle(records(0, 0), 60L);
        submitter.queueFetch(records(0, 2));

        WsStreamer.start(sink, submitter, MAPPER, "t", 0, 0L, OptionalInt.empty(), 10, token, direct());

        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(800L);
        while (submitter.fetchCallCount() < 2 && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        // Records may or may not have been delivered yet by the time the second fetch lands — the second
        // fetch's handleFetchResult schedules a drain that the direct executor runs synchronously. So once
        // fetchCallCount==2 we can also assert the records flowed through.
        assertTrue(submitter.fetchCallCount() >= 2,
            "next fetch must resume after the throttle deadline; saw " + submitter.fetchCallCount());
        assertEquals(2, sink.recordCount(), "records returned after the throttle must be delivered");
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

    @Test
    void submitterFailureDoesNotEchoThrowableMessageInErrorFrame() {
        // Information-disclosure invariant: the WS error frame's errorMessage MUST be sanitised to the
        // empty string regardless of what the upstream throwable's getMessage() returned. The path we are
        // pinning is failFetch → trySendErrorFrame("INTERNAL", null), where the null message becomes "".
        // Stock JDK NPE text like 'Cannot invoke "X.y()" because "z" is null' would leak broker class and
        // field names directly to clients if we ever regressed and passed throwable.getMessage() through.
        String sensitive =
            "Cannot invoke \"BrokerSession.getReplicaQuotaManager()\" because the return value of "
                + "\"BrokerServer.session()\" is null";
        submitter.queueFetchFailure(new NullPointerException(sensitive));

        WsStreamer.start(sink, submitter, MAPPER, "t", 0, 0L, OptionalInt.empty(), 10, token, direct());

        assertEquals(1, sink.errorCount(), "exactly one error frame on terminal failure");
        JsonNode err = sink.errorAt(0);
        assertEquals("INTERNAL", err.get("errorCode").asText());
        assertEquals("", err.get("errorMessage").asText(),
            "errorMessage must be sanitised to empty — never echo throwable.getMessage()");
        String emitted = err.toString();
        assertFalse(emitted.contains("BrokerSession"),
            "sanitised error frame must not leak any portion of the throwable message; saw " + emitted);
        assertFalse(emitted.contains("ReplicaQuotaManager"),
            "sanitised error frame must not leak any portion of the throwable message; saw " + emitted);
    }

    @Test
    void submitterCompletionExceptionUnwrappedButStillSanitisedInErrorFrame() {
        // CompletableFuture wraps the original cause in CompletionException. failFetch unwraps the cause so
        // the SERVER-SIDE log captures the actual broker exception, but the WIRE-SIDE error frame must stay
        // sanitised. Even with the unwrap, the cause's getMessage() must not surface to the client.
        NullPointerException cause = new NullPointerException("internal: ReplicaManager.fetchMessages() == null");
        CompletionException wrapped = new CompletionException(cause);
        submitter.queueFetchFailure(wrapped);

        WsStreamer.start(sink, submitter, MAPPER, "t", 0, 0L, OptionalInt.empty(), 10, token, direct());

        assertEquals(1, sink.errorCount());
        JsonNode err = sink.errorAt(0);
        assertEquals("INTERNAL", err.get("errorCode").asText());
        assertEquals("", err.get("errorMessage").asText(),
            "errorMessage must remain empty even after CompletionException unwrap");
        assertFalse(err.toString().contains("ReplicaManager"),
            "unwrap must not change wire-side sanitisation; saw " + err);
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

    // ----- executor rejection (Jetty pool saturated / shut down) -----

    @Test
    void executorRejectionOnInitialDispatchTearsDownStreamCleanly() {
        // The synchronous catch path inside scheduleDrain. Models a Jetty thread pool that's already shut
        // down when WsStreamer.start runs (or saturated past its queue): the very first httpExecutor.execute
        // throws RejectedExecutionException, the streamer logs it, clears drainScheduled, and tears down.
        // The limiter slot MUST be released — without this, every saturated upgrade would leak one slot.
        Executor rejecting = task -> {
            throw new RejectedExecutionException("test reject");
        };

        WsStreamer streamer =
            WsStreamer.start(sink, submitter, MAPPER, "t", 0, 0L, OptionalInt.empty(), 5, token, rejecting);

        assertTrue(streamer.isClosed(), "executor rejection on initial dispatch must close the stream");
        assertTrue(sink.closed.get(), "executor rejection must close the sink");
        assertEquals(0, limiter.inUse(), "executor rejection must release the limiter slot");
        assertEquals(0, submitter.fetchCallCount(), "no fetch may run when the initial drain never dispatched");
    }

    @Test
    void executorRejectionOnFetchDispatchSurfacesInternalAndTearsDown() {
        // The .exceptionally terminal-handler path inside maybeKickFetch. Models a Jetty thread pool that
        // accepts the initial drain but refuses subsequent dispatches — exactly the saturated-mid-stream
        // failure mode that motivated commit 3f6cbc4ade.
        //
        // Sequence:
        //   1. WsStreamer.start → scheduleDrain → httpExecutor.execute(drainAndMaybeFetch). [call #1: allowed]
        //   2. drainAndMaybeFetch runs inline, drainBufferWhileCredited returns (buffer empty),
        //      maybeKickFetch fetches via submitter (already-completed future).
        //   3. .whenCompleteAsync(handleFetchResult, httpExecutor) attempts to dispatch the callback
        //      onto httpExecutor since the future is already complete. [call #2: rejected]
        //   4. The dependent stage completes exceptionally with RejectedExecutionException; the
        //      registered .exceptionally lambda fires synchronously on the calling thread (since the
        //      stage is already complete-exceptionally).
        //   5. handleSchedulingFailure: clears fetchInFlight, emits sanitised INTERNAL error frame,
        //      calls close() — which releases the limiter token and closes the sink.
        //
        // Without the .exceptionally landed in commit 3f6cbc4ade, the RejectedExecutionException would
        // land on an unobserved future and the stream would wedge with fetchInFlight=true forever,
        // leaking the limiter slot until JVM shutdown.
        submitter.queueFetch(records(0, 3));
        Executor onceThenReject = new Executor() {
            private final AtomicInteger remaining = new AtomicInteger(1);
            @Override
            public void execute(Runnable r) {
                if (remaining.getAndDecrement() > 0) {
                    r.run();
                } else {
                    throw new RejectedExecutionException("test reject after first dispatch");
                }
            }
        };

        WsStreamer streamer = WsStreamer.start(sink, submitter, MAPPER, "t", 0, 0L, OptionalInt.empty(),
            5, token, onceThenReject);

        assertEquals(1, sink.errorCount(), "executor rejection on fetch dispatch must surface an error frame");
        JsonNode err = sink.errorAt(0);
        assertEquals("INTERNAL", err.get("errorCode").asText());
        assertEquals("", err.get("errorMessage").asText(),
            "errorMessage must stay sanitised — never leak the RejectedExecutionException's text");
        assertFalse(err.toString().toLowerCase(java.util.Locale.ROOT).contains("rejected"),
            "sanitised error frame must not echo the rejection message; saw " + err);
        assertTrue(streamer.isClosed(), "stream must close after executor rejection");
        assertTrue(sink.closed.get());
        assertEquals(0, limiter.inUse(), "executor rejection must release the limiter slot");
        // The records the submitter returned were never delivered — the rejection beat the drain.
        assertEquals(0, sink.recordCount(),
            "no records may be delivered when the post-fetch dispatch was rejected");
    }

    // ----- concurrency -----

    @Test
    void concurrentGrantsAndFetchesPreserveCreditAccounting() throws InterruptedException {
        // Race a real executor: enqueue many small fetch results and bombard the streamer with parallel
        // grants. Total delivered records must equal min(total queued, total granted). No over-delivery
        // is the load-bearing invariant — the test would fail if a credit-decrement race let a slot get
        // consumed twice.
        //
        // Pre-queue 200 records but issue only 100 grants. A credit-decrement race that consumed one slot
        // twice manifests as a 101st delivery from offset 100+; asserting the offset window IS in {0..99}
        // makes that race observable directly, where a count-only check could be subverted by polling-exit
        // timing on a fast CI host (the loop exits the moment recordCount reaches 100 — any late
        // over-delivery lands after the assert and would otherwise go unobserved).
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
            // Wait for delivery to settle to the expected count, then add a quiescence window so any
            // race-induced over-delivery has time to land before we assert. Without the settle, the
            // polling loop exits at exactly 100 and the over-delivery arrives a few ms later, AFTER the
            // assertion has already returned passing — the bug would be silent.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (sink.recordCount() < 100 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            Thread.sleep(200);
            assertEquals(100, sink.recordCount(), "exactly 100 records delivered for 100 grants");
            // Strong invariant: with 200 records pre-queued but only 100 grants issued, any over-delivery
            // would necessarily pull from offset 100+. Asserting offset uniqueness AND range catches both
            // credit-decrement races (extra record from offset >=100) and duplicate-delivery races (same
            // offset twice within {0..99}).
            java.util.Set<Long> seen = new java.util.HashSet<>();
            for (int i = 0; i < sink.recordCount(); i++) {
                long off = sink.recordOffsetAt(i);
                assertTrue(off < 100L,
                    "delivered offset " + off + " is beyond the 100 credits granted — credit-decrement race");
                assertTrue(seen.add(off),
                    "offset " + off + " was delivered more than once — credit double-spend");
            }
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void concurrentGrantsAcrossFetchBoundariesDoNotDuplicateDelivery() throws InterruptedException {
        // Race window we are pinning: in handleFetchResult, the streamer used to clear fetchInFlight BEFORE
        // staging records into the buffer. A credit-grant arriving in that window would run drainAndMaybeFetch
        // → maybeKickFetch, observe (buffer empty, fetchInFlight false, currentOffset unchanged), win the CAS,
        // and submit a fresh fetch at the SAME offset the just-completed fetch covered. The duplicate fetch
        // then returns the same records, both batches drain through the buffer, and the client sees every
        // offset twice.
        //
        // We can't deterministically force the race, so we maximise the probability instead: many small
        // fetches (50 batches × 5 records = 50 race points), four pool threads to ensure handleFetchResult
        // and drainAndMaybeFetch can genuinely overlap, and one credit-grant per record fired in parallel
        // so a fresh grant is always racing each fetch completion.
        //
        // With the bug, this reliably observed duplicates within a handful of runs. With the fix (stage
        // records and stamp throttle BEFORE clearing fetchInFlight) it must deliver every offset exactly once.
        final int batches = 50;
        final int batchSize = 5;
        for (int b = 0; b < batches; b++) {
            submitter.queueFetch(records((long) b * batchSize, batchSize));
        }

        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            WsStreamer streamer = WsStreamer.start(sink, submitter, MAPPER, "t", 0, 0L,
                OptionalInt.empty(), 0, token, pool);
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (int i = 0; i < batches * batchSize; i++) {
                futures.add(CompletableFuture.runAsync(() -> streamer.grantCredits(1), pool));
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (sink.recordCount() < batches * batchSize && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(batches * batchSize, sink.recordCount(),
                "every queued record must be delivered exactly once across all fetch boundaries");

            // Stronger invariant: every delivered offset must be unique. A duplicate delivery would show up
            // here as the same offset appearing twice. This is the load-bearing assertion — a count-only
            // check can pass even when the streamer over-delivers if some grants get parked.
            java.util.Set<Long> seen = new java.util.HashSet<>();
            for (int i = 0; i < sink.recordCount(); i++) {
                long off = sink.recordOffsetAt(i);
                assertTrue(seen.add(off),
                    "offset " + off + " was delivered more than once — fetch-boundary race re-opened");
            }
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

        void queueFetchWithThrottle(List<FetchResponseFormatter.FetchedRecord> records, long throttleMs) {
            queue.add(() -> {
                FetchResponseFormatter.PartitionFetch view = new FetchResponseFormatter.PartitionFetch(
                    0, Errors.NONE, null, 0L, 0L, 0L, records);
                return CompletableFuture.completedFuture(new FetchResult(view, throttleMs));
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
