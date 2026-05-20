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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.common.protocol.Errors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Drives a credit-gated WebSocket subscription on top of the existing single-partition fetch path.
 *
 * <p>The WebSocket counterpart to {@link SseStreamer}. The flow shape is the same — recursive long-poll
 * against {@link RequestSubmitter#submitFetch} — but the delivery contract is different: the SSE path
 * is implicit-backpressure (next fetch isn't issued until the previous write completed), while the
 * WebSocket path is explicit-backpressure ({@code initialCredits} caps the prefix of records delivered;
 * client must send {@code {"type":"flow","credits":N}} to release more).
 *
 * <p>PROMPT.md AC5 is the contract:
 * <ul>
 *   <li>subscribe with N initial credits → exactly N messages delivered → pause;</li>
 *   <li>flow with M more credits → exactly M more → pause again;</li>
 *   <li>zero credits → no delivery (the streamer must not even speculate by issuing a fetch);</li>
 *   <li>a fast producer cannot cause unbounded server buffering — the only buffer the streamer holds is
 *       the carry-over from one fetch when credits run out mid-batch.</li>
 * </ul>
 *
 * <p>Threading model. All state transitions happen on the supplied {@link Executor} so the streamer can be
 * called safely from any thread (Jetty's onText for grants, the broker's request-handler thread on fetch
 * completion). The dispatch is single-flight: {@link #scheduleDrain()} only enqueues a new drain pass if
 * none is already scheduled. The inner drain is non-reentrant — it pulls from {@code buffer} only after
 * winning a credit via CAS, which guarantees AC5's "exactly N" property under concurrent grants because
 * every delivered record corresponds to exactly one successful {@code credits.compareAndSet(c, c-1)}.
 *
 * <p>This class deliberately does not depend on Jetty types. The endpoint code passes a {@link FrameSink}
 * adapter that wraps the real {@code Session}; the streamer is fully unit-testable without bringing up an
 * HTTP server.
 */
public final class WsStreamer {

    private static final Logger LOG = LoggerFactory.getLogger(WsStreamer.class);

    private final FrameSink sink;
    private final RequestSubmitter submitter;
    private final ObjectMapper mapper;
    private final String topic;
    private final int partition;
    private final OptionalInt maxBytes;
    private final WsStreamLimiter.Token limiterToken;
    private final Executor httpExecutor;

    private final AtomicInteger credits = new AtomicInteger();
    // The carry-over buffer. A fetch can return more records than the remaining credit budget — those
    // records sit here until the client grants more. We do NOT speculate beyond one fetch worth of records,
    // which is what bounds memory: a misbehaving producer can fill at most max(broker fetch size) bytes per
    // subscription beyond what the client has asked for.
    private final Queue<FetchResponseFormatter.FetchedRecord> buffer = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean drainScheduled = new AtomicBoolean();
    // Mutual-exclusion gate around the body of drainAndMaybeFetch. The executor is potentially
    // multi-threaded (Jetty's server thread pool, which is shared with the produce/fetch path), so
    // simply resetting drainScheduled before doing the work would let a concurrent grant schedule a
    // second drain that races us — both drains would call drainBufferWhileCredited and step on
    // currentOffset, producing out-of-order frames or a stale next-fetch offset.
    private final AtomicBoolean draining = new AtomicBoolean();
    private final AtomicBoolean fetchInFlight = new AtomicBoolean();
    // Monotonic deadline (System.nanoTime() reading) before which maybeKickFetch must NOT submit a new fetch.
    // Set when the broker returns a positive throttleTimeMs; cleared once we cross the deadline. The synthetic
    // RequestChannel request has no socket for KafkaApis.requestHelper.throttle to mute, so this field is the
    // load-bearing backpressure signal for streaming fetch loops — without it a quota-exhausted subscription
    // tight-loops. nanoTime rather than currentTimeMillis so a wall-clock backstep (NTP adjust, suspend/resume)
    // can't extend the throttle window beyond what the broker asked for. Sentinel 0L means "no throttle stamped"
    // — the stored value is the *sum* nanoTime() + throttleNanos, which can land at exactly 0L only via signed
    // overflow wrap (probability ≈ 1 in 2^64 per stamping); the false-negative is negligible. Per the
    // System.nanoTime() Javadoc, nanoTime values are signed and can be negative, and an additive computation
    // like (nanoTime() + positive-delay) can wrap past Long.MAX_VALUE to a numerically smaller value. The
    // deadline is therefore NOT guaranteed to be numerically greater than the read it came from; what IS
    // guaranteed is that the JDK-documented overflow-safe subtraction idiom (deadline - now > 0) wraps
    // consistently in two's-complement and gives the correct sign for any elapsed interval < 2^63 ns (~292
    // years). The read site at maybeKickFetch() uses exactly that idiom — do NOT replace it with a direct
    // (now < deadline) absolute compare, which is unsafe under overflow.
    private final AtomicLong throttleUntilNanos = new AtomicLong(0L);

    private volatile long currentOffset;

    private WsStreamer(FrameSink sink, RequestSubmitter submitter, ObjectMapper mapper,
                       String topic, int partition, long startOffset, OptionalInt maxBytes,
                       int initialCredits, WsStreamLimiter.Token limiterToken, Executor httpExecutor) {
        this.sink = Objects.requireNonNull(sink, "sink must not be null");
        this.submitter = Objects.requireNonNull(submitter, "submitter must not be null");
        this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
        this.partition = partition;
        this.currentOffset = startOffset;
        this.maxBytes = Objects.requireNonNull(maxBytes, "maxBytes must not be null");
        if (initialCredits < 0) {
            throw new IllegalArgumentException("initialCredits must be non-negative, got " + initialCredits);
        }
        this.credits.set(initialCredits);
        this.limiterToken = Objects.requireNonNull(limiterToken, "limiterToken must not be null — caller must acquire before start()");
        this.httpExecutor = Objects.requireNonNull(httpExecutor, "httpExecutor must not be null");
    }

    /**
     * Construct a streamer and kick off the first drain pass. The caller (the WebSocket endpoint) is
     * responsible for acquiring a limiter token first and passing it in; the streamer takes ownership and
     * releases it on {@link #close()}.
     *
     * <p>The first drain pass runs synchronously on the executor if it's a direct executor (tests); on a real
     * thread pool it returns immediately and delivery happens asynchronously. Either way, AC5 is preserved
     * because the drain consults {@code credits} and only delivers records it has the budget for.
     */
    public static WsStreamer start(FrameSink sink, RequestSubmitter submitter, ObjectMapper mapper,
                                   String topic, int partition, long startOffset, OptionalInt maxBytes,
                                   int initialCredits, WsStreamLimiter.Token limiterToken,
                                   Executor httpExecutor) {
        WsStreamer streamer = new WsStreamer(sink, submitter, mapper, topic, partition, startOffset, maxBytes,
            initialCredits, limiterToken, httpExecutor);
        streamer.scheduleDrain();
        return streamer;
    }

    /**
     * Add {@code n} credits to the in-flight budget. Caller (the endpoint's onText handler) must have already
     * validated that {@code n > 0} via {@link WsSubscribeMessageParser}; the streamer guards against the
     * after-close race only.
     *
     * <p>The running total is saturated at {@link Integer#MAX_VALUE} rather than allowed to wrap. A naive
     * {@code credits.addAndGet(n)} lets two consecutive {@code Integer.MAX_VALUE} grants overflow to
     * negative — at which point {@link #drainBufferWhileCredited}'s {@code c <= 0} short-circuit wedges the
     * stream silently and the client has self-DoS'd its own subscription with no error frame. Saturating
     * with a CAS loop turns the pathological grant into a no-op at the ceiling instead of a stuck stream.
     */
    public void grantCredits(int n) {
        if (closed.get()) {
            // Grant arrived after the stream tore down. Drop silently — the client will see the close frame.
            return;
        }
        if (n <= 0) {
            throw new IllegalArgumentException("credits to grant must be positive, got " + n);
        }
        while (true) {
            int current = credits.get();
            // Saturating add: if current + n would overflow, clamp to MAX_VALUE. The (long) cast prevents
            // the overflow itself; we then narrow back to int after clamping.
            long sum = (long) current + (long) n;
            int next = sum > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) sum;
            if (next == current) {
                // Already at the ceiling — nothing to add and nothing to drain.
                return;
            }
            if (credits.compareAndSet(current, next)) {
                break;
            }
        }
        scheduleDrain();
    }

    /** RFC 6455 status code for "Internal Server Error" — used by every streamer-initiated failure path. */
    static final int WS_SERVER_ERROR = 1011;

    /**
     * Tear the stream down. Idempotent: redundant calls (e.g. from a close handler that also fires after an
     * error frame) leave the limiter count consistent because {@link WsStreamLimiter.Token#close()} is itself
     * idempotent.
     */
    public void close() {
        if (closed.compareAndSet(false, true)) {
            limiterToken.close();
            // Drop carry-over record refs eagerly. The streamer can stay alive past close while an
            // in-flight whenCompleteAsync chain settles (up to fetch.max.wait.ms); without this, every
            // staged FetchedRecord (key+value byte arrays) is pinned for that window. Concurrent
            // poll/offer racing this clear is safe: ConcurrentLinkedQueue.clear is non-blocking, racing
            // drains see null polls and exit, and handleFetchResult's offer loop is fenced by a
            // post-stage closed-check that mirrors this clear (so an offer that lands after this
            // clear runs is dropped at the gate, not pinned).
            buffer.clear();
            try {
                sink.close();
            } catch (RuntimeException e) {
                LOG.debug("FrameSink.close() failed for {}/{}: {}", topic, partition, e.toString());
            }
        }
    }

    /**
     * Tear the stream down with an explicit RFC 6455 status code (typically {@link #WS_SERVER_ERROR}). Used by
     * every internal failure path — broker rejection, executor refusal, send-fail, fetch-dispatch fail — so the
     * peer's close-handler can distinguish "the server is unhappy, back off" from a clean
     * {@code 1000 Normal Closure}. The error envelope on the wire carries the precise reason; the close code is
     * the load-bearing signal for clients that handle reconnects.
     */
    private void closeOnFailure(int statusCode, String reason) {
        if (closed.compareAndSet(false, true)) {
            limiterToken.close();
            // Same rationale as close(): drop staged record refs so they aren't pinned for the lifetime
            // of any in-flight whenCompleteAsync chain. handleFetchResult's post-stage closed-check
            // mirrors this clear to fence late-arriving offers.
            buffer.clear();
            try {
                sink.close(statusCode, reason);
            } catch (RuntimeException e) {
                LOG.debug("FrameSink.close({}) failed for {}/{}: {}", statusCode, topic, partition, e.toString());
            }
        }
    }

    /** Visible for the endpoint code that wires this into Jetty. */
    public boolean isClosed() {
        return closed.get();
    }

    private void scheduleDrain() {
        // Single-flight: if a drain is already scheduled, this call is a no-op. The scheduled drain will
        // pick up any state changes (new credits, new buffered records) when it runs. Two concurrent
        // grant calls collapse into one drain pass — fine, the drain is idempotent w.r.t. the underlying
        // state.
        if (drainScheduled.compareAndSet(false, true)) {
            try {
                httpExecutor.execute(this::drainAndMaybeFetch);
            } catch (RuntimeException e) {
                // Executor rejected the task — most likely shut down. Reset the latch so a future
                // grant can try again; surface as a stream-terminal failure.
                drainScheduled.set(false);
                LOG.warn("WS executor rejected drain dispatch for {}/{}", topic, partition, e);
                closeOnFailure(WS_SERVER_ERROR, "executor rejected drain");
            }
        }
    }

    private void drainAndMaybeFetch() {
        if (!draining.compareAndSet(false, true)) {
            // Another worker is already inside the drain body. drainScheduled was just CAS'd to
            // true by the scheduleDrain that queued *this* invocation; whether the running drain
            // re-iterates and picks our work up OR misses it depends on the timing. The
            // post-finally recovery in the running drain (below) is what makes the lost-wakeup
            // window safe — we can return silently here.
            return;
        }
        try {
            // Loop until no new drain requests are pending. Clearing drainScheduled INSIDE the
            // loop (and re-checking it at the bottom) lets a scheduleDrain that arrives during
            // the work re-arm us via the while-check.
            do {
                drainScheduled.set(false);
                if (closed.get()) {
                    return;
                }
                if (!drainBufferWhileCredited()) {
                    // drainBuffer signalled a send failure; stream is already closed.
                    return;
                }
                if (closed.get()) {
                    return;
                }
                maybeKickFetch();
            } while (drainScheduled.get());
        } finally {
            // Release the mutex, then close the lost-wakeup window. Sequence to defend against:
            //   1. We observe drainScheduled=false in the while-check above and exit the loop.
            //   2. A scheduleDrain races in here: CAS drainScheduled false→true succeeds and
            //      queues Drain B onto the executor.
            //   3. Drain B's thread runs before we reach this finally, fails its draining CAS,
            //      and returns silently.
            //   4. We now reach this finally and clear draining. Final state: drainScheduled=true,
            //      draining=false, NO task queued — future scheduleDrain calls would no-op on
            //      their CAS (already true) and the subscription stalls despite available work.
            // Recovery: after releasing draining, re-check drainScheduled. If it's still true,
            // a grant landed in the race window — directly dispatch a fresh task. We bypass
            // scheduleDrain because its CAS would refuse (flag is true). The newly-dispatched
            // task is guaranteed to win the draining CAS because we just released it; any other
            // racing task that loses the CAS is harmless because it leaves drainScheduled set
            // and our recovery (or a later one) will pick the work up.
            draining.set(false);
            if (drainScheduled.get() && !closed.get()) {
                try {
                    httpExecutor.execute(this::drainAndMaybeFetch);
                } catch (RuntimeException e) {
                    LOG.warn("WS executor rejected drain recovery for {}/{}", topic, partition, e);
                    closeOnFailure(WS_SERVER_ERROR, "executor rejected drain recovery");
                }
            }
        }
    }

    /**
     * Drain the carry-over buffer while we hold credit. Returns {@code false} if a send failed and the stream
     * was torn down — caller must not proceed to kick a fetch.
     */
    private boolean drainBufferWhileCredited() {
        while (!buffer.isEmpty()) {
            int c = credits.get();
            if (c <= 0) {
                return true;
            }
            if (!credits.compareAndSet(c, c - 1)) {
                // A concurrent grantCredits CAS raced us on the credit counter. The draining mutex
                // (drainAndMaybeFetch's CAS on `draining`) keeps any other drain body out, so the
                // racing thread is necessarily a grant, not another drain. Re-read and retry.
                continue;
            }
            FetchResponseFormatter.FetchedRecord r = buffer.poll();
            if (r == null) {
                // close()/closeOnFailure() called buffer.clear() between our isEmpty check and the
                // poll — those are the only producers of buffer-emptiness besides our own polling,
                // because the draining mutex serialises all drain bodies. Refund the credit we
                // reserved and exit; the next drain iteration (gated on closed.get()) will observe
                // the close and bail.
                credits.incrementAndGet();
                return true;
            }
            try {
                sendRecord(r);
            } catch (RuntimeException e) {
                LOG.debug("WS send failed for {}/{}: {}", topic, partition, e.toString());
                closeOnFailure(WS_SERVER_ERROR, "send failed");
                return false;
            }
            currentOffset = r.offset() + 1;
        }
        return true;
    }

    /** Issue a fetch only if the buffer is dry, we still have credit, and no other fetch is already running. */
    private void maybeKickFetch() {
        if (!buffer.isEmpty() || credits.get() <= 0) {
            return;
        }
        long deadlineNanos = throttleUntilNanos.get();
        if (deadlineNanos != 0L) {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos > 0) {
                // Broker-side fetch quota is asking us to back off. Re-arm at the deadline so a future drain
                // (this one OR a credit-grant racing during the throttle window) issues the fetch then.
                // scheduleDrain is single-flight, so multiple overlapping delayed wake-ups collapse harmlessly.
                // Floor at 1ms so a sub-millisecond remainder doesn't spin the timer wheel until the
                // deadline crosses zero — one extra ms of throttle is well within the broker's tolerance.
                long remainingMs = Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
                scheduleDrainAfter(remainingMs);
                return;
            }
            // Deadline passed — clear it. CAS guards against a concurrent handleFetchResult that just set a
            // fresher deadline.
            throttleUntilNanos.compareAndSet(deadlineNanos, 0L);
        }
        if (!fetchInFlight.compareAndSet(false, true)) {
            return;
        }
        // Post-CAS re-check. The buffer / throttle reads at the entry of this method ran BEFORE the CAS,
        // so the volatile-CAS-success happens-before fence (which orders reads AFTER the CAS) does NOT
        // make them safe: a concurrent handleFetchResult that staged records and then cleared
        // fetchInFlight in the window between our entry read and our CAS would have us launch a duplicate
        // fetch at the unchanged currentOffset (currentOffset only advances during delivery in
        // drainBufferWhileCredited, not when records are staged). The window is narrow — it requires our
        // thread to be context-switched between the entry read and the CAS while handleFetchResult
        // completes — but real, and an over-delivered offset is the operator-visible defect we promise
        // not to ship. Cheap defence: re-validate post-CAS and re-arm via scheduleDrain instead.
        if (!buffer.isEmpty()) {
            fetchInFlight.set(false);
            scheduleDrain();
            return;
        }
        long throttleDeadlineNanos = throttleUntilNanos.get();
        if (throttleDeadlineNanos != 0L) {
            long remainingNanos = throttleDeadlineNanos - System.nanoTime();
            if (remainingNanos > 0) {
                fetchInFlight.set(false);
                long remainingMs = Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
                scheduleDrainAfter(remainingMs);
                return;
            }
        }
        FetchRequestParser.FetchCommand command =
            new FetchRequestParser.FetchCommand(topic, partition, currentOffset, maxBytes);
        try {
            // .exceptionally captures TWO failure modes the surrounding try/catch cannot see:
            //   (a) RejectedExecutionException from httpExecutor when whenCompleteAsync tries to
            //       dispatch handleFetchResult — the JDK routes that to the dependent future, not
            //       the calling thread, so a saturated or shut-down Jetty pool would otherwise
            //       leave fetchInFlight=true forever and silently wedge the stream.
            //   (b) any RuntimeException thrown inside handleFetchResult itself (sendText through a
            //       half-broken sink, etc.). Without this terminal handler that throwable lands on
            //       an unobserved future and is logged only at shutdown.
            // Returning {@code null} satisfies the FetchResult-typed dependent stage; the value is
            // never observed because the handler is the terminal step in the chain.
            submitter.submitFetch(command)
                .whenCompleteAsync(this::handleFetchResult, httpExecutor)
                .exceptionally(t -> {
                    handleSchedulingFailure(t);
                    return null;
                });
        } catch (RuntimeException e) {
            fetchInFlight.set(false);
            // Log the real throwable server-side; never echo throwable.getMessage() to the client. Stock JDK
            // messages (e.g. NPE "Cannot invoke X.y() because z is null") leak broker class/field names.
            LOG.warn("WS fetch submission failed for {}/{}", topic, partition, e);
            trySendErrorFrame("INTERNAL", null);
            closeOnFailure(WS_SERVER_ERROR, "fetch dispatch failed");
        }
    }

    /**
     * Terminal handler for failures the synchronous try/catch around fetch dispatch cannot observe —
     * executor rejection on the {@code whenCompleteAsync} hand-off, and unexpected throwables from inside
     * {@link #handleFetchResult}. We mirror the catch block's behaviour: clear {@code fetchInFlight} so a
     * future grant cannot find the stream wedged, log the throwable server-side (never echo it to the
     * client — stock JDK messages leak broker internals), surface a sanitised INTERNAL error frame, and
     * close. Returning {@code null} keeps the dependent future shape stable.
     */
    private void handleSchedulingFailure(Throwable throwable) {
        if (closed.get()) {
            return;
        }
        fetchInFlight.set(false);
        LOG.warn("WS fetch dispatch failed for {}/{}", topic, partition, throwable);
        trySendErrorFrame("INTERNAL", null);
        closeOnFailure(WS_SERVER_ERROR, "scheduling failed");
    }

    private void handleFetchResult(RequestSubmitter.FetchResult result, Throwable throwable) {
        // Note: fetchInFlight is cleared LATE — only after records are staged and throttleUntilNanos is stamped.
        // The window between "previous fetch resolved" and "buffer/throttle visible to a concurrent drain"
        // used to admit a duplicate fetch at the unchanged currentOffset, delivering the same offsets twice.
        // See the long comment before fetchInFlight.set(false) below for the full race analysis.
        if (closed.get()) {
            fetchInFlight.set(false);
            return;
        }
        if (throwable != null) {
            // close() runs inside failFetch; once closed=true, drainAndMaybeFetch and maybeKickFetch are both
            // gated. Clearing fetchInFlight is defensive — irrelevant after close, but cheap and tidy.
            fetchInFlight.set(false);
            failFetch(throwable);
            return;
        }
        FetchResponseFormatter.PartitionFetch view = result.partition();
        if (view.error() != Errors.NONE) {
            fetchInFlight.set(false);
            String msg = view.errorMessage() != null ? view.errorMessage() : view.error().message();
            trySendErrorFrame(view.error().name(), msg);
            closeOnFailure(WS_SERVER_ERROR, view.error().name());
            return;
        }
        // Stage records BEFORE clearing fetchInFlight. handleFetchResult runs on the httpExecutor (a
        // multi-threaded Jetty thread pool in production), outside the draining mutex. A concurrent
        // grantCredits → scheduleDrain → drainAndMaybeFetch → maybeKickFetch can race us. The race window
        // we close here:
        //   1. We clear fetchInFlight (OLD ordering).
        //   2. Concurrent maybeKickFetch on another thread reads: buffer empty (records not yet offered),
        //      credits > 0 (grant just landed), throttleUntilNanos = 0 (not yet stamped), fetchInFlight = false.
        //   3. CAS fetchInFlight false→true succeeds, builds a FetchCommand at currentOffset = X (currentOffset
        //      is only advanced in drainBufferWhileCredited as records are *delivered*, not when staged), and
        //      submits a fresh fetch at X.
        //   4. We finally offer records (offsets X, X+1, ...) into the buffer.
        //   5. The duplicate fetch returns records at X, X+1, ... and offers them too.
        //   6. drainBufferWhileCredited drains the buffer in order — same offsets twice.
        // With the new ordering (stage records, stamp throttle, THEN clear fetchInFlight) a racing maybeKickFetch
        // sees one of: fetchInFlight=true (CAS refuses), buffer non-empty (early-return at the first guard),
        // or throttleUntilNanos > nanoTime() (deferred fetch). The AtomicBoolean write provides the necessary
        // happens-before so the buffer and throttle writes are visible to any thread that observes fetchInFlight=false.
        for (FetchResponseFormatter.FetchedRecord r : view.records()) {
            buffer.offer(r);
        }
        // Post-stage close gate. The closed-check at the entry of this method (above) guards entry only.
        // If close() (or closeOnFailure() — same CAS) lands between that entry-check and the offer loop
        // above, close()'s own buffer.clear() already ran BEFORE our offers, so the records we just staged
        // sit pinned in the buffer for the lifetime of the WsStreamer reference (Jetty drops the endpoint
        // shortly after onWebSocketClose, but until then the staged FetchedRecord key+value arrays are
        // live — the whole point of W45-DDD's eager clear). Mirror close()'s clear here so the two-sided
        // gating ("entry-check + post-stage clear") closes the window. Resetting fetchInFlight keeps the
        // book-keeping clean even though no future drain will read it.
        if (closed.get()) {
            buffer.clear();
            fetchInFlight.set(false);
            return;
        }
        long throttleMs = result.throttleTimeMs();
        if (throttleMs > 0) {
            // Stamp the deadline BEFORE clearing fetchInFlight for the same reason. An empty-but-throttled fetch
            // would otherwise leave the deadline invisible to a concurrent maybeKickFetch, which would skip the
            // back-off and tight-loop the broker.
            throttleUntilNanos.set(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(throttleMs));
        }
        // All state visible. Release the in-flight latch; subsequent drains will pick up the staged records
        // subject to credit, and a fresh fetch is gated on the buffer draining + throttle deadline passing.
        fetchInFlight.set(false);
        if (throttleMs > 0) {
            // Arm a wake-up at the deadline so the fetch resumes even if no client grant arrives.
            scheduleDrainAfter(throttleMs);
        }
        scheduleDrain();
    }

    private void scheduleDrainAfter(long delayMs) {
        if (closed.get()) {
            return;
        }
        try {
            // Delivery mechanism: the JDK static delayer fires the timer, then dispatches scheduleDrain
            // via httpExecutor. The .exceptionally handler catches an uncaught throw from inside
            // scheduleDrain itself — the action is already defensive (CAS-gated, closed-checked), so
            // this is a future-proofing safety net, not a routine path.
            // What it does NOT catch: a RejectedExecutionException raised when the delayer eventually
            // submits to a stopped or saturated httpExecutor surfaces inside the JDK delayer's
            // ASYNC_POOL worker (DelayedExecutor.TaskSubmitter.run has no try/catch around the inner
            // execute), NOT through the dependent future — so the throttle-resume drain simply never
            // runs in that race. Bounded impact: fetchInFlight is already false here, so a fresh client
            // grant → maybeKickFetch can still kick a new fetch; if no grant arrives, Jetty's
            // IDLE_TIMEOUT (5 min) fires onWebSocketError → tearDown and releases the limiter slot.
            // Symmetric site: KafkaWebSocketEndpoint#scheduleSubscribeDeadline (Wave 41 axis AAA).
            CompletableFuture.runAsync(this::scheduleDrain,
                CompletableFuture.delayedExecutor(delayMs, TimeUnit.MILLISECONDS, httpExecutor))
                .exceptionally(t -> {
                    handleSchedulingFailure(t);
                    return null;
                });
        } catch (RuntimeException e) {
            LOG.warn("WS delayed-drain dispatch failed for {}/{}", topic, partition, e);
            closeOnFailure(WS_SERVER_ERROR, "delayed-drain dispatch failed");
        }
    }

    private void failFetch(Throwable throwable) {
        // Unwrap CompletionException if present so the client sees the broker's actual cause rather than
        // the future-machinery wrapper.
        Throwable cause = throwable;
        if (throwable instanceof java.util.concurrent.CompletionException && throwable.getCause() != null) {
            cause = throwable.getCause();
        }
        // Log the cause server-side; never echo cause.getMessage() to the client. Per-partition errors with
        // sanitised Kafka text already flow through the view.error() != NONE branch in handleFetchResult; this
        // failFetch path is reached only on unexpected throwables where the message can leak internals.
        LOG.warn("WS fetch submission failed for {}/{} at offset {}", topic, partition, currentOffset, cause);
        trySendErrorFrame("INTERNAL", null);
        closeOnFailure(WS_SERVER_ERROR, "broker fetch failed");
    }

    private void sendRecord(FetchResponseFormatter.FetchedRecord r) {
        ObjectNode body = mapper.createObjectNode();
        // Explicit discriminator — clients distinguish records from error frames by the 'type' field, not by
        // field presence. Prevents a future error-envelope evolution from looking like a record.
        body.put("type", "record");
        body.put("offset", r.offset());
        body.put("timestamp", r.timestamp());
        body.set("key", r.key() == null
            ? mapper.nullNode()
            : ValueSerializer.encode(r.key(), null).toJson(mapper));
        body.set("value", ValueSerializer.encode(r.value(), r.contentType()).toJson(mapper));
        try {
            sink.sendText(mapper.writeValueAsString(body));
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            // Encoding our own payload should never fail; if it does, the stream is unrecoverable.
            throw new RuntimeException("record JSON encoding failed at " + topic + "/" + partition, e);
        }
    }

    private void trySendErrorFrame(String code, String message) {
        try {
            ObjectNode body = mapper.createObjectNode();
            body.put("type", "error");
            body.put("errorCode", code);
            body.put("errorMessage", message == null ? "" : message);
            sink.sendText(mapper.writeValueAsString(body));
        } catch (Exception ignored) {
            // Client may already be gone. We're closing the stream anyway.
        }
    }

    /**
     * The streamer's view of the WebSocket session. The endpoint provides a Jetty-backed implementation; tests
     * provide a capturing implementation. Keeping the streamer free of Jetty types lets us unit-test the credit
     * accounting without bringing up an HTTP server.
     */
    public interface FrameSink {
        /** Write a single text frame. The frame is already a complete JSON string. */
        void sendText(String text);

        /** Close the underlying session. Should be tolerant of being called multiple times. */
        void close();

        /**
         * Close the underlying session with an explicit RFC 6455 status code and reason. Default implementation
         * falls back to {@link #close()}; production sinks should override to convey the status code to the peer.
         * Used by the streamer's failure paths to signal {@code 1011 Server Error} for broker-initiated faults,
         * which clients (and proxies) treat differently from a clean {@code 1000 Normal Closure} — retry/back-off
         * heuristics on the peer side depend on the close code more than on the error envelope payload.
         */
        default void close(int statusCode, String reason) {
            close();
        }

        /** Whether the session is still open. The streamer consults this only as a hint. */
        boolean isOpen();
    }
}
