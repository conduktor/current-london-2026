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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
    private final AtomicBoolean fetchInFlight = new AtomicBoolean();

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
     */
    public void grantCredits(int n) {
        if (closed.get()) {
            // Grant arrived after the stream tore down. Drop silently — the client will see the close frame.
            return;
        }
        if (n <= 0) {
            throw new IllegalArgumentException("credits to grant must be positive, got " + n);
        }
        credits.addAndGet(n);
        scheduleDrain();
    }

    /**
     * Tear the stream down. Idempotent: redundant calls (e.g. from a close handler that also fires after an
     * error frame) leave the limiter count consistent because {@link WsStreamLimiter.Token#close()} is itself
     * idempotent.
     */
    public void close() {
        if (closed.compareAndSet(false, true)) {
            limiterToken.close();
            try {
                sink.close();
            } catch (RuntimeException e) {
                LOG.debug("FrameSink.close() failed for {}/{}: {}", topic, partition, e.toString());
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
                close();
            }
        }
    }

    private void drainAndMaybeFetch() {
        // Reset before the work so a concurrent grant that arrives mid-drain re-arms us cleanly.
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
                // Another thread (a concurrent grant or another drain run) raced us on credits. Re-read.
                continue;
            }
            FetchResponseFormatter.FetchedRecord r = buffer.poll();
            if (r == null) {
                // Race: between our isEmpty check and poll, another drain took the record. Refund the
                // credit we reserved and exit — there is nothing left to deliver right now.
                credits.incrementAndGet();
                return true;
            }
            try {
                sendRecord(r);
            } catch (RuntimeException e) {
                LOG.debug("WS send failed for {}/{}: {}", topic, partition, e.toString());
                close();
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
        if (!fetchInFlight.compareAndSet(false, true)) {
            return;
        }
        FetchRequestParser.FetchCommand command =
            new FetchRequestParser.FetchCommand(topic, partition, currentOffset, maxBytes);
        try {
            submitter.submitFetch(command).whenCompleteAsync(this::handleFetchResult, httpExecutor);
        } catch (RuntimeException e) {
            fetchInFlight.set(false);
            LOG.warn("WS fetch submission failed for {}/{}", topic, partition, e);
            trySendErrorFrame("INTERNAL", e.getMessage());
            close();
        }
    }

    private void handleFetchResult(RequestSubmitter.FetchResult result, Throwable throwable) {
        fetchInFlight.set(false);
        if (closed.get()) {
            return;
        }
        if (throwable != null) {
            failFetch(throwable);
            return;
        }
        FetchResponseFormatter.PartitionFetch view = result.partition();
        if (view.error() != Errors.NONE) {
            String msg = view.errorMessage() != null ? view.errorMessage() : view.error().message();
            trySendErrorFrame(view.error().name(), msg);
            close();
            return;
        }
        // Stage records into the buffer first; the drain pass will deliver them subject to credit.
        for (FetchResponseFormatter.FetchedRecord r : view.records()) {
            buffer.offer(r);
        }
        scheduleDrain();
    }

    private void failFetch(Throwable throwable) {
        // Unwrap CompletionException if present so the client sees the broker's actual cause rather than
        // the future-machinery wrapper.
        Throwable cause = throwable;
        if (throwable instanceof java.util.concurrent.CompletionException && throwable.getCause() != null) {
            cause = throwable.getCause();
        }
        LOG.warn("WS fetch submission failed for {}/{} at offset {}", topic, partition, currentOffset, cause);
        trySendErrorFrame("INTERNAL", cause.getMessage());
        close();
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

        /** Whether the session is still open. The streamer consults this only as a hint. */
        boolean isOpen();
    }
}
