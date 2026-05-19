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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Servlet that bridges Jetty's HTTP I/O to {@link KafkaHttpBridge}. Mounted at {@code /v1/topics/*}.
 *
 * <p>Path layout: {@code /v1/topics/{topic}/records}. The {@code {topic}} segment is extracted from
 * {@link HttpServletRequest#getPathInfo()}. Any path that doesn't match is rejected with {@code 404}.
 *
 * <p>Async by design: the bridge returns a {@link CompletableFuture}; the servlet uses {@link AsyncContext} to free
 * the Jetty thread while we wait for the broker, then writes the response on the future's completion thread. This
 * matches Kafka's existing thread budget — request-handler threads serve broker work, not HTTP I/O waits.
 *
 * <p>The servlet never throws to the container. Any exception (parse, encode, downstream) is converted into a 500
 * response in-band. This mirrors {@link KafkaHttpBridge}'s never-throws invariant.
 */
public final class KafkaHttpServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaHttpServlet.class);

    private static final String PATH_PREFIX_TOPICS = "/topics/";
    private static final String PATH_SUFFIX_RECORDS = "/records";
    private static final String HEADER_ACCEPT = "Accept";
    private static final String HEADER_RETRY_AFTER = "Retry-After";

    private final KafkaHttpBridge bridge;
    private final RequestSubmitter submitter;
    private final ObjectMapper mapper;
    private final int maxRequestBodyBytes;
    private final SseStreamLimiter sseLimiter;
    private final Executor httpExecutor;
    private final HttpBridgeMetrics metrics;

    public KafkaHttpServlet(KafkaHttpBridge bridge, RequestSubmitter submitter, ObjectMapper mapper,
                            int maxRequestBodyBytes, SseStreamLimiter sseLimiter, Executor httpExecutor,
                            HttpBridgeMetrics metrics) {
        this.bridge = Objects.requireNonNull(bridge, "bridge must not be null");
        this.submitter = Objects.requireNonNull(submitter, "submitter must not be null");
        this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
        if (maxRequestBodyBytes < 0) {
            throw new IllegalArgumentException("maxRequestBodyBytes must be non-negative, got " + maxRequestBodyBytes);
        }
        this.maxRequestBodyBytes = maxRequestBodyBytes;
        this.sseLimiter = Objects.requireNonNull(sseLimiter, "sseLimiter must not be null");
        this.httpExecutor = Objects.requireNonNull(httpExecutor, "httpExecutor must not be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
    }

    @Override
    protected void doTrace(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        // HttpServlet.doTrace echoes the request line + every header back as `message/http`. That's the
        // classic XST surface: an attacker that can run script in the origin uses TRACE to read cookies and
        // auth headers that are marked HttpOnly on the JS side. The bridge accepts POST/GET/HEAD/OPTIONS
        // only — closing TRACE here keeps the wire surface to what PROMPT.md actually documents.
        writeMethodNotAllowed(resp);
    }

    @Override
    protected void doOptions(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        // The default doOptions reflects every doXxx defined on the class hierarchy into an Allow header — that
        // still listed TRACE before we overrode it, and would re-introduce the leak the moment HttpServlet adds
        // a new default verb. Curate the Allow set explicitly: the bridge documents POST (produce on /records),
        // GET (SSE on /records?stream=true and fetch elsewhere), HEAD (default servlet behaviour for GET), and
        // OPTIONS (this method). Anything else is 405.
        resp.setStatus(HttpStatusMapper.OK);
        resp.setHeader("Allow", "GET, HEAD, OPTIONS, POST");
        resp.setContentLength(0);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        long startNanos = System.nanoTime();
        String topic = extractTopic(req.getPathInfo());
        if (topic == null) {
            writeNotFound(resp);
            metrics.recordRequest(HttpBridgeMetrics.Operation.PRODUCE, elapsedMs(startNanos), HttpStatusMapper.NOT_FOUND);
            return;
        }

        JsonNode body;
        // Wrap the request InputStream in a hard byte cap BEFORE handing it to Jackson — readTree() will consume the
        // entire stream into memory, and Jetty's default HttpConfiguration has no body-size limit of its own. Without
        // this wrapper a single multi-GiB POST can OOM the broker JVM before any Kafka admission control runs.
        try (BoundedRequestBody bounded = new BoundedRequestBody(req.getInputStream(), maxRequestBodyBytes)) {
            body = mapper.readTree(bounded);
        } catch (BodyTooLargeException e) {
            writePayloadTooLarge(resp, e.limit());
            metrics.recordOversizedBodyRejection();
            metrics.recordRequest(HttpBridgeMetrics.Operation.PRODUCE, elapsedMs(startNanos), HttpStatusMapper.PAYLOAD_TOO_LARGE);
            return;
        } catch (JsonProcessingException e) {
            // Log the parser's diagnostic server-side; never echo it to the client. Jackson's getOriginalMessage()
            // reveals byte offsets, snippet of input, and the parse rule that tripped — not a leak on the level of
            // an NPE class/field name, but the same defect class. A fixed generic phrase keeps the error envelope
            // shape stable across parse-failure modes.
            LOG.debug("rejected produce request with malformed JSON body", e);
            writeBadRequest(resp, "body is not valid JSON");
            metrics.recordRequest(HttpBridgeMetrics.Operation.PRODUCE, elapsedMs(startNanos), HttpStatusMapper.BAD_REQUEST);
            return;
        } catch (IOException e) {
            // Jackson wraps a BodyTooLargeException as itself (IOException → unchanged), but a deeply nested cause is
            // possible if some future Jackson revision adds buffering — check the cause chain so we still emit 413.
            if (rootCauseIsBodyTooLarge(e)) {
                writePayloadTooLarge(resp, maxRequestBodyBytes);
                metrics.recordOversizedBodyRejection();
                metrics.recordRequest(HttpBridgeMetrics.Operation.PRODUCE, elapsedMs(startNanos), HttpStatusMapper.PAYLOAD_TOO_LARGE);
                return;
            }
            // Anything else (Jetty's BadMessageException for malformed chunked transfer, oversized headers, ...) is
            // logged server-side but reported to the client as a fixed phrase. e.getMessage() on these throwables
            // can disclose Jetty internals and partial request state — same defect class as the NPE-text leak that
            // Fix #4 plugged on the 500 path.
            LOG.warn("HTTP bridge failed to read produce request body", e);
            writeBadRequest(resp, "could not read request body");
            metrics.recordRequest(HttpBridgeMetrics.Operation.PRODUCE, elapsedMs(startNanos), HttpStatusMapper.BAD_REQUEST);
            return;
        }

        // Resolve content-type from the Accept header BEFORE going async — req is no longer safe to read once the
        // async dispatch hands the response off to the callback thread.
        String contentType = ContentTypeNegotiator.resolve(req.getHeader(HEADER_ACCEPT));
        AsyncContext async = req.startAsync();
        // Disable Jetty's default AsyncContext timeout. KafkaHttpBridge.withTimeout already wraps the submitter
        // future with `orTimeout(http.bridge.request.timeout.ms)` — when that fires, the dependent stage writes a
        // 504/REQUEST_TIMED_OUT envelope via HttpStatusMapper. Leaving Jetty's default (30s) in place creates a
        // race: with the same nominal value, Jetty's timer has no dispatch-hop overhead and wins under load,
        // routing through JsonErrorHandler with a generic 500 envelope instead of the bridge's contracted 504.
        // setTimeout(0L) makes the bridge the single timeout authority and keeps the error code consistent.
        async.setTimeout(0L);
        // whenCompleteAsync(..., httpExecutor) dispatches the response write off the thread that completes the
        // submitter future. That thread is the broker's request-handler thread (RequestChannel callback) — running
        // a socket write there pins a Kafka API handler on slow-client I/O, which can starve the binary protocol.
        // Move the write onto Jetty's server thread pool instead.
        // .exceptionally catches the executor-handoff failure mode the surrounding code cannot see: if
        // httpExecutor rejects the dispatch (saturated/shutdown Jetty pool), the JDK routes that
        // RejectedExecutionException to the dependent future, NOT this thread — writeResponseAndComplete
        // never runs, async.complete() is never called, and metrics.recordRequest is never recorded. The
        // request would sit until Jetty's default async timeout (~30s) abandons it. Mirrors the streamer
        // pattern landed in commit 2e6fc859fa.
        bridge.produce(topic, body).whenCompleteAsync((response, throwable) ->
            writeResponseAndComplete(async, response, throwable, contentType,
                HttpBridgeMetrics.Operation.PRODUCE, startNanos), httpExecutor)
            .exceptionally(t -> {
                handleDispatchFailure(async, t, HttpBridgeMetrics.Operation.PRODUCE, startNanos);
                return null;
            });
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        long startNanos = System.nanoTime();
        String topic = extractTopic(req.getPathInfo());
        if (topic == null) {
            writeNotFound(resp);
            metrics.recordRequest(HttpBridgeMetrics.Operation.FETCH, elapsedMs(startNanos), HttpStatusMapper.NOT_FOUND);
            return;
        }

        QueryParams params = QueryParams.from(req.getParameterMap());
        String contentType = ContentTypeNegotiator.resolve(req.getHeader(HEADER_ACCEPT));

        // SSE branches before startAsync: we parse the same fetch command up-front so a malformed query string falls
        // out as a one-shot 400, not a half-opened event-stream that then immediately errors. After this point the
        // streamer owns the AsyncContext and the response lifetime.
        if (ContentTypeNegotiator.TEXT_EVENT_STREAM.equals(contentType)) {
            // Parse the query string BEFORE the HEAD short-circuit so a malformed `?partition=foo` on a HEAD
            // request returns the same 400 it would on the equivalent GET (RFC 9110 §9.3.2: "the response to
            // a HEAD request is identical to that of an equivalent GET"). The parse step is pure and cheap; it
            // is safe to run before deciding whether to acquire a limiter slot.
            FetchRequestParser.FetchCommand command;
            try {
                command = FetchRequestParser.parse(topic, params);
            } catch (ProduceRequestParser.BadRequestException e) {
                writeBadRequest(resp, e.getMessage());
                metrics.recordRequest(HttpBridgeMetrics.Operation.FETCH, elapsedMs(startNanos), HttpStatusMapper.BAD_REQUEST);
                return;
            }
            // HEAD short-circuit. Jetty's default HttpServlet.doHead wraps the response in a body-counting
            // NoBodyResponse and delegates to doGet; for the SSE branch that would acquire a limiter slot,
            // start an AsyncContext, and run the streamer against the wrapper — its writes never throw
            // IOException, so the slot would be held until the underlying socket finally closes. A bursty
            // HEAD probe could therefore exhaust the SSE cap. Emit only the headers (no body) and return
            // before any slot or async work. The query has already been validated above.
            if ("HEAD".equalsIgnoreCase(req.getMethod())) {
                resp.setStatus(HttpStatusMapper.OK);
                resp.setContentType(ContentTypeNegotiator.TEXT_EVENT_STREAM);
                resp.setCharacterEncoding("UTF-8");
                resp.setHeader("Cache-Control", "no-cache");
                return;
            }
            // Acquire the concurrent-stream slot BEFORE startAsync — if the cap is reached we want to emit a
            // one-shot 429 with a Retry-After hint, not a half-opened event-stream that immediately closes. The
            // limiter is the admission gate; without it a runaway client can exhaust the Jetty thread pool.
            SseStreamLimiter.Token token = sseLimiter.tryAcquire();
            if (token == null) {
                writeTooManyStreams(resp);
                metrics.recordSseCapRejection();
                metrics.recordRequest(HttpBridgeMetrics.Operation.FETCH, elapsedMs(startNanos), HttpStatusMapper.TOO_MANY_REQUESTS);
                return;
            }
            AsyncContext async;
            try {
                async = req.startAsync();
            } catch (RuntimeException e) {
                token.close();
                throw e;
            }
            // No per-request status recording for the SSE branch — the stream itself can run for hours and there is
            // no single "response status" to record at the end. Instead record the accept event in
            // SseStreamsOpened; paired with the ActiveSseStreams gauge and RejectedAtSseCap meter this gives
            // operators the full open-rate / point-in-time / reject-rate picture without contaminating
            // RequestLatencyMs with stream-lifetime samples. The meter is incremented from inside start() only
            // after the priming comment write succeeds — otherwise a connection that died before producing any
            // events would inflate the open counter relative to the gauge.
            SseStreamer.start(async, submitter, mapper, command, token, httpExecutor, metrics::recordSseStreamOpened);
            return;
        }

        AsyncContext async = req.startAsync();
        // Same rationale as doPost: KafkaHttpBridge.withTimeout owns the request-timeout contract (504/REQUEST_TIMED_OUT
        // via HttpStatusMapper). Disable Jetty's default 30s AsyncContext timeout so the two don't race and so a
        // broker stall produces the documented 504 envelope rather than a generic 500 from JsonErrorHandler.
        async.setTimeout(0L);
        // See doPost for why this is whenCompleteAsync, and why we attach a terminal .exceptionally:
        // the broker handler thread that completes the future must not be the thread that performs the
        // HTTP socket write, but executor rejection at the handoff completes the dependent future and
        // would otherwise leak the AsyncContext + metric.
        bridge.fetch(topic, params).whenCompleteAsync((response, throwable) ->
            writeResponseAndComplete(async, response, throwable, contentType,
                HttpBridgeMetrics.Operation.FETCH, startNanos), httpExecutor)
            .exceptionally(t -> {
                handleDispatchFailure(async, t, HttpBridgeMetrics.Operation.FETCH, startNanos);
                return null;
            });
    }

    /**
     * Pull the topic name out of {@code /topics/{topic}/records}. Returns null if the path doesn't match — the caller
     * emits 404 in that case rather than guessing.
     */
    static String extractTopic(String pathInfo) {
        if (pathInfo == null || !pathInfo.startsWith(PATH_PREFIX_TOPICS) || !pathInfo.endsWith(PATH_SUFFIX_RECORDS)) {
            return null;
        }
        String inner = pathInfo.substring(PATH_PREFIX_TOPICS.length(),
            pathInfo.length() - PATH_SUFFIX_RECORDS.length());
        if (inner.isEmpty() || inner.indexOf('/') >= 0) {
            return null;
        }
        return inner;
    }

    private void writeResponseAndComplete(AsyncContext async, HttpBridgeResponse response, Throwable throwable,
                                          String contentType, HttpBridgeMetrics.Operation operation, long startNanos) {
        HttpServletResponse resp = (HttpServletResponse) async.getResponse();
        try {
            if (throwable != null) {
                // Stock JDK exception messages can leak class and field names ("Cannot invoke X.y() because z is null").
                // The full throwable is logged for operators; the client gets a sanitised generic phrase.
                LOG.warn("HTTP bridge produced an unhandled exception", throwable);
                writeInternalError(resp, "internal server error");
            } else {
                writeBridgeResponse(resp, response, contentType);
            }
        } catch (IOException e) {
            // Slow clients that disconnect mid-write are routine — Jetty surfaces this as EofException /
            // ClosedChannelException / "broken pipe" / "connection reset". Logging every such case at WARN
            // creates alarm fatigue on dashboards that page on bridge WARN volume. True write failures
            // (encoding bug, runtime I/O fault on a still-open channel) stay at WARN.
            if (isClientDisconnect(e)) {
                LOG.debug("Client disconnected before response could be written", e);
            } else {
                LOG.warn("Failed to write HTTP response", e);
            }
        } finally {
            // Read the status from the response object rather than from the bridge result — this catches the 500
            // we wrote on `throwable != null` as well as anything writeBridgeResponse set.
            metrics.recordRequest(operation, elapsedMs(startNanos), resp.getStatus());
            async.complete();
        }
    }

    /**
     * Heuristic for distinguishing "client gone away" from "real write failure". Walks the cause chain because Jetty
     * sometimes wraps the underlying NIO failure inside its own {@code EofException}. Matches {@link ClosedChannelException}
     * by type, Jetty's {@code EofException} by simple name (avoids a hard dependency on Jetty's internal package), and
     * the canonical TCP-disconnect message phrases. Lowercased for portability across JDK locales.
     */
    private static boolean isClientDisconnect(IOException e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof ClosedChannelException) {
                return true;
            }
            if ("EofException".equals(t.getClass().getSimpleName())) {
                return true;
            }
            String msg = t.getMessage();
            if (msg != null) {
                String lower = msg.toLowerCase(Locale.ROOT);
                if (lower.contains("broken pipe") || lower.contains("connection reset")) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Terminal handler for failures the {@code whenCompleteAsync} dispatch cannot deliver into
     * {@link #writeResponseAndComplete}. The only way to reach here in normal operation is a
     * {@code RejectedExecutionException} from {@code httpExecutor} (saturated or shut-down Jetty
     * thread pool), but the handler also defends against unexpected throwables thrown by the
     * dependent stage itself. The response has not been committed in either case, so we can
     * still emit a sanitised 500 envelope; throwable.getMessage() is never propagated to the
     * client (same discipline as the {@link #writeResponseAndComplete} 500 path).
     */
    private void handleDispatchFailure(AsyncContext async, Throwable throwable,
                                       HttpBridgeMetrics.Operation operation, long startNanos) {
        HttpServletResponse resp = (HttpServletResponse) async.getResponse();
        try {
            LOG.warn("HTTP bridge dispatch failed for {}", operation, throwable);
            if (!resp.isCommitted()) {
                writeInternalError(resp, "internal server error");
            }
        } catch (IOException e) {
            LOG.warn("Failed to write HTTP dispatch-failure response", e);
        } catch (RuntimeException e) {
            LOG.warn("Unexpected error while writing HTTP dispatch-failure response", e);
        } finally {
            try {
                metrics.recordRequest(operation, elapsedMs(startNanos), resp.getStatus());
            } catch (RuntimeException e) {
                LOG.debug("metrics.recordRequest failed on dispatch failure: {}", e.toString());
            }
            try {
                async.complete();
            } catch (RuntimeException e) {
                LOG.debug("AsyncContext.complete() failed on dispatch failure: {}", e.toString());
            }
        }
    }

    private static long elapsedMs(long startNanos) {
        return Math.max(0L, (System.nanoTime() - startNanos) / 1_000_000L);
    }

    private void writeBridgeResponse(HttpServletResponse resp, HttpBridgeResponse response, String contentType)
            throws IOException {
        resp.setStatus(response.status());
        resp.setContentType(contentType);
        if (response.hasRetryAfter()) {
            resp.setHeader(HEADER_RETRY_AFTER, Integer.toString(response.retryAfterSeconds()));
        }
        byte[] payload = mapper.writeValueAsBytes(response.body());
        resp.setContentLength(payload.length);
        resp.getOutputStream().write(payload);
    }

    private void writeBadRequest(HttpServletResponse resp, String message) throws IOException {
        writeEnvelope(resp, HttpStatusMapper.BAD_REQUEST, message);
    }

    private void writePayloadTooLarge(HttpServletResponse resp, long limit) throws IOException {
        writeEnvelope(resp, HttpStatusMapper.PAYLOAD_TOO_LARGE,
            "request body exceeds the configured limit of " + limit + " bytes");
    }

    private void writeTooManyStreams(HttpServletResponse resp) throws IOException {
        // Set Retry-After on 429 so clients have a concrete back-off hint. Five seconds is short enough that a
        // well-behaved consumer notices quickly when capacity frees up; long enough not to thrash a saturated server.
        resp.setHeader(HEADER_RETRY_AFTER, "5");
        writeEnvelope(resp, HttpStatusMapper.TOO_MANY_REQUESTS,
            "too many concurrent SSE streams; try again later");
    }

    private static boolean rootCauseIsBodyTooLarge(Throwable t) {
        for (Throwable cause = t; cause != null; cause = cause.getCause()) {
            if (cause instanceof BodyTooLargeException) {
                return true;
            }
            if (cause == cause.getCause()) {
                break;
            }
        }
        return false;
    }

    private void writeNotFound(HttpServletResponse resp) throws IOException {
        writeEnvelope(resp, HttpStatusMapper.NOT_FOUND, "no such endpoint");
    }

    private void writeMethodNotAllowed(HttpServletResponse resp) throws IOException {
        // RFC 9110: a 405 response MUST generate an Allow header listing the methods that are allowed. We pin
        // the curated list here so the envelope path and the OPTIONS path agree on what the bridge actually
        // supports.
        resp.setHeader("Allow", "GET, HEAD, OPTIONS, POST");
        writeEnvelope(resp, HttpStatusMapper.METHOD_NOT_ALLOWED, "method not allowed");
    }

    private void writeInternalError(HttpServletResponse resp, String message) throws IOException {
        writeEnvelope(resp, HttpStatusMapper.INTERNAL_SERVER_ERROR,
            message == null ? "internal error" : message);
    }

    private void writeEnvelope(HttpServletResponse resp, int status, String message) throws IOException {
        // Error envelopes are plain JSON regardless of Accept: they carry no _links so the HAL+JSON media type would
        // be misleading. Clients that strictly demanded hal+json get application/json back on errors — that's the
        // honest answer.
        resp.setStatus(status);
        resp.setContentType(ContentTypeNegotiator.APPLICATION_JSON);
        byte[] payload = mapper.writeValueAsBytes(ErrorEnvelope.forMessage(mapper, status, message));
        resp.setContentLength(payload.length);
        resp.getOutputStream().write(payload);
    }
}
