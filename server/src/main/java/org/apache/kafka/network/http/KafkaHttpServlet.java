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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

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

    public KafkaHttpServlet(KafkaHttpBridge bridge, RequestSubmitter submitter, ObjectMapper mapper,
                            int maxRequestBodyBytes, SseStreamLimiter sseLimiter) {
        this.bridge = Objects.requireNonNull(bridge, "bridge must not be null");
        this.submitter = Objects.requireNonNull(submitter, "submitter must not be null");
        this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
        if (maxRequestBodyBytes < 0) {
            throw new IllegalArgumentException("maxRequestBodyBytes must be non-negative, got " + maxRequestBodyBytes);
        }
        this.maxRequestBodyBytes = maxRequestBodyBytes;
        this.sseLimiter = Objects.requireNonNull(sseLimiter, "sseLimiter must not be null");
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String topic = extractTopic(req.getPathInfo());
        if (topic == null) {
            writeNotFound(resp);
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
            return;
        } catch (JsonProcessingException e) {
            writeBadRequest(resp, "body is not valid JSON: " + e.getOriginalMessage());
            return;
        } catch (IOException e) {
            // Jackson wraps a BodyTooLargeException as itself (IOException → unchanged), but a deeply nested cause is
            // possible if some future Jackson revision adds buffering — check the cause chain so we still emit 413.
            if (rootCauseIsBodyTooLarge(e)) {
                writePayloadTooLarge(resp, maxRequestBodyBytes);
                return;
            }
            writeBadRequest(resp, "could not read request body: " + e.getMessage());
            return;
        }

        // Resolve content-type from the Accept header BEFORE going async — req is no longer safe to read once the
        // async dispatch hands the response off to the callback thread.
        String contentType = ContentTypeNegotiator.resolve(req.getHeader(HEADER_ACCEPT));
        AsyncContext async = req.startAsync();
        bridge.produce(topic, body).whenComplete((response, throwable) ->
            writeResponseAndComplete(async, response, throwable, contentType));
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String topic = extractTopic(req.getPathInfo());
        if (topic == null) {
            writeNotFound(resp);
            return;
        }

        QueryParams params = QueryParams.from(req.getParameterMap());
        String contentType = ContentTypeNegotiator.resolve(req.getHeader(HEADER_ACCEPT));

        // SSE branches before startAsync: we parse the same fetch command up-front so a malformed query string falls
        // out as a one-shot 400, not a half-opened event-stream that then immediately errors. After this point the
        // streamer owns the AsyncContext and the response lifetime.
        if (ContentTypeNegotiator.TEXT_EVENT_STREAM.equals(contentType)) {
            FetchRequestParser.FetchCommand command;
            try {
                command = FetchRequestParser.parse(topic, params);
            } catch (ProduceRequestParser.BadRequestException e) {
                writeBadRequest(resp, e.getMessage());
                return;
            }
            // Acquire the concurrent-stream slot BEFORE startAsync — if the cap is reached we want to emit a
            // one-shot 429 with a Retry-After hint, not a half-opened event-stream that immediately closes. The
            // limiter is the admission gate; without it a runaway client can exhaust the Jetty thread pool.
            SseStreamLimiter.Token token = sseLimiter.tryAcquire();
            if (token == null) {
                writeTooManyStreams(resp);
                return;
            }
            AsyncContext async;
            try {
                async = req.startAsync();
            } catch (RuntimeException e) {
                token.close();
                throw e;
            }
            SseStreamer.start(async, submitter, mapper, command, token);
            return;
        }

        AsyncContext async = req.startAsync();
        bridge.fetch(topic, params).whenComplete((response, throwable) ->
            writeResponseAndComplete(async, response, throwable, contentType));
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
                                          String contentType) {
        HttpServletResponse resp = (HttpServletResponse) async.getResponse();
        try {
            if (throwable != null) {
                LOG.warn("HTTP bridge produced an unhandled exception", throwable);
                writeInternalError(resp, throwable.getMessage());
            } else {
                writeBridgeResponse(resp, response, contentType);
            }
        } catch (IOException e) {
            LOG.warn("Failed to write HTTP response", e);
        } finally {
            async.complete();
        }
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
