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

import java.io.IOException;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Drives a Server-Sent Events stream on top of the existing single-partition fetch path.
 *
 * <p>The flow is a recursive long-poll: submit a fetch at {@code currentOffset}, write each returned record as a
 * {@code data: ...\n\n} SSE event, advance {@code currentOffset} past the last record, and re-submit. When the broker
 * returns an empty page (caught up to the high watermark; the broker's fetch purgatory held the request for its
 * configured max-wait), we re-submit immediately — the long-poll on the broker side is what throttles the loop, not
 * a client-side sleep. This is exactly the "replay then live, no reconnection" behaviour PROMPT.md AC6 asks for.
 *
 * <p>Backpressure is implicit: we never schedule the next fetch until the previous response has been written. Each
 * record is one {@link ServletOutputStream#write} + {@link ServletOutputStream#flush}; an IOException on either is
 * how we detect that the client disconnected. The first failure tears the stream down and completes the AsyncContext.
 *
 * <p>Per-partition errors mid-stream (the broker reports {@code error != NONE} on a fetch) are surfaced as a final
 * {@code event: error} SSE frame and then the stream closes. We do not attempt to recover; a streaming consumer that
 * loses its source partition wants to know about it, not silently retry forever.
 *
 * <p>This class deliberately does not touch {@link KafkaHttpBridge} or the formatters — SSE has its own response
 * shape (one event per record, not one body per fetch) so reusing {@link FetchResponseFormatter#format} would be
 * the wrong abstraction. We share the lower-level {@link RequestSubmitter} contract instead.
 */
final class SseStreamer {

    private static final Logger LOG = LoggerFactory.getLogger(SseStreamer.class);

    private static final byte[] CRLF = "\n\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    private static final byte[] DATA_PREFIX = "data: ".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    private static final byte[] ID_PREFIX = "id: ".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    private static final byte[] EVENT_ERROR = "event: error\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    private static final byte[] CONNECTED_COMMENT =
        ": connected\n\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    private static final byte LF = (byte) '\n';

    private final AsyncContext async;
    private final HttpServletResponse resp;
    private final ServletOutputStream out;
    private final RequestSubmitter submitter;
    private final ObjectMapper mapper;
    private final String topic;
    private final int partition;
    private final OptionalInt maxBytes;
    private final SseStreamLimiter.Token limiterToken;
    private final Executor httpExecutor;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile long currentOffset;

    private SseStreamer(AsyncContext async, RequestSubmitter submitter, ObjectMapper mapper,
                        String topic, int partition, long startOffset, OptionalInt maxBytes,
                        SseStreamLimiter.Token limiterToken, Executor httpExecutor) throws IOException {
        this.async = Objects.requireNonNull(async);
        this.resp = (HttpServletResponse) async.getResponse();
        this.out = resp.getOutputStream();
        this.submitter = Objects.requireNonNull(submitter);
        this.mapper = Objects.requireNonNull(mapper);
        this.topic = Objects.requireNonNull(topic);
        this.partition = partition;
        this.currentOffset = startOffset;
        this.maxBytes = Objects.requireNonNull(maxBytes);
        this.limiterToken = Objects.requireNonNull(limiterToken);
        this.httpExecutor = Objects.requireNonNull(httpExecutor);
    }

    /**
     * Initialize the SSE response and kick off the first fetch. The AsyncContext must already be started by the
     * caller. The streamer takes ownership: it will complete the AsyncContext when the stream terminates (client
     * disconnect, partition-level error, or unrecoverable submitter failure).
     */
    static void start(AsyncContext async, RequestSubmitter submitter, ObjectMapper mapper,
                      FetchRequestParser.FetchCommand command, SseStreamLimiter.Token limiterToken,
                      Executor httpExecutor) {
        Objects.requireNonNull(limiterToken, "limiterToken must not be null — caller must acquire before start()");
        Objects.requireNonNull(httpExecutor, "httpExecutor must not be null");
        SseStreamer streamer;
        try {
            HttpServletResponse resp = (HttpServletResponse) async.getResponse();
            resp.setStatus(HttpStatusMapper.OK);
            resp.setContentType(ContentTypeNegotiator.TEXT_EVENT_STREAM);
            resp.setCharacterEncoding("UTF-8");
            resp.setHeader("Cache-Control", "no-cache");
            // No Content-Length: this is a streaming response. Disable any servlet-container buffering so the first
            // event reaches the client immediately rather than waiting for a flush threshold.
            resp.setBufferSize(0);
            // SSE clients reconnect with Last-Event-ID after a disconnect. We don't implement reconnection semantics
            // server-side in v1 (the client gets to choose what offset to resume from), so leave that header alone.
            async.setTimeout(0L); // no servlet-side timeout — the broker's fetch max-wait is the only pacing

            streamer = new SseStreamer(async, submitter, mapper, command.topic(), command.partition(),
                command.offset(), command.maxBytes(), limiterToken, httpExecutor);
            // Write the framing comment so connection-buffering proxies flush the headers before any record arrives.
            streamer.out.write(CONNECTED_COMMENT);
            streamer.out.flush();
        } catch (IOException e) {
            // Couldn't even write the priming bytes — connection's already gone. Release the slot we reserved and
            // give up; we never made it to scheduleNextFetch so closeStream() won't run for us.
            LOG.debug("SSE stream aborted before first fetch: {}", e.toString());
            limiterToken.close();
            async.complete();
            return;
        } catch (RuntimeException e) {
            LOG.warn("SSE stream initialization failed", e);
            limiterToken.close();
            async.complete();
            return;
        }
        streamer.scheduleNextFetch();
    }

    private void scheduleNextFetch() {
        if (closed.get()) {
            return;
        }
        FetchRequestParser.FetchCommand command =
            new FetchRequestParser.FetchCommand(topic, partition, currentOffset, maxBytes);
        // whenCompleteAsync(..., httpExecutor) dispatches the next iteration off the thread that completed the
        // submitter future. That thread is the broker's request-handler thread (RequestChannel callback) — running
        // the SSE write loop there pins a Kafka API handler on a slow streaming client and can starve the binary
        // protocol. Move the write + scheduleNextFetch chain onto Jetty's server thread pool instead.
        submitter.submitFetch(command).whenCompleteAsync(this::handleFetchResult, httpExecutor);
    }

    private void handleFetchResult(RequestSubmitter.FetchResult result, Throwable throwable) {
        if (closed.get()) {
            return;
        }
        if (throwable != null) {
            LOG.warn("SSE fetch submission failed for {}/{} at offset {}", topic, partition, currentOffset, throwable);
            tryWriteErrorFrame("INTERNAL", throwable.getMessage());
            closeStream();
            return;
        }
        FetchResponseFormatter.PartitionFetch view = result.partition();
        if (view.error() != Errors.NONE) {
            tryWriteErrorFrame(view.error().name(),
                view.errorMessage() != null ? view.errorMessage() : view.error().message());
            closeStream();
            return;
        }

        boolean clientStillThere = true;
        for (FetchResponseFormatter.FetchedRecord record : view.records()) {
            try {
                writeRecordEvent(record);
            } catch (IOException e) {
                LOG.debug("SSE client disconnected for {}/{}: {}", topic, partition, e.toString());
                clientStillThere = false;
                break;
            }
            currentOffset = record.offset() + 1;
        }
        if (!clientStillThere) {
            closeStream();
            return;
        }
        // If the broker returned nothing, the fetch purgatory already held us up to its max-wait. Re-submitting
        // immediately is the correct behaviour — that's how SSE transitions from "replay" to "live tail" without a
        // reconnect.
        scheduleNextFetch();
    }

    private void writeRecordEvent(FetchResponseFormatter.FetchedRecord record) throws IOException {
        ObjectNode body = mapper.createObjectNode();
        body.put("offset", record.offset());
        body.put("timestamp", record.timestamp());
        body.set("key", record.key() == null
            ? mapper.nullNode()
            : ValueSerializer.encode(record.key(), null).toJson(mapper));
        body.set("value", ValueSerializer.encode(record.value(), record.contentType()).toJson(mapper));

        // `id:` carries the record's offset so an HTML5 EventSource that reconnects can resume by sending the value
        // back as Last-Event-ID. We don't enforce this server-side yet; supplying it costs nothing and unlocks the
        // simplest reconnection pattern for client libraries.
        out.write(ID_PREFIX);
        out.write(Long.toString(record.offset()).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        out.write(LF);
        out.write(DATA_PREFIX);
        // Records produced over HTTP never contain raw newlines unescaped, since the body is JSON. writeValueAsBytes
        // produces a single line of JSON; the closing \n\n then terminates the SSE event.
        out.write(mapper.writeValueAsBytes(body));
        out.write(CRLF);
        out.flush();
    }

    private void tryWriteErrorFrame(String code, String message) {
        try {
            ObjectNode payload = mapper.createObjectNode();
            payload.put("errorCode", code);
            payload.put("errorMessage", message == null ? "" : message);
            out.write(EVENT_ERROR);
            out.write(DATA_PREFIX);
            out.write(mapper.writeValueAsBytes(payload));
            out.write(CRLF);
            out.flush();
        } catch (IOException ignored) {
            // Client's gone; nothing we can do.
        }
    }

    private void closeStream() {
        if (closed.compareAndSet(false, true)) {
            // Release the SSE slot before completing the AsyncContext. Token.close() is idempotent, so even if a
            // future refactor pushes closeStream() through two paths the limiter count remains accurate.
            limiterToken.close();
            try {
                async.complete();
            } catch (RuntimeException e) {
                LOG.debug("AsyncContext.complete() failed (already completed?): {}", e.toString());
            }
        }
    }
}
