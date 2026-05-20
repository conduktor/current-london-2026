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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
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
    // SSE comment line: clients ignore it, but the write itself is our liveness probe — see handleFetchResult.
    private static final byte[] HEARTBEAT_COMMENT =
        ":\n\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
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
    // From the URL's ?from=earliest hint. Only consulted while currentOffset == 0L (the initial offset
    // the parser hands us for from=earliest). Once any record is delivered and currentOffset advances,
    // the flag is naturally moot. Without this propagation the bridge would drop the flag in
    // scheduleNextFetch, the submitter's OFFSET_OUT_OF_RANGE retry would never fire, and a retained or
    // compacted topic with logStartOffset > 0 would close every from=earliest stream with an error frame.
    private final boolean fromEarliest;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile long currentOffset;

    private SseStreamer(AsyncContext async, RequestSubmitter submitter, ObjectMapper mapper,
                        String topic, int partition, long startOffset, OptionalInt maxBytes,
                        boolean fromEarliest, SseStreamLimiter.Token limiterToken,
                        Executor httpExecutor) throws IOException {
        this.async = Objects.requireNonNull(async);
        this.resp = (HttpServletResponse) async.getResponse();
        this.out = resp.getOutputStream();
        this.submitter = Objects.requireNonNull(submitter);
        this.mapper = Objects.requireNonNull(mapper);
        this.topic = Objects.requireNonNull(topic);
        this.partition = partition;
        this.currentOffset = startOffset;
        this.maxBytes = Objects.requireNonNull(maxBytes);
        this.fromEarliest = fromEarliest;
        this.limiterToken = Objects.requireNonNull(limiterToken);
        this.httpExecutor = Objects.requireNonNull(httpExecutor);
    }

    /**
     * Initialize the SSE response and kick off the first fetch. The AsyncContext must already be started by the
     * caller. The streamer takes ownership: it will complete the AsyncContext when the stream terminates (client
     * disconnect, partition-level error, or unrecoverable submitter failure).
     *
     * <p>{@code onPrimed} runs exactly once iff the priming-comment write+flush succeeds, before the first fetch is
     * scheduled. The caller uses it to mark the "stream opened" metric — keeping the bookkeeping aligned with the
     * gauge: streams that fail at the priming write produced zero events and must not inflate the open counter.
     */
    static void start(AsyncContext async, RequestSubmitter submitter, ObjectMapper mapper,
                      FetchRequestParser.FetchCommand command, SseStreamLimiter.Token limiterToken,
                      Executor httpExecutor, Runnable onPrimed) {
        Objects.requireNonNull(limiterToken, "limiterToken must not be null — caller must acquire before start()");
        Objects.requireNonNull(httpExecutor, "httpExecutor must not be null");
        Objects.requireNonNull(onPrimed, "onPrimed must not be null");
        SseStreamer streamer;
        try {
            HttpServletResponse resp = (HttpServletResponse) async.getResponse();
            resp.setStatus(HttpStatusMapper.OK);
            resp.setContentType(ContentTypeNegotiator.TEXT_EVENT_STREAM);
            resp.setCharacterEncoding("UTF-8");
            resp.setHeader("Cache-Control", "no-cache");
            // nginx (and a few derivatives like OpenResty / Tengine) default to buffering proxied responses, which
            // turns SSE into a fixed-size lump arriving at the upstream's flush threshold rather than a live stream.
            // The reverse-proxy hint `X-Accel-Buffering: no` is the standard cross-vendor opt-out — nginx honours it,
            // other proxies that don't recognise it just pass it through harmlessly. Operators still need to disable
            // proxy_buffering server-side for upstream caches that buffer regardless of headers (Apache mod_proxy with
            // SetEnv proxy-sendcl, certain CDN tiers); the header just removes the most common foot-gun by default.
            resp.setHeader("X-Accel-Buffering", "no");
            // No Content-Length: this is a streaming response. Disable any servlet-container buffering so the first
            // event reaches the client immediately rather than waiting for a flush threshold.
            resp.setBufferSize(0);
            // SSE clients reconnect with Last-Event-ID after a disconnect. We don't implement reconnection semantics
            // server-side in v1 (the client gets to choose what offset to resume from), so leave that header alone.
            async.setTimeout(0L); // no servlet-side timeout — the broker's fetch max-wait is the only pacing

            streamer = new SseStreamer(async, submitter, mapper, command.topic(), command.partition(),
                command.offset(), command.maxBytes(), command.fromEarliest(), limiterToken, httpExecutor);
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
        // Priming write+flush has committed the response; the connection is live. Record "opened" now so the meter
        // matches the gauge: failures above this line never count as an open. The Runnable contract is not formally
        // declared no-throw, and the streamer is fully constructed by this point — token ownership has transferred,
        // the AsyncContext is started, and the response is committed. A throw out of onPrimed.run() at this point
        // would propagate to the servlet caller, but no path inside the streamer would ever run closeStream()
        // (scheduleNextFetch is skipped), so the limiter slot and AsyncContext would leak. Route any throw through
        // the streamer's own closeStream() so the cleanup is identical to a transport failure: token released,
        // AsyncContext completed, the slot freed for the next request.
        try {
            onPrimed.run();
        } catch (RuntimeException e) {
            LOG.warn("SSE onPrimed callback failed after priming flush — closing stream to release resources", e);
            streamer.closeStream();
            return;
        }
        streamer.scheduleNextFetch();
    }

    private void scheduleNextFetch() {
        if (closed.get()) {
            return;
        }
        // Keep propagating fromEarliest while currentOffset is still at its initial value (0L). The
        // submitter's OFFSET_OUT_OF_RANGE retry needs the flag to know that "0L" is a hint rather than
        // an explicit offset request. Once any record arrives, currentOffset advances and the flag is
        // automatically dropped — explicit offsets must not silently snap to logStartOffset.
        boolean propagateFromEarliest = fromEarliest && currentOffset == 0L;
        FetchRequestParser.FetchCommand command =
            new FetchRequestParser.FetchCommand(topic, partition, currentOffset, maxBytes, propagateFromEarliest);
        // whenCompleteAsync(..., httpExecutor) dispatches the next iteration off the thread that completed the
        // submitter future. That thread is the broker's request-handler thread (RequestChannel callback) — running
        // the SSE write loop there pins a Kafka API handler on a slow streaming client and can starve the binary
        // protocol. Move the write + scheduleNextFetch chain onto Jetty's server thread pool instead.
        // .exceptionally catches two failure modes the surrounding stage cannot: a
        // RejectedExecutionException when httpExecutor refuses to dispatch handleFetchResult (saturated
        // or shut-down Jetty pool — the JDK routes that to the dependent future, not the calling
        // thread), and any RuntimeException raised inside handleFetchResult itself. Without this
        // terminal handler the failure lands on an unobserved future, the recursive long-poll halts
        // mid-stream, and the limiter slot leaks until JVM shutdown.
        // Returning {@code null} satisfies the FetchResult-typed dependent stage; the value is never
        // observed because the handler is the terminal step in the chain.
        submitter.submitFetch(command)
            .whenCompleteAsync(this::handleFetchResult, httpExecutor)
            .exceptionally(t -> {
                handleSchedulingFailure(t);
                return null;
            });
    }

    /**
     * Terminal handler for failures the synchronous code cannot observe — executor rejection on the
     * {@code whenCompleteAsync} hand-off, and unexpected throwables from inside {@link #handleFetchResult}.
     * Sends a sanitised INTERNAL error frame (never echo {@code throwable.getMessage()} — stock JDK
     * messages leak broker class/field names) and tears the stream down so the limiter slot is reclaimed.
     */
    private void handleSchedulingFailure(Throwable throwable) {
        if (closed.get()) {
            return;
        }
        LOG.warn("SSE fetch dispatch failed for {}/{} at offset {}", topic, partition, currentOffset, throwable);
        tryWriteErrorFrame("INTERNAL", null);
        closeStream();
    }

    private void handleFetchResult(RequestSubmitter.FetchResult result, Throwable throwable) {
        if (closed.get()) {
            return;
        }
        if (throwable != null) {
            // Log the throwable server-side; never echo throwable.getMessage() to the client. Stock JDK
            // messages (e.g. NPE "Cannot invoke X.y() because z is null") leak broker class/field names.
            // Per-partition Kafka errors with sanitised text flow through the view.error() != NONE branch
            // below; this throwable branch is reached only on unexpected futures completion failures.
            LOG.warn("SSE fetch submission failed for {}/{} at offset {}", topic, partition, currentOffset, throwable);
            tryWriteErrorFrame("INTERNAL", null);
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
            } catch (IOException | RuntimeException e) {
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
        // Liveness probe for the empty-fetch path. When the broker has no new records the for-loop above
        // never attempts a write — and a Servlet streaming response has no other client-disconnect signal
        // until the next write fails. Without this heartbeat, a client that opens SSE on a quiet topic and
        // then drops its connection (TCP FIN) is undetectable: we would loop forever, holding an SSE
        // limiter slot and resubmitting broker fetches indefinitely. Writing a comment line on every empty
        // fetch bounds the leak window to one broker fetch max-wait (~500ms): the next iteration's write
        // throws IOException on a dead socket and we tear the stream down. Cost is 3 bytes per quiet-topic
        // poll (the {@code ":\n\n"} {@link #HEARTBEAT_COMMENT} constant), which is also the standard SSE
        // keep-alive pattern that prevents NAT/proxy idle timeouts.
        if (view.records().isEmpty()) {
            try {
                out.write(HEARTBEAT_COMMENT);
                out.flush();
            } catch (IOException | RuntimeException e) {
                LOG.debug("SSE client disconnected on heartbeat for {}/{}: {}", topic, partition, e.toString());
                closeStream();
                return;
            }
        }
        // If the broker returned nothing, the fetch purgatory already held us up to its max-wait. Re-submitting
        // immediately is the correct behaviour — that's how SSE transitions from "replay" to "live tail" without a
        // reconnect.
        //
        // EXCEPT under fetch-quota throttle. The bridge's synthetic RequestChannel.Request has no socket for
        // KafkaApis.requestHelper.throttle to mute, so a positive `result.throttleTimeMs()` is the only signal
        // we get to back off; ignoring it would tight-loop the broker under quota pressure. scheduleNextFetchAfter
        // routes a zero delay back through the immediate path, so the call site here stays branch-free (and the
        // method's NPath complexity stays under the checkstyle ceiling).
        scheduleNextFetchAfter(result.throttleTimeMs());
    }

    private void scheduleNextFetchAfter(long delayMs) {
        if (closed.get()) {
            return;
        }
        if (delayMs <= 0L) {
            scheduleNextFetch();
            return;
        }
        try {
            // Same shape as scheduleNextFetch: the delayedExecutor dispatches via httpExecutor when the
            // timer fires; if that dispatch is rejected the dependent future is what carries the
            // failure, not the calling thread. Terminal handler closes the stream so a throttled SSE
            // session cannot leak its limiter slot on shutdown.
            CompletableFuture.runAsync(this::scheduleNextFetch,
                CompletableFuture.delayedExecutor(delayMs, TimeUnit.MILLISECONDS, httpExecutor))
                .exceptionally(t -> {
                    handleSchedulingFailure(t);
                    return null;
                });
        } catch (RuntimeException e) {
            LOG.warn("SSE delayed-fetch dispatch failed for {}/{}", topic, partition, e);
            closeStream();
        }
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
        } catch (IOException | RuntimeException ignored) {
            // Client's gone; nothing we can do. Jetty's HttpOutput can also raise IllegalStateException /
            // WritePendingException when the stream is mid-flight or already closed — treat those as
            // disconnects so the caller (handleSchedulingFailure) still reaches closeStream() and the
            // SseStreamLimiter slot is released.
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
