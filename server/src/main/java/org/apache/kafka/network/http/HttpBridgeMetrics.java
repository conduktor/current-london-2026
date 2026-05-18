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

import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * HTTP-bridge-specific operational metrics, exposed via the broker's standard Yammer registry so they appear alongside
 * every other Kafka metric in JMX and via {@code kafka.tools.JmxTool}.
 *
 * <p>MBean namespace is {@code kafka.network.http:type=BridgeMetrics,name=...}, chosen to be consistent with the
 * existing {@code kafka.network:type=*} family rather than the {@code org.apache.kafka.network.http} package this
 * class lives in.
 *
 * <p>The metric set is deliberately minimal — request latency per operation, response volume per (operation,
 * status-class), the live SSE-stream gauge, an SSE-stream open meter, and 413/429 bridge-rejection counters.
 * Request-timeout 504s do not get a dedicated counter; they land in the aggregate {@code 5xx} {@code ResponseCount}
 * bucket alongside 500 (internal bridge error) and 503 (broker unavailable) — each subtype has a distinct log
 * signature, so subdividing the meter would duplicate signal that already exists in logs without giving operators
 * a new dashboard axis. Anything more would either be duplicating what the binary protocol's {@code RequestChannel}
 * metrics already expose or speculating about which percentiles an operator wants. The rule of thumb: every metric
 * here is something an operator would put on an alert; everything else belongs in access logs.
 *
 * <p>{@link #close()} is mandatory before constructing a second instance against the same JMX namespace. The default
 * Yammer registry refuses duplicate names, so the bridge lifecycle (BrokerServer start → stop → start) must remove
 * registered metrics on stop. This is enforced by {@link KafkaHttpServer#stop()}.
 */
public final class HttpBridgeMetrics implements AutoCloseable {

    static final String METRICS_GROUP = "kafka.network.http";
    static final String METRICS_TYPE = "BridgeMetrics";

    private static final String OPERATION_TAG = "operation";
    private static final String STATUS_CLASS_TAG = "statusClass";
    private static final String NAME_REQUEST_LATENCY_MS = "RequestLatencyMs";
    private static final String NAME_RESPONSE_COUNT = "ResponseCount";
    private static final String NAME_ACTIVE_SSE_STREAMS = "ActiveSseStreams";
    private static final String NAME_REJECTED_OVERSIZED_BODY = "RejectedOversizedBody";
    private static final String NAME_REJECTED_AT_SSE_CAP = "RejectedAtSseCap";
    private static final String NAME_SSE_STREAMS_OPENED = "SseStreamsOpened";

    /** Operation identifier for {@link #recordRequest}. Lower-case in tags, capitalised here for readability. */
    public enum Operation {
        PRODUCE("Produce"),
        FETCH("Fetch");

        final String tag;

        Operation(String tag) {
            this.tag = tag;
        }
    }

    private final KafkaMetricsGroup group;
    private final Map<Operation, Histogram> latencyHistograms;
    private final Map<Operation, Map<String, Meter>> responseMeters;
    private final Meter rejectedOversizedBody;
    private final Meter rejectedAtSseCap;
    private final Meter sseStreamsOpened;

    /** {@code true} if {@link #close()} has run; we then refuse {@code record*} calls so a late callback doesn't blow up. */
    private volatile boolean closed = false;

    public HttpBridgeMetrics(SseStreamLimiter sseLimiter) {
        Objects.requireNonNull(sseLimiter, "sseLimiter must not be null");
        this.group = new KafkaMetricsGroup(METRICS_GROUP, METRICS_TYPE);

        // Pre-create the per-operation latency histograms and per-(operation, status-class) response meters. The set
        // of operations is closed (Produce, Fetch) and the set of status classes we care about is closed (2xx, 4xx,
        // 5xx). Pre-allocating means recordRequest() is a hash lookup with zero allocation on the hot path.
        //
        // Registration is wrapped in a try/catch that rolls back any already-registered names if a later registration
        // throws (e.g. a stale duplicate from a prior partial-init in the same JVM). Without rollback the leftover
        // entries would block every subsequent KafkaHttpServer construction in the process with "duplicate metric
        // name", because no fully-constructed HttpBridgeMetrics instance exists for stop() to close().
        Map<Operation, Histogram> histograms = new LinkedHashMap<>();
        Map<Operation, Map<String, Meter>> meters = new LinkedHashMap<>();
        Meter oversized = null;
        Meter sseCap = null;
        Meter streamsOpened = null;
        try {
            for (Operation op : Operation.values()) {
                histograms.put(op, group.newHistogram(NAME_REQUEST_LATENCY_MS, true,
                    Collections.singletonMap(OPERATION_TAG, op.tag)));
                Map<String, Meter> perStatus = new LinkedHashMap<>();
                for (String family : statusFamilies()) {
                    perStatus.put(family, group.newMeter(NAME_RESPONSE_COUNT, "responses", TimeUnit.SECONDS,
                        operationStatusTags(op, family)));
                }
                meters.put(op, perStatus);
            }

            // Rejection counters: each cap that returns a non-2xx because the bridge said no (rather than because the
            // broker said no) gets its own meter so operators can alert on the *bridge* deciding to refuse traffic,
            // independent of which exact HTTP status family it landed in. See class javadoc for why 504 is not
            // subdivided out of the aggregate 5xx ResponseCount bucket.
            oversized = group.newMeter(NAME_REJECTED_OVERSIZED_BODY, "rejections", TimeUnit.SECONDS);
            sseCap = group.newMeter(NAME_REJECTED_AT_SSE_CAP, "rejections", TimeUnit.SECONDS);

            // SSE-stream-open meter: rate of accepted SSE subscriptions. Operators need this independently of the
            // ResponseCount meter — successful SSE streams are never counted in ResponseCount because a stream can run
            // for hours and there is no single "response status" to record at stream end. Pairing this meter with the
            // ActiveSseStreams gauge and RejectedAtSseCap meter gives the full SSE picture (rate-open / point-in-time-open
            // / rate-rejected) without polluting RequestLatencyMs with non-comparable stream-lifetime samples.
            streamsOpened = group.newMeter(NAME_SSE_STREAMS_OPENED, "streams", TimeUnit.SECONDS);

            // Live gauge for the SSE concurrent-stream count. Reading the limiter is lock-free (one AtomicInteger.get()
            // per JMX poll) so exposing it as a gauge has no observable cost.
            group.newGauge(NAME_ACTIVE_SSE_STREAMS, sseLimiter::inUse);
        } catch (RuntimeException constructionFailure) {
            // Best-effort rollback of every singleton name we might have registered. removeMetric() is no-op-on-absent
            // so it is safe to call unconditionally — and we MUST call it unconditionally rather than gating on the
            // local reference, because a hypothetical Yammer registration that registers-then-throws would leave the
            // local at null while still leaking the name into the registry. Suppress secondary exceptions onto the
            // original so we don't mask the real construction failure.
            for (Operation op : Operation.values()) {
                tryRemove(constructionFailure, NAME_REQUEST_LATENCY_MS, Collections.singletonMap(OPERATION_TAG, op.tag));
                for (String family : statusFamilies()) {
                    tryRemove(constructionFailure, NAME_RESPONSE_COUNT, operationStatusTags(op, family));
                }
            }
            tryRemove(constructionFailure, NAME_REJECTED_OVERSIZED_BODY, Collections.emptyMap());
            tryRemove(constructionFailure, NAME_REJECTED_AT_SSE_CAP, Collections.emptyMap());
            tryRemove(constructionFailure, NAME_SSE_STREAMS_OPENED, Collections.emptyMap());
            tryRemove(constructionFailure, NAME_ACTIVE_SSE_STREAMS, Collections.emptyMap());
            throw constructionFailure;
        }
        this.latencyHistograms = Collections.unmodifiableMap(histograms);
        this.responseMeters = Collections.unmodifiableMap(meters);
        this.rejectedOversizedBody = oversized;
        this.rejectedAtSseCap = sseCap;
        this.sseStreamsOpened = streamsOpened;
    }

    private void tryRemove(RuntimeException primary, String metric, Map<String, String> tags) {
        try {
            if (tags.isEmpty()) {
                group.removeMetric(metric);
            } else {
                group.removeMetric(metric, tags);
            }
        } catch (RuntimeException cleanupFailure) {
            primary.addSuppressed(cleanupFailure);
        }
    }

    /**
     * Record the latency and HTTP status of a completed produce or fetch. Called from
     * {@link KafkaHttpServlet#writeResponseAndComplete} on the response thread. Idempotent against {@link #close} —
     * a metric write after close is silently dropped, never an exception.
     */
    public void recordRequest(Operation operation, long latencyMs, int httpStatus) {
        if (closed) {
            return;
        }
        Histogram histogram = latencyHistograms.get(operation);
        if (histogram != null) {
            histogram.update(latencyMs);
        }
        Map<String, Meter> perStatus = responseMeters.get(operation);
        if (perStatus != null) {
            Meter meter = perStatus.get(statusFamily(httpStatus));
            if (meter != null) {
                meter.mark();
            }
        }
    }

    /**
     * Whether {@link #close()} has been called. Exposed so the surrounding lifecycle (e.g. {@link KafkaHttpServer#start})
     * can fail loudly on misuse instead of wiring a servlet to a metrics object that silently drops every recording.
     */
    public boolean isClosed() {
        return closed;
    }

    public void recordOversizedBodyRejection() {
        if (!closed) rejectedOversizedBody.mark();
    }

    public void recordSseCapRejection() {
        if (!closed) rejectedAtSseCap.mark();
    }

    public void recordSseStreamOpened() {
        if (!closed) sseStreamsOpened.mark();
    }

    /**
     * Map an HTTP status code to a 2xx / 4xx / 5xx family label. Out-of-range codes (1xx, 3xx, or anything {@code < 100}
     * or {@code > 599}) fall through to {@code "other"} — these are not states the bridge emits today, but a Meter for
     * them keeps the recording path infallible.
     */
    static String statusFamily(int httpStatus) {
        if (httpStatus >= 200 && httpStatus < 300) return "2xx";
        if (httpStatus >= 400 && httpStatus < 500) return "4xx";
        if (httpStatus >= 500 && httpStatus < 600) return "5xx";
        return "other";
    }

    private static String[] statusFamilies() {
        return new String[] {"2xx", "4xx", "5xx", "other"};
    }

    private static Map<String, String> operationStatusTags(Operation op, String family) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put(OPERATION_TAG, op.tag);
        tags.put(STATUS_CLASS_TAG, family);
        return tags;
    }

    /**
     * Unregister every metric this instance owns from the Yammer registry. Required before a second instance can be
     * constructed in the same JVM — the registry rejects duplicates. Idempotent: a second {@code close()} is a no-op.
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (Operation op : Operation.values()) {
            group.removeMetric(NAME_REQUEST_LATENCY_MS, Collections.singletonMap(OPERATION_TAG, op.tag));
            for (String family : statusFamilies()) {
                group.removeMetric(NAME_RESPONSE_COUNT, operationStatusTags(op, family));
            }
        }
        group.removeMetric(NAME_REJECTED_OVERSIZED_BODY);
        group.removeMetric(NAME_REJECTED_AT_SSE_CAP);
        group.removeMetric(NAME_SSE_STREAMS_OPENED);
        group.removeMetric(NAME_ACTIVE_SSE_STREAMS);
    }
}
