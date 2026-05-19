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

import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistryListener;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HttpBridgeMetricsTest {

    private SseStreamLimiter limiter;
    private WsStreamLimiter wsLimiter;
    private HttpBridgeMetrics metrics;

    @BeforeEach
    void setUp() {
        limiter = new SseStreamLimiter(4);
        wsLimiter = new WsStreamLimiter(4);
        metrics = new HttpBridgeMetrics(limiter, wsLimiter);
    }

    @AfterEach
    void tearDown() {
        if (metrics != null) {
            metrics.close();
        }
    }

    @Test
    void statusFamilyBucketsHttpCodesCorrectly() {
        // Spot-check the buckets that the bridge actually emits. 200 / 207 belong to 2xx; 400 / 403 / 413 / 429 to
        // 4xx; 500 / 503 / 504 to 5xx. Out-of-range codes fall through to "other" so the recording path stays
        // infallible — nothing in v1 emits these, but a future status added to HttpStatusMapper shouldn't NPE the
        // metric layer.
        assertEquals("2xx", HttpBridgeMetrics.statusFamily(200));
        assertEquals("2xx", HttpBridgeMetrics.statusFamily(207));
        assertEquals("4xx", HttpBridgeMetrics.statusFamily(400));
        assertEquals("4xx", HttpBridgeMetrics.statusFamily(403));
        assertEquals("4xx", HttpBridgeMetrics.statusFamily(413));
        assertEquals("4xx", HttpBridgeMetrics.statusFamily(429));
        assertEquals("5xx", HttpBridgeMetrics.statusFamily(500));
        assertEquals("5xx", HttpBridgeMetrics.statusFamily(503));
        assertEquals("5xx", HttpBridgeMetrics.statusFamily(504));
        assertEquals("other", HttpBridgeMetrics.statusFamily(100));
        assertEquals("other", HttpBridgeMetrics.statusFamily(304));
        assertEquals("other", HttpBridgeMetrics.statusFamily(999));
    }

    @Test
    void recordRequestUpdatesPerOperationLatencyHistogram() {
        // Recording two produce latencies must reflect in the produce histogram only — the fetch histogram must
        // remain at zero count. Without per-operation isolation a slow produce would skew the fetch latency
        // dashboard, defeating the whole point of tagging.
        metrics.recordRequest(HttpBridgeMetrics.Operation.PRODUCE, 12, 200);
        metrics.recordRequest(HttpBridgeMetrics.Operation.PRODUCE, 34, 200);

        Histogram produceLatency = lookupHistogram("RequestLatencyMs", "Produce");
        Histogram fetchLatency = lookupHistogram("RequestLatencyMs", "Fetch");
        assertEquals(2L, produceLatency.count(), "produce latency histogram must record both samples");
        assertEquals(0L, fetchLatency.count(), "fetch latency histogram must stay untouched");
        assertEquals(34.0, produceLatency.max(), 0.0001, "max sample should be the larger latency");
    }

    @Test
    void recordRequestIncrementsResponseMeterByStatusClass() {
        // Mix 200 / 4xx / 5xx on the fetch path. Each must land in its own (operation, statusClass) meter — no
        // cross-bucket contamination, no double counting.
        metrics.recordRequest(HttpBridgeMetrics.Operation.FETCH, 10, 200);
        metrics.recordRequest(HttpBridgeMetrics.Operation.FETCH, 11, 200);
        metrics.recordRequest(HttpBridgeMetrics.Operation.FETCH, 12, 403);
        metrics.recordRequest(HttpBridgeMetrics.Operation.FETCH, 13, 503);

        assertEquals(2L, lookupMeter("ResponseCount", "Fetch", "2xx").count());
        assertEquals(1L, lookupMeter("ResponseCount", "Fetch", "4xx").count());
        assertEquals(1L, lookupMeter("ResponseCount", "Fetch", "5xx").count());
        assertEquals(0L, lookupMeter("ResponseCount", "Fetch", "other").count());
        // Produce side must be entirely untouched.
        assertEquals(0L, lookupMeter("ResponseCount", "Produce", "2xx").count());
    }

    @Test
    void sseStreamsOpenedMeterCountsAcceptedStreamsSeparatelyFromRejections() {
        // Successful SSE accepts never land in ResponseCount (the stream has no terminal status), so they get a
        // dedicated meter. An operator needs all three signals to debug an SSE incident: open rate, point-in-time
        // concurrency (gauge), and reject rate. Without the dedicated open meter the gauge alone hides bursty
        // open-then-close traffic that would still indicate client misbehaviour.
        metrics.recordSseStreamOpened();
        metrics.recordSseStreamOpened();
        metrics.recordSseStreamOpened();
        metrics.recordSseCapRejection();

        assertEquals(3L, lookupMeter("SseStreamsOpened").count());
        assertEquals(1L, lookupMeter("RejectedAtSseCap").count());
    }

    @Test
    void rejectionCountersAreSeparatePerCap() {
        // The two bridge-side rejection caps each have their own meter so an operator alert can pin down WHICH cap
        // is firing without having to read access logs. Mixing them up would mean an oversized-body spike looks
        // identical to an SSE-cap spike on the dashboard. The request-timeout 504 is deliberately *not* a dedicated
        // counter — it lands in the aggregate Produce/Fetch 5xx response meter alongside 500 and 503; each subtype
        // is already distinguishable via its log signature so an extra meter would duplicate signal.
        metrics.recordOversizedBodyRejection();
        metrics.recordOversizedBodyRejection();
        metrics.recordOversizedBodyRejection();
        metrics.recordSseCapRejection();

        assertEquals(3L, lookupMeter("RejectedOversizedBody").count());
        assertEquals(1L, lookupMeter("RejectedAtSseCap").count());
    }

    @Test
    void wsSubscriptionsOpenedMeterCountsAcceptedSeparatelyFromRejections() {
        // Same rationale as the SSE-side meter: a WebSocket subscription has no terminal response status, so the
        // ResponseCount family never sees it. An operator debugging a WS incident needs open-rate, point-in-time
        // active count, and reject-rate independently.
        metrics.recordWsSubscriptionOpened();
        metrics.recordWsSubscriptionOpened();
        metrics.recordWsCapRejection();

        assertEquals(2L, lookupMeter("WsSubscriptionsOpened").count());
        assertEquals(1L, lookupMeter("RejectedAtWsCap").count());
    }

    @Test
    void wsAndSseRejectionCountersAreIndependentMeters() {
        // The two streaming paths each get their own cap and their own rejection meter. Alerting on bridge-side
        // refusals must be able to distinguish "SSE cap hit" from "WS cap hit" — different operator action.
        metrics.recordSseCapRejection();
        metrics.recordSseCapRejection();
        metrics.recordWsCapRejection();
        metrics.recordWsCapRejection();
        metrics.recordWsCapRejection();

        assertEquals(2L, lookupMeter("RejectedAtSseCap").count());
        assertEquals(3L, lookupMeter("RejectedAtWsCap").count());
    }

    @Test
    void activeWsSubscriptionsGaugeReflectsLimiterState() {
        // Mirror the ActiveSseStreams test: the gauge reads live from the WsStreamLimiter, with no cached snapshot.
        Gauge<?> gauge = lookupGauge("ActiveWsSubscriptions");
        assertEquals(0, gauge.value());

        WsStreamLimiter.Token a = wsLimiter.tryAcquire();
        WsStreamLimiter.Token b = wsLimiter.tryAcquire();
        assertEquals(2, gauge.value(), "gauge must observe newly-acquired WS slots");

        a.close();
        assertEquals(1, gauge.value(), "gauge must observe the released slot");
        b.close();
        assertEquals(0, gauge.value());
    }

    @Test
    void activeSseStreamsGaugeReflectsLimiterState() {
        // The gauge must read live from the limiter — not a cached snapshot. Acquiring two tokens must change the
        // observed value immediately.
        Gauge<?> gauge = lookupGauge("ActiveSseStreams");
        assertEquals(0, gauge.value());

        SseStreamLimiter.Token a = limiter.tryAcquire();
        SseStreamLimiter.Token b = limiter.tryAcquire();
        assertEquals(2, gauge.value(), "gauge must observe newly-acquired SSE slots");

        a.close();
        assertEquals(1, gauge.value(), "gauge must observe the released slot");
        b.close();
        assertEquals(0, gauge.value());
    }

    @Test
    void recordingAfterCloseIsSilentlyDropped() {
        // The bridge has a never-throws contract. A late callback firing after BrokerServer.stop() — perfectly
        // possible during shutdown — must not blow up the shutdown sequence with a "metric was removed" error.
        // Verify the calls go through without exception; the underlying meter is gone by then so we cannot assert
        // the counter, but absence of an exception is the contract under test.
        metrics.close();
        metrics.recordRequest(HttpBridgeMetrics.Operation.PRODUCE, 5, 200);
        metrics.recordOversizedBodyRejection();
        metrics.recordSseCapRejection();
        metrics.recordSseStreamOpened();
        metrics.recordWsCapRejection();
        metrics.recordWsSubscriptionOpened();
    }

    @Test
    void concurrentRecordAndCloseNeverThrows() throws Exception {
        // The check-then-act window between `if (closed)` and the actual meter.mark() can be interleaved with
        // close() unregistering the meter. Yammer Meter.mark() on a deregistered instance is safe (the registry
        // no longer exposes it, but the object itself is intact), so the contract we're asserting is: under
        // contention, no recorder thread ever observes an exception. This is the never-throws shutdown invariant.
        int writerCount = 4;
        int iterationsPerWriter = 5_000;
        java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(writerCount);
        java.util.concurrent.atomic.AtomicReference<Throwable> error = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(writerCount);
        try {
            for (int t = 0; t < writerCount; t++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < iterationsPerWriter; i++) {
                            metrics.recordRequest(HttpBridgeMetrics.Operation.PRODUCE, i, 200);
                            metrics.recordRequest(HttpBridgeMetrics.Operation.FETCH, i, 503);
                            metrics.recordOversizedBodyRejection();
                            metrics.recordSseCapRejection();
                            metrics.recordSseStreamOpened();
                            metrics.recordWsCapRejection();
                            metrics.recordWsSubscriptionOpened();
                        }
                    } catch (Throwable th) {
                        error.compareAndSet(null, th);
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            // Sleep a small slice so writers race past the closed check, then close mid-flight.
            Thread.sleep(2);
            metrics.close();
            // Bounded await so a future regression that deadlocks fails fast instead of hanging the suite. 10s is
            // ~1000× the longest observed run on a loaded laptop, so any timeout here means a real lockup.
            if (!done.await(10, java.util.concurrent.TimeUnit.SECONDS)) {
                throw new AssertionError("concurrent recorders did not finish within 10s — likely deadlock");
            }
        } finally {
            pool.shutdownNow();
        }
        if (error.get() != null) {
            throw new AssertionError("recorder thread threw under concurrent close", error.get());
        }
        // Setting metrics = null tells tearDown there's nothing to close anymore.
        metrics = null;
    }

    @Test
    void closeUnregistersEveryMetricSoASecondInstanceCanBeConstructed() {
        // Yammer's MetricsRegistry silently de-duplicates by name: getOrAdd returns the EXISTING metric on collision
        // rather than throwing. So a second `new HttpBridgeMetrics()` against the same registry SUCCEEDS even if
        // close() forgot to unregister a metric — the new instance silently rebinds to the leftover registration and
        // both lifecycle generations share a single Histogram / Meter / Gauge object. Quantiles, mean rates, and
        // gauge bindings all bleed across the boundary, which is exactly the misleading-JMX state the class javadoc
        // calls out.
        //
        // A test that only checks "did the second construction succeed?" is therefore blind to a forgotten
        // removeMetric call. To actually pin the contract, after close() walk every metric name this class owns
        // (matching the close() body exactly) and assert it is ABSENT from KafkaYammerMetrics.defaultRegistry().
        // Then construct a fresh instance to confirm the full round-trip.
        metrics.close();

        // Tagged metrics: latency histograms per operation, response meters per (operation, statusClass).
        for (HttpBridgeMetrics.Operation op : HttpBridgeMetrics.Operation.values()) {
            Map<String, String> latencyTags = new LinkedHashMap<>();
            latencyTags.put("operation", op.tag);
            assertNotRegistered("RequestLatencyMs", latencyTags);
            for (String family : new String[] {"2xx", "4xx", "5xx", "other"}) {
                Map<String, String> respTags = new LinkedHashMap<>();
                respTags.put("operation", op.tag);
                respTags.put("statusClass", family);
                assertNotRegistered("ResponseCount", respTags);
            }
        }
        // Untagged metrics.
        assertNotRegistered("RejectedOversizedBody");
        assertNotRegistered("RejectedAtSseCap");
        assertNotRegistered("SseStreamsOpened");
        assertNotRegistered("ActiveSseStreams");
        assertNotRegistered("RejectedAtWsCap");
        assertNotRegistered("WsSubscriptionsOpened");
        assertNotRegistered("ActiveWsSubscriptions");

        HttpBridgeMetrics fresh = null;
        try {
            fresh = new HttpBridgeMetrics(limiter, wsLimiter);
            // Round-trip: after a clean close, a fresh instance must rebind every metric name from scratch.
            assertNotNull(lookupHistogram("RequestLatencyMs", "Produce"));
        } finally {
            if (fresh != null) fresh.close();
        }
        // Setting metrics = null tells tearDown there's nothing to close anymore.
        metrics = null;
    }

    @Test
    void rejectsNullLimiters() {
        // A null limiter would NPE on the first gauge read. Fail fast at construction so the wiring layer can't
        // accidentally hand in null for either streaming path.
        assertThrows(NullPointerException.class, () -> new HttpBridgeMetrics(null, wsLimiter));
        assertThrows(NullPointerException.class, () -> new HttpBridgeMetrics(limiter, null));
    }

    @Test
    void closeUnregistersRemainingMetricsEvenWhenOneRemovalThrows() {
        // The Yammer registry exposes a listener hook fired from `removeMetric(MetricName)` — the listener call site
        // (`notifyMetricRemoved`) has no try/catch, so a listener that throws propagates out of `removeMetric`. In
        // production, a JMX MBean deregistration backed by this hook can throw (`InstanceNotFoundException`,
        // `MBeanRegistrationException`) when the platform MBean server is under stress or has been tampered with.
        //
        // The hardened close() must NOT abort the loop on a single failure: every metric name HttpBridgeMetrics owns
        // must be unregistered so the next bridge generation can rebind a fresh metric. Yammer's silent dedup would
        // otherwise let the new bridge inherit a stranded gauge whose Supplier closed over the dead limiter of the
        // previous lifecycle — making JMX show stale `ActiveSseStreams` / `ActiveWsSubscriptions` forever.
        //
        // Inject a listener that throws on a single metric name during close(), verify that (a) close() rethrows the
        // first failure to the caller (so the lifecycle layer learns close was not clean), (b) every OTHER metric this
        // class owns has been unregistered. The stranded-metric scenario is the load-bearing contract — if it
        // regressed, JMX gauges would silently span lifecycle boundaries again.
        MetricName victim = name("RejectedAtSseCap", new LinkedHashMap<>());
        ThrowingListener listener = new ThrowingListener(victim);
        KafkaYammerMetrics.defaultRegistry().addListener(listener);
        try {
            RuntimeException caught = assertThrows(RuntimeException.class, () -> metrics.close());
            assertSame(listener.injected, caught,
                "close() must rethrow the first removal failure so the lifecycle layer learns close was not clean");

            // Every other metric the bridge owns must be gone — the close loop continued past the throwing victim.
            for (HttpBridgeMetrics.Operation op : HttpBridgeMetrics.Operation.values()) {
                Map<String, String> latencyTags = new LinkedHashMap<>();
                latencyTags.put("operation", op.tag);
                assertNotRegistered("RequestLatencyMs", latencyTags);
                for (String family : new String[] {"2xx", "4xx", "5xx", "other"}) {
                    Map<String, String> respTags = new LinkedHashMap<>();
                    respTags.put("operation", op.tag);
                    respTags.put("statusClass", family);
                    assertNotRegistered("ResponseCount", respTags);
                }
            }
            assertNotRegistered("RejectedOversizedBody");
            assertNotRegistered("SseStreamsOpened");
            assertNotRegistered("ActiveSseStreams");
            assertNotRegistered("RejectedAtWsCap");
            assertNotRegistered("WsSubscriptionsOpened");
            assertNotRegistered("ActiveWsSubscriptions");
        } finally {
            KafkaYammerMetrics.defaultRegistry().removeListener(listener);
            // The listener only throws on the victim's removal; the metrics ConcurrentMap.remove() already executed
            // (the throw is in the post-remove notify), so the victim is gone from the registry. No extra cleanup
            // needed beyond the listener removal.
        }
        // Setting metrics = null prevents tearDown from double-closing.
        metrics = null;
    }

    @Test
    void closeIsIdempotent() {
        // BrokerServer's stop sequence may be triggered from multiple paths during a crash; the second close()
        // must not blow up if every metric is already gone.
        metrics.close();
        metrics.close();
        metrics.close();
        // Setting metrics = null prevents tearDown from double-closing.
        metrics = null;
    }

    // --- Yammer registry helpers ---

    private static MetricName name(String n, Map<String, String> tags) {
        StringBuilder mbean = new StringBuilder(HttpBridgeMetrics.METRICS_GROUP)
            .append(":type=").append(HttpBridgeMetrics.METRICS_TYPE)
            .append(",name=").append(n);
        String scope = null;
        if (!tags.isEmpty()) {
            StringBuilder tagStr = new StringBuilder();
            for (Map.Entry<String, String> e : tags.entrySet()) {
                if (tagStr.length() > 0) tagStr.append(",");
                tagStr.append(e.getKey()).append("=").append(e.getValue());
            }
            mbean.append(",").append(tagStr);
            scope = scopeOf(tags);
        }
        return new MetricName(HttpBridgeMetrics.METRICS_GROUP, HttpBridgeMetrics.METRICS_TYPE, n, scope, mbean.toString());
    }

    private static String scopeOf(Map<String, String> tags) {
        // KafkaMetricsGroup builds scope from tags in declaration order. Reproduce that here so MetricName
        // equality matches the registry's view.
        StringBuilder s = new StringBuilder();
        for (Map.Entry<String, String> e : tags.entrySet()) {
            if (s.length() > 0) s.append(".");
            s.append(e.getKey()).append(".").append(e.getValue());
        }
        return s.toString();
    }

    private static Histogram lookupHistogram(String n, String operationTag) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("operation", operationTag);
        Object metric = KafkaYammerMetrics.defaultRegistry().allMetrics().get(name(n, tags));
        assertNotNull(metric, "expected histogram " + n + " with operation=" + operationTag);
        return (Histogram) metric;
    }

    private static Meter lookupMeter(String n, String operationTag, String statusClassTag) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("operation", operationTag);
        tags.put("statusClass", statusClassTag);
        Object metric = KafkaYammerMetrics.defaultRegistry().allMetrics().get(name(n, tags));
        assertNotNull(metric, "expected meter " + n + " with operation=" + operationTag
            + ",statusClass=" + statusClassTag);
        return (Meter) metric;
    }

    private static Meter lookupMeter(String n) {
        Object metric = KafkaYammerMetrics.defaultRegistry().allMetrics().get(name(n, new LinkedHashMap<>()));
        assertNotNull(metric, "expected meter " + n);
        return (Meter) metric;
    }

    private static Gauge<?> lookupGauge(String n) {
        Object metric = KafkaYammerMetrics.defaultRegistry().allMetrics().get(name(n, new LinkedHashMap<>()));
        assertNotNull(metric, "expected gauge " + n);
        return (Gauge<?>) metric;
    }

    private static void assertNotRegistered(String n) {
        assertNotRegistered(n, new LinkedHashMap<>());
    }

    private static void assertNotRegistered(String n, Map<String, String> tags) {
        Object metric = KafkaYammerMetrics.defaultRegistry().allMetrics().get(name(n, tags));
        assertNull(metric, "metric " + n + " " + tags + " should not be registered after close()");
    }

    /**
     * MetricsRegistryListener that throws on a single victim removal. Used to simulate a JMX deregistration failure
     * mid-close and prove the hardened close() loop continues past the throw.
     */
    private static final class ThrowingListener implements MetricsRegistryListener {
        private final MetricName victim;
        final RuntimeException injected = new RuntimeException("simulated JMX deregistration failure");

        ThrowingListener(MetricName victim) {
            this.victim = victim;
        }

        @Override
        public void onMetricAdded(MetricName name, Metric metric) {
            // The listener is installed AFTER the metrics are constructed, so add-events for those are not observed.
            // If a stray add slips through (e.g. test-order dependency), keep silent — we only care about removes.
        }

        @Override
        public void onMetricRemoved(MetricName name) {
            if (name.equals(victim)) {
                throw injected;
            }
        }
    }
}
