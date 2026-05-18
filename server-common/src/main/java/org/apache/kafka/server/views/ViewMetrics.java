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
package org.apache.kafka.server.views;

import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Meter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Broker-side JMX/Yammer meters for the view feature. Wired by {@code KafkaApis} during broker
 * startup, threaded into {@link ViewRegistry} and {@link ViewFilter#apply(CompiledPredicate,
 * org.apache.kafka.common.record.MemoryRecords, int, ViewMetrics)} so the fetch hot path can
 * record what views are actually doing.
 *
 * Metric names live under {@code kafka.server:type=ViewMetrics,name=...} to match the rest of
 * the broker's Yammer conventions. The names are intentionally stable:
 *
 *   PredicateEvalRate   — records the predicate was asked to evaluate. Useful as the denominator
 *                         for skip rate and as a baseline rate signal.
 *   PredicateSkipRate   — records the predicate skipped (Optional.empty from
 *                         {@link CompiledPredicate#evaluate}: cost-cap hit, malformed JSON,
 *                         non-boolean result, etc.). Anomaly = operator action.
 *   FilterBytesIn       — bytes entering ViewFilter per second (pre-filter MemoryRecords size).
 *                         Pair with FilterBytesOut to estimate the dedup ratio per view.
 *   FilterBytesOut      — bytes emitted by ViewFilter per second.
 *   CompileFailures     — predicate compilations that failed inside ViewRegistry. Should be ~0
 *                         in steady state — LogConfig rejects bad predicates at config-set time,
 *                         so a non-zero rate here means validation was bypassed or a config
 *                         rolled forward to a broker on a different code version.
 *
 * Thread-safety: Yammer Meters are thread-safe. Internal counters use {@link LongAdder} for
 * cheap concurrent increment under load.
 *
 * Lifecycle: callers must invoke {@link #close()} on shutdown to deregister meters from the
 * Yammer singleton registry — otherwise the next broker instance in the same JVM (mainly tests)
 * sees stale, doubled-up readings.
 */
public final class ViewMetrics implements AutoCloseable {

    /** Single sentinel for code paths (unit tests, the unwired filter overload) that don't have a real metrics wired in. */
    public static final ViewMetrics NOOP = new ViewMetrics(true);

    private final KafkaMetricsGroup group;
    private final Meter evalMeter;
    private final Meter skipMeter;
    private final Meter bytesInMeter;
    private final Meter bytesOutMeter;
    private final Meter compileFailureMeter;
    private final boolean noop;

    // Accumulators backing the meters; LongAdder lets the hot path tick without locking the meter.
    // The values are surfaced live via the meters themselves; these adders are diagnostic-only
    // (visible via test-side `current*()` accessors).
    private final LongAdder evalAdder = new LongAdder();
    private final LongAdder skipAdder = new LongAdder();
    private final LongAdder bytesInAdder = new LongAdder();
    private final LongAdder bytesOutAdder = new LongAdder();
    private final LongAdder compileFailureAdder = new LongAdder();

    public ViewMetrics() {
        this(false);
    }

    private ViewMetrics(boolean noop) {
        this.noop = noop;
        if (noop) {
            this.group = null;
            this.evalMeter = null;
            this.skipMeter = null;
            this.bytesInMeter = null;
            this.bytesOutMeter = null;
            this.compileFailureMeter = null;
            return;
        }
        // Use the canonical broker metrics group so existing dashboards already pointed at
        // `kafka.server:type=...` discover view metrics without any reporter changes.
        this.group = new KafkaMetricsGroup("kafka.server", "ViewMetrics");
        this.evalMeter = group.newMeter("PredicateEvalRate", "records", TimeUnit.SECONDS);
        this.skipMeter = group.newMeter("PredicateSkipRate", "records", TimeUnit.SECONDS);
        this.bytesInMeter = group.newMeter("FilterBytesIn", "bytes", TimeUnit.SECONDS);
        this.bytesOutMeter = group.newMeter("FilterBytesOut", "bytes", TimeUnit.SECONDS);
        this.compileFailureMeter = group.newMeter("CompileFailures", "compiles", TimeUnit.SECONDS);
    }

    /** Record one predicate evaluation. Called from the filter hot path. */
    public void recordEvaluation() {
        evalAdder.increment();
        if (!noop) evalMeter.mark();
    }

    /** Record a predicate skip (Optional.empty from evaluate). Called from the filter hot path. */
    public void recordSkip() {
        skipAdder.increment();
        if (!noop) skipMeter.mark();
    }

    /** Record bytes entering and leaving ViewFilter for one fetch. */
    public void recordBytes(long bytesIn, long bytesOut) {
        if (bytesIn > 0) {
            bytesInAdder.add(bytesIn);
            if (!noop) bytesInMeter.mark(bytesIn);
        }
        if (bytesOut > 0) {
            bytesOutAdder.add(bytesOut);
            if (!noop) bytesOutMeter.mark(bytesOut);
        }
    }

    /** Record one predicate compile failure inside ViewRegistry. */
    public void recordCompileFailure() {
        compileFailureAdder.increment();
        if (!noop) compileFailureMeter.mark();
    }

    // ---- test-visible counters ----

    public long currentEvaluations() {
        return evalAdder.sum();
    }

    public long currentSkips() {
        return skipAdder.sum();
    }

    public long currentBytesIn() {
        return bytesInAdder.sum();
    }

    public long currentBytesOut() {
        return bytesOutAdder.sum();
    }

    public long currentCompileFailures() {
        return compileFailureAdder.sum();
    }

    /**
     * Snapshot of all current counters, mainly for human-readable assertions in tests and for
     * debug logging. Not part of the metrics surface — operators read the Yammer meters via JMX.
     */
    public Map<String, Long> snapshot() {
        Map<String, Long> out = new HashMap<>();
        out.put("evaluations", currentEvaluations());
        out.put("skips", currentSkips());
        out.put("bytesIn", currentBytesIn());
        out.put("bytesOut", currentBytesOut());
        out.put("compileFailures", currentCompileFailures());
        return Collections.unmodifiableMap(out);
    }

    @Override
    public void close() {
        if (noop || group == null) return;
        // Remove from the Yammer singleton registry so subsequent brokers in the same JVM
        // (test harness reuse) get clean readings.
        group.removeMetric("PredicateEvalRate");
        group.removeMetric("PredicateSkipRate");
        group.removeMetric("FilterBytesIn");
        group.removeMetric("FilterBytesOut");
        group.removeMetric("CompileFailures");
    }
}
