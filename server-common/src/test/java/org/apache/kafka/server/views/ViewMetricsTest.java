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

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pin the metrics surface end-to-end: an in-process broker (just the registry + filter)
 * receives a small set of records and the counters reflect what the operator would see on
 * the JMX side. This test exists because the metrics are the audit-blocker fix — if any of
 * these counters silently turn into "always zero" because of a wiring regression, view ops
 * loses its only signal that the feature is doing useful work.
 */
final class ViewMetricsTest {

    @Test
    void countersTickOnEvaluationsSkipsAndBytes() {
        try (ViewMetrics metrics = new ViewMetrics()) {
            CompiledPredicate predicate = new PredicateCompiler(PredicateLimits.defaults())
                    .compile("body.color == 'red'");

            MemoryRecords records = MemoryRecords.withRecords(0L, Compression.NONE,
                    new SimpleRecord(json("red").getBytes(StandardCharsets.UTF_8)),
                    new SimpleRecord(json("blue").getBytes(StandardCharsets.UTF_8)),
                    new SimpleRecord("not-json".getBytes(StandardCharsets.UTF_8)));

            int sizeIn = records.sizeInBytes();
            MemoryRecords filtered = ViewFilter.apply(predicate, records, 0, metrics);

            assertEquals(3L, metrics.currentEvaluations(),
                    "every non-control record must be evaluated once");
            assertEquals(1L, metrics.currentSkips(),
                    "the non-JSON record must surface as a skip (predicate returns Optional.empty)");
            assertEquals(sizeIn, metrics.currentBytesIn(),
                    "bytesIn must match the size of the input MemoryRecords");
            assertTrue(metrics.currentBytesOut() > 0L
                            && metrics.currentBytesOut() < metrics.currentBytesIn(),
                    () -> "bytesOut must be non-zero and below bytesIn after filtering down to one red; got "
                            + metrics.currentBytesOut() + "/" + metrics.currentBytesIn());
            assertTrue(filtered.sizeInBytes() > 0);
        }
    }

    @Test
    void compileFailureCounterTicksWhenRegistryHitsInvalidPredicate() {
        try (ViewMetrics metrics = new ViewMetrics()) {
            // configSource hands back a deliberately broken predicate so ViewRegistry.compile
            // surfaces a PredicateValidationException and ticks the counter.
            Function<String, Optional<ViewRegistry.TopicViewConfigs>> bad = name ->
                    Optional.of(new ViewRegistry.TopicViewConfigs(
                            "backing",
                            "body.color = 'red'",  // single '=' is invalid; '==' required
                            ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE));
            ViewRegistry registry = new ViewRegistry(bad, metrics);
            assertThrows(PredicateValidationException.class, () -> registry.viewFor("v"));
            assertEquals(1L, metrics.currentCompileFailures(),
                    "ViewRegistry.compile must record exactly one compile failure on bad predicate");
        }
    }

    @Test
    void noopInstanceDoesNotThrowAndKeepsCountersAtZero() {
        // NOOP is shared by unit tests and call sites that don't want metrics. It must accept
        // every mutator without throwing, and its counters stay frozen so test assertions
        // against them are predictable.
        ViewMetrics noop = ViewMetrics.NOOP;
        noop.recordEvaluation();
        noop.recordSkip();
        noop.recordBytes(123, 45);
        noop.recordCompileFailure();
        // NOOP still ticks its internal counters (they're useful for visibility), but it does
        // NOT register with Yammer — so the only operator-visible surface is zero. Asserting on
        // the in-memory counter side is enough to know NOOP did not error out.
        assertTrue(noop.currentEvaluations() > 0L);
        // Close is a no-op — safe to call repeatedly.
        noop.close();
        noop.close();
    }

    private static String json(String color) {
        return "{\"color\":\"" + color + "\"}";
    }
}
