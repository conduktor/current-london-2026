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
package org.apache.kafka.server.rules;

import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.server.rules.cel.CelCompiler;
import org.apache.kafka.server.rules.extract.ApiMessageActivation;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link RuleEngine} — the central object the broker uses to evaluate
 * a request against the currently-installed RuleSet.
 *
 * <p>This is the single component the {@code KafkaApis.handle()} interception
 * point talks to. The contract under test mirrors PROMPT.md's acceptance
 * criteria one-for-one:
 *
 * <ul>
 *   <li>Empty engine → fast-path ALLOW, no extractor invocation.</li>
 *   <li>Rule installed but targets a different api key → fast-path ALLOW,
 *       still no extractor invocation.</li>
 *   <li>Rule installed targeting this api key, predicate false → ALLOW.</li>
 *   <li>Rule installed targeting this api key, predicate true → DENY with
 *       the configured error code and rule id.</li>
 *   <li>Internal client-id prefix is unconditionally bypassed, even when
 *       a deny-all rule applies.</li>
 *   <li>Multiple rules on the same api key evaluate in declared order;
 *       the first matching DENY short-circuits subsequent evaluations.</li>
 *   <li>{@code install(RuleSet)} atomically swaps; concurrent readers
 *       always observe a fully-published snapshot.</li>
 * </ul>
 */
public class RuleEngineTest {

    private static Rule denyRule(String id, ApiKeys key, String cel, int errorCode) {
        return new Rule(id, Collections.singletonList(key), RuleAction.DENY, cel, errorCode, CelCompiler.compile(cel));
    }

    private static CreateTopicsRequestData createTopicsRequest(String... names) {
        CreatableTopicCollection topics = new CreatableTopicCollection();
        for (String n : names) {
            topics.add(new CreatableTopic().setName(n));
        }
        return new CreateTopicsRequestData().setTopics(topics);
    }

    private static Map<String, Object> activationFor(CreateTopicsRequestData req) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("request", ApiMessageActivation.from(req));
        return m;
    }

    @Test
    public void emptyEngineAllowsEverythingOnFastPath() {
        RuleEngine engine = new RuleEngine();
        AtomicBoolean extracted = new AtomicBoolean(false);
        RuleDecision decision = engine.evaluate(
            ApiKeys.CREATE_TOPICS, "external-client",
            recordingSupplier(extracted));
        assertSame(RuleDecision.ALLOW, decision);
        assertFalse(extracted.get(), "no rule targets the api key — extractor must not run");
    }

    @Test
    public void ruleTargetingDifferentApiKeyDoesNotTriggerExtraction() {
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("r1", ApiKeys.FETCH, "true", 99))
            .build());
        AtomicBoolean extracted = new AtomicBoolean(false);
        RuleDecision decision = engine.evaluate(
            ApiKeys.METADATA, "external-client",
            recordingSupplier(extracted));
        assertSame(RuleDecision.ALLOW, decision);
        assertFalse(extracted.get(),
            "bitset says no rule targets METADATA — must not allocate or invoke extractor");
    }

    private static java.util.function.Supplier<Map<String, Object>> recordingSupplier(AtomicBoolean flag) {
        return () -> {
            flag.set(true);
            return Collections.emptyMap();
        };
    }

    @Test
    public void ruleEvaluatesWhenApiKeyMatchesAndPredicateIsTrue() {
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("block-audit", ApiKeys.CREATE_TOPICS,
                "request.topics.exists(t, t.name.startsWith(\"audit-\"))", 42))
            .build());
        RuleDecision decision = engine.evaluate(
            ApiKeys.CREATE_TOPICS, "external-client",
            () -> activationFor(createTopicsRequest("audit-events", "metrics")));
        assertTrue(decision.denied());
        assertEquals(42, decision.errorCode());
        assertEquals("block-audit", decision.denyingRuleId());
    }

    @Test
    public void ruleEvaluatesAllowWhenPredicateIsFalse() {
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("block-audit", ApiKeys.CREATE_TOPICS,
                "request.topics.exists(t, t.name.startsWith(\"audit-\"))", 42))
            .build());
        RuleDecision decision = engine.evaluate(
            ApiKeys.CREATE_TOPICS, "external-client",
            () -> activationFor(createTopicsRequest("metrics", "events")));
        assertSame(RuleDecision.ALLOW, decision);
    }

    @Test
    public void firstMatchingDenyShortCircuitsAndReturnsItsErrorCode() {
        // Multiple DENY rules on the same api key. Both match. The first one
        // wins by declared order — its error code propagates, the second's does not.
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("first", ApiKeys.CREATE_TOPICS, "true", 100))
            .put(denyRule("second", ApiKeys.CREATE_TOPICS, "true", 200))
            .build());
        RuleDecision decision = engine.evaluate(
            ApiKeys.CREATE_TOPICS, "external-client",
            () -> Collections.singletonMap("request", Collections.emptyMap()));
        assertTrue(decision.denied());
        assertEquals(100, decision.errorCode());
        assertEquals("first", decision.denyingRuleId());
    }

    @Test
    public void firstFalseDoesNotPreventSubsequentRulesFromFiring() {
        // Counterpart to the above: the *first matching* rule, not the
        // first rule period, wins. A leading false rule must not block
        // a subsequent true rule.
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("never", ApiKeys.CREATE_TOPICS, "false", 10))
            .put(denyRule("always", ApiKeys.CREATE_TOPICS, "true", 20))
            .build());
        RuleDecision decision = engine.evaluate(
            ApiKeys.CREATE_TOPICS, "external-client",
            () -> Collections.singletonMap("request", Collections.emptyMap()));
        assertTrue(decision.denied());
        assertEquals(20, decision.errorCode());
        assertEquals("always", decision.denyingRuleId());
    }

    @Test
    public void internalClientPrefixIsAlwaysExemptEvenUnderDenyAll() {
        // Bootstrap-safety: a "deny all" rule must not block the broker's own
        // governance-topic reader from refilling the rule set. We identify the
        // reader by its client-id prefix.
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("deny-all-create-topics", ApiKeys.CREATE_TOPICS, "true", 1))
            .put(denyRule("deny-all-fetch", ApiKeys.FETCH, "true", 2))
            .put(denyRule("deny-all-metadata", ApiKeys.METADATA, "true", 3))
            .build());
        for (ApiKeys k : new ApiKeys[]{ApiKeys.CREATE_TOPICS, ApiKeys.FETCH, ApiKeys.METADATA}) {
            assertSame(RuleDecision.ALLOW,
                engine.evaluate(k, RuleEngine.INTERNAL_CLIENT_ID_PREFIX + "loader",
                    () -> Collections.emptyMap()),
                "internal client id must be exempt on api key " + k);
        }
        assertTrue(engine.evaluate(ApiKeys.FETCH, "regular-client",
            () -> Collections.emptyMap()).denied(),
            "non-internal client still subject to rules");
    }

    @Test
    public void nullOrEmptyClientIdIsNotExempt() {
        // Defensive: an absent client-id must NOT bypass rules. The exemption
        // is granted strictly via the named prefix.
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("deny", ApiKeys.METADATA, "true", 1))
            .build());
        assertTrue(engine.evaluate(ApiKeys.METADATA, null, () -> Collections.emptyMap()).denied());
        assertTrue(engine.evaluate(ApiKeys.METADATA, "", () -> Collections.emptyMap()).denied());
    }

    @Test
    public void activationSupplierIsInvokedAtMostOncePerEvaluation() {
        // The engine builds the activation lazily, on first need. Subsequent
        // rules on the same evaluation must reuse it — otherwise reflection
        // cost is multiplied by rule count.
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("r1", ApiKeys.CREATE_TOPICS, "false", 1))
            .put(denyRule("r2", ApiKeys.CREATE_TOPICS, "false", 2))
            .put(denyRule("r3", ApiKeys.CREATE_TOPICS, "false", 3))
            .build());
        AtomicInteger supplierCalls = new AtomicInteger(0);
        engine.evaluate(ApiKeys.CREATE_TOPICS, "client", () -> {
            supplierCalls.incrementAndGet();
            return Collections.singletonMap("request", Collections.emptyMap());
        });
        assertEquals(1, supplierCalls.get(),
            "activation supplier must be memoised across the rules on one evaluation");
    }

    @Test
    public void installSwapIsAtomicForConcurrentReaders() throws Exception {
        // Spec: concurrent readers always observe one of the fully-published
        // RuleSets, never a partially-constructed state. We exercise this by
        // hammering the engine from many threads while a writer flips
        // between two distinct, internally-consistent rule sets.
        RuleEngine engine = new RuleEngine();
        RuleSet a = new RuleSetBuilder()
            .put(denyRule("a", ApiKeys.METADATA, "true", 11))
            .build();
        RuleSet b = new RuleSetBuilder()
            .put(denyRule("b", ApiKeys.METADATA, "true", 22))
            .build();
        engine.install(a);

        int readers = 8;
        int iterations = 10_000;
        ExecutorService pool = Executors.newFixedThreadPool(readers + 1);
        CountDownLatch ready = new CountDownLatch(readers);
        CountDownLatch start = new CountDownLatch(1);
        AtomicBoolean failed = new AtomicBoolean(false);

        for (int r = 0; r < readers; r++) {
            pool.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                    for (int i = 0; i < iterations; i++) {
                        RuleDecision d = engine.evaluate(
                            ApiKeys.METADATA, "client",
                            () -> Collections.emptyMap());
                        if (!d.denied() || (d.errorCode() != 11 && d.errorCode() != 22)) {
                            failed.set(true);
                            return;
                        }
                    }
                } catch (Throwable t) {
                    failed.set(true);
                }
            });
        }
        pool.submit(() -> {
            try {
                start.await();
                for (int i = 0; i < iterations; i++) {
                    engine.install(i % 2 == 0 ? a : b);
                }
            } catch (Throwable t) {
                failed.set(true);
            }
        });

        ready.await();
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(15, TimeUnit.SECONDS), "pool should finish");
        assertFalse(failed.get(), "no reader should observe a partially-built RuleSet");
    }

    @Test
    public void multiKeyRuleFiresOnEachTargetedApi() {
        Rule cross = new Rule("c",
            Arrays.asList(ApiKeys.METADATA, ApiKeys.FETCH),
            RuleAction.DENY, "true", 7, CelCompiler.compile("true"));
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder().put(cross).build());
        assertTrue(engine.evaluate(ApiKeys.METADATA, "x", () -> Collections.emptyMap()).denied());
        assertTrue(engine.evaluate(ApiKeys.FETCH, "x", () -> Collections.emptyMap()).denied());
        assertSame(RuleDecision.ALLOW,
            engine.evaluate(ApiKeys.CREATE_TOPICS, "x", () -> Collections.emptyMap()));
    }

    @Test
    public void buggyPredicateThrowingErrorAlsoFailsOpenAndDoesNotKillRequestThread() {
        // CEL is hand-rolled and a pathological program could in principle raise
        // an Error (e.g. StackOverflowError on a deep comprehension), not just a
        // RuntimeException. The engine must catch Throwable so the request
        // thread is never killed by a buggy rule. We simulate via a CelProgram
        // Mockito mock that raises an Error inside evalBoolean.
        org.apache.kafka.server.rules.cel.CelProgram crashy =
            org.mockito.Mockito.mock(org.apache.kafka.server.rules.cel.CelProgram.class);
        org.mockito.Mockito.when(crashy.evalBoolean(org.mockito.ArgumentMatchers.any()))
            .thenThrow(new StackOverflowError("simulated runaway recursion"));
        Rule throwing = new Rule(
            "throwy",
            Collections.singletonList(ApiKeys.METADATA),
            RuleAction.DENY,
            "true",
            7,
            crashy);
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(throwing)
            .put(denyRule("after", ApiKeys.METADATA, "true", 99))
            .build());
        RuleDecision d = engine.evaluate(
            ApiKeys.METADATA, "x", () -> Collections.singletonMap("request", Collections.emptyMap()));
        assertTrue(d.denied());
        assertEquals(99, d.errorCode());
        assertEquals("after", d.denyingRuleId());
    }

    @Test
    public void mayDenyIsFalseWhenNoRuleTargetsTheApiKey() {
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("fetch-only", ApiKeys.FETCH, "true", 7))
            .build());
        assertFalse(engine.mayDeny(ApiKeys.METADATA, "client"),
            "no rule targets METADATA — gate must not allocate on the request path");
        assertTrue(engine.mayDeny(ApiKeys.FETCH, "client"),
            "rule targets FETCH — gate opens the slow path");
    }

    @Test
    public void mayDenyIsFalseForInternalClientIdEvenWhenRuleTargetsApiKey() {
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("deny-all", ApiKeys.METADATA, "true", 7))
            .build());
        assertFalse(engine.mayDeny(ApiKeys.METADATA,
                RuleEngine.INTERNAL_CLIENT_ID_PREFIX + "self"),
            "the internal-client bypass must short-circuit the gate too");
    }

    @Test
    public void mayDenyIsFalseOnEmptyEngine() {
        assertFalse(new RuleEngine().mayDeny(ApiKeys.METADATA, "client"));
        assertFalse(new RuleEngine().mayDeny(ApiKeys.METADATA, null));
    }

    @Test
    public void buggyPredicateFailsOpenAndDoesNotBlockSubsequentRules() {
        // A predicate that throws at evaluation (e.g. divide-by-zero) must
        // not crash the request path. The engine treats the throwing rule
        // as ALLOW (fail-open: the broker stays available even if a rule
        // is buggy) and proceeds to the next rule.
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("explody", ApiKeys.METADATA, "1 / 0 == 0", 1))
            .put(denyRule("after", ApiKeys.METADATA, "true", 99))
            .build());
        RuleDecision d = engine.evaluate(
            ApiKeys.METADATA, "x", () -> Collections.singletonMap("request", Collections.emptyMap()));
        assertTrue(d.denied());
        assertEquals(99, d.errorCode());
        assertEquals("after", d.denyingRuleId());
    }
}
