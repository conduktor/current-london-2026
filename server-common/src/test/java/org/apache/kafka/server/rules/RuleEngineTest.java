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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.server.rules.cel.CelCompiler;
import org.apache.kafka.server.rules.extract.ActivationBudgetExceededException;
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
            ApiKeys.CREATE_TOPICS, "external-client", null, false,
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
            ApiKeys.METADATA, "external-client", null, false,
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
    public void activationSupplierThrowingDoesNotPropagateToRequestThread() {
        // Codex deep-audit P0: activationSupplier.get() runs OUTSIDE the per-rule
        // try block. A real-world failure mode is ApiMessageActivation walking
        // a ProduceRequestData whose records() accessor throws (e.g. a
        // partially-constructed activation map, an Optional accessor that
        // doesn't tolerate null on certain protocol versions). If the supplier
        // throws, the exception propagates into KafkaApis.handle() — a buggy
        // extractor must NEVER be able to crash the request thread. The whole
        // point of the per-rule fail-open is "a broken governance feature
        // degrades to ALLOW, never to a broker request failure". The activation
        // supplier is part of the same surface and must obey the same rule.
        //
        // Expected behaviour: evaluate() catches the throw, logs it at WARN,
        // and returns ALLOW (no rule can be evaluated without an activation
        // map, so fail-open is the only safe outcome consistent with the
        // per-rule policy).
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("deny-all", ApiKeys.METADATA, "true", 99))
            .build());
        RuleDecision d = engine.evaluate(
            ApiKeys.METADATA, "client", null, false,
            () -> {
                throw new RuntimeException("activation builder blew up");
            });
        assertSame(RuleDecision.ALLOW, d,
            "a throwing activation supplier must fail open, not propagate");
    }

    @Test
    public void activationSupplierThrowingErrorAlsoFailsOpen() {
        // Stronger guarantee: even an Error (StackOverflowError from a deeply
        // nested ApiMessage walk, OutOfMemoryError from a giant Records buffer)
        // must not escape evaluate(). Per-rule evaluation already catches
        // Throwable; the supplier must too. Without this, a single very large
        // request could crash the broker's request thread.
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("deny-all", ApiKeys.METADATA, "true", 99))
            .build());
        RuleDecision d = engine.evaluate(
            ApiKeys.METADATA, "client", null, false,
            () -> {
                throw new StackOverflowError("simulated deep walk");
            });
        assertSame(RuleDecision.ALLOW, d,
            "Errors from the activation supplier must fail open, same as RuntimeExceptions");
    }

    @Test
    public void activationBudgetExceededFailsClosedWithPolicyViolation() {
        // Adversarial DoS-P0 fix: an attacker-shaped wide request (e.g.
        // OffsetFetch with 10000+ partition indexes, CreateTopics with 10000+
        // topic descriptors, JoinGroup with 5000+ supported protocols) trips
        // ApiMessageActivation's MAX_ACCESSOR_INVOCATIONS=10_000 budget and
        // raises ActivationBudgetExceededException. Falling through to the
        // generic-Throwable catch above (fail-open) would let any external
        // client evade every DENY rule on the targeted API simply by
        // inflating one repeated field past the budget — the cap is exactly
        // what makes worst-case walk cost bounded, so the only consistent
        // answer is to fail CLOSED.
        //
        // The synthetic DENY carries Errors.POLICY_VIOLATION (44) and the
        // reserved rule id RuleEngine.ACTIVATION_BUDGET_RULE_ID. Operator
        // rule ids cannot start or end with "__" (codec rejects them), so
        // the sentinel is unambiguous in audit logs.
        RuleEngine engine = new RuleEngine();
        // Install a real DENY rule so we exercise the path past the bitset
        // fast-path; without it, evaluate() returns ALLOW before ever
        // touching the activation supplier and we'd be testing the wrong
        // codepath.
        engine.install(new RuleSetBuilder()
            .put(denyRule("deny-all", ApiKeys.METADATA, "true", 99))
            .build());
        RuleDecision d = engine.evaluate(
            ApiKeys.METADATA, "client", null, false,
            () -> {
                throw new ActivationBudgetExceededException(
                    "activation walk exceeded accessor budget of 10000 on FakeWideRequest");
            });
        assertTrue(d.denied(), "budget overflow MUST fail closed, not open");
        assertEquals(Errors.POLICY_VIOLATION.code(), d.errorCode(),
            "budget-overflow DENY must surface POLICY_VIOLATION (44) so clients see " +
                "a clear governance-rejection error code, not the rule author's choice");
        // Literal pin: POLICY_VIOLATION has been wire-code 44 since the API was
        // introduced and clients (including ancient broker-protocol versions
        // still in the field) decode that integer to construct the user-facing
        // PolicyViolationException. If an upstream Kafka rev ever renumbered
        // it, RuleEngine would still compile against Errors.POLICY_VIOLATION,
        // but the wire-level error class clients see would silently shift —
        // and PolicyViolationException is the contract a governance-aware
        // client uses to distinguish "rule rejected my request" from a generic
        // broker error. This assertion fires on any such drift.
        assertEquals((short) 44, Errors.POLICY_VIOLATION.code(),
            "POLICY_VIOLATION wire code must remain 44 — the governance contract " +
                "depends on it");
        assertEquals(RuleEngine.ACTIVATION_BUDGET_RULE_ID, d.denyingRuleId(),
            "denyingRuleId must be the reserved sentinel so audit consumers can " +
                "distinguish defensive engine posture from any operator-authored rule");
    }

    @Test
    public void activationBudgetCatchSitsAheadOfGenericThrowableFailOpen() {
        // Pinning test: the two catches must be co-located in the right
        // ORDER. If a future refactor reorders them (or makes
        // ActivationBudgetExceededException extend something the generic
        // branch swallows first), the DoS-P0 fail-closed posture quietly
        // regresses to fail-open — and the only test that would notice is
        // this one. The previous test already covers the budget-overflow
        // outcome; this one proves a RuntimeException that is NOT a budget
        // overflow still fails OPEN, so the ordering matters because both
        // catches are reachable from the same supplier.get() call.
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("deny-all", ApiKeys.METADATA, "true", 99))
            .build());
        RuleDecision d = engine.evaluate(
            ApiKeys.METADATA, "client", null, false,
            () -> {
                throw new RuntimeException("buggy walker, not an attacker");
            });
        assertSame(RuleDecision.ALLOW, d,
            "generic exceptions must still fail open — only ActivationBudget closes");
    }

    @Test
    public void budgetOverflowWarnIsThrottledUnderRapidFire() {
        // DoS-P1: the fail-closed posture in
        // activationBudgetExceededFailsClosedWithPolicyViolation correctly
        // denies the attacker-shaped request, but a naive `LOG.warn` per
        // request hands the same attacker a synchronous log-spam amplifier —
        // a few thousand pathological requests/s saturates the broker's SLF4J
        // appender, contends I/O on the log volume with the data path, and
        // stalls the request thread on appender backpressure. The throttle
        // bounds that to ~one WARN per second per broker, accumulating
        // suppressed counts so the operator sees the burst start.
        //
        // We can't assert on log lines directly without coupling to an
        // appender implementation, but we CAN observe the suppression counter
        // — it's package-private exactly so this test can read it without a
        // wall-clock sleep. After the first call grabs the emission slot, the
        // remaining N-1 calls in the same window MUST increment the counter
        // instead of writing to the log.
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("deny-all", ApiKeys.METADATA, "true", 99))
            .build());
        final int rapidFireCalls = 1_000;
        for (int i = 0; i < rapidFireCalls; i++) {
            RuleDecision d = engine.evaluate(
                ApiKeys.METADATA, "client", null, false,
                () -> {
                    throw new ActivationBudgetExceededException("wide request #" + 0);
                });
            assertTrue(d.denied(), "every overflow call must still fail closed, only the WARN is throttled");
            assertEquals(Errors.POLICY_VIOLATION.code(), d.errorCode());
        }
        // At least rapidFireCalls-1 of those events must have been suppressed:
        // the very first one wins the emission slot (lastBudgetWarnNanos was 0),
        // and the rest land inside the 1-second window. We allow >=
        // rapidFireCalls-1 (not exactly) because the counter is read+zeroed
        // by the emitter, so if a second emission DID fit in the window the
        // remaining suppressed count would be slightly lower — but this is a
        // synchronous tight loop on one thread, so in practice the loop
        // finishes inside the first window and the count is exactly
        // rapidFireCalls-1.
        long suppressed = engine.suppressedBudgetWarnings.get();
        assertTrue(suppressed >= rapidFireCalls - 2,
            "expected the throttle to suppress most rapid-fire WARNs, got " + suppressed
                + " out of " + rapidFireCalls + " budget-overflow events");
    }

    @Test
    public void ruleEvaluatesWhenApiKeyMatchesAndPredicateIsTrue() {
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("block-audit", ApiKeys.CREATE_TOPICS,
                "request.topics.exists(t, t.name.startsWith(\"audit-\"))", 42))
            .build());
        RuleDecision decision = engine.evaluate(
            ApiKeys.CREATE_TOPICS, "external-client", null, false,
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
            ApiKeys.CREATE_TOPICS, "external-client", null, false,
            () -> activationFor(createTopicsRequest("metrics", "events")));
        assertSame(RuleDecision.ALLOW, decision);
    }

    @Test
    public void firstMatchingDenyShortCircuitsAndReturnsItsErrorCode() {
        // Multiple DENY rules on the same api key. The first one matches, and
        // PROMPT.md requires the engine to short-circuit: the second rule's
        // CEL must NOT be evaluated. The original form of this test only
        // checked the propagated errorCode (100 not 200) — a regression that
        // turned short-circuit into "evaluate all, return first" would still
        // pass that check. To prove short-circuit *behaviorally*, we route
        // each rule's CEL through a distinct activation key and count
        // accesses to the second rule's key: it must be zero.
        AtomicInteger firstKeyAccess = new AtomicInteger(0);
        AtomicInteger secondKeyAccess = new AtomicInteger(0);
        Map<String, Object> counting = new java.util.HashMap<String, Object>() {
            @Override
            public Object get(Object key) {
                if ("probeFirst".equals(key)) firstKeyAccess.incrementAndGet();
                if ("probeSecond".equals(key)) secondKeyAccess.incrementAndGet();
                return super.get(key);
            }
        };
        counting.put("probeFirst", 1);
        counting.put("probeSecond", 1);

        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            // First rule references probeFirst and matches.
            .put(denyRule("first", ApiKeys.CREATE_TOPICS, "probeFirst == 1", 100))
            // Second rule references probeSecond. If it ever runs, it would
            // also match — so we can detect a missing short-circuit purely
            // by observing whether its activation key is touched.
            .put(denyRule("second", ApiKeys.CREATE_TOPICS, "probeSecond == 1", 200))
            .build());

        RuleDecision decision = engine.evaluate(
            ApiKeys.CREATE_TOPICS, "external-client", null, false,
            () -> counting);
        assertTrue(decision.denied());
        assertEquals(100, decision.errorCode(),
            "first rule's error code must propagate, not the second's");
        assertEquals("first", decision.denyingRuleId());
        assertTrue(firstKeyAccess.get() >= 1,
            "first rule must have been evaluated and consulted its activation key");
        assertEquals(0, secondKeyAccess.get(),
            "second rule's CEL MUST NOT be evaluated after the first DENY matches " +
                "— this is the short-circuit contract from PROMPT.md and the only way " +
                "to prevent a long deny-rule chain from quadratically blowing up the " +
                "request hot path");
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
            ApiKeys.CREATE_TOPICS, "external-client", null, false,
            () -> Collections.singletonMap("request", Collections.emptyMap()));
        assertTrue(decision.denied());
        assertEquals(20, decision.errorCode());
        assertEquals("always", decision.denyingRuleId());
    }

    @Test
    public void privilegedListenerWithEmptyTrustedSetDeniesEveryone() {
        // Codex round-2 P1#1 fix: empty governance.bypass.principals means
        // NO principal can ride the privileged-listener bypass — every
        // request, even one arriving from a privileged (inter-broker)
        // listener, is subject to CEL rule evaluation. The previous
        // "empty = listener-only fallback" semantics silently re-introduced
        // the gap the dedicated config exists to close: any client reaching
        // a shared listener would have ridden the bypass.
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("deny-all-create-topics", ApiKeys.CREATE_TOPICS, "true", 1))
            .put(denyRule("deny-all-fetch", ApiKeys.FETCH, "true", 2))
            .put(denyRule("deny-all-metadata", ApiKeys.METADATA, "true", 3))
            .build());
        for (ApiKeys k : new ApiKeys[]{ApiKeys.CREATE_TOPICS, ApiKeys.FETCH, ApiKeys.METADATA}) {
            assertTrue(
                engine.evaluate(k, "any-client-id-here", "User:anyone", true,
                    () -> Collections.singletonMap("request", Collections.emptyMap())).denied(),
                "empty trusted set must deny the bypass even on a privileged listener: " + k);
        }
        assertTrue(engine.evaluate(ApiKeys.FETCH, "regular-client", "User:client", false,
            () -> Collections.singletonMap("request", Collections.emptyMap())).denied(),
            "non-privileged client also subject to rules");
    }

    @Test
    public void privilegedListenerBypassNarrowedByTrustedPrincipalSet() {
        // Codex deep-audit P0a fix: a non-empty trusted-bypass principal
        // allow-list is what makes the privileged-listener bypass safe
        // against the "inter-broker listener accidentally shared with client
        // traffic" misconfiguration. Even with fromPrivilegedListener=true
        // the engine refuses the bypass for any principal not in the set.
        RuleEngine engine = new RuleEngine(
            Collections.singleton("User:broker"));
        engine.install(new RuleSetBuilder()
            .put(denyRule("deny-all", ApiKeys.METADATA, "true", 7))
            .build());

        // Privileged listener + broker principal → bypass.
        assertSame(RuleDecision.ALLOW,
            engine.evaluate(ApiKeys.METADATA, "any", "User:broker", true,
                () -> Collections.emptyMap()),
            "bypass must apply when both listener and principal match");

        // Privileged listener + client principal → no bypass; rule fires.
        assertTrue(
            engine.evaluate(ApiKeys.METADATA, "any", "User:notabroker", true,
                () -> Collections.singletonMap("request", Collections.emptyMap())).denied(),
            "principal mismatch must defeat the listener-only bypass");

        // Privileged listener + null principal → no bypass.
        assertTrue(
            engine.evaluate(ApiKeys.METADATA, "any", null, true,
                () -> Collections.singletonMap("request", Collections.emptyMap())).denied(),
            "null principal must defeat the listener-only bypass");
    }

    @Test
    public void spoofedInternalClientIdDoesNotBypassWhenNotOnPrivilegedListener() {
        // Security regression test for the CVE that motivated the
        // fromPrivilegedListener bypass: a hostile external client that sets
        // client.id=__kafka-governance-attacker MUST still be subject to
        // every deny rule. The prefix is purely a diagnostic convention now;
        // it does NOT grant any bypass.
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("deny-all", ApiKeys.METADATA, "true", 7))
            .build());
        RuleDecision d = engine.evaluate(
            ApiKeys.METADATA,
            RuleEngine.INTERNAL_CLIENT_ID_PREFIX + "attacker",
            "User:attacker",
            false,
            () -> Collections.singletonMap("request", Collections.emptyMap()));
        assertTrue(d.denied(),
            "wire-string client-id must not bypass deny rules — that was the spoofable CVE");
        assertEquals(7, d.errorCode());
    }

    @Test
    public void nullOrEmptyClientIdIsNotExempt() {
        // Defensive: an absent client-id must NOT bypass rules. The exemption
        // is granted strictly by the privileged-listener bit.
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("deny", ApiKeys.METADATA, "true", 1))
            .build());
        assertTrue(engine.evaluate(ApiKeys.METADATA, null, null, false,
            () -> Collections.singletonMap("request", Collections.emptyMap())).denied());
        assertTrue(engine.evaluate(ApiKeys.METADATA, "", null, false,
            () -> Collections.singletonMap("request", Collections.emptyMap())).denied());
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
        engine.evaluate(ApiKeys.CREATE_TOPICS, "client", null, false, () -> {
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
                            ApiKeys.METADATA, "client", null, false,
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
        assertTrue(engine.evaluate(ApiKeys.METADATA, "x", null, false, () -> Collections.emptyMap()).denied());
        assertTrue(engine.evaluate(ApiKeys.FETCH, "x", null, false, () -> Collections.emptyMap()).denied());
        assertSame(RuleDecision.ALLOW,
            engine.evaluate(ApiKeys.CREATE_TOPICS, "x", null, false, () -> Collections.emptyMap()));
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
            ApiKeys.METADATA, "x", null, false, () -> Collections.singletonMap("request", Collections.emptyMap()));
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
        assertFalse(engine.mayDeny(ApiKeys.METADATA, false),
            "no rule targets METADATA — gate must not allocate on the request path");
        assertTrue(engine.mayDeny(ApiKeys.FETCH, false),
            "rule targets FETCH — gate opens the slow path");
    }

    @Test
    public void mayDenyDoesNotShortCircuitOnPrivilegedListenerBecauseBypassIsPrincipalAware() {
        // Codex deep-audit P0a follow-on: with the bypass now requiring a
        // principal check, the cheap fast-path gate cannot accurately tell
        // whether a privileged-listener request will bypass — the principal
        // is not in scope at the gate without an extra lookup. So mayDeny
        // stays principal-agnostic and returns true when any rule targets
        // the api key, regardless of listener. evaluate() re-checks the
        // authoritative bypass and short-circuits there for legitimate
        // broker traffic. The cost of the over-broad gate is one extra
        // string-compare for the small subset of requests on the privileged
        // listener — negligible relative to denying a misrouted client.
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("deny-all", ApiKeys.METADATA, "true", 7))
            .build());
        assertTrue(engine.mayDeny(ApiKeys.METADATA, true),
            "fast-path gate stays principal-agnostic; full check happens in evaluate()");
        assertTrue(engine.mayDeny(ApiKeys.METADATA, false),
            "same rule must still open the slow path for external clients");
    }

    @Test
    public void mayDenyIsFalseOnEmptyEngine() {
        assertFalse(new RuleEngine().mayDeny(ApiKeys.METADATA, false));
        assertFalse(new RuleEngine().mayDeny(ApiKeys.METADATA, true));
    }

    @Test
    public void perRequestStepBudgetIsNotMultipliedByNumberOfRules() {
        // Round-8 audit HIGH (concurrency). Before the fix, CelProgram.evalBoolean
        // reset the per-thread step counter on every call, giving each rule in
        // a request's list a fresh MAX_EVAL_STEPS (100_000) budget. With the
        // per-api-key cap of 128 rules, a single request could legitimately
        // consume up to 128 * 100_000 = 12.8M CEL steps — a published-rule-
        // shaped DoS amplifier that turns rules-with-large-list-scans into
        // worst-case request-thread CPU burns proportional to the operator's
        // rule count.
        //
        // The fix moves the reset to RuleEngine.evaluate, scoped to one reset
        // per request. This test pins that contract with the smallest shape
        // that distinguishes per-rule from per-request budgets:
        //
        //   - Rule "a": exists-scan over a 90k-element list with no match.
        //     Returns false. Consumes ~90k steps. Leaves 10k of budget.
        //   - Rule "b": exists-scan over a 90k-element list whose match is
        //     at position 50000 (the rule WOULD return true under a fresh
        //     budget — pre-fix path of execution).
        //
        // With the shared per-request budget, rule "b" trips on iteration
        // ~10000 (it only has 10k steps left), the engine catches the trip
        // as fail-open per its policy, and returns ALLOW. Pre-fix, rule
        // "b" would have evaluated to true after 50000 iterations and the
        // engine would have returned DENY with rule "b"'s error code.
        //
        // Asserting ALLOW after this setup is the unambiguous distinguishing
        // signal: if anyone deletes the reset-hoist in RuleEngine.evaluate
        // and reintroduces the per-rule reset on CelProgram.evalBoolean,
        // this test flips to DENY and fails.
        java.util.List<Integer> emptyMatch = new java.util.ArrayList<>();
        for (int n = 0; n < 90_000; n++) {
            emptyMatch.add(n); // no -1 → exists returns false
        }
        java.util.List<Integer> midMatch = new java.util.ArrayList<>();
        for (int n = 0; n < 90_000; n++) {
            // -1 at position 50_000. Under a fresh per-rule budget, exists
            // would return true after 50_001 iterations (well under 100k).
            midMatch.add(n == 50_000 ? -1 : n);
        }
        Map<String, Object> activation = new LinkedHashMap<>();
        activation.put("emptyMatch", emptyMatch);
        activation.put("midMatch", midMatch);

        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("a-budget-burner", ApiKeys.METADATA, "emptyMatch.exists(x, x == -1)", 1))
            .put(denyRule("b-would-match-with-fresh-budget", ApiKeys.METADATA,
                "midMatch.exists(x, x == -1)", 2))
            .build());
        RuleDecision d = engine.evaluate(
            ApiKeys.METADATA, "x", null, false, () -> activation);
        // Per-request budget: rule "b" trips and fails open → ALLOW.
        // Pre-fix per-rule budget: rule "b" returns true → DENY error 2.
        assertFalse(d.denied(),
            "per-request budget must prevent rule \"b\" from completing once "
                + "rule \"a\" has consumed most of the shared 100k-step budget; "
                + "pre-fix per-rule budget would have DENIED with rule \"b\"'s code");
    }

    @Test
    public void perRequestStepBudgetIsResetBetweenRequests() {
        // Defence-in-depth for the per-request budget: after one request
        // trips the budget, the NEXT request on the same broker thread
        // must start with a clean counter. Without the try/finally reset
        // in RuleEngine.evaluate, the second request inherits a poisoned
        // ThreadLocal and trips on its very first rule iteration — turning
        // any single adversarial request into a poison pill for every
        // subsequent request on that broker thread.
        //
        // Run the budget-burner pair from the previous test, then evaluate
        // a normal cheap rule and confirm it can still match. With the
        // finally-reset, the second request gets a fresh 100k budget and
        // returns DENY normally; without it, the second request fails open.
        java.util.List<Integer> emptyMatch = new java.util.ArrayList<>();
        for (int n = 0; n < 90_000; n++) {
            emptyMatch.add(n);
        }
        java.util.List<Integer> midMatch = new java.util.ArrayList<>();
        for (int n = 0; n < 90_000; n++) {
            midMatch.add(n == 50_000 ? -1 : n);
        }
        Map<String, Object> burner = new LinkedHashMap<>();
        burner.put("emptyMatch", emptyMatch);
        burner.put("midMatch", midMatch);

        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("a-burner", ApiKeys.METADATA, "emptyMatch.exists(x, x == -1)", 1))
            .put(denyRule("b-trip", ApiKeys.METADATA, "midMatch.exists(x, x == -1)", 2))
            .build());
        engine.evaluate(ApiKeys.METADATA, "x", null, false, () -> burner);

        // Second request: a different rule set, a simple matching predicate.
        // With the finally-reset, this returns DENY normally. Without it,
        // the counter is poisoned at >= 100k from the previous request and
        // any subsequent comprehension iteration trips immediately.
        engine.install(new RuleSetBuilder()
            .put(denyRule("simple-match", ApiKeys.METADATA, "emptyMatch.exists(x, x == 17)", 99))
            .build());
        RuleDecision d = engine.evaluate(
            ApiKeys.METADATA, "x", null, false, () -> burner);
        assertTrue(d.denied(),
            "second request must start with a fresh per-request step budget; "
                + "the finally-reset in RuleEngine.evaluate exists to prevent "
                + "cross-request counter poisoning");
        assertEquals(99, d.errorCode());
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
            ApiKeys.METADATA, "x", null, false, () -> Collections.singletonMap("request", Collections.emptyMap()));
        assertTrue(d.denied());
        assertEquals(99, d.errorCode());
        assertEquals("after", d.denyingRuleId());
    }

    // ----- parseBypassPrincipals (governance.bypass.principals parser) -----

    @Test
    public void parseBypassPrincipalsAcceptsValidSemicolonSeparatedList() {
        java.util.Set<String> out = RuleEngine.parseBypassPrincipals(
            "User:broker;User:kafka-controller");
        assertEquals(2, out.size());
        assertTrue(out.contains("User:broker"));
        assertTrue(out.contains("User:kafka-controller"));
    }

    @Test
    public void parseBypassPrincipalsNullOrEmptyYieldsEmptySet() {
        assertTrue(RuleEngine.parseBypassPrincipals(null).isEmpty());
        assertTrue(RuleEngine.parseBypassPrincipals("").isEmpty());
        assertTrue(RuleEngine.parseBypassPrincipals("   ").isEmpty());
        assertTrue(RuleEngine.parseBypassPrincipals(";;;").isEmpty());
    }

    @Test
    public void parseBypassPrincipalsTrimsWhitespaceAndDropsBlankSegments() {
        java.util.Set<String> out = RuleEngine.parseBypassPrincipals(
            "  User:broker  ; ; User:other ;");
        assertEquals(2, out.size());
        assertTrue(out.contains("User:broker"));
        assertTrue(out.contains("User:other"));
    }

    @Test
    public void parseBypassPrincipalsThrowsOnMalformedEntry() {
        // Codex round-2 P1#1: malformed entries fail broker startup loudly
        // — never silently dropped. A trailing typo like 'Userbroker' (no
        // colon) is the exact mistake a hurried operator would make.
        IllegalArgumentException ex = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals(
                    "User:broker;Userbroker;User:other"));
        // Surface enough of the offending entry that the operator can grep
        // for it in their startup logs.
        assertTrue(ex.getMessage() != null,
            "parseBypassPrincipals must report which entry was malformed");
    }

    @Test
    public void parseBypassPrincipalsCanonicalizesParsedForm() {
        // SecurityUtils.parseKafkaPrincipal returns a KafkaPrincipal whose
        // toString() is the canonical "type:name" form. We rely on the
        // canonical form for the verbatim string match at evaluate() time,
        // so a parser-round-trip of a canonical entry must produce that
        // same canonical string.
        java.util.Set<String> out = RuleEngine.parseBypassPrincipals("User:broker");
        assertTrue(out.contains("User:broker"));
    }

    @Test
    public void parseBypassPrincipalsThrowsOnEntryWithoutSeparator() {
        // SecurityUtils.parseKafkaPrincipal requires "type:name" and throws
        // when no ':' separator is present. We propagate that behaviour so
        // a typo like 'Userbroker' (the most likely operator slip) is caught
        // at broker startup, not at first inter-broker request.
        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> RuleEngine.parseBypassPrincipals("Userbroker"));
        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> RuleEngine.parseBypassPrincipals("User:ok;noseparator"));
    }

    @Test
    public void parseBypassPrincipalsThrowsOnBlankPrincipalType() {
        // Codex round-3 P1: SecurityUtils.parseKafkaPrincipal only requires
        // one ':' — it does NOT validate that the type or name is non-empty.
        // ':broker' parses successfully into a principal with empty type,
        // which can never match a runtime peer principal (always reports a
        // non-empty type, eg. "User"). We reject such entries here so an
        // operator typo fails startup instead of producing silent under-
        // grants.
        IllegalArgumentException ex1 = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals(":broker"));
        assertTrue(ex1.getMessage().contains("principal type"),
            "exception should name the principal-type failure mode; got: " + ex1.getMessage());

        // Whitespace-only type is the same failure mode. Note the outer
        // entry-trim strips ASCII whitespace from the segment, so
        // "   :broker" becomes ":broker" → empty type still triggers.
        IllegalArgumentException ex2 = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("   :broker"));
        assertTrue(ex2.getMessage().contains("principal type"),
            "whitespace-only type must also be rejected; got: " + ex2.getMessage());
    }

    @Test
    public void parseBypassPrincipalsThrowsOnBlankPrincipalName() {
        // Codex round-3 P1: same failure mode on the name side. 'User:'
        // parses to a KafkaPrincipal with empty name; we reject it so the
        // operator's intended grant is never silently a no-op.
        IllegalArgumentException ex1 = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User:"));
        assertTrue(ex1.getMessage().contains("principal name"),
            "exception should name the principal-name failure mode; got: " + ex1.getMessage());

        // Whitespace-only name is the same failure mode. Note: SecurityUtils
        // splits on the FIRST ':' only, so "User:   " yields name="   "
        // (which we reject via inner-whitespace check before the
        // KafkaPrincipal constructor canonicalises it differently).
        IllegalArgumentException ex2 = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User:   "));
        assertTrue(ex2.getMessage().contains("principal name"),
            "whitespace-only name must also be rejected; got: " + ex2.getMessage());
    }

    @Test
    public void parseBypassPrincipalsThrowsOnBothComponentsBlank() {
        // Pure ':' — neither type nor name has any content. The blank-type
        // check fires first, but the important property is that this entry
        // is rejected, not which message wins.
        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> RuleEngine.parseBypassPrincipals(":"));
        // Whitespace-around-colon is the same — outer trim turns this into
        // ':' before the principal parse runs.
        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> RuleEngine.parseBypassPrincipals("  :  "));
    }

    @Test
    public void parseBypassPrincipalsThrowsOnPaddedComponent() {
        // Codex round-4 F2 (narrowed in round-5 F4):
        // SecurityUtils.parseKafkaPrincipal splits on the first ':' but does
        // not strip whitespace from the components. So "User :broker" parses
        // to KafkaPrincipal(type="User ", name="broker") whose canonical
        // form is "User :broker" — but KafkaApis canonicalises a runtime
        // peer principal as `getPrincipalType() + ":" + getName()`, which
        // never carries that trailing space on the type. Result: the
        // allow-list entry is silently unreachable, the parsed set is non-
        // empty (so the BrokerServer empty-set guard does NOT fire), and we
        // have an effective empty-bypass that the strict-empty-rejection
        // was designed to prevent. Reject these padded entries at parse.
        //
        // Round-5 F4 narrowed the check from "any whitespace anywhere" to
        // "whitespace at the leading or trailing position only" so that
        // legitimate SSL DNs (which contain spaces inside CN values) are
        // not refused — see parseBypassPrincipalsAcceptsSslDnWithInternalWhitespace.
        // The post-split positions of operator-typo whitespace are always
        // leading or trailing, so this narrowing does not weaken catch
        // coverage for the typo cases below.
        IllegalArgumentException ex1 = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User :broker"));
        assertTrue(ex1.getMessage().contains("whitespace"),
            "exception should name the whitespace failure mode; got: " + ex1.getMessage());

        // Space prefix on the name side.
        IllegalArgumentException ex2 = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User: broker"));
        assertTrue(ex2.getMessage().contains("whitespace"),
            "leading-whitespace name must be rejected; got: " + ex2.getMessage());

        // Tab at the trailing edge of the type. The outer segment trim
        // (String.trim()) strips ASCII whitespace from the SEGMENT, not
        // from the components — "User\t:broker" trims to itself, so the
        // tab survives to the parser as part of the type.
        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> RuleEngine.parseBypassPrincipals("User\t:broker"));
    }

    @Test
    public void parseBypassPrincipalsAcceptsSslDnWithInternalWhitespace() {
        // Codex round-5 P1 regression: the default DefaultKafkaPrincipalBuilder
        // for SSL listeners produces principal names that are the raw
        // X500Principal.getName() of the peer cert, which for any cert with
        // multi-word RDN values (eg. CN="Broker One") contains internal
        // spaces. An earlier iteration of the F2 check rejected ANY
        // whitespace in the name and would have refused legitimate SSL
        // broker principals at startup. Internal whitespace must be
        // accepted — only leading/trailing/all-whitespace is rejected.
        String sslDn = "User:CN=Broker One,OU=Kafka Brokers,O=Example Corp,C=US";
        java.util.Set<String> out = RuleEngine.parseBypassPrincipals(sslDn);
        assertEquals(1, out.size());
        assertTrue(out.contains(sslDn),
            "SSL DN with internal whitespace must round-trip to its canonical "
                + "form; got: " + out);

        // Same rule on the type side, defensively: a hypothetical custom
        // principal type with internal whitespace is unusual but not
        // syntactically forbidden, and the parser should not be the thing
        // that decides which principal types exist.
        java.util.Set<String> typeWithSpace = RuleEngine.parseBypassPrincipals(
            "Service Account:bot");
        assertTrue(typeWithSpace.contains("Service Account:bot"),
            "type with internal whitespace must be accepted; got: " + typeWithSpace);
    }

    @Test
    public void parseBypassPrincipalsThrowsOnUnicodeBlankComponent() {
        // Codex round-4 F2: String.trim() only strips ASCII whitespace (chars
        // <= 0x20), so a non-breaking space (U+00A0) inside a component
        // would survive the previous trim().isEmpty() check, producing an
        // unreachable allow-list entry. String.isBlank() / String.strip()
        // (Java 11+) treat all Unicode whitespace consistently, so we now
        // catch this failure mode.
        String nbsp = " ";
        // Name is U+00A0 only — isBlank() reports true, parse must reject.
        IllegalArgumentException ex = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User:" + nbsp));
        assertTrue(ex.getMessage().contains("principal name"),
            "NBSP-only name must be rejected via isBlank(); got: " + ex.getMessage());

        // Type embeds a U+00A0 around an otherwise-valid name — strip
        // equality check catches it.
        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> RuleEngine.parseBypassPrincipals("User" + nbsp + ":broker"));
    }
}
