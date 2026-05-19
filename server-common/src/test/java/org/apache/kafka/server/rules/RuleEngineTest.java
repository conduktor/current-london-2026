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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    public void activationSupplierThrowingErrorPropagatesUnderRound15Hardening() {
        // Round-15 Walker HIGH H2 (reversed previous policy):
        // pre-round-15 this test asserted that an Error (StackOverflowError /
        // OutOfMemoryError / LinkageError) from the activation supplier
        // failed OPEN. That posture made sense before the walker was fully
        // budget-bounded — a single very large request could otherwise crash
        // the broker thread. With round-15 in place (MAX_DEPTH=32 across
        // BOTH message AND iterable layers per HIGH-H1, plus the existing
        // MAX_ACCESSOR_INVOCATIONS=10_000 budget covering nested- and flat-
        // scalar widths), the walker has a bounded worst-case work ceiling
        // on every dimension that attacker-shaped input can amplify.
        //
        // Under that guarantee, an Error from the supplier is no longer a
        // "buggy rule" symptom — it is either:
        //   (1) a bound-logic bug we MUST surface, not paper over, or
        //   (2) a JVM-level failure where the request thread is not actually
        //       safe to continue on anyway.
        // Catching it would silently fail-OPEN — and an attacker who can
        // pin a bound-logic bug to a single API key would then bypass every
        // DENY rule on that key on every subsequent request until the
        // operator noticed. The correct posture is to let the Error
        // propagate to KafkaApis's outer Throwable catch, which turns it
        // into a 5xx response per request — visible signal, fail-CLOSED at
        // the request layer.
        //
        // The narrowed catch site is `catch (Exception e)`; this test pins
        // that Error subclasses are NOT caught.
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("deny-all", ApiKeys.METADATA, "true", 99))
            .build());
        StackOverflowError thrown = assertThrows(StackOverflowError.class, () ->
            engine.evaluate(
                ApiKeys.METADATA, "client", null, false,
                () -> {
                    throw new StackOverflowError("simulated deep walk");
                }));
        assertEquals("simulated deep walk", thrown.getMessage(),
            "the original Error must propagate verbatim (not be re-wrapped)");
    }

    @Test
    public void activationSupplierThrowingErrorClearsInEvaluateThreadLocal() {
        // Companion to activationSupplierThrowingErrorPropagatesUnderRound15Hardening:
        // even though Error subclasses now propagate from the catch site,
        // the OUTER finally at the end of evaluate() must still clear the
        // IN_EVALUATE re-entry flag. Otherwise a pool thread that serviced a
        // request which raised an Error from the supplier would observe a
        // stale TRUE on its next request and throw IllegalStateException
        // from the re-entry guard — turning a one-request fault into a
        // thread-poisoning DoS.
        //
        // This test invokes evaluate() twice on the same thread: the first
        // call throws Error from the supplier, the second runs cleanly.
        // If the outer finally were skipped, the second call would throw
        // IllegalStateException instead of returning a clean decision.
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("deny-all", ApiKeys.METADATA, "true", 99))
            .build());
        // Call 1: Error propagates.
        assertThrows(OutOfMemoryError.class, () ->
            engine.evaluate(
                ApiKeys.METADATA, "client", null, false,
                () -> {
                    throw new OutOfMemoryError("simulated heap pressure");
                }));
        // Call 2: same thread, must run cleanly — if the outer finally was
        // skipped, this would throw IllegalStateException("re-entry").
        RuleDecision d = engine.evaluate(
            ApiKeys.METADATA, "client", null, false, Collections::emptyMap);
        assertTrue(d.denied(),
            "second evaluation on the same thread must not be blocked by " +
                "a leaked IN_EVALUATE=TRUE from the first call's Error propagation");
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
    public void perRuleCatchMustNotSwallowReentryFromInsideEvalBoolean() {
        // R23 #224: the engine's re-entry guard at the top of evaluate()
        // throws EvaluateReentryException — a private IllegalStateException
        // subclass that distinguishes the engine's own engine-state signal
        // from operator-rule failures that legitimately throw plain
        // IllegalStateException (see
        // buggyPredicateThrowingExceptionStillFailsOpenAndAdvancesToNextRule
        // for the operator-rule contract that this design preserves).
        //
        // Today the supplier-level re-entrant case is covered by the
        // activation-supplier catch at the supplier-failure site, which IS
        // intentionally fail-OPEN (pinned by
        // reentrantEvaluateFromActivationSupplierIsCaughtAndFailsOpen).
        // That posture is correct: the supplier "failed" from the engine's
        // perspective, so the activation-supplier WARN+fail-OPEN is the
        // right outcome.
        //
        // This test pins the DIFFERENT path: a future CEL host function or
        // a custom accessor that re-enters evaluate() from INSIDE the
        // per-rule evalBoolean() call. Without the marker-subclass re-throw,
        // the per-rule fail-OPEN catch below the rule-eval call would
        // swallow the re-entry signal and mark the offending rule ALLOW —
        // a hostile rule or a buggy supplier could thereby evade the very
        // DENY rule whose evaluation triggered it, silently and without
        // operator visibility. The fix surfaces the re-entry signal up to
        // KafkaApis (5xx).
        //
        // The test forces the path with a custom activation Map whose
        // get("r") recursively calls engine.evaluate(); the rule "r.x == 1"
        // looks up "r" from inside evalBoolean(), so the inner evaluate
        // sees IN_EVALUATE=TRUE → throws EvaluateReentryException → the
        // per-rule catch must NOT absorb it.
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("deny-on-x", ApiKeys.METADATA, "r.x == 1", 99))
            .build());
        Map<String, Object> reentrantActivation = new java.util.HashMap<String, Object>() {
            @Override
            public Object get(Object key) {
                if ("r".equals(key)) {
                    engine.evaluate(ApiKeys.METADATA, "c", null, false, Collections::emptyMap);
                }
                return super.get(key);
            }
        };
        IllegalStateException ise = assertThrows(IllegalStateException.class,
            () -> engine.evaluate(
                ApiKeys.METADATA, "c", null, false, () -> reentrantActivation));
        assertTrue(ise.getMessage().contains("must not be called recursively"),
            "expected re-entry guard message; got: " + ise.getMessage());
        assertTrue(ise instanceof RuleEngine.EvaluateReentryException,
            "expected EvaluateReentryException marker subclass so the per-rule catch "
                + "can distinguish engine-state re-entry from operator-rule ISE; got: "
                + ise.getClass().getName());
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
    public void evalErrorWarnIsThrottledUnderRapidFire() {
        // Round-14 BLOCKER L-1 (concurrency sub-agent): the fail-open WARN
        // emitted from RuleEngine.evaluate when a rule's predicate throws sits
        // on the per-rule, per-request hot path. Before this fix a single
        // buggy rule could fire one WARN line per request and saturate the
        // SLF4J appender from a published-rule-shaped trigger. This test
        // proves the throttle holds: a sustained tight loop of evaluations
        // against a rule whose CEL throws every time leaves at most one
        // unsuppressed WARN per ~1-second window. The suppression counter is
        // package-private exactly so this assertion does not need a slow
        // wall-clock pause.
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            // 1 / 0 throws CelEvaluationException at evaluation time — the
            // exact shape an operator-published bug would take.
            .put(denyRule("explody", ApiKeys.METADATA, "1 / 0 == 0", 99))
            .build());
        final int rapidFireCalls = 1_000;
        for (int i = 0; i < rapidFireCalls; i++) {
            // Predicate throws, engine fails open → ALLOW.
            RuleDecision d = engine.evaluate(
                ApiKeys.METADATA, "client", null, false,
                () -> Collections.singletonMap("request", Collections.emptyMap()));
            assertSame(RuleDecision.ALLOW, d,
                "buggy predicate must fail open — only the WARN is throttled");
        }
        long suppressed = engine.suppressedEvalErrorWarnings.get();
        assertTrue(suppressed >= rapidFireCalls - 2,
            "expected the eval-error throttle to suppress most rapid-fire WARNs, got " + suppressed
                + " out of " + rapidFireCalls + " eval-error events");
    }

    @Test
    public void activationSupplierFailureWarnIsThrottledUnderRapidFire() {
        // Round-15 recent-changes BLOCKER-1: the WARN emitted when
        // RuleEngine.evaluate's activationSupplier.get() throws a non-budget
        // exception was previously un-throttled and un-sanitised — exactly the
        // shape that round-14 BLOCKER L-1 closed for the per-rule eval-error
        // WARN. This test proves the new throttle holds: a sustained tight
        // loop of evaluations where the supplier throws every call leaves at
        // most one unsuppressed WARN per ~1-second window.
        //
        // The package-private suppression counter exposes the throttle state
        // without driving a slow wall-clock test.
        RuleEngine engine = new RuleEngine();
        // At least one DENY rule on the target API key — otherwise the engine
        // short-circuits at rulesFor(apiKey).isEmpty() and never invokes the
        // activation supplier (and so never enters the throttled WARN path).
        engine.install(new RuleSetBuilder()
            .put(denyRule("touch", ApiKeys.METADATA, "true", 99))
            .build());
        final int rapidFireCalls = 1_000;
        for (int i = 0; i < rapidFireCalls; i++) {
            // Supplier throws a non-budget RuntimeException — the exact shape
            // a broken ApiMessageActivation accessor would take. Engine fails
            // open to ALLOW; only the WARN is throttled.
            RuleDecision d = engine.evaluate(
                ApiKeys.METADATA, "client", null, false,
                () -> {
                    throw new IllegalStateException("walker bug");
                });
            assertSame(RuleDecision.ALLOW, d,
                "broken supplier must fail open — only the WARN is throttled");
        }
        long suppressed = engine.suppressedActivationFailureWarnings.get();
        assertTrue(suppressed >= rapidFireCalls - 2,
            "expected the activation-supplier-failure throttle to suppress most rapid-fire "
                + "WARNs, got " + suppressed + " out of " + rapidFireCalls + " events");
    }

    @Test
    public void cumulativeFailOpenAndFailClosedCountersAdvanceOnEveryEvent() {
        // Round-15 Request-path HIGH-3: the window-bound suppressedXxxWarnings
        // counters above are reset to 0 on every emitted WARN and therefore
        // cannot serve as a long-term operator-sampling signal. The
        // monotonically-increasing cumulative counters added by HIGH-3 must
        // bump on EVERY event (including events suppressed by the throttle),
        // never reset, and remain independent across the three event classes.
        // An operator (or a future JMX adapter) reading the public getters
        // sees the absolute event count since engine construction — the
        // signal needed for rate/anomaly metrics. This test pins all three
        // semantics on the same engine instance.
        RuleEngine engine = new RuleEngine();
        // Three rules so each event class fires from its own path. The
        // bypass-listener short-circuit is off (fromPrivilegedListener=false)
        // and no bypass principal is configured, so every call traverses the
        // throttled WARN methods.
        engine.install(new RuleSetBuilder()
            .put(denyRule("explody", ApiKeys.METADATA, "1 / 0 == 0", 99))
            .build());

        // Baselines should be zero on a fresh engine.
        assertEquals(0L, engine.activationFailureFailOpenCount(),
            "fresh engine has not yet observed any activation-supplier failure");
        assertEquals(0L, engine.evalErrorFailOpenCount(),
            "fresh engine has not yet observed any rule eval error");
        assertEquals(0L, engine.activationBudgetFailClosedCount(),
            "fresh engine has not yet observed any activation-budget overflow");

        // Drive 100 activation-supplier failures (non-budget). All but at
        // most one are suppressed by the 1-second WARN throttle, but every
        // call must still increment the cumulative counter.
        final int activationFailures = 100;
        for (int i = 0; i < activationFailures; i++) {
            RuleDecision d = engine.evaluate(
                ApiKeys.METADATA, "client", null, false,
                () -> {
                    throw new IllegalStateException("walker bug");
                });
            assertSame(RuleDecision.ALLOW, d,
                "broken supplier must fail open on every call");
        }
        assertEquals(activationFailures, engine.activationFailureFailOpenCount(),
            "every activation-supplier failure must bump the cumulative counter, "
                + "including events suppressed by the WARN throttle");
        // The other two counters must NOT have moved — events are independent.
        assertEquals(0L, engine.evalErrorFailOpenCount(),
            "activation-supplier failures must not bleed into the eval-error counter");
        assertEquals(0L, engine.activationBudgetFailClosedCount(),
            "activation-supplier failures must not bleed into the budget-closed counter");

        // Drive 100 per-rule eval errors. Same throttle behaviour, same
        // independence guarantee.
        final int evalErrors = 100;
        for (int i = 0; i < evalErrors; i++) {
            RuleDecision d = engine.evaluate(
                ApiKeys.METADATA, "client", null, false,
                () -> Collections.singletonMap("request", Collections.emptyMap()));
            assertSame(RuleDecision.ALLOW, d,
                "buggy predicate must fail open on every call");
        }
        assertEquals(evalErrors, engine.evalErrorFailOpenCount(),
            "every per-rule eval error must bump the cumulative counter, "
                + "including events suppressed by the WARN throttle");
        // activation-failure counter must not have moved further.
        assertEquals(activationFailures, engine.activationFailureFailOpenCount(),
            "eval errors must not bleed into the activation-supplier-failure counter");
        assertEquals(0L, engine.activationBudgetFailClosedCount(),
            "eval errors must not bleed into the budget-closed counter");

        // Drive 100 budget-exceeded fail-closed events.
        final int budgetTrips = 100;
        for (int i = 0; i < budgetTrips; i++) {
            RuleDecision d = engine.evaluate(
                ApiKeys.METADATA, "client", null, false,
                () -> {
                    throw new ActivationBudgetExceededException("wide request");
                });
            assertTrue(d.denied(),
                "budget overflow must fail CLOSED on every call");
            assertEquals(Errors.POLICY_VIOLATION.code(), d.errorCode());
        }
        assertEquals(budgetTrips, engine.activationBudgetFailClosedCount(),
            "every activation-budget overflow must bump the cumulative counter, "
                + "including events suppressed by the WARN throttle");
        // The other two counters must remain at the values left by their
        // own loops — no cross-bleed.
        assertEquals(activationFailures, engine.activationFailureFailOpenCount(),
            "budget overflows must not bleed into the activation-supplier-failure counter");
        assertEquals(evalErrors, engine.evalErrorFailOpenCount(),
            "budget overflows must not bleed into the eval-error counter");
    }

    @Test
    public void evalErrorCumulativeCounterIsPerEvaluateNotPerRule() {
        // Round-16 MED (audit-agent-A): pin that evalErrorFailOpenCount
        // dedupes to once per evaluate() call, not once per rule that
        // tripped. The pre-fix shape would bump the counter K times for
        // K buggy rules on the same api-key — a single attacker request
        // could amplify the counter by up to 128× under the per-api-key
        // rule cap, making JMX-derived fail-open rate dashboards
        // over-report by orders of magnitude. The post-fix shape bumps
        // the counter at most once per evaluate(), regardless of how
        // many rules tripped during that one request.
        //
        // The per-rule WARN log still fires per rule (each buggy rule's
        // diagnostic is independently useful); only the cumulative
        // counter is deduped. We verify both contracts here.
        RuleEngine engine = new RuleEngine();
        // Three buggy rules on the SAME api-key. Each rule's CEL throws
        // independently (1/0 division). Under the pre-fix code, a
        // single evaluate() would land in the per-rule catch three times
        // and bump the counter three times.
        engine.install(new RuleSetBuilder()
            .put(denyRule("explody-1", ApiKeys.METADATA, "1 / 0 == 0", 99))
            .put(denyRule("explody-2", ApiKeys.METADATA, "1 / 0 == 1", 99))
            .put(denyRule("explody-3", ApiKeys.METADATA, "1 / 0 == 2", 99))
            .build());

        assertEquals(0L, engine.evalErrorFailOpenCount(),
            "fresh engine baseline");
        long preEvalSuppressed = engine.suppressedEvalErrorWarnings.get();

        // Exactly one evaluate() call. Three rules trip inside it.
        RuleDecision d = engine.evaluate(
            ApiKeys.METADATA, "client", null, false,
            () -> Collections.singletonMap("request", Collections.emptyMap()));
        assertSame(RuleDecision.ALLOW, d,
            "all three rules fail open, request resolves ALLOW");

        // Cumulative counter: bumped EXACTLY once, not three times.
        assertEquals(1L, engine.evalErrorFailOpenCount(),
            "three rules tripped in one evaluate() must bump the cumulative "
                + "counter exactly once — per-evaluate dedup contract");

        // Per-rule WARN log still fires per rule. The first WARN emits
        // (lastEvalErrorWarnNanos is well in the past from construction);
        // the remaining two land in the throttle's else-branch and bump
        // the suppressed counter. Net: at least 2 entries in the
        // suppressed counter (rules 2 and 3, post-emit-of-rule-1). The
        // exact value depends on whether the wall-clock window rolled
        // mid-test; >=2 is the robust lower bound.
        long postEvalSuppressed = engine.suppressedEvalErrorWarnings.get();
        assertTrue(postEvalSuppressed - preEvalSuppressed >= 2L,
            "per-rule WARN log must still fire independently per rule — "
                + "expected >= 2 throttled WARN events for the 3 buggy rules, got "
                + (postEvalSuppressed - preEvalSuppressed));

        // A second evaluate() bumps the counter again — to 2, not to 4.
        // (Sanity check on the per-evaluate semantics.)
        engine.evaluate(
            ApiKeys.METADATA, "client", null, false,
            () -> Collections.singletonMap("request", Collections.emptyMap()));
        assertEquals(2L, engine.evalErrorFailOpenCount(),
            "second evaluate() bumps counter by exactly one more — per-evaluate semantics");

        // A third evaluate() with NO buggy rules in the activation map
        // (still using the same 3-buggy-rules engine, just observe that
        // the per-evaluate count is on the evaluate() call itself, not
        // on whether rules tripped). Actually, with the buggy rules
        // installed, every evaluate() trips them — there's no "no
        // tripping" path in this engine state. So this third call also
        // bumps the counter: post-condition = 3. Test the negative path
        // separately: a fresh engine with NO rules at all.
        RuleEngine emptyEngine = new RuleEngine();
        emptyEngine.install(RuleSet.EMPTY);
        emptyEngine.evaluate(
            ApiKeys.METADATA, "client", null, false,
            () -> Collections.singletonMap("request", Collections.emptyMap()));
        assertEquals(0L, emptyEngine.evalErrorFailOpenCount(),
            "evaluate() with no rules cannot trip eval errors — counter stays 0");
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
    public void installSwapStructuralInvariantsPreventTornReads() throws Exception {
        // R28 Concurrency F1 / Task #233: the runtime test
        // installSwapIsAtomicForConcurrentReaders below cannot actually
        // detect a torn-read regression, because the JMM guarantees that
        // make torn reads impossible are *structural* — they live on the
        // type declarations, not on the runtime behaviour:
        //
        //   1. RuleEngine.active is declared as AtomicReference<RuleSet>.
        //      A `.set(rs)` is a single volatile write of the reference;
        //      a `.get()` is a single volatile read. There is no window
        //      in which a reader could see the swap mid-way through.
        //
        //   2. Every field of RuleSet (denyBitset, rulesByApiKey,
        //      rulesById) is `final`. Per JLS §17.5, final fields are
        //      safely-published — any thread that observes a reference
        //      to the RuleSet observes the final fields fully initialised,
        //      regardless of how the reference reaches it.
        //
        // Together these mean that for any pair of pre-built RuleSets A
        // and B, no reader can ever see "A's reference with B's contents"
        // or "the new reference with the old contents" — the runtime
        // test below therefore passes vacuously and would continue to
        // pass even if both invariants were broken simultaneously (as
        // long as the test setup happens to pre-publish the RuleSets
        // safely, which it does by virtue of pool.submit).
        //
        // This test pins the invariants at their source — it FAILS the
        // moment either `active` is downgraded to a non-AtomicReference
        // or any RuleSet field is non-final.

        // Invariant 1: RuleEngine.active is AtomicReference, not a plain
        // reference field. A future "let's simplify, AtomicReference is
        // overkill" refactor must surface here.
        Field active = RuleEngine.class.getDeclaredField("active");
        assertEquals(AtomicReference.class, active.getType(),
            "RuleEngine.active must be AtomicReference<RuleSet> — concurrent "
                + "install/evaluate relies on its volatile semantics for "
                + "publication and reference atomicity; a plain reference "
                + "field would re-open torn-read hazards the engine has no "
                + "other defense against");
        assertTrue(Modifier.isFinal(active.getModifiers()),
            "RuleEngine.active must be final — the AtomicReference itself "
                + "must not be reassignable, or a writer reassigning the "
                + "AtomicReference would defeat the publication contract");

        // Invariant 2: every field of RuleSet is final. Without this, a
        // future field added to RuleSet without the `final` modifier
        // would silently break JMM safe-publication for that one field,
        // and torn reads become observable for it specifically.
        for (Field f : RuleSet.class.getDeclaredFields()) {
            // Static fields (EMPTY, MAX_API_KEY_ID) don't participate in
            // per-instance safe-publication; the contract is only on
            // instance state.
            if (Modifier.isStatic(f.getModifiers())) continue;
            assertTrue(Modifier.isFinal(f.getModifiers()),
                "RuleSet instance field '" + f.getName() + "' must be final — "
                    + "JMM safe-publication for the RuleSet's contents "
                    + "depends on every instance field being final so a "
                    + "concurrent reader that observes the reference also "
                    + "observes the field's fully-initialised value, "
                    + "regardless of synchronisation on the reference");
        }
    }

    @Test
    public void installSwapIsAtomicForConcurrentReaders() throws Exception {
        // Smoke test that complements installSwapStructuralInvariantsPreventTornReads
        // (Task #233): the structural test above is what actually proves
        // torn-read impossibility — this runtime test exists to confirm
        // the engine works correctly under concurrent install/evaluate
        // load (no NPE, no thrown exception, no obviously-wrong answer)
        // and serves as a smoke detector for *behavioural* regressions
        // that the structural invariants would not catch (e.g., a future
        // multi-step install that reads from one snapshot and writes to
        // another in-place).
        //
        // The (errorCode, denyingRuleId) pair on the decision is checked
        // as a unit: under the live contract each pair value comes from a
        // single Rule instance's two final fields, so a torn read that
        // mixed fields across snapshots (the only behavioural shape this
        // test could catch) would surface as a (11, "b") or (22, "a")
        // mismatch rather than just an unexpected errorCode.
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
                        if (!d.denied()) {
                            failed.set(true);
                            return;
                        }
                        // Pair (errorCode, denyingRuleId) must be coherent —
                        // either ("a", 11) or ("b", 22). Any other pair is a
                        // torn-snapshot symptom. Without this paired check, a
                        // refactor that returned errorCode from one snapshot and
                        // ruleId from another would still satisfy "errorCode is
                        // 11 or 22" and the test would pass.
                        boolean pairA = "a".equals(d.denyingRuleId()) && d.errorCode() == 11;
                        boolean pairB = "b".equals(d.denyingRuleId()) && d.errorCode() == 22;
                        if (!(pairA || pairB)) {
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
        assertFalse(failed.get(), "no reader should observe a partially-built RuleSet "
            + "or a (errorCode, denyingRuleId) pair sourced from two different snapshots");
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
    public void buggyPredicateThrowingErrorPropagatesUnderRound15Hardening() {
        // Round-15 Walker HIGH H2 (reversed previous policy):
        // pre-round-15 this test asserted that an Error (StackOverflowError
        // from a deep CEL comprehension, OutOfMemoryError from a huge string
        // op) raised inside per-rule evalBoolean was caught and the rule was
        // skipped to ALLOW (with a fall-through to the next rule). That
        // posture made sense when the CEL bounds were under-specified.
        //
        // Round-15 hardened the CEL step budget (CelProgram.MAX_EVAL_STEPS),
        // parse depth (MAX_PARSE_DEPTH), and collection-literal limits.
        // Under those caps, an Error subclass from CEL eval is no longer a
        // "buggy rule" symptom — it almost certainly indicates a CEL bound-
        // logic bug, and silently absorbing it lets the bug accumulate
        // across all production traffic. Letting Error propagate through
        // to KafkaApis's outer Throwable catch turns the request into a
        // 5xx and makes the bug visible.
        //
        // The narrowed catch site is `catch (Exception e)`; this test pins
        // that Error subclasses propagate from inside the per-rule loop —
        // they do NOT fall through to the next rule.
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
        StackOverflowError thrown = assertThrows(StackOverflowError.class, () ->
            engine.evaluate(
                ApiKeys.METADATA, "x", null, false,
                () -> Collections.singletonMap("request", Collections.emptyMap())));
        assertEquals("simulated runaway recursion", thrown.getMessage(),
            "the original Error must propagate verbatim (not be wrapped or swallowed)");
    }

    @Test
    public void buggyPredicateThrowingExceptionStillFailsOpenAndAdvancesToNextRule() {
        // Round-15 Walker HIGH H2 complement: the narrowed `catch (Exception)`
        // still catches the realistic per-rule failure modes — CelEvaluationException,
        // NullPointerException, IllegalStateException, ClassCastException from
        // a buggy predicate or malformed activation map. For these, posture
        // remains fail-OPEN at the per-rule level: a single buggy operator-
        // authored rule must not be able to deny every request, and the rule
        // loop must advance to evaluate subsequent rules.
        //
        // This test pins that contract: a rule that throws RuntimeException
        // is skipped, and the next rule's DENY still takes effect.
        org.apache.kafka.server.rules.cel.CelProgram buggy =
            org.mockito.Mockito.mock(org.apache.kafka.server.rules.cel.CelProgram.class);
        org.mockito.Mockito.when(buggy.evalBoolean(org.mockito.ArgumentMatchers.any()))
            .thenThrow(new IllegalStateException("simulated CEL bug — null traversal"));
        Rule throwing = new Rule(
            "buggy-rule",
            Collections.singletonList(ApiKeys.METADATA),
            RuleAction.DENY,
            "true",
            7,
            buggy);
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(throwing)
            .put(denyRule("after", ApiKeys.METADATA, "true", 99))
            .build());
        RuleDecision d = engine.evaluate(
            ApiKeys.METADATA, "x", null, false,
            () -> Collections.singletonMap("request", Collections.emptyMap()));
        assertTrue(d.denied(), "the next rule's DENY must take effect");
        assertEquals(99, d.errorCode());
        assertEquals("after", d.denyingRuleId(),
            "rule loop must advance past the buggy rule to the next candidate");
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
    public void parseBypassPrincipalsCanonicalFormMatchesKafkaApisRuntimeBuild() {
        // Round-22 HIGH (Agent 5 H-1): the runtime bypass check in
        // KafkaApis builds the principal-string by hand
        // (`p.getPrincipalType + ":" + p.getName`, see KafkaApis L246-247)
        // — NOT by calling `principal.toString()`. The reason is that
        // custom KafkaPrincipal subclasses (wired via a
        // KafkaPrincipalBuilder) sometimes override toString to append
        // role/group/auth metadata, and using toString would produce a
        // runtime string that never matches a canonical allow-list entry.
        // Pin that the allow-list-build side uses the same hand-built
        // form, so a future refactor of parseKafkaPrincipal (or the
        // wiring of a subclass) cannot silently break the bypass.
        //
        // The pin is asserted positionally: every entry returned must
        // equal `type + ":" + name` for the canonical KafkaPrincipal we
        // would have constructed from the input.
        String[] inputs = {
            "User:broker",
            "User:CN=Broker One,OU=Kafka Brokers,O=Example Corp,C=US",
            "Role:cluster-admin"
        };
        for (String in : inputs) {
            java.util.Set<String> out = RuleEngine.parseBypassPrincipals(in);
            assertEquals(1, out.size(), "single entry must parse to single canonical form: " + in);
            String got = out.iterator().next();
            // Build the same canonical form KafkaApis would build at
            // request time. If parseBypassPrincipals ever reverted to
            // `principal.toString()` and someone wired a subclass that
            // augments toString, this assertion would break with a clear
            // diff between the two forms.
            org.apache.kafka.common.security.auth.KafkaPrincipal p =
                org.apache.kafka.common.utils.SecurityUtils.parseKafkaPrincipal(in);
            String runtimeBuild = p.getPrincipalType() + ":" + p.getName();
            assertEquals(runtimeBuild, got,
                "parseBypassPrincipals must emit `type + \":\" + name` so it matches "
                    + "the KafkaApis runtime bypass check, not principal.toString() "
                    + "(which custom subclasses may override). input=" + in);
        }
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
    public void parseBypassPrincipalsRejectsCommaAsSeparatorTypo() {
        // R29 #270 [HIGH]: governance.bypass.principals uses `;` as the
        // separator (PROMPT.md), NOT `,` — because legitimate SSL DN
        // principal names contain commas (eg. CN=Broker One,OU=...,O=...,C=).
        // But every other Kafka list config (listeners, bootstrap.servers,
        // advertised.listeners, controller.quorum.voters, ...) is comma-
        // separated, so an operator typo replacing `;` with `,` is highly
        // plausible.
        //
        // The hazard: `"User:admin,User:broker"` parses successfully as a
        // SINGLE principal — SecurityUtils.parseKafkaPrincipal does
        // split(":", 2), so type=`User`, name=`admin,User:broker`. The
        // existing whitespace and invisible-codepoint guards do not catch
        // commas. Allow-list becomes non-empty (`{"User:admin,User:broker"}`)
        // so the BrokerServer empty-set startup guard passes. At runtime
        // the broker's own peer principal is `User:broker` — it will NEVER
        // match the literal `User:admin,User:broker` entry, silently
        // soft-bricking inter-broker traffic with NO startup diagnostic.
        //
        // Discriminator (must not false-positive on SSL DN):
        //   - `,` in TYPE → always rejected. Principal types are short
        //     identifiers (User/Group/Role/Service Account); commas only
        //     appear there via a typo of shape `"User,Group:admins"`.
        //   - `,<ident>:` in NAME → rejected as comma-separator typo.
        //     SSL DN attribute separators use `=` (`,OU=`, `,O=`, `,C=`,
        //     `,EMAILADDRESS=`, ...) — never `:` — so legitimate DNs
        //     containing commas are NOT affected by this check.

        // ---- The headline case: two principals comma-joined ----
        IllegalArgumentException ex1 = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User:admin,User:broker"));
        assertTrue(ex1.getMessage().toLowerCase().contains("comma"),
            "comma-separator typo diagnostic must name `comma`; got: "
                + ex1.getMessage());
        assertTrue(ex1.getMessage().contains(";"),
            "diagnostic must name the correct separator `;`; got: "
                + ex1.getMessage());
        assertTrue(ex1.getMessage().contains("User:admin,User:broker"),
            "diagnostic must echo the offending entry so the operator can "
                + "find it in their config; got: " + ex1.getMessage());

        // ---- Comma + space after (Kafka-idiom-ish formatting) ----
        IllegalArgumentException ex2 = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User:admin, User:broker"));
        assertTrue(ex2.getMessage().toLowerCase().contains("comma"),
            "comma-separator typo with whitespace after comma must also be "
                + "rejected (Kafka list configs often have ', ' formatting); got: "
                + ex2.getMessage());

        // ---- Different principal types ----
        IllegalArgumentException ex3 = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User:admin,Group:admins"));
        assertTrue(ex3.getMessage().toLowerCase().contains("comma"),
            "comma-separator typo with mixed principal types must be rejected; got: "
                + ex3.getMessage());

        // ---- Three principals comma-joined (no `;` at all) ----
        IllegalArgumentException ex4 = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals(
                    "User:admin,User:broker,User:client"));
        assertTrue(ex4.getMessage().toLowerCase().contains("comma"),
            "three-principal comma-joined typo must be rejected; got: "
                + ex4.getMessage());

        // ---- Mixed: some segments correct, one segment uses comma ----
        // `"User:broker;User:admin,User:client"` → split by `;` yields two
        // segments. The first is fine; the second has the comma typo. The
        // parser must abort on the second segment.
        IllegalArgumentException ex5 = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals(
                    "User:broker;User:admin,User:client"));
        assertTrue(ex5.getMessage().toLowerCase().contains("comma"),
            "mixed-separator config (semicolon-joined entry containing a "
                + "comma-typo entry) must be rejected on the bad segment; got: "
                + ex5.getMessage());

        // ---- Comma in TYPE: `"User,Group:admin"` ----
        // split(":", 2) gives type="User,Group", name="admin". Comma is in
        // the parsed type — reject with the same diagnostic family.
        IllegalArgumentException ex6 = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User,Group:admin"));
        assertTrue(ex6.getMessage().toLowerCase().contains("comma"),
            "comma in principal TYPE must be rejected; got: " + ex6.getMessage());

        // ---- LogSafe regression: typo with embedded CR must not forge a
        // log line. The diagnostic embeds the offending entry, which the
        // BrokerServer.startup fatal log line propagates via SLF4J.
        IllegalArgumentException exLog = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User:admin,User:broker\rX"));
        assertFalse(exLog.getMessage().contains("\r"),
            "comma-typo diagnostic must not embed raw CR (log-injection guard); "
                + "got: " + exLog.getMessage());
        assertTrue(exLog.getMessage().contains("\\u000D"),
            "comma-typo diagnostic must show CR as \\u000D; got: "
                + exLog.getMessage());

        // ---- Regression guard: legitimate SSL DN with commas is STILL
        // accepted. This is the critical no-false-positive assertion. If
        // the implementation widens to "reject any comma in name", this
        // test fails and we get a clear signal that the discriminator is
        // too broad.
        String sslDn = "User:CN=Broker One,OU=Kafka Brokers,O=Example Corp,C=US";
        java.util.Set<String> okSsl = RuleEngine.parseBypassPrincipals(sslDn);
        assertEquals(1, okSsl.size(),
            "SSL DN with commas (X.500 attribute separators followed by `=`) "
                + "must NOT trip the comma-separator-typo check; got: " + okSsl);
        assertTrue(okSsl.contains(sslDn),
            "SSL DN must round-trip to its canonical form; got: " + okSsl);

        // ---- Regression guard: a single ordinary principal (no commas
        // anywhere) is unaffected.
        java.util.Set<String> okPlain = RuleEngine.parseBypassPrincipals("User:broker");
        assertEquals(1, okPlain.size(), "plain principal must parse cleanly");
        assertTrue(okPlain.contains("User:broker"));

        // ---- Regression guard: name containing a comma but NOT in
        // `,<ident>:` shape (e.g. trailing comma in the operator's free-
        // form `name`). This is unusual but not the typo signature — a
        // name like `"foo,bar"` does not look like a chained principal.
        // Accept it to keep the discriminator narrow.
        java.util.Set<String> okFreeForm = RuleEngine.parseBypassPrincipals(
            "User:foo,bar");
        assertEquals(1, okFreeForm.size(),
            "name with trailing comma (no `<ident>:` after) is NOT the typo "
                + "shape; got: " + okFreeForm);
        assertTrue(okFreeForm.contains("User:foo,bar"));
    }

    @Test
    public void parseBypassPrincipalsRejectsBroaderCommaTypoShapes() {
        // R29 #272 [HIGH]: an adversarial audit of the R29 #270 fix
        // (commit 8ff2491e4f) found that the initial regex
        // `,\s*[A-Za-z][A-Za-z0-9_]*:` was too narrow. Six common
        // production-realistic principal-type shapes still soft-bricked
        // the broker silently:
        //
        //   - hyphen:        Service-Account
        //   - internal space: Service Account
        //   - FQCN dot:      com.example.Principal
        //   - empty ident:   `,:` (literal pasted separator)
        //   - non-ASCII:     Üser  (UTF-8 locale typo)
        //
        // The discriminator design is right (SSL DN attribute
        // separators use `=` before the next non-`,` token, never `:`),
        // but the identifier character class must accept anything except
        // `=`, `,`, and `:` — not just `[A-Za-z0-9_]`. This test makes
        // that contract explicit: each of these strings is a typo, and
        // each must abort startup with a comma-typo diagnostic.

        // ---- 1. Hyphen in type (Service-Account is the natural Kafka
        //      principal type for service accounts and shows up in many
        //      KafkaPrincipalBuilder implementations).
        IllegalArgumentException exHyphen = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals(
                    "User:admin,Service-Account:bot"));
        assertTrue(exHyphen.getMessage().toLowerCase().contains("comma"),
            "Service-Account (hyphen) typo must be rejected with a "
                + "comma-typo diagnostic; got: " + exHyphen.getMessage());

        // ---- 2. Internal space in type. The existing test
        //      `parseBypassPrincipalsAcceptsSslDnWithInternalWhitespace`
        //      already documents that `Service Account` is an accepted
        //      LEGITIMATE type when standalone. The typo case is when
        //      it appears AFTER `,` instead of `;`.
        IllegalArgumentException exSpace = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals(
                    "User:admin,Service Account:bot"));
        assertTrue(exSpace.getMessage().toLowerCase().contains("comma"),
            "Service Account (space) typo must be rejected; got: "
                + exSpace.getMessage());

        // ---- 3. FQCN-style dotted type (custom KafkaPrincipalBuilders
        //      sometimes use fully-qualified class names as the
        //      principal type for legacy gateways).
        IllegalArgumentException exDot = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals(
                    "User:admin,com.example.Principal:bot"));
        assertTrue(exDot.getMessage().toLowerCase().contains("comma"),
            "FQCN-typed comma typo must be rejected; got: "
                + exDot.getMessage());

        // ---- 4. Multi-entry partial: one segment correct, the next
        //      one carries the hyphen-type typo. The parser must abort
        //      on the bad segment.
        IllegalArgumentException exMulti = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals(
                    "User:broker;User:admin,Service-Account:client"));
        assertTrue(exMulti.getMessage().toLowerCase().contains("comma"),
            "mixed-separator partial (semicolon-joined entry containing a "
                + "hyphen-type comma typo) must be rejected; got: "
                + exMulti.getMessage());

        // ---- 5. Empty identifier `,:` — operator pasted the entry
        //      separator literally. Not a structurally valid principal
        //      and clearly a typo; must be rejected.
        IllegalArgumentException exEmpty = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals(
                    "User:admin,:broker"));
        assertTrue(exEmpty.getMessage().toLowerCase().contains("comma"),
            "empty-ident comma typo (`,:`) must be rejected; got: "
                + exEmpty.getMessage());

        // ---- 6. Non-ASCII type letter (`Üser`). The previous regex
        //      hard-coded `[A-Za-z]` and so let this shape through.
        //      Real-world: any locale where the operator's keyboard
        //      produced a typo'd Latin-1 character.
        IllegalArgumentException exNonAscii = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals(
                    "User:admin,Üser:broker"));
        assertTrue(exNonAscii.getMessage().toLowerCase().contains("comma"),
            "non-ASCII type letter comma typo must be rejected; got: "
                + exNonAscii.getMessage());

        // ---- Regression: legitimate SSL DN with multiple `=`-bearing
        //      RDN attributes is still accepted. This is the critical
        //      no-false-positive assertion for the broader regex —
        //      every X.500 attribute separator is `,<attr>=` (NEVER
        //      `,<attr>:`), so the new regex `,[^=,:]*:` does not match.
        String sslDn = "User:CN=Broker One,OU=Kafka Brokers,O=Example Corp,C=US";
        java.util.Set<String> okSsl = RuleEngine.parseBypassPrincipals(sslDn);
        assertEquals(1, okSsl.size(),
            "SSL DN with X.500 commas must remain accepted under the "
                + "broader regex; got: " + okSsl);
        assertTrue(okSsl.contains(sslDn));

        // ---- Regression: a single standalone Service-Account principal
        //      is unaffected. Only the `,<type>:` shape (i.e. the typo)
        //      is rejected; a standalone `Service-Account:bot` parses
        //      cleanly.
        java.util.Set<String> okStandalone = RuleEngine.parseBypassPrincipals(
            "Service-Account:bot");
        assertEquals(1, okStandalone.size(),
            "standalone Service-Account principal must still parse; got: "
                + okStandalone);
        assertTrue(okStandalone.contains("Service-Account:bot"));

        // ---- Regression: a properly-`;`-separated mix of plain User
        //      and Service-Account principals also parses cleanly. This
        //      pins the contract that the discriminator is about the
        //      `,` typo, not about Service-Account specifically.
        java.util.Set<String> okMixed = RuleEngine.parseBypassPrincipals(
            "User:broker;Service-Account:client");
        assertEquals(2, okMixed.size(),
            "properly `;`-separated User + Service-Account must parse; "
                + "got: " + okMixed);
        assertTrue(okMixed.contains("User:broker"));
        assertTrue(okMixed.contains("Service-Account:client"));
    }

    @Test
    public void parseBypassPrincipalsRejectsCommaAndSemicolonConfusableCodepoints() {
        // R29 #278 [HIGH]: an adversarial audit of the R29 #272 broader
        // regex found a SEVENTH bypass shape: non-ASCII codepoints that
        // are VISUALLY identical (or near-identical) to ASCII `,` or `;`
        // are not caught by the existing ASCII-only regex, and the
        // invisible-codepoint check (firstInvisibleCodePointLabel) only
        // covers C0/C1/zero-width/bidi — NOT printable punctuation.
        //
        // The realistic operator scenario: a Japanese / Chinese / Korean
        // IME produces FULLWIDTH COMMA (U+FF0C, `，`) by default when
        // pressed alongside a CJK input mode. An operator pasting from
        // a CJK-edited admin doc, or typing on a CJK keyboard, can
        // produce `User:admin，User:broker` with U+FF0C standing in for
        // the ASCII `,`. The string passes:
        //
        //   1. raw.split(";") — no ASCII `;`, single segment
        //   2. parseKafkaPrincipal split(":", 2) → type=`User`,
        //      name=`admin，User:broker`
        //   3. whitespace, invisible-codepoint, type-comma, ASCII-comma
        //      checks — all pass (U+FF0C is visible printable
        //      punctuation, NOT in any rejected set)
        //   4. set entry stored as `User:admin，User:broker` — non-empty
        //      so the BrokerServer empty-set startup gate accepts.
        //   5. runtime peer principal is `User:broker` — NEVER matches
        //      → silent soft-brick of inter-broker traffic.
        //
        // The same hazard exists for SEMICOLON confusables (the
        // `split(";")` entry separator): FULLWIDTH SEMICOLON U+FF1B
        // (`；`) and GREEK QUESTION MARK U+037E (`;` — visually identical
        // to ASCII `;` in most fonts).
        //
        // Discriminator: enumerate the well-known comma- and semicolon-
        // confusable codepoint sets in firstConfusableSeparatorLabel,
        // reject any occurrence with a "looks-like-a-separator" diagnostic
        // BEFORE the ASCII checks. The list mirrors the structure of the
        // existing firstInvisibleCodePointLabel helper.

        // ---- Comma confusables (representative subset) ----
        String[] commaConfusables = new String[] {
            // U+FF0C FULLWIDTH COMMA — CJK IME default
            "User:admin，User:broker",
            // U+3001 IDEOGRAPHIC COMMA — Japanese
            "User:admin、User:broker",
            // U+060C ARABIC COMMA — Arabic/Persian keyboards
            "User:admin،User:broker",
            // U+055D ARMENIAN COMMA
            "User:admin՝User:broker",
            // U+FE50 SMALL COMMA — presentation form
            "User:admin﹐User:broker",
            // U+FF64 HALFWIDTH IDEOGRAPHIC COMMA
            "User:admin､User:broker",
        };
        for (String input : commaConfusables) {
            IllegalArgumentException ex = org.junit.jupiter.api.Assertions
                .assertThrows(IllegalArgumentException.class,
                    () -> RuleEngine.parseBypassPrincipals(input),
                    "comma-confusable bypass must be rejected: '" + input + "'");
            String msg = ex.getMessage().toLowerCase();
            assertTrue(msg.contains("confusable") || msg.contains("look")
                    || msg.contains("comma"),
                "diagnostic must name the comma-confusable hazard; got: "
                    + ex.getMessage());
            assertTrue(ex.getMessage().contains(";"),
                "diagnostic must name the correct separator `;`; got: "
                    + ex.getMessage());
        }

        // ---- Semicolon confusables (split(";") entry-separator) ----
        String[] semicolonConfusables = new String[] {
            // U+FF1B FULLWIDTH SEMICOLON — CJK IME default
            "User:admin；User:broker",
            // U+037E GREEK QUESTION MARK — visually identical to ASCII `;`
            "User:admin;User:broker",
            // U+FE54 SMALL SEMICOLON — presentation form
            "User:admin﹔User:broker",
        };
        for (String input : semicolonConfusables) {
            IllegalArgumentException ex = org.junit.jupiter.api.Assertions
                .assertThrows(IllegalArgumentException.class,
                    () -> RuleEngine.parseBypassPrincipals(input),
                    "semicolon-confusable bypass must be rejected: '"
                        + input + "'");
            String msg = ex.getMessage().toLowerCase();
            assertTrue(msg.contains("confusable") || msg.contains("look")
                    || msg.contains("semicolon"),
                "diagnostic must name the semicolon-confusable hazard; got: "
                    + ex.getMessage());
        }

        // ---- Confusable in TYPE (not just NAME) ----
        // `Service，Account:bot` — a fullwidth comma inside the parsed
        // type. The existing typeContains-comma check would not catch this
        // (it scans for ASCII `,`). Must still reject.
        IllegalArgumentException exType = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals(
                    "Service，Account:bot"));
        assertTrue(exType.getMessage().toLowerCase().contains("confusable")
                || exType.getMessage().toLowerCase().contains("comma"),
            "confusable in TYPE must be rejected; got: " + exType.getMessage());

        // ---- LogSafe regression: diagnostic must not embed raw CR /
        //      non-ASCII bytes that could forge a startup log line.
        IllegalArgumentException exLog = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals(
                    "User:admin，User:broker\rX"));
        assertFalse(exLog.getMessage().contains("\r"),
            "confusable diagnostic must sanitise CR; got: "
                + exLog.getMessage());

        // ---- Regression guard: legitimate CJK name (Japanese ideographs
        //      in CN) with NO confusable separator is STILL accepted.
        //      `User:CN=東京一郎,OU=営業,O=Example,C=JP` — every comma is
        //      ASCII U+002C and every X.500 attribute uses `=`. The
        //      CJK codepoints (U+6771 etc.) are NOT in any rejected set.
        String cjkSslDn = "User:CN=東京一郎,OU=営業,"
            + "O=Example,C=JP";
        java.util.Set<String> okCjk = RuleEngine.parseBypassPrincipals(cjkSslDn);
        assertEquals(1, okCjk.size(),
            "legitimate CJK SSL DN with ASCII commas-as-X.500-separators "
                + "must remain accepted; got: " + okCjk);
        assertTrue(okCjk.contains(cjkSslDn));

        // ---- Regression guard: legitimate Arabic name in CN is accepted.
        //      `User:CN=محمد,OU=Ops`. The ASCII comma is the X.500
        //      separator; the Arabic letters (U+0645 U+062D U+0645 U+062F)
        //      are NOT comma-confusables.
        String arabicSslDn = "User:CN=محمد,OU=Ops";
        java.util.Set<String> okArabic =
            RuleEngine.parseBypassPrincipals(arabicSslDn);
        assertEquals(1, okArabic.size(),
            "legitimate Arabic SSL DN must remain accepted; got: " + okArabic);
        assertTrue(okArabic.contains(arabicSslDn));

        // ---- Order-of-checks: an entry that has BOTH an invisible
        //      codepoint AND a comma-confusable still throws (operator
        //      gets ONE diagnostic; the more specific invisible-codepoint
        //      diagnostic should win because invisible codepoints are the
        //      higher-severity smuggling vector).
        IllegalArgumentException exBoth = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals(
                    "User:admin​，User:broker"));
        assertTrue(exBoth.getMessage().toLowerCase().contains("invisible")
                || exBoth.getMessage().toLowerCase().contains("zero-width"),
            "when both invisible AND confusable present, the more specific "
                + "invisible diagnostic should fire first; got: "
                + exBoth.getMessage());
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

    @Test
    public void parseBypassPrincipalsRejectsInvisibleCodePointsInNameOrType() {
        // Round-19 BLOCKER (R19-A #1 / R19-D BLOCKER-1): PROMPT.md operator
        // contract claims parseBypassPrincipals aborts broker startup on
        // entries containing internal C0/C1/zero-width/bidi codepoints.
        // Without this rejection, a typo embedding an invisible codepoint
        // (e.g. zero-width space inside `broker`, indistinguishable on
        // screen) parses successfully but produces an allow-list entry
        // whose canonical form can never match a runtime peer principal —
        // a silent under-grant of the bypass that doesn't surface until
        // request time, when ALL traffic is being denied. The check must
        // run at startup so the operator sees the typo loudly.
        //
        // Each branch below uses a codepoint from one of the four contract
        // categories, embedded INTERNALLY in either the type or the name.
        // Leading/trailing whitespace is already caught by the existing
        // whitespace check (parseBypassPrincipalsThrowsOnPaddedComponent),
        // so this test focuses on the interior position.

        // ---- C0 control char (internal TAB inside name) ----
        IllegalArgumentException c0 = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User:bro\tker"));
        assertTrue(c0.getMessage().contains("C0 control")
            && c0.getMessage().contains("principal name"),
            "internal C0 (TAB) in name must abort startup with a C0-control "
                + "diagnostic; got: " + c0.getMessage());

        // ---- C1 control char (internal NEL U+0085 inside type) ----
        IllegalArgumentException c1 = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User:broker"));
        assertTrue(c1.getMessage().contains("C1 control")
            && c1.getMessage().contains("principal type"),
            "internal C1 (NEL U+0085) in type must abort startup with a "
                + "C1-control diagnostic; got: " + c1.getMessage());

        // ---- Zero-width space (internal ZWSP U+200B inside name) ----
        // This is the headline silent-under-grant case: `User:broker​` and
        // `User:broker` are visually identical, but the first contains a
        // ZWSP and parses to an unreachable allow-list entry. Operators
        // copy/paste principal strings out of dashboards, chat threads,
        // and tickets, all of which can carry zero-width contamination.
        IllegalArgumentException zwsp = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User:bro​ker"));
        assertTrue(zwsp.getMessage().contains("zero-width")
            && zwsp.getMessage().contains("U+200B")
            && zwsp.getMessage().contains("principal name"),
            "internal ZWSP (U+200B) in name must abort startup with a "
                + "zero-width diagnostic naming the codepoint; got: "
                + zwsp.getMessage());

        // ---- BOM / ZWNBSP (U+FEFF inside name) ----
        IllegalArgumentException bom = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User:bro﻿ker"));
        assertTrue(bom.getMessage().contains("zero-width")
            && bom.getMessage().contains("U+FEFF"),
            "internal BOM (U+FEFF) must abort startup with a zero-width "
                + "diagnostic naming the codepoint; got: " + bom.getMessage());

        // ---- Bidi override (LRE U+202A inside type) ----
        IllegalArgumentException lre = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("Us‪er:broker"));
        assertTrue(lre.getMessage().contains("bidi override")
            && lre.getMessage().contains("U+202A")
            && lre.getMessage().contains("principal type"),
            "internal LRE (U+202A) in type must abort startup with a "
                + "bidi-override diagnostic naming the codepoint; got: "
                + lre.getMessage());

        // ---- Bidi isolate (LRI U+2066 inside name) ----
        IllegalArgumentException lri = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User:bro⁦ker"));
        assertTrue(lri.getMessage().contains("bidi isolate")
            && lri.getMessage().contains("U+2066"),
            "internal LRI (U+2066) must abort startup with a bidi-isolate "
                + "diagnostic naming the codepoint; got: " + lri.getMessage());

        // Round-22 (R22-#163): close the bidi-isolate set — RLI/FSI/PDI
        // (U+2067/U+2068/U+2069) reorder display the same way as LRI and
        // were missing from the rejection set. Same hazard as LRI: an entry
        // like "User:broker⁧" displays as "User:broker" but its
        // canonical form never matches a runtime peer principal.
        IllegalArgumentException rli = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User:bro⁧ker"));
        assertTrue(rli.getMessage().contains("bidi isolate")
            && rli.getMessage().contains("U+2067"),
            "internal RLI (U+2067) must abort startup with a bidi-isolate "
                + "diagnostic naming the codepoint; got: " + rli.getMessage());
        IllegalArgumentException fsi = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User:bro⁨ker"));
        assertTrue(fsi.getMessage().contains("bidi isolate")
            && fsi.getMessage().contains("U+2068"),
            "internal FSI (U+2068) must abort startup with a bidi-isolate "
                + "diagnostic naming the codepoint; got: " + fsi.getMessage());
        IllegalArgumentException pdi = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User:bro⁩ker"));
        assertTrue(pdi.getMessage().contains("bidi isolate")
            && pdi.getMessage().contains("U+2069"),
            "internal PDI (U+2069) must abort startup with a bidi-isolate "
                + "diagnostic naming the codepoint; got: " + pdi.getMessage());

        // Round-22 (R22-#212): strong directional marks — LRM/RLM/ALM
        // (U+200E/U+200F/U+061C) reorder display in bidi-aware admin viewers
        // without changing codepoint content. An entry like
        // "User:broker‎" renders as "User:broker" but its canonical
        // form never matches a runtime peer principal, silently
        // under-granting the bypass. Reject with a "bidi mark" diagnostic.
        IllegalArgumentException lrm = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User:broker‎"));
        assertTrue(lrm.getMessage().contains("bidi mark")
            && lrm.getMessage().contains("U+200E"),
            "trailing LRM (U+200E) must abort startup with a bidi-mark "
                + "diagnostic naming the codepoint; got: " + lrm.getMessage());
        IllegalArgumentException rlm = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User:broker‏"));
        assertTrue(rlm.getMessage().contains("bidi mark")
            && rlm.getMessage().contains("U+200F"),
            "trailing RLM (U+200F) must abort startup with a bidi-mark "
                + "diagnostic naming the codepoint; got: " + rlm.getMessage());
        IllegalArgumentException alm = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User؜:broker"));
        assertTrue(alm.getMessage().contains("bidi mark")
            && alm.getMessage().contains("U+061C"),
            "ALM (U+061C) in type must abort startup with a bidi-mark "
                + "diagnostic naming the codepoint; got: " + alm.getMessage());

        // ---- ZWJ / ZWNJ also rejected (U+200C, U+200D) ----
        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> RuleEngine.parseBypassPrincipals("User:bro‌ker"),
            "ZWNJ (U+200C) in name must abort startup");
        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> RuleEngine.parseBypassPrincipals("User:bro‍ker"),
            "ZWJ (U+200D) in name must abort startup");

        // ---- DEL (U+007F) also rejected ----
        IllegalArgumentException del = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User:broker"));
        assertTrue(del.getMessage().contains("C0 control")
            && del.getMessage().contains("U+007F"),
            "DEL (U+007F) must be reported as C0 control; got: "
                + del.getMessage());

        // ---- Sanity: regression guard for the legitimate SSL DN path ----
        // The SSL-DN test asserts internal ASCII space is OK. Pin the
        // *negative* shape of the new check: nothing in `CN=Broker One,...`
        // is in any of the rejected ranges (space is U+0020 — outside C0,
        // not classified zero-width or bidi). If the implementation ever
        // accidentally widens to include U+0020, this test would catch
        // that immediately because the SSL DN test would start failing
        // with a "C0 control" message — but pin it explicitly here too so
        // a failure points directly at codepoint-rejection scope.
        String sslDn = "User:CN=Broker One,OU=Kafka Brokers,O=Example Corp,C=US";
        java.util.Set<String> okSsl = RuleEngine.parseBypassPrincipals(sslDn);
        assertEquals(1, okSsl.size(),
            "internal ASCII space (U+0020) must NOT be rejected by the "
                + "codepoint check; got: " + okSsl);
    }

    @Test
    public void parseBypassPrincipalsErrorMessagesAreLogSafe() {
        // Round-20 HIGH A-1: every diagnostic in parseBypassPrincipals
        // embeds the operator-supplied `trimmed` slot, which is then
        // propagated by BrokerServer.startup into a fatal SLF4J log line.
        // `String.trim()` at the top of the loop only strips edge ASCII
        // whitespace, so interior CR/LF/DEL/other-control codepoints reach
        // every throw site below. Without LogSafe.sanitize, a value like
        // `"User\r:broker"` would forge a second log line, and a value
        // carrying a bidi-override could smuggle ANSI sequences. Pin that
        // each of the five throw sites in parseBypassPrincipals replaces
        // raw control codepoints with the printable backslash-uXXXX form.
        //
        // Five sites total (in source order):
        //   1. SecurityUtils.parseKafkaPrincipal missing-':'-separator
        //      (clients/ — re-thrown locally with sanitised message)
        //   2. blank/all-whitespace/padded principal TYPE
        //   3. blank/all-whitespace/padded principal NAME
        //   4. invisible codepoint in TYPE
        //   5. invisible codepoint in NAME
        //
        // The exact escape form is LogSafe's contract: control chars become
        // backslash-uXXXX literal text — assert the raw CR is gone AND the
        // sanitised form is present.

        // ---- site 1: missing-':' separator with embedded \r ----
        IllegalArgumentException miss = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User\rbroker"));
        assertFalse(miss.getMessage().contains("\r"),
            "missing-separator diagnostic must not embed raw CR; got: "
                + miss.getMessage());
        assertTrue(miss.getMessage().contains("\\u000D"),
            "missing-separator diagnostic must show CR as \\u000D; got: "
                + miss.getMessage());

        // ---- site 2: trailing whitespace in TYPE via interior \r ----
        // `raw = "User\r:broker"` → segment.trim() is a no-op (`U` and `r`
        // are non-whitespace), trimmed = "User\r:broker". parseKafkaPrincipal
        // succeeds (one ':'), type="User\r", name="broker".
        // hasLeadingOrTrailingWhitespace(type) is TRUE because \r is
        // whitespace — branch fires with trimmed embedded.
        IllegalArgumentException wsType = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User\r:broker"));
        assertFalse(wsType.getMessage().contains("\r"),
            "type-whitespace diagnostic must not embed raw CR; got: "
                + wsType.getMessage());
        assertTrue(wsType.getMessage().contains("\\u000D"),
            "type-whitespace diagnostic must show CR as \\u000D; got: "
                + wsType.getMessage());

        // ---- site 3: leading whitespace in NAME with embedded LF ----
        // `raw = "User: broker\nX"` → segment.trim() no-op (edges 'U' and
        // 'X' are non-whitespace), trimmed="User: broker\nX".
        // parseKafkaPrincipal gives type="User", name=" broker\nX". The
        // leading space in `name` trips the trailing-whitespace branch
        // with `\n` still embedded in trimmed — this is the realistic
        // attack path: an operator config like `User: broker\n[FATAL]`
        // would otherwise inject a forged FATAL log line into startup.
        IllegalArgumentException wsName = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User: broker\nX"));
        assertFalse(wsName.getMessage().contains("\n"),
            "name-whitespace diagnostic must not embed raw LF; got: "
                + wsName.getMessage());
        assertTrue(wsName.getMessage().contains("\\u000A"),
            "name-whitespace diagnostic must show LF as \\u000A; got: "
                + wsName.getMessage());

        // ---- site 4: invisible codepoint in TYPE with DEL embedded ----
        // DEL (U+007F) is interior in `User` so the whitespace check
        // does not fire — but the codepoint check rejects it. The trimmed
        // slot in that diagnostic must be sanitised too.
        IllegalArgumentException cpType = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User:broker"));
        assertFalse(cpType.getMessage().contains(""),
            "codepoint-type diagnostic must not embed raw DEL; got: "
                + cpType.getMessage());
        assertTrue(cpType.getMessage().contains("\\u007F"),
            "codepoint-type diagnostic must show DEL as \\u007F in trimmed "
                + "slot; got: " + cpType.getMessage());

        // ---- site 5: invisible codepoint in NAME with C1 control ----
        // NEL (U+0085) is interior in `broker` — codepoint check
        // rejects with trimmed in the message. The codepoint label itself
        // is already safe (formatted via `U+%04X`), but the trimmed slot
        // contains the raw NEL byte and must be sanitised.
        IllegalArgumentException cpName = org.junit.jupiter.api.Assertions
            .assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals("User:broker"));
        assertFalse(cpName.getMessage().contains(""),
            "codepoint-name diagnostic must not embed raw C1 NEL; got: "
                + cpName.getMessage());
        assertTrue(cpName.getMessage().contains("\\u0085"),
            "codepoint-name diagnostic must show NEL as \\u0085 in trimmed "
                + "slot; got: " + cpName.getMessage());
    }

    @Test
    public void reentrantEvaluateFromActivationSupplierIsCaughtAndFailsOpen() {
        // Round-8 audit task #100: the per-request CEL step budget is reset
        // on entry and again in a finally on exit. A re-entrant evaluate()
        // call from within the activation supplier (or, hypothetically, from
        // within a rule predicate if user-defined functions are ever added)
        // would call resetEvalStepBudget() in the inner frame and silently
        // grant the outer evaluation a fresh 100k-step budget on return —
        // turning the per-request DoS guarantee into a per-call guarantee.
        //
        // The engine guards this with a ThreadLocal IN_EVALUATE flag that
        // throws IllegalStateException on detected re-entry. Because the
        // throw originates inside the supplier, the outer evaluate()'s
        // catch(Throwable) fail-open branch catches it and returns ALLOW —
        // the supplier "failed" from the engine's perspective, and the
        // documented fail-open posture is the only safe outcome (no
        // activation map ⇒ no rule can be evaluated). That posture matches
        // the existing test invariants for every other unrecoverable
        // supplier failure.
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("r1", ApiKeys.METADATA, "true", 7))
            .build());
        AtomicBoolean reentryAttempted = new AtomicBoolean(false);
        AtomicBoolean reentryThrew = new AtomicBoolean(false);
        java.util.function.Supplier<Map<String, Object>> reentrantSupplier = () -> {
            reentryAttempted.set(true);
            try {
                engine.evaluate(ApiKeys.METADATA, "c", null, false, Collections::emptyMap);
            } catch (IllegalStateException expected) {
                reentryThrew.set(true);
                throw expected;
            }
            return Collections.emptyMap();
        };
        RuleDecision d = engine.evaluate(
            ApiKeys.METADATA, "c", null, false, reentrantSupplier);
        assertTrue(reentryAttempted.get(), "supplier must have been invoked");
        assertTrue(reentryThrew.get(),
            "inner evaluate() must throw IllegalStateException on re-entry");
        assertSame(RuleDecision.ALLOW, d,
            "outer evaluate() must fail open when the supplier throws");
    }

    @Test
    public void reentryFlagIsClearedAfterEvaluation() {
        // The IN_EVALUATE ThreadLocal is removed in a finally, so a thread
        // that completes one evaluation (whether ALLOW, DENY, or fail-open)
        // must accept a fresh evaluation on the same thread immediately
        // after. Pin this: a pool thread serving back-to-back requests must
        // never observe a stale TRUE on the second request.
        //
        // Round-9 concurrency adversarial finding #3: the outer finally that
        // calls IN_EVALUATE.remove() is load-bearing for THREE exit paths,
        // not just the clean DENY return:
        //   - clean rule-loop exit (ALLOW or DENY)
        //   - activation supplier throws ActivationBudgetExceededException
        //     (caught at line 489 → synthetic POLICY_VIOLATION DENY)
        //   - activation supplier throws any other Throwable (caught at
        //     line 517 → fail-open ALLOW)
        // A natural-looking refactor that moves IN_EVALUATE.set(TRUE) inside
        // the inner try would silently leak the flag across requests on the
        // two catch paths. Exercise all three so any such regression breaks
        // a test instead of breaking production.
        RuleEngine engine = new RuleEngine();
        engine.install(new RuleSetBuilder()
            .put(denyRule("r1", ApiKeys.METADATA, "true", 7))
            .build());
        // Path 1 — clean DENY exit.
        RuleDecision d1 = engine.evaluate(
            ApiKeys.METADATA, "c", null, false, Collections::emptyMap);
        assertTrue(d1.denied(), "first evaluation must DENY via r1");
        // Path 2 — supplier throws ActivationBudgetExceededException; engine
        // catches and returns synthetic POLICY_VIOLATION DENY. The outer
        // finally must still clear IN_EVALUATE; otherwise the next call on
        // this thread would throw IllegalStateException from the guard.
        java.util.function.Supplier<Map<String, Object>> abeSupplier = () -> {
            throw new ActivationBudgetExceededException("synthetic-wide-request");
        };
        RuleDecision d2 = engine.evaluate(
            ApiKeys.METADATA, "c", null, false, abeSupplier);
        assertTrue(d2.denied(),
            "ABE-throwing supplier must produce synthetic POLICY_VIOLATION DENY");
        assertEquals(Errors.POLICY_VIOLATION.code(), d2.errorCode(),
            "ABE path must surface POLICY_VIOLATION (44), not a fail-open");
        // Path 3 — supplier throws an arbitrary RuntimeException; engine
        // catches and fail-opens (ALLOW). Same flag-clear invariant.
        java.util.function.Supplier<Map<String, Object>> throwingSupplier = () -> {
            throw new RuntimeException("synthetic-extractor-bug");
        };
        RuleDecision d3 = engine.evaluate(
            ApiKeys.METADATA, "c", null, false, throwingSupplier);
        assertSame(RuleDecision.ALLOW, d3,
            "Throwable-throwing supplier must fail open (no activation ⇒ ALLOW)");
        // Final probe: a fourth evaluation on the same thread after each of
        // the three exit paths above must still run cleanly. If any of the
        // three outer-finally branches above had leaked IN_EVALUATE=TRUE,
        // this call would throw IllegalStateException from the guard at
        // function entry, not reach the rule loop.
        RuleDecision d4 = engine.evaluate(
            ApiKeys.METADATA, "c", null, false, Collections::emptyMap);
        assertTrue(d4.denied(),
            "fourth evaluation on the same thread must DENY cleanly — "
                + "if IN_EVALUATE leaked from any of the prior three paths, "
                + "the guard would have thrown IllegalStateException instead");
    }
}
