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
package org.apache.kafka.server.rules.cel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CelProgramTest {

    /**
     * Round-8 audit HIGH (concurrency): the per-thread CEL step counter is no
     * longer reset by {@link CelProgram#evalBoolean}. With JUnit running
     * tests on a shared thread, a test that exhausts most of the budget
     * could otherwise poison the counter for a subsequent test on the same
     * thread, causing arbitrary unrelated failures depending on test order.
     * The engine resets the counter once per request; we mirror that here
     * once per test method.
     */
    @BeforeEach
    public void resetStepBudget() {
        CelProgram.resetEvalStepBudget();
    }

    /**
     * Leave a clean counter for any test class JUnit schedules next on the
     * same worker thread. This class contains several tests that
     * deliberately exhaust the budget; without this reset, the
     * MAX_EVAL_STEPS-poisoned counter would cause arbitrary unrelated
     * failures in {@code ApiMessageActivation*Test} classes that share the
     * thread pool.
     */
    @AfterEach
    public void leaveCounterClean() {
        CelProgram.resetEvalStepBudget();
    }

    private static boolean evalBool(String expr, Map<String, Object> env) {
        return CelCompiler.compile(expr).evalBoolean(env::get);
    }

    @Test
    public void literalBooleans() {
        assertTrue(evalBool("true", new HashMap<>()));
        assertFalse(evalBool("false", new HashMap<>()));
    }

    @Test
    public void integerComparison() {
        Map<String, Object> env = new HashMap<>();
        env.put("x", 5);
        assertTrue(evalBool("x > 3", env));
        assertTrue(evalBool("x >= 5", env));
        assertFalse(evalBool("x < 5", env));
        assertTrue(evalBool("x == 5", env));
        assertTrue(evalBool("x != 4", env));
    }

    @Test
    public void mixedIntAndLongComparisons() {
        Map<String, Object> env = new HashMap<>();
        env.put("a", 5);
        env.put("b", 7L);
        assertTrue(evalBool("a < b", env));
        assertTrue(evalBool("b > a", env));
        assertFalse(evalBool("a == b", env));
    }

    @Test
    public void equalityPromotesAcrossIntegerWrapperTypes() {
        // Integer 5 and Long 5 must compare equal — they're indistinguishable
        // in CEL's int type. Without promotion, request fields supplied by
        // generated Kafka data classes (mixed int / long getters) would silently
        // never match literal numeric constants in rules.
        Map<String, Object> env = new HashMap<>();
        env.put("asInt", 5);
        env.put("asLong", 5L);
        assertTrue(evalBool("asInt == 5", env));
        assertTrue(evalBool("asLong == 5", env));
        assertTrue(evalBool("asInt == asLong", env));
        assertTrue(evalBool("asInt in [5, 6]", env));
        assertTrue(evalBool("asLong in [5, 6]", env));
    }

    @Test
    public void stringEquality() {
        Map<String, Object> env = new HashMap<>();
        env.put("name", "foo");
        assertTrue(evalBool("name == \"foo\"", env));
        assertFalse(evalBool("name == \"bar\"", env));
    }

    @Test
    public void stringStartsEndsContains() {
        Map<String, Object> env = new HashMap<>();
        env.put("name", "internal-topic");
        assertTrue(evalBool("name.startsWith(\"internal\")", env));
        assertTrue(evalBool("name.endsWith(\"topic\")", env));
        assertTrue(evalBool("name.contains(\"-\")", env));
        assertFalse(evalBool("name.startsWith(\"public\")", env));
    }

    @Test
    public void methodCallNullReceiverPropagatesEvenWhenArgIsNonString() {
        // Round-17 MED: MethodCall.stringArg used to evaluate the argument
        // BEFORE checking the receiver type, so a CEL expression of the
        // shape `request.missing.startsWith(request.someNumeric)` threw
        // CelEvaluationException ("expected string argument, got Long")
        // and the rule fail-OPENed — instead of null-propagating to false
        // the way RegexMatch and Field do. Pin the tri-state behaviour:
        // a non-String receiver returns false WITHOUT ever evaluating the
        // argument (so an arg that would throw must NOT surface).
        Map<String, Object> env = new HashMap<>();
        env.put("count", 42L);
        // `missing` is absent → null receiver via Field null-propagation.
        // The arg `count` is a Long; if the arg were evaluated first under
        // stringArg, it would throw. Tri-state contract: returns false.
        assertFalse(evalBool("missing.startsWith(count)", env));
        assertFalse(evalBool("missing.endsWith(count)", env));
        assertFalse(evalBool("missing.contains(count)", env));
        // Same axis for a non-String receiver that IS in env.
        env.put("notAString", 99L);
        assertFalse(evalBool("notAString.startsWith(count)", env));
        // Sanity: a String receiver with a non-String arg still fail-OPENs
        // by throwing (engine catches CelEvaluationException). This pins
        // that the Round-17 fix only changed the null-receiver behaviour.
        env.put("name", "foo");
        assertThrows(CelEvaluationException.class,
            () -> evalBool("name.startsWith(count)", env),
            "non-String arg with String receiver still surfaces type mismatch");
    }

    @Test
    public void stringMatchesRegex() {
        Map<String, Object> env = new HashMap<>();
        env.put("name", "audit-1");
        assertTrue(evalBool("name.matches(\"audit-[0-9]+\")", env));
        assertFalse(evalBool("name.matches(\"x.*\")", env));
    }

    @Test
    public void logicalAndOrNot() {
        Map<String, Object> env = new HashMap<>();
        env.put("a", true);
        env.put("b", false);
        assertTrue(evalBool("a || b", env));
        assertFalse(evalBool("a && b", env));
        assertTrue(evalBool("!b", env));
        assertTrue(evalBool("!(a && b)", env));
    }

    @Test
    public void shortCircuitDoesNotEvaluateRhs() {
        Map<String, Object> env = new HashMap<>();
        env.put("a", false);
        // `missing` is not in env; a false short-circuits and prevents the failing lookup.
        assertFalse(evalBool("a && missing.foo == 1", env));
        env.put("b", true);
        assertTrue(evalBool("b || missing.foo == 1", env));
    }

    @Test
    public void dottedFieldAccess() {
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("topicName", "events.v1");
        req.put("validateOnly", true);
        Map<String, Object> env = new HashMap<>();
        env.put("request", req);
        assertTrue(evalBool("request.topicName == \"events.v1\"", env));
        assertTrue(evalBool("request.validateOnly", env));
    }

    @Test
    public void indexAccessOnList() {
        Map<String, Object> topic0 = new LinkedHashMap<>();
        topic0.put("name", "alpha");
        Map<String, Object> topic1 = new LinkedHashMap<>();
        topic1.put("name", "beta");
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("topics", Arrays.asList(topic0, topic1));
        Map<String, Object> env = new HashMap<>();
        env.put("request", req);
        assertTrue(evalBool("request.topics[0].name == \"alpha\"", env));
        assertTrue(evalBool("request.topics[1].name == \"beta\"", env));
    }

    @Test
    public void sizeBuiltin() {
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("topics", Arrays.asList(1, 2, 3));
        Map<String, Object> env = new HashMap<>();
        env.put("request", req);
        assertTrue(evalBool("size(request.topics) == 3", env));
        assertTrue(evalBool("size(request.topics) > 2", env));
    }

    @Test
    public void inOperatorForList() {
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("name", "audit");
        Map<String, Object> env = new HashMap<>();
        env.put("request", req);
        assertTrue(evalBool("request.name in [\"foo\", \"audit\", \"bar\"]", env));
        assertFalse(evalBool("request.name in [\"foo\", \"bar\"]", env));
    }

    @Test
    public void existentialMacroOverList() {
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("name", "alpha");
        Map<String, Object> b = new LinkedHashMap<>();
        b.put("name", "audit");
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("topics", Arrays.asList(a, b));
        Map<String, Object> env = new HashMap<>();
        env.put("request", req);
        assertTrue(evalBool("request.topics.exists(t, t.name.startsWith(\"audit\"))", env));
        assertFalse(evalBool("request.topics.exists(t, t.name == \"missing\")", env));
        assertTrue(evalBool("request.topics.all(t, size(t.name) >= 5)", env));
        assertFalse(evalBool("request.topics.all(t, t.name.startsWith(\"audit\"))", env));
    }

    @Test
    public void comprehensionPredicateRequiresBooleanResult() {
        // Round-20 MED D-1: the engine elsewhere is fail-noisy on non-Boolean
        // in boolean position (Not / Negate throw CelEvaluationException)
        // and tri-state on null (Field / RegexMatch / Compare / InList
        // propagate null). The pre-fix Comprehension.eval coerced every
        // non-Boolean predicate result to false silently, including strings
        // / numbers / lists / maps — hiding both an authorship bug
        // (`xs.exists(x, x.name)` forgot the comparison) and an entire
        // category of true matches the rule author intended.
        //
        // Two contracts to pin:
        //   (a) non-null non-Boolean predicate result → throw → engine
        //       fails open.
        //   (b) null predicate result is treated as false-y (EXISTS skips,
        //       ALL falsifies) — matching the engine's tri-state null
        //       propagation in other operators. Existing null-predicate
        //       behaviour is preserved.
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("name", "alpha");
        Map<String, Object> b = new LinkedHashMap<>();
        b.put("name", "beta");
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("topics", Arrays.asList(a, b));
        Map<String, Object> env = new HashMap<>();
        env.put("request", req);

        // (a-EXISTS) predicate returns a String — must throw, not return false.
        CelEvaluationException existsStringEx = assertThrows(CelEvaluationException.class,
            () -> evalBool("request.topics.exists(t, t.name)", env),
            "non-Boolean predicate (String) in EXISTS must throw, not silently coerce to false");
        // Round-21 MED (Agent 5 F-4 follow-up): pin the throw MESSAGE shape and the
        // offending type so the engine's diagnostic identifies the failed contract
        // by name (rather than the test being green on any throw, including an
        // arithmetic budget trip or a re-entry guard fire).
        assertTrue(existsStringEx.getMessage().contains("comprehension predicate must return boolean"),
            "throw message must identify the contract violated; got: " + existsStringEx.getMessage());
        assertTrue(existsStringEx.getMessage().contains("String"),
            "throw message must include the offending Java type; got: " + existsStringEx.getMessage());

        // (a-ALL) predicate returns a String — must throw, not return true (every coercion is false → ALL of-empty-truthy is false, but the early-return path is false-by-coercion which is itself a silent bug).
        assertThrows(CelEvaluationException.class,
            () -> evalBool("request.topics.all(t, t.name)", env),
            "non-Boolean predicate (String) in ALL must throw, not silently coerce to false");

        // (a-numeric) predicate returns a number — must throw.
        Map<String, Object> withNumbers = new HashMap<>();
        withNumbers.put("xs", Arrays.asList(1L, 2L, 3L));
        CelEvaluationException existsLongEx = assertThrows(CelEvaluationException.class,
            () -> evalBool("xs.exists(x, x)", withNumbers),
            "non-Boolean predicate (Long) in EXISTS must throw");
        assertTrue(existsLongEx.getMessage().contains("Long"),
            "throw message must include the offending Java type; got: " + existsLongEx.getMessage());

        // (b-null EXISTS) — Round-21 MED (Agent 5 F-4): the prior shape
        // `t.missing == "x"` was unsuitable for pinning the null arm.
        // valueEquals delegates to Objects.equals(null, "x"), which returns
        // Boolean.FALSE — not null. So that case hit branch (2)
        // `v instanceof Boolean` of Comprehension.eval, not branch (1)
        // `v == null`. A mutation that removed branch (1) would have left
        // the test green. Switch to a BARE missing field access
        // (`t.missing`): Field.eval(CelNode.java:84-91) returns null
        // when the receiver Map lacks the key, and the comprehension then
        // sees v == null directly. THIS is the case branch (1) defends.
        assertFalse(evalBool("request.topics.exists(t, t.missing)", env),
            "bare null predicate (Field.eval on missing key returns null) must be coerced "
                + "to false in EXISTS via Comprehension's `v == null` arm — not throw");

        // (b-null ALL) symmetric: null-predicate must falsify ALL without throwing.
        assertFalse(evalBool("request.topics.all(t, t.missing)", env),
            "bare null predicate must falsify ALL via Comprehension's `v == null` arm — not throw");

        // (b-control) — pin that the (b) cases above truly exercise the null
        // arm rather than the false-from-equality arm: a predicate
        // `t.missing == "x"` (the old shape) ALSO returns false but via
        // `valueEquals(null, "x") == Boolean.FALSE`. Keeping it here as a
        // companion so a future reader sees BOTH paths exercised:
        //   - bare `t.missing`         → null   → branch (1)  [the new pin]
        //   - `t.missing == "x"`       → false  → branch (2)  [the prior pin]
        assertFalse(evalBool("request.topics.exists(t, t.missing == \"x\")", env),
            "null-then-equality path must also resolve to false (branch (2) via Boolean.FALSE)");
    }

    @Test
    public void compilationFailsOnSyntaxError() {
        assertThrows(CelCompilationException.class, () -> CelCompiler.compile("foo &&"));
        assertThrows(CelCompilationException.class, () -> CelCompiler.compile("(unclosed"));
        assertThrows(CelCompilationException.class, () -> CelCompiler.compile(""));
    }

    @Test
    public void evaluationOfMissingIdentifierIsFalseInBooleanContext() {
        // A reference to a non-existent identifier should not throw — it produces null,
        // which is falsy in a Boolean context. This matters because rule predicates
        // running over heterogeneous Kafka APIs should not blow up on missing fields.
        assertFalse(evalBool("missing == 1", new HashMap<>()));
        assertFalse(evalBool("missing.foo == 1", new HashMap<>()));
    }

    @Test
    public void numericLiteralsAndArithmetic() {
        Map<String, Object> env = new HashMap<>();
        env.put("n", 10);
        assertTrue(evalBool("n + 2 == 12", env));
        assertTrue(evalBool("n - 5 == 5", env));
        assertTrue(evalBool("n * 3 == 30", env));
    }

    @Test
    public void sourceTextIsPreserved() {
        CelProgram p = CelCompiler.compile("request.name == \"x\"");
        assertEquals("request.name == \"x\"", p.source());
    }

    @Test
    public void sourceLengthIsBoundedBeforeLex() {
        // Codex final-audit P1#5: a multi-megabyte source — typically a
        // giant string or regex literal — must be rejected BEFORE tokenising,
        // not after. Without the MAX_EXPR_LEN gate the lexer allocates one
        // Token per character span, which can pin tens of megabytes of token
        // objects in the rule-load thread before MAX_NODES catches up at
        // parse time.
        StringBuilder sb = new StringBuilder("\"");
        for (int n = 0; n < CelLimits.MAX_EXPR_LEN + 16; n++) {
            sb.append('x');
        }
        sb.append("\"");
        CelCompilationException ex = assertThrows(
            CelCompilationException.class,
            () -> CelCompiler.compile(sb.toString()));
        assertTrue(ex.getMessage().contains("MAX_EXPR_LEN"),
            "expected MAX_EXPR_LEN error, got: " + ex.getMessage());
    }

    @Test
    public void parseDepthIsBoundedAgainstChainedNotOperators() {
        // A hand-rolled recursive descent compiler will overflow the JVM
        // stack on a deeply-chained unary `!`. The parser must refuse the
        // input at compile time with a CelCompilationException rather than
        // crash the rule-loader thread.
        StringBuilder sb = new StringBuilder();
        for (int n = 0; n < CelLimits.MAX_PARSE_DEPTH + 16; n++) {
            sb.append('!');
        }
        sb.append("x");
        CelCompilationException ex = assertThrows(
            CelCompilationException.class,
            () -> CelCompiler.compile(sb.toString()));
        assertTrue(ex.getMessage().contains("parse depth"),
            "expected parse-depth error, got: " + ex.getMessage());
    }

    @Test
    public void parseDepthIsBoundedAgainstChainedParentheses() {
        // Same defence, different vector: deeply-nested grouping descends
        // through parseExpr on every layer of parens. Bound it.
        StringBuilder open = new StringBuilder();
        StringBuilder close = new StringBuilder();
        for (int n = 0; n < CelLimits.MAX_PARSE_DEPTH + 16; n++) {
            open.append('(');
            close.append(')');
        }
        String src = open + "x" + close;
        assertThrows(CelCompilationException.class, () -> CelCompiler.compile(src));
    }

    @Test
    public void nodeBudgetIsEnforcedForLargeListLiterals() {
        // A long-but-shallow source like `[1,1,1,...]` does not blow the
        // parse stack but does allocate one Literal node per element. The
        // node budget catches it at compile time so an attacker-crafted
        // rule cannot exhaust broker heap during parsing.
        StringBuilder sb = new StringBuilder("[");
        int items = CelLimits.MAX_NODES + 16;
        for (int n = 0; n < items; n++) {
            if (n > 0) sb.append(',');
            sb.append('1');
        }
        sb.append(']');
        CelCompilationException ex = assertThrows(
            CelCompilationException.class,
            () -> CelCompiler.compile(sb.toString()));
        assertTrue(ex.getMessage().contains("node budget"),
            "expected node-budget error, got: " + ex.getMessage());
    }

    @Test
    public void matchesPatternIsCompiledAtRuleLoadTime() {
        // A malformed regex must surface to the operator at rule load, not
        // as a runtime exception on the request thread. Pre-compilation in
        // the parser is what enforces this.
        CelCompilationException ex = assertThrows(
            CelCompilationException.class,
            () -> CelCompiler.compile("name.matches(\"[\")"));
        assertTrue(ex.getMessage().contains("invalid regex"),
            "expected invalid-regex error, got: " + ex.getMessage());
    }

    @Test
    public void matchesRejectsDynamicPatternAtCompileTime() {
        // Dynamic regex patterns defeat pre-compilation, defeat compile-time
        // validation, and would require per-request Pattern.compile on the
        // hot path. The parser refuses them — operators who need pattern
        // variation should supply multiple literal-pattern rules.
        CelCompilationException ex = assertThrows(
            CelCompilationException.class,
            () -> CelCompiler.compile("name.matches(other)"));
        assertTrue(ex.getMessage().contains("literal"),
            "expected literal-pattern error, got: " + ex.getMessage());
    }

    @Test
    public void matchesRejectsOversizedReceiverInputAtRuntime() {
        // Even with a pre-compiled regex, catastrophic backtracking on a
        // megabyte-long input can stall the request thread. Cap the receiver
        // length; a request crafted with an extremely long string field
        // raises CelEvaluationException, which the engine fails open on.
        Map<String, Object> env = new HashMap<>();
        StringBuilder huge = new StringBuilder();
        for (int n = 0; n < CelLimits.MAX_REGEX_INPUT_LENGTH + 16; n++) {
            huge.append('a');
        }
        env.put("name", huge.toString());
        CelEvaluationException ex = assertThrows(
            CelEvaluationException.class,
            () -> evalBool("name.matches(\"a+\")", env));
        assertTrue(ex.getMessage().contains("receiver length"),
            "expected receiver-length error, got: " + ex.getMessage());
    }

    @Test
    public void evalStepBudgetKillsNestedComprehensionBlowup() {
        // Nested exists/all over attacker-controlled lists is O(N^depth).
        // The runtime step budget bounds it: a request that would otherwise
        // execute billions of iterations is killed at MAX_EVAL_STEPS and the
        // engine fails the rule open (logged and skipped).
        //
        // We need a predicate that does NOT short-circuit. Two nested
        // .exists with an always-false predicate: outer iterates all N,
        // inner exhausts all N before returning false, total N*N steps.
        int side = 400; // 400 * 400 = 160_000 > MAX_EVAL_STEPS=100_000
        java.util.List<Integer> outer = new java.util.ArrayList<>(side);
        for (int n = 0; n < side; n++) {
            outer.add(n);
        }
        Map<String, Object> env = new HashMap<>();
        env.put("xs", outer);
        assertThrows(
            CelEvaluationException.class,
            () -> evalBool("xs.exists(a, xs.exists(b, a == -1 && b == -1))", env));
    }

    @Test
    public void evalStepCounterIsResetBetweenInvocationsWhenCallerResets() {
        // Round-8 audit HIGH (concurrency): the per-thread step counter is
        // now reset by RuleEngine.evaluate once per REQUEST, not by
        // CelProgram.evalBoolean once per rule. Direct callers (this test,
        // future tooling) MUST therefore call resetEvalStepBudget() between
        // evaluations if they want a fresh budget — without it, the budget
        // legitimately accumulates across calls within the same logical
        // request, which is the intended DoS-defence semantics (an attacker
        // cannot multiply the cap by N rules).
        //
        // The counter is a ThreadLocal; without explicit reset it would
        // accumulate across requests on the same broker thread and trip
        // arbitrarily early for the second request. This test exercises the
        // reset itself, mirroring the reset RuleEngine.evaluate now performs.
        java.util.List<Integer> items = new java.util.ArrayList<>();
        for (int n = 0; n < 1000; n++) {
            items.add(n);
        }
        Map<String, Object> env = new HashMap<>();
        env.put("xs", items);
        // 1000 iterations per call; well under the budget. Run it many
        // times — should never trip, because we reset between each call
        // the way the engine does between requests.
        CelProgram p = CelCompiler.compile("xs.exists(a, a == -1)");
        for (int n = 0; n < 200; n++) {
            CelProgram.resetEvalStepBudget();
            assertFalse(p.evalBoolean(env::get));
        }
    }

    @Test
    public void evalStepCounterAccumulatesWithoutCallerReset() {
        // Round-8 audit HIGH (concurrency) pin: prove that without an explicit
        // reset, the step counter accumulates across evalBoolean calls. This
        // is the budget contract: a single RuleEngine.evaluate iterates many
        // rules under one budget, and the engine relies on this accumulation
        // to cap per-request CEL work at MAX_EVAL_STEPS regardless of how
        // many rules an operator has authored. Anyone who deletes the reset
        // in RuleEngine.evaluate must break this test, surfacing the change
        // as a visible regression rather than a silent re-amplification.
        //
        // Start fresh so this test doesn't depend on accumulated state from
        // prior tests on the same thread.
        CelProgram.resetEvalStepBudget();
        java.util.List<Integer> items = new java.util.ArrayList<>();
        // Just over half the budget per call, so two consecutive calls
        // without a reset must trip the second one.
        int half = CelLimits.MAX_EVAL_STEPS / 2 + 16;
        for (int n = 0; n < half; n++) {
            items.add(n);
        }
        Map<String, Object> env = new HashMap<>();
        env.put("xs", items);
        env.put("needle", -1); // never matches → full scan each call
        CelProgram p = CelCompiler.compile("needle in xs");
        // First call: ~half the budget consumed, succeeds.
        assertFalse(p.evalBoolean(env::get));
        // Second call without reset: combined ~half+half = full budget +
        // overhead → must trip.
        assertThrows(
            CelEvaluationException.class,
            () -> p.evalBoolean(env::get),
            "without resetEvalStepBudget between calls, the per-thread step "
                + "counter must accumulate and trip the second call");
        // Restore the per-test invariant.
        CelProgram.resetEvalStepBudget();
    }

    @Test
    public void evalStepBudgetIsAlsoEnforcedForInOperatorOverGiantList() {
        // Codex deep-audit P1c fix: a single `value in request.giantList` does
        // O(N) equality comparisons on the request thread without the budget.
        // The InList loop must bump the per-iteration counter just like the
        // comprehension loop, so an attacker cannot evade the budget by using
        // `in` instead of `.exists`. Build a list large enough to exhaust the
        // budget on its own — this same expression is fine under the cap when
        // the list is smaller, as covered by `inOperatorForList`.
        java.util.List<Integer> giant = new java.util.ArrayList<>();
        for (int n = 0; n < CelLimits.MAX_EVAL_STEPS + 16; n++) {
            giant.add(n);
        }
        Map<String, Object> env = new HashMap<>();
        env.put("xs", giant);
        env.put("needle", -1); // never matches → loop scans the full list
        assertThrows(
            CelEvaluationException.class,
            () -> evalBool("needle in xs", env));
    }

    @Test
    public void catastrophicBacktrackingPatternIsHandledInLinearTime() {
        // Codex/Gemini final-audit P1#4: the JDK's java.util.regex engine
        // exhibits catastrophic backtracking on patterns like `(a+)+b` when
        // matched against an input of repeated 'a' (no terminal 'b'). On
        // MAX_REGEX_INPUT_LENGTH=16384 the JDK engine would spin for minutes
        // — easily long enough to hang a request thread and turn a single
        // attacker-crafted request into a broker-wide DoS.
        //
        // The engine has been switched to com.google.re2j (Google's RE2 Java
        // port) which is worst-case linear in input length regardless of
        // pattern shape. This test pins that behaviour: a pathological
        // (operator-authored, attacker-targeted) pattern matched against a
        // 16384-char no-match receiver must complete promptly. A generous
        // 5-second deadline catches any regression to a backtracking engine
        // (the JDK engine would take many minutes on this input); a healthy
        // re2j eval is well under 100ms on a modern broker.
        StringBuilder huge = new StringBuilder();
        for (int n = 0; n < 16384; n++) {
            huge.append('a');
        }
        Map<String, Object> env = new HashMap<>();
        env.put("name", huge.toString());
        // Pattern that catastrophically backtracks on `a*` no-`b` input under
        // java.util.regex. Wrapped in System.nanoTime so a regression to a
        // backtracking engine fails loudly with a timing assertion rather
        // than wedging the test runner forever.
        long t0 = System.nanoTime();
        boolean result = evalBool("name.matches(\"(a+)+b\")", env);
        long elapsedNanos = System.nanoTime() - t0;
        assertFalse(result, "no 'b' in input — must not match");
        assertTrue(elapsedNanos < 5_000_000_000L,
            "RE2 must finish in linear time — took " + (elapsedNanos / 1_000_000) +
                "ms (>5s); regression to a backtracking engine?");
    }

    @Test
    public void stringConcatenationResultIsBoundedAtRuntime() {
        // A rule that concatenates an activation-supplied string with itself
        // would, under a doubling-tree shape, accumulate many MB of transient
        // String per evaluation. The CelLimits.MAX_STRING_RESULT_LEN guard
        // aborts the concat as soon as the result would exceed the cap —
        // CelEvaluationException is failed-open by the engine, so the rule
        // is logged and skipped without crashing the request thread.
        StringBuilder big = new StringBuilder();
        for (int n = 0; n < CelLimits.MAX_STRING_RESULT_LEN; n++) {
            big.append('x');
        }
        Map<String, Object> env = new HashMap<>();
        env.put("a", big.toString());
        // a + a doubles the receiver to 2 * MAX_STRING_RESULT_LEN chars.
        CelEvaluationException ex = assertThrows(
            CelEvaluationException.class,
            () -> evalBool("a + a == \"never\"", env));
        assertTrue(ex.getMessage().contains("string concatenation"),
            "expected string-concat error, got: " + ex.getMessage());
    }

    @Test
    public void typeErrorMessageDoesNotLeakActivationValue() {
        // Activation values can carry request-derived data (header values,
        // principal names) that an operator scanning broker logs should not
        // see. CelEvaluationException is logged at WARN by RuleEngine, so
        // the message must include the offending value's type only — not
        // its toString. This pins the "type-only descriptor" posture.
        Map<String, Object> env = new HashMap<>();
        env.put("secret", "very-sensitive-payload");
        // `secret` is a String but `> 5` requires a number → throws.
        CelEvaluationException ex = assertThrows(
            CelEvaluationException.class,
            () -> evalBool("secret > 5", env));
        assertFalse(ex.getMessage().contains("very-sensitive-payload"),
            "exception message must not echo activation value: " + ex.getMessage());
    }

    @Test
    public void matchesStillWorksOnNonStringReceiver() {
        // Behavioural parity with the old MethodCall-based path: a non-string
        // receiver yields false, not an exception. This matters because
        // rules running over heterogeneous Kafka APIs may encounter fields
        // that are absent or numeric on some request shapes.
        Map<String, Object> env = new HashMap<>();
        env.put("name", 42L);
        assertFalse(evalBool("name.matches(\"[a-z]+\")", env));
    }

    @Test
    public void fatContainsInsideComprehensionTripsStepBudget() {
        // Audit HIGH-2: before the fix, MethodCall.eval did not charge the
        // step budget at all — only Comprehension and InList did. That meant
        // a `request.list.exists(x, fatString.contains(otherFatString))`
        // could do MAX_EVAL_STEPS × O(|fatString| · |otherFatString|) char
        // compares per request, since each comprehension iteration cost
        // exactly one step regardless of the body's actual work. The fix
        // charges contains() work as receiver.length() × arg.length() so a
        // single 16384×16384 contains is rejected before we walk into the
        // JDK substring search.
        StringBuilder big = new StringBuilder();
        for (int n = 0; n < CelLimits.MAX_STRING_RESULT_LEN; n++) {
            big.append('x');
        }
        // A single contains() with both sides at the result-length cap
        // estimates ≈ 268M char compares — well over the per-request budget.
        Map<String, Object> env = new HashMap<>();
        env.put("a", big.toString());
        env.put("b", big.toString());
        // Bare contains, outside any comprehension: still tripped by the
        // proportional bump because the work estimate alone is over-budget.
        assertThrows(CelEvaluationException.class,
            () -> evalBool("a.contains(b)", env));
    }

    @Test
    public void stringStartsWithChargesArgLengthAgainstBudget() {
        // Argument-length cost bound: startsWith does at most arg.length()
        // char compares (linear in needle). The fix charges this so a
        // comprehension over many items × needle of length L cannot exceed
        // ~MAX_EVAL_STEPS character compares total.
        //
        // Construct a comprehension whose iteration count alone is well
        // under the budget (1000 iters × per-iter bump = 1000 steps), but
        // whose body does startsWith with a needle long enough to push the
        // total above the cap.
        java.util.List<String> items = new java.util.ArrayList<>();
        for (int n = 0; n < 1000; n++) {
            items.add("hay-" + n);
        }
        Map<String, Object> env = new HashMap<>();
        env.put("xs", items);
        // Needle of length 1000 → per-iter bump ≈ 1000 (startsWith). Plus
        // 1 (comprehension) = ~1001 per iter × 1000 iters = ~1M bumps,
        // over the 100k budget.
        StringBuilder needle = new StringBuilder();
        for (int n = 0; n < 1000; n++) {
            needle.append('a');
        }
        String expr = "xs.exists(x, x.startsWith(\"" + needle + "\"))";
        assertThrows(CelEvaluationException.class,
            () -> evalBool(expr, env));
    }

    @Test
    public void regexMatchChargesReceiverLengthAgainstBudget() {
        // Audit HIGH-2: the RegexMatch node now charges receiver.length()
        // to the step budget (RE2 is worst-case linear in input length). A
        // comprehension over many items × matches() on a long receiver must
        // trip the budget on total work, not only on iteration count.
        StringBuilder big = new StringBuilder();
        // 200 chars is well below MAX_REGEX_INPUT_LENGTH (so the receiver-
        // length guard does NOT fire), but × 1000 iterations = 200k bumps,
        // over the 100k budget.
        for (int n = 0; n < 200; n++) {
            big.append('a');
        }
        java.util.List<String> items = new java.util.ArrayList<>();
        for (int n = 0; n < 1000; n++) {
            items.add(big.toString());
        }
        Map<String, Object> env = new HashMap<>();
        env.put("xs", items);
        // Pattern that never matches (no 'b' in input) so all 1000
        // iterations run to completion under RE2 — without the per-call
        // bump, this would consume only 1000 comprehension steps and pass.
        assertThrows(CelEvaluationException.class,
            () -> evalBool("xs.exists(x, x.matches(\"a*b\"))", env));
    }

    @Test
    public void stringEqualsInsideInListChargesPrefixLengthAgainstBudget() {
        // Audit HIGH-2: InList's per-iteration step bump alone is not enough
        // — for a list of long strings against a long needle, valueEquals
        // walks shared prefix chars per element. The fix adds proportional
        // bumping inside the loop for string == string so an attacker cannot
        // amortise O(min(|v|,|item|)) char compares per element at one step
        // apiece.
        StringBuilder s = new StringBuilder();
        // 300 chars × 1000 items = 300k bumps, over 100k budget.
        for (int n = 0; n < 300; n++) {
            s.append('a');
        }
        java.util.List<String> items = new java.util.ArrayList<>();
        for (int n = 0; n < 1000; n++) {
            items.add(s.toString());
        }
        Map<String, Object> env = new HashMap<>();
        env.put("xs", items);
        // Needle differs from every item only at the very last char, so
        // valueEquals walks the full prefix on every comparison.
        StringBuilder needle = new StringBuilder(s).append('Z');
        env.put("needle", needle.toString());
        assertThrows(CelEvaluationException.class,
            () -> evalBool("needle in xs", env));
    }

    @Test
    public void listEqualityInsideInListChargesElementCostAgainstBudget() {
        // R27-B (Task #182): InList previously charged only 1 step per
        // iteration plus a string-prefix bump — but it did NOT charge for
        // list-vs-list element walks the way Compare.eval does. So
        // `needle in [bigListCopy1, bigListCopy2, ...]` could amortise
        // O(N) AbstractList.equals work per outer step.
        //
        // Pin the fix: 200-element needle × 600 candidate lists =
        // 600 × (1 baseline + 200 element-bumps) = 120_600 > 100_000.
        //
        // Every candidate is a 200-element list that matches the needle
        // on its first 199 elements and differs only at the last index, so
        // AbstractList.equals walks the full prefix before returning false.
        // `in` does NOT short-circuit because each comparison is false; the
        // loop runs the full 600 candidates and trips the budget.
        java.util.List<Object> needle = new java.util.ArrayList<>();
        for (int n = 0; n < 200; n++) {
            needle.add("e-" + n);
        }
        java.util.List<Object> xs = new java.util.ArrayList<>();
        for (int k = 0; k < 600; k++) {
            java.util.List<Object> copy = new java.util.ArrayList<>(needle);
            copy.set(copy.size() - 1, "DIFFERS-" + k);
            xs.add(copy);
        }
        Map<String, Object> env = new HashMap<>();
        env.put("xs", xs);
        env.put("needle", needle);
        assertThrows(CelEvaluationException.class,
            () -> evalBool("needle in xs", env));
    }

    @Test
    public void mapEqualityInsideInListChargesEntryCostAgainstBudget() {
        // R27-B (Task #182), map-side. AbstractMap.equals walks every
        // entry via Objects.equals; before this charge a needle Map
        // compared against a list of equal-shaped Maps amortised O(N)
        // entry compares per outer step. 200-entry needle × 600 candidate
        // maps = 600 × (1 + 200) = 120_600 > 100_000.
        //
        // Each candidate matches needle on the first 199 entries and
        // differs at one — the walk runs to that mismatch and returns
        // false, so `in` keeps iterating and the full 600 candidates run.
        java.util.LinkedHashMap<String, Object> needle = new java.util.LinkedHashMap<>();
        for (int n = 0; n < 200; n++) {
            needle.put("k-" + n, "v-" + n);
        }
        java.util.List<Object> xs = new java.util.ArrayList<>();
        for (int k = 0; k < 600; k++) {
            java.util.LinkedHashMap<String, Object> copy =
                new java.util.LinkedHashMap<>(needle);
            copy.put("k-199", "DIFFERS-" + k);
            xs.add(copy);
        }
        Map<String, Object> env = new HashMap<>();
        env.put("xs", xs);
        env.put("needle", needle);
        assertThrows(CelEvaluationException.class,
            () -> evalBool("needle in xs", env));
    }

    @Test
    public void unknownMethodIsRejectedAtCompileTime() {
        // Audit LOW-1: a typo like `name.startWith("audit")` (missing 's')
        // must fail at rule load — CelCompilationException — not silently
        // install and fail-open at request time. The whole point of
        // compile-time validation is that the rule submitter gets an
        // immediate, precise diagnostic instead of "this rule mysteriously
        // does nothing in production".
        CelCompilationException ex = assertThrows(CelCompilationException.class,
            () -> CelCompiler.compile("name.startWith(\"audit\")"));
        assertTrue(ex.getMessage().contains("startWith"),
            "compile error must name the offending method: " + ex.getMessage());
        // A genuinely-unknown method name surfaces the supported set so
        // the operator does not have to consult source code to fix the
        // rule.
        CelCompilationException ex2 = assertThrows(CelCompilationException.class,
            () -> CelCompiler.compile("name.totallyBogus(\"x\")"));
        assertTrue(ex2.getMessage().contains("startsWith"),
            "compile error must enumerate supported methods: " + ex2.getMessage());
    }

    @Test
    public void wrongArityForStringMethodIsRejectedAtCompileTime() {
        // Same posture as unknownMethodIsRejectedAtCompileTime: arity
        // mismatches are programming errors, not request-shape surprises.
        // They should be caught before any request walks an obviously-wrong
        // rule, not deferred to per-request fail-open behaviour.
        assertThrows(CelCompilationException.class,
            () -> CelCompiler.compile("name.startsWith()"));
        assertThrows(CelCompilationException.class,
            () -> CelCompiler.compile("name.contains(\"a\", \"b\")"));
        assertThrows(CelCompilationException.class,
            () -> CelCompiler.compile("name.endsWith(\"a\", \"b\", \"c\")"));
    }

    @Test
    public void supportedStringMethodsStillCompileCleanly() {
        // Regression test for unknownMethodIsRejectedAtCompileTime: a
        // tighter compile-time check could over-rotate and reject
        // legitimate rules. Pin that the four well-known methods continue
        // to parse without throwing.
        CelCompiler.compile("name.startsWith(\"audit\")");
        CelCompiler.compile("name.endsWith(\"-topic\")");
        CelCompiler.compile("name.contains(\"foo\")");
        CelCompiler.compile("name.matches(\"[a-z]+\")");
    }

    @Test
    public void sizeOfNullThrowsRatherThanReturningZero() {
        // Round-10 audit: returning 0 for null is a rule-author trap.
        // `size(request.missing) > 100` silently false; `size(...) == 0`
        // conflates empty and missing. Throw so the rule fails-open
        // (logged at WARN by RuleEngine) and the author sees the bug.
        Map<String, Object> env = new HashMap<>();
        Map<String, Object> req = new LinkedHashMap<>();
        env.put("request", req);
        CelEvaluationException ex = assertThrows(
            CelEvaluationException.class,
            () -> evalBool("size(request.missing) > 100", env));
        assertTrue(ex.getMessage().toLowerCase().contains("null"),
            "expected 'null' in: " + ex.getMessage());
    }

    @Test
    public void listMapEqualityChargesElementCostAgainstBudget() {
        // Round-10 audit: AbstractList/AbstractMap.equals walks every
        // element via Objects.equals. Before the fix, only string ==
        // string was charged proportionally; List == List and Map == Map
        // could amortise O(N) element compares per CEL step inside a
        // comprehension. A list of ~200 strings × 600 iterations =
        // 120k element-bumps, over the 100k budget.
        java.util.List<Object> needle = new java.util.ArrayList<>();
        for (int n = 0; n < 200; n++) {
            needle.add("needle-" + n);
        }
        // Every candidate is a fresh 200-element copy of the needle. So
        // `x == needle` is TRUE for every iteration: `all()` does not
        // short-circuit on TRUE — it walks every element. Each iteration
        // pays for 200 element compares plus the bumpSteps(200) charge
        // on Compare.eval. 600 iters × 200 = 120k step bumps, over the
        // 100k budget. Without the per-element bump, only 600 iteration
        // steps are charged and the predicate evaluates to true.
        java.util.List<Object> iters = new java.util.ArrayList<>();
        for (int k = 0; k < 600; k++) {
            iters.add(new java.util.ArrayList<>(needle));
        }
        Map<String, Object> env = new HashMap<>();
        env.put("xs", iters);
        env.put("needle", needle);
        assertThrows(CelEvaluationException.class,
            () -> evalBool("xs.all(x, x == needle)", env));
    }

    @Test
    public void nestedListEqualityRecursivelyChargesAgainstBudget() {
        // R28 Axis Walker F8 (Task #244): valueEquals used to delegate to
        // Objects.equals for Lists/Maps, which walks AbstractList/AbstractMap
        // .equals recursively WITHOUT charging the per-element cost at each
        // layer. The outer Compare/InList pre-charges only accounted for the
        // outermost size — a walker-produced shape like CreateTopics with N
        // topic descriptors × M configs each charged ~N steps but did N*M
        // leaf compares.
        //
        // With listEqualsDeep/mapEqualsDeep in valueEquals, every recursion
        // layer charges bumpSteps(size) before walking. For an outer list
        // of 100 inner lists of 100 strings each, == itself charges
        // 100 outer + 100*100 inner = 10_100 steps per Compare. 10 iters of
        // such a Compare inside all() = 101_000 > MAX_EVAL_STEPS(=100_000).
        //
        // Negative control: without inner-layer accounting (the pre-fix
        // shape), per-iter would charge ~101 (1 baseline + outer pre-charge);
        // 10 iters = ~1_010 steps, well under 100k — the rule would
        // evaluate to true and the test would FAIL on the missing throw.
        // The diff makes the failure mode observable: budget trips at
        // ~10k recursion-charged steps in the first iter pair.
        java.util.List<Object> needle = new java.util.ArrayList<>();
        for (int i = 0; i < 100; i++) {
            java.util.List<Object> inner = new java.util.ArrayList<>();
            for (int j = 0; j < 100; j++) {
                inner.add("v-" + i + "-" + j);
            }
            needle.add(inner);
        }
        java.util.List<Object> iters = new java.util.ArrayList<>();
        for (int k = 0; k < 10; k++) {
            // Deep copy so == walks every element in every iteration (no
            // reference-equality short-circuit inside AbstractList.equals
            // would have saved the pre-fix shape either, since every
            // element is a freshly-allocated string with the same content
            // but a different identity — Objects.equals goes to .equals).
            java.util.List<Object> copy = new java.util.ArrayList<>();
            for (Object inner : needle) {
                copy.add(new java.util.ArrayList<>((java.util.List<?>) inner));
            }
            iters.add(copy);
        }
        Map<String, Object> env = new HashMap<>();
        env.put("xs", iters);
        env.put("needle", needle);
        assertThrows(CelEvaluationException.class,
            () -> evalBool("xs.all(x, x == needle)", env));
    }

    @Test
    public void nestedMapEqualityRecursivelyChargesAgainstBudget() {
        // R28 Axis Walker F8 (Task #244): mirror for nested Map-of-Map. Same
        // amplification shape — pre-fix outer pre-charge accounted only for
        // entry count of the top map; AbstractMap.equals walked every nested
        // value uncharged. With mapEqualsDeep in valueEquals, every recursion
        // layer charges bumpSteps(size).
        //
        // For 50 outer × 50 inner entries: == itself charges 50 + 50*50 =
        // 2_550 steps. 41 iters inside all() = 104_550 > 100k. Without inner
        // accounting (negative control): 51 per iter × 41 = ~2_091 — far
        // under budget.
        java.util.Map<String, Object> needle = new java.util.LinkedHashMap<>();
        for (int i = 0; i < 50; i++) {
            java.util.Map<String, Object> inner = new java.util.LinkedHashMap<>();
            for (int j = 0; j < 50; j++) {
                inner.put("k-" + j, "v-" + i + "-" + j);
            }
            needle.put("k-" + i, inner);
        }
        java.util.List<Object> iters = new java.util.ArrayList<>();
        for (int k = 0; k < 41; k++) {
            java.util.Map<String, Object> copy = new java.util.LinkedHashMap<>();
            for (Map.Entry<String, Object> e : needle.entrySet()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> innerCopy =
                    new java.util.LinkedHashMap<>((Map<String, Object>) e.getValue());
                copy.put(e.getKey(), innerCopy);
            }
            iters.add(copy);
        }
        Map<String, Object> env = new HashMap<>();
        env.put("xs", iters);
        env.put("needle", needle);
        assertThrows(CelEvaluationException.class,
            () -> evalBool("xs.all(x, x == needle)", env));
    }

    @Test
    public void listLiteralConstructionChargesPerElementAgainstBudget() {
        // Round-10 audit: ListLiteral.eval charged zero per element. A
        // literal allocated inside a comprehension paid for its
        // construction every iteration but only at one step apiece.
        // 200-element literal × 600 iters = 120k bumps with the fix,
        // over the 100k budget.
        java.util.List<Object> iters = new java.util.ArrayList<>();
        // Iterate over values that do NOT appear in the literal, so the
        // comprehension does not short-circuit and pays for the literal
        // construction every iteration.
        for (int n = 0; n < 600; n++) {
            iters.add(Long.valueOf(n + 10_000));
        }
        Map<String, Object> env = new HashMap<>();
        env.put("xs", iters);
        // Build a 200-element list literal in source — short enough to
        // stay under MAX_NODES (=1024) but long enough that 600 iters
        // exceed MAX_EVAL_STEPS.
        StringBuilder lit = new StringBuilder("[");
        for (int n = 0; n < 200; n++) {
            if (n > 0) {
                lit.append(',');
            }
            lit.append(n);
        }
        lit.append(']');
        // Use all() so the entire iteration sequence runs (no
        // early-exit on a match). For a candidate not in the literal,
        // x in [literal] is false, so all(...) returns false eventually
        // — but only after every per-iter literal allocation has been
        // charged.
        String expr = "xs.all(x, !(x in " + lit + "))";
        assertThrows(CelEvaluationException.class,
            () -> evalBool(expr, env));
    }

    @Test
    public void booleanOperatorChainInsideComprehensionTripsBudget() {
        // R26-B F2 (Task #143): without per-node step charges on And/Or/Not/
        // Negate/Compare/Arith, a comprehension whose predicate is a chain
        // of logical operators amortises K free node-evals across one
        // iteration step. MAX_NODES=1024 bounds K per rule, but the per-
        // request budget (MAX_EVAL_STEPS=100_000) is shared across
        // MAX_RULES_PER_API_KEY=128 rules — so without this charge, a
        // single attacker request could burn ~131k free node-evals through
        // dense boolean trees and never trip the budget. This test pins
        // the fix: K logical operators × N iterations now charges N*K
        // steps, and at N*K > MAX_EVAL_STEPS the budget trips.
        //
        // Shape: 30 conjuncts of `x == x` inside an `all(...)` over a
        // 2000-element list of Long. Per iteration the predicate evaluates
        // 30 Compare nodes + 29 And nodes = 59 charged bumps (post-fix).
        // 2000 × 59 = 118_000 > 100_000 → trips. Pre-fix, both Compare
        // (numeric path) and And charged ZERO, so only 2000 comprehension
        // iter steps were charged — well under the budget and the
        // predicate would have returned true.
        //
        // x == x is deliberate: always-true so all(...) runs the full
        // iteration sequence (no short-circuit on a false element). Long
        // operands keep Compare on the numeric path so the only charge
        // that fires is the new baseline bumpStep — the pre-fix code path
        // does not charge for Long-vs-Long compare.
        java.util.List<Object> iters = new java.util.ArrayList<>();
        for (int n = 0; n < 2000; n++) {
            iters.add(Long.valueOf(n));
        }
        Map<String, Object> env = new HashMap<>();
        env.put("xs", iters);
        // Build 30-conjunct chain. Left-leaning parse → tree depth ~30,
        // safely under MAX_PARSE_DEPTH=64. Node count ~60 per predicate +
        // comprehension overhead, safely under MAX_NODES=1024.
        StringBuilder pred = new StringBuilder("x == x");
        for (int k = 1; k < 30; k++) {
            pred.append(" && x == x");
        }
        String expr = "xs.all(x, " + pred + ")";
        assertThrows(CelEvaluationException.class,
            () -> evalBool(expr, env),
            "30-conjunct chain × 2000 iters must trip MAX_EVAL_STEPS after "
                + "R26-B F2 per-node charges; without the fix the budget "
                + "would not trip because numeric Compare and And were free");
    }

    @Test
    public void integerOverflowSurfacesAsCelException() {
        // Round-10 audit: silent overflow flipped predicate truth values.
        // Math.addExact / multiplyExact / negateExact surface overflow
        // as ArithmeticException → CelEvaluationException → fail-open
        // at the RuleEngine boundary, instead of silently mismatching.
        Map<String, Object> env = new HashMap<>();
        env.put("x", Long.MAX_VALUE);
        assertThrows(CelEvaluationException.class,
            () -> evalBool("x + 1 > 0", env));
        // Negate of MIN_VALUE: parse '-9223372036854775808' as
        // Negate(Literal(9223372036854775808L)); but the literal itself
        // overflows at parse time. Use an evaluation-time path instead:
        Map<String, Object> env2 = new HashMap<>();
        env2.put("y", Long.MIN_VALUE);
        assertThrows(CelEvaluationException.class,
            () -> evalBool("-y > 0", env2));
        // DIV-overflow: MIN_VALUE / -1.
        Map<String, Object> env3 = new HashMap<>();
        env3.put("z", Long.MIN_VALUE);
        env3.put("w", -1L);
        assertThrows(CelEvaluationException.class,
            () -> evalBool("z / w > 0", env3));
    }

    @Test
    public void stringConcatChargesResultLengthAgainstBudget() {
        // Audit HIGH-2: Arith.ADD on strings now charges (l.length() +
        // r.length()) per call. A comprehension that builds long strings
        // inside its body must trip the budget on total chars copied, not
        // only on iteration count.
        // Each iter concatenates two 200-char strings → 400 bumps × 300
        // iters = 120k bumps, over 100k budget.
        StringBuilder s = new StringBuilder();
        for (int n = 0; n < 200; n++) {
            s.append('a');
        }
        java.util.List<String> items = new java.util.ArrayList<>();
        for (int n = 0; n < 300; n++) {
            items.add(s.toString());
        }
        Map<String, Object> env = new HashMap<>();
        env.put("xs", items);
        env.put("suffix", s.toString());
        // The body of exists computes (x + suffix == \"never\"), which is
        // string concat (charged) followed by string compare (also charged).
        assertThrows(CelEvaluationException.class,
            () -> evalBool("xs.exists(x, x + suffix == \"never\")", env));
    }
}
