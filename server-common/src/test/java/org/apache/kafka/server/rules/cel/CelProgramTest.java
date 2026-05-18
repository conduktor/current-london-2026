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
    public void evalStepCounterIsResetBetweenInvocations() {
        // The counter is a ThreadLocal; without explicit reset it would
        // accumulate across requests on the same broker thread and trip
        // arbitrarily early for the second request. The reset must happen
        // both before and after evaluation so a throwing evaluation does
        // not poison the next one.
        java.util.List<Integer> items = new java.util.ArrayList<>();
        for (int n = 0; n < 1000; n++) {
            items.add(n);
        }
        Map<String, Object> env = new HashMap<>();
        env.put("xs", items);
        // 1000 iterations per call; well under the budget. Run it many
        // times — should never trip.
        CelProgram p = CelCompiler.compile("xs.exists(a, a == -1)");
        for (int n = 0; n < 200; n++) {
            assertFalse(p.evalBoolean(env::get));
        }
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
