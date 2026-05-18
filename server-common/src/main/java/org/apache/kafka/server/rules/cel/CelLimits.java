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

/**
 * Centralised safety limits for the broker-side CEL compiler and interpreter.
 *
 * <p>These bounds exist to defend the broker request thread against three
 * adversarial inputs:
 *
 * <ul>
 *   <li><b>Parser stack-overflow.</b> Hand-rolled recursive descent on
 *       attacker-shaped sources like {@code "!!!!!!!!..."} or
 *       {@code "((((((..."} can blow the JVM stack at parse time. We cap
 *       parse-call depth at {@link #MAX_PARSE_DEPTH}.</li>
 *   <li><b>AST memory blow-up.</b> A long but well-formed CEL source can still
 *       allocate millions of nodes (deeply nested list/map literals,
 *       repeated comprehensions). We cap node count at {@link #MAX_NODES} —
 *       generous for any legitimate operator rule, fatal for fuzz input.</li>
 *   <li><b>Eval-time exponential blow-up.</b> Nested comprehensions are
 *       O(N^depth) in the size of the iterated lists, which the activation
 *       supplier can make adversarial (a request with a giant repeated field).
 *       The {@link #MAX_EVAL_STEPS} budget is checked on every comprehension
 *       iteration; exceeding it raises {@link CelEvaluationException}, which
 *       the engine treats as fail-open (one buggy rule does not crash the
 *       request thread — see {@code RuleEngine.evaluate}).</li>
 * </ul>
 *
 * <p>Separately, {@link #MAX_REGEX_INPUT_LENGTH} caps the string fed to
 * {@code Pattern.matcher().matches()}. A pre-compiled regex with bounded
 * input length cannot exhibit catastrophic backtracking that escapes the
 * request thread — the matcher's worst case is bounded by the input length
 * times the pattern's NFA size, both finite.
 *
 * <p>None of these limits are tunable at runtime. Operators who want to
 * relax them are signalling that they are about to make their broker
 * vulnerable to denial of service; that decision belongs in a code review,
 * not in a config file. Tighten them by editing this class.
 */
final class CelLimits {

    /**
     * Maximum source length (in {@code char} units) of a CEL rule expression.
     * Checked before lexing in {@link CelCompiler#compile(String)} so a giant
     * source — typically a multi-megabyte string or regex literal — never
     * even reaches the tokenizer's allocation path. Without this cap the
     * lexer happily walks the entire {@code char[]} producing one
     * {@link CelCompiler.Token} per character span; a single rule could
     * pin tens of megabytes of token objects before the {@link #MAX_NODES}
     * cap kicks in at parse time. Codex final-audit P1#5.
     *
     * <p>A typical production rule is &lt; 200 characters
     * ({@code request.topics.exists(t, t.name.startsWith("audit-"))} is 49).
     * 8192 chars is two orders of magnitude over typical and well above any
     * defensible legitimate use; rules approaching this cap are almost
     * certainly trying to bypass the engine's other bounds via a giant
     * string/regex literal and should be rewritten as a coarser filter.
     */
    static final int MAX_EXPR_LEN = 8192;

    /**
     * Maximum recursive-descent call depth. Each entry into
     * {@code parseExpr}, {@code parseNot}, or {@code parseUnary} consumes one
     * unit. 64 covers every legitimate rule (nesting is bounded by
     * parentheses, list/map nesting, and comprehension nesting — all rare
     * past depth 8) while killing fuzz inputs of the form {@code !!!!...x}
     * or {@code ((((((...x))))))} long before they overflow the JVM stack.
     */
    static final int MAX_PARSE_DEPTH = 64;

    /**
     * Maximum number of AST nodes a single compiled program may contain.
     * Inspected at parse time after each node allocation. A typical
     * production rule (e.g. {@code request.topics.exists(t, t.name.startsWith("audit"))})
     * is &lt; 20 nodes. 1024 leaves three orders of magnitude of headroom for
     * complex policies while bounding total AST memory.
     */
    static final int MAX_NODES = 1024;

    /**
     * Maximum number of comprehension iterations a single
     * {@link CelProgram#evalBoolean} call may consume across the whole AST.
     * One iteration of {@code list.exists}/{@code list.all} counts as one
     * step. The counter is reset at the start of every {@code evalBoolean}
     * call, so successive requests are independent.
     *
     * <p>100k steps is roughly 1ms of CPU on a modern broker — well below
     * any reasonable per-request budget. A rule that genuinely needs more
     * iterations than this is doing the wrong thing on the request hot path
     * and should be rewritten as a coarser filter.
     */
    static final int MAX_EVAL_STEPS = 100_000;

    /**
     * Maximum length of the receiver string passed to {@code Pattern.matches}.
     * Kafka identifiers are short by spec — topic names cap at 249 chars,
     * client/group IDs are typically &lt; 256. 16KB is three orders of
     * magnitude over that, but bounds the worst case of catastrophic
     * backtracking even when a pattern with a degenerate NFA slips past
     * review. A receiver longer than this raises {@link CelEvaluationException},
     * which the engine fails open on per its policy.
     */
    static final int MAX_REGEX_INPUT_LENGTH = 16384;

    /**
     * Per-thread step counter for the runtime eval-step budget. We use a
     * {@code ThreadLocal int[]} (not a {@code ThreadLocal&lt;Integer&gt;})
     * to allow in-place mutation without re-boxing on every bump — the
     * counter is incremented on the hot path of every comprehension
     * iteration.
     */
    private static final ThreadLocal<int[]> STEPS = ThreadLocal.withInitial(() -> new int[1]);

    private CelLimits() {
    }

    /**
     * Reset the per-thread step counter. Called by
     * {@link CelProgram#evalBoolean} at the start and end of every
     * evaluation so successive requests don't share state, even if one
     * threw mid-evaluation.
     */
    static void resetSteps() {
        STEPS.get()[0] = 0;
    }

    /**
     * Bump the per-thread step counter. Throws
     * {@link CelEvaluationException} when the cumulative step count for the
     * current {@code evalBoolean} call exceeds {@link #MAX_EVAL_STEPS}. The
     * engine treats this as fail-open (the rule is logged and skipped); we
     * never let an over-budget rule decide the outcome of a request.
     */
    static void bumpStep() {
        int n = ++STEPS.get()[0];
        if (n > MAX_EVAL_STEPS) {
            throw new CelEvaluationException(
                "CEL evaluation exceeded step budget of " + MAX_EVAL_STEPS);
        }
    }
}
