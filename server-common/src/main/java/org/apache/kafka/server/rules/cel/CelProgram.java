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

import java.util.function.Function;

/**
 * A compiled CEL-subset expression that can be evaluated against an
 * {@link #evalBoolean(Function) activation}. Compile once at rule load,
 * evaluate many times per request.
 *
 * <p>The supported subset is deliberately small to keep the broker fast
 * path lean: identifiers with dot/index access, comparison, arithmetic,
 * logical operators with short-circuit, string methods ({@code startsWith},
 * {@code endsWith}, {@code contains}, {@code matches}), the {@code in}
 * operator, the {@code size} builtin, and the {@code .exists}/{@code .all}
 * comprehension macros.
 */
public final class CelProgram {

    private final CelNode root;
    private final String source;

    CelProgram(CelNode root, String source) {
        this.root = root;
        this.source = source;
    }

    /**
     * Evaluate as a boolean. Null and missing identifiers are coerced to false
     * so that rule predicates cannot crash the request path on heterogeneous
     * Kafka APIs. Non-boolean, non-null values throw.
     *
     * <p>Round-8 audit HIGH (concurrency): the per-thread step counter (see
     * {@link CelLimits#bumpStep}) is NOT reset here. The budget is
     * <em>per-request</em>, not per-rule — {@link
     * org.apache.kafka.server.rules.RuleEngine#evaluate} resets the counter
     * once before iterating the rules for an api-key and once on the way out.
     * If the rule's evaluation crosses {@link CelLimits#MAX_EVAL_STEPS}, the
     * thrown {@link CelEvaluationException} bubbles to the engine which
     * treats it as fail-open and continues to the next rule; subsequent
     * rules in the same request will retrip on entry under the same shared
     * budget and also fail-open. That is the intended DoS-defence behaviour:
     * a request can never legitimately multiply the step budget by the
     * number of rules an operator happens to have published.
     *
     * <p>Direct callers outside the engine (unit tests, future tooling) MUST
     * call {@link CelLimits#resetSteps()} themselves before each evaluation
     * if they want a fresh budget — see the package-level rules engine test
     * for the canonical pattern.
     */
    public boolean evalBoolean(Function<String, Object> activation) {
        Object v = root.eval(activation);
        if (v == null) return false;
        if (v instanceof Boolean) return (Boolean) v;
        // Type-only descriptor for the offending value: the exception is
        // logged at WARN by RuleEngine.evaluate, and activation values can
        // carry request-derived data (header values, principal names,
        // topic names) we should not echo to broker logs. The rule
        // source is operator-authored and safe to include — it lets the
        // operator find the misauthored rule without reading the payload.
        throw new CelEvaluationException(
            "CEL expression did not evaluate to a boolean: source=" + source
                + " resultType=" + v.getClass().getSimpleName());
    }

    public String source() {
        return source;
    }

    /**
     * Reset the per-thread CEL step counter to zero. Called by
     * {@link org.apache.kafka.server.rules.RuleEngine#evaluate} once before
     * iterating an api-key's rule list and once on the way out (in a
     * {@code finally}) so the shared budget covers exactly one request and
     * never leaks across requests on the same broker thread. Direct callers
     * of {@link #evalBoolean(Function)} outside the engine (unit tests,
     * future tooling) call this themselves between evaluations when they
     * need a fresh budget.
     *
     * <p>This static delegate exists so the package-private {@link CelLimits}
     * machinery does not have to be exposed across the rules-engine package
     * boundary; {@link CelProgram} is the public face of the CEL subset, so
     * the budget-management entry point belongs here.
     */
    public static void resetEvalStepBudget() {
        CelLimits.resetSteps();
    }

    @Override
    public String toString() {
        return "CelProgram(" + source + ")";
    }
}
