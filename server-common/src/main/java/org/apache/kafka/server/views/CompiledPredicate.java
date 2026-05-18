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

import java.util.Optional;

/**
 * A predicate whose AST and limits have passed compile-time validation. Evaluation is
 * thread-safe: the AST is immutable and {@link Evaluator} state is stack-local per call.
 *
 * Evaluation returns:
 *  - {@code Optional.of(true)} — record matches, retain it.
 *  - {@code Optional.of(false)} — record does not match, drop it.
 *  - {@code Optional.empty()} — record could not be evaluated cleanly (cost cap hit, non-bool
 *    result, malformed body, etc.); the filter should treat this as drop, but the distinction
 *    is preserved for metrics/debugging.
 */
public final class CompiledPredicate {
    private final Ast.Node root;
    private final PredicateLimits limits;
    private final String source;

    CompiledPredicate(Ast.Node root, PredicateLimits limits, String source) {
        this.root = root;
        this.limits = limits;
        this.source = source;
    }

    public Optional<Boolean> evaluate(RecordContext ctx) {
        Object v = new Evaluator(limits).evaluate(root, ctx);
        if (v == Evaluator.SKIP) return Optional.empty();
        if (v instanceof Boolean) return Optional.of((Boolean) v);
        return Optional.empty();
    }

    /** Original source text — useful for logging and equality. */
    public String predicateText() {
        return source;
    }
}
