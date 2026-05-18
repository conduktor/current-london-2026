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

import java.util.List;
import java.util.Objects;

/**
 * Compiles a predicate source string into a {@link CompiledPredicate}.
 *
 * The pipeline is deliberately rigid: lex → parse → check top-level shape. Any failure throws
 * {@link PredicateValidationException} so that view-creation paths can surface the user-visible
 * error and reject the request before any state is mutated.
 *
 * The compiler is stateless and thread-safe — feel free to share a single instance per broker.
 */
public final class PredicateCompiler {
    private final PredicateLimits limits;

    public PredicateCompiler(PredicateLimits limits) {
        this.limits = Objects.requireNonNull(limits, "limits");
    }

    /**
     * Compile and validate. Throws {@link PredicateValidationException} on syntax errors,
     * banned constructs, AST limit overflows, or top-level shape mismatch.
     */
    public CompiledPredicate compile(String source) {
        if (source == null) {
            throw new PredicateValidationException("predicate source must not be null");
        }
        String trimmed = source.trim();
        if (trimmed.isEmpty()) {
            throw new PredicateValidationException("predicate source must not be empty");
        }
        List<Lexer.Token> tokens = new Lexer(source, limits).tokenize();
        Ast.Node root = new Parser(tokens, limits).parse();
        validateTopLevelShape(root);
        return new CompiledPredicate(root, limits, source);
    }

    /**
     * The top-level expression must evaluate to a boolean. Literals/numeric/string roots are
     * structurally non-boolean and rejected here; identifier paths are accepted because their
     * runtime type is JSON-driven and can legitimately be boolean.
     */
    private static void validateTopLevelShape(Ast.Node n) {
        if (n instanceof Ast.Literal) {
            if (((Ast.Literal) n).value instanceof Boolean) return;
            throw new PredicateValidationException(
                    "top-level predicate must be boolean; literal of non-boolean type is not allowed");
        }
        if (n instanceof Ast.Path) {
            // Identifier paths may resolve to a boolean at runtime — accept.
            return;
        }
        if (n.shape() == Ast.Shape.BOOL) return;
        throw new PredicateValidationException(
                "top-level predicate must be boolean (use comparison or logical operators)");
    }
}
