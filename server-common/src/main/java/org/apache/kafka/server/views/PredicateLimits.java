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

/**
 * Static bounds applied to view predicates. Compile-time limits cap parser work
 * and AST size; runtime limits cap per-record evaluation cost.
 *
 * Every limit guards against a real failure mode:
 *  - maxSourceLength: oversized predicate text — fail fast before lexing/parsing. Also closes the
 *    "deep parenthesisation → parser stack overflow" attack: parens don't add AST nodes, so a
 *    predicate like `(((((...)))))` would slip past maxNodes/maxDepth (the depth check runs
 *    POST-parse) and blow the JVM stack inside the recursive-descent parser. Capping source
 *    length is the cheapest hard ceiling on input size; {@link #maxParenDepth} catches the
 *    pathological shape directly during parsing.
 *  - maxParenDepth: explicit cap on parenthesis nesting during parse. Hard guarantee against
 *    parser recursion overflow on adversarial input, independent of source length.
 *  - maxNodes / maxDepth: malicious or buggy predicates that would balloon parser/evaluator memory or recursion.
 *  - maxStringLiteralLength: oversized literals embedded in the predicate text.
 *  - maxStepsPerEval: catastrophic per-record cost on a hot path.
 *  - maxBodyBytes: oversized record values feeding the JSON parser.
 *  - maxJsonDepth: deeply nested JSON exhausting the parser stack.
 *  - maxScalarStringChars: an individual JSON string scalar that exceeds this length is treated
 *    as malformed (record-skip) even when the surrounding body fits inside maxBodyBytes. The spec
 *    (PROMPT.md scenario list) calls out "oversized JSON strings" as a record-skip case, so a
 *    multi-MB single-field value cannot slip through merely because the rest of the body is small.
 */
public final class PredicateLimits {
    public final int maxSourceLength;
    public final int maxParenDepth;
    public final int maxNodes;
    public final int maxDepth;
    public final int maxStringLiteralLength;
    public final int maxStepsPerEval;
    public final int maxBodyBytes;
    public final int maxJsonDepth;
    public final int maxScalarStringChars;

    public PredicateLimits(int maxSourceLength, int maxParenDepth,
                           int maxNodes, int maxDepth, int maxStringLiteralLength,
                           int maxStepsPerEval, int maxBodyBytes, int maxJsonDepth,
                           int maxScalarStringChars) {
        requirePositive("maxSourceLength", maxSourceLength);
        requirePositive("maxParenDepth", maxParenDepth);
        requirePositive("maxNodes", maxNodes);
        requirePositive("maxDepth", maxDepth);
        requirePositive("maxStringLiteralLength", maxStringLiteralLength);
        requirePositive("maxStepsPerEval", maxStepsPerEval);
        requirePositive("maxBodyBytes", maxBodyBytes);
        requirePositive("maxJsonDepth", maxJsonDepth);
        requirePositive("maxScalarStringChars", maxScalarStringChars);
        this.maxSourceLength = maxSourceLength;
        this.maxParenDepth = maxParenDepth;
        this.maxNodes = maxNodes;
        this.maxDepth = maxDepth;
        this.maxStringLiteralLength = maxStringLiteralLength;
        this.maxStepsPerEval = maxStepsPerEval;
        this.maxBodyBytes = maxBodyBytes;
        this.maxJsonDepth = maxJsonDepth;
        this.maxScalarStringChars = maxScalarStringChars;
    }

    private static void requirePositive(String name, int value) {
        if (value <= 0) throw new IllegalArgumentException(name + " must be > 0");
    }

    public static PredicateLimits defaults() {
        return new PredicateLimits(
            /*maxSourceLength*/ 4096,
            /*maxParenDepth*/ 32,
            /*maxNodes*/ 128,
            /*maxDepth*/ 16,
            /*maxStringLiteralLength*/ 1024,
            /*maxStepsPerEval*/ 1000,
            /*maxBodyBytes*/ 1 << 20,
            /*maxJsonDepth*/ 32,
            /*maxScalarStringChars*/ 64 * 1024
        );
    }
}
