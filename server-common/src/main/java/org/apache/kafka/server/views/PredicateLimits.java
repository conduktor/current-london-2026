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
 *  - maxNodes / maxDepth: malicious or buggy predicates that would balloon parser/evaluator memory or recursion.
 *  - maxStringLiteralLength: oversized literals embedded in the predicate text.
 *  - maxStepsPerEval: catastrophic per-record cost on a hot path.
 *  - maxBodyBytes: oversized record values feeding the JSON parser.
 *  - maxJsonDepth: deeply nested JSON exhausting the parser stack.
 */
public final class PredicateLimits {
    public final int maxNodes;
    public final int maxDepth;
    public final int maxStringLiteralLength;
    public final int maxStepsPerEval;
    public final int maxBodyBytes;
    public final int maxJsonDepth;

    public PredicateLimits(int maxNodes, int maxDepth, int maxStringLiteralLength,
                           int maxStepsPerEval, int maxBodyBytes, int maxJsonDepth) {
        if (maxNodes <= 0) throw new IllegalArgumentException("maxNodes must be > 0");
        if (maxDepth <= 0) throw new IllegalArgumentException("maxDepth must be > 0");
        if (maxStringLiteralLength <= 0) throw new IllegalArgumentException("maxStringLiteralLength must be > 0");
        if (maxStepsPerEval <= 0) throw new IllegalArgumentException("maxStepsPerEval must be > 0");
        if (maxBodyBytes <= 0) throw new IllegalArgumentException("maxBodyBytes must be > 0");
        if (maxJsonDepth <= 0) throw new IllegalArgumentException("maxJsonDepth must be > 0");
        this.maxNodes = maxNodes;
        this.maxDepth = maxDepth;
        this.maxStringLiteralLength = maxStringLiteralLength;
        this.maxStepsPerEval = maxStepsPerEval;
        this.maxBodyBytes = maxBodyBytes;
        this.maxJsonDepth = maxJsonDepth;
    }

    public static PredicateLimits defaults() {
        return new PredicateLimits(
            /*maxNodes*/ 128,
            /*maxDepth*/ 16,
            /*maxStringLiteralLength*/ 1024,
            /*maxStepsPerEval*/ 1000,
            /*maxBodyBytes*/ 1 << 20,
            /*maxJsonDepth*/ 32
        );
    }
}
