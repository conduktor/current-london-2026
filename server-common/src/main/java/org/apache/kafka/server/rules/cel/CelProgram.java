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
     */
    public boolean evalBoolean(Function<String, Object> activation) {
        Object v = root.eval(activation);
        if (v == null) return false;
        if (v instanceof Boolean) return (Boolean) v;
        throw new CelEvaluationException(
            "CEL expression did not evaluate to a boolean: source=" + source + " value=" + v);
    }

    public String source() {
        return source;
    }

    @Override
    public String toString() {
        return "CelProgram(" + source + ")";
    }
}
