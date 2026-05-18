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
}
