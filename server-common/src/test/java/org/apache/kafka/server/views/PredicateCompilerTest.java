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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PredicateCompilerTest {

    private final PredicateCompiler compiler = new PredicateCompiler(PredicateLimits.defaults());

    @Test
    void compilesSimpleEquality() {
        CompiledPredicate p = compiler.compile("body.color == 'red'");
        assertNotNull(p);
    }

    @Test
    void compilesLogicalAndComparison() {
        assertNotNull(compiler.compile("body.color == 'red' && body.size > 10"));
        assertNotNull(compiler.compile("offset >= 100 || partition == 0"));
        assertNotNull(compiler.compile("!(body.deleted)"));
    }

    @Test
    void compilesNumericArithmeticInsideComparison() {
        assertNotNull(compiler.compile("body.price * 2 < 100"));
        assertNotNull(compiler.compile("body.a + body.b == body.c"));
    }

    @Test
    void compilesStringAndNumericLiterals() {
        assertNotNull(compiler.compile("body.x == 42"));
        assertNotNull(compiler.compile("body.x == 3.14"));
        assertNotNull(compiler.compile("body.x == 'hello'"));
        assertNotNull(compiler.compile("body.x == \"hello\""));
        assertNotNull(compiler.compile("body.x == true"));
        assertNotNull(compiler.compile("body.x == null"));
    }

    @Test
    void compilesNestedFieldAccess() {
        assertNotNull(compiler.compile("body.user.address.city == 'NYC'"));
    }

    @Test
    void compilesBracketIndexing() {
        assertNotNull(compiler.compile("headers['x-tenant'] == 'acme'"));
        assertNotNull(compiler.compile("body['type'] == 'order'"));
    }

    @Test
    void compilesAllRootIdentifiers() {
        assertNotNull(compiler.compile("offset >= 0"));
        assertNotNull(compiler.compile("partition == 0"));
        assertNotNull(compiler.compile("timestamp > 0"));
        assertNotNull(compiler.compile("key == 'k1'"));
        assertNotNull(compiler.compile("body.x == 1"));
        assertNotNull(compiler.compile("headers.h == 'v'"));
    }

    // ---------- compile-time rejections ----------

    @Test
    void rejectsMatchesFunctionCall() {
        PredicateValidationException ex = assertThrows(PredicateValidationException.class,
                () -> compiler.compile("body.s.matches('.*foo.*')"));
        assertTrue(ex.getMessage().toLowerCase(java.util.Locale.ROOT).contains("function call"),
                () -> "expected message about function calls, got: " + ex.getMessage());
    }

    @Test
    void rejectsAnyFunctionCall() {
        assertThrows(PredicateValidationException.class, () -> compiler.compile("size(body.s) > 0"));
        assertThrows(PredicateValidationException.class, () -> compiler.compile("body.exists(x, x > 0)"));
        assertThrows(PredicateValidationException.class, () -> compiler.compile("body.filter(x, x > 0)"));
        assertThrows(PredicateValidationException.class, () -> compiler.compile("body.map(x, x + 1)"));
        assertThrows(PredicateValidationException.class, () -> compiler.compile("starts_with(body.s, 'a')"));
    }

    @Test
    void rejectsBracketComprehensionSyntax() {
        // [for x in body.list: x > 0] type syntax isn't valid in our grammar — verify it's rejected
        assertThrows(PredicateValidationException.class, () -> compiler.compile("[1, 2, 3]"));
    }

    @Test
    void rejectsUnknownRootIdentifier() {
        assertThrows(PredicateValidationException.class, () -> compiler.compile("env.user == 'x'"));
        assertThrows(PredicateValidationException.class, () -> compiler.compile("rand > 0"));
    }

    @Test
    void rejectsNonBooleanTopLevelExpression() {
        // A bare number literal is not a boolean predicate
        assertThrows(PredicateValidationException.class, () -> compiler.compile("42"));
        assertThrows(PredicateValidationException.class, () -> compiler.compile("'red'"));
        // A bare numeric path can't be a boolean
        assertThrows(PredicateValidationException.class, () -> compiler.compile("body.x + 1"));
    }

    @Test
    void rejectsExpressionsExceedingNodeLimit() {
        PredicateLimits tight = new PredicateLimits(/*maxNodes*/5, /*maxDepth*/16,
                /*maxStringLiteralLength*/256, /*maxStepsPerEval*/1000,
                /*maxBodyBytes*/1 << 20, /*maxJsonDepth*/32);
        PredicateCompiler tightCompiler = new PredicateCompiler(tight);
        assertThrows(PredicateValidationException.class,
                () -> tightCompiler.compile("body.a == 1 && body.b == 2 && body.c == 3"));
    }

    @Test
    void rejectsExpressionsExceedingDepthLimit() {
        PredicateLimits shallow = new PredicateLimits(/*maxNodes*/256, /*maxDepth*/3,
                /*maxStringLiteralLength*/256, /*maxStepsPerEval*/1000,
                /*maxBodyBytes*/1 << 20, /*maxJsonDepth*/32);
        PredicateCompiler shallowCompiler = new PredicateCompiler(shallow);
        // Tree depth = 4 (AND over AND over EQ over MUL) — exceeds maxDepth=3.
        assertThrows(PredicateValidationException.class,
                () -> shallowCompiler.compile("body.a * 2 == 4 && body.b == 1 && body.c == 1"));
    }

    @Test
    void rejectsStringLiteralLongerThanLimit() {
        PredicateLimits tight = new PredicateLimits(64, 16, /*maxStringLiteralLength*/4,
                1000, 1 << 20, 32);
        PredicateCompiler tightCompiler = new PredicateCompiler(tight);
        assertThrows(PredicateValidationException.class,
                () -> tightCompiler.compile("body.s == 'too-long'"));
    }

    @Test
    void rejectsIntegerLiteralBeyondLongRange() {
        assertThrows(PredicateValidationException.class,
                () -> compiler.compile("body.x == 99999999999999999999"));
    }

    @Test
    void rejectsSyntaxErrors() {
        assertThrows(PredicateValidationException.class, () -> compiler.compile(""));
        assertThrows(PredicateValidationException.class, () -> compiler.compile("body.color =="));
        assertThrows(PredicateValidationException.class, () -> compiler.compile("&&"));
        assertThrows(PredicateValidationException.class, () -> compiler.compile("body..x"));
        assertThrows(PredicateValidationException.class, () -> compiler.compile("body.x ==="));
    }

    // ---------- top-level boolean shape ----------

    @Test
    void acceptsBooleanLiteralAtTopLevel() {
        assertNotNull(compiler.compile("true"));
        assertNotNull(compiler.compile("false"));
    }

    @Test
    void acceptsIdentifierPathAtTopLevelAsDynamicBoolean() {
        // Field access could be a boolean at runtime — accepted at compile time.
        assertNotNull(compiler.compile("body.flag"));
        assertNotNull(compiler.compile("headers.is_admin"));
    }

    @Test
    void normalizesEquivalentSyntax() {
        // Both single and double quotes for strings produce a working predicate
        CompiledPredicate p1 = compiler.compile("body.x == 'red'");
        CompiledPredicate p2 = compiler.compile("body.x == \"red\"");
        assertEquals(p1.predicateText(), "body.x == 'red'");
        assertEquals(p2.predicateText(), "body.x == \"red\"");
    }
}
