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

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CompiledPredicateTest {

    private final PredicateCompiler compiler = new PredicateCompiler(PredicateLimits.defaults());

    private static RecordContext jsonRecord(String json) {
        return RecordContexts.builder().body(json.getBytes()).build();
    }

    private static RecordContext jsonRecord(String json, PredicateLimits limits) {
        return RecordContexts.builder().body(json.getBytes()).build(limits);
    }

    @Test
    void matchesSimpleEquality() {
        CompiledPredicate p = compiler.compile("body.color == 'red'");
        assertTrue(p.evaluate(jsonRecord("{\"color\":\"red\"}")).orElse(false));
        assertFalse(p.evaluate(jsonRecord("{\"color\":\"blue\"}")).orElse(false));
    }

    @Test
    void matchesNumericComparison() {
        CompiledPredicate p = compiler.compile("body.price > 10");
        assertTrue(p.evaluate(jsonRecord("{\"price\":42}")).orElse(false));
        assertFalse(p.evaluate(jsonRecord("{\"price\":5}")).orElse(false));
        assertFalse(p.evaluate(jsonRecord("{\"price\":10}")).orElse(false));
    }

    @Test
    void matchesNumericEqualityAcrossIntAndDouble() {
        CompiledPredicate p = compiler.compile("body.x == 1");
        assertTrue(p.evaluate(jsonRecord("{\"x\":1}")).orElse(false));
        assertTrue(p.evaluate(jsonRecord("{\"x\":1.0}")).orElse(false));
        assertFalse(p.evaluate(jsonRecord("{\"x\":2}")).orElse(false));
    }

    @Test
    void matchesNestedJson() {
        CompiledPredicate p = compiler.compile("body.user.address.city == 'NYC'");
        assertTrue(p.evaluate(jsonRecord("{\"user\":{\"address\":{\"city\":\"NYC\"}}}")).orElse(false));
        assertFalse(p.evaluate(jsonRecord("{\"user\":{\"address\":{\"city\":\"LA\"}}}")).orElse(false));
    }

    @Test
    void matchesBracketAccessOnBody() {
        CompiledPredicate p = compiler.compile("body['type'] == 'order'");
        assertTrue(p.evaluate(jsonRecord("{\"type\":\"order\"}")).orElse(false));
    }

    @Test
    void evaluatesAndOrNot() {
        CompiledPredicate p = compiler.compile("body.color == 'red' && body.size > 10");
        assertTrue(p.evaluate(jsonRecord("{\"color\":\"red\",\"size\":20}")).orElse(false));
        assertFalse(p.evaluate(jsonRecord("{\"color\":\"red\",\"size\":5}")).orElse(false));
        assertFalse(p.evaluate(jsonRecord("{\"color\":\"blue\",\"size\":20}")).orElse(false));

        CompiledPredicate q = compiler.compile("body.x == 1 || body.x == 2");
        assertTrue(q.evaluate(jsonRecord("{\"x\":1}")).orElse(false));
        assertTrue(q.evaluate(jsonRecord("{\"x\":2}")).orElse(false));
        assertFalse(q.evaluate(jsonRecord("{\"x\":3}")).orElse(false));

        CompiledPredicate r = compiler.compile("!(body.deleted == true)");
        assertTrue(r.evaluate(jsonRecord("{\"deleted\":false}")).orElse(false));
        assertFalse(r.evaluate(jsonRecord("{\"deleted\":true}")).orElse(false));
    }

    @Test
    void evaluatesIdentifierPathAsBoolean() {
        CompiledPredicate p = compiler.compile("body.is_admin");
        assertTrue(p.evaluate(jsonRecord("{\"is_admin\":true}")).orElse(false));
        assertFalse(p.evaluate(jsonRecord("{\"is_admin\":false}")).orElse(false));
    }

    @Test
    void evaluatesHeadersIdentifier() {
        CompiledPredicate p = compiler.compile("headers['x-tenant'] == 'acme'");
        RecordContext ctx = RecordContexts.builder()
                .body("{}".getBytes())
                .header("x-tenant", "acme".getBytes())
                .build();
        assertTrue(p.evaluate(ctx).orElse(false));

        RecordContext ctx2 = RecordContexts.builder()
                .body("{}".getBytes())
                .header("x-tenant", "other".getBytes())
                .build();
        assertFalse(p.evaluate(ctx2).orElse(false));
    }

    @Test
    void evaluatesOffsetPartitionTimestamp() {
        CompiledPredicate p = compiler.compile("offset > 100 && partition == 0 && timestamp > 0");
        RecordContext ctx = RecordContexts.builder()
                .body("{}".getBytes())
                .offset(101).partition(0).timestamp(1000)
                .build();
        assertTrue(p.evaluate(ctx).orElse(false));

        RecordContext ctx2 = RecordContexts.builder()
                .body("{}".getBytes())
                .offset(100).partition(0).timestamp(1000)
                .build();
        assertFalse(p.evaluate(ctx2).orElse(false));
    }

    @Test
    void evaluatesKey() {
        CompiledPredicate p = compiler.compile("key == 'user-42'");
        assertTrue(p.evaluate(RecordContexts.builder()
                .body("{}".getBytes())
                .key("user-42".getBytes())
                .build()).orElse(false));
        assertFalse(p.evaluate(RecordContexts.builder()
                .body("{}".getBytes())
                .key("user-43".getBytes())
                .build()).orElse(false));
    }

    // ---------- silent-skip behaviour ----------

    @Test
    void skipsRecordWhenLeafKeyAppearsTwice() {
        // Predicates are an access-control boundary. RFC 8259 leaves duplicate-key behaviour
        // "undefined"; parsers disagree (Jackson last-wins; some first-wins). An adversary
        // who can write to the backing topic could craft {"region":"EU","region":"US"} to bypass
        // a body.region == "US" predicate that some downstream parser reads as "EU".
        // The navigator refuses the record (BODY_UNUSABLE → SKIP) regardless of which value
        // a downstream parser would have picked.
        CompiledPredicate p = compiler.compile("body.region == 'US'");
        Optional<Boolean> r = p.evaluate(jsonRecord("{\"region\":\"EU\",\"region\":\"US\"}"));
        assertTrue(r.isEmpty(),
                () -> "expected skip on duplicate leaf key, got " + r);
    }

    @Test
    void skipsRecordWhenLeafKeyDuplicatedWithSameValue() {
        // Even when both occurrences have the same value, refuse — the navigator can't know
        // a downstream parser will agree.
        CompiledPredicate p = compiler.compile("body.region == 'US'");
        Optional<Boolean> r = p.evaluate(jsonRecord("{\"region\":\"US\",\"region\":\"US\"}"));
        assertTrue(r.isEmpty(),
                () -> "expected skip on duplicate leaf key even with identical values, got " + r);
    }

    @Test
    void skipsRecordWhenLeafKeyDuplicateAppearsAfterOtherFields() {
        // The duplicate-scan must continue past the first match through the rest of the object.
        CompiledPredicate p = compiler.compile("body.region == 'US'");
        Optional<Boolean> r = p.evaluate(
                jsonRecord("{\"region\":\"US\",\"other\":1,\"region\":\"EU\"}"));
        assertTrue(r.isEmpty(),
                () -> "expected skip on duplicate leaf key after sibling field, got " + r);
    }

    @Test
    void leafKeyDuplicateInsideUnrelatedSubobjectDoesNotPoisonOtherPaths() {
        // body.color is at root; the duplicate is inside body.meta — shouldn't affect body.color.
        CompiledPredicate p = compiler.compile("body.color == 'red'");
        assertTrue(p.evaluate(jsonRecord(
                "{\"color\":\"red\",\"meta\":{\"x\":1,\"x\":2}}")).orElse(false));
    }

    @Test
    void skipsRecordWhenLongValueExceedsIeeeSafeRange() {
        // 2^53 = 9_007_199_254_740_992 is the largest integer all doubles can exactly represent.
        // A Long larger than that loses precision when promoted to double. An adversary could
        // craft Long values that "equal" the rounded double representation of a different literal,
        // bypassing a predicate. Refuse the equality outside the IEEE-safe range.
        CompiledPredicate p = compiler.compile("body.x == 9007199254740992.0");
        // Body has 9007199254740993 (one above 2^53). Without the safe-range check this would
        // promote to (double) 9007199254740992 and compare equal — wrong.
        Optional<Boolean> r = p.evaluate(jsonRecord("{\"x\":9007199254740993}"));
        assertFalse(r.orElse(true),
                () -> "expected false (no precision-loss match) for Long beyond 2^53, got " + r);
    }

    @Test
    void longInsideIeeeSafeRangeStillMatchesDoubleLiteral() {
        // Sanity: when the Long is inside the safe range, equality with a Double literal still works.
        CompiledPredicate p = compiler.compile("body.x == 42.0");
        assertTrue(p.evaluate(jsonRecord("{\"x\":42}")).orElse(false));
    }

    @Test
    void skipsRecordWhenOrderedComparisonAcrossLongDoubleLosesPrecision() {
        // The IEEE-safe-integer guard must apply to ordered comparison too, not just equality.
        // Without it: body Long 9007199254740993 promotes to (double) 9007199254740992 and
        // compares <= 9007199254740992.0 as true — bypassing an account_id ceiling check.
        // Acceptable outcomes: false (compare returned a definite no-match) or empty (compare
        // returned unknown, propagated as falsy → record skipped by the filter). NOT true.
        CompiledPredicate p = compiler.compile("body.account_id <= 9007199254740992.0");
        Optional<Boolean> r = p.evaluate(jsonRecord("{\"account_id\":9007199254740993}"));
        assertTrue(r.isEmpty() || !r.get(),
                () -> "expected no precision-loss match in ordered compare, got " + r);
    }

    @Test
    void orderedComparisonStillWorksInsideIeeeSafeRange() {
        // Sanity: mixed Long/Double comparison still works when the Long is safe.
        CompiledPredicate p = compiler.compile("body.x <= 10.5");
        assertTrue(p.evaluate(jsonRecord("{\"x\":10}")).orElse(false));
        assertFalse(p.evaluate(jsonRecord("{\"x\":11}")).orElse(false));
    }

    @Test
    void rejectsPrecisionLossFloatLiteralAtCompileTime() {
        // 9007199254740993.0 silently rounds to 9007199254740992.0 in Double. A predicate
        // body.x == 9007199254740993.0 therefore matches a Long that the author did not intend.
        // The lexer rejects the literal at compile time with a clear error.
        org.apache.kafka.server.views.PredicateValidationException ex =
                org.junit.jupiter.api.Assertions.assertThrows(
                        org.apache.kafka.server.views.PredicateValidationException.class,
                        () -> compiler.compile("body.x == 9007199254740993.0"));
        assertTrue(ex.getMessage().contains("precision-loss"),
                () -> "expected precision-loss message, got: " + ex.getMessage());
    }

    @Test
    void acceptsCommonFloatLiteralsBelowSafeRange() {
        // Sanity: 0.1, 1.5, scientific notation with safe magnitudes — none should be rejected.
        compiler.compile("body.x == 0.1");
        compiler.compile("body.x == 1.5");
        compiler.compile("body.x == 1.5e10");
        compiler.compile("body.x == 9007199254740992.0"); // exactly 2^53 — safe
    }

    @Test
    void skipsRecordWhenBodyIsMalformedJson() {
        CompiledPredicate p = compiler.compile("body.color == 'red'");
        Optional<Boolean> r = p.evaluate(RecordContexts.builder()
                .body("not-json".getBytes())
                .build());
        assertTrue(r.isEmpty() || !r.get(),
                () -> "expected skip or false on malformed body, got " + r);
    }

    @Test
    void skipsRecordWhenFieldIsMissing() {
        // Missing fields compare unequal to non-null literals → false (record skipped by filter)
        CompiledPredicate p = compiler.compile("body.color == 'red'");
        Optional<Boolean> r = p.evaluate(jsonRecord("{}"));
        assertFalse(r.orElse(true));
    }

    @Test
    void skipsRecordWhenTopLevelResultIsNonBoolean() {
        // body.x is a number but used at top-level — at runtime the result is not a boolean → empty
        CompiledPredicate p = compiler.compile("body.x");
        Optional<Boolean> r = p.evaluate(jsonRecord("{\"x\":42}"));
        assertTrue(r.isEmpty(),
                () -> "expected skip when top-level evaluates to non-boolean, got " + r);
    }

    @Test
    void skipsRecordWhenTypeMismatchInComparison() {
        // 'red' < 5 — different types, evaluator returns empty (skip).
        CompiledPredicate p = compiler.compile("body.x < 5");
        Optional<Boolean> r = p.evaluate(jsonRecord("{\"x\":\"red\"}"));
        assertTrue(r.isEmpty() || !r.get());
    }

    @Test
    void skipsRecordWhenStepCapExceeded() {
        PredicateLimits cheap = new PredicateLimits(4096, 32, 256, 16, 256,
                /*maxStepsPerEval*/3, 1 << 20, 32);
        CompiledPredicate p = new PredicateCompiler(cheap)
                .compile("body.a == 1 && body.b == 2 && body.c == 3 && body.d == 4");
        Optional<Boolean> r = p.evaluate(jsonRecord("{\"a\":1,\"b\":2,\"c\":3,\"d\":4}"));
        assertTrue(r.isEmpty(),
                () -> "expected skip when step cap exceeded, got " + r);
    }

    @Test
    void skipsRecordWhenBodyExceedsByteCap() {
        PredicateLimits tightBody = new PredicateLimits(4096, 32, 256, 16, 256, 1000,
                /*maxBodyBytes*/8, 32);
        CompiledPredicate p = new PredicateCompiler(tightBody).compile("body.x == 1");
        Optional<Boolean> r = p.evaluate(jsonRecord("{\"x\":1, \"padding\":\"too-big\"}", tightBody));
        assertTrue(r.isEmpty(),
                () -> "expected skip when body exceeds maxBodyBytes, got " + r);
    }

    @Test
    void skipsRecordWhenJsonDepthExceedsCap() {
        PredicateLimits shallowJson = new PredicateLimits(4096, 32, 256, 16, 256, 1000,
                1 << 20, /*maxJsonDepth*/2);
        CompiledPredicate p = new PredicateCompiler(shallowJson).compile("body.a.b.c == 1");
        Optional<Boolean> r = p.evaluate(jsonRecord("{\"a\":{\"b\":{\"c\":1}}}", shallowJson));
        assertTrue(r.isEmpty(),
                () -> "expected skip when JSON depth exceeds cap, got " + r);
    }

    @Test
    void invalidUtf8HeaderTreatedAsNullDoesNotCrash() {
        CompiledPredicate p = compiler.compile("headers['x-tenant'] == 'acme'");
        byte[] invalid = new byte[]{(byte) 0xC0, (byte) 0x80}; // overlong NUL — not valid UTF-8
        RecordContext ctx = RecordContexts.builder()
                .body("{}".getBytes())
                .header("x-tenant", invalid)
                .build();
        Optional<Boolean> r = p.evaluate(ctx);
        // result is empty or false — but evaluation must not throw
        assertTrue(r.isEmpty() || !r.get());
    }

    @Test
    void bodyOnlyPredicateIgnoresMalformedHeaders() {
        CompiledPredicate p = compiler.compile("body.color == 'red'");
        byte[] junk = new byte[]{(byte) 0xC0, (byte) 0x80};
        RecordContext ctx = RecordContexts.builder()
                .body("{\"color\":\"red\"}".getBytes())
                .header("x-bad", junk)
                .build();
        assertTrue(p.evaluate(ctx).orElse(false));
    }

    // ---------- thread-safety ----------

    @Test
    void compiledPredicateIsThreadSafe() throws Exception {
        CompiledPredicate p = compiler.compile("body.x == 1");
        int threads = 8;
        int iterations = 1000;
        Thread[] workers = new Thread[threads];
        AtomicInteger trueCount = new AtomicInteger();
        AtomicInteger falseCount = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        for (int i = 0; i < threads; i++) {
            final boolean shouldMatch = (i % 2) == 0;
            workers[i] = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException ignored) {
                }
                String body = shouldMatch ? "{\"x\":1}" : "{\"x\":2}";
                for (int j = 0; j < iterations; j++) {
                    Optional<Boolean> r = p.evaluate(jsonRecord(body));
                    if (r.orElse(false)) trueCount.incrementAndGet();
                    else falseCount.incrementAndGet();
                }
            });
            workers[i].start();
        }
        start.countDown();
        for (Thread t : workers) t.join();
        // Half threads ran with matching body, half not — totals must exactly match.
        assertEquals((threads / 2) * iterations, trueCount.get());
        assertEquals((threads / 2) * iterations, falseCount.get());
    }
}
