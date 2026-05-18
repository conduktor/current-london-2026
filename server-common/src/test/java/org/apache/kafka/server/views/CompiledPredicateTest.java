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
