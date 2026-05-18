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
package org.apache.kafka.network.http;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SseStreamLimiterTest {

    @Test
    void acquiresUpToTheCapThenRefuses() {
        // Three acquires must succeed against a cap of 3; the fourth must return null. This is the core admission
        // contract — without it, a runaway client exhausts the Jetty thread pool.
        SseStreamLimiter limiter = new SseStreamLimiter(3);
        List<SseStreamLimiter.Token> tokens = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            SseStreamLimiter.Token t = limiter.tryAcquire();
            assertNotNull(t, "acquire #" + i + " should succeed under the cap");
            tokens.add(t);
        }
        assertEquals(3, limiter.inUse());
        assertNull(limiter.tryAcquire(), "fourth acquire must be refused at the cap");
        // Cleanup
        for (SseStreamLimiter.Token t : tokens) {
            t.close();
        }
        assertEquals(0, limiter.inUse());
    }

    @Test
    void releasingFreesUpSlot() {
        // The cap is a high-water-mark, not a one-shot quota. After a release the next acquire must succeed.
        SseStreamLimiter limiter = new SseStreamLimiter(1);
        SseStreamLimiter.Token first = limiter.tryAcquire();
        assertNotNull(first);
        assertNull(limiter.tryAcquire(), "second acquire blocked at cap");
        first.close();
        SseStreamLimiter.Token third = limiter.tryAcquire();
        assertNotNull(third, "after release, a new acquire must succeed");
        third.close();
    }

    @Test
    void tokenCloseIsIdempotent() {
        // The SSE stream lifecycle can call closeStream() from multiple terminal branches. The token must release
        // exactly once even if close() is called more than once — otherwise the counter goes negative and the cap
        // silently grows.
        SseStreamLimiter limiter = new SseStreamLimiter(2);
        SseStreamLimiter.Token t = limiter.tryAcquire();
        assertEquals(1, limiter.inUse());
        t.close();
        t.close();
        t.close();
        assertEquals(0, limiter.inUse(), "redundant close calls must not over-release");
    }

    @Test
    void zeroCapRefusesEverything() {
        // Setting the cap to zero is a valid degenerate configuration — operator disables SSE entirely while
        // leaving produce/fetch open. Every acquire must fail; nothing about the limiter must blow up internally.
        SseStreamLimiter limiter = new SseStreamLimiter(0);
        assertNull(limiter.tryAcquire());
        assertEquals(0, limiter.inUse());
        assertEquals(0, limiter.maxConcurrent());
    }

    @Test
    void rejectsNegativeMaxConcurrent() {
        // A negative cap is a programming error in the wiring layer — fail fast at construction.
        assertThrows(IllegalArgumentException.class, () -> new SseStreamLimiter(-1));
    }

    @Test
    void concurrentAcquiresHonorTheCapExactly() throws InterruptedException {
        // 32 threads race to acquire against a cap of 10. Exactly 10 must succeed, exactly 22 must fail — no more,
        // no less. The CAS loop in tryAcquire is the only thing standing between us and an over-admission bug.
        int cap = 10;
        int contenders = 32;
        SseStreamLimiter limiter = new SseStreamLimiter(cap);
        AtomicInteger acquired = new AtomicInteger();
        AtomicInteger refused = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(contenders);
        ExecutorService pool = Executors.newFixedThreadPool(contenders);
        try {
            for (int i = 0; i < contenders; i++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        SseStreamLimiter.Token t = limiter.tryAcquire();
                        if (t != null) {
                            acquired.incrementAndGet();
                            // Hold the token — we want to see them ALL contending before any release.
                        } else {
                            refused.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(5, TimeUnit.SECONDS), "all contenders must finish within 5s");
            assertEquals(cap, acquired.get(), "exactly the cap must be admitted");
            assertEquals(contenders - cap, refused.get(), "everything else must be refused");
            assertEquals(cap, limiter.inUse(), "limiter count must equal admissions");
        } finally {
            pool.shutdownNow();
        }
    }
}
