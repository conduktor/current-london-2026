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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ViewRegistry}.
 *
 * Contract under test:
 *  - {@link ViewRegistry#viewFor(String)} returns {@code empty()} for non-view topics, and
 *    that absence is cached so the config source is not re-queried on every fetch.
 *  - A valid view config compiles into a {@link ViewSpec} that is cached and reused.
 *  - {@link ViewRegistry#invalidate(String)} drops the cache entry; the next call recompiles.
 *  - A view whose backing topic equals the view topic is rejected (no self-loop).
 *  - Bad predicates surface as {@link PredicateValidationException}.
 *  - Concurrent lookups for the same view compile exactly once.
 */
class ViewRegistryTest {

    @Test
    void nonViewTopicReturnsEmpty() {
        ViewRegistry registry = new ViewRegistry(name -> Optional.empty());
        assertEquals(Optional.empty(), registry.viewFor("regular-topic"));
    }

    @Test
    void viewConfigCompilesIntoSpec() {
        Function<String, Optional<ViewRegistry.TopicViewConfigs>> source = name ->
                Optional.of(new ViewRegistry.TopicViewConfigs(
                        "orders-raw",
                        "body.color == 'red'",
                        ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE));
        ViewRegistry registry = new ViewRegistry(source);

        Optional<ViewSpec> spec = registry.viewFor("red-orders");
        assertTrue(spec.isPresent());
        assertEquals("red-orders", spec.get().viewTopic());
        assertEquals("orders-raw", spec.get().backingTopic());
        assertEquals(ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE, spec.get().offsetMode());
        assertNotNull(spec.get().predicate());
        assertEquals("body.color == 'red'", spec.get().predicate().predicateText());
    }

    @Test
    void cachedSpecIsReusedAcrossLookups() {
        AtomicInteger calls = new AtomicInteger();
        ViewRegistry registry = new ViewRegistry(name -> {
            calls.incrementAndGet();
            return Optional.of(new ViewRegistry.TopicViewConfigs(
                    "orders-raw",
                    "body.color == 'red'",
                    ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE));
        });

        ViewSpec first = registry.viewFor("red-orders").orElseThrow();
        ViewSpec second = registry.viewFor("red-orders").orElseThrow();
        assertSame(first, second, "cached spec instance must be reused");
        assertEquals(1, calls.get(), "config source must be consulted only on miss");
    }

    @Test
    void missesAreCachedToo() {
        AtomicInteger calls = new AtomicInteger();
        ViewRegistry registry = new ViewRegistry(name -> {
            calls.incrementAndGet();
            return Optional.empty();
        });

        assertEquals(Optional.empty(), registry.viewFor("regular"));
        assertEquals(Optional.empty(), registry.viewFor("regular"));
        assertEquals(1, calls.get(), "miss should be cached so the fetch hot path doesn't reparse configs");
    }

    @Test
    void invalidateForcesRecompile() {
        AtomicInteger calls = new AtomicInteger();
        ViewRegistry registry = new ViewRegistry(name -> {
            calls.incrementAndGet();
            return Optional.of(new ViewRegistry.TopicViewConfigs(
                    "orders-raw",
                    "body.color == 'red'",
                    ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE));
        });

        registry.viewFor("red-orders");
        registry.viewFor("red-orders");
        registry.invalidate("red-orders");
        registry.viewFor("red-orders");

        assertEquals(2, calls.get(), "invalidate must force the next lookup to re-query the source");
    }

    @Test
    void clearDropsEverything() {
        ConcurrentHashMap<String, ViewRegistry.TopicViewConfigs> store = new ConcurrentHashMap<>();
        store.put("view-a", new ViewRegistry.TopicViewConfigs(
                "raw-a", "body.x == 1", ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE));
        store.put("view-b", new ViewRegistry.TopicViewConfigs(
                "raw-b", "body.y == 2", ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE));

        AtomicInteger calls = new AtomicInteger();
        ViewRegistry registry = new ViewRegistry(name -> {
            calls.incrementAndGet();
            return Optional.ofNullable(store.get(name));
        });

        registry.viewFor("view-a");
        registry.viewFor("view-b");
        assertEquals(2, calls.get());
        registry.clear();
        registry.viewFor("view-a");
        registry.viewFor("view-b");
        assertEquals(4, calls.get(), "clear() drops both cache entries");
    }

    @Test
    void rejectsSelfBacking() {
        ViewRegistry registry = new ViewRegistry(name -> Optional.of(
                new ViewRegistry.TopicViewConfigs(
                        name, "body.x == 1", ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE)));
        assertThrows(IllegalArgumentException.class, () -> registry.viewFor("loop-view"));
    }

    @Test
    void rejectsUnsupportedOffsetMode() {
        ViewRegistry registry = new ViewRegistry(name -> Optional.of(
                new ViewRegistry.TopicViewConfigs("raw", "body.x == 1", "renumber")));
        assertThrows(IllegalArgumentException.class, () -> registry.viewFor("v"));
    }

    @Test
    void propagatesPredicateCompileError() {
        ViewRegistry registry = new ViewRegistry(name -> Optional.of(
                new ViewRegistry.TopicViewConfigs(
                        "raw",
                        "body.s.matches('.*')",  // matches() is rejected by the sandbox
                        ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE)));
        assertThrows(PredicateValidationException.class, () -> registry.viewFor("v"));
    }

    @Test
    void topicViewConfigsFromMapAllOrNone() {
        Map<String, String> all = new HashMap<>();
        all.put(ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG, "orders-raw");
        all.put(ViewTopicConfig.VIEW_CEL_PREDICATE_CONFIG, "body.color == 'red'");
        all.put(ViewTopicConfig.VIEW_OFFSET_MODE_CONFIG, ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE);
        assertTrue(ViewRegistry.TopicViewConfigs.fromMap(all).isPresent());

        Map<String, String> backingOnly = new HashMap<>();
        backingOnly.put(ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG, "orders-raw");
        assertEquals(Optional.empty(), ViewRegistry.TopicViewConfigs.fromMap(backingOnly));

        Map<String, String> blankPredicate = new HashMap<>(all);
        blankPredicate.put(ViewTopicConfig.VIEW_CEL_PREDICATE_CONFIG, "   ");
        assertEquals(Optional.empty(), ViewRegistry.TopicViewConfigs.fromMap(blankPredicate),
                "blank predicate is treated as missing");
    }

    @Test
    void concurrentLookupsCompileOnce() throws InterruptedException, ExecutionException {
        AtomicInteger calls = new AtomicInteger();
        ViewRegistry registry = new ViewRegistry(name -> {
            calls.incrementAndGet();
            return Optional.of(new ViewRegistry.TopicViewConfigs(
                    "orders-raw",
                    "body.color == 'red'",
                    ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE));
        });

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            Future<?>[] futures = new Future<?>[threads];
            for (int i = 0; i < threads; i++) {
                futures[i] = pool.submit(() -> {
                    start.await();
                    return registry.viewFor("red-orders").orElseThrow();
                });
            }
            start.countDown();
            ViewSpec first = (ViewSpec) futures[0].get();
            for (int i = 1; i < threads; i++) {
                assertSame(first, futures[i].get(),
                        "all concurrent lookups must observe the same compiled spec");
            }
            assertEquals(1, calls.get(),
                    "the config source must be consulted exactly once even under contention");
        } finally {
            pool.shutdown();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
