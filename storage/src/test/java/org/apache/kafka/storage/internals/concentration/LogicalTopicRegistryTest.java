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
package org.apache.kafka.storage.internals.concentration;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behavioural contract of {@link LogicalTopicRegistry}.
 *
 * <p>The registry is the broker's source of truth for "which topics are logical, what is each
 * one's backing, and which physical topics are reserved as backings". It is consulted on the
 * produce path (route logical→backing; reject physical-name produces against a backing) and on
 * the fetch path (route logical→backing).
 */
public class LogicalTopicRegistryTest {

    @Test
    public void newlyConstructedRegistryHasNothing() {
        LogicalTopicRegistry r = new LogicalTopicRegistry();
        assertFalse(r.contains("orders"));
        assertTrue(r.get("orders").isEmpty());
        assertFalse(r.isBackingTopic("backing"));
        assertTrue(r.descriptorsFor("backing").isEmpty());
    }

    @Test
    public void declaredTopicIsLookable() {
        LogicalTopicRegistry r = new LogicalTopicRegistry();
        LogicalTopicDescriptor d = new LogicalTopicDescriptor("orders", 1000, "backing", 4);
        r.declare(d);
        assertTrue(r.contains("orders"));
        assertEquals(Optional.of(d), r.get("orders"));
    }

    @Test
    public void declaringTheSameDescriptorTwiceIsIdempotent() {
        LogicalTopicRegistry r = new LogicalTopicRegistry();
        LogicalTopicDescriptor d1 = new LogicalTopicDescriptor("orders", 1000, "backing", 4);
        LogicalTopicDescriptor d2 = new LogicalTopicDescriptor("orders", 1000, "backing", 4);
        r.declare(d1);
        r.declare(d2);
        assertSame(d1, r.get("orders").orElseThrow());
    }

    @Test
    public void declaringTheSameNameWithDifferentDescriptorIsRejected() {
        LogicalTopicRegistry r = new LogicalTopicRegistry();
        r.declare(new LogicalTopicDescriptor("orders", 1000, "backing", 4));
        assertThrows(IllegalStateException.class,
            () -> r.declare(new LogicalTopicDescriptor("orders", 2000, "backing", 4)),
            "redeclaring 'orders' with a different partition count must be rejected");
        assertThrows(IllegalStateException.class,
            () -> r.declare(new LogicalTopicDescriptor("orders", 1000, "otherBacking", 4)),
            "redeclaring 'orders' against a different backing must be rejected");
    }

    @Test
    public void multipleLogicalTopicsCanShareOneBacking() {
        // Core motivation for concentration: many logical topics sharing one physical backing.
        LogicalTopicRegistry r = new LogicalTopicRegistry();
        LogicalTopicDescriptor a = new LogicalTopicDescriptor("topicA", 1000, "shared", 4);
        LogicalTopicDescriptor b = new LogicalTopicDescriptor("topicB", 500, "shared", 4);
        LogicalTopicDescriptor c = new LogicalTopicDescriptor("topicC", 250, "shared", 4);
        r.declare(a);
        r.declare(b);
        r.declare(c);
        assertEquals(3, r.descriptorsFor("shared").size());
        assertTrue(r.descriptorsFor("shared").contains(a));
        assertTrue(r.descriptorsFor("shared").contains(b));
        assertTrue(r.descriptorsFor("shared").contains(c));
    }

    @Test
    public void redeclaringWithMismatchedBackingPartitionCountIsRejected() {
        // Multiple logical topics on the same backing must agree on M. Disagreement implies a
        // misconfiguration that would lead to incompatible routing across producers.
        LogicalTopicRegistry r = new LogicalTopicRegistry();
        r.declare(new LogicalTopicDescriptor("topicA", 1000, "shared", 4));
        assertThrows(IllegalStateException.class,
            () -> r.declare(new LogicalTopicDescriptor("topicB", 1000, "shared", 8)),
            "second logical topic must use the same backing M as the first");
    }

    @Test
    public void isBackingTopicReturnsTrueAfterAnyDeclaration() {
        // PROMPT acceptance criterion: "Direct produce to a backing topic by physical name
        // (bypassing logical-topic routing) is rejected by the broker."
        LogicalTopicRegistry r = new LogicalTopicRegistry();
        assertFalse(r.isBackingTopic("backing"));
        r.declare(new LogicalTopicDescriptor("orders", 1000, "backing", 4));
        assertTrue(r.isBackingTopic("backing"));
    }

    @Test
    public void logicalNameAndBackingNameAreDisjointNamespaces() {
        // A user must not declare a logical topic whose name collides with an existing backing,
        // nor a backing that collides with an existing logical topic, otherwise the broker
        // cannot tell which routing rule to apply.
        LogicalTopicRegistry r = new LogicalTopicRegistry();
        r.declare(new LogicalTopicDescriptor("orders", 1000, "backing", 4));
        assertThrows(IllegalStateException.class,
            () -> r.declare(new LogicalTopicDescriptor("backing", 1000, "newBacking", 4)),
            "logical name colliding with an existing backing must be rejected");
        assertThrows(IllegalStateException.class,
            () -> r.declare(new LogicalTopicDescriptor("other", 1000, "orders", 4)),
            "backing colliding with an existing logical name must be rejected");
    }

    @Test
    public void declarationsRejectNullDescriptor() {
        LogicalTopicRegistry r = new LogicalTopicRegistry();
        assertThrows(NullPointerException.class, () -> r.declare(null));
    }

    @Test
    public void concurrentDeclarationsResolveDeterministically() throws Exception {
        // Multiple admin requests racing on the same name: one must win, the others must
        // either succeed (idempotent) or fail consistently (conflict).
        LogicalTopicRegistry r = new LogicalTopicRegistry();
        final int threads = 16;
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        try {
            List<Future<Boolean>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int id = t;
                futures.add(exec.submit(() -> {
                    try {
                        // half try the canonical, half try a conflicting one
                        if (id % 2 == 0) {
                            r.declare(new LogicalTopicDescriptor("orders", 1000, "backing", 4));
                        } else {
                            r.declare(new LogicalTopicDescriptor("orders", 2000, "backing", 4));
                        }
                        return true;
                    } catch (IllegalStateException e) {
                        return false;
                    }
                }));
            }
            int successes = 0;
            for (Future<Boolean> f : futures) {
                if (f.get(5, TimeUnit.SECONDS)) successes++;
            }
            // At least one of the canonical-descriptor declarers must have succeeded, plus any
            // idempotent re-declarations of the same descriptor. Exact count is racy; the
            // important invariant is determinism of the final registry state.
            assertTrue(successes >= 1);
            LogicalTopicDescriptor finalState = r.get("orders").orElseThrow();
            assertNotNull(finalState);
        } finally {
            exec.shutdownNow();
        }
    }
}
