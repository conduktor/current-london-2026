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
package org.apache.kafka.network.iouring;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;

import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;

/**
 * JUnit 5 extension that flips Netty's {@link ResourceLeakDetector} to
 * {@link Level#PARANOID} for the duration of a test class. PARANOID samples
 * every allocation rather than the default 1-in-128, so any unreleased
 * {@code ByteBuf} the test code (or the io_uring transport it exercises) leaves
 * dangling will be detected at the next GC and logged as an SLF4J ERROR with a
 * full allocation backtrace.
 *
 * <p>Why a class-scoped extension and not a global JVM property: the default
 * Kafka test JVMs run many non-io_uring suites in the same fork. Flipping to
 * PARANOID globally would slow every Netty user in the codebase. Scoping to the
 * io_uring test classes that actually allocate {@code ByteBuf} keeps the cost
 * contained while still catching regressions in the only code paths this
 * branch introduced.
 *
 * <p>Why not fail-on-leak: Netty's leak reporter logs via {@code InternalLogger}
 * (routed to SLF4J / log4j2 in our test setup). Reliably wiring an appender
 * that fails the test would require either a custom
 * {@link io.netty.util.ResourceLeakDetectorFactory} installed before any Netty
 * class loads (impractical mid-suite — most ByteBuf detectors are static-final)
 * or a log-capturing appender on the {@code io.netty} logger. The audit task
 * (T-LEAK, #91) explicitly asked for PARANOID level, period. Operators / CI
 * read the stderr {@code LEAK:} markers; that contract is enough for v1. A
 * follow-up can install a programmatic appender if a regression slips through
 * stderr review.
 *
 * <p>The level is restored to whatever it was before the test class ran, so
 * neighbouring test classes in the same JVM fork are not affected.
 */
public class IoUringLeakDetectorExtension implements BeforeAllCallback, AfterAllCallback {

    private static final Namespace NS = Namespace.create(IoUringLeakDetectorExtension.class);
    private static final String PRIOR_LEVEL_KEY = "priorLevel";

    @Override
    public void beforeAll(ExtensionContext context) {
        Store store = context.getStore(NS);
        store.put(PRIOR_LEVEL_KEY, ResourceLeakDetector.getLevel());
        ResourceLeakDetector.setLevel(Level.PARANOID);
    }

    @Override
    public void afterAll(ExtensionContext context) {
        // Surface any unreached leaks before we restore the prior level. PARANOID detection
        // only fires when the JVM reclaims a leaked resource's referent — without a GC nudge,
        // a leak introduced late in the suite could escape detection entirely and the next
        // unrelated test class would inherit the blame in stderr. System.gc() is a hint, not
        // a guarantee, but on HotSpot it reliably triggers a young-gen collection which is
        // enough to release the PhantomReferences Netty's detector watches.
        System.gc();
        Level prior = context.getStore(NS).get(PRIOR_LEVEL_KEY, Level.class);
        if (prior != null) {
            ResourceLeakDetector.setLevel(prior);
        }
    }
}
