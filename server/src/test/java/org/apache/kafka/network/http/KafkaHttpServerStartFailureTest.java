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

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wave 31 axis BBB: a {@link KafkaHttpServer#start()} that fails partway through must call
 * {@code jetty.stop()} before propagating the failure. Without that unwind, the local Jetty reference is
 * dropped on throw — but Jetty's already-started beans ({@code QueuedThreadPool}, {@code ServerConnector}
 * acceptor / selector threads) keep running until they observe a stop signal. The pathological shapes are
 * a restart-loop after a port collision steadily growing the live thread count, and a boot-time failure
 * during {@code BrokerServer} startup leaving orphaned Jetty threads attached to a server reference the
 * broker can no longer reach.
 *
 * <p>This test is kept in its own class — not folded into {@link KafkaHttpServerIntegrationTest} —
 * because adding {@link ServerSocket} + {@link InetSocketAddress} to the integration test's reference
 * graph would push it over the {@code ClassDataAbstractionCoupling} ceiling.
 */
class KafkaHttpServerStartFailureTest {

    private static final int DEFAULT_TEST_MAX_BODY_BYTES = 1024 * 1024;
    private static final int DEFAULT_TEST_MAX_SSE_STREAMS = 100;
    private static final int DEFAULT_TEST_MAX_WS_SUBSCRIPTIONS = 100;

    private final ObjectMapper mapper = new ObjectMapper();
    // Anonymous unreachable submitter: start() fails on the port-bind step, well before any request
    // is dispatched, so the bridge never actually invokes either method. Throwing here surfaces a clear
    // failure if the test regresses such that a request DOES somehow reach the submitter.
    private final RequestSubmitter submitter = new RequestSubmitter() {
        @Override
        public java.util.concurrent.CompletableFuture<ProduceResult> submitProduce(
                ProduceRequestParser.ProduceCommand command) {
            throw new UnsupportedOperationException("start should have failed before any request");
        }

        @Override
        public java.util.concurrent.CompletableFuture<FetchResult> submitFetch(
                FetchRequestParser.FetchCommand command) {
            throw new UnsupportedOperationException("start should have failed before any request");
        }
    };

    @Test
    void startFailureUnwindsJettyInternalsInsteadOfLeakingThreads() throws Exception {
        // To pin the contract we squat on a port with a plain ServerSocket, then attempt to start the
        // bridge on the same port. start() must throw, and after a bounded wait the Jetty-side thread
        // signature must converge back to the baseline. Without the unwind, the threads spawned by the
        // partial start() would still be alive when we measure.
        try (ServerSocket squatter = new ServerSocket()) {
            squatter.setReuseAddress(false);
            squatter.bind(new InetSocketAddress("127.0.0.1", 0));
            int squattedPort = squatter.getLocalPort();
            long jettyBefore = countJettyOwnedThreads();
            KafkaHttpServer doomed = new KafkaHttpServer("127.0.0.1", squattedPort,
                new KafkaHttpBridge(mapper, submitter), submitter, mapper,
                DEFAULT_TEST_MAX_BODY_BYTES, DEFAULT_TEST_MAX_SSE_STREAMS, DEFAULT_TEST_MAX_WS_SUBSCRIPTIONS);
            try {
                // BindException (wrapped in something else by Jetty's lifecycle) — the exact type is not
                // load-bearing; what matters is that start() does throw, otherwise the test premise is gone.
                assertThrows(Exception.class, doomed::start,
                    "bridge bound on a taken port must fail to start; without the precondition the leak "
                        + "check below would pass vacuously");
                // Poll until Jetty's stop() unwind drains its threads (QueuedThreadPool.stop joins workers).
                // 5 s is comfortable: in practice the unwind completes inside 200 ms on every platform we
                // run on, but the deadline keeps the test from hanging if the fix regresses.
                long deadline = System.currentTimeMillis() + 5000L;
                long jettyAfter = countJettyOwnedThreads();
                while (jettyAfter > jettyBefore && System.currentTimeMillis() < deadline) {
                    Thread.sleep(50);
                    jettyAfter = countJettyOwnedThreads();
                }
                assertTrue(jettyAfter <= jettyBefore,
                    "failed start() must unwind every Jetty thread it spawned (without the unwind, the "
                        + "partial-start would leak the QueuedThreadPool and connector selectors). "
                        + "before=" + jettyBefore + " after=" + jettyAfter
                        + " — leaked threads: "
                        + Thread.getAllStackTraces().keySet().stream()
                            .filter(KafkaHttpServerStartFailureTest::isJettyOwnedThread)
                            .map(Thread::getName).sorted().toList());
            } finally {
                try {
                    doomed.stop();
                } catch (Exception ignored) {
                    // already unwound by the unwind block in start()
                }
            }
        }
    }

    /**
     * Count live threads owned by an actively running Jetty server. The signature is the conjunction of
     * the QueuedThreadPool's "qtp" prefix and the acceptor/selector/connector naming Jetty 12 uses for its
     * I/O threads.
     */
    private static long countJettyOwnedThreads() {
        return Thread.getAllStackTraces().keySet().stream()
            .filter(KafkaHttpServerStartFailureTest::isJettyOwnedThread)
            .count();
    }

    private static boolean isJettyOwnedThread(Thread t) {
        String n = t.getName();
        // Jetty 12 thread names: "qtp<id>-<n>" for QueuedThreadPool workers, names containing
        // "Acceptor", "Selector", "ServerConnector" for the I/O threads. Match on any.
        return n.startsWith("qtp") || n.contains("Acceptor") || n.contains("Selector@")
            || n.contains("ServerConnector");
    }
}
