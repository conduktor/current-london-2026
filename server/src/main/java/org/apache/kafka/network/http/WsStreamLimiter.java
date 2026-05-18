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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bounds the number of simultaneously-open WebSocket subscriptions the HTTP bridge will serve.
 *
 * <p>Why this exists: each WebSocket subscription holds a Jetty thread for the connection lifetime,
 * recursively long-polls the broker (gated on a client-granted credit budget), and is bounded only
 * by the client closing. A hostile or runaway client can otherwise open arbitrarily many
 * subscriptions until the Jetty thread pool is exhausted — at which point even one-shot
 * produce/fetch requests stall behind it. This limiter is the admission gate that prevents that
 * failure mode for the WebSocket path, mirroring {@link SseStreamLimiter} for SSE.
 *
 * <p>Contract: callers acquire a {@link Token} before promoting the HTTP request to a WebSocket and
 * {@code close()} the token exactly once when the subscription terminates (client disconnect,
 * broker error, completion). The token is idempotent on close so cleanup paths that have multiple
 * terminal branches can call it from each without double-releasing.
 *
 * <p>The cap and the SSE cap are independent: an operator can configure them separately because
 * the two paths have different per-connection cost profiles (SSE writes plain bytes; WebSocket
 * frames are slightly heavier and require client-driven credit accounting on top).
 */
public final class WsStreamLimiter {

    private final int maxConcurrent;
    private final AtomicInteger inUse = new AtomicInteger();

    public WsStreamLimiter(int maxConcurrent) {
        if (maxConcurrent < 0) {
            throw new IllegalArgumentException("maxConcurrent must be non-negative, got " + maxConcurrent);
        }
        this.maxConcurrent = maxConcurrent;
    }

    /**
     * Attempt to reserve a slot. Returns a {@link Token} on success or {@code null} if the cap is reached.
     *
     * <p>The CAS loop handles concurrent acquisitions: two threads each see {@code inUse=99} with a cap of 100,
     * both will try to set 100 — exactly one wins, the other re-reads and either fits or gives up. There is no
     * blocking; this is a try-acquire only.
     */
    public Token tryAcquire() {
        for (;;) {
            int current = inUse.get();
            if (current >= maxConcurrent) {
                return null;
            }
            if (inUse.compareAndSet(current, current + 1)) {
                return new Token(this);
            }
        }
    }

    /** Visible for tests and for the {@code ActiveWsSubscriptions} JMX gauge. */
    public int inUse() {
        return inUse.get();
    }

    /** Visible for tests. */
    public int maxConcurrent() {
        return maxConcurrent;
    }

    void release() {
        // We rely on Token to guarantee one release per acquire; a defensive guard here would mask a Token-side bug.
        inUse.decrementAndGet();
    }

    /**
     * Holds a single acquired slot. {@link #close()} is idempotent — releasing twice is a no-op so the
     * subscription lifecycle (which can complete down multiple paths — close handler, error frame, broker
     * disconnect) can call it from each terminal branch without bookkeeping.
     */
    public static final class Token implements AutoCloseable {
        private final WsStreamLimiter limiter;
        private final AtomicBoolean released = new AtomicBoolean();

        Token(WsStreamLimiter limiter) {
            this.limiter = limiter;
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                limiter.release();
            }
        }
    }
}
