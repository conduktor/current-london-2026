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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link SseStreamer} covering the limiter-release invariant on the failure paths
 * that route through {@link SseStreamer}'s {@code tryWriteErrorFrame}.
 *
 * <p>The regression these tests guard against: Jetty 12's {@code HttpOutput.write/flush} can raise
 * an unchecked {@link IllegalStateException} (or {@code WritePendingException}) when the response
 * has transitioned to {@code CLOSING}/{@code CLOSED} between the priming write and the next write.
 * The error-frame writer originally caught only {@link IOException}, so a propagating ISE skipped
 * {@code closeStream()} and the {@link SseStreamLimiter} slot leaked permanently — once per error
 * frame the streamer was ever asked to emit on a stream whose underlying response had just been
 * closed by Jetty's side. The fix widens the three catches in {@code SseStreamer} to
 * {@code IOException | RuntimeException}; this test wires a {@link ServletOutputStream} whose first
 * write+flush succeeds (the priming bytes) but every subsequent write throws ISE — exactly the
 * shape Jetty's failure mode produces — and asserts the slot is released.
 */
class SseStreamerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SseStreamLimiter limiter;
    private SseStreamLimiter.Token token;

    @BeforeEach
    void setUp() {
        limiter = new SseStreamLimiter(4);
        token = limiter.tryAcquire();
        assertNotNull(token, "limiter token must be available for setup");
    }

    @Test
    void schedulingFailureReleasesSlotWhenErrorFrameWriteThrowsIllegalStateException() throws IOException {
        // Sequence:
        //   1. start() runs the priming write/flush — succeeds (FailAfterFlushOutputStream allows the first flush
        //      then arms itself).
        //   2. onPrimed.run() fires, scheduleNextFetch() dispatches.
        //   3. The submitter returns a failed future — handleFetchResult is invoked with throwable != null.
        //   4. handleFetchResult calls tryWriteErrorFrame which attempts out.write(EVENT_ERROR). The arm has
        //      tripped: the output stream raises IllegalStateException, matching Jetty's CLOSING/CLOSED behaviour.
        //   5. Without the catch widening, ISE escapes tryWriteErrorFrame → handleFetchResult propagates →
        //      handleSchedulingFailure runs via .exceptionally → it ALSO calls tryWriteErrorFrame which throws
        //      again → closeStream() never runs on either path → the limiter slot leaks.
        //   6. With the fix, the catch swallows ISE and closeStream() runs, releasing the slot.
        FailAfterFlushOutputStream out = new FailAfterFlushOutputStream();
        HttpServletResponse resp = Mockito.mock(HttpServletResponse.class);
        Mockito.when(resp.getOutputStream()).thenReturn(out);

        AsyncContext async = Mockito.mock(AsyncContext.class);
        Mockito.when(async.getResponse()).thenReturn(resp);

        FailingSubmitter submitter =
            new FailingSubmitter(new RuntimeException("broker dropped the request"));
        AtomicInteger primedCount = new AtomicInteger();

        FetchRequestParser.FetchCommand command =
            new FetchRequestParser.FetchCommand("t", 0, 0L, OptionalInt.empty(), false);

        SseStreamer.start(async, submitter, MAPPER, command, token, Runnable::run,
            ConcurrentHashMap.newKeySet(), primedCount::incrementAndGet);

        assertEquals(1, primedCount.get(),
            "priming write must have completed before the failure path runs — otherwise this test would be "
                + "exercising the start() catch, not the tryWriteErrorFrame catch");
        assertEquals(0, limiter.inUse(),
            "slot must be released even when the error-frame write raises IllegalStateException");
        Mockito.verify(async).complete();
    }

    /** Submitter that always returns a failed future — used to drive handleFetchResult into the error path. */
    private static final class FailingSubmitter implements RequestSubmitter {
        private final Throwable cause;

        FailingSubmitter(Throwable cause) {
            this.cause = cause;
        }

        @Override
        public CompletableFuture<ProduceResult> submitProduce(ProduceRequestParser.ProduceCommand command) {
            throw new UnsupportedOperationException("produce not used in SseStreamer tests");
        }

        @Override
        public CompletableFuture<FetchResult> submitFetch(FetchRequestParser.FetchCommand command) {
            return CompletableFuture.failedFuture(cause);
        }
    }

    /**
     * ServletOutputStream that lets the priming write+flush sequence succeed and then refuses every
     * subsequent write with {@link IllegalStateException} — the exact shape Jetty produces when its
     * internal state transitions to CLOSING/CLOSED between the priming bytes and the next write.
     */
    private static final class FailAfterFlushOutputStream extends ServletOutputStream {
        private final AtomicBoolean armed = new AtomicBoolean(false);

        @Override
        public void write(int b) {
            failIfArmed();
        }

        @Override
        public void write(byte[] b) {
            failIfArmed();
        }

        @Override
        public void write(byte[] b, int off, int len) {
            failIfArmed();
        }

        @Override
        public void flush() {
            // First flush completes (the priming flush). After that, the next write is armed to throw —
            // mirroring how Jetty surfaces the CLOSING/CLOSED transition on the very next call.
            armed.set(true);
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setWriteListener(WriteListener writeListener) {
            // SseStreamer uses blocking IO on this stream — no WriteListener interaction expected.
        }

        private void failIfArmed() {
            if (armed.get()) {
                throw new IllegalStateException("test: response CLOSING/CLOSED");
            }
        }

        boolean isArmed() {
            return armed.get();
        }
    }

    @Test
    void onPrimedRunnableThrowingAfterPrimingReleasesSlotAndCompletesAsync() throws IOException {
        // F-#5: the onPrimed Runnable is invoked AFTER (a) the streamer is fully constructed (token ownership has
        // transferred) and (b) the priming bytes have been flushed (response is committed), but BEFORE the first
        // scheduleNextFetch() dispatches. If onPrimed throws at that point, no code path inside the streamer would
        // ever invoke closeStream() — scheduleNextFetch is skipped, and the priming-time IOException/RuntimeException
        // catches sit above this line. Without the explicit try/catch in start(), the throw would propagate out to
        // the servlet caller while the AsyncContext stays half-open and the limiter slot leaks indefinitely.
        //
        // Today the production onPrimed is `metrics.recordSseStreamOpened()` which is contractually no-throw (it
        // gates on `closed` and only calls `meter.mark()` otherwise), but the Runnable type is not formally
        // declared no-throw — a future maintainer wiring additional bookkeeping into the callback (logging,
        // observability hooks, audit-trail emission) could trigger this leak. Pin the resource-safety invariant.
        UncloseableOutputStream out = new UncloseableOutputStream();
        HttpServletResponse resp = Mockito.mock(HttpServletResponse.class);
        Mockito.when(resp.getOutputStream()).thenReturn(out);

        AsyncContext async = Mockito.mock(AsyncContext.class);
        Mockito.when(async.getResponse()).thenReturn(resp);

        // The submitter would never be reached because scheduleNextFetch is skipped, but wire one anyway so a
        // future refactor that does reach the submitter would surface a hard failure rather than NPE inside the
        // streamer.
        FailingSubmitter submitter =
            new FailingSubmitter(new RuntimeException("submitter must not be reached when onPrimed throws"));
        FetchRequestParser.FetchCommand command =
            new FetchRequestParser.FetchCommand("t", 0, 0L, OptionalInt.empty(), false);

        // Runnable that throws AFTER the priming flush has committed the response. RuntimeException — not Error —
        // because the production onPrimed contract is at most RuntimeException; Errors are JVM-fatal and not the
        // class of failure we're hardening against.
        RuntimeException injected = new RuntimeException("simulated onPrimed failure");
        Runnable onPrimed = () -> {
            throw injected;
        };

        SseStreamer.start(async, submitter, MAPPER, command, token, Runnable::run,
            ConcurrentHashMap.newKeySet(), onPrimed);

        assertTrue(out.primingWritten(),
            "priming bytes must have been written — the test exercises the post-priming throw path, not the "
                + "priming-failure catch above it");
        assertEquals(0, limiter.inUse(),
            "limiter slot must be released when onPrimed throws after the priming flush");
        Mockito.verify(async).complete();
    }

    /**
     * Trivial ServletOutputStream that captures whether ANY bytes were written before the priming flush. Unlike
     * FailAfterFlushOutputStream, this stream never throws — its job is to let the priming write+flush succeed so
     * we can then drive the onPrimed-throw path without interference.
     */
    private static final class UncloseableOutputStream extends ServletOutputStream {
        private boolean primingWritten = false;

        @Override
        public void write(int b) {
            primingWritten = true;
        }

        @Override
        public void write(byte[] b) {
            primingWritten = true;
        }

        @Override
        public void write(byte[] b, int off, int len) {
            primingWritten = true;
        }

        @Override
        public void flush() {
            // Nothing to do — the test only needs to observe that priming bytes were written.
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setWriteListener(WriteListener writeListener) {
            // Blocking IO — no WriteListener interaction.
        }

        boolean primingWritten() {
            return primingWritten;
        }
    }

    @Test
    void failAfterFlushHarnessBehavesAsAdvertised() {
        // Sanity check on the harness itself: the priming write+flush must succeed and arm the stream;
        // the next write must throw IllegalStateException. If this drifts the regression test silently
        // stops covering the real bug.
        FailAfterFlushOutputStream out = new FailAfterFlushOutputStream();
        out.write(new byte[] {1, 2, 3});
        out.flush();
        assertTrue(out.isArmed(), "harness must arm after the priming flush");
        try {
            out.write(new byte[] {4});
            throw new AssertionError("harness must throw IllegalStateException after arming");
        } catch (IllegalStateException expected) {
            // Expected.
        }
    }
}
