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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BoundedRequestBodyTest {

    @Test
    void readsBelowLimitPassThroughUnchanged() throws IOException {
        // Right at the limit must succeed — the cap is INCLUSIVE on the byte count we are willing to accept. Off-by-one
        // here would either let an oversized body through or reject a perfectly-sized one; both are user-visible.
        byte[] payload = bytes(1024);
        try (BoundedRequestBody bounded = new BoundedRequestBody(new ByteArrayInputStream(payload), 1024)) {
            byte[] read = bounded.readAllBytes();
            assertArrayEquals(payload, read);
        }
    }

    @Test
    void readOneByteOverLimitThrows() {
        // A single byte past the cap is the canonical failure mode for a chunked upload that doesn't declare
        // Content-Length up-front — we have to be strict so the catcher knows to emit 413.
        byte[] payload = bytes(1025);
        BoundedRequestBody bounded = new BoundedRequestBody(new ByteArrayInputStream(payload), 1024);
        BodyTooLargeException ex = assertThrows(BodyTooLargeException.class, bounded::readAllBytes);
        assertEquals(1024L, ex.limit());
    }

    @Test
    void singleByteReadsAreCounted() throws IOException {
        // Jackson reads occasionally drop to single-byte paths (e.g. peeking past a value boundary). The counter must
        // handle both bulk and single-byte reads with identical semantics — a one-byte read at the limit boundary is
        // the same as a bulk read that crosses it.
        byte[] payload = bytes(3);
        BoundedRequestBody bounded = new BoundedRequestBody(new ByteArrayInputStream(payload), 2);
        assertEquals(payload[0] & 0xFF, bounded.read());
        assertEquals(payload[1] & 0xFF, bounded.read());
        assertThrows(BodyTooLargeException.class, bounded::read);
    }

    @Test
    void zeroLengthLimitRejectsAnyBody() {
        // Operator who sets the limit to zero is asking for the bridge to refuse every produce — a degenerate but valid
        // configuration (e.g. disabling produce via the bridge entirely while leaving fetch open). One byte in must
        // fail; an empty body still reads as a no-op (verified by the next test).
        BoundedRequestBody bounded = new BoundedRequestBody(new ByteArrayInputStream(new byte[] {1}), 0);
        assertThrows(BodyTooLargeException.class, bounded::read);
    }

    @Test
    void emptyBodyAtZeroLengthLimitIsOk() throws IOException {
        // A zero-byte body against a zero-byte cap is fine: read() returns -1 (EOF) without ever incrementing the
        // counter. The error case is "wrote one byte THEN saw -1" — not "saw -1 immediately".
        BoundedRequestBody bounded = new BoundedRequestBody(new ByteArrayInputStream(new byte[0]), 0);
        assertEquals(-1, bounded.read());
    }

    @Test
    void closeDelegates() throws IOException {
        // The bounded wrapper owns nothing of its own; closing it must close the delegate so the Jetty container can
        // reclaim its byte buffer. We assert via a delegate that tracks its closed state.
        TrackingStream delegate = new TrackingStream(new byte[] {1, 2, 3});
        BoundedRequestBody bounded = new BoundedRequestBody(delegate, 10);
        bounded.close();
        assertTrue(delegate.closed);
    }

    @Test
    void availableDelegates() throws IOException {
        // available() is consulted by some parsers to size their internal buffers; if the wrapper masked it, those
        // parsers would over- or under-allocate. Delegating is correct since available() is an estimate only.
        TrackingStream delegate = new TrackingStream(new byte[] {1, 2, 3, 4, 5});
        try (BoundedRequestBody bounded = new BoundedRequestBody(delegate, 10)) {
            assertEquals(5, bounded.available());
        }
    }

    @Test
    void rejectsNegativeLimit() {
        // A negative cap is a programming error in the wiring layer — we fail fast at construction rather than letting
        // a confusing "limit -1" propagate into a runtime exception during the first read.
        assertThrows(IllegalArgumentException.class,
            () -> new BoundedRequestBody(new ByteArrayInputStream(new byte[0]), -1));
    }

    @Test
    void rejectsNullDelegate() {
        assertThrows(NullPointerException.class, () -> new BoundedRequestBody(null, 1024));
    }

    private static byte[] bytes(int n) {
        byte[] out = new byte[n];
        for (int i = 0; i < n; i++) {
            out[i] = (byte) ('a' + (i % 26));
        }
        return out;
    }

    private static final class TrackingStream extends InputStream {
        private final byte[] data;
        private int pos = 0;
        boolean closed = false;
        TrackingStream(byte[] data) {
            this.data = data;
        }
        @Override
        public int read() {
            return pos >= data.length ? -1 : data[pos++] & 0xFF;
        }
        @Override
        public int available() {
            return data.length - pos;
        }
        @Override
        public void close() {
            closed = true;
        }
    }
}
