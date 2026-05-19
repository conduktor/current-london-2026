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

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * Wraps a request body {@link InputStream} with a strict byte cap. Throws {@link BodyTooLargeException} as soon as a
 * read takes the cumulative byte count past {@link #limit}; subsequent calls also throw rather than letting partial
 * reads sneak through.
 *
 * <p>Why this exists: Jetty's default {@link jakarta.servlet.http.HttpServletRequest#getInputStream()} has no built-in
 * cap, and Jackson's {@code readTree} builds a {@link com.fasterxml.jackson.databind.JsonNode} tree that grows linearly
 * with the input size — each token in the wire bytes allocates one or more node objects in the tree. Jackson itself
 * does NOT pre-buffer the input ({@code UTF8StreamJsonParser._loadMore} reads through an ~8 KiB recycled byte buffer)
 * and {@link BridgeJsonMappers#MAX_DOCUMENT_LENGTH} would catch the byte count in production, but the cap here is the
 * upstream defence: a 1 GiB POST against any future code path that used a non-hardened mapper would grow the parsed
 * tree until OOM, and the cap fails such a request with an identifiable {@link BodyTooLargeException} (mapped to 413)
 * rather than an unstructured {@code OutOfMemoryError}. The bridge runs inside the broker; a misbehaving HTTP client
 * must not be able to take the binary protocol down with it.
 *
 * <p>The cap is INCLUSIVE — exactly {@code limit} bytes read are fine; byte {@code limit + 1} fails. We pre-validate
 * a negative limit at construction so callers can't pass {@code -1} and then be surprised by a runtime exception
 * during the first read.
 */
final class BoundedRequestBody extends InputStream {

    private final InputStream delegate;
    private final long limit;
    private long readSoFar;

    BoundedRequestBody(InputStream delegate, long limit) {
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be non-negative, got " + limit);
        }
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.limit = limit;
    }

    @Override
    public int read() throws IOException {
        int b = delegate.read();
        if (b == -1) {
            return -1;
        }
        if (++readSoFar > limit) {
            throw new BodyTooLargeException(limit);
        }
        return b;
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        int n = delegate.read(buf, off, len);
        if (n <= 0) {
            return n;
        }
        readSoFar += n;
        if (readSoFar > limit) {
            throw new BodyTooLargeException(limit);
        }
        return n;
    }

    @Override
    public int available() throws IOException {
        return delegate.available();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    /** Visible for tests. */
    long limit() {
        return limit;
    }
}
