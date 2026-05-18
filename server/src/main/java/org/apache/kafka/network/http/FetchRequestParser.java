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

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Translates the {@code GET /v1/topics/{topic}/records} query string into a typed {@link FetchCommand}.
 *
 * <p>Two equivalent ways to specify what to fetch:
 * <ul>
 *   <li>{@code ?partition=X&offset=Y} — the explicit form, used for first-call discovery.</li>
 *   <li>{@code ?cursor=BLOB} — the opaque cursor form, used when following a {@code _links} pointer. The cursor must
 *       reference the same topic in the path; otherwise we reject the request rather than silently switch topics.</li>
 * </ul>
 * The two forms are mutually exclusive — supplying both is a client bug.
 *
 * <p>Optional {@code max_bytes} caps the fetch budget; the broker still applies its own ceiling.
 */
public final class FetchRequestParser {

    private FetchRequestParser() {
    }

    public static FetchCommand parse(String topic, QueryParams query) {
        if (topic == null || topic.trim().isEmpty()) {
            throw new ProduceRequestParser.BadRequestException("topic must not be empty");
        }

        Optional<String> cursor = query.get("cursor");
        Optional<String> partition = query.get("partition");
        Optional<String> offset = query.get("offset");
        Optional<String> from = query.get("from");

        int partitionValue;
        long offsetValue;
        if (cursor.isPresent()) {
            if (partition.isPresent() || offset.isPresent() || from.isPresent()) {
                throw new ProduceRequestParser.BadRequestException(
                    "cursor is mutually exclusive with partition / offset / from");
            }
            CursorCodec.Cursor c = decodeCursor(cursor.get());
            if (!c.topic().equals(topic)) {
                throw new ProduceRequestParser.BadRequestException(
                    "cursor topic does not match request path topic");
            }
            partitionValue = c.partition();
            offsetValue = c.offset();
        } else {
            partitionValue = readPartition(partition);
            offsetValue = readStartOffset(offset, from);
        }

        OptionalInt maxBytes = readMaxBytes(query.get("max_bytes"));
        return new FetchCommand(topic, partitionValue, offsetValue, maxBytes);
    }

    /**
     * Resolves the starting offset. Either an explicit numeric {@code offset}, or {@code from=earliest} (offset 0).
     * The two are mutually exclusive: a client that asks for both is being ambiguous, not redundant.
     *
     * <p>{@code from=latest} is intentionally not supported yet — it requires a round-trip to discover the current
     * high watermark before the first fetch, which the v1 single-shot fetch path doesn't need. SSE-mode clients that
     * want "only new records" can omit historic catch-up by passing the high watermark as an explicit offset.
     */
    private static long readStartOffset(Optional<String> offset, Optional<String> from) {
        if (offset.isPresent() && from.isPresent()) {
            throw new ProduceRequestParser.BadRequestException("offset and from are mutually exclusive");
        }
        if (from.isPresent()) {
            String value = from.get();
            if ("earliest".equalsIgnoreCase(value)) {
                return 0L;
            }
            throw new ProduceRequestParser.BadRequestException(
                "from must be 'earliest' (got '" + value + "')");
        }
        return readOffset(offset);
    }

    private static CursorCodec.Cursor decodeCursor(String cursor) {
        try {
            return CursorCodec.decode(cursor);
        } catch (IllegalArgumentException e) {
            throw new ProduceRequestParser.BadRequestException("invalid cursor: " + e.getMessage(), e);
        }
    }

    private static int readPartition(Optional<String> raw) {
        if (raw.isEmpty()) {
            throw new ProduceRequestParser.BadRequestException("missing query parameter: partition (or cursor)");
        }
        int p;
        try {
            p = Integer.parseInt(raw.get());
        } catch (NumberFormatException e) {
            throw new ProduceRequestParser.BadRequestException("partition must be an integer", e);
        }
        if (p < 0) {
            throw new ProduceRequestParser.BadRequestException("partition must be non-negative");
        }
        return p;
    }

    private static long readOffset(Optional<String> raw) {
        if (raw.isEmpty()) {
            throw new ProduceRequestParser.BadRequestException("missing query parameter: offset");
        }
        long o;
        try {
            o = Long.parseLong(raw.get());
        } catch (NumberFormatException e) {
            throw new ProduceRequestParser.BadRequestException("offset must be a long", e);
        }
        if (o < 0) {
            throw new ProduceRequestParser.BadRequestException("offset must be non-negative");
        }
        return o;
    }

    private static OptionalInt readMaxBytes(Optional<String> raw) {
        if (raw.isEmpty()) {
            return OptionalInt.empty();
        }
        int m;
        try {
            m = Integer.parseInt(raw.get());
        } catch (NumberFormatException e) {
            throw new ProduceRequestParser.BadRequestException("max_bytes must be an integer", e);
        }
        if (m <= 0) {
            throw new ProduceRequestParser.BadRequestException("max_bytes must be positive");
        }
        return OptionalInt.of(m);
    }

    /** A validated fetch request, ready for the bridge orchestrator to submit. */
    public static final class FetchCommand {
        private final String topic;
        private final int partition;
        private final long offset;
        private final OptionalInt maxBytes;

        public FetchCommand(String topic, int partition, long offset, OptionalInt maxBytes) {
            this.topic = Objects.requireNonNull(topic, "topic must not be null");
            this.partition = partition;
            this.offset = offset;
            this.maxBytes = Objects.requireNonNull(maxBytes, "maxBytes must not be null");
        }

        public String topic() {
            return topic;
        }

        public int partition() {
            return partition;
        }

        public long offset() {
            return offset;
        }

        public OptionalInt maxBytes() {
            return maxBytes;
        }
    }
}
