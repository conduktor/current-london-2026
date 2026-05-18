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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/**
 * Encodes / decodes the opaque cursor used in HATEOAS links. The clear-text form is {@code topic|partition|offset};
 * the wire form is the base64url-encoded UTF-8 bytes of that string. The opacity is the point — clients must follow
 * the cursor we emit rather than building URLs from query parameters, which keeps the server free to change the layout.
 */
public final class CursorCodec {

    private static final char SEPARATOR = '|';

    private CursorCodec() {
    }

    public static String encode(String topic, int partition, long offset) {
        Objects.requireNonNull(topic, "topic must not be null");
        String raw = topic + SEPARATOR + partition + SEPARATOR + offset;
        return Base64.getUrlEncoder().withoutPadding().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    public static Cursor decode(String cursor) {
        Objects.requireNonNull(cursor, "cursor must not be null");
        byte[] decoded;
        try {
            decoded = Base64.getUrlDecoder().decode(cursor);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Cursor is not valid base64url", e);
        }

        String raw = new String(decoded, StandardCharsets.UTF_8);
        int firstSep = raw.indexOf(SEPARATOR);
        int lastSep = raw.lastIndexOf(SEPARATOR);
        if (firstSep <= 0 || lastSep <= firstSep) {
            throw new IllegalArgumentException("Cursor is missing required fields");
        }

        String topic = raw.substring(0, firstSep);
        try {
            int partition = Integer.parseInt(raw.substring(firstSep + 1, lastSep));
            long offset = Long.parseLong(raw.substring(lastSep + 1));
            // The explicit ?partition=&offset= path rejects negatives with a 400. A tampered or
            // hand-rolled cursor that decodes to negative values must take the same path — otherwise
            // negative partition / offset would leak into the broker's FetchRequest construction
            // and surface as an opaque server-side error rather than a clean client-facing 400.
            if (partition < 0) {
                throw new IllegalArgumentException("Cursor partition must be non-negative");
            }
            if (offset < 0L) {
                throw new IllegalArgumentException("Cursor offset must be non-negative");
            }
            return new Cursor(topic, partition, offset);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Cursor numeric fields are malformed", e);
        }
    }

    public static final class Cursor {
        private final String topic;
        private final int partition;
        private final long offset;

        public Cursor(String topic, int partition, long offset) {
            this.topic = Objects.requireNonNull(topic);
            this.partition = partition;
            this.offset = offset;
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Cursor)) return false;
            Cursor c = (Cursor) o;
            return partition == c.partition && offset == c.offset && topic.equals(c.topic);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, partition, offset);
        }

        @Override
        public String toString() {
            return "Cursor{topic='" + topic + "', partition=" + partition + ", offset=" + offset + '}';
        }
    }
}
