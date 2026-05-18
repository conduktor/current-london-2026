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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.nio.charset.CodingErrorAction.REPORT;

/**
 * Builders and concrete implementations of {@link RecordContext}.
 *
 * The default implementation lazily JSON-parses the body and lazily UTF-8-decodes header values
 * on first access, and caches the result. Malformed payloads are caught and converted to
 * {@code null} (body unusable — skip record) or {@link Optional#empty()} (path missing — value
 * is "unknown"). No exception escapes a context method.
 */
public final class RecordContexts {

    private RecordContexts() {}

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private byte[] key;
        private byte[] body;
        private final List<Header> headers = new ArrayList<>();
        private long offset;
        private int partition;
        private long timestamp;

        public Builder key(byte[] key) {
            this.key = key;
            return this;
        }

        public Builder body(byte[] body) {
            this.body = body;
            return this;
        }

        public Builder header(String name, byte[] value) {
            this.headers.add(new Header(name, value));
            return this;
        }

        public Builder offset(long offset) {
            this.offset = offset;
            return this;
        }

        public Builder partition(int partition) {
            this.partition = partition;
            return this;
        }

        public Builder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public RecordContext build() {
            return new DefaultRecordContext(this, PredicateLimits.defaults());
        }

        public RecordContext build(PredicateLimits limits) {
            return new DefaultRecordContext(this, limits);
        }
    }

    private static final class Header {
        final String name;
        final byte[] value;
        Header(String name, byte[] value) {
            this.name = name;
            this.value = value;
        }
    }

    private static final JsonFactory JSON_FACTORY;
    static {
        JSON_FACTORY = new JsonFactory();
        // Allow deep JSON at the parser level; the predicate enforces its own depth cap during
        // navigation so we can distinguish "user limit exceeded" from "Jackson refused".
        JSON_FACTORY.setStreamReadConstraints(StreamReadConstraints.builder()
                .maxNestingDepth(1024)
                .build());
    }

    private static final class DefaultRecordContext implements RecordContext {
        private final byte[] key;
        private final byte[] body;
        private final Map<String, byte[]> headers;
        private final long offset;
        private final int partition;
        private final long timestamp;
        private final PredicateLimits limits;

        private Optional<String> cachedKeyString;
        private final Map<String, Optional<String>> cachedHeaderStrings = new HashMap<>();
        // Map of accessor-path key → resolved scalar (or null for missing). When we discover the
        // body is unusable, the flag below short-circuits all future accesses.
        private final Map<String, Object> cachedBodyPaths = new HashMap<>();
        private boolean bodyUnusable;

        DefaultRecordContext(Builder b, PredicateLimits limits) {
            this.key = b.key;
            this.body = b.body;
            this.offset = b.offset;
            this.partition = b.partition;
            this.timestamp = b.timestamp;
            this.limits = limits;
            this.headers = new HashMap<>(b.headers.size());
            for (Header h : b.headers) {
                // Last write wins on duplicate names — mirrors Headers.lastHeader semantics.
                this.headers.put(h.name, h.value);
            }
        }

        @Override public byte[] rawKey() {
            return key;
        }

        @Override public byte[] rawBody() {
            return body;
        }

        @Override public long offset() {
            return offset;
        }

        @Override public int partition() {
            return partition;
        }

        @Override public long timestamp() {
            return timestamp;
        }

        @Override
        public Optional<String> keyAsString() {
            if (cachedKeyString != null) {
                return cachedKeyString;
            }
            cachedKeyString = decodeUtf8Strict(key);
            return cachedKeyString;
        }

        @Override
        public Optional<String> header(String name) {
            if (cachedHeaderStrings.containsKey(name)) {
                return cachedHeaderStrings.get(name);
            }
            byte[] raw = headers.get(name);
            Optional<String> decoded = decodeUtf8Strict(raw);
            cachedHeaderStrings.put(name, decoded);
            return decoded;
        }

        @Override
        public Object bodyAt(List<String> path) {
            if (body == null) {
                return BODY_UNUSABLE;
            }
            if (bodyUnusable) {
                return BODY_UNUSABLE;
            }
            if (body.length > limits.maxBodyBytes) {
                bodyUnusable = true;
                return BODY_UNUSABLE;
            }
            String cacheKey = pathCacheKey(path);
            if (cachedBodyPaths.containsKey(cacheKey)) {
                return cachedBodyPaths.get(cacheKey);
            }
            Object result = parseAndNavigate(body, path, limits.maxJsonDepth);
            if (result == BODY_UNUSABLE) {
                bodyUnusable = true;
                return BODY_UNUSABLE;
            }
            cachedBodyPaths.put(cacheKey, result);
            return result;
        }

        /** NUL-delimited cache key. JSON field names containing NUL bytes aren't reachable via
         *  our dotted syntax, and string-literal accessors carrying NUL are rejected by the
         *  lexer through standard string-handling, so collisions are not possible. */
        private static String pathCacheKey(List<String> path) {
            StringBuilder sb = new StringBuilder();
            for (String p : path) {
                sb.append('\0').append(p);
            }
            return sb.toString();
        }
    }

    /**
     * Streams the JSON in {@code body} and walks {@code path}, returning:
     *   - {@link RecordContext#BODY_UNUSABLE}  on parse error or over-limit depth.
     *   - {@code null}                         if the path doesn't resolve to a scalar
     *                                          (missing, JSON null, container, etc.).
     *   - {@code Long} / {@code Double} / {@code String} / {@code Boolean} for a scalar leaf.
     *
     * <p>Duplicate-key handling: at the LEAF level (the last path segment), a duplicate
     * occurrence of the wanted key inside the same object is treated as
     * {@link RecordContext#BODY_UNUSABLE}. Predicates are an access-control boundary;
     * RFC 8259 says duplicate-key behaviour is "undefined" and parsers diverge (Jackson is
     * last-wins, this navigator was historically first-wins). An adversary who can write to
     * the backing topic could craft {@code {"region":"EU","region":"US"}} to bypass a predicate
     * like {@code body.region == "US"} that some-but-not-all parsers would have read as "EU".
     * Refusing the record at the leaf shuts that bypass down regardless of which value any
     * downstream parser would have picked.
     *
     * <p>At INTERMEDIATE levels (any path segment that descends into a sub-object), the
     * navigator remains first-wins. Detecting duplicates there without buffering whole subtrees
     * is significantly more work, and intermediate-compound-duplicates require an exotic JSON
     * shape (an object with two keys of the same name both pointing at sub-objects) that
     * realistic producers — including Jackson's serializer — do not emit. We document this as
     * a known limitation rather than guarding it: predicates whose path traverses through an
     * intermediate level with duplicate sibling keys see the FIRST occurrence.
     */
    private static Object parseAndNavigate(byte[] body, List<String> path, int maxDepth) {
        try (JsonParser p = JSON_FACTORY.createParser(new ByteArrayInputStream(body))) {
            JsonToken t = p.nextToken();
            if (t == null) {
                return null;
            }
            int depth = 0;
            int pi = 0;
            Object capturedLeaf = null;
            while (pi < path.size()) {
                if (t != JsonToken.START_OBJECT) {
                    return null;
                }
                depth++;
                if (depth > maxDepth) {
                    return RecordContext.BODY_UNUSABLE;
                }
                boolean isLastStep = pi == path.size() - 1;
                NavStep step = findField(p, path.get(pi), isLastStep);
                if (step == NavStep.MALFORMED) {
                    return RecordContext.BODY_UNUSABLE;
                }
                if (step.token == null) {
                    return null; // field not found
                }
                t = step.token;
                if (step.capturedScalar != ScalarSlot.UNSET) {
                    // Leaf was pre-captured by findField; the parser has scanned past it
                    // to detect duplicates, so don't try to re-read it.
                    capturedLeaf = step.capturedScalar;
                }
                pi++;
            }
            if (capturedLeaf != ScalarSlot.UNSET && capturedLeaf != null) {
                return capturedLeaf;
            }
            // Leaf wasn't pre-captured (e.g. compound at leaf position, or intermediate-only paths
            // in the {@code path.isEmpty()} case where {@code t} is still the root). Fall back to
            // reading at the current parser position.
            return extractLeaf(p, t);
        } catch (IOException e) {
            return RecordContext.BODY_UNUSABLE;
        }
    }

    /** Sentinel marker for "no captured scalar". {@code null} is a valid captured value (JSON null
     *  / missing scalar), so we need a distinct sentinel object. */
    private static final class ScalarSlot {
        static final Object UNSET = new Object();
        private ScalarSlot() {}
    }

    /**
     * Scans the current object for {@code wanted}.
     *
     * <p>When {@code isLastStep} is true, the navigator scans the ENTIRE current object — even
     * after finding a match — to detect duplicate occurrences of {@code wanted}. The matched
     * scalar value is captured eagerly because the parser advances past it during the duplicate
     * scan; the captured value rides back on {@link NavStep#capturedScalar} so
     * {@link #parseAndNavigate} can return it without re-reading the parser.
     *
     * <p>When {@code isLastStep} is false (i.e. there are more path segments to descend), the
     * navigator returns on first match so the outer loop can descend into the matched value's
     * sub-object. Duplicate detection in this case would require buffering whole sub-object
     * trees, which is significantly more work and only matters in adversarial JSON shapes —
     * see the {@link #parseAndNavigate} javadoc.
     */
    private static NavStep findField(JsonParser p, String wanted, boolean isLastStep) throws IOException {
        JsonToken t;
        JsonToken matchedToken = null;
        Object capturedScalar = ScalarSlot.UNSET;
        while ((t = p.nextToken()) != JsonToken.END_OBJECT && t != null) {
            if (t != JsonToken.FIELD_NAME) {
                return NavStep.MALFORMED;
            }
            String field = p.currentName();
            JsonToken valueToken = p.nextToken();
            if (field.equals(wanted)) {
                if (matchedToken != null) {
                    // Duplicate at this level — refuse the record.
                    return NavStep.MALFORMED;
                }
                matchedToken = valueToken;
                if (!isLastStep) {
                    // Intermediate level: return immediately so the outer loop descends into
                    // the matched value. First-wins for intermediate paths; see javadoc.
                    return new NavStep(valueToken, ScalarSlot.UNSET);
                }
                // Leaf level: capture the scalar value before the parser moves on.
                capturedScalar = captureLeaf(p, valueToken);
                if (valueToken == JsonToken.START_OBJECT || valueToken == JsonToken.START_ARRAY) {
                    // Compound at leaf position; captured value is null (extractLeaf semantics).
                    // We still must skip children so the duplicate-scan stays at the correct level.
                    p.skipChildren();
                }
                continue;
            }
            if (valueToken == JsonToken.START_OBJECT || valueToken == JsonToken.START_ARRAY) {
                p.skipChildren();
            }
        }
        if (matchedToken == null) {
            return NavStep.NOT_FOUND;
        }
        return new NavStep(matchedToken, capturedScalar);
    }

    /** Same shape as {@link #extractLeaf} but called by {@link #findField} during the
     *  duplicate-aware leaf scan. Reads at the parser's current position (which is at the value
     *  token just consumed by findField). */
    private static Object captureLeaf(JsonParser p, JsonToken t) throws IOException {
        return extractLeaf(p, t);
    }

    /** Tiny tuple type used by {@link #findField}. */
    private static final class NavStep {
        static final NavStep NOT_FOUND = new NavStep(null, ScalarSlot.UNSET);
        static final NavStep MALFORMED = new NavStep(null, ScalarSlot.UNSET);
        final JsonToken token;
        /** Pre-captured leaf value, or {@link ScalarSlot#UNSET} if no leaf was captured.
         *  {@code null} is a valid captured value, so we distinguish via the UNSET sentinel. */
        final Object capturedScalar;
        NavStep(JsonToken token, Object capturedScalar) {
            this.token = token;
            this.capturedScalar = capturedScalar;
        }
    }

    private static Object extractLeaf(JsonParser p, JsonToken t) throws IOException {
        switch (t) {
            case VALUE_STRING:
                return p.getValueAsString();
            case VALUE_NUMBER_INT:
                return extractLongOrNull(p);
            case VALUE_NUMBER_FLOAT:
                return p.getDoubleValue();
            case VALUE_TRUE:
                return Boolean.TRUE;
            case VALUE_FALSE:
                return Boolean.FALSE;
            case VALUE_NULL:
            case START_OBJECT:
            case START_ARRAY:
            default:
                return null;
        }
    }

    private static Object extractLongOrNull(JsonParser p) {
        try {
            return p.getLongValue();
        } catch (IOException overflow) {
            // Integer that doesn't fit in a long — silently treat as missing.
            return null;
        }
    }

    private static Optional<String> decodeUtf8Strict(byte[] bytes) {
        if (bytes == null) return Optional.empty();
        try {
            return Optional.of(StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(REPORT)
                    .onUnmappableCharacter(REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString());
        } catch (CharacterCodingException e) {
            return Optional.empty();
        }
    }
}
