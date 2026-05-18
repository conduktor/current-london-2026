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
import com.fasterxml.jackson.databind.util.TokenBuffer;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
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
        private ByteBuffer key;
        private ByteBuffer body;
        private final List<Header> headers = new ArrayList<>();
        private long offset;
        private int partition;
        private long timestamp;

        public Builder key(byte[] key) {
            this.key = key == null ? null : ByteBuffer.wrap(key);
            return this;
        }

        public Builder body(byte[] body) {
            this.body = body == null ? null : ByteBuffer.wrap(body);
            return this;
        }

        /** Zero-copy entry point used by the fetch hot path — stores the ByteBuffer reference
         *  directly so we don't copy the underlying record bytes per record. The buffer's
         *  position/limit are NOT mutated: every consumer (raw-byte materialisation, JSON parse)
         *  reads via index or via a duplicate. The caller must keep the buffer valid for the
         *  lifetime of the resulting RecordContext (typically one shouldRetainRecord call). */
        public Builder keyBuffer(ByteBuffer key) {
            this.key = key;
            return this;
        }

        public Builder bodyBuffer(ByteBuffer body) {
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
        // Bytes are held as ByteBuffer references so the fetch hot path can pass through the
        // underlying record buffer without copying. byte[] materialisation is lazy — most
        // predicates touch one or two derived values and never need the raw bytes at all.
        private final ByteBuffer keyBuf;
        private final ByteBuffer bodyBuf;
        private final Map<String, byte[]> headers;
        private final long offset;
        private final int partition;
        private final long timestamp;
        private final PredicateLimits limits;

        private byte[] cachedRawKey;
        private boolean rawKeyMaterialised;
        private byte[] cachedRawBody;
        private boolean rawBodyMaterialised;
        private Optional<String> cachedKeyString;
        private final Map<String, Optional<String>> cachedHeaderStrings = new HashMap<>();
        // Map of accessor-path key → resolved scalar (or null for missing). When we discover the
        // body is unusable, the flag below short-circuits all future accesses.
        private final Map<String, Object> cachedBodyPaths = new HashMap<>();
        private boolean bodyUnusable;

        DefaultRecordContext(Builder b, PredicateLimits limits) {
            this.keyBuf = b.key;
            this.bodyBuf = b.body;
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
            if (!rawKeyMaterialised) {
                cachedRawKey = bufferToArray(keyBuf);
                rawKeyMaterialised = true;
            }
            return cachedRawKey;
        }

        @Override public byte[] rawBody() {
            if (!rawBodyMaterialised) {
                cachedRawBody = bufferToArray(bodyBuf);
                rawBodyMaterialised = true;
            }
            return cachedRawBody;
        }

        @Override public byte[] rawHeader(String name) {
            return headers.get(name);
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
            cachedKeyString = decodeUtf8Strict(keyBuf);
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
            if (bodyBuf == null) {
                // Tombstones (record with null value) reach this branch on a compacted backing
                // topic. We treat "no body" as BODY_UNUSABLE — same outcome as malformed JSON —
                // so any body-accessing predicate skips the record. The alternative would be to
                // surface body.X as null and let `body.region != "blocked"` retain tombstones,
                // but that would silently pass null-body records through negated predicates
                // (the same anti-pattern PROMPT.md line 46 calls out for invalid-UTF-8 headers
                // and out-of-long-range integers). Predicates that intentionally want to retain
                // tombstones can do so by not referencing body at all (e.g. `key == 'foo'`).
                return BODY_UNUSABLE;
            }
            if (bodyUnusable) {
                return BODY_UNUSABLE;
            }
            if (bodyBuf.remaining() > limits.maxBodyBytes) {
                bodyUnusable = true;
                return BODY_UNUSABLE;
            }
            String cacheKey = pathCacheKey(path);
            if (cachedBodyPaths.containsKey(cacheKey)) {
                return cachedBodyPaths.get(cacheKey);
            }
            Object result = parseAndNavigate(bodyBuf, path, limits.maxJsonDepth, limits.maxScalarStringChars);
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
     * same duplicate-detection guard applies. When {@code findField} matches an intermediate
     * key it captures the matched value's subtree into a {@link TokenBuffer} and continues
     * scanning the rest of the parent object; a second occurrence of the same key produces
     * {@link RecordContext#BODY_UNUSABLE}. The captured subtree is then replayed via
     * {@link TokenBuffer#asParser()} so the outer loop can descend into it. This costs one
     * subtree-sized buffer at each intermediate level (bounded by
     * {@link PredicateLimits#maxBodyBytes}), traded for an access-control guarantee:
     * predicates whose path traverses an intermediate level with duplicate sibling keys
     * always refuse the record rather than silently picking one occurrence.
     */
    private static Object parseAndNavigate(ByteBuffer body, List<String> path, int maxDepth, int maxScalarStringChars) {
        // Replay parsers (one per intermediate-level descent that captured a subtree into a
        // TokenBuffer) must be closed alongside the root parser. Tracked in a list because we
        // only know how many we need as we descend.
        List<JsonParser> open = new ArrayList<>(1);
        try {
            return navigate(body, path, maxDepth, maxScalarStringChars, open);
        } catch (IOException e) {
            return RecordContext.BODY_UNUSABLE;
        } finally {
            closeAll(open);
        }
    }

    /** The descent body of {@link #parseAndNavigate}. Pulled out so the outer method holds only
     *  the resource-management try/catch/finally — the descent logic alone keeps NPath complexity
     *  inside the project's checkstyle threshold. */
    private static Object navigate(ByteBuffer body, List<String> path, int maxDepth, int maxScalarStringChars,
                                   List<JsonParser> open) throws IOException {
        JsonParser current = parserFor(body);
        open.add(current);
        JsonToken t = current.nextToken();
        if (t == null) {
            return null;
        }
        Object capturedLeaf = null;
        for (int pi = 0; pi < path.size(); pi++) {
            if (t != JsonToken.START_OBJECT) {
                return null;
            }
            if (pi + 1 > maxDepth) {
                return RecordContext.BODY_UNUSABLE;
            }
            NavStep step = findField(current, path.get(pi), pi == path.size() - 1, maxScalarStringChars);
            if (step == NavStep.MALFORMED) {
                return RecordContext.BODY_UNUSABLE;
            }
            if (step.token == null) {
                return null; // field not found
            }
            t = step.token;
            if (step.capturedScalar != ScalarSlot.UNSET) {
                // Leaf pre-captured by findField; the parser has scanned past it.
                capturedLeaf = step.capturedScalar;
            }
            if (step.replayParser != null) {
                // Intermediate descent through a captured subtree: subsequent findField
                // calls operate on the replay parser, positioned AT the matched value token.
                current = step.replayParser;
                open.add(current);
            }
        }
        if (capturedLeaf != ScalarSlot.UNSET && capturedLeaf != null) {
            return capturedLeaf;
        }
        // Leaf wasn't pre-captured (e.g. compound at leaf position, or empty path so {@code t}
        // is still the root). Fall back to reading at the current parser position.
        return extractLeaf(current, t, maxScalarStringChars);
    }

    private static void closeAll(List<JsonParser> parsers) {
        for (JsonParser p : parsers) {
            try {
                p.close();
            } catch (IOException ignored) {
                // Closing must not mask the navigation result.
            }
        }
    }

    /** Sentinel marker for "no captured scalar". {@code null} is a valid captured value (JSON null
     *  / missing scalar), so we need a distinct sentinel object. */
    private static final class ScalarSlot {
        static final Object UNSET = new Object();
        private ScalarSlot() {}
    }

    /**
     * Scans the current object for {@code wanted} and detects duplicate occurrences at this
     * level. The scan ALWAYS reads the whole object — at both leaf and intermediate levels —
     * because RFC 8259 leaves duplicate-key behaviour undefined and predicates are an
     * access-control boundary (see {@link #parseAndNavigate} javadoc for the threat model).
     *
     * <p>At LEAF level the matched scalar value is captured eagerly (parser advances past it
     * during the duplicate scan); the captured value rides back on
     * {@link NavStep#capturedScalar}.
     *
     * <p>At INTERMEDIATE level the matched value's whole subtree is captured into a
     * {@link TokenBuffer}, and a replay parser ({@link NavStep#replayParser}) is handed back
     * so the outer loop can descend into the captured subtree on the next iteration.
     */
    private static NavStep findField(JsonParser p, String wanted, boolean isLastStep, int maxScalarStringChars)
            throws IOException {
        JsonToken t;
        JsonToken matchedToken = null;
        Object capturedScalar = ScalarSlot.UNSET;
        TokenBuffer capturedSubtree = null;
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
                if (isLastStep) {
                    // Leaf level: capture the scalar value before the parser moves on.
                    capturedScalar = extractLeaf(p, valueToken, maxScalarStringChars);
                    if (valueToken == JsonToken.START_OBJECT || valueToken == JsonToken.START_ARRAY) {
                        // Compound at leaf position; captured value is null (extractLeaf semantics).
                        // We still must skip children so the duplicate-scan stays at the correct level.
                        p.skipChildren();
                    }
                } else {
                    // Intermediate level: capture the matched subtree so we can keep scanning
                    // the parent for duplicates without losing position. copyCurrentStructure
                    // advances the parser to the closing token of the captured value, leaving
                    // the outer scan free to read the next FIELD_NAME or END_OBJECT.
                    capturedSubtree = new TokenBuffer(p);
                    capturedSubtree.copyCurrentStructure(p);
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
        if (capturedSubtree != null) {
            // Replay parser: positioned BEFORE the first token, so advance once to put it AT
            // the captured matched value (mirroring what the original parser would have been at
            // when we returned matchedToken in the no-TokenBuffer version).
            JsonParser replay = capturedSubtree.asParser();
            JsonToken first = replay.nextToken();
            return new NavStep(first, ScalarSlot.UNSET, replay);
        }
        return new NavStep(matchedToken, capturedScalar, null);
    }

    /** Tiny tuple type used by {@link #findField}. */
    private static final class NavStep {
        static final NavStep NOT_FOUND = new NavStep(null, ScalarSlot.UNSET, null);
        static final NavStep MALFORMED = new NavStep(null, ScalarSlot.UNSET, null);
        final JsonToken token;
        /** Pre-captured leaf value, or {@link ScalarSlot#UNSET} if no leaf was captured.
         *  {@code null} is a valid captured value, so we distinguish via the UNSET sentinel. */
        final Object capturedScalar;
        /** When non-null, the outer loop must switch to this parser for the next descent step.
         *  Used when an intermediate-level match was captured into a {@link TokenBuffer} to
         *  allow continued duplicate-scanning of the parent. */
        final JsonParser replayParser;
        NavStep(JsonToken token, Object capturedScalar, JsonParser replayParser) {
            this.token = token;
            this.capturedScalar = capturedScalar;
            this.replayParser = replayParser;
        }
    }

    private static Object extractLeaf(JsonParser p, JsonToken t, int maxScalarStringChars) throws IOException {
        switch (t) {
            case VALUE_STRING:
                // PROMPT.md scenario: "oversized JSON strings ... silently skips those records".
                // Jackson exposes the un-decoded character length on the parser without forcing
                // a String allocation, so we can refuse over-cap scalars before they materialise.
                if (p.getTextLength() > maxScalarStringChars) {
                    return RecordContext.BODY_UNUSABLE;
                }
                return p.getValueAsString();
            case VALUE_NUMBER_INT:
                return extractLongOrNull(p);
            case VALUE_NUMBER_FLOAT:
                return extractDoubleOrUnusable(p);
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
            // Integer beyond Long range: a downstream `body.field != 1` predicate would
            // otherwise see null and (because null != 1 is true under equality semantics)
            // wrongly retain the record. PROMPT.md scenario "out-of-long-range integers
            // silently skip those records" requires record-skip, so we surface BODY_UNUSABLE.
            return RecordContext.BODY_UNUSABLE;
        }
    }

    /** 2^53 — beyond this magnitude IEEE-754 doubles cannot represent successive integers
     *  exactly. Mirrors {@code Lexer.IEEE_SAFE_INTEGER}; the JSON path enforces the same
     *  precision contract on incoming record bodies that the lexer enforces on predicate
     *  literals. */
    private static final long IEEE_SAFE_INTEGER = 1L << 53;

    private static Object extractDoubleOrUnusable(JsonParser p) throws IOException {
        double d = p.getDoubleValue();
        // JSON floats beyond IEEE-754 finite range parse to NaN/+Infinity/-Infinity. A predicate
        // like `body.limit >= body.spent` would otherwise admit a record with limit=1e9998 and
        // spent=1e9999 — both round to +Infinity, and Infinity >= Infinity is TRUE under IEEE
        // semantics, even though the exact JSON numbers should reject. NaN is unreachable from
        // valid JSON (Jackson rejects it by default unless ALLOW_NON_NUMERIC_NUMBERS is enabled,
        // which we don't enable), but guard it for completeness — defense-in-depth.
        if (!Double.isFinite(d)) {
            return RecordContext.BODY_UNUSABLE;
        }
        // Mirror Lexer.rejectUnsafeFloatLiteral: a JSON literal like 9.007199254740993e15 parses
        // silently to 9007199254740992.0; an attacker who controls the backing record and knows a
        // predicate `body.x == 9007199254740992.0` can therefore admit records whose intended
        // value is one above 2^53. Same for subnormal underflow (1e-324 parsing to 0.0). We use
        // the exact BigDecimal exposed by Jackson without forcing a string round-trip.
        BigDecimal exact = p.getDecimalValue();
        if (d == 0.0 && exact.signum() != 0) {
            return RecordContext.BODY_UNUSABLE;
        }
        BigDecimal stripped = exact.stripTrailingZeros();
        if (stripped.scale() <= 0) {
            BigInteger magnitude = stripped.toBigIntegerExact().abs();
            if (magnitude.compareTo(BigInteger.valueOf(IEEE_SAFE_INTEGER)) > 0) {
                return RecordContext.BODY_UNUSABLE;
            }
        } else if (d == Math.rint(d)) {
            // Exact-fractional aliases like 1.0000000000000001 parse to integral 1.0 and
            // would compare equal to Long 1 under the evaluator's safe mixed numeric equality.
            return RecordContext.BODY_UNUSABLE;
        }
        return d;
    }

    private static Optional<String> decodeUtf8Strict(byte[] bytes) {
        if (bytes == null) return Optional.empty();
        return decodeUtf8Strict(ByteBuffer.wrap(bytes));
    }

    private static Optional<String> decodeUtf8Strict(ByteBuffer buf) {
        if (buf == null) return Optional.empty();
        // Duplicate so the strict decoder advances its own position cursor rather than the
        // caller's. The cost is a small wrapper object; the underlying bytes are NOT copied.
        try {
            return Optional.of(StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(REPORT)
                    .onUnmappableCharacter(REPORT)
                    .decode(buf.duplicate())
                    .toString());
        } catch (CharacterCodingException e) {
            return Optional.empty();
        }
    }

    /** Materialise a ByteBuffer into a fresh byte[]. Only called when a predicate explicitly asks
     *  for raw bytes (e.g. {@link RecordContext#rawKey()}); the hot path stays on ByteBuffer. */
    private static byte[] bufferToArray(ByteBuffer buf) {
        if (buf == null) return null;
        ByteBuffer view = buf.duplicate();
        byte[] out = new byte[view.remaining()];
        view.get(out);
        return out;
    }

    /** Create a Jackson parser over a ByteBuffer without copying the underlying bytes when the
     *  buffer is heap-backed (the common Kafka case — MemoryRecords are heap-backed). For direct
     *  buffers we have no choice but to materialise a byte[]; this path is unreachable from the
     *  normal fetch flow but stays safe in case a caller hands us one. */
    private static JsonParser parserFor(ByteBuffer body) throws IOException {
        if (body.hasArray()) {
            return JSON_FACTORY.createParser(
                    body.array(),
                    body.arrayOffset() + body.position(),
                    body.remaining());
        }
        byte[] tmp = new byte[body.remaining()];
        body.duplicate().get(tmp);
        return JSON_FACTORY.createParser(tmp);
    }
}
