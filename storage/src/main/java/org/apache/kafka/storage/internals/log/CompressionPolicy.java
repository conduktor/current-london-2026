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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.record.CompressionType;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Server-side policy for compression of producer batches written to a topic.
 *
 * Registered as the topic config {@code compression.policy}. Four shapes are valid:
 * <ul>
 *   <li>{@code "none"} ({@link #NONE}): default. No per-batch enforcement happens on the
 *       produce path — vanilla Kafka behaviour is preserved exactly.</li>
 *   <li>{@code "required"} ({@link #REQUIRED}): the broker rejects batches whose
 *       {@link CompressionType} is {@code NONE}, per-partition, with {@code INVALID_RECORD}.</li>
 *   <li>{@code "forbidden"} ({@link #FORBIDDEN}): the mirror image of {@code required}. The
 *       broker rejects any batch whose {@link CompressionType} is <em>not</em> {@code NONE},
 *       with the same per-partition {@code INVALID_RECORD} response. Useful when downstream
 *       tools read the on-disk batch format directly and cannot perform per-codec decompression.
 *       Caveat: {@code compression.policy=forbidden} gates the entry codec only; if the topic
 *       has {@code compression.type} set to anything other than {@code producer} or
 *       {@code uncompressed}, broker-side recompression can still rewrite the on-disk codec.</li>
 *   <li>A comma-separated allow-list of codec names (e.g. {@code "gzip,lz4,zstd"}): the broker
 *       accepts only batches whose {@link CompressionType} appears in the list, and rejects all
 *       others with {@code INVALID_RECORD}. The list cannot include {@code none} — use
 *       {@code compression.policy=forbidden} for that. The list cannot be empty.</li>
 * </ul>
 *
 * Enforcement runs in the produce request handler, ahead of the replication and log layers.
 * Replication, transaction state, and group-coordinator appends therefore bypass the check by
 * construction: already-stored batches remain replicable even if the policy is enabled after
 * the fact.
 */
public final class CompressionPolicy {

    /**
     * Discrete shapes recognised by {@link #parse(String)}. Useful for testing and for
     * callers that want to switch on the policy kind explicitly rather than pattern-match
     * on {@link #value()}.
     */
    public enum Kind {
        NONE,
        REQUIRED,
        FORBIDDEN,
        ALLOW_LIST
    }

    public static final CompressionPolicy NONE = new CompressionPolicy(Kind.NONE, "none", Collections.emptySet());
    public static final CompressionPolicy REQUIRED = new CompressionPolicy(Kind.REQUIRED, "required", Collections.emptySet());
    public static final CompressionPolicy FORBIDDEN = new CompressionPolicy(Kind.FORBIDDEN, "forbidden", Collections.emptySet());

    private static final List<CompressionPolicy> WELL_KNOWN =
        Collections.unmodifiableList(Arrays.asList(NONE, REQUIRED, FORBIDDEN));
    // unmodifiableList because names() exposes this directly to callers (validator toString,
    // tests, doc generation) — they must not be able to mutate the static list and corrupt
    // future calls. Arrays.asList alone is fixed-size but still allows set(int, T).
    private static final List<String> WELL_KNOWN_NAMES =
        Collections.unmodifiableList(Arrays.asList(NONE.value, REQUIRED.value, FORBIDDEN.value));

    private final Kind kind;
    private final String value;
    private final Set<CompressionType> allowedCodecs;

    private CompressionPolicy(Kind kind, String value, Set<CompressionType> allowedCodecs) {
        this.kind = kind;
        this.value = value;
        this.allowedCodecs = allowedCodecs;
    }

    /**
     * @return the configuration value (e.g. {@code "none"}, {@code "required"}, or {@code "gzip,lz4"})
     *         under which this policy is exposed in the topic config. Round-trippable through
     *         {@link #parse(String)}.
     */
    public String value() {
        return value;
    }

    /**
     * @return the discrete shape of this policy. Useful for callers that need to switch on
     *         the policy without pattern-matching {@link #value()}.
     */
    public Kind kind() {
        return kind;
    }

    /**
     * @return the codec allow-list. Non-empty only for policies of kind {@link Kind#ALLOW_LIST}.
     *         Immutable.
     */
    public Set<CompressionType> allowedCodecs() {
        return allowedCodecs;
    }

    /**
     * @return {@code true} if a batch with the given compression type violates this policy and
     *         must be rejected with {@code INVALID_RECORD}.
     */
    public boolean isViolatedBy(CompressionType batchCompression) {
        switch (kind) {
            case NONE:
                return false;
            case REQUIRED:
                return batchCompression == CompressionType.NONE;
            case FORBIDDEN:
                return batchCompression != CompressionType.NONE;
            case ALLOW_LIST:
                return !allowedCodecs.contains(batchCompression);
            default:
                throw new IllegalStateException("Unreachable: unhandled compression policy kind " + kind);
        }
    }

    /**
     * @return the well-known short names ({@code "none"}, {@code "required"}, {@code "forbidden"}).
     *         Does not include any allow-list values: those are unbounded and validated by
     *         {@link #parse(String)} rather than enumerated.
     */
    public static List<String> names() {
        return WELL_KNOWN_NAMES;
    }

    /**
     * Parses a {@code compression.policy} configuration value into a {@link CompressionPolicy}.
     * Accepts one of the well-known names ({@code "none"}, {@code "required"}, {@code "forbidden"})
     * or a non-empty comma-separated list of codec names (each from {@code [gzip, snappy, lz4, zstd]};
     * whitespace around commas is tolerated).
     *
     * @throws IllegalArgumentException if the value is not a recognised well-known name and is
     *         not parseable as a non-empty allow-list of valid, non-{@code none} codec names.
     */
    public static CompressionPolicy parse(String configValue) {
        if (configValue == null) {
            throw new IllegalArgumentException("compression.policy value must not be null");
        }
        String normalized = configValue.trim().toLowerCase(Locale.ROOT);
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("compression.policy value must not be empty; expected one of "
                + WELL_KNOWN_NAMES + " or a comma-separated codec allow-list");
        }
        for (CompressionPolicy known : WELL_KNOWN) {
            if (known.value.equals(normalized)) {
                return known;
            }
        }
        return parseAllowList(normalized);
    }

    private static CompressionPolicy parseAllowList(String normalized) {
        // The normalized form is already lowercased and trimmed; tokens are still trimmed
        // individually because callers may write "gzip, lz4" (with spaces around commas).
        String[] tokens = normalized.split(",", -1);
        Set<CompressionType> codecs = EnumSet.noneOf(CompressionType.class);
        Set<String> seenTokens = new LinkedHashSet<>();
        for (String raw : tokens) {
            String token = raw.trim();
            if (token.isEmpty()) {
                throw new IllegalArgumentException("compression.policy value '" + normalized
                    + "' contains an empty codec token; allow-list entries must be non-empty");
            }
            if (!seenTokens.add(token)) {
                throw new IllegalArgumentException("compression.policy value '" + normalized
                    + "' contains duplicate codec '" + token + "'");
            }
            CompressionType codec;
            try {
                codec = CompressionType.forName(token);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("compression.policy value '" + normalized
                    + "' contains unknown codec '" + token + "'; expected one of [gzip, snappy, lz4, zstd]");
            }
            if (codec == CompressionType.NONE) {
                throw new IllegalArgumentException("compression.policy value '" + normalized
                    + "' may not include 'none' in an allow-list; use compression.policy=forbidden to "
                    + "reject every compressed codec, or list only the codecs you accept");
            }
            codecs.add(codec);
        }
        if (codecs.isEmpty()) {
            // Defensive: split(",", -1) on a non-empty string always returns ≥ 1 token, and the
            // empty-token check above already rejects empty entries — so this should be unreachable
            // for any normalized input that survived the earlier checks. Kept as a guard for
            // future refactors of the tokenizer.
            throw new IllegalArgumentException("compression.policy allow-list must be non-empty");
        }
        return new CompressionPolicy(Kind.ALLOW_LIST, normalized, Collections.unmodifiableSet(codecs));
    }

    /**
     * Backwards-compatible alias for {@link #parse(String)} so existing callers that use
     * {@code CompressionPolicy.forName(...)} continue to compile and behave identically for the
     * three well-known values.
     */
    public static CompressionPolicy forName(String configValue) {
        return parse(configValue);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CompressionPolicy)) return false;
        CompressionPolicy that = (CompressionPolicy) o;
        return kind == that.kind && value.equals(that.value) && allowedCodecs.equals(that.allowedCodecs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, value, allowedCodecs);
    }

    @Override
    public String toString() {
        return "CompressionPolicy(" + value + ")";
    }
}
