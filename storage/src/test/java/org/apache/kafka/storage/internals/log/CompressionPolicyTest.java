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
import org.apache.kafka.storage.internals.log.CompressionPolicy.Kind;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the public contract of {@link CompressionPolicy}: the three well-known short names plus
 * the comma-separated codec allow-list. These tests exist for two reasons:
 *
 *  1. The validator wired into LogConfig delegates to {@link CompressionPolicy#parse(String)},
 *     so the broker's acceptance/rejection of compression.policy values is whatever this method
 *     accepts/rejects. Any drift here changes the broker's external configuration surface.
 *
 *  2. The hot-path enforcement loop in KafkaApis uses {@code policy eq CompressionPolicy.NONE}
 *     (reference equality) as a fast-path. This file pins that the well-known parse paths return
 *     the singleton static instances, not freshly-allocated copies — otherwise the fast-path
 *     branch would silently regress.
 */
class CompressionPolicyTest {

    // ---------- well-known names ----------

    @Test
    void parseRecognisesNoneAsTheSingletonInstance() {
        assertSame(CompressionPolicy.NONE, CompressionPolicy.parse("none"),
            "the fast-path 'eq NONE' check requires parse(\"none\") to return the singleton");
        assertEquals(Kind.NONE, CompressionPolicy.NONE.kind());
        assertEquals("none", CompressionPolicy.NONE.value());
    }

    @Test
    void parseRecognisesRequiredAsTheSingletonInstance() {
        assertSame(CompressionPolicy.REQUIRED, CompressionPolicy.parse("required"));
        assertEquals(Kind.REQUIRED, CompressionPolicy.REQUIRED.kind());
        assertEquals("required", CompressionPolicy.REQUIRED.value());
    }

    @Test
    void parseRecognisesForbiddenAsTheSingletonInstance() {
        assertSame(CompressionPolicy.FORBIDDEN, CompressionPolicy.parse("forbidden"));
        assertEquals(Kind.FORBIDDEN, CompressionPolicy.FORBIDDEN.kind());
        assertEquals("forbidden", CompressionPolicy.FORBIDDEN.value());
    }

    @ParameterizedTest
    @CsvSource({
        "NONE,      none",
        "Required,  required",
        "FORBIDDEN, forbidden",
        " none , none",
        "  required  , required"
    })
    void parseIsCaseInsensitiveAndTrimsWhitespaceForWellKnownNames(String input, String expectedValue) {
        CompressionPolicy parsed = CompressionPolicy.parse(input);
        assertEquals(expectedValue, parsed.value());
        // Should still be the singleton; case/whitespace must not produce a fresh instance.
        if ("none".equals(expectedValue)) assertSame(CompressionPolicy.NONE, parsed);
        if ("required".equals(expectedValue)) assertSame(CompressionPolicy.REQUIRED, parsed);
        if ("forbidden".equals(expectedValue)) assertSame(CompressionPolicy.FORBIDDEN, parsed);
    }

    @Test
    void namesReturnsOnlyTheThreeWellKnownShortNames() {
        assertEquals(java.util.Arrays.asList("none", "required", "forbidden"), CompressionPolicy.names(),
            "names() must list exactly the short, enumerable values; allow-list values are unbounded "
                + "and validated by parse() rather than enumerated.");
    }

    // ---------- isViolatedBy for the well-known values ----------

    @Test
    void isViolatedByForNonePolicyAlwaysReturnsFalse() {
        for (CompressionType codec : CompressionType.values()) {
            assertFalse(CompressionPolicy.NONE.isViolatedBy(codec),
                "policy=none must never reject any batch (codec=" + codec + ")");
        }
    }

    @Test
    void isViolatedByForRequiredPolicyRejectsOnlyNoneCodec() {
        assertTrue(CompressionPolicy.REQUIRED.isViolatedBy(CompressionType.NONE));
        assertFalse(CompressionPolicy.REQUIRED.isViolatedBy(CompressionType.GZIP));
        assertFalse(CompressionPolicy.REQUIRED.isViolatedBy(CompressionType.SNAPPY));
        assertFalse(CompressionPolicy.REQUIRED.isViolatedBy(CompressionType.LZ4));
        assertFalse(CompressionPolicy.REQUIRED.isViolatedBy(CompressionType.ZSTD));
    }

    @Test
    void isViolatedByForForbiddenPolicyAcceptsOnlyNoneCodec() {
        assertFalse(CompressionPolicy.FORBIDDEN.isViolatedBy(CompressionType.NONE));
        assertTrue(CompressionPolicy.FORBIDDEN.isViolatedBy(CompressionType.GZIP));
        assertTrue(CompressionPolicy.FORBIDDEN.isViolatedBy(CompressionType.SNAPPY));
        assertTrue(CompressionPolicy.FORBIDDEN.isViolatedBy(CompressionType.LZ4));
        assertTrue(CompressionPolicy.FORBIDDEN.isViolatedBy(CompressionType.ZSTD));
    }

    // ---------- allow-list parsing ----------

    @ParameterizedTest
    @ValueSource(strings = {"gzip", "snappy", "lz4", "zstd"})
    void parseAcceptsSingleCodecAllowList(String codec) {
        CompressionPolicy parsed = CompressionPolicy.parse(codec);
        assertEquals(Kind.ALLOW_LIST, parsed.kind(),
            "a single non-well-known codec name must parse as a one-element allow-list, not as a well-known shape");
        assertEquals(codec, parsed.value());
        assertEquals(Set.of(CompressionType.forName(codec)), parsed.allowedCodecs());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "gzip,lz4",
        "gzip,snappy,lz4,zstd",
        "GZIP,LZ4",
        "gzip, lz4",
        " gzip , lz4 , zstd "
    })
    void parseAcceptsMultiCodecAllowListAndNormalisesIt(String configValue) {
        CompressionPolicy parsed = CompressionPolicy.parse(configValue);
        assertEquals(Kind.ALLOW_LIST, parsed.kind());
        // The parsed instance must accept every codec in the list and reject every codec NOT in it.
        for (CompressionType codec : parsed.allowedCodecs()) {
            assertFalse(parsed.isViolatedBy(codec), "codec=" + codec + " must be accepted by " + parsed);
        }
        // NONE must always be rejected by an allow-list (allow-list cannot contain none).
        assertTrue(parsed.isViolatedBy(CompressionType.NONE),
            "an allow-list must reject the uncompressed codec; only compression.policy=none allows that");
    }

    @Test
    void parseAllowListReturnsDistinctInstancesForDistinctLists() {
        CompressionPolicy a = CompressionPolicy.parse("gzip,lz4");
        CompressionPolicy b = CompressionPolicy.parse("gzip,lz4");
        // Identity *not* guaranteed for allow-lists (every parse builds a new value object),
        // but value equality must hold.
        assertEquals(a, b);
        CompressionPolicy c = CompressionPolicy.parse("gzip,zstd");
        assertNotSame(a, c);
        assertFalse(a.equals(c), "different allow-lists must compare unequal");
    }

    // ---------- allow-list rejection paths ----------

    @ParameterizedTest
    @ValueSource(strings = {
        "",
        "   ",
        ",",
        ",gzip",
        "gzip,",
        "gzip,,lz4",
        "gzip, ,lz4"
    })
    void parseRejectsEmptyOrMalformedTokens(String configValue) {
        assertThrows(IllegalArgumentException.class, () -> CompressionPolicy.parse(configValue));
    }

    @Test
    void parseRejectsNullInput() {
        assertThrows(IllegalArgumentException.class, () -> CompressionPolicy.parse(null));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "bogus",
        "gzip,bogus",
        "bogus,gzip",
        "GZIP,BOGUS"
    })
    void parseRejectsUnknownCodecNames(String configValue) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> CompressionPolicy.parse(configValue));
        assertTrue(ex.getMessage().toLowerCase(java.util.Locale.ROOT).contains("unknown codec"),
            "rejection message must say 'unknown codec', got: " + ex.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "none,gzip",
        "gzip,none",
        "GZIP,NONE",
        "lz4,zstd,none"
    })
    void parseRejectsNoneInsideAllowList(String configValue) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> CompressionPolicy.parse(configValue));
        assertTrue(ex.getMessage().toLowerCase(java.util.Locale.ROOT).contains("may not include 'none'"),
            "rejection message must direct the operator to use compression.policy=forbidden, "
                + "got: " + ex.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "gzip,gzip",
        "gzip,lz4,gzip",
        "GZIP,gzip"
    })
    void parseRejectsDuplicateCodecs(String configValue) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> CompressionPolicy.parse(configValue));
        assertTrue(ex.getMessage().toLowerCase(java.util.Locale.ROOT).contains("duplicate"),
            "rejection message must mention 'duplicate', got: " + ex.getMessage());
    }
}
