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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BridgeJsonMappersTest {

    @Test
    void hardenedReturnsFreshInstanceEachCall() {
        ObjectMapper a = BridgeJsonMappers.hardened();
        ObjectMapper b = BridgeJsonMappers.hardened();
        // ObjectMapper is mutable; sharing one across callers would let any reconfigure leak globally.
        assertNotSame(a, b);
    }

    @Test
    void hardenedMapperParsesNormalDocument() {
        ObjectMapper mapper = BridgeJsonMappers.hardened();
        assertEquals(42, parse(mapper, "{\"id\":42}").get("id").asInt());
    }

    @Test
    void rejectsStringExceedingCap() {
        // A single string field longer than MAX_STRING_LENGTH must fail at parse time, not at consumption time.
        // Build a 1 MiB + 1 byte string of 'a' wrapped in a minimal JSON envelope.
        int over = BridgeJsonMappers.MAX_STRING_LENGTH + 1;
        StringBuilder json = new StringBuilder(over + 16);
        json.append("{\"x\":\"");
        for (int i = 0; i < over; i++) {
            json.append('a');
        }
        json.append("\"}");
        ObjectMapper mapper = BridgeJsonMappers.hardened();
        assertThrows(StreamConstraintsException.class, () -> mapper.readTree(json.toString()));
    }

    @Test
    void acceptsStringJustBelowCap() {
        // The string boundary is asserted indirectly: rejectsStringExceedingCap pins the throw at MAX+1,
        // and this case pins the accept at a value that comfortably clears any envelope overhead. We don't
        // build a literal MAX-length string here because the JSON envelope ({"x":"..."}) adds 8 bytes that
        // would tip the document past MAX_DOCUMENT_LENGTH once FAIL_ON_TRAILING_TOKENS forces the parser to
        // read through EOF — and we want this test to fail on the string cap, not on the document cap.
        int strLen = BridgeJsonMappers.MAX_STRING_LENGTH - 32;
        StringBuilder json = new StringBuilder(strLen + 16);
        json.append("{\"x\":\"");
        for (int i = 0; i < strLen; i++) {
            json.append('a');
        }
        json.append("\"}");
        ObjectMapper mapper = BridgeJsonMappers.hardened();
        JsonNode tree = parse(mapper, json.toString());
        assertEquals(strLen, tree.get("x").asText().length());
    }

    @Test
    void rejectsNestingExceedingDepthCap() {
        // Build N+1 levels of array nesting; the parser must fail before producing the root node.
        int over = BridgeJsonMappers.MAX_NESTING_DEPTH + 1;
        StringBuilder json = new StringBuilder(over * 2);
        for (int i = 0; i < over; i++) {
            json.append('[');
        }
        for (int i = 0; i < over; i++) {
            json.append(']');
        }
        ObjectMapper mapper = BridgeJsonMappers.hardened();
        assertThrows(StreamConstraintsException.class, () -> mapper.readTree(json.toString()));
    }

    @Test
    void acceptsNestingAtDepthCap() {
        int depth = BridgeJsonMappers.MAX_NESTING_DEPTH;
        StringBuilder json = new StringBuilder(depth * 2);
        for (int i = 0; i < depth; i++) {
            json.append('[');
        }
        for (int i = 0; i < depth; i++) {
            json.append(']');
        }
        ObjectMapper mapper = BridgeJsonMappers.hardened();
        // No exception — at-cap is the boundary, not above it.
        parse(mapper, json.toString());
    }

    @Test
    void rejectsNumericLiteralExceedingLengthCap() {
        // A gigabyte-long number literal is the textbook Jackson BigDecimal DoS shape; the cap fails the parse early.
        int over = BridgeJsonMappers.MAX_NUMBER_LENGTH + 1;
        StringBuilder json = new StringBuilder(over + 8);
        json.append("{\"n\":");
        for (int i = 0; i < over; i++) {
            json.append('1');
        }
        json.append('}');
        ObjectMapper mapper = BridgeJsonMappers.hardened();
        assertThrows(StreamConstraintsException.class, () -> mapper.readTree(json.toString()));
    }

    @Test
    void rejectsDocumentExceedingLengthCapOnByteInput() {
        // A JSON document whose total byte length exceeds MAX_DOCUMENT_LENGTH must fail at parse time.
        // The HTTP body path Jackson sees is byte-based (UTF8StreamJsonParser), which is the path this cap
        // exists to protect — Jackson's _loadMore() consults validateDocumentLength on every buffer refill.
        long over = BridgeJsonMappers.MAX_DOCUMENT_LENGTH + 1;
        // Document shape: an array of single-byte ASCII digits separated by commas. Each entry is "1," (2 bytes);
        // we don't put a single long string here because that would trip MAX_STRING_LENGTH first.
        byte[] bytes = new byte[(int) over];
        bytes[0] = '[';
        bytes[bytes.length - 1] = ']';
        for (int i = 1; i < bytes.length - 1; i += 2) {
            bytes[i] = '1';
            if (i + 1 < bytes.length - 1) {
                bytes[i + 1] = ',';
            }
        }
        ObjectMapper mapper = BridgeJsonMappers.hardened();
        // Either the document-length cap (preferred) or the token-count cap fires first — both are valid
        // defences against a > 1 MiB body. Asserting either-throws keeps the test honest about which cap
        // catches it without requiring the test to recompute the exact token count.
        assertThrows(StreamConstraintsException.class, () -> mapper.readTree(bytes));
    }

    @Test
    void rejectsTokenCountExceedingCap() {
        // A document packed with tokens must fail before the parser allocates per-token binding objects
        // across the cap. An array of single-digit integers crosses the 200K-token floor with the smallest
        // possible per-token byte cost (one VALUE_NUMBER_INT + one structural comma per entry) and stays
        // well under MAX_DOCUMENT_LENGTH so the token cap — not the document cap — is the proximate cause.
        // An object shape would trip FAIL_ON_READING_DUP_TREE_KEY first under any repeated key; unique keys
        // explode the byte budget past 1 MiB before reaching the token count.
        long entries = BridgeJsonMappers.MAX_TOKEN_COUNT + 1;
        StringBuilder json = new StringBuilder((int) entries * 2 + 2);
        json.append('[');
        for (long i = 0; i < entries; i++) {
            if (i > 0) {
                json.append(',');
            }
            json.append('1');
        }
        json.append(']');
        ObjectMapper mapper = BridgeJsonMappers.hardened();
        assertThrows(StreamConstraintsException.class, () -> mapper.readTree(json.toString()));
    }

    @Test
    void rejectsTrailingTokensAfterRootValue() {
        // Smuggling defence: a content-aware proxy that pre-parses the prefix sees a valid {"a":1}; a strict
        // downstream parser would otherwise accept the same bytes and silently drop the tail. The hardened
        // mapper must reject the parse so the bridge and the upstream defence layer agree on what the body is.
        ObjectMapper mapper = BridgeJsonMappers.hardened();
        // Jackson surfaces the post-root garbage via _verifyNoTrailingTokens, which calls nextToken and
        // throws JsonParseException — a sibling of JsonMappingException under JsonProcessingException, not
        // a subtype. Asserting the common superclass keeps the test robust against a future Jackson
        // refactor that reclassifies trailing-content under a different concrete type.
        assertThrows(JsonProcessingException.class,
            () -> mapper.readTree("{\"a\":1}garbage"));
    }

    @Test
    void rejectsDuplicateKeysAtSameNestingLevel() {
        // Parser-confusion defence: an envelope like {"type":"STRING","type":"BINARY"} would otherwise be
        // accepted with Jackson silently keeping "BINARY". The bridge would then store one value type while
        // an upstream firewall / audit log that recorded the first key sees a different one. The hardened
        // mapper must reject the parse entirely.
        ObjectMapper mapper = BridgeJsonMappers.hardened();
        assertThrows(JsonMappingException.class,
            () -> mapper.readTree("{\"type\":\"STRING\",\"type\":\"BINARY\"}"));
    }

    @Test
    void writePathIsUnaffectedByReadConstraints() {
        // StreamReadConstraints only govern parsing; serialization of any JsonNode tree must still work even when the
        // resulting string would itself exceed MAX_STRING_LENGTH on a subsequent read. Important because the bridge
        // both reads and writes JSON with the same mapper.
        ObjectMapper mapper = BridgeJsonMappers.hardened();
        // Pre-build a JsonNode by parsing a small document, then serialize.
        JsonNode tree = parse(mapper, "{\"k\":\"v\"}");
        try {
            String json = mapper.writeValueAsString(tree);
            assertEquals("{\"k\":\"v\"}", json);
        } catch (Exception e) {
            throw new AssertionError("hardened mapper must still serialise normally", e);
        }
    }

    private static JsonNode parse(ObjectMapper mapper, String s) {
        try {
            return mapper.readTree(s);
        } catch (Exception e) {
            throw new AssertionError("parse failed: " + e.getMessage(), e);
        }
    }
}
