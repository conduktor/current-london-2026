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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ValueSerializerTest {

    private final ObjectMapper mapper = new ObjectMapper();

    // ----- encode(bytes, contentType) -----

    @Test
    void encodeNullBytesProducesNullEnvelope() {
        ValueSerializer.EncodedValue v = ValueSerializer.encode(null, null);
        assertEquals(ValueSerializer.ValueType.NULL, v.type());
        assertTrue(v.toJson(mapper).get("data").isNull());
    }

    @Test
    void encodeNullBytesIgnoresContentType() {
        ValueSerializer.EncodedValue v = ValueSerializer.encode(null, "application/json");
        assertEquals(ValueSerializer.ValueType.NULL, v.type());
    }

    @Test
    void encodeJsonContentTypeParsesPayload() throws Exception {
        byte[] bytes = "{\"x\":1}".getBytes(StandardCharsets.UTF_8);
        ValueSerializer.EncodedValue v = ValueSerializer.encode(bytes, "application/json");
        assertEquals(ValueSerializer.ValueType.JSON, v.type());
        JsonNode node = v.toJson(mapper);
        assertEquals(1, node.get("data").get("x").asInt());
    }

    @Test
    void encodeJsonContentTypeWithCharsetParameterIsRecognised() {
        byte[] bytes = "[1,2,3]".getBytes(StandardCharsets.UTF_8);
        ValueSerializer.EncodedValue v = ValueSerializer.encode(bytes, "application/json; charset=utf-8");
        assertEquals(ValueSerializer.ValueType.JSON, v.type());
        assertEquals(3, v.toJson(mapper).get("data").size());
    }

    @Test
    void encodeJsonContentTypeButInvalidJsonFallsThroughChain() {
        // content-type says JSON but payload is not valid JSON — fall through to UTF-8 / binary
        byte[] bytes = "not json".getBytes(StandardCharsets.UTF_8);
        ValueSerializer.EncodedValue v = ValueSerializer.encode(bytes, "application/json");
        // "not json" is valid UTF-8 with no control characters
        assertEquals(ValueSerializer.ValueType.STRING, v.type());
        assertEquals("not json", v.toJson(mapper).get("data").asText());
    }

    @Test
    void encodeValidUtf8BecomesString() {
        byte[] bytes = "héllo café".getBytes(StandardCharsets.UTF_8);
        ValueSerializer.EncodedValue v = ValueSerializer.encode(bytes, null);
        assertEquals(ValueSerializer.ValueType.STRING, v.type());
        assertEquals("héllo café", v.toJson(mapper).get("data").asText());
    }

    @Test
    void encodeTabNewlineCarriageReturnAreAllowedInString() {
        byte[] bytes = "line1\nline2\tcol\r".getBytes(StandardCharsets.UTF_8);
        ValueSerializer.EncodedValue v = ValueSerializer.encode(bytes, null);
        assertEquals(ValueSerializer.ValueType.STRING, v.type());
    }

    @Test
    void encodeBytesWithNullByteFallsToBinary() {
        byte[] bytes = {72, 73, 0, 74};
        ValueSerializer.EncodedValue v = ValueSerializer.encode(bytes, null);
        assertEquals(ValueSerializer.ValueType.BINARY, v.type());
        // base64 of {72, 73, 0, 74} = "SEkASg=="
        assertEquals("SEkASg==", v.toJson(mapper).get("data").asText());
    }

    @Test
    void encodeInvalidUtf8FallsToBinary() {
        // 0xFF is not valid UTF-8 lead byte
        byte[] bytes = {(byte) 0xFF, (byte) 0xFE, (byte) 0xFD};
        ValueSerializer.EncodedValue v = ValueSerializer.encode(bytes, null);
        assertEquals(ValueSerializer.ValueType.BINARY, v.type());
    }

    @Test
    void encodeEmptyBytesIsEmptyString() {
        ValueSerializer.EncodedValue v = ValueSerializer.encode(new byte[0], null);
        assertEquals(ValueSerializer.ValueType.STRING, v.type());
        assertEquals("", v.toJson(mapper).get("data").asText());
    }

    @Test
    void encodeContentTypeWithUnknownMediaUsesUtf8OrBinaryChain() {
        byte[] bytes = "plain".getBytes(StandardCharsets.UTF_8);
        ValueSerializer.EncodedValue v = ValueSerializer.encode(bytes, "text/plain");
        assertEquals(ValueSerializer.ValueType.STRING, v.type());
    }

    // ----- decode(JsonNode) -----

    @Test
    void decodeNullEnvelopeProducesNullBytes() throws Exception {
        JsonNode node = mapper.readTree("{\"type\":\"NULL\"}");
        assertNull(ValueSerializer.decode(node));
    }

    @Test
    void decodeJsonEnvelopeRetainsJsonShape() throws Exception {
        JsonNode node = mapper.readTree("{\"type\":\"JSON\",\"data\":{\"a\":1}}");
        byte[] bytes = ValueSerializer.decode(node);
        // The decoded bytes should be a valid JSON serialisation of {"a":1}
        JsonNode roundTrip = mapper.readTree(bytes);
        assertEquals(1, roundTrip.get("a").asInt());
    }

    @Test
    void decodeStringEnvelopeBecomesUtf8Bytes() throws Exception {
        JsonNode node = mapper.readTree("{\"type\":\"STRING\",\"data\":\"hello\"}");
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), ValueSerializer.decode(node));
    }

    @Test
    void decodeBinaryEnvelopeDecodesBase64() throws Exception {
        JsonNode node = mapper.readTree("{\"type\":\"BINARY\",\"data\":\"SEkASg==\"}");
        assertArrayEquals(new byte[]{72, 73, 0, 74}, ValueSerializer.decode(node));
    }

    @Test
    void decodeUnknownTypeFails() throws Exception {
        JsonNode node = mapper.readTree("{\"type\":\"BANANA\",\"data\":\"x\"}");
        assertThrows(IllegalArgumentException.class, () -> ValueSerializer.decode(node));
    }

    @Test
    void decodeMissingTypeFails() throws Exception {
        JsonNode node = mapper.readTree("{\"data\":\"x\"}");
        assertThrows(IllegalArgumentException.class, () -> ValueSerializer.decode(node));
    }

    @Test
    void decodeBinaryWithInvalidBase64Fails() throws Exception {
        JsonNode node = mapper.readTree("{\"type\":\"BINARY\",\"data\":\"###not_base64###\"}");
        assertThrows(IllegalArgumentException.class, () -> ValueSerializer.decode(node));
    }

    @Test
    void decodeStringEnvelopeWithMissingDataFails() throws Exception {
        JsonNode node = mapper.readTree("{\"type\":\"STRING\"}");
        assertThrows(IllegalArgumentException.class, () -> ValueSerializer.decode(node));
    }

    // ----- contentTypeFor: the inverse hint for storing records -----

    @Test
    void contentTypeForReturnsApplicationJsonForJsonEnvelope() throws Exception {
        JsonNode node = mapper.readTree("{\"type\":\"JSON\",\"data\":{}}");
        assertEquals("application/json", ValueSerializer.contentTypeFor(node));
    }

    @Test
    void contentTypeForReturnsNullForNonJsonEnvelopes() throws Exception {
        assertNull(ValueSerializer.contentTypeFor(mapper.readTree("{\"type\":\"STRING\",\"data\":\"x\"}")));
        assertNull(ValueSerializer.contentTypeFor(mapper.readTree("{\"type\":\"BINARY\",\"data\":\"AA==\"}")));
        assertNull(ValueSerializer.contentTypeFor(mapper.readTree("{\"type\":\"NULL\"}")));
    }
}
