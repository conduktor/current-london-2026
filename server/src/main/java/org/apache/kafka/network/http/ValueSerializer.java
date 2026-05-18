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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;

/**
 * Translates record value bytes to/from the four-shape JSON envelope used by the HTTP API.
 *
 * Output chain (bytes → envelope): null → {@code NULL}; content-type {@code application/json} parseable → {@code JSON};
 * valid UTF-8 with no disallowed control characters → {@code STRING}; otherwise base64-encoded {@code BINARY}.
 *
 * Input direction is the inverse: {@code NULL} → null bytes; {@code JSON} → canonical JSON bytes; {@code STRING} → UTF-8
 * bytes; {@code BINARY} → base64-decoded bytes.
 */
public final class ValueSerializer {

    private static final String APPLICATION_JSON = "application/json";

    public enum ValueType { NULL, JSON, STRING, BINARY }

    public static final class EncodedValue {
        private final ValueType type;
        private final JsonNode data;

        private EncodedValue(ValueType type, JsonNode data) {
            this.type = type;
            this.data = data;
        }

        public ValueType type() {
            return type;
        }

        public ObjectNode toJson(ObjectMapper mapper) {
            ObjectNode node = mapper.createObjectNode();
            node.put("type", type.name());
            node.set("data", data);
            return node;
        }
    }

    private ValueSerializer() {
    }

    /**
     * Encode raw record bytes for inclusion in an HTTP JSON response.
     *
     * @param bytes raw record value bytes, possibly null
     * @param contentType optional content-type header value attached to the record
     * @return the envelope to embed in the JSON response
     */
    public static EncodedValue encode(byte[] bytes, String contentType) {
        if (bytes == null) {
            return new EncodedValue(ValueType.NULL, JsonNodeFactory.instance.nullNode());
        }
        if (isJsonContentType(contentType)) {
            JsonNode parsed = tryParseJson(bytes);
            if (parsed != null) {
                return new EncodedValue(ValueType.JSON, parsed);
            }
            // Fall through to the rest of the chain if the body lied about being JSON.
        }
        String decoded = tryDecodeUtf8(bytes);
        if (decoded != null) {
            return new EncodedValue(ValueType.STRING, JsonNodeFactory.instance.textNode(decoded));
        }
        String base64 = Base64.getEncoder().encodeToString(bytes);
        return new EncodedValue(ValueType.BINARY, JsonNodeFactory.instance.textNode(base64));
    }

    /**
     * Decode a JSON envelope sent by an HTTP producer into the bytes that will be stored as the record value.
     *
     * @return the bytes for the record value, or null if the envelope encodes a NULL value
     * @throws IllegalArgumentException if the envelope is malformed
     */
    public static byte[] decode(JsonNode envelope) {
        ValueType type = readType(envelope);
        switch (type) {
            case NULL:
                return null;
            case JSON:
                return jsonDataAsBytes(envelope);
            case STRING:
                return stringDataAsBytes(envelope);
            case BINARY:
                return binaryDataAsBytes(envelope);
            default:
                throw new IllegalArgumentException("Unhandled value type: " + type);
        }
    }

    /**
     * Returns the content-type that should be stored alongside a record produced from this envelope, or null when no
     * specific content-type applies. Only JSON envelopes carry a content-type so that consumers can later pick the
     * {@code JSON} branch of the output chain.
     */
    public static String contentTypeFor(JsonNode envelope) {
        return readType(envelope) == ValueType.JSON ? APPLICATION_JSON : null;
    }

    private static ValueType readType(JsonNode envelope) {
        if (envelope == null || !envelope.hasNonNull("type")) {
            throw new IllegalArgumentException("Value envelope must have a 'type' field");
        }
        String raw = envelope.get("type").asText();
        try {
            return ValueType.valueOf(raw);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown value type: " + raw);
        }
    }

    private static byte[] jsonDataAsBytes(JsonNode envelope) {
        if (!envelope.has("data")) {
            throw new IllegalArgumentException("JSON envelope must carry 'data'");
        }
        try {
            return JSON_WRITER.writeValueAsBytes(envelope.get("data"));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not re-serialise JSON envelope data", e);
        }
    }

    private static byte[] stringDataAsBytes(JsonNode envelope) {
        JsonNode data = envelope.get("data");
        if (data == null || !data.isTextual()) {
            throw new IllegalArgumentException("STRING envelope requires textual 'data'");
        }
        return data.asText().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] binaryDataAsBytes(JsonNode envelope) {
        JsonNode data = envelope.get("data");
        if (data == null || !data.isTextual()) {
            throw new IllegalArgumentException("BINARY envelope requires base64 'data'");
        }
        try {
            return Base64.getDecoder().decode(data.asText());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("BINARY envelope data is not valid base64", e);
        }
    }

    private static boolean isJsonContentType(String contentType) {
        if (contentType == null) {
            return false;
        }
        String trimmed = contentType.trim().toLowerCase(Locale.ROOT);
        if (trimmed.equals(APPLICATION_JSON)) {
            return true;
        }
        int semi = trimmed.indexOf(';');
        return semi > 0 && trimmed.substring(0, semi).trim().equals(APPLICATION_JSON);
    }

    private static JsonNode tryParseJson(byte[] bytes) {
        try {
            return JSON_WRITER.readTree(bytes);
        } catch (Exception e) {
            return null;
        }
    }

    private static String tryDecodeUtf8(byte[] bytes) {
        try {
            String decoded = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes))
                .toString();
            return hasDisallowedControlChars(decoded) ? null : decoded;
        } catch (CharacterCodingException e) {
            return null;
        }
    }

    private static boolean hasDisallowedControlChars(String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < 0x20 && c != '\t' && c != '\n' && c != '\r') {
                return true;
            }
        }
        return false;
    }

    private static final ObjectMapper JSON_WRITER = new ObjectMapper();
}
