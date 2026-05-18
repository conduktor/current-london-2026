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

import java.util.OptionalInt;

/**
 * Parses the client-to-server WebSocket text frames on {@code /v1/topics/{topic}/subscribe}.
 *
 * <p>Two message shapes:
 * <pre>
 * {"type":"subscribe","partition":&lt;int&gt;,"offset":&lt;long&gt;,"initialCredits":&lt;int&gt;[,"maxBytes":&lt;int&gt;]}
 * {"type":"flow","credits":&lt;int&gt;}
 * </pre>
 *
 * <p>The first frame on a connection must be {@code subscribe}; every subsequent frame must be
 * {@code flow}. Ordering is enforced by the endpoint, not the parser.
 *
 * <p>Validation failures raise {@link BadMessageException}, which the endpoint turns into a
 * {@code 1003 / Unsupported Data} close frame with the {@code errorCode}/{@code errorMessage} envelope.
 * The {@code type} field is required — there is no permissive "subscribe-by-shape" fallback so a
 * stray flow-shaped first frame fails loudly instead of being misinterpreted.
 */
public final class WsSubscribeMessageParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private WsSubscribeMessageParser() {
    }

    public static WsClientMessage parse(String json) {
        if (json == null || json.isEmpty()) {
            throw new BadMessageException("message must be non-empty JSON");
        }
        JsonNode root;
        try {
            root = MAPPER.readTree(json);
        } catch (JsonProcessingException e) {
            throw new BadMessageException("message is not valid JSON: " + e.getOriginalMessage(), e);
        }
        if (root == null || !root.isObject()) {
            throw new BadMessageException("message must be a JSON object");
        }
        JsonNode typeNode = root.get("type");
        if (typeNode == null || typeNode.isNull()) {
            throw new BadMessageException("missing field: type");
        }
        if (!typeNode.isTextual()) {
            throw new BadMessageException("field 'type' must be a string");
        }
        String type = typeNode.asText();
        switch (type) {
            case "subscribe":
                return parseSubscribe(root);
            case "flow":
                return parseFlow(root);
            default:
                throw new BadMessageException(
                    "unknown message type: '" + type + "' (expected 'subscribe' or 'flow')");
        }
    }

    private static WsSubscribeCommand parseSubscribe(JsonNode root) {
        int partition = readNonNegativeInt(root, "partition");
        long offset = readNonNegativeLong(root, "offset");
        int initialCredits = readNonNegativeInt(root, "initialCredits");

        OptionalInt maxBytes;
        JsonNode maxBytesNode = root.get("maxBytes");
        if (maxBytesNode == null || maxBytesNode.isNull()) {
            maxBytes = OptionalInt.empty();
        } else {
            if (!maxBytesNode.canConvertToInt()) {
                throw new BadMessageException("field 'maxBytes' must be a positive integer");
            }
            int v = maxBytesNode.asInt();
            if (v <= 0) {
                throw new BadMessageException("field 'maxBytes' must be a positive integer");
            }
            maxBytes = OptionalInt.of(v);
        }
        return new WsSubscribeCommand(partition, offset, initialCredits, maxBytes);
    }

    private static WsFlowCommand parseFlow(JsonNode root) {
        JsonNode creditsNode = root.get("credits");
        if (creditsNode == null || creditsNode.isNull()) {
            throw new BadMessageException("missing field: credits");
        }
        if (!creditsNode.canConvertToInt()) {
            throw new BadMessageException("field 'credits' must be a positive integer");
        }
        int credits = creditsNode.asInt();
        if (credits <= 0) {
            throw new BadMessageException(
                "field 'credits' must be a positive integer (flow messages cannot retract credit)");
        }
        return new WsFlowCommand(credits);
    }

    private static int readNonNegativeInt(JsonNode root, String field) {
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            throw new BadMessageException("missing field: " + field);
        }
        if (!node.canConvertToInt()) {
            throw new BadMessageException("field '" + field + "' must be a non-negative integer");
        }
        int v = node.asInt();
        if (v < 0) {
            throw new BadMessageException("field '" + field + "' must be a non-negative integer");
        }
        return v;
    }

    private static long readNonNegativeLong(JsonNode root, String field) {
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            throw new BadMessageException("missing field: " + field);
        }
        if (!node.canConvertToLong()) {
            throw new BadMessageException("field '" + field + "' must be a non-negative integer");
        }
        long v = node.asLong();
        if (v < 0L) {
            throw new BadMessageException("field '" + field + "' must be a non-negative integer");
        }
        return v;
    }

    // ----- result types -----

    /** Sealed base for the two recognised client-to-server message shapes. */
    public interface WsClientMessage {
    }

    /** First frame on a connection — sets the partition/offset/initial credit budget. */
    public static final class WsSubscribeCommand implements WsClientMessage {
        private final int partition;
        private final long offset;
        private final int initialCredits;
        private final OptionalInt maxBytes;

        public WsSubscribeCommand(int partition, long offset, int initialCredits, OptionalInt maxBytes) {
            this.partition = partition;
            this.offset = offset;
            this.initialCredits = initialCredits;
            this.maxBytes = maxBytes;
        }

        public int partition() {
            return partition;
        }

        public long offset() {
            return offset;
        }

        public int initialCredits() {
            return initialCredits;
        }

        public OptionalInt maxBytes() {
            return maxBytes;
        }
    }

    /** Subsequent frame — adds credit to the in-flight budget. */
    public static final class WsFlowCommand implements WsClientMessage {
        private final int credits;

        public WsFlowCommand(int credits) {
            this.credits = credits;
        }

        public int credits() {
            return credits;
        }
    }

    /** Thrown when a frame fails validation. The endpoint maps this to a 1003 close. */
    public static final class BadMessageException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public BadMessageException(String message) {
            super(message);
        }

        public BadMessageException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
