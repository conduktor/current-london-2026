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

    // Hardened against deeply-nested / oversized-string DoS shapes per BridgeJsonMappers. WS text frames are themselves
    // capped at 8 KiB by the endpoint, but the per-frame byte cap alone does not bound parser work per byte (a 1 KiB
    // deep-nested object still blows the stack). The Jackson StreamReadConstraints turn that into a clean 1003 close.
    private static final ObjectMapper MAPPER = BridgeJsonMappers.hardened();

    /**
     * Defence-in-depth caps on the integer fields a client may submit. Without these, a single subscribe frame can drive
     * the broker into a tight loop of {@code fetchResponseMaxBytes}-sized fetches against one partition until the topic
     * is fully drained — a 1:50,000,000 byte amplification per frame byte (see Wave 24 axis L/M findings).
     *
     * <p>Numbers picked so that a well-behaved client is never bothered:
     * <ul>
     *   <li>{@code MAX_INITIAL_CREDITS = 10_000} — far more records than any realistic client would buffer before
     *       starting to ack; a streaming consumer that wants more simply grants further credits as it drains.</li>
     *   <li>{@code MAX_FLOW_CREDITS = 10_000} — same ceiling on each subsequent flow message; an attacker cannot bypass
     *       the initial cap by issuing many huge {@code flow} grants because each one is checked here.</li>
     *   <li>{@code MAX_PER_PARTITION_FETCH_BYTES = 50 * 1024 * 1024} — aligned with the broker's
     *       {@code fetchResponseMaxBytes} (50 MiB) so a client can request as much as the broker will ever return on
     *       one fetch, but no more. Asking for 2 GiB and getting capped at 50 MiB is invisible to a correct client
     *       and protects the broker from the bridge being used as a fetch-amplifier.</li>
     * </ul>
     * The caps are not part of the wire protocol — they raise {@link BadMessageException} → 1003 close frame, the
     * standard validation-failure path. Exposing them as configuration would be welcome in a future revision; for now
     * they are fixed at the parser level because their primary role is "no one byte of attacker input maps to many
     * megabytes of broker work".
     */
    static final int MAX_INITIAL_CREDITS = 10_000;
    static final int MAX_FLOW_CREDITS = 10_000;
    static final int MAX_PER_PARTITION_FETCH_BYTES = 50 * 1024 * 1024;

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
            // Same defect class as the produce-body parse-error sanitisation: the BadMessageException reason text
            // reaches the wire (via KafkaWebSocketEndpoint.closeWithProtocolError → close frame + error envelope),
            // and Jackson's getOriginalMessage() exposes byte offsets / a snippet of the malformed input. Keep the
            // wire-side phrase fixed; the full Jackson diagnostic is captured server-side by the endpoint's debug
            // log line on BadMessageException.
            throw new BadMessageException("message is not valid JSON", e);
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
                    "unknown message type: '" + sanitizeShortPreview(type) + "' (expected 'subscribe' or 'flow')");
        }
    }

    /**
     * Renders an attacker-controlled string fit to echo into a {@link BadMessageException} message. The exception text
     * flows both to the broker log (via {@code LOG.debug} in the endpoint) and to the WebSocket close-frame
     * {@code errorMessage} envelope, so the raw value must not carry CR/LF (log-line forgery on aggregators that parse
     * by line) and must not balloon the close frame.
     *
     * <p>Substitutes a literal {@code '?'} for every C0 control character (0x00–0x1F) and DEL (0x7F); all other code
     * points pass through unchanged so Unicode field values stay readable for clients debugging their integration.
     * Truncates above 32 chars with a trailing {@code ...} marker.
     */
    static String sanitizeShortPreview(String value) {
        if (value == null) {
            return "null";
        }
        int max = Math.min(value.length(), 32);
        StringBuilder sb = new StringBuilder(max + 3);
        for (int i = 0; i < max; i++) {
            char c = value.charAt(i);
            sb.append(c < 0x20 || c == 0x7F ? '?' : c);
        }
        if (value.length() > max) {
            sb.append("...");
        }
        return sb.toString();
    }

    private static WsSubscribeCommand parseSubscribe(JsonNode root) {
        int partition = readNonNegativeInt(root, "partition");
        long offset = readNonNegativeLong(root, "offset");
        int initialCredits = readNonNegativeInt(root, "initialCredits");
        if (initialCredits > MAX_INITIAL_CREDITS) {
            // See MAX_INITIAL_CREDITS javadoc. We do not silently clamp — a clamp would let an attacker who
            // submits initialCredits = 2_000_000_000 still drive the broker as if they had MAX_INITIAL_CREDITS
            // permission, just one fetch loop slower; the explicit rejection forces the misbehaving client
            // to issue a sensible request.
            throw new BadMessageException("field 'initialCredits' exceeds the per-subscription cap of "
                + MAX_INITIAL_CREDITS);
        }

        OptionalInt maxBytes;
        JsonNode maxBytesNode = root.get("maxBytes");
        if (maxBytesNode == null || maxBytesNode.isNull()) {
            maxBytes = OptionalInt.empty();
        } else {
            // canConvertToInt alone would accept fractional doubles like 1.9 and silently truncate
            // via asInt(). For a wire protocol where integers carry semantic weight (credits,
            // byte budgets) we require the JSON value to actually be an integer.
            if (!maxBytesNode.isIntegralNumber() || !maxBytesNode.canConvertToInt()) {
                throw new BadMessageException("field 'maxBytes' must be a positive integer");
            }
            int v = maxBytesNode.asInt();
            if (v <= 0) {
                throw new BadMessageException("field 'maxBytes' must be a positive integer");
            }
            if (v > MAX_PER_PARTITION_FETCH_BYTES) {
                throw new BadMessageException("field 'maxBytes' exceeds the per-fetch cap of "
                    + MAX_PER_PARTITION_FETCH_BYTES);
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
        if (!creditsNode.isIntegralNumber() || !creditsNode.canConvertToInt()) {
            throw new BadMessageException("field 'credits' must be a positive integer");
        }
        int credits = creditsNode.asInt();
        if (credits <= 0) {
            throw new BadMessageException(
                "field 'credits' must be a positive integer (flow messages cannot retract credit)");
        }
        if (credits > MAX_FLOW_CREDITS) {
            // Per-message cap. An attacker can still issue many flow messages over time, but each one is bounded;
            // combined with the WsStreamer's saturating addExact on the credit counter, the steady-state credit
            // is bounded by client patience rather than by one malicious frame.
            throw new BadMessageException("field 'credits' exceeds the per-message cap of " + MAX_FLOW_CREDITS);
        }
        return new WsFlowCommand(credits);
    }

    private static int readNonNegativeInt(JsonNode root, String field) {
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            throw new BadMessageException("missing field: " + field);
        }
        if (!node.isIntegralNumber() || !node.canConvertToInt()) {
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
        if (!node.isIntegralNumber() || !node.canConvertToLong()) {
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
