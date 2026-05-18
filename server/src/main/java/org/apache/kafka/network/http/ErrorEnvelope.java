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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.common.protocol.Errors;

/**
 * Builds the {@code {errorCode, errorMessage}} JSON envelope used by every error response surface of the HTTP bridge.
 *
 * <p>PROMPT.md's "every error response carries {@code errorCode} and {@code errorMessage}" contract is on the field
 * <em>names</em>, not their JSON types. Each transport picks the type that's natural to its envelope shape, and the two
 * factory methods reflect that:
 *
 * <ul>
 *   <li>{@link #forError(ObjectMapper, Errors, String)} — used for per-partition fetch/produce entries. {@code errorCode}
 *       is the raw Kafka {@link Errors#code()} integer, so a binary-protocol client and the HTTP client read the same
 *       wire-level identifier for the same broker condition. {@code errorMessage} is the broker's built-in description,
 *       optionally overridden by the bridge when it rejected the request before it reached Kafka.</li>
 *   <li>{@link #forMessage(ObjectMapper, int, String)} — used for top-level HTTP responses (400, 404, 413, 429, 500).
 *       {@code errorCode} is the HTTP status code itself, an integer the client already sees on the status line; mirroring
 *       it into the body lets a client that only logs the body still see what happened.</li>
 * </ul>
 *
 * <p>The streaming surfaces ({@code SseStreamer} error events, {@code WsStreamer} close-frame payloads) deliberately do
 * <strong>not</strong> use this builder: they emit {@code errorCode} as a Kafka error <em>name</em> ({@code String}) like
 * {@code "NOT_LEADER_OR_FOLLOWER"} or a synthetic name like {@code "INTERNAL"} / {@code "BAD_MESSAGE"}. EventSource and
 * WebSocket clients are typically scripted code paths that branch on a label, not the numeric protocol code, so the
 * string form is the ergonomic choice for that audience. The field name is still {@code errorCode}, so the contract is
 * preserved.
 *
 * <p>None of these are interchangeable across surfaces — tests in
 * {@code KafkaHttpServerIntegrationTest} pin each shape, and a refactor that unified them would be a wire-format break.
 */
public final class ErrorEnvelope {

    private static final String GENERIC_FALLBACK = "Unspecified error";

    private ErrorEnvelope() {
    }

    public static ObjectNode forError(ObjectMapper mapper, Errors error, String overrideMessage) {
        ObjectNode node = mapper.createObjectNode();
        node.put("errorCode", error.code());
        node.put("errorMessage", resolveMessage(error, overrideMessage));
        return node;
    }

    public static ObjectNode forMessage(ObjectMapper mapper, int errorCode, String message) {
        ObjectNode node = mapper.createObjectNode();
        node.put("errorCode", errorCode);
        node.put("errorMessage", message == null ? GENERIC_FALLBACK : message);
        return node;
    }

    private static String resolveMessage(Errors error, String overrideMessage) {
        if (overrideMessage != null) {
            return overrideMessage;
        }
        String builtIn = error.message();
        return builtIn != null ? builtIn : GENERIC_FALLBACK;
    }
}
