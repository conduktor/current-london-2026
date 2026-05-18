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
 * Builds the {@code {errorCode, errorMessage}} JSON envelope used by every error response from the HTTP bridge.
 *
 * The spec mandates that {@code errorCode} be the raw Kafka {@link Errors} code, and {@code errorMessage} a
 * human-readable string. Callers may pass an override message for cases where the bridge itself rejected the request
 * (for example, a malformed JSON body) and the Kafka built-in message would be misleading.
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
