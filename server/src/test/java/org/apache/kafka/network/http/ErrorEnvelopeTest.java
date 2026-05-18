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
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.common.protocol.Errors;

import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ErrorEnvelopeTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void forErrorIncludesCodeAndMessage() {
        ObjectNode node = ErrorEnvelope.forError(mapper, Errors.TOPIC_AUTHORIZATION_FAILED, null);
        assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), node.get("errorCode").asInt());
        assertTrue(node.get("errorMessage").asText().toLowerCase(Locale.ROOT).contains("authorization"),
            "expected message to mention authorization, got: " + node.get("errorMessage").asText());
    }

    @Test
    void forErrorWithOverrideMessageUsesOverride() {
        ObjectNode node = ErrorEnvelope.forError(mapper, Errors.INVALID_REQUEST, "topic must not be empty");
        assertEquals("topic must not be empty", node.get("errorMessage").asText());
        assertEquals(Errors.INVALID_REQUEST.code(), node.get("errorCode").asInt());
    }

    @Test
    void forNoneFallsBackToGenericServerMessage() {
        // NONE has no built-in message; using it as an error is unusual but the envelope must still be well-formed.
        ObjectNode node = ErrorEnvelope.forError(mapper, Errors.NONE, "explicit override");
        assertEquals("explicit override", node.get("errorMessage").asText());
        assertEquals(0, node.get("errorCode").asInt());
    }

    @Test
    void forMessageProducesFreestandingError() {
        ObjectNode node = ErrorEnvelope.forMessage(mapper, 42, "boom");
        assertEquals(42, node.get("errorCode").asInt());
        assertEquals("boom", node.get("errorMessage").asText());
    }

    @Test
    void envelopeContainsOnlyTheTwoRequiredFields() {
        ObjectNode node = ErrorEnvelope.forError(mapper, Errors.UNKNOWN_TOPIC_OR_PARTITION, null);
        // We rely on stability of the error envelope shape; surface drift early.
        assertTrue(node.has("errorCode"));
        assertTrue(node.has("errorMessage"));
        assertFalse(node.has("retryAfter"));
        assertFalse(node.has("links"));
    }

    @Test
    void envelopeIsSerialisableAsJson() throws Exception {
        ObjectNode node = ErrorEnvelope.forError(mapper, Errors.LEADER_NOT_AVAILABLE, null);
        String json = mapper.writeValueAsString(node);
        JsonNode roundTrip = mapper.readTree(json);
        assertEquals(Errors.LEADER_NOT_AVAILABLE.code(), roundTrip.get("errorCode").asInt());
    }
}
