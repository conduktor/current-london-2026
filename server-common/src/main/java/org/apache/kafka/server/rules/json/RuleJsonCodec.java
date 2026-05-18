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
package org.apache.kafka.server.rules.json;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.server.rules.Rule;
import org.apache.kafka.server.rules.RuleAction;
import org.apache.kafka.server.rules.cel.CelCompilationException;
import org.apache.kafka.server.rules.cel.CelCompiler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

/**
 * JSON {@link Rule} codec for the {@code __governance} compacted topic.
 *
 * <p>The wire envelope is intentionally minimal:
 *
 * <pre>
 *   {
 *     "apiKeys":  ["CREATE_TOPICS", "CREATE_PARTITIONS"],
 *     "action":   "DENY",
 *     "when":     "request.topics.exists(t, t.name.startsWith(\"audit-\"))",
 *     "errorCode": 47
 *   }
 * </pre>
 *
 * <p>The rule's id is the Kafka record key, not part of the body. That keeps
 * tombstones (null-value records keyed by id) expressible from any plain
 * producer — no special framing required.
 *
 * <p>Any structural defect — bad JSON, unknown api-key name, unsupported
 * action, uncompilable CEL, missing required field — surfaces as a
 * {@link RuleEnvelopeException} so the loader can reject this record while
 * keeping the previously-good RuleSet active. We deliberately do NOT use
 * Jackson's databind annotations: keeping the parse manual lets every
 * validation rule live next to the field it constrains, and the error
 * message names the offending value.
 *
 * <p>Unknown top-level fields are tolerated. This is a forward-compatibility
 * affordance: a v2 envelope that adds (say) "priority" should still load
 * cleanly on a v1 broker, dropping the unknown field.
 */
public final class RuleJsonCodec {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String FIELD_API_KEYS = "apiKeys";
    private static final String FIELD_ACTION = "action";
    private static final String FIELD_WHEN = "when";
    private static final String FIELD_ERROR_CODE = "errorCode";

    private RuleJsonCodec() {
    }

    /**
     * Parse a single {@code __governance} record body into a {@link Rule}.
     *
     * @param id the record key (rule id) — must be non-null and non-empty
     * @param value the record value bytes — UTF-8 JSON; must be non-null
     * @throws RuleEnvelopeException on any structural / semantic problem
     */
    public static Rule decode(String id, byte[] value) {
        if (id == null || id.isEmpty()) {
            throw new RuleEnvelopeException("rule id (record key) must be non-empty");
        }
        if (value == null) {
            throw new RuleEnvelopeException("rule envelope is null (tombstones must be handled by the loader, not the codec)");
        }
        JsonNode root = parseJson(value);
        if (!root.isObject()) {
            throw new RuleEnvelopeException("rule envelope must be a JSON object, got " + root.getNodeType());
        }
        List<ApiKeys> apiKeys = parseApiKeys(root.get(FIELD_API_KEYS));
        RuleAction action = parseAction(root.get(FIELD_ACTION));
        String when = parseRequiredString(root, FIELD_WHEN);
        int errorCode = parseRequiredInt(root, FIELD_ERROR_CODE);
        try {
            return new Rule(id, apiKeys, action, when, errorCode, CelCompiler.compile(when));
        } catch (CelCompilationException e) {
            throw new RuleEnvelopeException(
                "rule '" + id + "' has uncompilable CEL: " + e.getMessage(), e);
        }
    }

    /**
     * Render a {@link Rule} back to canonical envelope bytes. Round-trips with
     * {@link #decode(String, byte[])}. Useful for tests and for rule-authoring
     * tooling that consumes the {@code Rule} object model.
     */
    public static byte[] encode(Rule rule) {
        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode keys = root.putArray(FIELD_API_KEYS);
        for (ApiKeys k : rule.apiKeys()) {
            keys.add(k.name());
        }
        root.put(FIELD_ACTION, rule.action().name());
        root.put(FIELD_WHEN, rule.whenSource());
        root.put(FIELD_ERROR_CODE, rule.errorCode());
        try {
            return MAPPER.writeValueAsBytes(root);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to encode rule " + rule.id(), e);
        }
    }

    private static JsonNode parseJson(byte[] value) {
        try {
            return MAPPER.readTree(value);
        } catch (Exception e) {
            throw new RuleEnvelopeException("malformed JSON envelope: " + e.getMessage(), e);
        }
    }

    private static List<ApiKeys> parseApiKeys(JsonNode node) {
        if (node == null || !node.isArray()) {
            throw new RuleEnvelopeException("'apiKeys' must be a JSON array of api-key names");
        }
        if (node.isEmpty()) {
            throw new RuleEnvelopeException("'apiKeys' must contain at least one api-key name");
        }
        List<ApiKeys> out = new ArrayList<>(node.size());
        for (JsonNode el : node) {
            if (!el.isTextual()) {
                throw new RuleEnvelopeException(
                    "'apiKeys' entries must be strings; got " + el.getNodeType());
            }
            String name = el.asText();
            try {
                out.add(ApiKeys.valueOf(name));
            } catch (IllegalArgumentException e) {
                throw new RuleEnvelopeException("unknown api key name: '" + name + "'");
            }
        }
        return out;
    }

    private static RuleAction parseAction(JsonNode node) {
        if (node == null || !node.isTextual()) {
            throw new RuleEnvelopeException("'action' must be a string");
        }
        String s = node.asText();
        try {
            return RuleAction.valueOf(s);
        } catch (IllegalArgumentException e) {
            throw new RuleEnvelopeException(
                "unsupported action '" + s + "' (only DENY is supported in v1)");
        }
    }

    private static String parseRequiredString(JsonNode root, String name) {
        JsonNode n = root.get(name);
        if (n == null || !n.isTextual()) {
            throw new RuleEnvelopeException("'" + name + "' must be a string");
        }
        return n.asText();
    }

    private static int parseRequiredInt(JsonNode root, String name) {
        JsonNode n = root.get(name);
        if (n == null || !n.isInt()) {
            throw new RuleEnvelopeException("'" + name + "' must be an integer");
        }
        return n.asInt();
    }
}
