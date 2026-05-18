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
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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

    /**
     * Maximum envelope size accepted by {@link #decode(String, byte[])}.
     * A legitimate envelope is well under 1 KB — four fields, with the longest
     * being the CEL source (itself bounded by
     * {@code CelLimits.MAX_EXPR_LEN}=8192) and an apiKeys array (bounded by
     * ApiKeys.values().length). 65 KB leaves three orders of magnitude of
     * headroom for any legitimate authoring tool while bounding the work the
     * broker's drain thread does on a malicious or accidentally-large record
     * before Jackson's parser has to walk it.
     *
     * <p>The cap is a defense-in-depth complement to the broker-side
     * {@code max.message.bytes} (default 1 MiB). Without this cap a single
     * adversarial admin-published record at the broker-config limit would
     * force the drain thread to allocate a multi-MB JsonNode tree before
     * even hitting the per-field validation.
     */
    static final int MAX_ENVELOPE_BYTES = 65 * 1024;

    /**
     * Api-keys on which a DENY rule would brick the cluster — rejected at rule
     * load time. The CEL engine sits at the top of {@code KafkaApis.handle()}
     * (single interception point per PROMPT.md), which means a DENY rule on a
     * pre-authentication api-key short-circuits the request BEFORE the
     * authentication state machine gets to run. The result for an operator is
     * a soft brick: no client can connect, no SASL exchange completes, no
     * control-plane forwarding lands.
     *
     * <p>This is a static, intentionally short list — explicitly enumerated
     * here rather than derived from {@code ApiKeys.clusterAction} or similar
     * because the upstream metadata covers authorisation policy, not "this
     * api-key is part of the handshake itself". The four cases enumerated:
     *
     * <ul>
     *   <li>{@code API_VERSIONS} — the very first request every client sends.
     *       Clients negotiate the protocol version before anything else; a
     *       DENY here means every connection (data-plane producer, consumer,
     *       admin, AND broker replica fetcher) is unable to handshake.</li>
     *   <li>{@code SASL_HANDSHAKE} — selects the SASL mechanism on a
     *       SASL_PLAINTEXT / SASL_SSL listener. A DENY blocks every
     *       authenticating client and the broker's own SASL inter-broker
     *       traffic.</li>
     *   <li>{@code SASL_AUTHENTICATE} — carries the SASL credentials
     *       themselves. A DENY here matches the SASL_HANDSHAKE failure mode
     *       for the same reason.</li>
     *   <li>{@code ENVELOPE} — broker→controller forwarding (KIP-590). The
     *       broker is the sender, the controller's {@code ControllerApis} is
     *       the receiver, so the broker's {@code KafkaApis} does not handle
     *       inbound ENVELOPE today and a DENY rule has no live effect on
     *       this code path. We still reject the rule at intake: it is
     *       operator-confusion-shaped (the operator thinks they are denying
     *       admin forwarding when in fact the rule is a no-op), and the
     *       reservation costs nothing.</li>
     * </ul>
     *
     * <p>The list is small on purpose. We do NOT extend it to "every
     * inter-broker api-key" — that is the privileged-listener bypass's job,
     * and adding more api-keys here would make the deny-list a second,
     * parallel mechanism that drifts from the bypass over time. The contract
     * is narrow: "rules on api-keys whose denial cannot be recovered from
     * without operator intervention". Operators who genuinely need a DENY
     * rule on, say, {@code METADATA} can author one — the privileged-listener
     * bypass protects inter-broker traffic, and external metadata callers
     * surfacing a denial can retry on a different broker or refresh their
     * connection.
     *
     * <p>EnumSet is used so the contains-check on the hot path of rule load
     * is bit-test cheap.
     */
    static final Set<ApiKeys> FORBIDDEN_API_KEYS = EnumSet.of(
        ApiKeys.API_VERSIONS,
        ApiKeys.SASL_HANDSHAKE,
        ApiKeys.SASL_AUTHENTICATE,
        ApiKeys.ENVELOPE);

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
        // Reserve the "__name__" id shape for engine-internal sentinels.
        // RuleEngine.ACTIVATION_BUDGET_RULE_ID is the only one today (used as
        // RuleDecision.denyingRuleId on synthetic fail-closed DENYs from
        // ApiMessageActivation budget overflow), but the convention exists
        // independently so audit consumers can attribute any future internal
        // posture without a collision. Operator rules that respect the
        // convention are blocked here at intake.
        if (id.startsWith("__") && id.endsWith("__")) {
            throw new RuleEnvelopeException(
                "rule id '" + id + "' uses the reserved \"__name__\" shape "
                    + "(double-underscore prefix and suffix); these ids are reserved "
                    + "for engine-internal synthetic decisions and may not be authored "
                    + "by operators");
        }
        if (value == null) {
            throw new RuleEnvelopeException("rule envelope is null (tombstones must be handled by the loader, not the codec)");
        }
        // Reject oversized envelopes before letting Jackson walk them. The
        // broker drain thread allocates a JsonNode tree proportional to the
        // input; capping the input size bounds that allocation regardless of
        // the broker's max.message.bytes configuration.
        if (value.length > MAX_ENVELOPE_BYTES) {
            throw new RuleEnvelopeException(
                "rule envelope is " + value.length + " bytes; max allowed is "
                    + MAX_ENVELOPE_BYTES + " (rule id: '" + id + "')");
        }
        JsonNode root = parseJson(value);
        if (!root.isObject()) {
            throw new RuleEnvelopeException("rule envelope must be a JSON object, got " + root.getNodeType());
        }
        List<ApiKeys> apiKeys = parseApiKeys(root.get(FIELD_API_KEYS));
        RuleAction action = parseAction(root.get(FIELD_ACTION));
        String when = parseRequiredString(root, FIELD_WHEN);
        int errorCode = parseErrorCode(root.get(FIELD_ERROR_CODE));
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
        // Dedupe while preserving declared order. Without this, a rule with
        // apiKeys=["FETCH","FETCH",...] would, after RuleSetBuilder.build(),
        // appear N times in the per-API-key evaluation list and be evaluated
        // N times per request — a published-rule-shaped DoS vector even though
        // the envelope itself is well under MAX_ENVELOPE_BYTES. LinkedHashSet
        // gives us O(1) dedup with stable iteration order so the documented
        // "first matching DENY in declared order" semantics still hold.
        LinkedHashSet<ApiKeys> out = new LinkedHashSet<>(node.size());
        for (JsonNode el : node) {
            if (!el.isTextual()) {
                throw new RuleEnvelopeException(
                    "'apiKeys' entries must be strings; got " + el.getNodeType());
            }
            String name = el.asText();
            ApiKeys parsed;
            try {
                parsed = ApiKeys.valueOf(name);
            } catch (IllegalArgumentException e) {
                throw new RuleEnvelopeException("unknown api key name: '" + name + "'");
            }
            // Pre-auth and broker→controller forwarding api-keys are
            // unrulable: a DENY would brick handshake / control plane (see
            // FORBIDDEN_API_KEYS javadoc). Reject at intake so the rule never
            // lands in the live RuleSet — the operator sees a clear error
            // pointing at the offending key rather than a soft cluster brick.
            if (FORBIDDEN_API_KEYS.contains(parsed)) {
                throw new RuleEnvelopeException(
                    "api key '" + name + "' is on the engine's forbidden list "
                        + "and cannot be the target of a DENY rule. A rule on this "
                        + "api-key would prevent clients from completing the "
                        + "pre-authentication handshake (API_VERSIONS / SASL_*) "
                        + "or block broker→controller forwarding (ENVELOPE), "
                        + "with no path to recovery without operator intervention. "
                        + "Forbidden api-keys: " + FORBIDDEN_API_KEYS);
            }
            out.add(parsed);
        }
        return new ArrayList<>(out);
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

    /**
     * Parse and validate the rule's {@code errorCode}. Kafka error codes are
     * wire-protocol shorts; KafkaApis narrows the rule's int back down to a
     * short via {@code (short) errorCode} when constructing the deny response.
     * Without an explicit range check here, an envelope with
     * {@code "errorCode": 65536} silently truncates to 0 = {@code Errors.NONE}
     * and fails the request <em>open</em> — the rule fires but no error is
     * surfaced to the client.
     *
     * <p>The accepted range is {@code [1, Short.MAX_VALUE]}:
     * <ul>
     *   <li>{@code 0} is reserved for {@code Errors.NONE} — a DENY that returns
     *       "no error" makes no sense and would also fail open.</li>
     *   <li>Negative codes are not valid Kafka error codes — Kafka's wire
     *       protocol uses signed shorts but every assigned {@code Errors} enum
     *       value is positive. Rejecting negatives here is defense-in-depth
     *       against future operators copy-pasting an arbitrary int.</li>
     *   <li>Codes above {@link Short#MAX_VALUE} cannot be expressed on the wire
     *       at all, so the narrowing to short would silently mis-map them.</li>
     * </ul>
     */
    private static int parseErrorCode(JsonNode n) {
        if (n == null || !n.isInt()) {
            throw new RuleEnvelopeException("'" + FIELD_ERROR_CODE + "' must be an integer");
        }
        int code = n.asInt();
        if (code < 1 || code > Short.MAX_VALUE) {
            throw new RuleEnvelopeException(
                "'" + FIELD_ERROR_CODE + "' must be in [1, " + Short.MAX_VALUE
                    + "] (Kafka wire-protocol short error code; 0 is Errors.NONE), got " + code);
        }
        return code;
    }
}
