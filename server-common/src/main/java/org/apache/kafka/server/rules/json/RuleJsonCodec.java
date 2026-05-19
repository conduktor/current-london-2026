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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.server.rules.LogSafe;
import org.apache.kafka.server.rules.Rule;
import org.apache.kafka.server.rules.RuleAction;
import org.apache.kafka.server.rules.cel.CelCompilationException;
import org.apache.kafka.server.rules.cel.CelCompiler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
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

    // Round-11 audit (JSON-codec sub-agent, MEDIUM): enable strict-duplicate
    // detection so an envelope like {"apiKeys":["A"],"apiKeys":["METADATA"]}
    // is rejected rather than silently last-wins. Without this, a rule-
    // authoring tool that inspects the canonical encoded bytes would see
    // ["METADATA"] while the same envelope as authored by a malicious or
    // buggy publisher writes ["A"] in the first key and ["METADATA"] in the
    // second — Jackson's default keeps the last and drops the first without
    // warning. Cheap defence-in-depth at rule-load cadence; no impact on
    // legitimate envelopes which never have duplicate keys.
    //
    // R28 #250: also enable FAIL_ON_TRAILING_TOKENS. By default
    // ObjectMapper.readTree(byte[]) reads ONE complete tree and silently
    // discards everything after the closing brace, so a record like
    // `{...valid envelope...}TRAILING_GARBAGE` would decode as if the
    // garbage were absent. Same canonical-form-drift threat class as the
    // duplicate-keys case: a signer / audit-replay tool that hashes the
    // published bytes would see a different fingerprint than the broker's
    // loaded view. Enabling FAIL_ON_TRAILING_TOKENS makes the second pass
    // of the parser hit the trailing material and throw, which the
    // parseJson catch wraps as "malformed JSON envelope". MAX_ENVELOPE_BYTES
    // already caps the wasted-bytes axis at 65 KB; this closes the
    // canonical-drift axis.
    private static final ObjectMapper MAPPER = new ObjectMapper()
        .configure(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION, true)
        .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS, true);

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
     * Maximum rule-id length accepted by {@link #decode(String, byte[])},
     * measured in <b>UTF-8 bytes</b>. Rule ids are Kafka record keys on the
     * {@code __governance} topic; their only upper bound at the protocol layer
     * is {@code max.message.bytes} (default 1 MiB). Round-11 audit (audit-
     * forgery sub-agent, CRITICAL): an unbounded id is a log-amplification
     * primitive — every DENY emission in {@code KafkaApis.handle} logs the id
     * verbatim, so a 900&#x202F;KB id yields a 900&#x202F;KB log line per
     * denied request, gigabytes per second on a hot api-key.
     *
     * <p><b>R28 adversarial (#235):</b> the cap is measured in UTF-8 bytes,
     * NOT in {@link String#length()} (UTF-16 code units) or codepoints. The
     * threat model is log amplification: broker log lines are written as
     * UTF-8, so the bytes-on-disk cost of an id scales with its UTF-8
     * encoding, not its Java char count. Under a chars-axis cap, a 256-char
     * id of CJK BMP codepoints (3 UTF-8 bytes each) would be admitted at
     * up to 768 bytes — a 3x silent amplification factor — and a 256-char
     * Latin-1-supplement id would be admitted at up to 512 bytes. Measuring
     * in bytes pins the threat axis directly: 256 bytes is exactly what a
     * single emission costs the broker log.
     *
     * <p>256 bytes is comfortably wider than any legitimate operator
     * identifier (file-system path components, audit handles, JIRA ticket
     * shapes — all ASCII in practice, so 1 byte per char) while keeping a
     * single log line cheap. The cap is intake-only: existing well-formed
     * ASCII ids are not affected; non-ASCII ids longer than ~85 CJK chars or
     * ~128 Latin-1-supplement chars are deliberately rejected as
     * indistinguishable from log-amplification probes.
     */
    static final int MAX_RULE_ID_BYTES = 256;

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
        // Round-11 audit (audit-forgery sub-agent, CRITICAL): bound the id
        // length BEFORE any per-char or downstream emit can see it. Rule ids
        // are Kafka record keys, capped only by max.message.bytes (default
        // 1 MiB), so without this check a published 900 KB id would amplify
        // every DENY log line in KafkaApis to ~900 KB — gigabytes/s on a hot
        // api-key. The bound is placed early so the same envelope cannot
        // reach the forbidden-codepoint scan and the engine-internal
        // reservation check with an absurd id either. Operator-authored ids
        // (audit handles, ticket-shaped strings, namespaced names) live well
        // inside 256 bytes; nothing legitimate is affected.
        //
        // R28 adversarial (#235): measure in UTF-8 bytes — the broker log is
        // UTF-8, so bytes-on-disk is the threat axis, not String.length()
        // which counts UTF-16 code units. See MAX_RULE_ID_BYTES docstring.
        int idBytes = id.getBytes(StandardCharsets.UTF_8).length;
        if (idBytes > MAX_RULE_ID_BYTES) {
            throw new RuleEnvelopeException(
                "rule id length " + idBytes + " UTF-8 bytes exceeds max of "
                    + MAX_RULE_ID_BYTES + "; rule ids are operator-authored "
                    + "identifiers (audit handles, ticket numbers, namespaced "
                    + "names) and have no legitimate use for multi-kilobyte "
                    + "strings — every DENY emission logs the id verbatim, so "
                    + "an unbounded id is a log-amplification primitive");
        }
        // Reject any whitespace in the rule id. Rule ids are operator-authored
        // identifiers — log keys, audit handles — and have no legitimate use
        // for embedded whitespace. The reservation check below is anchored on
        // startsWith/endsWith against the raw id, so a leading- or trailing-
        // whitespace variant of the engine sentinel (e.g. " __activation-
        // budget-exceeded__") would pass the reservation check but render
        // identically to the sentinel in any downstream audit consumer that
        // trims or normalises whitespace on display — defeating the unambiguous-
        // attribution promise that the reserved-shape exists to provide.
        // Rejecting whitespace at intake closes this trim-impersonation surface
        // and matches the same axis as the empty-id rejection above (an id
        // composed entirely of whitespace is functionally a non-id).
        // Round-10 audit (regression-axis sub-agent, MEDIUM): the earlier
        // `Character.isWhitespace` check matched Java's strip()/trim()
        // semantics but missed several Unicode codepoints that DOWNSTREAM
        // audit consumers DO collapse — most notably:
        //   - U+00A0  NO-BREAK SPACE             (Python's str.strip(), CSS render)
        //   - U+202F  NARROW NO-BREAK SPACE      (CSS render, many normalisers)
        //   - U+2007  FIGURE SPACE               (CSS render)
        //   - U+200B  ZERO-WIDTH SPACE           (regex \s under UNICODE flag, ES analyzer)
        //   - U+200C  ZERO-WIDTH NON-JOINER      (regex \s under UNICODE flag)
        //   - U+200D  ZERO-WIDTH JOINER          (regex \s under UNICODE flag)
        //   - U+FEFF  ZERO-WIDTH NO-BREAK SPACE / BOM (most normalisers)
        //   - U+180E  MONGOLIAN VOWEL SEPARATOR  (legacy whitespace in some tooling)
        // An id like " __activation-budget-exceeded__" would pass both
        // the old whitespace check (NBSP isWhitespace = false) AND the
        // __name__ reservation check (startsWith("__") is false because
        // index 0 is NBSP), then render in a non-Java audit UI as the
        // engine sentinel — defeating the unambiguous-attribution promise.
        //
        // Two layers of defence:
        //   (a) Reject Character.isWhitespace (ASCII whitespace).
        //   (b) Reject Character.isSpaceChar (Unicode Space_Separator class:
        //       NBSP, NNBSP, EM/EN SPACE, MEDIUM MATH SPACE, IDEOGRAPHIC
        //       SPACE, …).
        //   (c) Reject explicit zero-width / format / BOM codepoints that
        //       no normalisation step preserves but the JVM's
        //       isSpaceChar/isWhitespace do not flag.
        // Operator ids have no legitimate need for ANY of these — they are
        // log keys, not display strings.
        // R23 #223: iterate by codepoint, not by char. A char-indexed walk
        // sees supplementary-plane codepoints (e.g. U+E0100-U+E01EF
        // variation selectors, U+E0020-U+E007F language tags) as two
        // separate UTF-16 code units, each of which is an unpaired
        // surrogate in 0xD800-0xDFFF that does not match any of the
        // historic explicit cases. Codepoint iteration sees the
        // supplementary codepoint as one value and the
        // Character.getType==FORMAT branch in isForbiddenIdCodepoint
        // catches it.
        for (int i = 0; i < id.length(); ) {
            int cp = id.codePointAt(i);
            if (isForbiddenIdCodepoint(cp)) {
                // Round-13 BLOCKER-1: the id is wire-derived and may itself
                // contain the very codepoint we're rejecting. Sanitise it
                // before embedding in the exception message so the WARN
                // slot in GovernanceLoader (which logs e.getMessage()) does
                // not re-introduce control characters into the broker log.
                throw new RuleEnvelopeException(
                    "rule id '" + LogSafe.sanitize(id) + "' contains forbidden codepoint U+"
                        + String.format("%04X", cp) + " at index " + i
                        + "; rule ids may not contain whitespace, zero-width, or BOM "
                        + "characters (operator-authored identifiers have no legitimate "
                        + "use for these, and a Unicode-padded id could be rendered "
                        + "identically to an engine-internal sentinel in audit consumers "
                        + "that normalise on display — Python's str.strip(), CSS rendering, "
                        + "Elasticsearch's default analyzer, and regex \\s under UNICODE "
                        + "flag all collapse these)");
            }
            i += Character.charCount(cp);
        }
        // Reserve the "__name__" id shape for engine-internal sentinels.
        // RuleEngine.ACTIVATION_BUDGET_RULE_ID is the only one today (used as
        // RuleDecision.denyingRuleId on synthetic fail-closed DENYs from
        // ApiMessageActivation budget overflow), but the convention exists
        // independently so audit consumers can attribute any future internal
        // posture without a collision. Operator rules that respect the
        // convention are blocked here at intake.
        if (isReservedNameShape(id)) {
            // Round-13 BLOCKER-1: same as the forbidden-codepoint branch
            // — sanitise the id before embedding, since the loader's
            // rejection WARN logs e.getMessage().
            throw new RuleEnvelopeException(
                "rule id '" + LogSafe.sanitize(id) + "' uses the reserved \"__name__\" shape "
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
            // R25-B F1: CelCompilationException.getMessage() embeds the
            // operator-controlled `when` source via CelCompiler.truncateForLog,
            // which bounds length but NOT control bytes. Wrap with
            // LogSafe.sanitize so the wire-derived fragment cannot inject CR/LF
            // into the SLF4J line a caller writes from this exception. This
            // closes the R23 #237 overclaim: the throw-site contract was
            // documented as "uniform" but two siblings (here and parseJson)
            // were unsanitised.
            throw new RuleEnvelopeException(
                "rule '" + id + "' has uncompilable CEL: " + LogSafe.sanitize(e.getMessage()), e);
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

    /**
     * True when {@code id} uses the engine-reserved "{@code __name__}" shape:
     * a double-underscore prefix AND suffix. These ids are reserved for
     * engine-internal synthetic decisions (today only
     * {@code RuleEngine.ACTIVATION_BUDGET_RULE_ID}, used as the
     * {@code denyingRuleId} on fail-closed DENYs raised by activation-budget
     * overflow) and may not be authored by operators.
     *
     * <p>Exposed (and not inlined) because the same check must be applied on
     * both sides of the {@code __governance} record stream:
     * <ul>
     *   <li>Update records ({@code value != null}) — enforced inside
     *       {@link #decode}.</li>
     *   <li>Tombstone records ({@code value == null}) — enforced by
     *       {@code GovernanceLoader.apply} BEFORE the working-state mutation,
     *       so an operator-published tombstone cannot silently delete a
     *       future engine-internal sentinel rule. Round-12 audit
     *       (tombstone/compaction sub-agent, MEDIUM-1).</li>
     * </ul>
     *
     * <p>{@code null} returns {@code false}; the caller is expected to have
     * already rejected null ids on its own axis (the codec rejects them as
     * empty, the loader as null-key drops).
     */
    public static boolean isReservedNameShape(String id) {
        return id != null && id.startsWith("__") && id.endsWith("__");
    }

    /**
     * Run the operator-authored rule-id contract checks against {@code id}:
     * non-null, non-empty, no longer than {@link #MAX_RULE_ID_BYTES} UTF-8
     * bytes, no forbidden codepoints (C0/C1 controls, whitespace, zero-width,
     * BOM, bidi-format), and not in the engine-reserved {@code __name__}
     * shape. Throws {@link RuleEnvelopeException} on the first violation.
     *
     * <p>Exposed because the same checks must run on BOTH sides of the
     * {@code __governance} record stream:
     *
     * <ul>
     *   <li>Update records ({@code value != null}) — enforced inside {@link
     *       #decode}.</li>
     *   <li>Tombstone records ({@code value == null}) — enforced by
     *       {@code GovernanceLoader.apply} BEFORE the working-state mutation.
     *       <b>Round-23 BLOCKER (task #211):</b> without this call, an
     *       operator-published tombstone whose key is {@code " __activation-
     *       budget-exceeded__"} (leading NBSP) — or any id carrying ANSI
     *       escape bytes, zero-width, or bidi-format codepoints — would render
     *       identically to an engine-internal sentinel in any normalising
     *       audit viewer AND would silently {@code working.remove(key)}
     *       without any check. The trim-impersonation and log-injection
     *       primitives that round-10/11/12 closed on the update path would
     *       re-open on the tombstone path. The 256-char cap also matters on
     *       tombstones: the loader's reject branch logs the key verbatim, so
     *       an unbounded id is a log-amplification primitive on either record
     *       shape.</li>
     * </ul>
     *
     * <p>Today {@link #decode} still has the same inline checks (preserved
     * for the detailed Round-10/11/13 rationale comments next to each one);
     * this method is a thin re-statement of that contract for the
     * pre-decode caller. The two stay in lockstep because each codepoint
     * extension or cap change must land in both, and the test fixtures
     * exercise both surfaces.
     */
    public static void validateRuleId(String id) {
        if (id == null || id.isEmpty()) {
            throw new RuleEnvelopeException("rule id (record key) must be non-empty");
        }
        // R28 adversarial (#235): UTF-8 bytes, mirroring the decode-path
        // check at the top of decode(). Both surfaces stay in lockstep.
        int idBytes = id.getBytes(StandardCharsets.UTF_8).length;
        if (idBytes > MAX_RULE_ID_BYTES) {
            throw new RuleEnvelopeException(
                "rule id length " + idBytes + " UTF-8 bytes exceeds max of "
                    + MAX_RULE_ID_BYTES + "; rule ids are operator-authored "
                    + "identifiers (audit handles, ticket numbers, namespaced "
                    + "names) and have no legitimate use for multi-kilobyte "
                    + "strings — every DENY emission logs the id verbatim, so "
                    + "an unbounded id is a log-amplification primitive");
        }
        // R23 #223: see decode() for the rationale of codepoint iteration —
        // both surfaces must use the same walk so a supplementary-plane
        // variation selector or language tag cannot slip past one of them.
        for (int i = 0; i < id.length(); ) {
            int cp = id.codePointAt(i);
            if (isForbiddenIdCodepoint(cp)) {
                throw new RuleEnvelopeException(
                    "rule id '" + LogSafe.sanitize(id) + "' contains forbidden codepoint U+"
                        + String.format("%04X", cp) + " at index " + i
                        + "; rule ids may not contain whitespace, zero-width, BOM, "
                        + "bidi-format controls, variation selectors, language tags, "
                        + "lone surrogates, or C0/C1 control codepoints "
                        + "(operator-authored identifiers have no legitimate use for "
                        + "these, and a Unicode-padded id could be rendered identically "
                        + "to an engine-internal sentinel in audit consumers that "
                        + "normalise on display)");
            }
            i += Character.charCount(cp);
        }
        if (isReservedNameShape(id)) {
            throw new RuleEnvelopeException(
                "rule id '" + LogSafe.sanitize(id) + "' uses the reserved \"__name__\" shape "
                    + "(double-underscore prefix and suffix); these ids are reserved "
                    + "for engine-internal synthetic decisions and may not be authored "
                    + "by operators");
        }
    }

    /**
     * True when {@code c} is a codepoint forbidden in operator-authored rule
     * ids: ASCII whitespace ({@link Character#isWhitespace}), the broader
     * Unicode Space_Separator class ({@link Character#isSpaceChar} —
     * {@code U+00A0} NBSP, {@code U+202F} NNBSP, {@code U+2007} FIGURE SPACE,
     * and friends), C0 / C1 control codes (Round-11 audit-forgery sub-agent,
     * CRITICAL — ASCII {@code 0x00–0x1F} including BEL/BS/ESC, {@code 0x7F}
     * DEL, and the C1 range {@code 0x80–0x9F}), plus explicit zero-width /
     * BOM / line-separator codepoints that downstream normalisers collapse
     * but the JVM's isWhitespace/isSpaceChar do not flag.
     *
     * <p>The C0/C1 range matters specifically for log-injection: every DENY
     * emission in {@code KafkaApis.handle} writes the id verbatim into the
     * broker log; an id containing {@code ESC[2J ESC[H} clears the operator's
     * terminal and can repaint forged audit lines (impersonating a
     * "WARN [audit] approved by admin@…" entry while suppressing the real
     * denial). NUL truncates id strings in some legacy log shippers
     * (rsyslog, older fluentd parsers). Operator-authored identifiers have
     * no legitimate use for any control codepoint.
     *
     * <p>Centralising the list keeps the codec-intake check and any future
     * audit-log emitter in sync. Adding a codepoint here is a strictly
     * additive constraint — operator ids never contain these.
     */
    private static boolean isForbiddenIdCodepoint(int cp) {
        // C0 controls (0x00-0x1F), DEL (0x7F), C1 controls (0x80-0x9F).
        // Note that several ASCII whitespace codepoints (TAB, LF, VT, FF, CR)
        // are inside this range and would also be flagged by isWhitespace
        // below — but a dedicated range check is cheaper than a per-char
        // method call and pins the log-injection rationale next to the check.
        if (cp <= 0x1F || cp == 0x7F || (cp >= 0x80 && cp <= 0x9F)) {
            return true;
        }
        if (Character.isWhitespace(cp) || Character.isSpaceChar(cp)) {
            return true;
        }
        // R23 #223: lone / unpaired surrogates. A well-formed Java String
        // built from valid UTF-8 / UTF-16 never contains an unpaired
        // surrogate, but a String constructed from a raw byte[] cast, a
        // hand-rolled (char) literal, or a poisoned ByteBuffer-decoded
        // record can carry them. They are not valid Unicode and many
        // downstream consumers (regex /u, Elasticsearch analyzers,
        // protobuf string fields) reject or silently drop them — exactly
        // the normaliser-vs-storage divergence the rest of this list
        // defends against. Reject the entire U+D800-U+DFFF range; valid
        // codepoint iteration would never produce a value here because
        // String.codePointAt always pairs surrogates into supplementary
        // codepoints when the next char is a valid low surrogate, but a
        // dangling high surrogate at end-of-string OR a lone low surrogate
        // would surface here.
        if (cp >= 0xD800 && cp <= 0xDFFF) {
            return true;
        }
        // R23 #223: Character.getType == FORMAT is Unicode's umbrella for
        // codepoints that influence formatting without contributing a
        // visible glyph. This catches a wide swath in one stroke — bidi
        // controls (U+202A-U+202E, U+2066-U+2069), strong bidi marks
        // (U+200E, U+200F, U+061C), zero-width joiners (U+200C, U+200D),
        // WORD JOINER (U+2060), invisible operators (U+2061-U+2064),
        // Mongolian FVS (U+180B-U+180F including U+180E), language tags
        // (U+E0001, U+E0020-U+E007F), SOFT HYPHEN (U+00AD), and others.
        // Future FORMAT additions are automatically covered without a
        // code change. The explicit switch arms below remain for hazard-
        // class documentation (so a reader sees WHY each codepoint is
        // unsafe) and as defence-in-depth in case a JVM ships a stale
        // Unicode data file.
        if (Character.getType(cp) == Character.FORMAT) {
            return true;
        }
        // R23 #223: variation selectors. VS1-16 (U+FE00-U+FE0F) and
        // VS17-256 (U+E0100-U+E01EF) are Unicode-category Mn (Mark,
        // Non-spacing), NOT Cf (FORMAT) — so the umbrella check above
        // does not catch them. They are invisible by definition: a VS
        // appended to any base character requests a font-specific glyph
        // variation but the codepoint itself contributes zero visible
        // width. An id like "rule︀X" displays identically to "ruleX"
        // in any viewer without a variation-selector-aware font, defeating
        // the unambiguous-attribution promise the rest of this list rests
        // on. Reject the entire VS range — operator-authored ids never
        // legitimately use them.
        if ((cp >= 0xFE00 && cp <= 0xFE0F) || (cp >= 0xE0100 && cp <= 0xE01EF)) {
            return true;
        }
        // Mongolian Free Variation Selectors (FVS1-FVS4): U+180B, U+180C,
        // U+180D, U+180F. Same invisible-glyph hazard class as the standard
        // VS ranges above but categorised as Mn (Mark, Non-spacing), so the
        // FORMAT umbrella misses them. Note: U+180E (MONGOLIAN VOWEL
        // SEPARATOR) IS Cf and is already caught by the FORMAT umbrella;
        // it's listed here for completeness of the FVS hazard family.
        if (cp == 0x180B || cp == 0x180C || cp == 0x180D || cp == 0x180F) {
            return true;
        }
        // R23 #223: invisible / confusable codepoints that are NOT in the
        // FORMAT category but still render as nothing or as a space-like
        // glyph in common viewers — same hazard class as the FORMAT
        // controls above.
        switch (cp) {
            case 0x034F: // COMBINING GRAPHEME JOINER (Mn category, invisible)
            case 0x115F: // HANGUL CHOSEONG FILLER (Lo, renders as space)
            case 0x1160: // HANGUL JUNGSEONG FILLER (Lo, renders as space)
            case 0x3164: // HANGUL FILLER (Lo, renders as space)
            case 0xFFA0: // HALFWIDTH HANGUL FILLER (Lo, renders as space)
                return true;
            default:
                // Fall through to the legacy switch below — historic explicit
                // arms for FORMAT codepoints, kept for hazard-class documentation.
                break;
        }
        switch (cp) {
            case '​': // ZERO-WIDTH SPACE
            case '‌': // ZERO-WIDTH NON-JOINER
            case '‍': // ZERO-WIDTH JOINER
            case ' ': // LINE SEPARATOR
            case ' ': // PARAGRAPH SEPARATOR
            case '‪': // LEFT-TO-RIGHT EMBEDDING
            case '‫': // RIGHT-TO-LEFT EMBEDDING
            case '‬': // POP DIRECTIONAL FORMATTING
            case '‭': // LEFT-TO-RIGHT OVERRIDE
            case '‮': // RIGHT-TO-LEFT OVERRIDE
            // Round-22 (Agent 3 R22-#212): strong directional marks. U+200E
            // LRM / U+200F RLM / U+061C ALM are NOT classified as whitespace
            // by either Character.isWhitespace or Character.isSpaceChar and
            // have ZERO visible width. An id like \"rule-X\u200E\" renders
            // identically to \"rule-X\" in any bidi-aware viewer (Kibana,
            // modern terminals, X11 trees); an operator searching the admin
            // tool for \"rule-X\" would never find the stored rule, defeating
            // the unambiguous-attribution property the rest of this list rests
            // on. Same hazard class as the U+202A-U+202E embeddings/overrides.
            case '‎': // LEFT-TO-RIGHT MARK
            case '‏': // RIGHT-TO-LEFT MARK
            case '؜': // ARABIC LETTER MARK
            // Round-22 / R15 #163: Unicode bidi isolates were missing from the
            // earlier round. Same hazard class as the strong marks above and
            // the U+202A-U+202E embeddings/overrides: they reorder display
            // without changing codepoint content. A trailing PDI may legitimately
            // appear alone in non-id text, but operator-authored rule ids have
            // no legitimate use for any directional formatting control, so we
            // reject every isolate codepoint.
            case '⁦': // LEFT-TO-RIGHT ISOLATE
            case '⁧': // RIGHT-TO-LEFT ISOLATE
            case '⁨': // FIRST STRONG ISOLATE
            case '⁩': // POP DIRECTIONAL ISOLATE
            case '⁠': // WORD JOINER
            case '﻿': // ZERO-WIDTH NO-BREAK SPACE / BOM
            case '᠎': // MONGOLIAN VOWEL SEPARATOR
                return true;
            default:
                return false;
        }
    }

    private static JsonNode parseJson(byte[] value) {
        try {
            return MAPPER.readTree(value);
        } catch (Exception e) {
            // R25-B F1: Jackson's exception message echoes the offending
            // source fragment verbatim (e.g. "Unexpected character ('X' …)
            // at [Source: …; line: 1, column: 5]"), which can carry
            // attacker-controlled bytes from the wire envelope. Sanitise the
            // embedded message so a downstream `log.error(e.getMessage())`
            // cannot be coerced into log-line injection. This closes the
            // R23 #237 overclaim alongside the L322 uncompilable-CEL site.
            throw new RuleEnvelopeException(
                "malformed JSON envelope: " + LogSafe.sanitize(e.getMessage()), e);
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
                // R23 #237: rule-id parse errors sanitise the wire-derived
                // value at the throw site (see L269/L294 above); mirror that
                // here so RuleEnvelopeException.getMessage() is safe to log
                // even from a future call site that forgets to wrap with
                // LogSafe. Current log path already sanitises at the log
                // site via sanitizePoisonMessage(), so this is
                // defense-in-depth against a regression where someone logs
                // e.getMessage() directly.
                throw new RuleEnvelopeException(
                    "unknown api key name: '" + LogSafe.sanitize(name) + "'");
            }
            // Pre-auth and broker→controller forwarding api-keys are
            // unrulable: a DENY would brick handshake / control plane (see
            // FORBIDDEN_API_KEYS javadoc). Reject at intake so the rule never
            // lands in the live RuleSet — the operator sees a clear error
            // pointing at the offending key rather than a soft cluster brick.
            if (FORBIDDEN_API_KEYS.contains(parsed)) {
                // R23 #237: defense-in-depth — `name` here is `parsed.name()`
                // (enum constant, charset-safe) since FORBIDDEN_API_KEYS is
                // built from ApiKeys instances, but go through LogSafe so the
                // throw-site contract is uniform: every RuleEnvelopeException
                // message is safe to log raw.
                throw new RuleEnvelopeException(
                    "api key '" + LogSafe.sanitize(name) + "' is on the engine's forbidden list "
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
            // R23 #237: sanitise at the throw site so
            // RuleEnvelopeException.getMessage() is uniformly safe to log
            // raw, mirroring the rule-id parse path (L269/L294).
            throw new RuleEnvelopeException(
                "unsupported action '" + LogSafe.sanitize(s) + "' (only DENY is supported in v1)");
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
     *
     * <p>The value must also map to a known {@link Errors} enum constant — see
     * the round-8 audit's "validate errorCode against the allowed Errors set"
     * finding. Kafka has no runtime per-API allowed-errors registry (the
     * api-key → errors mapping lives only in the response message-spec JSON
     * files as comments), so a literal per-API check would require pinning the
     * engine to a specific Kafka version. The achievable subset is the
     * Errors-known check: an envelope with {@code "errorCode": 999} would
     * otherwise satisfy the range bound but {@link Errors#forCode} silently
     * folds it to {@link Errors#UNKNOWN_SERVER_ERROR} on the wire — the rule
     * fires but the client sees a generic server error rather than the
     * operator's intended denial code. Rejecting unknown codes at load time
     * surfaces the operator typo at rule-publish time rather than at
     * request-time on the broker.
     */
    private static int parseErrorCode(JsonNode n) {
        if (n == null || !n.isInt()) {
            throw new RuleEnvelopeException("'" + FIELD_ERROR_CODE + "' must be an integer");
        }
        int code = n.asInt();
        // Reject Errors.NONE explicitly, before the range bound, on its own
        // semantic axis. Today the `code < 1` bound below also rejects 0, but
        // that conflates "0 is Errors.NONE" (the actual reason) with "the
        // wire-protocol range starts at 1" (a coincidence at the lower bound).
        // The known-Errors equality check at the tail of this method does NOT
        // catch 0 — Errors.forCode((short) 0) returns Errors.NONE which round-
        // trips cleanly through `code() != code`. So if a future refactor ever
        // relaxes the lower bound (e.g. to allow signed Kafka error codes for
        // some controller-only API), 0 would silently pass and we'd be back to
        // the original silent fail-open hazard. Pin the rejection on the NONE
        // identity so it survives that refactor.
        if (code == Errors.NONE.code()) {
            throw new RuleEnvelopeException(
                "'" + FIELD_ERROR_CODE + "' must not be " + Errors.NONE.code()
                    + " (Errors.NONE) — a DENY rule that returns \"no error\" "
                    + "fires the rule but fails the request open (no exception "
                    + "surfaces to the client). Pick a non-NONE code from "
                    + "org.apache.kafka.common.protocol.Errors.");
        }
        if (code < 1 || code > Short.MAX_VALUE) {
            throw new RuleEnvelopeException(
                "'" + FIELD_ERROR_CODE + "' must be in [1, " + Short.MAX_VALUE
                    + "] (Kafka wire-protocol short error code; 0 is Errors.NONE), got " + code);
        }
        // Errors.forCode returns UNKNOWN_SERVER_ERROR (code -1) for unknown
        // codes — equality on the returned enum's code() is the precise
        // "known to Kafka" check that survives any future Errors enum
        // re-ordering or addition. We deliberately do not let the equality
        // pass through for UNKNOWN_SERVER_ERROR itself: its code is -1 which
        // the range bound above has already rejected, so an operator cannot
        // accidentally end up here with code == -1.
        Errors mapped = Errors.forCode((short) code);
        if (mapped.code() != code) {
            throw new RuleEnvelopeException(
                "'" + FIELD_ERROR_CODE + "' " + code + " is not a known Kafka "
                    + "error code (Errors.forCode(" + code + ") returns "
                    + mapped.name() + " by fallback). Pick a code from "
                    + "org.apache.kafka.common.protocol.Errors that matches "
                    + "the denial you want clients to see.");
        }
        return code;
    }
}
