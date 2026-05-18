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
import org.apache.kafka.server.rules.Rule;
import org.apache.kafka.server.rules.RuleAction;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link RuleJsonCodec} — the boundary between the on-the-wire JSON
 * envelope stored in the {@code __governance} compacted topic and the typed
 * {@link Rule} object the engine consumes.
 *
 * <p>The schema (per PROMPT.md):
 * <pre>
 * {
 *   "apiKeys":  ["CREATE_TOPICS", "CREATE_PARTITIONS"],
 *   "action":   "DENY",
 *   "when":     "request.topics.exists(t, t.name.startsWith(\"audit-\"))",
 *   "errorCode": 47
 * }
 * </pre>
 *
 * <p>The rule's id is the compacted-topic key, not part of the body — that
 * keeps tombstones (null-value records keyed by id) trivially expressible
 * by any plain producer.
 */
public class RuleJsonCodecTest {

    private static final String SAMPLE = "{"
        + "\"apiKeys\":[\"CREATE_TOPICS\"],"
        + "\"action\":\"DENY\","
        + "\"when\":\"request.topics.exists(t, t.name.startsWith(\\\"audit-\\\"))\","
        + "\"errorCode\":47"
        + "}";

    @Test
    public void decodeHappyPath() {
        Rule r = RuleJsonCodec.decode("rule-1", SAMPLE.getBytes(StandardCharsets.UTF_8));
        assertEquals("rule-1", r.id());
        assertEquals(1, r.apiKeys().size());
        assertEquals(ApiKeys.CREATE_TOPICS, r.apiKeys().get(0));
        assertEquals(RuleAction.DENY, r.action());
        assertEquals(47, r.errorCode());
        assertTrue(r.whenSource().contains("audit-"));
        // Compiled program is non-null and ready to evaluate.
        assertEquals(r.whenSource(), r.compiled().source());
    }

    @Test
    public void multiKeyRuleParses() {
        String json = "{\"apiKeys\":[\"CREATE_TOPICS\",\"CREATE_PARTITIONS\"],"
            + "\"action\":\"DENY\",\"when\":\"true\",\"errorCode\":1}";
        Rule r = RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8));
        assertEquals(2, r.apiKeys().size());
        assertEquals(ApiKeys.CREATE_TOPICS, r.apiKeys().get(0));
        assertEquals(ApiKeys.CREATE_PARTITIONS, r.apiKeys().get(1));
    }

    @Test
    public void unknownApiKeyNameRejected() {
        String json = "{\"apiKeys\":[\"NOT_A_REAL_API\"],\"action\":\"DENY\","
            + "\"when\":\"true\",\"errorCode\":1}";
        RuleEnvelopeException e = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
        assertTrue(e.getMessage().contains("NOT_A_REAL_API"),
            "error must name the offending api key: " + e.getMessage());
    }

    @Test
    public void emptyApiKeyListRejected() {
        // A rule with no targets is meaningless and would silently dead-code.
        // Reject at the boundary so the loader replays a clear error.
        String json = "{\"apiKeys\":[],\"action\":\"DENY\",\"when\":\"true\",\"errorCode\":1}";
        assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void unknownActionRejected() {
        // Only DENY is supported in the minimum-viable outcome. ALLOW / FILTER
        // are explicitly stretch goals — and unknown actions like "PURGE" must
        // not silently degrade to DENY or be silently dropped.
        String json = "{\"apiKeys\":[\"METADATA\"],\"action\":\"PURGE\","
            + "\"when\":\"true\",\"errorCode\":1}";
        RuleEnvelopeException e = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
        assertTrue(e.getMessage().contains("PURGE"));
    }

    @Test
    public void invalidCelExpressionRejected() {
        String json = "{\"apiKeys\":[\"METADATA\"],\"action\":\"DENY\","
            + "\"when\":\"foo &&\",\"errorCode\":1}";
        // Compile errors must surface as a clean envelope-level rejection,
        // not as a leaked CelCompilationException — the loader catches
        // RuleEnvelopeException specifically to fence off bad records.
        assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void parseFailureMessageDoesNotEchoFullSource() {
        // Round-11 audit (audit-forgery sub-agent, CRITICAL): the
        // CelCompiler's RuntimeException catch previously embedded the full
        // CEL source verbatim in the failure message. Since MAX_EXPR_LEN is
        // 8192 chars, a deliberately malformed envelope could pin an 8 KB
        // string into every GovernanceLoader WARN log line. The fix
        // truncates the source to ~80 chars + a length annotation.
        //
        // A long all-digit literal triggers Long.parseLong overflow in the
        // lexer (CelCompiler$Lexer.number), which throws NumberFormatException —
        // a RuntimeException — caught at CelCompiler.java's outer catch.
        // This is exactly the path that previously echoed the full source.
        StringBuilder hugeNum = new StringBuilder();
        for (int i = 0; i < 400; i++) {
            hugeNum.append('9');
        }
        String json = "{\"apiKeys\":[\"METADATA\"],\"action\":\"DENY\","
            + "\"when\":\"" + hugeNum + "\",\"errorCode\":1}";
        RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
        String msg = ex.getMessage();
        // Full 400-char source must NOT appear; truncation marker MUST.
        assertTrue(!msg.contains(hugeNum.toString()),
            "log line must not echo full 400-char source: " + msg);
        assertTrue(msg.contains("truncated"),
            "message must signal truncation: " + msg);
        assertTrue(msg.contains("400 chars"),
            "message must report the original length so an operator can correlate: " + msg);
    }

    @Test
    public void missingActionRejected() {
        String json = "{\"apiKeys\":[\"METADATA\"],\"when\":\"true\",\"errorCode\":1}";
        assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void missingWhenRejected() {
        String json = "{\"apiKeys\":[\"METADATA\"],\"action\":\"DENY\",\"errorCode\":1}";
        assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void missingErrorCodeRejected() {
        String json = "{\"apiKeys\":[\"METADATA\"],\"action\":\"DENY\",\"when\":\"true\"}";
        assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void malformedJsonRejected() {
        String json = "this is not json";
        assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void nullValueIsRejectedAsAnEnvelope() {
        // Tombstones are signalled by the *record value* being null at the
        // loader level — not by this codec. Calling decode(..., null) is
        // therefore a programmer error and must throw a clean exception
        // rather than NPE.
        assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", null));
    }

    @Test
    public void nullOrEmptyIdRejected() {
        assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode(null, SAMPLE.getBytes(StandardCharsets.UTF_8)));
        assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("", SAMPLE.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void whitespaceInIdRejected() {
        // Whitespace-padded rule ids are rejected at intake. The codec's
        // reservation check uses raw startsWith/endsWith against the id, so a
        // leading-space variant of the engine sentinel (e.g.
        // " __activation-budget-exceeded__") would otherwise PASS the
        // reservation check (because index 0 is a space, not '_') and land as
        // a normal operator-authored rule. A downstream audit consumer that
        // trims or normalises whitespace on display would then render the
        // operator id identically to the engine sentinel — defeating the
        // unambiguous-attribution promise that the reserved-shape exists to
        // provide. Round-9 codec adversarial finding LOW-1.
        //
        // We probe all the whitespace shapes Character.isWhitespace recognises
        // because the impersonation surface is the union, not just the space
        // form: trim() in Java collapses every isWhitespace codepoint, so an
        // attacker who knows the consumer trims could pick any of these.
        String[] padded = {
            " __activation-budget-exceeded__",        // leading space
            "__activation-budget-exceeded__ ",        // trailing space
            "\t__activation-budget-exceeded__",       // tab prefix
            "\n__activation-budget-exceeded__",       // newline prefix
            "rule with embedded space",               // operator-authored, still rejected
        };
        for (String id : padded) {
            RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
                () -> RuleJsonCodec.decode(id, SAMPLE.getBytes(StandardCharsets.UTF_8)),
                "id should be rejected: '" + id + "'");
            assertTrue(ex.getMessage().contains("forbidden codepoint")
                    || ex.getMessage().contains("whitespace"),
                "rejection must name whitespace/forbidden-codepoint as the cause: " + ex.getMessage());
        }
    }

    @Test
    public void unicodeInvisibleCodepointsInIdRejected() {
        // Round-10 audit (regression-axis sub-agent, MEDIUM): the round-9
        // whitespace check used Character.isWhitespace, which by Java
        // semantics is FALSE for U+00A0 NBSP, U+200B ZWSP, U+FEFF BOM,
        // U+202F NNBSP and others. The round-9 commit claimed to close
        // "any downstream audit consumer that trims, normalises, or
        // renders" — but Python's str.strip() DOES strip NBSP, CSS
        // collapses NBSP on render, Elasticsearch's default analyzer
        // normalises NBSP to ASCII space, and regex \s under
        // UNICODE_CHARACTER_CLASS matches ZWSP/NNBSP/figure-space. An
        // NBSP-prefixed id " __activation-budget-exceeded__" would
        // pass both the old whitespace check AND the __name__
        // reservation (startsWith("__") is false because index 0 is
        // NBSP), then render in those consumers as the engine sentinel.
        // Round-10 widens to Character.isSpaceChar + the explicit
        // zero-width / BOM / bidi-format codepoints below.
        String[] invisible = {
            " __activation-budget-exceeded__",  // NBSP prefix
            "__activation budget-exceeded__",   // NBSP embedded
            " __activation-budget-exceeded__",  // NNBSP prefix
            " __activation-budget-exceeded__",  // FIGURE SPACE prefix
            "​__activation-budget-exceeded__",  // ZWSP prefix
            "‌__activation-budget-exceeded__",  // ZWNJ prefix
            "‍__activation-budget-exceeded__",  // ZWJ prefix
            "﻿__activation-budget-exceeded__",  // BOM prefix
            " __activation-budget-exceeded__",  // LINE SEPARATOR
            " __activation-budget-exceeded__",  // PARAGRAPH SEPARATOR
            "⁠__activation-budget-exceeded__",  // WORD JOINER
            "᠎__activation-budget-exceeded__",  // MONGOLIAN VOWEL SEPARATOR
            "‪__activation-budget-exceeded__",  // LRE (bidi format)
            "‮__activation-budget-exceeded__",  // RLO (bidi format)
            "rule with nbsp",              // embedded NBSP, plain operator id
        };
        for (String id : invisible) {
            RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
                () -> RuleJsonCodec.decode(id, SAMPLE.getBytes(StandardCharsets.UTF_8)),
                "id should be rejected: '" + id + "'");
            assertTrue(ex.getMessage().contains("forbidden codepoint"),
                "rejection must name the forbidden codepoint: " + ex.getMessage());
        }
    }

    @Test
    public void controlCharactersInIdRejected() {
        // Round-11 audit (audit-forgery sub-agent, CRITICAL): rule ids are
        // logged verbatim by every DENY emission in KafkaApis.handle and by
        // every codec/loader rejection. An id containing C0 control codes
        // (ESC, BEL, NUL, BS) or DEL/C1 controls is a log-injection
        // primitive: ESC sequences can clear an operator's terminal and
        // repaint forged audit lines on a tail -f / Kibana-render pipeline,
        // NUL can truncate the id in legacy log shippers, BS can rewrite
        // earlier characters on a terminal. The old isForbiddenIdCodepoint
        // covered whitespace + zero-width + bidi but NOT the C0/C1 control
        // ranges (specifically: ESC 0x1B, BEL 0x07, NUL 0x00 are not
        // isWhitespace under Java semantics; DEL 0x7F is also not).
        //
        // We probe one representative per band — NUL, BEL, BS, ESC, DEL, and
        // a C1 control — plus an in-the-wild attack shape (ESC[2J ESC[H to
        // clear screen and home cursor, followed by a fake audit line).
        String[] hostile = {
            "rule\u0000id",                       // embedded NUL (truncates in legacy log shippers)
            "rule\u0007id",                       // embedded BEL (audible bell in terminal)
            "rule\u0008id",                       // embedded BS (rewrites earlier chars on terminal)
            "rule\u001Bid",                       // embedded ESC (start of ANSI sequence)
            "rule\u007Fid",                       // embedded DEL
            "rule\u0085id",                       // C1 NEL (Next Line)
            "evil\u001B[2J\u001B[H[audit] approved",   // full ANSI attack: clear+home then forged audit
        };
        for (String id : hostile) {
            RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
                () -> RuleJsonCodec.decode(id, SAMPLE.getBytes(StandardCharsets.UTF_8)),
                "id should be rejected: '" + id + "'");
            assertTrue(ex.getMessage().contains("forbidden codepoint"),
                "rejection must name the forbidden codepoint: " + ex.getMessage());
        }
    }

    @Test
    public void overlongIdRejected() {
        // Round-11 audit (audit-forgery sub-agent, CRITICAL): rule ids are
        // Kafka record keys, bounded only by max.message.bytes (default
        // 1 MiB). Every DENY in KafkaApis.handle logs the id verbatim, so
        // a 900 KB id would amplify the broker log by ~900 KB per denial —
        // gigabytes/s on a hot api-key. Bound at 256.
        StringBuilder huge = new StringBuilder();
        for (int i = 0; i < 257; i++) {
            huge.append('x');
        }
        String id = huge.toString();
        RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode(id, SAMPLE.getBytes(StandardCharsets.UTF_8)));
        assertTrue(ex.getMessage().contains("exceeds max"),
            "rejection must name the length cap: " + ex.getMessage());
        // The boundary case — exactly the max — must still be accepted.
        StringBuilder boundary = new StringBuilder();
        for (int i = 0; i < 256; i++) {
            boundary.append('x');
        }
        Rule ok = RuleJsonCodec.decode(boundary.toString(), SAMPLE.getBytes(StandardCharsets.UTF_8));
        assertEquals(256, ok.id().length());
    }

    @Test
    public void doubleUnderscoreIdShapeReserved() {
        // The "__name__" rule id shape is reserved for engine-internal
        // synthetic decisions (today: ACTIVATION_BUDGET_RULE_ID, surfaced as
        // RuleDecision.denyingRuleId when ApiMessageActivation overflows its
        // accessor budget and the engine fails closed). Operator rules using
        // this shape must be rejected at intake so audit consumers can
        // attribute a `__…__` denying-rule-id unambiguously to an engine
        // posture, not to an operator-authored rule that picked a colliding id.
        RuleEnvelopeException prefixAndSuffix = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("__activation-budget-exceeded__",
                SAMPLE.getBytes(StandardCharsets.UTF_8)));
        assertTrue(prefixAndSuffix.getMessage().contains("reserved"),
            "rejection message must explain the reservation: " + prefixAndSuffix.getMessage());
        // A single-underscore prefix or a `__` only on one side is fine — only
        // the matching `__…__` shape is reserved. Operators routinely use a
        // leading underscore for "internal" naming and we don't want to over-
        // reach.
        RuleJsonCodec.decode("__only-prefix", SAMPLE.getBytes(StandardCharsets.UTF_8));
        RuleJsonCodec.decode("only-suffix__", SAMPLE.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void encodeRoundTrip() {
        Rule r = RuleJsonCodec.decode("round", SAMPLE.getBytes(StandardCharsets.UTF_8));
        byte[] enc = RuleJsonCodec.encode(r);
        Rule decoded = RuleJsonCodec.decode("round", enc);
        assertEquals(r.id(), decoded.id());
        assertEquals(r.apiKeys(), decoded.apiKeys());
        assertEquals(r.action(), decoded.action());
        assertEquals(r.errorCode(), decoded.errorCode());
        assertEquals(r.whenSource(), decoded.whenSource());
    }

    @Test
    public void oversizedEnvelopeRejectedBeforeParsing() {
        // A single admin-published record at the broker's max.message.bytes
        // would otherwise force the drain thread to allocate a multi-MB
        // JsonNode tree. RuleJsonCodec caps the input size up front; the
        // operator sees a clear error pointing at the rule id, the broker's
        // drain thread never walks the payload.
        StringBuilder pad = new StringBuilder("{\"apiKeys\":[\"METADATA\"],\"action\":\"DENY\","
            + "\"when\":\"true\",\"errorCode\":1,\"padding\":\"");
        for (int n = 0; n < RuleJsonCodec.MAX_ENVELOPE_BYTES + 64; n++) {
            pad.append('x');
        }
        pad.append("\"}");
        RuleEnvelopeException ex = assertThrows(
            RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("huge", pad.toString().getBytes(StandardCharsets.UTF_8)));
        assertTrue(ex.getMessage().contains("max allowed"),
            "expected size-limit error, got: " + ex.getMessage());
    }

    @Test
    public void errorCodeZeroRejected() {
        // errorCode 0 is Errors.NONE — a DENY decision that returns "no error"
        // would fire the rule but fail the request *open* (no exception
        // surfaces to the client). Reject at the parse boundary so the
        // operator sees the misconfiguration replayed as a clear envelope
        // error rather than as a silent fail-open at request time.
        //
        // Round-8 task #99 strengthens this: the rejection must name
        // Errors.NONE specifically — pinning the *semantic* axis ("NONE means
        // fail-open"), not the *syntactic* coincidence ("the wire range
        // happens to start at 1"). If a future refactor ever relaxes the
        // lower bound (e.g. to allow signed Kafka error codes), this test
        // pins the NONE-identity check that prevents 0 from slipping
        // through the known-Errors equality check (which would happily
        // round-trip Errors.NONE).
        String json = "{\"apiKeys\":[\"METADATA\"],\"action\":\"DENY\","
            + "\"when\":\"true\",\"errorCode\":0}";
        RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
        assertTrue(ex.getMessage().contains("errorCode"),
            "error must name the offending field: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("NONE"),
            "error must name Errors.NONE so the rejection is anchored on the "
                + "semantic axis, not the syntactic range bound: " + ex.getMessage());
    }

    @Test
    public void errorCodeAboveShortMaxRejected() {
        // KafkaApis narrows the rule's int errorCode to a short. 65536 → 0
        // → Errors.NONE → silent fail-open. Reject at the parse boundary.
        String json = "{\"apiKeys\":[\"METADATA\"],\"action\":\"DENY\","
            + "\"when\":\"true\",\"errorCode\":65536}";
        RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
        assertTrue(ex.getMessage().contains("65536"),
            "error must name the offending value: " + ex.getMessage());
    }

    @Test
    public void errorCodeNegativeRejected() {
        // Kafka error codes are positive; the wire protocol uses signed
        // shorts but every assigned Errors enum value is positive. Negatives
        // are likely operator typos and would, after narrowing, map to
        // Errors.UNKNOWN_SERVER_ERROR or worse — reject up front.
        String json = "{\"apiKeys\":[\"METADATA\"],\"action\":\"DENY\","
            + "\"when\":\"true\",\"errorCode\":-1}";
        assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void errorCodeAtHighestKnownErrorAccepted() {
        // Boundary: the highest currently-assigned Errors code must round-trip
        // cleanly. Before round-8 task #96 the codec accepted any int in
        // [1, Short.MAX_VALUE] — including codes that mapped to nothing on
        // the wire — so this test used Short.MAX_VALUE (32767). The new
        // contract is "must be a known Errors enum value", so we use the
        // top-end known code. If Kafka adds a new Errors entry above this
        // and this test starts failing, update to the new highest — the
        // intent is to pin the boundary, not freeze a specific number.
        String json = "{\"apiKeys\":[\"METADATA\"],\"action\":\"DENY\","
            + "\"when\":\"true\",\"errorCode\":" + Errors.REBOOTSTRAP_REQUIRED.code() + "}";
        Rule r = RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8));
        assertEquals(Errors.REBOOTSTRAP_REQUIRED.code(), r.errorCode());
    }

    @Test
    public void errorCodeAboveHighestKnownButInShortRangeRejected() {
        // Round-8 task #96: an errorCode in the wire-protocol range
        // [1, Short.MAX_VALUE] that does NOT correspond to any known
        // Errors enum value must be rejected at parse time. Without this
        // check, Errors.forCode((short) 999) folds the value to
        // UNKNOWN_SERVER_ERROR on the wire — the rule fires but clients
        // see a generic server error instead of the operator's intent.
        // The operator typo is invisible until traffic hits a denial.
        String json = "{\"apiKeys\":[\"METADATA\"],\"action\":\"DENY\","
            + "\"when\":\"true\",\"errorCode\":999}";
        RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
        assertTrue(ex.getMessage().contains("999"),
            "error must name the offending value: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("UNKNOWN_SERVER_ERROR"),
            "error must explain the fallback the operator would otherwise see "
                + "on the wire: " + ex.getMessage());
    }

    @Test
    public void errorCodeJustAboveHighestKnownRejected() {
        // Round-8 task #96 boundary: REBOOTSTRAP_REQUIRED is currently the
        // highest assigned Errors code (129). The next int up — 130 — is
        // still inside the wire-protocol range bound and would have been
        // accepted before the known-code check. Pin that the very next
        // step above the known range is rejected; this is the most likely
        // operator typo shape ("I picked an error code one greater than
        // the latest one I saw in the docs").
        int oneAboveHighestKnown = Errors.REBOOTSTRAP_REQUIRED.code() + 1;
        String json = "{\"apiKeys\":[\"METADATA\"],\"action\":\"DENY\","
            + "\"when\":\"true\",\"errorCode\":" + oneAboveHighestKnown + "}";
        RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
        assertTrue(ex.getMessage().contains(String.valueOf(oneAboveHighestKnown)),
            "error must name the offending value: " + ex.getMessage());
    }

    @Test
    public void duplicateApiKeysAreDedupedInDeclaredOrder() {
        // A rule with apiKeys=["FETCH","FETCH",...] would, without dedup,
        // appear N times in RuleSetBuilder's per-API-key list and be
        // evaluated N times per request — a published-rule-shaped DoS amp.
        // We dedupe at the parse boundary while preserving the *first*
        // occurrence's position so the documented "first matching DENY in
        // declared order" semantic still holds across the deduped sequence.
        String json = "{\"apiKeys\":[\"FETCH\",\"METADATA\",\"FETCH\",\"FETCH\",\"METADATA\"],"
            + "\"action\":\"DENY\",\"when\":\"true\",\"errorCode\":1}";
        Rule r = RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8));
        assertEquals(2, r.apiKeys().size(),
            "duplicate api keys must be collapsed: got " + r.apiKeys());
        assertEquals(ApiKeys.FETCH, r.apiKeys().get(0),
            "first declared key must be first after dedup");
        assertEquals(ApiKeys.METADATA, r.apiKeys().get(1),
            "second distinct key must be second after dedup");
    }

    @Test
    public void preAuthAndForwardingApiKeysRejected() {
        // Round-8 BLOCKER: the CEL engine sits at the top of
        // KafkaApis.handle() — a single interception point per PROMPT.md.
        // That gate is shared by every api-key the broker dispatches,
        // including the pre-authentication handshake (API_VERSIONS,
        // SASL_HANDSHAKE, SASL_AUTHENTICATE) and the broker→controller
        // forwarding wire (ENVELOPE). A DENY rule on any of those would
        // soft-brick the cluster (no client can connect; no SASL exchange
        // completes; no admin forwarding lands) with no path to recovery
        // without operator intervention. Reject at intake so the rule never
        // reaches RuleSet.
        for (String name : new String[]{"API_VERSIONS", "SASL_HANDSHAKE", "SASL_AUTHENTICATE", "ENVELOPE"}) {
            String json = "{\"apiKeys\":[\"" + name + "\"],\"action\":\"DENY\","
                + "\"when\":\"true\",\"errorCode\":1}";
            RuleEnvelopeException ex = assertThrows(
                RuleEnvelopeException.class,
                () -> RuleJsonCodec.decode("brick-" + name, json.getBytes(StandardCharsets.UTF_8)),
                "forbidden api-key " + name + " must be rejected at intake");
            assertTrue(ex.getMessage().contains(name),
                "rejection message must name the offending api-key: " + ex.getMessage());
            assertTrue(ex.getMessage().contains("forbidden"),
                "rejection message must explain the contract: " + ex.getMessage());
        }
    }

    @Test
    public void forbiddenApiKeyAmongAllowedKeysStillRejectsWholeEnvelope() {
        // A rule must not silently lose its forbidden entry by partial
        // acceptance: rejecting only the forbidden api-key and keeping the
        // rest would let an operator publish `[METADATA, API_VERSIONS]`
        // and end up with an active METADATA-only rule that they did not
        // explicitly author at that scope. Reject the whole envelope so
        // the operator sees the error replayed verbatim and re-authors.
        String json = "{\"apiKeys\":[\"METADATA\",\"API_VERSIONS\"],"
            + "\"action\":\"DENY\",\"when\":\"true\",\"errorCode\":1}";
        RuleEnvelopeException ex = assertThrows(
            RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("mixed", json.getBytes(StandardCharsets.UTF_8)));
        assertTrue(ex.getMessage().contains("API_VERSIONS"),
            "rejection must name the forbidden entry, not just say \"some key invalid\": " + ex.getMessage());
    }

    @Test
    public void unknownTopLevelFieldsAreToleratedForForwardCompatibility() {
        // Rule envelopes are written by users / tooling. Tolerate unknown
        // top-level fields so v1 brokers don't reject v2-augmented envelopes.
        // (Unknown fields inside required fields like apiKeys are still
        // rejected — see unknownApiKeyNameRejected.)
        String json = "{\"apiKeys\":[\"METADATA\"],\"action\":\"DENY\","
            + "\"when\":\"true\",\"errorCode\":1,\"futureField\":\"ignored\"}";
        Rule r = RuleJsonCodec.decode("fwd", json.getBytes(StandardCharsets.UTF_8));
        assertEquals(RuleAction.DENY, r.action());
    }
}
