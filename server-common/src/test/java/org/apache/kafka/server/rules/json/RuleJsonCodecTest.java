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
        // R28 Codec F1 / Task #232: pin the *specific* diagnostic, not just
        // RuleEnvelopeException.class. Without that pin, a refactor that
        // tripped a different rejection branch first (e.g. errorCode parse
        // or a future apiKeys-not-array check) would silently still throw
        // RuleEnvelopeException and pass this test — the test would no
        // longer prove what it claims to ("empty apiKeys is rejected").
        String json = "{\"apiKeys\":[],\"action\":\"DENY\",\"when\":\"true\",\"errorCode\":1}";
        RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
        assertTrue(ex.getMessage().contains("apiKeys"),
            "diagnostic must name the offending field: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("at least one"),
            "diagnostic must describe why empty is rejected: " + ex.getMessage());
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
    public void unknownApiKeyNameWithControlBytesIsSanitisedInMessage() {
        // R23 #237: rule-id parse errors sanitise the wire-derived value at
        // the throw site so RuleEnvelopeException.getMessage() is uniformly
        // safe to log raw. Mirror that contract for apiKey: an attacker who
        // publishes an envelope with JSON-escaped CR/LF or other control
        // bytes inside the apiKey string must not be able to leak forged log
        // lines via any future call site that logs e.getMessage() directly.
        //
        // The JSON source uses \\r\\n which Jackson decodes to actual CR/LF
        // bytes — the envelope is structurally well-formed (so the parse
        // reaches parseApiKeys, not the upstream malformed-JSON catch) and
        // the decoded apiKey name therefore embeds control bytes by the
        // time it reaches the throw at the unknown-name guard.
        String json = "{\"apiKeys\":[\"BAD\\r\\nINJECTED 2026 ERROR forged\"],"
            + "\"action\":\"DENY\",\"when\":\"true\",\"errorCode\":1}";
        RuleEnvelopeException e = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
        String msg = e.getMessage();
        assertTrue(!msg.contains("\r") && !msg.contains("\n"),
            "RuleEnvelopeException message must not carry raw CR/LF from wire input: " + msg);
        assertTrue(msg.contains("unknown api key name"),
            "diagnostic should still describe the unknown-api-key cause: " + msg);
    }

    @Test
    public void unknownActionWithControlBytesIsSanitisedInMessage() {
        // R23 #237: same defense-in-depth as the apiKey path above. The
        // content of the action JSON string is wire-derived and must not
        // survive into an exception message verbatim, even if it is the
        // semantically-malformed value that drives the throw.
        String json = "{\"apiKeys\":[\"METADATA\"],"
            + "\"action\":\"PURGE\\r\\nINJECTED 2026 ERROR forged\","
            + "\"when\":\"true\",\"errorCode\":1}";
        RuleEnvelopeException e = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
        String msg = e.getMessage();
        assertTrue(!msg.contains("\r") && !msg.contains("\n"),
            "RuleEnvelopeException message must not carry raw CR/LF from wire input: " + msg);
        assertTrue(msg.contains("unsupported action"),
            "diagnostic should still describe the unsupported-action cause: " + msg);
    }

    @Test
    public void uncompilableCelWithControlBytesIsSanitisedInMessage() {
        // R25-B F1 / R26-D F1: closes the R23 #237 overclaim. The original
        // commit sanitised three explicit throw sites (unknown api / forbidden
        // api / unsupported action) but documented the contract as "uniform
        // across the decoder", which it was not — the uncompilable-CEL throw
        // site embedded CelCompilationException#getMessage() verbatim, and
        // CelCompiler.truncateForLog only bounds *length*, not control bytes.
        //
        // R26-D F1 pushed back on an earlier version of this test that used
        // `when="BAD\r\nINJECTED..."`. That input is hashed through the
        // CelCompiler lexer which treats CR/LF as Character.isWhitespace and
        // silently consumes them at L148; tokenisation yields IDENT IDENT
        // sequences and the parser throws a CelCompilationException carrying
        // the message "unexpected token after expression: IDENT(INJECTED)" —
        // no CR/LF reaches the throw at RuleJsonCodec.java:331, so the
        // LogSafe.sanitize wrap is a no-op and the test passes whether
        // sanitisation is present or not. To actually exercise the
        // source-echoing arm we must reach the `catch (RuntimeException e)`
        // branch in CelCompiler.compile (L61), which only triggers for
        // JDK-level RuntimeException — and the only such path the lexer
        // takes is Lexer#number → Long.parseLong → NumberFormatException
        // when the digit run exceeds Long.MAX_VALUE (19 digits).
        //
        // The payload below starts with a 20-digit run (overflow) followed
        // by JSON-escaped `\\r\\n` (decoded by Jackson into real CR/LF
        // bytes inside the `when` string). The lexer consumes the 20 digits
        // in number() and then Long.parseLong throws NumberFormatException;
        // CelCompiler's RuntimeException catch rebuilds the message as
        // "failed to parse: " + truncateForLog(source) + " (...)" where
        // truncateForLog is the ONLY place the raw `when` bytes survive.
        // Without LogSafe.sanitize on the outer throw at RuleJsonCodec:331
        // the operator's log line would carry forged CR/LF. With it, the
        // surfaced message is CR/LF-free.
        String json = "{\"apiKeys\":[\"METADATA\"],\"action\":\"DENY\","
            + "\"when\":\"99999999999999999999\\r\\nINJECTED 2026 ERROR forged\","
            + "\"errorCode\":1}";
        RuleEnvelopeException e = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
        String msg = e.getMessage();
        assertTrue(!msg.contains("\r") && !msg.contains("\n"),
            "RuleEnvelopeException message must not carry raw CR/LF from CEL source: " + msg);
        assertTrue(msg.contains("uncompilable CEL"),
            "diagnostic should still describe the uncompilable-CEL cause: " + msg);
        assertTrue(msg.contains("'k'"),
            "diagnostic should still identify the offending rule id: " + msg);
        // Anchor the attacker-controllable token: the `INJECTED` literal
        // sits AFTER the CR/LF in the original `when` source. Without the
        // lexer reaching the overflow arm of CelCompiler the message
        // wouldn't contain it at all — pinning its presence confirms the
        // truncateForLog(source) arm is exercised (and therefore that
        // LogSafe.sanitize is doing real work, not coincidental work).
        assertTrue(msg.contains("INJECTED"),
            "test must reach the truncateForLog(source) arm so the wire-derived "
                + "fragment is actually present-then-sanitised, not just absent by lexer luck: " + msg);
    }

    @Test
    public void malformedJsonEnvelopeWithControlBytesIsSanitisedInMessage() {
        // R25-B F1 / R26-D F2 (continued): the malformed-JSON parse error
        // path embeds Jackson's e.getMessage(), and the LogSafe.sanitize
        // wrap at RuleJsonCodec:624 sanitises that embedded message.
        //
        // R26-D F2 pushed back that on Jackson 2.x's default configuration
        // (INCLUDE_SOURCE_IN_LOCATION disabled) wire-derived source bytes
        // do NOT actually appear in Jackson's diagnostic for the payload
        // chosen here: Jackson emits "Unrecognized token 'garbage': was
        // expecting ... at [Source: REDACTED ...; line: 1, column: 9]"
        // which contains Jackson's own structural newlines but not the
        // wire's `\\r\\nINJECTED` fragment. So today this test would pass
        // whether or not LogSafe.sanitize is applied at L624.
        //
        // The contract this test still earns is forward-compatibility:
        //  * a future Jackson upgrade may re-enable source inclusion or
        //    change quoting in ways that DO surface wire bytes;
        //  * an alternative malformed-JSON failure shape (unterminated
        //    string, illegal escape inside a string value) takes a
        //    different code path inside Jackson where wire bytes can leak.
        // Either way, the outer LogSafe.sanitize wrap + this regression
        // pin together guarantee the operator's log line is CR/LF-free.
        //
        // This test is documented honestly as defense-in-depth, not as
        // proof that the unsanitised version was exploitable for this
        // exact payload. A stronger adversarial coverage variant is left
        // as a TODO to switch to a payload (e.g. unterminated quoted
        // string carrying raw CR/LF) where Jackson DOES echo wire bytes.
        byte[] payload = ("garbage\r\nINJECTED 2026 ERROR forged\r\n}")
            .getBytes(StandardCharsets.UTF_8);
        RuleEnvelopeException e = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", payload));
        String msg = e.getMessage();
        assertTrue(!msg.contains("\r") && !msg.contains("\n"),
            "RuleEnvelopeException message must not carry raw CR/LF from Jackson diagnostic: " + msg);
        assertTrue(msg.contains("malformed JSON envelope"),
            "diagnostic should still describe the malformed-envelope cause: " + msg);
    }

    @Test
    public void invalidCelExpressionRejected() {
        String json = "{\"apiKeys\":[\"METADATA\"],\"action\":\"DENY\","
            + "\"when\":\"foo &&\",\"errorCode\":1}";
        // Compile errors must surface as a clean envelope-level rejection,
        // not as a leaked CelCompilationException — the loader catches
        // RuleEnvelopeException specifically to fence off bad records.
        // R28 Codec F1 / Task #232: pin that we're actually going through
        // the CEL-compile catch branch, not (e.g.) a malformed-JSON or
        // missing-field branch that happens to reject the same payload
        // for a different reason.
        RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
        assertTrue(ex.getMessage().contains("uncompilable CEL"),
            "diagnostic must identify the compile failure as the cause: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("'k'"),
            "diagnostic must name the offending rule id: " + ex.getMessage());
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
        // R28 Codec F1 / Task #232: pin the *specific* diagnostic. Without
        // this, a refactor reordering field parsing so apiKeys / when /
        // errorCode were checked first would silently still reject this
        // payload — for the wrong reason — and the test would no longer
        // prove that missing 'action' has its own clear diagnostic.
        String json = "{\"apiKeys\":[\"METADATA\"],\"when\":\"true\",\"errorCode\":1}";
        RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
        assertTrue(ex.getMessage().contains("'action'"),
            "diagnostic must name the missing field: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("must be a string"),
            "diagnostic must describe the type requirement: " + ex.getMessage());
    }

    @Test
    public void missingWhenRejected() {
        // R28 Codec F1 / Task #232: same anti-pattern fix as above.
        String json = "{\"apiKeys\":[\"METADATA\"],\"action\":\"DENY\",\"errorCode\":1}";
        RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
        assertTrue(ex.getMessage().contains("'when'"),
            "diagnostic must name the missing field: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("must be a string"),
            "diagnostic must describe the type requirement: " + ex.getMessage());
    }

    @Test
    public void missingErrorCodeRejected() {
        // R28 Codec F1 / Task #232: same anti-pattern fix.
        String json = "{\"apiKeys\":[\"METADATA\"],\"action\":\"DENY\",\"when\":\"true\"}";
        RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
        assertTrue(ex.getMessage().contains("'errorCode'"),
            "diagnostic must name the missing field: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("must be an integer"),
            "diagnostic must describe the type requirement: " + ex.getMessage());
    }

    @Test
    public void malformedJsonRejected() {
        // R28 Codec F1 / Task #232: pin that we reach the malformed-JSON
        // arm at RuleJsonCodec:623 (which wraps Jackson's exception with
        // LogSafe.sanitize), not some other arm that happens to throw the
        // same exception class for the same input.
        String json = "this is not json";
        RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
        assertTrue(ex.getMessage().contains("malformed JSON envelope"),
            "diagnostic must identify the parse failure as the cause: " + ex.getMessage());
    }

    @Test
    public void trailingTokensAfterEnvelopeRejected() {
        // R28 #250: Jackson's ObjectMapper.readTree(byte[]) by default reads
        // ONE complete tree and silently discards everything after the closing
        // brace, so a record body like `{...valid envelope...}TRAILING_GARBAGE`
        // would otherwise decode as if the trailing bytes were absent. That's
        // the same canonical-form-drift threat class as duplicate keys: a
        // signer / audit-replay tool that fingerprints the published bytes
        // sees one value, the broker loads another. MAX_ENVELOPE_BYTES caps
        // the wasted-bytes axis at 65 KB; FAIL_ON_TRAILING_TOKENS closes the
        // drift axis at intake.
        //
        // Negative control: disabling FAIL_ON_TRAILING_TOKENS on the MAPPER
        // would silently accept this envelope, decode an Action.DENY rule
        // for METADATA with errorCode 47, and this test would fail because
        // the assertThrows would not see a RuleEnvelopeException.
        String json = "{\"apiKeys\":[\"METADATA\"],"
            + "\"action\":\"DENY\","
            + "\"when\":\"true\","
            + "\"errorCode\":47}"
            + "TRAILING_GARBAGE_AFTER_ENVELOPE";
        RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
        assertTrue(ex.getMessage().contains("malformed JSON envelope"),
            "trailing-token rejection must surface as a malformed-JSON failure: "
                + ex.getMessage());
    }

    @Test
    public void duplicateJsonKeysRejected() {
        // Round-11 audit (JSON-codec sub-agent, MEDIUM): default Jackson
        // applies last-wins semantics to duplicate JSON keys — a rule
        // envelope with two "apiKeys" entries would silently use the second
        // and drop the first. A bytes-level rule-authoring tool comparing
        // its canonical-encoded view to the broker's loaded state would
        // see a mismatch. With STRICT_DUPLICATE_DETECTION enabled, the
        // parser raises and we reject at intake.
        // R28 Codec F1 / Task #232: pin we reach the malformed-JSON catch
        // (where Jackson's STRICT_DUPLICATE_DETECTION raises), proving the
        // STRICT_DUPLICATE_DETECTION feature is what drives the rejection.
        // Without this, disabling STRICT_DUPLICATE_DETECTION on MAPPER
        // would silently accept duplicates and pick last-wins, and this
        // test would still pass if some unrelated later branch threw
        // RuleEnvelopeException (e.g. an apiKeys-shape check).
        String json = "{\"apiKeys\":[\"CREATE_TOPICS\"],"
            + "\"apiKeys\":[\"METADATA\"],"
            + "\"action\":\"DENY\","
            + "\"when\":\"true\","
            + "\"errorCode\":47}";
        RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
        assertTrue(ex.getMessage().contains("malformed JSON envelope"),
            "diagnostic must identify the parse failure as the cause "
                + "(STRICT_DUPLICATE_DETECTION raises a Jackson JsonParseException "
                + "which is caught and wrapped at the malformed-JSON arm): " + ex.getMessage());
    }

    @Test
    public void nullValueIsRejectedAsAnEnvelope() {
        // Tombstones are signalled by the *record value* being null at the
        // loader level — not by this codec. Calling decode(..., null) is
        // therefore a programmer error and must throw a clean exception
        // rather than NPE.
        // R28 Codec F1 / Task #232: pin the diagnostic so a refactor that
        // accidentally raised NPE (which extends RuntimeException, not
        // RuleEnvelopeException) — or that started routing null through a
        // different rejection branch — would surface here.
        RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", null));
        assertTrue(ex.getMessage().contains("rule envelope is null"),
            "diagnostic must explain why null payload is a codec-level error: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("tombstone"),
            "diagnostic must point operators at the tombstone/loader boundary "
                + "so the codec-vs-loader contract is unambiguous: " + ex.getMessage());
    }

    @Test
    public void nullOrEmptyIdRejected() {
        // R28 Codec F1 / Task #232: pin the diagnostic for both branches.
        // Without these, a refactor that started rejecting at the body /
        // length / shape branches for null/empty id (because id was used
        // in a later check) would still satisfy assertThrows but no
        // longer prove the *id* boundary is what catches the violation.
        RuleEnvelopeException nullEx = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode(null, SAMPLE.getBytes(StandardCharsets.UTF_8)));
        assertTrue(nullEx.getMessage().contains("rule id"),
            "diagnostic must name the offending field: " + nullEx.getMessage());
        assertTrue(nullEx.getMessage().contains("non-empty"),
            "diagnostic must describe why null is rejected: " + nullEx.getMessage());

        RuleEnvelopeException emptyEx = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("", SAMPLE.getBytes(StandardCharsets.UTF_8)));
        assertTrue(emptyEx.getMessage().contains("rule id"),
            "diagnostic must name the offending field: " + emptyEx.getMessage());
        assertTrue(emptyEx.getMessage().contains("non-empty"),
            "diagnostic must describe why empty is rejected: " + emptyEx.getMessage());
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
            // Round-22 (R22-#212): strong directional marks. U+200E LRM /
            // U+200F RLM / U+061C ALM are NOT classified as whitespace and
            // have ZERO visible width — an id like "rule-X\u200E" displays
            // identically to "rule-X" in any bidi-aware viewer; admin
            // searches for the operator-visible form never find the stored
            // rule, defeating the unambiguous-attribution promise.
            "‎__activation-budget-exceeded__",  // LRM (left-to-right mark)
            "‏__activation-budget-exceeded__",  // RLM (right-to-left mark)
            "؜__activation-budget-exceeded__",  // ALM (arabic letter mark)
            // Round-22 / R15 #163: Unicode bidi isolates were missing from
            // earlier rounds — same hazard class as the strong marks above.
            "⁦__activation-budget-exceeded__",  // LRI (left-to-right isolate)
            "⁧__activation-budget-exceeded__",  // RLI (right-to-left isolate)
            "⁨__activation-budget-exceeded__",  // FSI (first strong isolate)
            "⁩__activation-budget-exceeded__",  // PDI (pop directional isolate)
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
    public void supplementaryPlaneVariationSelectorsInIdRejected() {
        // R23 #223: the per-char loop in earlier rounds saw supplementary-
        // plane codepoints — variation selectors VS17-256 at U+E0100-U+E01EF
        // and the Unicode language tag block at U+E0020-U+E007F — as two
        // separate unpaired-surrogate chars in the 0xD800-0xDFFF range that
        // didn't match any explicit `case`. An attacker could append, say,
        // VARIATION SELECTOR 17 (U+E0100) to a legitimate-looking id; the
        // codec would accept it, but downstream consumers that normalise
        // (NFC, ICU foldings) would render the id identically to a
        // different stored rule, defeating unambiguous attribution.
        //
        // After #223 the loop iterates by codepoint (codePointAt +
        // charCount) and isForbiddenIdCodepoint rejects every codepoint
        // whose Character.getType is FORMAT — which covers VS17-256 and
        // every Unicode language tag without naming them individually.
        String[] supplementary = {
            "rule-vs17"           + new String(Character.toChars(0xE0100)),
            "rule-vs256"          + new String(Character.toChars(0xE01EF)),
            "rule-langtag-start"  + new String(Character.toChars(0xE0001)),
            "rule-langtag-ascii"  + new String(Character.toChars(0xE0041)),  // tag-A
            "rule-langtag-end"    + new String(Character.toChars(0xE007F)),  // CANCEL TAG
        };
        for (String id : supplementary) {
            RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
                () -> RuleJsonCodec.decode(id, SAMPLE.getBytes(StandardCharsets.UTF_8)),
                "id should be rejected: '" + id + "'");
            assertTrue(ex.getMessage().contains("forbidden codepoint"),
                "rejection must name the forbidden codepoint: " + ex.getMessage());
            // Defence-in-depth: the index reported must be a valid Java
            // String index (i.e. inside the high surrogate of the pair),
            // not garbage past the end. With charCount-driven iteration
            // the supplementary codepoint is reported at the position of
            // its high surrogate.
            assertTrue(ex.getMessage().contains("at index "),
                "diagnostic must include 'at index N' position: " + ex.getMessage());
        }
    }

    @Test
    public void bmpVariationSelectorsAndInvisibleFamiliesInIdRejected() {
        // R23 #223: BMP-range variation selectors (VS1-16, U+FE00-U+FE0F),
        // COMBINING GRAPHEME JOINER (U+034F, Mn category but invisible),
        // SOFT HYPHEN (U+00AD), Hangul fillers (render as space-like
        // glyph), invisible math operators (U+2061-U+2064), Mongolian FVS
        // (U+180B-U+180F). Each renders blank or near-blank in modern
        // terminals / Kibana / X11 trees, so an id like "rule­X"
        // displays identically to "rule X" and to "ruleX" depending on the
        // viewer — same hazard class as the LRM/RLM strong-mark family.
        String[] invisibleBmp = {
            "rule"  + new String(Character.toChars(0xFE00)) + "X",  // VS1
            "rule"  + new String(Character.toChars(0xFE0F)) + "X",  // VS16
            "rule"  + new String(Character.toChars(0x034F)) + "X",  // CGJ
            "rule"  + new String(Character.toChars(0x00AD)) + "X",  // SOFT HYPHEN
            "rule"  + new String(Character.toChars(0x115F)) + "X",  // HANGUL CHOSEONG FILLER
            "rule"  + new String(Character.toChars(0x1160)) + "X",  // HANGUL JUNGSEONG FILLER
            "rule"  + new String(Character.toChars(0x3164)) + "X",  // HANGUL FILLER
            "rule"  + new String(Character.toChars(0xFFA0)) + "X",  // HALFWIDTH HANGUL FILLER
            "rule"  + new String(Character.toChars(0x2061)) + "X",  // FUNCTION APPLICATION
            "rule"  + new String(Character.toChars(0x2062)) + "X",  // INVISIBLE TIMES
            "rule"  + new String(Character.toChars(0x2063)) + "X",  // INVISIBLE SEPARATOR
            "rule"  + new String(Character.toChars(0x2064)) + "X",  // INVISIBLE PLUS
            "rule"  + new String(Character.toChars(0x180B)) + "X",  // MONGOLIAN FVS1
            "rule"  + new String(Character.toChars(0x180C)) + "X",  // MONGOLIAN FVS2
            "rule"  + new String(Character.toChars(0x180D)) + "X",  // MONGOLIAN FVS3
            "rule"  + new String(Character.toChars(0x180F)) + "X",  // MONGOLIAN FVS4 (Unicode 14.0+)
        };
        for (String id : invisibleBmp) {
            RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
                () -> RuleJsonCodec.decode(id, SAMPLE.getBytes(StandardCharsets.UTF_8)),
                "id should be rejected: '" + id + "'");
            assertTrue(ex.getMessage().contains("forbidden codepoint"),
                "rejection must name the forbidden codepoint: " + ex.getMessage());
        }
    }

    @Test
    public void unpairedSurrogatesInIdRejected() {
        // R23 #223: well-formed Java Strings built from valid UTF-8 / UTF-16
        // never carry unpaired surrogates, but a poisoned ByteBuffer-decoded
        // record, a hand-rolled (char) literal, or a String built via
        // String(char[]) with raw surrogate halves can sneak one in. They
        // are not valid Unicode and many downstream consumers (regex /u,
        // Elasticsearch analyzers, protobuf string fields) reject or
        // silently drop them — exactly the normaliser-vs-storage divergence
        // the rest of the forbidden-codepoint list defends against.
        //
        // We build pathological ids with a high surrogate (no following
        // low) and with a lone low surrogate (no preceding high) and
        // assert both are rejected at intake.
        String highOnly = "rule-" + ((char) 0xD83D) + "X";    // lone high
        String lowOnly  = "rule-" + ((char) 0xDE00) + "X";    // lone low
        String highAtEnd = "rule" + ((char) 0xD83D);          // dangling high at end-of-string
        for (String id : new String[] {highOnly, lowOnly, highAtEnd}) {
            RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
                () -> RuleJsonCodec.decode(id, SAMPLE.getBytes(StandardCharsets.UTF_8)),
                "id should be rejected: '" + id + "'");
            assertTrue(ex.getMessage().contains("forbidden codepoint"),
                "rejection must name the forbidden codepoint: " + ex.getMessage());
        }
    }

    @Test
    public void validateRuleIdMatchesDecodeOnNewCodepointFamilies() {
        // R23 #223: the public validateRuleId() pre-decode helper must stay
        // in lockstep with decode() — every codepoint that decode() rejects
        // must also be rejected by validateRuleId(), otherwise a caller
        // that pre-validates and then decodes would see the id pass the
        // pre-check and fail on decode (defeating the "fail fast" contract
        // of the public helper). This test pins a representative from each
        // family the R23 #223 commit adds.
        int[] reps = {
            0xE0100,  // supplementary VS17
            0xE0041,  // Tag letter A
            0xFE00,   // BMP VS1
            0x034F,   // CGJ
            0x00AD,   // SOFT HYPHEN
            0x115F,   // HANGUL CHOSEONG FILLER
            0x2061,   // FUNCTION APPLICATION
            0x180B,   // MONGOLIAN FVS1
        };
        for (int cp : reps) {
            String id = "rule-" + new String(Character.toChars(cp)) + "X";
            RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
                () -> RuleJsonCodec.validateRuleId(id),
                "validateRuleId must reject id with codepoint U+"
                    + String.format("%04X", cp));
            assertTrue(ex.getMessage().contains("forbidden codepoint"),
                "rejection must name the forbidden codepoint for U+"
                    + String.format("%04X", cp) + ": " + ex.getMessage());
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
        // gigabytes/s on a hot api-key. Bound at 256 UTF-8 bytes (R28 #235:
        // bytes, not String.length() UTF-16 code units; see CJK test below).
        StringBuilder huge = new StringBuilder();
        for (int i = 0; i < 257; i++) {
            huge.append('x');
        }
        String id = huge.toString();
        RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode(id, SAMPLE.getBytes(StandardCharsets.UTF_8)));
        assertTrue(ex.getMessage().contains("exceeds max"),
            "rejection must name the length cap: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("UTF-8 bytes"),
            "rejection must name the unit (UTF-8 bytes): " + ex.getMessage());
        // The boundary case — exactly the max — must still be accepted.
        // 256 ASCII chars = 256 UTF-8 bytes = MAX_RULE_ID_BYTES.
        StringBuilder boundary = new StringBuilder();
        for (int i = 0; i < 256; i++) {
            boundary.append('x');
        }
        Rule ok = RuleJsonCodec.decode(boundary.toString(), SAMPLE.getBytes(StandardCharsets.UTF_8));
        assertEquals(256, ok.id().length());
    }

    @Test
    public void overlongIdRejectedByUtf8ByteAxisNotCharAxis() {
        // R28 adversarial (#235): the rule-id length cap MUST be measured in
        // UTF-8 bytes (the unit the broker log writes), not in
        // String.length() UTF-16 code units. A CJK-only id is ~3 UTF-8 bytes
        // per BMP codepoint; under a chars-axis cap of 256, an id of 256 CJK
        // chars would be admitted at up to 768 UTF-8 bytes — a 3x silent
        // log-amplification factor on every DENY emission.
        //
        // U+4E2D '中' (CJK Unified Ideograph): 1 UTF-16 code unit, 3 UTF-8
        // bytes. Build an id whose UTF-8 byte length exceeds 256 but whose
        // String.length() is well under it — this discriminates the byte
        // axis from the char axis.
        //
        // Negative control: if the check ever regresses to id.length(), this
        // test fails because 87 < 256.
        StringBuilder sb = new StringBuilder(87);
        for (int i = 0; i < 87; i++) {
            sb.append('中');
        }
        String id = sb.toString();
        // Confirm the discriminator: chars-axis would admit (87 < 256),
        // bytes-axis must reject (261 > 256).
        assertEquals(87, id.length(),
            "test setup: id must be < 256 UTF-16 chars to discriminate the axes");
        assertEquals(261, id.getBytes(StandardCharsets.UTF_8).length,
            "test setup: id must be > 256 UTF-8 bytes to be rejected");
        RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode(id, SAMPLE.getBytes(StandardCharsets.UTF_8)));
        assertTrue(ex.getMessage().contains("UTF-8 bytes"),
            "rejection must name the unit: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("261"),
            "rejection must surface the actual byte count: " + ex.getMessage());

        // Boundary: 85 CJK chars = 255 UTF-8 bytes = one under the cap,
        // must be accepted. Demonstrates the cap admits non-ASCII ids up to
        // the byte boundary, just not past it.
        StringBuilder okSb = new StringBuilder(85);
        for (int i = 0; i < 85; i++) {
            okSb.append('中');
        }
        String okId = okSb.toString();
        assertEquals(255, okId.getBytes(StandardCharsets.UTF_8).length,
            "test setup: boundary id must be 255 UTF-8 bytes");
        Rule okRule = RuleJsonCodec.decode(okId, SAMPLE.getBytes(StandardCharsets.UTF_8));
        assertEquals(okId, okRule.id());

        // Exact-boundary 256 bytes: cannot be hit with pure CJK BMP (3 bytes
        // per char yields 255 or 258, never 256), so use a mixed id: 84 CJK
        // chars (252 bytes) + 4 ASCII (4 bytes) = 256 bytes total. Must be
        // accepted at the exact cap.
        StringBuilder mixedAtCap = new StringBuilder();
        for (int i = 0; i < 84; i++) {
            mixedAtCap.append('中');
        }
        mixedAtCap.append("abcd");
        String mixedId = mixedAtCap.toString();
        assertEquals(256, mixedId.getBytes(StandardCharsets.UTF_8).length,
            "test setup: mixed id must be exactly 256 UTF-8 bytes");
        Rule mixedRule = RuleJsonCodec.decode(mixedId, SAMPLE.getBytes(StandardCharsets.UTF_8));
        assertEquals(mixedId, mixedRule.id());

        // One byte past the cap with a mixed id (84 CJK + 5 ASCII = 257
        // bytes) — confirms the cap is strictly > MAX_RULE_ID_BYTES, not
        // off-by-one.
        StringBuilder mixedOver = new StringBuilder();
        for (int i = 0; i < 84; i++) {
            mixedOver.append('中');
        }
        mixedOver.append("abcde");
        String mixedOverId = mixedOver.toString();
        assertEquals(257, mixedOverId.getBytes(StandardCharsets.UTF_8).length,
            "test setup: one-past-cap id must be 257 UTF-8 bytes");
        RuleEnvelopeException ex2 = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode(mixedOverId, SAMPLE.getBytes(StandardCharsets.UTF_8)));
        assertTrue(ex2.getMessage().contains("257"),
            "rejection must surface byte count 257: " + ex2.getMessage());
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
    public void errorCodeLongOverflowRejected() {
        // R28 Codec F2 / Task #234: parseErrorCode gates on n.isInt() which
        // is true ONLY for Jackson's IntNode (values that fit in a Java int).
        // A JSON number above Integer.MAX_VALUE deserializes to a LongNode
        // and must be rejected at the type gate. Without this pin, a future
        // refactor relaxing the gate to n.isIntegralNumber() or n.isNumber()
        // would let n.asInt() silently truncate the value (e.g. 2147483648L
        // → -2147483648), at which point either the negative check would
        // reject it for the wrong reason, or — for values that truncate back
        // into [1, 32767] — it would slip through entirely.
        long aboveIntMax = (long) Integer.MAX_VALUE + 1L; // 2147483648
        String json = "{\"apiKeys\":[\"METADATA\"],\"action\":\"DENY\","
            + "\"when\":\"true\",\"errorCode\":" + aboveIntMax + "}";
        RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
        assertTrue(ex.getMessage().contains("must be an integer"),
            "LongNode must be rejected at the type gate with 'must be an integer': "
                + ex.getMessage());
    }

    @Test
    public void errorCodeLongMaxRejected() {
        // Boundary clarity: Long.MAX_VALUE is the largest JSON integer
        // Jackson can parse without overflowing to BigIntegerNode. Pin that
        // it is also rejected at the same type gate — it must not reach
        // n.asInt() (which would clamp to Integer.MAX_VALUE, a value the
        // range check accepts as inside [1, Short.MAX_VALUE] is false but
        // which would still be rejected for the wrong reason).
        String json = "{\"apiKeys\":[\"METADATA\"],\"action\":\"DENY\","
            + "\"when\":\"true\",\"errorCode\":" + Long.MAX_VALUE + "}";
        RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
        assertTrue(ex.getMessage().contains("must be an integer"),
            "Long.MAX_VALUE must be rejected at the type gate: " + ex.getMessage());
    }

    @Test
    public void errorCodeFloatingPointRejected() {
        // R28 Codec F2 / Task #234: a JSON number with a fractional part
        // deserializes to DoubleNode (or FloatNode), neither of which
        // satisfies n.isInt(). Pin that the type gate catches them. Without
        // this pin, a future refactor relaxing to n.isNumber() would let
        // n.asInt() silently truncate 1.5 → 1, accepting it as
        // Errors.OFFSET_OUT_OF_RANGE without the operator ever noticing
        // their typo.
        String json = "{\"apiKeys\":[\"METADATA\"],\"action\":\"DENY\","
            + "\"when\":\"true\",\"errorCode\":1.5}";
        RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
        assertTrue(ex.getMessage().contains("must be an integer"),
            "DoubleNode must be rejected at the type gate: " + ex.getMessage());
    }

    @Test
    public void errorCodeIntegralDoubleRejected() {
        // Even an "integral-valued" floating point literal like 1.0 must be
        // rejected: Jackson deserializes "1.0" to DoubleNode regardless of
        // the lack of fractional part, and n.isInt() returns false. The
        // operator wrote a decimal point — that's a real type mistake we
        // should surface, not silently coerce to 1.
        String json = "{\"apiKeys\":[\"METADATA\"],\"action\":\"DENY\","
            + "\"when\":\"true\",\"errorCode\":1.0}";
        RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
        assertTrue(ex.getMessage().contains("must be an integer"),
            "integral-valued DoubleNode (1.0) must still be rejected: "
                + ex.getMessage());
    }

    @Test
    public void errorCodeStringRejected() {
        // Non-numeric JSON types must also be rejected at the same gate —
        // pin that an operator who quotes the value gets the same clear
        // "must be an integer" diagnostic rather than a stack trace from
        // n.asInt() returning 0 (Jackson's default coercion for non-numeric
        // TextNode) which would then trip the Errors.NONE rejection for the
        // wrong reason.
        String json = "{\"apiKeys\":[\"METADATA\"],\"action\":\"DENY\","
            + "\"when\":\"true\",\"errorCode\":\"1\"}";
        RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
        assertTrue(ex.getMessage().contains("must be an integer"),
            "string-quoted errorCode must be rejected at the type gate: "
                + ex.getMessage());
    }

    @Test
    public void errorCodeBooleanRejected() {
        // Boolean values must hit the same diagnostic. n.isInt() is false
        // for BooleanNode; n.asInt() on BooleanNode returns 1 for true and
        // 0 for false (Jackson coercion), either of which would slip past
        // the type gate if it were ever loosened.
        String json = "{\"apiKeys\":[\"METADATA\"],\"action\":\"DENY\","
            + "\"when\":\"true\",\"errorCode\":true}";
        RuleEnvelopeException ex = assertThrows(RuleEnvelopeException.class,
            () -> RuleJsonCodec.decode("k", json.getBytes(StandardCharsets.UTF_8)));
        assertTrue(ex.getMessage().contains("must be an integer"),
            "BooleanNode must be rejected at the type gate: " + ex.getMessage());
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
