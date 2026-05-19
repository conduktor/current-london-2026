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
package org.apache.kafka.server.rules;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.server.rules.cel.CelCompiler;
import org.apache.kafka.server.rules.cel.CelProgram;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RuleTest {

    private static final CelProgram TRUE = CelCompiler.compile("true");

    @Test
    public void carriesAllFields() {
        Rule rule = new Rule("r1", Arrays.asList(ApiKeys.CREATE_TOPICS), RuleAction.DENY, "true", 42, TRUE);
        assertEquals("r1", rule.id());
        assertEquals(Collections.singletonList(ApiKeys.CREATE_TOPICS), rule.apiKeys());
        assertSame(RuleAction.DENY, rule.action());
        assertEquals("true", rule.whenSource());
        assertEquals(42, rule.errorCode());
        assertSame(TRUE, rule.compiled());
    }

    @Test
    public void apiKeysAreCopiedAndImmutable() {
        java.util.List<ApiKeys> input = new java.util.ArrayList<>();
        input.add(ApiKeys.CREATE_TOPICS);
        Rule rule = new Rule("r1", input, RuleAction.DENY, "true", 42, TRUE);
        input.clear();
        assertEquals(1, rule.apiKeys().size());
        assertThrows(UnsupportedOperationException.class, () -> rule.apiKeys().add(ApiKeys.FETCH));
    }

    @Test
    public void rejectsEmptyApiKeys() {
        assertThrows(IllegalArgumentException.class,
            () -> new Rule("r1", Collections.emptyList(), RuleAction.DENY, "true", 42, TRUE));
    }

    @Test
    public void rejectsNullFields() {
        assertThrows(NullPointerException.class,
            () -> new Rule(null, Collections.singletonList(ApiKeys.FETCH), RuleAction.DENY, "true", 42, TRUE));
        assertThrows(NullPointerException.class,
            () -> new Rule("r1", null, RuleAction.DENY, "true", 42, TRUE));
        assertThrows(NullPointerException.class,
            () -> new Rule("r1", Collections.singletonList(ApiKeys.FETCH), null, "true", 42, TRUE));
        assertThrows(NullPointerException.class,
            () -> new Rule("r1", Collections.singletonList(ApiKeys.FETCH), RuleAction.DENY, null, 42, TRUE));
        assertThrows(NullPointerException.class,
            () -> new Rule("r1", Collections.singletonList(ApiKeys.FETCH), RuleAction.DENY, "true", 42, null));
    }

    @Test
    public void rejectsDuplicateApiKeys() {
        // R28 adversarial (#246): the codec dedupes operator input via
        // LinkedHashSet, but the Rule constructor is also called from tests
        // and would otherwise admit [METADATA, METADATA]. A duplicate-bearing
        // list would (i) charge the per-API-key cap by 2, (ii) duplicate the
        // rule in the per-key evaluation list, and (iii) double-charge the
        // per-request CEL step budget when the rule does not fire on the
        // first hit. Surface the caller bug rather than swallow it.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> new Rule("r1",
                Arrays.asList(ApiKeys.METADATA, ApiKeys.METADATA),
                RuleAction.DENY, "true", 42, TRUE));
        assertTrue(ex.getMessage().contains("duplicate"),
            "diagnostic must name the contract violation: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("METADATA"),
            "diagnostic must name the offending apiKey: " + ex.getMessage());

        // Non-adjacent duplicate ("ABC...A...") is caught just as well, since
        // EnumSet.add returns false on the second occurrence regardless of
        // position. Pins that the dedup check is not a lookback-of-1 hack.
        IllegalArgumentException ex2 = assertThrows(IllegalArgumentException.class,
            () -> new Rule("r2",
                Arrays.asList(ApiKeys.METADATA, ApiKeys.FETCH, ApiKeys.METADATA),
                RuleAction.DENY, "true", 42, TRUE));
        assertTrue(ex2.getMessage().contains("METADATA"),
            "non-adjacent duplicate must still surface the apiKey name: "
                + ex2.getMessage());

        // Negative control: a list with no duplicates must still be accepted.
        Rule ok = new Rule("r3",
            Arrays.asList(ApiKeys.METADATA, ApiKeys.FETCH, ApiKeys.PRODUCE),
            RuleAction.DENY, "true", 42, TRUE);
        assertEquals(3, ok.apiKeys().size());
    }

    @Test
    public void equalsAndHashCodeByContent() {
        Rule a = new Rule("r1", Collections.singletonList(ApiKeys.FETCH), RuleAction.DENY, "true", 42, TRUE);
        Rule b = new Rule("r1", Collections.singletonList(ApiKeys.FETCH), RuleAction.DENY, "true", 42, TRUE);
        Rule c = new Rule("r2", Collections.singletonList(ApiKeys.FETCH), RuleAction.DENY, "true", 42, TRUE);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertTrue(!a.equals(c));
    }

    @Test
    public void toStringSanitisesControlBytesInWhenSource() {
        // R34-C-1 [HIGH]: Rule.toString() is a latent log-injection footgun.
        // The CEL lexer's quoted-string path admits raw C0/C1 control bytes
        // (only the small \n \t \r \\ \" \' escape set is special; every
        // other byte between quotes passes through verbatim). Jackson at
        // the JSON layer accepts `` escape sequences and decodes
        // them to actual char 0x1B — so an operator-published rule whose
        // `when` clause contains escaped control bytes will land here
        // with raw control bytes in `whenSource`.
        //
        // No production log site currently invokes Rule.toString(), but
        // any future LOG.warn("rule fired: {}", rule) would dump CR/LF/
        // ANSI/bidi bytes straight into broker.log. This test pins the
        // sanitisation contract on toString() so the latent footgun is
        // structurally closed.
        //
        // The CEL source need not match the compiled program in this
        // test — the constructor stores `whenSource` verbatim and uses
        // `compiled` independently for evaluation. We exercise toString
        // shape only.

        // (a) Raw C0 controls (ESC, BEL, NL embedded as raw char) in
        //     whenSource: toString must not emit them as raw bytes.
        String evilSource = "x == \"foo[2Kbar\nbaz\"";
        Rule rule = new Rule("r1",
            Collections.singletonList(ApiKeys.FETCH),
            RuleAction.DENY, evilSource, 42, TRUE);

        String rendered = rule.toString();
        // The toString output must NOT contain raw control bytes — these
        // are exactly the bytes LogSafe.sanitize escapes/strips. We
        // assert each forbidden raw byte individually so a partial-fix
        // (one byte sanitised, another missed) still trips the test.
        assertEquals(-1, rendered.indexOf(''),
            "raw ESC (U+001B) must not appear in toString(): " + rendered);
        assertEquals(-1, rendered.indexOf(''),
            "raw BEL (U+0007) must not appear in toString(): " + rendered);
        assertEquals(-1, rendered.indexOf('\n'),
            "raw LF must not appear in toString() — log-injection vector: "
                + rendered);

        // (b) Negative control: a clean rule renders normally, including
        //     identifier punctuation. Pins that sanitisation is targeted
        //     and not over-broad.
        Rule clean = new Rule("audit-rule",
            Collections.singletonList(ApiKeys.FETCH),
            RuleAction.DENY, "request.topic == \"audit\"", 42, TRUE);
        String cleanRendered = clean.toString();
        assertTrue(cleanRendered.contains("audit-rule"),
            "clean rule id must pass through toString verbatim: "
                + cleanRendered);
        assertTrue(cleanRendered.contains("request.topic == \\\"audit\\\"")
                || cleanRendered.contains("request.topic == \"audit\""),
            "clean whenSource must pass through (modulo LogSafe quote "
                + "escape) — got: " + cleanRendered);

        // (c) whenSource() accessor still returns the AUTHENTIC source
        //     with raw bytes intact — only toString is sanitised. This
        //     preserves the codec round-trip contract (encode() passes
        //     whenSource through Jackson, which re-escapes control bytes
        //     for the wire) and keeps toString as the only render path
        //     that has to be log-safe.
        assertEquals(evilSource, rule.whenSource(),
            "whenSource() must return the unmodified source — only "
                + "toString() is the sanitised render path");
    }

    @Test
    public void toStringSanitisesControlBytesInIdEvenThoughCodecAlreadyRejects() {
        // R34-C-1 [HIGH] defense-in-depth: the codec's validateRuleId
        // rejects all forbidden codepoints in rule id at intake, so a
        // Rule reaching toString with control bytes in its id is a
        // caller-bug shape (Rule constructor bypassing the codec — only
        // possible from a test or a future internal pathway). Still
        // sanitise so that if such a Rule does materialise, the audit
        // render path stays log-safe rather than re-introducing the
        // exact log-injection the codec gate is supposed to prevent.
        Rule rule = new Rule("ruleid",
            Collections.singletonList(ApiKeys.FETCH),
            RuleAction.DENY, "true", 42, TRUE);
        String rendered = rule.toString();
        assertEquals(-1, rendered.indexOf(''),
            "raw ESC in rule id must not survive toString: " + rendered);
    }
}
