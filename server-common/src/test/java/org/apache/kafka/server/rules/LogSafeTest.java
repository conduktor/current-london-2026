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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-13 BLOCKER-1 / HIGH-1: every wire-derived string about to land in
 * a broker log MUST be control-escaped + length-capped. These tests pin
 * the boundary so a future "small cleanup" of the helper cannot reopen
 * the log-injection surface.
 */
class LogSafeTest {

    @Test
    void nullInputBecomesLiteralNullString() {
        assertEquals("null", LogSafe.sanitize(null));
    }

    @Test
    void emptyStringIsPreserved() {
        assertEquals("", LogSafe.sanitize(""));
    }

    @Test
    void asciiPrintablePassesThrough() {
        String raw = "rule-deny-audit-prefix";
        assertEquals(raw, LogSafe.sanitize(raw));
    }

    @Test
    void escEscapeSequenceIsNeutralised() {
        // ESC [2J is "clear screen" — the round-13 BLOCKER-1 attack
        // payload. Operator reading the log on a terminal would otherwise
        // see the screen wiped.
        String attack = "foo[2Jbar";
        String safe = LogSafe.sanitize(attack);
        assertFalse(safe.contains(""),
            "ESC must be escaped, got: " + safe);
        assertTrue(safe.contains("\\u001B"),
            "ESC must appear as hex escape, got: " + safe);
        assertTrue(safe.contains("foo") && safe.contains("[2Jbar"),
            "printable parts must be preserved, got: " + safe);
    }

    @Test
    void allC0ControlCharsAreEscaped() {
        StringBuilder all = new StringBuilder();
        for (int cp = 0; cp <= 0x1F; cp++) {
            all.appendCodePoint(cp);
        }
        String safe = LogSafe.sanitize(all.toString());
        for (int cp = 0; cp <= 0x1F; cp++) {
            assertFalse(safe.indexOf(cp) >= 0,
                "U+" + String.format("%04X", cp) + " must not appear raw");
        }
    }

    @Test
    void delAndC1AreEscaped() {
        // DEL (0x7F) and the C1 range (0x80..0x9F) are valid Unicode but
        // can still trip log forwarders and terminal renderers.
        String raw = "xyzw";
        String safe = LogSafe.sanitize(raw);
        assertFalse(safe.contains(""));
        assertFalse(safe.contains(""));
        assertFalse(safe.contains(""));
        assertTrue(safe.contains("\\u007F"));
        assertTrue(safe.contains("\\u0085"));
        assertTrue(safe.contains("\\u009F"));
    }

    @Test
    void newlineAndCarriageReturnAreEscaped() {
        // CR/LF are part of the C0 set and are escaped — a raw CR+LF in
        // a log line would let an attacker forge a *new* log line. The
        // grep-injection attack works even without a terminal.
        String attack = "rule-1\r\nWARN forged.audit cluster admin logged in";
        String safe = LogSafe.sanitize(attack);
        assertFalse(safe.contains("\r"));
        assertFalse(safe.contains("\n"));
        assertTrue(safe.contains("\\u000D"));
        assertTrue(safe.contains("\\u000A"));
    }

    @Test
    void tabIsEscaped() {
        assertEquals("a\\u0009b", LogSafe.sanitize("a\tb"));
    }

    @Test
    void truncatedWithLengthAnnotation() {
        // 200 'x' chars — well past MAX_LEN=128 — must truncate, and
        // the annotation must report the ORIGINAL length so operators
        // see how big the attacker payload was.
        StringBuilder big = new StringBuilder();
        for (int i = 0; i < 200; i++) {
            big.append('x');
        }
        String safe = LogSafe.sanitize(big.toString());
        assertTrue(safe.contains("...[truncated, 200 chars]"),
            "annotation must report original length, got: " + safe);
        assertTrue(safe.startsWith("xxxx"));
        // The kept body is exactly MAX_LEN chars before the annotation.
        int annotationAt = safe.indexOf("...[truncated");
        assertEquals(LogSafe.MAX_LEN, annotationAt);
    }

    @Test
    void noTruncationAtBoundary() {
        // Exactly MAX_LEN chars — no truncation annotation.
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < LogSafe.MAX_LEN; i++) {
            s.append('a');
        }
        String safe = LogSafe.sanitize(s.toString());
        assertEquals(LogSafe.MAX_LEN, safe.length());
        assertFalse(safe.contains("truncated"));
    }

    @Test
    void truncationCountsCharsNotEscapedBytes() {
        // 100 ESC chars: each escapes to "\\u001B" (6 chars). The truncation
        // must be by ORIGINAL char count, not output length — otherwise a
        // pathological all-control input would be rendered in full at
        // 6x the cap. The original input is 100 chars < 128, so no
        // truncation occurs even though the output is much longer.
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            s.append('');
        }
        String safe = LogSafe.sanitize(s.toString());
        assertFalse(safe.contains("truncated"));
        assertEquals(600, safe.length()); // 100 × 6 chars per escape
    }

    @Test
    void supplementaryPlaneCodepointPreserved() {
        // Emoji surrogate pair (U+1F600) — printable, non-control,
        // must pass through unchanged and count as ONE codepoint
        // against the length cap, not two chars.
        String emoji = new String(Character.toChars(0x1F600));
        assertEquals(emoji, LogSafe.sanitize(emoji));
    }

    @Test
    void replacementCharPassesThrough() {
        // U+FFFD is the standard "invalid UTF-8" replacement; it's
        // legitimate output from Kafka's wire-decoder and must not be
        // escaped (would just spam logs with �).
        String raw = "client-�-id";
        assertEquals(raw, LogSafe.sanitize(raw));
    }

    @Test
    void loneHighSurrogateIsEscaped() {
        // A lone surrogate (without its mate) is illegal in well-formed
        // UTF-16 but can sneak in via JVM-side decoder bugs. Escape it
        // so it can't confuse downstream log processors.
        String raw = "x" + ((char) 0xD800) + "y"; // lone high surrogate
        String safe = LogSafe.sanitize(raw);
        assertTrue(safe.contains("\\uD800"));
        assertFalse(safe.indexOf(0xD800) >= 0);
    }

    @Test
    void mixedAttackPayloadIsFullyNeutralised() {
        // The combined attack from BLOCKER-1: ESC sequence + CRLF +
        // forged audit line + long padding. Must escape every control
        // codepoint AND truncate the bulk.
        StringBuilder attack = new StringBuilder()
            .append("safe-id")
            .append('').append("[2K[1F WARN [audit] cluster-admin authenticated as root")
            .append('\r').append('\n');
        // Pad past MAX_LEN
        for (int i = 0; i < 200; i++) {
            attack.append('A');
        }
        String safe = LogSafe.sanitize(attack.toString());
        assertFalse(safe.contains(""));
        assertFalse(safe.contains("\r"));
        assertFalse(safe.contains("\n"));
        assertTrue(safe.contains("...[truncated,"));
    }

    // ----- Round-47 wave-C F1: bidi / format / zero-width / invisibles -----

    @Test
    void bidiRloOverrideIsEscaped() {
        // U+202E RIGHT-TO-LEFT OVERRIDE — the canonical identifier-
        // spoofing primitive. Without escape, a hostile clientId
        // appended with reversed-then-RLO bytes renders in a bidi-
        // aware terminal as a completely different identifier,
        // defeating visual identification by the operator.
        String attack = "rule-evil" + "‮" + "rehtgib";
        String safe = LogSafe.sanitize(attack);
        assertFalse(safe.indexOf(0x202E) >= 0, "RLO must not appear raw");
        assertTrue(safe.contains("\\u202E"));
        assertTrue(safe.contains("rule-evil"));
        assertTrue(safe.contains("rehtgib"));
    }

    @Test
    void bidiMarksAndIsolatesAreEscaped() {
        // LRM U+200E, RLM U+200F, ALM U+061C — invisible direction
        // marks that subtly reorder neighbouring runs. LRI U+2066,
        // RLI U+2067, FSI U+2068, PDI U+2069 — isolate variants
        // added in Unicode 6.3. All Cf-class, all caught by the
        // single getType == FORMAT check in isUnsafeForLog.
        String raw = "a‎b‏c؜d⁦e⁧f⁨g⁩h";
        String safe = LogSafe.sanitize(raw);
        assertTrue(safe.contains("\\u200E"));
        assertTrue(safe.contains("\\u200F"));
        assertTrue(safe.contains("\\u061C"));
        assertTrue(safe.contains("\\u2066"));
        assertTrue(safe.contains("\\u2067"));
        assertTrue(safe.contains("\\u2068"));
        assertTrue(safe.contains("\\u2069"));
        // Printable letters still come through.
        assertTrue(safe.startsWith("a"));
        assertTrue(safe.endsWith("h"));
    }

    @Test
    void zeroWidthFamilyIsEscaped() {
        // ZWSP U+200B, ZWNJ U+200C, ZWJ U+200D — render as nothing,
        // letting an attacker split an identifier so it appears the
        // same as a legitimate one. Word joiner U+2060 and the
        // invisible-operator block U+2061..U+2064 are the same
        // hazard at a different codepoint.
        String raw = "evil​‌‍⁠⁡⁢⁣⁤rule";
        String safe = LogSafe.sanitize(raw);
        assertTrue(safe.contains("\\u200B"));
        assertTrue(safe.contains("\\u200C"));
        assertTrue(safe.contains("\\u200D"));
        assertTrue(safe.contains("\\u2060"));
        assertTrue(safe.contains("\\u2061"));
        assertTrue(safe.contains("\\u2062"));
        assertTrue(safe.contains("\\u2063"));
        assertTrue(safe.contains("\\u2064"));
        assertTrue(safe.startsWith("evil") && safe.endsWith("rule"));
    }

    @Test
    void bomIsEscaped() {
        // U+FEFF — Byte Order Mark / Zero-Width No-Break Space.
        // Confuses log parsers and is invisible in terminals.
        String raw = "﻿rule-id";
        String safe = LogSafe.sanitize(raw);
        assertTrue(safe.startsWith("\\uFEFF"));
        assertTrue(safe.endsWith("rule-id"));
    }

    @Test
    void languageTagBlockIsEscaped() {
        // U+E0020 TAG SPACE — supplementary plane Cf-class codepoint.
        // The tag block (U+E0001, U+E0020..U+E007F) lets an attacker
        // smuggle arbitrary text invisibly across terminals that
        // strip them. Also exercises the `\U` escape path for
        // codepoints above the BMP.
        String raw = "rule" + new String(Character.toChars(0xE0020)) + "id";
        String safe = LogSafe.sanitize(raw);
        assertTrue(safe.contains("\\U000E0020"),
            "language tag must be escaped via \\U form, got: " + safe);
        assertTrue(safe.contains("rule") && safe.contains("id"));
    }

    @Test
    void variationSelectorsAreEscaped() {
        // VS16 U+FE0F (the emoji-presentation selector) and VS17
        // U+E0100 (supplementary plane). Mn-class default-ignorable
        // modifiers that mutate rendering of the preceding char.
        // VS1-256 can be chained behind any letter to subtly alter
        // its glyph in an operator's terminal.
        String raw = "x️y" + new String(Character.toChars(0xE0100));
        String safe = LogSafe.sanitize(raw);
        assertTrue(safe.contains("\\uFE0F"));
        assertTrue(safe.contains("\\U000E0100"));
        assertTrue(safe.contains("x") && safe.contains("y"));
    }

    @Test
    void mongolianFvsIsEscaped() {
        // U+180B..U+180D + U+180F — Mongolian free variation
        // selectors. Mn class, zero-width, same identifier-spoofing
        // attack as VS1-256 but lives in a different range.
        String raw = "a᠋b᠌c᠍d᠏e";
        String safe = LogSafe.sanitize(raw);
        assertTrue(safe.contains("\\u180B"));
        assertTrue(safe.contains("\\u180C"));
        assertTrue(safe.contains("\\u180D"));
        assertTrue(safe.contains("\\u180F"));
    }

    @Test
    void cgjIsEscaped() {
        // U+034F COMBINING GRAPHEME JOINER — Mn class, zero-width.
        // No legitimate use in identifiers; its presence in a log
        // line is a strong signal of crafting.
        String raw = "id͏mark";
        String safe = LogSafe.sanitize(raw);
        assertTrue(safe.contains("\\u034F"));
    }

    @Test
    void hangulFillersAreEscaped() {
        // U+115F / U+1160 / U+3164 / U+FFA0 — Lo-class fillers that
        // render zero-width. Lo means the JDK reports them as
        // "letters", which would naturally pass an "is this
        // printable?" check — they need an explicit escape.
        String raw = "idᅟᅠㅤﾠend";
        String safe = LogSafe.sanitize(raw);
        assertTrue(safe.contains("\\u115F"));
        assertTrue(safe.contains("\\u1160"));
        assertTrue(safe.contains("\\u3164"));
        assertTrue(safe.contains("\\uFFA0"));
    }

    @Test
    void khmerInherentVowelsAreEscaped() {
        // U+17B4 / U+17B5 — Mn class, zero-width, default-ignorable.
        // Khmer is rare on the broker boundary but the codepoints
        // are cheap to add to the predicate set.
        String raw = "a឴b឵c";
        String safe = LogSafe.sanitize(raw);
        assertTrue(safe.contains("\\u17B4"));
        assertTrue(safe.contains("\\u17B5"));
    }

    @Test
    void bidiSpoofEndToEnd() {
        // Round-47 wave-C F1 scenario: an attacker registers a
        // clientId or rule-id containing an RLO that mirrors a
        // suffix, then triggers a DENY-path INFO log. Before this
        // fix, the operator's terminal renders the spoof; after,
        // every Cf codepoint is escaped and the spoof becomes
        // a visible literal in the log.
        String spoofedId = "rule-prod" + "‮" + "yrettab";
        String safe = LogSafe.sanitize(spoofedId);
        assertFalse(safe.indexOf(0x202E) >= 0,
            "RLO must not survive sanitisation");
        assertTrue(safe.contains("\\u202E"));
        // The escape stays adjacent to the suffix so a grep for
        // the underlying bytes still finds the offending record.
        assertTrue(safe.contains("rule-prod"));
        assertTrue(safe.contains("yrettab"));
    }
}
