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
}
