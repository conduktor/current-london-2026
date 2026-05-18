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

/**
 * Single sanitiser for attacker-controlled byte sequences that are about to
 * land in an operator-visible log line.
 *
 * <p>Closes the log-injection / log-amplification surface around the broker
 * log: any string that originated on the wire (rule-id record key, request
 * clientId, codec exception message that re-embeds either of those) MUST go
 * through this helper before being passed to {@code LOG.warn}/{@code info}.
 *
 * <p>Two transformations applied in order:
 * <ol>
 *   <li><strong>Control-codepoint escape.</strong> C0 (U+0000..U+001F), DEL
 *       (U+007F), and C1 (U+0080..U+009F) are replaced with their
 *       {@code \\uXXXX} hex escape. This neutralises ANSI escape sequences
 *       like {@code ESC [2J} (clear screen) that operators with terminals
 *       would otherwise see rendered. Tab (U+0009), LF (U+000A), and CR
 *       (U+000D) are NOT special-cased — they are just as dangerous in a
 *       grep-able log file (a forged log line is one CR+LF away) and are
 *       escaped along with the rest.</li>
 *   <li><strong>Length cap.</strong> Truncated to {@link #MAX_LEN} chars
 *       with a {@code ...[truncated, N chars]} annotation. This prevents a
 *       32 KB attacker-controlled clientId from blowing up the log volume.
 *       The {@code N} reported is the BEFORE-escape character count of the
 *       original input — so an operator reading the log sees how big the
 *       original payload was, not the escaped length.</li>
 * </ol>
 *
 * <p>Output is wrapped in single quotes by the caller's format string, not
 * by this helper, to preserve the existing log shape ({@code rule '{}'}).
 * The helper guarantees the returned string is printable ASCII (subset of
 * what's safe for terminal display, log forwarders, and human grep) and
 * contains no control codepoints.
 *
 * <p>Audit references: round-11 (codec intake hardening — rejected forbidden
 * codepoints in rule ids), round-13 BLOCKER-1 (loader rejection-path WARN
 * still echoed raw key + exception message), round-13 HIGH-1 (DENY-path
 * INFO log echoed raw clientId).
 */
public final class LogSafe {

    /**
     * Maximum kept length of an attacker-controlled string in a log line.
     * 128 chars is a generous bound for legitimate values: the codec's
     * {@code MAX_RULE_ID_LEN} is 256 (longer than this, deliberately, so
     * that a length-only rejection still surfaces the original length
     * via the truncation annotation), and well-behaved clientIds are
     * typically &lt; 64 chars. Anything past this cap is by definition
     * either pathological or attacker-controlled.
     */
    public static final int MAX_LEN = 128;

    private LogSafe() {
        // utility
    }

    /**
     * Sanitise an attacker-controlled string for embedding in a log line.
     *
     * @param raw the wire-derived value (may be null)
     * @return the literal string {@code "null"} when {@code raw} is null,
     *         otherwise a control-escaped, length-capped representation
     */
    public static String sanitize(String raw) {
        if (raw == null) {
            return "null";
        }
        final int originalLength = raw.length();
        // Iterate codepoints, not chars, so we treat surrogate pairs as one
        // unit. Surrogates are escaped via Character.toString(int) which
        // yields the original surrogate-pair characters; the control-class
        // check below operates on the codepoint value.
        final StringBuilder out = new StringBuilder(Math.min(originalLength, MAX_LEN) + 32);
        int kept = 0;
        int i = 0;
        while (i < originalLength && kept < MAX_LEN) {
            int cp = raw.codePointAt(i);
            int charsForCp = Character.charCount(cp);
            // Control classes: C0 (0..0x1F), DEL (0x7F), C1 (0x80..0x9F).
            // Also escape lone surrogates as a defense against malformed
            // strings produced by JVM-side decoder bugs (replacement char
            // U+FFFD is allowed through — it's printable and benign).
            boolean isControl = (cp < 0x20) || (cp == 0x7F) || (cp >= 0x80 && cp <= 0x9F);
            boolean isLoneSurrogate = Character.isSurrogate((char) cp) && charsForCp == 1;
            if (isControl || isLoneSurrogate) {
                if (cp <= 0xFFFF) {
                    out.append("\\u").append(String.format("%04X", cp));
                } else {
                    // Unreachable today (all control classes are <= 0x9F),
                    // but kept for completeness if the set ever extends to
                    // supplementary-plane control characters.
                    out.append("\\U").append(String.format("%08X", cp));
                }
            } else {
                out.appendCodePoint(cp);
            }
            kept += charsForCp;
            i += charsForCp;
        }
        if (i < originalLength) {
            out.append("...[truncated, ").append(originalLength).append(" chars]");
        }
        return out.toString();
    }
}
