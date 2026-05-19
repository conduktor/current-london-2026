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
 *   <li><strong>Unsafe-codepoint escape.</strong> The codepoint classes
 *       below are replaced with their {@code \\uXXXX} hex escape (or
 *       {@code \\UXXXXXXXX} above the BMP). The {@code \\u}-form is
 *       deliberately the same shape Java source uses, so an operator
 *       can paste it back into a code search:
 *       <ul>
 *         <li><strong>C0 / DEL / C1.</strong> U+0000..U+001F, U+007F,
 *             U+0080..U+009F. Neutralises ANSI escape sequences like
 *             {@code ESC [2J} (clear screen) and prevents log-line
 *             forgery via raw CR/LF. Tab, LF, CR are NOT special-cased.</li>
 *         <li><strong>Lone surrogates.</strong> Illegal in well-formed
 *             UTF-16 but can sneak in via JVM decoder bugs; escape so
 *             downstream log processors don't choke.</li>
 *         <li><strong>Format-class (Unicode Cf).</strong> Catches the
 *             bidi family (LRM/RLM/ALM U+200E/200F/061C, the
 *             LRE/RLE/PDF/LRO/RLO embeddings U+202A..U+202E, and the
 *             LRI/RLI/FSI/PDI isolates U+2066..U+2069), the zero-width
 *             family (ZWSP/ZWNJ/ZWJ U+200B..U+200D, word joiner
 *             U+2060, invisible operators U+2061..U+2064), BOM /
 *             ZWNBSP U+FEFF, interlinear annotations U+FFF9..U+FFFB,
 *             and the language-tag block (U+E0001, U+E0020..U+E007F).
 *             Without this class, an attacker-controlled clientId can
 *             reorder display in an operator's bidi-aware terminal so
 *             {@code "rule-evil"} renders as {@code "live-elur"},
 *             defeating visual identification of the offending source
 *             at exactly the moment the operator most needs to see
 *             it.</li>
 *         <li><strong>Variation selectors.</strong> VS1..VS16
 *             (U+FE00..U+FE0F), VS17..VS256 (U+E0100..U+E01EF), and
 *             the Mongolian free variation selectors
 *             U+180B..U+180D / U+180F. Mn-class default-ignorable
 *             modifiers that mutate the rendering of the preceding
 *             character.</li>
 *         <li><strong>Other invisible / default-ignorable.</strong>
 *             CGJ U+034F, Khmer inherent vowels U+17B4 / U+17B5,
 *             Hangul fillers U+115F / U+1160 / U+3164 / U+FFA0 —
 *             technically letters or combining marks but render as
 *             zero-width, so admit the same identifier-spoofing
 *             attack vector.</li>
 *       </ul>
 *       U+FFFD (replacement char) is deliberately left through — it's
 *       the standard signal that wire data was already malformed and
 *       escaping it would just spam logs with {@code \\uFFFD} for
 *       every legitimate decoder fallback.</li>
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
 * INFO log echoed raw clientId), round-47 wave-C F1 HIGH (bidi/format/
 * zero-width passthrough — extended the predicate to Cf class, variation
 * selectors, and invisible default-ignorable codepoints).
 */
public final class LogSafe {

    /**
     * Maximum kept length of an attacker-controlled string in a log line.
     * 128 chars is a generous bound for legitimate values: the codec's
     * {@code MAX_RULE_ID_BYTES} is 256 UTF-8 bytes (longer than this,
     * deliberately, so that a length-only rejection still surfaces the
     * original length via the truncation annotation), and well-behaved
     * clientIds are typically &lt; 64 chars. Anything past this cap is by
     * definition either pathological or attacker-controlled.
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
            if (isUnsafeForLog(cp, charsForCp)) {
                if (cp <= 0xFFFF) {
                    out.append("\\u").append(String.format("%04X", cp));
                } else {
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

    /**
     * Predicate carving the codepoint classes documented on the class
     * javadoc. Kept private and inlined into the sanitise loop; broken
     * out as a helper purely so the per-class rationale stays adjacent
     * to the predicate it controls, instead of buried in the hot path.
     */
    private static boolean isUnsafeForLog(int cp, int charsForCp) {
        // C0 (U+0000..U+001F), DEL (U+007F), C1 (U+0080..U+009F).
        if (cp < 0x20 || cp == 0x7F || (cp >= 0x80 && cp <= 0x9F)) {
            return true;
        }
        // Lone surrogate — `charsForCp == 1` means codePointAt returned
        // the raw surrogate as its own codepoint, i.e. it had no mate.
        if (Character.isSurrogate((char) cp) && charsForCp == 1) {
            return true;
        }
        // Cf (Format) — covers the bidi family, zero-width controls,
        // BOM, language tags. Single getType call replaces ~25 explicit
        // ranges; the Cf set is fixed by Unicode and JDK upgrades only
        // ever add new codepoints, never remove existing ones, so the
        // predicate stays correct across JDKs.
        if (Character.getType(cp) == Character.FORMAT) {
            return true;
        }
        // Variation selectors (Mn class — not in Cf):
        //   VS1..VS16        U+FE00..U+FE0F
        //   VS17..VS256      U+E0100..U+E01EF
        //   Mongolian FVS    U+180B..U+180D, U+180F
        if (cp >= 0xFE00 && cp <= 0xFE0F) {
            return true;
        }
        if (cp >= 0xE0100 && cp <= 0xE01EF) {
            return true;
        }
        if ((cp >= 0x180B && cp <= 0x180D) || cp == 0x180F) {
            return true;
        }
        // Combining Grapheme Joiner — Mn class, zero-width, no rendering.
        if (cp == 0x034F) {
            return true;
        }
        // Khmer inherent vowels — Mn class, render zero-width.
        if (cp == 0x17B4 || cp == 0x17B5) {
            return true;
        }
        // Hangul fillers — Lo class but render zero-width, used in
        // identifier-spoofing attacks alongside the bidi family.
        if (cp == 0x115F || cp == 0x1160 || cp == 0x3164 || cp == 0xFFA0) {
            return true;
        }
        return false;
    }
}
