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
package kafka.server

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/**
 * Direct unit tests for [[BrokerServer.parseRequireLocalReplica]] — the parser
 * for the {@code governance.bootstrap.require.local.replica} broker property.
 *
 * <p>Round-39 audit finding R39-E-2: prior to the parser extraction this knob
 * had no direct test coverage — every existing test reached it only through
 * the bootstrap-level error-message assertions in
 * {@link BrokerGovernanceBootstrapTest}, which exercise the {@code true}
 * branch only. The fail-closed contract — that any value OTHER than an exact
 * lowercase/trimmed match of {@code "false"}, {@code "no"}, or {@code "0"}
 * leaves the safety gate engaged — is a security invariant: a permissive
 * boolean coercion (e.g. accepting {@code "False"} but mishandling
 * {@code "off"} or {@code "fals"}) would let a typo silently flip a P0 gate
 * to fail-open. The tests below pin every branch.
 */
class BrokerServerRequireLocalReplicaTest {

  // ---------------------------------------------------------------------
  // The three exact strings that disable the gate (case-insensitive, trimmed)
  // ---------------------------------------------------------------------

  @Test
  def exactFalseLowercaseDisables(): Unit = {
    assertFalse(BrokerServer.parseRequireLocalReplica("false"))
  }

  @Test
  def exactNoLowercaseDisables(): Unit = {
    assertFalse(BrokerServer.parseRequireLocalReplica("no"))
  }

  @Test
  def exactZeroDisables(): Unit = {
    assertFalse(BrokerServer.parseRequireLocalReplica("0"))
  }

  @Test
  def uppercaseFalseDisables(): Unit = {
    // Operators casually type "FALSE" in server.properties — the parser
    // lowercases before matching so this is accepted.
    assertFalse(BrokerServer.parseRequireLocalReplica("FALSE"))
  }

  @Test
  def mixedCaseNoDisables(): Unit = {
    assertFalse(BrokerServer.parseRequireLocalReplica("No"))
  }

  @Test
  def whitespacePaddedFalseDisables(): Unit = {
    // Property files easily pick up trailing whitespace from copy-paste; the
    // parser trims before matching.
    assertFalse(BrokerServer.parseRequireLocalReplica("  false  "))
  }

  @Test
  def whitespacePaddedZeroDisables(): Unit = {
    assertFalse(BrokerServer.parseRequireLocalReplica("\t0\n"))
  }

  // ---------------------------------------------------------------------
  // Fail-closed: absent / unknown / typo / garbage all leave the gate ENGAGED
  // ---------------------------------------------------------------------

  @Test
  def nullValueDefaultsToTrue(): Unit = {
    // Key absent from server.properties — the gate stays engaged.
    assertTrue(BrokerServer.parseRequireLocalReplica(null))
  }

  @Test
  def emptyStringDefaultsToTrue(): Unit = {
    // Operator wrote `governance.bootstrap.require.local.replica=` with no
    // value. The fail-closed contract says: anything not in the exact
    // disable-set leaves the gate engaged.
    assertTrue(BrokerServer.parseRequireLocalReplica(""))
  }

  @Test
  def whitespaceOnlyDefaultsToTrue(): Unit = {
    assertTrue(BrokerServer.parseRequireLocalReplica("   "))
  }

  @Test
  def explicitTrueKeepsGateEngaged(): Unit = {
    assertTrue(BrokerServer.parseRequireLocalReplica("true"))
  }

  @Test
  def explicitYesKeepsGateEngaged(): Unit = {
    // "yes" is NOT in the disable-set; the parser treats it as the safe
    // default rather than a synonym for "no" via negation.
    assertTrue(BrokerServer.parseRequireLocalReplica("yes"))
  }

  @Test
  def explicitOneKeepsGateEngaged(): Unit = {
    assertTrue(BrokerServer.parseRequireLocalReplica("1"))
  }

  @Test
  def typoFalsKeepsGateEngaged(): Unit = {
    // The most dangerous case: an operator typed "fals" intending "false".
    // A permissive parser (e.g. Boolean.parseBoolean which returns false for
    // ANY non-"true" string) would silently flip the gate to fail-open here.
    // The fail-closed parser keeps the gate engaged.
    assertTrue(BrokerServer.parseRequireLocalReplica("fals"))
  }

  @Test
  def offDoesNotDisable(): Unit = {
    // "off" is a common boolean-like value but is NOT in the exact disable
    // set. Fail-closed posture: keep the gate engaged.
    assertTrue(BrokerServer.parseRequireLocalReplica("off"))
  }

  @Test
  def disableDoesNotDisable(): Unit = {
    // Same defence as "off" — only the three exact strings disable the gate.
    assertTrue(BrokerServer.parseRequireLocalReplica("disable"))
  }

  @Test
  def numericNonZeroKeepsGateEngaged(): Unit = {
    assertTrue(BrokerServer.parseRequireLocalReplica("2"))
  }

  @Test
  def garbageKeepsGateEngaged(): Unit = {
    assertTrue(BrokerServer.parseRequireLocalReplica("FOO_BAR"))
  }

  @Test
  def booleanObjectFalseDisables(): Unit = {
    // KafkaConfig.originals() returns Object — a programmatic caller could
    // pass a boxed Boolean. toString.toLowerCase produces "false".
    assertFalse(BrokerServer.parseRequireLocalReplica(java.lang.Boolean.FALSE))
  }

  @Test
  def booleanObjectTrueKeepsGateEngaged(): Unit = {
    assertTrue(BrokerServer.parseRequireLocalReplica(java.lang.Boolean.TRUE))
  }

  // ---------------------------------------------------------------------
  // Hostile Unicode whitespace — Java String.trim() only strips codepoints
  // <= U+0020. Anything above survives trimming and fails the exact match,
  // which is the SAFE outcome. R40-F-2 + R41-D wave pin the contract the
  // javadoc claims.
  //
  // All codepoints are written as `\uXXXX` escapes (R41-D-1) so a future
  // editor / git filter / source-NFC normalisation cannot silently mutate
  // the literal to ASCII space without the test still claiming to test what
  // it claimed before.
  //
  // Locale.ROOT case-folding is checked manually: none of the codepoints
  // below map to ASCII under `toLowerCase(Locale.ROOT)`, so the lowercase
  // step cannot accidentally strip the hostile codepoint either.
  // ---------------------------------------------------------------------

  @Test
  def nbspPrefixedFalseKeepsGateEngaged(): Unit = {
    // U+00A0 NO-BREAK SPACE — not stripped by String.trim().
    assertTrue(BrokerServer.parseRequireLocalReplica("\u00A0false"))
  }

  @Test
  def nnbspPrefixedFalseKeepsGateEngaged(): Unit = {
    // U+202F NARROW NO-BREAK SPACE — not stripped by String.trim().
    assertTrue(BrokerServer.parseRequireLocalReplica("\u202Ffalse"))
  }

  @Test
  def zwspPrefixedFalseKeepsGateEngaged(): Unit = {
    // U+200B ZERO-WIDTH SPACE — invisible, not stripped by String.trim().
    assertTrue(BrokerServer.parseRequireLocalReplica("\u200Bfalse"))
  }

  @Test
  def ideographicSpacePrefixedFalseKeepsGateEngaged(): Unit = {
    // U+3000 IDEOGRAPHIC SPACE — not stripped by String.trim().
    assertTrue(BrokerServer.parseRequireLocalReplica("\u3000false"))
  }

  @Test
  def softHyphenPrefixedFalseKeepsGateEngaged(): Unit = {
    // U+00AD SOFT HYPHEN — highest-realism paste hazard from Word /
    // Confluence / sloppy property files. Not stripped by String.trim().
    // R41-D-2.
    assertTrue(BrokerServer.parseRequireLocalReplica("\u00ADfalse"))
  }

  @Test
  def bomPrefixedFalseKeepsGateEngaged(): Unit = {
    // U+FEFF BYTE-ORDER MARK / ZERO WIDTH NO-BREAK SPACE — common paste
    // hazard from files saved as UTF-8-with-BOM. Not stripped by
    // String.trim(). R41-D-2.
    //
    // R42-D follow-up: an earlier revision of this comment named
    // `Pattern.UNICODE_CHARACTER_CLASS` `\s` as the regression vector —
    // that was wrong. U+FEFF has Unicode property `White_Space=No`, so
    // `\s` under UNICODE_CHARACTER_CLASS (= `\p{IsWhite_Space}`) does NOT
    // match it. Same for `Character.isWhitespace(0xFEFF) == false`, which
    // means a refactor to `String.strip()` would NOT regress this case
    // either. The realistic regression vector for U+FEFF is a BOM-removal
    // preprocessor upstream of trim — a BOM-aware Reader, an explicit
    // `replaceFirst("^\\uFEFF", "")` to handle UTF-8-with-BOM property
    // files, or `org.apache.commons.io.input.BOMInputStream`. Any of those
    // would silently flip this case to fail-OPEN; this test pins the
    // contract that no such preprocessor sits in the parser chain today.
    assertTrue(BrokerServer.parseRequireLocalReplica("\uFEFFfalse"))
  }

  @Test
  def stringTrimInvariantHoldsForHostileCodepoints(): Unit = {
    // Orthogonal invariant test (R41-D-3): the SAFE behaviour above hinges
    // on the JDK contract that `String.trim()` only strips codepoints
    // <= U+0020. If that ever changes (JDK behaviour change, or upstream
    // someone replaces `.trim` with `.strip()` / `replaceAll("\\s+", ...)`)
    // every test above silently becomes vacuous (would pass for the wrong
    // reason: trim DOES strip, then "false" matches, then assertTrue still
    // says true on a different code path). This test pins the invariant
    // directly so a regression there breaks loudly.
    val hostileCodepoints = Seq(
      0x00A0, // NBSP
      0x202F, // NNBSP
      0x200B, // ZWSP
      0x3000, // IDEOGRAPHIC SPACE
      0x00AD, // SOFT HYPHEN
      0xFEFF, // BOM / ZWNBSP
      0x2007, // FIGURE SPACE
      0x2028, // LINE SEPARATOR
      0x2029, // PARAGRAPH SEPARATOR
      0x2060, // WORD JOINER
      0x1680, // OGHAM SPACE MARK
      0x180E  // MONGOLIAN VOWEL SEPARATOR
    )
    for (cp <- hostileCodepoints) {
      val s = new String(Character.toChars(cp)) + "false"
      assertEquals(
        s, s.trim,
        f"U+$cp%04X must survive String.trim — parser fail-closed contract depends on this"
      )
      // R42-D follow-up: ALSO call the parser directly so a refactor that
      // swaps `.trim` for `.strip()` (which DOES strip U+00A0, U+202F,
      // U+2007, U+1680, U+180E etc. under JDK 11+ `Character.isWhitespace`)
      // breaks this test on the codepoints the dedicated single-codepoint
      // tests above do NOT cover. Without this assertion the invariant test
      // is a JDK tautology: it would pass even if the parser was changed to
      // a hostile permissive form, because the loop body never invoked the
      // parser. Pinning both contracts in the same iteration closes that
      // gap and makes the relationship between "trim is the strict primitive"
      // and "parser inherits the strict primitive's behaviour" assertable.
      assertTrue(
        BrokerServer.parseRequireLocalReplica(s),
        f"U+$cp%04X-prefixed 'false' must NOT disable the gate — parser fail-closed contract"
      )
    }
  }
}
