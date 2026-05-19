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
}
