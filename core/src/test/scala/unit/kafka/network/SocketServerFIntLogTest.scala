/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.network

import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.network.iouring.SelectorImplementation
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/**
 * Pins the F-INT-LOG line shape against the resolution-matrix examples advertised in
 * `server/.../iouring/README.md` lines 127-130. The contract is part of the operator
 * UX: drift between the README's example output and what the broker actually emits
 * leaves operators grepping for a substring that doesn't exist, especially when
 * triaging silent NIO fallbacks. Tested here at the helper level so a regression in
 * either the message text or the annotation logic fails fast, in isolation, without
 * spinning up a full SocketServer.
 *
 * Each assertion below corresponds to a row of the resolution matrix in the README
 * Operator-observable log line section. If the README is updated, this test must be
 * updated in lockstep — that is the point of pinning.
 */
class SocketServerFIntLogTest {

  // README example (line 128): PLAINTEXT listener that resolved to io_uring.
  // No annotation: the operator got what they implicitly or explicitly asked for.
  @Test
  def plaintextResolvedToIoUringMatchesReadmeExample(): Unit = {
    assertEquals(
      "Listener PLAINTEXT://0.0.0.0:9092 resolved to io_uring backend",
      SocketServer.buildResolvedBackendLogLine(
        "PLAINTEXT", "0.0.0.0", 9092,
        SecurityProtocol.PLAINTEXT,
        SelectorImplementation.AUTO,
        effectiveUsesIoUring = true))
  }

  // README example (line 129): SSL listener under `auto` falls back to nio due to the
  // v1 PLAINTEXT-only contract — annotation must say so.
  @Test
  def sslAutoResolvedToNioCarriesContractAnnotation(): Unit = {
    assertEquals(
      "Listener SSL://0.0.0.0:9093 resolved to nio backend (PLAINTEXT-only contract for io_uring v1)",
      SocketServer.buildResolvedBackendLogLine(
        "SSL", "0.0.0.0", 9093,
        SecurityProtocol.SSL,
        SelectorImplementation.AUTO,
        effectiveUsesIoUring = false))
  }

  // Same row but with explicit io_uring (operator pinned it on a non-PLAINTEXT
  // listener). Silent NIO fallback still applies, and the annotation must fire —
  // otherwise the operator can't tell their pinning was ignored.
  @Test
  def sslExplicitIoUringResolvedToNioCarriesContractAnnotation(): Unit = {
    assertEquals(
      "Listener SSL://0.0.0.0:9093 resolved to nio backend (PLAINTEXT-only contract for io_uring v1)",
      SocketServer.buildResolvedBackendLogLine(
        "SSL", "0.0.0.0", 9093,
        SecurityProtocol.SSL,
        SelectorImplementation.IO_URING,
        effectiveUsesIoUring = false))
  }

  // SASL_PLAINTEXT and SASL_SSL must carry the same annotation — they're "non-PLAINTEXT"
  // for the purposes of the io_uring contract.
  @Test
  def saslListenersCarryContractAnnotationWhenDowngraded(): Unit = {
    for (proto <- Seq(SecurityProtocol.SASL_PLAINTEXT, SecurityProtocol.SASL_SSL)) {
      assertEquals(
        s"Listener ${proto.name}://0.0.0.0:9094 resolved to nio backend (PLAINTEXT-only contract for io_uring v1)",
        SocketServer.buildResolvedBackendLogLine(
          proto.name, "0.0.0.0", 9094,
          proto,
          SelectorImplementation.AUTO,
          effectiveUsesIoUring = false),
        s"expected PLAINTEXT-only annotation for $proto")
    }
  }

  // Operator pinned nio explicitly on a non-PLAINTEXT listener: NO annotation, because
  // nothing was downgraded — they got what they asked for. Showing the io_uring-flavoured
  // annotation here would be misleading.
  @Test
  def sslExplicitNioResolvedToNioOmitsContractAnnotation(): Unit = {
    assertEquals(
      "Listener SSL://0.0.0.0:9093 resolved to nio backend",
      SocketServer.buildResolvedBackendLogLine(
        "SSL", "0.0.0.0", 9093,
        SecurityProtocol.SSL,
        SelectorImplementation.NIO,
        effectiveUsesIoUring = false))
  }

  // PLAINTEXT listener that resolved to nio (e.g. platform without io_uring, or
  // port=0 + auto silent downgrade): NO annotation. The PLAINTEXT-only clause is
  // about the protocol mismatch, not the platform or port mismatch — different
  // reasons, different log signal.
  @Test
  def plaintextResolvedToNioOmitsContractAnnotation(): Unit = {
    assertEquals(
      "Listener PLAINTEXT://0.0.0.0:9092 resolved to nio backend",
      SocketServer.buildResolvedBackendLogLine(
        "PLAINTEXT", "0.0.0.0", 9092,
        SecurityProtocol.PLAINTEXT,
        SelectorImplementation.AUTO,
        effectiveUsesIoUring = false))
  }

  // Operator pinned nio explicitly on a PLAINTEXT listener — no annotation, exactly
  // as the operator requested. Provides the "explicit-nio is a first-class outcome"
  // sanity check.
  @Test
  def plaintextExplicitNioResolvedToNioOmitsContractAnnotation(): Unit = {
    assertEquals(
      "Listener PLAINTEXT://0.0.0.0:9092 resolved to nio backend",
      SocketServer.buildResolvedBackendLogLine(
        "PLAINTEXT", "0.0.0.0", 9092,
        SecurityProtocol.PLAINTEXT,
        SelectorImplementation.NIO,
        effectiveUsesIoUring = false))
  }

  // Wildcard binds: Kafka stores `null` for an empty host (see SocketServerConfigs).
  // The README example uses `0.0.0.0` as the display string for wildcard binds.
  @Test
  def nullHostRendersAsZeroZeroZeroZero(): Unit = {
    assertEquals(
      "Listener PLAINTEXT://0.0.0.0:9092 resolved to io_uring backend",
      SocketServer.buildResolvedBackendLogLine(
        "PLAINTEXT", null, 9092,
        SecurityProtocol.PLAINTEXT,
        SelectorImplementation.AUTO,
        effectiveUsesIoUring = true))
  }

  @Test
  def blankHostRendersAsZeroZeroZeroZero(): Unit = {
    assertEquals(
      "Listener PLAINTEXT://0.0.0.0:9092 resolved to io_uring backend",
      SocketServer.buildResolvedBackendLogLine(
        "PLAINTEXT", "   ", 9092,
        SecurityProtocol.PLAINTEXT,
        SelectorImplementation.AUTO,
        effectiveUsesIoUring = true))
  }

  // Non-wildcard host is preserved verbatim so operators searching for "localhost",
  // an internal hostname, or a specific NIC bind can still grep for it.
  @Test
  def explicitHostIsPreservedVerbatim(): Unit = {
    assertEquals(
      "Listener PLAINTEXT://10.0.0.1:9092 resolved to io_uring backend",
      SocketServer.buildResolvedBackendLogLine(
        "PLAINTEXT", "10.0.0.1", 9092,
        SecurityProtocol.PLAINTEXT,
        SelectorImplementation.AUTO,
        effectiveUsesIoUring = true))
  }
}
