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

import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.server.rules.GovernanceTopic

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util

/**
 * Unit tests for [[BrokerServer.requireGovernanceTopicCompactPolicy]] — the
 * fail-closed gate on `__governance` cleanup.policy raised by the round-12
 * tombstone/compaction sub-agent (HIGH-1).
 *
 * <p>Until round-12 the bootstrap WARN-and-proceeded on a non-compact
 * `__governance`, on the rationale that "the hazard window is hours/days
 * away and hard-failing startup over a topic config the broker doesn't own
 * would be a heavy hammer for a config typo". That posture was a latent
 * fail-OPEN — once `retention.ms` elapsed (default 7 days), rule records
 * silently disappeared and the broker accepted every previously-denied
 * request. The audit reversed that call: a WARN that an operator can ignore
 * is not a safety mechanism for a security-critical gate. The remediation
 * is one `kafka-configs.sh --alter` invocation, executed once.
 *
 * <p>The check is exercised here at the helper level so every branch — topic
 * absent, topic-level override compliant, topic-level override non-compliant,
 * broker-default fallback compliant, broker-default fallback non-compliant —
 * is covered without standing up a cluster.
 */
class BrokerServerGovernanceCompactionTest {

  @Test
  def topicAbsentIsAllowed(): Unit = {
    // Fresh cluster, operator has not yet created __governance. The broker
    // must come up so the operator can create the topic with the right
    // policy via the running cluster. A throw here would fail the test.
    BrokerServer.requireGovernanceTopicCompactPolicy(
      topicExists = false, topicLevelCleanupPolicy = null, brokerDefaultList("delete"))
  }

  @Test
  def topicLevelOverrideCompactIsAllowed(): Unit = {
    // Operator created the topic with the recommended policy; broker-default
    // is irrelevant when a topic-level override exists.
    BrokerServer.requireGovernanceTopicCompactPolicy(
      topicExists = true, TopicConfig.CLEANUP_POLICY_COMPACT, brokerDefaultList("delete"))
  }

  @Test
  def topicLevelOverrideCompactAndDeleteFailsClosed(): Unit = {
    // Round-13 HIGH-1 tightening: "compact,delete" is a valid Kafka policy
    // (compaction PLUS retention-based whole-segment deletion), but it
    // defeats the entire purpose of this gate — once retention.ms (default
    // 7 days) elapses, rule records get deleted by the retention path even
    // though compaction is also enabled. The previous round-12 substring
    // check ("contains compact") let this through; the round-13 fix
    // requires exact-match on the policy set.
    val ex = assertThrows(classOf[IllegalStateException],
      () => BrokerServer.requireGovernanceTopicCompactPolicy(
        topicExists = true, "compact,delete", brokerDefaultList("delete")))
    val msg = ex.getMessage
    assertTrue(msg.contains("compact,delete"),
      s"message must reflect the offending policy verbatim, got: $msg")
    assertTrue(msg.contains("retention.ms"),
      s"message must name retention.ms as the failure mode, got: $msg")
    assertTrue(msg.contains("kafka-configs.sh"),
      s"message must include the remediation command, got: $msg")
  }

  @Test
  def deleteCompactOrderIsNormalisedAndFailsClosed(): Unit = {
    // Order is normalised — "delete,compact" is semantically the same
    // mixed policy as "compact,delete" and must fail-closed the same way.
    val ex = assertThrows(classOf[IllegalStateException],
      () => BrokerServer.requireGovernanceTopicCompactPolicy(
        topicExists = true, "delete,compact", brokerDefaultList("delete")))
    assertTrue(ex.getMessage.contains("delete,compact"),
      s"message must reflect the policy as authored, got: ${ex.getMessage}")
  }

  @Test
  def whitespaceInPolicyListIsTolerated(): Unit = {
    // "compact, delete" (stray space after the comma) is still the
    // mixed policy — accept the value as-authored but fail-closed for
    // the same reason. The trim normalisation must not silently turn
    // "compact" into "compact" while allowing " compact" to be a different
    // token; both must split, trim, then compare against {compact}.
    val ex = assertThrows(classOf[IllegalStateException],
      () => BrokerServer.requireGovernanceTopicCompactPolicy(
        topicExists = true, "compact, delete", brokerDefaultList("delete")))
    assertTrue(ex.getMessage.contains("compact, delete"),
      s"message must reflect the policy as authored, got: ${ex.getMessage}")
  }

  @Test
  def trimmedCompactValueIsAllowed(): Unit = {
    // " compact " (leading + trailing whitespace) — just compaction with
    // ergonomic whitespace, must be accepted after trim.
    BrokerServer.requireGovernanceTopicCompactPolicy(
      topicExists = true, " compact ", brokerDefaultList("delete"))
  }

  @Test
  def mixedCaseCompactIsAllowed(): Unit = {
    // Kafka's TopicConfig parses cleanup.policy via ConfigDef, which
    // typically lowercases the value, but the @ClusterConfigProperty path
    // and direct Admin --add-config calls do not always normalise case.
    // Accept "Compact" the same as "compact" so a case-only typo doesn't
    // trip the gate and confuse operators.
    BrokerServer.requireGovernanceTopicCompactPolicy(
      topicExists = true, "Compact", brokerDefaultList("delete"))
  }

  @Test
  def topicLevelOverrideDeleteFailsClosed(): Unit = {
    val ex = assertThrows(classOf[IllegalStateException],
      () => BrokerServer.requireGovernanceTopicCompactPolicy(
        topicExists = true, "delete", brokerDefaultList("delete")))
    val msg = ex.getMessage
    assertTrue(msg.contains(GovernanceTopic.NAME),
      s"message must name __governance, got: $msg")
    assertTrue(msg.contains("delete"),
      s"message must name the offending policy, got: $msg")
    assertTrue(msg.contains("kafka-configs.sh"),
      s"message must include the remediation command, got: $msg")
    assertTrue(msg.contains("compact"),
      s"message must name the required policy, got: $msg")
  }

  @Test
  def brokerDefaultCompactIsAllowedWhenNoTopicLevelOverride(): Unit = {
    // Operator has tuned log.cleanup.policy=compact at the broker level
    // (uncommon but valid). No topic-level override → effective policy =
    // broker default = compact → allowed.
    BrokerServer.requireGovernanceTopicCompactPolicy(
      topicExists = true, topicLevelCleanupPolicy = null, brokerDefaultList("compact"))
  }

  @Test
  def brokerDefaultDeleteFailsClosedWhenNoTopicLevelOverride(): Unit = {
    // The hazard case: operator created __governance via Admin client
    // without an explicit cleanup.policy. The topic inherits the broker
    // default — which is "delete" in stock Kafka. Records age out by
    // retention.ms and the broker fails OPEN. Closing this is the whole
    // point of the round-12 HIGH-1 fix.
    val ex = assertThrows(classOf[IllegalStateException],
      () => BrokerServer.requireGovernanceTopicCompactPolicy(
        topicExists = true, topicLevelCleanupPolicy = null, brokerDefaultList("delete")))
    assertTrue(ex.getMessage.contains("delete"),
      s"effective policy must be reported, got: ${ex.getMessage}")
  }

  @Test
  def brokerDefaultListJoinedWithCommaFailsClosed(): Unit = {
    // Round-13 HIGH-1: a multi-element broker-default list of
    // [compact, delete] is a valid Kafka mixed policy at the log layer but
    // is rejected here for the same reason as topic-level "compact,delete"
    // — retention.ms still applies and rule records age out. The helper
    // joins with commas to match the canonical form of the topic-level
    // override and the same fail-closed check then trips.
    val ex = assertThrows(classOf[IllegalStateException],
      () => BrokerServer.requireGovernanceTopicCompactPolicy(
        topicExists = true, topicLevelCleanupPolicy = null,
        brokerDefaultList("compact", "delete")))
    assertTrue(ex.getMessage.contains("compact,delete"),
      s"message must reflect the joined broker-default policy, got: ${ex.getMessage}")
  }

  @Test
  def topicLevelOverrideTakesPrecedenceOverBrokerDefault(): Unit = {
    // If the operator pinned the topic to "delete" explicitly, that wins
    // over a broker default of "compact". This is exactly the misconfig
    // shape the gate is designed to catch — "I changed the broker default
    // but the topic still has the old policy".
    val ex = assertThrows(classOf[IllegalStateException],
      () => BrokerServer.requireGovernanceTopicCompactPolicy(
        topicExists = true, "delete", brokerDefaultList("compact")))
    assertTrue(ex.getMessage.contains("delete"),
      s"effective policy must reflect the topic-level override, got: ${ex.getMessage}")
  }

  private def brokerDefaultList(values: String*): util.List[String] = {
    val out = new util.ArrayList[String]()
    values.foreach(out.add)
    out
  }
}
