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

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.test.{KafkaClusterTestKit, TestKitNodes}
import org.apache.kafka.server.config.ServerConfigs
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Tag, Test, Timeout}

import java.util

/**
 * End-to-end startup test for the `governance.bypass.principals` empty-check
 * in `BrokerServer.startup`.
 *
 * The check is a deliberate fail-closed posture (Codex round-3 P0): an empty
 * bypass set would leave broker-internal traffic — replica fetchers, the
 * `__governance` log consumer, KRaft metadata fetches — subject to evaluation
 * by an operator-published DENY-all/FETCH rule, which would in turn break
 * the very mechanism that controls those rules.
 *
 * The parser-level coverage lives in
 * `RuleEngineTest.parseBypassPrincipalsNullOrEmptyYieldsEmptySet`; this test
 * pins the integration glue: BrokerServer.startup actually rejects an empty
 * parsed set, and the rejection surfaces as a ConfigException naming the
 * offending config key so the operator can fix it.
 */
@Timeout(120)
@Tag("integration")
class GovernanceBypassPrincipalsValidationTest {

  @Test
  def startupFailsClosedWhenGovernanceBypassPrincipalsIsEmpty(): Unit = {
    val overrides = new util.HashMap[Integer, util.Map[String, String]]()
    val brokerProps = new util.HashMap[String, String]()
    brokerProps.put(ServerConfigs.GOVERNANCE_BYPASS_PRINCIPALS_CONFIG, "")
    overrides.put(0, brokerProps)

    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder()
        .setNumBrokerNodes(1)
        .setNumControllerNodes(1)
        .setPerServerProperties(overrides)
        .build()
    ).build()

    try {
      cluster.format()
      val ex = assertThrows(classOf[Throwable], () => cluster.startup())
      val chain = causeChain(ex)
      val configEx = chain.collectFirst { case c: ConfigException => c }
      assertTrue(configEx.isDefined,
        s"expected a ConfigException somewhere in the cause chain, got: " +
          chain.map(c => s"${c.getClass.getSimpleName}: ${c.getMessage}").mkString(" :: "))
      val msg = configEx.get.getMessage
      assertTrue(msg.contains(ServerConfigs.GOVERNANCE_BYPASS_PRINCIPALS_CONFIG),
        s"ConfigException message must name the offending config key, got: $msg")
      assertTrue(msg.contains("non-empty"),
        s"ConfigException message must explain the empty-bypass posture, got: $msg")
    } finally {
      cluster.close()
    }
  }

  private def causeChain(t: Throwable): List[Throwable] = {
    val out = scala.collection.mutable.ListBuffer.empty[Throwable]
    var cur: Throwable = t
    val seen = scala.collection.mutable.HashSet.empty[Throwable]
    while (cur != null && !seen.contains(cur)) {
      out += cur
      seen += cur
      cur = cur.getCause
    }
    out.toList
  }
}
