/**
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

package kafka.server

import kafka.utils.TestUtils
import org.apache.kafka.common.config.{ConfigException, ConfigResource}
import org.apache.kafka.common.config.ConfigResource.Type.{BROKER, BROKER_LOGGER, CLIENT_METRICS, GROUP, TOPIC}
import org.apache.kafka.common.config.TopicConfig.{REMOTE_LOG_STORAGE_ENABLE_CONFIG, SEGMENT_BYTES_CONFIG, SEGMENT_JITTER_MS_CONFIG, SEGMENT_MS_CONFIG}
import org.apache.kafka.common.errors.{InvalidConfigurationException, InvalidRequestException, InvalidTopicException}
import org.apache.kafka.coordinator.group.GroupConfig
import org.apache.kafka.server.metrics.ClientMetricsConfigs
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util
import java.util.Collections.emptyMap

class ControllerConfigurationValidatorTest {
  val config = new KafkaConfig(TestUtils.createDummyBrokerConfig())
  val validator = new ControllerConfigurationValidator(config)

  @Test
  def testDefaultTopicResourceIsRejected(): Unit = {
    assertEquals("Default topic resources are not allowed.",
        assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(TOPIC, ""), emptyMap(), emptyMap())). getMessage)
  }

  @Test
  def testInvalidTopicNameRejected(): Unit = {
    assertEquals("Topic name is invalid: '(<-invalid->)' contains " +
      "one or more characters other than ASCII alphanumerics, '.', '_' and '-'",
        assertThrows(classOf[InvalidTopicException], () => validator.validate(
          new ConfigResource(TOPIC, "(<-invalid->)"), emptyMap(), emptyMap())). getMessage)
  }

  @Test
  def testUnknownResourceType(): Unit = {
    assertEquals("Unknown resource type BROKER_LOGGER",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(BROKER_LOGGER, "foo"), emptyMap(), emptyMap())). getMessage)
  }

  @Test
  def testNullTopicConfigValue(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(SEGMENT_JITTER_MS_CONFIG, "10")
    config.put(SEGMENT_BYTES_CONFIG, null)
    config.put(SEGMENT_MS_CONFIG, null)
    assertEquals("Null value not supported for topic configs: segment.bytes,segment.ms",
      assertThrows(classOf[InvalidConfigurationException], () => validator.validate(
        new ConfigResource(TOPIC, "foo"), config, emptyMap())). getMessage)
  }

  @Test
  def testValidTopicConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(SEGMENT_JITTER_MS_CONFIG, "1000")
    config.put(SEGMENT_BYTES_CONFIG, "67108864")
    validator.validate(new ConfigResource(TOPIC, "foo"), config, emptyMap())
  }

  @Test
  def testInvalidTopicConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(SEGMENT_JITTER_MS_CONFIG, "1000")
    config.put(SEGMENT_BYTES_CONFIG, "67108864")
    config.put("foobar", "abc")
    assertEquals("Unknown topic config name: foobar",
      assertThrows(classOf[InvalidConfigurationException], () => validator.validate(
        new ConfigResource(TOPIC, "foo"), config, emptyMap())). getMessage)
  }

  @ParameterizedTest(name = "testDisablingRemoteStorageTopicConfig with wasRemoteStorageEnabled: {0}")
  @ValueSource(booleans = Array(true, false))
  def testDisablingRemoteStorageTopicConfig(wasRemoteStorageEnabled: Boolean): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false")
    if (wasRemoteStorageEnabled) {
      assertEquals("It is invalid to disable remote storage without deleting remote data. " +
        "If you want to keep the remote data and turn to read only, please set `remote.storage.enable=true,remote.log.copy.disable=true`. " +
        "If you want to disable remote storage and delete all remote data, please set `remote.storage.enable=false,remote.log.delete.on.disable=true`.",
        assertThrows(classOf[InvalidConfigurationException], () => validator.validate(
          new ConfigResource(TOPIC, "foo"), config, util.Collections.singletonMap(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"))).getMessage)
    } else {
      validator.validate(
        new ConfigResource(TOPIC, "foo"), config, util.Collections.emptyMap())
      validator.validate(
        new ConfigResource(TOPIC, "foo"), config, util.Collections.singletonMap(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"))
    }
  }

  @Test
  def testInvalidBrokerEntity(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(SEGMENT_JITTER_MS_CONFIG, "1000")
    assertEquals("Unable to parse broker name as a base 10 number.",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(BROKER, "blah"), config, emptyMap())). getMessage)
  }

  @Test
  def testInvalidNegativeBrokerId(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(SEGMENT_JITTER_MS_CONFIG, "1000")
    assertEquals("Invalid negative broker ID.",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(BROKER, "-1"), config, emptyMap())). getMessage)
  }

  @Test
  def testValidClientMetricsConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(ClientMetricsConfigs.PUSH_INTERVAL_MS, "2000")
    config.put(ClientMetricsConfigs.SUBSCRIPTION_METRICS, "org.apache.kafka.client.producer.partition.queue.,org.apache.kafka.client.producer.partition.latency")
    config.put(ClientMetricsConfigs.CLIENT_MATCH_PATTERN, "client_instance_id=b69cc35a-7a54-4790-aa69-cc2bd4ee4538,client_id=1" +
      ",client_software_name=apache-kafka-java,client_software_version=2.8.0-SNAPSHOT,client_source_address=127.0.0.1," +
      "client_source_port=1234")
    validator.validate(new ConfigResource(CLIENT_METRICS, "subscription-1"), config, emptyMap())
  }

  @Test
  def testInvalidSubscriptionNameClientMetricsConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    assertEquals("Subscription name can't be empty",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(CLIENT_METRICS, ""), config, emptyMap())). getMessage)
  }

  @Test
  def testInvalidIntervalClientMetricsConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(ClientMetricsConfigs.PUSH_INTERVAL_MS, "10")
    assertEquals("Invalid value 10 for interval.ms, interval must be between 100 and 3600000 (1 hour)",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(CLIENT_METRICS, "subscription-1"), config, emptyMap())). getMessage)

    config.put(ClientMetricsConfigs.PUSH_INTERVAL_MS, "3600001")
    assertEquals("Invalid value 3600001 for interval.ms, interval must be between 100 and 3600000 (1 hour)",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(CLIENT_METRICS, "subscription-1"), config, emptyMap())). getMessage)
  }

  @Test
  def testUndefinedConfigClientMetricsConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put("random", "10")
    assertEquals("Unknown client metrics configuration: random",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(CLIENT_METRICS, "subscription-1"), config, emptyMap())). getMessage)
  }

  @Test
  def testInvalidMatchClientMetricsConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(ClientMetricsConfigs.CLIENT_MATCH_PATTERN, "10")
    assertEquals("Illegal client matching pattern: 10",
      assertThrows(classOf[InvalidConfigurationException], () => validator.validate(
        new ConfigResource(CLIENT_METRICS, "subscription-1"), config, emptyMap())). getMessage)
  }

  @Test
  def testBrokerConfigRejectsTenantPrefixedSuperUsersOnController(): Unit = {
    // Controller-side defence in depth: a writer reaching the controller
    // listener directly (e.g. a CLUSTER_ACTION holder bypassing the broker
    // preprocess in ConfigAdminManager) must NOT be able to persist a poison
    // super.users record into the metadata log. The broker would otherwise
    // catch it at startup or in DynamicBrokerConfig — too late, the metadata
    // is already corrupted and every broker restart fails.
    val config = new util.TreeMap[String, String]()
    config.put("super.users", "User:Operator;User:__tenant_acme.alice")
    val ex = assertThrows(classOf[ConfigException], () => validator.validate(
      new ConfigResource(BROKER, "1"), config, emptyMap()))
    val msg = ex.getMessage
    assertTrue(msg.contains("__tenant_acme.alice"), s"expected offender listed in: $msg")
    assertTrue(msg.contains("reserved tenant prefix"), s"expected explanation in: $msg")
  }

  @Test
  def testBrokerConfigAcceptsOperatorSuperUsersOnController(): Unit = {
    // Non-tenant principals in super.users are the legitimate operator case.
    val config = new util.TreeMap[String, String]()
    config.put("super.users", "User:Operator;User:Admin")
    validator.validate(new ConfigResource(BROKER, "1"), config, emptyMap())
  }

  @Test
  def testBrokerConfigSuperUsersValidationAggregatesOffendersOnController(): Unit = {
    // Mirrors TenantConfig.validateSuperUsersAreNotTenantPrefixed behaviour:
    // every offending entry is named in the error so an operator can fix the
    // request in one round-trip instead of discovering them one at a time.
    val config = new util.TreeMap[String, String]()
    config.put("super.users", "User:__tenant_acme.alice;User:Operator;User:__tenant_beta.bob")
    val ex = assertThrows(classOf[ConfigException], () => validator.validate(
      new ConfigResource(BROKER, "1"), config, emptyMap()))
    val msg = ex.getMessage
    assertTrue(msg.contains("__tenant_acme.alice"), s"expected first offender in: $msg")
    assertTrue(msg.contains("__tenant_beta.bob"), s"expected second offender in: $msg")
  }

  @Test
  def testBrokerConfigWithoutSuperUsersPassesOnController(): Unit = {
    // Unrelated broker configs must not be impeded by the tenant guard.
    val config = new util.TreeMap[String, String]()
    config.put("log.retention.ms", "604800000")
    validator.validate(new ConfigResource(BROKER, "1"), config, emptyMap())
  }

  @Test
  def testBrokerConfigRejectsAddingListenerTenantIdOnController(): Unit = {
    // Controller-side defence: the listener→tenant binding is established
    // once at broker startup from server.properties. Persisting a different
    // value into the metadata log via AlterConfigs would silently re-route
    // the listener on the next broker restart — the corrupted record reaches
    // every tenant resource (topics, ACLs, group ids, principals) at once.
    // DynamicConfig.Broker.validate accepts unknown listener-prefixed keys
    // (customPropsAllowed=true), so without this guard the request would
    // sail through to the controller.
    val config = new util.TreeMap[String, String]()
    config.put("listener.name.tenant_acme.tenant.id", "evilTenant")
    val ex = assertThrows(classOf[ConfigException], () => validator.validate(
      new ConfigResource(BROKER, "1"), config, emptyMap()))
    val msg = ex.getMessage
    assertTrue(msg.contains("listener.name.tenant_acme.tenant.id"),
      s"expected offending key listed in: $msg")
    assertTrue(msg.contains("AlterConfigs"),
      s"expected explanation that AlterConfigs is forbidden in: $msg")
  }

  @Test
  def testBrokerConfigRejectsModifyingListenerTenantIdOnController(): Unit = {
    // The bound listener already routes to tenant=acme; AlterConfigs tries
    // to flip it to tenant=beta. Refuse — the broker would silently re-route
    // every connection on the listener at the next restart.
    val config = new util.TreeMap[String, String]()
    config.put("listener.name.tenant_acme.tenant.id", "beta")
    val old = new util.TreeMap[String, String]()
    old.put("listener.name.tenant_acme.tenant.id", "acme")
    val ex = assertThrows(classOf[ConfigException], () => validator.validate(
      new ConfigResource(BROKER, "1"), config, old))
    assertTrue(ex.getMessage.contains("listener.name.tenant_acme.tenant.id"),
      s"expected offending key listed in: ${ex.getMessage}")
  }

  @Test
  def testBrokerConfigRejectsDeletingListenerTenantIdOnController(): Unit = {
    // The metadata log carries a tenant.id binding; AlterConfigs proposes
    // to delete it (key absent from newConfigs, present in oldConfigs).
    // Refuse — the listener would lose its binding on the next restart and
    // silently become an open cluster-wide listener.
    val config = new util.TreeMap[String, String]()
    val old = new util.TreeMap[String, String]()
    old.put("listener.name.tenant_acme.tenant.id", "acme")
    val ex = assertThrows(classOf[ConfigException], () => validator.validate(
      new ConfigResource(BROKER, "1"), config, old))
    assertTrue(ex.getMessage.contains("listener.name.tenant_acme.tenant.id"),
      s"expected offending key listed in: ${ex.getMessage}")
  }

  @Test
  def testBrokerConfigAcceptsTenantIdUnchangedOnController(): Unit = {
    // A no-op AlterConfigs that re-asserts the existing binding (same key,
    // same value in both maps) is benign and must be accepted.
    val config = new util.TreeMap[String, String]()
    config.put("listener.name.tenant_acme.tenant.id", "acme")
    val old = new util.TreeMap[String, String]()
    old.put("listener.name.tenant_acme.tenant.id", "acme")
    validator.validate(new ConfigResource(BROKER, "1"), config, old)
  }

  @Test
  def testValidGroupConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "50000")
    config.put(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, "5000")
    validator.validate(new ConfigResource(GROUP, "group"), config, emptyMap())
  }

  @Test
  def testInvalidGroupNameGroupConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    assertEquals("Default group resources are not allowed.",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(GROUP, ""), config, emptyMap())).getMessage)
  }

  @Test
  def testNullGroupConfigValue(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "50000")
    config.put(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, null)
    assertEquals("Null value not supported for group configs: consumer.heartbeat.interval.ms",
      assertThrows(classOf[InvalidConfigurationException], () => validator.validate(
        new ConfigResource(GROUP, "group"), config, emptyMap())).getMessage)
  }

  @Test
  def testInvalidGroupConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "50000")
    config.put("foobar", "abc")
    assertEquals("Unknown group config name: foobar",
      assertThrows(classOf[InvalidConfigurationException], () => validator.validate(
        new ConfigResource(GROUP, "group"), config, emptyMap())).getMessage)
  }
}
