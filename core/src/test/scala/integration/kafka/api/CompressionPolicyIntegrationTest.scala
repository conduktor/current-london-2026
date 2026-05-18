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
package kafka.api

import kafka.server.{KafkaBroker, KafkaConfig, QuorumTestHarness}
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.storage.internals.log.LogConfig
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}

import java.util.{Collections, Properties}
import java.util.concurrent.ExecutionException

/**
 * End-to-end coverage for the `compression.policy` topic config.
 *
 * Exercises real topic config propagation through the controller, a real
 * `KafkaProducer` over plaintext, and the produce-request handler path. The
 * unit tests in `KafkaApisTest` cover the per-partition response shape with
 * mocks; this test pins the contract observable by stock clients:
 *
 *   - `compression.policy=required` rejects an uncompressed batch with
 *     `INVALID_RECORD`, with no client recompile or wire change;
 *   - the same topic accepts an LZ4-compressed batch;
 *   - a sibling topic without the config (default `none`) is unaffected, so
 *     vanilla behaviour is preserved for unconfigured topics.
 */
class CompressionPolicyIntegrationTest extends QuorumTestHarness {

  private val brokerId = 0
  private var broker: KafkaBroker = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    val props = TestUtils.createBrokerConfig(brokerId)
    broker = createBroker(new KafkaConfig(props))
  }

  @AfterEach
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(Seq(broker))
    super.tearDown()
  }

  @Test
  def testCompressionPolicyRequiredRejectsUncompressedAndAcceptsCompressed(): Unit = {
    val requiredTopic = "compression-required"
    val openTopic = "compression-open"

    val admin = TestUtils.createAdminClient(Seq(broker),
      ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    try {
      val requiredCfg = new Properties()
      requiredCfg.put(LogConfig.COMPRESSION_POLICY_CONFIG, "required")
      TestUtils.createTopicWithAdmin(admin, requiredTopic, Seq(broker), controllerServers,
        topicConfig = requiredCfg)
      TestUtils.createTopicWithAdmin(admin, openTopic, Seq(broker), controllerServers)
    } finally {
      admin.close()
    }

    val bootstrapServers = TestUtils.plaintextBootstrapServers(Seq(broker))

    // 1. Uncompressed producer hitting the `required` topic must fail with INVALID_RECORD.
    val uncompressed = newProducer(bootstrapServers, "none")
    try {
      val ee = assertThrows(classOf[ExecutionException],
        () => uncompressed.send(new ProducerRecord(requiredTopic, "v".getBytes)).get())
      assertTrue(ee.getCause.isInstanceOf[InvalidRecordException],
        s"expected InvalidRecordException, got ${ee.getCause.getClass.getName}: ${ee.getCause.getMessage}")

      // The same producer must be able to write to the unconfigured topic — vanilla path is unaffected.
      val metaOpen = uncompressed.send(new ProducerRecord(openTopic, "v".getBytes)).get()
      assertEquals(0L, metaOpen.offset())
    } finally {
      uncompressed.close()
    }

    // 2. Compressed producer (lz4) must succeed against the `required` topic.
    val compressed = newProducer(bootstrapServers, "lz4")
    try {
      val metaRequired = compressed.send(new ProducerRecord(requiredTopic, "v".getBytes)).get()
      assertEquals(0L, metaRequired.offset())
    } finally {
      compressed.close()
    }
  }

  /**
   * Pins the per-partition response shape end-to-end on a real broker: one configured
   * topic and one open topic, both targeted by the same uncompressed producer.
   *
   * The unit tests in `KafkaApisTest` (mock-based) already pin the same-request shape;
   * this test pins the contract against a real `ReplicaManager` / `MetadataCache` to
   * catch any wiring regression where a single broker response could conflate the two
   * topics.
   */
  @Test
  def testCompressionPolicyAppliesPerPartitionAcrossMixedTopicsEndToEnd(): Unit = {
    val requiredTopic = "mixed-required"
    val openTopic = "mixed-open"

    val admin = TestUtils.createAdminClient(Seq(broker),
      ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    try {
      val requiredCfg = new Properties()
      requiredCfg.put(LogConfig.COMPRESSION_POLICY_CONFIG, "required")
      TestUtils.createTopicWithAdmin(admin, requiredTopic, Seq(broker), controllerServers,
        topicConfig = requiredCfg)
      TestUtils.createTopicWithAdmin(admin, openTopic, Seq(broker), controllerServers)
    } finally {
      admin.close()
    }

    val bootstrapServers = TestUtils.plaintextBootstrapServers(Seq(broker))
    val producer = newProducer(bootstrapServers, "none")
    try {
      // Send both records before driving completion so the producer has the chance to
      // batch them; with a single broker this typically lands as a single ProduceRequest.
      // Even if the producer chose to split, the broker-side per-partition contract is the
      // same and is what we assert here.
      val openFuture = producer.send(new ProducerRecord(openTopic, "v".getBytes))
      val requiredFuture = producer.send(new ProducerRecord(requiredTopic, "v".getBytes))

      val ee = assertThrows(classOf[ExecutionException], () => requiredFuture.get())
      assertTrue(ee.getCause.isInstanceOf[InvalidRecordException],
        s"required-topic partition must be rejected with InvalidRecordException, got " +
          s"${ee.getCause.getClass.getName}: ${ee.getCause.getMessage}")

      val openMeta = openFuture.get()
      assertEquals(0L, openMeta.offset(),
        "open-topic partition in the same producer run must succeed independently of the rejected one")
    } finally {
      producer.close()
    }
  }

  /**
   * Pins the `compression.policy` validator at the CreateTopic API boundary. The
   * `LogConfig`-constructor-level rejection is unit-tested in `LogConfigTest`; this
   * test confirms the value-set validator is actually wired through the AdminClient
   * surface, so an unknown value (e.g. `"yes"`) is rejected before the topic is created.
   */
  @Test
  def testCreateTopicRejectsUnknownCompressionPolicyValue(): Unit = {
    val topic = "compression-bad-policy"
    val admin = TestUtils.createAdminClient(Seq(broker),
      ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    try {
      val badConfig = new java.util.HashMap[String, String]()
      badConfig.put(LogConfig.COMPRESSION_POLICY_CONFIG, "yes")

      val result = admin.createTopics(Collections.singletonList(
        new NewTopic(topic, 1, 1.toShort).configs(badConfig)))

      val ee = assertThrows(classOf[ExecutionException], () => result.all().get())
      assertTrue(ee.getCause.isInstanceOf[InvalidConfigurationException],
        s"expected InvalidConfigurationException for unknown compression.policy value, got " +
          s"${ee.getCause.getClass.getName}: ${ee.getCause.getMessage}")

      // The topic must not have been created.
      val listed = admin.listTopics().names().get()
      assertFalse(listed.contains(topic),
        s"topic $topic must not exist after CreateTopic rejected its compression.policy value")
    } finally {
      admin.close()
    }
  }

  private def newProducer(bootstrap: String, compression: String): KafkaProducer[Array[Byte], Array[Byte]] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression)
    // Disable retries so an INVALID_RECORD surfaces fast to the caller; the broker classifies it as non-retriable.
    props.put(ProducerConfig.RETRIES_CONFIG, "0")
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000")
    new KafkaProducer(props, new ByteArraySerializer, new ByteArraySerializer)
  }
}
