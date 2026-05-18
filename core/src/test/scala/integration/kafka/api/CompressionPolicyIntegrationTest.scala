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
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin.{AlterConfigOp, AlterConfigsOptions, ConfigEntry, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.storage.internals.log.LogConfig
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

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

  /**
   * Pins the `compression.policy` validator at the IncrementalAlterConfigs API boundary.
   * The acceptance criteria call out both CreateTopic *and* AlterConfig as the points where
   * unknown values must be rejected. The `LogConfigDef.in(...)` validator covers both, but
   * we exercise the AlterConfig path here separately to catch any wiring regression where
   * alter-time validation could diverge from create-time validation.
   */
  @Test
  def testIncrementalAlterConfigRejectsUnknownCompressionPolicyValue(): Unit = {
    val topic = "compression-alter-bad-policy"
    val admin = TestUtils.createAdminClient(Seq(broker),
      ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    try {
      // Create the topic with the default (none) policy so we can attempt to alter it.
      TestUtils.createTopicWithAdmin(admin, topic, Seq(broker), controllerServers)

      val topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
      val alterOps = Collections.singletonList(
        new AlterConfigOp(new ConfigEntry(LogConfig.COMPRESSION_POLICY_CONFIG, "always"), OpType.SET))
      val alterResult = admin.incrementalAlterConfigs(Collections.singletonMap(topicResource, alterOps))

      val ee = assertThrows(classOf[ExecutionException], () => alterResult.all().get())
      assertTrue(ee.getCause.isInstanceOf[InvalidConfigurationException],
        s"expected InvalidConfigurationException for unknown compression.policy value on AlterConfig, got " +
          s"${ee.getCause.getClass.getName}: ${ee.getCause.getMessage}")

      // The topic's compression.policy must remain at its prior (default) value.
      val configs = admin.describeConfigs(Collections.singletonList(topicResource)).all().get()
      val policyEntry = configs.get(topicResource).get(LogConfig.COMPRESSION_POLICY_CONFIG)
      assertEquals(LogConfig.DEFAULT_COMPRESSION_POLICY, policyEntry.value(),
        s"compression.policy must remain at its prior value after AlterConfig was rejected")
    } finally {
      admin.close()
    }
  }

  /**
   * Broker-matrix coverage of the produce-side contract: every non-NONE codec the wire
   * format supports must satisfy `compression.policy=required`. The matrix axis here is
   * the compression byte the broker observes on the produce path, which is what the
   * policy actually inspects (it does not look at the producer config string, only at
   * the per-batch compression header). The `lz4` case is also covered by the smoke test
   * above; running every codec here pins that no codec is silently rejected by the
   * enforcement check (i.e. no false positives).
   */
  @ParameterizedTest
  @ValueSource(strings = Array("gzip", "snappy", "lz4", "zstd"))
  def testCompressionPolicyRequiredAcceptsEveryCompressedCodec(codec: String): Unit = {
    val topic = s"required-codec-$codec"
    val admin = TestUtils.createAdminClient(Seq(broker),
      ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    try {
      val cfg = new Properties()
      cfg.put(LogConfig.COMPRESSION_POLICY_CONFIG, "required")
      TestUtils.createTopicWithAdmin(admin, topic, Seq(broker), controllerServers, topicConfig = cfg)
    } finally {
      admin.close()
    }

    val producer = newProducer(TestUtils.plaintextBootstrapServers(Seq(broker)), codec)
    try {
      val meta = producer.send(new ProducerRecord(topic, "v".getBytes)).get()
      assertEquals(0L, meta.offset(),
        s"codec=$codec must satisfy compression.policy=required and append at offset 0")
    } finally {
      producer.close()
    }
  }

  /**
   * Pins the alter-then-relax lifecycle: a topic created with `compression.policy=required`
   * rejects an uncompressed batch; after AlterConfig flips it back to `none`, the same
   * producer succeeds against the same topic. This is the operator-facing escape hatch —
   * a policy applied in error must be reversible without recreating the topic.
   */
  @Test
  def testReverseAlterRelaxesRequiredBackToNone(): Unit = {
    val topic = "reverse-alter"
    val admin = TestUtils.createAdminClient(Seq(broker),
      ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    try {
      val cfg = new Properties()
      cfg.put(LogConfig.COMPRESSION_POLICY_CONFIG, "required")
      TestUtils.createTopicWithAdmin(admin, topic, Seq(broker), controllerServers, topicConfig = cfg)

      val producer = newProducer(TestUtils.plaintextBootstrapServers(Seq(broker)), "none")
      try {
        val rejected = assertThrows(classOf[ExecutionException],
          () => producer.send(new ProducerRecord(topic, "v".getBytes)).get())
        assertTrue(rejected.getCause.isInstanceOf[InvalidRecordException],
          s"required policy must reject uncompressed batch up front")

        val topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
        val alterOps = Collections.singletonList(
          new AlterConfigOp(new ConfigEntry(LogConfig.COMPRESSION_POLICY_CONFIG, "none"), OpType.SET))
        admin.incrementalAlterConfigs(Collections.singletonMap(topicResource, alterOps)).all().get()

        // AlterConfig propagates through the controller's metadata commit; poll until the
        // broker's view actually reflects the change rather than racing the first produce.
        TestUtils.waitUntilTrue(() => {
          val meta = producer.send(new ProducerRecord(topic, "v".getBytes))
          try { meta.get(); true } catch { case _: ExecutionException => false }
        }, "policy relaxation did not propagate to the broker in time")
      } finally {
        producer.close()
      }
    } finally {
      admin.close()
    }
  }

  /**
   * Pins the DELETE-op semantics on `compression.policy`: removing the override resets the
   * topic to the default (`none`), matching the documented behaviour of every other dynamic
   * topic config. Without this test, a future refactor could regress to "DELETE is a no-op"
   * or "DELETE leaves the prior value cached on the broker" without anyone noticing.
   */
  @Test
  def testAlterConfigDeleteResetsCompressionPolicyToDefault(): Unit = {
    val topic = "delete-policy"
    val admin = TestUtils.createAdminClient(Seq(broker),
      ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    try {
      val cfg = new Properties()
      cfg.put(LogConfig.COMPRESSION_POLICY_CONFIG, "required")
      TestUtils.createTopicWithAdmin(admin, topic, Seq(broker), controllerServers, topicConfig = cfg)

      val topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
      val deleteOps = Collections.singletonList(
        new AlterConfigOp(new ConfigEntry(LogConfig.COMPRESSION_POLICY_CONFIG, null), OpType.DELETE))
      admin.incrementalAlterConfigs(Collections.singletonMap(topicResource, deleteOps)).all().get()

      // describeConfigs must reflect the default after DELETE. The broker can lag the
      // controller's metadata commit by a few ms; wait for the eventual state.
      TestUtils.waitUntilTrue(() => {
        val configs = admin.describeConfigs(Collections.singletonList(topicResource)).all().get()
        configs.get(topicResource).get(LogConfig.COMPRESSION_POLICY_CONFIG).value() ==
          LogConfig.DEFAULT_COMPRESSION_POLICY
      }, "DELETE op did not reset compression.policy to the default value")

      // The behavioural test: an uncompressed producer must now succeed.
      val producer = newProducer(TestUtils.plaintextBootstrapServers(Seq(broker)), "none")
      try {
        TestUtils.waitUntilTrue(() => {
          val meta = producer.send(new ProducerRecord(topic, "v".getBytes))
          try { meta.get(); true } catch { case _: ExecutionException => false }
        }, "uncompressed producer should succeed after compression.policy was DELETE-reset to default")
      } finally {
        producer.close()
      }
    } finally {
      admin.close()
    }
  }

  /**
   * Pins the operator-visibility contract: `compression.policy` is round-trippable through
   * `describeConfigs`. A configured topic returns the explicit value; an unconfigured topic
   * returns the default. Operators rely on this to audit which topics carry a non-default
   * policy without reading log files.
   */
  @Test
  def testDescribeConfigsRoundTripsCompressionPolicy(): Unit = {
    val configuredTopic = "describe-required"
    val defaultTopic = "describe-default"
    val admin = TestUtils.createAdminClient(Seq(broker),
      ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    try {
      val cfg = new Properties()
      cfg.put(LogConfig.COMPRESSION_POLICY_CONFIG, "required")
      TestUtils.createTopicWithAdmin(admin, configuredTopic, Seq(broker), controllerServers, topicConfig = cfg)
      TestUtils.createTopicWithAdmin(admin, defaultTopic, Seq(broker), controllerServers)

      val configuredResource = new ConfigResource(ConfigResource.Type.TOPIC, configuredTopic)
      val defaultResource = new ConfigResource(ConfigResource.Type.TOPIC, defaultTopic)
      val described = admin.describeConfigs(
        java.util.Arrays.asList(configuredResource, defaultResource)).all().get()

      val configuredValue = described.get(configuredResource).get(LogConfig.COMPRESSION_POLICY_CONFIG)
      assertEquals("required", configuredValue.value(),
        "configured topic must round-trip the explicit compression.policy value")

      val defaultValue = described.get(defaultResource).get(LogConfig.COMPRESSION_POLICY_CONFIG)
      assertEquals(LogConfig.DEFAULT_COMPRESSION_POLICY, defaultValue.value(),
        "unconfigured topic must report the default compression.policy through describeConfigs")
    } finally {
      admin.close()
    }
  }

  /**
   * Pins the `validateOnly=true` AlterConfig semantics: the validator must run even when the
   * change is not committed. A bad value reported via `validateOnly` shields operators from
   * pushing a misconfiguration into the live state and tools (e.g. CI gates) rely on this
   * shape — a silently-accepted dry-run would hand a false green to a deploy pipeline.
   */
  @Test
  def testValidateOnlyAlterConfigRejectsBadValueAndPreservesState(): Unit = {
    val topic = "validate-only-bad"
    val admin = TestUtils.createAdminClient(Seq(broker),
      ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    try {
      TestUtils.createTopicWithAdmin(admin, topic, Seq(broker), controllerServers)

      val topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
      val alterOps = Collections.singletonList(
        new AlterConfigOp(new ConfigEntry(LogConfig.COMPRESSION_POLICY_CONFIG, "bogus"), OpType.SET))
      val result = admin.incrementalAlterConfigs(
        Collections.singletonMap(topicResource, alterOps),
        new AlterConfigsOptions().validateOnly(true))

      val ee = assertThrows(classOf[ExecutionException], () => result.all().get())
      assertTrue(ee.getCause.isInstanceOf[InvalidConfigurationException],
        s"validateOnly must surface InvalidConfigurationException for unknown value, got " +
          s"${ee.getCause.getClass.getName}: ${ee.getCause.getMessage}")

      // State must remain at the default after a rejected dry-run.
      val described = admin.describeConfigs(Collections.singletonList(topicResource)).all().get()
      val entry = described.get(topicResource).get(LogConfig.COMPRESSION_POLICY_CONFIG)
      assertEquals(LogConfig.DEFAULT_COMPRESSION_POLICY, entry.value(),
        "validateOnly rejection must not mutate the topic's compression.policy")
    } finally {
      admin.close()
    }
  }

  /**
   * Pins the idempotent producer path against `compression.policy=required`. Idempotent
   * producers attach a `ProducerId/Epoch/Sequence` header and are handled by the same
   * `handleProduceRequest` codepath as non-idempotent ones, so they should observe the
   * exact same policy: an uncompressed batch is rejected with `INVALID_RECORD`; a
   * compressed batch is accepted. This pins that nothing in the idempotent path silently
   * bypasses the check (e.g. via a different verification branch).
   */
  @Test
  def testIdempotentProducerHonoursCompressionPolicy(): Unit = {
    val topic = "idempotent-required"
    val admin = TestUtils.createAdminClient(Seq(broker),
      ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    try {
      val cfg = new Properties()
      cfg.put(LogConfig.COMPRESSION_POLICY_CONFIG, "required")
      TestUtils.createTopicWithAdmin(admin, topic, Seq(broker), controllerServers, topicConfig = cfg)
    } finally {
      admin.close()
    }

    val bootstrap = TestUtils.plaintextBootstrapServers(Seq(broker))

    val idempotentUncompressed = newIdempotentProducer(bootstrap, "none")
    try {
      val ee = assertThrows(classOf[ExecutionException],
        () => idempotentUncompressed.send(new ProducerRecord(topic, "v".getBytes)).get())
      assertTrue(ee.getCause.isInstanceOf[InvalidRecordException],
        s"idempotent + compression.type=none must be rejected the same way as a vanilla producer, " +
          s"got ${ee.getCause.getClass.getName}: ${ee.getCause.getMessage}")
    } finally {
      idempotentUncompressed.close()
    }

    val idempotentCompressed = newIdempotentProducer(bootstrap, "lz4")
    try {
      val meta = idempotentCompressed.send(new ProducerRecord(topic, "v".getBytes)).get()
      assertEquals(0L, meta.offset(),
        "idempotent + compression.type=lz4 must satisfy compression.policy=required")
    } finally {
      idempotentCompressed.close()
    }
  }

  /**
   * Pins the `compression.policy=forbidden` end-to-end contract. The mirror image of the
   * `required` contract: any non-NONE codec must be rejected with `InvalidRecordException`,
   * and a NONE-codec producer must succeed. The codec axis (lz4 / gzip) protects against
   * accidentally special-casing a single codec on the rejection path.
   */
  @ParameterizedTest
  @ValueSource(strings = Array("gzip", "snappy", "lz4", "zstd"))
  def testCompressionPolicyForbiddenRejectsEveryCompressedCodec(codec: String): Unit = {
    val topic = s"compression-forbidden-$codec"
    val admin = TestUtils.createAdminClient(Seq(broker),
      ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    try {
      val cfg = new Properties()
      cfg.put(LogConfig.COMPRESSION_POLICY_CONFIG, "forbidden")
      TestUtils.createTopicWithAdmin(admin, topic, Seq(broker), controllerServers, topicConfig = cfg)
    } finally {
      admin.close()
    }

    val bootstrap = TestUtils.plaintextBootstrapServers(Seq(broker))

    val compressed = newProducer(bootstrap, codec)
    try {
      val ee = assertThrows(classOf[ExecutionException],
        () => compressed.send(new ProducerRecord(topic, "v".getBytes)).get())
      assertTrue(ee.getCause.isInstanceOf[InvalidRecordException],
        s"codec=$codec must be rejected by compression.policy=forbidden with " +
          s"InvalidRecordException, got ${ee.getCause.getClass.getName}: ${ee.getCause.getMessage}")
    } finally {
      compressed.close()
    }

    val uncompressed = newProducer(bootstrap, "none")
    try {
      val meta = uncompressed.send(new ProducerRecord(topic, "v".getBytes)).get()
      assertEquals(0L, meta.offset(),
        "uncompressed producer must succeed against compression.policy=forbidden")
    } finally {
      uncompressed.close()
    }
  }

  /**
   * Pins the operator retry story: an application that sends to a `required` topic with
   * `compression.type=none` is rejected on the first send, then succeeds on a second send
   * once it has been reconfigured to use a compressed codec. This is the realistic remediation
   * flow — the application learns about the policy via the rejected send, fixes its own
   * `compression.type` config, and retries.
   *
   * We model this with two producer instances against the same topic and the same broker:
   * a stock `KafkaProducer` does not let you flip `compression.type` after construction, so
   * the "reconfigure" step is necessarily a new producer. The contract pinned here is the
   * broker-side one: the same topic accepts the same logical record once the wire-format
   * compression header on the batch changes from NONE to LZ4.
   */
  @Test
  def testRetryWithCompressionAfterRequiredRejection(): Unit = {
    val topic = "retry-after-reject"
    val admin = TestUtils.createAdminClient(Seq(broker),
      ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    try {
      val cfg = new Properties()
      cfg.put(LogConfig.COMPRESSION_POLICY_CONFIG, "required")
      TestUtils.createTopicWithAdmin(admin, topic, Seq(broker), controllerServers, topicConfig = cfg)
    } finally {
      admin.close()
    }

    val bootstrap = TestUtils.plaintextBootstrapServers(Seq(broker))
    val payload = "v".getBytes

    val uncompressed = newProducer(bootstrap, "none")
    try {
      val ee = assertThrows(classOf[ExecutionException],
        () => uncompressed.send(new ProducerRecord(topic, payload)).get())
      assertTrue(ee.getCause.isInstanceOf[InvalidRecordException],
        s"first send must be rejected with InvalidRecordException, got " +
          s"${ee.getCause.getClass.getName}: ${ee.getCause.getMessage}")
    } finally {
      uncompressed.close()
    }

    // The application reconfigures itself with a compressed codec and retries the same logical
    // record. The broker must accept it — this is the documented escape hatch for callers who
    // discover `compression.policy=required` at runtime.
    val retried = newProducer(bootstrap, "lz4")
    try {
      val meta = retried.send(new ProducerRecord(topic, payload)).get()
      assertEquals(0L, meta.offset(),
        "retry with compression.type=lz4 must satisfy compression.policy=required and append at offset 0")
    } finally {
      retried.close()
    }
  }

  /**
   * Pins the `validateOnly=true` silent-success semantics: a syntactically valid AlterConfig
   * call is reported as successful but must NOT mutate the live config. Operators rely on
   * this dry-run shape to confirm a planned change is acceptable before committing it; if
   * `validateOnly` silently committed a "good" value, every dry-run would become a live
   * change and the API would be broken.
   *
   * The sibling test `testValidateOnlyAlterConfigRejectsBadValueAndPreservesState` covers the
   * bad-value rejection path; this test covers the good-value-but-not-committed path that
   * the bad-value test cannot exercise (since rejection short-circuits the commit anyway).
   */
  @Test
  def testValidateOnlyAlterConfigDoesNotCommitGoodValue(): Unit = {
    val topic = "validate-only-good"
    val admin = TestUtils.createAdminClient(Seq(broker),
      ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    try {
      TestUtils.createTopicWithAdmin(admin, topic, Seq(broker), controllerServers)

      val topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
      val alterOps = Collections.singletonList(
        new AlterConfigOp(new ConfigEntry(LogConfig.COMPRESSION_POLICY_CONFIG, "required"), OpType.SET))

      // validateOnly must succeed (the value is valid) but must not mutate the broker view.
      admin.incrementalAlterConfigs(
        Collections.singletonMap(topicResource, alterOps),
        new AlterConfigsOptions().validateOnly(true)
      ).all().get()

      val described = admin.describeConfigs(Collections.singletonList(topicResource)).all().get()
      val entry = described.get(topicResource).get(LogConfig.COMPRESSION_POLICY_CONFIG)
      assertEquals(LogConfig.DEFAULT_COMPRESSION_POLICY, entry.value(),
        "validateOnly with a syntactically valid value must report success without committing the change")

      // Behavioural confirmation: an uncompressed producer must still succeed (the topic is
      // still at the default `none` policy, not the dry-run `required` value).
      val producer = newProducer(TestUtils.plaintextBootstrapServers(Seq(broker)), "none")
      try {
        val meta = producer.send(new ProducerRecord(topic, "v".getBytes)).get()
        assertEquals(0L, meta.offset(),
          "uncompressed producer must succeed: validateOnly must not have committed compression.policy=required")
      } finally {
        producer.close()
      }
    } finally {
      admin.close()
    }
  }

  /**
   * Allow-list happy path: a topic created with `compression.policy=gzip,lz4` accepts a producer
   * configured for either codec in the list. Pins that the validator switch (from a fixed
   * enumeration of "none/required/forbidden" to a delegated parse) actually propagates
   * allow-list values into the topic's effective config and through to the enforcement loop.
   */
  @ParameterizedTest
  @ValueSource(strings = Array("gzip", "lz4"))
  def testCompressionPolicyAllowListAcceptsCodecInList(codec: String): Unit = {
    val topic = s"allow-list-$codec"
    val admin = TestUtils.createAdminClient(Seq(broker),
      ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    try {
      val cfg = new Properties()
      cfg.put(LogConfig.COMPRESSION_POLICY_CONFIG, "gzip,lz4")
      TestUtils.createTopicWithAdmin(admin, topic, Seq(broker), controllerServers, topicConfig = cfg)
    } finally {
      admin.close()
    }

    val producer = newProducer(TestUtils.plaintextBootstrapServers(Seq(broker)), codec)
    try {
      val meta = producer.send(new ProducerRecord(topic, "v".getBytes)).get()
      assertEquals(0L, meta.offset(),
        s"codec=$codec is in the allow-list gzip,lz4 and must append at offset 0")
    } finally {
      producer.close()
    }
  }

  /**
   * Allow-list rejection path: a topic created with `compression.policy=gzip,lz4` must reject
   * a codec NOT in the list (e.g. zstd) with `INVALID_RECORD`, and must also reject the
   * uncompressed codec (the allow-list cannot contain `none`). This is the pair that
   * distinguishes an allow-list from `compression.policy=required`.
   */
  @ParameterizedTest
  @ValueSource(strings = Array("zstd", "snappy", "none"))
  def testCompressionPolicyAllowListRejectsCodecNotInList(codec: String): Unit = {
    val topic = s"allow-list-reject-$codec"
    val admin = TestUtils.createAdminClient(Seq(broker),
      ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    try {
      val cfg = new Properties()
      cfg.put(LogConfig.COMPRESSION_POLICY_CONFIG, "gzip,lz4")
      TestUtils.createTopicWithAdmin(admin, topic, Seq(broker), controllerServers, topicConfig = cfg)
    } finally {
      admin.close()
    }

    val producer = newProducer(TestUtils.plaintextBootstrapServers(Seq(broker)), codec)
    try {
      val ex = assertThrows(classOf[ExecutionException],
        () => producer.send(new ProducerRecord(topic, "v".getBytes)).get())
      assertTrue(ex.getCause.isInstanceOf[InvalidRecordException],
        s"codec=$codec is not in allow-list gzip,lz4 and must be rejected with INVALID_RECORD; got: ${ex.getCause}")
    } finally {
      producer.close()
    }
  }

  /**
   * Pins that an allow-list value round-trips through DescribeConfigs byte-for-byte: the
   * validator validates without transforming, so the on-disk topic config preserves exactly
   * what the operator set. This matters for IaC diffing and dashboards — drift between the
   * input string and the value surfaced back to tooling would generate spurious "config has
   * changed" alerts on every reconciliation pass.
   */
  @Test
  def testCompressionPolicyAllowListRoundTripsThroughDescribeConfigs(): Unit = {
    val topic = "allow-list-describe"
    val admin = TestUtils.createAdminClient(Seq(broker),
      ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    try {
      // Mix of upper/lower case and inner spaces: parse() tolerates this, and we want to
      // confirm DescribeConfigs surfaces it verbatim rather than echoing the normalised form.
      val originalValue = "GZIP, LZ4 , zstd"
      val cfg = new Properties()
      cfg.put(LogConfig.COMPRESSION_POLICY_CONFIG, originalValue)
      TestUtils.createTopicWithAdmin(admin, topic, Seq(broker), controllerServers, topicConfig = cfg)

      val topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
      val described = admin.describeConfigs(Collections.singletonList(topicResource)).all().get()
      val entry = described.get(topicResource).get(LogConfig.COMPRESSION_POLICY_CONFIG)
      assertNotNull(entry, "compression.policy must appear in describeConfigs output")
      assertEquals(originalValue, entry.value(),
        "describeConfigs must surface the original allow-list value byte-for-byte so " +
          "operator tooling and IaC diffing see exactly what was set")
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

  private def newIdempotentProducer(bootstrap: String, compression: String): KafkaProducer[Array[Byte], Array[Byte]] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression)
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    // We intentionally do NOT set retries=0: idempotent producers reject that at construction
    // time. INVALID_RECORD is non-retriable, so the first attempt surfaces directly through the
    // future regardless of how high the producer's internal retry budget is.
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000")
    new KafkaProducer(props, new ByteArraySerializer, new ByteArraySerializer)
  }
}
