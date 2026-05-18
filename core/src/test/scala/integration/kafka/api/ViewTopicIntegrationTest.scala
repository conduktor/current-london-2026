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
package kafka.api

import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.clients.consumer.{ConsumerConfig, GroupProtocol}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.server.views.ViewTopicConfig
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test, TestInfo}

import java.time.Duration
import java.util.{Collections, Locale, Properties}
import java.util.concurrent.ExecutionException

/**
 * End-to-end tests for topic views. Spins up a real broker, creates a backing topic and a
 * view topic with a CEL predicate, then drives produce/consume through the standard clients
 * (which see the view as just another topic).
 *
 * These tests guard the properties of the view feature that are most likely to regress once
 * the implementation lands and people start optimising:
 *  1. The broker actually applies the predicate (matching records pass, others are dropped).
 *  2. Offsets returned are SOURCE offsets — sparse, never renumbered.
 *  3. Producing to a view is rejected up-front; the backing topic never sees the write.
 *  4. A batch where every record fails the predicate must still let the consumer advance —
 *     otherwise the consumer loops forever on the same offset range.
 *  5. Editing the view's predicate config takes effect on the next fetch — the ViewRegistry
 *     drops its cached compiled spec via the config-change hook, and the SAME consumer's
 *     next poll sees behavior switch (proves session-cache invalidation, not just fresh-group).
 *  6. The filter handles compressed FileRecords on disk — the realistic production shape.
 *
 * The first three are PROMPT.md acceptance criteria. The fourth is the counter-intuitive
 * fix called out in the PROMPT ("Empty filtered response → consumer re-fetches the same
 * offset → infinite loop"). The fifth proves the invalidation wiring actually reaches the
 * fetch path. The sixth pins the on-disk materialization shape the unit tests cannot
 * exercise (unit tests feed pre-built MemoryRecords; real fetches land as FileRecords).
 */
class ViewTopicIntegrationTest extends IntegrationTestHarness {

  override protected def brokerCount: Int = 1

  private val backingTopic = "orders-raw"
  private val viewTopic = "red-orders"
  private val partition = new TopicPartition(viewTopic, 0)
  private val backingPartition = new TopicPartition(backingTopic, 0)

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    // The view rejects produce BEFORE backing-topic resolution, so the producer must not buffer
    // through retries — otherwise the test waits the full retry budget on the expected failure.
    producerConfig.setProperty("max.block.ms", "5000")
    producerConfig.setProperty("delivery.timeout.ms", "10000")
    producerConfig.setProperty("request.timeout.ms", "5000")
    producerConfig.setProperty("retries", "0")

    // GROUP_PROTOCOL is required by IntegrationTestHarness.createConsumer.
    consumerConfig.setProperty(ConsumerConfig.GROUP_PROTOCOL_CONFIG,
      GroupProtocol.CLASSIC.name.toLowerCase(Locale.ROOT))
    consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    // Snappy metadata refresh keeps the test fast when we mutate configs mid-flight.
    consumerConfig.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "100")

    super.setUp(testInfo)
  }

  @Test
  def testViewFiltersAndReturnsSourceOffsets(): Unit = {
    createTopic(backingTopic)
    createViewTopic(viewTopic, backingTopic, "body.color == 'red'")

    val producer = createProducer()
    val sentOffsets = produceColors(producer, Seq("red", "blue", "red", "green", "red"))
    // We never moved away from a single-partition setup, so source offsets are 0..4 in order.
    assertEquals(Seq(0L, 1L, 2L, 3L, 4L), sentOffsets,
      "sanity: backing topic must have been written densely so we can assert sparse fetch later")

    val consumer = createConsumer()
    try {
      consumer.assign(Collections.singletonList(partition))
      consumer.seekToBeginning(Collections.singletonList(partition))

      val matched = pollUntil(consumer, expectedCount = 3, timeout = Duration.ofSeconds(15))
      // The fetch must return ONLY red records, at the original (sparse) source offsets.
      // Sparse offsets are the property that lets a consumer LSO advance correctly past gaps.
      assertEquals(Seq(0L, 2L, 4L), matched.map(_._1),
        "view consumer must see matching records at their source offsets (sparse, never renumbered)")
      assertEquals(Seq("red", "red", "red"), matched.map(_._2.get("color")))
    } finally {
      consumer.close(Duration.ofSeconds(5))
    }
  }

  @Test
  def testProduceToViewIsRejectedWithInvalidRequest(): Unit = {
    createTopic(backingTopic)
    createViewTopic(viewTopic, backingTopic, "body.color == 'red'")

    val producer = createProducer()
    val record = new ProducerRecord[Array[Byte], Array[Byte]](viewTopic, 0, null, jsonBytes("red"))

    val ex = assertThrows(classOf[ExecutionException], () => producer.send(record).get())
    // Wrapped because send() is async: ExecutionException -> InvalidRequestException.
    assertTrue(ex.getCause.isInstanceOf[InvalidRequestException],
      s"produce to view must surface as InvalidRequestException, got: ${ex.getCause}")

    // And critically: the backing topic must not contain a write keyed at view-topic. Read the
    // backing topic to its end — no records of any colour should be there.
    val verifier = createConsumer(configOverrides = newGroupConfig("verify-no-backing-write"))
    try {
      verifier.assign(Collections.singletonList(backingPartition))
      verifier.seekToBeginning(Collections.singletonList(backingPartition))
      val records = verifier.poll(Duration.ofSeconds(2))
      assertEquals(0, records.count(),
        "produce-rejection must short-circuit before backing-topic resolution — the backing log " +
          "must remain empty even though the user-facing produce failed gracefully")
    } finally {
      verifier.close(Duration.ofSeconds(5))
    }
  }

  @Test
  def testConsumerAdvancesPastFullyFilteredBatch(): Unit = {
    createTopic(backingTopic)
    createViewTopic(viewTopic, backingTopic, "body.color == 'red'")

    val producer = createProducer()
    // No record matches the predicate. Without the header-only batch fix, the consumer's fetch
    // would receive zero records back, re-issue the same fetch at the same offset, and loop.
    val sent = produceColors(producer, Seq("blue", "green", "yellow"))
    val target = sent.last + 1L

    val consumer = createConsumer()
    try {
      consumer.assign(Collections.singletonList(partition))
      consumer.seekToBeginning(Collections.singletonList(partition))

      // Drive a few poll cycles. The consumer must NOT receive any records (none match), but its
      // position MUST progress past the fully-filtered range. We allow a small budget of polls
      // because metadata + first-fetch handshake takes a beat under the test harness.
      var lastPosition = consumer.position(partition)
      val deadline = System.currentTimeMillis() + 10_000
      while (lastPosition < target && System.currentTimeMillis() < deadline) {
        val records = consumer.poll(Duration.ofMillis(500))
        assertEquals(0, records.count(),
          s"no records match — consumer should not see any record, got ${records.count()}")
        lastPosition = consumer.position(partition)
      }
      assertTrue(lastPosition >= target,
        s"consumer position must advance past the filtered range; stuck at offset=$lastPosition, " +
          s"expected to advance past offset $target " +
          "(this is the 'empty filtered batch → infinite loop' regression the header-only batch fix prevents)")
    } finally {
      consumer.close(Duration.ofSeconds(5))
    }
  }

  @Test
  def testPredicateChangeIsHotReloaded(): Unit = {
    createTopic(backingTopic)
    createViewTopic(viewTopic, backingTopic, "body.color == 'red'")

    val producer = createProducer()
    produceColors(producer, Seq("red", "blue", "red", "blue", "red"))

    // ONE consumer across the config change, so the broker-side fetch session is the same on
    // both sides. If the registry's compiled-predicate cache were keyed by anything stickier
    // than the topic name (or if the TopicConfigHandler hook failed to fire), the second poll
    // below would still see the OLD predicate even though metadata propagation completed.
    val consumer = createConsumer(configOverrides = newGroupConfig("hot-reload"))
    try {
      consumer.assign(Collections.singletonList(partition))
      consumer.seekToBeginning(Collections.singletonList(partition))
      val firstPass = pollUntil(consumer, expectedCount = 3, timeout = Duration.ofSeconds(15))
      assertEquals(Seq("red", "red", "red"), firstPass.map(_._2.get("color")))

      val admin = createAdminClient()
      try {
        val resource = new ConfigResource(ConfigResource.Type.TOPIC, viewTopic)
        val alterOp = new AlterConfigOp(
          new ConfigEntry(ViewTopicConfig.VIEW_CEL_PREDICATE_CONFIG, "body.color == 'blue'"),
          AlterConfigOp.OpType.SET)
        admin.incrementalAlterConfigs(
          Collections.singletonMap(resource, Collections.singletonList(alterOp))).all().get()
        // KRaft publish pipeline drives both the metadata-image swap and the
        // DynamicConfigPublisher (which fires the TopicConfigHandler hook that invalidates
        // the ViewRegistry cache). By the time this returns, the registry has dropped the
        // cached spec and the next fetch will recompile.
        ensureConsistentKRaftMetadata()
      } finally {
        admin.close(Duration.ofSeconds(5))
      }

      // Re-seek so the very next fetch crosses the config-change boundary on the same fetch
      // session. If the cache hadn't been invalidated, this poll would either hang waiting
      // for non-existent red records past offset 4, or return red records under the old
      // predicate — both equally fatal.
      consumer.seekToBeginning(Collections.singletonList(partition))
      val secondPass = pollUntil(consumer, expectedCount = 2, timeout = Duration.ofSeconds(15))
      assertEquals(Seq(1L, 3L), secondPass.map(_._1),
        "after predicate change, the view must surface the NEW predicate's matches at source offsets")
      assertEquals(Seq("blue", "blue"), secondPass.map(_._2.get("color")))
    } finally {
      consumer.close(Duration.ofSeconds(5))
    }
  }

  @Test
  def testTwoViewsOverSameBackingInOneFetchRejectsSecondView(): Unit = {
    // The replica fetch API is keyed by TopicIdPartition, so two views over the same backing
    // partition in a single FetchRequest cannot both be dispatched. The previous implementation
    // would have silently overwritten one view in `viewRewrites`, mis-attributing the response.
    // The current implementation rejects the second view with INVALID_REQUEST so operators see
    // the collision instead of debugging "why did consumer for viewB receive records that match
    // viewA's predicate?" months later. This is a hard rejection — not a silent merge.
    val viewA = "view-red"
    val viewB = "view-blue"
    createTopic(backingTopic)
    createViewTopic(viewA, backingTopic, "body.color == 'red'")
    createViewTopic(viewB, backingTopic, "body.color == 'blue'")

    val producer = createProducer()
    produceColors(producer, Seq("red", "blue", "red", "blue"))

    val consumer = createConsumer(configOverrides = newGroupConfig("multi-view-collision"))
    try {
      val pA = new TopicPartition(viewA, 0)
      val pB = new TopicPartition(viewB, 0)
      consumer.assign(java.util.Arrays.asList(pA, pB))
      consumer.seekToBeginning(java.util.Arrays.asList(pA, pB))

      // Drive poll cycles for a bounded window. The broker must fail this fetch — either by
      // throwing an InvalidRequestException up out of poll() or by surfacing an error code at
      // the consumer's partition that translates to one. The exact surface depends on the
      // consumer's retry policy, but the IMPORTANT property is that the response is NOT a
      // silent half-correct merge of viewA's data delivered against viewB's topic. If this
      // assertion ever fails by "no exception, returns records", that is the regression
      // Codex flagged: a silent collision in a single FetchRequest.
      val deadline = System.currentTimeMillis() + 10_000
      var caught: Option[Throwable] = None
      while (caught.isEmpty && System.currentTimeMillis() < deadline) {
        try {
          consumer.poll(Duration.ofMillis(500))
        } catch {
          case t: Throwable => caught = Some(t)
        }
      }
      assertTrue(caught.isDefined,
        "expected the consumer to surface the multi-view collision (no exception within budget — broker may be " +
          "silently mis-attributing records)")
      // The classic consumer surfaces a per-partition INVALID_REQUEST (error code 42) either as
      // InvalidRequestException directly OR wrapped as IllegalStateException("Unexpected error code
      // 42 ..."). Either is fine — what matters is that the broker rejected loudly rather than
      // silently merging two views over one backing partition.
      val chain = causeChain(caught.get)
      val directlyInvalidRequest = chain.exists(_.isInstanceOf[InvalidRequestException])
      val wrappedInvalidRequest = chain.exists(t =>
        Option(t.getMessage).exists(m => m.contains("error code 42") || m.contains("INVALID_REQUEST")))
      assertTrue(directlyInvalidRequest || wrappedInvalidRequest,
        s"expected INVALID_REQUEST signal from broker collision-reject, got: ${chain.map(_.toString).mkString(" -> ")}")
    } finally {
      consumer.close(Duration.ofSeconds(5))
    }
  }

  @Test
  def testViewFilterHandlesCompressedBatches(): Unit = {
    createTopic(backingTopic)
    createViewTopic(viewTopic, backingTopic, "body.color == 'red'")

    // Compressed batches are the realistic production shape. Without an explicit test, the
    // FileRecords→MemoryRecords materialization could regress to "works on uncompressed only"
    // and we wouldn't notice until a real producer with compression.type=lz4 (Kafka default
    // for many fleets) sent its first batch.
    producerConfig.setProperty(org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
    producerConfig.setProperty(org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG, "16384")
    producerConfig.setProperty(org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG, "50")

    val producer = createProducer()
    // produceColors awaits each .get() individually, but the broker still receives them as
    // batches when produced in quick succession with linger.ms>0; LZ4 compression applies
    // per batch. The exact batch boundaries are not the point — the point is that the
    // FileRecords on disk has compressed inner batches that the filter must decompress.
    produceColors(producer, Seq("red", "blue", "red", "green", "red", "blue", "red"))

    val consumer = createConsumer()
    try {
      consumer.assign(Collections.singletonList(partition))
      consumer.seekToBeginning(Collections.singletonList(partition))
      val matched = pollUntil(consumer, expectedCount = 4, timeout = Duration.ofSeconds(15))
      assertEquals(Seq(0L, 2L, 4L, 6L), matched.map(_._1),
        "filter must traverse decompressed inner records and emit at source offsets")
      assertEquals(Seq("red", "red", "red", "red"), matched.map(_._2.get("color")))
    } finally {
      consumer.close(Duration.ofSeconds(5))
    }
  }

  // ---------------------------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------------------------

  private def createViewTopic(name: String, backing: String, predicate: String): Unit = {
    val props = new Properties()
    props.setProperty(ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG, backing)
    props.setProperty(ViewTopicConfig.VIEW_CEL_PREDICATE_CONFIG, predicate)
    props.setProperty(ViewTopicConfig.VIEW_OFFSET_MODE_CONFIG, ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE)
    createTopic(name, numPartitions = 1, replicationFactor = 1, topicConfig = props)
  }

  private def produceColors(producer: org.apache.kafka.clients.producer.Producer[Array[Byte], Array[Byte]],
                            colors: Seq[String]): Seq[Long] = {
    val futures = colors.map { c =>
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](backingTopic, 0, null, jsonBytes(c)))
    }
    futures.map(_.get().offset()).toIndexedSeq
  }

  private def jsonBytes(color: String): Array[Byte] =
    s"""{"color":"$color"}""".getBytes("UTF-8")

  /**
   * Poll the consumer until {@code expectedCount} records arrive or the timeout elapses, then
   * decode each record's value as a tiny JSON object. Returns (offset, parsed) pairs in
   * fetch order.
   *
   * We use a hand-rolled JSON tap rather than pulling in jackson here because the values are
   * single-field objects under our test's control — keeping the test parser-free avoids
   * dependency drift between this test and the production CEL JSON layer.
   */
  private def pollUntil(consumer: org.apache.kafka.clients.consumer.Consumer[Array[Byte], Array[Byte]],
                        expectedCount: Int,
                        timeout: Duration): Seq[(Long, MiniJson)] = {
    val collected = scala.collection.mutable.ArrayBuffer.empty[(Long, MiniJson)]
    val deadline = System.currentTimeMillis() + timeout.toMillis
    while (collected.size < expectedCount && System.currentTimeMillis() < deadline) {
      val records = consumer.poll(Duration.ofMillis(500))
      val it = records.iterator()
      while (it.hasNext) {
        val r = it.next()
        collected += ((r.offset(), MiniJson.parse(new String(r.value(), "UTF-8"))))
      }
    }
    assertEquals(expectedCount, collected.size,
      s"expected $expectedCount matching records within ${timeout.toMillis}ms, got ${collected.size}")
    collected.toIndexedSeq
  }

  /** Flatten the exception cause chain into a Seq for easy scanning. */
  private def causeChain(t: Throwable): Seq[Throwable] = {
    val out = scala.collection.mutable.ArrayBuffer.empty[Throwable]
    var cur: Throwable = t
    while (cur != null && !out.contains(cur)) {
      out += cur
      cur = cur.getCause
    }
    out.toIndexedSeq
  }

  private def newGroupConfig(groupId: String): Properties = {
    val p = new Properties()
    p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    p
  }

  /** Single-level JSON object parser. Only handles flat {"key":"value"} objects — anything
   *  else in our test fixtures is a test-authoring bug. */
  private final class MiniJson private(private val fields: Map[String, String]) {
    def get(name: String): String = fields.getOrElse(name,
      throw new AssertionError(s"missing field '$name' in $fields"))
  }
  private object MiniJson {
    def parse(s: String): MiniJson = {
      val trimmed = s.trim
      require(trimmed.startsWith("{") && trimmed.endsWith("}"), s"not a JSON object: $s")
      val inner = trimmed.substring(1, trimmed.length - 1)
      val pairs = if (inner.isEmpty) Array.empty[String] else inner.split(",")
      val map = pairs.iterator.map { pair =>
        val kv = pair.split(":", 2)
        require(kv.length == 2, s"bad pair: $pair")
        (unquote(kv(0).trim), unquote(kv(1).trim))
      }.toMap
      new MiniJson(map)
    }
    private def unquote(s: String): String = {
      require(s.startsWith("\"") && s.endsWith("\""), s"not a quoted string: $s")
      s.substring(1, s.length - 1)
    }
  }
}
