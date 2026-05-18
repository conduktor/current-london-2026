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
package kafka.network.http

import kafka.network.RequestChannel
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.message.ProduceResponseData
import org.apache.kafka.common.message.ProduceResponseData.{PartitionProduceResponse, TopicProduceResponse, TopicProduceResponseCollection}
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.message.FetchResponseData.{FetchableTopicResponse, PartitionData => FetchPartitionData}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.message.ApiMessageType
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, FetchResponse, ProduceRequest, ProduceResponse}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.network.http.{FetchRequestParser, ProduceRequestParser}
import org.apache.kafka.network.metrics.RequestChannelMetrics
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import java.nio.charset.StandardCharsets
import java.util
import java.util.OptionalInt
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class KafkaApiRequestSubmitterTest {

  private val time = new MockTime()
  private val principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice")
  private val listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
  private val topicId = Uuid.randomUuid()
  private val topic = "orders"

  private var channel: RequestChannel = _
  private var fakeHandler: FakeApiHandler = _

  @BeforeEach
  def setUp(): Unit = {
    // Real RequestChannelMetrics — updateErrorMetrics performs a non-null .apply lookup before our callback fires
    // (so HTTP-originated requests still feed broker error counters). A Mockito mock returns null and crashes.
    val metrics = new RequestChannelMetrics(ApiMessageType.ListenerType.BROKER)
    channel = new RequestChannel(queueSize = 16, metricNamePrefix = "test", time, metrics)
  }

  @AfterEach
  def tearDown(): Unit = {
    if (fakeHandler != null) fakeHandler.shutdown()
  }

  private def newSubmitter(): KafkaApiRequestSubmitter =
    new KafkaApiRequestSubmitter(
      requestChannel = channel,
      time = time,
      principal = principal,
      listenerName = listenerName,
      topicIdLookup = name => if (name == topic) topicId else Uuid.ZERO_UUID
    )

  // ----- pure helpers ----------------------------------------------------------------------------------------------

  @Test
  def buildProduceRequestGroupsRecordsByPartition(): Unit = {
    val submitter = newSubmitter()
    val cmd = new ProduceRequestParser.ProduceCommand(topic, util.List.of(
      newRecord(0, "k0", "v0"),
      newRecord(2, "k2", "v2"),
      newRecord(0, "k0b", "v0b") // a second record on partition 0
    ))

    val request = submitter.buildProduceRequest(cmd)
    val topicData = request.data().topicData().asScala.find(_.name() == topic).get
    val partitions = topicData.partitionData().asScala.sortBy(_.index())

    assertEquals(Seq(0, 2), partitions.map(_.index()).toSeq)
    assertEquals(2, recordCount(partitions(0).records().asInstanceOf[MemoryRecords]),
      "two records were sent to partition 0; they must travel together in the same PartitionProduceData")
    assertEquals(1, recordCount(partitions(1).records().asInstanceOf[MemoryRecords]))
  }

  @Test
  def buildProduceRequestRoutesUnspecifiedPartitionToPartitionZero(): Unit = {
    val submitter = newSubmitter()
    val cmd = new ProduceRequestParser.ProduceCommand(topic, util.List.of(
      new ProduceRequestParser.RecordEntry(OptionalInt.empty(), "k".getBytes, "v".getBytes, null)
    ))

    val request = submitter.buildProduceRequest(cmd)
    val partitions = request.data().topicData().asScala.find(_.name() == topic).get.partitionData().asScala

    assertEquals(Seq(0), partitions.map(_.index()).toSeq,
      "missing partition must deterministically route to partition 0 — documented v1 behaviour")
  }

  @Test
  def buildProduceRequestCarriesContentTypeAsRecordHeader(): Unit = {
    val submitter = newSubmitter()
    val cmd = new ProduceRequestParser.ProduceCommand(topic, util.List.of(
      new ProduceRequestParser.RecordEntry(OptionalInt.of(0), null, "{\"a\":1}".getBytes,
        "application/json")
    ))

    val request = submitter.buildProduceRequest(cmd)
    val records = request.data().topicData().iterator().next().partitionData().get(0)
      .records().asInstanceOf[MemoryRecords]
    val record = records.records().iterator().next()
    val header: Header = record.headers().toSeq.find(_.key() == "content-type").get

    assertEquals("application/json", new String(header.value(), StandardCharsets.UTF_8),
      "content-type must round-trip into the Kafka record header named 'content-type'")
  }

  @Test
  def translateProduceMapsErrorAndOffsetForEachPartition(): Unit = {
    val submitter = newSubmitter()

    val partitionResponses = util.List.of(
      new PartitionProduceResponse().setIndex(0).setBaseOffset(42).setErrorCode(0),
      new PartitionProduceResponse().setIndex(1)
        .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())
        .setErrorMessage("not the leader"))
    val topicResponse = new TopicProduceResponse().setName(topic).setPartitionResponses(partitionResponses)
    val data = new ProduceResponseData()
      .setResponses(new TopicProduceResponseCollection(util.List.of(topicResponse).iterator()))
      .setThrottleTimeMs(1500)
    val response = new ProduceResponse(data)

    val result = submitter.translateProduce(response, topic)

    assertEquals(2, result.partitions().size())
    val p0 = result.partitions().get(0)
    assertEquals(0, p0.partition())
    assertEquals(42, p0.offset())
    assertEquals(Errors.NONE, p0.error())

    val p1 = result.partitions().get(1)
    assertEquals(1, p1.partition())
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, p1.error())
    assertEquals("not the leader", p1.errorMessage())
    assertEquals(-1L, p1.offset(), "failed partitions must surface offset=-1 so the formatter emits null")

    assertEquals(1500L, result.throttleTimeMs())
  }

  @Test
  def buildFetchRequestUsesTopicIdAndExplicitMaxBytes(): Unit = {
    val submitter = newSubmitter()
    val cmd = new FetchRequestParser.FetchCommand(topic, 7, 42L, OptionalInt.of(2048))

    val request = submitter.buildFetchRequest(cmd)
    // FetchRequest.fetchData(topicNames) keys the map by TopicIdPartition (id + name + partition), not TopicPartition.
    val fetchData = request.fetchData(util.Map.of(topicId, topic))
    val tip = new TopicIdPartition(topicId, new TopicPartition(topic, 7))

    val partitionData = fetchData.get(tip)
    assertNotNull(partitionData, "submitter must register the requested (topicId, partition) on the FetchRequest")
    assertEquals(42L, partitionData.fetchOffset)
    assertEquals(2048, partitionData.maxBytes)
    assertEquals(topicId, partitionData.topicId, "resolved topic id must be threaded into the request")
  }

  @Test
  def translateFetchSurfacesRecordsAndContentType(): Unit = {
    val submitter = newSubmitter()
    val records = MemoryRecords.withRecords(Compression.NONE,
      new SimpleRecord(1700_000_000L, "k".getBytes, "{\"a\":1}".getBytes,
        Array(new RecordHeader("content-type", "application/json".getBytes(StandardCharsets.UTF_8)))))

    val partitionData = new FetchPartitionData()
      .setPartitionIndex(3)
      .setHighWatermark(10L)
      .setLogStartOffset(0L)
      .setRecords(records)

    val topicResp = new FetchableTopicResponse().setTopicId(topicId)
      .setPartitions(util.List.of(partitionData))
    val data = new FetchResponseData()
      .setResponses(util.List.of(topicResp))
      .setThrottleTimeMs(750)
    val response = new FetchResponse(data)

    val cmd = new FetchRequestParser.FetchCommand(topic, 3, 0L, OptionalInt.empty())
    val result = submitter.translateFetch(response, cmd)

    val fetch = result.partition()
    assertEquals(3, fetch.partition())
    assertEquals(Errors.NONE, fetch.error())
    assertEquals(10L, fetch.highWatermark())
    assertEquals(1, fetch.records().size())

    val record = fetch.records().get(0)
    assertEquals("k", new String(record.key(), StandardCharsets.UTF_8))
    assertEquals("{\"a\":1}", new String(record.value(), StandardCharsets.UTF_8))
    assertEquals("application/json", record.contentType(),
      "content-type header on the wire must surface back through the bridge")
    assertEquals(750L, result.throttleTimeMs())
  }

  @Test
  def translateFetchPreservesNullRecordValues(): Unit = {
    // Compacted-topic tombstones and HTTP-produced {"type":"NULL"} records reach the bridge as Kafka records with
    // record.hasValue() == false. The translation MUST surface those as Java null (not Array.emptyByteArray) so the
    // downstream ValueSerializer emits {"type":"NULL"} instead of misclassifying as {"type":"STRING","data":""}.
    val submitter = newSubmitter()
    val records = MemoryRecords.withRecords(Compression.NONE,
      new SimpleRecord(1700_000_000L, "k".getBytes, null: Array[Byte]))

    val partitionData = new FetchPartitionData()
      .setPartitionIndex(0)
      .setHighWatermark(1L)
      .setLogStartOffset(0L)
      .setRecords(records)
    val topicResp = new FetchableTopicResponse().setTopicId(topicId)
      .setPartitions(util.List.of(partitionData))
    val response = new FetchResponse(new FetchResponseData().setResponses(util.List.of(topicResp)))

    val cmd = new FetchRequestParser.FetchCommand(topic, 0, 0L, OptionalInt.empty())
    val result = submitter.translateFetch(response, cmd)

    assertEquals(1, result.partition().records().size())
    val record = result.partition().records().get(0)
    assertNull(record.value(),
      "null Kafka record values must surface as Java null so ValueSerializer can emit {type:NULL}; " +
        "Array.emptyByteArray would collapse to {type:STRING,data:''} and lose the tombstone signal")
  }

  @Test
  def translateFetchTreatsThrottledEmptyResponseAsSuccessfulEmptyPage(): Unit = {
    // KafkaApis.handleFetchRequest returns an empty FetchResponse (no partition data) with throttleTimeMs > 0 when the
    // consumer's fetch quota is exceeded. The bridge MUST NOT fabricate UNKNOWN_TOPIC_OR_PARTITION here — that collapses
    // to HTTP 404 and drops Retry-After per PROMPT.md AC3. Surface as an empty NONE page so the formatter emits
    // 200 + Retry-After.
    val submitter = newSubmitter()
    val response = new FetchResponse(new FetchResponseData()
      .setThrottleTimeMs(750))

    val cmd = new FetchRequestParser.FetchCommand(topic, 0, 42L, OptionalInt.empty())
    val result = submitter.translateFetch(response, cmd)

    assertEquals(Errors.NONE, result.partition().error(),
      "throttled empty fetch must not be reported as UNKNOWN_TOPIC_OR_PARTITION (would drop Retry-After on 404)")
    assertEquals(0, result.partition().records().size())
    assertEquals(42L, result.partition().requestedOffset(),
      "requested offset must round-trip so the formatter's _links cursors remain accurate")
    assertEquals(42L, result.partition().logStartOffset(),
      "throttled response carries no broker offsets; the honest fallback is the request offset so cursor-following " +
        "clients do not snap to 0 (which would replay from the beginning under a transient throttle)")
    assertEquals(42L, result.partition().highWatermark(),
      "highWatermark must mirror the request offset for the same reason — an HWM at the requested offset means " +
        "'no records past where you asked', not 'the partition is empty'")
    assertEquals(750L, result.throttleTimeMs(),
      "throttle hint must flow through so the formatter can emit Retry-After")
  }

  @Test
  def translateFetchSurfacesPartitionErrors(): Unit = {
    val submitter = newSubmitter()

    val partitionData = new FetchPartitionData()
      .setPartitionIndex(0)
      .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
    val topicResp = new FetchableTopicResponse().setTopicId(topicId)
      .setPartitions(util.List.of(partitionData))
    val response = new FetchResponse(new FetchResponseData().setResponses(util.List.of(topicResp)))

    val cmd = new FetchRequestParser.FetchCommand(topic, 0, 99L, OptionalInt.empty())
    val result = submitter.translateFetch(response, cmd)

    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, result.partition().error())
    assertEquals(0, result.partition().records().size())
  }

  // ----- end-to-end through a real RequestChannel ------------------------------------------------------------------

  @Test
  def submitProduceRoundTripsThroughChannel(): Unit = {
    val expectedReceived = new mutable.ArrayBuffer[AbstractRequest]()
    fakeHandler = new FakeApiHandler(channel, request => {
      expectedReceived += request
      val partitionResponses = util.List.of(
        new PartitionProduceResponse().setIndex(0).setBaseOffset(7L).setErrorCode(0))
      val topicResponse = new TopicProduceResponse().setName(topic).setPartitionResponses(partitionResponses)
      new ProduceResponse(new ProduceResponseData()
        .setResponses(new TopicProduceResponseCollection(util.List.of(topicResponse).iterator()))
        .setThrottleTimeMs(0))
    })
    fakeHandler.start()

    val submitter = newSubmitter()
    val cmd = new ProduceRequestParser.ProduceCommand(topic, util.List.of(newRecord(0, "k", "v")))
    val future = submitter.submitProduce(cmd)

    val result = future.get(5, TimeUnit.SECONDS)
    assertEquals(1, result.partitions().size())
    assertEquals(7L, result.partitions().get(0).offset())
    assertEquals(Errors.NONE, result.partitions().get(0).error())

    assertEquals(1, expectedReceived.size, "the channel should have received exactly one request")
    assertTrue(expectedReceived.head.isInstanceOf[ProduceRequest])
  }

  @Test
  def translateExceptionInCallbackFailsTheFuture(): Unit = {
    // The handler returns the WRONG response type for the request kind. The translate lambda casts to ProduceResponse;
    // the ClassCastException must escape via future.completeExceptionally rather than killing the request-handler thread.
    fakeHandler = new FakeApiHandler(channel, _ => new FetchResponse(new FetchResponseData()))
    fakeHandler.start()

    val submitter = newSubmitter()
    val cmd = new ProduceRequestParser.ProduceCommand(topic, util.List.of(newRecord(0, "k", "v")))
    val future = submitter.submitProduce(cmd)

    val ex = assertThrows(classOf[java.util.concurrent.ExecutionException], () => future.get(5, TimeUnit.SECONDS))
    assertTrue(ex.getCause.isInstanceOf[ClassCastException],
      s"expected ClassCastException, was ${ex.getCause.getClass.getName}: ${ex.getCause.getMessage}")
  }

  @Test
  def submitFetchRoundTripsThroughChannel(): Unit = {
    val responseRecords = MemoryRecords.withRecords(Compression.NONE,
      new SimpleRecord(time.milliseconds(), "k".getBytes, "v".getBytes))
    fakeHandler = new FakeApiHandler(channel, _ => {
      val partitionData = new FetchPartitionData()
        .setPartitionIndex(0)
        .setHighWatermark(1L)
        .setLogStartOffset(0L)
        .setRecords(responseRecords)
      val topicResp = new FetchableTopicResponse().setTopicId(topicId)
        .setPartitions(util.List.of(partitionData))
      new FetchResponse(new FetchResponseData().setResponses(util.List.of(topicResp)))
    })
    fakeHandler.start()

    val submitter = newSubmitter()
    val cmd = new FetchRequestParser.FetchCommand(topic, 0, 0L, OptionalInt.empty())
    val future = submitter.submitFetch(cmd)

    val result = future.get(5, TimeUnit.SECONDS)
    val fetch = result.partition()
    assertEquals(Errors.NONE, fetch.error())
    assertEquals(1, fetch.records().size())
    assertEquals("v", new String(fetch.records().get(0).value(), StandardCharsets.UTF_8))
  }

  // ----- helpers ---------------------------------------------------------------------------------------------------

  private def newRecord(partition: Int, key: String, value: String): ProduceRequestParser.RecordEntry =
    new ProduceRequestParser.RecordEntry(OptionalInt.of(partition), key.getBytes, value.getBytes, null)

  private def recordCount(records: MemoryRecords): Int = {
    var count = 0
    records.records().forEach(_ => count += 1)
    count
  }

  /**
   * Pretends to be the broker's API handler thread: pulls requests off the channel, invokes the responder lambda
   * to obtain the AbstractResponse the test wants to surface, and hands it back via sendResponse. Because the
   * RequestChannel.Request carries the requestCompletionCallback, sendResponse short-circuits the SendResponse path
   * and completes the submitter's future directly.
   */
  private final class FakeApiHandler(
    channel: RequestChannel,
    responder: AbstractRequest => AbstractResponse
  ) extends Thread("fake-api-handler") {
    setDaemon(true)
    private val stopFlag = new AtomicBoolean(false)
    private val started = new CountDownLatch(1)

    override def run(): Unit = {
      started.countDown()
      while (!stopFlag.get()) {
        val received = channel.receiveRequest(50L)
        received match {
          case null => // poll timeout — loop
          case req: RequestChannel.Request =>
            val response = responder(req.body[AbstractRequest])
            channel.sendResponse(req, response, None)
          case _ => // ignore other BaseRequest subtypes
        }
      }
    }

    override def start(): Unit = {
      super.start()
      started.await(2L, TimeUnit.SECONDS)
    }

    def shutdown(): Unit = {
      stopFlag.set(true)
      join(2000L)
    }
  }
}
