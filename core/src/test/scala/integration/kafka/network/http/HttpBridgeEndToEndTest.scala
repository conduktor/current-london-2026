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

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.test.{KafkaClusterTestKit, TestKitNodes}
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.network.SocketServerConfigs
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Tag, Test, Timeout}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Collections
import java.util.Properties

import scala.jdk.CollectionConverters._

/**
 * End-to-end integration test for the embedded HTTP bridge. Boots a real single-broker KRaft cluster with
 * [[SocketServerConfigs.HTTP_BRIDGE_ENABLED_CONFIG http.bridge.enabled=true]] on an ephemeral port, drives produce and
 * fetch through HTTP, and verifies the records are real Kafka messages by also consuming them through the binary
 * protocol with a [[KafkaConsumer]]. This is the only test that exercises the full pipeline — HTTP servlet → bridge
 * orchestrator → KafkaApiRequestSubmitter → RequestChannel → KafkaApis → log → fetch purgatory — so a regression
 * anywhere in that chain shows up here.
 */
@Timeout(120)
@Tag("integration")
class HttpBridgeEndToEndTest {

  private val topic = "http-bridge-e2e"
  private val httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()
  private val mapper = new ObjectMapper()

  @Test
  def producedRecordIsVisibleViaHttpFetchAndBinaryConsumer(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder()
        .setNumBrokerNodes(1)
        .setNumControllerNodes(1)
        .build())
      .setConfigProp(SocketServerConfigs.HTTP_BRIDGE_ENABLED_CONFIG, "true")
      .setConfigProp(SocketServerConfigs.HTTP_BRIDGE_HOST_CONFIG, "127.0.0.1")
      .setConfigProp(SocketServerConfigs.HTTP_BRIDGE_PORT_CONFIG, "0") // ephemeral
      .build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()

      val broker = cluster.brokers().get(0)
      TestUtils.waitUntilTrue(() => broker.brokerState == BrokerState.RUNNING,
        "Broker never reached RUNNING.")
      TestUtils.waitUntilTrue(() => broker.httpBridgeServer != null && broker.httpBridgeServer.boundPort() > 0,
        "HTTP bridge never bound its port.")
      val bridgePort = broker.httpBridgeServer.boundPort()

      createTopic(cluster, topic, partitions = 2)

      // ----- HTTP produce -----------------------------------------------------------------------------------------
      val produceBody =
        s"""
           |{
           |  "records": [
           |    { "partition": 0, "key": { "type": "STRING", "data": "k0" }, "value": { "type": "STRING", "data": "hello-0" } },
           |    { "partition": 0,                                              "value": { "type": "STRING", "data": "hello-1" } },
           |    { "partition": 1,                                              "value": { "type": "JSON",   "data": { "name": "alice", "n": 7 } } }
           |  ]
           |}
           |""".stripMargin

      val produceResponse = postJson(s"http://127.0.0.1:$bridgePort/v1/topics/$topic/records", produceBody)
      assertEquals(200, produceResponse.statusCode(),
        s"expected 200 OK from /v1/topics/$topic/records, body=${produceResponse.body()}")

      val produceJson = parseJson(produceResponse.body())
      assertEquals(topic, produceJson.get("topic").asText())
      val results = produceJson.get("results")
      // Two PARTITION results, not three records: the protocol's per-partition base-offset return shape (records that
      // share a partition arrive at base, base+1, …) is what Kafka actually emits.
      assertEquals(2, results.size(), "one results entry per partition that was written to")
      val byPartition = (0 until results.size()).map { i =>
        val r = results.get(i)
        r.get("partition").asInt() -> r
      }.toMap
      val p0 = byPartition(0)
      val p1 = byPartition(1)
      assertEquals(0, p0.get("errorCode").asInt(), p0.toString)
      assertEquals(0, p1.get("errorCode").asInt(), p1.toString)
      assertEquals(0L, p0.get("offset").asLong(),
        "partition 0's base offset is 0 — first record of the topic")
      assertEquals(0L, p1.get("offset").asLong(),
        "partition 1's base offset is 0 — first record of that partition")

      // ----- HTTP fetch round-trips JSON value envelopes -----------------------------------------------------------
      val fetchResponse = httpGet(s"http://127.0.0.1:$bridgePort/v1/topics/$topic/records?partition=1&offset=0")
      assertEquals(200, fetchResponse.statusCode(), s"fetch failed: ${fetchResponse.body()}")
      val fetchJson = parseJson(fetchResponse.body())
      val partitions = fetchJson.get("partitions")
      assertEquals(1, partitions.size(), "single-partition fetch must return exactly one partition block")
      val records = partitions.get(0).get("records")
      assertTrue(records.size() >= 1, s"expected at least one record on partition 1, body=${fetchResponse.body()}")
      val firstRecord = records.get(0)
      val valueEnvelope = firstRecord.get("value")
      assertEquals("JSON", valueEnvelope.get("type").asText(),
        "JSON envelope on the wire must round-trip back as a JSON envelope (content-type record header preserved)")
      val valueData = valueEnvelope.get("data")
      assertEquals("alice", valueData.get("name").asText())
      assertEquals(7, valueData.get("n").asInt())
      assertNotNull(partitions.get(0).get("_links").get("self"), "self cursor must be emitted")
      assertNotNull(fetchJson.get("_links").get("next"), "next cursor must be emitted on the root")

      // ----- Binary consumer reads the same records ----------------------------------------------------------------
      val consumed = consumeBinary(cluster, topic, partition = 0, fromOffset = 0L, expectedRecords = 2)
      assertEquals(Seq("hello-0", "hello-1"), consumed.map { case (_, v) => v },
        "binary consumer must see the same values that were posted over HTTP")
      assertEquals("k0", consumed.head._1,
        "the first record's key from HTTP must survive into the binary protocol")
    } finally {
      cluster.close()
    }
  }

  // ----- helpers -------------------------------------------------------------------------------------------------

  private def createTopic(cluster: KafkaClusterTestKit, name: String, partitions: Int): Unit = {
    val admin = Admin.create(cluster.clientProperties())
    try {
      admin.createTopics(Collections.singletonList(
        new NewTopic(name, partitions, 1.toShort))).all().get()
      TestUtils.waitUntilTrue(
        () => admin.listTopics().names().get().contains(name),
        s"topic $name never showed up in admin listing")
    } finally {
      admin.close()
    }
  }

  private def consumeBinary(
    cluster: KafkaClusterTestKit,
    topic: String,
    partition: Int,
    fromOffset: Long,
    expectedRecords: Int
  ): Seq[(String, String)] = {
    val props = new Properties()
    props.putAll(cluster.clientProperties())
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "http-bridge-e2e-consumer")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

    val consumer = new KafkaConsumer[String, String](props)
    try {
      val tp = new TopicPartition(topic, partition)
      consumer.assign(Collections.singletonList(tp))
      consumer.seek(tp, fromOffset)

      val collected = scala.collection.mutable.ArrayBuffer.empty[(String, String)]
      TestUtils.waitUntilTrue(() => {
        consumer.poll(Duration.ofMillis(500)).records(tp).asScala.foreach { rec =>
          collected += ((Option(rec.key()).orNull, rec.value()))
        }
        collected.size >= expectedRecords
      }, s"binary consumer never saw $expectedRecords records on $tp; saw ${collected.size}")

      collected.take(expectedRecords).toSeq
    } finally {
      consumer.close(Duration.ofSeconds(5))
    }
  }

  private def postJson(url: String, body: String): HttpResponse[String] = {
    val request = HttpRequest.newBuilder(URI.create(url))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
      .timeout(Duration.ofSeconds(15))
      .build()
    httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
  }

  private def httpGet(url: String): HttpResponse[String] = {
    val request = HttpRequest.newBuilder(URI.create(url))
      .GET()
      .timeout(Duration.ofSeconds(15))
      .build()
    httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
  }

  private def parseJson(body: String): JsonNode = mapper.readTree(body)
}
