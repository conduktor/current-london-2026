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
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent}
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.test.{KafkaClusterTestKit, TestKitNodes}
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.metadata.authorizer.StandardAuthorizer
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.server.authorizer.{Action, AuthorizableRequestContext, AuthorizationResult}
import org.apache.kafka.server.config.ServerConfigs
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Tag, Test, Timeout}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util
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

  @Test
  def sseStreamsPostedRecordsAcrossReplayAndLiveBoundary(): Unit = {
    // PROMPT.md AC6 / FS6: a real-broker check that the SSE endpoint streams the historical backlog AND continues
    // to deliver new records produced after the consumer is already attached, without a reconnect. We attach the SSE
    // consumer first, then post two records (so they show up in "replay" mode for the consumer's offset=0 starting
    // point), wait for those to land on the wire, then post a third record AFTER the consumer has consumed the first
    // two — and assert the third arrives on the same socket. That third arrival is the "no reconnect transition"
    // contract: replay-phase and live-phase are the same connection.
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder()
        .setNumBrokerNodes(1)
        .setNumControllerNodes(1)
        .build())
      .setConfigProp(SocketServerConfigs.HTTP_BRIDGE_ENABLED_CONFIG, "true")
      .setConfigProp(SocketServerConfigs.HTTP_BRIDGE_HOST_CONFIG, "127.0.0.1")
      .setConfigProp(SocketServerConfigs.HTTP_BRIDGE_PORT_CONFIG, "0")
      .build()
    val topicName = "http-bridge-sse"
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()

      val broker = cluster.brokers().get(0)
      TestUtils.waitUntilTrue(() => broker.brokerState == BrokerState.RUNNING, "Broker never reached RUNNING.")
      TestUtils.waitUntilTrue(() => broker.httpBridgeServer != null && broker.httpBridgeServer.boundPort() > 0,
        "HTTP bridge never bound its port.")
      val bridgePort = broker.httpBridgeServer.boundPort()

      createTopic(cluster, topicName, partitions = 1)

      // Seed two records BEFORE the SSE consumer connects — these will be the replay-phase backlog.
      val seedBody =
        s"""
           |{
           |  "records": [
           |    { "partition": 0, "value": { "type": "STRING", "data": "replay-1" } },
           |    { "partition": 0, "value": { "type": "STRING", "data": "replay-2" } }
           |  ]
           |}
           |""".stripMargin
      val seedResp = postJson(s"http://127.0.0.1:$bridgePort/v1/topics/$topicName/records", seedBody)
      assertEquals(200, seedResp.statusCode(),
        s"seed produce must succeed before opening the SSE stream, body=${seedResp.body()}")

      // Open the SSE stream. Use HttpURLConnection because the JDK HttpClient buffers small responses too aggressively
      // for a streaming assertion — raw connection gives us byte-by-byte control over what's on the socket.
      val url = new java.net.URL(
        s"http://127.0.0.1:$bridgePort/v1/topics/$topicName/records?partition=0&from=earliest")
      val conn = url.openConnection().asInstanceOf[java.net.HttpURLConnection]
      conn.setRequestMethod("GET")
      conn.setRequestProperty("Accept", "text/event-stream")
      conn.setConnectTimeout(5000)
      conn.setReadTimeout(15000)
      conn.connect()
      assertEquals(200, conn.getResponseCode)
      val contentType = conn.getHeaderField("Content-Type")
      assertTrue(contentType.startsWith("text/event-stream"),
        s"expected text/event-stream content-type, got: $contentType")

      val reader = new java.io.BufferedReader(
        new java.io.InputStreamReader(conn.getInputStream, java.nio.charset.StandardCharsets.UTF_8))
      try {
        val replayed = readSseEvents(reader, expected = 2)
        // Replay-phase: both seeded records arrived in order. We extract the value.data field, which the bridge encoded
        // via ValueSerializer.encode → STRING envelope (the UTF-8-valid branch).
        assertEquals(Seq("replay-1", "replay-2"),
          replayed.map(_.data.get("value").get("data").asText()),
          "replay phase must deliver the seeded records in order")
        // Each event's `id:` carries the broker-assigned offset; first two are 0 and 1.
        assertEquals(Seq("0", "1"), replayed.map(_.id.getOrElse("")),
          "every replay-phase event must carry the broker's offset as `id:` so Last-Event-ID can resume the stream")

        // Now post a third record AFTER the consumer has already received the backlog — this is the live-tail event.
        val liveBody =
          s"""
             |{
             |  "records": [
             |    { "partition": 0, "value": { "type": "STRING", "data": "live-3" } }
             |  ]
             |}
             |""".stripMargin
        val liveResp = postJson(s"http://127.0.0.1:$bridgePort/v1/topics/$topicName/records", liveBody)
        assertEquals(200, liveResp.statusCode(),
          s"live-phase produce must succeed while SSE consumer is attached, body=${liveResp.body()}")

        val live = readSseEvents(reader, expected = 1)
        assertEquals(Seq("live-3"), live.map(_.data.get("value").get("data").asText()),
          "live phase must deliver the record produced after the consumer connected — no reconnection")
        // Offset on the live record matches what the broker actually assigned (third record on this partition → 2).
        assertEquals(2L, live.head.data.get("offset").asLong(),
          "live-phase record's offset must be the broker's actual assignment, not a fabricated one")
        // The reconnect contract: ids form a strictly monotonic sequence (0, 1, 2) across replay→live with no gaps. A
        // client that disconnected after id=1 can resume with Last-Event-ID: 1 and pick up cleanly at id=2.
        val allIds = (replayed ++ live).flatMap(_.id).map(_.toLong)
        assertEquals(Seq(0L, 1L, 2L), allIds,
          "ids must be strictly monotonic with no gaps across the replay→live boundary — Last-Event-ID contract")
      } finally {
        reader.close()
        conn.disconnect()
      }
    } finally {
      cluster.close()
    }
  }

  /** A single parsed SSE event: the optional `id:` line and the JSON `data:` payload. */
  private case class SseEvent(id: Option[String], data: JsonNode)

  /**
   * Pulls `expected` SSE events off the reader. The bridge writes each event as `id: N\ndata: {json}\n\n`; we
   * accumulate `id:` and `data:` lines and emit an event whenever we hit the blank-line frame terminator. `:`-comments
   * (the connect prelude) and unknown framing lines are skipped.
   */
  private def readSseEvents(reader: java.io.BufferedReader, expected: Int): Seq[SseEvent] = {
    val out = scala.collection.mutable.ArrayBuffer.empty[SseEvent]
    var pendingId: Option[String] = None
    var pendingData: Option[String] = None
    while (out.size < expected) {
      val line = reader.readLine()
      if (line == null) {
        throw new java.io.EOFException(s"SSE stream closed after ${out.size}/$expected events")
      }
      if (line.isEmpty) {
        pendingData.foreach { d => out += SseEvent(pendingId, mapper.readTree(d)) }
        pendingId = None
        pendingData = None
      } else if (line.startsWith("id: ")) {
        pendingId = Some(line.substring(4))
      } else if (line.startsWith("data: ")) {
        pendingData = Some(line.substring(6))
      }
      // `event:` and `:`-comments are skipped — they're framing, not payload.
    }
    out.toSeq
  }

  @Test
  def multiPartitionMixedResultReturns207(): Unit = {
    // Spec scenario FS1 (PROMPT.md:42): a POST that targets 3 partitions where one fails returns HTTP 207 with a
    // body listing each partition's outcome. We trigger the failure by writing to a partition that doesn't exist on
    // the topic (partition 99 on a 2-partition topic); the broker surfaces UNKNOWN_TOPIC_OR_PARTITION on that
    // partition while the other two succeed. This is the cleanest way to provoke a per-partition error from a real
    // broker without killing replicas, and it proves that per-partition errorCode propagation flows end-to-end.
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder()
        .setNumBrokerNodes(1)
        .setNumControllerNodes(1)
        .build())
      .setConfigProp(SocketServerConfigs.HTTP_BRIDGE_ENABLED_CONFIG, "true")
      .setConfigProp(SocketServerConfigs.HTTP_BRIDGE_HOST_CONFIG, "127.0.0.1")
      .setConfigProp(SocketServerConfigs.HTTP_BRIDGE_PORT_CONFIG, "0")
      .build()
    val topicName = "http-bridge-207"
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()

      val broker = cluster.brokers().get(0)
      TestUtils.waitUntilTrue(() => broker.brokerState == BrokerState.RUNNING, "Broker never reached RUNNING.")
      TestUtils.waitUntilTrue(() => broker.httpBridgeServer != null && broker.httpBridgeServer.boundPort() > 0,
        "HTTP bridge never bound its port.")
      val bridgePort = broker.httpBridgeServer.boundPort()

      createTopic(cluster, topicName, partitions = 2)

      val mixedBody =
        s"""
           |{
           |  "records": [
           |    { "partition": 0,  "value": { "type": "STRING", "data": "p0" } },
           |    { "partition": 1,  "value": { "type": "STRING", "data": "p1" } },
           |    { "partition": 99, "value": { "type": "STRING", "data": "ghost" } }
           |  ]
           |}
           |""".stripMargin

      val resp = postJson(s"http://127.0.0.1:$bridgePort/v1/topics/$topicName/records", mixedBody)
      assertEquals(207, resp.statusCode(),
        s"mixed-partition produce must surface HTTP 207 Multi-Status; got status=${resp.statusCode()}, body=${resp.body()}")

      val body = parseJson(resp.body())
      val results = body.get("results")
      assertEquals(3, results.size(), s"expected one result per partition, body=${resp.body()}")

      val byPartition = (0 until results.size()).map { i =>
        val r = results.get(i)
        r.get("partition").asInt() -> r
      }.toMap

      // Partitions 0 and 1 should have succeeded with offset 0 each (first write on each partition).
      assertEquals(0, byPartition(0).get("errorCode").asInt(), s"partition 0 expected to succeed, was ${byPartition(0)}")
      assertEquals(0L, byPartition(0).get("offset").asLong())
      assertEquals(0, byPartition(1).get("errorCode").asInt(), s"partition 1 expected to succeed, was ${byPartition(1)}")
      assertEquals(0L, byPartition(1).get("offset").asLong())

      // Partition 99 doesn't exist on the topic; broker must report it without affecting the other two partitions.
      // Errors.UNKNOWN_TOPIC_OR_PARTITION = 3.
      assertEquals(3, byPartition(99).get("errorCode").asInt(),
        s"partition 99 must surface UNKNOWN_TOPIC_OR_PARTITION (code 3), was ${byPartition(99)}")
      assertTrue(byPartition(99).get("offset").isNull,
        s"failed partition must surface null offset, was ${byPartition(99).get("offset")}")
    } finally {
      cluster.close()
    }
  }

  @Test
  def aclDeniedTopicReturns403(): Unit = {
    // PROMPT.md FS4: a POST to a topic the ANONYMOUS principal lacks WRITE on must return 403, and the body must
    // carry the standard {errorCode, errorMessage} envelope. We boot the cluster with a custom authorizer that
    // selectively denies WRITE on the test topic for ANONYMOUS — every other operation (CLUSTER bootstrap,
    // CreateTopics, etc.) is allowed so the cluster still functions and the admin client still works. This is the
    // smallest setup that exercises the full Kafka authorizer pipeline end-to-end through the HTTP bridge without
    // needing SASL/SSL plumbing.
    val deniedTopic = HttpBridgeEndToEndTest.DenyAnonymousAuthorizer.DeniedTopic
    val allowedTopic = "http-bridge-acl-allow"
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder()
        .setNumBrokerNodes(1)
        .setNumControllerNodes(1)
        .build())
      .setConfigProp(SocketServerConfigs.HTTP_BRIDGE_ENABLED_CONFIG, "true")
      .setConfigProp(SocketServerConfigs.HTTP_BRIDGE_HOST_CONFIG, "127.0.0.1")
      .setConfigProp(SocketServerConfigs.HTTP_BRIDGE_PORT_CONFIG, "0")
      .setConfigProp(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG,
        classOf[HttpBridgeEndToEndTest.DenyAnonymousAuthorizer].getName)
      // ANONYMOUS must still be super on the controller-internal path so the broker can register itself, fetch
      // metadata, and answer heartbeats — those calls don't go near our deniedTopic guard.
      .setConfigProp(StandardAuthorizer.SUPER_USERS_CONFIG, "User:ANONYMOUS")
      .build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()

      val broker = cluster.brokers().get(0)
      TestUtils.waitUntilTrue(() => broker.brokerState == BrokerState.RUNNING, "Broker never reached RUNNING.")
      TestUtils.waitUntilTrue(() => broker.httpBridgeServer != null && broker.httpBridgeServer.boundPort() > 0,
        "HTTP bridge never bound its port.")
      val bridgePort = broker.httpBridgeServer.boundPort()

      // Both topics exist — only the WRITE on deniedTopic should fail. createTopic itself is allowed for ANONYMOUS
      // because the custom authorizer only denies WRITE on the named topic, not CREATE.
      createTopic(cluster, deniedTopic, partitions = 1)
      createTopic(cluster, allowedTopic, partitions = 1)

      // Sanity: the allowed topic still produces successfully, proving the authorizer isn't a blanket deny.
      val okBody =
        s"""
           |{ "records": [ { "partition": 0, "value": { "type": "STRING", "data": "ok" } } ] }
           |""".stripMargin
      val okResp = postJson(s"http://127.0.0.1:$bridgePort/v1/topics/$allowedTopic/records", okBody)
      assertEquals(200, okResp.statusCode(),
        s"control-group produce on $allowedTopic must succeed under the same authorizer, body=${okResp.body()}")

      // The denied path: uniform TOPIC_AUTHORIZATION_FAILED across all partitions must collapse to a uniform-failure
      // 403 (the formatter only emits 207 for *mixed* outcomes — a single-partition deny is uniform).
      val denyBody =
        s"""
           |{ "records": [ { "partition": 0, "value": { "type": "STRING", "data": "should-fail" } } ] }
           |""".stripMargin
      val denyResp = postJson(s"http://127.0.0.1:$bridgePort/v1/topics/$deniedTopic/records", denyBody)
      assertEquals(403, denyResp.statusCode(),
        s"ACL deny on $deniedTopic must surface HTTP 403; got status=${denyResp.statusCode()}, body=${denyResp.body()}")
      // PROMPT.md AC7: 403 must NOT carry Retry-After — that header is reserved for 503/504/throttle paths.
      assertNull(denyResp.headers().firstValue("Retry-After").orElse(null),
        s"403 must not carry Retry-After, headers=${denyResp.headers().map()}")
      val denyJson = parseJson(denyResp.body())
      // The body shape is the same as 200 and 207 — {topic, results: [{partition, offset, errorCode, errorMessage}]}.
      // PROMPT.md AC7 mandates errorCode + errorMessage on every error response; here they live per-partition (the
      // shape callers also see on 207, so a single parser handles all produce responses). errorCode is the raw Kafka
      // Errors code (TOPIC_AUTHORIZATION_FAILED = 29).
      assertEquals(deniedTopic, denyJson.get("topic").asText(),
        s"body must keep the topic context, body=${denyResp.body()}")
      val denyEntry = denyJson.get("results").get(0)
      assertEquals(0, denyEntry.get("partition").asInt())
      assertTrue(denyEntry.get("offset").isNull,
        s"failed partition must surface null offset, body=${denyResp.body()}")
      assertEquals(29, denyEntry.get("errorCode").asInt(),
        s"errorCode must be the raw Kafka code (29 = TOPIC_AUTHORIZATION_FAILED), body=${denyResp.body()}")
      assertFalse(denyEntry.get("errorMessage").isNull,
        s"errorMessage is mandatory on every error response, body=${denyResp.body()}")
    } finally {
      cluster.close()
    }
  }

  @Test
  def aclDeniedFetchReturns403(): Unit = {
    // PROMPT.md FS4 (fetch side): a GET on a topic where the ANONYMOUS principal lacks READ must return 403 with the
    // standard error envelope. Mirrors aclDeniedTopicReturns403 (which covers the WRITE-deny path on POST) but
    // exercises a separate code path inside KafkaApis (handleFetchRequest's authHelper.filterByAuthorized → per-
    // partition TOPIC_AUTHORIZATION_FAILED on the FetchResponse). The fetch-side authorizer leaves WRITE allowed so we
    // can seed a record before asserting the deny.
    val deniedTopic = HttpBridgeEndToEndTest.DenyAnonymousFetchAuthorizer.DeniedTopic
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder()
        .setNumBrokerNodes(1)
        .setNumControllerNodes(1)
        .build())
      .setConfigProp(SocketServerConfigs.HTTP_BRIDGE_ENABLED_CONFIG, "true")
      .setConfigProp(SocketServerConfigs.HTTP_BRIDGE_HOST_CONFIG, "127.0.0.1")
      .setConfigProp(SocketServerConfigs.HTTP_BRIDGE_PORT_CONFIG, "0")
      .setConfigProp(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG,
        classOf[HttpBridgeEndToEndTest.DenyAnonymousFetchAuthorizer].getName)
      .setConfigProp(StandardAuthorizer.SUPER_USERS_CONFIG, "User:ANONYMOUS")
      .build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()

      val broker = cluster.brokers().get(0)
      TestUtils.waitUntilTrue(() => broker.brokerState == BrokerState.RUNNING, "Broker never reached RUNNING.")
      TestUtils.waitUntilTrue(() => broker.httpBridgeServer != null && broker.httpBridgeServer.boundPort() > 0,
        "HTTP bridge never bound its port.")
      val bridgePort = broker.httpBridgeServer.boundPort()

      createTopic(cluster, deniedTopic, partitions = 1)

      // Seed a record — WRITE is allowed for the test topic; only READ is denied. We seed through the HTTP bridge for
      // consistency with the rest of the file (and because that path is already proven to work in
      // producedRecordIsVisibleViaHttpFetchAndBinaryConsumer).
      val seedBody =
        s"""
           |{ "records": [ { "partition": 0, "value": { "type": "STRING", "data": "seed" } } ] }
           |""".stripMargin
      val seedResp = postJson(s"http://127.0.0.1:$bridgePort/v1/topics/$deniedTopic/records", seedBody)
      assertEquals(200, seedResp.statusCode(),
        s"seed produce must succeed when only READ is denied, body=${seedResp.body()}")

      // The denied fetch: per-partition TOPIC_AUTHORIZATION_FAILED in the FetchResponse collapses to HTTP 403.
      val fetchResp = httpGet(s"http://127.0.0.1:$bridgePort/v1/topics/$deniedTopic/records?partition=0&offset=0")
      assertEquals(403, fetchResp.statusCode(),
        s"ACL deny on READ must surface HTTP 403; got status=${fetchResp.statusCode()}, body=${fetchResp.body()}")
      // PROMPT.md AC7: 403 must NOT carry Retry-After — that header is reserved for 503 / 504 / throttle paths.
      assertNull(fetchResp.headers().firstValue("Retry-After").orElse(null),
        s"403 must not carry Retry-After, headers=${fetchResp.headers().map()}")
      val errJson = parseJson(fetchResp.body())
      // Error envelope: {errorCode, errorMessage, topic, partition}. errorCode 29 = TOPIC_AUTHORIZATION_FAILED.
      assertEquals(29, errJson.get("errorCode").asInt(),
        s"errorCode must be 29 = TOPIC_AUTHORIZATION_FAILED, body=${fetchResp.body()}")
      assertFalse(errJson.get("errorMessage").isNull,
        s"errorMessage is mandatory on every error response, body=${fetchResp.body()}")
      assertEquals(deniedTopic, errJson.get("topic").asText(),
        s"error body must keep the topic context, body=${fetchResp.body()}")
      assertEquals(0, errJson.get("partition").asInt(),
        s"error body must name the partition that was denied, body=${fetchResp.body()}")
    } finally {
      cluster.close()
    }
  }

  @Test
  def fetchNextCursorFollowsAcrossPagesContiguously(): Unit = {
    // PROMPT.md FS5: a client that drives navigation through the HAL+JSON `_links.next` cursor must land on a
    // contiguous next page — no gaps, no duplicates. This proves two things end-to-end against a real broker:
    //
    //   (a) `next` after a caught-up fetch is idempotent — the cursor points at "the offset after the last record
    //       we returned", and if no new records exist yet, refetching with that cursor returns an empty records
    //       array and the same `next` again. That's the long-poll / SSE-style follow contract — a client can loop
    //       on `next` without server-side pagination state.
    //
    //   (b) The cursor is a logical offset, not a server-side handle: after the caught-up fetch, we produce more
    //       records and re-issue a GET with the SAME cursor. The new records appear (with offsets strictly greater
    //       than the previous page's last record), proving the cursor refers to a position in the log rather than a
    //       fragile session pointer.
    //
    // The unit-level cursor round-trip is already covered by FetchResponseFormatterTest + CursorCodecTest; this is
    // the real-wire counterpart that exercises servlet + bridge + RequestChannel + KafkaApis with cursor strings the
    // formatter itself produced and the parser then decodes.
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder()
        .setNumBrokerNodes(1)
        .setNumControllerNodes(1)
        .build())
      .setConfigProp(SocketServerConfigs.HTTP_BRIDGE_ENABLED_CONFIG, "true")
      .setConfigProp(SocketServerConfigs.HTTP_BRIDGE_HOST_CONFIG, "127.0.0.1")
      .setConfigProp(SocketServerConfigs.HTTP_BRIDGE_PORT_CONFIG, "0")
      .build()
    val topicName = "http-bridge-cursor"
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()

      val broker = cluster.brokers().get(0)
      TestUtils.waitUntilTrue(() => broker.brokerState == BrokerState.RUNNING, "Broker never reached RUNNING.")
      TestUtils.waitUntilTrue(() => broker.httpBridgeServer != null && broker.httpBridgeServer.boundPort() > 0,
        "HTTP bridge never bound its port.")
      val bridgePort = broker.httpBridgeServer.boundPort()

      createTopic(cluster, topicName, partitions = 1)

      // Seed three records — first page.
      val firstPageBody =
        s"""
           |{
           |  "records": [
           |    { "partition": 0, "value": { "type": "STRING", "data": "page1-r1" } },
           |    { "partition": 0, "value": { "type": "STRING", "data": "page1-r2" } },
           |    { "partition": 0, "value": { "type": "STRING", "data": "page1-r3" } }
           |  ]
           |}
           |""".stripMargin
      val firstPageProduce = postJson(s"http://127.0.0.1:$bridgePort/v1/topics/$topicName/records", firstPageBody)
      assertEquals(200, firstPageProduce.statusCode(),
        s"first-page seed produce must succeed, body=${firstPageProduce.body()}")

      // First fetch: discover the page via the explicit ?partition&offset form. This is the "first call" path —
      // the client doesn't have a cursor yet. The response carries the next cursor we'll follow.
      val firstFetch = httpGet(s"http://127.0.0.1:$bridgePort/v1/topics/$topicName/records?partition=0&offset=0")
      assertEquals(200, firstFetch.statusCode(), s"first fetch failed: ${firstFetch.body()}")
      val firstJson = parseJson(firstFetch.body())
      val firstRecords = firstJson.get("partitions").get(0).get("records")
      assertEquals(3, firstRecords.size(), s"first fetch must see all three seeded records, body=${firstFetch.body()}")
      // Sanity: offsets are 0, 1, 2 — contiguous from the log start.
      val firstOffsets = (0 until firstRecords.size()).map(i => firstRecords.get(i).get("offset").asLong())
      assertEquals(Seq(0L, 1L, 2L), firstOffsets,
        "first-page record offsets must be the broker's actual assignments (0, 1, 2), not formatter-fabricated")

      val nextCursor = firstJson.get("_links").get("next").asText()
      assertNotNull(nextCursor, "the next cursor is mandatory on every fetch response — it's how clients page forward")

      // Second fetch via the cursor — we've caught up to the high watermark, so the response must be empty AND the
      // `next` cursor must still point at the same logical offset. That's the idempotent "no progress yet" signal a
      // long-poll client uses.
      val caughtUp = httpGet(s"http://127.0.0.1:$bridgePort/v1/topics/$topicName/records?cursor=" +
        java.net.URLEncoder.encode(nextCursor, java.nio.charset.StandardCharsets.UTF_8))
      assertEquals(200, caughtUp.statusCode(), s"cursor follow into caught-up state must succeed: ${caughtUp.body()}")
      val caughtUpJson = parseJson(caughtUp.body())
      val caughtUpRecords = caughtUpJson.get("partitions").get(0).get("records")
      assertEquals(0, caughtUpRecords.size(),
        s"caught-up fetch must return zero records, body=${caughtUp.body()}")
      assertEquals(nextCursor, caughtUpJson.get("_links").get("next").asText(),
        "caught-up fetch must echo back the same `next` cursor — it's the idempotent long-poll contract")

      // Now seed a SECOND page after the caught-up read. Re-following the SAME cursor must see only the new
      // records, with offsets strictly after the first page's last offset (= 2). This is the load-bearing part of
      // FS5: the cursor is a logical offset, not a server-side pagination handle that could be invalidated by an
      // intervening empty fetch.
      val secondPageBody =
        s"""
           |{
           |  "records": [
           |    { "partition": 0, "value": { "type": "STRING", "data": "page2-r1" } },
           |    { "partition": 0, "value": { "type": "STRING", "data": "page2-r2" } }
           |  ]
           |}
           |""".stripMargin
      val secondPageProduce = postJson(s"http://127.0.0.1:$bridgePort/v1/topics/$topicName/records", secondPageBody)
      assertEquals(200, secondPageProduce.statusCode(),
        s"second-page seed produce must succeed, body=${secondPageProduce.body()}")

      val secondFetch = httpGet(s"http://127.0.0.1:$bridgePort/v1/topics/$topicName/records?cursor=" +
        java.net.URLEncoder.encode(nextCursor, java.nio.charset.StandardCharsets.UTF_8))
      assertEquals(200, secondFetch.statusCode(), s"second-page fetch via cursor failed: ${secondFetch.body()}")
      val secondJson = parseJson(secondFetch.body())
      val secondRecords = secondJson.get("partitions").get(0).get("records")
      assertEquals(2, secondRecords.size(),
        s"second-page fetch must see only the records produced AFTER the first cursor, body=${secondFetch.body()}")
      val secondOffsets = (0 until secondRecords.size()).map(i => secondRecords.get(i).get("offset").asLong())
      // Offsets must be 3 and 4 — strictly greater than the first page's last offset (2). No gaps, no duplicates.
      assertEquals(Seq(3L, 4L), secondOffsets,
        s"second-page offsets must be contiguous with the first page's last (no gaps, no dupes), got $secondOffsets")
      // And the values are exactly the new ones — proving we didn't accidentally re-read the first page.
      val secondValues = (0 until secondRecords.size())
        .map(i => secondRecords.get(i).get("value").get("data").asText())
      assertEquals(Seq("page2-r1", "page2-r2"), secondValues,
        "cursor-followed fetch must return only the new records, not a re-read of the first page")
      // The `next` cursor on the second page must have advanced past the new records (logically at offset 5 — one
      // past the last record at offset 4). We don't decode the cursor here (CursorCodecTest covers that); we only
      // assert it has CHANGED relative to the first response's `next`. If pagination were broken — say, the
      // formatter handed back the same cursor regardless of records returned — a client following `next` in a loop
      // would re-read the same records forever. This catches that class of regression cheaply.
      val secondNextCursor = secondJson.get("_links").get("next").asText()
      assertNotEquals(nextCursor, secondNextCursor,
        "second page's `next` cursor must advance past the new records, not echo the first page's cursor")
    } finally {
      cluster.close()
    }
  }

  @Test
  def quotaThrottleSurfacesRetryAfterOn200(): Unit = {
    // PROMPT.md AC3 / FS3: when the broker's producer-byte-rate quota for `User:ANONYMOUS` is exceeded, the HTTP
    // produce response is still 200 (Kafka considers throttled produces successful — records are written, offsets are
    // assigned) but carries a `Retry-After` header so the client knows to back off. This is the only path in the
    // bridge where a 200 response carries Retry-After; every other 200 response (an un-throttled produce) drops the
    // hint. The unit layer covers this in ProduceResponseFormatterTest.positiveThrottleSetsRetryAfterOn200; this is
    // the real-wire counterpart that exercises broker-side quota enforcement → ProduceResponse.throttleTimeMs →
    // bridge → HTTP header end-to-end.
    //
    // Why the test is fast even with a tight quota: the bridge submitter wires a per-request completion callback
    // through processor=-1 + memoryPool=NONE on the RequestChannel.Request. RequestChannel.sendResponse short-
    // circuits on that callback — no Processor lookup, no channel muting, no response queueing. The broker computes
    // the throttle and embeds it in the ProduceResponse, the callback fires immediately, and the bridge propagates
    // throttleTimeMs to Retry-After. So a throttle of 18 000 ms (the magnitude this fixture produces) does NOT delay
    // the HTTP response — it only sets the header.
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder()
        .setNumBrokerNodes(1)
        .setNumControllerNodes(1)
        .build())
      .setConfigProp(SocketServerConfigs.HTTP_BRIDGE_ENABLED_CONFIG, "true")
      .setConfigProp(SocketServerConfigs.HTTP_BRIDGE_HOST_CONFIG, "127.0.0.1")
      .setConfigProp(SocketServerConfigs.HTTP_BRIDGE_PORT_CONFIG, "0")
      .build()
    val topicName = "http-bridge-quota"
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()

      val broker = cluster.brokers().get(0)
      TestUtils.waitUntilTrue(() => broker.brokerState == BrokerState.RUNNING, "Broker never reached RUNNING.")
      TestUtils.waitUntilTrue(() => broker.httpBridgeServer != null && broker.httpBridgeServer.boundPort() > 0,
        "HTTP bridge never bound its port.")
      val bridgePort = broker.httpBridgeServer.boundPort()

      createTopic(cluster, topicName, partitions = 1)

      // Provision a tight producer-byte-rate quota on User:ANONYMOUS. The bridge submits every HTTP request as
      // ANONYMOUS, so this targets exactly the bridge's traffic. We pick 1024 B/s — small enough that any payload
      // larger than the 11-second sliding window quota (11 × 1024 = 11 264 bytes) will trip a positive throttle on
      // the first request, big enough to be a believable operator setting that isn't fighting the quota algorithm's
      // sample-window edge cases at the bottom of the range.
      val admin = Admin.create(cluster.clientProperties())
      try {
        val entity = new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, "ANONYMOUS"))
        val ops = Collections.singletonList(new ClientQuotaAlteration.Op("producer_byte_rate", 1024.0d))
        admin.alterClientQuotas(Collections.singletonList(
          new ClientQuotaAlteration(entity, ops))).all().get()
        // Wait for the quota to land on the broker — alterClientQuotas's future completes when the controller wrote
        // the metadata record, not necessarily when every broker has consumed it. Polling describeClientQuotas via
        // an Admin client routed through the broker proves visibility from the broker's quota manager's perspective.
        val filter = ClientQuotaFilter.containsOnly(Collections.singletonList(
          ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, "ANONYMOUS")))
        TestUtils.waitUntilTrue(() => {
          val described = admin.describeClientQuotas(filter).entities().get()
          val ent = described.get(entity)
          ent != null && ent.containsKey("producer_byte_rate")
        }, "broker never saw producer_byte_rate quota for User:ANONYMOUS")
      } finally {
        admin.close()
      }

      // Build a single produce body large enough to clear the 11 264-byte window budget by a comfortable margin. The
      // STRING envelope decodes directly to UTF-8 bytes (no Base64 round-trip on the inbound path — that only applies
      // to the outbound BINARY envelope), so 24 KiB of ASCII "x" becomes ~24 KiB of record value bytes plus a small
      // per-record header. We're comfortably above the 11 KiB window budget; the exact throttle magnitude is not
      // load-bearing — the test only asserts it's positive — so the margin protects against future quota-window
      // tuning shifting the threshold underneath us.
      val padding = "x" * 24576
      val body =
        s"""
           |{ "records": [ { "partition": 0, "value": { "type": "STRING", "data": "$padding" } } ] }
           |""".stripMargin

      val resp = postJson(s"http://127.0.0.1:$bridgePort/v1/topics/$topicName/records", body)
      assertEquals(200, resp.statusCode(),
        s"throttled produce must still be 200 (records were written, just rate-limited), body=${resp.body()}")

      // PROMPT.md AC3: a positive throttleTimeMs from the broker MUST surface as a Retry-After header on the HTTP
      // response. The unit-level mapping is covered by RetryAfterCalculator.forStatus(200, throttleMs); this
      // assertion proves the broker → bridge → HTTP path actually delivers it end-to-end.
      val retryAfter = resp.headers().firstValue("Retry-After")
      assertTrue(retryAfter.isPresent,
        s"throttled produce must surface Retry-After; headers=${resp.headers().map()}")
      val seconds = retryAfter.get().toInt
      // RetryAfterCalculator does ceil(throttleMs/1000) so the floor for any positive throttle is 1 second.
      assertTrue(seconds >= 1,
        s"Retry-After must be a positive seconds value (ceiling of throttleMs/1000), was: ${retryAfter.get()}")

      // Sanity: the body is the standard success shape, not an error envelope. Throttled produces still return the
      // per-partition offset block — they're not errors, just rate-limited successes.
      val json = parseJson(resp.body())
      assertEquals(topicName, json.get("topic").asText())
      val results = json.get("results")
      assertEquals(1, results.size())
      val partResult = results.get(0)
      assertEquals(0, partResult.get("partition").asInt())
      assertEquals(0, partResult.get("errorCode").asInt(),
        s"throttled produce records succeed on the broker side — errorCode must be 0, body=${resp.body()}")
      assertEquals(0L, partResult.get("offset").asLong(),
        "throttled produce still gets a real offset (Kafka writes the record then sends back the throttle hint)")
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

object HttpBridgeEndToEndTest {
  /**
   * Narrowest possible test authorizer for the FS4 deny case: extends StandardAuthorizer so cluster bootstrap,
   * topic creation, fetch metadata, etc. all flow through the normal allow path, but selectively returns DENIED for
   * WRITE on a single named topic when the principal is ANONYMOUS. We avoid configuring deny ACLs through the admin
   * client because super.users (which we need so the broker can register itself as ANONYMOUS) bypasses ACLs entirely —
   * subclassing authorize() is the only way to enforce a deny that the super-user path can't dodge.
   */
  class DenyAnonymousAuthorizer extends StandardAuthorizer {
    override def authorize(
      requestContext: AuthorizableRequestContext,
      actions: util.List[Action]
    ): util.List[AuthorizationResult] = {
      val anonymous = requestContext.principal() == KafkaPrincipal.ANONYMOUS
      val results = new util.ArrayList[AuthorizationResult](actions.size())
      val it = actions.iterator()
      while (it.hasNext) {
        val a = it.next()
        if (anonymous
            && a.resourcePattern().resourceType() == ResourceType.TOPIC
            && a.resourcePattern().name() == DenyAnonymousAuthorizer.DeniedTopic
            && a.operation() == AclOperation.WRITE) {
          results.add(AuthorizationResult.DENIED)
        } else {
          results.add(AuthorizationResult.ALLOWED)
        }
      }
      results
    }
  }

  object DenyAnonymousAuthorizer {
    val DeniedTopic = "http-bridge-acl-deny"
  }

  /**
   * Sibling of [[DenyAnonymousAuthorizer]] for the fetch-side ACL test: same shape but denies READ on the test topic
   * instead of WRITE. The override returns ALLOWED/DENIED directly (it does not call {@code super.authorize}); the
   * {@code super.users=User:ANONYMOUS} the test sets is a safety belt for any code path that consults the super-user
   * list outside this method (e.g. internal controller bootstrap), not the mechanism by which non-target actions are
   * permitted here. We keep this as a separate class rather than parameterising the existing authorizer because each
   * test names its own AUTHORIZER_CLASS_NAME_CONFIG and the topic + operation are intrinsically linked, so
   * parameterisation would cost a config-prop trip without saving any code.
   */
  class DenyAnonymousFetchAuthorizer extends StandardAuthorizer {
    override def authorize(
      requestContext: AuthorizableRequestContext,
      actions: util.List[Action]
    ): util.List[AuthorizationResult] = {
      val anonymous = requestContext.principal() == KafkaPrincipal.ANONYMOUS
      val results = new util.ArrayList[AuthorizationResult](actions.size())
      val it = actions.iterator()
      while (it.hasNext) {
        val a = it.next()
        if (anonymous
            && a.resourcePattern().resourceType() == ResourceType.TOPIC
            && a.resourcePattern().name() == DenyAnonymousFetchAuthorizer.DeniedTopic
            && a.operation() == AclOperation.READ) {
          results.add(AuthorizationResult.DENIED)
        } else {
          results.add(AuthorizationResult.ALLOWED)
        }
      }
      results
    }
  }

  object DenyAnonymousFetchAuthorizer {
    val DeniedTopic = "http-bridge-acl-fetch-deny"
  }
}
