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

import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.message.ProduceRequestData
import org.apache.kafka.common.message.ProduceRequestData.{PartitionProduceData, TopicProduceData, TopicProduceDataCollection}
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{MemoryRecords, Records, SimpleRecord}
import org.apache.kafka.common.requests.{AbstractResponse, FetchRequest, FetchResponse, ProduceRequest, ProduceResponse, RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.network.http.{ProduceRequestParser, RequestSubmitter}
import org.apache.kafka.network.http.FetchRequestParser.FetchCommand
import org.apache.kafka.network.http.FetchResponseFormatter.{FetchedRecord, PartitionFetch}
import org.apache.kafka.network.http.ProduceRequestParser.ProduceCommand
import org.apache.kafka.network.http.ProduceResponseFormatter.PartitionResult
import org.apache.kafka.network.http.RequestSubmitter.{FetchResult, ProduceResult}

import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.util
import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

/**
 * Production [[RequestSubmitter]] that hands HTTP-originated produce/fetch work to the broker through the same
 * [[RequestChannel]] the binary protocol uses. The result: authorization, quotas, replication and metrics are
 * inherited "for free" — they are evaluated by the existing [[kafka.server.KafkaApis]] code path against a synthetic
 * [[RequestChannel.Request]] that carries the HTTP caller's principal.
 *
 * The submitter is the inverse of [[kafka.network.SocketServer]] for a single request:
 *   1. Group the HTTP records by partition; build a [[ProduceRequest]] (or [[FetchRequest]]) targeting the same topic.
 *   2. Serialize through wire format so [[RequestContext.parseRequest]] sees byte-identical input to a binary request.
 *   3. Construct a [[RequestChannel.Request]] with `processor = -1` (no socket to write back to) and install a
 *      `requestCompletionCallback` that the patched [[RequestChannel.sendResponse]] hook invokes synchronously on the
 *      KafkaRequestHandler thread.
 *   4. Translate the resulting [[AbstractResponse]] into the formatter-friendly result types.
 *
 * The callback runs on a request-handler thread; we never block it. The future is completed in the callback and the
 * Jetty servlet thread reading it continues elsewhere.
 *
 * Records submitted without an explicit partition are routed to partition 0 — a deliberate, documented choice for v1.
 * No client-side partitioner runs; if a future revision wants key-hash or sticky partitioning, this is the seam.
 *
 * @param requestChannel    the broker's data-plane request channel
 * @param time              clock used for record timestamps and request start times
 * @param principal         the principal attributed to HTTP-originated requests (authorization is evaluated against it)
 * @param listenerName      the listener name attributed to HTTP-originated requests; must match a configured listener
 * @param topicIdLookup     resolves topic name → topic id; pass [[kafka.server.MetadataCache#getTopicId]] in production
 * @param clientIdSupplier  request-header client.id (default "http-bridge")
 * @param produceAcks       acks value carried in the ProduceRequest (default -1 = all)
 * @param produceTimeoutMs  request timeout the broker sees (default 30s)
 * @param fetchMaxWaitMs    long-poll deadline for the broker's fetch purgatory (default 500ms — short for v1 sync HTTP)
 * @param fetchMinBytes     fetch min-bytes (default 1; we want to return as soon as data is available)
 * @param fetchDefaultMaxBytes default per-partition max bytes when the client doesn't supply one (default 1 MiB)
 */
class KafkaApiRequestSubmitter(
  requestChannel: RequestChannel,
  time: Time,
  principal: KafkaPrincipal,
  listenerName: ListenerName,
  topicIdLookup: String => Uuid,
  clientIdSupplier: String = "http-bridge",
  produceAcks: Short = -1,
  produceTimeoutMs: Int = 30_000,
  fetchMaxWaitMs: Int = 500,
  fetchMinBytes: Int = 1,
  fetchDefaultMaxBytes: Int = 1 * 1024 * 1024,
  fetchResponseMaxBytes: Int = 50 * 1024 * 1024
) extends RequestSubmitter {

  private val correlationIds = new AtomicInteger(0)

  override def submitProduce(command: ProduceCommand): CompletableFuture[ProduceResult] = {
    val future = new CompletableFuture[ProduceResult]()
    try {
      val request = buildProduceRequest(command)
      enqueue(request, future, response =>
        translateProduce(response.asInstanceOf[ProduceResponse], command.topic()))
    } catch {
      case t: Throwable => future.completeExceptionally(t)
    }
    future
  }

  override def submitFetch(command: FetchCommand): CompletableFuture[FetchResult] = {
    val future = new CompletableFuture[FetchResult]()
    try {
      val request = buildFetchRequest(command)
      enqueue(request, future, response =>
        translateFetch(response.asInstanceOf[FetchResponse], command))
    } catch {
      case t: Throwable => future.completeExceptionally(t)
    }
    future
  }

  // ----- request building --------------------------------------------------------------------------------------------

  /** Group records by partition (records without an explicit partition land on partition 0) and build a ProduceRequest
   *  whose wire shape matches what a binary client would send. */
  private[http] def buildProduceRequest(command: ProduceCommand): ProduceRequest = {
    val grouped: Map[Int, Seq[ProduceRequestParser.RecordEntry]] = command.records().asScala
      .groupBy(entry => if (entry.partition().isPresent) entry.partition().getAsInt else 0)
      .map { case (p, rs) => (p, rs.toSeq) }
      .toMap

    val partitionData: util.List[PartitionProduceData] = grouped.toSeq.sortBy(_._1).map { case (partition, entries) =>
      val simpleRecords: Array[SimpleRecord] = entries.map(toSimpleRecord).toArray
      new PartitionProduceData()
        .setIndex(partition)
        .setRecords(MemoryRecords.withRecords(Compression.NONE, simpleRecords: _*))
    }.asJava

    val topicData = new TopicProduceData()
      .setName(command.topic())
      .setPartitionData(partitionData)

    val topicCollection = new TopicProduceDataCollection(util.List.of(topicData).iterator())

    val data = new ProduceRequestData()
      .setAcks(produceAcks)
      .setTimeoutMs(produceTimeoutMs)
      .setTopicData(topicCollection)

    ProduceRequest.builder(data).build()
  }

  private def toSimpleRecord(entry: ProduceRequestParser.RecordEntry): SimpleRecord = {
    val headers: Array[Header] =
      if (entry.contentType() != null && !entry.contentType().isEmpty)
        Array(new RecordHeader("content-type", entry.contentType().getBytes(StandardCharsets.UTF_8)))
      else
        Array.empty
    new SimpleRecord(time.milliseconds(), entry.key(), entry.value(), headers)
  }

  /** Construct a single-partition consumer FetchRequest pointing at the requested offset. */
  private[http] def buildFetchRequest(command: FetchCommand): FetchRequest = {
    val topicId = Option(topicIdLookup(command.topic())).getOrElse(Uuid.ZERO_UUID)
    val maxBytes = if (command.maxBytes().isPresent) command.maxBytes().getAsInt else fetchDefaultMaxBytes

    val partitionData = new FetchRequest.PartitionData(
      topicId,
      command.offset(),
      FetchRequest.INVALID_LOG_START_OFFSET,
      maxBytes,
      Optional.empty[Integer]()
    )

    val toFetch = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]()
    toFetch.put(new TopicPartition(command.topic(), command.partition()), partitionData)

    FetchRequest.Builder
      .forConsumer(ApiKeys.FETCH.latestVersion(), fetchMaxWaitMs, fetchMinBytes, toFetch)
      .setMaxBytes(fetchResponseMaxBytes)
      .build()
  }

  // ----- response translation ----------------------------------------------------------------------------------------

  private[http] def translateProduce(response: ProduceResponse, topic: String): ProduceResult = {
    val topicMatch = response.data().responses().asScala.find(_.name() == topic)
    val partitions = topicMatch match {
      case None =>
        // No topic-level entry — the broker either rejected the request before per-partition processing or returned
        // an empty response shape. Surface this as a single placeholder error so the formatter can still emit a
        // structured envelope.
        util.List.of[PartitionResult]()
      case Some(topicData) =>
        topicData.partitionResponses().asScala.map { p =>
          val error = Errors.forCode(p.errorCode())
          val offset = if (error == Errors.NONE) p.baseOffset() else -1L
          new PartitionResult(p.index(), offset, error, p.errorMessage())
        }.toList.asJava
    }
    new ProduceResult(partitions, response.data().throttleTimeMs().toLong)
  }

  private[http] def translateFetch(response: FetchResponse, command: FetchCommand): FetchResult = {
    val partitionDataOpt = response.data().responses().asScala.flatMap(_.partitions().asScala)
      .find(_.partitionIndex() == command.partition())

    val partitionFetch = partitionDataOpt match {
      case None =>
        new PartitionFetch(command.partition(), Errors.UNKNOWN_TOPIC_OR_PARTITION,
          "Broker returned no partition data for the requested partition",
          command.offset(), 0L, 0L, util.List.of[FetchedRecord]())
      case Some(p) =>
        val error = Errors.forCode(p.errorCode())
        if (error != Errors.NONE) {
          // FetchResponseData.PartitionData has no errorMessage field — use the canonical Errors message.
          new PartitionFetch(command.partition(), error, error.message(),
            command.offset(),
            Math.max(p.logStartOffset(), 0L),
            Math.max(p.highWatermark(), 0L),
            util.List.of[FetchedRecord]())
        } else {
          val records = extractRecords(p)
          new PartitionFetch(command.partition(), Errors.NONE, null,
            command.offset(),
            Math.max(p.logStartOffset(), 0L),
            Math.max(p.highWatermark(), 0L),
            records)
        }
    }
    new FetchResult(partitionFetch, response.data().throttleTimeMs().toLong)
  }

  private def extractRecords(partition: FetchResponseData.PartitionData): util.List[FetchedRecord] = {
    val out = new util.ArrayList[FetchedRecord]()
    val baseRecords = partition.records()
    // The broker returns FileRecords for on-disk fetches and MemoryRecords for in-memory ones; both implement Records,
    // which is where iteration lives. Casting to Records (not MemoryRecords!) is what makes a real fetch work — the
    // broker hands us FileRecords zero-copy from the segment file.
    baseRecords match {
      case null => out
      case r: Records =>
        r.records().forEach { record =>
          val offset = record.offset()
          val timestamp = record.timestamp()
          val key: Array[Byte] = if (record.hasKey) bytesOf(record.key()) else null
          val value: Array[Byte] = if (record.hasValue) bytesOf(record.value()) else Array.emptyByteArray
          val contentType: String = record.headers().toSeq.find(_.key() == "content-type") match {
            case Some(h) if h.value() != null => new String(h.value(), StandardCharsets.UTF_8)
            case _ => null
          }
          out.add(new FetchedRecord(offset, key, value, contentType, timestamp))
        }
        out
      case other =>
        throw new IllegalStateException(
          s"Unsupported records implementation in fetch response: ${other.getClass.getName}")
    }
  }

  private def bytesOf(buf: java.nio.ByteBuffer): Array[Byte] = {
    val copy = new Array[Byte](buf.remaining())
    buf.duplicate().get(copy)
    copy
  }

  // ----- channel plumbing --------------------------------------------------------------------------------------------

  private def enqueue[T](
    abstractRequest: org.apache.kafka.common.requests.AbstractRequest,
    future: CompletableFuture[T],
    translate: AbstractResponse => T
  ): Unit = {
    val correlationId = correlationIds.incrementAndGet()
    val header = new RequestHeader(abstractRequest.apiKey, abstractRequest.version, clientIdSupplier, correlationId)
    val buffer = abstractRequest.serializeWithHeader(header)
    val parsedHeader = RequestHeader.parse(buffer) // advances buffer past the header — Request ctor expects this state

    val context = new RequestContext(
      parsedHeader,
      KafkaApiRequestSubmitter.ConnectionId,
      InetAddress.getLoopbackAddress,
      Optional.empty(),
      principal,
      listenerName,
      SecurityProtocol.PLAINTEXT,
      ClientInformation.EMPTY,
      false,
      Optional.empty()
    )

    val request = new RequestChannel.Request(
      processor = KafkaApiRequestSubmitter.SyntheticProcessorId,
      context = context,
      startTimeNanos = time.nanoseconds(),
      memoryPool = MemoryPool.NONE,
      buffer = buffer,
      metrics = requestChannel.metrics,
      envelope = None
    )

    request.requestCompletionCallback = Some { response =>
      try future.complete(translate(response))
      catch { case t: Throwable => future.completeExceptionally(t) }
    }

    requestChannel.sendRequest(request)
  }
}

object KafkaApiRequestSubmitter {
  // Sentinel processor id for synthetic in-process requests. The dispatcher never looks this up because
  // requestCompletionCallback short-circuits the SendResponse path — but metrics tags still see the value.
  private val SyntheticProcessorId: Int = -1
  private val ConnectionId: String = "http-bridge"
}
