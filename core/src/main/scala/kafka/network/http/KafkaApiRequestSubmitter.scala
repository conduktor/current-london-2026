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
import kafka.utils.Logging

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
) extends RequestSubmitter with Logging {

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
    val firstAttempt = submitFetchOnce(command)
    if (!command.fromEarliest) {
      return firstAttempt
    }
    // from=earliest is a hint, not an offset. The parser hands us offset=0 because the parser can't afford
    // a ListOffsets round-trip per HTTP request — but on a retained or compacted topic logStartOffset may
    // be far past 0, so the first fetch returns OFFSET_OUT_OF_RANGE. Retry once at the broker-reported
    // logStartOffset so the client sees the actual earliest retained record, not an error frame. The retry
    // submits with fromEarliest=false to bound recursion and to make any second OOR (e.g. logStartOffset
    // changed under us between attempts due to retention sweep) surface as a real error.
    firstAttempt.thenCompose { result =>
      if (result.partition().error() == Errors.OFFSET_OUT_OF_RANGE) {
        val retryOffset = result.partition().logStartOffset()
        val retryCommand = new FetchCommand(
          command.topic(), command.partition(), retryOffset, command.maxBytes(), false)
        submitFetchOnce(retryCommand)
      } else {
        CompletableFuture.completedFuture(result)
      }
    }
  }

  private def submitFetchOnce(command: FetchCommand): CompletableFuture[FetchResult] = {
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

    // Pin the request version to the latest STABLE version. ProduceRequest.builder(data) delegates to
    // ApiKeys.PRODUCE.latestVersion() (the no-arg form), which calls highestSupportedVersion(true) and so
    // enables any in-development version flagged latestVersionUnstable in ProduceRequest.json. Binary
    // clients never see those versions in ApiVersionsResponse (which is gated on the stable ceiling), so
    // letting HTTP-originated requests exercise them creates a canary risk: the bridge would silently
    // run the broker at protocol versions no shipped client speaks, masking divergence and amplifying
    // the blast radius of any in-development breakage. No-op at the current schema (Produce 3-12, no
    // unstable marker) but defensive against the next schema bump that adds one.
    new ProduceRequest.Builder(
      ApiKeys.PRODUCE.oldestVersion(),
      ApiKeys.PRODUCE.latestVersion(false),
      data
    ).build()
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

    // Pin to the latest STABLE Fetch version. The no-arg ApiKeys.FETCH.latestVersion() returns
    // highestSupportedVersion(true), which includes any in-development version flagged
    // latestVersionUnstable in FetchRequest.json. Binary consumers never negotiate such a version
    // through ApiVersionsResponse (stable-gated), so allowing HTTP-originated fetches to use one
    // would let the bridge exercise broker behaviour no shipped client speaks. Same canary-risk
    // mitigation as the Produce branch above; no-op today (Fetch 4-17 has no unstable marker).
    FetchRequest.Builder
      .forConsumer(ApiKeys.FETCH.latestVersion(false), fetchMaxWaitMs, fetchMinBytes, toFetch)
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
    val throttleTimeMs = response.data().throttleTimeMs()
    val partitionDataOpt = response.data().responses().asScala.flatMap(_.partitions().asScala)
      .find(_.partitionIndex() == command.partition())

    val partitionFetch = partitionDataOpt match {
      case None if throttleTimeMs > 0 =>
        // Consumer fetch quota throttle: KafkaApis returns an empty response with throttleTimeMs > 0 and no partition
        // data (see KafkaApis.handleFetchRequest → fetchContext.getThrottledResponse). The throttle is not a fetch
        // error; surface it as an empty successful page so the formatter emits 200 + Retry-After per PROMPT.md AC3
        // instead of fabricating UNKNOWN_TOPIC_OR_PARTITION (which collapses to 404 and drops Retry-After).
        //
        // logStartOffset and highWatermark: the broker did not include them. Reporting them as 0L would
        // tell the formatter "the partition starts at 0 and is empty up to 0", which a cursor-following
        // client would read as "snap back to 0 and replay from the beginning" — exactly the wrong move
        // under a transient throttle. The honest fallback is command.offset(): we know the broker did not
        // reject with OFFSET_OUT_OF_RANGE so logStartOffset <= command.offset(), and an HWM at the
        // requested offset means "no records past where you asked" which is what an empty page implies.
        // Cursors then refuse to advance, the Retry-After header tells the client to wait, and the next
        // poll resumes from the same offset.
        new PartitionFetch(command.partition(), Errors.NONE, null,
          command.offset(), command.offset(), command.offset(), util.List.of[FetchedRecord]())
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
    new FetchResult(partitionFetch, throttleTimeMs.toLong)
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
        // Iterate per-batch so we can skip transaction commit/abort markers (isControlBatch). Records.records()
        // would flatten ALL batches including control batches; emitting those to HTTP/SSE/WS consumers leaks
        // producer-internal coordination state (producer id, marker payload) as if it were a user record. Standard
        // Kafka consumers filter at the record-iterator level — the bridge must do the same.
        r.batches().forEach { batch =>
          if (!batch.isControlBatch) {
            batch.forEach { record =>
              val offset = record.offset()
              val timestamp = record.timestamp()
              val key: Array[Byte] = if (record.hasKey) bytesOf(record.key()) else null
              // Preserve null-value semantics for compacted-topic tombstones and HTTP-produced {"type":"NULL"} records.
              // Returning Array.emptyByteArray here would let ValueSerializer.encode emit {"type":"STRING","data":""} —
              // an incorrect downgrade that loses the tombstone signal. Match the key handling on the line above.
              val value: Array[Byte] = if (record.hasValue) bytesOf(record.value()) else null
              val contentType: String = record.headers().toSeq.find(_.key() == "content-type") match {
                case Some(h) if h.value() != null => new String(h.value(), StandardCharsets.UTF_8)
                case _ => null
              }
              out.add(new FetchedRecord(offset, key, value, contentType, timestamp))
            }
          }
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
      // clientAddress is deliberately the loopback address, NOT the HTTP caller's real peer IP. The bridge's
      // v1 security model (BrokerServer.scala "WARNING — security posture") attributes every HTTP-originated
      // request to a single synthetic identity — ANONYMOUS principal at the bridge's local boundary —
      // because there is no per-request authentication. The trust boundary is "host can reach
      // http.bridge.port", not "the real peer IP at the broker authorizer". Surfacing the real remote IP
      // here would create the illusion of per-request distinguishability (e.g. let host-based ACLs
      // discriminate between bridge callers) that the v1 operator guidance explicitly tells operators not
      // to rely on; the documented ACL posture is topic-scoped, not host-scoped. The synthetic loopback
      // address is also what the broker logs as the request peer — that's accurate to the v1 design:
      // the bridge IS the peer, every caller is the same logical principal at the same physical interface.
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
      try {
        val result = translate(response)
        if (!future.complete(result)) {
          // Future was already settled (almost always: the bridge's orTimeout fired before the broker
          // responded). The translated result is dropped, which is correct — the client has already seen
          // the 504. But operators staring at a long-tail latency dashboard need a breadcrumb that the
          // broker reply DID land, just late, so they can distinguish "request timed out because the
          // broker is wedged" from "request timed out because the broker replied 200ms after our cap".
          // Debug-level: this happens on every legitimate timeout and would spam at info or higher.
          debug(s"Submitter response landed after the future was already completed (likely orTimeout fired first); " +
            s"dropping translated result of type ${result.getClass.getSimpleName}")
        }
      } catch {
        case t: Throwable =>
          if (!future.completeExceptionally(t)) {
            // Same race as above but for the translation-failure path: the translate() body threw, the
            // future was already completed, and the throwable would otherwise vanish. Translation
            // exceptions are not user-visible (the client got a 504 from the timeout), but a real
            // translation bug looks identical to "broker replied late" without this log line. Warn-level
            // here — translation exceptions are not expected on the happy path, so volume is bounded
            // by actual defects rather than legitimate timeouts.
            warn(s"Submitter translation failed after the future was already completed; original throwable below", t)
          }
      }
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
