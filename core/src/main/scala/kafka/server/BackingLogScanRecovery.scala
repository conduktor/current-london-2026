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

import kafka.log.{LogManager, UnifiedLog}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.storage.log.FetchIsolation
import org.apache.kafka.storage.internals.concentration.{ConcentrationKernel, LogicalPartition, LogicalPartitionMapper, LogicalRecoveryExtractor, RecoveryRecord}
import org.slf4j.LoggerFactory

import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Drives the PROMPT acceptance criterion "restart without index → full log scan to rebuild the
 * mapping; no data loss." The cheap path ({@link ConcentrationKernel#recoverFromDisk}) handles the
 * common restart-with-intact-sidecars case at BrokerServer startup. This class handles the
 * subset of declared (logicalTopic, logicalPartition) tuples whose sidecar file is missing or
 * was rejected as corrupt: it groups them by their owning backing TopicPartition, opens the
 * UnifiedLog for each, streams records page-by-page through the
 * {@link LogicalRecoveryExtractor}, and feeds the resulting {@link RecoveryRecord}s into
 * {@link ConcentrationKernel#recoverFromBackingScan}, which truncates and rebuilds those
 * sidecars + the in-memory tracker.
 *
 * <p>Run-once at broker startup, after {@link LogManager#startup} has run (the backing logs must
 * be open) but before the broker advertises itself as ready to serve. Idempotent: if every
 * declared logical partition has a sidecar on disk, this is a fast no-op.
 *
 * <p>Multiple backing partitions are scanned sequentially. The order is arbitrary because the
 * {@link LogicalPartitionMapper} is many-to-one but never overlaps across backing partitions:
 * every logical partition belongs to exactly one backing partition, so each call to
 * {@code recoverFromBackingScan} touches a disjoint set of sidecars from the others.
 *
 * <p>Page size defaults to 1 MiB. A backing partition that has never been produced to is treated
 * as "no records to recover" — the sidecar stays absent and the tracker stays at the default
 * {@code (0, 0)} for every logical partition the backing hosts.
 */
class BackingLogScanRecovery(
    kernel: ConcentrationKernel,
    logManager: LogManager,
    readBufferBytes: Int = 1 << 20) {

  private val log = LoggerFactory.getLogger(classOf[BackingLogScanRecovery])

  /**
   * Scan and rebuild sidecars for any declared logical partition that lacks one. Safe to call
   * even when no logical topics are declared (it short-circuits on an empty needs-set).
   *
   * @throws java.io.IOException if a backing-log read or sidecar rebuild fails
   * @throws IllegalStateException if a backing record exhibits a logical-offset gap
   *     (BackingScanRecoverer surfaces this as a hard failure rather than silent skip)
   */
  def run(): Unit = {
    val needed: util.List[LogicalPartition] = kernel.partitionsWithoutSidecar()
    if (needed.isEmpty) {
      log.debug("BackingLogScanRecovery: no logical partitions missing a sidecar — skipping")
      return
    }

    val grouped = groupByBacking(needed)
    log.info(s"BackingLogScanRecovery: scanning ${grouped.size} backing partition(s) to rebuild " +
      s"${needed.size} logical partition sidecar(s)")

    grouped.foreach { case (tp, filter) =>
      logManager.getLog(tp) match {
        case None =>
          // No backing log on this broker for the partition. Two legitimate cases:
          //   - The partition has never been produced to (no segments yet).
          //   - The broker is not a replica for this backing partition.
          // Either way, there's nothing to rebuild from; leave the tracker at (0, 0) and move on.
          log.info(s"BackingLogScanRecovery: no UnifiedLog for backing partition $tp; " +
            s"${filter.size} logical partitions stay at default (0, 0)")
        case Some(unifiedLog) =>
          recoverOneBackingPartition(tp, unifiedLog, filter)
      }
    }
  }

  private def groupByBacking(
      needed: util.List[LogicalPartition]
  ): Map[TopicPartition, util.Set[LogicalPartition]] = {
    val map = mutable.LinkedHashMap[TopicPartition, util.Set[LogicalPartition]]()
    needed.asScala.foreach { lp =>
      val descriptorOpt = kernel.describe(lp.logicalTopic())
      if (descriptorOpt.isPresent) {
        val descriptor = descriptorOpt.get()
        val backingPartition = LogicalPartitionMapper.backingPartitionFor(descriptor, lp.logicalPartition())
        val tp = new TopicPartition(descriptor.backingTopic(), backingPartition)
        val set = map.getOrElseUpdate(tp, new util.HashSet[LogicalPartition]())
        set.add(lp)
      } else {
        log.warn(s"BackingLogScanRecovery: logical partition $lp is missing a sidecar but its " +
          "logical topic is no longer declared — skipping")
      }
    }
    map.toMap
  }

  private def recoverOneBackingPartition(
      tp: TopicPartition,
      unifiedLog: UnifiedLog,
      filter: util.Set[LogicalPartition]): Unit = {
    val startOffset = unifiedLog.logStartOffset
    val endOffset = unifiedLog.logEndOffset
    if (startOffset >= endOffset) {
      log.info(s"BackingLogScanRecovery: backing partition $tp is empty " +
        s"(startOffset=$startOffset == endOffset=$endOffset); ${filter.size} sidecars stay absent")
      return
    }
    log.info(s"BackingLogScanRecovery: scanning $tp [$startOffset, $endOffset) for " +
      s"${filter.size} logical partition(s)")
    val iter = new BackingLogPageIterator(unifiedLog, filter, readBufferBytes, startOffset, endOffset)
    kernel.recoverFromBackingScan(iter)
  }
}

/**
 * Iterator that walks a single {@link UnifiedLog} page-by-page and yields one
 * {@link RecoveryRecord} per stamped backing record whose logical partition appears in
 * {@code filter}. Uses {@link FetchIsolation#LOG_END} — recovery must see all durable records on
 * this replica, not just those past the high watermark. The high watermark is not yet
 * trustworthy at recovery time anyway, because it gets recomputed once replication catches up.
 */
private class BackingLogPageIterator(
    unifiedLog: UnifiedLog,
    filter: util.Set[LogicalPartition],
    pageBytes: Int,
    private var nextOffset: Long,
    endOffset: Long) extends util.Iterator[RecoveryRecord] {

  private val buffer = new util.ArrayDeque[RecoveryRecord]()

  override def hasNext: Boolean = {
    refill()
    !buffer.isEmpty
  }

  override def next(): RecoveryRecord = {
    refill()
    if (buffer.isEmpty) throw new util.NoSuchElementException()
    buffer.poll()
  }

  private def refill(): Unit = {
    while (buffer.isEmpty && nextOffset < endOffset) {
      val fdi = unifiedLog.read(
        startOffset = nextOffset,
        maxLength = pageBytes,
        isolation = FetchIsolation.LOG_END,
        minOneMessage = true)
      val records = fdi.records
      // Walk batches to find the last offset we read so we can advance; do this BEFORE we exit
      // the loop so an "all-filtered-out" page still advances.
      val batchIt = records.batches().iterator()
      var lastOffsetOnPage = -1L
      while (batchIt.hasNext) {
        val b = batchIt.next()
        if (b.lastOffset() > lastOffsetOnPage) lastOffsetOnPage = b.lastOffset()
      }
      if (lastOffsetOnPage < 0) {
        // No records read at this offset. With minOneMessage=true this should not happen mid-log,
        // but to be safe, do not loop forever — bail out and let the recovery treat what we have.
        nextOffset = endOffset
        return
      }
      val extracted = LogicalRecoveryExtractor.extract(records, filter)
      extracted.forEach(buffer.add)
      nextOffset = lastOffsetOnPage + 1
      // If extracted was empty (records on the page belong to other logical partitions on the
      // same backing topic), loop again to read the next page. Otherwise hasNext will return
      // true on the buffered records.
    }
  }
}

object BackingLogScanRecovery {
  // Default page size: 1 MiB. Matches the load-buffer size used by other broker startup scans
  // (e.g., the group coordinator's CoordinatorLoaderImpl).
  val DEFAULT_READ_BUFFER_BYTES: Int = 1 << 20

  /**
   * Utility to close a kernel/logManager pair while preserving the original exception. Use this
   * when wiring the driver into broker startup so partial failures during recovery don't leak
   * resources — but exception handling at the broker level is the caller's responsibility.
   */
  def closeQuietly(closeable: AutoCloseable, name: String): Unit = {
    Utils.closeQuietly(closeable, name)
  }
}
