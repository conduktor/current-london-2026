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

import kafka.log.UnifiedLog
import kafka.utils.Logging

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.rules.{GovernanceLoader, GovernanceTopic, RuleEngine}
import org.apache.kafka.server.storage.log.FetchIsolation
import org.apache.kafka.server.util.KafkaScheduler

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong

/**
 * Bootstraps and maintains the broker's [[RuleEngine]] from the
 * [[org.apache.kafka.server.rules.GovernanceTopic]] log.
 *
 * <p>The bootstrap problem: a single broker cannot satisfy
 * <em>"drain all rules from the rules topic before accepting client connections"</em>
 * using a [[org.apache.kafka.clients.consumer.KafkaConsumer]] — the consumer
 * needs the SocketServer accepting requests in order to fetch from this very
 * broker, so a pre-socket drain would deadlock.
 *
 * <p>The solution, mirroring how the group coordinator loads `__consumer_offsets`,
 * is to read the local log file directly through [[ReplicaManager.getLog]]. This
 * bypasses the network completely. The drain is performed in the broker
 * startup thread before [[kafka.network.SocketServer.enableRequestProcessing]],
 * so the request path sees a fully-populated [[RuleEngine]] from the very first
 * client byte.
 *
 * <p>If the local log does not exist yet (first-startup of a fresh cluster,
 * or this broker is not assigned a replica of `__governance`), the drain is
 * a no-op — there are no existing rules to enforce, so an empty
 * [[org.apache.kafka.server.rules.RuleSet]] is correct. The periodic re-drain
 * scheduled after startup will pick up rules once the log becomes available
 * locally.
 *
 * <p>Ongoing updates use [[KafkaScheduler]] to call [[drainOnce]] periodically.
 * That keeps the implementation self-contained: no PartitionListener wiring,
 * no consumer threads, no extra network traffic. The trade-off is sub-second
 * latency for rule propagation, which is acceptable for governance state
 * that changes on human timescales.
 *
 * <p>This is the broker's own consumer of the rules topic, so the work is
 * already unconditionally exempt from rule evaluation: no
 * [[RuleEngine#evaluate]] calls are made anywhere in this class. The
 * [[RuleEngine#INTERNAL_CLIENT_ID_PREFIX]] bypass is for the
 * [[org.apache.kafka.clients.consumer.Consumer]]-driven path
 * ([[org.apache.kafka.server.rules.GovernanceTopicReader]]); the direct-log
 * path used here never enters [[RuleEngine#evaluate]] at all.
 */
class BrokerGovernanceBootstrap(replicaManager: ReplicaManager,
                                ruleEngine: RuleEngine,
                                topicPartition: TopicPartition =
                                  new TopicPartition(GovernanceTopic.NAME, 0),
                                injectedLoader: GovernanceLoader = null)
  extends Logging {

  // Visible for tests so a Mockito spy/mock can simulate a poisoned record.
  // Production callers leave this null and get the default loader.
  private[server] val loader: GovernanceLoader =
    if (injectedLoader != null) injectedLoader
    else new GovernanceLoader(ruleEngine)

  // Per-partition cursor of the last offset we already replayed. Records at
  // offsets <= this cursor are skipped. Starts at -1 so a brand-new log
  // (start offset 0) is fully consumed on the first drainOnce.
  private val nextOffset = new AtomicLong(0L)

  /**
   * Read every record from `nextOffset` to the current log-end offset, apply
   * each to the [[GovernanceLoader]], and atomically install the resulting
   * [[org.apache.kafka.server.rules.RuleSet]] into the [[RuleEngine]].
   *
   * <p>If the local log does not exist (this broker is not a replica, or the
   * topic has not been created yet), this method does a single empty commit so
   * that [[RuleEngine#active]] is well-defined and returns immediately.
   *
   * <p>This method is safe to call repeatedly; each call advances `nextOffset`
   * past records it has already replayed. Tombstones in the log are passed
   * through to the loader, which removes the corresponding rule from the
   * working set.
   */
  def drainOnce(): Long = {
    val tp = topicPartition
    replicaManager.getLog(tp) match {
      case None =>
        // Log does not exist locally — no rules to drain.  Install an empty
        // (or unchanged) snapshot so the engine has a well-defined state.
        loader.commit()
        0L

      case Some(log) =>
        val startOffset = math.max(log.logStartOffset, nextOffset.get())
        val endOffset = log.logEndOffset
        if (startOffset >= endOffset) {
          // Up to date — commit once so the engine reflects the working state
          // even when nothing new arrived (idempotent install).
          loader.commit()
          return 0L
        }
        val replayed = replay(log, startOffset, endOffset)
        loader.commit()
        nextOffset.set(endOffset)
        replayed
    }
  }

  /**
   * Schedule [[drainOnce]] on `scheduler` every `intervalMs` milliseconds.
   * Returns a handle that the broker calls on shutdown to stop the task.
   */
  def scheduleOngoing(scheduler: KafkaScheduler,
                      intervalMs: Long): Unit = {
    val task: Runnable = () => {
      try {
        drainOnce()
      } catch {
        case t: Throwable =>
          warn(s"governance rules drain failed: ${t.getMessage}")
      }
    }
    scheduler.schedule("governance-rules-drain", task, intervalMs, intervalMs)
  }

  private def replay(log: UnifiedLog, startOffset: Long, endOffset: Long): Long = {
    var currentOffset = startOffset
    var replayed = 0L
    val readBufferBytes = 1024 * 1024
    var readAtLeast = true
    while (currentOffset < endOffset && readAtLeast) {
      val fetchInfo = log.read(
        startOffset = currentOffset,
        maxLength = readBufferBytes,
        isolation = FetchIsolation.LOG_END,
        minOneMessage = true)
      val records = fetchInfo.records
      val sizeInBytes = records.sizeInBytes()
      readAtLeast = sizeInBytes > 0
      if (!readAtLeast) {
        // Defensive: read returned no records before endOffset. Bail out to
        // avoid a tight spin; next drainOnce() will retry.
        return replayed
      }
      // The records may be FileRecords or MemoryRecords. We don't care which —
      // org.apache.kafka.common.record.Records exposes batches() for both.
      val it = records.batches().iterator()
      while (it.hasNext) {
        val batch = it.next()
        // Skip control batches (transaction markers); the governance topic
        // is not transactional but the code path must be tolerant of them.
        if (!batch.isControlBatch) {
          val recIt = batch.iterator()
          while (recIt.hasNext) {
            val rec = recIt.next()
            // Per-record fault isolation: a single corrupt record must not halt
            // the drain. If loader.apply (or key/value extraction) throws, log
            // the failure and advance past the record — otherwise nextOffset
            // never moves past the bad record and the scheduler busy-loops on
            // the same poison forever. The next drain picks up records after
            // it. The previously-installed RuleSet is unaffected: GovernanceLoader
            // only commits on the outer drainOnce, not per-record.
            try {
              val key = bytes(rec.key())
              val value = bytes(rec.value())
              val keyStr =
                if (key == null) null
                else new String(key, StandardCharsets.UTF_8)
              loader.apply(keyStr, value)
            } catch {
              case t: Throwable =>
                warn(s"skipping poisoned __governance record at offset " +
                  s"${rec.offset()}: ${t.toString}")
            }
            replayed += 1
          }
        }
        currentOffset = batch.nextOffset()
      }
    }
    replayed
  }

  private def bytes(buf: ByteBuffer): Array[Byte] = {
    if (buf == null) return null
    Utils.toArray(buf.duplicate())
  }
}
