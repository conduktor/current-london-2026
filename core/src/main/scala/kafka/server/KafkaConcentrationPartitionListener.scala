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

import kafka.cluster.PartitionListener
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.storage.internals.concentration.ConcentrationKernel
import org.slf4j.LoggerFactory

/**
 * Bridges a backing-topic Partition's lifecycle events into the ConcentrationKernel's
 * per-backing in-memory caches.
 *
 * Why this exists: the kernel's idempotent-batch cache is populated only while THIS broker
 * is leader for the backing partition. On leader-loss (becoming follower, going offline,
 * being deleted) those entries are stale — a future producer request, if its sequence and
 * producer-id happened to collide, could short-circuit to a stale cached ACK before the
 * leader-epoch correctness floor in {@code lookupIdempotentBatch} catches it.
 *
 * The leader-epoch check in the kernel is the correctness floor; this listener is the
 * proactive invalidation that makes the floor's residual race window unreachable in
 * practice. It is therefore safe to omit (kernel still correct) but expected to be wired
 * for production: see PROMPT.md / Codex GAP 2.
 *
 * Scope of v1 (this listener): only the idempotent cache is invalidated. The offset
 * tracker is NOT dropped here, because dropping it without a coordinated rehydrate on
 * the next leader-acquisition would cause the new leader to re-assign logical offsets
 * from zero on a partition that already has produced data. Tracker drop + epoch-fenced
 * rehydrate is intentionally deferred to a follow-up commit (the listener will gain a
 * companion {@code onBecomingLeader}-equivalent path).
 *
 * Concurrency / idempotence: this listener is hashed/equated by (topicPartition, kernel),
 * so {@code Partition.maybeAddListener} (a CopyOnWriteArraySet add) is idempotent under
 * repeated {@code makeLeader} calls across leader-epoch bumps. Without this, every epoch
 * bump would leak an additional listener instance.
 */
final class KafkaConcentrationPartitionListener(
  private val backingTopicPartition: TopicPartition,
  private val kernel: ConcentrationKernel
) extends PartitionListener {

  override def onFailed(partition: TopicPartition): Unit =
    invalidate(partition, "onFailed")

  override def onDeleted(partition: TopicPartition): Unit =
    invalidate(partition, "onDeleted")

  override def onBecomingFollower(partition: TopicPartition): Unit =
    invalidate(partition, "onBecomingFollower")

  private def invalidate(partition: TopicPartition, reason: String): Unit = {
    try {
      kernel.invalidateIdempotentCacheForBacking(partition.topic)
    } catch {
      case t: Throwable =>
        // Listener callbacks are notification-only; swallowing here protects the
        // Partition's lock-holding caller from a kernel-side fault propagating into
        // replication state. Surface for observability.
        KafkaConcentrationPartitionListener.log.warn(
          s"Concentration kernel invalidation failed for backing ${partition.topic} on $reason: ${t.getMessage}", t)
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: KafkaConcentrationPartitionListener =>
      this.backingTopicPartition == that.backingTopicPartition && (this.kernel eq that.kernel)
    case _ => false
  }

  override def hashCode(): Int =
    31 * backingTopicPartition.hashCode + System.identityHashCode(kernel)
}

object KafkaConcentrationPartitionListener {
  private val log = LoggerFactory.getLogger(classOf[KafkaConcentrationPartitionListener])
}
