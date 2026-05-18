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
 * Scope of v1 (this listener): two effects on a leader-loss transition.
 *
 * 1. The kernel's idempotent-batch cache for this backing is invalidated — see Commit A
 *    rationale above.
 *
 * 2. The kernel's per-backing readiness gate is closed via
 *    {@code kernel.markBackingUnready(backingTopicPartition)}. After this point the
 *    produce hot path in {@code KafkaApis} sees the gate closed and rejects every
 *    logical-topic produce routed onto this backing with {@code NOT_LEADER_OR_FOLLOWER},
 *    which stock idempotent producers handle by refreshing metadata and retrying.
 *
 *    The tracker itself is deliberately NOT dropped here. Dropping it without a
 *    coordinated rehydrate on the next leader-acquisition would race in-flight
 *    {@code commitProduceBatch} callbacks (the reservation lock is held until the
 *    backing append's response callback fires, which can be many milliseconds after the
 *    listener runs). Instead, the upcoming {@code KafkaConcentrationLeaderRecoverer}
 *    (Commit B.3) will REPLACE the tracker state atomically from sidecars on leader
 *    re-acquisition, under an epoch fence so any state rebuilt against a stale epoch is
 *    discarded. The gate stays closed in the meantime, so the stale-in-memory tracker
 *    cannot be observed.
 *
 * Concurrency / idempotence: this listener is hashed/equated by {@code TopicPartition}
 * only, so {@code Partition.maybeAddListener} (a CopyOnWriteArraySet add) is idempotent
 * under repeated {@code makeLeader} calls across leader-epoch bumps. Without this, every
 * epoch bump would leak an additional listener instance. The broker has a single
 * concentration kernel per process; folding kernel identity into equality is unnecessary
 * (both Codex and Gemini called this out in the Commit A review).
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
    // Close the produce-gate FIRST, then evict the idempotent cache. The gate closure is
    // what prevents fresh produces from observing the now-untrusted tracker; doing it before
    // the cache eviction means a producer racing with the leader-loss event can at worst see
    // NOT_LEADER_OR_FOLLOWER from the gate (retriable) or a cache-miss-then-NLOF from a race
    // with the eviction — both correct. The reverse order would briefly leave the gate open
    // while the cache was already wiped, which is mostly equivalent but feels wrong.
    try {
      kernel.markBackingUnready(backingTopicPartition)
    } catch {
      case t: Throwable =>
        KafkaConcentrationPartitionListener.log.warn(
          s"Concentration kernel markBackingUnready failed for ${backingTopicPartition} on $reason: ${t.getMessage}", t)
    }
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
      this.backingTopicPartition == that.backingTopicPartition
    case _ => false
  }

  override def hashCode(): Int = backingTopicPartition.hashCode
}

object KafkaConcentrationPartitionListener {
  private val log = LoggerFactory.getLogger(classOf[KafkaConcentrationPartitionListener])
}
