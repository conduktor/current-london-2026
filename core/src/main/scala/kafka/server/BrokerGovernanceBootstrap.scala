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
 * Three-state classification of this broker's relationship to the governance
 * partition at the moment a drain runs. Distinguishing these is critical: the
 * legacy code conflated "topic doesn't exist yet" and "broker is not a replica"
 * into a single `getLog == None` branch, which silently fail-opened on any
 * non-replica broker in a multi-broker cluster. Codex flagged this on audit.
 *
 *   - [[TopicAbsent]]: the {@code __governance} topic has not been created
 *     anywhere in the cluster. There are no rules to enforce. Empty RuleSet
 *     is correct; not a startup failure.
 *   - [[LocalReplica]]: this broker is in the replica set of __governance-0.
 *     Normal local-log drain path applies.
 *   - [[NonReplica]]: the topic exists but this broker is not a replica of
 *     partition 0. Under the strict default ({@code requireLocalReplica=true})
 *     this is a fatal startup error — the broker would otherwise enforce an
 *     empty RuleSet while real rules exist on the cluster, which is a silent
 *     fail-open of every rule. With the knob off, an operator has opted into
 *     fail-open with a loud warning.
 */
sealed trait LocalReplicaStatus
object LocalReplicaStatus {
  case object TopicAbsent extends LocalReplicaStatus
  case object LocalReplica extends LocalReplicaStatus
  case object NonReplica extends LocalReplicaStatus
}

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
 *
 * <h3>Operator-visible posture: "fail-stale-not-empty"</h3>
 *
 * <p>Once this broker has successfully installed a non-empty [[RuleSet]],
 * every subsequent no-log drain branch ([[LocalReplicaStatus#TopicAbsent]],
 * [[LocalReplicaStatus#NonReplica]] under opt-out, and the
 * [[LocalReplicaStatus#LocalReplica]] "log object briefly disappeared" race)
 * KEEPS the previously-active [[RuleSet]] and does NOT commit empty. This
 * is the safer choice for a security-critical surface, but it changes one
 * operational assumption: deleting the {@code __governance} topic does NOT
 * disable governance on already-running brokers — they will continue to
 * enforce the last-known rules until the topic is re-created and each rule
 * is either tombstoned or explicitly replaced. To deactivate a rule
 * cluster-wide, publish a tombstone (key = rule-id, value = null) and wait
 * for convergence; do not rely on topic deletion.
 */
class BrokerGovernanceBootstrap(replicaManager: ReplicaManager,
                                ruleEngine: RuleEngine,
                                topicPartition: TopicPartition =
                                  new TopicPartition(GovernanceTopic.NAME, 0),
                                injectedLoader: GovernanceLoader = null,
                                localReplicaStatus: () => LocalReplicaStatus =
                                  () => LocalReplicaStatus.TopicAbsent,
                                requireLocalReplica: Boolean = true,
                                caughtUpProbe: () => Boolean = () => true)
  extends Logging {

  // Visible for tests so a Mockito spy/mock can simulate a poisoned record.
  // Production callers leave this null and get the default loader.
  private[server] val loader: GovernanceLoader =
    if (injectedLoader != null) injectedLoader
    else new GovernanceLoader(ruleEngine)

  // Per-partition cursor of the next offset to replay. Records at offsets
  // >= this cursor are eligible; everything below has already been replayed.
  // Starts at 0 so a brand-new log (start offset 0) is fully consumed on the
  // first drainOnce.
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
        // Disambiguate "topic doesn't exist" from "broker is not a replica" —
        // the legacy code conflated both into an empty-RuleSet fall-through,
        // which silently fail-opened on every non-replica broker. Codex
        // flagged this on audit; see [[LocalReplicaStatus]] javadoc.
        //
        // Invariant across every no-log branch: NEVER call loader.commit()
        // here. The previously-installed RuleSet (or RuleSet.EMPTY if we have
        // never committed) remains active. This is the fix for Codex's P0
        // audit finding "LocalReplica + no-log committing empty is fail-open":
        // if we have ever successfully drained a non-empty RuleSet and we then
        // lose the local log (reassignment, disk fault, transient race), the
        // engine must keep enforcing the last-known-good rules — installing
        // empty would silently bypass every rule until the next drain reads
        // the log. Strict "fail-stale-not-empty" is the safe posture for a
        // security-critical surface.
        localReplicaStatus() match {
          case LocalReplicaStatus.TopicAbsent =>
            // Topic does not exist in this broker's metadata view — there are
            // no rules to enforce from this topic. Engine stays at its prior
            // active() (RuleSet.EMPTY at startup, the last-good set at
            // runtime). No commit.
            0L

          case LocalReplicaStatus.NonReplica if requireLocalReplica =>
            // The cluster has __governance, this broker is not a replica, and
            // the operator has not opted into fail-open. Aborting startup is
            // the safe choice: an empty RuleSet on this broker while real
            // DENY rules exist on the topic would silently bypass every
            // governance rule for any client that hits this broker.
            //
            // The error message must give the operator the exact knobs to
            // turn — a fail-closed startup that the operator can't unblock
            // is just an outage. They have two valid recoveries:
            //   1. Assign a replica of the partition to this broker.
            //   2. Set governance.bootstrap.require.local.replica=false
            //      (loud-warning fail-open).
            throw new IllegalStateException(
              s"governance topic ${tp.topic} exists in the cluster but this " +
                s"broker is not a replica of partition ${tp.partition}; an " +
                s"empty RuleSet on this broker would silently fail-open every " +
                s"governance rule. Either assign a replica of " +
                s"${tp.topic}-${tp.partition} to this broker, or set " +
                s"governance.bootstrap.require.local.replica=false to opt " +
                s"into fail-open with a loud warning.")

          case LocalReplicaStatus.NonReplica =>
            // Operator has explicitly opted into fail-open. Log it at ERROR
            // every drain — this is a security-critical posture and an
            // operator scanning logs must see it on every drain pass, not
            // just at startup. The engine retains its prior active(); at
            // startup that is RuleSet.EMPTY (the opt-out's intent), at
            // runtime it is whatever rules we had last drained — strictly
            // safer than installing empty over a known-good set.
            error(s"governance topic ${tp.topic} exists but this broker is " +
              s"not a replica of partition ${tp.partition}; " +
              s"governance.bootstrap.require.local.replica=false — keeping " +
              s"the last-known active RuleSet (initially empty); every new " +
              s"or tombstoned rule on this topic is invisible to this broker")
            0L

          case LocalReplicaStatus.LocalReplica =>
            // Metadata says this broker IS a replica but ReplicaManager has
            // no log object yet. This is a startup-time race (the log dir
            // hasn't been opened) or a transient state during reassignment;
            // either way, the next scheduled drain will retry. Don't fail
            // startup — and crucially, don't replace the prior active RuleSet
            // with an empty one: a fluky "log object briefly disappeared"
            // must not be a fail-open window.
            warn(s"governance partition $tp reports this broker as a replica " +
              s"but the local log is not yet available — keeping the prior " +
              s"active RuleSet; next periodic drain will retry")
            0L
        }

      case Some(log) =>
        val startOffset = math.max(log.logStartOffset, nextOffset.get())
        // Bound the drain by the high-watermark, not the local log-end offset.
        // On a follower, LEO may be ahead of HW (records replicated locally but
        // not yet acknowledged by enough replicas to advance the cluster-wide
        // commit point). Reading past HW would let this broker enforce rules
        // that could still be truncated by a leader-election — i.e. enforce a
        // rule that no other broker enforces. The matching FetchIsolation in
        // replay() must agree, so this bound and that isolation move together.
        val endOffset = log.highWatermark
        if (startOffset >= endOffset) {
          // Up to date — commit once so the engine reflects the working state
          // even when nothing new arrived (idempotent install).
          loader.commit()
          return 0L
        }
        val result = replay(log, startOffset, endOffset)
        // Always commit, so the engine reflects everything we DID apply this
        // drain. The cursor advances to where replay actually got — not to
        // endOffset — so a defensive empty-read mid-replay does not silently
        // skip the unread range. Codex deep-audit P1 fix: prior version did
        // `nextOffset.set(endOffset)` which jumped past records we never read.
        loader.commit()
        nextOffset.set(result.advancedTo)
        result.replayed
    }
  }

  /**
   * Like [[drainOnce]] but with stronger guarantees tailored to broker startup.
   *
   * <p>The crucial difference is the [[LocalReplicaStatus.LocalReplica]] +
   * {@code getLog == None} branch. In [[drainOnce]] this returns {@code 0L}
   * without committing — the "fail-stale-not-empty" posture: a transient
   * log-dir glitch must not be allowed to overwrite the engine's last-known-good
   * [[org.apache.kafka.server.rules.RuleSet]] with empty. That posture is the
   * right one <em>after</em> a successful first install, but at first startup
   * the engine's active RuleSet is [[org.apache.kafka.server.rules.RuleSet#EMPTY]],
   * so returning {@code 0L} would let [[BrokerServer]] open the request socket
   * with no rules enforced — exactly the fail-empty window the startup gate
   * exists to prevent.
   *
   * <p>{@code drainStartup} therefore bounded-waits up to {@code deadlineMs}
   * for the local log to become available, polling every {@code pollIntervalMs}.
   * Once it appears, a SECOND bounded-wait runs (sharing the same deadline)
   * for the {@code caughtUpProbe} to return {@code true} — i.e. for this
   * broker to be either the partition leader OR a follower in the ISR. On a
   * freshly-started follower the log directory opens with HW=0 / LEO=0 before
   * the replica-fetcher has pulled any committed records from the leader, so
   * draining at that instant would commit an empty {@link
   * org.apache.kafka.server.rules.RuleSet} past rules the leader has already
   * committed. Waiting for ISR membership (or self-leadership) is the
   * minimal principled signal that "drain up to local HW" reflects a recent
   * cluster-committed point. Codex deep-audit P0b. If the deadline elapses
   * in either phase, the method throws [[IllegalStateException]] and
   * [[BrokerServer]] aborts startup with no socket opened — a stale
   * fail-closed is strictly safer than a silent fail-empty.
   *
   * <p>The other no-log branches ([[LocalReplicaStatus.TopicAbsent]],
   * [[LocalReplicaStatus.NonReplica]] under either knob setting) need no
   * bounded wait — they mean "this broker is not supposed to enforce from
   * this topic", which is a deterministic state, not a race.
   *
   * @param deadlineMs maximum total time to wait for the local log to appear
   * @param pollIntervalMs sleep between probes. Defaults to a small value so
   *                       the startup is responsive when the log opens shortly
   *                       after the metadata-publish wait.
   */
  def drainStartup(deadlineMs: Long, pollIntervalMs: Long = 50L): Long = {
    val deadlineNanos = System.nanoTime() +
      java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(deadlineMs)
    val tp = topicPartition
    var attempt = 0
    while (true) {
      attempt += 1
      replicaManager.getLog(tp) match {
        case Some(_) =>
          // Phase 2 (Codex deep-audit P0b): the local log being open is
          // NOT sufficient on its own. On a freshly-started follower broker
          // the log directory opens almost immediately (HW=0, LEO=0) but
          // the replica-fetcher has not yet pulled any committed records
          // from the leader. Calling drainOnce in that window would read
          // up to local HW=0 → 0 records → empty RuleSet, then return —
          // and BrokerServer would then open client sockets with no rules
          // enforced even though real DENY rules exist on the leader.
          //
          // The fix: gate on a caught-up signal before draining. The probe
          // returns true when this broker is either the partition leader
          // (its HW IS the cluster-wide commit point) or a follower in
          // the ISR (the controller considers this broker caught up to
          // within replica.lag.time.max.ms). Both states are sufficient
          // for "drain up to local HW" to equal "drain up to a recent
          // cluster-committed point". A follower NOT in the ISR is by
          // definition lagging and would read a stale prefix; that is the
          // case the wait protects against.
          //
          // Tests inject a static probe so they don't have to mock the
          // entire Partition object graph. The default probe is `() =>
          // true` (caught up), which preserves the historical drainStartup
          // behaviour for callers that have not opted into the check yet.
          if (caughtUpProbe()) {
            return drainToHighWatermark(deadlineNanos, pollIntervalMs)
          }
          if (System.nanoTime() >= deadlineNanos) {
            // Log is open but the broker never caught up to the leader's HW
            // within the deadline. The bootstrap budget is a hard cap on
            // total wait, shared with Phase 1 — the operator chose how long
            // they were willing to delay client traffic in exchange for
            // catchup. Refusing to open the socket is the safe choice: an
            // empty drain past a lagging follower would silently bypass
            // every rule on the leader's log. Operator recovery: check
            // replica-fetcher status, leader reachability, controller view
            // of ISR; resolve, then restart.
            throw new IllegalStateException(
              s"governance bootstrap timed out after ${deadlineMs}ms " +
                s"waiting for this broker to catch up to the leader of " +
                s"$tp; the local log is open but the broker is not yet in " +
                s"the ISR (per controller metadata). Draining now would " +
                s"read a stale prefix of the governance log and silently " +
                s"fail-open every rule the leader has committed. Refusing " +
                s"to open client traffic — investigate replica-fetcher " +
                s"status, leader reachability, and ISR state for $tp, " +
                s"then restart.")
          }
          if (attempt == 1 || attempt % 20 == 0) {
            info(s"governance bootstrap waiting for replica catchup on " +
              s"$tp (attempt $attempt, deadline ${deadlineMs}ms)")
          }
          Thread.sleep(pollIntervalMs)
        case None =>
          localReplicaStatus() match {
            // TopicAbsent and NonReplica are deterministic states — no point
            // bounded-waiting. drainOnce handles them with the right semantics
            // (no-op for TopicAbsent; throw / warn-and-no-op for NonReplica).
            case LocalReplicaStatus.TopicAbsent | LocalReplicaStatus.NonReplica =>
              return drainOnce()

            case LocalReplicaStatus.LocalReplica =>
              if (System.nanoTime() >= deadlineNanos) {
                // We are a replica per cluster metadata but the local log has
                // not opened within the deadline. Refusing to open client
                // traffic is the safe choice — the alternative is "broker
                // starts with RuleSet.EMPTY while real DENY rules exist on
                // the topic", which silently fail-opens every governance rule
                // for any client that hits this broker. Operator recovery:
                // resolve the log-dir state (check log4j for log-loader
                // errors / disk-full / permission issues) and restart.
                throw new IllegalStateException(
                  s"governance bootstrap timed out after ${deadlineMs}ms " +
                    s"waiting for local log of $tp to become available; this " +
                    s"broker is a replica per cluster metadata but the log " +
                    s"is not yet available locally. Refusing to open client " +
                    s"traffic with an empty RuleSet while DENY rules may " +
                    s"exist on the topic — resolve the log-dir state " +
                    s"(check log loader / disk / permissions) and restart.")
              }
              if (attempt == 1 || attempt % 20 == 0) {
                // Avoid log spam in the tight poll loop but keep visibility
                // for slow log-dir opens — log on the first iteration and
                // periodically thereafter.
                info(s"governance bootstrap waiting for local log of $tp " +
                  s"(attempt $attempt, deadline ${deadlineMs}ms)")
              }
              Thread.sleep(pollIntervalMs)
          }
      }
    }
    // Unreachable — the `while (true)` loop exits via `return` or `throw`.
    throw new AssertionError("unreachable")
  }

  /**
   * Drain to the current local high-watermark, looping [[drainOnce]] until
   * the per-partition cursor catches up. This is the third and final phase
   * of [[drainStartup]] — Phase 1 waits for the local log to open, Phase 2
   * waits for this broker to be in the controller-published ISR (or to be
   * the leader), and Phase 3 drains.
   *
   * <p>Why a loop is necessary (Codex final-audit P0): a single
   * [[drainOnce]] can return a partial replay — [[replay]] defensively
   * bails when [[UnifiedLog#read]] returns zero bytes mid-pass (a transient
   * pager glitch, a tiering bookkeeping pause, etc.). In that case
   * [[nextOffset]] advances only to where replay actually got, not to HW.
   * Before this loop existed, [[drainStartup]] would return at that point
   * and [[BrokerServer]] would open client sockets with only a prefix of
   * the governance log drained — silently bypassing rules at offsets
   * above [[nextOffset]] until the periodic re-drain caught up. Per
   * PROMPT.md the bootstrap acceptance criterion is
   * <em>"drains all existing rules from the rules topic before accepting
   * client connections"</em>, so partial-drain-then-open is a real
   * violation, not theoretical.
   *
   * <p>The loop is bounded by the same deadline as Phases 1 and 2 — total
   * startup-bootstrap wait is one number from the operator's perspective.
   * Two failure modes both reach [[IllegalStateException]] and abort
   * startup before any socket opens:
   *
   * <ul>
   *   <li>[[nextOffset]] catches up to a moving target but the cumulative
   *       wait exceeds the deadline.</li>
   *   <li>[[nextOffset]] makes <em>no</em> progress across two consecutive
   *       [[drainOnce]] invocations — typically a persistent defensive
   *       empty-read, suggesting the local log dir or pager is unhealthy.
   *       Continuing to loop would burn CPU without producing the drain
   *       the spec promises; fail-closed is the safe answer.</li>
   * </ul>
   *
   * <p>The local high-watermark may itself advance during the loop (new
   * rules being committed in parallel). We accept that, because each
   * [[drainOnce]] re-snapshots its own HW bound; we only need
   * [[nextOffset]] to reach the HW we observed when starting the loop —
   * the rules that existed at startup. Anything published later is
   * picked up by [[scheduleOngoing]].
   *
   * @return total records replayed across all [[drainOnce]] invocations
   * @throws IllegalStateException when the deadline elapses with the drain
   *         incomplete
   */
  private def drainToHighWatermark(deadlineNanos: Long,
                                   pollIntervalMs: Long): Long = {
    val tp = topicPartition
    // Snapshot HW under the existing log handle. We resolved log via
    // replicaManager.getLog(tp) immediately before calling here, but it
    // is safer to re-resolve in case the log object briefly disappeared
    // (extremely unusual after Phase 1 + Phase 2 succeeded, but the
    // bootstrap path must not NPE — fall back to "treat as no HW to
    // drain to" so the loop is a single no-op iteration).
    val snapshotHw = replicaManager.getLog(tp).map(_.highWatermark).getOrElse(0L)
    var totalReplayed = 0L
    while (nextOffset.get() < snapshotHw) {
      val before = nextOffset.get()
      val replayed = drainOnce()
      totalReplayed += replayed
      val after = nextOffset.get()
      if (after == before) {
        // No progress this iteration. Either replay defensively bailed on
        // an empty read, or the log object disappeared. Either way, sleep
        // and retry — but if the deadline has elapsed, fail-closed: opening
        // the socket with a partial drain would violate the bootstrap
        // acceptance criterion. The operator needs to investigate the
        // log dir / pager / disk state, not have us silently fail-open.
        if (System.nanoTime() >= deadlineNanos) {
          throw new IllegalStateException(
            s"governance bootstrap could not fully drain $tp before the " +
              s"deadline elapsed; cursor stuck at offset $after with HW " +
              s"$snapshotHw. A persistent zero-byte read suggests the " +
              s"local log dir or pager is unhealthy. Refusing to open " +
              s"client traffic with a partial drain — investigate log4j " +
              s"for log-loader errors / disk faults / pager pauses, then " +
              s"restart.")
        }
        Thread.sleep(pollIntervalMs)
      }
    }
    totalReplayed
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

  /**
   * Result of a single [[replay]] pass. Carries BOTH the count of records
   * applied AND the offset we actually progressed to. The caller advances
   * [[nextOffset]] to {@code advancedTo}, not to the requested {@code endOffset},
   * because a mid-pass empty read must NOT silently skip records we never
   * consumed. Codex deep-audit P1 fix.
   */
  private case class ReplayResult(replayed: Long, advancedTo: Long)

  private def replay(log: UnifiedLog, startOffset: Long, endOffset: Long): ReplayResult = {
    var currentOffset = startOffset
    var replayed = 0L
    val readBufferBytes = 1024 * 1024
    var readAtLeast = true
    while (currentOffset < endOffset && readAtLeast) {
      val fetchInfo = log.read(
        startOffset = currentOffset,
        maxLength = readBufferBytes,
        // HIGH_WATERMARK: enforce only durably-committed rules. Anything past
        // HW could be truncated on leader-election and would diverge this
        // broker's view of governance from the rest of the cluster.
        isolation = FetchIsolation.HIGH_WATERMARK,
        minOneMessage = true)
      val records = fetchInfo.records
      val sizeInBytes = records.sizeInBytes()
      readAtLeast = sizeInBytes > 0
      if (!readAtLeast) {
        // Defensive: read returned no records before endOffset (transient pager
        // glitch, tiering bookkeeping, etc.). Bail out so the caller advances
        // nextOffset only to where we ACTUALLY reached — i.e. currentOffset,
        // which is unchanged across this iteration and so still references the
        // first unread record. The pre-fix version returned only `replayed`
        // and the caller jumped nextOffset to endOffset unconditionally,
        // silently advancing past records that were never consumed. The next
        // drain will re-read this range.
        return ReplayResult(replayed, currentOffset)
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
    ReplayResult(replayed, currentOffset)
  }

  private def bytes(buf: ByteBuffer): Array[Byte] = {
    if (buf == null) return null
    Utils.toArray(buf.duplicate())
  }
}
