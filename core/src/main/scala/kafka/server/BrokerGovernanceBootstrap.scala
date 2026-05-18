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
import org.apache.kafka.server.rules.{GovernanceLoader, GovernanceTopic, LogSafe, RuleEngine}
import org.apache.kafka.server.storage.log.FetchIsolation
import org.apache.kafka.server.util.KafkaScheduler

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

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
                                caughtUpProbe: () => Boolean = () => true,
                                partitionCountProbe: () => Int = () => 1)
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

  // Set to true when the truncation guard clears the loader's working state
  // (cursor was past HW → reset() + rewind). Persists across drainOnce calls
  // until a drain successfully replays at least one record and commits a
  // fresh RuleSet — then it is cleared.
  //
  // While this flag is set, the "up-to-date" branch of drainOnce MUST NOT
  // call loader.commit() — doing so would publish the just-reset (empty)
  // working state over the engine's previously-good active(), violating the
  // "fail-stale-not-empty" posture established in this file's javadoc. The
  // first drain that observes records on this partition exits the up-to-date
  // branch via the normal replay+commit path, which clears the flag.
  //
  // Cross-thread publication: drainStartup runs on the broker main thread
  // (before scheduleOngoing is called), and subsequent drainOnce calls run on
  // KafkaScheduler — a ScheduledThreadPoolExecutor backed by
  // config.backgroundThreads workers. ScheduledExecutorService guarantees that
  // successive executions of a single periodic task never overlap, but they
  // may land on different worker threads. The JMM happens-before chain
  // through the executor's work queue technically covers the field, but
  // @volatile makes that intent explicit and survives any future refactor
  // that calls drainOnce from a different thread (e.g. an admin path or a
  // test harness). One keystroke, no measurable cost on a path that runs at
  // most every few hundred ms. Codex adversarial-audit HIGH-1.
  @volatile private var holdingStalePostTruncation: Boolean = false

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
        // Bound the drain by the high-watermark, not the local log-end offset.
        // On a follower, LEO may be ahead of HW (records replicated locally but
        // not yet acknowledged by enough replicas to advance the cluster-wide
        // commit point). Reading past HW would let this broker enforce rules
        // that could still be truncated by a leader-election — i.e. enforce a
        // rule that no other broker enforces. The matching FetchIsolation in
        // replay() must agree, so this bound and that isolation move together.
        val endOffset = log.highWatermark
        // Truncation guard: if our cursor is ahead of the current HW, the
        // log has been truncated below records we already applied. That
        // happens on a leader-election with epoch divergence — a follower
        // truncates back to the new leader's offset, removing records this
        // bootstrap had already replayed. Without a reset, the loader's
        // working set retains zombie rules that no longer exist on the
        // topic and re-installs them on every commit. Reset the loader
        // and rewind the cursor so the next iteration re-reads the log
        // from the current log-start offset. The engine's currently
        // installed RuleSet stays in place until the re-drain commits,
        // honouring the "fail-stale-not-empty" posture.
        if (nextOffset.get() > endOffset) {
          warn(s"governance partition $tp truncated: cursor was at " +
            s"${nextOffset.get()} but HW is now $endOffset. Resetting " +
            s"loader and re-draining from log-start offset " +
            s"${log.logStartOffset}.")
          loader.reset()
          nextOffset.set(log.logStartOffset)
          holdingStalePostTruncation = true
        }
        val startOffset = math.max(log.logStartOffset, nextOffset.get())
        if (startOffset >= endOffset) {
          // The replay range is empty. Two paths into this branch:
          //
          //  1. Steady state: cursor has caught up to HW. The working set
          //     already reflects every record we've ever applied; an
          //     idempotent commit just publishes it (no-op if nothing
          //     changed since the previous commit).
          //
          //  2. Holding-stale-post-truncation: a prior drain (this one OR
          //     an earlier one) tripped the truncation guard and reset the
          //     loader's working state, but no drain has yet observed a
          //     record to rebuild it. holdingStalePostTruncation is set.
          //     Committing the still-empty working state would publish
          //     RuleSet.EMPTY over the engine's previously-good active(),
          //     violating the "fail-stale-not-empty" posture. Defer the
          //     commit until a drain that actually replays records hits
          //     the path below and clears the flag.
          //
          // Note: a brief adversarial-audit finding pointed out that
          // making the flag local to drainOnce protected only the first
          // post-truncation drain — subsequent drains while HW stayed at
          // logStartOffset would re-enter this branch with the flag
          // re-defaulted to false and unconditionally commit the still-
          // empty working state. Promoting the flag to a field fixes that
          // cross-drain regression; the test
          // drainOnceTruncationToEmptyHoldsStalePersistentlyAcrossDrains
          // pins the multi-drain case.
          if (holdingStalePostTruncation) {
            warn(s"governance partition $tp held-stale: replay range " +
              s"[$startOffset, $endOffset) is empty post-truncation; " +
              s"deferring commit so the engine keeps its last-known-good " +
              s"active RuleSet. Next drain that observes records on this " +
              s"partition will rebuild and install fresh state.")
            return 0L
          }
          loader.commit()
          return 0L
        }
        val result = replay(log, startOffset, endOffset)
        // Always advance the cursor to where replay actually got — not to
        // endOffset — so a defensive empty-read mid-replay does not silently
        // skip the unread range. Codex deep-audit P1 fix: prior version did
        // `nextOffset.set(endOffset)` which jumped past records we never read.
        nextOffset.set(result.advancedTo)
        // Audit B1: a drain that READ records but APPLIED none (every record
        // was rejected by the loader — null key, malformed envelope, cap hit)
        // is NOT forward progress through the topic. Committing the still-
        // empty working state during the held-stale window would publish
        // RuleSet.EMPTY over the engine's previously-good active(), exactly
        // the fail-stale-to-empty regression the held-stale flag exists to
        // prevent. Defer the commit; the next drain that successfully applies
        // at least one record will rebuild and install fresh state. The
        // cursor still advances so we don't busy-loop on the same poison.
        //
        // Round-14 audit BLOCKER C-1 extension: a drain that READ records
        // AND APPLIED them, BUT left the working set empty, is the
        // tombstone-only-post-truncation shape. GovernanceLoader.apply()
        // returns true for every tombstone (idempotent on absent ids), so
        // the original applied>0 gate fires even when the entire batch was
        // tombstones for ids that the reset() already cleared. Without this
        // extension, the commit installs RuleSet.EMPTY over a previously-good
        // active() — exactly the regression the held-stale flag exists to
        // prevent. Pairing applied>0 with !workingIsEmpty() closes it: a
        // tombstone-only batch advances the cursor, leaves the flag set,
        // and defers the commit. Operator recovery is documented on
        // GovernanceLoader.workingIsEmpty.
        if (holdingStalePostTruncation && (result.applied == 0L || loader.workingIsEmpty())) {
          val reason =
            if (result.applied == 0L)
              s"drain read ${result.read} record(s) but the loader applied none — every " +
                s"record was rejected (null key, malformed envelope, cap hit)"
            else
              s"drain applied ${result.applied} of ${result.read} record(s) but the working set " +
                s"is empty (tombstone-only batch post-reset)"
          warn(s"governance partition $tp held-stale: $reason. " +
            s"Deferring commit so the engine keeps its last-known-good " +
            s"active RuleSet. Next drain that lands a non-empty working " +
            s"state will rebuild and install fresh state. To recover when " +
            s"every rule is intentionally deleted, publish any valid update " +
            s"to transition the working set off-empty.")
          return result.read
        }
        loader.commit()
        // A successful replay+commit means the working state is now a fresh,
        // record-derived RuleSet — the held-stale flag (if any) is cleared
        // here, NOT inside the up-to-date branch. Clearing it elsewhere would
        // be wrong: a drain that only does an idempotent commit on a stable
        // working state has not rebuilt anything from the topic, so the
        // "post-truncation, no records yet observed" condition is unchanged.
        // Codex audit follow-on, paired with the field-promotion fix above.
        // Round-14 BLOCKER C-1: AND with !workingIsEmpty() so the gate above
        // and the gate below remain in perfect symmetry — never clear the
        // flag in a state where commit would have been deferred.
        if (holdingStalePostTruncation && result.applied > 0L && !loader.workingIsEmpty()) {
          holdingStalePostTruncation = false
        }
        result.read
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
   * The deadline is checked at the top of every iteration (Codex round-2
   * audit finding: previously only zero-progress iterations checked it, so
   * a [[drainOnce]] making slow forward progress could run past the
   * deadline indefinitely). Three failure modes all reach
   * [[IllegalStateException]] and abort startup before any socket opens:
   *
   * <ul>
   *   <li>The cumulative drain wait exceeds the deadline, whether the
   *       cursor is moving forward or stuck.</li>
   *   <li>[[nextOffset]] makes <em>no</em> progress across consecutive
   *       [[drainOnce]] invocations — typically a persistent defensive
   *       empty-read, suggesting the local log dir or pager is unhealthy.
   *       Continuing to loop would burn CPU without producing the drain
   *       the spec promises; we sleep and retry, but the deadline above
   *       eventually trips us closed.</li>
   *   <li>The local log object disappears while the loop is running. The
   *       prior implementation treated a missing log as "no HW to drain
   *       to" and silently returned 0 replayed — fail-open. The bootstrap
   *       acceptance criterion is "all existing rules drained before
   *       sockets open"; a vanished log means we cannot even compute the
   *       HW we need to drain to, so the only safe answer is to refuse
   *       to proceed. Codex round-2 audit finding.</li>
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
   *         incomplete, or when the local log object disappears
   */
  private def drainToHighWatermark(deadlineNanos: Long,
                                   pollIntervalMs: Long): Long = {
    val tp = topicPartition
    // Snapshot HW under the existing log handle. Phase 1 + Phase 2 already
    // proved the log was open and this broker was in the replica set; the
    // log disappearing here means a catastrophic local event (log dir
    // unmount, dir-failure handler hitting it concurrently). The only
    // safe answer is fail-closed — we cannot drain to an HW we do not
    // know, and opening sockets without draining violates the bootstrap
    // acceptance criterion. Codex round-2 audit P2 finding.
    val snapshotHw = replicaManager.getLog(tp).map(_.highWatermark).getOrElse {
      throw new IllegalStateException(
        s"governance bootstrap: local log for $tp disappeared between Phase 2 " +
          s"and Phase 3 — cannot determine high-watermark to drain to. Refusing " +
          s"to open client traffic with an undrained rules log. Investigate " +
          s"log-dir health / log-dir failure handlers and restart.")
    }
    var totalReplayed = 0L
    while (nextOffset.get() < snapshotHw) {
      // Deadline gate on every iteration, not only zero-progress ones. A
      // drainOnce that advances slowly (eg. 1 record per call against a
      // pager-thrashed disk) must NOT bypass the deadline by virtue of
      // making forward progress — startup-bootstrap latency is a single
      // operator-visible number and we honour it strictly.
      if (System.nanoTime() >= deadlineNanos) {
        throw new IllegalStateException(
          s"governance bootstrap could not fully drain $tp before the " +
            s"deadline elapsed; cursor at offset ${nextOffset.get()} with HW " +
            s"$snapshotHw (replayed $totalReplayed records so far). Refusing " +
            s"to open client traffic with a partial drain — increase the " +
            s"bootstrap-deadline knob if the log is healthy but large, or " +
            s"investigate disk / pager performance if drain is stalling.")
      }
      val before = nextOffset.get()
      val replayed = drainOnce()
      totalReplayed += replayed
      val after = nextOffset.get()
      if (after == before) {
        // No progress this iteration. Either replay defensively bailed on
        // an empty read, or the log object briefly hiccuped. Sleep and
        // retry — the next iteration's deadline check is what eventually
        // fails us closed if the situation persists.
        Thread.sleep(pollIntervalMs)
      }
    }
    totalReplayed
  }

  /**
   * Schedule [[drainOnce]] on `scheduler` every `intervalMs` milliseconds.
   * Returns a handle that the broker calls on shutdown to stop the task.
   *
   * <p>If [[drainOnce]] throws on every tick (the canonical example is a
   * broker that has been reassigned away from {@code __governance-0} mid-
   * runtime under {@code requireLocalReplica=true}, where every drain
   * throws an [[IllegalStateException]]), naive logging would burn one
   * WARN per scheduler tick — at the default interval that is one WARN
   * every few seconds, indefinitely. The de-dup ledger below collapses a
   * repeating identical message: the first occurrence WARNs immediately,
   * subsequent identical occurrences are counted silently and rolled up
   * into a single WARN every [[FailureWarnIntervalMs]]. A new distinct
   * message resets the ledger and WARNs immediately again — so an
   * operator scanning logs always sees the transition to a NEW failure
   * mode promptly, and a steady-state recurring failure never floods.
   */
  def scheduleOngoing(scheduler: KafkaScheduler,
                      intervalMs: Long): Unit = {
    val task: Runnable = () => {
      try {
        // Round-14 HIGH H-1 (compaction sub-agent): peek at the local log's
        // resolved LogConfig and emit a throttled audit WARN if cleanup.policy
        // has drifted away from the operator contract {compact}. The startup
        // gate in BrokerServer.requireGovernanceTopicCompactPolicy closes the
        // fresh-boot vector, but a runtime AlterConfigs from {compact} to
        // {delete} or {compact,delete} on a live broker would otherwise be
        // silent: drains keep returning records, the engine keeps installing
        // them, and once retention.ms elapses on the now-deletable segments
        // the rules silently age out — the same fail-OPEN-after-restart the
        // startup gate is designed to close, just on a slower timer. A WARN
        // surfaced on every drain tick (deduped by maybeWarnSuppressed) gives
        // an operator scanning logs a positive signal during the
        // retention-window grace period to revert the config change before
        // the rule set begins eroding.
        maybeWarnIfCleanupPolicyDrifted()
        // Round-15 BLOCKER-2 (recent-changes sub-agent): catch runtime
        // AlterPartitions that grows __governance past one partition, which
        // would otherwise silently drop every rule that hashes to a non-0
        // partition. Like the cleanup-policy drift check this is non-fatal —
        // a throttled WARN gives the operator a positive signal.
        maybeWarnIfPartitionCountDrifted()
        drainOnce()
      } catch {
        case t: Throwable =>
          // Round-11 audit (audit-forgery sub-agent, HIGH): include the
          // exception class in the dedup key, not just the message text.
          // Two distinct bugs whose getMessage happens to collide (e.g.
          // an IOException and an IllegalStateException that both report
          // "governance partition not present" via different code paths)
          // would otherwise be conflated — the operator would see a
          // single "repeated N times" rollup instead of separate WARN
          // bursts for each fault. Class-qualified key ensures distinct
          // failure modes surface distinctly.
          maybeWarnSuppressed(s"${t.getClass.getName}: ${t.getMessage}")
      }
    }
    scheduler.schedule("governance-rules-drain", task, intervalMs, intervalMs)
  }

  /**
   * Round-14 HIGH H-1 (compaction sub-agent): runtime drift detector for the
   * {@code __governance} cleanup policy.
   *
   * <p>The startup gate {@link BrokerServer#requireGovernanceTopicCompactPolicy}
   * runs once and aborts startup if the effective policy is not exactly
   * {@code compact}. After startup an operator AlterConfigs to
   * {@code delete} or {@code compact,delete} would otherwise slip through
   * silently — the broker keeps draining records, the engine keeps installing
   * them, and once {@code retention.ms} elapses on segments now deletable by
   * the retention path, the rules age out one segment at a time and the
   * broker fail-OPENs on the affected DENY rules. The hazard window is the
   * smaller of the retention.ms and the broker uptime, both typically on the
   * order of days — long enough that no human is watching.
   *
   * <p>This method peeks at {@link UnifiedLog#config} (the resolved LogConfig
   * including topic-level overrides) and routes a WARN through
   * {@link #maybeWarnSuppressed} whenever the effective policy is not
   * exactly {@code {compact}}. The throttled WARN dedupes identical drift
   * messages within {@link #FailureWarnIntervalMs} while still firing a fresh
   * line if the operator transitions from one bad policy to a different bad
   * policy (the dedup key is the message string). Visible for tests as a
   * private method invoked from the scheduleOngoing task.
   *
   * <p>The check is intentionally non-fatal: it does not stop the drain, does
   * not throw, and does not modify the engine. The point is to give an
   * operator a loud, persistent audit signal during the retention-window
   * grace period before the rules begin to actually disappear. A fatal
   * reaction on the live request path would amplify a config-typo into an
   * outage; a loud audit signal is the right cost-benefit trade.
   */
  private[server] def maybeWarnIfCleanupPolicyDrifted(): Unit = {
    replicaManager.getLog(topicPartition).foreach { log =>
      val cfg = log.config
      // Exact-match {compact}: compaction enabled AND delete disabled. The
      // mixed {compact,delete} policy sets both booleans, which is exactly
      // the silent-erosion shape this check exists to surface.
      if (!cfg.compact || cfg.delete) {
        val effective = (cfg.compact, cfg.delete) match {
          case (true, true)  => "compact,delete"
          case (false, true) => "delete"
          case (true, false) => "compact"        // unreachable given guard
          case _             => "(none)"
        }
        cleanupPolicyDriftThrottle.emit(
          s"cleanup.policy drift detected on ${topicPartition.topic}: " +
            s"effective policy is '$effective', expected 'compact'. " +
            s"This is a runtime divergence from the broker-startup contract " +
            s"(round-12 HIGH-1) — retention.ms will eventually delete rule " +
            s"records and the broker will silently fail-OPEN on the affected " +
            s"DENY rules. Revert with: bin/kafka-configs.sh --bootstrap-server " +
            s"<broker> --alter --entity-type topics --entity-name " +
            s"${topicPartition.topic} --add-config cleanup.policy=compact " +
            s"(round-14 HIGH H-1).")
      }
    }
  }

  /**
   * Round-15 BLOCKER-2 (recent-changes sub-agent): runtime drift detector for
   * the {@code __governance} partition count.
   *
   * <p>The startup gate
   * {@link BrokerServer#requireGovernanceTopicSinglePartition} runs once and
   * aborts startup if {@code __governance} has more than one partition. After
   * startup an operator {@code kafka-topics.sh --alter --partitions N} on
   * {@code __governance} would otherwise slip through silently —
   * {@link BrokerGovernanceBootstrap} hardcodes its cursor on partition 0,
   * so rules whose key hashes to any other partition disappear from this
   * broker's view of the rule set. That is a fail-OPEN of the entire DENY
   * engine for the affected rules, with no operator-visible signal.
   *
   * <p>Unlike the cleanup-policy drift detector, this check is metadata-based
   * rather than local-log-based: a fresh metadata image lists the topic's
   * partition count from the controller's authoritative view, which lets a
   * non-replica broker also catch the drift (HIGH-3 from the same audit).
   * The {@code partitionCountProbe} constructor parameter is wired by
   * {@link BrokerServer} to read from {@code metadataCache.currentImage()};
   * the default {@code () => 1} is silent-by-default for tests that don't
   * opt in to the check.
   *
   * <p>Non-fatal by design, mirroring the cleanup-policy drift detector: a
   * runtime check on the live drain tick should never throw because the
   * blast radius of a false positive on a security-critical surface is far
   * worse than a delayed remediation on a true positive. The throttled WARN
   * is the operator signal; the partitions on the rules topic are still
   * silently dropped until the operator either repartitions the topic to 1
   * (which Kafka does not natively support — they would need to delete +
   * recreate) or accepts the new partitioning by extending
   * {@link BrokerGovernanceBootstrap} to fan out across partitions.
   */
  private[server] def maybeWarnIfPartitionCountDrifted(): Unit = {
    val count = partitionCountProbe()
    if (count > 1) {
      partitionCountDriftThrottle.emit(
        s"partition-count drift detected on ${topicPartition.topic}: " +
          s"the topic now has $count partitions but this broker drains " +
          s"only partition 0. Rules whose key hashes to partitions 1..${count - 1} " +
          s"are invisible to this broker — every DENY rule on those partitions " +
          s"is silently fail-OPEN. The startup gate " +
          s"(BrokerServer.requireGovernanceTopicSinglePartition) caught this " +
          s"shape at boot; an operator AlterPartitions to grow the partition " +
          s"count post-startup bypassed it. Recovery is destructive: delete " +
          s"and recreate ${topicPartition.topic} with --partitions 1 " +
          s"(rules will need to be republished). " +
          s"(round-15 BLOCKER-2).")
    }
  }

  private val FailureWarnIntervalMs: Long = 60_000L
  // Visible for tests so the suppression window can be advanced synthetically.
  private[server] var failureWarnNowMs: () => Long = () => System.currentTimeMillis()
  // Visible for tests so they can assert how many WARNs actually fired —
  // capturing SLF4J output across the codebase is heavy and brittle. Shared
  // across all three throttles (drain failures, cleanup-policy drift,
  // partition-count drift) so a single test assertion captures the total
  // emission count regardless of which category fired.
  private[server] val warnEmissions = new AtomicLong(0L)

  /**
   * Round-15 HIGH-1 (recent-changes sub-agent): each WARN category has its
   * own dedup ledger so dedupe state does not leak across distinct concerns.
   * Before this split, drain failures, cleanup-policy drift, and
   * partition-count drift all routed through one
   * {@code maybeWarnSuppressed} ledger — a cleanup-policy drift WARN would
   * silence a subsequent partition-count drift WARN (different concern but
   * same "previous != msg" check evaluates true → first WARN fires, but the
   * cross-category collision still wasted one slot of the dedupe window for
   * an unrelated category). Worse, the legacy {@code maybeWarnSuppressed}
   * hard-coded the prefix {@code "governance rules drain failed"} — every
   * drift WARN announced itself as a drain failure, which an operator
   * scanning logs would naturally interpret as "the topic is unreadable",
   * not "the topic is misconfigured but draining fine".
   *
   * <p>Three throttle instances, one per category, each with its own prefix:
   * {@code drainFailureThrottle}, {@code cleanupPolicyDriftThrottle},
   * {@code partitionCountDriftThrottle}. They share only
   * {@link #warnEmissions} (a cumulative counter that tests assert against)
   * and {@link #failureWarnNowMs} (the time source that tests inject).
   */
  private class WarnThrottle(prefix: String) {
    private val lastWarnedMessage = new AtomicReference[String](null)
    private val lastWarnAtMs = new AtomicLong(0L)
    private val suppressedSinceLastWarn = new AtomicLong(0L)

    def emit(message: String): Unit = {
      val msg = if (message == null) "<null>" else message
      val previous = lastWarnedMessage.get()
      val now = failureWarnNowMs()
      if (previous == null || previous != msg) {
        val suppressed = suppressedSinceLastWarn.getAndSet(0L)
        lastWarnedMessage.set(msg)
        lastWarnAtMs.set(now)
        if (suppressed > 0L && previous != null) {
          warn(s"$prefix: $msg (previous '$previous' " +
            s"repeated and was suppressed $suppressed time(s) before this new message)")
        } else {
          warn(s"$prefix: $msg")
        }
        warnEmissions.incrementAndGet()
      } else if (now - lastWarnAtMs.get() >= FailureWarnIntervalMs) {
        val rolled = suppressedSinceLastWarn.getAndSet(0L)
        lastWarnAtMs.set(now)
        warn(s"$prefix: $msg (same condition repeated $rolled " +
          s"time(s) in the last ${FailureWarnIntervalMs}ms)")
        warnEmissions.incrementAndGet()
      } else {
        suppressedSinceLastWarn.incrementAndGet()
      }
    }
  }

  // The drain-failure throttle keeps the legacy "governance rules drain
  // failed" prefix so log-watchers that grep on that text continue to fire.
  private val drainFailureThrottle = new WarnThrottle("governance rules drain failed")
  // Drift throttles use distinct prefixes that name the actual concern.
  // Operator searching for "drift detected" finds both categories; searching
  // for "partition-count drift" or "cleanup.policy drift" narrows by category.
  private val cleanupPolicyDriftThrottle =
    new WarnThrottle("governance config drift detected")
  private val partitionCountDriftThrottle =
    new WarnThrottle("governance partitioning drift detected")

  private[server] def maybeWarnSuppressed(message: String): Unit =
    drainFailureThrottle.emit(message)

  /**
   * Result of a single [[replay]] pass. Carries three numbers:
   *
   * <ul>
   *   <li>{@code read} — total records iterated over, including ones that
   *       the loader rejected (malformed envelopes, null keys, cap hits).
   *       This is the count surfaced as [[drainOnce]]'s return value and
   *       what test assertions for "records iterated" pin.</li>
   *   <li>{@code applied} — records that successfully contributed to the
   *       loader's working state (a decoded update was put, or a tombstone
   *       removed/no-op'd an id). Used by the held-stale guard to decide
   *       whether to commit: a drain that read records but applied none
   *       is NOT forward progress through the log and must not clear a
   *       previously-good RuleSet. Audit B1.</li>
   *   <li>{@code advancedTo} — the offset replay actually got to. The
   *       caller advances [[nextOffset]] to this value, not to the
   *       requested {@code endOffset}, because a mid-pass empty read must
   *       NOT silently skip records we never consumed. Codex deep-audit
   *       P1 fix.</li>
   * </ul>
   */
  private case class ReplayResult(read: Long, applied: Long, advancedTo: Long)

  private def replay(log: UnifiedLog, startOffset: Long, endOffset: Long): ReplayResult = {
    var currentOffset = startOffset
    var read = 0L
    var applied = 0L
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
        return ReplayResult(read, applied, currentOffset)
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
            //
            // Audit B1: capture the boolean return of loader.apply so the
            // drainOnce held-stale guard can tell "read but rejected" from
            // "read and successfully applied". A thrown exception is treated
            // as a rejection here (didApply stays false), matching the
            // loader.apply contract for record-level faults.
            var didApply = false
            try {
              val key = bytes(rec.key())
              val value = bytes(rec.value())
              val keyStr =
                if (key == null) null
                else new String(key, StandardCharsets.UTF_8)
              didApply = loader.apply(keyStr, value)
            } catch {
              case t: Throwable =>
                warn(s"skipping poisoned __governance record at offset " +
                  s"${rec.offset()}: ${sanitizePoisonMessage(t)}")
            }
            if (didApply) {
              applied += 1
            }
            read += 1
          }
        }
        currentOffset = batch.nextOffset()
      }
    }
    ReplayResult(read, applied, currentOffset)
  }

  private def bytes(buf: ByteBuffer): Array[Byte] = {
    if (buf == null) return null
    Utils.toArray(buf.duplicate())
  }

  /**
   * Round-15 HIGH-2 (recent-changes sub-agent): the catch block around
   * {@link GovernanceLoader#apply} fires on producer-controlled record
   * content. If the upstream exception's {@code getMessage} embeds
   * attacker-chosen bytes — Jackson parse errors quote offending input,
   * and codec exceptions rethrow wrapping their cause — passing
   * {@code t.toString} into the WARN line would inject forged log records
   * into broker.log (CR/LF, ANSI escapes, bidi-isolate codepoints, …).
   *
   * <p>The fix is two-part: emit the exception {@code Class.getName}
   * unsanitised (always a trusted JVM-class identifier) so the operator
   * gets a clean diagnostic anchor, then run {@code getMessage} through
   * {@link LogSafe#sanitize} which collapses CR/LF, strips C0/C1 controls
   * and bidi-isolate codepoints, and bounds the length to
   * {@link LogSafe#MAX_LEN}. Visible for tests so a regression can pin
   * the exact post-sanitisation output without intercepting SLF4J.
   */
  private[server] def sanitizePoisonMessage(t: Throwable): String =
    s"${t.getClass.getName}: ${LogSafe.sanitize(t.getMessage)}"
}
