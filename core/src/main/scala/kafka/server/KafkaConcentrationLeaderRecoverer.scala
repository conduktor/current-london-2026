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

import kafka.cluster.Partition
import kafka.log.LogManager
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.KafkaThread
import org.apache.kafka.storage.internals.concentration.{ConcentrationKernel, LogicalPartition}
import org.slf4j.LoggerFactory

import java.util.concurrent.{ExecutorService, Executors, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

/**
 * Per-partition leader-acquisition recovery for backing topics.
 *
 * Wires the third leg of GAP 2 from Codex's review: when this broker becomes leader of a
 * backing topic-partition, the in-memory tracker and on-disk sidecars for every logical
 * partition mapped onto that backing may be stale (rebuilt against a prior leader's view) or
 * missing (the broker was never leader since startup). Until the rebuild lands, the per-backing
 * readiness gate (B.1/B.2) keeps every logical Produce/Fetch/ListOffsets/DeleteRecords routed
 * to that backing rejected with {@code NOT_LEADER_OR_FOLLOWER}. This recoverer is what closes
 * the gate from "rebuilt" → "open".
 *
 * <h3>Lifecycle</h3>
 *
 * Called from {@link ReplicaManager#applyLocalLeadersDelta} immediately after a successful
 * {@code partition.makeLeader(...)}. Per Codex Q2, that is the right seam — late enough that
 * local partition leadership exists (so {@code logManager.getLog(tp)} is meaningful), early
 * enough that the broker hasn't yet advertised this partition as ready to serve.
 *
 * Each invocation:
 *  1. closes the gate via {@code kernel.markBackingUnready(backingTp)} — this bumps the
 *     per-backing generation token by 1;
 *  2. captures the new generation;
 *  3. submits a Runnable to the recovery executor that scans the backing log from
 *     {@code logStartOffset} to {@code logEndOffset}, applies the rebuilt sidecar state via
 *     {@code kernel.recoverFromBackingScan}, and finally publishes by calling
 *     {@code kernel.publishIfGenerationMatches(backingTp, generation, no-op)} which flips the
 *     gate back to ready under the per-key lock IFF the generation is still current.
 *
 * If makeLeader fails or another leadership transition arrives before the scan finishes, the
 * listener-driven {@code markBackingUnready} bumps generation again. When the scan eventually
 * completes its {@code publishIfGenerationMatches} call returns {@code false}, the gate stays
 * closed, and the recoverer trusts the next leader-acquisition event to retry. State written
 * by the stale scan is hidden behind the closed gate until that next acquisition's scan
 * overwrites it (the {@code recoverFromBackingScan} path truncates the sidecar before
 * rebuilding it, so stale on-disk state is naturally evicted on retry).
 *
 * <h3>Why in-place rebuild rather than atomic snapshot</h3>
 *
 * Codex Q3 recommended "publish recovered sidecar state by atomic snapshot replacement, not
 * incremental mutation visible to readers." This recoverer does {@em incremental mutation}
 * (via {@code recoverFromBackingScan} which truncates + refills sidecar files in place) but
 * under a {@em closed gate}: every concurrent reader sees {@code isBackingReady → false} and
 * is rejected with {@code NOT_LEADER_OR_FOLLOWER} before consulting any tracker state. The
 * gate-open transition is itself the atomic publish: it happens under
 * {@code ConcurrentHashMap.compute()}'s per-key lock inside
 * {@code publishIfGenerationMatches}, so a reader either sees gate-closed (and is rejected)
 * or gate-open (and reads the final state).
 *
 * The observable contract this maintains: a gate-open backing always reflects the latest
 * applied scan. Codex's "snapshot replacement" recommendation makes a stronger invariant
 * ("on-disk state during scan never reflects a partial rebuild"), but the gate-closed
 * invisibility window is sufficient for v1 correctness. A follow-up may add snapshot
 * replacement if a partial-mutation-during-scan reader path is added (it is not currently).
 *
 * <h3>Threading</h3>
 *
 * Recoveries for different backing partitions are independent (each touches a disjoint set of
 * sidecars; the kernel's per-(logicalTopic, logicalPartition) data structures are
 * thread-safe). The recoverer runs them on a small dedicated thread pool — independent of the
 * data-plane request handlers and the metadata-publishing thread — so a long scan on one
 * backing does not block leader-acquisition for another. The pool size is small (4 by
 * default) because the scan is I/O-bound on the backing log and the marginal speedup from a
 * larger pool is dominated by disk contention.
 *
 * Recoveries for the {@em same} backing partition ARE serialised — via
 * {@code kernel.backingScanLock(backingTp)}, acquired by {@link #runScan} for the duration of
 * pre-truncate → stream-consume → publish. Without this serialisation two scans for the same
 * backing would open separate {@code LogicalSidecarIndex} handles to the same on-disk file and
 * race their writes; the generation CAS at publish time only fences which gate-open wins, not
 * which scan's bytes survive. After acquiring the scan lock, {@code runScan} re-checks the
 * captured generation against the kernel's current generation: if it has moved (another
 * leader-acquisition arrived while we were queued), the scan abandons before touching state and
 * trusts the in-flight scan to complete the rebuild.
 *
 * <h3>Shutdown</h3>
 *
 * {@link #close} drains the executor with a bounded timeout, then forcibly cancels remaining
 * tasks. In-flight scans see a "no log" or "log closed" exception and abort cleanly — the
 * gate stays closed so any partial state is invisible.
 */
class KafkaConcentrationLeaderRecoverer(
    kernel: ConcentrationKernel,
    logManager: LogManager,
    executor: ExecutorService,
    readBufferBytes: Int = 1 << 20,
    shutdownTimeoutMs: Long = KafkaConcentrationLeaderRecoverer.SHUTDOWN_TIMEOUT_MS,
    shutdownForceTimeoutMs: Long = KafkaConcentrationLeaderRecoverer.SHUTDOWN_FORCE_TIMEOUT_MS) extends AutoCloseable {

  import KafkaConcentrationLeaderRecoverer._

  /**
   * Called by ReplicaManager.applyLocalLeadersDelta after a successful makeLeader on a backing
   * topic partition. Synchronous, fast (no I/O on the caller's thread): closes the gate,
   * captures (epoch, generation), submits the scan. Returns immediately.
   *
   * If {@code backingTp.topic} is not a declared backing topic, this is a no-op. Caller is
   * expected to filter via {@code kernel.isBackingTopic(...)} before invoking, to keep the
   * applyLocalLeadersDelta hot path free of unnecessary work — but this method tolerates a
   * non-backing call (returns silently rather than throwing) so it can be wired without
   * order-of-operation surprises.
   */
  def onMakeLeader(backingTp: TopicPartition, partition: Partition): Unit = {
    if (!kernel.isBackingTopic(backingTp.topic)) return

    // Close the gate (bumps generation). This is the source-of-truth invalidation: every
    // logical-topic read/write routed to this backing now sees gate-closed until the scan
    // republishes. Done synchronously on the caller's thread so there is no observable
    // window where a logical request can land between "the partition is locally leader" and
    // "the gate is closed".
    kernel.markBackingUnready(backingTp)
    val capturedGeneration = kernel.currentGeneration(backingTp)
    val capturedEpoch = partition.getLeaderEpoch
    val filter = kernel.logicalPartitionsForBacking(backingTp)

    if (filter.isEmpty) {
      // No logical partitions mapped to this backing. Open the gate immediately: there is no
      // sidecar state to rebuild, so the tracker's default (0, 0) is correct. A subsequent
      // declare() of a new logical topic that maps onto this backing will go through the same
      // makeLeader path the next time and trigger its own recovery.
      val opened = kernel.publishIfGenerationMatches(backingTp, capturedGeneration, () => ())
      if (!opened) {
        log.info(s"Concentration recovery: gate for $backingTp at gen=$capturedGeneration not " +
          "opened (generation changed between mark-unready and publish — another leadership " +
          "event is in progress)")
      }
      return
    }

    val taskId = nextTaskId.incrementAndGet()
    log.info(s"Concentration recovery [task=$taskId] queued for $backingTp at " +
      s"epoch=$capturedEpoch gen=$capturedGeneration with ${filter.size} logical partition(s)")

    val runnable: Runnable = () => runScan(taskId, backingTp, partition, capturedEpoch,
      capturedGeneration, filter)
    try {
      executor.submit(runnable)
    } catch {
      case ex: java.util.concurrent.RejectedExecutionException =>
        // Executor shut down. Gate stays closed; next leadership event will retry. We log at
        // info, not warn — a shutting-down broker isn't a fault; logical traffic on this
        // backing will just continue to see NOT_LEADER_OR_FOLLOWER until shutdown completes.
        log.info(s"Concentration recovery [task=$taskId] rejected for $backingTp " +
          s"(executor shut down); gate stays closed: ${ex.getMessage}")
    }
  }

  private def runScan(
      taskId: Long,
      backingTp: TopicPartition,
      partition: Partition,
      capturedEpoch: Int,
      capturedGeneration: Long,
      filter: java.util.Set[LogicalPartition]): Unit = {
    val started = System.nanoTime()
    // Resolve the scan lock under a guarded try: kernel.backingScanLock invokes ensureOpen() and
    // throws IllegalStateException if the kernel has been closed between executor.submit and now.
    // Without this guard the exception escapes runScan uncaught and the executor silently
    // swallows it — operators get no log, just a perpetually closed gate.
    val scanLock = try {
      kernel.backingScanLock(backingTp)
    } catch {
      case e: IllegalStateException =>
        log.info(s"Concentration recovery [task=$taskId] $backingTp: kernel closed before scan " +
          s"could start; aborting (gate stays closed): ${e.getMessage}")
        return
    }
    // Acquire BEFORE touching any sidecar state. Codex BLOCKER 1: without this, two concurrent
    // scans for the same backing race their truncate+append calls on the same sidecar file. The
    // lock makes "rebuild the sidecar for this backing" a critical section; the generation
    // re-check below is the stale-fence that lets a queued scan early-exit once a fresher scan
    // has already done the work.
    //
    // Codex round-7 HIGH 2: must be lockInterruptibly() — the recovery executor's shutdownNow
    // sends Thread.interrupt to its workers, and an uninterruptible lock acquisition would
    // strand a queued scan behind a long-running scan on the same backing until that scan
    // finishes naturally. The interruptible variant lets shutdownNow actually unblock the
    // queue; we abort with the gate closed (same outcome as InterruptedScanException below).
    try {
      scanLock.lockInterruptibly()
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        log.info(s"Concentration recovery [task=$taskId] $backingTp: scan lock acquisition " +
          s"interrupted before this scan could run; aborting (gate stays closed)")
        return
    }
    try {
      // Post-acquire generation re-check. If a later leader-acquisition has already bumped the
      // generation while we were queued for the scan lock, the next event's scan will run after
      // ours releases — there is no point in us scanning and publishing only to be overwritten.
      // Abort cleanly: the gate stays closed (markBackingUnready was called by every event), and
      // the next scan in the queue (or the one already running on its own backing) handles the
      // rebuild.
      val currentGen = kernel.currentGeneration(backingTp)
      if (currentGen != capturedGeneration) {
        log.info(s"Concentration recovery [task=$taskId] $backingTp: generation moved " +
          s"$capturedGeneration → $currentGen before scan lock was acquired; aborting (gate stays closed)")
        return
      }
      val unifiedLogOpt = logManager.getLog(backingTp)
      if (unifiedLogOpt.isEmpty) {
        // No local log for this backing partition. This is legitimate if the partition was
        // just deleted or moved off this broker between makeLeader and the recoverer running.
        // Reset every filter partition to (persistedStart, 0) under the closed gate so any
        // stale local sidecar/tracker state from a previous incarnation does not survive past
        // the publish below. Then attempt the generation-fenced publish: either it opens an
        // empty-state gate (the partition is still locally a backing-tp leader but with no
        // data yet) or generation has moved on and we leave the gate closed for the next
        // event to handle.
        log.info(s"Concentration recovery [task=$taskId] $backingTp: no local UnifiedLog; " +
          s"resetting ${filter.size} filter partition(s) and attempting empty-state publish " +
          s"at gen=$capturedGeneration")
        kernel.recoverFromBackingScan(java.util.Collections.emptyIterator(), filter)
        kernel.publishIfGenerationMatches(backingTp, capturedGeneration, () => ())
        return
      }
      val unifiedLog = unifiedLogOpt.get
      val startOffset = unifiedLog.logStartOffset
      val endOffset = unifiedLog.logEndOffset

      if (startOffset >= endOffset) {
        // Empty backing log. Reset every filter partition to (persistedStart, 0) under the
        // closed gate before opening it — otherwise a logical partition that USED to host
        // data on a previous leader of this backing (now empty) would publish stale sidecar
        // state and re-expose ghost records to consumers.
        log.info(s"Concentration recovery [task=$taskId] $backingTp empty " +
          s"([$startOffset, $endOffset)); resetting ${filter.size} filter partition(s) and " +
          s"opening gate at gen=$capturedGeneration")
        kernel.recoverFromBackingScan(java.util.Collections.emptyIterator(), filter)
        kernel.publishIfGenerationMatches(backingTp, capturedGeneration, () => ())
        return
      }

      // Cheap pre-fence: if the partition is no longer leader at the captured epoch, do not
      // bother scanning. This is an optimisation, not a correctness gate — the correctness
      // gate is the generation CAS at publish time. The epoch read is volatile so the worst
      // case here is a spurious scan, never an incorrect publish.
      val currentEpoch = partition.getLeaderEpoch
      if (currentEpoch != capturedEpoch) {
        log.info(s"Concentration recovery [task=$taskId] $backingTp: epoch moved " +
          s"$capturedEpoch → $currentEpoch before scan started; aborting (gate stays closed)")
        return
      }

      log.info(s"Concentration recovery [task=$taskId] $backingTp scanning " +
        s"[$startOffset, $endOffset) for ${filter.size} logical partition(s)")
      // Positional args match the existing call site in BackingLogScanRecovery; the iterator's
      // third parameter is named `nextOffset` internally (it tracks the cursor) — we hand it
      // the captured `startOffset` to begin the scan at the live logStartOffset.
      val iter = new BackingLogPageIterator(unifiedLog, filter, readBufferBytes, startOffset, endOffset)
      // 2-arg form: pre-truncates every filter partition to (persistedStart, 0), then advances
      // those that the stream actually yields records for. Filter partitions absent from the
      // stream remain at (persistedStart, 0) — the correct "no records on the new leader" state.
      // This closes Codex BLOCKER 2.
      kernel.recoverFromBackingScan(iter, filter)

      val opened = kernel.publishIfGenerationMatches(backingTp, capturedGeneration, () => ())
      val elapsedMs = (System.nanoTime() - started) / 1_000_000L
      if (opened) {
        log.info(s"Concentration recovery [task=$taskId] $backingTp completed in ${elapsedMs}ms " +
          s"at epoch=$capturedEpoch gen=$capturedGeneration; gate open")
      } else {
        // Generation moved on while we were scanning. The state we wrote IS on disk, but it
        // is hidden behind the closed gate. The next leadership event will trigger a fresh
        // scan that overwrites it (recoverFromBackingScan truncates the sidecar before
        // rebuilding).
        log.info(s"Concentration recovery [task=$taskId] $backingTp scan finished in " +
          s"${elapsedMs}ms but generation moved past gen=$capturedGeneration; state hidden, " +
          "will be rebuilt on next leader acquisition")
      }
    } catch {
      case _: InterruptedScanException =>
        // Cooperative interrupt from BackingLogPageIterator (B.6). Typical trigger: broker
        // shutdown calling close → shutdownNow → Thread.interrupt on the executor's worker.
        // The gate stays closed (publishIfGenerationMatches was not reached). Log at info,
        // not warn — shutdown-time abort is expected behaviour and operators should not be
        // paged for it. The interrupt flag has already been restored by the thrower so the
        // executor's awaitTermination observes a properly-cancelled thread.
        log.info(s"Concentration recovery [task=$taskId] $backingTp interrupted mid-scan at " +
          s"epoch=$capturedEpoch gen=$capturedGeneration; gate stays closed (shutdown or " +
          s"forced cancellation)")
      case t: Throwable =>
        // Any throw leaves the gate closed (publishIfGenerationMatches was either not reached
        // or itself preserves the closed state on throw). The next leadership event retries.
        // Log at warn so operators see persistent failures; the gate-closed state surfaces
        // as visible NOT_LEADER_OR_FOLLOWER on every logical request to the backing.
        log.warn(s"Concentration recovery [task=$taskId] $backingTp FAILED at " +
          s"epoch=$capturedEpoch gen=$capturedGeneration; gate stays closed: ${t.getMessage}", t)
    } finally {
      scanLock.unlock()
    }
  }

  /**
   * Drain the recovery executor. Existing in-flight scans get up to {@code timeoutMs}
   * milliseconds to finish; remaining tasks are forcibly cancelled. The kernel itself is NOT
   * closed here — the caller (BrokerServer.shutdown) closes it separately so logical-topic
   * data plane requests see a clean ConcentrationKernel.ensureOpen() failure rather than
   * dangling state.
   */
  override def close(): Unit = {
    executor.shutdown()
    try {
      if (!executor.awaitTermination(shutdownTimeoutMs, TimeUnit.MILLISECONDS)) {
        log.warn(s"Concentration recovery executor did not drain in ${shutdownTimeoutMs}ms; " +
          "forcing cancellation of in-flight scans (gate stays closed for those backings)")
        executor.shutdownNow()
        // Second awaitTermination (B.6). shutdownNow only SIGNALS via Thread.interrupt — it does
        // NOT wait. In-flight scans honour the signal at the next page boundary via
        // BackingLogPageIterator.refill's Thread.interrupted() check, but that takes time to
        // observe. Without this wait, close() returns while scan threads are still running:
        // - UnifiedLog handles stay open past kernel.close (next broker boot races on them)
        // - executor worker threads leak (KafkaThread daemons exit only on JVM termination,
        //   but a unit test or graceful-restart context hangs on them)
        // - half-baked scan state may yet call publishIfGenerationMatches with a stale gen,
        //   which is a no-op for correctness but pollutes logs and confuses operators
        // Bounded wait so a pathological scan cannot block shutdown indefinitely.
        if (!executor.awaitTermination(shutdownForceTimeoutMs, TimeUnit.MILLISECONDS)) {
          log.warn(s"Concentration recovery executor did not honour interrupt within " +
            s"${shutdownForceTimeoutMs}ms after shutdownNow; some scan threads may still " +
            s"be running. Per-backing gates remain closed for those backings.")
        }
      }
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        executor.shutdownNow()
    }
  }
}

object KafkaConcentrationLeaderRecoverer {
  private val log = LoggerFactory.getLogger(classOf[KafkaConcentrationLeaderRecoverer])

  private val nextTaskId = new AtomicLong(0L)

  // Bounded shutdown wait. Long enough to let typical scans finish (a few hundred MB of
  // backing log at 1 MiB per page is well under a second of I/O), short enough that a
  // pathological scan cannot stall broker shutdown indefinitely.
  private val SHUTDOWN_TIMEOUT_MS: Long = 10_000L

  // Bounded post-shutdownNow wait (B.6). After we forcibly interrupt in-flight scans we still
  // need to let them honour the interrupt at the next page boundary (~1 MiB per page in
  // BackingLogPageIterator.refill). Five seconds covers a couple of slow I/O pages plus the
  // unwinding through the recoverer's try/finally. Beyond that we give up — there is no
  // graceful path left, only logging.
  private val SHUTDOWN_FORCE_TIMEOUT_MS: Long = 5_000L

  /**
   * Default pool: 4 threads. Backing-log scans are I/O-bound; oversizing the pool hurts more
   * than it helps because page-cache contention dominates. 4 matches the typical number of
   * I/O threads a broker uses for similar background tasks.
   */
  def newDefaultExecutor(brokerId: Int): ExecutorService = {
    val tf: ThreadFactory = (r: Runnable) => KafkaThread.daemon(
      s"concentration-leader-recovery-broker-$brokerId-${nextThreadIdx.incrementAndGet()}",
      r)
    Executors.newFixedThreadPool(4, tf)
  }

  private val nextThreadIdx = new AtomicLong(0L)
}
