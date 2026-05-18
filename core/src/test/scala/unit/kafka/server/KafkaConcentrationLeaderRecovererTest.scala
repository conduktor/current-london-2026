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
import kafka.log.{LogManager, UnifiedLog}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.storage.internals.concentration.{ConcentrationKernel, LogicalPartition, RecoveryRecord}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.{any, eq => mockEq}
import org.mockito.Mockito._

import java.util
import java.util.concurrent.{Executors, RejectedExecutionException, TimeUnit}
import java.util.concurrent.locks.ReentrantLock

class KafkaConcentrationLeaderRecovererTest {

  private val backingTp = new TopicPartition("orders-backing", 7)
  private val nonBackingTp = new TopicPartition("ordinary-topic", 0)

  // Runs Runnables synchronously on the caller's thread so we can assert scan-path behaviour
  // without juggling timeouts. Real broker wiring uses a real fixed-thread pool — that
  // executor's behaviour is delegated to the JDK; only the recoverer's orchestration logic
  // needs unit coverage here.
  private def synchronousExecutor(): java.util.concurrent.ExecutorService = {
    val exec = mock(classOf[java.util.concurrent.ExecutorService])
    when(exec.submit(any[Runnable])).thenAnswer { invocation =>
      val r = invocation.getArgument[Runnable](0)
      r.run()
      null
    }
    exec
  }

  // runScan acquires kernel.backingScanLock(backingTp) before any sidecar mutation. Mockito's
  // mock default for ReentrantLock-returning methods is null, so every test that drives runScan
  // must stub the accessor with a real lock — otherwise the very first call lock.lock() NPEs
  // and the test failure obscures the actual scan-path behaviour we are trying to assert.
  private def stubScanLock(kernel: ConcentrationKernel, tp: TopicPartition): ReentrantLock = {
    val lock = new ReentrantLock()
    when(kernel.backingScanLock(tp)).thenReturn(lock)
    lock
  }

  private def filterOf(logicalTopic: String, partitions: Int*): util.Set[LogicalPartition] = {
    val set = new util.HashSet[LogicalPartition]()
    partitions.foreach(p => set.add(new LogicalPartition(logicalTopic, p)))
    set
  }

  @Test
  def onMakeLeaderOnNonBackingTopicIsANoop(): Unit = {
    // Mirrors maybeStartConcentrationRecovery's expected behaviour for ordinary topics:
    // the recoverer must short-circuit before any kernel state is touched, so that
    // leader-acquisition for the broker's everyday partitions stays free of overhead.
    val kernel = mock(classOf[ConcentrationKernel])
    val logManager = mock(classOf[LogManager])
    val executor = mock(classOf[java.util.concurrent.ExecutorService])
    when(kernel.isBackingTopic(nonBackingTp.topic)).thenReturn(false)
    val partition = mock(classOf[Partition])
    val recoverer = new KafkaConcentrationLeaderRecoverer(kernel, logManager, executor)

    recoverer.onMakeLeader(nonBackingTp, partition)

    verify(kernel).isBackingTopic(nonBackingTp.topic)
    verifyNoMoreInteractions(kernel)
    verifyNoInteractions(executor)
    verifyNoInteractions(logManager)
  }

  @Test
  def onMakeLeaderEmptyFilterOpensGateSynchronouslyWithoutSubmittingScan(): Unit = {
    // No logical partitions mapped onto this backing → no sidecar state to rebuild.
    // The default (0, 0) tracker state is correct, so we open the gate immediately
    // under the captured generation and skip the executor entirely. Any subsequent
    // declare() that maps a logical partition onto this backing flows through a fresh
    // leader-acquisition path.
    val kernel = mock(classOf[ConcentrationKernel])
    val logManager = mock(classOf[LogManager])
    val executor = mock(classOf[java.util.concurrent.ExecutorService])
    when(kernel.isBackingTopic(backingTp.topic)).thenReturn(true)
    when(kernel.currentGeneration(backingTp)).thenReturn(42L)
    when(kernel.logicalPartitionsForBacking(backingTp)).thenReturn(new util.HashSet[LogicalPartition]())
    when(kernel.publishIfGenerationMatches(mockEq(backingTp), mockEq(42L), any[Runnable])).thenReturn(true)
    val partition = mock(classOf[Partition])
    when(partition.getLeaderEpoch).thenReturn(3)
    val recoverer = new KafkaConcentrationLeaderRecoverer(kernel, logManager, executor)

    recoverer.onMakeLeader(backingTp, partition)

    verify(kernel).markBackingUnready(backingTp)
    verify(kernel).publishIfGenerationMatches(mockEq(backingTp), mockEq(42L), any[Runnable])
    verifyNoInteractions(executor)
    verifyNoInteractions(logManager)
  }

  @Test
  def onMakeLeaderWithFilterClosesGateAndSubmitsScanToExecutor(): Unit = {
    // Non-empty filter → markBackingUnready closes the gate synchronously, then a single
    // Runnable is submitted to the recovery executor for the async scan. The
    // markBackingUnready call happens BEFORE submission so a concurrent producer
    // request that races between makeLeader and submit cannot observe a stale-but-open
    // gate.
    val kernel = mock(classOf[ConcentrationKernel])
    val logManager = mock(classOf[LogManager])
    val executor = mock(classOf[java.util.concurrent.ExecutorService])
    when(kernel.isBackingTopic(backingTp.topic)).thenReturn(true)
    when(kernel.currentGeneration(backingTp)).thenReturn(100L)
    when(kernel.logicalPartitionsForBacking(backingTp))
      .thenReturn(filterOf("orders", 0, 1, 2))
    val partition = mock(classOf[Partition])
    when(partition.getLeaderEpoch).thenReturn(5)
    val recoverer = new KafkaConcentrationLeaderRecoverer(kernel, logManager, executor)

    recoverer.onMakeLeader(backingTp, partition)

    val order = inOrder(kernel, executor)
    order.verify(kernel).markBackingUnready(backingTp)
    order.verify(executor).submit(any[Runnable])
    // The scan itself runs async; publish must not have happened on this thread.
    verify(kernel, never()).publishIfGenerationMatches(any[TopicPartition], any[Long].asInstanceOf[Long], any[Runnable])
  }

  @Test
  def onMakeLeaderSwallowsRejectedExecutionFromShutdownExecutor(): Unit = {
    // A leader-acquisition arriving while the broker is shutting down is legitimate; the
    // recoverer must absorb the RejectedExecutionException so it does NOT propagate into
    // applyLocalLeadersDelta. The gate stays closed (markBackingUnready already ran), and
    // a subsequent restart's first scan will rebuild whatever state was unfinished.
    val kernel = mock(classOf[ConcentrationKernel])
    val logManager = mock(classOf[LogManager])
    val executor = mock(classOf[java.util.concurrent.ExecutorService])
    when(kernel.isBackingTopic(backingTp.topic)).thenReturn(true)
    when(kernel.currentGeneration(backingTp)).thenReturn(7L)
    when(kernel.logicalPartitionsForBacking(backingTp))
      .thenReturn(filterOf("orders", 0))
    when(executor.submit(any[Runnable])).thenThrow(new RejectedExecutionException("shut down"))
    val partition = mock(classOf[Partition])
    when(partition.getLeaderEpoch).thenReturn(1)
    val recoverer = new KafkaConcentrationLeaderRecoverer(kernel, logManager, executor)

    // Must not throw.
    recoverer.onMakeLeader(backingTp, partition)

    verify(kernel).markBackingUnready(backingTp)
    verify(executor).submit(any[Runnable])
    // Gate stays closed — no synchronous publish on the rejection path.
    verify(kernel, never()).publishIfGenerationMatches(any[TopicPartition], any[Long].asInstanceOf[Long], any[Runnable])
  }

  @Test
  def scanWhenLogManagerHasNoLogAttemptsEmptyPublish(): Unit = {
    // Partition moved off this broker between makeLeader and the scan running, or the
    // backing partition genuinely has no segments yet. Either way: there is no log to
    // scan, so try to publish empty state under the captured generation. If the
    // generation has moved on the CAS in publishIfGenerationMatches will reject and
    // the next leadership event handles it.
    val kernel = mock(classOf[ConcentrationKernel])
    val logManager = mock(classOf[LogManager])
    val executor = synchronousExecutor()
    when(kernel.isBackingTopic(backingTp.topic)).thenReturn(true)
    when(kernel.currentGeneration(backingTp)).thenReturn(11L)
    when(kernel.logicalPartitionsForBacking(backingTp))
      .thenReturn(filterOf("orders", 0))
    stubScanLock(kernel, backingTp)
    when(logManager.getLog(mockEq(backingTp), any[Boolean])).thenReturn(None)
    val partition = mock(classOf[Partition])
    when(partition.getLeaderEpoch).thenReturn(2)
    val recoverer = new KafkaConcentrationLeaderRecoverer(kernel, logManager, executor)

    recoverer.onMakeLeader(backingTp, partition)

    verify(kernel).markBackingUnready(backingTp)
    verify(kernel).publishIfGenerationMatches(mockEq(backingTp), mockEq(11L), any[Runnable])
    // BLOCKER 2 fix: even on the missing-log path we MUST reset every filter partition's
    // local state to (persistedStart, 0) before opening the gate, otherwise stale sidecar
    // entries from a previous incarnation as leader would survive past the publish.
    // The recover call uses an empty iterator — only the pre-truncate side-effect matters.
    verify(kernel).recoverFromBackingScan(any[util.Iterator[RecoveryRecord]], any[util.Set[LogicalPartition]])
  }

  @Test
  def scanOnEmptyBackingLogResetsFilterAndOpensGate(): Unit = {
    // Backing log has no records (logStartOffset == logEndOffset) but stale local state
    // from a previous incarnation as leader may exist. BLOCKER 2 fix: pre-truncate every
    // filter partition (via recoverFromBackingScan with an empty stream) BEFORE opening the
    // gate. Otherwise the stale sidecar/tracker entries become visible to readers the
    // instant publish flips the gate.
    val kernel = mock(classOf[ConcentrationKernel])
    val logManager = mock(classOf[LogManager])
    val executor = synchronousExecutor()
    when(kernel.isBackingTopic(backingTp.topic)).thenReturn(true)
    when(kernel.currentGeneration(backingTp)).thenReturn(99L)
    when(kernel.logicalPartitionsForBacking(backingTp))
      .thenReturn(filterOf("orders", 0, 1))
    stubScanLock(kernel, backingTp)
    val unifiedLog = mock(classOf[UnifiedLog])
    when(unifiedLog.logStartOffset).thenReturn(0L)
    when(unifiedLog.logEndOffset).thenReturn(0L)
    when(logManager.getLog(mockEq(backingTp), any[Boolean])).thenReturn(Some(unifiedLog))
    val partition = mock(classOf[Partition])
    when(partition.getLeaderEpoch).thenReturn(4)
    val recoverer = new KafkaConcentrationLeaderRecoverer(kernel, logManager, executor)

    recoverer.onMakeLeader(backingTp, partition)

    verify(kernel).recoverFromBackingScan(any[util.Iterator[RecoveryRecord]], any[util.Set[LogicalPartition]])
    verify(kernel).publishIfGenerationMatches(mockEq(backingTp), mockEq(99L), any[Runnable])
  }

  @Test
  def scanAbortsWithoutPublishWhenEpochMovedBeforeScanStarted(): Unit = {
    // Cheap pre-fence: if the partition is no longer leader at the captured epoch when
    // the scan thread starts, abandon the scan. This is an optimisation, not a
    // correctness gate — the correctness gate is the generation CAS at publish time —
    // so we explicitly assert NO publish happens on this path. Either the next
    // leadership event scheduled a fresh recovery (whose listener already bumped
    // generation) or it will, and that recovery handles the rebuild.
    val kernel = mock(classOf[ConcentrationKernel])
    val logManager = mock(classOf[LogManager])
    val executor = synchronousExecutor()
    when(kernel.isBackingTopic(backingTp.topic)).thenReturn(true)
    when(kernel.currentGeneration(backingTp)).thenReturn(50L)
    when(kernel.logicalPartitionsForBacking(backingTp))
      .thenReturn(filterOf("orders", 0))
    stubScanLock(kernel, backingTp)
    val unifiedLog = mock(classOf[UnifiedLog])
    when(unifiedLog.logStartOffset).thenReturn(0L)
    when(unifiedLog.logEndOffset).thenReturn(1000L)
    when(logManager.getLog(backingTp)).thenReturn(Some(unifiedLog))
    val partition = mock(classOf[Partition])
    // First call (capture) returns 5; second call (pre-fence inside runScan) returns 6.
    when(partition.getLeaderEpoch).thenReturn(5, 6)
    val recoverer = new KafkaConcentrationLeaderRecoverer(kernel, logManager, executor)

    recoverer.onMakeLeader(backingTp, partition)

    verify(kernel).markBackingUnready(backingTp)
    verify(kernel, never()).recoverFromBackingScan(any())
    verify(kernel, never()).recoverFromBackingScan(any[util.Iterator[RecoveryRecord]], any[util.Set[LogicalPartition]])
    verify(kernel, never()).publishIfGenerationMatches(any[TopicPartition], any[Long].asInstanceOf[Long], any[Runnable])
  }

  @Test
  def scanFailureLeavesGateClosedAndDoesNotPropagate(): Unit = {
    // recoverFromBackingScan throws (corrupt page, I/O error, etc.). The Runnable must
    // not let the exception escape the executor — uncaught exceptions in
    // ThreadPoolExecutor tasks are silently logged at WARN by Kafka's worker thread
    // factory and discarded, but here we explicitly catch and log so we have an audit
    // trail keyed on the task id. Critically: the gate stays closed because no publish
    // was attempted, so the next leader-acquisition retries.
    val kernel = mock(classOf[ConcentrationKernel])
    val logManager = mock(classOf[LogManager])
    val executor = synchronousExecutor()
    when(kernel.isBackingTopic(backingTp.topic)).thenReturn(true)
    when(kernel.currentGeneration(backingTp)).thenReturn(7L)
    when(kernel.logicalPartitionsForBacking(backingTp))
      .thenReturn(filterOf("orders", 0))
    stubScanLock(kernel, backingTp)
    val unifiedLog = mock(classOf[UnifiedLog])
    when(unifiedLog.logStartOffset).thenReturn(0L)
    when(unifiedLog.logEndOffset).thenReturn(100L)
    when(logManager.getLog(mockEq(backingTp), any[Boolean])).thenReturn(Some(unifiedLog))
    doThrow(new RuntimeException("simulated scan fault"))
      .when(kernel).recoverFromBackingScan(any[util.Iterator[RecoveryRecord]], any[util.Set[LogicalPartition]])
    val partition = mock(classOf[Partition])
    when(partition.getLeaderEpoch).thenReturn(1)
    val recoverer = new KafkaConcentrationLeaderRecoverer(kernel, logManager, executor)

    // Must not throw — the Runnable's try/catch absorbs the fault.
    recoverer.onMakeLeader(backingTp, partition)

    verify(kernel, never()).publishIfGenerationMatches(any[TopicPartition], any[Long].asInstanceOf[Long], any[Runnable])
  }

  @Test
  def scanAbortsWithoutTouchingStateWhenGenerationMovedBeforeLockAcquired(): Unit = {
    // BLOCKER 1 fix: runScan acquires kernel.backingScanLock(backingTp) before any sidecar
    // mutation, and then re-checks the generation. If the kernel reports a different generation
    // than the one captured at submit time, another leader-acquisition has already bumped the
    // gate and a fresher scan is in flight (or done). The stale scan MUST early-exit without
    // calling recoverFromBackingScan — otherwise it would re-truncate the rebuilt sidecar with
    // its own (now invalid) view and the gate-open of the fresher scan would expose a partial
    // rebuild. The publish-time CAS alone would not save us here: the corruption happens during
    // the scan, before the publish is even attempted.
    val kernel = mock(classOf[ConcentrationKernel])
    val logManager = mock(classOf[LogManager])
    val executor = synchronousExecutor()
    when(kernel.isBackingTopic(backingTp.topic)).thenReturn(true)
    // captured at submit time (inside onMakeLeader): generation = 5
    when(kernel.currentGeneration(backingTp)).thenReturn(5L, 9L)
    when(kernel.logicalPartitionsForBacking(backingTp))
      .thenReturn(filterOf("orders", 0))
    stubScanLock(kernel, backingTp)
    val partition = mock(classOf[Partition])
    when(partition.getLeaderEpoch).thenReturn(3)
    val recoverer = new KafkaConcentrationLeaderRecoverer(kernel, logManager, executor)

    recoverer.onMakeLeader(backingTp, partition)

    // Gate-close still happened (markBackingUnready ran on the submitter's thread before scan
    // was queued), but the scan body did not touch any sidecar state and did not publish.
    verify(kernel).markBackingUnready(backingTp)
    verify(kernel, never()).recoverFromBackingScan(any[util.Iterator[RecoveryRecord]], any[util.Set[LogicalPartition]])
    verify(kernel, never()).publishIfGenerationMatches(any[TopicPartition], any[Long].asInstanceOf[Long], any[Runnable])
    // logManager.getLog must NOT have been consulted either — early-exit fires BEFORE any I/O.
    verifyNoInteractions(logManager)
  }

  @Test
  def scanAcquiresAndReleasesBackingScanLockEvenOnException(): Unit = {
    // The lock-acquire / lock-release contract: every runScan path — happy, exception, early
    // exit — must release the per-backing scan lock so a later leader-acquisition is not
    // permanently blocked. Use a real ReentrantLock so we can assert isLocked() after the call.
    val kernel = mock(classOf[ConcentrationKernel])
    val logManager = mock(classOf[LogManager])
    val executor = synchronousExecutor()
    when(kernel.isBackingTopic(backingTp.topic)).thenReturn(true)
    when(kernel.currentGeneration(backingTp)).thenReturn(7L)
    when(kernel.logicalPartitionsForBacking(backingTp))
      .thenReturn(filterOf("orders", 0))
    val realLock = stubScanLock(kernel, backingTp)
    val unifiedLog = mock(classOf[UnifiedLog])
    when(unifiedLog.logStartOffset).thenReturn(0L)
    when(unifiedLog.logEndOffset).thenReturn(100L)
    when(logManager.getLog(mockEq(backingTp), any[Boolean])).thenReturn(Some(unifiedLog))
    doThrow(new RuntimeException("simulated scan fault"))
      .when(kernel).recoverFromBackingScan(any[util.Iterator[RecoveryRecord]], any[util.Set[LogicalPartition]])
    val partition = mock(classOf[Partition])
    when(partition.getLeaderEpoch).thenReturn(1)
    val recoverer = new KafkaConcentrationLeaderRecoverer(kernel, logManager, executor)

    recoverer.onMakeLeader(backingTp, partition)

    assertFalse(realLock.isLocked, "scan lock must be released even after a scan exception")
  }

  @Test
  def scanAbortsCleanlyWhenKernelClosedBeforeScanLockResolved(): Unit = {
    // Operability contract: if the kernel is closed between executor.submit and runScan reaching
    // kernel.backingScanLock(...), the IllegalStateException from ensureOpen() must be caught,
    // logged, and the scan must abort cleanly — NOT escape uncaught into the Runnable wrapper
    // (where the executor would silently swallow it, leaving operators with a perpetually closed
    // gate and no log trail). This is a defensive guard against the narrow shutdown-ordering
    // window where the broker closes the kernel before draining the recovery executor.
    val kernel = mock(classOf[ConcentrationKernel])
    val logManager = mock(classOf[LogManager])
    val executor = synchronousExecutor()
    when(kernel.isBackingTopic(backingTp.topic)).thenReturn(true)
    when(kernel.currentGeneration(backingTp)).thenReturn(3L)
    when(kernel.logicalPartitionsForBacking(backingTp))
      .thenReturn(filterOf("orders", 0))
    when(kernel.backingScanLock(backingTp))
      .thenThrow(new IllegalStateException("ConcentrationKernel is closed"))
    val partition = mock(classOf[Partition])
    when(partition.getLeaderEpoch).thenReturn(1)
    val recoverer = new KafkaConcentrationLeaderRecoverer(kernel, logManager, executor)

    // Must not throw. The scan body must never have been entered.
    recoverer.onMakeLeader(backingTp, partition)

    verify(kernel, never()).recoverFromBackingScan(any[util.Iterator[RecoveryRecord]], any[util.Set[LogicalPartition]])
    verify(kernel, never()).publishIfGenerationMatches(any[TopicPartition], any[Long].asInstanceOf[Long], any[Runnable])
    verifyNoInteractions(logManager)
  }

  @Test
  def closeWaitsForInFlightScanAfterShutdownNowToHonourInterrupt(): Unit = {
    // B.6 second-awaitTermination contract. The scenario: a scan is mid-flight when broker
    // shutdown calls recoverer.close(). The first awaitTermination times out (the scan is
    // still running). close() then calls shutdownNow which sends Thread.interrupt to the
    // worker. WITHOUT a second awaitTermination, close() would return immediately and the
    // worker thread would still be racing with the broker's other shutdown steps — closing
    // log handles, terminating other thread pools — leading to a window where in-flight
    // scan I/O races against kernel.close.
    //
    // We pin the contract by:
    //   1. Submitting a long task that respects Thread.interrupt (the standard
    //      cooperative-cancel pattern that BackingLogPageIterator.refill follows).
    //   2. Calling close() with the production close paths but compressed timeouts so the
    //      test completes quickly while still exercising the full two-phase drain.
    //   3. Asserting close() returns ONLY after the worker has finished honouring the
    //      interrupt — i.e. that the executor's awaitTermination was called twice and the
    //      second one actually waited.
    val testShutdownTimeoutMs = 200L
    val testShutdownForceTimeoutMs = 2_000L
    val executor = Executors.newSingleThreadExecutor()
    val taskStarted = new java.util.concurrent.CountDownLatch(1)
    val taskFinished = new java.util.concurrent.atomic.AtomicBoolean(false)
    // The task sleeps comfortably longer than the first shutdown window so the first
    // awaitTermination is guaranteed to time out. Then it cooperatively-checks the
    // interrupt flag — same pattern as the page iterator. Without the close-second-wait,
    // the test's taskFinished assertion would fail (the task is interrupted but
    // close() returned before the catch+finally ran).
    executor.submit(new Runnable {
      override def run(): Unit = {
        taskStarted.countDown()
        try {
          // Long enough that the first awaitTermination definitely times out, but short
          // enough that the SECOND awaitTermination has time to observe completion within
          // its own bounded window.
          Thread.sleep(testShutdownTimeoutMs * 5)
        } catch {
          case _: InterruptedException =>
            // Cooperative honour: restore the flag and exit cleanly, exactly as
            // BackingLogPageIterator.refill is required to do.
            Thread.currentThread().interrupt()
        } finally {
          taskFinished.set(true)
        }
      }
    })
    assertTrue(taskStarted.await(2, TimeUnit.SECONDS), "task must start")

    val kernel = mock(classOf[ConcentrationKernel])
    val logManager = mock(classOf[LogManager])
    val recoverer = new KafkaConcentrationLeaderRecoverer(kernel, logManager, executor,
      shutdownTimeoutMs = testShutdownTimeoutMs,
      shutdownForceTimeoutMs = testShutdownForceTimeoutMs)

    val before = System.nanoTime()
    recoverer.close()
    val elapsedMs = (System.nanoTime() - before) / 1_000_000L

    // After close() returns:
    //   - The task MUST have observed the interrupt and exited the finally block. That is
    //     the second-awaitTermination's job.
    //   - The total elapsed time MUST be at least testShutdownTimeoutMs (first wait timed
    //     out) and well under the worst-case ceiling (first + force) because the task
    //     exits promptly on interrupt, so the second wait returns early.
    assertTrue(taskFinished.get,
      "B.6: close() must wait for in-flight task to honour interrupt before returning")
    assertTrue(executor.isTerminated, "executor must be terminated after close()")
    assertTrue(elapsedMs >= testShutdownTimeoutMs,
      s"close() must wait at least the first-shutdown window (${testShutdownTimeoutMs} ms); " +
        s"actual elapsed: ${elapsedMs}ms")
    // Sanity: we should be well under the worst case (first + force = 2.2s).
    // 1500ms leaves enough buffer for CI jitter while still detecting a regression where
    // close() ends up waiting the full second-window.
    assertTrue(elapsedMs < testShutdownTimeoutMs + 1_500L,
      s"close() should return shortly after the interrupted task exits, not wait the " +
        s"full second window. Elapsed: ${elapsedMs}ms")
  }

  @Test
  def closeDrainsExecutorWithinBoundedTimeout(): Unit = {
    // The default executor is a daemon-threaded fixed-thread-pool; close() must drain it
    // so broker shutdown cannot leave concentration recovery threads alive. The bounded
    // timeout protects shutdown from a pathological in-flight scan.
    val executor = Executors.newSingleThreadExecutor()
    val kernel = mock(classOf[ConcentrationKernel])
    val logManager = mock(classOf[LogManager])
    val recoverer = new KafkaConcentrationLeaderRecoverer(kernel, logManager, executor)

    recoverer.close()

    assertTrue(executor.isShutdown, "Executor must be shut down after close()")
    assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS),
      "Executor must terminate within the shutdown timeout")
  }

  @Test
  def newDefaultExecutorProducesUsableDaemonPool(): Unit = {
    // Sanity check the construction helper — broker startup wires this from
    // BrokerServer, so a failure here would brick broker boot.
    val exec = KafkaConcentrationLeaderRecoverer.newDefaultExecutor(brokerId = 0)
    try {
      val ran = new java.util.concurrent.atomic.AtomicBoolean(false)
      val future = exec.submit(new Runnable { override def run(): Unit = ran.set(true) })
      future.get(1, TimeUnit.SECONDS)
      assertTrue(ran.get, "Default executor must run submitted Runnables")
    } finally {
      exec.shutdownNow()
    }
  }
}
