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
