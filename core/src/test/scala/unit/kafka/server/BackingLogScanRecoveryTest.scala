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
import kafka.log.LogTestUtils
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.utils.{MockTime, Utils}
import org.apache.kafka.server.util.KafkaScheduler
import org.apache.kafka.storage.internals.concentration.{ConcentrationKernel, LogicalProduceStamper, LogicalTopicDescriptor}
import org.apache.kafka.storage.internals.log.LogConfig
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._
import org.mockito.Mockito.{mock, when}

import java.io.File

/**
 * Behavioural contract of {@link BackingLogScanRecovery}. The driver pages a backing UnifiedLog
 * and feeds extracted {@link org.apache.kafka.storage.internals.concentration.RecoveryRecord}s
 * into the {@link ConcentrationKernel} so that sidecars whose files are missing on disk
 * are rebuilt from the durable log. This is the PROMPT acceptance criterion
 * "restart without index → full log scan to rebuild the mapping; no data loss."
 *
 * <p>Pins:
 * <ul>
 *   <li>Empty needs-set is a fast no-op (does not open any UnifiedLog).</li>
 *   <li>Multiple logical partitions interleaved on the same backing partition are rebuilt with
 *       correct logical offsets per partition.</li>
 *   <li>Backing partition with no UnifiedLog instance on this broker is skipped (the partitions
 *       it would have owned stay at the tracker default).</li>
 *   <li>Records on the backing log that don't belong to a partition needing recovery are
 *       silently ignored (mixed-logical-topic backing partition is the normal case).</li>
 * </ul>
 */
class BackingLogScanRecoveryTest {

  private val brokerTopicStats = new BrokerTopicStats
  private val time = new MockTime
  private val scheduler = new KafkaScheduler(1, true, "scan-recovery-test")
  private val tmpDir: File = TestUtils.tempDir()
  private val logDir: File = TestUtils.randomPartitionLogDir(tmpDir)
  private val sidecarDir: File = TestUtils.tempDir()

  private var unifiedLog: UnifiedLog = _
  private var logManager: LogManager = _
  private var kernel: ConcentrationKernel = _

  @BeforeEach
  def setUp(): Unit = {
    scheduler.startup()
    val logConfig: LogConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    unifiedLog = LogTestUtils.createLog(logDir, logConfig, brokerTopicStats, scheduler, time)
    logManager = mock(classOf[LogManager])
    kernel = new ConcentrationKernel(sidecarDir)
  }

  @AfterEach
  def tearDown(): Unit = {
    try {
      Utils.closeQuietly(kernel, "kernel")
      Utils.closeQuietly(unifiedLog, "unifiedLog")
      brokerTopicStats.close()
      scheduler.shutdown()
    } finally {
      Utils.delete(tmpDir)
      Utils.delete(sidecarDir)
    }
  }

  @Test
  def runIsNoOpWhenAllSidecarsPresent(): Unit = {
    // No logical topics declared → partitionsWithoutSidecar() returns empty → the driver must
    // not invoke logManager at all (verified by leaving the mock unstubbed; any call would
    // return null and trip a downstream NPE).
    new BackingLogScanRecovery(kernel, logManager).run()
    // Reaching here without exception is the assertion.
  }

  @Test
  def runIsNoOpWhenBackingLogAbsent(): Unit = {
    val descriptor = new LogicalTopicDescriptor("orders", 4, "backing-orders", 2)
    kernel.declare(descriptor)
    // logManager mock returns Optional.empty for any TopicPartition.
    when(logManager.getLog(new TopicPartition("backing-orders", 0), false)).thenReturn(None)
    when(logManager.getLog(new TopicPartition("backing-orders", 1), false)).thenReturn(None)

    new BackingLogScanRecovery(kernel, logManager).run()

    // All 4 logical partitions still have no sidecar — that's the correct outcome: no data was
    // available to rebuild from. Tracker stays at default (0, 0).
    assertEquals(4, kernel.partitionsWithoutSidecar().size,
      "no backing log means no rebuild — all 4 logical partitions remain unrecovered")
  }

  @Test
  def runRebuildsSidecarFromStampedBackingRecords(): Unit = {
    // The core scenario: 3 logical topics on one backing partition. Sidecars are missing.
    // The driver scans the backing log and the kernel rebuilds the per-logical-partition
    // sidecars and tracker. Stock producer sees logical offsets after the scan.
    val backingTopic = "backing-shared"
    val descriptorOrders = new LogicalTopicDescriptor("orders", 2, backingTopic, 1)
    val descriptorEvents = new LogicalTopicDescriptor("events", 2, backingTopic, 1)
    kernel.declare(descriptorOrders)
    kernel.declare(descriptorEvents)

    // Stamp three records, interleaving logical topics + partitions on the same backing
    // partition. Order matches what KafkaApis would have written.
    appendStamped("orders", 0, Array(0L), key = "k0", value = "orders-0")
    appendStamped("events", 1, Array(0L), key = "k1", value = "events-1-0")
    appendStamped("orders", 0, Array(1L), key = "k2", value = "orders-1")
    appendStamped("orders", 1, Array(0L), key = "k3", value = "orders-1-0")
    appendStamped("events", 1, Array(1L), key = "k4", value = "events-1-1")

    val tp = new TopicPartition(backingTopic, 0)
    when(logManager.getLog(tp, false)).thenReturn(Some(unifiedLog))

    new BackingLogScanRecovery(kernel, logManager).run()

    // After rebuild, every declared logical partition that had at least one record on the
    // backing log must have its sidecar present and its tracker seeded to nextLogicalOffset
    // = number-of-stamped-records.
    val remaining = kernel.partitionsWithoutSidecar()
    // orders/0 had 2 records, orders/1 had 1, events/1 had 2. events/0 had none → its sidecar
    // is still missing because the kernel does not create empty sidecars.
    assertEquals(1, remaining.size,
      s"only events/0 (never produced) should remain without a sidecar; got $remaining")
    assertEquals("events", remaining.get(0).logicalTopic())
    assertEquals(0, remaining.get(0).logicalPartition())

    assertEquals(2L, kernel.nextLogicalOffset("orders", 0))
    assertEquals(1L, kernel.nextLogicalOffset("orders", 1))
    assertEquals(2L, kernel.nextLogicalOffset("events", 1))
    // events/0 was never produced → tracker default.
    assertEquals(0L, kernel.nextLogicalOffset("events", 0))
  }

  @Test
  def runIgnoresRecordsOutsideRecoveryNeedSet(): Unit = {
    // The needs-set is computed from partitionsWithoutSidecar(). After the first run, the
    // orders/0 sidecar exists, so a second run must NOT touch it even though the backing log
    // still contains its records. This guards the contract that already-recovered sidecars
    // are not clobbered by a stale scan.
    val backingTopic = "backing-shared"
    kernel.declare(new LogicalTopicDescriptor("orders", 1, backingTopic, 1))
    appendStamped("orders", 0, Array(0L), key = "k0", value = "v0")
    appendStamped("orders", 0, Array(1L), key = "k1", value = "v1")

    val tp = new TopicPartition(backingTopic, 0)
    when(logManager.getLog(tp, false)).thenReturn(Some(unifiedLog))

    new BackingLogScanRecovery(kernel, logManager).run()
    assertEquals(2L, kernel.nextLogicalOffset("orders", 0))
    assertEquals(0, kernel.partitionsWithoutSidecar().size)

    // Second run: needs-set is empty → driver must not touch the kernel state. We verify by
    // confirming nextLogicalOffset is unchanged (and no exception is thrown).
    new BackingLogScanRecovery(kernel, logManager).run()
    assertEquals(2L, kernel.nextLogicalOffset("orders", 0))
  }

  @Test
  def runHandlesEmptyBackingLog(): Unit = {
    // An empty backing partition (declared but never produced) must not throw.
    val backingTopic = "backing-empty"
    kernel.declare(new LogicalTopicDescriptor("orders", 1, backingTopic, 1))
    val tp = new TopicPartition(backingTopic, 0)
    when(logManager.getLog(tp, false)).thenReturn(Some(unifiedLog))

    new BackingLogScanRecovery(kernel, logManager).run()

    // No sidecar, tracker default. Both must hold.
    assertEquals(1, kernel.partitionsWithoutSidecar().size)
    assertEquals(0L, kernel.nextLogicalOffset("orders", 0))
  }

  @Test
  def scanHonoursThreadInterruptAtPageBoundary(): Unit = {
    // B.6 cooperative-cancellation contract. A scan runs on the recovery executor; broker
    // shutdown calls executor.shutdownNow which sets Thread.interrupt on the worker. The
    // iterator MUST notice this at the next page-read boundary and abort with
    // InterruptedScanException — without the check, a long backing log would keep paging
    // for arbitrarily long and stall shutdown.
    //
    // We exercise the path through BackingLogScanRecovery.run() running on a side thread so
    // the test thread can interrupt it from outside. Using the public driver (rather than
    // poking the file-private BackingLogPageIterator directly) verifies the contract end-to-
    // end: page-iterator interrupt-check propagates through kernel.recoverFromBackingScan
    // back to the caller as a plain throw.
    val backingTopic = "backing-interrupt"
    kernel.declare(new LogicalTopicDescriptor("orders", 1, backingTopic, 1))
    // Enough records to span multiple page reads of the iterator's 1 MiB buffer at the
    // default broker config — but in this test the read buffer is set to 16 bytes so even
    // a handful of records forces multiple refill loops, giving the interrupt a place to land.
    for (i <- 0 until 16) {
      appendStamped("orders", 0, Array(i.toLong), key = s"k$i", value = s"v$i")
    }
    val tp = new TopicPartition(backingTopic, 0)
    when(logManager.getLog(tp, false)).thenReturn(Some(unifiedLog))

    val started = new java.util.concurrent.CountDownLatch(1)
    val raised = new java.util.concurrent.atomic.AtomicReference[Throwable](null)
    val scanThread = new Thread(() => {
      started.countDown()
      try {
        // Pre-interrupt before starting the scan: the first refill() iteration sees the flag
        // and throws. Deterministic enough for the test without coordinating "exactly at
        // refill #N" — the contract is "honoured at the NEXT page boundary".
        Thread.currentThread().interrupt()
        new BackingLogScanRecovery(kernel, logManager, readBufferBytes = 16).run()
      } catch {
        case t: Throwable => raised.set(t)
      }
    }, "scan-interrupt-test")
    scanThread.start()
    assertTrue(started.await(5, java.util.concurrent.TimeUnit.SECONDS), "scan thread must start")
    scanThread.join(5_000L)
    assertFalse(scanThread.isAlive, "scan thread must terminate after interrupt")
    val t = raised.get()
    assertNotNull(t, "scan must propagate an exception when interrupted, not swallow it")
    assertTrue(t.isInstanceOf[InterruptedScanException],
      s"expected InterruptedScanException, got ${t.getClass.getSimpleName}: ${t.getMessage}")
  }

  @Test
  def runIgnoresUnstampedRecordsOnBackingLog(): Unit = {
    // A backing partition could in principle contain pre-feature records or records produced
    // by a path that bypassed the stamper. The scan must skip them silently rather than crash.
    unifiedLog.appendAsLeader(MemoryRecords.withRecords(Compression.NONE,
      new SimpleRecord("u".getBytes, "unstamped".getBytes)), 0)
    appendStamped("orders", 0, Array(0L), key = "k", value = "good")

    val backingTopic = "backing-mix"
    kernel.declare(new LogicalTopicDescriptor("orders", 1, backingTopic, 1))
    val tp = new TopicPartition(backingTopic, 0)
    when(logManager.getLog(tp, false)).thenReturn(Some(unifiedLog))

    new BackingLogScanRecovery(kernel, logManager).run()

    assertEquals(1L, kernel.nextLogicalOffset("orders", 0),
      "unstamped record must be skipped; the one good record must seed nextLogicalOffset = 1")
  }

  // ---- helpers ----

  private def appendStamped(logicalTopic: String, logicalPartition: Int, logicalOffsets: Array[Long],
                            key: String, value: String): Unit = {
    val src = MemoryRecords.withRecords(Compression.NONE,
      new SimpleRecord(time.milliseconds(), key.getBytes, value.getBytes))
    val stamped = LogicalProduceStamper.stamp(src, logicalTopic, logicalPartition, logicalOffsets)
    unifiedLog.appendAsLeader(stamped, 0)
  }
}
