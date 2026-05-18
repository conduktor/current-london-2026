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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.storage.internals.concentration.ConcentrationKernel
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._

import java.io.File
import java.nio.file.Files

class KafkaConcentrationPartitionListenerTest {

  private val backingTp = new TopicPartition("orders-backing", 7)

  @Test
  def onFailedInvalidatesIdempotentCacheForBackingTopic(): Unit = {
    val kernel = mock(classOf[ConcentrationKernel])
    val listener = new KafkaConcentrationPartitionListener(backingTp, kernel)

    listener.onFailed(backingTp)

    verify(kernel).invalidateIdempotentCacheForBacking(backingTp.topic)
    verifyNoMoreInteractions(kernel)
  }

  @Test
  def onDeletedInvalidatesIdempotentCacheForBackingTopic(): Unit = {
    val kernel = mock(classOf[ConcentrationKernel])
    val listener = new KafkaConcentrationPartitionListener(backingTp, kernel)

    listener.onDeleted(backingTp)

    verify(kernel).invalidateIdempotentCacheForBacking(backingTp.topic)
    verifyNoMoreInteractions(kernel)
  }

  @Test
  def onBecomingFollowerInvalidatesIdempotentCacheForBackingTopic(): Unit = {
    val kernel = mock(classOf[ConcentrationKernel])
    val listener = new KafkaConcentrationPartitionListener(backingTp, kernel)

    listener.onBecomingFollower(backingTp)

    verify(kernel).invalidateIdempotentCacheForBacking(backingTp.topic)
    verifyNoMoreInteractions(kernel)
  }

  @Test
  def onHighWatermarkUpdatedIsNotForwardedToTheKernel(): Unit = {
    // The kernel does not consume HWM updates today. Forwarding them would be a wasted
    // call per replicated batch on every backing partition the broker leads — make sure
    // we don't accidentally regress into doing that.
    val kernel = mock(classOf[ConcentrationKernel])
    val listener = new KafkaConcentrationPartitionListener(backingTp, kernel)

    listener.onHighWatermarkUpdated(backingTp, 12345L)

    verifyNoInteractions(kernel)
  }

  @Test
  def kernelExceptionIsSwallowedSoReplicationStateIsNotCorrupted(): Unit = {
    // The PartitionListener contract states callbacks run on the thread that triggers
    // the transition AND that locks may be held during execution. A kernel-side fault
    // must not propagate up into Partition.markOffline / delete / makeFollower.
    val kernel = mock(classOf[ConcentrationKernel])
    doThrow(new RuntimeException("simulated kernel fault"))
      .when(kernel).invalidateIdempotentCacheForBacking(any[String])
    val listener = new KafkaConcentrationPartitionListener(backingTp, kernel)

    // None of these should throw.
    listener.onFailed(backingTp)
    listener.onDeleted(backingTp)
    listener.onBecomingFollower(backingTp)

    verify(kernel, times(3)).invalidateIdempotentCacheForBacking(backingTp.topic)
  }

  @Test
  def equalsAndHashCodeAreScopedByTopicPartitionAndKernelIdentity(): Unit = {
    val kernel1 = mock(classOf[ConcentrationKernel])
    val kernel2 = mock(classOf[ConcentrationKernel])
    val a = new KafkaConcentrationPartitionListener(backingTp, kernel1)
    val aDup = new KafkaConcentrationPartitionListener(backingTp, kernel1)
    val otherPartition = new KafkaConcentrationPartitionListener(new TopicPartition(backingTp.topic, 9), kernel1)
    val otherKernel = new KafkaConcentrationPartitionListener(backingTp, kernel2)

    assertEquals(a, aDup, "Same (tp, kernel-identity) must be equal — required for CopyOnWriteArraySet idempotence")
    assertEquals(a.hashCode, aDup.hashCode)
    assertNotEquals(a, otherPartition, "Different TopicPartition must not be equal")
    assertNotEquals(a, otherKernel, "Different kernel identity must not be equal")
  }

  @Test
  def repeatedRegistrationOnRealKernelInstanceDoesNotDuplicate(): Unit = {
    // End-to-end sanity: under repeated leader-epoch bumps, the same listener instance
    // re-added via Partition.maybeAddListener must collapse to a single registration.
    // The CopyOnWriteArraySet behind Partition.listeners uses equals; we verify the
    // listener's equals/hashCode lets the set treat dupes as identical.
    val sidecarDir = Files.createTempDirectory("concentration-listener-test").toFile
    try {
      val kernel = new ConcentrationKernel(new File(sidecarDir, "_sidecars"))
      val l1 = new KafkaConcentrationPartitionListener(backingTp, kernel)
      val l2 = new KafkaConcentrationPartitionListener(backingTp, kernel)

      val set = new java.util.concurrent.CopyOnWriteArraySet[kafka.cluster.PartitionListener]()
      assertTrue(set.add(l1))
      assertFalse(set.add(l2), "Duplicate by equals/hashCode must be rejected by the listener set")
      assertEquals(1, set.size)

      kernel.close()
    } finally {
      deleteRecursively(sidecarDir)
    }
  }

  private def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles).foreach(_.foreach(deleteRecursively))
    f.delete()
  }
}
