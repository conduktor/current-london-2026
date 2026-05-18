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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.server.rules.{GovernanceLoader, GovernanceTopic, RuleDecision, RuleEngine}
import org.apache.kafka.server.storage.log.FetchIsolation
import org.apache.kafka.storage.internals.log.{FetchDataInfo, LogOffsetMetadata}

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{doAnswer, doThrow, mock, when}

import java.nio.charset.StandardCharsets
import java.util.Collections

/**
 * Unit tests for [[BrokerGovernanceBootstrap]] — the direct-log-read drain
 * that satisfies PROMPT.md's "drain all rules before accepting client
 * connections" acceptance criterion.
 *
 * <p>The tests mock [[ReplicaManager.getLog]] to return a mocked
 * [[UnifiedLog]] populated with real [[MemoryRecords]] — the only behaviour
 * the bootstrap depends on is `logStartOffset`, `logEndOffset` and `read()`,
 * so a small Mockito stub is enough. No broker startup, no SocketServer.
 */
class BrokerGovernanceBootstrapTest {

  private val tp = new TopicPartition(GovernanceTopic.NAME, 0)

  private def envelope(when: String, apiKey: ApiKeys, errorCode: Int): Array[Byte] = {
    val json = "{\"apiKeys\":[\"" + apiKey.name() + "\"],\"action\":\"DENY\"," +
      "\"when\":\"" + when + "\",\"errorCode\":" + errorCode + "}"
    json.getBytes(StandardCharsets.UTF_8)
  }

  private def recordsAt(startOffset: Long, records: SimpleRecord*): FetchDataInfo = {
    val mem = MemoryRecords.withRecords(startOffset, Compression.NONE, records: _*)
    new FetchDataInfo(new LogOffsetMetadata(startOffset), mem)
  }

  @Test
  def drainOnceIsNoOpWhenLogDoesNotExist(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(None)

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    val n = boot.drainOnce()

    assertEquals(0L, n, "missing log should drain nothing")
    assertEquals(0, engine.active().size(), "no rules should be installed")
  }

  @Test
  def drainOnceReadsAllRecordsAndInstallsRules(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))
    when(log.logStartOffset).thenReturn(0L)
    when(log.logEndOffset).thenReturn(3L)
    when(log.read(0L, 1024 * 1024, FetchIsolation.LOG_END, true)).thenReturn(
      recordsAt(0L,
        new SimpleRecord("r1".getBytes(StandardCharsets.UTF_8), envelope("true", ApiKeys.METADATA, 7)),
        new SimpleRecord("r2".getBytes(StandardCharsets.UTF_8), envelope("true", ApiKeys.FETCH, 11)),
        new SimpleRecord("r3".getBytes(StandardCharsets.UTF_8), envelope("true", ApiKeys.CREATE_TOPICS, 13))))

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    val n = boot.drainOnce()

    assertEquals(3L, n, "all three records should be replayed")
    assertEquals(3, engine.active().size())
    assertTrue(engine.evaluate(ApiKeys.METADATA, "c", () => Collections.emptyMap()).denied)
    assertTrue(engine.evaluate(ApiKeys.FETCH, "c", () => Collections.emptyMap()).denied)
    assertTrue(engine.evaluate(ApiKeys.CREATE_TOPICS, "c", () => Collections.emptyMap()).denied)
  }

  @Test
  def drainOnceAppliesTombstonesAsRemovals(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))
    when(log.logStartOffset).thenReturn(0L)
    when(log.logEndOffset).thenReturn(2L)
    when(log.read(0L, 1024 * 1024, FetchIsolation.LOG_END, true)).thenReturn(
      recordsAt(0L,
        new SimpleRecord("r1".getBytes(StandardCharsets.UTF_8), envelope("true", ApiKeys.METADATA, 7)),
        // tombstone (null value) — should remove r1
        new SimpleRecord("r1".getBytes(StandardCharsets.UTF_8), null)))

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    val n = boot.drainOnce()

    assertEquals(2L, n, "both records should be replayed including the tombstone")
    assertSame(RuleDecision.ALLOW,
      engine.evaluate(ApiKeys.METADATA, "c", () => Collections.emptyMap()),
      "tombstone must have removed the rule")
  }

  @Test
  def drainOnceSkipsAlreadyReplayedRecordsOnSecondCall(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))
    when(log.logStartOffset).thenReturn(0L)

    // First call: log end offset is 1, one record present.
    when(log.logEndOffset).thenReturn(1L)
    when(log.read(0L, 1024 * 1024, FetchIsolation.LOG_END, true)).thenReturn(
      recordsAt(0L,
        new SimpleRecord("r1".getBytes(StandardCharsets.UTF_8), envelope("true", ApiKeys.METADATA, 7))))

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    val first = boot.drainOnce()
    assertEquals(1L, first)
    assertTrue(engine.evaluate(ApiKeys.METADATA, "c", () => Collections.emptyMap()).denied)

    // Second call: nothing new, drainOnce should not re-read the same record.
    val second = boot.drainOnce()
    assertEquals(0L, second, "second drain must not double-count records already replayed")
    assertEquals(1, engine.active().size(), "rule set unchanged")
  }

  @Test
  def drainOncePoisonedRecordIsSkippedAndDrainProceeds(): Unit = {
    // If loader.apply throws (e.g. an unexpected RuntimeException slips past
    // the loader's own envelope catch, or the record extraction itself blows
    // up), the bootstrap MUST skip the bad record and continue. Otherwise
    // nextOffset stalls and every subsequent drainOnce re-reads — and re-
    // throws on — the same poison forever.
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    val spyLoader = mock(classOf[GovernanceLoader])
    // Default to a no-op (instead of calling-real-method, which would NPE
    // because the spy is not constructed with a backing engine).
    doAnswer(_ => null).when(spyLoader).apply(anyString(), any())
    doAnswer(_ => null).when(spyLoader).commit()
    // Second record poisons the loader; first and third are fine.
    doThrow(new RuntimeException("poison"))
      .when(spyLoader).apply(org.mockito.ArgumentMatchers.eq("r2"), any())

    when(rm.getLog(tp)).thenReturn(Some(log))
    when(log.logStartOffset).thenReturn(0L)
    when(log.logEndOffset).thenReturn(3L)
    when(log.read(0L, 1024 * 1024, FetchIsolation.LOG_END, true)).thenReturn(
      recordsAt(0L,
        new SimpleRecord("r1".getBytes(StandardCharsets.UTF_8), envelope("true", ApiKeys.METADATA, 7)),
        new SimpleRecord("r2".getBytes(StandardCharsets.UTF_8), envelope("true", ApiKeys.FETCH, 11)),
        new SimpleRecord("r3".getBytes(StandardCharsets.UTF_8), envelope("true", ApiKeys.CREATE_TOPICS, 13))))

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp, spyLoader)
    val n = boot.drainOnce()

    // All three records were *visited* (one threw, two applied) — the loop
    // does not bail on the throw; replayed count includes the poison.
    assertEquals(3L, n, "the poisoned record must not stall replay")

    // A second drainOnce on the same log MUST be a no-op: the cursor advanced
    // past the poison, so the bug doesn't loop forever.
    when(log.logEndOffset).thenReturn(3L)
    val second = boot.drainOnce()
    assertEquals(0L, second, "cursor must have advanced past the poison")
  }

  @Test
  def drainOnceWhenNothingNewStillProducesAValidRuleSet(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))
    when(log.logStartOffset).thenReturn(0L)
    when(log.logEndOffset).thenReturn(0L)

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    val n = boot.drainOnce()

    assertEquals(0L, n)
    assertNotNull(engine.active(), "engine must have a non-null active RuleSet")
    assertEquals(0, engine.active().size())
  }
}
