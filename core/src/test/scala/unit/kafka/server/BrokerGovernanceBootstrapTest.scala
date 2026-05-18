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

    // Default probe says TopicAbsent — no rules to enforce, empty RuleSet is correct.
    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    val n = boot.drainOnce()

    assertEquals(0L, n, "missing log should drain nothing")
    assertEquals(0, engine.active().size(), "no rules should be installed")
  }

  @Test
  def drainOnceFailsClosedWhenTopicExistsButBrokerIsNotAReplica(): Unit = {
    // Codex P0 #1: when __governance exists on the cluster but this broker is
    // not a replica of partition 0, drainOnce MUST throw under the strict
    // default. Empty-RuleSet-fallthrough would silently fail-open every rule
    // for clients hitting this broker.
    val rm = mock(classOf[ReplicaManager])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(None)

    val probe: () => LocalReplicaStatus = () => LocalReplicaStatus.NonReplica
    val boot = new BrokerGovernanceBootstrap(rm, engine, tp,
      injectedLoader = null,
      localReplicaStatus = probe,
      requireLocalReplica = true)

    val ex = assertThrows(classOf[IllegalStateException], () => boot.drainOnce())
    // Error message must surface both the actionable knobs to the operator:
    // either assign a replica, or set the config to fail-open with a warning.
    val msg = ex.getMessage
    assertTrue(msg.contains("not a replica"),
      s"error must say the broker is not a replica, got: $msg")
    assertTrue(msg.contains("governance.bootstrap.require.local.replica"),
      s"error must name the config knob, got: $msg")
    // No rules were installed — engine stays at the broker's startup-empty state.
    assertEquals(0, engine.active().size())
  }

  @Test
  def drainOnceWarnsAndDoesNotOverwriteActiveWhenNonReplicaAndKnobIsOff(): Unit = {
    // With require.local.replica=false the operator has explicitly opted into
    // fail-open. drainOnce must NOT throw — and must NOT install empty over a
    // prior good RuleSet, which would be a hidden second fail-open vector when
    // a previously-replica broker loses its replica via reassignment. The
    // expected behaviour is "keep the last-known active": at startup that's
    // RuleSet.EMPTY (no commit ever made), at runtime that's whatever the
    // previous drain committed.
    val rm = mock(classOf[ReplicaManager])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(None)

    val probe: () => LocalReplicaStatus = () => LocalReplicaStatus.NonReplica
    val boot = new BrokerGovernanceBootstrap(rm, engine, tp,
      injectedLoader = null,
      localReplicaStatus = probe,
      requireLocalReplica = false)

    val n = boot.drainOnce()
    assertEquals(0L, n)
    assertEquals(0, engine.active().size(),
      "at startup, no-log path leaves engine at RuleSet.EMPTY initial state")
  }

  @Test
  def drainOnceTreatsLocalReplicaWithoutLogAsTransientNotFatal(): Unit = {
    // Metadata says we ARE a replica but ReplicaManager has no log object
    // yet (startup race: log dir not opened, or reassignment in flight). This
    // must NOT abort startup — the periodic re-drain will pick up records
    // once the log opens. Until then, the engine's prior active RuleSet stays
    // in force; at startup that's RuleSet.EMPTY (no commit ever made), and at
    // runtime that's the last-known-good set (verified in a separate test).
    val rm = mock(classOf[ReplicaManager])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(None)

    val probe: () => LocalReplicaStatus = () => LocalReplicaStatus.LocalReplica
    val boot = new BrokerGovernanceBootstrap(rm, engine, tp,
      injectedLoader = null,
      localReplicaStatus = probe,
      requireLocalReplica = true)

    val n = boot.drainOnce()
    assertEquals(0L, n)
    assertEquals(0, engine.active().size())
  }

  @Test
  def drainOnceNoLogBranchesPreserveLastKnownActiveRuleSet(): Unit = {
    // Codex P0 audit regression-lock: when this broker had previously drained
    // a non-empty RuleSet from a healthy log and then loses access to the log
    // (reassignment-away, transient log-dir failure, or metadata flicker), the
    // engine.active() MUST keep enforcing the prior rules — installing an
    // empty RuleSet would silently fail-open every rule until the next
    // successful drain. This test exercises all three reachable no-log
    // branches (TopicAbsent, NonReplica-opt-out, LocalReplica-no-log) and
    // asserts the engine's active() is byte-identical to what was installed
    // before drainOnce ran.
    val cases = Seq(
      ("TopicAbsent", LocalReplicaStatus.TopicAbsent, true),
      ("NonReplica-opt-out", LocalReplicaStatus.NonReplica, false),
      ("LocalReplica-no-log", LocalReplicaStatus.LocalReplica, true)
    )

    for ((label, status, requireReplica) <- cases) {
      val rm = mock(classOf[ReplicaManager])
      val engine = new RuleEngine()
      when(rm.getLog(tp)).thenReturn(None)

      // Pre-install a non-empty RuleSet directly, simulating "we already
      // drained the log successfully at some point in the past".
      val r = org.apache.kafka.server.rules.json.RuleJsonCodec.decode(
        "preserved", envelope("true", ApiKeys.METADATA, 42))
      val builder = new org.apache.kafka.server.rules.RuleSetBuilder()
      builder.put(r)
      val preInstalled = builder.build()
      engine.install(preInstalled)
      assertSame(preInstalled, engine.active(),
        s"$label: precondition — non-empty RuleSet must be installed before drain")

      val probe: () => LocalReplicaStatus = () => status
      val boot = new BrokerGovernanceBootstrap(rm, engine, tp,
        injectedLoader = null,
        localReplicaStatus = probe,
        requireLocalReplica = requireReplica)

      val n = boot.drainOnce()
      assertEquals(0L, n, s"$label: no-log branch must report 0 replayed")
      // The exact same RuleSet reference must still be active — drainOnce
      // must not have called loader.commit(), which would install a new
      // (empty) snapshot from the loader's working state.
      assertSame(preInstalled, engine.active(),
        s"$label: active RuleSet must be preserved — a no-log drain must " +
          s"NEVER overwrite the engine's prior active with empty")
      // The pre-installed deny rule is still enforced end-to-end.
      assertTrue(engine.evaluate(ApiKeys.METADATA, "c", false,
        () => Collections.emptyMap()).denied,
        s"$label: prior rule must still deny after no-log drain")
    }
  }

  @Test
  def drainOnceReadsAllRecordsAndInstallsRules(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))
    when(log.logStartOffset).thenReturn(0L)
    // HW is the drain bound; on a happy-path single-broker test it matches LEO.
    when(log.highWatermark).thenReturn(3L)
    when(log.read(0L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true)).thenReturn(
      recordsAt(0L,
        new SimpleRecord("r1".getBytes(StandardCharsets.UTF_8), envelope("true", ApiKeys.METADATA, 7)),
        new SimpleRecord("r2".getBytes(StandardCharsets.UTF_8), envelope("true", ApiKeys.FETCH, 11)),
        new SimpleRecord("r3".getBytes(StandardCharsets.UTF_8), envelope("true", ApiKeys.CREATE_TOPICS, 13))))

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    val n = boot.drainOnce()

    assertEquals(3L, n, "all three records should be replayed")
    assertEquals(3, engine.active().size())
    assertTrue(engine.evaluate(ApiKeys.METADATA, "c", false, () => Collections.emptyMap()).denied)
    assertTrue(engine.evaluate(ApiKeys.FETCH, "c", false, () => Collections.emptyMap()).denied)
    assertTrue(engine.evaluate(ApiKeys.CREATE_TOPICS, "c", false, () => Collections.emptyMap()).denied)
  }

  @Test
  def drainOnceAppliesTombstonesAsRemovals(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))
    when(log.logStartOffset).thenReturn(0L)
    when(log.highWatermark).thenReturn(2L)
    when(log.read(0L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true)).thenReturn(
      recordsAt(0L,
        new SimpleRecord("r1".getBytes(StandardCharsets.UTF_8), envelope("true", ApiKeys.METADATA, 7)),
        // tombstone (null value) — should remove r1
        new SimpleRecord("r1".getBytes(StandardCharsets.UTF_8), null)))

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    val n = boot.drainOnce()

    assertEquals(2L, n, "both records should be replayed including the tombstone")
    assertSame(RuleDecision.ALLOW,
      engine.evaluate(ApiKeys.METADATA, "c", false, () => Collections.emptyMap()),
      "tombstone must have removed the rule")
  }

  @Test
  def drainOnceSkipsAlreadyReplayedRecordsOnSecondCall(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))
    when(log.logStartOffset).thenReturn(0L)

    // First call: HW is at 1, one record committed.
    when(log.highWatermark).thenReturn(1L)
    when(log.read(0L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true)).thenReturn(
      recordsAt(0L,
        new SimpleRecord("r1".getBytes(StandardCharsets.UTF_8), envelope("true", ApiKeys.METADATA, 7))))

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    val first = boot.drainOnce()
    assertEquals(1L, first)
    assertTrue(engine.evaluate(ApiKeys.METADATA, "c", false, () => Collections.emptyMap()).denied)

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
    when(log.highWatermark).thenReturn(3L)
    when(log.read(0L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true)).thenReturn(
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
    val second = boot.drainOnce()
    assertEquals(0L, second, "cursor must have advanced past the poison")
  }

  @Test
  def drainOncePropagatesHardLogReadFailureSoBrokerCanFailClosed(): Unit = {
    // The bootstrap drain runs before SocketServer.enableRequestProcessing and
    // is the security-critical entry point. If the broker IS a replica of
    // __governance but the local log is unreadable (corrupt segment, disk I/O
    // error, etc.), drainOnce MUST propagate — otherwise BrokerServer would
    // swallow the failure and open the request socket with an empty RuleSet
    // while real DENY rules exist on the topic. That's silent fail-open.
    //
    // The "log does not exist" path (None) is different: there are no rules
    // to enforce on this broker, and an empty RuleSet is the correct state.
    // Only the unreadable-replica path should throw.
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))
    when(log.logStartOffset).thenReturn(0L)
    when(log.highWatermark).thenReturn(5L)
    when(log.read(0L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true))
      .thenThrow(new org.apache.kafka.common.errors.CorruptRecordException("simulated corrupt segment"))

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    val ex = assertThrows(classOf[org.apache.kafka.common.errors.CorruptRecordException],
      () => boot.drainOnce())
    assertEquals("simulated corrupt segment", ex.getMessage)
    assertEquals(0, engine.active().size(),
      "no rules should be installed when read fails — engine stays at the empty state " +
        "the broker started with, and the broker is expected to refuse to open " +
        "request processing until the operator resolves the disk fault")
  }

  @Test
  def drainOnceWhenNothingNewStillProducesAValidRuleSet(): Unit = {
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))
    when(log.logStartOffset).thenReturn(0L)
    when(log.highWatermark).thenReturn(0L)

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    val n = boot.drainOnce()

    assertEquals(0L, n)
    assertNotNull(engine.active(), "engine must have a non-null active RuleSet")
    assertEquals(0, engine.active().size())
  }

  @Test
  def drainOnceStopsAtHighWatermarkEvenWhenLogEndOffsetIsAhead(): Unit = {
    // Regression-locking test for the LOG_END → HIGH_WATERMARK change.
    //
    // Scenario: this broker is a replica of __governance whose local log has
    // advanced LEO past the committed cluster-wide HW (e.g. it just replicated
    // records from the leader but the cluster hasn't yet acknowledged them as
    // committed). Reading past HW would let this broker enforce rules that
    // could still be lost on a leader-election truncation — i.e. enforce a
    // rule that no other broker enforces. The drain must stop at HW.
    //
    // We seed LEO = 3 and HW = 1. Only the first record must reach the engine;
    // records at offsets 1 and 2 (past HW) must be ignored. Crucially, the
    // cursor must also stop at HW so the next drainOnce — after HW advances —
    // picks them up.
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))
    when(log.logStartOffset).thenReturn(0L)
    when(log.logEndOffset).thenReturn(3L) // leader-end position, stale here
    when(log.highWatermark).thenReturn(1L)
    when(log.read(0L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true)).thenReturn(
      recordsAt(0L,
        new SimpleRecord("r1".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.METADATA, 7))))

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    val first = boot.drainOnce()
    assertEquals(1L, first, "only committed (HW) records should be replayed")
    assertEquals(1, engine.active().size())
    assertTrue(engine.evaluate(ApiKeys.METADATA, "c", false, () => Collections.emptyMap()).denied)

    // HW catches up to LEO; the deferred records become committed and the
    // next drain MUST pick them up — the previous drain stopped at HW=1, so
    // its cursor is 1, not LEO=3. If we had used LOG_END semantics, the
    // cursor would have already been set to 3 and these would be lost forever.
    when(log.highWatermark).thenReturn(3L)
    when(log.read(1L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true)).thenReturn(
      recordsAt(1L,
        new SimpleRecord("r2".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.FETCH, 11)),
        new SimpleRecord("r3".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.CREATE_TOPICS, 13))))

    val second = boot.drainOnce()
    assertEquals(2L, second, "deferred records become enforceable once HW advances")
    assertEquals(3, engine.active().size())
    assertTrue(engine.evaluate(ApiKeys.FETCH, "c", false, () => Collections.emptyMap()).denied)
    assertTrue(engine.evaluate(ApiKeys.CREATE_TOPICS, "c", false, () => Collections.emptyMap()).denied)
  }
}
