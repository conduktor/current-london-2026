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
import java.util.concurrent.atomic.AtomicLong

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
  def drainStartupBoundedWaitsForLocalLogToBecomeAvailable(): Unit = {
    // Codex deep-audit P0: drainOnce returning 0L for LocalReplica + getLog==None
    // is the correct "fail-stale-not-empty" behaviour AFTER a successful prior
    // install (engine.active() carries the last-known-good rules forward through
    // a transient log-dir glitch). At FIRST STARTUP, however, engine.active() is
    // RuleSet.EMPTY — returning 0L would let BrokerServer.enableRequestProcessing
    // open client traffic before any DENY rules on the topic are enforced. The
    // startup-only `drainStartup` method must bounded-wait for the log to become
    // available, then drain. This test exercises the "log appears mid-wait" case.
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()

    // First two getLog() calls return None; the third returns Some(log) — the
    // bounded-wait must keep polling and then succeed once the log appears.
    val noneAnswer = org.mockito.Mockito.doReturn(None, Seq.empty: _*)
      .doReturn(None, Seq.empty: _*)
      .doReturn(Some(log), Seq.empty: _*)
    noneAnswer.when(rm).getLog(tp)
    when(log.logStartOffset).thenReturn(0L)
    when(log.highWatermark).thenReturn(1L)
    when(log.read(0L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true)).thenReturn(
      recordsAt(0L,
        new SimpleRecord("r1".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.METADATA, 7))))

    val probe: () => LocalReplicaStatus = () => LocalReplicaStatus.LocalReplica
    val boot = new BrokerGovernanceBootstrap(rm, engine, tp,
      injectedLoader = null,
      localReplicaStatus = probe,
      requireLocalReplica = true)

    val n = boot.drainStartup(deadlineMs = 5000L, pollIntervalMs = 1L)
    assertEquals(1L, n, "the record must be drained once the log appears")
    assertEquals(1, engine.active().size())
    assertTrue(engine.evaluate(ApiKeys.METADATA, "c", false,
      () => Collections.emptyMap()).denied)
  }

  @Test
  def drainStartupFailsClosedIfLocalReplicaButLogNeverAppears(): Unit = {
    // Codex deep-audit P0 fail-closed half: if the deadline elapses while we
    // are a LocalReplica but the log is still unavailable, drainStartup MUST
    // throw so BrokerServer aborts startup before enableRequestProcessing
    // opens client traffic with an empty RuleSet. The error must name the
    // partition and explain the operator's recovery path.
    val rm = mock(classOf[ReplicaManager])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(None)

    val probe: () => LocalReplicaStatus = () => LocalReplicaStatus.LocalReplica
    val boot = new BrokerGovernanceBootstrap(rm, engine, tp,
      injectedLoader = null,
      localReplicaStatus = probe,
      requireLocalReplica = true)

    val ex = assertThrows(classOf[IllegalStateException],
      () => boot.drainStartup(deadlineMs = 50L, pollIntervalMs = 1L))
    val msg = ex.getMessage
    assertTrue(msg.contains(tp.toString),
      s"error must name the partition, got: $msg")
    assertTrue(msg.contains("not yet available") || msg.contains("log"),
      s"error must mention the log unavailability, got: $msg")
    assertEquals(0, engine.active().size())
  }

  @Test
  def drainStartupDoesNotWaitWhenTopicIsAbsent(): Unit = {
    // TopicAbsent at startup is not an error — there are no rules to enforce.
    // drainStartup must return immediately (no busy-wait against a deadline)
    // so broker startup is not artificially delayed.
    val rm = mock(classOf[ReplicaManager])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(None)

    val probe: () => LocalReplicaStatus = () => LocalReplicaStatus.TopicAbsent
    val boot = new BrokerGovernanceBootstrap(rm, engine, tp,
      injectedLoader = null,
      localReplicaStatus = probe,
      requireLocalReplica = true)

    val started = System.nanoTime()
    val n = boot.drainStartup(deadlineMs = 60_000L, pollIntervalMs = 100L)
    val elapsedMs = (System.nanoTime() - started) / 1_000_000L
    assertEquals(0L, n)
    assertEquals(0, engine.active().size())
    assertTrue(elapsedMs < 5_000L,
      s"TopicAbsent must short-circuit drainStartup, but it took ${elapsedMs}ms")
  }

  @Test
  def drainStartupForwardsToDrainOnceForNonReplicaStrictMode(): Unit = {
    // NonReplica + requireLocalReplica=true at startup is a hard error and
    // drainStartup must surface it as IllegalStateException with the same
    // operator-actionable message drainOnce produces. We deliberately do NOT
    // duplicate the error-message text in tests — that would couple them to
    // wording. Instead we assert the type + the named config knob.
    val rm = mock(classOf[ReplicaManager])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(None)

    val probe: () => LocalReplicaStatus = () => LocalReplicaStatus.NonReplica
    val boot = new BrokerGovernanceBootstrap(rm, engine, tp,
      injectedLoader = null,
      localReplicaStatus = probe,
      requireLocalReplica = true)

    val ex = assertThrows(classOf[IllegalStateException],
      () => boot.drainStartup(deadlineMs = 50L, pollIntervalMs = 1L))
    assertTrue(ex.getMessage.contains("governance.bootstrap.require.local.replica"),
      s"error must name the config knob: ${ex.getMessage}")
  }

  @Test
  def replayDefensiveEmptyReadDoesNotSilentlySkipUnreadRecords(): Unit = {
    // Codex deep-audit P1: when log.read returns 0 bytes mid-replay (e.g. a
    // transient pager glitch), replay() returns early. The PRE-fix caller then
    // called loader.commit() AND set nextOffset = endOffset, which silently
    // advanced past records we never read. This test reproduces that bug by
    // making the FIRST read return zero bytes before any record is processed:
    // the cursor must NOT advance past startOffset, so the next drain picks up
    // exactly the records we missed.
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))
    when(log.logStartOffset).thenReturn(0L)
    when(log.highWatermark).thenReturn(3L)

    // First call returns an empty FetchDataInfo (sizeInBytes == 0). The
    // pre-fix code committed and jumped to endOffset; the post-fix code must
    // leave the cursor at 0 so the next drain can re-read.
    val empty = new FetchDataInfo(new LogOffsetMetadata(0L), MemoryRecords.EMPTY)
    when(log.read(0L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true))
      .thenReturn(empty)
      .thenReturn(recordsAt(0L,
        new SimpleRecord("r1".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.METADATA, 7)),
        new SimpleRecord("r2".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.FETCH, 11)),
        new SimpleRecord("r3".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.CREATE_TOPICS, 13))))

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)

    // First drain: no records replayed, cursor must NOT have advanced.
    val first = boot.drainOnce()
    assertEquals(0L, first, "no records were actually consumed")
    assertEquals(0, engine.active().size(),
      "empty read mid-replay must not install an empty RuleSet — it must keep " +
        "prior active(); at startup that's RuleSet.EMPTY, but here we are " +
        "verifying that NO new commit happened either way")

    // Second drain: log now returns the full three records. They MUST all be
    // visible — proving the cursor stayed at startOffset across the empty read.
    val second = boot.drainOnce()
    assertEquals(3L, second, "all three records become visible on the retry — " +
      "if the cursor had advanced past them, this would be 0")
    assertEquals(3, engine.active().size())
    assertTrue(engine.evaluate(ApiKeys.METADATA, "c", false,
      () => Collections.emptyMap()).denied)
    assertTrue(engine.evaluate(ApiKeys.FETCH, "c", false,
      () => Collections.emptyMap()).denied)
    assertTrue(engine.evaluate(ApiKeys.CREATE_TOPICS, "c", false,
      () => Collections.emptyMap()).denied)
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

  @Test
  def drainStartupWaitsForReplicaCatchupAfterLogOpens(): Unit = {
    // Codex deep-audit P0b: the local log being open is not sufficient. On a
    // freshly-started follower the log directory opens immediately (HW=0)
    // before the replica-fetcher has pulled any records from the leader.
    // drainStartup must wait for ISR membership (or self-leadership) before
    // draining; otherwise the engine commits empty past rules the leader has
    // already committed. This test simulates a follower that catches up
    // after a few polls — drainStartup must keep polling until the probe
    // flips to true, then drain.
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))
    when(log.logStartOffset).thenReturn(0L)
    when(log.highWatermark).thenReturn(1L)
    when(log.read(0L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true)).thenReturn(
      recordsAt(0L,
        new SimpleRecord("r1".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.METADATA, 7))))

    // Probe returns false for the first two polls, then true. This models a
    // follower whose ISR membership lands on the third poll.
    val callCount = new java.util.concurrent.atomic.AtomicInteger(0)
    val caughtUpProbe: () => Boolean = () => callCount.incrementAndGet() >= 3

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp,
      injectedLoader = null,
      localReplicaStatus = () => LocalReplicaStatus.LocalReplica,
      requireLocalReplica = true,
      caughtUpProbe = caughtUpProbe)

    val n = boot.drainStartup(deadlineMs = 5000L, pollIntervalMs = 1L)
    assertEquals(1L, n,
      "drain must run once the probe reports caught-up, not before")
    assertEquals(1, engine.active().size())
    // At least three probe invocations (two false + one true) before drain.
    assertTrue(callCount.get() >= 3,
      s"probe must be polled until it returns true, got ${callCount.get()} calls")
  }

  @Test
  def drainStartupFailsClosedIfBrokerNeverCatchesUp(): Unit = {
    // The other half of P0b: if the log is open but the broker never reaches
    // ISR within the deadline, drainStartup MUST throw rather than drain a
    // stale prefix. The error must name the partition and steer the operator
    // toward replica-fetcher / ISR investigation (NOT toward the log-dir
    // recovery path that the log-never-opened case points to — they need
    // different fixes).
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))
    when(log.logStartOffset).thenReturn(0L)
    when(log.highWatermark).thenReturn(0L)

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp,
      injectedLoader = null,
      localReplicaStatus = () => LocalReplicaStatus.LocalReplica,
      requireLocalReplica = true,
      caughtUpProbe = () => false)

    val ex = assertThrows(classOf[IllegalStateException],
      () => boot.drainStartup(deadlineMs = 50L, pollIntervalMs = 1L))
    val msg = ex.getMessage
    assertTrue(msg.contains(tp.toString),
      s"error must name the partition, got: $msg")
    assertTrue(msg.contains("catch up") || msg.contains("ISR"),
      s"error must mention replica catchup / ISR, got: $msg")
    assertEquals(0, engine.active().size(),
      "no rule must be installed when drainStartup throws")
  }

  @Test
  def drainStartupDoesNotWaitForCatchupWhenTopicIsAbsent(): Unit = {
    // The caughtUpProbe is consulted only in the log-open branch. Topic-absent
    // and non-replica branches must short-circuit without ever asking the probe
    // — those are deterministic states where catchup is not even a meaningful
    // concept (no replica fetcher is running for a topic we don't host).
    val rm = mock(classOf[ReplicaManager])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(None)

    val probeCalls = new java.util.concurrent.atomic.AtomicInteger(0)
    val caughtUpProbe: () => Boolean = () => {
      probeCalls.incrementAndGet()
      false
    }

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp,
      injectedLoader = null,
      localReplicaStatus = () => LocalReplicaStatus.TopicAbsent,
      requireLocalReplica = true,
      caughtUpProbe = caughtUpProbe)

    val n = boot.drainStartup(deadlineMs = 5000L, pollIntervalMs = 1L)
    assertEquals(0L, n)
    assertEquals(0, probeCalls.get(),
      "TopicAbsent path must short-circuit without consulting the catchup probe")
  }

  @Test
  def drainStartupLoopsDrainOnceUntilHighWatermarkReached(): Unit = {
    // Codex final-audit P0: drainStartup must fully drain to HW before
    // returning, not bail on a partial replay. replay() defensively returns
    // early when log.read produces zero bytes mid-pass — drainOnce in that
    // case commits and advances nextOffset only to where replay got. Before
    // the Phase-3 loop existed, drainStartup would return at that point and
    // BrokerServer would open client sockets with an unread suffix of the
    // governance log still ahead of nextOffset. PROMPT.md requires
    // "drains all existing rules from the rules topic before accepting
    // client connections", so a partial drain + open-sockets is a real
    // violation, not theoretical.
    //
    // Scenario: HW=3, first log.read returns empty (defensive bail), second
    // returns the three records. drainStartup MUST call drainOnce twice and
    // return 3, not 0.
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))
    when(log.logStartOffset).thenReturn(0L)
    when(log.highWatermark).thenReturn(3L)

    val empty = new FetchDataInfo(new LogOffsetMetadata(0L), MemoryRecords.EMPTY)
    when(log.read(0L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true))
      .thenReturn(empty)
      .thenReturn(recordsAt(0L,
        new SimpleRecord("r1".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.METADATA, 7)),
        new SimpleRecord("r2".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.FETCH, 11)),
        new SimpleRecord("r3".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.CREATE_TOPICS, 13))))

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    val n = boot.drainStartup(deadlineMs = 5_000L, pollIntervalMs = 1L)
    assertEquals(3L, n,
      "drainStartup must loop drainOnce after a defensive empty-read; " +
        "if it returned 0, sockets would open with the governance log " +
        "still un-drained")
    assertEquals(3, engine.active().size())
    assertTrue(engine.evaluate(ApiKeys.METADATA, "c", false,
      () => Collections.emptyMap()).denied)
    assertTrue(engine.evaluate(ApiKeys.FETCH, "c", false,
      () => Collections.emptyMap()).denied)
    assertTrue(engine.evaluate(ApiKeys.CREATE_TOPICS, "c", false,
      () => Collections.emptyMap()).denied)
  }

  @Test
  def drainStartupFailsClosedWhenDeadlineElapsesDuringForwardProgress(): Unit = {
    // Codex round-2 audit P2: the prior implementation only checked the
    // deadline on zero-progress iterations. A drainOnce that makes slow
    // forward progress (eg. 1 record at a time against a pager-thrashed
    // disk, or a tiered storage tier-down stall) would loop indefinitely
    // past the operator-configured deadline because the cursor was
    // advancing on every call.
    //
    // The fix: the deadline is gated at the TOP of every iteration so
    // forward-progress drains are still bounded by the same single number
    // operators set.
    //
    // Scenario: HW=10, drainOnce takes longer than the entire deadline to
    // complete one iteration (simulated by sleeping inside log.read). The
    // first iteration does make forward progress (returns one record) but
    // the second iteration's top-of-loop deadline check trips closed.
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))
    when(log.logStartOffset).thenReturn(0L)
    when(log.highWatermark).thenReturn(10L)

    when(log.read(0L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true))
      .thenAnswer { _ =>
        // Burn the deadline inside the first drainOnce.
        Thread.sleep(120L)
        recordsAt(0L, new SimpleRecord("r1".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.METADATA, 7)))
      }
    // If we ever reach a second drainOnce, this stub catches it.
    when(log.read(1L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true))
      .thenReturn(new FetchDataInfo(new LogOffsetMetadata(1L), MemoryRecords.EMPTY))

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    val ex = assertThrows(classOf[IllegalStateException],
      () => boot.drainStartup(deadlineMs = 50L, pollIntervalMs = 1L))
    val msg = ex.getMessage
    assertTrue(msg.contains("deadline elapsed") || msg.contains("partial drain"),
      s"error must signal fail-closed on deadline despite forward progress, got: $msg")
    // The first drainOnce did advance the cursor by one record, so the engine
    // legitimately reflects that partial drain — the throw exists to abort
    // BrokerServer startup BEFORE any client socket opens, not to roll back
    // the in-engine RuleSet. The acceptance criterion is "no client traffic
    // sees a partially-drained engine", and the throw delivers exactly that.
    assertTrue(engine.active().size() < 10,
      "partial drain expected; the test exists to prove that the deadline " +
        "fires despite forward progress, not that drained rules are unwound")
  }

  @Test
  def drainStartupFailsClosedWhenLocalLogDisappearsBetweenPhase2AndPhase3(): Unit = {
    // Codex round-2 audit P2: the prior drainToHighWatermark resolved the
    // log handle with `replicaManager.getLog(tp).map(_.highWatermark).getOrElse(0L)`.
    // If the log disappeared between Phase 2's check and Phase 3's HW
    // snapshot (eg. a log-dir failure handler running concurrently), the
    // loop condition `nextOffset < 0` was false immediately, the method
    // returned 0 silently, and BrokerServer happily opened sockets with an
    // empty RuleSet despite real rules potentially existing on the leader.
    //
    // The fix: a missing log at Phase 3 is fail-closed. We cannot drain to
    // an HW we cannot determine, and opening sockets without a drain
    // violates the bootstrap acceptance criterion.
    //
    // Scenario: getLog returns Some(log) for Phase 1 + Phase 2 (proves the
    // log was open and the broker is in the ISR), then None for the Phase
    // 3 snapshot. drainStartup must throw, not return 0.
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    // Phase 1 + Phase 2 see the log; Phase 3 sees it gone.
    when(rm.getLog(tp)).thenReturn(Some(log)).thenReturn(None)
    when(log.logStartOffset).thenReturn(0L)
    when(log.highWatermark).thenReturn(3L)

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    val ex = assertThrows(classOf[IllegalStateException],
      () => boot.drainStartup(deadlineMs = 5_000L, pollIntervalMs = 1L))
    val msg = ex.getMessage
    assertTrue(msg.contains("disappeared") || msg.contains("high-watermark"),
      s"error must signal log-disappearance fail-closed, got: $msg")
    assertEquals(0, engine.active().size(),
      "no rule must be installed when the local log disappears at Phase 3")
  }

  @Test
  def drainOnceDetectsTruncationAndResetsLoaderWorkingState(): Unit = {
    // Adversarial M3: on a leader-election with epoch divergence, a follower
    // can truncate its local log back to the new leader's offset, removing
    // records this broker had already replayed. Without a reset, the
    // loader's working set keeps the zombie rules and re-installs them on
    // every subsequent commit — even though the topic has revoked them.
    //
    // Scenario: drain three records, advancing nextOffset to 3. Then the
    // log truncates to HW=1 (records at offsets 1 and 2 are gone — the
    // tombstone for r1 written in the new epoch lives at offset 0 now).
    // The next drainOnce must detect cursor>HW, reset the loader's working
    // state, rewind to log-start, and re-drain. The final RuleSet must
    // reflect only the post-truncation log content (a single rule, r-new).
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))

    // First drain: HW=3, three rules installed.
    when(log.logStartOffset).thenReturn(0L)
    when(log.highWatermark).thenReturn(3L)
    when(log.read(0L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true)).thenReturn(
      recordsAt(0L,
        new SimpleRecord("r1".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.METADATA, 7)),
        new SimpleRecord("r2".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.FETCH, 11)),
        new SimpleRecord("r3".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.CREATE_TOPICS, 13))))

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    val first = boot.drainOnce()
    assertEquals(3L, first)
    assertEquals(3, engine.active().size(), "all three rules installed")

    // Truncation: HW now back to 1, log content reduced to a single fresh
    // rule. The cursor (3) is past HW (1), so drainOnce must reset the
    // loader and re-drain from log-start.
    when(log.highWatermark).thenReturn(1L)
    when(log.read(0L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true)).thenReturn(
      recordsAt(0L,
        new SimpleRecord("r-new".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.LIST_OFFSETS, 23))))

    val second = boot.drainOnce()
    assertEquals(1L, second, "one new record replayed after truncation")
    assertEquals(1, engine.active().size(),
      "post-truncation RuleSet must contain exactly the surviving record — " +
        "the three pre-truncation rules must have been dropped from working state")
    assertTrue(engine.evaluate(ApiKeys.LIST_OFFSETS, "c", false,
      () => Collections.emptyMap()).denied,
      "the surviving rule must be enforced")
    // The pre-truncation rules must NOT still be enforced (would prove the
    // loader's working state was not reset).
    assertSame(RuleDecision.ALLOW,
      engine.evaluate(ApiKeys.METADATA, "c", false, () => Collections.emptyMap()),
      "pre-truncation rule on METADATA must NOT survive — loader reset required")
    assertSame(RuleDecision.ALLOW,
      engine.evaluate(ApiKeys.FETCH, "c", false, () => Collections.emptyMap()),
      "pre-truncation rule on FETCH must NOT survive — loader reset required")
    assertSame(RuleDecision.ALLOW,
      engine.evaluate(ApiKeys.CREATE_TOPICS, "c", false, () => Collections.emptyMap()),
      "pre-truncation rule on CREATE_TOPICS must NOT survive — loader reset required")
  }

  @Test
  def drainOnceTruncationToEmptyDoesNotInstallEmptyOverPreviouslyGoodRuleSet(): Unit = {
    // Defensive fix for the narrow but real concurrency race the adversarial
    // audit flagged: the truncation guard runs loader.reset() and rewinds
    // nextOffset to logStartOffset, but if the post-reset replay range is
    // empty (logStartOffset >= HW — e.g. a leader-election that rolls HW
    // back to logStartOffset, or a topic that was compacted to nothing
    // moments before the truncation observation), the previous code path
    // fell into the "up to date" branch and committed an empty working
    // state — installing RuleSet.EMPTY over the engine's previously-good
    // active().
    //
    // That is exactly the "fail-stale-not-empty" violation called out in
    // BrokerGovernanceBootstrap's own javadoc: a momentary empty view (which
    // a leader-election aftermath qualifies as) must NOT overwrite a
    // known-good security state. Clients should keep seeing the last-known
    // DENY rules until a drain actually observes a record. The first record
    // landing on this partition after the truncation will commit a fresh
    // RuleSet built from the (then non-empty) log.
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))

    // First drain: install two rules (offsets 0, 1).
    when(log.logStartOffset).thenReturn(0L)
    when(log.highWatermark).thenReturn(2L)
    when(log.read(0L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true)).thenReturn(
      recordsAt(0L,
        new SimpleRecord("r1".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.METADATA, 7)),
        new SimpleRecord("r2".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.FETCH, 11))))

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    assertEquals(2L, boot.drainOnce())
    assertEquals(2, engine.active().size(), "baseline: two rules installed")
    val previouslyGood = engine.active()

    // Truncation race: cursor is at 2 (advanced past two records), but a
    // leader-election just rolled HW back to logStartOffset=0. The
    // truncation guard fires (cursor 2 > HW 0), resets the loader, and
    // rewinds nextOffset to 0. The post-reset replay range
    // [logStartOffset=0, HW=0) is empty — there's nothing to read.
    //
    // Pre-fix: this would commit the empty working state, replacing the
    // engine's previously-good active() with RuleSet.EMPTY.
    // Post-fix: the bootstrap returns 0 without committing; engine.active()
    // is unchanged.
    when(log.highWatermark).thenReturn(0L)
    assertEquals(0L, boot.drainOnce(),
      "post-truncation empty replay range must return 0 (no records replayed)")
    assertSame(previouslyGood, engine.active(),
      "engine.active() must NOT change when post-truncation replay range is " +
        "empty — committing empty over previously-good rules is the exact " +
        "fail-stale-not-empty violation this guard exists to prevent")
    // Belt and braces: prove the previously-installed rules are still
    // enforced post-truncation.
    assertTrue(engine.evaluate(ApiKeys.METADATA, "c", false,
      () => Collections.emptyMap()).denied,
      "previously-good METADATA DENY must still be enforced after empty truncation")
    assertTrue(engine.evaluate(ApiKeys.FETCH, "c", false,
      () => Collections.emptyMap()).denied,
      "previously-good FETCH DENY must still be enforced after empty truncation")

    // Recovery: when the topic is repopulated (post leader-election the new
    // leader's HW advances back above logStartOffset), the next drain
    // commits the fresh state and the engine reflects it.
    when(log.highWatermark).thenReturn(1L)
    when(log.read(0L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true)).thenReturn(
      recordsAt(0L,
        new SimpleRecord("r-fresh".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.LIST_OFFSETS, 23))))
    assertEquals(1L, boot.drainOnce())
    assertEquals(1, engine.active().size(),
      "post-recovery commit replaces the held-stale RuleSet with the fresh one")
    assertTrue(engine.evaluate(ApiKeys.LIST_OFFSETS, "c", false,
      () => Collections.emptyMap()).denied)
    // The previously-held-stale rules are gone now — recovery installed a
    // fresh set containing only r-fresh.
    assertSame(RuleDecision.ALLOW,
      engine.evaluate(ApiKeys.METADATA, "c", false, () => Collections.emptyMap()),
      "after recovery commit, previously-held-stale METADATA rule is dropped")
    assertSame(RuleDecision.ALLOW,
      engine.evaluate(ApiKeys.FETCH, "c", false, () => Collections.emptyMap()),
      "after recovery commit, previously-held-stale FETCH rule is dropped")
  }

  @Test
  def drainOnceTruncationToEmptyHoldsStalePersistentlyAcrossDrains(): Unit = {
    // Regression pin for an adversarial-audit finding on the first cut of
    // the "defer commit when post-truncation range is empty" fix: that fix
    // made the "did the truncation guard fire this drain?" flag LOCAL to
    // drainOnce, which protected only the very first post-truncation drain.
    // On the SECOND drain (and every subsequent one) while HW remained at
    // logStartOffset, the truncation guard wouldn't re-fire (cursor was
    // already at logStartOffset), the local flag would default to false,
    // and the up-to-date branch would unconditionally call loader.commit()
    // — installing RuleSet.EMPTY over the previously-good active(). The
    // exact regression the fix was meant to prevent, one drain later.
    //
    // This test exercises THREE drains:
    //   1. Initial drain: install 2 rules.
    //   2. Truncation-to-empty drain: HW dropped to logStartOffset → defer
    //      (this case was already covered by
    //      drainOnceTruncationToEmptyDoesNotInstallEmptyOverPreviouslyGoodRuleSet).
    //   3. Subsequent drain with HW STILL at logStartOffset: the flag must
    //      still be set, so the engine still holds previously-good rules.
    //      A locally-scoped flag would fail this third drain.
    //
    // Finally, recovery: HW advances → replay+commit fires → flag clears →
    // a 4th drain that returns to the "up-to-date" steady state commits
    // idempotently rather than deferring.
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))

    // Drain 1: install two rules.
    when(log.logStartOffset).thenReturn(0L)
    when(log.highWatermark).thenReturn(2L)
    when(log.read(0L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true)).thenReturn(
      recordsAt(0L,
        new SimpleRecord("r1".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.METADATA, 7)),
        new SimpleRecord("r2".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.FETCH, 11))))
    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    assertEquals(2L, boot.drainOnce())
    val previouslyGood = engine.active()
    assertEquals(2, previouslyGood.size())

    // Drain 2: truncation-to-empty. Guard fires; flag set; defer.
    when(log.highWatermark).thenReturn(0L)
    assertEquals(0L, boot.drainOnce())
    assertSame(previouslyGood, engine.active(),
      "drain 2 must preserve previously-good active() (truncation guard fires)")

    // Drain 3 (THE REGRESSION CASE): HW STILL at logStartOffset. Truncation
    // guard does NOT fire (cursor was already rewound to logStartOffset).
    // If the holding-stale flag were local to drainOnce, it would default
    // to false here and the up-to-date branch would commit the empty
    // working state, flipping engine.active() to RuleSet.EMPTY. Pinning
    // assertSame here proves the field-promoted flag persists.
    assertEquals(0L, boot.drainOnce())
    assertSame(previouslyGood, engine.active(),
      "drain 3 must STILL preserve previously-good active() — this is the " +
        "subsequent-drain regression the local-flag version reintroduced")
    // Drain 4 (still empty): same invariant must hold indefinitely.
    assertEquals(0L, boot.drainOnce())
    assertSame(previouslyGood, engine.active(),
      "drain 4 must STILL preserve previously-good active() — held-stale " +
        "posture persists across arbitrarily many empty drains")

    // Recovery: HW advances → replay → commit clears the flag.
    when(log.highWatermark).thenReturn(1L)
    when(log.read(0L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true)).thenReturn(
      recordsAt(0L,
        new SimpleRecord("r-fresh".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.LIST_OFFSETS, 23))))
    assertEquals(1L, boot.drainOnce(),
      "recovery drain reads the one fresh record")
    val fresh = engine.active()
    assertEquals(1, fresh.size())
    assertNotSame(previouslyGood, fresh,
      "recovery commit must replace the held-stale RuleSet")

    // Drain 6 (post-recovery, steady state, no new records): the
    // up-to-date branch now commits idempotently. The held-stale flag is
    // clear, so engine.active() may be a fresh instance (the idempotent
    // commit publishes a new snapshot), but it must STILL contain the
    // single fresh rule — proving we did not slip back into defer-mode.
    assertEquals(0L, boot.drainOnce())
    assertEquals(1, engine.active().size(),
      "post-recovery steady state still reflects the fresh rule (idempotent " +
        "commit did not regress to the held-stale defer path)")
    assertTrue(engine.evaluate(ApiKeys.LIST_OFFSETS, "c", false,
      () => Collections.emptyMap()).denied)
  }

  @Test
  def drainOnceTruncationFollowedByAllMalformedRecordsKeepsPreviouslyGoodRuleSet(): Unit = {
    // Audit B1 regression pin. The held-stale flag was originally cleared
    // by ANY successful replay+commit, where "successful" only meant "the
    // per-record loop ran without an uncaught exception". A truncation
    // followed by a stream of malformed envelopes would therefore:
    //
    //   1. Truncation guard fires → loader.reset() → holdingStalePostTruncation = true.
    //   2. Next drain reads N malformed records. Each one is rejected by
    //      RuleJsonCodec.decode, the loader logs a WARN and DROPS the record
    //      — the working state stays empty.
    //   3. The drain's per-record loop returns replayed=N (>0), the old
    //      code unconditionally called loader.commit() AND cleared the
    //      held-stale flag — publishing RuleSet.EMPTY over the engine's
    //      previously-good active().
    //
    // That is the exact fail-stale-to-empty regression the held-stale flag
    // exists to prevent, sneaking in through "read records but applied
    // none". The B1 fix gates the held-stale-clearance commit on a fresh
    // `applied > 0` counter (records the loader returned true for), so a
    // batch of poison cannot evict known-good rules.
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))

    // Drain 1: install two good rules.
    when(log.logStartOffset).thenReturn(0L)
    when(log.highWatermark).thenReturn(2L)
    when(log.read(0L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true)).thenReturn(
      recordsAt(0L,
        new SimpleRecord("r1".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.METADATA, 7)),
        new SimpleRecord("r2".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.FETCH, 11))))
    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    assertEquals(2L, boot.drainOnce())
    val previouslyGood = engine.active()
    assertEquals(2, previouslyGood.size(), "baseline: two rules installed")

    // Truncation race: cursor at 2, but the log has been rewound and
    // logStartOffset is now 100; HW is now 103 with three new records, all
    // of which are MALFORMED (the codec will reject every one of them).
    // The truncation guard fires, loader.reset() runs, nextOffset rewinds
    // to logStartOffset=100, and the held-stale flag is set.
    when(log.logStartOffset).thenReturn(100L)
    when(log.highWatermark).thenReturn(103L)
    when(log.read(100L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true)).thenReturn(
      recordsAt(100L,
        new SimpleRecord("bad-1".getBytes(StandardCharsets.UTF_8),
          "this is not json at all".getBytes(StandardCharsets.UTF_8)),
        new SimpleRecord("bad-2".getBytes(StandardCharsets.UTF_8),
          "{\"apiKeys\":[\"NOT_A_REAL_API\"]}".getBytes(StandardCharsets.UTF_8)),
        new SimpleRecord("bad-3".getBytes(StandardCharsets.UTF_8),
          "{\"apiKeys\":[\"METADATA\"],\"action\":\"PURGE\",\"when\":\"true\",\"errorCode\":1}"
            .getBytes(StandardCharsets.UTF_8))))

    // Drain 2: cursor (2) is BEHIND new logStartOffset (100), but more
    // importantly cursor (2) is also BEHIND HW (103) — actually wait, that
    // misses the truncation guard. Force it by setting HW below cursor for
    // a single observation: bump HW first, then expect the next drain to
    // see HW < cursor, fire the guard, reset, and rewind. The robust way
    // is two drains:
    //
    //   - Drain 2a: HW drops to 1 (below cursor=2) → guard fires → reset →
    //     rewind to logStartOffset=100. Replay range [100, 1) is empty,
    //     held-stale defer triggers, no commit.
    //   - Drain 2b: HW now 103 with three malformed records → drain reads
    //     3, applies 0; held-stale defer must STILL trigger because no
    //     record successfully contributed to the working state.
    when(log.highWatermark).thenReturn(1L)
    assertEquals(0L, boot.drainOnce(),
      "drain 2a: truncation guard fires, replay range is empty, defer commit")
    assertSame(previouslyGood, engine.active(),
      "drain 2a must preserve previously-good active() (truncation defer)")

    when(log.highWatermark).thenReturn(103L)
    val nMalformed = boot.drainOnce()
    assertEquals(3L, nMalformed,
      "drain 2b: read=3 (all three malformed records were iterated)")
    assertSame(previouslyGood, engine.active(),
      "drain 2b is the B1 regression pin: drain READ 3 records but APPLIED " +
        "none (every envelope was rejected by the codec). Committing the still-" +
        "empty working state here would publish RuleSet.EMPTY over previously-" +
        "good rules — the fail-stale-to-empty regression. Engine must keep its " +
        "last-known-good RuleSet until a drain successfully applies a record.")
    // Belt-and-braces: prove the previously-good rules are still actively
    // enforced post-malformed-drain.
    assertTrue(engine.evaluate(ApiKeys.METADATA, "c", false,
      () => Collections.emptyMap()).denied,
      "previously-good METADATA DENY must still be enforced after all-malformed drain")
    assertTrue(engine.evaluate(ApiKeys.FETCH, "c", false,
      () => Collections.emptyMap()).denied,
      "previously-good FETCH DENY must still be enforced after all-malformed drain")

    // Recovery: a single good record lands. The drain reads 1, applies 1,
    // the held-stale flag clears, and the engine swaps to the fresh set.
    when(log.highWatermark).thenReturn(104L)
    when(log.read(103L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true)).thenReturn(
      recordsAt(103L,
        new SimpleRecord("good".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.LIST_OFFSETS, 23))))
    assertEquals(1L, boot.drainOnce(),
      "recovery drain reads the one good record")
    val fresh = engine.active()
    assertEquals(1, fresh.size())
    assertNotSame(previouslyGood, fresh,
      "recovery commit must replace the held-stale RuleSet with the fresh one")
    assertTrue(engine.evaluate(ApiKeys.LIST_OFFSETS, "c", false,
      () => Collections.emptyMap()).denied)
    // Previously-good rules are gone now — recovery installed only the new one.
    assertSame(RuleDecision.ALLOW,
      engine.evaluate(ApiKeys.METADATA, "c", false, () => Collections.emptyMap()),
      "after recovery commit, previously-held-stale METADATA rule is dropped")
  }

  @Test
  def drainOnceTruncationFollowedByTombstonesOnlyKeepsPreviouslyGoodRuleSet(): Unit = {
    // Round-14 audit BLOCKER C-1 regression pin. Sibling to
    // `drainOnceTruncationFollowedByAllMalformedRecordsKeepsPreviouslyGoodRuleSet`
    // (audit B1) — same fail-stale-to-empty risk, different vector.
    //
    // The B1 fix gated the held-stale-clearance commit on `applied > 0`,
    // closing the malformed-records vector. But GovernanceLoader.apply()
    // returns true for EVERY tombstone (null-value record) — idempotent
    // on absent ids, documented behaviour. So a post-truncation re-drain
    // that reads ONLY tombstones (a realistic compacted-topic shape:
    // operator has just pruned every rule and the surviving updates were
    // compacted out before the broker re-read) satisfies `applied > 0`,
    // the held-stale flag clears, loader.commit() installs RuleSet.EMPTY,
    // and every DENY rule fail-opens. That is the exact regression the
    // held-stale flag exists to prevent, reopened through the tombstone
    // channel.
    //
    // The C-1 fix pairs `applied > 0` with `!loader.workingIsEmpty()`:
    // a tombstone-only batch advances the cursor, leaves the flag set,
    // and DEFERS the commit. Engine keeps its last-known-good RuleSet.
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))

    // Drain 1: install two good rules.
    when(log.logStartOffset).thenReturn(0L)
    when(log.highWatermark).thenReturn(2L)
    when(log.read(0L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true)).thenReturn(
      recordsAt(0L,
        new SimpleRecord("r1".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.METADATA, 7)),
        new SimpleRecord("r2".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.FETCH, 11))))
    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    assertEquals(2L, boot.drainOnce())
    val previouslyGood = engine.active()
    assertEquals(2, previouslyGood.size(), "baseline: two rules installed")

    // Truncation race: HW drops below cursor (forces the truncation guard
    // to fire), then logStartOffset jumps to 100 with three TOMBSTONE
    // records — null values, no surviving updates. The codec routes
    // tombstones around decode (they are key-only), and the loader's
    // apply() returns true for each tombstone idempotently.
    when(log.logStartOffset).thenReturn(100L)
    when(log.highWatermark).thenReturn(1L)
    assertEquals(0L, boot.drainOnce(),
      "drain 2a: truncation guard fires, replay range is empty, defer commit")
    assertSame(previouslyGood, engine.active(),
      "drain 2a must preserve previously-good active() (truncation defer)")

    when(log.highWatermark).thenReturn(103L)
    when(log.read(100L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true)).thenReturn(
      recordsAt(100L,
        new SimpleRecord("r1".getBytes(StandardCharsets.UTF_8), null.asInstanceOf[Array[Byte]]),
        new SimpleRecord("r2".getBytes(StandardCharsets.UTF_8), null.asInstanceOf[Array[Byte]]),
        new SimpleRecord("r3-never-existed".getBytes(StandardCharsets.UTF_8),
          null.asInstanceOf[Array[Byte]])))
    val nTombstones = boot.drainOnce()
    assertEquals(3L, nTombstones,
      "drain 2b: read=3 (all three tombstones were iterated)")
    assertSame(previouslyGood, engine.active(),
      "drain 2b is the C-1 regression pin: drain READ 3 tombstones and APPLIED " +
        "3 of them (loader.apply returns true idempotently for tombstones), but " +
        "the working set is empty after reset+replay. Committing here would " +
        "publish RuleSet.EMPTY over previously-good rules — the fail-stale-to-" +
        "empty regression through the tombstone channel. Engine must keep its " +
        "last-known-good RuleSet until a drain lands a non-empty working state.")
    assertTrue(engine.evaluate(ApiKeys.METADATA, "c", false,
      () => Collections.emptyMap()).denied,
      "previously-good METADATA DENY must still be enforced after tombstone-only drain")
    assertTrue(engine.evaluate(ApiKeys.FETCH, "c", false,
      () => Collections.emptyMap()).denied,
      "previously-good FETCH DENY must still be enforced after tombstone-only drain")

    // Recovery: a single good record lands. The drain reads 1, applies 1,
    // working set becomes non-empty, held-stale flag clears, engine swaps
    // to the fresh set. The operator-recovery escape hatch documented on
    // GovernanceLoader.workingIsEmpty.
    when(log.highWatermark).thenReturn(104L)
    when(log.read(103L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true)).thenReturn(
      recordsAt(103L,
        new SimpleRecord("good".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.LIST_OFFSETS, 23))))
    assertEquals(1L, boot.drainOnce(),
      "recovery drain reads the one good record")
    val fresh = engine.active()
    assertEquals(1, fresh.size())
    assertNotSame(previouslyGood, fresh,
      "recovery commit must replace the held-stale RuleSet with the fresh one")
    assertTrue(engine.evaluate(ApiKeys.LIST_OFFSETS, "c", false,
      () => Collections.emptyMap()).denied)
    assertSame(RuleDecision.ALLOW,
      engine.evaluate(ApiKeys.METADATA, "c", false, () => Collections.emptyMap()),
      "after recovery commit, previously-held-stale METADATA rule is dropped")
  }

  @Test
  def drainOnceProceedsEvenWhenADenyAllFetchRuleIsActive(): Unit = {
    // Adversarial M4: PROMPT.md requires the broker to keep enforcing the
    // governance topic itself even if an operator publishes a deny-all rule
    // covering FETCH. The bootstrap uses ReplicaManager.getLog (local log
    // read) — not a Kafka client over the wire — so the drain path does
    // not enter RuleEngine.evaluate at all. This test pins that structural
    // invariant by installing a deny-all FETCH rule BEFORE drainOnce runs
    // and verifying the drain still applies the next record.
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))
    when(log.logStartOffset).thenReturn(0L)
    when(log.highWatermark).thenReturn(1L)
    when(log.read(0L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true)).thenReturn(
      recordsAt(0L,
        new SimpleRecord("after-deny".getBytes(StandardCharsets.UTF_8),
          envelope("true", ApiKeys.METADATA, 42))))

    // Install a deny-all FETCH rule directly. If the bootstrap were
    // routing its drain reads through RuleEngine.evaluate (or any path
    // that consulted the active RuleSet for FETCH), this rule would
    // block its own delivery and the second-rule install would never
    // happen — a classic chicken-and-egg failure mode for security-
    // critical topics.
    val denyAllFetch = org.apache.kafka.server.rules.json.RuleJsonCodec.decode(
      "deny-fetch", envelope("true", ApiKeys.FETCH, 1))
    val preInstalled = new org.apache.kafka.server.rules.RuleSetBuilder()
      .put(denyAllFetch).build()
    engine.install(preInstalled)
    assertTrue(engine.evaluate(ApiKeys.FETCH, "c", false,
      () => Collections.emptyMap()).denied,
      "precondition: deny-all FETCH must be in force before drain runs")

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    val n = boot.drainOnce()
    // The structural proof is n == 1: the bootstrap successfully consumed
    // the record from the local log. If the drain path had any dependency
    // on RuleEngine.evaluate(FETCH, ...) it would have been short-circuited
    // by the pre-installed deny-all-FETCH rule and n would be 0. The
    // post-commit engine state (1 rule, replacing the manually pre-installed
    // one) is a side-effect of loader.commit() installing working.build()
    // wholesale — in production no path other than the loader writes to the
    // engine, so this replace-on-commit semantics is correct.
    assertEquals(1L, n,
      "drain must succeed despite the deny-all FETCH rule — proves the bootstrap " +
        "uses local log read, not a wire FETCH gated by RuleEngine.evaluate")
    assertEquals(1, engine.active().size(),
      "loader.commit() installs the working set wholesale; the manually " +
        "pre-installed deny-FETCH is replaced by the freshly-drained rule")
    assertTrue(engine.evaluate(ApiKeys.METADATA, "c", false,
      () => Collections.emptyMap()).denied,
      "the newly-drained METADATA rule must be enforced")
  }

  @Test
  def scheduledDrainFailureWarningsAreDeduplicatedAndRolledUp(): Unit = {
    // MINOR-2: when a broker is reassigned away from __governance-0 mid-
    // runtime under requireLocalReplica=true (or any other deterministic
    // throw from drainOnce), the scheduled drain runs at sub-second
    // cadence and would WARN on every tick — quickly burying the rest of
    // broker.log under the same message. The deduplication policy:
    //   - first occurrence WARNs (count +1)
    //   - identical occurrences within FailureWarnIntervalMs are
    //     silently suppressed and counted
    //   - the same identical occurrence past the interval rolls up the
    //     suppressed count into a single WARN
    //   - a NEW distinct message resets the ledger and WARNs immediately
    val rm = mock(classOf[ReplicaManager])
    val engine = new RuleEngine()
    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    val clock = new AtomicLong(0L)
    boot.failureWarnNowMs = () => clock.get()

    // 1st occurrence — fresh message, fires.
    boot.maybeWarnSuppressed("reassigned away")
    assertEquals(1L, boot.warnEmissions.get(),
      "first occurrence of a fresh failure must WARN")

    // 2nd–6th identical occurrences within the suppression window —
    // suppressed silently.
    clock.set(5_000L); boot.maybeWarnSuppressed("reassigned away")
    clock.set(10_000L); boot.maybeWarnSuppressed("reassigned away")
    clock.set(20_000L); boot.maybeWarnSuppressed("reassigned away")
    clock.set(40_000L); boot.maybeWarnSuppressed("reassigned away")
    clock.set(59_999L); boot.maybeWarnSuppressed("reassigned away")
    assertEquals(1L, boot.warnEmissions.get(),
      "identical occurrences inside the suppression window must NOT WARN — " +
        "the broker.log floor must not be buried under 12 Hz repeats")

    // Crossing the suppression window — same message rolls up.
    clock.set(60_001L)
    boot.maybeWarnSuppressed("reassigned away")
    assertEquals(2L, boot.warnEmissions.get(),
      "crossing the suppression window with the same message must roll up the count " +
        "into exactly ONE WARN, not one per skipped occurrence")

    // A new distinct failure resets the ledger and WARNs immediately,
    // mentioning the previously-suppressed message so an operator
    // scanning logs sees the transition.
    clock.set(60_500L)
    boot.maybeWarnSuppressed("disk faulted")
    assertEquals(3L, boot.warnEmissions.get(),
      "a brand-new distinct failure must WARN immediately — operators must see " +
        "transitions to a new failure mode without waiting for the suppression window")

    // The same new message is now itself suppressed for the next window.
    clock.set(60_600L); boot.maybeWarnSuppressed("disk faulted")
    clock.set(80_000L); boot.maybeWarnSuppressed("disk faulted")
    assertEquals(3L, boot.warnEmissions.get(),
      "the new failure becomes subject to the same per-message suppression policy")
  }

  // ── Round-14 HIGH H-1: cleanup.policy runtime drift detector ────────────

  @Test
  def maybeWarnIfCleanupPolicyDriftedIsSilentWhenLogIsCompactOnly(): Unit = {
    // Happy path: operator follows the contract, log is cleanup.policy=compact.
    // The drift check must NOT emit a WARN.
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))
    val props = new java.util.HashMap[String, Object]()
    props.put(org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG, "compact")
    when(log.config).thenReturn(new org.apache.kafka.storage.internals.log.LogConfig(props))

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    boot.maybeWarnIfCleanupPolicyDrifted()

    assertEquals(0L, boot.warnEmissions.get(),
      "compact-only policy must not emit a drift WARN")
  }

  @Test
  def maybeWarnIfCleanupPolicyDriftedFiresOnDeleteOnly(): Unit = {
    // Hot-reload regression: operator AlterConfigs the topic to cleanup.policy
    // = delete on a running broker (the startup gate already ran with compact).
    // The drift check must surface a WARN — the broker can't abort startup
    // anymore, but it can give the operator a positive audit signal during
    // the retention-window grace period before rules begin to silently age out.
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))
    val props = new java.util.HashMap[String, Object]()
    props.put(org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG, "delete")
    when(log.config).thenReturn(new org.apache.kafka.storage.internals.log.LogConfig(props))

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    boot.maybeWarnIfCleanupPolicyDrifted()

    assertEquals(1L, boot.warnEmissions.get(),
      "delete-only drift must emit exactly one WARN")
  }

  @Test
  def maybeWarnIfCleanupPolicyDriftedFiresOnMixedCompactDelete(): Unit = {
    // Round-13 HIGH-1 closed the substring-check loophole at startup. The
    // runtime drift check must reject the same mixed policy: compact AND
    // delete both enabled means retention.ms still deletes rule records.
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))
    val props = new java.util.HashMap[String, Object]()
    props.put(org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG, "compact,delete")
    when(log.config).thenReturn(new org.apache.kafka.storage.internals.log.LogConfig(props))

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    boot.maybeWarnIfCleanupPolicyDrifted()

    assertEquals(1L, boot.warnEmissions.get(),
      "compact,delete mixed policy must emit a drift WARN")
  }

  @Test
  def maybeWarnIfCleanupPolicyDriftedDedupesRepeatedDrifts(): Unit = {
    // The drift detector runs on every drain tick (every 200ms in production).
    // Sustained drift must NOT spam the log — maybeWarnSuppressed should
    // dedupe by message, emitting one WARN initially and rolling up the rest
    // until the FailureWarnIntervalMs window elapses. We don't advance the
    // clock here; the first call wins the slot, the rest are silently
    // suppressed.
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))
    val props = new java.util.HashMap[String, Object]()
    props.put(org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG, "delete")
    when(log.config).thenReturn(new org.apache.kafka.storage.internals.log.LogConfig(props))

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    for (_ <- 0 until 50) {
      boot.maybeWarnIfCleanupPolicyDrifted()
    }
    assertEquals(1L, boot.warnEmissions.get(),
      "repeated identical drift must emit one WARN, not one per tick")
  }

  @Test
  def maybeWarnIfCleanupPolicyDriftedIsNoOpWhenLogIsAbsent(): Unit = {
    // When the local log object is not available (broker not a replica,
    // log dir still opening, transient race), there is no LogConfig to peek
    // at — the drift check returns silently. The NonReplica posture is
    // already covered by drainOnce's own ERROR/throw paths; we don't want
    // a redundant WARN here.
    val rm = mock(classOf[ReplicaManager])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(None)

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    boot.maybeWarnIfCleanupPolicyDrifted()

    assertEquals(0L, boot.warnEmissions.get(),
      "absent log must not emit a drift WARN")
  }

  // ── Round-15 BLOCKER-2: __governance partition-count runtime drift ──────

  @Test
  def maybeWarnIfPartitionCountDriftedIsSilentOnSinglePartition(): Unit = {
    // Happy path: live partition count is 1, no drift to warn about.
    val rm = mock(classOf[ReplicaManager])
    val engine = new RuleEngine()
    val boot = new BrokerGovernanceBootstrap(rm, engine, tp,
      partitionCountProbe = () => 1)
    boot.maybeWarnIfPartitionCountDrifted()
    assertEquals(0L, boot.warnEmissions.get(),
      "partitionCount=1 must not emit a partition-count drift WARN")
  }

  @Test
  def maybeWarnIfPartitionCountDriftedIsSilentOnAbsentTopic(): Unit = {
    // Probe convention: returns 1 when the topic does not yet exist in the
    // metadata image, so the operator does not see a false-positive WARN
    // before the topic is even created. The check fires only on a real
    // > 1 observation.
    val rm = mock(classOf[ReplicaManager])
    val engine = new RuleEngine()
    val boot = new BrokerGovernanceBootstrap(rm, engine, tp,
      partitionCountProbe = () => 1)
    boot.maybeWarnIfPartitionCountDrifted()
    assertEquals(0L, boot.warnEmissions.get(),
      "absent-topic probe response of 1 must not WARN")
  }

  @Test
  def maybeWarnIfPartitionCountDriftedFiresOnGrowToThree(): Unit = {
    // Hot-reload regression: operator AlterPartitions to grow __governance
    // from 1 to 3 partitions. The drift check must surface a WARN — the
    // broker can't abort startup anymore, but it can give the operator a
    // positive signal during the window before any rule on partition 1 or 2
    // is silently dropped.
    val rm = mock(classOf[ReplicaManager])
    val engine = new RuleEngine()
    val boot = new BrokerGovernanceBootstrap(rm, engine, tp,
      partitionCountProbe = () => 3)
    boot.maybeWarnIfPartitionCountDrifted()
    assertEquals(1L, boot.warnEmissions.get(),
      "partitionCount>1 must emit exactly one partition-count drift WARN")
  }

  @Test
  def maybeWarnIfPartitionCountDriftedFiresOnGrowToTwo(): Unit = {
    // Two partitions is the minimum-impact misconfig (roughly half the
    // rules silently dropped) but still fail-OPEN. No exemption for the
    // "small" case — exactly mirrors the startup gate's posture.
    val rm = mock(classOf[ReplicaManager])
    val engine = new RuleEngine()
    val boot = new BrokerGovernanceBootstrap(rm, engine, tp,
      partitionCountProbe = () => 2)
    boot.maybeWarnIfPartitionCountDrifted()
    assertEquals(1L, boot.warnEmissions.get(),
      "even partitionCount=2 must WARN — no minimum-impact exemption")
  }

  @Test
  def maybeWarnIfPartitionCountDriftedDedupesRepeatedDrifts(): Unit = {
    // The drift detector runs on every drain tick. A sustained 3-partition
    // condition must NOT spam the log — the partition-count throttle dedupes
    // by message identical to the cleanup-policy throttle's behaviour.
    val rm = mock(classOf[ReplicaManager])
    val engine = new RuleEngine()
    val boot = new BrokerGovernanceBootstrap(rm, engine, tp,
      partitionCountProbe = () => 3)
    for (_ <- 0 until 50) {
      boot.maybeWarnIfPartitionCountDrifted()
    }
    assertEquals(1L, boot.warnEmissions.get(),
      "repeated identical partition-count drift must emit one WARN, not one per tick")
  }

  // ── Round-15 HIGH-1: drift WARN namespace separation ────────────────────

  @Test
  def cleanupPolicyDriftDoesNotInterfereWithPartitionCountDriftDedupe(): Unit = {
    // The throttles must keep independent ledgers. Before round-15 HIGH-1
    // both categories shared a single dedup ledger keyed on the message
    // string, so a cleanup-policy drift WARN would burn the dedupe slot
    // that a subsequent partition-count drift WARN should have taken — the
    // second message would still fire (it's a distinct string) but the
    // shared ledger contaminated cross-category dedupe state. Worse, the
    // hard-coded "governance rules drain failed" prefix on the shared
    // helper labelled every drift WARN as a drain failure.
    //
    // Independence check: a single cleanup-policy drift WARN followed by
    // 10 partition-count drift WARNs must produce exactly two emissions
    // (one of each), with the partition-count throttle deduping internally
    // across the 10 repeats. If the throttles were shared, the partition-
    // count WARN would fire once (distinct message), THEN dedupe, but the
    // shared lastWarnedMessage would have been clobbered → cross-category
    // bleed when the cleanup-policy drift WARN re-runs. We model that
    // round-trip explicitly below.
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))
    val props = new java.util.HashMap[String, Object]()
    props.put(org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG, "delete")
    when(log.config).thenReturn(new org.apache.kafka.storage.internals.log.LogConfig(props))

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp,
      partitionCountProbe = () => 3)

    // First tick: both drifts fire (2 emissions).
    boot.maybeWarnIfCleanupPolicyDrifted()
    boot.maybeWarnIfPartitionCountDrifted()
    assertEquals(2L, boot.warnEmissions.get(),
      "first observation of each independent drift category must each WARN once")

    // Subsequent ticks: each category dedupes within its own ledger.
    // 49 more ticks of each → still 2 emissions total.
    for (_ <- 0 until 49) {
      boot.maybeWarnIfCleanupPolicyDrifted()
      boot.maybeWarnIfPartitionCountDrifted()
    }
    assertEquals(2L, boot.warnEmissions.get(),
      "each category's throttle dedupes independently; interleaving must not " +
        "spuriously refire either category — round-15 HIGH-1 namespace " +
        "separation invariant")
  }

  @Test
  def drainFailureDedupeIsIndependentOfDriftDedupe(): Unit = {
    // The legacy maybeWarnSuppressed code path is reserved for actual drain
    // failures (drainOnce threw). It must NOT share a ledger with the drift
    // WARNs — an operator reassigning a broker mid-runtime sees the drain
    // throw once per tick, but in parallel the cleanup-policy may also have
    // drifted. The two events are independent diagnoses and must each
    // surface a separate steady-state WARN.
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))
    val props = new java.util.HashMap[String, Object]()
    props.put(org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG, "delete")
    when(log.config).thenReturn(new org.apache.kafka.storage.internals.log.LogConfig(props))

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)

    boot.maybeWarnSuppressed("reassigned away")
    boot.maybeWarnIfCleanupPolicyDrifted()
    assertEquals(2L, boot.warnEmissions.get(),
      "a drain-failure WARN and a cleanup-policy drift WARN are distinct categories " +
        "and must each emit once on first observation, not collapse via a shared ledger")

    // Sustained: 20 more ticks of each — still 2.
    for (_ <- 0 until 20) {
      boot.maybeWarnSuppressed("reassigned away")
      boot.maybeWarnIfCleanupPolicyDrifted()
    }
    assertEquals(2L, boot.warnEmissions.get(),
      "each category dedupes inside its own ledger")
  }

  @Test
  def drainStartupFailsClosedIfDrainMakesNoProgressBeforeDeadline(): Unit = {
    // Codex final-audit P0 fail-closed branch: if drainOnce never advances
    // the cursor (a persistent zero-byte read suggesting log-dir / pager
    // unhealthiness), drainStartup must NOT silently accept the partial
    // drain and let BrokerServer open sockets. It must throw, leaving the
    // broker in a fail-closed state the operator can investigate.
    //
    // Scenario: HW=3 but log.read ALWAYS returns empty. The loop sleeps,
    // retries, and eventually exhausts the deadline.
    val rm = mock(classOf[ReplicaManager])
    val log = mock(classOf[UnifiedLog])
    val engine = new RuleEngine()
    when(rm.getLog(tp)).thenReturn(Some(log))
    when(log.logStartOffset).thenReturn(0L)
    when(log.highWatermark).thenReturn(3L)

    val empty = new FetchDataInfo(new LogOffsetMetadata(0L), MemoryRecords.EMPTY)
    when(log.read(0L, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, true))
      .thenReturn(empty)

    val boot = new BrokerGovernanceBootstrap(rm, engine, tp)
    val ex = assertThrows(classOf[IllegalStateException],
      () => boot.drainStartup(deadlineMs = 50L, pollIntervalMs = 1L))
    val msg = ex.getMessage
    assertTrue(msg.contains("partial drain") || msg.contains("Refusing"),
      s"error must signal fail-closed posture, got: $msg")
    assertEquals(0, engine.active().size(),
      "no rule must be installed when drainStartup throws on a partial drain")
  }
}
