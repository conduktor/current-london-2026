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
package org.apache.kafka.server.rules;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.server.rules.json.RuleJsonCodec;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link GovernanceLoader} — the pure-logic side of the
 * {@code __governance} topic consumer.
 *
 * <p>The loader is decoupled from the actual Kafka consumer for testability:
 * it consumes (key, value) records via {@link GovernanceLoader#apply(String, byte[])}
 * and exposes {@link GovernanceLoader#commit()} to atomically swap the
 * accumulated state into the engine. The reader thread (Kafka-aware) drives
 * this from polled records; tests drive it directly.
 *
 * <p>Acceptance criteria exercised here:
 *
 * <ul>
 *   <li>Tombstoning an absent rule is idempotent (at-least-once delivery).</li>
 *   <li>A bad envelope is dropped per-record; the previously-good RuleSet
 *       remains intact in the loader's working state.</li>
 *   <li>An update to an existing rule replaces in-place and preserves the
 *       rule's declared-order position.</li>
 *   <li>commit() yields a RuleSet whose contents reflect every successful
 *       apply() since construction (or since the previous commit).</li>
 * </ul>
 */
public class GovernanceLoaderTest {

    private static byte[] envelope(String when, ApiKeys key, int errorCode) {
        String json = "{\"apiKeys\":[\"" + key.name() + "\"],\"action\":\"DENY\","
            + "\"when\":\"" + when + "\",\"errorCode\":" + errorCode + "}";
        return json.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    public void emptyLoaderInstallsEmptyRuleSet() {
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        loader.commit();
        assertSame(RuleSet.EMPTY, engine.active());
    }

    @Test
    public void appliedRuleSurvivesCommit() {
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        loader.apply("r1", envelope("true", ApiKeys.METADATA, 7));
        loader.commit();
        RuleDecision d = engine.evaluate(
            ApiKeys.METADATA, "client", false, Collections::emptyMap);
        assertTrue(d.denied());
        assertEquals(7, d.errorCode());
        assertEquals("r1", d.denyingRuleId());
    }

    @Test
    public void tombstoneRemovesRule() {
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        loader.apply("r1", envelope("true", ApiKeys.METADATA, 7));
        loader.apply("r1", null);
        loader.commit();
        assertSame(RuleDecision.ALLOW,
            engine.evaluate(ApiKeys.METADATA, "client", false, Collections::emptyMap));
    }

    @Test
    public void tombstoneIsIdempotentOnAbsentRule() {
        // At-least-once delivery semantics mean we may see a tombstone for a
        // rule that's already gone — the loader must accept it silently.
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        loader.apply("never-existed", null);
        loader.commit();
        assertSame(RuleSet.EMPTY, engine.active());
    }

    @Test
    public void malformedEnvelopeIsDroppedPerRecord() {
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        loader.apply("good", envelope("true", ApiKeys.METADATA, 7));
        loader.apply("bad", "not json".getBytes(StandardCharsets.UTF_8));
        loader.commit();
        // 'good' must remain active; 'bad' must have been dropped, NOT
        // halt the whole batch.
        RuleDecision d = engine.evaluate(
            ApiKeys.METADATA, "client", false, Collections::emptyMap);
        assertTrue(d.denied());
        assertEquals("good", d.denyingRuleId());
    }

    @Test
    public void badEnvelopeDoesNotOverwriteExistingGoodVersion() {
        // PROMPT.md: "Rule envelope rejection preserves the previous good
        // RuleSet". An update that arrives malformed for an *existing* rule
        // must NOT remove the existing version.
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        loader.apply("r1", envelope("true", ApiKeys.METADATA, 7));
        loader.commit();
        loader.apply("r1", "still not json".getBytes(StandardCharsets.UTF_8));
        loader.commit();
        // The original r1 is still in effect.
        RuleDecision d = engine.evaluate(
            ApiKeys.METADATA, "client", false, Collections::emptyMap);
        assertTrue(d.denied());
        assertEquals(7, d.errorCode());
    }

    @Test
    public void updateReplacesInPlacePreservingDeclaredOrder() {
        // Two rules; update the first one. Its position must be preserved so
        // declared-order semantics for the api key are not silently changed
        // by an update.
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        loader.apply("first", envelope("true", ApiKeys.METADATA, 11));
        loader.apply("second", envelope("true", ApiKeys.METADATA, 22));
        loader.commit();
        // Update 'first' to a new error code; its position must still come
        // before 'second' so that 'first' is the rule that fires.
        loader.apply("first", envelope("true", ApiKeys.METADATA, 33));
        loader.commit();
        RuleDecision d = engine.evaluate(
            ApiKeys.METADATA, "client", false, Collections::emptyMap);
        assertTrue(d.denied());
        assertEquals("first", d.denyingRuleId());
        assertEquals(33, d.errorCode());
    }

    @Test
    public void commitInstallsNewRuleSetEachTime() {
        // Each commit is observable: the engine sees a new active() RuleSet.
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        RuleSet rs0 = engine.active();
        loader.apply("r1", envelope("true", ApiKeys.METADATA, 1));
        loader.commit();
        RuleSet rs1 = engine.active();
        assertFalse(rs0 == rs1, "commit must publish a new active RuleSet");
        loader.apply("r2", envelope("true", ApiKeys.FETCH, 2));
        loader.commit();
        RuleSet rs2 = engine.active();
        assertFalse(rs1 == rs2);
        assertEquals(2, rs2.size());
    }

    @Test
    public void nullKeyOnAnyRecordIsRejectedSilently() {
        // We cannot tombstone a rule with no id, and we cannot decode an
        // envelope without one. The loader must not crash on a null key —
        // log + drop and continue.
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        loader.apply(null, envelope("true", ApiKeys.METADATA, 1));
        loader.apply(null, null);
        loader.apply("ok", envelope("true", ApiKeys.METADATA, 2));
        loader.commit();
        RuleDecision d = engine.evaluate(
            ApiKeys.METADATA, "client", false, Collections::emptyMap);
        assertTrue(d.denied());
        assertEquals("ok", d.denyingRuleId());
    }

    @Test
    public void tombstoneThenMalformedUpdateInSameBatchHonoursCompactionOrder() {
        // Regression test for the GovernanceLoader.apply batch-atomicity audit
        // finding. Scenario: a single uncommitted batch contains, in this order,
        //   1. PUT r1  (good)            — installs r1
        //   2. DELETE r1 (tombstone)      — removes r1
        //   3. PUT r1  (malformed update) — decode fails; previous step is NOT undone
        //
        // The correct end state is "r1 is gone". This is what log-compaction
        // semantics say happens by-offset: the tombstone at offset 2 supersedes
        // the PUT at offset 1, and the malformed PUT at offset 3 fails to
        // install a replacement. The loader's decode-then-mutate ordering means
        // the bad update never touches the working state, so the tombstone's
        // effect is preserved — there is no silent overwrite, no half-staged
        // state, no atomicity bug.
        //
        // What is NOT a bug: that the tombstone "removes the previously good"
        // r1. That removal is the explicit, ordered intent of the tombstone
        // record. The audit finding was a misread of compaction semantics;
        // this test pins the actual behaviour so a future refactor doesn't
        // drift back to the buggy "delete X first, then try to install bad X"
        // pattern (which would NOT preserve r1 anyway and would behave
        // identically to today).
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        loader.apply("r1", envelope("true", ApiKeys.METADATA, 7));
        loader.apply("r1", null);
        loader.apply("r1", "not json".getBytes(StandardCharsets.UTF_8));
        loader.commit();
        assertSame(RuleDecision.ALLOW,
            engine.evaluate(ApiKeys.METADATA, "client", false, Collections::emptyMap),
            "tombstone followed by bad update must leave r1 removed (compaction order)");
    }

    @Test
    public void malformedUpdateAfterCommittedGoodOnlyTouchesWorkingStateIfDecodeSucceeded() {
        // Reordering of the same edge case, with a commit between the good
        // PUT and the bad PUT — the bad envelope must NEVER displace a good
        // rule that's already in the working state. This is the half of task
        // #13's concern that does need to hold: validate-then-mutate.
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        loader.apply("r1", envelope("true", ApiKeys.METADATA, 7));
        loader.commit();
        // Now apply a bad update. The working state already has r1; the bad
        // decode must NOT remove or replace it. (Without the validate-first
        // ordering, a buggy refactor that did `working.remove(key); decode();
        // working.put(rule);` would zero out r1 before failing to install
        // its replacement.)
        loader.apply("r1", "still not json".getBytes(StandardCharsets.UTF_8));
        loader.commit();
        RuleDecision d = engine.evaluate(
            ApiKeys.METADATA, "client", false, Collections::emptyMap);
        assertTrue(d.denied(), "previously good r1 must still be active");
        assertEquals(7, d.errorCode());
        assertEquals("r1", d.denyingRuleId());
    }

    @Test
    public void capExceededIsTreatedAsPerRecordFailure() {
        // RuleSetBuilder.MAX_RULES bounds the worst-case per-request CEL
        // evaluation cost. A record that would push the working set past
        // that cap must be rejected per-record (preserving the previously
        // installed RuleSet), not abort the drain or corrupt state. This
        // pins the loader's catch of the IllegalStateException thrown by
        // RuleSetBuilder.put.
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        // Fill the working set to the cap with distinct rules, committing
        // once so the engine has a valid pre-overflow snapshot.
        for (int i = 0; i < RuleSetBuilder.MAX_RULES; i++) {
            loader.apply("r-" + i, envelope("true", ApiKeys.METADATA, 1 + (i % 100)));
        }
        loader.commit();
        RuleSet capSnapshot = engine.active();
        assertEquals(RuleSetBuilder.MAX_RULES, capSnapshot.size());
        // The (cap+1)'th new rule must be rejected, but the drain must keep
        // flowing — a subsequent good update to an existing id must still
        // land and a commit must publish a coherent RuleSet.
        loader.apply("r-overflow", envelope("true", ApiKeys.METADATA, 42));
        loader.apply("r-0", envelope("true", ApiKeys.METADATA, 999));
        loader.commit();
        RuleSet afterOverflow = engine.active();
        assertEquals(RuleSetBuilder.MAX_RULES, afterOverflow.size(),
            "overflow record must not have landed");
        // 'r-0' was updated to errorCode 999; verify the in-place update
        // still works at the cap.
        RuleDecision d = engine.evaluate(
            ApiKeys.METADATA, "client", false, Collections::emptyMap);
        assertTrue(d.denied(), "engine must still be denying via the cap'd set");
        assertEquals("r-0", d.denyingRuleId(),
            "updated r-0 must still be first in declared order");
        assertEquals(999, d.errorCode(),
            "in-place update at the cap must take effect");
    }

    @Test
    public void encodeDecodeViaCodecLinesUpWithLoader() {
        // Sanity: the loader and the codec must agree on what a record looks
        // like. We use the codec's own encode() to produce input — if a
        // future codec change broke the loader, this would catch it.
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        Rule r = RuleJsonCodec.decode("authored",
            envelope("true", ApiKeys.METADATA, 99));
        loader.apply(r.id(), RuleJsonCodec.encode(r));
        loader.commit();
        RuleDecision d = engine.evaluate(
            ApiKeys.METADATA, "client", false, Collections::emptyMap);
        assertTrue(d.denied());
        assertEquals(99, d.errorCode());
    }
}
