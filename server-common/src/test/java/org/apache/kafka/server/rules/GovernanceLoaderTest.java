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
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

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

    /**
     * Mirror of {@code RuleJsonCodec.FORBIDDEN_API_KEYS} (kept in this package
     * so tests across both rules sub-packages stay independent of the codec's
     * package-private visibility). Tests that spread rules across "every api
     * key" to exercise the GLOBAL {@code MAX_RULES} cap must skip these —
     * authoring a rule against them is rejected at codec intake, so the
     * iteration would otherwise short-circuit before reaching the cap.
     */
    private static final Set<ApiKeys> FORBIDDEN_TARGETS = EnumSet.of(
        ApiKeys.API_VERSIONS,
        ApiKeys.SASL_HANDSHAKE,
        ApiKeys.SASL_AUTHENTICATE,
        ApiKeys.ENVELOPE);

    private static ApiKeys[] denyTargetableApiKeys() {
        List<ApiKeys> keep = new ArrayList<>();
        for (ApiKeys k : ApiKeys.values()) {
            if (!FORBIDDEN_TARGETS.contains(k)) {
                keep.add(k);
            }
        }
        return keep.toArray(new ApiKeys[0]);
    }

    @Test
    public void emptyLoaderInstallsEmptyRuleSet() {
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        loader.commit();
        assertSame(RuleSet.EMPTY, engine.active());
    }

    @Test
    public void workingIsEmptyTracksWorkingSet() {
        // Round-14 audit BLOCKER C-1: the bootstrap's held-stale flag-clear
        // gate keys off `applied > 0` AND `!workingIsEmpty()`. This probe
        // must reflect the working state — not the engine's installed
        // RuleSet — so a tombstone-only post-truncation batch (which
        // advances `applied` but leaves the working set empty) defers the
        // commit and preserves the engine's last-known-good active().
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        assertTrue(loader.workingIsEmpty(), "fresh loader: working set empty");
        loader.apply("r1", envelope("true", ApiKeys.METADATA, 7));
        assertFalse(loader.workingIsEmpty(), "after PUT r1: working set non-empty");
        loader.apply("r1", null);
        assertTrue(loader.workingIsEmpty(), "after tombstone of r1: working set empty");
        loader.apply("r2", envelope("true", ApiKeys.FETCH, 11));
        loader.apply("r3", envelope("true", ApiKeys.LIST_OFFSETS, 13));
        assertFalse(loader.workingIsEmpty(), "after PUT r2+r3: working set non-empty");
        loader.reset();
        assertTrue(loader.workingIsEmpty(), "after reset: working set empty");
        // Idempotent tombstones (the C-1 vector) keep the working set
        // empty — applied returns true, but workingIsEmpty stays true.
        loader.apply("never-existed-1", null);
        loader.apply("never-existed-2", null);
        assertTrue(loader.workingIsEmpty(),
            "after only-idempotent-tombstones: working set still empty (C-1 vector)");
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
        //
        // Spread rules across every deny-targetable api key so we trip the
        // GLOBAL MAX_RULES cap, not the per-api-key cap
        // (MAX_RULES_PER_API_KEY=128). FORBIDDEN_TARGETS are skipped — the
        // codec rejects rules against them at intake, which would otherwise
        // short-circuit this loop before it reached MAX_RULES.
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        ApiKeys[] keys = denyTargetableApiKeys();
        // 1 + (i % 100) generates codes in [1, 100], all of which are
        // currently-assigned Errors enum values (the codec rejects unknown
        // codes per round-8 task #96).
        for (int i = 0; i < RuleSetBuilder.MAX_RULES; i++) {
            loader.apply("r-" + i, envelope("true", keys[i % keys.length], 1 + (i % 100)));
        }
        loader.commit();
        RuleSet capSnapshot = engine.active();
        assertEquals(RuleSetBuilder.MAX_RULES, capSnapshot.size());
        // The (cap+1)'th new rule must be rejected, but the drain must keep
        // flowing — a subsequent good update to an existing id must still
        // land and a commit must publish a coherent RuleSet. Use keys[0]
        // for the overflow attempt and the r-0 update so the in-place
        // update lands on the same api key where r-0 originally lived.
        ApiKeys k0 = keys[0];
        loader.apply("r-overflow", envelope("true", k0, 42));
        loader.apply("r-0", envelope("true", k0, 129));
        loader.commit();
        RuleSet afterOverflow = engine.active();
        assertEquals(RuleSetBuilder.MAX_RULES, afterOverflow.size(),
            "overflow record must not have landed");
        // 'r-0' was updated to errorCode 129 (REBOOTSTRAP_REQUIRED — a
        // known Errors code that is distinct from r-0's original code so
        // the in-place update is observable); verify the update still works
        // at the cap (evaluate against the api key where r-0 lives —
        // keys[0]).
        RuleDecision d = engine.evaluate(
            k0, "client", false, Collections::emptyMap);
        assertTrue(d.denied(), "engine must still be denying via the cap'd set");
        assertEquals("r-0", d.denyingRuleId(),
            "updated r-0 must still be first in declared order");
        assertEquals(129, d.errorCode(),
            "in-place update at the cap must take effect");
    }

    @Test
    public void applyReturnsTrueForSuccessfulUpdateAndTombstoneFalseForRejection() {
        // Audit B1 contract pin. The drain-loop in BrokerGovernanceBootstrap
        // distinguishes "drain read records and made forward progress" from
        // "drain read records but rejected every one" via this boolean. The
        // distinction is what keeps a malformed-only batch after a truncation
        // reset from publishing RuleSet.EMPTY over previously-good rules.
        // Pin the contract directly here — if it ever regresses, every audit
        // chain that depends on it (the held-stale defer, the per-batch
        // accounting in replay()) breaks silently.
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);

        // Successful update → true.
        assertTrue(loader.apply("good", envelope("true", ApiKeys.METADATA, 7)),
            "decoded + put update must signal forward progress");

        // Successful tombstone (id was present) → true.
        assertTrue(loader.apply("good", null),
            "tombstone of a present id must signal forward progress");

        // Idempotent tombstone of an absent id → true (the spec says null
        // value means "this key is now absent"; we honoured it, so it counts
        // as the loader making sense of the record).
        assertTrue(loader.apply("never-existed", null),
            "tombstone of an absent id is idempotent — still forward progress");

        // Null key → false. We cannot identify the rule and dropped the
        // record; the working state is untouched.
        assertFalse(loader.apply(null, envelope("true", ApiKeys.METADATA, 1)),
            "null-key record must signal no progress");
        assertFalse(loader.apply(null, null),
            "null-key tombstone must also signal no progress (cannot identify id)");

        // Malformed envelope → false. The codec rejects; the working state
        // is preserved; the loader signals "no progress".
        assertFalse(loader.apply("bad", "not json".getBytes(StandardCharsets.UTF_8)),
            "malformed envelope must signal no progress");

        // Builder cap exceeded → false. Fill to the cap, then push one over.
        // (Reuse the same loader — putting good rules in does count, the cap
        // overflow at the end is the only rejection.) Spread across every
        // deny-targetable api key so we trip the GLOBAL MAX_RULES cap (not
        // the per-api-key cap); FORBIDDEN_TARGETS are skipped because the
        // codec rejects them at intake.
        ApiKeys[] keys = denyTargetableApiKeys();
        // 1 + (i % 100) generates codes in [1, 100], all known Errors codes.
        for (int i = 0; i < RuleSetBuilder.MAX_RULES; i++) {
            assertTrue(
                loader.apply("cap-" + i, envelope("true", keys[i % keys.length], 1 + (i % 100))),
                "puts up to the cap must all signal progress");
        }
        assertFalse(
            loader.apply("cap-overflow", envelope("true", keys[0], 1)),
            "cap-overflow update must signal no progress (previously-good state preserved)");
    }

    @Test
    public void reservedNameShapeRejectedOnUpdatePath() {
        // Round-12 audit (tombstone/compaction sub-agent, MEDIUM-1): the codec
        // already rejects __name__-shape ids inside decode(); reasserted here
        // at the loader boundary so a future refactor that bypasses decode on
        // the update path (e.g. an envelope cache) cannot regress this.
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        assertFalse(loader.apply("__activation-budget-exceeded__",
                envelope("true", ApiKeys.METADATA, 7)),
            "update record with __name__-shape id must be rejected");
        loader.commit();
        assertEquals(RuleDecision.ALLOW,
            engine.evaluate(ApiKeys.METADATA, "client", false, Collections::emptyMap),
            "rejected update must NOT install any rule");
    }

    @Test
    public void reservedNameShapeRejectedOnTombstonePath() {
        // Round-12 audit (tombstone/compaction sub-agent, MEDIUM-1): the
        // tombstone path used to bypass the codec entirely (working.remove(key)
        // straight through). No __name__-shape rule exists in the working set
        // today (the codec rejects them at intake), but if a future engine-
        // internal sentinel ever populates one, an operator-published tombstone
        // would silently delete it. Close the asymmetry: tombstones for
        // __name__-shape ids are dropped with WARN and signal no progress.
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        // Seed a real operator rule so we can observe that the rejected
        // tombstone left the working state untouched.
        assertTrue(loader.apply("operator-rule", envelope("true", ApiKeys.METADATA, 7)));
        // An operator-published tombstone targeting an engine-internal sentinel.
        assertFalse(loader.apply("__activation-budget-exceeded__", null),
            "tombstone for __name__-shape id must be rejected");
        // And on the symmetric reservation shape (double-underscore both ends).
        assertFalse(loader.apply("__anything__", null),
            "tombstone for any __name__-shape id must be rejected");
        loader.commit();
        // Operator's own rule survived: the spurious tombstone made no
        // mutation to the working state.
        RuleDecision d = engine.evaluate(
            ApiKeys.METADATA, "client", false, Collections::emptyMap);
        assertTrue(d.denied());
        assertEquals("operator-rule", d.denyingRuleId());
    }

    @Test
    public void tombstoneRejectsForbiddenCodepointInKey() {
        // R23 BLOCKER (#211): the tombstone path used to dispatch straight to
        // working.remove(key) after only the reserved-shape check. An
        // operator-published tombstone with key " __activation-budget-exceeded__"
        // (leading NBSP) would render identically to the engine sentinel in
        // any normalising audit viewer AND would silently delete a working-
        // state entry. validateRuleId on both paths closes the asymmetry.
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        // Seed a real operator rule so we can prove a rejected forbidden-
        // codepoint tombstone made no working-state mutation.
        assertTrue(loader.apply("operator-rule", envelope("true", ApiKeys.METADATA, 7)));

        // (a) Leading NBSP (U+00A0) — trim-impersonation primitive.
        assertFalse(loader.apply(" operator-rule", null),
            "tombstone with leading NBSP must be rejected");
        // (b) Zero-width space (U+200B) embedded inside id.
        assertFalse(loader.apply("operator​-rule", null),
            "tombstone with embedded zero-width space must be rejected");
        // (c) ESC control (U+001B) — log-injection primitive.
        assertFalse(loader.apply("operator-rule[2J", null),
            "tombstone with embedded ESC control must be rejected");
        // (d) Bidi-format mark (U+200E LRM) — invisible padding hazard.
        assertFalse(loader.apply("operator-rule‎", null),
            "tombstone with trailing LRM must be rejected");
        // (e) Bidi isolate (U+2068 FSI).
        assertFalse(loader.apply("⁨operator-rule⁩", null),
            "tombstone with FSI/PDI brackets must be rejected");

        // Working state survived every rejected tombstone.
        loader.commit();
        RuleDecision d = engine.evaluate(
            ApiKeys.METADATA, "client", false, Collections::emptyMap);
        assertTrue(d.denied(), "operator-rule survived all rejected tombstones");
        assertEquals("operator-rule", d.denyingRuleId());
    }

    @Test
    public void tombstoneRejectsOverLengthKey() {
        // R23 BLOCKER (#211): the loader's WARN slot logs the sanitised key
        // verbatim, so an unbounded id is a log-amplification primitive on
        // either record shape. The 256-char cap must apply to tombstones too.
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        assertTrue(loader.apply("operator-rule", envelope("true", ApiKeys.METADATA, 7)));

        // 257 ASCII chars = 257 UTF-8 bytes — exactly one past
        // MAX_RULE_ID_BYTES=256.
        StringBuilder sb = new StringBuilder(257);
        for (int i = 0; i < 257; i++) {
            sb.append('a');
        }
        assertFalse(loader.apply(sb.toString(), null),
            "tombstone with over-length id must be rejected");

        // Operator's rule survived.
        loader.commit();
        RuleDecision d = engine.evaluate(
            ApiKeys.METADATA, "client", false, Collections::emptyMap);
        assertTrue(d.denied());
        assertEquals("operator-rule", d.denyingRuleId());
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
