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
import org.apache.kafka.server.rules.cel.CelCompiler;
import org.apache.kafka.server.rules.cel.CelProgram;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RuleSetTest {

    private static final CelProgram TRUE_PROGRAM = CelCompiler.compile("true");
    private static final CelProgram FALSE_PROGRAM = CelCompiler.compile("false");

    private static Rule rule(String id, ApiKeys key) {
        return new Rule(id, Collections.singletonList(key), RuleAction.DENY, "true", 42, TRUE_PROGRAM);
    }

    @Test
    public void emptyHasNoActiveApiKeys() {
        RuleSet rs = RuleSet.EMPTY;
        assertFalse(rs.hasDenyRuleFor((short) ApiKeys.FETCH.id));
        assertEquals(0, rs.rulesFor((short) ApiKeys.FETCH.id).size());
        assertTrue(rs.isEmpty());
    }

    @Test
    public void bitsetReflectsTargetedApiKeys() {
        RuleSet rs = new RuleSetBuilder()
            .put(rule("r1", ApiKeys.CREATE_TOPICS))
            .put(rule("r2", ApiKeys.FETCH))
            .build();
        assertTrue(rs.hasDenyRuleFor((short) ApiKeys.CREATE_TOPICS.id));
        assertTrue(rs.hasDenyRuleFor((short) ApiKeys.FETCH.id));
        assertFalse(rs.hasDenyRuleFor((short) ApiKeys.METADATA.id));
    }

    @Test
    public void preservesDeclaredOrderPerApiKey() {
        RuleSet rs = new RuleSetBuilder()
            .put(rule("r1", ApiKeys.CREATE_TOPICS))
            .put(rule("r2", ApiKeys.CREATE_TOPICS))
            .put(rule("r3", ApiKeys.CREATE_TOPICS))
            .build();
        List<Rule> rules = rs.rulesFor((short) ApiKeys.CREATE_TOPICS.id);
        assertEquals(Arrays.asList("r1", "r2", "r3"),
            Arrays.asList(rules.get(0).id(), rules.get(1).id(), rules.get(2).id()));
    }

    @Test
    public void putReplacesExistingByIdPreservingPosition() {
        Rule r2v1 = rule("r2", ApiKeys.CREATE_TOPICS);
        Rule r2v2 = new Rule("r2", Collections.singletonList(ApiKeys.CREATE_TOPICS),
            RuleAction.DENY, "false", 99, FALSE_PROGRAM);
        RuleSet rs = new RuleSetBuilder()
            .put(rule("r1", ApiKeys.CREATE_TOPICS))
            .put(r2v1)
            .put(rule("r3", ApiKeys.CREATE_TOPICS))
            .put(r2v2)
            .build();
        List<Rule> rules = rs.rulesFor((short) ApiKeys.CREATE_TOPICS.id);
        assertEquals(Arrays.asList("r1", "r2", "r3"),
            Arrays.asList(rules.get(0).id(), rules.get(1).id(), rules.get(2).id()));
        assertEquals(99, rules.get(1).errorCode());
    }

    @Test
    public void removeIsIdempotent() {
        // Tombstoning an absent rule must be a no-op (at-least-once delivery from rules topic).
        RuleSet base = new RuleSetBuilder()
            .put(rule("r1", ApiKeys.CREATE_TOPICS))
            .build();
        RuleSet afterRemove = new RuleSetBuilder().from(base).remove("r-missing").build();
        assertEquals(base.rulesFor((short) ApiKeys.CREATE_TOPICS.id).size(),
            afterRemove.rulesFor((short) ApiKeys.CREATE_TOPICS.id).size());
        RuleSet emptyAfterDouble = new RuleSetBuilder().from(base).remove("r1").remove("r1").build();
        assertFalse(emptyAfterDouble.hasDenyRuleFor((short) ApiKeys.CREATE_TOPICS.id));
    }

    @Test
    public void multiKeyRuleAppearsUnderEachKey() {
        Rule cross = new Rule("c", Arrays.asList(ApiKeys.FETCH, ApiKeys.METADATA),
            RuleAction.DENY, "true", 1, TRUE_PROGRAM);
        RuleSet rs = new RuleSetBuilder().put(cross).build();
        assertTrue(rs.hasDenyRuleFor((short) ApiKeys.FETCH.id));
        assertTrue(rs.hasDenyRuleFor((short) ApiKeys.METADATA.id));
        assertEquals("c", rs.rulesFor((short) ApiKeys.FETCH.id).get(0).id());
        assertEquals("c", rs.rulesFor((short) ApiKeys.METADATA.id).get(0).id());
    }

    @Test
    public void removingMultiKeyRuleClearsAllKeys() {
        Rule cross = new Rule("c", Arrays.asList(ApiKeys.FETCH, ApiKeys.METADATA),
            RuleAction.DENY, "true", 1, TRUE_PROGRAM);
        RuleSet rs = new RuleSetBuilder().put(cross).remove("c").build();
        assertFalse(rs.hasDenyRuleFor((short) ApiKeys.FETCH.id));
        assertFalse(rs.hasDenyRuleFor((short) ApiKeys.METADATA.id));
    }

    @Test
    public void rulesForUnknownKeyReturnsEmpty() {
        RuleSet rs = new RuleSetBuilder().put(rule("r1", ApiKeys.FETCH)).build();
        assertTrue(rs.rulesFor((short) 9999).isEmpty());
    }

    @Test
    public void rulesForReturnsUnmodifiable() {
        RuleSet rs = new RuleSetBuilder().put(rule("r1", ApiKeys.FETCH)).build();
        List<Rule> rules = rs.rulesFor((short) ApiKeys.FETCH.id);
        assertThrows(UnsupportedOperationException.class,
            () -> rules.add(rule("r2", ApiKeys.FETCH)));
    }

    @Test
    public void builderFromCopiesState() {
        RuleSet base = new RuleSetBuilder().put(rule("r1", ApiKeys.FETCH)).build();
        RuleSet derived = new RuleSetBuilder().from(base).put(rule("r2", ApiKeys.FETCH)).build();
        assertEquals(1, base.rulesFor((short) ApiKeys.FETCH.id).size());
        assertEquals(2, derived.rulesFor((short) ApiKeys.FETCH.id).size());
        assertNotSame(base, derived);
    }

    @Test
    public void emptyIsSingleton() {
        assertSame(RuleSet.EMPTY, new RuleSetBuilder().build());
    }

    @Test
    public void putRejectsNewIdsPastMaxRulesCap() {
        // Fill the builder to the cap with distinct ids, then verify the
        // (cap+1)'th distinct id is rejected. The cap exists so per-request
        // CEL evaluation cost is bounded regardless of how many rules the
        // operator publishes to __governance.
        RuleSetBuilder b = new RuleSetBuilder();
        for (int i = 0; i < RuleSetBuilder.MAX_RULES; i++) {
            b.put(rule("rule-" + i, ApiKeys.FETCH));
        }
        assertEquals(RuleSetBuilder.MAX_RULES, b.size());
        IllegalStateException ex = assertThrows(IllegalStateException.class,
            () -> b.put(rule("rule-overflow", ApiKeys.FETCH)));
        assertTrue(ex.getMessage().contains("rule-overflow"),
            "exception must name the rejected rule id: " + ex.getMessage());
        assertTrue(ex.getMessage().contains(String.valueOf(RuleSetBuilder.MAX_RULES)),
            "exception must name the cap value: " + ex.getMessage());
    }

    @Test
    public void putAllowsReplacementAtMaxRulesCap() {
        // Updates to an existing rule id never grow the working set, so they
        // must succeed even at the cap. Otherwise the only way to ever fix
        // or tombstone a rule once the cap is full would be to drop the
        // entire RuleSet — a brittle operational property.
        RuleSetBuilder b = new RuleSetBuilder();
        for (int i = 0; i < RuleSetBuilder.MAX_RULES; i++) {
            b.put(rule("rule-" + i, ApiKeys.FETCH));
        }
        // Same id, different content: must replace in place.
        Rule replacement = new Rule("rule-0", Collections.singletonList(ApiKeys.METADATA),
            RuleAction.DENY, "false", 99, FALSE_PROGRAM);
        b.put(replacement);
        RuleSet rs = b.build();
        assertEquals(RuleSetBuilder.MAX_RULES, rs.size(),
            "replacement must not grow the set");
        // The replacement now targets METADATA, not FETCH — verify it
        // actually landed by checking the per-key list rather than relying
        // on size alone.
        assertTrue(rs.hasDenyRuleFor((short) ApiKeys.METADATA.id),
            "replacement rule's new apiKey must be reflected in the bitset");
    }
}
