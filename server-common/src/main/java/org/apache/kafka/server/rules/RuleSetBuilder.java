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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Mutable builder for {@link RuleSet}. Add or replace rules with {@link #put},
 * remove them by id with {@link #remove} (idempotent), then {@link #build} an
 * immutable snapshot ready for atomic installation.
 *
 * <p>Builders preserve insertion order across rules; per-API-key lists honour
 * the original insertion order so the first matching DENY in declared order
 * short-circuits subsequent evaluations.
 *
 * <p>The builder caps the total number of distinct rules at {@link #MAX_RULES}.
 * The cap is defense-in-depth against an operator (or a compromised authoring
 * pipeline) publishing tens of thousands of distinct rule envelopes to the
 * {@code __governance} topic: every distinct rule lands in the per-API-key
 * evaluation list of every API it targets, so worst-case per-request CEL
 * evaluation cost is O(rules-targeting-this-api). The bitset short-circuit
 * keeps the no-rule fast path free, but once a rule targets the api a
 * pathological rule count translates directly into per-request latency.
 * 1024 is large enough that no realistic operator will hit it (compression
 * policy alone is one rule per cluster) and small enough that worst-case
 * evaluation cost is bounded.
 */
public final class RuleSetBuilder {

    /**
     * Maximum distinct rule count the builder will accept. See class doc.
     * Replacement of an existing rule id is always allowed regardless of size.
     */
    public static final int MAX_RULES = 1024;

    /** id -> Rule, insertion-ordered. */
    private final LinkedHashMap<String, Rule> rulesById = new LinkedHashMap<>();

    public RuleSetBuilder from(RuleSet base) {
        Objects.requireNonNull(base, "base");
        rulesById.clear();
        rulesById.putAll(base.rulesById);
        return this;
    }

    /**
     * Add a new rule or replace an existing one with the same id. Throws
     * {@link IllegalStateException} when adding a <em>new</em> id would push
     * the working set over {@link #MAX_RULES}; the loader catches this and
     * preserves the previously installed snapshot so the engine never
     * silently drops the cap'th rule while accepting the (cap+1)'th. Updates
     * to an existing id are always accepted (they do not grow the set).
     */
    public RuleSetBuilder put(Rule rule) {
        Objects.requireNonNull(rule, "rule");
        if (rulesById.size() >= MAX_RULES && !rulesById.containsKey(rule.id())) {
            throw new IllegalStateException(
                "rule count cap reached (" + MAX_RULES + "); rejecting new rule '"
                    + rule.id() + "' to bound per-request CEL evaluation cost");
        }
        // Use put-and-restore semantics so replacement preserves the original
        // insertion position. LinkedHashMap.put(k, v) when k exists updates
        // the value in place without reordering — exactly what we need.
        rulesById.put(rule.id(), rule);
        return this;
    }

    /** Visible for tests and for the loader's diagnostic logging. */
    public int size() {
        return rulesById.size();
    }

    public RuleSetBuilder remove(String ruleId) {
        Objects.requireNonNull(ruleId, "ruleId");
        rulesById.remove(ruleId);
        return this;
    }

    public RuleSet build() {
        if (rulesById.isEmpty()) return RuleSet.EMPTY;
        long[] bitset = new long[(RuleSet.MAX_API_KEY_ID / 64) + 1];
        Map<Short, List<Rule>> byKey = new java.util.HashMap<>();
        for (Rule r : rulesById.values()) {
            if (r.action() != RuleAction.DENY) continue;
            for (ApiKeys k : r.apiKeys()) {
                short id = k.id;
                int word = id >>> 6;
                bitset[word] |= 1L << (id & 0x3F);
                byKey.computeIfAbsent(id, ignored -> new ArrayList<>()).add(r);
            }
        }
        // Freeze per-key lists.
        Map<Short, List<Rule>> frozen = new java.util.HashMap<>(byKey.size());
        for (Map.Entry<Short, List<Rule>> e : byKey.entrySet()) {
            frozen.put(e.getKey(), Collections.unmodifiableList(e.getValue()));
        }
        return new RuleSet(bitset, frozen, Collections.unmodifiableMap(new LinkedHashMap<>(rulesById)));
    }
}
