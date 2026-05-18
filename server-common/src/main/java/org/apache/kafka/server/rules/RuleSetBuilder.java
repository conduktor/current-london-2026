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
 */
public final class RuleSetBuilder {

    /** id -> Rule, insertion-ordered. */
    private final LinkedHashMap<String, Rule> rulesById = new LinkedHashMap<>();

    public RuleSetBuilder from(RuleSet base) {
        Objects.requireNonNull(base, "base");
        rulesById.clear();
        rulesById.putAll(base.rulesById);
        return this;
    }

    public RuleSetBuilder put(Rule rule) {
        Objects.requireNonNull(rule, "rule");
        // Use put-and-restore semantics so replacement preserves the original
        // insertion position. LinkedHashMap.put(k, v) when k exists updates
        // the value in place without reordering — exactly what we need.
        rulesById.put(rule.id(), rule);
        return this;
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
