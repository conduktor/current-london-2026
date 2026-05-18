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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Immutable snapshot of the currently active rules. Reads are O(1) on
 * the fast path: a single bitset bit per API key determines whether any
 * DENY rule could fire.
 *
 * <p>Build a new {@code RuleSet} through {@link RuleSetBuilder} and swap
 * it atomically through {@link RuleEngine#install(RuleSet)}.
 */
public final class RuleSet {

    /** Maximum {@link ApiKeys#id} observable at runtime; used to size the bitset. */
    static final int MAX_API_KEY_ID;
    static {
        int max = 0;
        for (ApiKeys k : ApiKeys.values()) max = Math.max(max, k.id);
        MAX_API_KEY_ID = max;
    }

    public static final RuleSet EMPTY = new RuleSet(
        new long[(MAX_API_KEY_ID / 64) + 1],
        Collections.emptyMap(),
        Collections.emptyMap());

    private final long[] denyBitset;
    private final Map<Short, List<Rule>> rulesByApiKey;
    /** Rules keyed by id, in original insertion order. Visible to RuleSetBuilder. */
    final Map<String, Rule> rulesById;

    RuleSet(long[] denyBitset, Map<Short, List<Rule>> rulesByApiKey, Map<String, Rule> rulesById) {
        this.denyBitset = denyBitset;
        this.rulesByApiKey = rulesByApiKey;
        this.rulesById = rulesById;
    }

    /**
     * O(1) bitset check. Returns true if at least one DENY rule in this
     * snapshot targets the given API key id.
     */
    public boolean hasDenyRuleFor(short apiKeyId) {
        if (apiKeyId < 0 || apiKeyId > MAX_API_KEY_ID) return false;
        int word = apiKeyId >>> 6;
        long mask = 1L << (apiKeyId & 0x3F);
        return (denyBitset[word] & mask) != 0L;
    }

    /** Rules targeting this API key in declared order. Empty when none. */
    public List<Rule> rulesFor(short apiKeyId) {
        List<Rule> r = rulesByApiKey.get(apiKeyId);
        return r == null ? Collections.emptyList() : r;
    }

    /** True if no rules are loaded. */
    public boolean isEmpty() {
        return rulesById.isEmpty();
    }

    /** Number of distinct rules currently loaded. */
    public int size() {
        return rulesById.size();
    }

    // Internal helpers used by the builder.

    static long[] copyBitset(long[] src) {
        long[] out = new long[src.length];
        System.arraycopy(src, 0, out, 0, src.length);
        return out;
    }

    static Map<Short, List<Rule>> copyIndex(Map<Short, List<Rule>> src) {
        Map<Short, List<Rule>> out = new HashMap<>(src.size());
        for (Map.Entry<Short, List<Rule>> e : src.entrySet()) {
            out.put(e.getKey(), new java.util.ArrayList<>(e.getValue()));
        }
        return out;
    }

    static Map<String, Rule> copyById(Map<String, Rule> src) {
        return new LinkedHashMap<>(src);
    }
}
