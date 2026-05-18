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
import java.util.HashMap;
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
 * <h3>Cap policy</h3>
 *
 * <p>The builder enforces TWO complementary caps, both defense in depth
 * against operator (or compromised-authoring-pipeline) configurations that
 * would translate directly into per-request CEL evaluation latency:
 *
 * <ul>
 *   <li>{@link #MAX_RULES} (1024): the total number of distinct rules in
 *       the working set. Bounds the worst-case memory footprint of a single
 *       RuleSet snapshot.</li>
 *   <li>{@link #MAX_RULES_PER_API_KEY} (128): the number of DENY rules that
 *       may target any single API key. The 1024 global cap alone is not
 *       enough — the audit (CEL DoS HIGH-1) noted that an operator could
 *       point all 1024 rules at a single hot API (e.g. FETCH) and force the
 *       engine to evaluate 1024 CEL programs per Fetch request, since the
 *       per-request walk is over the rules targeting that one API. The
 *       per-key cap fences that off at a level realistic operator policies
 *       cluster well below.</li>
 * </ul>
 *
 * <p>A new id that would breach EITHER cap is rejected; the loader catches
 * the exception and preserves the previously installed snapshot. Replacement
 * (same id) is always accepted as long as the post-replacement state stays
 * within the per-key cap — the same id keeping the same apiKeys never moves
 * any counter and so always passes; an id-replacement that adds apiKeys is
 * checked against the cap for each newly-added key. Removals always succeed
 * and uncount.
 */
public final class RuleSetBuilder {

    /**
     * Maximum distinct rule count the builder will accept. See class doc.
     * Replacement of an existing rule id is always allowed regardless of size.
     */
    public static final int MAX_RULES = 1024;

    /**
     * Maximum number of DENY rules that may target any single API key.
     * See class doc. Only DENY rules count: non-DENY actions do not enter
     * the per-API-key evaluation list and therefore do not contribute to
     * per-request latency. Today the codec accepts only DENY, but counting
     * is action-aware so ALLOW / FILTER can be added later without
     * over-charging the cap.
     */
    public static final int MAX_RULES_PER_API_KEY = 128;

    /** id -> Rule, insertion-ordered. */
    private final LinkedHashMap<String, Rule> rulesById = new LinkedHashMap<>();

    /**
     * Per-API-key count of DENY rules currently in {@link #rulesById}. Kept
     * in sync with {@link #put} / {@link #remove} / {@link #from} so the
     * per-key cap check on a new rule is O(rule.apiKeys().size()) rather
     * than O(|rulesById|). An absent entry is implicitly zero (we drop
     * entries that decrement to zero, which keeps the map's footprint
     * proportional to the active keys rather than to the API surface area).
     */
    private final Map<Short, Integer> apiKeyCounts = new HashMap<>();

    public RuleSetBuilder from(RuleSet base) {
        Objects.requireNonNull(base, "base");
        rulesById.clear();
        apiKeyCounts.clear();
        rulesById.putAll(base.rulesById);
        for (Rule r : base.rulesById.values()) {
            if (r.action() == RuleAction.DENY) {
                for (ApiKeys k : r.apiKeys()) {
                    apiKeyCounts.merge(k.id, 1, Integer::sum);
                }
            }
        }
        return this;
    }

    /**
     * Add a new rule or replace an existing one with the same id. Throws
     * {@link IllegalStateException} when adding a <em>new</em> id would push
     * the working set over {@link #MAX_RULES}, or when ANY of the rule's
     * targeted API keys would exceed {@link #MAX_RULES_PER_API_KEY}; the
     * loader catches this and preserves the previously installed snapshot
     * so the engine never silently drops the cap'th rule while accepting
     * the (cap+1)'th. Updates to an existing id are always accepted as long
     * as the post-replacement per-key counts stay within the cap (an
     * apiKeys-preserving update never moves any counter, so updates to the
     * predicate / errorCode of an existing rule trivially pass).
     */
    public RuleSetBuilder put(Rule rule) {
        Objects.requireNonNull(rule, "rule");
        Rule existing = rulesById.get(rule.id());
        // Compute the per-API-key delta this put would apply. A replacement
        // that targets the same keys nets to zero on those keys; one that
        // swaps key sets cancels exact-matches and only deltas the differing
        // keys. Only DENY rules contribute — non-DENY actions never enter
        // the per-key evaluation list.
        Map<Short, Integer> delta = new HashMap<>();
        if (existing != null && existing.action() == RuleAction.DENY) {
            for (ApiKeys k : existing.apiKeys()) {
                delta.merge(k.id, -1, Integer::sum);
            }
        }
        if (rule.action() == RuleAction.DENY) {
            for (ApiKeys k : rule.apiKeys()) {
                delta.merge(k.id, 1, Integer::sum);
            }
        }
        // Per-key cap: reject if ANY targeted key would exceed the cap
        // after this put. We only need to check positive deltas — a key
        // that is decrementing (apiKey was on the existing rule but not on
        // the replacement) can only shrink its count.
        for (Map.Entry<Short, Integer> e : delta.entrySet()) {
            if (e.getValue() <= 0) {
                continue;
            }
            int post = apiKeyCounts.getOrDefault(e.getKey(), 0) + e.getValue();
            if (post > MAX_RULES_PER_API_KEY) {
                throw new IllegalStateException(
                    "per-API-key rule cap reached for api id " + e.getKey()
                        + " (" + MAX_RULES_PER_API_KEY + "); rejecting rule '"
                        + rule.id() + "' to bound per-request CEL evaluation cost "
                        + "for that api");
            }
        }
        // Global cap on distinct ids. Only fires for genuinely new ids;
        // replacement keeps the same id and so cannot grow the set.
        if (existing == null && rulesById.size() >= MAX_RULES) {
            throw new IllegalStateException(
                "rule count cap reached (" + MAX_RULES + "); rejecting new rule '"
                    + rule.id() + "' to bound per-request CEL evaluation cost");
        }
        // Commit: write the new rule first, then settle the per-key counts.
        // LinkedHashMap.put(k, v) when k exists updates the value in place
        // without reordering — exactly what we need to preserve declared
        // order across replacement.
        rulesById.put(rule.id(), rule);
        for (Map.Entry<Short, Integer> e : delta.entrySet()) {
            if (e.getValue() == 0) {
                continue;
            }
            int post = apiKeyCounts.getOrDefault(e.getKey(), 0) + e.getValue();
            if (post == 0) {
                apiKeyCounts.remove(e.getKey());
            } else {
                apiKeyCounts.put(e.getKey(), post);
            }
        }
        return this;
    }

    /** Visible for tests and for the loader's diagnostic logging. */
    public int size() {
        return rulesById.size();
    }

    public RuleSetBuilder remove(String ruleId) {
        Objects.requireNonNull(ruleId, "ruleId");
        Rule removed = rulesById.remove(ruleId);
        if (removed != null && removed.action() == RuleAction.DENY) {
            for (ApiKeys k : removed.apiKeys()) {
                int post = apiKeyCounts.getOrDefault(k.id, 0) - 1;
                if (post <= 0) {
                    apiKeyCounts.remove(k.id);
                } else {
                    apiKeyCounts.put(k.id, post);
                }
            }
        }
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
        return new RuleSet(bitset, Collections.unmodifiableMap(frozen),
            Collections.unmodifiableMap(new LinkedHashMap<>(rulesById)));
    }
}
