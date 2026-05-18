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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Central entry point for broker-side request authorisation by CEL rules.
 *
 * <p>The engine holds the currently-installed {@link RuleSet} in an
 * {@link AtomicReference}. {@link #install(RuleSet)} atomically swaps in a
 * new snapshot — concurrent readers observe either the previous or the new
 * snapshot, never a torn intermediate state.
 *
 * <p>{@link #evaluate(ApiKeys, String, Supplier)} is on the hot path of
 * {@code KafkaApis.handle()} and is designed to do the absolute minimum
 * work when no rule targets the request:
 *
 * <ol>
 *   <li>If the {@code clientId} starts with {@link #INTERNAL_CLIENT_ID_PREFIX},
 *       short-circuit to ALLOW. This is the bootstrap-safety hatch: the broker's
 *       own consumer of the governance topic must never be rule-blocked, even
 *       under a misconfigured "deny everything" rule set.</li>
 *   <li>Bitset check: if no DENY rule in the snapshot targets the request's
 *       API key, return ALLOW without invoking the activation supplier.
 *       This keeps the cost of "rules feature enabled but no rule applies"
 *       at one bitset bit-test per request.</li>
 *   <li>Lazily materialise the activation map by invoking the supplier
 *       exactly once for the whole evaluation — subsequent rules share it.</li>
 *   <li>Iterate the rules targeting this API key in declared order. The
 *       first one whose predicate evaluates to {@code true} wins and
 *       short-circuits subsequent evaluation.</li>
 *   <li>A predicate that throws (divide by zero, type mismatch, etc.) is
 *       treated as fail-open: the throwing rule is logged and skipped,
 *       evaluation continues with the next rule. A buggy rule must not be
 *       able to crash the request path.</li>
 * </ol>
 */
public final class RuleEngine {

    private static final Logger LOG = LoggerFactory.getLogger(RuleEngine.class);

    /**
     * Any clientId that starts with this prefix is unconditionally exempt
     * from rule evaluation. The broker's own governance-topic loader must
     * use this prefix so it can refill the rule set after a "deny all"
     * rule is installed.
     */
    public static final String INTERNAL_CLIENT_ID_PREFIX = "__kafka-governance-";

    private final AtomicReference<RuleSet> active = new AtomicReference<>(RuleSet.EMPTY);

    /**
     * Atomically swap the active rule set. Concurrent readers will observe
     * either the previous or the new {@code rs}, never a partial state.
     */
    public void install(RuleSet rs) {
        active.set(Objects.requireNonNull(rs, "rs"));
    }

    /** The currently active rule set. Useful for diagnostics. */
    public RuleSet active() {
        return active.get();
    }

    /**
     * Evaluate the request against the active rule set.
     *
     * @param apiKey the request's API key
     * @param clientId the request's client-id (may be null/empty for clients
     *                 that did not set one — those are still subject to rules)
     * @param activationSupplier lazy builder of the CEL activation map; only
     *                           invoked if at least one rule targets the API
     *                           key, and only once per call regardless of how
     *                           many rules fire
     * @return {@link RuleDecision#ALLOW} unless a DENY rule matches
     */
    public RuleDecision evaluate(ApiKeys apiKey,
                                 String clientId,
                                 Supplier<Map<String, Object>> activationSupplier) {
        if (clientId != null && !clientId.isEmpty() && clientId.startsWith(INTERNAL_CLIENT_ID_PREFIX)) {
            return RuleDecision.ALLOW;
        }
        RuleSet snapshot = active.get();
        if (!snapshot.hasDenyRuleFor(apiKey.id)) {
            return RuleDecision.ALLOW;
        }
        List<Rule> rules = snapshot.rulesFor(apiKey.id);
        if (rules.isEmpty()) {
            return RuleDecision.ALLOW;
        }
        Map<String, Object> activation = activationSupplier.get();
        for (Rule rule : rules) {
            boolean matched;
            try {
                matched = rule.compiled().evalBoolean(activation::get);
            } catch (RuntimeException e) {
                LOG.warn("rule '{}' failed open due to evaluation error on apiKey {}: {}",
                    rule.id(), apiKey, e.toString());
                continue;
            }
            if (matched) {
                return RuleDecision.deny(rule.errorCode(), rule.id());
            }
        }
        return RuleDecision.ALLOW;
    }
}
