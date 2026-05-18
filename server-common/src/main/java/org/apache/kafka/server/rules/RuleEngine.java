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
 * <p>{@link #evaluate(ApiKeys, String, boolean, Supplier)} is on the hot path of
 * {@code KafkaApis.handle()} and is designed to do the absolute minimum
 * work when no rule targets the request:
 *
 * <ol>
 *   <li>If the request arrived on a <em>privileged listener</em>
 *       (typically the broker's inter-broker listener), short-circuit to
 *       ALLOW. This is the bootstrap-safety hatch: the broker's own consumer
 *       of the governance topic must never be rule-blocked, even under a
 *       misconfigured "deny everything" rule set. Crucially, the
 *       {@code fromPrivilegedListener} bit is set by the network layer
 *       based on which TCP listener accepted the connection — it is not
 *       derived from any wire field a client controls, so external clients
 *       cannot bypass the engine by spoofing a client-id, principal, or
 *       any other application-level identifier.</li>
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
     * Naming convention for the broker's own governance-topic consumer.
     * Purely diagnostic — the engine no longer treats this prefix as
     * authoritative for the bypass. The actual bypass is granted by the
     * {@code fromPrivilegedListener} flag set by the network layer, which
     * external clients cannot forge. Use {@code GovernanceTopic.readerClientId}
     * when constructing the broker-internal consumer.
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
     * @param clientId the request's client-id (may be null/empty; purely
     *                 diagnostic — used in WARN logs, never authoritative)
     * @param fromPrivilegedListener {@code true} iff the request arrived on a
     *                 listener the broker treats as inter-broker. This is the
     *                 sole authoritative bypass; external clients cannot
     *                 forge it because the network layer derives it from the
     *                 accepting listener, not the wire payload.
     * @param activationSupplier lazy builder of the CEL activation map; only
     *                           invoked if at least one rule targets the API
     *                           key, and only once per call regardless of how
     *                           many rules fire
     * @return {@link RuleDecision#ALLOW} unless a DENY rule matches
     */
    public RuleDecision evaluate(ApiKeys apiKey,
                                 String clientId,
                                 boolean fromPrivilegedListener,
                                 Supplier<Map<String, Object>> activationSupplier) {
        if (fromPrivilegedListener) {
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
        Map<String, Object> activation;
        try {
            activation = activationSupplier.get();
        } catch (Throwable t) {
            // Codex deep-audit P0 fix: a throwing activation supplier MUST NOT
            // propagate into the request thread. The most realistic failure mode
            // is ApiMessageActivation walking a request whose accessor blows up
            // (Optional/Stream accessor, partially-constructed protocol object,
            // version-specific schema mismatch). Without this catch the
            // exception escapes into KafkaApis.handle(), turning a buggy
            // governance extractor into a request-thread crash.
            //
            // We catch Throwable to match the per-rule policy below: a
            // StackOverflowError from a deeply nested ApiMessage walk or an
            // OutOfMemoryError from a giant Records buffer must not be allowed
            // to take the request thread with it either.
            //
            // Posture is "fail-open": a broken governance feature degrades to
            // ALLOW, never to a broker request failure. This matches the
            // per-rule fail-open below ("a buggy rule must not be able to
            // crash the request path") and is the only outcome consistent
            // with that policy — no rule can be evaluated without an
            // activation map, so ALLOW is the only available safe answer.
            LOG.warn("activation supplier failed for apiKey {} — failing open: {}",
                apiKey, t.toString());
            return RuleDecision.ALLOW;
        }
        for (Rule rule : rules) {
            boolean matched;
            try {
                matched = rule.compiled().evalBoolean(activation::get);
            } catch (Throwable t) {
                // Catch Throwable, not just RuntimeException: a pathological CEL
                // expression can raise StackOverflowError (deep comprehensions),
                // OutOfMemoryError (huge string ops), or other Error subclasses.
                // The request thread must never die because of a buggy rule —
                // log loudly and treat the rule as ALLOW, then move to the next.
                LOG.warn("rule '{}' failed open due to evaluation error on apiKey {}: {}",
                    rule.id(), apiKey, t.toString());
                continue;
            }
            if (matched) {
                return RuleDecision.deny(rule.errorCode(), rule.id());
            }
        }
        return RuleDecision.ALLOW;
    }

    /**
     * Cheap fast-path guard: returns {@code true} only if the active snapshot
     * has at least one DENY rule that <em>could</em> apply to this request.
     * Callers use this to skip allocating an activation supplier closure on
     * the request hot path when there is no possible deny outcome.
     *
     * <p>Specifically, returns {@code false} when either:
     * <ul>
     *   <li>the request arrived on a privileged (inter-broker) listener — the
     *       network layer sets {@code fromPrivilegedListener} for those and
     *       external clients cannot forge it; or</li>
     *   <li>no DENY rule in the active snapshot targets this API key.</li>
     * </ul>
     *
     * <p>This method makes no allocations and does no reflection. Wire it
     * directly into {@code KafkaApis.handle()} as the gate around the
     * activation-supplier lambda.
     */
    public boolean mayDeny(ApiKeys apiKey, boolean fromPrivilegedListener) {
        if (fromPrivilegedListener) {
            return false;
        }
        return active.get().hasDenyRuleFor(apiKey.id);
    }
}
