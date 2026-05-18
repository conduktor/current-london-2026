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
import org.apache.kafka.common.utils.SecurityUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
 * <p>{@link #evaluate(ApiKeys, String, String, boolean, Supplier)} is on the
 * hot path of {@code KafkaApis.handle()} and is designed to do the absolute
 * minimum work when no rule targets the request:
 *
 * <ol>
 *   <li>If the request arrived on a <em>privileged listener</em>
 *       (typically the broker's inter-broker listener) AND the peer
 *       principal is in the trusted-bypass allow-list configured at
 *       construction time, short-circuit to ALLOW. This is the
 *       bootstrap-safety hatch: the broker's own consumer of the governance
 *       topic must never be rule-blocked, even under a misconfigured "deny
 *       everything" rule set. The {@code fromPrivilegedListener} bit is set
 *       by the network layer based on which TCP listener accepted the
 *       connection — clients cannot forge it. The principal check is the
 *       defence-in-depth against a misconfiguration where the operator
 *       points the inter-broker listener at a listener also accepting
 *       client traffic: even then, only requests whose authenticated
 *       principal appears in the allow-list ride the bypass. When the
 *       allow-list is empty (legacy construction), the principal check is
 *       skipped — a WARN is logged at construction time so the operator
 *       sees the gap.</li>
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
     * Allow-list of principal strings (e.g. {@code "User:broker"}) that may
     * exercise the privileged-listener bypass. Sourced from the dedicated
     * {@code governance.bypass.principals} broker config — independent of
     * {@code super.users} by design (Codex P1#1 deep-audit finding):
     *
     * <ul>
     *   <li>{@code super.users} is a broad authorization concept tied to the
     *       authorizer. Coupling the bypass to it over-grants the bypass to
     *       non-broker super-users that happen to reach the inter-broker
     *       listener, and under-grants when the broker is ACL-authorized but
     *       not enrolled in super.users.</li>
     *   <li>{@code governance.bypass.principals} is a narrow identity concept
     *       used only to protect broker-internal traffic (replica fetchers,
     *       __governance log consumer, KRaft metadata fetches) from being
     *       blocked by a misconfigured DENY rule.</li>
     * </ul>
     *
     * <p>An empty set means <strong>no principal</strong> can exercise the
     * bypass; ALL traffic — including inter-broker — is subject to rule
     * evaluation. This is the strict, fail-closed posture. Operators MUST
     * enroll the broker's own principal explicitly for steady-state safety.
     *
     * <p>The set is constructor-immutable. Operators rotate broker
     * credentials by restarting brokers, so a hot-reload knob would buy
     * nothing operational while widening the in-memory surface a runaway
     * thread could observe at the wrong moment.
     */
    private final Set<String> trustedBypassPrincipals;

    /**
     * Backwards-compatible constructor for tests and old callers that don't
     * configure a trusted-principal allow-list. Equivalent to
     * {@code new RuleEngine(Collections.emptySet())}: the privileged-listener
     * bypass is denied for every principal. New production code must pass
     * the operator's parsed {@code governance.bypass.principals} set so the
     * broker's own principal can ride the bypass.
     */
    public RuleEngine() {
        this(Collections.emptySet());
    }

    /**
     * Construct an engine that requires {@code principalName ∈ trustedBypassPrincipals}
     * in addition to {@code fromPrivilegedListener=true} to grant the bypass.
     * Pass {@code Collections.emptySet()} to refuse the bypass for every
     * principal — every request, even from a privileged listener, is then
     * subject to rule evaluation.
     *
     * <p>The required source for {@code trustedBypassPrincipals} is the
     * dedicated {@code governance.bypass.principals} broker config, parsed
     * via {@link #parseBypassPrincipals(String)} so that a malformed entry
     * fails broker startup rather than silently dropping.
     */
    public RuleEngine(Set<String> trustedBypassPrincipals) {
        this.trustedBypassPrincipals = Set.copyOf(trustedBypassPrincipals);
        if (this.trustedBypassPrincipals.isEmpty()) {
            LOG.warn("RuleEngine constructed with empty governance.bypass.principals — "
                + "no principal can ride the privileged-listener bypass. "
                + "ALL traffic, including inter-broker (replica fetchers, "
                + "__governance log consumer, KRaft metadata fetches), is subject "
                + "to CEL rule evaluation. Enroll the broker's own principal in "
                + "governance.bypass.principals for steady-state safety.");
        } else {
            LOG.info("RuleEngine privileged-listener bypass narrowed to principals: {}",
                this.trustedBypassPrincipals);
        }
    }

    /**
     * Parse the value of the {@code governance.bypass.principals} config.
     *
     * <p>Format: semicolon-separated list of Kafka principals, each parseable
     * by {@link SecurityUtils#parseKafkaPrincipal(String)} (e.g.
     * {@code "User:broker;User:kafka-controller"}). Whitespace around
     * separators is tolerated; empty segments between separators are
     * tolerated and skipped (so {@code "User:a; ;User:b"} is equivalent to
     * {@code "User:a;User:b"}).
     *
     * <p><strong>Fails fast on any malformed entry</strong> — throws
     * {@link IllegalArgumentException} so the broker refuses to start rather
     * than silently dropping the entry and producing an under-protected
     * runtime. This matches upstream {@code StandardAuthorizer}'s
     * super.users parsing, which also throws on malformed entries.
     *
     * <p>Returns the canonical string form of each parsed principal
     * (mirroring how the network layer reports the authenticated peer
     * principal at request time) so that {@link #bypassIsAuthorisedFor(String)}
     * can do a verbatim string-equals match.
     *
     * @param raw the config value (may be {@code null} or empty — both
     *            return an empty set, granting no bypass)
     * @return canonical principal strings; never {@code null}
     * @throws IllegalArgumentException if any non-empty segment fails to
     *         parse as a Kafka principal
     */
    public static Set<String> parseBypassPrincipals(String raw) {
        if (raw == null) {
            return Collections.emptySet();
        }
        Set<String> out = new LinkedHashSet<>();
        for (String segment : raw.split(";")) {
            String trimmed = segment.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            // SecurityUtils.parseKafkaPrincipal throws IllegalArgumentException
            // on a malformed entry (no ':' separator, empty type, etc.). We let
            // that propagate so broker startup fails loudly.
            String canonical = SecurityUtils.parseKafkaPrincipal(trimmed).toString();
            out.add(canonical);
        }
        return Collections.unmodifiableSet(out);
    }

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
     * @param principalName the authenticated peer principal as a string (e.g.
     *                 {@code "User:broker"}); may be null when the request is
     *                 unauthenticated. Authoritative for narrowing the
     *                 privileged-listener bypass.
     * @param fromPrivilegedListener {@code true} iff the request arrived on a
     *                 listener the broker treats as inter-broker. Necessary
     *                 but not sufficient for the bypass: if the engine was
     *                 constructed with a non-empty trusted-bypass principal
     *                 allow-list, the {@code principalName} must also appear
     *                 there. External clients cannot forge
     *                 {@code fromPrivilegedListener} because the network
     *                 layer derives it from the accepting listener, not the
     *                 wire payload.
     * @param activationSupplier lazy builder of the CEL activation map; only
     *                           invoked if at least one rule targets the API
     *                           key, and only once per call regardless of how
     *                           many rules fire
     * @return {@link RuleDecision#ALLOW} unless a DENY rule matches
     */
    public RuleDecision evaluate(ApiKeys apiKey,
                                 String clientId,
                                 String principalName,
                                 boolean fromPrivilegedListener,
                                 Supplier<Map<String, Object>> activationSupplier) {
        if (fromPrivilegedListener && bypassIsAuthorisedFor(principalName)) {
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
     * Backwards-compatible 4-argument form: passes {@code null} as the
     * principal name. Equivalent to legacy behaviour iff the engine was
     * constructed without a trusted-bypass principal set — in that case the
     * principal is ignored anyway. When a trusted set IS configured this
     * form will never grant the bypass (null principal cannot match), so
     * callers that want the principal narrowing to actually take effect
     * must use the 5-argument form. Provided so existing governance unit
     * tests need not change in lockstep with the engine surface change.
     */
    public RuleDecision evaluate(ApiKeys apiKey,
                                 String clientId,
                                 boolean fromPrivilegedListener,
                                 Supplier<Map<String, Object>> activationSupplier) {
        return evaluate(apiKey, clientId, null, fromPrivilegedListener, activationSupplier);
    }

    /**
     * Cheap fast-path guard: returns {@code true} when the active snapshot
     * <em>might</em> deny this request. Callers use this to skip allocating
     * an activation supplier closure when no deny outcome is possible.
     *
     * <p>Important: this guard does NOT short-circuit on
     * {@code fromPrivilegedListener=true}. The full bypass is principal-aware
     * (see {@link #evaluate}) and the principal is not in scope at this
     * fast-path call site without an extra lookup. Returning {@code true} on
     * the privileged listener is harmless: the call site re-checks the
     * authoritative bypass inside {@link #evaluate} and short-circuits there
     * for legitimate broker traffic. The cost is one extra string-compare
     * against the (typically tiny) trusted-principal set for the small
     * subset of requests on the privileged listener — far below the cost of
     * actually denying a misrouted external client.
     */
    public boolean mayDeny(ApiKeys apiKey, boolean fromPrivilegedListener) {
        return active.get().hasDenyRuleFor(apiKey.id);
    }

    /**
     * Returns true when the privileged-listener bypass is authorised for the
     * given peer principal. Strict, fail-closed semantics: an empty allow-list
     * grants the bypass to nobody (so every request, including inter-broker,
     * is subject to rule evaluation). When an allow-list IS configured, the
     * principal must appear in it — matched as a verbatim string against the
     * principal's {@code toString()} representation (e.g. {@code "User:broker"}).
     *
     * <p>Codex P1#1 fix: previous behaviour fell back to listener-only when
     * the allow-list was empty, which silently re-introduced the very gap the
     * dedicated config exists to close (any client reaching a shared listener
     * would ride the bypass). Empty now means no bypass.
     */
    private boolean bypassIsAuthorisedFor(String principalName) {
        if (trustedBypassPrincipals.isEmpty()) {
            return false;
        }
        return principalName != null && trustedBypassPrincipals.contains(principalName);
    }
}
