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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.server.rules.cel.CelProgram;
import org.apache.kafka.server.rules.extract.ActivationBudgetExceededException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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

    /**
     * Synthetic rule id surfaced in audit logs (and {@link RuleDecision#denyingRuleId()})
     * when the engine fails a request closed because the activation walker
     * exhausted its accessor budget — see
     * {@link ActivationBudgetExceededException} for the policy rationale. The
     * matching {@code "__name__"} shape is rejected by
     * {@link org.apache.kafka.server.rules.json.RuleJsonCodec#decode} as a
     * reserved engine-internal sentinel shape, so this id cannot collide with
     * any rule an operator publishes to the {@code __governance} topic — an
     * audit consumer that sees this denying-rule-id can attribute the DENY
     * unambiguously to engine posture.
     */
    public static final String ACTIVATION_BUDGET_RULE_ID = "__activation-budget-exceeded__";

    /**
     * Per-thread "currently inside evaluate()" flag — guards against re-entrant
     * evaluation from within the activation supplier or (hypothetically) from
     * within a rule predicate.
     *
     * <p>The per-request CEL step budget ({@code CelLimits.MAX_EVAL_STEPS}) is
     * reset on the way in and again in a {@code finally} on the way out of
     * {@link #evaluate}. A re-entrant call would call
     * {@code CelProgram.resetEvalStepBudget()} on the inner entry, silently
     * granting the outer evaluation a fresh 100k-step budget once the inner
     * returned — turning the per-request DoS guarantee into a per-call
     * guarantee with no upper bound on calls. The bound on stack depth (each
     * frame still pays its own steps) would not save the budget; the budget
     * is the gate.
     *
     * <p>Today re-entry is structurally impossible: the supported CEL subset
     * has no user-defined functions and the production activation supplier
     * (the {@code ApiMessageActivation} reflective walker) cannot reach back
     * into {@code RuleEngine}. This guard is forward-looking defence-in-depth
     * — a future contributor extending the CEL subset, adding a
     * principal-attribute provider, or wiring in a custom activation supplier
     * will trip it loudly with a message pointing at the cause, instead of
     * silently breaking the budget guarantee.
     *
     * <p>{@link ThreadLocal#remove()} is called in the outer {@code finally}
     * so a thread pool used by a future caller does not carry a stale
     * {@code TRUE} into a subsequent request on the same thread.
     */
    private static final ThreadLocal<Boolean> IN_EVALUATE =
        ThreadLocal.withInitial(() -> Boolean.FALSE);

    private final AtomicReference<RuleSet> active = new AtomicReference<>(RuleSet.EMPTY);

    /**
     * Throttle for the fail-closed budget-overflow WARN. The fail-closed
     * posture is the right answer to attacker-shaped wide requests
     * (see {@link ActivationBudgetExceededException}), but writing one
     * synchronous SLF4J WARN per request gives the same attacker an
     * unbounded log-spam channel: a few thousand pathological requests per
     * second saturates the broker's logger appender, drives I/O on the log
     * volume that competes with the broker's data path, and stalls the
     * request thread on appender backpressure.
     *
     * <p>The pattern mirrors {@code BrokerGovernanceBootstrap.maybeWarnSuppressed}:
     * the first event in each window emits a single WARN that includes the
     * count of events suppressed in the previous window. Subsequent events
     * in the same window only increment a counter. The window is one second
     * — large enough to bound log volume to ~1 line/s even under sustained
     * attack, small enough that an operator scanning logs sees the event
     * promptly when it first starts.
     *
     * <p>{@code lastBudgetWarnNanos} is the {@link System#nanoTime} of the
     * last emitted WARN (initialised to 0 so the very first event always
     * fires immediately). {@code suppressedBudgetWarnings} accumulates
     * intermediate events; it is read and zeroed atomically when a WARN
     * does fire, so the count is exactly "what happened since the last
     * line written to the log". Both fields use atomics because evaluate()
     * runs on every request thread concurrently.
     *
     * <p>Visible for testing as package-private so test code can read the
     * suppression counter without driving a slow real-time wall-clock test.
     */
    private final AtomicLong lastBudgetWarnNanos = new AtomicLong(0L);
    final AtomicLong suppressedBudgetWarnings = new AtomicLong(0L);
    static final long BUDGET_WARN_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);

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
     * <p>Codex round-3 P1: {@link SecurityUtils#parseKafkaPrincipal(String)}
     * only requires the string to contain a single {@code ':'} — it does NOT
     * reject empty {@code principalType} or empty {@code name}. So
     * {@code ":broker"} parses to a principal with empty type, and
     * {@code "User:"} parses to a principal with empty name. Both forms
     * would almost certainly silently under-grant the bypass (the runtime
     * peer principal is never reported with an empty component). We reject
     * them here so an operator typo fails startup instead of producing an
     * unreachable allow-list entry.
     *
     * <p>Codex round-4 F2: an entry like {@code "User :broker"} or
     * {@code "User: broker"} also parses to a non-empty principal whose
     * canonical form (literal {@code "User :broker"} with the inner space)
     * can never match a runtime peer principal — {@code KafkaApis}
     * canonicalises it as {@code getPrincipalType() + ":" + getName()},
     * which never carries surrounding whitespace inside a component. Such
     * entries produce a non-empty allow-list (so the BrokerServer empty-set
     * guard does NOT fire) yet are silently unreachable — an effective
     * empty allow-list and exactly the failure mode the empty-set guard
     * exists to prevent. We reject any component containing whitespace
     * recognised by either {@link Character#isWhitespace(int)} or
     * {@link Character#isSpaceChar(int)} — the union catches both
     * ASCII whitespace and the non-breaking variants (NBSP U+00A0,
     * NARROW NBSP U+202F, FIGURE SPACE U+2007) that the JDK's
     * {@code isWhitespace} historically excludes, so {@code String.isBlank()}
     * and {@code String.strip()} alone are not sufficient.
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
     *         parse as a Kafka principal, or has blank principal type / name
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
            // on a missing ':' separator. We let that propagate so broker
            // startup fails loudly.
            KafkaPrincipal principal = SecurityUtils.parseKafkaPrincipal(trimmed);
            // Codex round-3 P1 + round-4 F2: SecurityUtils does not validate
            // that the type and name are non-blank, nor that they have no
            // inner whitespace. Reject both failure modes here — the runtime
            // peer principal never carries a blank component, and the
            // canonical form `getPrincipalType() + ":" + getName()` never
            // contains inner whitespace either. So `":broker"`, `"User:"`,
            // `"User :broker"`, `"User: broker"`, `"User: "` and
            // similar all parse non-empty but their canonical form is
            // unreachable, producing an effective empty allow-list that
            // bypasses the BrokerServer empty-set guard.
            //
            // Reject empty, all-whitespace, or leading/trailing-whitespace
            // components. Do NOT reject INTERNAL whitespace: SSL broker
            // principals legitimately contain spaces inside the DN — the
            // default DefaultKafkaPrincipalBuilder uses
            // `X500Principal.getName()` which produces strings like
            // `CN=Broker One,OU=Kafka Brokers,O=Example Corp,C=US` where the
            // spaces inside the CN value are part of the canonical runtime
            // peer principal. Earlier rounds of this fix rejected ALL
            // whitespace and would have refused valid SSL operator configs
            // at startup (Codex round-5 P1).
            //
            // Use the union of Character.isWhitespace AND
            // Character.isSpaceChar so non-breaking Unicode spaces (NBSP
            // U+00A0, NARROW NBSP U+202F, FIGURE SPACE U+2007) are also
            // caught at the leading/trailing positions and in the
            // all-whitespace check — String.isBlank()/strip() miss those.
            String type = principal.getPrincipalType();
            String name = principal.getName();
            if (type.isEmpty() || isAllWhitespace(type) || hasLeadingOrTrailingWhitespace(type)) {
                throw new IllegalArgumentException(
                    "governance.bypass.principals entry has blank, whitespace-"
                    + "only or whitespace-padded principal type: '" + trimmed
                    + "'. Format is `type:name` (eg. `User:broker`); the type "
                    + "must be non-empty and must not start or end with "
                    + "whitespace (ASCII or Unicode). Internal whitespace is "
                    + "allowed (eg. inside an SSL DN).");
            }
            if (name.isEmpty() || isAllWhitespace(name) || hasLeadingOrTrailingWhitespace(name)) {
                throw new IllegalArgumentException(
                    "governance.bypass.principals entry has blank, whitespace-"
                    + "only or whitespace-padded principal name: '" + trimmed
                    + "'. Format is `type:name` (eg. `User:broker`); the name "
                    + "must be non-empty and must not start or end with "
                    + "whitespace (ASCII or Unicode). Internal whitespace is "
                    + "allowed (eg. SSL DNs like `CN=Broker One,OU=...`).");
            }
            out.add(principal.toString());
        }
        return Collections.unmodifiableSet(out);
    }

    /**
     * True if {@code c} is recognised as whitespace by either
     * {@link Character#isWhitespace(int)} (covers ASCII space, tab,
     * newline, and the Unicode SPACE_SEPARATOR / LINE_SEPARATOR /
     * PARAGRAPH_SEPARATOR categories <i>except</i> the non-breaking
     * variants) or {@link Character#isSpaceChar(int)} (covers the
     * non-breaking variants: NBSP U+00A0, FIGURE SPACE U+2007, NARROW
     * NO-BREAK SPACE U+202F). The union is what an operator would call
     * "any whitespace-looking character".
     *
     * <p>We need both predicates because the JDK's {@code isWhitespace}
     * historically excludes the non-breaking variants for compatibility
     * with legacy formatting rules, so {@code String.isBlank()} and
     * {@code String.strip()} also miss them. An entry like
     * {@code User:[NBSP]} would slip past those checks even though its
     * canonical form can never match a runtime peer principal.
     */
    private static boolean isAnyWhitespaceCodePoint(int c) {
        return Character.isWhitespace(c) || Character.isSpaceChar(c);
    }

    /** True if {@code s} is non-empty and every code point is whitespace. */
    private static boolean isAllWhitespace(String s) {
        return !s.isEmpty()
            && s.codePoints().allMatch(RuleEngine::isAnyWhitespaceCodePoint);
    }

    /**
     * True if {@code s}'s first or last code point is whitespace. Internal
     * whitespace is NOT flagged here — that is intentional, see the caller
     * in {@link #parseBypassPrincipals(String)}.
     */
    private static boolean hasLeadingOrTrailingWhitespace(String s) {
        if (s.isEmpty()) {
            return false;
        }
        int first = s.codePointAt(0);
        int last = s.codePointBefore(s.length());
        return isAnyWhitespaceCodePoint(first) || isAnyWhitespaceCodePoint(last);
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
        // Re-entry guard — see IN_EVALUATE javadoc. Checked at function entry
        // so the throw lands BEFORE any budget reset or supplier invocation,
        // which means an inner re-entrant call does not corrupt the outer
        // evaluation's state. The early-return paths below (bypass, no-rule,
        // empty-rule-list) never set IN_EVALUATE, so re-entrant short-circuits
        // remain valid — only a re-entrant call that would actually reach the
        // budget-reset or rule loop trips here.
        if (Boolean.TRUE.equals(IN_EVALUATE.get())) {
            throw new IllegalStateException(
                "RuleEngine.evaluate must not be called recursively on the same "
                    + "thread. The per-request CEL step budget guarantee depends "
                    + "on a single evaluate() entry per request thread; a "
                    + "re-entrant call would reset the outer evaluation's budget. "
                    + "Re-entry detected on apiKey=" + apiKey + ". This usually "
                    + "indicates an activation supplier or rule predicate that "
                    + "calls back into the engine — neither is supported.");
        }
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
        // Past this point we touch the per-request CEL step budget and call
        // the activation supplier. Set the guard NOW so any re-entry from
        // either trips the check at the top — and clear it in the outer
        // finally so a pool thread does not carry a stale TRUE to the next
        // request.
        IN_EVALUATE.set(Boolean.TRUE);
        try {
            Map<String, Object> activation;
            try {
                activation = activationSupplier.get();
            } catch (ActivationBudgetExceededException budget) {
                // Attacker-shaped wide request: the walker exhausted its
                // MAX_ACCESSOR_INVOCATIONS budget (e.g. an OffsetFetch with 10000+
                // partition indexes, a CreateTopics with 10000+ topic descriptors,
                // a JoinGroup with 5000+ protocols). Falling through to the generic
                // Throwable branch below would fail OPEN, which is the right answer
                // for a buggy extractor (broker bug should not 503 every request)
                // but the wrong answer here: any external client could then evade
                // every DENY rule on the targeted API simply by inflating a single
                // repeated field past the budget, since the budget cap is exactly
                // what makes worst-case walk cost bounded.
                //
                // Fail CLOSED with a synthetic DENY whose error code is
                // POLICY_VIOLATION (44) and whose denyingRuleId is the reserved
                // sentinel ACTIVATION_BUDGET_RULE_ID — codec rules forbid operator
                // rule ids that start or end with "__", so an audit consumer can
                // attribute this DENY to the engine's defensive posture without
                // ambiguity. See ActivationBudgetExceededException javadoc for the
                // full broker-bug-vs-attacker-shape policy distinction.
                //
                // Throttle the WARN: the request itself is fail-closed, but writing
                // one synchronous SLF4J line per attacker request would re-open the
                // log-spam DoS vector that the fail-closed posture is designed to
                // shut. One line per window with the suppression count is enough
                // for an operator to notice the event start; subsequent attacker
                // requests in the same window only bump the counter.
                maybeWarnBudgetExceeded(apiKey, budget);
                return RuleDecision.deny(Errors.POLICY_VIOLATION.code(), ACTIVATION_BUDGET_RULE_ID);
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
                //
                // The attacker-shape exception above is caught FIRST so it never
                // falls through this generic branch; see that catch's comment for
                // why budget overflow needs the opposite posture.
                LOG.warn("activation supplier failed for apiKey {} — failing open: {}",
                    apiKey, t.toString());
                return RuleDecision.ALLOW;
            }
            // Round-8 audit HIGH (concurrency): the CEL eval-step budget is
            // per-request, not per-rule. Resetting here (and not inside
            // CelProgram.evalBoolean) means all rules targeting this api-key
            // share the single MAX_EVAL_STEPS ceiling. With the per-api-key cap
            // of 128 rules, the pre-fix per-rule reset gave a single request up
            // to 128 * 100k = 12.8M CEL steps of legitimate budget — a
            // published-rule-shaped DoS amplifier. With one reset per request,
            // the bound is the documented 100k regardless of how many rules an
            // operator has authored. Reset both before and after in a
            // try/finally so a throwing evaluation (CelEvaluationException,
            // StackOverflowError, OutOfMemoryError) cannot poison the next
            // request's budget on the same broker thread.
            CelProgram.resetEvalStepBudget();
            try {
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
                        //
                        // Note that after one rule trips the per-request budget, the
                        // next rule in this loop will retrip on entry under the shared
                        // counter and also land here as fail-open. That is the intended
                        // DoS-defence behaviour: a request cannot multiply the step
                        // budget by the number of rules an operator happens to have
                        // published. See CelLimits.resetSteps javadoc.
                        LOG.warn("rule '{}' failed open due to evaluation error on apiKey {}: {}",
                            rule.id(), apiKey, t.toString());
                        continue;
                    }
                    if (matched) {
                        return RuleDecision.deny(rule.errorCode(), rule.id());
                    }
                }
                return RuleDecision.ALLOW;
            } finally {
                CelProgram.resetEvalStepBudget();
            }
        } finally {
            // Pair with IN_EVALUATE.set(TRUE) above. remove() (not set(FALSE))
            // so the ThreadLocal slot is released back to the GC when the
            // thread is parked between requests; a pool thread observing
            // initialValue=FALSE on its next request is equivalent to
            // observing the explicitly-cleared FALSE here.
            IN_EVALUATE.remove();
        }
    }

    /**
     * Backwards-compatible 4-argument form: passes {@code null} as the
     * principal name. Codex round-3 P0: production deployments always
     * configure a non-empty {@code governance.bypass.principals} (broker
     * startup refuses an empty list), so this overload never grants the
     * bypass at run-time — a {@code null} principal cannot match any entry
     * in the allow-list. Callers that need the principal narrowing to
     * actually take effect MUST use the 5-argument form. Retained so
     * existing governance unit tests (which test rule semantics, not the
     * bypass) need not change in lockstep with the engine surface change.
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

    /**
     * Emit a throttled WARN for activation budget overflow. See the field
     * comment on {@link #lastBudgetWarnNanos} for the threat model. The
     * compareAndSet on {@code lastBudgetWarnNanos} guarantees at most one
     * thread per window wins the emission slot — losing threads only bump
     * the suppressed counter, never block waiting for the appender.
     */
    private void maybeWarnBudgetExceeded(ApiKeys apiKey, ActivationBudgetExceededException budget) {
        long now = System.nanoTime();
        long last = lastBudgetWarnNanos.get();
        if (now - last >= BUDGET_WARN_INTERVAL_NANOS
            && lastBudgetWarnNanos.compareAndSet(last, now)) {
            long suppressed = suppressedBudgetWarnings.getAndSet(0L);
            if (suppressed > 0) {
                LOG.warn("activation budget exceeded on apiKey {} — failing closed (POLICY_VIOLATION), "
                    + "suppressed {} similar events in the previous window: {}",
                    apiKey, suppressed, budget.getMessage());
            } else {
                LOG.warn("activation budget exceeded on apiKey {} — failing closed (POLICY_VIOLATION): {}",
                    apiKey, budget.getMessage());
            }
        } else {
            suppressedBudgetWarnings.incrementAndGet();
        }
    }
}
