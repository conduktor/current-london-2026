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

import javax.security.auth.x500.X500Principal;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;

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
     * Sentinel client-id prefix that an attacker might plausibly set on the
     * wire to try to spoof "I am the broker reading my own governance topic".
     * NO PRODUCTION CALLER constructs this string today: the broker reads
     * {@code __governance} directly via {@code ReplicaManager.getLog}, with no
     * consumer and no client-id on the wire. This constant therefore exists
     * solely as a defensive-test fixture — see
     * {@code RuleEngineTest#spoofedInternalClientIdDoesNotBypassWhenNotOnPrivilegedListener},
     * which pins that an external client setting
     * {@code client.id=__kafka-governance-attacker} does NOT bypass any rule.
     * The actual bypass is granted by the {@code fromPrivilegedListener} flag
     * the network layer attaches to every request based on which TCP listener
     * accepted the connection, which external clients cannot forge.
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

    /**
     * Marker subclass of {@link IllegalStateException} thrown by the re-entry
     * guard. Distinguishes the engine's own engine-state signal from operator-
     * authored rule failures that legitimately throw {@code IllegalStateException}
     * (e.g. a buggy CEL host function or a misauthored accessor). The per-rule
     * fail-OPEN catch in {@link #evaluate} swallows generic
     * {@code IllegalStateException} (so a buggy rule cannot deny every
     * request) but selectively re-throws this subclass so a re-entrant engine
     * call surfaces as a 5xx in {@code KafkaApis} instead of silently advancing
     * to ALLOW. Public callers can still catch {@code IllegalStateException}
     * — the marker IS-A IllegalStateException; only the dispatch inside
     * {@link #evaluate} treats it specifically. See R23 #224.
     */
    static final class EvaluateReentryException extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        EvaluateReentryException(String msg) {
            super(msg);
        }
    }

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
     *
     * <p>Round-15 recent-changes MED-1: initial value is
     * {@code System.nanoTime() - INTERVAL_NANOS - 1} (not 0) so the very first
     * WARN always fires even when the JVM started within the first second of
     * host uptime — under HotSpot's CLOCK_MONOTONIC-since-boot, {@code now < INTERVAL_NANOS}
     * at that moment makes {@code now - 0 < INTERVAL_NANOS}, which would
     * silently suppress the first observed event. {@code nanoTime() - INTERVAL_NANOS - 1}
     * guarantees the first {@code now - last >= INTERVAL_NANOS} check passes,
     * regardless of the absolute nanoTime origin.
     */
    private final AtomicLong lastBudgetWarnNanos =
        new AtomicLong(System.nanoTime() - BUDGET_WARN_INTERVAL_NANOS - 1);
    final AtomicLong suppressedBudgetWarnings = new AtomicLong(0L);
    static final long BUDGET_WARN_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);

    /**
     * Round-14 BLOCKER L-1 (concurrency sub-agent): throttle the per-rule
     * fail-open WARN on rule evaluation errors. Same window-and-suppressed-count
     * pattern as {@link #lastBudgetWarnNanos} above, applied to the WARN emitted
     * from {@link #evaluate} when a rule throws during {@link
     * org.apache.kafka.server.rules.cel.CelProgram#evalBoolean}. Without this
     * throttle, a single rule that consistently throws (e.g. an OOM-trigger or
     * StackOverflowError-trigger CEL expression) would fire one WARN per
     * request, per rule — a published-rule-shaped log amplifier. With the
     * throttle the broker emits at most one line per window plus a
     * suppressed-count tail.
     */
    private final AtomicLong lastEvalErrorWarnNanos =
        new AtomicLong(System.nanoTime() - EVAL_ERROR_WARN_INTERVAL_NANOS - 1);
    final AtomicLong suppressedEvalErrorWarnings = new AtomicLong(0L);
    static final long EVAL_ERROR_WARN_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);

    /**
     * Round-15 recent-changes BLOCKER-1: throttle the activation-supplier
     * fail-open WARN. Mirrors {@link #lastEvalErrorWarnNanos} above (which
     * fixed the per-rule eval-error path in round-14 L-1) for the structurally
     * identical hot-path WARN emitted when {@code activationSupplier.get()}
     * throws a non-budget exception in {@link #evaluate}.
     *
     * <p>Without this throttle, an attacker who can craft a request that
     * breaks {@link org.apache.kafka.server.rules.extract.ApiMessageActivation}
     * — a version-skew accessor, a partially-constructed protocol object, a
     * malformed nested-collection shape — gets one synchronous SLF4J WARN per
     * request, and {@code t.toString()} carries wire-derived strings (topic
     * names, principal strings, accessor names, exception messages quoting
     * request bytes). Hundreds of requests per second from a single attacker
     * reproduce the BLOCKER-1 log-injection and BLOCKER L-1 appender-DoS
     * vectors on a hotter code path than either of the originals.
     */
    private final AtomicLong lastActivationFailureWarnNanos =
        new AtomicLong(System.nanoTime() - ACTIVATION_FAILURE_WARN_INTERVAL_NANOS - 1);
    final AtomicLong suppressedActivationFailureWarnings = new AtomicLong(0L);
    static final long ACTIVATION_FAILURE_WARN_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);

    /**
     * Round-15 Request-path HIGH-3: monotonically-increasing cumulative
     * counters for engine fail-open / fail-closed events. These are
     * deliberately independent of the {@code suppressedXxxWarnings} counters
     * above — those are window-bound and reset to zero on every emitted WARN,
     * so they can only report rate within a single 1-second window. An
     * operator sampling them externally would see arbitrarily-zeroed values
     * with no useful long-term signal.
     *
     * <p>These three fields, in contrast, accumulate one count per event for
     * the lifetime of the engine instance, independent of throttle gating.
     * They are the data source for any future JMX/metric adapter that wants
     * to expose fail-open and fail-closed rates as broker telemetry —
     * dashboards, alerts, anomaly detection. The adapter itself is not
     * wired up in this change; it can be added later without modifying the
     * engine, by reading the public getters
     * {@link #activationFailureFailOpenCount()},
     * {@link #evalErrorFailOpenCount()}, and
     * {@link #activationBudgetFailClosedCount()}.
     *
     * <p>Each counter is incremented unconditionally on every event entry —
     * BEFORE the window-based WARN gate — so events suppressed by the
     * throttle still register in the cumulative count. The split is:
     *
     * <ul>
     *   <li>{@code activationFailureFailOpenCount}: every time
     *       {@code activationSupplier.get()} threw a non-budget exception
     *       and the request fell open. A non-zero rate here indicates a
     *       walker bug, a partially-constructed protocol object reaching
     *       evaluate(), or a published rule shape that exercises a brittle
     *       accessor.</li>
     *   <li>{@code evalErrorFailOpenCount}: every {@link #evaluate} call
     *       during which at least one rule's {@code evalBoolean} threw and
     *       the engine moved on. Counted PER-EVALUATE, not per-rule —
     *       multiple buggy rules tripping inside a single request still
     *       only bump the counter by one. A non-zero rate here indicates
     *       a published rule with a bug, or a malformed activation map
     *       for an in-effect rule. Per-rule diagnostic detail is in the
     *       throttled WARN log, not in this counter.</li>
     *   <li>{@code activationBudgetFailClosedCount}: every time the walker
     *       exhausted its budget and the request was fail-CLOSED with
     *       POLICY_VIOLATION + {@link #ACTIVATION_BUDGET_RULE_ID}. A
     *       sustained rate here is a deliberate-attacker-shape signal —
     *       the engine is doing exactly what it should, but an operator
     *       wants to see it happening.</li>
     * </ul>
     *
     * <p>{@link AtomicLong} for thread safety on the request hot path. A
     * single contended increment per request is fine — the alternative
     * (a {@code LongAdder}) trades read cost for write cost, and these
     * counters are written far more often than read.
     */
    private final AtomicLong activationFailureFailOpenCount = new AtomicLong(0L);
    private final AtomicLong evalErrorFailOpenCount = new AtomicLong(0L);
    private final AtomicLong activationBudgetFailClosedCount = new AtomicLong(0L);

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
            // startup fails loudly — but we re-throw with `LogSafe.sanitize`
            // applied, because the upstream message format
            // "expected a string in format principalType:principalName but
            // got <str>" embeds the raw operator-supplied value, and that
            // value can contain CR/LF or other control codepoints (no
            // separator means `String.trim()` at the top of the loop only
            // strips edge whitespace, so interior CR/LF survive). The fatal
            // SLF4J log line emitted from BrokerServer.startup must not be
            // forgeable by a malicious config value. Round-20 HIGH A-1.
            KafkaPrincipal principal;
            try {
                principal = SecurityUtils.parseKafkaPrincipal(trimmed);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "governance.bypass.principals entry is not in `type:name` "
                    + "format: '" + LogSafe.sanitize(trimmed) + "' (e.g. "
                    + "`User:broker`).");
            }
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
                // Round-20 HIGH A-1: `trimmed` is operator-supplied config
                // and `String.trim()` at the top of the loop only strips
                // edge ASCII whitespace — interior \r/\n survive (e.g.
                // `raw = "User\r:broker"` → type="User\r" trips the
                // trailing-whitespace branch with `\r` still embedded in
                // `trimmed`). Sanitise before interpolating so the fatal
                // SLF4J log line emitted from BrokerServer.startup cannot
                // be forged by a malicious config value.
                throw new IllegalArgumentException(
                    "governance.bypass.principals entry has blank, whitespace-"
                    + "only or whitespace-padded principal type: '"
                    + LogSafe.sanitize(trimmed)
                    + "'. Format is `type:name` (eg. `User:broker`); the type "
                    + "must be non-empty and must not start or end with "
                    + "whitespace (ASCII or Unicode). Internal whitespace is "
                    + "allowed (eg. inside an SSL DN).");
            }
            if (name.isEmpty() || isAllWhitespace(name) || hasLeadingOrTrailingWhitespace(name)) {
                // Round-20 HIGH A-1: see type-branch comment above for the
                // sanitisation rationale.
                throw new IllegalArgumentException(
                    "governance.bypass.principals entry has blank, whitespace-"
                    + "only or whitespace-padded principal name: '"
                    + LogSafe.sanitize(trimmed)
                    + "'. Format is `type:name` (eg. `User:broker`); the name "
                    + "must be non-empty and must not start or end with "
                    + "whitespace (ASCII or Unicode). Internal whitespace is "
                    + "allowed (eg. SSL DNs like `CN=Broker One,OU=...`).");
            }
            // R32 #287 [HIGH]: KafkaPrincipal type is case-sensitive at
            // runtime. KafkaApis builds `principalName = getPrincipalType() +
            // ":" + getName()` and compares via String.equals against this
            // set. Every stock KafkaPrincipalBuilder emits the literal
            // constant KafkaPrincipal.USER_TYPE = "User" (see
            // DefaultKafkaPrincipalBuilder L84/L94/L104/L106, SASL/PLAIN/
            // GSSAPI/SCRAM/SSL all funnel through it; PLAINTEXT uses the
            // KafkaPrincipal.ANONYMOUS constant with the same "User" type).
            // A case-variant typo like `user:broker`, `USER:broker`, or
            // `User:Broker` (the name capitalisation, caught implicitly by
            // not matching the runtime peer's name) parses cleanly here but
            // its stored canonical form never matches the runtime peer ⇒
            // silent fail-CLOSED soft-brick of the inter-broker bypass —
            // same impact class as R29 #270/#278, R30 #280, R31 #281/#284.
            //
            // Scope: this check only catches case-variant typos of the
            // "User" type. A custom KafkaPrincipalBuilder that legitimately
            // emits a non-"User" type (eg. "Group", "Role", a vendor-
            // specific identity class) is allowed through unchanged — we
            // refuse to whitelist a closed type-set because the
            // KafkaPrincipalBuilder SPI is intentionally open. A `User`
            // case-mismatch, however, is unambiguously a typo: if the
            // operator meant a custom type, they would have spelled it
            // with deliberate capitalisation, not by mangling "User".
            if (KafkaPrincipal.USER_TYPE.equalsIgnoreCase(type)
                    && !KafkaPrincipal.USER_TYPE.equals(type)) {
                throw new IllegalArgumentException(
                    "governance.bypass.principals entry has case-mismatched "
                    + "principal type '" + LogSafe.sanitize(type)
                    + "': KafkaPrincipal type is case-sensitive at runtime "
                    + "and every stock KafkaPrincipalBuilder emits the "
                    + "exact constant '" + KafkaPrincipal.USER_TYPE
                    + "' (capital U). The stored canonical form '"
                    + LogSafe.sanitize(type + ":" + name)
                    + "' would never match a runtime peer principal — "
                    + "did you mean '" + KafkaPrincipal.USER_TYPE
                    + "'? Entry: '" + LogSafe.sanitize(trimmed) + "'.");
            }
            // Round-19 BLOCKER (R19-A #1 / R19-D BLOCKER-1): PROMPT.md
            // operator contract claims this parser rejects "internal
            // C0/C1/zero-width/bidi codepoints". Without this check, an
            // operator typo embedding an invisible codepoint inside an
            // otherwise-valid component (e.g. `User:broker​` — looks
            // identical to `User:broker` on screen) parses successfully
            // but produces an allow-list entry whose canonical form can
            // never match a runtime peer principal — silent under-grant of
            // the bypass, with no diagnostic. The leading/trailing
            // whitespace check above does NOT catch these: zero-width and
            // bidi codepoints are not classified as whitespace by either
            // Character.isWhitespace or Character.isSpaceChar; C0 and C1
            // control chars are reachable in the interior because the
            // whitespace check only inspects the first and last code
            // points. Reject any internal occurrence and surface the
            // failure at broker startup, where it can be fixed.
            String typeInvisible = firstInvisibleCodePointLabel(type);
            if (typeInvisible != null) {
                // Round-20 HIGH A-1: the offending entry is operator-supplied
                // config that has just been shown to contain a CR/LF/other
                // control codepoint. This exception propagates to
                // BrokerServer.startup's fatal(...) log via SLF4J; without
                // LogSafe.sanitize the embedded `trimmed` would carry that
                // codepoint into the startup log and could forge a second
                // log line (CR/LF) or smuggle bidi/ANSI bytes. The codepoint
                // label itself (`U+%04X` formatted) is already safe.
                throw new IllegalArgumentException(
                    "governance.bypass.principals entry has invisible "
                    + "codepoint (" + typeInvisible + ") in principal "
                    + "type: '" + LogSafe.sanitize(trimmed) + "'. Invisible "
                    + "codepoints (C0/C1 controls, zero-width, bidi "
                    + "overrides) produce an allow-list entry whose "
                    + "canonical form can never match a runtime peer "
                    + "principal; reject at startup rather than "
                    + "under-granting the bypass silently.");
            }
            String nameInvisible = firstInvisibleCodePointLabel(name);
            if (nameInvisible != null) {
                // Round-20 HIGH A-1: see typeInvisible branch above for the
                // sanitisation rationale.
                throw new IllegalArgumentException(
                    "governance.bypass.principals entry has invisible "
                    + "codepoint (" + nameInvisible + ") in principal "
                    + "name: '" + LogSafe.sanitize(trimmed) + "'. Invisible "
                    + "codepoints (C0/C1 controls, zero-width, bidi "
                    + "overrides) produce an allow-list entry whose "
                    + "canonical form can never match a runtime peer "
                    + "principal; reject at startup rather than "
                    + "under-granting the bypass silently.");
            }
            // R30 #280 [HIGH]: detect non-ASCII whitespace codepoints inside
            // the component. The leading/trailing whitespace check above
            // covers edges; the invisible-codepoint check covers C0/C1
            // controls and the zero-width/bidi block. Neither catches
            // visible-but-non-ASCII space variants (NBSP U+00A0, NARROW
            // NBSP U+202F, FIGURE SPACE U+2007, IDEOGRAPHIC SPACE U+3000,
            // ...) which render identically to ASCII space in most fonts
            // but are not collapsed at runtime by String.equals. Operator
            // paste-from-word-processor footgun: a DN like
            // `CN=Broker One,...` looks correct on screen, parses
            // successfully here, then NEVER matches the runtime
            // `CN=Broker One,...` peer principal — same soft-brick class
            // as R29 #270/#272/#278.
            String typeNonAsciiWs = firstNonAsciiWhitespaceCodePointLabel(type);
            if (typeNonAsciiWs != null) {
                throw new IllegalArgumentException(
                    "governance.bypass.principals entry has "
                    + typeNonAsciiWs + " in the principal type: '"
                    + LogSafe.sanitize(trimmed) + "'. This codepoint looks "
                    + "like an ASCII space but is not — the entry's "
                    + "canonical form will never match a runtime peer "
                    + "principal (whose DN spaces are ASCII U+0020). "
                    + "Common cause: a paste from a word-processor or web "
                    + "page that auto-replaced ASCII space with a "
                    + "non-breaking variant.");
            }
            String nameNonAsciiWs = firstNonAsciiWhitespaceCodePointLabel(name);
            if (nameNonAsciiWs != null) {
                throw new IllegalArgumentException(
                    "governance.bypass.principals entry has "
                    + nameNonAsciiWs + " in the principal name: '"
                    + LogSafe.sanitize(trimmed) + "'. This codepoint looks "
                    + "like an ASCII space but is not — the entry's "
                    + "canonical form will never match a runtime peer "
                    + "principal (whose DN spaces are ASCII U+0020). "
                    + "Common cause: a paste from a word-processor or web "
                    + "page that auto-replaced ASCII space with a "
                    + "non-breaking variant.");
            }
            // R29 #278 [HIGH]: detect non-ASCII comma/semicolon
            // confusables BEFORE the ASCII-only comma-typo regex below.
            // The invisible-codepoint check above covers smuggled
            // codepoints (C0/C1/zero-width/bidi); the confusable set
            // here covers VISIBLE printable punctuation that the
            // invisible helper deliberately ignores.
            //
            // Hazard: an operator using a CJK / Arabic / Armenian IME
            // can produce `User:admin，User:broker` (U+FF0C FULLWIDTH
            // COMMA, the CJK IME default) or `User:admin；User:broker`
            // (U+FF1B FULLWIDTH SEMICOLON) — neither is ASCII `,` or
            // `;`, so split(";") sees one segment, the regex below
            // sees no ASCII `,`, and the bypass entry is stored
            // intact. The runtime peer principal NEVER matches, and
            // inter-broker traffic silently soft-bricks.
            //
            // Reject any confusable in TYPE or NAME with a label that
            // names the codepoint explicitly so the operator can find
            // and fix it.
            String typeConfusable = firstConfusableSeparatorLabel(type);
            if (typeConfusable != null) {
                throw new IllegalArgumentException(
                    "governance.bypass.principals entry has a "
                    + typeConfusable + " in the principal type: '"
                    + LogSafe.sanitize(trimmed) + "'. This codepoint looks "
                    + "like an ASCII comma/semicolon but is not — the "
                    + "entry parses as a single segment whose canonical "
                    + "form will never match a runtime peer principal. "
                    + "The legitimate entry separator is `;` (ASCII "
                    + "U+003B). Common cause: a CJK / Arabic IME or a "
                    + "paste from a document edited in a non-Latin "
                    + "locale.");
            }
            String nameConfusable = firstConfusableSeparatorLabel(name);
            if (nameConfusable != null) {
                throw new IllegalArgumentException(
                    "governance.bypass.principals entry has a "
                    + nameConfusable + " in the principal name: '"
                    + LogSafe.sanitize(trimmed) + "'. This codepoint looks "
                    + "like an ASCII comma/semicolon but is not — the "
                    + "entry parses as a single segment whose canonical "
                    + "form will never match a runtime peer principal. "
                    + "The legitimate entry separator is `;` (ASCII "
                    + "U+003B). Common cause: a CJK / Arabic IME or a "
                    + "paste from a document edited in a non-Latin "
                    + "locale.");
            }
            // R29 #270 [HIGH]: detect the comma-as-separator typo. Every
            // other Kafka list config (listeners, bootstrap.servers,
            // advertised.listeners, controller.quorum.voters, …) uses
            // comma as the separator. PROMPT.md mandates `;` here because
            // legitimate SSL DN principal names contain commas (e.g.
            // `User:CN=Broker One,OU=Kafka Brokers,O=Example Corp,C=US`).
            // An operator typo using `,` instead of `;` produces ONE
            // segment that parses successfully: split(":", 2) consumes
            // only the first colon, so `"User:admin,User:broker"` becomes
            // type=`User`, name=`admin,User:broker`. The whitespace and
            // invisible-codepoint guards above do not catch commas. The
            // allow-list becomes non-empty so the BrokerServer empty-set
            // startup guard passes. At runtime the broker's own peer
            // principal `User:broker` will NEVER match the literal entry
            // — silently soft-bricking inter-broker traffic with NO
            // startup diagnostic.
            //
            // Discriminator (must not false-positive on SSL DNs):
            //   1. Reject any `,` in TYPE. Principal types are short
            //      identifiers (User/Group/Role/Service Account) and
            //      never contain commas; commas in `type` only appear
            //      via a typo of shape `"User,Group:admin"`.
            //   2. Reject `,<chars-without-`=,:`>:` shape inside NAME.
            //      SSL DN attribute separators use `=` (`,OU=`, `,O=`,
            //      `,C=`, `,EMAILADDRESS=`, …) — never `:` — so
            //      legitimate DNs with embedded commas are NOT affected:
            //      the `=` blocks the run before it can reach a `:`.
            //      The smoking gun for the comma-typo is the colon
            //      after the run (`,User:`, `,Group:`, `,Role:`,
            //      `,Service-Account:`, `,com.example.Principal:`, even
            //      a literal `,:` from a pasted entry separator). The
            //      identifier character class is deliberately
            //      `[^=,:]` rather than `[A-Za-z0-9_]` so that hyphens,
            //      dots, internal spaces, and non-ASCII letters in the
            //      typo'd type are all caught — see R29 #272.
            if (type.indexOf(',') >= 0) {
                throw new IllegalArgumentException(
                    "governance.bypass.principals entry contains a comma "
                    + "in the principal type: '" + LogSafe.sanitize(trimmed)
                    + "'. The separator between entries is `;` (semicolon), "
                    + "NOT `,` (comma) — note this differs from other Kafka "
                    + "list configs (listeners, bootstrap.servers, …) "
                    + "because legitimate SSL DN principal names embed "
                    + "commas (eg. "
                    + "`User:CN=Broker One,OU=Kafka Brokers,O=Example Corp,C=US`).");
            }
            if (COMMA_SEPARATOR_TYPO.matcher(name).find()) {
                throw new IllegalArgumentException(
                    "governance.bypass.principals entry contains "
                    + "`,<identifier>:` inside the principal name, which is "
                    + "the shape of a comma-as-separator typo: '"
                    + LogSafe.sanitize(trimmed)
                    + "'. The separator between entries is `;` (semicolon), "
                    + "NOT `,` (comma) — this differs from other Kafka list "
                    + "configs because legitimate SSL DNs use `=` as the "
                    + "attribute separator (eg. "
                    + "`User:CN=Broker One,OU=Kafka Brokers,O=Example Corp,C=US`).");
            }
            // R32 #282 [HIGH]: DN normalisation gap. The natural operator
            // workflow for discovering an SSL broker's principal is
            //   $ openssl x509 -in broker.pem -noout -subject
            //   subject=CN = broker-1, OU = kafka, O = corp, C = US
            // (RFC 2253 form with spaces around `=` and after `,`). But
            // DefaultKafkaPrincipalBuilder.build() funnels the runtime peer
            // through X500Principal.getName() (the default RFC 2253 form),
            // which always returns the no-space canonical form
            //   CN=broker-1,OU=kafka,O=corp,C=US
            // and always upper-cases known RDN keywords. R30 #280
            // deliberately allowed internal ASCII U+0020 inside `name` so
            // legitimate DNs like `CN=Broker One,...` work — that allowance
            // means the openssl-padded paste sails through every existing
            // R29-R32 guard and parses cleanly. Stored verbatim, it never
            // matches the runtime peer's canonical form ⇒ silent
            // fail-CLOSED soft-brick of the inter-broker bypass, same
            // impact class as the codepoint/confusable findings.
            //
            // Fix: when `name` looks DN-shaped (contains `=`), round-trip
            // it through X500Principal.getName(). The JDK constructor
            //   - parses both openssl-padded AND canonical forms cleanly
            //   - rejects non-DN strings with `IllegalArgumentException`
            //     (eg. unknown RDN keywords like `User=broker` throw)
            //   - upper-cases keywords on output (matches what
            //     DefaultKafkaPrincipalBuilder produces at runtime)
            //   - returns the RFC 2253 canonical form (no spaces,
            //     deterministic across JDK versions)
            // If the constructor throws, the operator did not give us a
            // DN — leave `name` verbatim (no change vs prior behaviour).
            // If the canonical form differs from the input, emit a
            // one-time INFO line so the operator sees the rewrite at
            // startup and can audit it.
            //
            // Idempotency: an already-canonical DN round-trips to itself
            // (no log, no rewrite). The gate `name.indexOf('=') >= 0`
            // avoids touching non-DN names like `User:broker` or
            // SASL-mapped `User:svc-account`.
            //
            // Why not RFC2253-CANONICAL form? `getName(CANONICAL)`
            // lower-cases everything (`cn=broker-1,...`), which does NOT
            // match what DefaultKafkaPrincipalBuilder emits. The
            // no-argument `getName()` (= RFC 2253 default) is the right
            // target.
            if (name.indexOf('=') >= 0) {
                String canonical = canonicaliseDnIfPossible(name);
                if (canonical != null && !canonical.equals(name)) {
                    LOG.info(
                        "governance.bypass.principals entry '{}' was "
                        + "rewritten to its X500 canonical form '{}:{}' "
                        + "to match the runtime peer principal that "
                        + "DefaultKafkaPrincipalBuilder produces via "
                        + "X500Principal.getName(). The original DN was "
                        + "valid but in a non-canonical shape (typically "
                        + "from `openssl x509 -noout -subject` output, "
                        + "which space-pads `=` and `,`). The bypass set "
                        + "now stores the canonical form so the runtime "
                        + "match succeeds.",
                        LogSafe.sanitize(trimmed),
                        LogSafe.sanitize(type),
                        LogSafe.sanitize(canonical));
                    name = canonical;
                }
            }
            // Round-22 HIGH (Agent 5 H-1): assemble the canonical
            // "type:name" form explicitly rather than calling toString.
            // SecurityUtils.parseKafkaPrincipal currently always returns the
            // base KafkaPrincipal class, whose toString() is documented as
            // `principalType + ":" + name`. But the runtime match site in
            // KafkaApis already builds the canonical form by hand
            // (`p.getPrincipalType + ":" + p.getName`, see KafkaApis L246-247)
            // precisely because custom KafkaPrincipal subclasses sometimes
            // override toString to append role/group/auth metadata. Closing
            // the asymmetry here means that if parseKafkaPrincipal is ever
            // refactored to return a subclass — or someone wires a
            // KafkaPrincipalBuilder-derived principal into this method — the
            // allow-list entries stay canonical and continue to match the
            // KafkaApis bypass check. Cost: two field reads, no allocation
            // change (StringBuilder concat already used by toString).
            out.add(type + ":" + name);
        }
        return Collections.unmodifiableSet(out);
    }

    /**
     * R32 #282 [HIGH]: round-trip a candidate DN through
     * {@link X500Principal} and return the canonical RFC 2253 form
     * (no spaces, upper-cased keywords) if the JDK accepts it as a
     * valid DN, or {@code null} if the JDK rejects it.
     *
     * <p>This matches what {@code DefaultKafkaPrincipalBuilder.build()}
     * does for SSL peers at runtime — it calls {@code X500Principal
     * .getName()} on the cert subject. The bypass set must store the
     * same canonical form for the runtime {@code Set.contains} lookup
     * to succeed.
     *
     * <p>Returning {@code null} on failure (rather than throwing) lets
     * the caller fall back to the verbatim input — non-DN names like
     * {@code "broker"}, {@code "svc-account-1"}, or
     * {@code "User:foo@example.com"} legitimately don't parse as DNs
     * and must be left alone.
     *
     * <p>The default {@code getName()} (= RFC 2253) is the right
     * target, not {@code getName(CANONICAL)} — CANONICAL lower-cases
     * everything and does not match what
     * {@code DefaultKafkaPrincipalBuilder} produces at runtime.
     */
    private static String canonicaliseDnIfPossible(String name) {
        try {
            return new X500Principal(name).getName();
        } catch (IllegalArgumentException e) {
            return null;
        }
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

    /**
     * R29 #270 / #272 [HIGH]: smoking-gun shape of a comma-as-separator typo
     * in {@code governance.bypass.principals}: comma, then any run of
     * characters that contains no {@code =}, {@code ,}, or {@code :}, then a
     * colon. Examples: {@code ",User:"}, {@code ", Group:"},
     * {@code ",Service-Account:"}, {@code ",com.example.Principal:"}, even
     * {@code ",:"} when the operator pasted the entry separator literally.
     *
     * <p>The character class is intentionally {@code [^=,:]} (not
     * {@code [A-Za-z0-9_]}). An R29 #272 adversarial audit of the initial
     * narrow regex found six production-realistic bypasses where the
     * principal type contains a hyphen ({@code Service-Account}), a space
     * ({@code Service Account}), a dot ({@code com.example.Principal}), an
     * empty identifier, or a non-ASCII letter ({@code Üser}). Each shape
     * silently soft-bricked inter-broker traffic. The signal is not "what
     * the identifier looks like" — it is "comma, then something, then a
     * colon, with no {@code =} in between". That discriminator is exactly
     * what the broader class captures.
     *
     * <p>SSL DN attribute separators use {@code =} (eg. {@code ,OU=},
     * {@code ,O=}, {@code ,C=}, {@code ,EMAILADDRESS=}) — NEVER {@code :}
     * — so legitimate SSL DNs containing commas do NOT match this pattern:
     * the {@code =} blocks the {@code [^=,:]*} run before it can reach a
     * {@code :}. See {@link #parseBypassPrincipals(String)} for the full
     * rationale.
     */
    private static final Pattern COMMA_SEPARATOR_TYPO =
        Pattern.compile(",[^=,:]*:");

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
     * Scan {@code s} for any invisible code point that would survive both
     * the whitespace check and {@link String#isBlank()} but make the
     * canonical principal form unreachable at runtime. Returns a short
     * human-readable label for the first offending code point, or
     * {@code null} if {@code s} is clean.
     *
     * <p>Round-19 BLOCKER. Rejects:
     * <ul>
     *   <li>C0 control chars (U+0000–U+001F) and DEL (U+007F). Leading and
     *       trailing TAB/LF/CR are already caught by
     *       {@link #hasLeadingOrTrailingWhitespace} above; this also
     *       catches them <i>internally</i>, plus the rest of the C0
     *       block which is not classified as whitespace.</li>
     *   <li>C1 control chars (U+0080–U+009F).</li>
     *   <li>Zero-width spacing characters: ZWSP (U+200B), ZWNJ (U+200C),
     *       ZWJ (U+200D), and BOM / ZWNBSP (U+FEFF). These render as
     *       nothing, so a typo embedding one is visually indistinguishable
     *       from the intended value.</li>
     *   <li>Bidi formatting controls: LRE/RLE/PDF/LRO/RLO
     *       (U+202A–U+202E) and LRI/RLI/FSI/PDI (U+2066–U+2069). A typo
     *       embedding one of these reorders the visible component on
     *       screen while the codepoint sits invisibly in the underlying
     *       string — a silent under-grant vector.</li>
     * </ul>
     *
     * <p>The visible-ASCII space (U+0020) and the SSL-DN-friendly inner
     * spaces inside e.g. {@code CN=Broker One,...} remain unaffected: they
     * are not in any of the ranges above.
     */
    private static String firstInvisibleCodePointLabel(String s) {
        int len = s.length();
        for (int i = 0; i < len; ) {
            int cp = s.codePointAt(i);
            if (cp <= 0x001F || cp == 0x007F) {
                return String.format("C0 control U+%04X", cp);
            }
            if (cp >= 0x0080 && cp <= 0x009F) {
                return String.format("C1 control U+%04X", cp);
            }
            if (cp == 0x200B || cp == 0x200C || cp == 0x200D || cp == 0xFEFF) {
                return String.format("zero-width U+%04X", cp);
            }
            if (cp >= 0x202A && cp <= 0x202E) {
                return String.format("bidi override U+%04X", cp);
            }
            if (cp >= 0x2066 && cp <= 0x2069) {
                return String.format("bidi isolate U+%04X", cp);
            }
            // Round-22 (R22-#212): strong directional marks were missed in
            // earlier rounds. Same hazard class as the bidi override/isolate
            // sets above — they reorder display without changing codepoint
            // content. A bypass entry like "User:broker‎" displays as
            // "User:broker" in every bidi-aware admin viewer but its canonical
            // form never matches a runtime peer principal, silently
            // under-granting the bypass.
            if (cp == 0x200E || cp == 0x200F || cp == 0x061C) {
                return String.format("bidi mark U+%04X", cp);
            }
            // R31 #281 [HIGH]: Hangul fillers — Lo (Other_Letter), so not
            // detected by firstNonAsciiWhitespaceCodePointLabel (which gates
            // on Character.isWhitespace || isSpaceChar), and not detected by
            // firstConfusableSeparatorLabel (not a comma/semicolon shape).
            // U+3164 is the famous spoofing vector used to register blank
            // usernames on Twitter/Discord/Steam. The other three (U+115F
            // CHOSEONG FILLER, U+1160 JUNGSEONG FILLER, U+FFA0 HALFWIDTH
            // FILLER) render as the same blank glyph in most fonts.
            // Operator-side soft-brick: "User:ㅤbroker" parses to name
            // "ㅤbroker"; runtime peer is "User:broker"; equal returns
            // false ⇒ silent fail-CLOSED bypass under-grant.
            if (cp == 0x115F || cp == 0x1160 || cp == 0x3164 || cp == 0xFFA0) {
                return String.format("Hangul filler U+%04X", cp);
            }
            // R31 #281 [HIGH] / closes R23 #223 scope over-claim: variation
            // selectors VS1–VS16 (U+FE00–U+FE0F) and VS17–VS256
            // (U+E0100–U+E01EF), combining grapheme joiner (U+034F), and
            // Mongolian free-variation selectors / vowel separator
            // (U+180B–U+180E) are Default_Ignorable_Code_Point per Unicode
            // contract — visually invisible in compliant renderers but
            // not whitespace and not punctuation. Same soft-brick class as
            // R22 #212: invisible content in a principal entry diverges
            // from the canonical runtime form.
            if ((cp >= 0xFE00 && cp <= 0xFE0F)
                    || (cp >= 0xE0100 && cp <= 0xE01EF)
                    || cp == 0x034F
                    || (cp >= 0x180B && cp <= 0x180E)) {
                return String.format("invisible mark U+%04X", cp);
            }
            // R31 #281 [HIGH] / R33 #290 [HIGH] / closes R23 #223: format
            // characters that the Unicode standard requires renderers to
            // suppress.
            //   U+00AD  SOFT HYPHEN
            //   U+2060  WORD JOINER                  (R33 #290 — was the
            //                                        off-by-one neighbour
            //                                        of the family below)
            //   U+2061  FUNCTION APPLICATION
            //   U+2062  INVISIBLE TIMES
            //   U+2063  INVISIBLE SEPARATOR
            //   U+2064  INVISIBLE PLUS
            //   U+FFF9  INTERLINEAR ANNOTATION ANCHOR
            //   U+FFFA  INTERLINEAR ANNOTATION SEPARATOR
            //   U+FFFB  INTERLINEAR ANNOTATION TERMINATOR
            //   U+FFFC  OBJECT REPLACEMENT CHARACTER
            //
            // R33 #290 [HIGH]: U+2060 WORD JOINER (Cf, Default_Ignorable)
            // sits immediately below U+2061 in the same Unicode "invisible
            // math/format" family. Auto-inserted by many Markdown
            // renderers (chat clients, wikis, docs sites) around technical
            // strings to prevent unwanted line breaks. An operator pasting
            // `User:broker⁠` would store the verbatim form while the
            // runtime peer's principal is `User:broker` — silent
            // fail-CLOSED soft-brick, identical impact class to
            // R29-R32 #270/#272/#278/#280/#281/#284/#287/#282. The codec
            // (RuleJsonCodec.isForbiddenIdCodepoint) already catches it
            // both via the `Character.FORMAT` umbrella AND an explicit
            // `case '⁠'` arm — this parser was the only remaining
            // path with the gap.
            if (cp == 0x00AD
                    || (cp >= 0x2060 && cp <= 0x2064)
                    || (cp >= 0xFFF9 && cp <= 0xFFFC)) {
                return String.format("invisible format U+%04X", cp);
            }
            // R31 #281 [HIGH]: Unicode tag characters U+E0000–U+E007F.
            // Used in real-world supply-chain attacks (trojan-source/2021)
            // to smuggle invisible instructions into source. Operator paste
            // from any document edited under an attacker-influenced
            // template could carry them. None are whitespace or punctuation.
            if (cp >= 0xE0000 && cp <= 0xE007F) {
                return String.format("tag char U+%04X", cp);
            }
            // R31 #281 [HIGH]: U+2800 BRAILLE PATTERN BLANK — category So,
            // not whitespace, but renders as a 2x4 blank cell in every
            // braille-aware font. Same blank-glyph spoofing vector as
            // Hangul fillers above.
            if (cp == 0x2800) {
                return String.format("Braille blank U+%04X", cp);
            }
            i += Character.charCount(cp);
        }
        return null;
    }

    /**
     * R30 #280 [HIGH]: detect any non-ASCII whitespace codepoint inside
     * a parsed component. The parser intentionally allows internal ASCII
     * space (U+0020) so legitimate SSL DN values like
     * {@code CN=Broker One,OU=Kafka Brokers,O=Example Corp,C=US} parse
     * correctly. But Unicode has many other space-class codepoints
     * (NBSP U+00A0, NARROW NO-BREAK SPACE U+202F, FIGURE SPACE U+2007,
     * EN SPACE U+2002, EM SPACE U+2003, IDEOGRAPHIC SPACE U+3000, ...)
     * that render visually identical to ASCII space in most fonts but
     * are NOT collapsed by the JDK String comparator at runtime.
     *
     * <p>Hazard: an operator pastes
     * {@code User:CN=Broker One,OU=...} from a word-processor or
     * web page that auto-replaced ASCII space with NBSP. The parser
     * stores the entry intact — NBSP is not in
     * {@link #firstInvisibleCodePointLabel} (which covers only invisible
     * smuggling: C0/C1/zero-width/bidi) and not in
     * {@link #firstConfusableSeparatorLabel} (which covers only comma
     * and semicolon confusables). At runtime the broker's canonical
     * peer principal is built from {@code X500Principal.getName()}
     * which yields ASCII U+0020 — verbatim mismatch — silent
     * soft-brick of the bypass, identical user-impact to
     * R29 #270/#272/#278.
     *
     * <p>The leading/trailing whitespace rejection above already covers
     * edge cases via {@link #isAnyWhitespaceCodePoint}; this helper
     * covers the interior. C0 controls (TAB/LF/CR/...) are caught
     * earlier by {@link #firstInvisibleCodePointLabel}; this helper
     * fires only for non-control whitespace that slipped through.
     *
     * <p>Returns a label of the form
     * {@code "non-ASCII whitespace U+00A0"} on first hit; null
     * otherwise. ASCII space U+0020 is deliberately excluded — SSL DN
     * values contain it legitimately.
     */
    private static String firstNonAsciiWhitespaceCodePointLabel(String s) {
        int len = s.length();
        for (int i = 0; i < len; ) {
            int cp = s.codePointAt(i);
            if (cp != 0x0020
                    && (Character.isWhitespace(cp) || Character.isSpaceChar(cp))) {
                return String.format("non-ASCII whitespace U+%04X", cp);
            }
            i += Character.charCount(cp);
        }
        return null;
    }

    /**
     * R29 #278 [HIGH]: detect non-ASCII codepoints that are VISUALLY
     * identical or near-identical to ASCII {@code ,} (the comma-typo
     * shape rejected by {@link #COMMA_SEPARATOR_TYPO}) or ASCII
     * {@code ;} (the legitimate entry separator). Both classes produce
     * the same silent soft-brick as R29 #270/#272: the parsed entry
     * looks like one principal but the operator typed it as two, so
     * the runtime peer principal never matches.
     *
     * <p>The {@link #firstInvisibleCodePointLabel} helper covers only
     * invisible smuggling (C0/C1, zero-width, bidi). The codepoints
     * enumerated here are <em>visible</em> printable punctuation and
     * would not be caught by that helper. They are reachable from
     * everyday CJK / Arabic / Armenian / Greek input methods —
     * U+FF0C ({@code ，}) is the CJK IME default for the comma key,
     * and U+037E ({@code ;}, GREEK QUESTION MARK) is visually
     * identical to ASCII {@code ;} in most fonts.
     *
     * <p>Returns a label of the form {@code "comma-confusable U+FF0C"}
     * or {@code "semicolon-confusable U+FF1B"} on first hit; null
     * otherwise.
     */
    private static String firstConfusableSeparatorLabel(String s) {
        int len = s.length();
        for (int i = 0; i < len; ) {
            int cp = s.codePointAt(i);
            // Comma confusables — Unicode UTC confusables data, restricted
            // to the set that arises from real keyboards / pastes.
            //   U+055D  ARMENIAN COMMA
            //   U+060C  ARABIC COMMA
            //   U+07F8  NKO COMMA                  ← R31 #284, Mande locale IME `,`
            //   U+1363  ETHIOPIC COMMA
            //   U+1802  MONGOLIAN COMMA
            //   U+1808  MONGOLIAN MANCHU COMMA
            //   U+2E32  TURNED COMMA
            //   U+2E34  RAISED COMMA
            //   U+2E41  REVERSED COMMA
            //   U+2E4C  MEDIEVAL COMMA            ← R30 #279, same block as U+2E32/34/41
            //   U+3001  IDEOGRAPHIC COMMA
            //   U+A4FE  LISU PUNCTUATION COMMA    ← R31 #284, Tibeto-Burman
            //   U+A60D  VAI COMMA                 ← R30 #279, Vai script (named "COMMA")
            //   U+A6F5  BAMUM COMMA               ← R30 #279, Bamum script (named "COMMA")
            //   U+FE10  PRESENTATION FORM COMMA
            //   U+FE11  PRESENTATION FORM IDEOGRAPHIC COMMA
            //   U+FE50  SMALL COMMA
            //   U+FE51  SMALL IDEOGRAPHIC COMMA
            //   U+FF0C  FULLWIDTH COMMA            ← CJK IME default
            //   U+FF64  HALFWIDTH IDEOGRAPHIC COMMA
            //   U+16E97 MEDEFAIDRIN COMMA         ← R31 #284, supplementary plane
            if (cp == 0x055D || cp == 0x060C || cp == 0x07F8 || cp == 0x1363
                    || cp == 0x1802 || cp == 0x1808
                    || cp == 0x2E32 || cp == 0x2E34 || cp == 0x2E41
                    || cp == 0x2E4C
                    || cp == 0x3001
                    || cp == 0xA4FE
                    || cp == 0xA60D || cp == 0xA6F5
                    || cp == 0xFE10 || cp == 0xFE11
                    || cp == 0xFE50 || cp == 0xFE51
                    || cp == 0xFF0C || cp == 0xFF64
                    || cp == 0x16E97) {
                return String.format("comma-confusable U+%04X", cp);
            }
            // Semicolon confusables.
            //   U+037E  GREEK QUESTION MARK   ← visually identical to ASCII `;`
            //   U+061B  ARABIC SEMICOLON      ← R31 #284, Arabic keyboard `;`
            //   U+1364  ETHIOPIC SEMICOLON    ← R31 #284, pair to U+1363
            //   U+204F  REVERSED SEMICOLON
            //   U+2E35  TURNED SEMICOLON
            //   U+A6F6  BAMUM SEMICOLON       ← R31 #284, pair to U+A6F5
            //   U+FE54  SMALL SEMICOLON
            //   U+FF1B  FULLWIDTH SEMICOLON   ← CJK IME default
            if (cp == 0x037E || cp == 0x061B || cp == 0x1364
                    || cp == 0x204F || cp == 0x2E35 || cp == 0xA6F6
                    || cp == 0xFE54 || cp == 0xFF1B) {
                return String.format("semicolon-confusable U+%04X", cp);
            }
            i += Character.charCount(cp);
        }
        return null;
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
        // evaluation's state.
        //
        // Note on the early-return paths below (bypass / no-rule / empty-rule-
        // list at lines 468-477): those returns happen BEFORE the
        // IN_EVALUATE.set(TRUE) at line 484, so an evaluation that exits via
        // any of them never marks the flag. The invariant is "a subsequent
        // call on the same thread starts with IN_EVALUATE=FALSE", regardless
        // of which exit path the previous call took. This matters for two
        // reasons: (1) a request thread that processed an ALLOW-via-bypass
        // request is free to land on a deny-targeting api-key next without
        // tripping the guard; (2) the per-request-budget guarantee is not
        // weakened by short-circuit exits — the guard fires only when a
        // re-entrant call would actually reach the budget-reset or the rule
        // loop (i.e. when there is real state to corrupt).
        if (Boolean.TRUE.equals(IN_EVALUATE.get())) {
            throw new EvaluateReentryException(
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
            } catch (Exception e) {
                // Codex deep-audit P0 fix: a throwing activation supplier MUST NOT
                // propagate into the request thread. The most realistic failure mode
                // is ApiMessageActivation walking a request whose accessor blows up
                // (Optional/Stream accessor, partially-constructed protocol object,
                // version-specific schema mismatch). Without this catch the
                // exception escapes into KafkaApis.handle(), turning a buggy
                // governance extractor into a request-thread crash.
                //
                // Round-15 Walker HIGH H2: narrowed from {@code Throwable} to
                // {@code Exception}. With the round-15 walker hardening in place
                // (MAX_DEPTH=32 across both message AND iterable layers per
                // HIGH-H1, MAX_ACCESSOR_INVOCATIONS=10_000 across nested- and
                // flat-scalar widths) the walker has a bounded worst-case work
                // ceiling on EVERY dimension that can amplify legitimate input
                // shapes. Under that guarantee, an {@link Error} subclass from
                // here (OutOfMemoryError, StackOverflowError, LinkageError, an
                // InternalError or UnknownError from the JVM, ThreadDeath, …)
                // is no longer a "buggy rule" symptom — it is either:
                //   (1) a bound-logic bug we MUST surface, not paper over, or
                //   (2) a global JVM-level failure where the request thread is
                //       not actually safe to continue on anyway.
                // Either case is worse to silently fail-open on than to let
                // propagate. Specifically: continuing to ALLOW under a heap-
                // exhausted JVM means an attacker who can pin OOM (or trigger
                // a bound-logic bug) gets a DENY-rule bypass on every
                // subsequent request until the operator notices. KafkaApis.
                // handle has its own outer Throwable catch that turns Error
                // propagation into a 5xx response per request — visible signal,
                // fail-CLOSED at the request layer.
                //
                // The narrowed Exception catch still covers every realistic
                // walker failure mode: any RuntimeException (NullPointer,
                // ClassCast, IllegalArg, ReflectiveOperation wrappers, …) and
                // any checked exception that surfaces here. Posture remains
                // fail-OPEN for these because they are local, bounded faults
                // in the extractor that should degrade to ALLOW, not a request
                // failure, while the broker bug gets fixed.
                //
                // The attacker-shape ActivationBudgetExceededException above
                // is caught FIRST so it never falls through this generic
                // branch; see that catch's comment for why budget overflow
                // needs the opposite posture.
                //
                // Round-15 recent-changes BLOCKER-1: route the WARN through a
                // throttle that mirrors maybeWarnEvalError below — same hot-path,
                // same wire-derived-string-in-toString hazard, same need to
                // sanitise via LogSafe before writing to SLF4J.
                maybeWarnActivationSupplierFailed(apiKey, e);
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
            // Round-16 MED (audit-agent-A): per-evaluate dedup for the
            // evalErrorFailOpenCount cumulative counter. The previous shape
            // (increment inside maybeWarnEvalError) bumped the counter once
            // per rule-eval that threw. A single request to an api-key with
            // K rules whose first rule trips the per-request CEL step budget
            // would retrip on entry for every subsequent rule (each lands in
            // the catch below — see the per-rule catch comment) and bump the
            // counter K times for what is, semantically, one fail-OPEN
            // event. With the 128-rule per-api-key cap, a single attacker
            // request could amplify the counter by up to 128× — JMX-derived
            // fail-open rate dashboards would over-report and alert thresholds
            // calibrated on event-rate would fire on a single request. Track
            // observation via a local boolean and bump the counter at most
            // once per evaluate() in the inner finally.
            boolean evalErrorObserved = false;
            // R28 Axis 1 (HIGH, Task #219): hoist the activation method
            // reference out of the per-rule loop. `activation::get` is a
            // capturing lambda that allocates a fresh Function<String, Object>
            // instance per evaluation site in the general case — the JIT may
            // elide it under escape analysis but the JLS gives no guarantee.
            // With the 128-rule per-api-key cap (CelLimits.MAX_RULES_PER_API_KEY)
            // the pre-fix inner loop produced up to 128 short-lived Function
            // allocations per request on the request hot path. The activation
            // map is invariant across rules within a single evaluate(), so a
            // single hoisted reference is correct and removes the per-rule
            // allocation regardless of JIT behaviour.
            Function<String, Object> activationGet = activation::get;
            try {
                for (Rule rule : rules) {
                    boolean matched;
                    try {
                        matched = rule.compiled().evalBoolean(activationGet);
                    } catch (EvaluateReentryException reentry) {
                        // R23 #224: the re-entry guard at the top of
                        // evaluate() throws EvaluateReentryException (an
                        // IllegalStateException subclass — see field
                        // declaration). It is the engine's own signal that a
                        // supplier or compiled CEL program recursively called
                        // evaluate() on the same thread. The per-rule
                        // fail-OPEN catch below intentionally swallows
                        // generic IllegalStateException — a buggy operator-
                        // authored rule throwing ISE must NOT be able to
                        // deny every request — but the engine-state re-entry
                        // signal is a different category: silently treating
                        // it as ALLOW lets the buggy/hostile reentrant call
                        // evade the very DENY rule whose evaluation
                        // triggered it, with no operator visibility.
                        // Re-throwing the marker subclass propagates up to
                        // KafkaApis's outer Throwable catch (5xx), mirroring
                        // the round-15 H2 reasoning for Error subclasses:
                        // programming-error / engine-state signals belong
                        // above the rule-level catch.
                        //
                        // Today the supplier-level re-entrant case is
                        // covered by the activation-supplier catch at line
                        // ~1158 (which IS intentionally fail-OPEN — see
                        // reentrantEvaluateFromActivationSupplierIsCaughtAndFailsOpen).
                        // This narrowed re-throw covers the path where a
                        // future CEL host function or custom accessor
                        // re-enters evaluate() from inside evalBoolean.
                        throw reentry;
                    } catch (Exception e) {
                        evalErrorObserved = true;
                        // Round-15 Walker HIGH H2: narrowed from {@code Throwable}
                        // to {@code Exception}. The CEL step-budget cap
                        // (CelProgram.MAX_EVAL_STEPS) plus the parse-depth and
                        // collection-literal caps already bound legitimate CEL
                        // evaluation work; the per-rule fail-open here used to
                        // catch StackOverflowError / OutOfMemoryError from a
                        // pathological rule, but that posture silently absorbs
                        // a JVM-level signal that almost certainly means
                        // something is wrong globally (a CEL bound-logic bug,
                        // a runaway allocation in a custom string op). Letting
                        // Error subclasses propagate through to KafkaApis's
                        // outer Throwable catch turns the request into a clear
                        // 5xx and makes the bug visible — far better than
                        // marking the rule ALLOW and continuing to the next
                        // rule in the loop, which would re-trip on entry under
                        // the same shared per-request CEL step counter.
                        //
                        // CelEvaluationException, NullPointerException,
                        // IllegalStateException, ClassCastException — the
                        // realistic failure modes of an operator-published
                        // rule under a malformed activation map — are all
                        // RuntimeException subclasses and still caught.
                        // Posture for them remains fail-OPEN at the per-rule
                        // level: a single buggy rule must not be able to deny
                        // every request, but a JVM-level fault deserves to
                        // surface.
                        //
                        // Note that after one rule trips the per-request CEL
                        // step budget, the next rule in this loop will retrip
                        // on entry under the shared counter and also land here
                        // as fail-open. That is the intended DoS-defence
                        // behaviour: a request cannot multiply the step budget
                        // by the number of rules an operator happens to have
                        // published. See CelLimits.resetSteps javadoc.
                        // Round-14 BLOCKER L-1: this WARN sits on the per-rule,
                        // per-request hot path. A single buggy rule that throws
                        // consistently would otherwise fire one line per request
                        // — published-rule-shaped log amplifier. Sanitise the
                        // exception text (which can carry CelEvaluationException
                        // messages with wire-derived values) and throttle the
                        // emission to at most one WARN per window with a
                        // suppressed-count tail. rule.id() is codec-validated
                        // (strict charset, length cap) but goes through
                        // LogSafe for consistency with other wire-into-log
                        // sites.
                        maybeWarnEvalError(rule.id(), apiKey, e);
                        continue;
                    }
                    if (matched) {
                        return RuleDecision.deny(rule.errorCode(), rule.id());
                    }
                }
                return RuleDecision.ALLOW;
            } finally {
                CelProgram.resetEvalStepBudget();
                // Round-16 MED (audit-agent-A): per-evaluate dedup. Bumps
                // once per evaluate() that observed at least one caught
                // Exception during per-rule eval, regardless of how many
                // rules tripped. Slight over-report under Error-escape is
                // accepted: Error propagation is independently visible as
                // 5xx-rate in KafkaApis metrics, and per-request CEL
                // step-budget + MAX_DEPTH walker caps make Error escape
                // exceptionally rare in practice.
                if (evalErrorObserved) {
                    evalErrorFailOpenCount.incrementAndGet();
                }
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
     * canonical {@code type + ":" + name} form built by hand from
     * {@link org.apache.kafka.common.security.auth.KafkaPrincipal#getPrincipalType()}
     * and {@link org.apache.kafka.common.security.auth.KafkaPrincipal#getName()}
     * at the call site (mirroring how {@link #normaliseTrustedBypassPrincipals}
     * canonicalises the operator-supplied allow-list at construction time).
     * The match is intentionally NOT against {@code KafkaPrincipal#toString()}
     * because a subclass could override that to emit a non-canonical or
     * attacker-chosen string and bypass an operator allow-list keyed on the
     * canonical form.
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
        // Round-15 Request-path HIGH-3: bump the cumulative fail-closed
        // counter on EVERY event, independent of whether the WARN actually
        // fires in this window. This gives operators a long-term sampling
        // signal that the window-bound suppressedBudgetWarnings counter
        // (which resets to 0 on every emitted WARN) cannot provide.
        activationBudgetFailClosedCount.incrementAndGet();
        long now = System.nanoTime();
        long last = lastBudgetWarnNanos.get();
        if (now - last >= BUDGET_WARN_INTERVAL_NANOS
            && lastBudgetWarnNanos.compareAndSet(last, now)) {
            long suppressed = suppressedBudgetWarnings.getAndSet(0L);
            // R23 HIGH (#221): sanitise budget.getMessage() to match the
            // sibling sites (maybeWarnEvalError, maybeWarnActivationFailure).
            // The budget message today only embeds JVM class names and integer
            // constants — all safe — but the asymmetry was a footgun: any
            // future walker change that adds wire-derived data (a topic name,
            // a field label, an o.toString()) to the exception message would
            // land unsanitised in operator logs. Three sites now have the
            // same posture.
            String budgetMessage = LogSafe.sanitize(budget.getMessage());
            if (suppressed > 0) {
                LOG.warn("activation budget exceeded on apiKey {} — failing closed (POLICY_VIOLATION), "
                    + "suppressed {} similar events in the previous window: {}",
                    apiKey, suppressed, budgetMessage);
            } else {
                LOG.warn("activation budget exceeded on apiKey {} — failing closed (POLICY_VIOLATION): {}",
                    apiKey, budgetMessage);
            }
        } else {
            suppressedBudgetWarnings.incrementAndGet();
        }
    }

    /**
     * Throttled WARN for the per-rule fail-open eval-error path. Round-14
     * BLOCKER L-1 (concurrency sub-agent). Mirrors {@link
     * #maybeWarnBudgetExceeded}: window-based CAS, suppressed-count carried
     * forward to the next emitted line.
     *
     * <p>Both wire-into-log slots are sanitised: {@code ruleId} (codec-
     * validated upstream, but consistent with the rest of the codebase) and
     * {@code throwable.toString()} (CelEvaluationException messages may carry
     * trimmed wire-derived values from the activation map). The {@code apiKey}
     * is enum-typed and printable-safe.
     *
     * <p>Two emission shapes — the second includes the suppressed count so
     * operators see the rate of the storm, not just one example.
     */
    private void maybeWarnEvalError(String ruleId, ApiKeys apiKey, Throwable t) {
        // Round-16 MED (audit-agent-A): cumulative counter increment moved
        // out of this per-rule WARN method to a per-evaluate finally block
        // in evaluate(). The WARN itself stays per-rule (each buggy rule's
        // diagnostic is independently useful); only the cumulative counter
        // is deduped to once-per-evaluate, since that is what an operator
        // sampling JMX for "fail-open request rate" expects.
        long now = System.nanoTime();
        long last = lastEvalErrorWarnNanos.get();
        if (now - last >= EVAL_ERROR_WARN_INTERVAL_NANOS
            && lastEvalErrorWarnNanos.compareAndSet(last, now)) {
            long suppressed = suppressedEvalErrorWarnings.getAndSet(0L);
            if (suppressed > 0) {
                LOG.warn("rule '{}' failed open due to evaluation error on apiKey {} "
                    + "(suppressed {} similar events in the previous window): {}",
                    LogSafe.sanitize(ruleId), apiKey, suppressed,
                    LogSafe.sanitize(t.toString()));
            } else {
                LOG.warn("rule '{}' failed open due to evaluation error on apiKey {}: {}",
                    LogSafe.sanitize(ruleId), apiKey,
                    LogSafe.sanitize(t.toString()));
            }
        } else {
            suppressedEvalErrorWarnings.incrementAndGet();
        }
    }

    /**
     * Round-15 recent-changes BLOCKER-1: throttled + sanitised WARN for the
     * activation-supplier fail-open path. The recent-changes audit observed
     * that the un-throttled, un-sanitised inline WARN at the activation catch
     * site was the exact shape that round-14 BLOCKER L-1 closed for the
     * per-rule eval-error WARN two lines further down — same hot path, same
     * wire-derived-string-in-toString hazard. The fix is mechanically
     * identical: window-based CAS for log-rate bounding, suppressed-count
     * carried forward, LogSafe on {@code t.toString()} (which can carry
     * accessor exception messages quoting wire-derived names).
     *
     * <p>{@code apiKey} is enum-typed so it does not need sanitisation.
     */
    private void maybeWarnActivationSupplierFailed(ApiKeys apiKey, Throwable t) {
        // Round-15 Request-path HIGH-3: cumulative fail-open counter, see
        // field javadoc on activationFailureFailOpenCount. Incremented
        // BEFORE the throttle CAS so suppressed events still register.
        activationFailureFailOpenCount.incrementAndGet();
        long now = System.nanoTime();
        long last = lastActivationFailureWarnNanos.get();
        if (now - last >= ACTIVATION_FAILURE_WARN_INTERVAL_NANOS
            && lastActivationFailureWarnNanos.compareAndSet(last, now)) {
            long suppressed = suppressedActivationFailureWarnings.getAndSet(0L);
            if (suppressed > 0) {
                LOG.warn("activation supplier failed for apiKey {} — failing open "
                    + "(suppressed {} similar events in the previous window): {}",
                    apiKey, suppressed, LogSafe.sanitize(t.toString()));
            } else {
                LOG.warn("activation supplier failed for apiKey {} — failing open: {}",
                    apiKey, LogSafe.sanitize(t.toString()));
            }
        } else {
            suppressedActivationFailureWarnings.incrementAndGet();
        }
    }

    /**
     * Cumulative count of activation-supplier failures that caused this
     * engine to fall open since construction. Monotonically non-decreasing;
     * survives across warn-throttle windows; sampled by external operators
     * (JMX/metric adapter, dashboards, alerting). See the field javadoc on
     * the underlying counter for what each event means and how to interpret
     * a sustained non-zero rate.
     *
     * <p>Round-15 Request-path HIGH-3.
     */
    public long activationFailureFailOpenCount() {
        return activationFailureFailOpenCount.get();
    }

    /**
     * Cumulative count of per-rule eval-error fail-open events since engine
     * construction. See {@link #activationFailureFailOpenCount} for sampling
     * semantics. A sustained non-zero rate indicates a published rule that
     * throws under the current request mix.
     *
     * <p>Round-15 Request-path HIGH-3.
     */
    public long evalErrorFailOpenCount() {
        return evalErrorFailOpenCount.get();
    }

    /**
     * Cumulative count of activation-budget-exceeded fail-CLOSED events
     * since engine construction. See {@link #activationFailureFailOpenCount}
     * for sampling semantics. A sustained non-zero rate is a deliberate-
     * attacker-shape signal — the engine is correctly fail-closing wide
     * requests; an operator wants to see the rate and source.
     *
     * <p>Round-15 Request-path HIGH-3.
     */
    public long activationBudgetFailClosedCount() {
        return activationBudgetFailClosedCount.get();
    }
}
