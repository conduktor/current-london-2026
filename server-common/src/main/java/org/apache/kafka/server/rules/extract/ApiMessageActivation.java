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
package org.apache.kafka.server.rules.extract;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.record.BaseRecords;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reflection-based, per-Class-cached field extractor that turns any
 * {@link ApiMessage} (and the message subtrees it contains) into a plain
 * {@code Map<String, Object>} suitable for use as the activation environment
 * of a CEL-style expression evaluator.
 *
 * <p>The mapping rules are intentionally narrow so rule authors can predict
 * what their CEL expressions see:
 *
 * <ul>
 *   <li>Public, zero-argument, non-void instance methods that aren't part of
 *       the {@code Message} / {@code ApiMessage} / {@code Element} framework
 *       contract are treated as field accessors. The key in the output map is
 *       the Java getter name (e.g. {@code timeoutMs}).</li>
 *   <li>Numeric scalars (Integer/Short/Byte) are normalised to {@code Long}
 *       so a CEL expression comparing against an integer literal does not
 *       hit cross-wrapper inequality. (CEL's int type is 64-bit; aligning
 *       the activation with that convention avoids surprises.)</li>
 *   <li>{@code Iterable} values become {@code List<Object>} with each element
 *       recursively converted. This covers both standard {@code List} and the
 *       generated {@code ImplicitLinkedHashMultiCollection} containers.</li>
 *   <li>Nested generated messages are walked recursively into nested maps.
 *       The recursion terminates when a value has no introspectable accessors
 *       — at which point we keep the value as-is (for {@code byte[]} /
 *       {@code ByteBuffer}) or fall back to {@code toString()}.</li>
 * </ul>
 *
 * <p>Per-{@code Class} accessor lists are cached in a static
 * {@link ConcurrentHashMap}. Discovery touches reflection exactly once per
 * Kafka data class encountered for the lifetime of the JVM.
 *
 * <p>This class has no per-API hand-coded logic. Adding a new Kafka API
 * requires nothing here.
 */
public final class ApiMessageActivation {

    private static final ConcurrentHashMap<Class<?>, List<Accessor>> CACHE = new ConcurrentHashMap<>();

    /**
     * Names declared by the {@code Message} / {@code ApiMessage} interfaces
     * (and overridden in every generated subclass) that we never expose to
     * rules. These describe wire-protocol mechanics, not domain fields.
     */
    private static final Set<String> EXCLUDED_NAMES = new HashSet<>(Arrays.asList(
        // Object
        "toString", "hashCode", "getClass",
        // Message
        "lowestSupportedVersion", "highestSupportedVersion",
        "unknownTaggedFields", "duplicate",
        // ApiMessage
        "apiKey",
        // ImplicitLinkedHashCollection.Element — generated record classes implement this
        "prev", "next"
    ));

    /**
     * Field names that are NEVER surfaced to CEL rules because their value is
     * security-sensitive — exposing them via the activation map would let any
     * operator with rule-write access exfiltrate credentials simply by writing
     * a rule whose predicate compares the field against a constant (eg.
     * {@code request.authBytes == b"\\x00admin\\x00s3cret"} → "DENY", then
     * watch the audit log to learn which guesses matched).
     *
     * <p>The walker has no context about whether a given CEL expression is
     * benign or hostile, so the safe answer is to never produce these values
     * in the activation map at all. A rule that references
     * {@code request.authBytes} will see a missing key (CEL's null) rather
     * than the actual SASL bytes.
     *
     * <p>These names are matched case-insensitively against the Java getter
     * name (in {@code isFieldAccessor}). Fields enumerated here:
     * <ul>
     *   <li>{@code authBytes} — {@link org.apache.kafka.common.message.SaslAuthenticateRequestData}
     *       and {@link org.apache.kafka.common.message.SaslAuthenticateResponseData}:
     *       the raw SASL bytes exchanged during authentication.</li>
     *   <li>{@code salt} and {@code saltedPassword} — the SCRAM credential
     *       components in {@link org.apache.kafka.common.message.AlterUserScramCredentialsRequestData}.
     *       Either is enough to mount an offline credential-stuffing attack.</li>
     *   <li>{@code password} / {@code passwords} — defense in depth for any
     *       future protocol additions that carry a raw password.</li>
     *   <li>{@code hmac} — delegation-token HMAC (the secret half of the token).</li>
     *   <li>{@code secret} / {@code clientSecret} — defense in depth.</li>
     * </ul>
     *
     * <p>If a Kafka protocol revision adds a new credential-bearing field, add
     * its accessor name here. We deliberately use a fixed list rather than a
     * loose pattern like "anything containing 'password'" because over-matching
     * silently strips legitimate fields and is hard to spot in production.
     * Add new names explicitly.
     */
    private static final Set<String> SENSITIVE_NAMES;
    static {
        // The set is matched case-insensitively, so store everything lower-case.
        Set<String> s = new HashSet<>();
        s.add("authbytes");
        s.add("salt");
        s.add("saltedpassword");
        s.add("password");
        s.add("passwords");
        s.add("hmac");
        s.add("secret");
        s.add("clientsecret");
        SENSITIVE_NAMES = Collections.unmodifiableSet(s);
    }

    /**
     * Generated Kafka data classes that carry a credential-bearing
     * {@code (name, value)} pair where the name decides whether the value is
     * sensitive. The flat {@link #SENSITIVE_NAMES} denylist cannot handle
     * these because the getter is just called {@code value} — a name we want
     * to keep visible for non-credential configs (eg. {@code retention.ms}).
     *
     * <p>For each class listed here, after the walker builds the nested map
     * for a single instance, {@link #redactSensitiveConfigValue} inspects
     * the resulting {@code name} entry and nulls the {@code value} entry when
     * that name matches a sensitive config-key pattern (see
     * {@link #isSensitiveConfigName}). Codex/Gemini final-audit P1.
     *
     * <p>FQNs are stored as strings, not {@code Class} references, to avoid a
     * compile-time dependency on the generated classes from server-common —
     * the generator output lives in {@code clients} and we deliberately do
     * not import it for the walker (the walker is type-agnostic by design).
     */
    private static final Set<String> CONFIG_PAIR_CLASS_NAMES = new HashSet<>(Arrays.asList(
        "org.apache.kafka.common.message.AlterConfigsRequestData$AlterableConfig",
        "org.apache.kafka.common.message.IncrementalAlterConfigsRequestData$AlterableConfig",
        "org.apache.kafka.common.message.CreateTopicsRequestData$CreatableTopicConfig"
    ));

    /**
     * Maximum recursion depth for the reflection walk. Each entry into
     * {@link #toMap(Object, int, int[])} (the recursive call for a nested
     * message) counts as one level. Kafka's generated DTOs do not contain
     * reference cycles, so any legitimate message is far shallower than this.
     * The cap is defense in depth against a future protocol with deeper
     * nesting than we anticipated, or a hostile request that constructed a
     * cycle by other means — the walker cannot tell the difference and must
     * not be allowed to blow the stack or spin forever.
     */
    static final int MAX_DEPTH = 32;

    /**
     * Maximum number of accessor invocations a single {@link #from(ApiMessage)}
     * call may perform across the whole walk. The depth cap alone is not
     * enough: a request with a single shallow layer that contains a
     * megabyte-sized repeated field (e.g. a {@code Records} batch with a few
     * hundred-thousand inner records) would walk a flat list whose recursion
     * depth is 2 but whose element count is unbounded.
     *
     * <p>The bound is intentionally generous: a legitimate Kafka admin
     * request rarely contains more than a few hundred topic/partition
     * descriptors. 10k accessor invocations leaves three orders of magnitude
     * of headroom for normal traffic while killing pathological extraction in
     * single-digit milliseconds. A request that exceeds this raises
     * {@link ActivationBudgetExceededException}, which {@link RuleEngine#evaluate}
     * catches and fails the request <em>closed</em> — see that exception's
     * class javadoc for the fail-closed-vs-open policy distinction (attacker-
     * shaped wide requests must NOT fall through the generic {@code Throwable}
     * branch that protects against buggy extractors).
     */
    static final int MAX_ACCESSOR_INVOCATIONS = 10_000;

    private ApiMessageActivation() {
    }

    /**
     * Walk {@code msg} into a fresh {@code Map<String, Object>}. {@code null}
     * input yields an empty map (defensive — the engine should never call
     * this with a null request, but we never want a NullPointerException to
     * escape into the request path).
     */
    public static Map<String, Object> from(ApiMessage msg) {
        if (msg == null) {
            return Collections.emptyMap();
        }
        return toMap(msg);
    }

    /**
     * Build the activation map the broker actually feeds to CEL: the
     * extracted message wrapped under the top-level key {@code "request"}, so
     * rules authored as {@code request.topics.exists(...)} (the documented
     * envelope shape — see {@link org.apache.kafka.server.rules.json.RuleJsonCodec}'s
     * class javadoc) can resolve {@code request} to the message tree.
     *
     * <p>Without this wrap, a rule referencing {@code request.*} would fail to
     * resolve {@code request} and either evaluate to false or raise a CEL
     * evaluation error — silently bypassing the rule on the broker's request
     * path. That regression is locked in by KafkaApisTest's request-shape
     * tests; do not inline this method back into the engine call site without
     * preserving the {@code "request"} envelope.
     */
    public static Map<String, Object> requestActivation(ApiMessage msg) {
        Map<String, Object> m = new LinkedHashMap<>(2);
        m.put("request", from(msg));
        return m;
    }

    static Map<String, Object> toMap(Object o) {
        return toMap(o, 0, new int[1]);
    }

    /**
     * Recursive form with depth counter and shared invocation budget.
     * {@code depth} is incremented on each descent into a nested message.
     * When it reaches {@link #MAX_DEPTH} we return an empty map rather than
     * recurse further — the rule sees the upper levels intact, the bottom is
     * truncated. {@code invocations} is a one-element array used as a shared
     * mutable counter across the whole walk; each {@link Accessor#invoke}
     * call bumps it, and overflowing {@link #MAX_ACCESSOR_INVOCATIONS}
     * raises {@link ActivationBudgetExceededException} — a typed signal
     * {@link RuleEngine#evaluate} catches separately from the generic
     * {@code Throwable} branch so attacker-shaped wide requests fail closed.
     * A one-element {@code int[]} is the smallest reliable way to share an
     * integer counter across recursive calls without boxing or a dedicated
     * holder class.
     */
    private static Map<String, Object> toMap(Object o, int depth, int[] invocations) {
        if (depth >= MAX_DEPTH) {
            return Collections.emptyMap();
        }
        Map<String, Object> out = new LinkedHashMap<>();
        for (Accessor a : accessorsFor(o.getClass())) {
            if (++invocations[0] > MAX_ACCESSOR_INVOCATIONS) {
                throw new ActivationBudgetExceededException(
                    "activation walk exceeded accessor budget of "
                        + MAX_ACCESSOR_INVOCATIONS + " on " + o.getClass().getName());
            }
            out.put(a.name, convert(a.invoke(o), depth, invocations));
        }
        redactSensitiveConfigValue(o.getClass(), out);
        return out;
    }

    /**
     * Codex/Gemini final-audit P1: a generated config-pair class
     * (eg. {@code AlterableConfig}) exposes a {@code value} field whose
     * sensitivity depends on its sibling {@code name}. The flat
     * {@link #SENSITIVE_NAMES} denylist cannot redact this because the getter
     * is literally named {@code value} — redacting it unconditionally would
     * blind legitimate rules that inspect non-credential configs (eg.
     * {@code retention.ms}).
     *
     * <p>This method runs after the walker has built the nested map for a
     * single instance of a class enumerated in {@link #CONFIG_PAIR_CLASS_NAMES}.
     * If the resulting {@code name} entry matches a known credential pattern
     * ({@code ssl.*.password}, {@code sasl.jaas.config}, etc.) the
     * corresponding {@code value} entry is overwritten with {@code null} so a
     * rule comparing {@code c.value} to a guessed password evaluates to
     * {@code null} (falsy) rather than {@code true} — closing the
     * exfiltration channel.
     *
     * <p>The map key is preserved (not removed) so a hostile rule cannot use
     * {@code !c.value} or {@code c.value == null} to discover that
     * "yes, this name was redacted" — every config-pair carries a {@code value}
     * key, populated or null per the rules above. CEL null is falsy and safe
     * to chain through {@code .startsWith}/{@code .matches}/etc. (see
     * {@link CelProgram} contract).
     */
    private static void redactSensitiveConfigValue(Class<?> klass, Map<String, Object> out) {
        if (!CONFIG_PAIR_CLASS_NAMES.contains(klass.getName())) {
            return;
        }
        Object name = out.get("name");
        // Round-10 audit (LOW, defence-in-depth): redact-by-default when the
        // sibling name field is null or any non-String shape. The wire path
        // cannot produce a null Name today (none of the three schema files
        // — AlterConfigsRequest / IncrementalAlterConfigsRequest /
        // CreateTopicsRequest — declare nullableVersions on the Name field).
        // But if a future schema revision adds nullableVersions, or an
        // in-process caller constructs an instance with setName(null), the
        // prior `name instanceof String && isSensitive(...)` check would
        // silently fail-OPEN and leak the value. The doc on
        // isSensitiveConfigName already states the bias-to-redact policy:
        // "Over-redacting a value with a misleading name is a small CEL-
        // usability inconvenience; under-redacting a real credential is a
        // security incident. When in doubt, redact." Treat any non-String
        // shape as in doubt.
        if (!(name instanceof String) || isSensitiveConfigName((String) name)) {
            // Keep the key — only the value is sensitive.
            out.put("value", null);
        }
    }

    /**
     * True when a Kafka config key is known to carry credential material. The
     * patterns cover the canonical set defined by
     * {@code org.apache.kafka.common.config.SslConfigs} /
     * {@code SaslConfigs} (the {@code ConfigDef.Type.PASSWORD} fields in those
     * classes) plus listener-prefixed variants of those keys and a defensive
     * catch-all for operator-defined sensitive names that follow Kafka's
     * naming convention.
     *
     * <p>Matching is case-insensitive against the full config key. The
     * patterns are:
     * <ul>
     *   <li>ends with {@code .password} — covers
     *       {@code ssl.key.password}, {@code ssl.keystore.password},
     *       {@code ssl.truststore.password} (the three SSL password configs
     *       defined in {@code SslConfigs}), the listener-prefixed forms such
     *       as {@code listener.name.internal.ssl.keystore.password}, plus any
     *       operator-defined {@code *.password} entries.</li>
     *   <li>equals {@code sasl.jaas.config} OR ends with
     *       {@code .sasl.jaas.config} — the SASL login module configuration
     *       carries the SASL principal's password as part of its module
     *       options string. Kafka permits per-listener-and-mechanism overrides
     *       like {@code listener.name.internal.scram-sha-256.sasl.jaas.config}
     *       (defined in {@code ListenerName} / {@code BrokerSecurityConfigs});
     *       the prefixed form holds the same secret material and must be
     *       redacted with the same care. Codex round-2 audit finding.</li>
     *   <li>contains {@code .keystore.key} — PEM-form private key material
     *       (eg. {@code ssl.keystore.key}, the PEM variant of the keystore,
     *       and its listener-prefixed counterparts).</li>
     *   <li>ends with {@code ssl.keystore.certificate.chain} — the PEM
     *       certificate chain that accompanies {@code ssl.keystore.key}.
     *       Although a public certificate chain is not strictly secret,
     *       {@code SslConfigs} classifies it as {@code ConfigDef.Type.PASSWORD}
     *       so that AlterConfigs/IncrementalAlterConfigs round-trip handling
     *       (and broker log output) treats it as opaque. Surfacing it via
     *       the activation map would let CEL rules read material that
     *       upstream redaction explicitly hides. Listener-prefixed forms
     *       (eg. {@code listener.name.internal.ssl.keystore.certificate.chain})
     *       are covered by {@code endsWith}. Codex round-3 P2 finding.</li>
     *   <li>ends with {@code ssl.truststore.certificates} — same reasoning
     *       as the certificate chain: {@code SslConfigs} declares it
     *       PASSWORD-typed; matching here keeps the rule engine consistent
     *       with broker config redaction. Listener-prefixed forms covered
     *       by {@code endsWith}. Codex round-3 P2 finding.</li>
     *   <li>contains {@code secret} — defensive catch-all for any operator
     *       config whose name advertises that it carries a shared secret.</li>
     * </ul>
     *
     * <p>The patterns are deliberately broad. Over-redacting a value with a
     * misleading name (eg. a user-defined config called
     * {@code my.password.policy.enabled}) is a small CEL-usability
     * inconvenience that the operator can rewrite around; under-redacting a
     * real credential value is a security incident. When in doubt, redact.
     */
    private static boolean isSensitiveConfigName(String name) {
        String lc = name.toLowerCase(Locale.ROOT);
        if (lc.endsWith(".password")) {
            return true;
        }
        if (lc.equals("sasl.jaas.config") || lc.endsWith(".sasl.jaas.config")) {
            return true;
        }
        if (lc.contains(".keystore.key")) {
            return true;
        }
        if (isExactOrListenerPrefixed(lc, "ssl.keystore.certificate.chain")) {
            return true;
        }
        if (isExactOrListenerPrefixed(lc, "ssl.truststore.certificates")) {
            return true;
        }
        return lc.contains("secret");
    }

    /**
     * True iff {@code lc} equals {@code key} (bare form) or ends with
     * {@code "." + key} (listener-prefixed form, eg.
     * {@code listener.name.internal.ssl.keystore.certificate.chain}). Both
     * arguments are expected lower-cased.
     */
    private static boolean isExactOrListenerPrefixed(String lc, String key) {
        return lc.equals(key) || lc.endsWith("." + key);
    }

    private static Object convert(Object v, int depth, int[] invocations) {
        if (v == null) {
            return null;
        }
        // Double / Float are opaque to the rule engine. Audit round-7 finding
        // H2 (HIGH): if we passed them through as Number, CelNode.valueEquals
        // and compareValues coerce both sides via ((Number) v).longValue(),
        // which silently truncates fractional values. A predicate like
        // `request.ops.exists(op, op.value == 1)` would then match
        // OpData.value = 1.0, 1.49, 0.5, 0.9 — silent overmatch on every
        // float64 schema field (AlterClientQuotas, DescribeClientQuotas).
        //
        // Surface as null (the key stays in the activation map but the value
        // is explicitly null) so a comparison resolves to false consistently
        // — fail-noisy-on-shape rather than fail-silent-on-truncation.
        // Operators who genuinely need numeric float access in rules will
        // see the predicate never match and surface that requirement
        // explicitly, rather than discovering the truncation in production.
        //
        // The check sits in `convert`, NOT in `convertScalar`, because a
        // null return from `convertScalar` means "fall through to BaseRecords
        // / Iterable / accessorsFor descent" — and Double has a thick set of
        // public no-arg accessors (doubleValue, floatValue, intValue, ...)
        // that the walker would otherwise descend through, eventually
        // reflecting into JDK-internal Stream types and failing with
        // IllegalAccessException at request time.
        if (v instanceof Double || v instanceof Float) {
            return null;
        }
        Object scalar = convertScalar(v);
        if (scalar != null) {
            return scalar;
        }
        // Record payloads (PRODUCE request body, FETCH response body, etc.) are
        // intentionally opaque to the activation walker. Surfacing them via
        // reflection would have two problems:
        //
        //   1. Cost — MemoryRecords/FileRecords expose accessors like
        //      batches() and records() that iterate every RecordBatch and
        //      every Record, and reading a compressed batch decompresses it
        //      eagerly. A single max-size PRODUCE request can carry millions
        //      of inner records; walking them per rule evaluation would
        //      multiply request-path CPU by orders of magnitude.
        //   2. Sensitivity — Record.value() returns the raw application
        //      payload bytes. Producers routinely write credentials, tokens,
        //      PII, and other secrets into record values. A rule of the form
        //      `request.partitionData.exists(p, p.records.batches.exists(b,
        //      b.iterator.exists(r, r.value.startsWith("<guessed prefix>"))))`
        //      would let any operator with rule-write access exfiltrate
        //      application payloads via the rule-engine audit channel, the
        //      same way SENSITIVE_NAMES protects authBytes / SCRAM salt /
        //      delegation HMAC at the protocol layer.
        //
        // We surface only sizeInBytes() — large enough to write usefully
        // size-bounded DENY rules (e.g. "deny PRODUCE requests over N MiB to
        // a specific topic") but not granular enough to leak any byte of the
        // payload itself. The result is a tiny fixed-shape map so CEL rules
        // that probe r.value / r.batches / r.records observe null instead of
        // a walkable subtree.
        if (v instanceof BaseRecords) {
            BaseRecords records = (BaseRecords) v;
            Map<String, Object> descriptor = new LinkedHashMap<>(1);
            descriptor.put("sizeInBytes", (long) records.sizeInBytes());
            return descriptor;
        }
        if (v instanceof Iterable) {
            return convertIterable((Iterable<?>) v, depth, invocations);
        }
        // Anything else with accessors: walk recursively. We do NOT restrict to
        // ApiMessage — nested records inside generated classes implement just
        // Message, and inner-collection element types implement
        // ImplicitLinkedHashCollection.Element. The presence of any field
        // accessor at all is the signal that this is structured data we want
        // to surface, not an opaque scalar.
        if (!accessorsFor(v.getClass()).isEmpty()) {
            return toMap(v, depth + 1, invocations);
        }
        return v;
    }

    private static Object convertScalar(Object v) {
        if (v instanceof String || v instanceof Boolean || v instanceof Long) {
            return v;
        }
        if (v instanceof Integer || v instanceof Short || v instanceof Byte) {
            return ((Number) v).longValue();
        }
        // byte[] / ByteBuffer fields surface ONLY as a {sizeInBytes} descriptor,
        // mirroring the BaseRecords policy. Audit round-6 finding B1 (BLOCKER)
        // / H1 (HIGH): without this normalisation the walker handed back the
        // live byte[] or ByteBuffer, which created two compounding hazards on
        // the rule path:
        //
        //   1. Shape: CEL has no usable contract for a raw byte[] or
        //      ByteBuffer. A rule of the form
        //      `request.requestData == b"<prefix>"` or
        //      `request.requestData.size() > 0` either compares by JVM
        //      reference identity (silently false, even for content-equal
        //      arrays) or raises a CEL type mismatch. Either way, the rule
        //      fails open — RuleEngine.evaluate catches the throw and
        //      returns ALLOW (documented fail-open posture for broken
        //      predicates). A rule the operator believes is enforcing is
        //      silently bypassed on every request.
        //
        //   2. Cost / leak: ByteBuffer turns up on EnvelopeRequestData,
        //      PushTelemetryRequestData, ConsumerProtocolSubscription, and
        //      similar. EnvelopeRequest's payload is a whole *serialised
        //      inner request*, potentially carrying SASL bytes or admin
        //      mutation data, and is bounded only by
        //      socket.request.max.bytes. Handing this back as a live buffer
        //      pinned in the activation map is both a memory hazard and a
        //      defence-in-depth gap relative to the BaseRecords carve-out
        //      that exists precisely so PRODUCE payloads cannot be probed
        //      via rule predicates.
        //
        // SENSITIVE_NAMES already strips authBytes / salt / saltedPassword /
        // hmac / secret accessors at the toMap level, so byte-shaped
        // credentials never reach convertScalar. This branch is the
        // defence-in-depth for the remaining non-credential byte fields
        // (envelope payload, telemetry blob, raft-voter directory id, etc.).
        // Operators wanting size-bounded DENY rules can still write
        // `request.requestData.sizeInBytes > N`.
        if (v instanceof byte[]) {
            Map<String, Object> descriptor = new LinkedHashMap<>(1);
            descriptor.put("sizeInBytes", (long) ((byte[]) v).length);
            return descriptor;
        }
        if (v instanceof ByteBuffer) {
            Map<String, Object> descriptor = new LinkedHashMap<>(1);
            descriptor.put("sizeInBytes", (long) ((ByteBuffer) v).remaining());
            return descriptor;
        }
        if (v instanceof Number) {
            return v;
        }
        // Uuid identifies topics (MetadataRequest v10+, FetchRequest v13+,
        // OffsetForLeaderEpoch, DeleteTopics by id, etc.). Without this branch
        // the walker would descend into Uuid via its mostSignificantBits/
        // leastSignificantBits accessors and emit a two-entry map. A rule
        // author writing the natural predicate
        // `request.topics.exists(t, t.topicId == "<base64url>")` would then
        // silently always evaluate to false — a rule-bypass-by-shape failure
        // mode the engine cannot diagnose. Normalising to Kafka's canonical
        // base64url string form (Uuid.toString) makes id-based rules behave
        // the way operators expect: identifiers go in as strings, comparisons
        // use string equality / startsWith / matches. Audit round-5 finding
        // afa1a332 (MEDIUM).
        if (v instanceof Uuid) {
            return v.toString();
        }
        return null;
    }

    private static List<Object> convertIterable(Iterable<?> it, int depth, int[] invocations) {
        // Round-15 Walker HIGH H1: depth bound must apply to iterable chains
        // too. Before this guard, depth only incremented when toMap descended
        // into a Message (convert -> toMap(v, depth + 1)). An attacker-shaped
        // value of type Iterable<Iterable<Iterable<...>>> with no Message at
        // the leaves walked convert -> convertIterable -> convert ->
        // convertIterable -> ... at constant depth = 0 forever. The
        // per-element accessor-budget bump bounds aggregate work but does
        // not bound stack depth — a single-element chain N deep blows the
        // JVM stack around N ~= 5..10k well before MAX_ACCESSOR_INVOCATIONS
        // = 10_000 fires (each frame ~100B, default 512KB stack). Bumping
        // depth on each iterable layer aligns stack depth with the depth
        // counter, and the explicit depth check at the top of this method
        // raises a typed ActivationBudgetExceededException — same fail-CLOSED
        // signal RuleEngine catches separately from the generic Throwable
        // branch — instead of letting recursion run until the JVM dies.
        //
        // The check fires BEFORE constructing the result list so a
        // pathological deep chain costs O(MAX_DEPTH) frames and one
        // exception, not O(N) heap allocations.
        if (depth >= MAX_DEPTH) {
            throw new ActivationBudgetExceededException(
                "activation walk exceeded depth limit of " + MAX_DEPTH
                    + " while walking iterable of " + it.getClass().getName());
        }
        // Codex deep-audit P1d: the per-iteration bump is what bounds a wide
        // flat scalar list. Without it, an attacker who can fit a giant
        // repeated scalar field (e.g. millions of partition ids or topic
        // names) into a max-size protocol request gets an unbudgeted O(N)
        // list copy through this method, since scalar elements go through
        // convertScalar() which never touches `invocations`. Counting each
        // element against the same MAX_ACCESSOR_INVOCATIONS budget folds
        // iterable widths into the same overall walk-cost ceiling that
        // bounds nested-object accessor invocations.
        List<Object> list = new ArrayList<>();
        for (Object item : it) {
            if (++invocations[0] > MAX_ACCESSOR_INVOCATIONS) {
                throw new ActivationBudgetExceededException(
                    "activation walk exceeded accessor budget of "
                        + MAX_ACCESSOR_INVOCATIONS
                        + " while walking iterable of "
                        + it.getClass().getName());
            }
            // Round-15 Walker HIGH H1: bump depth on each iterable layer so
            // the MAX_DEPTH guard at the top is meaningful. A pure Iterable
            // chain (no Messages at the leaves) without this bump descends
            // at constant depth = caller's depth and can stack-blow before
            // the accessor budget fires.
            list.add(convert(item, depth + 1, invocations));
        }
        return list;
    }

    /**
     * Visible for testing — exposes the per-Class accessor cache so tests
     * can confirm that repeated lookups return the same instance (no
     * re-discovery on the hot path).
     */
    static List<Accessor> accessorsFor(Class<?> klass) {
        return CACHE.computeIfAbsent(klass, ApiMessageActivation::discover);
    }

    private static List<Accessor> discover(Class<?> klass) {
        List<Accessor> out = new ArrayList<>();
        for (Method m : klass.getMethods()) {
            if (!isFieldAccessor(m)) {
                continue;
            }
            out.add(new Accessor(m.getName(), m));
        }
        // Stable order keeps test output predictable and serialisation diffable.
        out.sort(Comparator.comparing(a -> a.name));
        return Collections.unmodifiableList(out);
    }

    private static boolean isFieldAccessor(Method m) {
        if (Modifier.isStatic(m.getModifiers())) {
            return false;
        }
        if (m.getParameterCount() != 0) {
            return false;
        }
        if (m.getReturnType() == void.class) {
            return false;
        }
        if (m.isSynthetic() || m.isBridge()) {
            return false;
        }
        if (m.getDeclaringClass() == Object.class) {
            return false;
        }
        String name = m.getName();
        if (EXCLUDED_NAMES.contains(name)) {
            return false;
        }
        // Case-insensitive match against the credential-bearing field denylist.
        // See SENSITIVE_NAMES javadoc for the rationale and the enumerated list.
        if (SENSITIVE_NAMES.contains(name.toLowerCase(java.util.Locale.ROOT))) {
            return false;
        }
        return true;
    }

    static final class Accessor {
        final String name;
        private final Method method;

        Accessor(String name, Method method) {
            this.name = name;
            this.method = method;
        }

        Object invoke(Object target) {
            try {
                return method.invoke(target);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(
                    "failed to invoke " + name + " on " + target.getClass().getName(), e);
            }
        }
    }
}
