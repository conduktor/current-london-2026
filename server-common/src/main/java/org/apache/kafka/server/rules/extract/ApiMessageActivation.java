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

import org.apache.kafka.common.protocol.ApiMessage;

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
     * Maximum recursion depth for the reflection walk. Each entry into
     * {@link #toMap(Object, int)} (the recursive call for a nested message)
     * counts as one level. Kafka's generated DTOs do not contain reference
     * cycles, so any legitimate message is far shallower than this. The cap
     * is defense in depth against a future protocol with deeper nesting than
     * we anticipated, or a hostile request that constructed a cycle by other
     * means — the walker cannot tell the difference and must not be allowed to
     * blow the stack or spin forever.
     */
    static final int MAX_DEPTH = 32;

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
        return toMap(o, 0);
    }

    /**
     * Recursive form with depth counter. {@code depth} is incremented on each
     * descent into a nested message. When it reaches {@link #MAX_DEPTH} we
     * return an empty map rather than recurse further — the rule sees the
     * upper levels intact, the bottom is truncated. See {@link #MAX_DEPTH}
     * javadoc for why this matters.
     */
    private static Map<String, Object> toMap(Object o, int depth) {
        if (depth >= MAX_DEPTH) {
            return Collections.emptyMap();
        }
        Map<String, Object> out = new LinkedHashMap<>();
        for (Accessor a : accessorsFor(o.getClass())) {
            out.put(a.name, convert(a.invoke(o), depth));
        }
        return out;
    }

    private static Object convert(Object v, int depth) {
        if (v == null) {
            return null;
        }
        Object scalar = convertScalar(v);
        if (scalar != null) {
            return scalar;
        }
        if (v instanceof Iterable) {
            return convertIterable((Iterable<?>) v, depth);
        }
        // Anything else with accessors: walk recursively. We do NOT restrict to
        // ApiMessage — nested records inside generated classes implement just
        // Message, and inner-collection element types implement
        // ImplicitLinkedHashCollection.Element. The presence of any field
        // accessor at all is the signal that this is structured data we want
        // to surface, not an opaque scalar.
        if (!accessorsFor(v.getClass()).isEmpty()) {
            return toMap(v, depth + 1);
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
        if (v instanceof Number || v instanceof byte[] || v instanceof ByteBuffer) {
            return v;
        }
        return null;
    }

    private static List<Object> convertIterable(Iterable<?> it, int depth) {
        List<Object> list = new ArrayList<>();
        for (Object item : it) {
            list.add(convert(item, depth));
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
