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
import org.apache.kafka.server.rules.cel.CelProgram;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

/**
 * Immutable record of a single broker rule. The {@link #id()} is the
 * compacted-topic key used by the loader for tombstones; the {@link #compiled()}
 * program is built once at load time and reused on every evaluation.
 */
public final class Rule {

    private final String id;
    private final List<ApiKeys> apiKeys;
    private final RuleAction action;
    private final String whenSource;
    private final int errorCode;
    private final CelProgram compiled;

    public Rule(String id,
                List<ApiKeys> apiKeys,
                RuleAction action,
                String whenSource,
                int errorCode,
                CelProgram compiled) {
        this.id = Objects.requireNonNull(id, "id");
        Objects.requireNonNull(apiKeys, "apiKeys");
        if (apiKeys.isEmpty()) {
            throw new IllegalArgumentException("rule must target at least one apiKey");
        }
        // R28 adversarial (#246): reject duplicates. The codec
        // (RuleJsonCodec.parseApiKeys) dedupes operator input via
        // LinkedHashSet — but the Rule constructor is also called from tests
        // and would otherwise admit a list like [METADATA, METADATA], which
        // (i) charges the per-API-key cap (RuleSetBuilder.put) by 2,
        // (ii) duplicates the rule in byKey[METADATA] (RuleSetBuilder.build),
        // and (iii) double-evaluates the rule per request (RuleEngine.evaluate)
        // — silently double-charging the CEL step budget when the rule does
        // not fire on the first hit. The codec already dedupes before
        // construction, so a duplicate-bearing list reaching this constructor
        // is a caller bug; surface it rather than swallow it.
        EnumSet<ApiKeys> seen = EnumSet.noneOf(ApiKeys.class);
        for (ApiKeys k : apiKeys) {
            if (k == null) {
                throw new NullPointerException("apiKeys contains null");
            }
            if (!seen.add(k)) {
                throw new IllegalArgumentException(
                    "apiKeys contains duplicate '" + k.name() + "'; the codec "
                        + "dedupes operator input — a duplicate reaching this "
                        + "constructor is a caller bug that would double-charge "
                        + "the per-API-key rule cap and per-request CEL budget");
            }
        }
        this.apiKeys = Collections.unmodifiableList(new ArrayList<>(apiKeys));
        this.action = Objects.requireNonNull(action, "action");
        this.whenSource = Objects.requireNonNull(whenSource, "whenSource");
        this.errorCode = errorCode;
        this.compiled = Objects.requireNonNull(compiled, "compiled");
    }

    public String id() {
        return id;
    }

    public List<ApiKeys> apiKeys() {
        return apiKeys;
    }

    public RuleAction action() {
        return action;
    }

    public String whenSource() {
        return whenSource;
    }

    public int errorCode() {
        return errorCode;
    }

    public CelProgram compiled() {
        return compiled;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Rule)) {
            return false;
        }
        Rule r = (Rule) o;
        return errorCode == r.errorCode
            && id.equals(r.id)
            && apiKeys.equals(r.apiKeys)
            && action == r.action
            && whenSource.equals(r.whenSource);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, apiKeys, action, whenSource, errorCode);
    }

    /**
     * R34-C-1 [HIGH]: the {@code whenSource} field is operator-authored and
     * its bytes — while constrained to a JSON string at codec intake — may
     * include escaped control codepoints ({@code "\\u001B"}, {@code "\\u0007"},
     * etc.) that Jackson decodes to actual C0/C1 bytes in the Java String.
     * The CEL lexer's quoted-string path admits these bytes (only the small
     * {@code \\n \\t \\r \\\\ \\" \\'} escape set is recognised; every other
     * byte between quotes passes through verbatim), so the post-compile
     * {@code whenSource} we store here can carry raw control bytes despite
     * the JSON envelope being well-formed.
     *
     * <p>No production log site currently invokes {@code Rule.toString()}
     * — but the method is a latent log-injection footgun: a future
     * {@code LOG.warn("rule fired: {}", rule)} or audit-trail dump would
     * paste raw CR/LF/ANSI/bidi-isolate bytes straight into broker.log.
     * Running both the rule id and the {@code whenSource} through
     * {@link LogSafe#sanitize(String)} here closes that footgun structurally
     * — the same posture the per-rule eval-error WARN in
     * {@link RuleEngine#maybeWarnEvalError} already takes for the rule id.
     * The id is already validated against forbidden codepoints at codec
     * intake ({@code RuleJsonCodec.validateRuleId}), so re-sanitising it is
     * defensive — a future codec change that admits a control byte in id
     * would not silently re-open this site.
     *
     * <p>This does NOT touch {@link #whenSource()} — code that needs the
     * raw source (e.g. {@code RuleJsonCodec.encode}, which round-trips
     * through Jackson and re-escapes control bytes anyway) gets the
     * authentic value. Only the human-facing {@code toString()} renders
     * the sanitised form.
     */
    @Override
    public String toString() {
        return "Rule(" + LogSafe.sanitize(id) + ", keys=" + apiKeys + ", action=" + action
            + ", when=" + LogSafe.sanitize(whenSource) + ", errorCode=" + errorCode + ")";
    }
}
