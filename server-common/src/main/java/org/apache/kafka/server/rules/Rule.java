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
        for (ApiKeys k : apiKeys) {
            if (k == null) {
                throw new NullPointerException("apiKeys contains null");
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

    @Override
    public String toString() {
        return "Rule(" + id + ", keys=" + apiKeys + ", action=" + action
            + ", when=" + whenSource + ", errorCode=" + errorCode + ")";
    }
}
