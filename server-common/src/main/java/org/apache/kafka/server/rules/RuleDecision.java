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

import java.util.Objects;

/**
 * Outcome of evaluating a request against the active {@link RuleSet}.
 * Use {@link #ALLOW} for the fast path (no DENY rule matched) and
 * {@link #deny(int, String)} when a DENY rule short-circuited evaluation.
 */
public final class RuleDecision {

    public static final RuleDecision ALLOW = new RuleDecision(false, 0, null);

    private final boolean denied;
    private final int errorCode;
    private final String denyingRuleId;

    private RuleDecision(boolean denied, int errorCode, String denyingRuleId) {
        this.denied = denied;
        this.errorCode = errorCode;
        this.denyingRuleId = denyingRuleId;
    }

    public static RuleDecision allow() {
        return ALLOW;
    }

    public static RuleDecision deny(int errorCode, String denyingRuleId) {
        return new RuleDecision(true, errorCode, Objects.requireNonNull(denyingRuleId, "denyingRuleId"));
    }

    public boolean denied() {
        return denied;
    }

    public int errorCode() {
        return errorCode;
    }

    public String denyingRuleId() {
        return denyingRuleId;
    }

    @Override
    public String toString() {
        return denied
            ? "Deny(errorCode=" + errorCode + ", rule=" + denyingRuleId + ")"
            : "Allow";
    }
}
