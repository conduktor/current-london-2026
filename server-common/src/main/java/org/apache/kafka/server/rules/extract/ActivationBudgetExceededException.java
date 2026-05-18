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

/**
 * Signals that {@link ApiMessageActivation} aborted a request-activation walk
 * because the walk would exceed one of its hard budgets
 * ({@link ApiMessageActivation#MAX_ACCESSOR_INVOCATIONS}).
 *
 * <p>This is deliberately a distinct exception class from the
 * {@link Throwable} catch-all in
 * {@code org.apache.kafka.server.rules.RuleEngine#evaluate}: budget overflow
 * is an attacker-shaped event (a request constructed to be pathologically wide
 * — e.g. an {@code OffsetFetch} with 10000+ partition indexes, or a
 * {@code CreateTopics} with 10000+ topic descriptors), NOT a broker bug. The
 * generic-Throwable catch fails the request <em>open</em> as a defensive
 * posture against buggy CEL extractors that might otherwise crash the
 * request thread. Failing budget-overflow open is wrong for the same reason
 * the cap exists in the first place: it would let any external client evade
 * every DENY rule on the targeted API simply by inflating a single repeated
 * field past the budget. The engine catches this exception specifically,
 * ahead of the {@code Throwable} branch, and fails the request
 * <em>closed</em> with a synthetic DENY decision.
 *
 * <p>The fix-side rationale lives in {@code RuleEngine.evaluate}; this class
 * is the typed signal that lets the two policies (broker-bug fail-open vs
 * attacker-shape fail-closed) sit side by side cleanly.
 */
public final class ActivationBudgetExceededException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ActivationBudgetExceededException(String message) {
        super(message);
    }
}
