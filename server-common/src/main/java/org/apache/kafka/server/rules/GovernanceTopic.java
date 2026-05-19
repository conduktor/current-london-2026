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

/**
 * Constants identifying the broker-internal compacted topic that distributes
 * CEL rule envelopes.
 *
 * <p>The topic is a standard Kafka compacted topic — any stock producer can
 * write to it. Rules are encoded as JSON envelopes
 * ({@link org.apache.kafka.server.rules.json.RuleJsonCodec}) keyed by rule id;
 * tombstones (null values) delete a rule.
 *
 * <p><b>The broker reads {@code __governance} by reading the local log
 * directly via {@code ReplicaManager.getLog} — there is no internal consumer
 * and no client-id on the wire.</b> Operator-tooling consumers of this topic
 * (admin clients, audit pipelines) ARE network clients, and they are exempt
 * from rule evaluation only if both: (a) they arrive on a privileged
 * (inter-broker) listener — the {@code fromPrivilegedListener} flag the
 * network layer attaches to every request based on which TCP listener
 * accepted the connection, which external clients cannot forge — AND (b)
 * their authenticated peer principal is enrolled in
 * {@code governance.bypass.principals}. The broker refuses to start with an
 * empty allow-list (Codex round-3 P0), so by run-time the second condition is
 * always meaningfully enforced. See
 * {@link RuleEngine#evaluate(org.apache.kafka.common.protocol.ApiKeys, String, String, boolean, java.util.function.Supplier)}
 * for the bypass contract.
 */
public final class GovernanceTopic {

    /** Topic name. Starts with {@code __} to follow the Kafka internal-topic convention. */
    public static final String NAME = "__governance";

    private GovernanceTopic() {
    }
}
