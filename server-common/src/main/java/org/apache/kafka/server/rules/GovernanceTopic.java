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
 * CEL rule envelopes, and the sentinel client-id prefix the broker uses when
 * reading from it.
 *
 * <p>The topic is a standard Kafka compacted topic — any stock producer can
 * write to it. Rules are encoded as JSON envelopes
 * ({@link org.apache.kafka.server.rules.json.RuleJsonCodec}) keyed by rule id;
 * tombstones (null values) delete a rule.
 *
 * <p>The broker's own consumer of this topic must use a client-id that starts
 * with {@link RuleEngine#INTERNAL_CLIENT_ID_PREFIX} so it is unconditionally
 * exempt from rule evaluation. Without that, a "deny everything" rule would
 * block the very read that loads its replacement.
 */
public final class GovernanceTopic {

    /** Topic name. Starts with {@code __} to follow the Kafka internal-topic convention. */
    public static final String NAME = "__governance";

    /**
     * Suggested client-id for the broker's internal reader of this topic.
     * Must start with {@link RuleEngine#INTERNAL_CLIENT_ID_PREFIX} so it is
     * exempt from rule evaluation. The {@code reader-<brokerId>} suffix lets
     * each broker's reader be distinguished in client-quota and request-log
     * output.
     */
    public static String readerClientId(int brokerId) {
        return RuleEngine.INTERNAL_CLIENT_ID_PREFIX + "reader-" + brokerId;
    }

    private GovernanceTopic() {
    }
}
