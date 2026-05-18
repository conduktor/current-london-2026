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
package org.apache.kafka.storage.internals.concentration;

import org.apache.kafka.common.internals.Topic;

import java.util.Objects;

/**
 * Immutable declaration of a logical topic concentrated onto a backing physical topic.
 * N logical partitions are multiplexed over M backing partitions, with N &ge; M.
 */
public record LogicalTopicDescriptor(
    String logicalName,
    int numLogicalPartitions,
    String backingTopic,
    int numBackingPartitions
) {
    public LogicalTopicDescriptor {
        Objects.requireNonNull(logicalName, "logicalName");
        Objects.requireNonNull(backingTopic, "backingTopic");
        if (logicalName.isBlank()) {
            throw new IllegalArgumentException("logicalName must not be blank");
        }
        if (backingTopic.isBlank()) {
            throw new IllegalArgumentException("backingTopic must not be blank");
        }
        // Enforce the same name rules stock Kafka applies on CreateTopics: length, allowed chars,
        // reserved names. Without this an operator could declare "logical:foo" (invalid char) or a
        // 300-character name, and the protocol layer would then reject every produce/fetch with a
        // confusing INVALID_TOPIC_EXCEPTION at request time instead of at declare time.
        Topic.validate(logicalName, "logicalName", message -> {
            throw new IllegalArgumentException(message);
        });
        Topic.validate(backingTopic, "backingTopic", message -> {
            throw new IllegalArgumentException(message);
        });
        // Reject declarations that would shadow internal topics. The METADATA path stamps
        // isInternal=false on synthesized logical responses (KafkaApis.scala), so a logical
        // declaration named "__consumer_offsets" would surface to clients as a normal topic and
        // mis-route group coordinator traffic. Same reasoning for the backing topic — concentration
        // is for application data, not for piling tenant payload onto coordinator state.
        if (Topic.isInternal(logicalName)) {
            throw new IllegalArgumentException(
                "logicalName must not shadow a Kafka internal topic; received '" + logicalName + "'");
        }
        if (Topic.isInternal(backingTopic)) {
            throw new IllegalArgumentException(
                "backingTopic must not be a Kafka internal topic; received '" + backingTopic + "'");
        }
        if (logicalName.equals(backingTopic)) {
            throw new IllegalArgumentException(
                "logicalName must differ from backingTopic; received both as '" + logicalName + "'");
        }
        if (numLogicalPartitions < 1) {
            throw new IllegalArgumentException(
                "numLogicalPartitions must be >= 1, was " + numLogicalPartitions);
        }
        if (numBackingPartitions < 1) {
            throw new IllegalArgumentException(
                "numBackingPartitions must be >= 1, was " + numBackingPartitions);
        }
        if (numLogicalPartitions < numBackingPartitions) {
            throw new IllegalArgumentException(
                "numLogicalPartitions (" + numLogicalPartitions
                    + ") must be >= numBackingPartitions (" + numBackingPartitions + ")");
        }
    }
}
