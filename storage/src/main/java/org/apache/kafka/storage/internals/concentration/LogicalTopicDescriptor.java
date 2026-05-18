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
