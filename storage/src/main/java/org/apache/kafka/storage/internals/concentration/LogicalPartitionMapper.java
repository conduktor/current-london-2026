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

/**
 * Deterministic mapping from a logical partition to its backing partition.
 * Simple modulo: stable, covers all backing partitions when N &ge; M, and is trivially
 * derivable without persisted metadata so a recovering broker can verify routing.
 */
public final class LogicalPartitionMapper {

    private LogicalPartitionMapper() {
    }

    public static int backingPartitionFor(LogicalTopicDescriptor descriptor, int logicalPartition) {
        if (logicalPartition < 0 || logicalPartition >= descriptor.numLogicalPartitions()) {
            throw new IllegalArgumentException(
                "logicalPartition " + logicalPartition + " out of range [0, "
                    + descriptor.numLogicalPartitions() + ") for " + descriptor.logicalName());
        }
        return logicalPartition % descriptor.numBackingPartitions();
    }
}
