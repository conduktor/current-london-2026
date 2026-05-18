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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Broker-side registry of logical topics and their backing physical topics. Consulted on the
 * produce path (route logical→backing; reject physical-name produces against a backing) and on
 * the fetch path.
 *
 * <p>Declarations are validated for cross-descriptor consistency: multiple logical topics
 * sharing one backing must agree on the backing partition count M, and logical and backing
 * names must form disjoint namespaces so the broker can resolve routing unambiguously.
 */
public final class LogicalTopicRegistry {

    private final ConcurrentHashMap<String, LogicalTopicDescriptor> byLogicalName = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<LogicalTopicDescriptor>> byBackingTopic = new ConcurrentHashMap<>();
    private final Object writeLock = new Object();

    public void declare(LogicalTopicDescriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");
        synchronized (writeLock) {
            LogicalTopicDescriptor existing = byLogicalName.get(descriptor.logicalName());
            if (existing != null) {
                if (!existing.equals(descriptor)) {
                    throw new IllegalStateException(
                        "logical topic '" + descriptor.logicalName()
                            + "' is already declared with " + existing + "; rejecting " + descriptor);
                }
                return; // idempotent re-declare
            }
            if (byBackingTopic.containsKey(descriptor.logicalName())) {
                throw new IllegalStateException(
                    "name '" + descriptor.logicalName()
                        + "' is already in use as a backing topic; logical and backing namespaces are disjoint");
            }
            if (byLogicalName.containsKey(descriptor.backingTopic())) {
                throw new IllegalStateException(
                    "name '" + descriptor.backingTopic()
                        + "' is already in use as a logical topic; logical and backing namespaces are disjoint");
            }
            List<LogicalTopicDescriptor> siblings = byBackingTopic.get(descriptor.backingTopic());
            if (siblings != null && !siblings.isEmpty()) {
                LogicalTopicDescriptor sibling = siblings.get(0);
                if (sibling.numBackingPartitions() != descriptor.numBackingPartitions()) {
                    throw new IllegalStateException(
                        "backing '" + descriptor.backingTopic() + "' is already concentrated with M="
                            + sibling.numBackingPartitions() + "; cannot accept " + descriptor.numBackingPartitions());
                }
            }
            byLogicalName.put(descriptor.logicalName(), descriptor);
            byBackingTopic.computeIfAbsent(descriptor.backingTopic(), b -> new ArrayList<>()).add(descriptor);
        }
    }

    public boolean contains(String logicalName) {
        return byLogicalName.containsKey(logicalName);
    }

    public Optional<LogicalTopicDescriptor> get(String logicalName) {
        return Optional.ofNullable(byLogicalName.get(logicalName));
    }

    public boolean isBackingTopic(String physicalName) {
        List<LogicalTopicDescriptor> list = byBackingTopic.get(physicalName);
        return list != null && !list.isEmpty();
    }

    public Collection<LogicalTopicDescriptor> descriptorsFor(String backingTopic) {
        List<LogicalTopicDescriptor> list = byBackingTopic.get(backingTopic);
        if (list == null) return Collections.emptyList();
        synchronized (writeLock) {
            return List.copyOf(list);
        }
    }

    /**
     * Immutable snapshot of every declared logical topic. Used at broker startup so the recovery
     * pass can enumerate the registry-known partitions and seed their sidecars from disk. Safe to
     * iterate without holding any lock — the returned list is a copy.
     */
    public Collection<LogicalTopicDescriptor> all() {
        return List.copyOf(byLogicalName.values());
    }
}
