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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.record.CompressionType;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Server-side policy for compression of producer batches written to a topic.
 *
 * Registered as the topic config {@code compression.policy}. The default {@link #NONE}
 * preserves vanilla Kafka behaviour: no per-batch enforcement happens on the produce
 * path. Setting it to {@link #REQUIRED} causes the broker to reject batches whose
 * {@link CompressionType} is {@code NONE}, per-partition, with {@code INVALID_RECORD}.
 *
 * Enforcement runs in the produce request handler, ahead of the replication and log
 * layers. Replication, transaction state, and group-coordinator appends therefore
 * bypass the check by construction: already-stored batches remain replicable even
 * if the policy is enabled after the fact.
 */
public enum CompressionPolicy {
    NONE("none") {
        @Override
        public boolean isViolatedBy(CompressionType batchCompression) {
            return false;
        }
    },
    REQUIRED("required") {
        @Override
        public boolean isViolatedBy(CompressionType batchCompression) {
            return batchCompression == CompressionType.NONE;
        }
    };

    private final String value;

    CompressionPolicy(String value) {
        this.value = value;
    }

    /**
     * @return the configuration value (e.g. {@code "none"}, {@code "required"}) under which
     *         this policy is exposed in the topic config. Deliberately not called {@code name}
     *         so it does not shadow {@link Enum#name()}.
     */
    public String value() {
        return value;
    }

    /**
     * @return {@code true} if a batch with the given compression type violates this policy and
     *         must be rejected with {@code INVALID_RECORD}.
     */
    public abstract boolean isViolatedBy(CompressionType batchCompression);

    public static List<String> names() {
        return Stream.of(values()).map(p -> p.value).collect(Collectors.toList());
    }

    public static CompressionPolicy forName(String n) {
        String lower = n.toLowerCase(Locale.ROOT);
        return Stream.of(values())
            .filter(p -> p.value.equals(lower))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown compression policy: " + n));
    }
}
