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

import org.apache.kafka.common.config.ConfigException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Parses the {@code concentration.logical.topics} broker config into a list of
 * {@link LogicalTopicDescriptor}s. The string carries comma-separated
 * {@code logical:N:backing:M} quads, e.g. {@code orders:100:shared:4,payments:50:shared:4}.
 *
 * <p>This is the v1 shortcut for hook #6 (admin declaration). The long-term path is a KRaft
 * metadata record so declarations survive controller restarts and are replicated to followers.
 * Until then, broker operators put the declarations in {@code server.properties}; the broker
 * reads them at startup and calls {@link ConcentrationKernel#declare}.
 *
 * <p>Malformed input throws {@link ConfigException} — the standard Kafka startup-time
 * rejection. Each error message quotes the offending entry and includes its 1-based index in
 * the input list, so an operator can locate the problem in a long config line without trial
 * and error.
 */
public final class LogicalTopicConfigParser {

    private LogicalTopicConfigParser() { }

    public static List<LogicalTopicDescriptor> parse(String raw) {
        if (raw == null || raw.isBlank()) {
            return Collections.emptyList();
        }
        String[] entries = raw.split(",");
        List<LogicalTopicDescriptor> descriptors = new ArrayList<>(entries.length);
        for (int i = 0; i < entries.length; i++) {
            String entry = entries[i].trim();
            if (entry.isEmpty()) {
                // Trailing comma or "a,,b" — tolerated; matches Kafka's own list-config behaviour.
                continue;
            }
            descriptors.add(parseEntry(entry, i + 1));
        }
        return descriptors;
    }

    private static LogicalTopicDescriptor parseEntry(String entry, int index) {
        String[] tokens = entry.split(":");
        if (tokens.length != 4) {
            throw new ConfigException(
                "concentration.logical.topics entry #" + index + " '" + entry
                    + "' is invalid: expected logical:N:backing:M (4 colon-separated tokens), got " + tokens.length);
        }
        String logical = tokens[0].trim();
        String backing = tokens[2].trim();
        int n;
        int m;
        try {
            n = Integer.parseInt(tokens[1].trim());
            m = Integer.parseInt(tokens[3].trim());
        } catch (NumberFormatException nfe) {
            throw new ConfigException(
                "concentration.logical.topics entry #" + index + " '" + entry
                    + "' is invalid: partition counts must be integers (" + nfe.getMessage() + ")");
        }
        try {
            return new LogicalTopicDescriptor(logical, n, backing, m);
        } catch (IllegalArgumentException iae) {
            throw new ConfigException(
                "concentration.logical.topics entry #" + index + " '" + entry
                    + "' is invalid: " + iae.getMessage());
        }
    }
}
