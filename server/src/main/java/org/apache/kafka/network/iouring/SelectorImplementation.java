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
package org.apache.kafka.network.iouring;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.network.SocketServerConfigs;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Which broker-side I/O backend to use for accepting and serving client connections.
 *
 * <p>Selection happens once at broker startup; there is no per-request dispatch.
 * {@link #AUTO} resolves to {@link #IO_URING} on a Linux host with kernel io_uring support and
 * a PLAINTEXT listener, and to {@link #NIO} everywhere else.
 */
public enum SelectorImplementation {
    /** Java NIO Selector-based path; the historical Kafka default. */
    NIO("nio"),
    /** Netty io_uring-backed path; Linux + PLAINTEXT only in this version. */
    IO_URING("io_uring"),
    /** Pick {@link #IO_URING} when supported, otherwise {@link #NIO}. */
    AUTO("auto");

    private final String configValue;

    SelectorImplementation(String configValue) {
        this.configValue = configValue;
    }

    public String configValue() {
        return configValue;
    }

    public static SelectorImplementation fromConfig(String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new ConfigException(SocketServerConfigs.SOCKET_SELECTOR_IMPLEMENTATION_CONFIG,
                value, "must be one of " + allowedValues());
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        for (SelectorImplementation impl : values()) {
            if (impl.configValue.equals(normalized)) {
                return impl;
            }
        }
        throw new ConfigException(SocketServerConfigs.SOCKET_SELECTOR_IMPLEMENTATION_CONFIG,
            value, "must be one of " + allowedValues());
    }

    private static String allowedValues() {
        return Arrays.stream(values())
            .map(SelectorImplementation::configValue)
            .collect(Collectors.joining(", "));
    }
}
