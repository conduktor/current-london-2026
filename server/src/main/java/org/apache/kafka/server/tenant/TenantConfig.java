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
package org.apache.kafka.server.tenant;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.network.ListenerName;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Broker-wide view of which listeners are bound to which tenant id. Built once
 * at broker startup from the {@code listener.name.<lname>.tenant.id} keys in
 * broker config. Listener names are normalised to upper case at lookup time to
 * match the case-insensitive contract used elsewhere in Kafka.
 *
 * <p>This is the lookup table KafkaApis / ControllerApis consult to decide
 * whether to build a tenant-aware {@link TenantContext} for an incoming
 * request and whether to enforce the privileged-on-tenant-listener guard.
 */
public final class TenantConfig {

    public static final String TENANT_ID_KEY = "tenant.id";
    public static final String LISTENER_PREFIX = "listener.name.";

    private static final TenantConfig EMPTY = new TenantConfig(new HashMap<>());

    private final Map<String, String> bindingsByListener;  // upper-cased keys

    private TenantConfig(Map<String, String> bindingsByListener) {
        this.bindingsByListener = bindingsByListener;
    }

    public static TenantConfig empty() {
        return EMPTY;
    }

    public static TenantConfig from(Map<String, ?> brokerProps) {
        Map<String, String> bindings = new HashMap<>();
        for (Map.Entry<String, ?> e : brokerProps.entrySet()) {
            String key = e.getKey();
            if (!key.startsWith(LISTENER_PREFIX)) {
                continue;
            }
            int suffixStart = key.indexOf('.', LISTENER_PREFIX.length());
            if (suffixStart < 0) {
                continue;
            }
            String suffix = key.substring(suffixStart + 1);
            if (!suffix.equals(TENANT_ID_KEY)) {
                continue;
            }
            String listener = key.substring(LISTENER_PREFIX.length(), suffixStart);
            String tenantId = String.valueOf(e.getValue()).trim();
            if (tenantId.isEmpty()) {
                continue;
            }
            try {
                TenantNamespace.validateTenantId(tenantId);
            } catch (IllegalArgumentException ex) {
                throw new ConfigException(key, e.getValue(), ex.getMessage());
            }
            bindings.put(listener.toUpperCase(Locale.ROOT), tenantId);
        }
        if (bindings.isEmpty()) {
            return EMPTY;
        }
        return new TenantConfig(bindings);
    }

    public Optional<String> boundTenantFor(ListenerName listenerName) {
        if (listenerName == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(
            bindingsByListener.get(listenerName.value().toUpperCase(Locale.ROOT)));
    }

    public boolean isTenantBoundListener(ListenerName listenerName) {
        return boundTenantFor(listenerName).isPresent();
    }

    public boolean isEmpty() {
        return bindingsByListener.isEmpty();
    }

    /**
     * Every tenant id with at least one listener binding on this broker. Used by
     * cluster-wide handlers to refuse requests that would create or address
     * resources in a known tenant's namespace from an unbound listener — the
     * "outside-in pollution" trap a super-user on an open listener could
     * otherwise drop topics into.
     */
    public Set<String> allTenants() {
        return Set.copyOf(bindingsByListener.values());
    }
}
