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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    public static final String PRINCIPAL_BUILDER_CLASS_KEY = "principal.builder.class";

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

    /**
     * Listener names (upper-cased) that have a {@code tenant.id} binding in the
     * supplied broker originals. Returned in declaration order. Exposed for the
     * principal-builder integrity check in {@link #validatePrincipalBuilderBindings};
     * production code should prefer {@link #boundTenantFor} via a constructed
     * instance.
     */
    static List<String> listenersWithTenantBinding(Map<String, ?> brokerProps) {
        List<String> listeners = new ArrayList<>();
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
            String value = String.valueOf(e.getValue()).trim();
            if (value.isEmpty()) {
                continue;
            }
            listeners.add(key.substring(LISTENER_PREFIX.length(), suffixStart));
        }
        return listeners;
    }

    /**
     * Refuse to start a broker whose tenant-bound listener is paired with the
     * wrong principal builder. Codex round-3 flagged this as a HIGH defect: a
     * listener with {@code tenant.id} but no {@link TenantPrincipalBuilder}
     * resolves to {@link org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder}
     * (or whatever else is configured), which produces an unstamped principal.
     * KafkaApis then sees a privileged principal arriving on a tenant-bound
     * listener and refuses every request as {@code TOPIC_AUTHORIZATION_FAILED}
     * — a silent denial of service that looks like an ACL bug.
     *
     * <p>The check resolves the effective principal builder for each tenant-bound
     * listener the same way the broker would at runtime: a per-listener override
     * (raw or pre-parsed) wins over the broker-wide value. The effective class
     * must be {@link TenantPrincipalBuilder} or a subclass. Any mismatch is
     * fatal — surfaces at broker startup rather than silently breaking every
     * tenant client.
     *
     * @param brokerProps         the broker's raw originals map (config keys as on disk)
     * @param defaultBuilderClass the broker-wide principal.builder.class (may be null)
     * @throws ConfigException with every offending listener listed
     */
    public static void validatePrincipalBuilderBindings(Map<String, ?> brokerProps,
                                                       Class<?> defaultBuilderClass) {
        List<String> tenantListeners = listenersWithTenantBinding(brokerProps);
        if (tenantListeners.isEmpty()) {
            return;
        }
        List<String> offenders = new ArrayList<>();
        for (String listener : tenantListeners) {
            String diagnosis = diagnoseBuilderBinding(brokerProps, listener, defaultBuilderClass);
            if (diagnosis != null) {
                offenders.add(diagnosis);
            }
        }
        if (offenders.isEmpty()) {
            return;
        }
        String key = LISTENER_PREFIX + "<listener>." + PRINCIPAL_BUILDER_CLASS_KEY;
        throw new ConfigException(key, "", "Listener(s) " + String.join(", ", offenders)
            + " have " + TENANT_ID_KEY + " configured but do not use "
            + TenantPrincipalBuilder.class.getName() + " as principal.builder.class. "
            + "A tenant-bound listener must mint __tenant_<id>.<user> principals; "
            + "otherwise the broker silently refuses every request on that listener.");
    }

    /**
     * Returns {@code null} when the listener's effective principal builder is
     * {@link TenantPrincipalBuilder} (or a subclass), or a short diagnostic
     * string naming the offence otherwise. Split out of
     * {@link #validatePrincipalBuilderBindings} to keep that method below the
     * checkstyle NPath threshold.
     */
    private static String diagnoseBuilderBinding(Map<String, ?> brokerProps,
                                                 String listener,
                                                 Class<?> defaultBuilderClass) {
        String listenerKey = LISTENER_PREFIX + listener.toLowerCase(Locale.ROOT)
            + "." + PRINCIPAL_BUILDER_CLASS_KEY;
        Object listenerOverride = brokerProps.get(listenerKey);
        BuilderRef ref = resolveBuilderRef(listenerOverride, defaultBuilderClass);
        if (ref.name == null) {
            return listener + " (no principal.builder.class configured)";
        }
        Class<?> resolved = ref.clazz;
        if (resolved == null) {
            try {
                resolved = Class.forName(ref.name, false,
                    Thread.currentThread().getContextClassLoader());
            } catch (ClassNotFoundException ex) {
                return listener + " (uses " + ref.name + ", class not found)";
            }
        }
        if (!TenantPrincipalBuilder.class.isAssignableFrom(resolved)) {
            return listener + " (uses " + ref.name + ")";
        }
        return null;
    }

    private static BuilderRef resolveBuilderRef(Object listenerOverride, Class<?> defaultBuilderClass) {
        if (listenerOverride instanceof Class<?>) {
            Class<?> c = (Class<?>) listenerOverride;
            return new BuilderRef(c, c.getName());
        }
        if (listenerOverride != null) {
            String trimmed = listenerOverride.toString().trim();
            if (!trimmed.isEmpty()) {
                return new BuilderRef(null, trimmed);
            }
        }
        if (defaultBuilderClass != null) {
            return new BuilderRef(defaultBuilderClass, defaultBuilderClass.getName());
        }
        return new BuilderRef(null, null);
    }

    private static final class BuilderRef {
        final Class<?> clazz;
        final String name;
        BuilderRef(Class<?> clazz, String name) {
            this.clazz = clazz;
            this.name = name;
        }
    }
}
