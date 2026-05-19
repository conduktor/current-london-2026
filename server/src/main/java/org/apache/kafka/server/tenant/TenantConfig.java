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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

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
    // Mirrors {@code StandardAuthorizer.SUPER_USERS_CONFIG}; duplicated as a
    // string so this module does not depend on {@code metadata}.
    static final String SUPER_USERS_KEY = "super.users";

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

    /**
     * Refuse to start a broker whose {@code super.users} list contains an entry
     * whose principal name carries the reserved {@link TenantNamespace#PRINCIPAL_PREFIX}.
     * A super-user is exempt from every ACL check ({@code StandardAuthorizerData}
     * matches on the full principal string), so a misconfigured
     * {@code super.users=User:__tenant_acme.alice} would silently elevate a
     * tenant principal to cluster-wide bypass — defeating both tenant isolation
     * and the privileged-on-tenant-listener guard, which still treats the
     * principal as a normal tenant request at the handler level.
     *
     * <p>The {@code super.users} property is a {@code ;}-separated list of
     * {@code User:name} entries (Kafka convention). Any entry whose post-{@code :}
     * name starts with the tenant prefix is fatal, listed in the
     * {@link ConfigException}.
     *
     * @param brokerProps the broker's raw originals map
     * @throws ConfigException listing every offending entry
     */
    public static void validateSuperUsersAreNotTenantPrefixed(Map<String, ?> brokerProps) {
        Object raw = brokerProps.get(SUPER_USERS_KEY);
        if (raw == null) {
            return;
        }
        String value = raw.toString();
        if (value.isEmpty()) {
            return;
        }
        List<String> offenders = new ArrayList<>();
        for (String entry : value.split(";")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int colon = trimmed.indexOf(':');
            // A super.users entry without `:` is malformed and the authorizer
            // itself will reject it. We only flag the tenant-prefix case so the
            // error message is precise — leave other validation to the auth path.
            String name = colon < 0 ? trimmed : trimmed.substring(colon + 1);
            if (name.startsWith(TenantNamespace.PRINCIPAL_PREFIX)) {
                offenders.add(trimmed);
            }
        }
        if (offenders.isEmpty()) {
            return;
        }
        throw new ConfigException(SUPER_USERS_KEY, value,
            "super.users entries " + String.join(", ", offenders)
                + " use the reserved tenant prefix '" + TenantNamespace.PRINCIPAL_PREFIX
                + "'. A super-user bypasses every ACL check, so granting it to a "
                + "tenant principal defeats tenant isolation. Configure super.users "
                + "with operator principals only; never with tenant principals.");
    }

    /**
     * Refuse any AlterConfigs that would set, change, or delete a
     * {@code tenant.id} key (either bare or {@code listener.name.<lname>.tenant.id})
     * in the broker config metadata. The tenant-to-listener binding is
     * established once at broker startup from {@code server.properties} and is
     * the load-bearing fact for the entire isolation model: principal builder
     * stamping, the privileged-on-tenant-listener refusal, topic-prefix
     * rewriting, ACL prefix scoping. Persisting a different value to the
     * cluster metadata log silently re-routes the listener on the next broker
     * restart — every existing tenant resource is now reachable from a
     * different tenant id, and the operator has no audit trail beyond the
     * AlterConfigs record itself.
     *
     * <p>The keys are not part of {@link KafkaConfig#configNames}, so
     * {@code DynamicConfig.Broker.validate} (which allows unknown listener-
     * prefixed properties via {@code customPropsAllowed=true}) does not catch
     * them. The controller validator is the only choke point that sees both
     * old and new state and can recognise a change.
     *
     * <p>Comparison is by string value across the union of keys in both maps,
     * so the check rejects ADD ({@code newConfigs} has it, {@code oldConfigs}
     * does not), MODIFY (both have it with different values), and DELETE
     * ({@code oldConfigs} has it, {@code newConfigs} does not). The typical
     * case — neither map carrying the key because the binding lives only in
     * broker-static config — is a no-op.
     *
     * @param newConfigs the post-merge effective config state proposed by the
     *                   AlterConfigs request (must not be {@code null}, may be empty)
     * @param oldConfigs the pre-existing metadata config state (must not be
     *                   {@code null}, may be empty)
     * @throws ConfigException listing every tenant-id key whose value would change
     */
    public static void validateTenantIdNotInAlterConfig(Map<String, ?> newConfigs,
                                                       Map<String, ?> oldConfigs) {
        Map<String, ?> safeNew = newConfigs == null ? Map.of() : newConfigs;
        Map<String, ?> safeOld = oldConfigs == null ? Map.of() : oldConfigs;
        Set<String> allKeys = new HashSet<>(safeNew.keySet());
        allKeys.addAll(safeOld.keySet());
        Set<String> offenders = new TreeSet<>();
        for (String key : allKeys) {
            if (!isTenantIdKey(key)) {
                continue;
            }
            String newVal = valueAsString(safeNew.get(key));
            String oldVal = valueAsString(safeOld.get(key));
            if (!Objects.equals(newVal, oldVal)) {
                offenders.add(key);
            }
        }
        if (offenders.isEmpty()) {
            return;
        }
        // Report the first offending key as the ConfigException's `name` to
        // surface in `kafka-configs.sh` output, but list all offenders in the
        // message so an operator fixes the request in one round-trip.
        String firstKey = offenders.iterator().next();
        Object firstValue = safeNew.get(firstKey);
        throw new ConfigException(firstKey, firstValue,
            "tenant.id binding(s) " + String.join(", ", offenders)
                + " cannot be set, modified, or deleted via AlterConfigs. "
                + "The tenant-to-listener binding is fixed at broker startup "
                + "(server.properties); changing it via the metadata log "
                + "would silently re-route the listener on the next broker "
                + "restart. Update server.properties and restart the broker "
                + "instead.");
    }

    private static boolean isTenantIdKey(String key) {
        if (key == null) {
            return false;
        }
        if (key.equals(TENANT_ID_KEY)) {
            return true;
        }
        if (!key.startsWith(LISTENER_PREFIX)) {
            return false;
        }
        int suffixStart = key.indexOf('.', LISTENER_PREFIX.length());
        if (suffixStart < 0) {
            return false;
        }
        return key.substring(suffixStart + 1).equals(TENANT_ID_KEY);
    }

    private static String valueAsString(Object v) {
        return v == null ? null : v.toString();
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
