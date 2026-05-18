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
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TenantConfigTest {

    @Test
    void extractsTenantIdFromListenerPrefixedKey() {
        Map<String, Object> props = new HashMap<>();
        props.put("listener.name.tenant_acme.tenant.id", "acme");

        TenantConfig cfg = TenantConfig.from(props);

        assertEquals(Optional.of("acme"),
            cfg.boundTenantFor(new ListenerName("TENANT_ACME")));
        assertTrue(cfg.isTenantBoundListener(new ListenerName("TENANT_ACME")));
    }

    @Test
    void listenerNameLookupIsCaseInsensitive() {
        // Kafka listener names are case-insensitive in config — `listener.name.X`
        // and ListenerName("X") must match regardless of casing.
        Map<String, Object> props = new HashMap<>();
        props.put("listener.name.tenant_acme.tenant.id", "acme");

        TenantConfig cfg = TenantConfig.from(props);

        assertEquals(Optional.of("acme"),
            cfg.boundTenantFor(new ListenerName("tenant_acme")));
        assertEquals(Optional.of("acme"),
            cfg.boundTenantFor(new ListenerName("Tenant_Acme")));
    }

    @Test
    void unboundListenerReturnsEmpty() {
        TenantConfig cfg = TenantConfig.from(new HashMap<>());

        assertEquals(Optional.empty(),
            cfg.boundTenantFor(new ListenerName("PLAINTEXT")));
        assertFalse(cfg.isTenantBoundListener(new ListenerName("PLAINTEXT")));
    }

    @Test
    void multipleListenersSupported() {
        Map<String, Object> props = new HashMap<>();
        props.put("listener.name.tenant_acme.tenant.id", "acme");
        props.put("listener.name.tenant_beta.tenant.id", "beta");
        props.put("listener.name.internal.foo.bar", "ignored");

        TenantConfig cfg = TenantConfig.from(props);

        assertEquals(Optional.of("acme"), cfg.boundTenantFor(new ListenerName("TENANT_ACME")));
        assertEquals(Optional.of("beta"), cfg.boundTenantFor(new ListenerName("TENANT_BETA")));
        assertEquals(Optional.empty(), cfg.boundTenantFor(new ListenerName("INTERNAL")));
    }

    @Test
    void rejectsInvalidTenantIdAtParseTime() {
        // Validation happens at config load so a misconfiguration surfaces at
        // broker startup, not silently at handshake.
        Map<String, Object> props = new HashMap<>();
        props.put("listener.name.tenant_bad.tenant.id", "a.b");

        assertThrows(ConfigException.class, () -> TenantConfig.from(props));
    }

    @Test
    void ignoresEmptyTenantIdValue() {
        // An empty value is treated as "no binding" — operators can comment-out
        // a listener mapping without removing the key.
        Map<String, Object> props = new HashMap<>();
        props.put("listener.name.tenant_acme.tenant.id", "");

        TenantConfig cfg = TenantConfig.from(props);

        assertFalse(cfg.isTenantBoundListener(new ListenerName("TENANT_ACME")));
    }

    @Test
    void emptyReturnsAlwaysUnbound() {
        TenantConfig cfg = TenantConfig.empty();

        assertFalse(cfg.isTenantBoundListener(new ListenerName("X")));
        assertEquals(Optional.empty(), cfg.boundTenantFor(new ListenerName("X")));
    }

    @Test
    void allTenantsReturnsEveryConfiguredId() {
        // The cluster-wide-listener outside-in pollution guard iterates this
        // set to decide whether a topic name starts with a tenant prefix.
        Map<String, Object> props = new HashMap<>();
        props.put("listener.name.tenant_acme.tenant.id", "acme");
        props.put("listener.name.tenant_beta.tenant.id", "beta");

        TenantConfig cfg = TenantConfig.from(props);

        assertEquals(Set.of("acme", "beta"), cfg.allTenants());
    }

    @Test
    void allTenantsReturnsEmptyWhenUnconfigured() {
        assertTrue(TenantConfig.empty().allTenants().isEmpty());
    }

    @Test
    void validatePrincipalBuilderBindingsPassesWhenListenerHasTenantBuilder() {
        Map<String, Object> props = new HashMap<>();
        props.put("listener.name.tenant_acme.tenant.id", "acme");
        props.put("listener.name.tenant_acme.principal.builder.class",
            TenantPrincipalBuilder.class.getName());

        TenantConfig.validatePrincipalBuilderBindings(props, /*default*/ null);
    }

    @Test
    void validatePrincipalBuilderBindingsAcceptsBrokerWideDefault() {
        // Operator sets principal.builder.class once at the broker level; the
        // tenant listener inherits it without a listener-prefixed override.
        Map<String, Object> props = new HashMap<>();
        props.put("listener.name.tenant_acme.tenant.id", "acme");

        TenantConfig.validatePrincipalBuilderBindings(props, TenantPrincipalBuilder.class);
    }

    @Test
    void validatePrincipalBuilderBindingsAcceptsClassObjectOverride() {
        // KafkaConfig stores the listener-prefixed override as a Class object
        // (post-ConfigDef parsing) on some code paths. The check must accept
        // both raw String values and resolved Class<?> values.
        Map<String, Object> props = new HashMap<>();
        props.put("listener.name.tenant_acme.tenant.id", "acme");
        props.put("listener.name.tenant_acme.principal.builder.class",
            TenantPrincipalBuilder.class);

        TenantConfig.validatePrincipalBuilderBindings(props, null);
    }

    @Test
    void validatePrincipalBuilderBindingsRejectsDefaultBuilder() {
        // The trap Codex round-3 flagged: a tenant-bound listener inherits the
        // stock DefaultKafkaPrincipalBuilder, SASL produces a plain principal,
        // KafkaApis refuses every request as `isPrivilegedOnTenantListener`.
        Map<String, Object> props = new HashMap<>();
        props.put("listener.name.tenant_acme.tenant.id", "acme");
        props.put("listener.name.tenant_acme.principal.builder.class",
            "org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder");

        ConfigException ex = assertThrows(ConfigException.class,
            () -> TenantConfig.validatePrincipalBuilderBindings(props, null));
        assertTrue(ex.getMessage().contains("tenant_acme"),
            "error should name the offending listener; was: " + ex.getMessage());
        assertTrue(ex.getMessage().contains(TenantPrincipalBuilder.class.getName()),
            "error should name the required builder; was: " + ex.getMessage());
    }

    @Test
    void validatePrincipalBuilderBindingsRejectsBrokerWideNonTenantDefault() {
        // No per-listener override; broker-wide default is the stock builder.
        // The listener is tenant-bound, so inheritance produces a silent DoS.
        Map<String, Object> props = new HashMap<>();
        props.put("listener.name.tenant_acme.tenant.id", "acme");

        // Pass a non-tenant class as the broker-wide default.
        ConfigException ex = assertThrows(ConfigException.class,
            () -> TenantConfig.validatePrincipalBuilderBindings(props, String.class));
        assertTrue(ex.getMessage().contains("tenant_acme"),
            "error should name the offending listener; was: " + ex.getMessage());
    }

    @Test
    void validatePrincipalBuilderBindingsAggregatesAllOffenders() {
        // Operator misconfigures two listeners at once; we report both so they
        // can be fixed in a single deploy.
        Map<String, Object> props = new HashMap<>();
        props.put("listener.name.tenant_acme.tenant.id", "acme");
        props.put("listener.name.tenant_acme.principal.builder.class",
            "org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder");
        props.put("listener.name.tenant_beta.tenant.id", "beta");
        props.put("listener.name.tenant_beta.principal.builder.class",
            "org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder");

        ConfigException ex = assertThrows(ConfigException.class,
            () -> TenantConfig.validatePrincipalBuilderBindings(props, null));
        assertTrue(ex.getMessage().contains("tenant_acme"),
            "error should name both offenders; was: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("tenant_beta"),
            "error should name both offenders; was: " + ex.getMessage());
    }

    @Test
    void validatePrincipalBuilderBindingsNoOpWhenNoTenantListeners() {
        // Untouched cluster — no tenant.id keys anywhere — must not refuse to
        // start regardless of what principal.builder.class is set to.
        Map<String, Object> props = new HashMap<>();
        TenantConfig.validatePrincipalBuilderBindings(props, String.class);
    }

    @Test
    void validatePrincipalBuilderBindingsFailsWhenNoBuilderConfigured() {
        Map<String, Object> props = new HashMap<>();
        props.put("listener.name.tenant_acme.tenant.id", "acme");

        // Neither listener-prefixed nor broker-wide default.
        ConfigException ex = assertThrows(ConfigException.class,
            () -> TenantConfig.validatePrincipalBuilderBindings(props, null));
        assertTrue(ex.getMessage().contains("tenant_acme"),
            "error should name the offending listener; was: " + ex.getMessage());
    }

    @Test
    void validateSuperUsersPassesWhenUnset() {
        Map<String, Object> props = new HashMap<>();
        TenantConfig.validateSuperUsersAreNotTenantPrefixed(props);
    }

    @Test
    void validateSuperUsersPassesForOperatorPrincipals() {
        // The legitimate shape: operator principals only, no tenant prefix.
        Map<String, Object> props = new HashMap<>();
        props.put("super.users", "User:admin;User:alice;User:bob");
        TenantConfig.validateSuperUsersAreNotTenantPrefixed(props);
    }

    @Test
    void validateSuperUsersRejectsTenantPrefixedPrincipal() {
        // The trap: super.users grants cluster-wide ACL bypass to anything it
        // contains. A tenant-stamped principal in this list would silently
        // bypass tenant isolation — the handler-level guard does not save you
        // because the authorizer never asks.
        Map<String, Object> props = new HashMap<>();
        props.put("super.users", "User:admin;User:__tenant_acme.alice");

        ConfigException ex = assertThrows(ConfigException.class,
            () -> TenantConfig.validateSuperUsersAreNotTenantPrefixed(props));
        assertTrue(ex.getMessage().contains("__tenant_acme.alice"),
            "error should quote the offending entry; was: " + ex.getMessage());
        assertTrue(ex.getMessage().contains(TenantNamespace.PRINCIPAL_PREFIX),
            "error should explain the reserved prefix; was: " + ex.getMessage());
    }

    @Test
    void validateSuperUsersAggregatesMultipleOffenders() {
        // Two tenant-prefixed entries: report both so they can be fixed in a
        // single deploy. Order of offenders is not asserted (Set semantics) —
        // only that each one appears in the message.
        Map<String, Object> props = new HashMap<>();
        props.put("super.users",
            "User:__tenant_acme.alice;User:admin;User:__tenant_beta.bob");

        ConfigException ex = assertThrows(ConfigException.class,
            () -> TenantConfig.validateSuperUsersAreNotTenantPrefixed(props));
        assertTrue(ex.getMessage().contains("__tenant_acme.alice"),
            "error should name first offender; was: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("__tenant_beta.bob"),
            "error should name second offender; was: " + ex.getMessage());
    }

    @Test
    void validateSuperUsersIgnoresEmptyAndWhitespaceEntries() {
        // The split on ';' produces empty trailing entries when the value ends
        // with ';' or contains '  '; treat them as no-op rather than refusing.
        Map<String, Object> props = new HashMap<>();
        props.put("super.users", "User:admin;  ;User:bob;");
        TenantConfig.validateSuperUsersAreNotTenantPrefixed(props);
    }

    @Test
    void validateSuperUsersHandlesMalformedEntryWithoutColon() {
        // A malformed entry (no `:`) is rejected by the authorizer itself; this
        // check only flags the tenant-prefix concern. A bare `__tenant_acme.alice`
        // (without `User:`) STILL has the reserved prefix at position 0, so we
        // must catch it. Conversely, an unrelated malformed entry like `admin`
        // is left alone.
        Map<String, Object> okProps = new HashMap<>();
        okProps.put("super.users", "admin");
        TenantConfig.validateSuperUsersAreNotTenantPrefixed(okProps);

        Map<String, Object> badProps = new HashMap<>();
        badProps.put("super.users", "__tenant_acme.alice");
        ConfigException ex = assertThrows(ConfigException.class,
            () -> TenantConfig.validateSuperUsersAreNotTenantPrefixed(badProps));
        assertTrue(ex.getMessage().contains("__tenant_acme.alice"),
            "error should quote the offender even when the User: prefix is missing");
    }
}
