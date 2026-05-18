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
}
