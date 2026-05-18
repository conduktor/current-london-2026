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
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.PlaintextAuthenticationContext;
import org.apache.kafka.common.security.auth.SaslAuthenticationContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.security.sasl.SaslServer;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TenantPrincipalBuilderTest {

    private static final String LISTENER_NAME = "TENANT_ACME";

    private static SaslAuthenticationContext saslContext(String authId) throws UnknownHostException {
        SaslServer saslServer = Mockito.mock(SaslServer.class);
        Mockito.when(saslServer.getMechanismName()).thenReturn("PLAIN");
        Mockito.when(saslServer.getAuthorizationID()).thenReturn(authId);
        return new SaslAuthenticationContext(
            saslServer,
            SecurityProtocol.SASL_PLAINTEXT,
            InetAddress.getByName("127.0.0.1"),
            LISTENER_NAME);
    }

    @Test
    void buildsTenantPrefixedPrincipalFromSaslOnBoundListener() throws Exception {
        TenantPrincipalBuilder builder = new TenantPrincipalBuilder();
        builder.configure(configMap("acme"));

        KafkaPrincipal p = builder.build(saslContext("alice"));

        assertEquals(KafkaPrincipal.USER_TYPE, p.getPrincipalType());
        assertEquals("__tenant_acme.alice", p.getName());
    }

    @Test
    void buildsUnprefixedPrincipalWhenListenerHasNoTenantId() throws Exception {
        TenantPrincipalBuilder builder = new TenantPrincipalBuilder();
        // No tenant.id configured → listener is open, behave like default.
        builder.configure(new HashMap<>());

        KafkaPrincipal p = builder.build(saslContext("admin"));

        assertEquals("admin", p.getName());
    }

    @Test
    void rejectsConfigWithInvalidTenantId() {
        TenantPrincipalBuilder builder = new TenantPrincipalBuilder();
        // Dots in tenant id would make logical/physical translation ambiguous.
        assertThrows(ConfigException.class, () -> builder.configure(configMap("a.b")));
    }

    @Test
    void rejectsConfigWithReservedPrefixTenantId() {
        TenantPrincipalBuilder builder = new TenantPrincipalBuilder();
        assertThrows(ConfigException.class, () -> builder.configure(configMap("__foo")));
    }

    @Test
    void plaintextContextProducesAnonymousPrincipalUnchanged() throws Exception {
        // PLAINTEXT yields ANONYMOUS; we don't wrap ANONYMOUS in a tenant prefix
        // — that would let unauthenticated connections claim tenant identity.
        TenantPrincipalBuilder builder = new TenantPrincipalBuilder();
        builder.configure(configMap("acme"));

        KafkaPrincipal p = builder.build(new PlaintextAuthenticationContext(
            InetAddress.getByName("127.0.0.1"), LISTENER_NAME));

        assertEquals(KafkaPrincipal.ANONYMOUS, p);
    }

    @Test
    void serializeRoundtripsTenantPrefixedName() throws Exception {
        // PRINCIPAL forwarding (controller envelope) requires serde to preserve
        // the tenant prefix.
        TenantPrincipalBuilder builder = new TenantPrincipalBuilder();
        builder.configure(configMap("acme"));

        KafkaPrincipal original = builder.build(saslContext("alice"));
        byte[] bytes = builder.serialize(original);
        KafkaPrincipal restored = builder.deserialize(bytes);

        assertEquals(original, restored);
        assertEquals("__tenant_acme.alice", restored.getName());
    }

    private static Map<String, Object> configMap(String tenantId) {
        Map<String, Object> m = new HashMap<>();
        m.put(TenantPrincipalBuilder.TENANT_ID_CONFIG, tenantId);
        return m;
    }
}
