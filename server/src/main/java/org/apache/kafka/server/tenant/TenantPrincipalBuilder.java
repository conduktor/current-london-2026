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

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
import org.apache.kafka.common.security.ssl.SslPrincipalMapper;

import java.util.Map;

/**
 * A {@link KafkaPrincipalBuilder} that wraps {@link DefaultKafkaPrincipalBuilder}
 * and stamps the resulting principal with a tenant prefix when the listener it
 * was loaded on has a configured tenant id.
 *
 * <p>Configured via per-listener overrides:
 * <pre>
 *   listener.name.tenant_acme.principal.builder.class = \
 *       org.apache.kafka.server.tenant.TenantPrincipalBuilder
 *   listener.name.tenant_acme.tenant.id = acme
 * </pre>
 *
 * <p>If {@value #TENANT_ID_CONFIG} is absent, the builder behaves as the
 * default — useful for inter-broker / cluster-wide listeners that should not be
 * forcibly bound to a tenant.
 *
 * <p>{@link KafkaPrincipal#ANONYMOUS} is never wrapped; an unauthenticated
 * connection must not be able to claim a tenant identity.
 */
public class TenantPrincipalBuilder
        implements KafkaPrincipalBuilder, KafkaPrincipalSerde, Configurable {

    public static final String TENANT_ID_CONFIG = "tenant.id";

    private final DefaultKafkaPrincipalBuilder delegate;
    private String tenantId;  // null → behave as default

    public TenantPrincipalBuilder() {
        // KerberosShortNamer / SslPrincipalMapper are not propagated through
        // configure(Map); custom builders that need them must rebuild from
        // configs. v1 is SASL_PLAIN / SCRAM only and does not need either.
        this(new DefaultKafkaPrincipalBuilder(
            (KerberosShortNamer) null, (SslPrincipalMapper) null));
    }

    // Visible for delegate injection from tests; not part of the public API.
    TenantPrincipalBuilder(DefaultKafkaPrincipalBuilder delegate) {
        this.delegate = delegate;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Object raw = configs.get(TENANT_ID_CONFIG);
        if (raw == null) {
            this.tenantId = null;
            return;
        }
        String id = raw.toString().trim();
        try {
            TenantNamespace.validateTenantId(id);
        } catch (IllegalArgumentException e) {
            throw new ConfigException(TENANT_ID_CONFIG, raw, e.getMessage());
        }
        this.tenantId = id;
    }

    @Override
    public KafkaPrincipal build(AuthenticationContext context) {
        KafkaPrincipal base = delegate.build(context);
        if (tenantId == null) {
            return base;
        }
        if (KafkaPrincipal.ANONYMOUS.equals(base)) {
            // Refuse to attach tenant identity to an unauthenticated connection.
            return base;
        }
        return new KafkaPrincipal(
            base.getPrincipalType(),
            TenantNamespace.encodePrincipalName(tenantId, base.getName()),
            base.tokenAuthenticated());
    }

    @Override
    public byte[] serialize(KafkaPrincipal principal) {
        return delegate.serialize(principal);
    }

    @Override
    public KafkaPrincipal deserialize(byte[] bytes) {
        return delegate.deserialize(bytes);
    }
}
