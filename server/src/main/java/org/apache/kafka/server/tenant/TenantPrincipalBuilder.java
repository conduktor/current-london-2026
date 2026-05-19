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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * A {@link KafkaPrincipalBuilder} that wraps {@link DefaultKafkaPrincipalBuilder}
 * and stamps the resulting principal with a tenant prefix when the listener it
 * arrived on is bound to a tenant id.
 *
 * <p>Configured via per-listener overrides at the broker level:
 * <pre>
 *   listener.name.tenant_acme.principal.builder.class = \
 *       org.apache.kafka.server.tenant.TenantPrincipalBuilder
 *   listener.name.tenant_acme.tenant.id = acme
 * </pre>
 *
 * <p>The resolution is intentionally done from the broker-wide originals — not
 * from the per-listener stripped configs Kafka hands to {@link #configure}.
 * Kafka strips a listener prefix only when the stripped key is a known
 * {@code ConfigDef} entry, and {@code tenant.id} is not one. Without this
 * indirection a listener-prefixed {@code tenant.id} would arrive at the
 * builder under its full key, the lookup of {@code tenant.id} would miss, and
 * principals on that listener would silently stay unwrapped — making every
 * tenant request look "privileged-on-tenant-listener" and get refused.
 *
 * <p>The build path uses {@link AuthenticationContext#listenerName()} to pick
 * the right binding for the connection at hand. This is also why a single
 * builder instance correctly serves multiple tenant listeners.
 *
 * <p>{@link KafkaPrincipal#ANONYMOUS} is never wrapped; an unauthenticated
 * connection must not be able to claim a tenant identity.
 */
public class TenantPrincipalBuilder
        implements KafkaPrincipalBuilder, KafkaPrincipalSerde, Configurable {

    public static final String TENANT_ID_CONFIG = "tenant.id";
    public static final String LISTENER_PREFIX = "listener.name.";

    private final DefaultKafkaPrincipalBuilder delegate;
    private Map<String, String> tenantByListener;  // upper-cased listener name → tenant id

    public TenantPrincipalBuilder() {
        // KerberosShortNamer / SslPrincipalMapper are not propagated through
        // configure(Map); custom builders that need them must rebuild from
        // configs. v1 is SASL_PLAIN / SCRAM only and does not need either.
        //
        // SslPrincipalMapper.fromRules(null) resolves to the DEFAULT identity
        // mapping rule (see SslPrincipalMapper.DEFAULT_SSL_PRINCIPAL_MAPPING_RULES).
        // Passing a null mapper would NPE at DefaultKafkaPrincipalBuilder
        // .applySslPrincipalMapper as soon as a peer presents an X500Principal —
        // the standard mTLS case. Even though v1 does not advertise mTLS, this
        // guards against accidental SSL-listener wiring producing an NPE on
        // every handshake instead of an authentication failure with a usable
        // identity.
        this(new DefaultKafkaPrincipalBuilder(
            (KerberosShortNamer) null, SslPrincipalMapper.fromRules(null)));
    }

    // Visible for delegate injection from tests; not part of the public API.
    TenantPrincipalBuilder(DefaultKafkaPrincipalBuilder delegate) {
        this.delegate = delegate;
        this.tenantByListener = new HashMap<>();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Map<String, String> bindings = new HashMap<>();
        // 1. Broker-wide originals: listener.name.<lname>.tenant.id=<id>. This
        //    path is what production deployments hit — the listener prefix is
        //    preserved because tenant.id is not a defined ConfigDef key.
        for (Map.Entry<String, ?> e : configs.entrySet()) {
            String key = e.getKey();
            if (!key.startsWith(LISTENER_PREFIX)) {
                continue;
            }
            int suffixStart = key.indexOf('.', LISTENER_PREFIX.length());
            if (suffixStart < 0) {
                continue;
            }
            String suffix = key.substring(suffixStart + 1);
            if (!suffix.equals(TENANT_ID_CONFIG)) {
                continue;
            }
            String listener = key.substring(LISTENER_PREFIX.length(), suffixStart);
            String tenantId = String.valueOf(e.getValue()).trim();
            if (tenantId.isEmpty()) {
                continue;
            }
            validateOrThrow(key, e.getValue(), tenantId);
            bindings.put(listener.toUpperCase(Locale.ROOT), tenantId);
        }
        // 2. Unprefixed tenant.id: legacy / test path. The empty listener key
        //    acts as a wildcard so a builder instantiated directly (no broker
        //    config plumbing) still resolves a tenant for every connection.
        Object raw = configs.get(TENANT_ID_CONFIG);
        if (raw != null) {
            String tenantId = raw.toString().trim();
            if (!tenantId.isEmpty()) {
                validateOrThrow(TENANT_ID_CONFIG, raw, tenantId);
                bindings.put("", tenantId);
            }
        }
        this.tenantByListener = bindings;
    }

    private static void validateOrThrow(String configKey, Object raw, String tenantId) {
        try {
            TenantNamespace.validateTenantId(tenantId);
        } catch (IllegalArgumentException e) {
            throw new ConfigException(configKey, raw, e.getMessage());
        }
    }

    @Override
    public KafkaPrincipal build(AuthenticationContext context) {
        KafkaPrincipal base = delegate.build(context);
        if (KafkaPrincipal.ANONYMOUS.equals(base)) {
            // Refuse to attach tenant identity to an unauthenticated connection.
            return base;
        }
        String tenantId = tenantFor(context);
        if (base.getName().startsWith(TenantNamespace.PRINCIPAL_PREFIX)) {
            // The base principal already carries the reserved tenant prefix.
            // Three ways this happens:
            //   1. Delegation-token replay: a token issued under tenant A is
            //      presented on tenant B's listener. SCRAM/tokenauth surfaces
            //      the token owner name as authorizationID, so base name is
            //      "__tenant_A.<user>". Stamping again would yield
            //      "__tenant_B.__tenant_A.<user>" which TenantNamespace splits
            //      at the first dot and resolves to tenant B — silently
            //      crossing the isolation boundary.
            //   2. SCRAM username spoof: an operator-created SCRAM credential
            //      named "__tenant_..." used as authorizationID on any listener.
            //   3. A test or admin-tool oversight passing a pre-stamped name.
            // In all three cases the safe answer is ANONYMOUS unless the base
            // principal's tenant matches this listener exactly (same-tenant
            // re-auth, preserved unchanged so the existing connection identity
            // is honoured).
            Optional<String> baseTenant = TenantNamespace.parseTenantId(base);
            if (baseTenant.isEmpty()) {
                return KafkaPrincipal.ANONYMOUS;
            }
            if (tenantId != null && tenantId.equals(baseTenant.get())) {
                return base;
            }
            return KafkaPrincipal.ANONYMOUS;
        }
        if (tenantId == null) {
            return base;
        }
        return new KafkaPrincipal(
            base.getPrincipalType(),
            TenantNamespace.encodePrincipalName(tenantId, base.getName()),
            base.tokenAuthenticated());
    }

    private String tenantFor(AuthenticationContext context) {
        if (tenantByListener.isEmpty()) {
            return null;
        }
        String listener = context.listenerName();
        if (listener != null) {
            String bound = tenantByListener.get(listener.toUpperCase(Locale.ROOT));
            if (bound != null) {
                return bound;
            }
        }
        // Wildcard / unprefixed binding (test path).
        return tenantByListener.get("");
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
