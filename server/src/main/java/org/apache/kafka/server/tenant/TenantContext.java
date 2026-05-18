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

import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.Objects;
import java.util.Optional;

/**
 * Immutable view of the tenant identity attached to a single request: who the
 * caller is, and which tenant the listener is bound to (if any).
 *
 * <p>The effective tenant is the principal's tenant when present; otherwise the
 * listener's binding (which lets us still detect "no principal tenant on a
 * tenant-bound listener" — see {@link #isPrivilegedOnTenantListener}).
 *
 * <p>The handler reads this once at request entry, rewrites topic names on the
 * way in via {@link #toPhysical(String)}, and rewrites them back on the way out
 * via {@link #toLogical(String)} — including in error responses.
 */
public final class TenantContext {

    private static final TenantContext EMPTY = new TenantContext(null, null);

    private final String principalTenantId;  // nullable
    private final String listenerTenantId;   // nullable

    private TenantContext(String principalTenantId, String listenerTenantId) {
        this.principalTenantId = principalTenantId;
        this.listenerTenantId = listenerTenantId;
    }

    public static TenantContext none() {
        return EMPTY;
    }

    public static TenantContext of(KafkaPrincipal principal, Optional<String> listenerTenantId) {
        Objects.requireNonNull(principal, "principal");
        Objects.requireNonNull(listenerTenantId, "listenerTenantId");
        String principalTenant = TenantNamespace.parseTenantId(principal).orElse(null);
        String listenerTenant = listenerTenantId.orElse(null);
        if (principalTenant == null && listenerTenant == null) {
            return EMPTY;
        }
        return new TenantContext(principalTenant, listenerTenant);
    }

    public Optional<String> effectiveTenant() {
        if (principalTenantId != null) {
            return Optional.of(principalTenantId);
        }
        return Optional.ofNullable(listenerTenantId);
    }

    public boolean isTenantBoundListener() {
        return listenerTenantId != null;
    }

    /**
     * The trap PROMPT.md warns about: the listener is tenant-bound but the
     * caller's principal carries no tenant identity. Without a guard the broker
     * would silently rewrite the call into the tenant's namespace using only
     * the listener's binding — letting a super-user pollute a tenant.
     */
    public boolean isPrivilegedOnTenantListener() {
        return listenerTenantId != null && principalTenantId == null;
    }

    public boolean hasPrincipalListenerMismatch() {
        return listenerTenantId != null
            && principalTenantId != null
            && !listenerTenantId.equals(principalTenantId);
    }

    public String toPhysical(String logicalTopic) {
        return effectiveTenant()
            .map(t -> TenantNamespace.toPhysical(t, logicalTopic))
            .orElse(logicalTopic);
    }

    public String toLogical(String physicalTopic) {
        return effectiveTenant()
            .map(t -> TenantNamespace.toLogical(t, physicalTopic))
            .orElse(physicalTopic);
    }

    public boolean belongsToTenant(String physicalTopic) {
        return effectiveTenant()
            .map(t -> TenantNamespace.belongsTo(t, physicalTopic))
            .orElse(false);
    }
}
