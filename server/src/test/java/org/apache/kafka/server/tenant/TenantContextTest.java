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
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TenantContextTest {

    private static final KafkaPrincipal ACME = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice");
    private static final KafkaPrincipal PLAIN = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice");
    private static final KafkaPrincipal SUPER = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin");

    @Test
    void effectiveTenantIsPrincipalTenantWhenListenerOpen() {
        TenantContext ctx = TenantContext.of(ACME, Optional.empty());
        assertEquals(Optional.of("acme"), ctx.effectiveTenant());
    }

    @Test
    void effectiveTenantIsListenerTenantWhenPrincipalIsPlain() {
        // The listener owns the binding when the principal has no tenant prefix;
        // this is what the privileged-on-tenant guard inspects.
        TenantContext ctx = TenantContext.of(PLAIN, Optional.of("acme"));
        assertEquals(Optional.of("acme"), ctx.effectiveTenant());
    }

    @Test
    void effectiveTenantIsEmptyWhenNeitherSet() {
        TenantContext ctx = TenantContext.of(SUPER, Optional.empty());
        assertEquals(Optional.empty(), ctx.effectiveTenant());
    }

    @Test
    void isPrivilegedCallerOnTenantListenerTrapDetected() {
        // PROMPT.md: a privileged caller on a tenant-bound listener without a
        // __tenant_ principal silently pollutes the tenant namespace. This guard
        // detects that exact trap.
        TenantContext ctx = TenantContext.of(SUPER, Optional.of("acme"));
        assertTrue(ctx.isPrivilegedOnTenantListener());
    }

    @Test
    void isPrivilegedCallerOnTenantListenerFalseWhenPrincipalCarriesTenant() {
        TenantContext ctx = TenantContext.of(ACME, Optional.of("acme"));
        assertFalse(ctx.isPrivilegedOnTenantListener());
    }

    @Test
    void isPrivilegedCallerOnTenantListenerFalseOnOpenListener() {
        TenantContext ctx = TenantContext.of(SUPER, Optional.empty());
        assertFalse(ctx.isPrivilegedOnTenantListener());
    }

    @Test
    void principalListenerMismatchIsTreatedAsPrivileged() {
        // A principal claiming "beta" arrives on the "acme" listener: refuse to
        // rewrite into beta's namespace. The listener wins, but the mismatch is
        // surfaced so the handler can reject rather than guess.
        TenantContext ctx = TenantContext.of(
            new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_beta.alice"),
            Optional.of("acme"));
        assertTrue(ctx.hasPrincipalListenerMismatch());
    }

    @Test
    void rewriteInDelegatesToNamespace() {
        TenantContext ctx = TenantContext.of(ACME, Optional.empty());
        assertEquals("acme.orders", ctx.toPhysical("orders"));
    }

    @Test
    void rewriteInPassesInternalTopicThrough() {
        TenantContext ctx = TenantContext.of(ACME, Optional.empty());
        assertEquals("__consumer_offsets", ctx.toPhysical("__consumer_offsets"));
    }

    @Test
    void rewriteInIsIdentityForNonTenantContext() {
        TenantContext ctx = TenantContext.of(SUPER, Optional.empty());
        assertEquals("orders", ctx.toPhysical("orders"));
    }

    @Test
    void rewriteOutDelegatesToNamespace() {
        TenantContext ctx = TenantContext.of(ACME, Optional.empty());
        assertEquals("orders", ctx.toLogical("acme.orders"));
    }

    @Test
    void rewriteOutIsIdentityForNonTenantContext() {
        TenantContext ctx = TenantContext.of(SUPER, Optional.empty());
        assertEquals("acme.orders", ctx.toLogical("acme.orders"));
    }
}
