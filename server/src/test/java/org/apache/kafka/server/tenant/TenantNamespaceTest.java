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

import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TenantNamespaceTest {

    @Test
    void principalNameContainsReservedPrefixAndDotSeparator() {
        assertEquals("__tenant_acme.alice", TenantNamespace.encodePrincipalName("acme", "alice"));
    }

    @Test
    void principalNameAllowsDotsInUserName() {
        // User names may legitimately contain dots; only the *first* dot after the
        // reserved prefix separates tenant id from user.
        assertEquals("__tenant_acme.alice.user", TenantNamespace.encodePrincipalName("acme", "alice.user"));
    }

    @Test
    void parseTenantFromPrincipalReturnsTenantWhenPrefixed() {
        KafkaPrincipal p = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice");
        assertEquals(Optional.of("acme"), TenantNamespace.parseTenantId(p));
    }

    @Test
    void parseTenantFromPrincipalReturnsTenantWhenUserHasDots() {
        KafkaPrincipal p = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice.user.x");
        assertEquals(Optional.of("acme"), TenantNamespace.parseTenantId(p));
    }

    @Test
    void parseTenantFromPrincipalReturnsEmptyForUnprefixed() {
        KafkaPrincipal p = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice");
        assertEquals(Optional.empty(), TenantNamespace.parseTenantId(p));
    }

    @Test
    void parseTenantFromPrincipalReturnsEmptyWhenSeparatorMissing() {
        // No dot after the prefix → malformed, treat as no tenant.
        KafkaPrincipal p = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme");
        assertEquals(Optional.empty(), TenantNamespace.parseTenantId(p));
    }

    @Test
    void toPhysicalPrefixesUserTopics() {
        assertEquals("acme.orders", TenantNamespace.toPhysical("acme", "orders"));
    }

    @Test
    void toPhysicalIsIdentityForInternalTopics() {
        // __consumer_offsets and __transaction_state must pass through unchanged
        // because they key by tenant-scoped identifiers, not by visible topic name.
        assertEquals("__consumer_offsets", TenantNamespace.toPhysical("acme", "__consumer_offsets"));
        assertEquals("__transaction_state", TenantNamespace.toPhysical("acme", "__transaction_state"));
        assertEquals("__share_group_state", TenantNamespace.toPhysical("acme", "__share_group_state"));
    }

    @Test
    void toLogicalStripsTenantPrefix() {
        assertEquals("orders", TenantNamespace.toLogical("acme", "acme.orders"));
    }

    @Test
    void toLogicalPreservesDotsInLogicalName() {
        assertEquals("legacy.event", TenantNamespace.toLogical("acme", "acme.legacy.event"));
    }

    @Test
    void toLogicalIsIdentityForInternalTopics() {
        assertEquals("__consumer_offsets", TenantNamespace.toLogical("acme", "__consumer_offsets"));
        assertEquals("__transaction_state", TenantNamespace.toLogical("acme", "__transaction_state"));
    }

    @Test
    void toLogicalReturnsPhysicalUnchangedWhenPrefixDoesNotMatch() {
        // A tenant on its listener cannot see another tenant's topic — but if the
        // broker shows it for any reason (mis-routing, debugging), we surface the
        // physical name verbatim rather than fabricating a misleading logical one.
        assertEquals("beta.orders", TenantNamespace.toLogical("acme", "beta.orders"));
    }

    @Test
    void belongsToReturnsTrueOnlyForTopicsWithMatchingPrefix() {
        assertTrue(TenantNamespace.belongsTo("acme", "acme.orders"));
        assertFalse(TenantNamespace.belongsTo("acme", "beta.orders"));
        assertFalse(TenantNamespace.belongsTo("acme", "orders"));
        // Internal topics never belong to a tenant.
        assertFalse(TenantNamespace.belongsTo("acme", "__consumer_offsets"));
    }

    @Test
    void tenantIdMustNotContainDot() {
        // Dots are reserved as the prefix separator; allowing them in the id would
        // make logical/physical translation ambiguous.
        assertThrows(IllegalArgumentException.class, () -> TenantNamespace.validateTenantId("a.b"));
    }

    @Test
    void tenantIdMustNotBeEmpty() {
        assertThrows(IllegalArgumentException.class, () -> TenantNamespace.validateTenantId(""));
    }

    @Test
    void tenantIdMustNotStartWithReservedUnderscore() {
        // Avoid colliding with Kafka's __internal namespace.
        assertThrows(IllegalArgumentException.class, () -> TenantNamespace.validateTenantId("__foo"));
    }

    @Test
    void tenantIdMustNotContainInvalidTopicChars() {
        // Tenant id ends up as a topic-name prefix → must satisfy Topic.LEGAL_CHARS.
        assertThrows(IllegalArgumentException.class, () -> TenantNamespace.validateTenantId("acme/x"));
    }

    @Test
    void tenantIdAcceptsAlphanumericDashUnderscore() {
        TenantNamespace.validateTenantId("acme");
        TenantNamespace.validateTenantId("acme-1");
        TenantNamespace.validateTenantId("acme_1");
        TenantNamespace.validateTenantId("Acme1");
    }

    @Test
    void toPhysicalRefusesLogicalNameThatAlreadyHasTenantPrefix() {
        // A logical name starting with `<tenantId>.` is the round-trip of a
        // physical name. Rewriting it would silently produce `acme.acme.orders`.
        InvalidTopicException ex = assertThrows(InvalidTopicException.class,
            () -> TenantNamespace.toPhysical("acme", "acme.orders"));
        assertTrue(ex.getMessage().contains("acme.orders"));
        assertTrue(ex.getMessage().contains("reserved"));
    }

    @Test
    void isOverlongLogicalFormFlagsNamesThatBlowPastKafkaMaxLengthAfterPrefixing() {
        // Kafka caps topic names at 249 characters. Handlers must reject
        // logical names whose combined `<tenantId>.<name>` form would exceed
        // that bound BEFORE forwarding — otherwise the controller's rejection
        // would carry the physical form back in the error message.
        String tooLongLogical = "a".repeat(245); // "acme." + 245 = 250 → over the cap
        assertTrue(TenantNamespace.isOverlongLogicalForm("acme", tooLongLogical));
    }

    @Test
    void isOverlongLogicalFormBoundaryCaseAcceptsExactly249Chars() {
        // "acme." + 244-char logical = 249-char physical → exactly at the limit
        String boundaryLogical = "a".repeat(244);
        assertFalse(TenantNamespace.isOverlongLogicalForm("acme", boundaryLogical));
        assertEquals("acme." + boundaryLogical,
            TenantNamespace.toPhysical("acme", boundaryLogical));
    }

    @Test
    void isOverlongLogicalFormIgnoresInternalTopics() {
        // Internal topics aren't prefixed at all, so length validation does
        // not apply.
        assertFalse(TenantNamespace.isOverlongLogicalForm("acme", "__consumer_offsets"));
    }

    @Test
    void isOverlongLogicalFormIsFalseForNullName() {
        assertFalse(TenantNamespace.isOverlongLogicalForm("acme", null));
    }

    @Test
    void toPhysicalAllowsLogicalNameStartingWithUnrelatedTenantPrefix() {
        // "beta.orders" is a perfectly valid logical name for tenant acme — it
        // just contains a dot. The contract is per-tenant: acme's reserved
        // prefix is "acme.", not "beta.".
        assertEquals("acme.beta.orders", TenantNamespace.toPhysical("acme", "beta.orders"));
    }

    @Test
    void toPhysicalAcceptsLogicalNameEqualToTenantIdWithoutSeparator() {
        // "acme" (no trailing dot) is not the reserved form; rewriting to
        // "acme.acme" is the standard single prefix.
        assertEquals("acme.acme", TenantNamespace.toPhysical("acme", "acme"));
    }

    @Test
    void isReservedPhysicalFormFlagsTenantPrefixedNames() {
        assertTrue(TenantNamespace.isReservedPhysicalForm("acme", "acme.orders"));
        assertTrue(TenantNamespace.isReservedPhysicalForm("acme", "acme.legacy.event"));
        assertFalse(TenantNamespace.isReservedPhysicalForm("acme", "orders"));
        assertFalse(TenantNamespace.isReservedPhysicalForm("acme", "beta.orders"));
        assertFalse(TenantNamespace.isReservedPhysicalForm("acme", "acme"));
        // Internal topics are never tenant-namespaced.
        assertFalse(TenantNamespace.isReservedPhysicalForm("acme", "__consumer_offsets"));
    }

    @Test
    void isInternalTopicMirrorsKafkaInternals() {
        assertTrue(TenantNamespace.isInternalTopic("__consumer_offsets"));
        assertTrue(TenantNamespace.isInternalTopic("__transaction_state"));
        assertFalse(TenantNamespace.isInternalTopic("orders"));
        assertFalse(TenantNamespace.isInternalTopic("acme.orders"));
    }

    @Test
    void isInvalidLogicalFormFlagsEmptyDotAndDoubleDot() {
        // Kafka's Topic.validate refuses these literally. If we let them
        // through, the controller would reject the prefixed form ("acme.",
        // "acme..", "acme...") and the error message would carry the
        // physical name back to the tenant.
        assertTrue(TenantNamespace.isInvalidLogicalForm("acme", ""));
        assertTrue(TenantNamespace.isInvalidLogicalForm("acme", "."));
        assertTrue(TenantNamespace.isInvalidLogicalForm("acme", ".."));
    }

    @Test
    void isInvalidLogicalFormFlagsNamesWithIllegalChars() {
        // LEGAL_CHARS is `[a-zA-Z0-9._-]`. A slash would prefix to "acme.a/b"
        // which the controller refuses with the physical form in the message.
        assertTrue(TenantNamespace.isInvalidLogicalForm("acme", "a/b"));
        assertTrue(TenantNamespace.isInvalidLogicalForm("acme", "a:b"));
        assertTrue(TenantNamespace.isInvalidLogicalForm("acme", "a b"));
    }

    @Test
    void isInvalidLogicalFormAcceptsLegalNames() {
        assertFalse(TenantNamespace.isInvalidLogicalForm("acme", "orders"));
        assertFalse(TenantNamespace.isInvalidLogicalForm("acme", "legacy.event"));
        assertFalse(TenantNamespace.isInvalidLogicalForm("acme", "a-1_b.2"));
    }

    @Test
    void isInvalidLogicalFormIgnoresInternalTopics() {
        // Internal topics never go through the rewrite, so length / pattern
        // validation does not apply to them.
        assertFalse(TenantNamespace.isInvalidLogicalForm("acme", "__consumer_offsets"));
        assertFalse(TenantNamespace.isInvalidLogicalForm("acme", "__transaction_state"));
    }

    @Test
    void isInvalidLogicalFormIsFalseForNullName() {
        // Handlers guard against null separately; the namespace helper treats
        // null as "no opinion" to keep call-site semantics simple.
        assertFalse(TenantNamespace.isInvalidLogicalForm("acme", null));
    }

    @Test
    void isInvalidLogicalFormFlagsNamesAlsoFlaggedByTopicIsValid() {
        // Topic.isValid also rejects names > 249 chars. We don't rely on
        // this overlap (isOverlongLogicalForm catches the prefixed case at
        // a tighter bound), but document that the predicates are
        // independent enough to coexist without inversion.
        String tooLong = "a".repeat(250);
        assertTrue(TenantNamespace.isInvalidLogicalForm("acme", tooLong));
    }
}
