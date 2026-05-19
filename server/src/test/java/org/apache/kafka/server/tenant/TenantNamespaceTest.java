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
    void encodePrincipalNameRefusesUserNameWithReservedPrefix() {
        // Defence-in-depth against the delegation-token cross-listener replay
        // vector. If a user name "__tenant_acme.alice" reached this function we
        // would silently produce "__tenant_beta.__tenant_acme.alice", which
        // parseTenantId splits at the FIRST dot and resolves to "beta". This
        // is the path TenantPrincipalBuilder.build closes off — but any other
        // caller (admin tooling, future builders) that lands here with such a
        // user name must fail fast rather than materialise an
        // identity-laundering principal.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> TenantNamespace.encodePrincipalName("beta", "__tenant_acme.alice"));
        assertTrue(ex.getMessage().contains("__tenant_acme.alice"),
            "error should quote the offending user name; was: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("__tenant_"),
            "error should explain the reserved prefix; was: " + ex.getMessage());
    }

    @Test
    void encodePrincipalNameRefusesBareReservedPrefix() {
        // A SCRAM credential literally named "__tenant_" must not slip
        // through. parseTenantId would return empty (no dot), but the
        // defence-in-depth refuses any user name that starts with the prefix
        // — including the bare prefix itself.
        assertThrows(IllegalArgumentException.class,
            () -> TenantNamespace.encodePrincipalName("acme", "__tenant_"));
    }

    @Test
    void encodePrincipalNameRefusesNullUserName() {
        // Java string concatenation with null produces the literal "null", so
        // encodePrincipalName("acme", null) would silently materialise the
        // principal "__tenant_acme.null" — indistinguishable from a user
        // named "null" and accepted by parseTenantId as tenant "acme" with
        // user "null". Fail fast.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> TenantNamespace.encodePrincipalName("acme", null));
        assertTrue(ex.getMessage().contains("null"),
            "error should mention null user name; was: " + ex.getMessage());
    }

    @Test
    void encodePrincipalNameRefusesEmptyUserName() {
        // An empty user name produces "__tenant_acme." — which (post-fix)
        // parseTenantId refuses, but belongsToCallerTenant would still
        // happily match any write target starting with "__tenant_acme.".
        // Refuse here so no caller — admin tool, future builder — can mint
        // such a phantom-owner principal from the encode side.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> TenantNamespace.encodePrincipalName("acme", ""));
        assertTrue(ex.getMessage().contains("empty"),
            "error should mention empty user name; was: " + ex.getMessage());
    }

    @Test
    void parseTenantFromPrincipalReturnsEmptyForEmptyUserName() {
        // Principal "__tenant_acme." (separator with no user portion) cannot
        // be minted via SCRAM or PLAIN (both refuse empty user names) but a
        // deliberate operator misconfig could plant one. If accepted as
        // tenant "acme", the empty-user principal would become a phantom
        // owner: belongsToCallerTenant matches "__tenant_acme." against any
        // write target starting with the prefix and would grant
        // cross-target tenant-"acme" ownership. Treat as no tenant so the
        // outside-in guard then refuses every write into the reserved
        // namespace.
        KafkaPrincipal p = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.");
        assertEquals(Optional.empty(), TenantNamespace.parseTenantId(p));
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
    void parseTenantFromPrincipalReturnsEmptyForGroupTypedPrincipal() {
        // Tenant identity is a USER-typed principal. A Group-typed principal
        // carrying `__tenant_acme.bob` in its name (e.g. produced by a custom
        // principal builder, a SCRAM extension, or an Envelope-forwarded
        // principal whose principalType was tampered with) would otherwise be
        // treated as tenant `acme` by every L1/L2 guard while remaining
        // invisible to the StandardAuthorizer comparing against `User:` ACL
        // bindings — crossing the isolation boundary with no authority. Refuse
        // here so the caller is treated as non-tenant.
        KafkaPrincipal p = new KafkaPrincipal("Group", "__tenant_acme.bob");
        assertEquals(Optional.empty(), TenantNamespace.parseTenantId(p));
    }

    @Test
    void parseTenantFromPrincipalReturnsEmptyForCustomTypePrincipal() {
        // Same as the Group case: any non-User type is refused. Custom
        // principal types ("Role", "ServiceAccount", lowercase "user") never
        // confer tenant identity.
        assertEquals(Optional.empty(),
            TenantNamespace.parseTenantId(new KafkaPrincipal("Role", "__tenant_acme.bob")));
        assertEquals(Optional.empty(),
            TenantNamespace.parseTenantId(new KafkaPrincipal("ServiceAccount", "__tenant_acme.bob")));
        assertEquals(Optional.empty(),
            TenantNamespace.parseTenantId(new KafkaPrincipal("user", "__tenant_acme.bob")));
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

    // --- Consumer-group ID rewrites (PROMPT.md scenario 49) ---

    @Test
    void groupToPhysicalPrefixesLogicalIdWithTenantPrincipalPrefix() {
        // The group prefix reuses PRINCIPAL_PREFIX so the wire form cannot
        // collide with a topic name (topic uses just `<id>.`).
        assertEquals("__tenant_acme.orders-consumer",
            TenantNamespace.groupToPhysical("acme", "orders-consumer"));
    }

    @Test
    void groupToPhysicalPassesThroughAlreadyPrefixedIdForSameTenant() {
        // PROMPT.md scenario 49: an admin tool explicitly addressing
        // `__tenant_acme.foo` must succeed without double-prefixing into
        // `__tenant_acme.__tenant_acme.foo`.
        assertEquals("__tenant_acme.foo",
            TenantNamespace.groupToPhysical("acme", "__tenant_acme.foo"));
    }

    @Test
    void groupToPhysicalRejectsCrossTenantPrefixedId() {
        // A tenant addressing `__tenant_other.foo` is either hostile or
        // confused; refusing here keeps the broker from ever storing an
        // uglier `__tenant_acme.__tenant_other.foo` in acme's namespace.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> TenantNamespace.groupToPhysical("acme", "__tenant_other.foo"));
        assertTrue(ex.getMessage().contains("reserved prefix"));
    }

    @Test
    void groupToPhysicalRejectsReservedPrefixWithoutDot() {
        // `__tenant_xyz` (no separator) cannot be a valid encoded form for any
        // tenant; treating it as logical would silently materialise the name
        // in this tenant's namespace.
        assertThrows(IllegalArgumentException.class,
            () -> TenantNamespace.groupToPhysical("acme", "__tenant_xyz"));
    }

    @Test
    void groupToPhysicalReturnsNullForNullInput() {
        // Some protocol versions allow an empty/null groupId (e.g. legacy
        // OffsetCommit). The namespace helper treats null as "no opinion" so
        // handlers can call it unconditionally before reaching the coordinator.
        assertEquals(null, TenantNamespace.groupToPhysical("acme", null));
    }

    @Test
    void groupToPhysicalAllowsEmptyLogicalId() {
        // Kafka does not forbid empty group ids at the protocol layer; we
        // mirror that: an empty logical id maps to the bare prefix. The
        // coordinator owns any further validation.
        assertEquals("__tenant_acme.", TenantNamespace.groupToPhysical("acme", ""));
    }

    @Test
    void groupToPhysicalAllowsDotsInsideLogicalId() {
        // Unlike topic names, group ids have no charset restriction. A
        // logical id `foo.bar` must round-trip to `__tenant_acme.foo.bar`.
        assertEquals("__tenant_acme.foo.bar",
            TenantNamespace.groupToPhysical("acme", "foo.bar"));
    }

    @Test
    void groupToPhysicalRejectsNullOrEmptyTenantId() {
        // The same invariant that protects principal encoding: a tenant id
        // missing here would mean we're rewriting on behalf of nobody.
        assertThrows(IllegalArgumentException.class,
            () -> TenantNamespace.groupToPhysical(null, "foo"));
        assertThrows(IllegalArgumentException.class,
            () -> TenantNamespace.groupToPhysical("", "foo"));
    }

    @Test
    void groupToLogicalStripsTenantPrefixWhenPresent() {
        assertEquals("orders-consumer",
            TenantNamespace.groupToLogical("acme", "__tenant_acme.orders-consumer"));
    }

    @Test
    void groupToLogicalReturnsForeignGroupIdUnchanged() {
        // Defensive: the coordinator should never hand back a foreign group
        // to a tenant request, but if it did, do not silently rewrite — keep
        // the wire form so the surprise is visible.
        assertEquals("__tenant_other.foo",
            TenantNamespace.groupToLogical("acme", "__tenant_other.foo"));
    }

    @Test
    void groupToLogicalReturnsNullForNullInput() {
        assertEquals(null, TenantNamespace.groupToLogical("acme", null));
    }

    @Test
    void groupBelongsToOnlyMatchesExactTenantPrefix() {
        assertTrue(TenantNamespace.groupBelongsTo("acme", "__tenant_acme.foo"));
        assertFalse(TenantNamespace.groupBelongsTo("acme", "__tenant_other.foo"));
        assertFalse(TenantNamespace.groupBelongsTo("acme", "foo"));
        assertFalse(TenantNamespace.groupBelongsTo("acme", "__tenant_acmebis.foo"));
        assertFalse(TenantNamespace.groupBelongsTo("acme", null));
    }

    // --- Transactional ID rewrites (PROMPT.md line 47) ---

    @Test
    void txnIdToPhysicalPrefixesLogicalIdWithTenantPrincipalPrefix() {
        // Same encoding as consumer-group ids: __transaction_state is keyed by
        // hash(transactional_id) so two tenants using the same external id must
        // collide-distinct on coordinator state.
        assertEquals("__tenant_acme.orders-txn",
            TenantNamespace.txnIdToPhysical("acme", "orders-txn"));
    }

    @Test
    void txnIdToPhysicalPassesThroughAlreadyPrefixedIdForSameTenant() {
        // Admin-tool passthrough: explicitly addressing the physical form must
        // not double-prefix.
        assertEquals("__tenant_acme.foo",
            TenantNamespace.txnIdToPhysical("acme", "__tenant_acme.foo"));
    }

    @Test
    void txnIdToPhysicalRejectsCrossTenantPrefixedId() {
        // Hostile or confused: refuse rather than store
        // __tenant_acme.__tenant_other.foo in acme's namespace.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> TenantNamespace.txnIdToPhysical("acme", "__tenant_other.foo"));
        assertTrue(ex.getMessage().contains("reserved prefix"));
        // Error label distinguishes the entity kind — useful for the operator
        // diagnosing a misuse from a noisy log line.
        assertTrue(ex.getMessage().contains("Transactional"));
    }

    @Test
    void txnIdToPhysicalRejectsReservedPrefixWithoutDot() {
        assertThrows(IllegalArgumentException.class,
            () -> TenantNamespace.txnIdToPhysical("acme", "__tenant_xyz"));
    }

    @Test
    void txnIdToPhysicalReturnsNullForNullInput() {
        // InitProducerId allows null transactional_id (idempotent-only producers).
        // Treat null as no-op so handlers can rewrite unconditionally.
        assertEquals(null, TenantNamespace.txnIdToPhysical("acme", null));
    }

    @Test
    void txnIdToPhysicalAllowsEmptyLogicalId() {
        // Kafka does not forbid empty transactional ids at the namespace layer;
        // the coordinator owns higher-level validation.
        assertEquals("__tenant_acme.", TenantNamespace.txnIdToPhysical("acme", ""));
    }

    @Test
    void txnIdToPhysicalAllowsDotsInsideLogicalId() {
        assertEquals("__tenant_acme.foo.bar",
            TenantNamespace.txnIdToPhysical("acme", "foo.bar"));
    }

    @Test
    void txnIdToPhysicalRejectsNullOrEmptyTenantId() {
        assertThrows(IllegalArgumentException.class,
            () -> TenantNamespace.txnIdToPhysical(null, "foo"));
        assertThrows(IllegalArgumentException.class,
            () -> TenantNamespace.txnIdToPhysical("", "foo"));
    }

    @Test
    void txnIdToLogicalStripsTenantPrefixWhenPresent() {
        assertEquals("orders-txn",
            TenantNamespace.txnIdToLogical("acme", "__tenant_acme.orders-txn"));
    }

    @Test
    void txnIdToLogicalReturnsForeignTxnIdUnchanged() {
        // Defensive: never silently rewrite a foreign id into the tenant's
        // namespace on the response path.
        assertEquals("__tenant_other.foo",
            TenantNamespace.txnIdToLogical("acme", "__tenant_other.foo"));
    }

    @Test
    void txnIdToLogicalReturnsNullForNullInput() {
        assertEquals(null, TenantNamespace.txnIdToLogical("acme", null));
    }

    @Test
    void txnIdBelongsToOnlyMatchesExactTenantPrefix() {
        assertTrue(TenantNamespace.txnIdBelongsTo("acme", "__tenant_acme.foo"));
        assertFalse(TenantNamespace.txnIdBelongsTo("acme", "__tenant_other.foo"));
        assertFalse(TenantNamespace.txnIdBelongsTo("acme", "foo"));
        assertFalse(TenantNamespace.txnIdBelongsTo("acme", "__tenant_acmebis.foo"));
        assertFalse(TenantNamespace.txnIdBelongsTo("acme", null));
    }
}
