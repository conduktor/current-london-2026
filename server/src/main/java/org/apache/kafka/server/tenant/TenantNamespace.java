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
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.Optional;

/**
 * Translates between the logical names a tenant sees and the physical names the
 * broker stores, and encodes tenant identity in a {@link KafkaPrincipal} name.
 *
 * <p>Principal format: {@code __tenant_<id>.<user>}. The reserved prefix
 * {@value #PRINCIPAL_PREFIX} marks the principal as tenant-bound; the first dot
 * after the id separates tenant from user. User names may themselves contain
 * dots.
 *
 * <p>Topic translation: a tenant's logical name {@code orders} maps to physical
 * {@code <id>.orders}. Kafka-internal topics ({@code __consumer_offsets} and
 * friends) are never prefixed — they key by tenant-scoped identifiers (group id,
 * transactional id) and are shared by all tenants on the same broker.
 */
public final class TenantNamespace {

    public static final String PRINCIPAL_PREFIX = "__tenant_";
    static final char SEPARATOR = '.';
    // Mirrors Topic.MAX_NAME_LENGTH (which is private). Kept as a duplicated
    // constant so the rewrite can refuse over-long names BEFORE forwarding to
    // the controller (where the rejection would carry the physical name).
    private static final int MAX_TOPIC_NAME_LENGTH = 249;

    private TenantNamespace() { }

    public static String encodePrincipalName(String tenantId, String userName) {
        validateTenantId(tenantId);
        if (userName != null && userName.startsWith(PRINCIPAL_PREFIX)) {
            // Defence-in-depth: callers (TenantPrincipalBuilder) already refuse
            // a pre-stamped base name. Refuse here too so any other code path
            // that lands a `__tenant_*` user name in this function — admin
            // tooling, future builders — surfaces the bug at the call site
            // rather than materialising a double-stamped principal whose
            // parseTenantId then resolves to the OUTER tenant and crosses the
            // isolation boundary.
            throw new IllegalArgumentException(
                "User name '" + userName + "' begins with the reserved tenant prefix '"
                    + PRINCIPAL_PREFIX + "' and cannot be tenant-encoded");
        }
        return PRINCIPAL_PREFIX + tenantId + SEPARATOR + userName;
    }

    public static Optional<String> parseTenantId(KafkaPrincipal principal) {
        String name = principal.getName();
        if (!name.startsWith(PRINCIPAL_PREFIX)) {
            return Optional.empty();
        }
        int dot = name.indexOf(SEPARATOR, PRINCIPAL_PREFIX.length());
        if (dot < 0 || dot == PRINCIPAL_PREFIX.length()) {
            return Optional.empty();
        }
        return Optional.of(name.substring(PRINCIPAL_PREFIX.length(), dot));
    }

    public static String toPhysical(String tenantId, String logicalTopic) {
        if (logicalTopic == null) {
            // Defence-in-depth: callers are expected to consult
            // {@link #isInvalidLogicalForm} first, but a null slipping through
            // would otherwise concatenate to the literal string "<id>.null".
            throw new InvalidTopicException(
                "Logical topic name must not be null for tenant '" + tenantId + "'");
        }
        if (isInternalTopic(logicalTopic)) {
            return logicalTopic;
        }
        if (isReservedPhysicalForm(tenantId, logicalTopic)) {
            // A logical name that already begins with `<tenantId>.` is the
            // round-trip of a physical name; rewriting it would double-prefix
            // and silently materialise a topic like `acme.acme.orders`. The
            // namespace contract is that tenants only ever name logical topics,
            // so refuse this upstream rather than create the surprise on disk.
            throw new InvalidTopicException(
                "Logical topic name '" + logicalTopic + "' begins with the tenant prefix '"
                    + tenantId + SEPARATOR + "' and is therefore reserved");
        }
        return tenantId + SEPARATOR + logicalTopic;
    }

    /**
     * True if {@code logicalTopic}, when rewritten to the physical form
     * {@code <tenantId>.<logicalTopic>}, would exceed Kafka's MAX_NAME_LENGTH
     * (249 chars). Handlers consult this BEFORE calling {@link #toPhysical} so a
     * single over-long entry can be refused per-batch entry without aborting the
     * whole request — and so the error message quotes the LOGICAL name the
     * tenant sent rather than letting the controller reject with a message
     * containing the physical form.
     */
    public static boolean isOverlongLogicalForm(String tenantId, String logicalTopic) {
        if (logicalTopic == null || isInternalTopic(logicalTopic)) {
            return false;
        }
        return tenantId.length() + 1 + logicalTopic.length() > MAX_TOPIC_NAME_LENGTH;
    }

    /**
     * True if {@code logicalTopic} cannot be safely rewritten because it would
     * fail Kafka's own topic validation ("", ".", "..", non-LEGAL_CHARS, or
     * length &gt; MAX_NAME_LENGTH). Internal topics are exempt — they are
     * never prefixed and pass through untouched.
     *
     * <p>Handlers consult this BEFORE calling {@link #toPhysical} so the
     * rejection error message can quote the LOGICAL name the tenant sent.
     * Letting the controller reject "{@code <tenantId>.}" or "{@code <tenantId>..}"
     * would (a) carry the physical prefix back in the error string, leaking the
     * namespace, and (b) on Produce auto-create, briefly materialise the
     * malformed name before the rejection landed.
     */
    public static boolean isInvalidLogicalForm(String tenantId, String logicalTopic) {
        if (logicalTopic == null || isInternalTopic(logicalTopic)) {
            return false;
        }
        return !Topic.isValid(logicalTopic);
    }

    /**
     * True if {@code logicalTopic} begins with {@code <tenantId>.} and is not an
     * internal topic. Such a name is what {@link #toLogical} would produce for
     * a physical topic in this tenant's namespace; accepting it on the inbound
     * side would double-prefix.
     */
    public static boolean isReservedPhysicalForm(String tenantId, String logicalTopic) {
        if (logicalTopic == null || isInternalTopic(logicalTopic)) {
            return false;
        }
        return logicalTopic.startsWith(tenantId + SEPARATOR);
    }

    public static String toLogical(String tenantId, String physicalTopic) {
        if (isInternalTopic(physicalTopic)) {
            return physicalTopic;
        }
        String prefix = tenantId + SEPARATOR;
        if (physicalTopic.startsWith(prefix)) {
            return physicalTopic.substring(prefix.length());
        }
        return physicalTopic;
    }

    public static boolean belongsTo(String tenantId, String physicalTopic) {
        if (isInternalTopic(physicalTopic)) {
            return false;
        }
        return physicalTopic.startsWith(tenantId + SEPARATOR);
    }

    public static boolean isInternalTopic(String topic) {
        return Topic.isInternal(topic);
    }

    /**
     * Translates a tenant's logical consumer-group id to the physical form the
     * broker stores: {@code __tenant_<id>.<logicalGroupId>}. The reserved
     * principal prefix is reused so the wire form is self-describing and never
     * collides with a topic name (topic prefix is just {@code <id>.}).
     *
     * <p>If the caller already passes the physical form (i.e. the id already
     * starts with this tenant's prefix), it is returned as-is. This implements
     * PROMPT.md scenario 49: an admin tool that explicitly addresses
     * {@code __tenant_acme.foo} must succeed without double-prefixing.
     *
     * <p>Any other {@code __tenant_} prefix is rejected — it would either be a
     * cross-tenant attempt or, if accepted, would silently materialise an
     * uglier physical name in this tenant's namespace.
     */
    public static String groupToPhysical(String tenantId, String logicalGroupId) {
        return withPrincipalPrefix(tenantId, logicalGroupId, "Group");
    }

    /**
     * Inverse of {@link #groupToPhysical(String, String)}. If the physical id
     * carries this tenant's prefix, returns the logical part; otherwise returns
     * the input unchanged. Used on the response side so the tenant only ever
     * sees the logical name it submitted.
     */
    public static String groupToLogical(String tenantId, String physicalGroupId) {
        return stripPrincipalPrefix(tenantId, physicalGroupId);
    }

    /**
     * True if {@code physicalGroupId} is in {@code tenantId}'s namespace, i.e.
     * starts with {@code __tenant_<id>.}. Handlers use this to filter
     * all-partitions OffsetFetch responses or to refuse cross-tenant lookups.
     */
    public static boolean groupBelongsTo(String tenantId, String physicalGroupId) {
        return hasPrincipalPrefix(tenantId, physicalGroupId);
    }

    /**
     * Translates a tenant's logical transactional id to the physical form the
     * broker stores: {@code __tenant_<id>.<logicalTxnId>}. Same wire encoding
     * as group ids — both live in coordinator-keyed namespaces that partition
     * by hash, so two tenants sharing the same external transactional id must
     * resolve to distinct coordinator state.
     *
     * <p>Idempotent if the input already carries this tenant's prefix
     * (admin-tool passthrough). Rejects any foreign {@code __tenant_} prefix.
     */
    public static String txnIdToPhysical(String tenantId, String logicalTxnId) {
        return withPrincipalPrefix(tenantId, logicalTxnId, "Transactional");
    }

    /**
     * Inverse of {@link #txnIdToPhysical(String, String)}. Strips this
     * tenant's prefix if present; returns the input unchanged otherwise. Used
     * on response paths that echo the transactional id back to the client.
     */
    public static String txnIdToLogical(String tenantId, String physicalTxnId) {
        return stripPrincipalPrefix(tenantId, physicalTxnId);
    }

    /**
     * True if {@code physicalTxnId} is in {@code tenantId}'s namespace. Useful
     * for refusing cross-tenant transactional lookups at the FindCoordinator
     * boundary.
     */
    public static boolean txnIdBelongsTo(String tenantId, String physicalTxnId) {
        return hasPrincipalPrefix(tenantId, physicalTxnId);
    }

    // Shared encoding for principal-prefixed coordinator-keyed namespaces
    // (consumer groups, transactional ids). The encoding is identical because
    // both are stored in __consumer_offsets / __transaction_state by hash of
    // the id and must be tenant-distinct; only the error-message label
    // differs so a caller sees which class of id was refused.
    private static String withPrincipalPrefix(String tenantId, String logicalId, String entityKind) {
        validateTenantId(tenantId);
        if (logicalId == null) {
            return null;
        }
        String prefix = PRINCIPAL_PREFIX + tenantId + SEPARATOR;
        if (logicalId.startsWith(prefix)) {
            return logicalId;
        }
        if (logicalId.startsWith(PRINCIPAL_PREFIX)) {
            throw new IllegalArgumentException(
                entityKind + " id '" + logicalId + "' uses reserved prefix '"
                    + PRINCIPAL_PREFIX + "' but does not match tenant '" + tenantId + "'");
        }
        return prefix + logicalId;
    }

    private static String stripPrincipalPrefix(String tenantId, String physicalId) {
        if (physicalId == null) {
            return null;
        }
        String prefix = PRINCIPAL_PREFIX + tenantId + SEPARATOR;
        if (physicalId.startsWith(prefix)) {
            return physicalId.substring(prefix.length());
        }
        return physicalId;
    }

    private static boolean hasPrincipalPrefix(String tenantId, String physicalId) {
        if (physicalId == null) {
            return false;
        }
        return physicalId.startsWith(PRINCIPAL_PREFIX + tenantId + SEPARATOR);
    }

    public static void validateTenantId(String tenantId) {
        if (tenantId == null || tenantId.isEmpty()) {
            throw new IllegalArgumentException("Tenant id must be non-empty");
        }
        if (tenantId.indexOf(SEPARATOR) >= 0) {
            throw new IllegalArgumentException(
                "Tenant id must not contain '" + SEPARATOR + "': " + tenantId);
        }
        if (tenantId.startsWith("__")) {
            throw new IllegalArgumentException(
                "Tenant id must not start with reserved prefix '__': " + tenantId);
        }
        if (!Topic.isValid(tenantId)) {
            throw new IllegalArgumentException(
                "Tenant id must satisfy Kafka topic name charset: " + tenantId);
        }
    }
}
