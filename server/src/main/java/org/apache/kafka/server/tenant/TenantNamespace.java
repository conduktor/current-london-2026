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

    private TenantNamespace() { }

    public static String encodePrincipalName(String tenantId, String userName) {
        validateTenantId(tenantId);
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
        if (isInternalTopic(logicalTopic)) {
            return logicalTopic;
        }
        return tenantId + SEPARATOR + logicalTopic;
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
