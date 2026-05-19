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
package org.apache.kafka.network.http;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Helpers that classify HTTP bridge bind addresses for operator-facing diagnostics.
 *
 * <p>The bridge has no per-request authentication in v1 and runs every request as {@code KafkaPrincipal.ANONYMOUS}.
 * A wildcard bind (any-local address) plus a missing front-end is the canonical
 * "anonymous Kafka data-plane takeover" misconfiguration — so the {@link kafka.server.BrokerServer} startup path
 * emits an extra WARN whenever the configured host resolves to a wildcard. We extract the predicate into its own
 * class so the rule is one place, unit-testable, and not buried in BrokerServer.
 *
 * <p>Detection is semantic, not syntactic: Java accepts many spellings of the any-local address
 * ({@code 0.0.0.0}, {@code 0}, {@code ::}, {@code 0:0:0:0:0:0:0:0}, etc.), and matching exact strings misses
 * most of them. We resolve the host through {@link InetAddress#getAllByName(String)} and ask each result whether
 * {@link InetAddress#isAnyLocalAddress() isAnyLocalAddress} returns {@code true}.
 */
public final class HostBindingValidator {

    private HostBindingValidator() {
    }

    /**
     * Return {@code true} when {@code host} either is blank or resolves to a wildcard (any-local) address.
     *
     * <p>Blank is reported as wildcard because Jetty interprets {@code null}/empty as "all interfaces" — the same
     * operational exposure as an explicit {@code 0.0.0.0}, so an operator who left the value empty should see the
     * same WARN.
     *
     * <p>If the host fails to resolve, the predicate returns {@code false}: a bad value is somebody else's problem
     * (Jetty will fail loudly when it tries to bind), not a security alert from this helper.
     *
     * @param host the value of {@code http.bridge.host}
     * @return {@code true} when the binding is or would be a wildcard
     */
    public static boolean isWildcardBind(String host) {
        if (host == null) {
            return true;
        }
        String trimmed = host.trim();
        if (trimmed.isEmpty()) {
            return true;
        }
        // Strip the brackets some operators carry over from RFC 2732 URI literal form (e.g. `[::]`).
        // {@link InetAddress#getAllByName} already accepts both `[::]` and `::` identically — its RFC 2732
        // path strips the brackets internally and parses the inner string as an IPv6 literal, with no DNS
        // lookup. The strip here is a normalisation hygiene step so the trimmed value is a single canonical
        // form, not a behavioural fix for a JDK quirk.
        if (trimmed.startsWith("[") && trimmed.endsWith("]") && trimmed.length() >= 2) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }
        try {
            InetAddress[] resolved = InetAddress.getAllByName(trimmed);
            for (InetAddress addr : resolved) {
                if (addr.isAnyLocalAddress()) {
                    return true;
                }
            }
            return false;
        } catch (UnknownHostException e) {
            return false;
        }
    }
}
