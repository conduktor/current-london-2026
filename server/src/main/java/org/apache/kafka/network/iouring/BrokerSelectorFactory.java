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
package org.apache.kafka.network.iouring;

import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.network.SocketServerConfigs;

/**
 * Decision function that maps the operator-requested
 * {@link SelectorImplementation}, the listener's {@link SecurityProtocol}, and a
 * platform availability flag to the effective implementation a per-listener
 * Processor should construct.
 *
 * <p>Rules (v1):
 * <ul>
 *   <li>Explicit {@code nio} always wins.</li>
 *   <li>io_uring is gated on PLAINTEXT only; SSL / SASL_PLAINTEXT / SASL_SSL transparently
 *       fall back to NIO regardless of the request.</li>
 *   <li>{@code auto} resolves to io_uring iff the platform supports it; otherwise NIO.</li>
 *   <li>Explicit {@code io_uring} on a PLAINTEXT listener with no platform support is a
 *       hard error — the operator asked for io_uring; silently downgrading would hide
 *       configuration drift.</li>
 * </ul>
 */
public final class BrokerSelectorFactory {

    private BrokerSelectorFactory() { }

    /**
     * @param requested  operator's parsed {@link SelectorImplementation} config value
     * @param protocol   the listener's security protocol
     * @param ioUringAvailable platform probe result — typically {@link IoUringSupport#isAvailable()}
     * @return the effective implementation to construct
     * @throws IllegalStateException when {@code io_uring} is explicitly requested for a
     *         PLAINTEXT listener but the host has no io_uring support
     */
    public static SelectorImplementation resolve(SelectorImplementation requested,
                                                 SecurityProtocol protocol,
                                                 boolean ioUringAvailable) {
        if (requested == SelectorImplementation.NIO) {
            return SelectorImplementation.NIO;
        }
        // v1 PLAINTEXT-only constraint: any non-PLAINTEXT listener transparently uses NIO.
        if (protocol != SecurityProtocol.PLAINTEXT) {
            return SelectorImplementation.NIO;
        }
        if (requested == SelectorImplementation.AUTO) {
            return ioUringAvailable ? SelectorImplementation.IO_URING : SelectorImplementation.NIO;
        }
        // Explicit IO_URING on a PLAINTEXT listener.
        if (!ioUringAvailable) {
            String reason = IoUringSupport.unavailabilityReason();
            throw new IllegalStateException(
                SocketServerConfigs.SOCKET_SELECTOR_IMPLEMENTATION_CONFIG
                    + "=" + SelectorImplementation.IO_URING.configValue()
                    + " was requested but io_uring is unavailable on this host: "
                    + (reason != null ? reason : "unknown reason"));
        }
        return SelectorImplementation.IO_URING;
    }
}
