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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link BrokerSelectorFactory#resolve}: the pure decision function that picks
 * the effective selector implementation given the operator request, the listener's
 * security protocol, and whether the host supports io_uring.
 *
 * <p>This is the entry-point all per-Processor selector construction goes through —
 * if the logic is wrong here, the whole feature is wrong.
 */
class BrokerSelectorFactoryTest {

    @Test
    void nioRequestAlwaysReturnsNio() {
        for (SecurityProtocol proto : SecurityProtocol.values()) {
            assertEquals(SelectorImplementation.NIO,
                BrokerSelectorFactory.resolve(SelectorImplementation.NIO, proto, /* available= */ true),
                "nio request must never escalate to io_uring (protocol=" + proto + ")");
            assertEquals(SelectorImplementation.NIO,
                BrokerSelectorFactory.resolve(SelectorImplementation.NIO, proto, /* available= */ false));
        }
    }

    @Test
    void autoOnLinuxWithIoUringPicksIoUringForPlaintextOnly() {
        assertEquals(SelectorImplementation.IO_URING,
            BrokerSelectorFactory.resolve(SelectorImplementation.AUTO, SecurityProtocol.PLAINTEXT, true),
            "AUTO + io_uring available + PLAINTEXT → io_uring");
        for (SecurityProtocol proto : nonPlaintext()) {
            assertEquals(SelectorImplementation.NIO,
                BrokerSelectorFactory.resolve(SelectorImplementation.AUTO, proto, true),
                "AUTO + io_uring available + " + proto + " must fall back to NIO (v1 PLAINTEXT-only)");
        }
    }

    @Test
    void autoWithoutIoUringAlwaysFallsBackToNio() {
        for (SecurityProtocol proto : SecurityProtocol.values()) {
            assertEquals(SelectorImplementation.NIO,
                BrokerSelectorFactory.resolve(SelectorImplementation.AUTO, proto, /* available= */ false),
                "AUTO without io_uring support falls back to NIO (protocol=" + proto + ")");
        }
    }

    @Test
    void explicitIoUringRespectsPlaintextOnly() {
        assertEquals(SelectorImplementation.IO_URING,
            BrokerSelectorFactory.resolve(SelectorImplementation.IO_URING, SecurityProtocol.PLAINTEXT, true));
        for (SecurityProtocol proto : nonPlaintext()) {
            assertEquals(SelectorImplementation.NIO,
                BrokerSelectorFactory.resolve(SelectorImplementation.IO_URING, proto, true),
                "explicit IO_URING + " + proto + " must fall back to NIO rather than fail");
        }
    }

    @Test
    void explicitIoUringWithoutPlatformSupportIsAHardError() {
        // PLAINTEXT case — the only one where the request could survive protocol fallback —
        // must blow up with a clear message rather than silently downgrade.
        IllegalStateException ex = assertThrows(IllegalStateException.class,
            () -> BrokerSelectorFactory.resolve(SelectorImplementation.IO_URING,
                SecurityProtocol.PLAINTEXT, /* available= */ false));
        // We control the message wording; assert the literal token io_uring rather than
        // lowercasing the message (which would require a Locale and trigger checkstyle).
        assertTrue(ex.getMessage().contains("io_uring"),
            "error must name io_uring, got: " + ex.getMessage());
    }

    private static SecurityProtocol[] nonPlaintext() {
        return new SecurityProtocol[]{SecurityProtocol.SSL, SecurityProtocol.SASL_PLAINTEXT, SecurityProtocol.SASL_SSL};
    }
}
