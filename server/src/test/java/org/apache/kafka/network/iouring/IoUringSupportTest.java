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

import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link IoUringSupport}'s platform probe. We don't assume the test host
 * has io_uring — we assert the probe's invariants either way.
 */
class IoUringSupportTest {

    @Test
    void linuxDetectionReflectsOsName() {
        boolean expected = System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("linux");
        assertEquals(expected, IoUringSupport.isLinux(),
            "isLinux() must agree with the JVM's os.name property");
    }

    @Test
    void unavailabilityReportsACauseWhenUnavailable() {
        if (IoUringSupport.isAvailable()) return;
        String reason = IoUringSupport.unavailabilityReason();
        assertNotNull(reason, "unavailabilityReason() must return a non-null explanation");
        assertFalse(reason.isBlank(), "explanation cannot be blank");
    }

    @Test
    void availabilityReasonIsAvailableWhenAvailable() {
        if (!IoUringSupport.isAvailable()) return;
        assertNull(IoUringSupport.unavailabilityReason(),
            "unavailabilityReason() must be null when isAvailable() is true");
    }

    @Test
    void isAvailableImpliesLinux() {
        if (IoUringSupport.isAvailable()) {
            assertTrue(IoUringSupport.isLinux(),
                "io_uring can only be available on Linux");
        }
    }

    private static void assertNull(Object o, String msg) {
        if (o != null) throw new AssertionError(msg + " (got: " + o + ")");
    }
}
