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

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SelectorImplementationTest {

    @Test
    void parsesCanonicalForms() {
        assertEquals(SelectorImplementation.NIO, SelectorImplementation.fromConfig("nio"));
        assertEquals(SelectorImplementation.IO_URING, SelectorImplementation.fromConfig("io_uring"));
        assertEquals(SelectorImplementation.AUTO, SelectorImplementation.fromConfig("auto"));
    }

    @Test
    void parsingIsCaseInsensitiveAndTrims() {
        assertEquals(SelectorImplementation.NIO, SelectorImplementation.fromConfig("NIO"));
        assertEquals(SelectorImplementation.IO_URING, SelectorImplementation.fromConfig(" IO_URING "));
        assertEquals(SelectorImplementation.AUTO, SelectorImplementation.fromConfig("Auto"));
    }

    @Test
    void rejectsUnknownValues() {
        ConfigException ex = assertThrows(ConfigException.class, () -> SelectorImplementation.fromConfig("epoll"));
        // Helpful diagnostic, not a guess
        assert ex.getMessage().toLowerCase(Locale.ROOT).contains("socket.selector.implementation")
            : "expected the config key in the error, got: " + ex.getMessage();
    }

    @Test
    void rejectsNullAndEmpty() {
        assertThrows(ConfigException.class, () -> SelectorImplementation.fromConfig(null));
        assertThrows(ConfigException.class, () -> SelectorImplementation.fromConfig(""));
        assertThrows(ConfigException.class, () -> SelectorImplementation.fromConfig("   "));
    }

    @Test
    void configValueIsStable() {
        // The string forms are part of the broker-config contract and must not drift.
        assertEquals("nio", SelectorImplementation.NIO.configValue());
        assertEquals("io_uring", SelectorImplementation.IO_URING.configValue());
        assertEquals("auto", SelectorImplementation.AUTO.configValue());
    }
}
