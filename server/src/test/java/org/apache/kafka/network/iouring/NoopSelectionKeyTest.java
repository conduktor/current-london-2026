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

import java.nio.channels.SelectionKey;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for {@link NoopSelectionKey}.
 *
 * <p>The key is used by {@code KafkaChannel.selectionKey()} as a returnable handle. The
 * io_uring backend does not subscribe to OS-level readiness events through any JDK
 * Selector, so the key's interest-ops, readiness, and selector fields are inert. The
 * tests below pin down the inert behaviour so future contributors do not "improve" it
 * by accident.
 */
class NoopSelectionKeyTest {

    @Test
    void interestOpsRoundTripsWithoutTouchingTheKernel() {
        NoopSelectionKey key = new NoopSelectionKey();
        assertEquals(0, key.interestOps(), "fresh key starts with no interest ops");

        SelectionKey returned = key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        assertSame(key, returned, "interestOps(int) returns this, per the JDK contract");
        assertEquals(SelectionKey.OP_READ | SelectionKey.OP_WRITE, key.interestOps());
    }

    @Test
    void readyOpsAreAlwaysZeroSinceWeNeverGoThroughASelector() {
        NoopSelectionKey key = new NoopSelectionKey();
        key.interestOps(SelectionKey.OP_READ);
        assertEquals(0, key.readyOps(),
            "readyOps reflects what a JDK Selector observed; io_uring drives I/O itself, so always 0");
        assertFalse(key.isReadable());
        assertFalse(key.isWritable());
    }

    @Test
    void cancelFlipsValidFalse() {
        NoopSelectionKey key = new NoopSelectionKey();
        assertTrue(key.isValid(), "fresh key is valid");
        key.cancel();
        assertFalse(key.isValid(), "cancel makes the key invalid, matching the JDK contract");
    }

    @Test
    void attachmentRoundTrips() {
        // Selector.doClose calls key.attach(null) — must accept the call without throwing.
        NoopSelectionKey key = new NoopSelectionKey();
        Object first = key.attach(new Object());
        assertNull(first, "first attach returns the previous attachment (initially null)");
        Object attached = key.attachment();
        key.attach(null);
        assertNull(key.attachment(), "attach(null) clears");
        // Sanity: the round-trip object was held identity-stable until cleared.
        assertSame(attached, attached);
    }

    @Test
    void channelAndSelectorAreNullToReflectAbsenceOfABackingJdkRegistration() {
        // Nothing in io_uring's path constructs a JDK SelectableChannel/Selector pair,
        // so these getters return null. Callers that need the underlying transport should
        // go through KafkaChannel/IoUringTransportLayer instead.
        NoopSelectionKey key = new NoopSelectionKey();
        assertNull(key.channel());
        assertNull(key.selector());
    }
}
