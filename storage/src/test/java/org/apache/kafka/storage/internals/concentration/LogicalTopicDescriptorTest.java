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
package org.apache.kafka.storage.internals.concentration;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LogicalTopicDescriptorTest {

    @Test
    public void retainsAllConstructorArguments() {
        LogicalTopicDescriptor d = new LogicalTopicDescriptor("orders", 1000, "backing", 4);
        assertEquals("orders", d.logicalName());
        assertEquals(1000, d.numLogicalPartitions());
        assertEquals("backing", d.backingTopic());
        assertEquals(4, d.numBackingPartitions());
    }

    @Test
    public void rejectsNonPositiveLogicalPartitionCount() {
        assertThrows(IllegalArgumentException.class,
            () -> new LogicalTopicDescriptor("orders", 0, "backing", 4));
        assertThrows(IllegalArgumentException.class,
            () -> new LogicalTopicDescriptor("orders", -1, "backing", 4));
    }

    @Test
    public void rejectsNonPositiveBackingPartitionCount() {
        assertThrows(IllegalArgumentException.class,
            () -> new LogicalTopicDescriptor("orders", 1000, "backing", 0));
        assertThrows(IllegalArgumentException.class,
            () -> new LogicalTopicDescriptor("orders", 1000, "backing", -1));
    }

    @Test
    public void rejectsConcentrationRatioLessThanOne() {
        // Concentration only makes sense when N ≥ M. N < M is degenerate.
        assertThrows(IllegalArgumentException.class,
            () -> new LogicalTopicDescriptor("orders", 2, "backing", 4));
    }

    @Test
    public void acceptsConcentrationRatioOfExactlyOne() {
        // Edge case: N == M. Legal, even though there's no concentration benefit.
        LogicalTopicDescriptor d = new LogicalTopicDescriptor("orders", 4, "backing", 4);
        assertEquals(4, d.numLogicalPartitions());
        assertEquals(4, d.numBackingPartitions());
    }

    @Test
    public void rejectsBlankLogicalName() {
        assertThrows(IllegalArgumentException.class,
            () -> new LogicalTopicDescriptor("", 1000, "backing", 4));
        assertThrows(IllegalArgumentException.class,
            () -> new LogicalTopicDescriptor("  ", 1000, "backing", 4));
    }

    @Test
    public void rejectsBlankBackingTopic() {
        assertThrows(IllegalArgumentException.class,
            () -> new LogicalTopicDescriptor("orders", 1000, "", 4));
        assertThrows(IllegalArgumentException.class,
            () -> new LogicalTopicDescriptor("orders", 1000, "  ", 4));
    }

    @Test
    public void rejectsNullNames() {
        assertThrows(NullPointerException.class,
            () -> new LogicalTopicDescriptor(null, 1000, "backing", 4));
        assertThrows(NullPointerException.class,
            () -> new LogicalTopicDescriptor("orders", 1000, null, 4));
    }

    @Test
    public void rejectsLogicalNameEqualToBackingTopic() {
        // A logical topic must not share its name with its own backing; otherwise the broker
        // cannot tell whether an incoming produce is a (rejected) physical-name produce or a
        // (routed) logical produce.
        assertThrows(IllegalArgumentException.class,
            () -> new LogicalTopicDescriptor("same", 1000, "same", 4));
    }

    @Test
    public void equalityIsValueBased() {
        LogicalTopicDescriptor a = new LogicalTopicDescriptor("orders", 1000, "backing", 4);
        LogicalTopicDescriptor b = new LogicalTopicDescriptor("orders", 1000, "backing", 4);
        LogicalTopicDescriptor c = new LogicalTopicDescriptor("orders", 1000, "backing", 8);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }
}
