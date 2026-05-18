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

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LogicalPartitionMapperTest {

    private static final LogicalTopicDescriptor DESCRIPTOR =
        new LogicalTopicDescriptor("orders", 1000, "backing", 4);

    @Test
    public void mapsLogicalPartitionToBackingPartitionDeterministically() {
        int first = LogicalPartitionMapper.backingPartitionFor(DESCRIPTOR, 7);
        int second = LogicalPartitionMapper.backingPartitionFor(DESCRIPTOR, 7);
        assertEquals(first, second);
    }

    @Test
    public void mapsToValidBackingRange() {
        for (int logical = 0; logical < DESCRIPTOR.numLogicalPartitions(); logical++) {
            int backing = LogicalPartitionMapper.backingPartitionFor(DESCRIPTOR, logical);
            assertTrue(backing >= 0 && backing < DESCRIPTOR.numBackingPartitions(),
                "backing partition out of range for logical=" + logical + ": " + backing);
        }
    }

    @Test
    public void coversAllBackingPartitions() {
        Set<Integer> seen = new HashSet<>();
        for (int logical = 0; logical < DESCRIPTOR.numLogicalPartitions(); logical++) {
            seen.add(LogicalPartitionMapper.backingPartitionFor(DESCRIPTOR, logical));
        }
        for (int backing = 0; backing < DESCRIPTOR.numBackingPartitions(); backing++) {
            assertTrue(seen.contains(backing),
                "backing partition " + backing + " never mapped to");
        }
    }

    @Test
    public void rejectsLogicalPartitionOutOfRange() {
        assertThrows(IllegalArgumentException.class,
            () -> LogicalPartitionMapper.backingPartitionFor(DESCRIPTOR, -1));
        assertThrows(IllegalArgumentException.class,
            () -> LogicalPartitionMapper.backingPartitionFor(DESCRIPTOR,
                DESCRIPTOR.numLogicalPartitions()));
    }

    @Test
    public void firstFewLogicalPartitionsMapByModulo() {
        // The exact mapping function is not part of the contract, but a modulo mapping is the
        // simplest stable choice. Pin it so accidental changes are caught: same logical id always
        // lands on the same backing.
        LogicalTopicDescriptor d = new LogicalTopicDescriptor("t", 10, "b", 3);
        assertEquals(0, LogicalPartitionMapper.backingPartitionFor(d, 0));
        assertEquals(1, LogicalPartitionMapper.backingPartitionFor(d, 1));
        assertEquals(2, LogicalPartitionMapper.backingPartitionFor(d, 2));
        assertEquals(0, LogicalPartitionMapper.backingPartitionFor(d, 3));
        assertEquals(1, LogicalPartitionMapper.backingPartitionFor(d, 7));
        assertEquals(0, LogicalPartitionMapper.backingPartitionFor(d, 9));
    }
}
