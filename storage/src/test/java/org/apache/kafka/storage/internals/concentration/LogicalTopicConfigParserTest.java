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

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behavioural contract of {@link LogicalTopicConfigParser}. The parser is the v1 shortcut
 * for declaring logical topics at broker startup — see PROMPT.md hook #6. Long-term path is
 * a KRaft metadata record; until then, the broker config {@code concentration.logical.topics}
 * carries the declarations as a comma-separated list of {@code logical:N:backing:M} quads.
 *
 * <p>The parser is the single seam where malformed input becomes a {@link ConfigException}
 * that fails broker startup, so every invariant the kernel relies on must surface here as a
 * loud, attributable failure rather than a silent skip or a deferred crash.
 */
public class LogicalTopicConfigParserTest {

    @Test
    public void nullInputYieldsEmptyList() {
        assertTrue(LogicalTopicConfigParser.parse(null).isEmpty());
    }

    @Test
    public void blankInputYieldsEmptyList() {
        assertTrue(LogicalTopicConfigParser.parse("").isEmpty());
        assertTrue(LogicalTopicConfigParser.parse("   ").isEmpty());
        assertTrue(LogicalTopicConfigParser.parse("\t\n").isEmpty());
    }

    @Test
    public void singleEntryParsesIntoOneDescriptor() {
        List<LogicalTopicDescriptor> result = LogicalTopicConfigParser.parse("orders:100:shared:4");
        assertEquals(1, result.size());
        LogicalTopicDescriptor d = result.get(0);
        assertEquals("orders", d.logicalName());
        assertEquals(100, d.numLogicalPartitions());
        assertEquals("shared", d.backingTopic());
        assertEquals(4, d.numBackingPartitions());
    }

    @Test
    public void multipleEntriesShareABacking() {
        List<LogicalTopicDescriptor> result = LogicalTopicConfigParser.parse(
            "orders:100:shared:4,payments:50:shared:4");
        assertEquals(2, result.size());
        assertEquals("orders", result.get(0).logicalName());
        assertEquals("payments", result.get(1).logicalName());
        assertEquals("shared", result.get(0).backingTopic());
        assertEquals("shared", result.get(1).backingTopic());
    }

    @Test
    public void whitespaceInsideAndAroundEntriesIsTrimmed() {
        // Real configs come from properties files where humans add spaces. The parser must
        // tolerate spaces around the comma boundaries and around the colon-delimited tokens.
        List<LogicalTopicDescriptor> result = LogicalTopicConfigParser.parse(
            "  orders : 100 : shared : 4  ,  payments : 50 : shared : 4  ");
        assertEquals(2, result.size());
        assertEquals("orders", result.get(0).logicalName());
        assertEquals(100, result.get(0).numLogicalPartitions());
        assertEquals("payments", result.get(1).logicalName());
    }

    @Test
    public void trailingCommaIsTolerated() {
        List<LogicalTopicDescriptor> result = LogicalTopicConfigParser.parse("orders:100:shared:4,");
        assertEquals(1, result.size());
    }

    @Test
    public void emptyEntryBetweenCommasIsTolerated() {
        // A double comma is treated as a single separator. This is consistent with how Kafka's
        // own list-config parsing tolerates empties, and avoids gratuitous failures on configs
        // built by template engines.
        List<LogicalTopicDescriptor> result = LogicalTopicConfigParser.parse(
            "orders:100:shared:4,,payments:50:shared:4");
        assertEquals(2, result.size());
    }

    @Test
    public void wrongNumberOfTokensRaisesConfigException() {
        // 3 tokens — missing M
        ConfigException three = assertThrows(ConfigException.class,
            () -> LogicalTopicConfigParser.parse("orders:100:shared"));
        assertTrue(three.getMessage().contains("orders:100:shared"),
            "error must quote the offending entry: " + three.getMessage());

        // 5 tokens — extra garbage
        ConfigException five = assertThrows(ConfigException.class,
            () -> LogicalTopicConfigParser.parse("orders:100:shared:4:extra"));
        assertTrue(five.getMessage().contains("orders:100:shared:4:extra"));

        // No colons at all
        assertThrows(ConfigException.class,
            () -> LogicalTopicConfigParser.parse("nonsense"));
    }

    @Test
    public void nonNumericPartitionCountRaisesConfigException() {
        ConfigException n = assertThrows(ConfigException.class,
            () -> LogicalTopicConfigParser.parse("orders:lots:shared:4"));
        assertTrue(n.getMessage().contains("orders:lots:shared:4"),
            "error must quote the offending entry: " + n.getMessage());

        ConfigException m = assertThrows(ConfigException.class,
            () -> LogicalTopicConfigParser.parse("orders:100:shared:few"));
        assertTrue(m.getMessage().contains("orders:100:shared:few"));
    }

    @Test
    public void descriptorInvariantViolationsBubbleUpAsConfigException() {
        // N < M — the LogicalTopicDescriptor constructor enforces this.
        ConfigException nLessThanM = assertThrows(ConfigException.class,
            () -> LogicalTopicConfigParser.parse("orders:3:shared:4"));
        assertTrue(nLessThanM.getMessage().contains("orders:3:shared:4"));

        // Logical name == backing name.
        assertThrows(ConfigException.class,
            () -> LogicalTopicConfigParser.parse("shared:4:shared:4"));

        // Zero / negative partition counts.
        assertThrows(ConfigException.class,
            () -> LogicalTopicConfigParser.parse("orders:0:shared:4"));
        assertThrows(ConfigException.class,
            () -> LogicalTopicConfigParser.parse("orders:100:shared:0"));
    }

    @Test
    public void errorMessageIncludesEntryIndexForMultiEntryInput() {
        // Index helps an operator find the offending entry in a long config line.
        ConfigException e = assertThrows(ConfigException.class,
            () -> LogicalTopicConfigParser.parse("orders:100:shared:4,broken:nope:also-broken:4"));
        // index #2 — the second entry is the broken one (1-based for human-friendly errors).
        assertTrue(e.getMessage().contains("#2"),
            "error must include entry index: " + e.getMessage());
        assertTrue(e.getMessage().contains("broken:nope:also-broken:4"));
    }
}
