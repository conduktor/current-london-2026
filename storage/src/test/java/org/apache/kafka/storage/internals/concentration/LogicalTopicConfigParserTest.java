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

    // ---- r19 ADV-CONFIG #111: duplicate logical-name entries must be rejected ----

    @Test
    public void duplicateLogicalNameWithIdenticalBodyIsRejected() {
        // r19 ADV-CONFIG MEDIUM #111: silent acceptance of an exact-repeat entry is a footgun —
        // an operator pasting the same declaration twice will assume the second one took effect
        // (perhaps with different intent that they typo'd). Reject loudly so the operator notices.
        ConfigException e = assertThrows(ConfigException.class,
            () -> LogicalTopicConfigParser.parse("orders:100:shared:4,orders:100:shared:4"));
        assertTrue(e.getMessage().contains("orders"),
            "error must name the duplicate logical topic: " + e.getMessage());
        assertTrue(e.getMessage().contains("#2"),
            "error must point at the duplicate's 1-based index: " + e.getMessage());
    }

    @Test
    public void duplicateLogicalNameWithDifferentBodyIsRejectedWithBothQuoted() {
        // The conflict case is louder than the identical-body case: the parser must surface BOTH
        // bodies so the operator can see which declaration was already applied and which one
        // collided. Without quoting both, the message is uninterpretable when the operator
        // doesn't have the previous config to compare against.
        ConfigException e = assertThrows(ConfigException.class,
            () -> LogicalTopicConfigParser.parse("orders:100:shared:4,orders:50:other:2"));
        assertTrue(e.getMessage().contains("orders"),
            "error must name the duplicate logical topic: " + e.getMessage());
        // Both partition counts must appear so the operator can tell the two entries apart.
        assertTrue(e.getMessage().contains("100") && e.getMessage().contains("50"),
            "error must quote both conflicting bodies: " + e.getMessage());
    }

    // ---- r19 ADV-CONFIG #109: parseAndDeclare must propagate kernel rejection as ConfigException ----

    @Test
    public void parseAndDeclareRejectsCrossDescriptorBackingConflictAsConfigException() {
        // r19 ADV-CONFIG HIGH #109: LogicalTopicRegistry#declare throws IllegalStateException for
        // cross-descriptor invariants (here: two logical topics sharing a backing must agree on M).
        // Until we routed every declare through parseAndDeclare the broker startup site raised the
        // raw IllegalStateException — wrong exception class, half-applied kernel state. The helper
        // must rewrap as a ConfigException so broker startup fails uniformly with other malformed
        // config and the operator sees a sensible error message.
        ConcentrationKernel kernel = new ConcentrationKernel(stubSidecarDir());
        try {
            ConfigException e = assertThrows(ConfigException.class,
                () -> LogicalTopicConfigParser.parseAndDeclare(
                    "orders:100:shared:4,payments:50:shared:8", kernel));
            assertTrue(e.getMessage().contains("payments:50:shared:8")
                    || e.getMessage().contains("payments"),
                "error must reference the rejected entry: " + e.getMessage());
            // The exception cause is the IllegalStateException from the kernel so operators with
            // exception-aware tooling can drill in if needed.
            assertTrue(e.getCause() instanceof IllegalStateException,
                "underlying cause must be the kernel's IllegalStateException, was: " + e.getCause());
        } finally {
            closeQuietly(kernel);
        }
    }

    @Test
    public void parseAndDeclareDeclaresEveryDescriptorOnSuccess() {
        // Happy path: the helper feeds every parsed descriptor into the kernel in order so a
        // multi-line config produces a kernel whose registry contains all of them. We assert
        // through the kernel's public API rather than reaching into the registry.
        ConcentrationKernel kernel = new ConcentrationKernel(stubSidecarDir());
        try {
            LogicalTopicConfigParser.parseAndDeclare(
                "orders:100:shared:4,payments:50:shared:4", kernel);
            assertTrue(kernel.isLogicalTopicDeclared("orders"),
                "orders must be declared after parseAndDeclare");
            assertTrue(kernel.isLogicalTopicDeclared("payments"),
                "payments must be declared after parseAndDeclare");
        } finally {
            closeQuietly(kernel);
        }
    }

    @Test
    public void parseAndDeclareIsANoOpForEmptyConfig() {
        ConcentrationKernel kernel = new ConcentrationKernel(stubSidecarDir());
        try {
            LogicalTopicConfigParser.parseAndDeclare("", kernel);
            LogicalTopicConfigParser.parseAndDeclare(null, kernel);
            assertTrue(kernel.allDeclaredLogicalTopicNames().isEmpty(),
                "empty config must leave the kernel empty");
        } finally {
            closeQuietly(kernel);
        }
    }

    // ---- r19 ADV-CONFIG #126: deterministic fingerprint for cross-stack drift detection ----

    @Test
    public void fingerprintIsDeterministicAcrossOrderingAndWhitespace() {
        // r19 ADV-CONFIG HIGH #126: broker and controller parse the same string but the
        // declarations cannot be exchanged in v1 (no KRaft metadata record yet). The only way
        // an operator can detect drift is to compare a fingerprint emitted at startup. The
        // fingerprint must be stable under (a) ordering of entries and (b) whitespace, so a
        // broker and controller with the same SEMANTIC config produce the same string regardless
        // of how the operator typed it.
        String a = LogicalTopicConfigParser.declarationFingerprint(
            "orders:100:shared:4,payments:50:shared:4");
        String b = LogicalTopicConfigParser.declarationFingerprint(
            "payments:50:shared:4 , orders:100:shared:4");
        assertEquals(a, b,
            "fingerprint must be insensitive to entry ordering and whitespace");

        // Different declarations must produce different fingerprints — otherwise the operator
        // tooling cannot detect drift.
        String c = LogicalTopicConfigParser.declarationFingerprint(
            "orders:100:shared:4,payments:50:shared:8");
        assertTrue(!a.equals(c),
            "fingerprint must change when M differs");

        // Empty config produces a distinct, recognizable fingerprint (not the empty string),
        // so log scraping can tell apart "operator forgot to declare" from a missing field.
        String empty = LogicalTopicConfigParser.declarationFingerprint("");
        assertTrue(!empty.isEmpty(), "empty config still yields a printable fingerprint");
    }

    private static java.io.File stubSidecarDir() {
        // Use a unique tmp dir per test; we never actually write sidecars from these tests since
        // we only exercise the registry/declaration path. close() does not touch the dir if no
        // sidecars were opened, so this is purely a constructor sink.
        try {
            return java.nio.file.Files.createTempDirectory("logical-config-parser-test").toFile();
        } catch (java.io.IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private static void closeQuietly(ConcentrationKernel kernel) {
        try {
            kernel.close();
        } catch (java.io.IOException ignored) {
            // Test teardown — a failure to close a kernel that owns no sidecars is not a
            // signal worth propagating.
        }
    }
}
