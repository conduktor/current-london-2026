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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Parses the {@code concentration.logical.topics} broker config into a list of
 * {@link LogicalTopicDescriptor}s. The string carries comma-separated
 * {@code logical:N:backing:M} quads, e.g. {@code orders:100:shared:4,payments:50:shared:4}.
 *
 * <p>This is the v1 shortcut for hook #6 (admin declaration). The long-term path is a KRaft
 * metadata record so declarations survive controller restarts and are replicated to followers.
 * Until then, broker operators put the declarations in {@code server.properties}; the broker
 * reads them at startup and calls {@link ConcentrationKernel#declare}.
 *
 * <p>Malformed input throws {@link ConfigException} — the standard Kafka startup-time
 * rejection. Each error message quotes the offending entry and includes its 1-based index in
 * the input list, so an operator can locate the problem in a long config line without trial
 * and error.
 *
 * <p>Three failure modes the parser owns end-to-end (r19 ADV-CONFIG):
 * <ul>
 *   <li>#109 (HIGH): {@link #parseAndDeclare} rewraps any {@link IllegalStateException} raised
 *       by {@link ConcentrationKernel#declare} into a {@link ConfigException} so broker startup
 *       fails with a uniform exception class. Callers that previously did
 *       {@code parse(raw).forEach(kernel::declare)} leaked the {@code IllegalStateException}
 *       past the startup machinery — the kernel reaches a partial-declare state before the
 *       throw, and the operator sees an unhelpful stack trace.</li>
 *   <li>#111 (MEDIUM): duplicate logical-name entries are rejected at parse time. The kernel's
 *       {@link LogicalTopicRegistry#declare} treats an identical-body re-declare as an idempotent
 *       no-op, which means an operator who pasted the same line twice (perhaps with a typo on
 *       one copy) gets silent acceptance. The parser refuses both identical-body and
 *       conflicting-body duplicates and quotes both entries' raw text in the error.</li>
 *   <li>#126 (HIGH): cross-stack drift detection. Broker and controller each parse this same
 *       config independently — there is no v1 mechanism for them to exchange declared sets.
 *       {@link #declarationFingerprint} produces an ordering- and whitespace-insensitive
 *       fingerprint that BrokerServer and ControllerServer log at startup; an operator (or
 *       automation) can grep across node logs to confirm a single value cluster-wide and
 *       catch the asymmetry before the first client request fails.</li>
 * </ul>
 */
public final class LogicalTopicConfigParser {

    private LogicalTopicConfigParser() { }

    public static List<LogicalTopicDescriptor> parse(String raw) {
        if (raw == null || raw.isBlank()) {
            return Collections.emptyList();
        }
        String[] entries = raw.split(",");
        List<LogicalTopicDescriptor> descriptors = new ArrayList<>(entries.length);
        // Track logical names already seen so that a duplicate entry — whether identical-body or
        // conflicting-body — is rejected at parse time instead of silently no-op'd by the kernel's
        // idempotent re-declare branch. An operator pasting the same line twice almost always
        // means they intended TWO different declarations and made a typo on one (r19
        // ADV-CONFIG #111). Map value carries the 1-based index + raw entry so the duplicate
        // error can quote both entries.
        Map<String, ParsedEntry> seenByLogicalName = new HashMap<>();
        for (int i = 0; i < entries.length; i++) {
            String entry = entries[i].trim();
            if (entry.isEmpty()) {
                // Trailing comma or "a,,b" — tolerated; matches Kafka's own list-config behaviour.
                continue;
            }
            int index = i + 1;
            LogicalTopicDescriptor descriptor = parseEntry(entry, index);
            ParsedEntry prior = seenByLogicalName.get(descriptor.logicalName());
            if (prior != null) {
                throw new ConfigException(
                    "concentration.logical.topics entry #" + index + " '" + entry
                        + "' is invalid: logical topic '" + descriptor.logicalName()
                        + "' is already declared at entry #" + prior.index + " '" + prior.raw
                        + "'. Each logical topic must appear at most once — duplicate or "
                        + "conflicting entries are rejected so an accidentally-repeated line "
                        + "(with or without typos) cannot silently shadow the operator's intent.");
            }
            seenByLogicalName.put(descriptor.logicalName(), new ParsedEntry(index, entry));
            descriptors.add(descriptor);
        }
        return descriptors;
    }

    /**
     * Parse {@code raw} and declare every descriptor on {@code kernel} in one pass.
     *
     * <p>Why a single helper instead of {@code parse(raw).forEach(kernel::declare)}: the
     * {@link ConcentrationKernel#declare(LogicalTopicDescriptor)} contract raises
     * {@link IllegalStateException} on cross-descriptor invariant violations (shared backing
     * with mismatched M, name re-used across logical/backing namespaces, etc.). The broker
     * startup site previously surfaced those failures as the wrong exception class — not the
     * {@link ConfigException} startup machinery is shaped around — and left the kernel
     * half-populated with whatever descriptors had succeeded before the failing one (r19
     * ADV-CONFIG #109).
     *
     * <p>This helper centralises the right behaviour: every kernel rejection is rewrapped as a
     * {@link ConfigException} that quotes the offending descriptor; the underlying
     * {@link IllegalStateException} is preserved as the cause for tooling that drills in.
     *
     * <p>The half-applied-kernel concern is moot in practice because Kafka startup terminates
     * on the {@link ConfigException} and the kernel object is discarded, but the consolidated
     * call-site is still cleaner: every descriptor either lands or the parser's promises are
     * surfaced as a config failure.
     */
    public static void parseAndDeclare(String raw, ConcentrationKernel kernel) {
        List<LogicalTopicDescriptor> descriptors = parse(raw);
        for (LogicalTopicDescriptor d : descriptors) {
            try {
                kernel.declare(d);
            } catch (IllegalStateException ise) {
                // The descriptor's canonical form uniquely identifies the offending entry; we
                // intentionally do not back-reference the raw entry index because parse() may
                // have collapsed empty entries (trailing commas / double-commas) and the index
                // would no longer match the raw config string.
                ConfigException ce = new ConfigException(
                    "concentration.logical.topics entry '" + renderEntry(d)
                        + "' was rejected by the kernel: " + ise.getMessage()
                        + ". Fix the broker config (the long-term path is a KRaft metadata "
                        + "record; v1 declarations are static and must be self-consistent).");
                ce.initCause(ise);
                throw ce;
            }
        }
    }

    /**
     * Deterministic fingerprint of the parsed declaration set. Stable across (a) entry ordering
     * and (b) whitespace differences, so two nodes with the SAME semantic config produce the
     * SAME fingerprint regardless of how the operator typed it.
     *
     * <p>Why this exists (r19 ADV-CONFIG #126). Broker and controller each parse their own
     * {@code concentration.logical.topics} independently. There is no v1 mechanism for the
     * controller to publish its declared set to brokers (or vice versa) — declarations live in
     * static broker config, not in the KRaft metadata image — so an operator who updates the
     * broker config but forgets the controller (or vice versa) creates a split-brain where
     * {@code CreateTopics("orders")} is rejected on one side but accepted on the other.
     *
     * <p>This fingerprint is the v1 operational workaround: each node logs its fingerprint at
     * startup (see {@code BrokerServer.scala} and {@code ControllerServer.scala}), and an
     * operator (or automation) can grep for "concentration declarations fingerprint" across
     * every node's log and confirm a single value. A mismatch surfaces drift immediately
     * instead of waiting for the first client request to fail confusingly.
     *
     * <p>The hash uses SHA-256 (mandated on every Java platform) over a canonical form: each
     * descriptor rendered as {@code logical:N:backing:M}, descriptors sorted by string order,
     * and joined with {@code ,}. Whitespace and ordering in the input are normalised away by
     * {@link #parse} → render → sort. Truncating to 16 hex chars (8 bytes) keeps the log line
     * short while leaving more than enough collision-resistance for the number of declarations
     * a realistic deployment will ever configure.
     */
    public static String declarationFingerprint(String raw) {
        List<LogicalTopicDescriptor> descriptors = parse(raw);
        if (descriptors.isEmpty()) {
            // Distinct, recognizable fingerprint for the no-declarations case so log scraping
            // can tell "operator forgot to declare" apart from a truncated log line. The string
            // is intentionally not a valid SHA-256 prefix so it is unambiguous.
            return "empty";
        }
        TreeSet<String> canonical = new TreeSet<>();
        for (LogicalTopicDescriptor d : descriptors) {
            canonical.add(renderEntry(d));
        }
        String joined = String.join(",", canonical);
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(joined.getBytes(StandardCharsets.UTF_8));
            byte[] prefix = new byte[8];
            System.arraycopy(digest, 0, prefix, 0, 8);
            return HexFormat.of().formatHex(prefix);
        } catch (NoSuchAlgorithmException nsae) {
            // SHA-256 is mandated on every Java platform (FIPS 180-4). This branch is
            // unreachable; the canonical string is a fall-through that still lets an operator
            // compare values across nodes rather than crashing on a startup path.
            return joined;
        }
    }

    private static String renderEntry(LogicalTopicDescriptor d) {
        return d.logicalName() + ":" + d.numLogicalPartitions()
            + ":" + d.backingTopic() + ":" + d.numBackingPartitions();
    }

    private static LogicalTopicDescriptor parseEntry(String entry, int index) {
        String[] tokens = entry.split(":");
        if (tokens.length != 4) {
            throw new ConfigException(
                "concentration.logical.topics entry #" + index + " '" + entry
                    + "' is invalid: expected logical:N:backing:M (4 colon-separated tokens), got " + tokens.length);
        }
        String logical = tokens[0].trim();
        String backing = tokens[2].trim();
        int n;
        int m;
        try {
            n = Integer.parseInt(tokens[1].trim());
            m = Integer.parseInt(tokens[3].trim());
        } catch (NumberFormatException nfe) {
            throw new ConfigException(
                "concentration.logical.topics entry #" + index + " '" + entry
                    + "' is invalid: partition counts must be integers (" + nfe.getMessage() + ")");
        }
        try {
            return new LogicalTopicDescriptor(logical, n, backing, m);
        } catch (IllegalArgumentException iae) {
            throw new ConfigException(
                "concentration.logical.topics entry #" + index + " '" + entry
                    + "' is invalid: " + iae.getMessage());
        }
    }

    private static final class ParsedEntry {
        final int index;
        final String raw;
        ParsedEntry(int index, String raw) {
            this.index = index;
            this.raw = raw;
        }
    }
}
