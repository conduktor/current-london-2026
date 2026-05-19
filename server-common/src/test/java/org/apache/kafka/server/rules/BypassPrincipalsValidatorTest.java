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
package org.apache.kafka.server.rules;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * R33 #292 [HIGH]: tests that the ConfigDef-level admission test for
 * {@code governance.bypass.principals} accepts the same values
 * {@link RuleEngine#parseBypassPrincipals(String)} accepts and rejects the
 * same values it rejects — closing the gap where a malformed admin-API
 * write would have silently landed in KRaft metadata and only surfaced as
 * a broker-refuses-to-start at the next restart.
 */
final class BypassPrincipalsValidatorTest {

    private static final String CONFIG_NAME = "governance.bypass.principals";

    private final BypassPrincipalsValidator validator = new BypassPrincipalsValidator();

    @Test
    void acceptsNull() {
        // Mirrors parseBypassPrincipals(null) → empty set. The validator is
        // a thin admission test; defensive null-tolerance keeps it
        // equivalent in shape to the parser it gates.
        assertDoesNotThrow(() -> validator.ensureValid(CONFIG_NAME, null));
    }

    @Test
    void acceptsEmptyString() {
        // The empty list is the configured default and the empty-set
        // rejection is a startup-time contract, not an admin-API-time
        // contract. Admins must be able to write the empty default back
        // through the ConfigDef path (clear-and-resubmit flow).
        assertDoesNotThrow(() -> validator.ensureValid(CONFIG_NAME, ""));
    }

    @Test
    void acceptsSingleValidUser() {
        assertDoesNotThrow(() -> validator.ensureValid(CONFIG_NAME, "User:broker"));
    }

    @Test
    void acceptsMultipleValidUsers() {
        assertDoesNotThrow(() ->
            validator.ensureValid(CONFIG_NAME, "User:broker;User:kafka-controller"));
    }

    @Test
    void acceptsSslDnWithSpaces() {
        // X500Principal canonical form has spaces inside DN values.
        // Internal whitespace must remain legal (parser contract pinned
        // by parseBypassPrincipalsAcceptsInternalWhitespaceInSslDn).
        assertDoesNotThrow(() ->
            validator.ensureValid(CONFIG_NAME,
                "User:CN=Broker One,OU=Kafka Brokers,O=Example Corp,C=US"));
    }

    @Test
    void rejectsMissingColon() {
        // The most common operator typo: forgot the `:` separator.
        ConfigException ex = assertThrows(ConfigException.class,
            () -> validator.ensureValid(CONFIG_NAME, "User-broker"));
        // ConfigException's message embeds the config name verbatim;
        // assert on its presence rather than the full string format.
        assertTrue(ex.getMessage().contains(CONFIG_NAME),
            "ConfigException should name the offending config: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("`type:name` format"),
            "Diagnostic should explain expected format: " + ex.getMessage());
    }

    @Test
    void rejectsBlankType() {
        ConfigException ex = assertThrows(ConfigException.class,
            () -> validator.ensureValid(CONFIG_NAME, ":broker"));
        assertTrue(ex.getMessage().contains("principal type"),
            "Diagnostic should name the blank component: " + ex.getMessage());
    }

    @Test
    void rejectsBlankName() {
        ConfigException ex = assertThrows(ConfigException.class,
            () -> validator.ensureValid(CONFIG_NAME, "User:"));
        assertTrue(ex.getMessage().contains("principal name"),
            "Diagnostic should name the blank component: " + ex.getMessage());
    }

    @Test
    void rejectsCommaSeparatorTypo() {
        // R29 #270 contract: comma-as-separator typo is a HIGH because
        // an operator-typed `User:broker,User:kafka-controller` looks
        // correct but soft-bricks half the bypass list. Admission test
        // must reject it.
        ConfigException ex = assertThrows(ConfigException.class,
            () -> validator.ensureValid(CONFIG_NAME, "User:broker,User:kafka-controller"));
        // R42-D (test-quality follow-up): assert ONLY against the helper
        // label substring "comma". Earlier version also allowed
        // `ex.getMessage().contains(",")` as a fallback — but ConfigException
        // formats as `"Invalid value <raw-input> for <name>: <msg>"` and
        // LogSafe.sanitize passes U+002C through unchanged, so the fallback
        // was permanently satisfied by the input echo. A regression that
        // dropped the comma-aware diagnostic (replaced with a generic
        // "invalid principal entry" message) would have passed silently. The
        // tightened assertion pins the operator-facing label directly.
        assertTrue(ex.getMessage().toLowerCase().contains("comma"),
            "Diagnostic should call out the comma typo by name: " + ex.getMessage());
    }

    @Test
    void rejectsInvisibleCodepointInName() {
        // R31 #281 / R33 #290 contract: invisible-format codepoints
        // (U+2060 WORD JOINER here) inside the name silently soft-brick
        // the bypass because runtime peer principals never carry them.
        ConfigException ex = assertThrows(ConfigException.class,
            () -> validator.ensureValid(CONFIG_NAME, "User:bro⁠ker"));
        assertTrue(ex.getMessage().toLowerCase().contains("invisible")
                || ex.getMessage().toLowerCase().contains("codepoint")
                || ex.getMessage().toLowerCase().contains("u+"),
            "Diagnostic should mention the offending codepoint: " + ex.getMessage());
    }

    @Test
    void rejectsLeadingWhitespaceInName() {
        // hasLeadingOrTrailingWhitespace branch on the NAME component —
        // "User: broker" has a leading space inside the name (after the
        // colon, before "broker") that never matches the canonical
        // runtime form (KafkaPrincipal.getName() never carries it).
        // NB: trailing whitespace at the SEGMENT level is stripped by
        // segment.trim() at the top of the parser loop, so the rejection
        // only fires for whitespace INSIDE the type or name parts.
        ConfigException ex = assertThrows(ConfigException.class,
            () -> validator.ensureValid(CONFIG_NAME, "User: broker"));
        assertTrue(ex.getMessage().contains("whitespace"),
            "Diagnostic should mention the whitespace rejection: " + ex.getMessage());
    }

    @Test
    void rejectsTrailingWhitespaceInType() {
        // Companion case for the type branch: "User :broker" has a
        // trailing space on the type component that survives
        // segment.trim() (the trim only strips edge whitespace, not
        // whitespace adjacent to the internal `:`).
        ConfigException ex = assertThrows(ConfigException.class,
            () -> validator.ensureValid(CONFIG_NAME, "User :broker"));
        assertTrue(ex.getMessage().contains("whitespace"),
            "Diagnostic should mention the whitespace rejection: " + ex.getMessage());
    }

    @Test
    void exceptionEmbedsConfigName() {
        ConfigException ex = assertThrows(ConfigException.class,
            () -> validator.ensureValid(CONFIG_NAME, "garbage"));
        // ConfigException's getMessage() format is
        // "Invalid value <value> for configuration <name>: <reason>".
        // Confirm the admin reading the error knows which config to fix.
        assertTrue(ex.getMessage().contains(CONFIG_NAME),
            "Admin diagnostic must name the config: " + ex.getMessage());
    }

    @Test
    void exceptionMessageIsLogSafeSanitised() {
        // Round-20 HIGH A-1 contract: malformed entries that embed CR/LF
        // or other control codepoints must not propagate verbatim into
        // the SLF4J log line. parseBypassPrincipals sanitises via
        // LogSafe.sanitize before throwing; the validator passes the
        // message through unchanged so the sanitisation reaches the
        // ConfigException message verbatim.
        ConfigException ex = assertThrows(ConfigException.class,
            () -> validator.ensureValid(CONFIG_NAME, "User\r:broker"));
        assertTrue(!ex.getMessage().contains("\r"),
            "ConfigException message must not embed raw CR: <" + ex.getMessage() + ">");
        assertTrue(!ex.getMessage().contains("\n"),
            "ConfigException message must not embed raw LF: <" + ex.getMessage() + ">");
    }

    @Test
    void toStringDescribesFormat() {
        // ConfigDef.toRst / toEnrichedRst surface the validator's
        // toString() in generated documentation. Pin a non-empty
        // descriptive string so generated docs include the constraint.
        String s = validator.toString();
        assertTrue(s.contains("type:name"),
            "toString should describe the type:name format: " + s);
        assertTrue(s.contains("semicolon"),
            "toString should describe the separator: " + s);
    }

    @Test
    void integrationAcceptsKnownGoodAndRejectsKnownBad() {
        // Cross-check that the validator's accept/reject set matches
        // parseBypassPrincipals's accept/reject set on a small matrix —
        // protects against accidental drift where the validator and the
        // startup parser diverge on a future hardening.
        String[] good = {
            "",
            "User:broker",
            "User:broker;User:kafka-controller",
            "User:CN=Broker One,OU=Kafka Brokers,O=Example Corp,C=US",
        };
        for (String v : good) {
            assertDoesNotThrow(() -> validator.ensureValid(CONFIG_NAME, v),
                "validator should accept: <" + v + ">");
            // Parser must also accept (sanity).
            assertDoesNotThrow(() -> RuleEngine.parseBypassPrincipals(v),
                "parser should accept: <" + v + ">");
        }

        String[] bad = {
            "garbage",
            "User",
            ":broker",
            "User:",
            "User:broker,User:kafka-controller",
            "User:bro⁠ker",
            "User: broker",   // leading-whitespace-in-name branch
            "User :broker",   // trailing-whitespace-in-type branch
        };
        for (String v : bad) {
            assertThrows(ConfigException.class,
                () -> validator.ensureValid(CONFIG_NAME, v),
                "validator should reject: <" + v + ">");
            // Parser must also reject (sanity).
            assertThrows(IllegalArgumentException.class,
                () -> RuleEngine.parseBypassPrincipals(v),
                "parser should reject: <" + v + ">");
        }
        assertEquals(8, bad.length);
    }
}
