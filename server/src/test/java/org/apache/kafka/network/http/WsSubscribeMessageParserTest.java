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
package org.apache.kafka.network.http;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WsSubscribeMessageParserTest {

    // ----- subscribe happy paths -----

    @Test
    void parsesSubscribeWithRequiredFields() {
        WsSubscribeMessageParser.WsClientMessage msg = WsSubscribeMessageParser.parse(
            "{\"type\":\"subscribe\",\"partition\":0,\"offset\":0,\"initialCredits\":5}");

        WsSubscribeMessageParser.WsSubscribeCommand sub =
            assertInstanceOf(WsSubscribeMessageParser.WsSubscribeCommand.class, msg);
        assertEquals(0, sub.partition());
        assertEquals(0L, sub.offset());
        assertEquals(5, sub.initialCredits());
        assertTrue(sub.maxBytes().isEmpty());
    }

    @Test
    void parsesSubscribeWithOptionalMaxBytes() {
        WsSubscribeMessageParser.WsClientMessage msg = WsSubscribeMessageParser.parse(
            "{\"type\":\"subscribe\",\"partition\":3,\"offset\":100,\"initialCredits\":10,\"maxBytes\":65536}");

        WsSubscribeMessageParser.WsSubscribeCommand sub =
            assertInstanceOf(WsSubscribeMessageParser.WsSubscribeCommand.class, msg);
        assertEquals(3, sub.partition());
        assertEquals(100L, sub.offset());
        assertEquals(10, sub.initialCredits());
        assertTrue(sub.maxBytes().isPresent());
        assertEquals(65536, sub.maxBytes().getAsInt());
    }

    @Test
    void parsesSubscribeWithZeroInitialCredits() {
        // AC5 explicitly says: "Zero credits → no delivery". Zero is a valid grant.
        WsSubscribeMessageParser.WsClientMessage msg = WsSubscribeMessageParser.parse(
            "{\"type\":\"subscribe\",\"partition\":0,\"offset\":0,\"initialCredits\":0}");

        WsSubscribeMessageParser.WsSubscribeCommand sub =
            assertInstanceOf(WsSubscribeMessageParser.WsSubscribeCommand.class, msg);
        assertEquals(0, sub.initialCredits());
    }

    // ----- flow happy paths -----

    @Test
    void parsesFlowWithCredits() {
        WsSubscribeMessageParser.WsClientMessage msg = WsSubscribeMessageParser.parse(
            "{\"type\":\"flow\",\"credits\":10}");

        WsSubscribeMessageParser.WsFlowCommand flow =
            assertInstanceOf(WsSubscribeMessageParser.WsFlowCommand.class, msg);
        assertEquals(10, flow.credits());
    }

    @Test
    void parsesFlowAtTheCap() {
        // The per-message flow cap is MAX_FLOW_CREDITS (see WsSubscribeMessageParser javadoc).
        // Exactly-at-cap must succeed — the rejection is for values strictly above.
        WsSubscribeMessageParser.WsClientMessage msg = WsSubscribeMessageParser.parse(
            "{\"type\":\"flow\",\"credits\":" + WsSubscribeMessageParser.MAX_FLOW_CREDITS + "}");

        WsSubscribeMessageParser.WsFlowCommand flow =
            assertInstanceOf(WsSubscribeMessageParser.WsFlowCommand.class, msg);
        assertEquals(WsSubscribeMessageParser.MAX_FLOW_CREDITS, flow.credits());
    }

    // ----- malformed JSON / structural -----

    @Test
    void rejectsNotJson() {
        WsSubscribeMessageParser.BadMessageException ex = assertThrows(
            WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse("not json at all"));
        assertTrue(ex.getMessage().toLowerCase(java.util.Locale.ROOT).contains("json"),
            "expected message to mention JSON, was: " + ex.getMessage());
    }

    @Test
    void rejectsEmptyMessage() {
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(""));
    }

    @Test
    void rejectsNonObjectRoot() {
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse("[1,2,3]"));
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse("\"a string\""));
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse("42"));
    }

    // ----- type discriminator -----

    @Test
    void rejectsMissingType() {
        WsSubscribeMessageParser.BadMessageException ex = assertThrows(
            WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(
                "{\"partition\":0,\"offset\":0,\"initialCredits\":5}"));
        assertTrue(ex.getMessage().toLowerCase(java.util.Locale.ROOT).contains("type"),
            "expected message to mention 'type' discriminator, was: " + ex.getMessage());
    }

    @Test
    void rejectsUnknownType() {
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(
                "{\"type\":\"close\",\"partition\":0,\"offset\":0,\"initialCredits\":5}"));
    }

    @Test
    void unknownTypeMessageStripsControlCharsAndTruncates() {
        // The unknown-type message echoes the attacker-controlled 'type' value into a string that
        // reaches BOTH the broker log (via SLF4J at the endpoint's debug line) and the WebSocket
        // close-frame errorMessage envelope. Without sanitisation, CR/LF would let a hostile client
        // forge log lines on aggregators that parse by line, and unbounded length would bloat the
        // close frame. The sanitiser substitutes '?' for C0 controls + DEL and truncates above 32
        // characters with a trailing '...' marker; readable Unicode passes through.
        WsSubscribeMessageParser.BadMessageException ex = assertThrows(
            WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(
                "{\"type\":\"foo\\r\\nfake-log-line\",\"partition\":0,\"offset\":0,\"initialCredits\":5}"));
        // The exact CR / LF / NUL substring must not survive into the message; '?' takes its place.
        assertFalse(ex.getMessage().contains("\r"), () -> "raw CR leaked: " + ex.getMessage());
        assertFalse(ex.getMessage().contains("\n"), () -> "raw LF leaked: " + ex.getMessage());
        // Sanity: the literal 'foo' prefix and the '?' substitutes for \r\n still appear so the
        // diagnostic stays useful.
        assertTrue(ex.getMessage().contains("foo??fake-log-line"),
            () -> "expected sanitised preview, was: " + ex.getMessage());

        // Truncation: a 40-character type field must be capped at 32 chars + "..." in the message.
        String longType = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN"; // 40 chars
        WsSubscribeMessageParser.BadMessageException truncated = assertThrows(
            WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(
                "{\"type\":\"" + longType + "\",\"partition\":0,\"offset\":0,\"initialCredits\":5}"));
        assertTrue(truncated.getMessage().contains(longType.substring(0, 32) + "..."),
            () -> "expected 32-char + '...' truncation, was: " + truncated.getMessage());
        assertFalse(truncated.getMessage().contains(longType),
            () -> "full type leaked through: " + truncated.getMessage());

        // Unicode (here: Japanese for 'unknown') passes through unchanged — only C0 / DEL are stripped.
        WsSubscribeMessageParser.BadMessageException unicode = assertThrows(
            WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(
                "{\"type\":\"\\u8cfc\\u8aad\",\"partition\":0,\"offset\":0,\"initialCredits\":5}"));
        assertTrue(unicode.getMessage().contains("購読"),
            () -> "Unicode preview was stripped, was: " + unicode.getMessage());

        // DEL (0x7F) is C1-adjacent and is the canonical "non-printable" character not caught by
        // the C0 (< 0x20) check. Pin it as substituted too — paranoid log-aggregator parsers can
        // be confused by stray DEL bytes.
        WsSubscribeMessageParser.BadMessageException withDel = assertThrows(
            WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(
                "{\"type\":\"foo\\u007fbar\",\"partition\":0,\"offset\":0,\"initialCredits\":5}"));
        assertFalse(withDel.getMessage().contains(""),
            () -> "DEL leaked: " + withDel.getMessage());
        assertTrue(withDel.getMessage().contains("foo?bar"),
            () -> "expected '?' substitute for DEL, was: " + withDel.getMessage());
    }

    @Test
    void rejectsNonStringType() {
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(
                "{\"type\":42,\"partition\":0,\"offset\":0,\"initialCredits\":5}"));
    }

    // ----- subscribe validation -----

    @Test
    void rejectsSubscribeMissingPartition() {
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(
                "{\"type\":\"subscribe\",\"offset\":0,\"initialCredits\":5}"));
    }

    @Test
    void rejectsSubscribeMissingOffset() {
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(
                "{\"type\":\"subscribe\",\"partition\":0,\"initialCredits\":5}"));
    }

    @Test
    void rejectsSubscribeMissingInitialCredits() {
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(
                "{\"type\":\"subscribe\",\"partition\":0,\"offset\":0}"));
    }

    @Test
    void rejectsNegativePartition() {
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(
                "{\"type\":\"subscribe\",\"partition\":-1,\"offset\":0,\"initialCredits\":5}"));
    }

    @Test
    void rejectsNegativeOffset() {
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(
                "{\"type\":\"subscribe\",\"partition\":0,\"offset\":-1,\"initialCredits\":5}"));
    }

    @Test
    void rejectsNegativeInitialCredits() {
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(
                "{\"type\":\"subscribe\",\"partition\":0,\"offset\":0,\"initialCredits\":-1}"));
    }

    @Test
    void rejectsNonIntegerPartition() {
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(
                "{\"type\":\"subscribe\",\"partition\":\"zero\",\"offset\":0,\"initialCredits\":5}"));
    }

    @Test
    void rejectsNonIntegerOffset() {
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(
                "{\"type\":\"subscribe\",\"partition\":0,\"offset\":\"start\",\"initialCredits\":5}"));
    }

    @Test
    void rejectsNonIntegerInitialCredits() {
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(
                "{\"type\":\"subscribe\",\"partition\":0,\"offset\":0,\"initialCredits\":\"five\"}"));
    }

    @Test
    void rejectsZeroOrNegativeMaxBytes() {
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(
                "{\"type\":\"subscribe\",\"partition\":0,\"offset\":0,\"initialCredits\":5,\"maxBytes\":0}"));
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(
                "{\"type\":\"subscribe\",\"partition\":0,\"offset\":0,\"initialCredits\":5,\"maxBytes\":-1}"));
    }

    // ----- flow validation -----

    @Test
    void rejectsFlowMissingCredits() {
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse("{\"type\":\"flow\"}"));
    }

    @Test
    void rejectsFlowNegativeCredits() {
        // The protocol cannot retract a previously granted credit. Negative credits
        // are a client bug and must be flagged, not silently treated as zero.
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse("{\"type\":\"flow\",\"credits\":-1}"));
    }

    @Test
    void rejectsFlowZeroCredits() {
        // A flow message granting zero is meaningless. Surface it as a client bug
        // rather than silently no-op.
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse("{\"type\":\"flow\",\"credits\":0}"));
    }

    @Test
    void rejectsFlowNonIntegerCredits() {
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse("{\"type\":\"flow\",\"credits\":\"more\"}"));
    }

    @Test
    void rejectsFractionalCreditsInFlow() {
        // Jackson's canConvertToInt accepts 1.9 and asInt() silently truncates to 1. For a wire
        // protocol where integers carry semantic weight, fractional inputs are a client bug —
        // reject loudly rather than silently round.
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse("{\"type\":\"flow\",\"credits\":1.9}"));
    }

    @Test
    void rejectsFractionalInitialCreditsInSubscribe() {
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(
                "{\"type\":\"subscribe\",\"partition\":0,\"offset\":0,\"initialCredits\":2.5}"));
    }

    @Test
    void rejectsFractionalOffsetInSubscribe() {
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(
                "{\"type\":\"subscribe\",\"partition\":0,\"offset\":1.5,\"initialCredits\":5}"));
    }

    @Test
    void rejectsFractionalMaxBytesInSubscribe() {
        assertThrows(WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(
                "{\"type\":\"subscribe\",\"partition\":0,\"offset\":0,\"initialCredits\":5,\"maxBytes\":1024.5}"));
    }

    // ----- amplification caps (Wave 24) -----

    @Test
    void subscribeRejectsInitialCreditsExceedingCap() {
        // Without this cap, one subscribe frame with initialCredits = 2_000_000_000 drives the broker
        // into a tight loop of MAX_PER_PARTITION_FETCH_BYTES-sized fetches until the partition drains.
        // The cap converts the 1:50M byte amplifier into a clean BadMessageException → 1003 close.
        int overCap = WsSubscribeMessageParser.MAX_INITIAL_CREDITS + 1;
        WsSubscribeMessageParser.BadMessageException ex = assertThrows(
            WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(
                "{\"type\":\"subscribe\",\"partition\":0,\"offset\":0,\"initialCredits\":" + overCap + "}"));
        assertTrue(ex.getMessage().contains("initialCredits"),
            () -> "expected message to name the offending field, was: " + ex.getMessage());
    }

    @Test
    void subscribeAcceptsInitialCreditsAtCap() {
        // Exactly-at-cap must succeed — the rejection is for values strictly above.
        WsSubscribeMessageParser.WsClientMessage msg = WsSubscribeMessageParser.parse(
            "{\"type\":\"subscribe\",\"partition\":0,\"offset\":0,\"initialCredits\":"
                + WsSubscribeMessageParser.MAX_INITIAL_CREDITS + "}");
        WsSubscribeMessageParser.WsSubscribeCommand sub =
            assertInstanceOf(WsSubscribeMessageParser.WsSubscribeCommand.class, msg);
        assertEquals(WsSubscribeMessageParser.MAX_INITIAL_CREDITS, sub.initialCredits());
    }

    @Test
    void subscribeRejectsMaxBytesExceedingCap() {
        // Aligned with the broker's fetchResponseMaxBytes — asking for more is asking the bridge to
        // act as a fetch-amplifier, since the broker still caps the response at 50 MiB.
        long overCap = (long) WsSubscribeMessageParser.MAX_PER_PARTITION_FETCH_BYTES + 1L;
        WsSubscribeMessageParser.BadMessageException ex = assertThrows(
            WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(
                "{\"type\":\"subscribe\",\"partition\":0,\"offset\":0,\"initialCredits\":5,\"maxBytes\":"
                    + overCap + "}"));
        assertTrue(ex.getMessage().contains("maxBytes"),
            () -> "expected message to name the offending field, was: " + ex.getMessage());
    }

    @Test
    void subscribeAcceptsMaxBytesAtCap() {
        WsSubscribeMessageParser.WsClientMessage msg = WsSubscribeMessageParser.parse(
            "{\"type\":\"subscribe\",\"partition\":0,\"offset\":0,\"initialCredits\":5,\"maxBytes\":"
                + WsSubscribeMessageParser.MAX_PER_PARTITION_FETCH_BYTES + "}");
        WsSubscribeMessageParser.WsSubscribeCommand sub =
            assertInstanceOf(WsSubscribeMessageParser.WsSubscribeCommand.class, msg);
        assertTrue(sub.maxBytes().isPresent());
        assertEquals(WsSubscribeMessageParser.MAX_PER_PARTITION_FETCH_BYTES, sub.maxBytes().getAsInt());
    }

    @Test
    void flowRejectsCreditsExceedingCap() {
        int overCap = WsSubscribeMessageParser.MAX_FLOW_CREDITS + 1;
        WsSubscribeMessageParser.BadMessageException ex = assertThrows(
            WsSubscribeMessageParser.BadMessageException.class,
            () -> WsSubscribeMessageParser.parse(
                "{\"type\":\"flow\",\"credits\":" + overCap + "}"));
        assertTrue(ex.getMessage().contains("credits"),
            () -> "expected message to name the offending field, was: " + ex.getMessage());
    }
}
