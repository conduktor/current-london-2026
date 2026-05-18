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
    void parsesFlowWithLargeCredits() {
        WsSubscribeMessageParser.WsClientMessage msg = WsSubscribeMessageParser.parse(
            "{\"type\":\"flow\",\"credits\":1000000}");

        WsSubscribeMessageParser.WsFlowCommand flow =
            assertInstanceOf(WsSubscribeMessageParser.WsFlowCommand.class, msg);
        assertEquals(1_000_000, flow.credits());
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
}
