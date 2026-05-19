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

import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FetchRequestParserTest {

    @Test
    void parsesExplicitPartitionAndOffset() {
        FetchRequestParser.FetchCommand cmd = FetchRequestParser.parse(
            "orders", QueryParams.of("partition", "3", "offset", "42"));

        assertEquals("orders", cmd.topic());
        assertEquals(3, cmd.partition());
        assertEquals(42L, cmd.offset());
        assertEquals(OptionalInt.empty(), cmd.maxBytes());
    }

    @Test
    void parsesOptionalMaxBytes() {
        FetchRequestParser.FetchCommand cmd = FetchRequestParser.parse(
            "orders", QueryParams.of("partition", "0", "offset", "0", "max_bytes", "1048576"));

        assertEquals(1048576, cmd.maxBytes().getAsInt());
    }

    @Test
    void parsesCursorAsAlternativeToPartitionAndOffset() {
        // A client following the "next" link sends only the cursor — no partition / offset duplication.
        String cursor = CursorCodec.encode("orders", 2, 500);
        FetchRequestParser.FetchCommand cmd = FetchRequestParser.parse(
            "orders", QueryParams.of("cursor", cursor));

        assertEquals(2, cmd.partition());
        assertEquals(500L, cmd.offset());
    }

    @Test
    void cursorAndExplicitParamsAreMutuallyExclusive() {
        String cursor = CursorCodec.encode("orders", 0, 0);
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> FetchRequestParser.parse("orders", QueryParams.of(
                "cursor", cursor, "partition", "0", "offset", "0")));
    }

    @Test
    void cursorMustReferenceTheRequestedTopic() {
        // Prevents accidentally following a cursor minted against a different topic.
        String wrongTopic = CursorCodec.encode("orders", 0, 0);
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> FetchRequestParser.parse("payments", QueryParams.of("cursor", wrongTopic)));
    }

    @Test
    void invalidCursorIsRejected() {
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> FetchRequestParser.parse("orders", QueryParams.of("cursor", "!!!")));
    }

    @Test
    void missingPartitionAndCursorIsRejected() {
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> FetchRequestParser.parse("orders", QueryParams.of("offset", "0")));
    }

    @Test
    void missingOffsetIsRejected() {
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> FetchRequestParser.parse("orders", QueryParams.of("partition", "0")));
    }

    @Test
    void negativePartitionIsRejected() {
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> FetchRequestParser.parse("orders", QueryParams.of("partition", "-1", "offset", "0")));
    }

    @Test
    void negativeOffsetIsRejected() {
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> FetchRequestParser.parse("orders", QueryParams.of("partition", "0", "offset", "-1")));
    }

    @Test
    void nonNumericPartitionIsRejected() {
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> FetchRequestParser.parse("orders", QueryParams.of("partition", "lol", "offset", "0")));
    }

    @Test
    void nonNumericOffsetIsRejected() {
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> FetchRequestParser.parse("orders", QueryParams.of("partition", "0", "offset", "wat")));
    }

    @Test
    void zeroOrNegativeMaxBytesIsRejected() {
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> FetchRequestParser.parse("orders",
                QueryParams.of("partition", "0", "offset", "0", "max_bytes", "0")));
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> FetchRequestParser.parse("orders",
                QueryParams.of("partition", "0", "offset", "0", "max_bytes", "-1")));
    }

    @Test
    void maxBytesAboveTheWsCapIsRejected() {
        // Wave 30 axis UUU: the WebSocket subscribe path enforces MAX_PER_PARTITION_FETCH_BYTES (50 MiB).
        // The HTTP fetch path is a parallel wire format over the same broker fetch primitive — letting
        // it accept Integer.MAX_VALUE while the WS path rejects > 50 MiB is a request-amplification
        // asymmetry. Pin the symmetric contract: any max_bytes above the WS cap is a 400.
        int overCap = WsSubscribeMessageParser.MAX_PER_PARTITION_FETCH_BYTES + 1;
        ProduceRequestParser.BadRequestException ex = assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> FetchRequestParser.parse("orders",
                QueryParams.of("partition", "0", "offset", "0", "max_bytes", Integer.toString(overCap))));
        assertTrue(ex.getMessage().contains("per-fetch cap"),
            "rejection message must name the cap so clients can correct the call, got: " + ex.getMessage());
        // Boundary: exactly the cap is accepted — the WS parser uses the same inclusive-cap shape.
        FetchRequestParser.FetchCommand atCap = FetchRequestParser.parse("orders",
            QueryParams.of("partition", "0", "offset", "0", "max_bytes",
                Integer.toString(WsSubscribeMessageParser.MAX_PER_PARTITION_FETCH_BYTES)));
        assertEquals(WsSubscribeMessageParser.MAX_PER_PARTITION_FETCH_BYTES, atCap.maxBytes().getAsInt(),
            "max_bytes exactly at the cap must be accepted — clients that ask for the documented maximum "
                + "should not see a 400");
        // Integer.MAX_VALUE is the canonical adversarial value (and the exact concern that motivated the cap):
        // without the guard the HTTP path would silently accept it and pass it down to the broker.
        assertThrows(ProduceRequestParser.BadRequestException.class,
            () -> FetchRequestParser.parse("orders",
                QueryParams.of("partition", "0", "offset", "0", "max_bytes",
                    Integer.toString(Integer.MAX_VALUE))));
    }

    @Test
    void topicMustNotBeBlank() {
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> FetchRequestParser.parse("   ", QueryParams.of("partition", "0", "offset", "0")));
    }

    @Test
    void parsesFromEarliestAsOffsetZero() {
        FetchRequestParser.FetchCommand cmd = FetchRequestParser.parse(
            "orders", QueryParams.of("partition", "1", "from", "earliest"));

        assertEquals(1, cmd.partition());
        assertEquals(0L, cmd.offset());
        assertTrue(cmd.fromEarliest(),
            "fromEarliest must surface to the submitter so it can retry at logStartOffset on a truncated topic "
                + "(otherwise from=earliest on a retained partition returns OFFSET_OUT_OF_RANGE)");
    }

    @Test
    void parsesExplicitOffsetWithoutFromEarliestFlag() {
        FetchRequestParser.FetchCommand cmd = FetchRequestParser.parse(
            "orders", QueryParams.of("partition", "1", "offset", "0"));

        assertEquals(0L, cmd.offset());
        assertFalse(cmd.fromEarliest(),
            "explicit offset=0 is a deliberate choice — must not trigger the earliest-retry path");
    }

    @Test
    void parsesCursorWithoutFromEarliestFlag() {
        String cursor = CursorCodec.encode("orders", 0, 0);
        FetchRequestParser.FetchCommand cmd = FetchRequestParser.parse(
            "orders", QueryParams.of("cursor", cursor));

        assertFalse(cmd.fromEarliest(),
            "cursor offsets are explicit; the streamer's follow-up fetches must not chase OOR back to logStartOffset");
    }

    @Test
    void fromAndOffsetAreMutuallyExclusive() {
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> FetchRequestParser.parse("orders",
                QueryParams.of("partition", "0", "from", "earliest", "offset", "5")));
    }

    @Test
    void fromOnlyAcceptsEarliestForNow() {
        // from=latest would require a broker round-trip to discover the high watermark before the first fetch; the
        // v1 single-shot fetch path doesn't carry that machinery. Reject explicitly so it isn't silently treated as
        // a typo for "earliest" or the default.
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> FetchRequestParser.parse("orders", QueryParams.of("partition", "0", "from", "latest")));
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> FetchRequestParser.parse("orders", QueryParams.of("partition", "0", "from", "gibberish")));
    }

    @Test
    void cursorAndFromAreMutuallyExclusive() {
        String cursor = CursorCodec.encode("orders", 0, 0);
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> FetchRequestParser.parse("orders",
                QueryParams.of("cursor", cursor, "from", "earliest")));
    }

    @Test
    void blankParamsBehaveAsMissing() {
        // Empty string is what some clients send when an input is absent — treat as "not present".
        assertFalse(QueryParams.of("partition", "").get("partition").isPresent());
        assertTrue(QueryParams.of("partition", " 3 ").get("partition").isPresent());
        assertEquals("3", QueryParams.of("partition", " 3 ").get("partition").get());
    }
}
