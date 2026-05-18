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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProduceRequestParserTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private JsonNode body(String json) {
        try {
            return mapper.readTree(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ----- happy paths -----

    @Test
    void parsesSingleStringRecordWithExplicitPartition() {
        JsonNode body = body(
            "{\"records\":[{\"partition\":0,\"value\":{\"type\":\"STRING\",\"data\":\"hello\"}}]}");

        ProduceRequestParser.ProduceCommand cmd = ProduceRequestParser.parse("orders", body);

        assertEquals("orders", cmd.topic());
        assertEquals(1, cmd.records().size());
        ProduceRequestParser.RecordEntry entry = cmd.records().get(0);
        assertTrue(entry.partition().isPresent());
        assertEquals(0, entry.partition().getAsInt());
        assertNull(entry.key());
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), entry.value());
    }

    @Test
    void parsesMultipleRecordsAcrossDifferentPartitions() {
        // Spec scenario: a single POST fanning records across partitions 0, 1, 2.
        JsonNode body = body(
            "{\"records\":["
            + "{\"partition\":0,\"value\":{\"type\":\"STRING\",\"data\":\"a\"}},"
            + "{\"partition\":1,\"value\":{\"type\":\"STRING\",\"data\":\"b\"}},"
            + "{\"partition\":2,\"value\":{\"type\":\"STRING\",\"data\":\"c\"}}]}");

        ProduceRequestParser.ProduceCommand cmd = ProduceRequestParser.parse("orders", body);

        assertEquals(3, cmd.records().size());
        assertEquals(0, cmd.records().get(0).partition().getAsInt());
        assertEquals(1, cmd.records().get(1).partition().getAsInt());
        assertEquals(2, cmd.records().get(2).partition().getAsInt());
    }

    @Test
    void parsesNullValueAsNullBytes() {
        JsonNode body = body(
            "{\"records\":[{\"value\":{\"type\":\"NULL\",\"data\":null}}]}");

        ProduceRequestParser.ProduceCommand cmd = ProduceRequestParser.parse("orders", body);

        assertNull(cmd.records().get(0).value());
    }

    @Test
    void parsesBinaryRecordViaBase64() {
        // "Zm9v" is "foo" base64-encoded
        JsonNode body = body(
            "{\"records\":[{\"value\":{\"type\":\"BINARY\",\"data\":\"Zm9v\"}}]}");

        ProduceRequestParser.ProduceCommand cmd = ProduceRequestParser.parse("orders", body);

        assertArrayEquals(new byte[] {'f', 'o', 'o'}, cmd.records().get(0).value());
    }

    @Test
    void parsesJsonValueAndPropagatesContentType() {
        JsonNode body = body(
            "{\"records\":[{\"value\":{\"type\":\"JSON\",\"data\":{\"id\":42}}}]}");

        ProduceRequestParser.ProduceCommand cmd = ProduceRequestParser.parse("orders", body);
        ProduceRequestParser.RecordEntry entry = cmd.records().get(0);

        assertEquals("application/json", entry.contentType());
        // value bytes are canonical JSON of {"id":42}
        assertEquals("{\"id\":42}", new String(entry.value(), StandardCharsets.UTF_8));
    }

    @Test
    void missingPartitionMeansBrokerDecides() {
        JsonNode body = body(
            "{\"records\":[{\"value\":{\"type\":\"STRING\",\"data\":\"x\"}}]}");

        ProduceRequestParser.ProduceCommand cmd = ProduceRequestParser.parse("orders", body);

        assertEquals(OptionalInt.empty(), cmd.records().get(0).partition());
    }

    @Test
    void parsesKeyWhenPresent() {
        JsonNode body = body(
            "{\"records\":[{\"key\":{\"type\":\"STRING\",\"data\":\"k\"},"
            + "\"value\":{\"type\":\"STRING\",\"data\":\"v\"}}]}");

        ProduceRequestParser.ProduceCommand cmd = ProduceRequestParser.parse("orders", body);
        ProduceRequestParser.RecordEntry entry = cmd.records().get(0);

        assertArrayEquals("k".getBytes(StandardCharsets.UTF_8), entry.key());
        assertArrayEquals("v".getBytes(StandardCharsets.UTF_8), entry.value());
    }

    @Test
    void keyAbsentMeansNullKey() {
        JsonNode body = body(
            "{\"records\":[{\"value\":{\"type\":\"STRING\",\"data\":\"v\"}}]}");

        ProduceRequestParser.ProduceCommand cmd = ProduceRequestParser.parse("orders", body);

        assertNull(cmd.records().get(0).key());
    }

    // ----- failure paths -----

    @Test
    void bodyMustBeAnObject() {
        JsonNode body = body("[]");
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> ProduceRequestParser.parse("orders", body));
    }

    @Test
    void missingRecordsFieldIsRejected() {
        JsonNode body = body("{}");
        ProduceRequestParser.BadRequestException ex = assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> ProduceRequestParser.parse("orders", body));
        assertTrue(ex.getMessage().toLowerCase(Locale.ROOT).contains("records"));
    }

    @Test
    void recordsFieldMustBeArray() {
        JsonNode body = body("{\"records\":{}}");
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> ProduceRequestParser.parse("orders", body));
    }

    @Test
    void emptyRecordsArrayIsRejected() {
        // No useful semantics for "produce nothing"; clients sending [] are buggy.
        JsonNode body = body("{\"records\":[]}");
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> ProduceRequestParser.parse("orders", body));
    }

    @Test
    void recordEntryMustBeObject() {
        JsonNode body = body("{\"records\":[\"oops\"]}");
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> ProduceRequestParser.parse("orders", body));
    }

    @Test
    void partitionMustBeIntegerWhenPresent() {
        JsonNode body = body(
            "{\"records\":[{\"partition\":\"zero\",\"value\":{\"type\":\"STRING\",\"data\":\"x\"}}]}");
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> ProduceRequestParser.parse("orders", body));
    }

    @Test
    void negativePartitionIsRejected() {
        // Negative partition would mean "any" in some Kafka APIs but our HTTP surface uses "omit field" for that.
        JsonNode body = body(
            "{\"records\":[{\"partition\":-1,\"value\":{\"type\":\"STRING\",\"data\":\"x\"}}]}");
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> ProduceRequestParser.parse("orders", body));
    }

    @Test
    void valueFieldIsRequired() {
        JsonNode body = body("{\"records\":[{\"partition\":0}]}");
        ProduceRequestParser.BadRequestException ex = assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> ProduceRequestParser.parse("orders", body));
        assertTrue(ex.getMessage().toLowerCase(Locale.ROOT).contains("value"));
    }

    @Test
    void malformedValueEnvelopeBubblesAsBadRequest() {
        // Underlying ValueSerializer throws IllegalArgumentException for unknown type — bridge translates to 400.
        JsonNode body = body(
            "{\"records\":[{\"value\":{\"type\":\"WHO_KNOWS\",\"data\":\"x\"}}]}");
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> ProduceRequestParser.parse("orders", body));
    }

    @Test
    void topicMustNotBeNullOrBlank() {
        JsonNode body = body("{\"records\":[{\"value\":{\"type\":\"STRING\",\"data\":\"x\"}}]}");
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> ProduceRequestParser.parse("", body));
        assertThrows(
            ProduceRequestParser.BadRequestException.class,
            () -> ProduceRequestParser.parse("   ", body));
    }

    @Test
    void contentTypeAbsentForNonJsonValue() {
        JsonNode body = body(
            "{\"records\":[{\"value\":{\"type\":\"STRING\",\"data\":\"x\"}}]}");

        ProduceRequestParser.ProduceCommand cmd = ProduceRequestParser.parse("orders", body);

        assertNull(cmd.records().get(0).contentType());
    }

    @Test
    void parserDoesNotMutateInputBody() {
        // Smoke test: parsing should be read-only; we depend on this when logging the original body on errors.
        JsonNode body = body(
            "{\"records\":[{\"value\":{\"type\":\"STRING\",\"data\":\"hi\"}}]}");
        String before = body.toString();

        ProduceRequestParser.parse("orders", body);

        assertEquals(before, body.toString());
    }

    @Test
    void parserToleratesLargeNumberOfRecords() {
        // Sanity check that the loop has no O(n^2) accidents for typical batch sizes.
        StringBuilder sb = new StringBuilder("{\"records\":[");
        for (int i = 0; i < 200; i++) {
            if (i > 0) sb.append(',');
            sb.append("{\"partition\":").append(i % 4)
                .append(",\"value\":{\"type\":\"STRING\",\"data\":\"r").append(i).append("\"}}");
        }
        sb.append("]}");
        JsonNode body = body(sb.toString());

        ProduceRequestParser.ProduceCommand cmd = ProduceRequestParser.parse("orders", body);

        assertEquals(200, cmd.records().size());
        assertFalse(cmd.records().get(199).partition().isEmpty());
    }
}
