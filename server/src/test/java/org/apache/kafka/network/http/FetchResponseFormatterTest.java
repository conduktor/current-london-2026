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

import org.apache.kafka.common.protocol.Errors;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FetchResponseFormatterTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final FetchResponseFormatter formatter = new FetchResponseFormatter(mapper);

    private static FetchResponseFormatter.FetchedRecord rec(long offset, String value) {
        return new FetchResponseFormatter.FetchedRecord(
            offset, null, value.getBytes(StandardCharsets.UTF_8), null, 1000L + offset);
    }

    private static FetchResponseFormatter.PartitionFetch ok(int partition, long requestedOffset,
            long logStartOffset, long highWatermark, List<FetchResponseFormatter.FetchedRecord> records) {
        return new FetchResponseFormatter.PartitionFetch(
            partition, Errors.NONE, null, requestedOffset, logStartOffset, highWatermark, records);
    }

    private static FetchResponseFormatter.PartitionFetch err(int partition, Errors error) {
        return new FetchResponseFormatter.PartitionFetch(
            partition, error, null, 0L, 0L, 0L, Collections.emptyList());
    }

    // ----- status -----

    @Test
    void successReturns200() {
        List<FetchResponseFormatter.FetchedRecord> records = new ArrayList<>();
        records.add(rec(100, "first"));
        records.add(rec(101, "second"));

        FetchResponseFormatter.PartitionFetch p = ok(0, 100, 0, 200, records);
        HttpBridgeResponse f = formatter.format("orders", p, 0);

        assertEquals(200, f.status());
    }

    @Test
    void emptyRecordSetIsStill200() {
        // A fetch that lands at the high watermark legitimately returns zero records — still 200, not 404.
        FetchResponseFormatter.PartitionFetch p = ok(0, 200, 0, 200, Collections.emptyList());
        HttpBridgeResponse f = formatter.format("orders", p, 0);

        assertEquals(200, f.status());
        assertEquals(0, f.body().get("partitions").get(0).get("records").size());
    }

    @Test
    void authFailureReturns403() {
        HttpBridgeResponse f = formatter.format("orders", err(0, Errors.TOPIC_AUTHORIZATION_FAILED), 0);
        assertEquals(403, f.status());
    }

    @Test
    void unknownTopicReturns404() {
        HttpBridgeResponse f = formatter.format("orders", err(0, Errors.UNKNOWN_TOPIC_OR_PARTITION), 0);
        assertEquals(404, f.status());
    }

    @Test
    void notLeaderReturns503() {
        HttpBridgeResponse f = formatter.format("orders", err(0, Errors.NOT_LEADER_OR_FOLLOWER), 0);
        assertEquals(503, f.status());
    }

    // ----- body shape (success) -----

    @Test
    void successBodyHasTopicAndPartitionsArray() {
        FetchResponseFormatter.PartitionFetch p = ok(0, 100, 0, 200, Collections.singletonList(rec(100, "x")));
        JsonNode body = formatter.format("orders", p, 0).body();

        assertEquals("orders", body.get("topic").asText());
        assertTrue(body.get("partitions").isArray());
        assertEquals(1, body.get("partitions").size());
    }

    @Test
    void partitionEntryCarriesOffsetsAndRecords() {
        FetchResponseFormatter.PartitionFetch p = ok(7, 100, 50, 250, Collections.singletonList(rec(100, "hi")));
        JsonNode entry = formatter.format("orders", p, 0).body().get("partitions").get(0);

        assertEquals(7, entry.get("partition").asInt());
        assertEquals(50, entry.get("logStartOffset").asLong());
        assertEquals(250, entry.get("highWatermark").asLong());
        assertTrue(entry.get("records").isArray());
        assertEquals(1, entry.get("records").size());
    }

    @Test
    void recordEntryUsesValueEnvelope() {
        FetchResponseFormatter.PartitionFetch p = ok(0, 100, 0, 200, Collections.singletonList(rec(100, "hi")));
        JsonNode r = formatter.format("orders", p, 0).body()
            .get("partitions").get(0).get("records").get(0);

        assertEquals(100, r.get("offset").asLong());
        assertNotNull(r.get("value"));
        assertEquals("STRING", r.get("value").get("type").asText());
        assertEquals("hi", r.get("value").get("data").asText());
    }

    @Test
    void recordEntryIncludesTimestamp() {
        FetchResponseFormatter.PartitionFetch p = ok(0, 100, 0, 200, Collections.singletonList(rec(100, "hi")));
        JsonNode r = formatter.format("orders", p, 0).body()
            .get("partitions").get(0).get("records").get(0);

        assertEquals(1100L, r.get("timestamp").asLong());
    }

    @Test
    void recordEntryEmitsKeyEnvelopeWhenPresent() {
        FetchResponseFormatter.FetchedRecord r = new FetchResponseFormatter.FetchedRecord(
            100,
            "k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8),
            null,
            1234L);
        FetchResponseFormatter.PartitionFetch p = ok(0, 100, 0, 200, Collections.singletonList(r));
        JsonNode entry = formatter.format("orders", p, 0).body()
            .get("partitions").get(0).get("records").get(0);

        assertNotNull(entry.get("key"));
        assertEquals("STRING", entry.get("key").get("type").asText());
        assertEquals("k", entry.get("key").get("data").asText());
    }

    @Test
    void recordEntryHasNullKeyWhenAbsent() {
        FetchResponseFormatter.PartitionFetch p = ok(0, 100, 0, 200, Collections.singletonList(rec(100, "v")));
        JsonNode entry = formatter.format("orders", p, 0).body()
            .get("partitions").get(0).get("records").get(0);

        assertTrue(entry.get("key").isNull());
    }

    // ----- HATEOAS links -----

    @Test
    void partitionCarriesSelfLink() {
        FetchResponseFormatter.PartitionFetch p = ok(0, 100, 0, 200, Collections.singletonList(rec(100, "v")));
        JsonNode entry = formatter.format("orders", p, 0).body().get("partitions").get(0);

        assertNotNull(entry.get("_links"));
        assertNotNull(entry.get("_links").get("self"));

        // self cursor should decode to the requested offset
        CursorCodec.Cursor self = CursorCodec.decode(entry.get("_links").get("self").asText());
        assertEquals("orders", self.topic());
        assertEquals(0, self.partition());
        assertEquals(100, self.offset());
    }

    @Test
    void rootLinksIncludeFirstPreviousNextLast() {
        List<FetchResponseFormatter.FetchedRecord> records = new ArrayList<>();
        records.add(rec(100, "a"));
        records.add(rec(101, "b"));
        records.add(rec(102, "c"));

        FetchResponseFormatter.PartitionFetch p = ok(0, 100, 50, 250, records);
        JsonNode links = formatter.format("orders", p, 0).body().get("_links");

        assertNotNull(links);
        assertNotNull(links.get("first"));
        assertNotNull(links.get("previous"));
        assertNotNull(links.get("next"));
        assertNotNull(links.get("last"));
    }

    @Test
    void firstLinkPointsAtLogStartOffset() {
        FetchResponseFormatter.PartitionFetch p = ok(0, 100, 50, 250, Collections.singletonList(rec(100, "x")));
        String firstCursor = formatter.format("orders", p, 0).body().get("_links").get("first").asText();
        CursorCodec.Cursor c = CursorCodec.decode(firstCursor);

        assertEquals(50, c.offset());
    }

    @Test
    void nextLinkPointsPastLastReturnedRecord() {
        List<FetchResponseFormatter.FetchedRecord> records = new ArrayList<>();
        records.add(rec(100, "a"));
        records.add(rec(101, "b"));
        records.add(rec(102, "c"));

        FetchResponseFormatter.PartitionFetch p = ok(0, 100, 0, 200, records);
        CursorCodec.Cursor next = CursorCodec.decode(
            formatter.format("orders", p, 0).body().get("_links").get("next").asText());

        assertEquals(103, next.offset());
    }

    @Test
    void nextLinkPointsAtRequestedOffsetWhenNoRecordsReturned() {
        // Long-polling at the watermark: client gets nothing; "next" still points at "try again here".
        FetchResponseFormatter.PartitionFetch p = ok(0, 200, 0, 200, Collections.emptyList());
        CursorCodec.Cursor next = CursorCodec.decode(
            formatter.format("orders", p, 0).body().get("_links").get("next").asText());

        assertEquals(200, next.offset());
    }

    @Test
    void previousLinkIsNullAtLogStart() {
        FetchResponseFormatter.PartitionFetch p = ok(0, 0, 0, 200, Collections.singletonList(rec(0, "x")));
        JsonNode previous = formatter.format("orders", p, 0).body().get("_links").get("previous");

        assertTrue(previous.isNull());
    }

    @Test
    void previousLinkStepsOneOffsetBackOtherwise() {
        FetchResponseFormatter.PartitionFetch p = ok(0, 100, 0, 200, Collections.singletonList(rec(100, "x")));
        CursorCodec.Cursor prev = CursorCodec.decode(
            formatter.format("orders", p, 0).body().get("_links").get("previous").asText());

        assertEquals(99, prev.offset());
    }

    @Test
    void lastLinkPointsAtHighWatermark() {
        FetchResponseFormatter.PartitionFetch p = ok(0, 100, 0, 250, Collections.singletonList(rec(100, "x")));
        CursorCodec.Cursor last = CursorCodec.decode(
            formatter.format("orders", p, 0).body().get("_links").get("last").asText());

        assertEquals(250, last.offset());
    }

    // ----- Retry-After / throttle -----

    @Test
    void positiveThrottleSetsRetryAfter() {
        FetchResponseFormatter.PartitionFetch p = ok(0, 100, 0, 200, Collections.singletonList(rec(100, "x")));
        HttpBridgeResponse f = formatter.format("orders", p, 1500);

        assertEquals(200, f.status());
        assertTrue(f.hasRetryAfter());
        assertEquals(2, f.retryAfterSeconds());
    }

    @Test
    void zeroThrottleHasNoRetryAfter() {
        FetchResponseFormatter.PartitionFetch p = ok(0, 100, 0, 200, Collections.singletonList(rec(100, "x")));

        assertFalse(formatter.format("orders", p, 0).hasRetryAfter());
    }

    // ----- failure body -----

    @Test
    void failureBodyIsErrorEnvelope() {
        HttpBridgeResponse f = formatter.format("orders", err(0, Errors.TOPIC_AUTHORIZATION_FAILED), 0);

        assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), f.body().get("errorCode").asInt());
        assertNotNull(f.body().get("errorMessage"));
        assertNull(f.body().get("partitions"));
    }

    @Test
    void topLevelErrorBypassesPartitions() {
        HttpBridgeResponse f =
            formatter.topLevelError("orders", Errors.INVALID_REQUEST, "offset must be non-negative", 0);

        assertEquals(400, f.status());
        assertEquals("offset must be non-negative", f.body().get("errorMessage").asText());
        assertEquals("orders", f.body().get("topic").asText());
        assertNull(f.body().get("partitions"));
    }
}
