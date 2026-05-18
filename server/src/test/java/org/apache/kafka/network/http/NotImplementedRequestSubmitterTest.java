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

import org.apache.kafka.common.protocol.Errors;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the contract of the placeholder submitter: every call must surface as a 504 ({@link Errors#REQUEST_TIMED_OUT})
 * with a non-empty errorMessage. The class is deliberately temporary, but while it lives it is the only behaviour an
 * operator gets when they flip {@code http.bridge.enabled=true}, so we want it to lie deterministically rather than
 * pretend to succeed.
 */
class NotImplementedRequestSubmitterTest {

    private final NotImplementedRequestSubmitter submitter = new NotImplementedRequestSubmitter();

    @Test
    void produceReturnsTimedOutWithMessage() throws Exception {
        ProduceRequestParser.ProduceCommand cmd = new ProduceRequestParser.ProduceCommand(
            "orders",
            Collections.singletonList(new ProduceRequestParser.RecordEntry(
                OptionalInt.of(3), null, new byte[]{1, 2, 3}, null)));

        RequestSubmitter.ProduceResult result = submitter.submitProduce(cmd).get();

        assertEquals(1, result.partitions().size());
        ProduceResponseFormatter.PartitionResult part = result.partitions().get(0);
        assertEquals(3, part.partition());
        assertEquals(Errors.REQUEST_TIMED_OUT, part.error());
        assertEquals(-1L, part.offset());
        assertFalse(part.errorMessage() == null || part.errorMessage().isBlank(),
            "errorMessage should clearly state that the bridge is not yet implemented");
        assertEquals(0L, result.throttleTimeMs());
    }

    @Test
    void fetchReturnsTimedOutWithMessage() throws Exception {
        FetchRequestParser.FetchCommand cmd = new FetchRequestParser.FetchCommand(
            "orders", 7, 1234L, OptionalInt.empty());

        RequestSubmitter.FetchResult result = submitter.submitFetch(cmd).get();

        assertEquals(7, result.partition().partition());
        assertEquals(Errors.REQUEST_TIMED_OUT, result.partition().error());
        assertTrue(result.partition().records().isEmpty());
        assertFalse(result.partition().errorMessage() == null || result.partition().errorMessage().isBlank());
        assertEquals(0L, result.throttleTimeMs());
    }
}
