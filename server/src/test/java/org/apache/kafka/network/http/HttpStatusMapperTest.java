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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HttpStatusMapperTest {

    @Test
    void noneMapsTo200() {
        assertEquals(200, HttpStatusMapper.toHttpStatus(Errors.NONE));
    }

    @Test
    void topicAuthorizationFailedMapsTo403() {
        assertEquals(403, HttpStatusMapper.toHttpStatus(Errors.TOPIC_AUTHORIZATION_FAILED));
    }

    @Test
    void groupAuthorizationFailedMapsTo403() {
        assertEquals(403, HttpStatusMapper.toHttpStatus(Errors.GROUP_AUTHORIZATION_FAILED));
    }

    @Test
    void clusterAuthorizationFailedMapsTo403() {
        assertEquals(403, HttpStatusMapper.toHttpStatus(Errors.CLUSTER_AUTHORIZATION_FAILED));
    }

    @Test
    void unknownTopicOrPartitionMapsTo404() {
        assertEquals(404, HttpStatusMapper.toHttpStatus(Errors.UNKNOWN_TOPIC_OR_PARTITION));
    }

    @Test
    void unknownTopicIdMapsTo404() {
        assertEquals(404, HttpStatusMapper.toHttpStatus(Errors.UNKNOWN_TOPIC_ID));
    }

    @Test
    void invalidRequestMapsTo400() {
        assertEquals(400, HttpStatusMapper.toHttpStatus(Errors.INVALID_REQUEST));
    }

    @Test
    void invalidTopicExceptionMapsTo400() {
        assertEquals(400, HttpStatusMapper.toHttpStatus(Errors.INVALID_TOPIC_EXCEPTION));
    }

    @Test
    void invalidRequiredAcksMapsTo400() {
        assertEquals(400, HttpStatusMapper.toHttpStatus(Errors.INVALID_REQUIRED_ACKS));
    }

    @Test
    void offsetOutOfRangeMapsTo400() {
        assertEquals(400, HttpStatusMapper.toHttpStatus(Errors.OFFSET_OUT_OF_RANGE));
    }

    @Test
    void messageTooLargeMapsTo413() {
        assertEquals(413, HttpStatusMapper.toHttpStatus(Errors.MESSAGE_TOO_LARGE));
    }

    @Test
    void recordListTooLargeMapsTo413() {
        assertEquals(413, HttpStatusMapper.toHttpStatus(Errors.RECORD_LIST_TOO_LARGE));
    }

    @Test
    void requestTimedOutMapsTo504() {
        assertEquals(504, HttpStatusMapper.toHttpStatus(Errors.REQUEST_TIMED_OUT));
    }

    @Test
    void leaderNotAvailableMapsTo503() {
        assertEquals(503, HttpStatusMapper.toHttpStatus(Errors.LEADER_NOT_AVAILABLE));
    }

    @Test
    void notLeaderOrFollowerMapsTo503() {
        assertEquals(503, HttpStatusMapper.toHttpStatus(Errors.NOT_LEADER_OR_FOLLOWER));
    }

    @Test
    void replicaNotAvailableMapsTo503() {
        assertEquals(503, HttpStatusMapper.toHttpStatus(Errors.REPLICA_NOT_AVAILABLE));
    }

    @Test
    void brokerNotAvailableMapsTo503() {
        assertEquals(503, HttpStatusMapper.toHttpStatus(Errors.BROKER_NOT_AVAILABLE));
    }

    @Test
    void notEnoughReplicasMapsTo503() {
        assertEquals(503, HttpStatusMapper.toHttpStatus(Errors.NOT_ENOUGH_REPLICAS));
    }

    @Test
    void unknownServerErrorMapsTo500() {
        assertEquals(500, HttpStatusMapper.toHttpStatus(Errors.UNKNOWN_SERVER_ERROR));
    }

    @Test
    void unmappedErrorDefaultsTo500() {
        // NOT_CONTROLLER cannot legitimately surface via produce/fetch (it is a metadata-RPC error), so it
        // is left unmapped and falls through to 500. Wave 27 moved CORRUPT_MESSAGE → 400 (produce-side
        // client-fault dominates), so this sentinel changed.
        assertEquals(500, HttpStatusMapper.toHttpStatus(Errors.NOT_CONTROLLER));
    }

    @Test
    void unsupportedVersionMapsTo400() {
        assertEquals(400, HttpStatusMapper.toHttpStatus(Errors.UNSUPPORTED_VERSION));
    }

    // ----- Wave 27: client-fault produce/fetch errors that previously fell to 500 -----

    @Test
    void invalidRecordMapsTo400() {
        // Broker LogValidator rejected the produce batch (bad magic, null key on compacted topic, parse-time
        // CRC failure). Client-fault — re-submitting the same bytes will fail again. Must NOT be 500/503
        // because that invites harmful retries.
        assertEquals(400, HttpStatusMapper.toHttpStatus(Errors.INVALID_RECORD));
    }

    @Test
    void unsupportedCompressionTypeMapsTo400() {
        // Produced batch uses a codec the broker / topic config does not accept. Client must re-encode.
        assertEquals(400, HttpStatusMapper.toHttpStatus(Errors.UNSUPPORTED_COMPRESSION_TYPE));
    }

    @Test
    void corruptMessageMapsTo400() {
        // Produce-side CRC mismatch (client wrote a bad batch). Same client-fault as INVALID_RECORD. The
        // fetch-side rationale (broker-side log corruption) is rare enough that 400 is acceptable — the
        // Errors.name() in the envelope still discriminates for diagnostics.
        assertEquals(400, HttpStatusMapper.toHttpStatus(Errors.CORRUPT_MESSAGE));
    }

    @Test
    void offsetMovedToTieredStorageMapsTo400() {
        // Tiered-storage clusters: requested offset moved below the local logStartOffset. Same class as
        // OFFSET_OUT_OF_RANGE — caller needs a different cursor (e.g. _links.first), not a retry.
        assertEquals(400, HttpStatusMapper.toHttpStatus(Errors.OFFSET_MOVED_TO_TIERED_STORAGE));
    }

    @Test
    void positionOutOfRangeMapsTo400() {
        // Analogous to OFFSET_OUT_OF_RANGE. Same client-fault classification.
        assertEquals(400, HttpStatusMapper.toHttpStatus(Errors.POSITION_OUT_OF_RANGE));
    }

    @Test
    void listenerNotFoundMapsTo503() {
        // Transient: the broker is up but listener metadata has not yet propagated to this node. Retryable
        // once metadata refreshes.
        assertEquals(503, HttpStatusMapper.toHttpStatus(Errors.LISTENER_NOT_FOUND));
        // And the Retry-After contract holds for 503.
        assertTrue(HttpStatusMapper.statusCarriesRetryAfter(
            HttpStatusMapper.toHttpStatus(Errors.LISTENER_NOT_FOUND)));
    }

    // ----- carriesRetryAfter -----

    @Test
    void retryAfterAppliesToTransientServerErrors() {
        assertTrue(HttpStatusMapper.statusCarriesRetryAfter(503));
        assertTrue(HttpStatusMapper.statusCarriesRetryAfter(504));
    }

    @Test
    void retryAfterDoesNotApplyToClientErrors() {
        assertFalse(HttpStatusMapper.statusCarriesRetryAfter(400));
        assertFalse(HttpStatusMapper.statusCarriesRetryAfter(403));
        assertFalse(HttpStatusMapper.statusCarriesRetryAfter(404));
        assertFalse(HttpStatusMapper.statusCarriesRetryAfter(413));
        assertFalse(HttpStatusMapper.statusCarriesRetryAfter(415));
    }

    @Test
    void unsupportedMediaTypeConstantIs415() {
        // The 415 constant is consumed by KafkaHttpServlet's Content-Type guard on the produce path; pin its value so
        // a refactor of the mapper's constants cannot accidentally re-route 415 to a different status.
        assertEquals(415, HttpStatusMapper.UNSUPPORTED_MEDIA_TYPE);
    }

    @Test
    void retryAfterDoesNotApplyTo200() {
        // Note: HTTP produce quota path uses 200 + Retry-After deliberately. The header is set
        // explicitly by the produce code path — this helper indicates the *general* policy of
        // which status codes carry Retry-After for error responses.
        assertFalse(HttpStatusMapper.statusCarriesRetryAfter(200));
    }
}
