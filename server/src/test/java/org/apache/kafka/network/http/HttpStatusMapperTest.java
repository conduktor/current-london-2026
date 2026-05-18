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
        // CORRUPT_MESSAGE has no explicit mapping; fall back to 500
        assertEquals(500, HttpStatusMapper.toHttpStatus(Errors.CORRUPT_MESSAGE));
    }

    @Test
    void unsupportedVersionMapsTo400() {
        assertEquals(400, HttpStatusMapper.toHttpStatus(Errors.UNSUPPORTED_VERSION));
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
    }

    @Test
    void retryAfterDoesNotApplyTo200() {
        // Note: HTTP produce quota path uses 200 + Retry-After deliberately. The header is set
        // explicitly by the produce code path — this helper indicates the *general* policy of
        // which status codes carry Retry-After for error responses.
        assertFalse(HttpStatusMapper.statusCarriesRetryAfter(200));
    }
}
