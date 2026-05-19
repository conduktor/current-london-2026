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

import java.util.EnumMap;
import java.util.Map;

/**
 * Maps the broker's internal {@link Errors} enum to the HTTP status code returned to the caller.
 *
 * The mapping is deliberately narrow: it covers the codes that the produce / fetch paths can realistically surface, and
 * falls back to {@code 500 Internal Server Error} for anything unmapped. The bridge keeps the original {@code errorCode}
 * and {@code errorMessage} in the JSON body so callers can recover the exact Kafka condition when they need to.
 */
public final class HttpStatusMapper {

    public static final int OK = 200;
    public static final int MULTI_STATUS = 207;
    public static final int BAD_REQUEST = 400;
    public static final int FORBIDDEN = 403;
    public static final int NOT_FOUND = 404;
    public static final int METHOD_NOT_ALLOWED = 405;
    public static final int PAYLOAD_TOO_LARGE = 413;
    public static final int TOO_MANY_REQUESTS = 429;
    public static final int INTERNAL_SERVER_ERROR = 500;
    public static final int SERVICE_UNAVAILABLE = 503;
    public static final int GATEWAY_TIMEOUT = 504;

    private static final Map<Errors, Integer> STATUS_BY_ERROR = buildMapping();

    private HttpStatusMapper() {
    }

    public static int toHttpStatus(Errors error) {
        return STATUS_BY_ERROR.getOrDefault(error, INTERNAL_SERVER_ERROR);
    }

    /**
     * Whether the given HTTP status code carries a {@code Retry-After} header by general policy. The produce-quota path
     * sets {@code Retry-After} on a 200 response deliberately (see spec) — that is decided at the call site, not here.
     */
    public static boolean statusCarriesRetryAfter(int status) {
        return status == SERVICE_UNAVAILABLE || status == GATEWAY_TIMEOUT;
    }

    private static Map<Errors, Integer> buildMapping() {
        Map<Errors, Integer> m = new EnumMap<>(Errors.class);

        m.put(Errors.NONE, OK);

        m.put(Errors.TOPIC_AUTHORIZATION_FAILED, FORBIDDEN);
        m.put(Errors.GROUP_AUTHORIZATION_FAILED, FORBIDDEN);
        m.put(Errors.CLUSTER_AUTHORIZATION_FAILED, FORBIDDEN);
        m.put(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED, FORBIDDEN);
        m.put(Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED, FORBIDDEN);

        m.put(Errors.UNKNOWN_TOPIC_OR_PARTITION, NOT_FOUND);
        m.put(Errors.UNKNOWN_TOPIC_ID, NOT_FOUND);

        m.put(Errors.INVALID_REQUEST, BAD_REQUEST);
        m.put(Errors.INVALID_TOPIC_EXCEPTION, BAD_REQUEST);
        m.put(Errors.INVALID_REQUIRED_ACKS, BAD_REQUEST);
        m.put(Errors.INVALID_FETCH_SIZE, BAD_REQUEST);
        m.put(Errors.INVALID_PARTITIONS, BAD_REQUEST);
        m.put(Errors.INVALID_REPLICATION_FACTOR, BAD_REQUEST);
        m.put(Errors.INVALID_TIMESTAMP, BAD_REQUEST);
        m.put(Errors.OFFSET_OUT_OF_RANGE, BAD_REQUEST);
        m.put(Errors.UNSUPPORTED_VERSION, BAD_REQUEST);
        m.put(Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT, BAD_REQUEST);
        m.put(Errors.POLICY_VIOLATION, BAD_REQUEST);
        // Client-fault produce/fetch failures that were previously falling through to 500. Telling a caller
        // "500 try again" when the truth is "your payload is malformed" causes harmful retries: the original
        // bytes will be rejected the second time too, and a worse failure mode (retry storms on a runaway
        // producer) is what shows up in operator dashboards. Map to 400 explicitly.
        // - INVALID_RECORD: broker LogValidator rejected the produce batch (bad magic, null key on compacted
        //   topic, parse-time CRC failure).
        // - UNSUPPORTED_COMPRESSION_TYPE: produced batch uses a codec the broker / topic config does not accept.
        // - CORRUPT_MESSAGE: produce-side CRC mismatch (client wrote a bad batch). The fetch-side rationale
        //   is broker-side log corruption (legitimately 500), but the produce path is the common case for an
        //   externally-facing bridge and the produce semantics dominate the decision. The fetch case is rare
        //   enough that a 400 here is acceptable (caller can still recover via Errors.name() in the envelope).
        // - OFFSET_MOVED_TO_TIERED_STORAGE / POSITION_OUT_OF_RANGE: same class as OFFSET_OUT_OF_RANGE — caller
        //   needs a different cursor, not a retry of the same offset.
        m.put(Errors.INVALID_RECORD, BAD_REQUEST);
        m.put(Errors.UNSUPPORTED_COMPRESSION_TYPE, BAD_REQUEST);
        m.put(Errors.CORRUPT_MESSAGE, BAD_REQUEST);
        m.put(Errors.OFFSET_MOVED_TO_TIERED_STORAGE, BAD_REQUEST);
        m.put(Errors.POSITION_OUT_OF_RANGE, BAD_REQUEST);

        m.put(Errors.MESSAGE_TOO_LARGE, PAYLOAD_TOO_LARGE);
        m.put(Errors.RECORD_LIST_TOO_LARGE, PAYLOAD_TOO_LARGE);

        m.put(Errors.REQUEST_TIMED_OUT, GATEWAY_TIMEOUT);

        m.put(Errors.LEADER_NOT_AVAILABLE, SERVICE_UNAVAILABLE);
        m.put(Errors.NOT_LEADER_OR_FOLLOWER, SERVICE_UNAVAILABLE);
        m.put(Errors.REPLICA_NOT_AVAILABLE, SERVICE_UNAVAILABLE);
        m.put(Errors.BROKER_NOT_AVAILABLE, SERVICE_UNAVAILABLE);
        m.put(Errors.NOT_ENOUGH_REPLICAS, SERVICE_UNAVAILABLE);
        // NOT_ENOUGH_REPLICAS_AFTER_APPEND is intentionally mapped to 503 (retryable) despite the durability
        // ambiguity: the record IS persisted to the leader but not yet replicated to enough ISRs. A 503
        // invites the client to retry, which may duplicate the produce. The alternative — 500 / 207 / a
        // new envelope flag — would diverge from the standard wire contract without a clear win, and the
        // client's idempotence / dedup policy is outside the bridge's surface. Document the trade-off here
        // so a future contributor doesn't accidentally "fix" this mapping without considering the upgrade
        // path for a richer envelope shape.
        m.put(Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND, SERVICE_UNAVAILABLE);
        m.put(Errors.COORDINATOR_NOT_AVAILABLE, SERVICE_UNAVAILABLE);
        m.put(Errors.COORDINATOR_LOAD_IN_PROGRESS, SERVICE_UNAVAILABLE);
        // LISTENER_NOT_FOUND is transient: the broker is up but the listener metadata has not yet propagated
        // to this node. A retry against the same coordinator will eventually succeed once metadata refreshes.
        m.put(Errors.LISTENER_NOT_FOUND, SERVICE_UNAVAILABLE);

        return m;
    }
}
