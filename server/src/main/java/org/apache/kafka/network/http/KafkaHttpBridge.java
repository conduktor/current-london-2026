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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The orchestrator that wires HTTP requests to the broker's request machinery and back.
 *
 * <p>Three phases per request:
 * <ol>
 *   <li><strong>Parse</strong> — turn the HTTP body / query into a typed command via {@link ProduceRequestParser} /
 *       {@link FetchRequestParser}. A {@link ProduceRequestParser.BadRequestException} here yields a {@code 400} with
 *       an {@link ErrorEnvelope} body; we never reach the submitter.</li>
 *   <li><strong>Submit</strong> — hand the command to the {@link RequestSubmitter}, which talks to the broker. The
 *       submitter is the only piece of the bridge that knows about {@code RequestChannel} and {@code KafkaApis}; the
 *       rest of this class is broker-agnostic and unit-testable with a fake.</li>
 *   <li><strong>Format</strong> — project the submitter's outcome into an {@link HttpBridgeResponse} via the existing
 *       {@link ProduceResponseFormatter} / {@link FetchResponseFormatter}, including HATEOAS cursors and Retry-After.</li>
 * </ol>
 *
 * <p>The contract with the transport layer (Jetty handler, in-process test) is: {@link #produce(String, JsonNode)} and
 * {@link #fetch(String, QueryParams)} always complete with an {@link HttpBridgeResponse}, never throw, never complete
 * exceptionally. Every error path — bad input, broker rejection, submitter exception — is mapped to a well-formed
 * response. This invariant is what lets the transport adapter be a thin shell with no error-handling logic of its own.
 */
public final class KafkaHttpBridge {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaHttpBridge.class);

    /** Sentinel that disables the safety net — for tests that want to assert pre-timeout behaviour deterministically. */
    public static final long NO_TIMEOUT = 0L;

    private final ObjectMapper mapper;
    private final RequestSubmitter submitter;
    private final ProduceResponseFormatter produceFormatter;
    private final FetchResponseFormatter fetchFormatter;
    private final long requestTimeoutMs;

    /**
     * Test-friendly constructor that defaults the request-timeout safety net to disabled. Production callers must use
     * the explicit-timeout overload; running with no timeout is a deliberate test affordance, not a production option.
     */
    public KafkaHttpBridge(ObjectMapper mapper, RequestSubmitter submitter) {
        this(mapper, submitter, NO_TIMEOUT);
    }

    public KafkaHttpBridge(ObjectMapper mapper, RequestSubmitter submitter, long requestTimeoutMs) {
        this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
        this.submitter = Objects.requireNonNull(submitter, "submitter must not be null");
        if (requestTimeoutMs < 0) {
            throw new IllegalArgumentException("requestTimeoutMs must be non-negative, got " + requestTimeoutMs);
        }
        this.requestTimeoutMs = requestTimeoutMs;
        this.produceFormatter = new ProduceResponseFormatter(mapper);
        this.fetchFormatter = new FetchResponseFormatter(mapper);
    }

    public CompletableFuture<HttpBridgeResponse> produce(String topic, JsonNode body) {
        ProduceRequestParser.ProduceCommand command;
        try {
            command = ProduceRequestParser.parse(topic, body);
        } catch (ProduceRequestParser.BadRequestException e) {
            return CompletableFuture.completedFuture(
                produceFormatter.topLevelError(safeTopic(topic), Errors.INVALID_REQUEST, e.getMessage(), 0L));
        }

        return withTimeout(submitter.submitProduce(command))
            .handle((result, throwable) -> {
                if (isTimeout(throwable)) {
                    return produceFormatter.topLevelError(
                        command.topic(), Errors.REQUEST_TIMED_OUT,
                        "request timed out after " + requestTimeoutMs + "ms", 0L);
                }
                if (throwable != null) {
                    // Log the real cause server-side; never echo throwable.getMessage() to the client. Stock JDK
                    // messages (e.g. "Cannot invoke X.y() because z is null") leak class and field names; sanitised
                    // Kafka errors already flow through the partition/topic-error paths above, not this branch.
                    LOG.warn("HTTP bridge produce failed unexpectedly for {}", command.topic(), throwable);
                    return produceFormatter.topLevelError(
                        command.topic(), Errors.UNKNOWN_SERVER_ERROR, null, 0L);
                }
                return produceFormatter.format(command.topic(), result.partitions(), result.throttleTimeMs());
            });
    }

    public CompletableFuture<HttpBridgeResponse> fetch(String topic, QueryParams query) {
        FetchRequestParser.FetchCommand command;
        try {
            command = FetchRequestParser.parse(topic, query);
        } catch (ProduceRequestParser.BadRequestException e) {
            return CompletableFuture.completedFuture(
                fetchFormatter.topLevelError(safeTopic(topic), Errors.INVALID_REQUEST, e.getMessage(), 0L));
        }

        return withTimeout(submitter.submitFetch(command))
            .handle((result, throwable) -> {
                if (isTimeout(throwable)) {
                    return fetchFormatter.topLevelError(
                        command.topic(), Errors.REQUEST_TIMED_OUT,
                        "request timed out after " + requestTimeoutMs + "ms", 0L);
                }
                if (throwable != null) {
                    // See produce() above — never echo throwable.getMessage() to the client.
                    LOG.warn("HTTP bridge fetch failed unexpectedly for {}", command.topic(), throwable);
                    return fetchFormatter.topLevelError(
                        command.topic(), Errors.UNKNOWN_SERVER_ERROR, null, 0L);
                }
                return fetchFormatter.format(command.topic(), result.partition(), result.throttleTimeMs());
            });
    }

    /**
     * Apply the configured timeout to the submitter future. With {@link #NO_TIMEOUT} we pass the future through
     * unchanged — that lets tests deterministically assert pre-timeout behaviour without racing the safety net.
     *
     * <p>Why a safety net at all: the {@code RequestChannel} completion hook only short-circuits the 3-arg
     * {@code sendResponse} path. If a future broker change routes a response through {@code sendNoOpResponse} or a
     * disconnected processor's 1-arg {@code sendResponse(Response)}, the submitter future would never complete and the
     * HTTP connection would hang. We'd rather emit a 504 than orphan the client.
     */
    private <T> CompletableFuture<T> withTimeout(CompletableFuture<T> future) {
        if (requestTimeoutMs == NO_TIMEOUT) {
            return future;
        }
        return future.orTimeout(requestTimeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * {@link CompletableFuture#orTimeout} surfaces the timeout as a {@link TimeoutException}, but the framework wraps
     * it once more in a {@link CompletionException} as it propagates through the {@code .handle(...)} stage. Unwrap one
     * layer to recognise the case cleanly.
     */
    private static boolean isTimeout(Throwable throwable) {
        if (throwable == null) {
            return false;
        }
        if (throwable instanceof TimeoutException) {
            return true;
        }
        Throwable cause = (throwable instanceof CompletionException) ? throwable.getCause() : null;
        return cause instanceof TimeoutException;
    }

    /** Topic for the error path: parsing failed because topic was null/blank, so emit a placeholder rather than NPE. */
    private static String safeTopic(String topic) {
        return topic == null || topic.trim().isEmpty() ? "(unknown)" : topic;
    }
}
