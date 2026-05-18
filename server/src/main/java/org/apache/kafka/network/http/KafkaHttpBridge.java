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

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

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

    private final ObjectMapper mapper;
    private final RequestSubmitter submitter;
    private final ProduceResponseFormatter produceFormatter;
    private final FetchResponseFormatter fetchFormatter;

    public KafkaHttpBridge(ObjectMapper mapper, RequestSubmitter submitter) {
        this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
        this.submitter = Objects.requireNonNull(submitter, "submitter must not be null");
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

        return submitter.submitProduce(command)
            .handle((result, throwable) -> {
                if (throwable != null) {
                    return produceFormatter.topLevelError(
                        command.topic(), Errors.UNKNOWN_SERVER_ERROR, throwable.getMessage(), 0L);
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

        return submitter.submitFetch(command)
            .handle((result, throwable) -> {
                if (throwable != null) {
                    return fetchFormatter.topLevelError(
                        command.topic(), Errors.UNKNOWN_SERVER_ERROR, throwable.getMessage(), 0L);
                }
                return fetchFormatter.format(command.topic(), result.partition(), result.throttleTimeMs());
            });
    }

    /** Topic for the error path: parsing failed because topic was null/blank, so emit a placeholder rather than NPE. */
    private static String safeTopic(String topic) {
        return topic == null || topic.trim().isEmpty() ? "(unknown)" : topic;
    }
}
