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

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * Placeholder {@link RequestSubmitter} used while the production wiring through {@code RequestChannel} → {@code KafkaApis}
 * is still under construction.
 *
 * <p>It exists so the broker can wire the HTTP bridge into {@code BrokerServer} today and have the endpoint return
 * something deterministic — every request gets {@link Errors#REQUEST_TIMED_OUT} (mapped to HTTP 503 by
 * {@link HttpStatusMapper}) with a clear "not yet implemented" {@code errorMessage}. That gives operators a way to
 * verify the listener is bound and reachable without the bridge silently accepting work it cannot actually carry out.
 *
 * <p>Once the production submitter lands, this class will be deleted; its name and contents are intentionally not part
 * of any public API.
 */
public final class NotImplementedRequestSubmitter implements RequestSubmitter {

    private static final String MESSAGE = "HTTP bridge is wired but the RequestChannel integration is not yet implemented "
        + "in this build. The endpoint is reachable and the request was parsed correctly; nothing was actually submitted "
        + "to the broker.";

    @Override
    public CompletableFuture<ProduceResult> submitProduce(ProduceRequestParser.ProduceCommand command) {
        // We can't synthesize partition-level results without knowing the topic's partitions, so we return an empty list
        // of results and let ProduceResponseFormatter trip into the "empty -> 500" path. That's not great, so we instead
        // emit a single synthetic partition result carrying REQUEST_TIMED_OUT so the response shows the user a real 503.
        ProduceResult result = new ProduceResult(
            Collections.singletonList(
                new ProduceResponseFormatter.PartitionResult(
                    command.records().isEmpty() ? 0 : firstPartition(command),
                    -1L,
                    Errors.REQUEST_TIMED_OUT,
                    MESSAGE)),
            0L);
        return CompletableFuture.completedFuture(result);
    }

    @Override
    public CompletableFuture<FetchResult> submitFetch(FetchRequestParser.FetchCommand command) {
        FetchResult result = new FetchResult(
            new FetchResponseFormatter.PartitionFetch(
                command.partition(),
                Errors.REQUEST_TIMED_OUT,
                MESSAGE,
                command.offset(),
                0,
                0,
                Collections.emptyList()),
            0L);
        return CompletableFuture.completedFuture(result);
    }

    private static int firstPartition(ProduceRequestParser.ProduceCommand command) {
        return command.records().get(0).partition().orElse(0);
    }
}
