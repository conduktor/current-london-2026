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

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * The seam between the HTTP bridge's pure translation layer and the broker's request machinery.
 *
 * <p>Production implementations build {@code ProduceRequest} / {@code FetchRequest} from the typed command, hand them
 * to {@code RequestChannel}, and project the broker's response back into the formatter-friendly types declared here.
 * Tests substitute a fake implementation that yields whatever outcome the test needs to exercise the orchestrator.
 *
 * <p>Authorization, quotas and replication are not the bridge's concern — they are evaluated inside the broker by the
 * same code paths the binary protocol uses. The submitter merely reports what the broker decided: per-partition errors
 * (which the formatters then translate to HTTP status), throttle delay (which becomes {@code Retry-After}), and so on.
 *
 * <p>Asynchronous because the broker is — produce / fetch responses come back via callbacks on the request handler
 * threads, not on the HTTP request thread. Returning a {@link CompletableFuture} lets the Jetty layer free its
 * connection thread while we wait for the broker.
 */
public interface RequestSubmitter {

    CompletableFuture<ProduceResult> submitProduce(ProduceRequestParser.ProduceCommand command);

    CompletableFuture<FetchResult> submitFetch(FetchRequestParser.FetchCommand command);

    /** What the bridge needs to format a produce response: per-partition outcomes plus throttle. */
    final class ProduceResult {
        private final List<ProduceResponseFormatter.PartitionResult> partitions;
        private final long throttleTimeMs;

        public ProduceResult(List<ProduceResponseFormatter.PartitionResult> partitions, long throttleTimeMs) {
            this.partitions = Objects.requireNonNull(partitions, "partitions must not be null");
            this.throttleTimeMs = throttleTimeMs;
        }

        public List<ProduceResponseFormatter.PartitionResult> partitions() {
            return partitions;
        }

        public long throttleTimeMs() {
            return throttleTimeMs;
        }
    }

    /** What the bridge needs to format a fetch response: the partition view plus throttle. */
    final class FetchResult {
        private final FetchResponseFormatter.PartitionFetch partition;
        private final long throttleTimeMs;

        public FetchResult(FetchResponseFormatter.PartitionFetch partition, long throttleTimeMs) {
            this.partition = Objects.requireNonNull(partition, "partition must not be null");
            this.throttleTimeMs = throttleTimeMs;
        }

        public FetchResponseFormatter.PartitionFetch partition() {
            return partition;
        }

        public long throttleTimeMs() {
            return throttleTimeMs;
        }
    }
}
