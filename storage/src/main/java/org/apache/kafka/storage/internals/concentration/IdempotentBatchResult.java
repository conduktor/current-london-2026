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
package org.apache.kafka.storage.internals.concentration;

/**
 * Result of a previously-committed idempotent batch on a logical partition. Cached so that an
 * idempotent producer retry of the same {@link IdempotentBatchKey} short-circuits the produce
 * path: we return the original logical offsets to the client instead of reserving fresh offsets
 * and waiting for the backing log's {@code ProducerStateManager} to dedup. The pre-emption is what
 * keeps the {@code nextLogicalOffset} contiguous (PROMPT.md scenario 6 — "logical offset sequence
 * remains contiguous").
 *
 * <p>logAppendTime is preserved because Kafka's wire response carries it and idempotent producers
 * may use it for client-side timestamp inspection; leaking the retry's wall-clock timestamp here
 * would be a subtle correctness drift.
 */
public final class IdempotentBatchResult {
    private final long logicalBaseOffset;
    private final long logicalLastOffset;
    private final long logStartOffset;
    private final long logAppendTime;

    public IdempotentBatchResult(long logicalBaseOffset,
                                 long logicalLastOffset,
                                 long logStartOffset,
                                 long logAppendTime) {
        this.logicalBaseOffset = logicalBaseOffset;
        this.logicalLastOffset = logicalLastOffset;
        this.logStartOffset = logStartOffset;
        this.logAppendTime = logAppendTime;
    }

    public long logicalBaseOffset() {
        return logicalBaseOffset;
    }

    public long logicalLastOffset() {
        return logicalLastOffset;
    }

    public long logStartOffset() {
        return logStartOffset;
    }

    public long logAppendTime() {
        return logAppendTime;
    }

    @Override
    public String toString() {
        return "IdempotentBatchResult(logicalBaseOffset=" + logicalBaseOffset
            + ", logicalLastOffset=" + logicalLastOffset
            + ", logStartOffset=" + logStartOffset
            + ", logAppendTime=" + logAppendTime + ")";
    }
}
