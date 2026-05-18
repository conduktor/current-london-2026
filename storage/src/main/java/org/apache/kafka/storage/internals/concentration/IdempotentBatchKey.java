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

import java.util.Objects;

/**
 * Identity of an idempotent produce batch: (producerId, producerEpoch, baseSequence, lastSequence).
 * The concentration kernel keys its retry-deduplication cache on these four fields, exactly matching
 * the dedup window Kafka's {@code ProducerStateEntry} maintains on the backing log. Equality is
 * required for the cache lookup; producerId alone is insufficient because a producer with a new
 * epoch must NOT receive a cached result from the previous epoch (epoch bumps imply
 * idempotent-state reset on the client side).
 */
public final class IdempotentBatchKey {
    private final long producerId;
    private final short producerEpoch;
    private final int baseSequence;
    private final int lastSequence;

    public IdempotentBatchKey(long producerId, short producerEpoch, int baseSequence, int lastSequence) {
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.lastSequence = lastSequence;
    }

    public long producerId() {
        return producerId;
    }

    public short producerEpoch() {
        return producerEpoch;
    }

    public int baseSequence() {
        return baseSequence;
    }

    public int lastSequence() {
        return lastSequence;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IdempotentBatchKey)) return false;
        IdempotentBatchKey that = (IdempotentBatchKey) o;
        return producerId == that.producerId
            && producerEpoch == that.producerEpoch
            && baseSequence == that.baseSequence
            && lastSequence == that.lastSequence;
    }

    @Override
    public int hashCode() {
        return Objects.hash(producerId, producerEpoch, baseSequence, lastSequence);
    }

    @Override
    public String toString() {
        return "IdempotentBatchKey(producerId=" + producerId
            + ", producerEpoch=" + producerEpoch
            + ", baseSequence=" + baseSequence
            + ", lastSequence=" + lastSequence + ")";
    }
}
