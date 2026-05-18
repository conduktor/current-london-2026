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

import org.apache.kafka.common.TopicPartition;

import java.io.IOException;

/**
 * Thrown by {@link ConcentrationKernel#commitProduce} and
 * {@link ConcentrationKernel#commitProduceBatch} when the backing partition's gate generation has
 * advanced between reserve and commit. Indicates that the broker lost leadership of the backing
 * (or the gate was otherwise closed) while a produce callback was in flight, so committing this
 * reservation would publish a logical→backing mapping for an offset a new leader may truncate.
 *
 * <p>Extends {@link IOException} so the existing produce-error rollback path in
 * {@code KafkaApis.handleProduceAppend} catches it without a new branch. The broker translates
 * any {@code IOException} from {@code commitProduceBatch} into {@code NOT_LEADER_OR_FOLLOWER}
 * on the produce response — the producer retries against the new leader, which is the correct
 * recovery for a gate-close race (PROMPT.md Section 3 "produce after losing leadership").
 *
 * <p>The reservation is rolled back inside {@code commitProduceBatch} before this exception is
 * thrown, so the partition's tracker state is consistent on return.
 */
public class BackingGenerationChangedException extends IOException {
    private static final long serialVersionUID = 1L;

    public BackingGenerationChangedException(TopicPartition backing, long expectedGeneration, long currentGeneration) {
        super("backing " + backing + " gate generation changed during produce: expected "
            + expectedGeneration + ", current " + currentGeneration
            + " (broker lost leadership of backing while produce was in flight)");
    }
}
