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
 * A pending logical-offset assignment held by a caller between reserve() and commit()/rollback().
 * Carries enough identity to address the right (logicalTopic, logicalPartition) sidecar index.
 */
public final class Reservation {
    enum State { OPEN, COMMITTED, ROLLED_BACK }

    private final String logicalTopic;
    private final int logicalPartition;
    private final long logicalOffset;
    private State state;

    Reservation(String logicalTopic, int logicalPartition, long logicalOffset) {
        this.logicalTopic = logicalTopic;
        this.logicalPartition = logicalPartition;
        this.logicalOffset = logicalOffset;
        this.state = State.OPEN;
    }

    public String logicalTopic() {
        return logicalTopic;
    }

    public int logicalPartition() {
        return logicalPartition;
    }

    public long logicalOffset() {
        return logicalOffset;
    }

    State state() {
        return state;
    }

    void markCommitted() {
        state = State.COMMITTED;
    }

    void markRolledBack() {
        state = State.ROLLED_BACK;
    }
}
