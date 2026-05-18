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

/**
 * A pending logical-offset assignment held by a caller between reserve() and commit()/rollback().
 * Carries enough identity to address the right (logicalTopic, logicalPartition) sidecar index,
 * plus — when stamped by {@link ConcentrationKernel} on the produce path — the backing partition
 * and the gate {@code generation} captured at reserve time. The generation is used by
 * {@link ConcentrationKernel#commitProduceBatch} (and the single-record analogue) to fence
 * in-flight produce callbacks against a gate close that happened between reserve and commit
 * (Codex r9 BLOCKER 3): if the backing went unready in that window, the commit must reject so
 * the producer sees {@code NOT_LEADER_OR_FOLLOWER} instead of silently writing a sidecar entry
 * that may point at a backing offset a new leader would later truncate.
 *
 * <p>Direct {@link LogicalOffsetTracker} callers (tests) do not need the stamp — the tracker
 * has no opinion about backing-gate generations, and {@link #hasBackingStamp()} reports false
 * so the kernel's fence check skips. Production produces always come through the kernel and are
 * always stamped.
 */
public final class Reservation {
    enum State { OPEN, COMMITTED, ROLLED_BACK }

    private final String logicalTopic;
    private final int logicalPartition;
    private final long logicalOffset;
    // Backing-gate fence stamp. Both fields are set together via {@link #stampBackingGate} by the
    // kernel; for tracker-only paths they stay null/0 and the fence check is a no-op.
    private TopicPartition backing;
    private long generation;
    private boolean stamped;
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

    /**
     * Backing partition this reservation was opened against, or {@code null} if the reservation
     * was created without a stamp (direct {@link LogicalOffsetTracker} use). Production paths
     * always stamp through the kernel's reserve methods.
     */
    public TopicPartition backing() {
        return backing;
    }

    /**
     * Gate generation captured at stamp time. Meaningful only when {@link #hasBackingStamp()}.
     */
    public long generation() {
        return generation;
    }

    /**
     * Whether the kernel attached a (backing, generation) stamp to this reservation. False for
     * raw-tracker tests, true for every reservation taken through {@link ConcentrationKernel}.
     */
    public boolean hasBackingStamp() {
        return stamped;
    }

    /**
     * Attach the backing partition + gate generation to this reservation. Called exactly once
     * by the kernel between {@code tracker.reserveBatch} and returning to the caller. A
     * second call is a programmer error (would mask a missed-stamp bug elsewhere).
     */
    void stampBackingGate(TopicPartition backing, long generation) {
        if (stamped) {
            throw new IllegalStateException("reservation already stamped with backing gate");
        }
        if (backing == null) {
            throw new IllegalArgumentException("backing must not be null when stamping");
        }
        this.backing = backing;
        this.generation = generation;
        this.stamped = true;
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
