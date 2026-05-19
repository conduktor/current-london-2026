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
package org.apache.kafka.server.views;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ViewSpec} — focused on the runtime defense-in-depth gate that rejects
 * reserved Kafka-internal topics as view backings, independent of the controller-side
 * configuration validator. The gate exists because metadata-log replay paths
 * ({@code ConfigurationControlManager.replay}, {@code ConfigurationDelta.replay}) do not run
 * the validator: an upgraded broker replaying a pre-fix ConfigRecord, or a direct metadata-log
 * write that bypasses the controller, would otherwise produce a {@code ViewSpec} bound to a
 * coordinator-managed or cluster-internal topic. {@code KafkaApis} fetch redirect skips the
 * backing-ACL check by design, so this is the last line of defense before serving raw
 * coordinator state to any principal granted READ on the view.
 */
class ViewSpecTest {

    private static final CompiledPredicate ALWAYS_TRUE =
            new PredicateCompiler(PredicateLimits.defaults()).compile("true");

    @Test
    void isReservedInternalBackingCoversCoordinatorTopics() {
        assertTrue(ViewSpec.isReservedInternalBacking("__consumer_offsets"));
        assertTrue(ViewSpec.isReservedInternalBacking("__transaction_state"));
        assertTrue(ViewSpec.isReservedInternalBacking("__share_group_state"));
    }

    @Test
    void isReservedInternalBackingCoversClusterAndRemoteLogMetadata() {
        assertTrue(ViewSpec.isReservedInternalBacking("__cluster_metadata"));
        assertTrue(ViewSpec.isReservedInternalBacking("__remote_log_metadata"));
    }

    @Test
    void isReservedInternalBackingTreatsUserTopicsAsAllowed() {
        assertFalse(ViewSpec.isReservedInternalBacking("orders-raw"));
        assertFalse(ViewSpec.isReservedInternalBacking("__user_chose_underscores"),
                "user topics that happen to start with '__' must not be rejected");
        assertFalse(ViewSpec.isReservedInternalBacking(""),
                "empty string is not a reserved internal name");
        assertFalse(ViewSpec.isReservedInternalBacking(null),
                "null is handled defensively (not internal, but constructor rejects via NPE)");
    }

    @Test
    void constructorRejectsCoordinatorManagedBackings() {
        for (String backing : new String[]{
            "__consumer_offsets", "__transaction_state", "__share_group_state"}) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> new ViewSpec("view", backing, ALWAYS_TRUE,
                            ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE),
                    "must reject coordinator-managed backing: " + backing);
            assertTrue(e.getMessage().contains(backing),
                    "error must name the rejected backing, got: " + e.getMessage());
            assertTrue(e.getMessage().contains("reserved Kafka-internal"),
                    "error must explain why, got: " + e.getMessage());
        }
    }

    @Test
    void constructorRejectsClusterMetadataBacking() {
        assertThrows(IllegalArgumentException.class,
                () -> new ViewSpec("view", "__cluster_metadata", ALWAYS_TRUE,
                        ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE));
    }

    @Test
    void constructorRejectsRemoteLogMetadataBacking() {
        assertThrows(IllegalArgumentException.class,
                () -> new ViewSpec("view", "__remote_log_metadata", ALWAYS_TRUE,
                        ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE));
    }

    @Test
    void constructorAcceptsUserBacking() {
        ViewSpec spec = new ViewSpec("red-orders", "orders-raw", ALWAYS_TRUE,
                ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE);
        assertEquals("orders-raw", spec.backingTopic(),
                "regular user backing must be accepted by the runtime gate");
    }

    @Test
    void selfLoopStillRejectedAheadOfInternalCheck() {
        // The self-loop gate runs first (and produces a self-loop-specific error message)
        // for the case where a user topic happens to share its own name. Mainly a regression
        // guard: if someone reorders the checks, the existing self-loop message must remain.
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new ViewSpec("user-topic", "user-topic", ALWAYS_TRUE,
                        ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE));
        assertTrue(e.getMessage().contains("cannot back itself"),
                "self-loop must keep its precise error message; got: " + e.getMessage());
    }
}
