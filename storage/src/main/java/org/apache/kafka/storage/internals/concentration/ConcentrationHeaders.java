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
 * Record header keys used to mark a backing-topic record with its logical identity. Stamped on
 * every record by the produce path ({@link LogicalProduceStamper}) and read back by the fetch
 * path and by {@link BackingScanRecoverer} when rebuilding the sidecar index after a restart
 * without a durable index.
 *
 * <p>Names start with double-underscore to make collisions with user-supplied headers
 * statistically unlikely, mirroring the convention used by other internal Kafka metadata.
 */
public final class ConcentrationHeaders {
    private ConcentrationHeaders() { }

    /**
     * UTF-8 encoded logical-topic name. Required to demultiplex records from two logical topics
     * that share the same backing partition.
     */
    public static final String LOGICAL_TOPIC_HEADER = "__concentration_logical_topic";

    /**
     * 8-byte big-endian {@code long} carrying the per-logical-topic offset assigned to this
     * record by {@link LogicalOffsetTracker}. Stamped as a header so the recovery path can
     * rebuild the sidecar entirely from a backing-log scan when the durable sidecar is missing
     * or stale.
     */
    public static final String LOGICAL_OFFSET_HEADER = "__concentration_logical_offset";
}
