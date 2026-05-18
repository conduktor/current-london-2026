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
 * One record observed during a backing-log scan: a logical identity stamped on a physical
 * record by the produce path, plus the backing offset where it was found. The broker extracts
 * these from the headers of each backing record during recovery and streams them through
 * {@link BackingScanRecoverer} to rebuild sidecars.
 */
public record RecoveryRecord(
    String logicalTopic,
    int logicalPartition,
    long logicalOffset,
    long backingOffset
) {
    public RecoveryRecord {
        Objects.requireNonNull(logicalTopic, "logicalTopic");
        if (logicalOffset < 0) throw new IllegalArgumentException("logicalOffset < 0: " + logicalOffset);
        if (backingOffset < 0) throw new IllegalArgumentException("backingOffset < 0: " + backingOffset);
    }
}
