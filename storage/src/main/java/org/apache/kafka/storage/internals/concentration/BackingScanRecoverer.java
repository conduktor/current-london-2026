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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * Rebuilds tracker state and sidecar files on broker restart.
 *
 * <p>Two paths, both required by the PROMPT acceptance criteria:
 * <ul>
 *   <li>{@link #recoverFromSidecars} — cheap startup when sidecars are intact: just open them
 *       and seed the tracker with their last logical offsets.</li>
 *   <li>{@link #recoverFromScan} — full rebuild when sidecars are missing or corrupt: stream
 *       through backing-log records (which the broker has stamped with
 *       <code>(logicalTopic, logicalPartition, logicalOffset)</code> headers at produce time),
 *       and reconstruct sidecars + tracker.</li>
 * </ul>
 *
 * <p>Sidecar files live under <code>sidecarDir/&lt;logicalTopic&gt;/&lt;partition&gt;.sidecar</code>.
 * The per-topic subdirectory makes the file naming unambiguous for any legal topic name and
 * sidesteps name-encoding concerns at the cost of a few extra inodes.
 */
public final class BackingScanRecoverer {

    private final File sidecarDir;

    public BackingScanRecoverer(File sidecarDir) {
        this.sidecarDir = Objects.requireNonNull(sidecarDir, "sidecarDir");
        if (!sidecarDir.isDirectory() && !sidecarDir.mkdirs()) {
            throw new IllegalArgumentException("could not create sidecarDir " + sidecarDir);
        }
    }

    public File sidecarFile(String logicalTopic, int logicalPartition) {
        File topicDir = new File(sidecarDir, logicalTopic);
        if (!topicDir.isDirectory() && !topicDir.mkdirs()) {
            throw new IllegalStateException("could not create topic dir " + topicDir);
        }
        return new File(topicDir, logicalPartition + ".sidecar");
    }

    public LogicalSidecarIndex openSidecar(String logicalTopic, int logicalPartition) throws IOException {
        return new LogicalSidecarIndex(sidecarFile(logicalTopic, logicalPartition), logicalTopic, logicalPartition);
    }

    /**
     * Cheap startup. Open each sidecar, read its size, seed the tracker. No backing-log scan.
     */
    public void recoverFromSidecars(Collection<LogicalPartition> partitions, LogicalOffsetTracker tracker) throws IOException {
        Objects.requireNonNull(tracker, "tracker");
        for (LogicalPartition p : partitions) {
            try (LogicalSidecarIndex sidecar = openSidecar(p.logicalTopic(), p.logicalPartition())) {
                tracker.restorePartition(p.logicalTopic(), p.logicalPartition(), 0L, sidecar.size());
            }
        }
    }

    /**
     * Full rebuild. Streams through {@link RecoveryRecord}s — one per record observed on the
     * backing log in backing-offset order — and reconstructs sidecars + tracker. Detects
     * corruption: any logical-offset gap or regression for the same (topic, partition) is
     * surfaced as {@link IllegalStateException} rather than silently swallowed.
     *
     * <p>Sidecar files for the partitions present in the stream are truncated to zero before
     * the scan begins so that a partial rebuild does not splice with stale tail content.
     */
    public void recoverFromScan(Iterator<RecoveryRecord> stream, LogicalOffsetTracker tracker) throws IOException {
        Objects.requireNonNull(stream, "stream");
        Objects.requireNonNull(tracker, "tracker");
        Map<LogicalPartition, LogicalSidecarIndex> open = new HashMap<>();
        try {
            while (stream.hasNext()) {
                RecoveryRecord r = stream.next();
                LogicalPartition key = new LogicalPartition(r.logicalTopic(), r.logicalPartition());
                LogicalSidecarIndex sidecar = open.computeIfAbsent(key, k -> {
                    try {
                        LogicalSidecarIndex s = openSidecar(k.logicalTopic(), k.logicalPartition());
                        s.truncateTo(0);
                        return s;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                long expected = sidecar.size();
                if (r.logicalOffset() != expected) {
                    throw new IllegalStateException(
                        "logical-offset discontinuity for " + key
                            + ": expected " + expected + ", got " + r.logicalOffset());
                }
                sidecar.append(r.backingOffset());
            }
            for (Map.Entry<LogicalPartition, LogicalSidecarIndex> e : open.entrySet()) {
                LogicalPartition key = e.getKey();
                LogicalSidecarIndex sidecar = e.getValue();
                tracker.restorePartition(key.logicalTopic(), key.logicalPartition(), 0L, sidecar.size());
            }
        } finally {
            for (LogicalSidecarIndex sidecar : open.values()) {
                try {
                    sidecar.close();
                } catch (IOException ignored) {
                    // best-effort close; the recovery exception (if any) takes precedence
                }
            }
        }
    }
}
