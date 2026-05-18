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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
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

    /**
     * Sibling file to the sidecar carrying the partition's logical {@code startOffset}. Written by
     * {@link #persistStartOffset} on DeleteRecords (so the advance is durable) and read on restart
     * by {@link #recoverFromSidecars} / {@link #recoverFromScan} (so the advance survives a bounce).
     *
     * <p>Why a separate file rather than a sidecar header: the sidecar's binary format is "8-byte
     * backing offset, positionally indexed by logical offset", and a header would shift every entry
     * by the header size — every existing test and the on-the-wire fetch translator's offset math
     * would have to be reworked. A tiny sibling file keeps the sidecar shape pristine and isolates
     * the durability fix to one new code path. The two files do not need to be coherent in real
     * time: the sidecar grows on every produce, the start-offset file mutates only on DeleteRecords.
     */
    public File startOffsetFile(String logicalTopic, int logicalPartition) {
        File topicDir = new File(sidecarDir, logicalTopic);
        if (!topicDir.isDirectory() && !topicDir.mkdirs()) {
            throw new IllegalStateException("could not create topic dir " + topicDir);
        }
        return new File(topicDir, logicalPartition + ".startoffset");
    }

    public LogicalSidecarIndex openSidecar(String logicalTopic, int logicalPartition) throws IOException {
        return new LogicalSidecarIndex(sidecarFile(logicalTopic, logicalPartition), logicalTopic, logicalPartition);
    }

    /**
     * Atomically persist the new logical {@code startOffset} for a partition. Writes 8 big-endian
     * bytes to a temp file, fsyncs, then renames over the real file with {@link
     * StandardCopyOption#ATOMIC_MOVE}. After this call returns, a crash + restart will read the
     * same value — without it, DeleteRecords would silently regress on every broker bounce.
     *
     * <p>fsync on this path is acceptable per PROMPT line 10 ("Do not fsync per-append on small
     * segments — throughput collapses"). DeleteRecords is a rare, low-volume admin operation, not
     * the hot produce path. Mirroring an offset-index flush cadence here would be incorrect — we
     * MUST be durable before returning success to the operator, otherwise a crash between
     * acknowledgement and disk-write leaves the cluster reporting "records deleted" while the
     * tracker says otherwise on the next restart.
     */
    public void persistStartOffset(String logicalTopic, int logicalPartition, long startOffset) throws IOException {
        if (startOffset < 0) {
            throw new IllegalArgumentException("startOffset must be non-negative, got " + startOffset);
        }
        File target = startOffsetFile(logicalTopic, logicalPartition);
        File tmp = new File(target.getPath() + ".tmp");
        try (FileChannel ch = FileChannel.open(tmp.toPath(),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buf = ByteBuffer.allocate(8).putLong(startOffset);
            buf.flip();
            while (buf.hasRemaining()) {
                ch.write(buf);
            }
            // Force file contents AND metadata before rename so the post-rename file is durable on
            // crash. Without this the rename can land in the directory entry while the 8 bytes are
            // still in the page cache; recovery would then read whatever zero-pad the filesystem
            // chose, which is exactly the bug we're trying to prevent.
            ch.force(true);
        }
        Files.move(tmp.toPath(), target.toPath(),
            StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * Read the persisted startOffset for a partition, or return 0 if the file does not exist.
     * "Not existing" means "DeleteRecords was never called on this partition" — the default of 0
     * matches an in-memory tracker that's never seen advanceStartOffset.
     */
    public long readStartOffset(String logicalTopic, int logicalPartition) throws IOException {
        File f = startOffsetFile(logicalTopic, logicalPartition);
        if (!f.isFile()) return 0L;
        try (FileChannel ch = FileChannel.open(f.toPath(), StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocate(8);
            int read = 0;
            while (read < 8) {
                int n = ch.read(buf);
                if (n < 0) {
                    // Truncated. A torn write here is a recovery problem we cannot resolve safely
                    // — the partition's true startOffset is now ambiguous. Surface as IOException
                    // so the operator sees the corruption rather than the broker silently regressing
                    // to a stale value.
                    throw new IOException("truncated startOffset file " + f + " (read " + read + " of 8)");
                }
                read += n;
            }
            buf.flip();
            long value = buf.getLong();
            if (value < 0) {
                throw new IOException("invalid startOffset " + value + " in " + f);
            }
            return value;
        }
    }

    /**
     * Cheap startup. Open each sidecar, read its size, seed the tracker. No backing-log scan.
     *
     * <p>The persisted {@code startOffset} from {@link #readStartOffset} is forwarded into the
     * tracker — without this, a broker bounce after DeleteRecords would silently regress the
     * partition's start offset back to 0 and re-expose every "deleted" record to consumers.
     */
    public void recoverFromSidecars(Collection<LogicalPartition> partitions, LogicalOffsetTracker tracker) throws IOException {
        Objects.requireNonNull(tracker, "tracker");
        for (LogicalPartition p : partitions) {
            try (LogicalSidecarIndex sidecar = openSidecar(p.logicalTopic(), p.logicalPartition())) {
                long persistedStart = readStartOffset(p.logicalTopic(), p.logicalPartition());
                long size = sidecar.size();
                // A startOffset persisted past the sidecar's last entry would violate
                // restorePartition's invariant (startOffset <= nextOffset). Clamp to the sidecar
                // size and surface as an exception rather than silently corrupting — this case
                // only happens if the .startoffset and .sidecar files are mutually inconsistent
                // (e.g. the sidecar was truncated by a concurrent recovery while .startoffset
                // wasn't). The operator should investigate, not have us paper over it.
                if (persistedStart > size) {
                    throw new IOException("startOffset " + persistedStart + " > sidecar size "
                        + size + " for " + p);
                }
                tracker.restorePartition(p.logicalTopic(), p.logicalPartition(), persistedStart, size);
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
                LogicalSidecarIndex sidecar = open.get(key);
                if (sidecar == null) {
                    // Open + truncate must be one transaction wrt the open map. If truncateTo
                    // throws after the sidecar handle has been allocated but before we register
                    // it, close it inline — otherwise the file descriptor leaks past the finally.
                    LogicalSidecarIndex fresh = openSidecar(r.logicalTopic(), r.logicalPartition());
                    try {
                        fresh.truncateTo(0);
                    } catch (IOException | RuntimeException e) {
                        try {
                            fresh.close();
                        } catch (IOException ignored) {
                            // original exception takes precedence
                        }
                        throw e;
                    }
                    open.put(key, fresh);
                    sidecar = fresh;
                }
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
                // Same rationale as recoverFromSidecars: a persisted startOffset must survive a
                // full backing-log scan too. The scan rebuilds the sidecar from records on disk
                // but the start-offset file is independent — losing it here would re-expose
                // "deleted" records the moment the broker decided to take the scan path
                // (e.g. corrupt sidecar triggered a rebuild).
                long persistedStart = readStartOffset(key.logicalTopic(), key.logicalPartition());
                long size = sidecar.size();
                if (persistedStart > size) {
                    throw new IOException("startOffset " + persistedStart + " > rebuilt sidecar "
                        + "size " + size + " for " + key);
                }
                tracker.restorePartition(key.logicalTopic(), key.logicalPartition(), persistedStart, size);
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
