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

import org.apache.kafka.common.utils.Crc32C;
import org.apache.kafka.storage.internals.log.CorruptIndexException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;

/**
 * Per-(logicalTopic, logicalPartition) sidecar that maps every logical offset to its backing
 * offset. Logical offsets are positionally encoded — entry i is the backing offset for logical
 * offset i — so lookup is O(1) and no relative-offset / binary-search machinery is required.
 *
 * <p>Append-only at the tail; truncateTo() rolls the tail back during recovery. Writes are not
 * fsync'd per append (PROMPT explicitly forbids that — throughput collapse). Single-writer
 * model: the broker's per-partition produce serialisation already ensures only one thread
 * appends; reads from the fetch path are safe concurrently.
 *
 * <p>On-disk entry layout (Codex r19 ADV-STORAGE BLOCKER #132 — data-loss without per-entry CRC):
 * <pre>
 *   offset 0..7   : 8-byte big-endian backing offset
 *   offset 8..11  : 4-byte big-endian CRC32C of bytes 0..7
 * </pre>
 * A torn write (partial tail entry) is caught at open time by the {@code length % ENTRY_SIZE != 0}
 * check; a bit-flip inside an aligned 12-byte entry — undetectable from length alone — is caught
 * by the CRC mismatch on the next read. Without the CRC a corrupted backing offset survives the
 * length sanity check and silently delivers the wrong record (cross-tenant on a shared backing,
 * out-of-bounds on a single-tenant one). CRC32C is the same algorithm Kafka's record format uses
 * for its own data CRC, hardware-accelerated on x86 SSE 4.2 / ARMv8 CRC32 — adding it here is
 * 32 bits per entry on disk and a few cycles per append/lookup.
 *
 * <p>On crash mid-append the file may be inconsistent. v1 recovery rebuilds the sidecar from
 * the backing log when sanity checks fail; see {@link BackingScanRecoverer}.
 */
public final class LogicalSidecarIndex implements Closeable {

    /**
     * 8-byte backing offset + 4-byte CRC32C of those 8 bytes. The two fields together make every
     * entry self-verifying: a torn write that produces a half-written entry is caught by the
     * length-not-multiple-of-ENTRY_SIZE check at open time, and a bit-flip inside a full entry is
     * caught by the CRC mismatch at read time. Both surface as
     * {@link CorruptIndexException} so the {@link BackingScanRecoverer} backing-log scan fallback
     * can rebuild the sidecar from {@link ConcentrationHeaders}-stamped records.
     */
    static final int OFFSET_BYTES = Long.BYTES;
    static final int CRC_BYTES = Integer.BYTES;
    static final int ENTRY_SIZE = OFFSET_BYTES + CRC_BYTES;

    private final File file;
    private final String logicalTopic;
    private final int logicalPartition;
    private final RandomAccessFile raf;
    private final FileChannel channel;

    private long entries;
    private long lastBackingOffset;  // -1 when no entries
    private boolean closed;

    public LogicalSidecarIndex(File file, String logicalTopic, int logicalPartition) throws IOException {
        this.file = Objects.requireNonNull(file, "file");
        this.logicalTopic = Objects.requireNonNull(logicalTopic, "logicalTopic");
        this.logicalPartition = logicalPartition;
        RandomAccessFile rafLocal = new RandomAccessFile(file, "rw");
        try {
            FileChannel channelLocal = rafLocal.getChannel();
            long length = channelLocal.size();
            if (length % ENTRY_SIZE != 0) {
                // Tail-torn write left a partial entry — surface this as corruption so the caller
                // can trigger a rebuild from the backing log. CorruptIndexException is the local
                // idiom shared with OffsetIndex / TimeIndex; the sidecar is the same flavour of
                // artefact and should announce corruption the same way.
                throw new CorruptIndexException(
                    "sidecar " + file + " size " + length + " is not a multiple of " + ENTRY_SIZE);
            }
            this.entries = length / ENTRY_SIZE;
            // readEntryAt() touches the channel field, so commit raf/channel first; do it last so
            // an exception above closes raf without ever publishing the handles.
            this.raf = rafLocal;
            this.channel = channelLocal;
            this.lastBackingOffset = entries > 0 ? readEntryAt(entries - 1) : -1L;
        } catch (IOException | RuntimeException e) {
            try {
                rafLocal.close();
            } catch (IOException closeError) {
                e.addSuppressed(closeError);
            }
            throw e;
        }
    }

    public String logicalTopic() {
        return logicalTopic;
    }

    public int logicalPartition() {
        return logicalPartition;
    }

    public File file() {
        return file;
    }

    public synchronized long size() {
        return entries;
    }

    public synchronized long nextLogicalOffset() {
        return entries;
    }

    public synchronized void append(long backingOffset) throws IOException {
        ensureOpen();
        if (backingOffset <= lastBackingOffset) {
            throw new IllegalArgumentException(
                "backingOffset " + backingOffset + " is not strictly greater than last appended "
                    + lastBackingOffset);
        }
        ByteBuffer buf = ByteBuffer.allocate(ENTRY_SIZE);
        buf.putLong(backingOffset);
        // CRC32C over the 8 backing-offset bytes only. The CRC sits in the entry's tail so a
        // half-written entry surfaces as a length-not-multiple-of-ENTRY_SIZE failure at open time
        // (caught before any read happens); a bit-flip in either field surfaces as a CRC mismatch
        // at read time. The two cases together cover every silent-corruption mode that produces a
        // wrong logical→backing mapping on the fetch path.
        long crc = Crc32C.compute(buf.array(), 0, OFFSET_BYTES);
        buf.putInt((int) crc).flip();
        long position = entries * ENTRY_SIZE;
        while (buf.hasRemaining()) {
            int written = channel.write(buf, position + (ENTRY_SIZE - buf.remaining()));
            if (written < 0) {
                throw new IOException("unexpected short write to " + file);
            }
        }
        entries++;
        lastBackingOffset = backingOffset;
    }

    public long lookup(long logicalOffset) throws IOException {
        long entriesNow;
        synchronized (this) {
            ensureOpen();
            entriesNow = entries;
        }
        if (logicalOffset < 0 || logicalOffset >= entriesNow) {
            throw new IndexOutOfBoundsException(
                "logicalOffset " + logicalOffset + " out of [0, " + entriesNow + ")");
        }
        return readEntryAt(logicalOffset);
    }

    public synchronized void truncateTo(long newSize) throws IOException {
        ensureOpen();
        if (newSize < 0 || newSize > entries) {
            throw new IllegalArgumentException(
                "newSize " + newSize + " out of [0, " + entries + "]");
        }
        channel.truncate(newSize * ENTRY_SIZE);
        entries = newSize;
        lastBackingOffset = entries > 0 ? readEntryAt(entries - 1) : -1L;
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) return;
        closed = true;
        channel.close();
        raf.close();
    }

    private long readEntryAt(long index) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(ENTRY_SIZE);
        long position = index * ENTRY_SIZE;
        while (buf.hasRemaining()) {
            int read = channel.read(buf, position + (ENTRY_SIZE - buf.remaining()));
            if (read < 0) {
                throw new IOException("unexpected EOF at sidecar " + file + " position " + position);
            }
        }
        buf.flip();
        long backingOffset = buf.getLong();
        int storedCrc = buf.getInt();
        // Recompute the CRC over the just-read 8 backing-offset bytes and compare. A mismatch
        // means either the stored backing-offset bytes or the stored CRC itself were corrupted
        // since the entry was written (bit-rot, partial-page write the OS reported as complete,
        // misdirected write from a buggy storage stack). Either way the on-disk mapping cannot
        // be trusted — silently returning {@code backingOffset} would route a fetch to the wrong
        // physical record, which on a shared backing means delivering another logical topic's
        // bytes to the consumer of this one. CorruptIndexException is the local idiom shared with
        // OffsetIndex / TimeIndex; surfacing it tells {@link BackingScanRecoverer} the sidecar
        // must be rebuilt from the backing log.
        long expectedCrc = Crc32C.compute(buf.array(), 0, OFFSET_BYTES);
        if ((int) expectedCrc != storedCrc) {
            throw new CorruptIndexException("sidecar " + file + " entry " + index + " failed CRC "
                + "check (stored=" + Integer.toUnsignedString(storedCrc) + ", computed="
                + Integer.toUnsignedString((int) expectedCrc) + "); backing-offset bytes or CRC "
                + "field corrupted on disk — sidecar must be rebuilt from backing log");
        }
        return backingOffset;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("sidecar " + file + " is closed");
        }
    }
}
