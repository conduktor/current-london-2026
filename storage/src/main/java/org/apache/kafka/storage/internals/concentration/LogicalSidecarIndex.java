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
 * <p>On crash mid-append the file may be inconsistent. v1 recovery rebuilds the sidecar from
 * the backing log when sanity checks fail; see {@link BackingScanRecoverer}.
 */
public final class LogicalSidecarIndex implements Closeable {

    private static final int ENTRY_SIZE = Long.BYTES;

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
        this.raf = new RandomAccessFile(file, "rw");
        this.channel = raf.getChannel();
        long length = channel.size();
        if (length % ENTRY_SIZE != 0) {
            // Tail-torn write left a partial entry — surface this as corruption so the caller
            // can trigger a rebuild from the backing log. CorruptIndexException is the local
            // idiom shared with OffsetIndex / TimeIndex; the sidecar is the same flavour of
            // artefact and should announce corruption the same way.
            throw new CorruptIndexException(
                "sidecar " + file + " size " + length + " is not a multiple of " + ENTRY_SIZE);
        }
        this.entries = length / ENTRY_SIZE;
        this.lastBackingOffset = entries > 0 ? readEntryAt(entries - 1) : -1L;
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
        buf.putLong(backingOffset).flip();
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
        return buf.getLong();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("sidecar " + file + " is closed");
        }
    }
}
