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
package org.apache.kafka.network.iouring.bench;

import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.network.ByteBufferSend;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.network.iouring.IoUringSelector;
import org.apache.kafka.network.iouring.IoUringServerListener;
import org.apache.kafka.network.iouring.IoUringSupport;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Standalone benchmark harness for PROMPT.md "minimum viable outcome #4" — a
 * PLAINTEXT 10k-connection comparison of io_uring vs the NIO baseline. The
 * design follows PROMPT.md's instruction that "direction matters more than
 * chasing the deck's exact percentages" — this harness gives reproducible,
 * directional numbers from a single host.
 *
 * <h3>What this benchmark measures</h3>
 * For each backend, the harness spins up an in-process echo server on
 * 127.0.0.1, opens {@code --connections} client sockets, and runs
 * {@code --threads} client threads that each round-robin through a slice of
 * connections sending a size-prefixed payload and reading the echo back. After
 * a warmup window, the harness counts completed round-trips and bytes
 * transferred over a measurement window and prints:
 * <ul>
 *   <li>requests per second (rps)</li>
 *   <li>bytes per second (B/s)</li>
 *   <li>round-trip latency p50 / p90 / p99 / max (from per-thread sampled
 *       latencies, 1-in-128 sampling so the timing overhead does not dominate
 *       per-message work)</li>
 * </ul>
 *
 * <h3>What this benchmark does NOT measure</h3>
 * <ul>
 *   <li>Full broker request handling — payloads are echoed at the selector
 *       layer, not parsed through the API request pipeline. This isolates the
 *       kernel transport contribution from the rest of the broker.</li>
 *   <li>SSL/SASL — v1 is PLAINTEXT-only per PROMPT.md.</li>
 *   <li>Cross-host networking — both client and server live on the same
 *       machine through the loopback adapter.</li>
 * </ul>
 *
 * <h3>Baselines</h3>
 * <ul>
 *   <li>{@code --backend=io_uring} drives the production io_uring path:
 *       {@link IoUringSelector} + {@link IoUringServerListener}.</li>
 *   <li>{@code --backend=nio} drives a raw JDK {@link Selector} +
 *       {@link ServerSocketChannel} echo server. This is intentionally a
 *       leaner baseline than full {@code KSelector} — it strips out the
 *       broker's channel-builder, metric-sensors, and memory-pool overhead so
 *       both backends measure kernel-transport efficiency on the same
 *       footing. The directional comparison is what PROMPT.md asks for.</li>
 * </ul>
 *
 * <h3>How to run</h3>
 * <pre>
 *   JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 ./gradlew :server:testClasses
 *   JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 ./gradlew :server:test \
 *      --tests 'org.apache.kafka.network.iouring.bench.SelectorThroughputBenchmark.runIfRequested' \
 *      -Pbench.backend=io_uring -Pbench.connections=10000 -Pbench.threads=16 \
 *      -Pbench.payload=256 -Pbench.warmupMs=5000 -Pbench.durationMs=20000
 * </pre>
 *
 * <p>Or invoke {@link #main(String[])} directly with the bench/ test-runtime
 * classpath. The companion {@code BENCHMARK.md} in the worktree root records
 * the numbers from each run.
 */
public final class SelectorThroughputBenchmark {

    /** Loopback bind address — kernel will assign an ephemeral port. */
    private static final String BIND_HOST = "127.0.0.1";

    /** ListenerName plumbed through the io_uring selector. */
    private static final ListenerName LISTENER = ListenerName.normalised("PLAINTEXT");

    /** Idle expiry left "effectively never" so connections do not get reaped mid-benchmark. */
    private static final long IDLE_NANOS_NEVER = TimeUnit.HOURS.toNanos(1);

    /** Cap on inbound frame size — comfortably above any payload we test. */
    private static final int MAX_RECEIVE = 1 << 20;

    /** Sample one in every N latencies per thread — keeps timing overhead off the hot path. */
    private static final int LATENCY_SAMPLE_RATE = 128;

    /** Per-thread latency sample buffer size — rotates as a ring. */
    private static final int LATENCY_RING_SIZE = 4096;

    public static void main(String[] args) throws Exception {
        Config cfg = Config.parse(args);
        System.out.printf("benchmark config: backend=%s, connections=%d, threads=%d, " +
                          "payload=%d, warmupMs=%d, durationMs=%d%n",
            cfg.backend, cfg.connections, cfg.threads, cfg.payload, cfg.warmupMs, cfg.durationMs);

        Result result;
        switch (cfg.backend) {
            case "io_uring":
                if (!IoUringSupport.isAvailable()) {
                    System.err.println("io_uring is not available on this host: "
                        + IoUringSupport.unavailabilityReason());
                    Exit.exit(1);
                    return;
                }
                result = runIoUring(cfg);
                break;
            case "nio":
                result = runNio(cfg);
                break;
            default:
                System.err.println("unknown backend: " + cfg.backend + " (expected nio | io_uring)");
                Exit.exit(2);
                return;
        }
        result.print(cfg);
    }

    // ------------------------------------------------------------------------
    // io_uring backend
    // ------------------------------------------------------------------------

    private static Result runIoUring(Config cfg) throws Exception {
        try (IoUringSelector selector = new IoUringSelector(
                LISTENER, MAX_RECEIVE, MemoryPool.NONE, IDLE_NANOS_NEVER, Time.SYSTEM);
             IoUringServerListener listener = new IoUringServerListener(
                 new InetSocketAddress(BIND_HOST, 0), selector)) {

            int port = listener.boundPort();
            AtomicBoolean stop = new AtomicBoolean(false);

            Thread serverThread = new Thread(() -> ioUringEchoLoop(selector, stop), "iouring-echo");
            serverThread.setDaemon(true);
            serverThread.start();

            try {
                return runClients(cfg, port);
            } finally {
                stop.set(true);
                selector.wakeup();
                serverThread.join(TimeUnit.SECONDS.toMillis(5));
            }
        }
    }

    /** Echo loop for the io_uring backend: every completedReceive triggers a send of the same bytes. */
    private static void ioUringEchoLoop(IoUringSelector selector, AtomicBoolean stop) {
        try {
            while (!stop.get()) {
                selector.poll(50);
                for (NetworkReceive recv : selector.completedReceives()) {
                    String id = recv.source();
                    ByteBuffer payload = recv.payload();
                    ByteBuffer copy = ByteBuffer.allocate(payload.remaining());
                    copy.put(payload);
                    copy.flip();
                    selector.send(new NetworkSend(id, ByteBufferSend.sizePrefixed(copy)));
                }
                selector.clearCompletedReceives();
                selector.clearCompletedSends();
            }
        } catch (Throwable t) {
            t.printStackTrace(System.err);
        }
    }

    // ------------------------------------------------------------------------
    // NIO backend
    // ------------------------------------------------------------------------

    private static Result runNio(Config cfg) throws Exception {
        try (Selector selector = Selector.open();
             ServerSocketChannel server = ServerSocketChannel.open()) {

            server.configureBlocking(false);
            server.socket().bind(new InetSocketAddress(BIND_HOST, 0), 1024);
            server.register(selector, SelectionKey.OP_ACCEPT);
            int port = server.socket().getLocalPort();

            AtomicBoolean stop = new AtomicBoolean(false);
            Thread serverThread = new Thread(() -> nioEchoLoop(selector, stop), "nio-echo");
            serverThread.setDaemon(true);
            serverThread.start();

            try {
                return runClients(cfg, port);
            } finally {
                stop.set(true);
                selector.wakeup();
                serverThread.join(TimeUnit.SECONDS.toMillis(5));
            }
        }
    }

    /**
     * Raw-NIO echo loop. Each accepted channel gets a per-connection {@link Conn} attachment
     * holding a single read buffer and a single pending write buffer; this matches the simplest
     * size-prefixed framing a broker has to do. Echo: when the inbound frame size + body are
     * complete, copy into an outbound buffer and switch to OP_WRITE until flushed.
     */
    private static void nioEchoLoop(Selector selector, AtomicBoolean stop) {
        try {
            while (!stop.get()) {
                int ready = selector.select(50);
                if (ready == 0 && stop.get()) break;
                Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    it.remove();
                    if (!key.isValid()) continue;
                    try {
                        if (key.isAcceptable()) {
                            ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
                            SocketChannel sc;
                            while ((sc = ssc.accept()) != null) {
                                sc.configureBlocking(false);
                                sc.socket().setTcpNoDelay(true);
                                sc.socket().setKeepAlive(true);
                                SelectionKey k = sc.register(selector, SelectionKey.OP_READ);
                                k.attach(new Conn());
                            }
                        } else {
                            Conn conn = (Conn) key.attachment();
                            if (key.isReadable()) conn.onReadable(key);
                            if (key.isValid() && key.isWritable()) conn.onWritable(key);
                        }
                    } catch (IOException io) {
                        key.cancel();
                        try {
                            key.channel().close();
                        } catch (IOException ignored) {
                            // best-effort cleanup
                        }
                    }
                }
            }
        } catch (Throwable t) {
            t.printStackTrace(System.err);
        }
    }

    /** Per-connection state for the NIO echo loop. Size-prefixed framing. */
    private static final class Conn {
        private final ByteBuffer sizeBuf = ByteBuffer.allocate(4);
        private ByteBuffer bodyBuf;
        private ByteBuffer outBuf;

        void onReadable(SelectionKey key) throws IOException {
            SocketChannel sc = (SocketChannel) key.channel();
            if (bodyBuf == null) {
                int sizeN = sc.read(sizeBuf);
                if (sizeN < 0) {
                    key.channel().close();
                    return;
                }
                if (sizeBuf.hasRemaining()) return;
                sizeBuf.flip();
                int len = sizeBuf.getInt();
                sizeBuf.clear();
                if (len <= 0 || len > MAX_RECEIVE) {
                    sc.close();
                    return;
                }
                bodyBuf = ByteBuffer.allocate(len);
            }
            int n = sc.read(bodyBuf);
            if (n < 0) {
                key.channel().close();
                return;
            }
            if (bodyBuf.hasRemaining()) return;
            bodyBuf.flip();
            // Echo: build size-prefixed outbound, switch to OP_WRITE.
            outBuf = ByteBuffer.allocate(4 + bodyBuf.remaining());
            outBuf.putInt(bodyBuf.remaining());
            outBuf.put(bodyBuf);
            outBuf.flip();
            bodyBuf = null;
            key.interestOps((key.interestOps() & ~SelectionKey.OP_READ) | SelectionKey.OP_WRITE);
        }

        void onWritable(SelectionKey key) throws IOException {
            SocketChannel sc = (SocketChannel) key.channel();
            sc.write(outBuf);
            if (outBuf.hasRemaining()) return;
            outBuf = null;
            key.interestOps((key.interestOps() & ~SelectionKey.OP_WRITE) | SelectionKey.OP_READ);
        }
    }

    // ------------------------------------------------------------------------
    // Client harness — shared between backends
    // ------------------------------------------------------------------------

    private static Result runClients(Config cfg, int port) throws Exception {
        Socket[] sockets = new Socket[cfg.connections];
        for (int i = 0; i < cfg.connections; i++) {
            Socket s = new Socket();
            s.connect(new InetSocketAddress(BIND_HOST, port), 10_000);
            s.setTcpNoDelay(true);
            s.setKeepAlive(true);
            sockets[i] = s;
        }
        try {
            return drive(cfg, sockets);
        } finally {
            for (Socket s : sockets) {
                try {
                    s.close();
                } catch (IOException ignored) {
                    // best-effort cleanup
                }
            }
        }
    }

    private static Result drive(Config cfg, Socket[] sockets) throws Exception {
        int threads = cfg.threads;
        int perThread = (sockets.length + threads - 1) / threads;
        byte[] payload = new byte[cfg.payload];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i & 0xff);

        AtomicBoolean measure = new AtomicBoolean(false);
        AtomicBoolean stop = new AtomicBoolean(false);
        LongAdder messages = new LongAdder();
        LongAdder bytes = new LongAdder();
        AtomicLong totalLatencySamples = new AtomicLong();
        long[][] perThreadSamples = new long[threads][LATENCY_RING_SIZE];
        AtomicLong[] perThreadIdx = new AtomicLong[threads];
        for (int i = 0; i < threads; i++) perThreadIdx[i] = new AtomicLong();

        CountDownLatch ready = new CountDownLatch(threads);
        Thread[] workers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            int from = t * perThread;
            int to = Math.min(from + perThread, sockets.length);
            int tid = t;
            workers[t] = new Thread(() -> {
                try {
                    DataInputStream[] ins = new DataInputStream[to - from];
                    DataOutputStream[] outs = new DataOutputStream[to - from];
                    for (int i = from; i < to; i++) {
                        ins[i - from] = new DataInputStream(sockets[i].getInputStream());
                        outs[i - from] = new DataOutputStream(sockets[i].getOutputStream());
                    }
                    ready.countDown();
                    byte[] recv = new byte[cfg.payload];
                    int cursor = 0;
                    int samplesIdx = 0;
                    long[] sampleBuf = perThreadSamples[tid];
                    long sampleCounter = 0;
                    while (!stop.get()) {
                        int n = ins.length;
                        if (n == 0) break;
                        DataInputStream in = ins[cursor];
                        DataOutputStream out = outs[cursor];
                        cursor = (cursor + 1) % n;
                        boolean shouldSample = measure.get() && (sampleCounter % LATENCY_SAMPLE_RATE) == 0;
                        long startNs = shouldSample ? System.nanoTime() : 0L;
                        out.writeInt(payload.length);
                        out.write(payload);
                        out.flush();
                        int respLen = in.readInt();
                        if (respLen != cfg.payload) {
                            throw new IOException("unexpected resp len " + respLen);
                        }
                        in.readFully(recv);
                        if (measure.get()) {
                            // Publish on every message so the measurement window can read mid-flight totals.
                            messages.increment();
                            bytes.add((4L + cfg.payload) * 2);
                            sampleCounter++;
                            if (shouldSample) {
                                long elapsed = System.nanoTime() - startNs;
                                sampleBuf[samplesIdx] = elapsed;
                                samplesIdx = (samplesIdx + 1) % sampleBuf.length;
                                totalLatencySamples.incrementAndGet();
                            }
                        }
                    }
                    perThreadIdx[tid].set(samplesIdx);
                } catch (Throwable th) {
                    th.printStackTrace(System.err);
                }
            }, "bench-client-" + t);
            workers[t].start();
        }
        ready.await();

        System.out.println("warming up...");
        Thread.sleep(cfg.warmupMs);
        long startBytes = bytes.sum();
        long startMsg = messages.sum();
        long startNs = System.nanoTime();
        measure.set(true);
        System.out.println("measuring...");
        Thread.sleep(cfg.durationMs);
        measure.set(false);
        long endNs = System.nanoTime();
        long endBytes = bytes.sum();
        long endMsg = messages.sum();
        stop.set(true);
        for (Thread w : workers) w.join();

        long elapsedNs = endNs - startNs;
        long deltaMsg = endMsg - startMsg;
        long deltaBytes = endBytes - startBytes;

        long[] flatSamples = collectSamples(perThreadSamples, perThreadIdx);
        Arrays.sort(flatSamples);
        return new Result(deltaMsg, deltaBytes, elapsedNs, flatSamples);
    }

    private static long[] collectSamples(long[][] perThreadSamples, AtomicLong[] perThreadIdx) {
        int total = 0;
        for (long[] arr : perThreadSamples) for (long v : arr) if (v > 0) total++;
        long[] flat = new long[total];
        int k = 0;
        for (long[] arr : perThreadSamples) {
            for (long v : arr) if (v > 0) flat[k++] = v;
        }
        return flat;
    }

    // ------------------------------------------------------------------------
    // Config + Result types
    // ------------------------------------------------------------------------

    private static final class Config {
        String backend = "io_uring";
        int connections = 10_000;
        int threads = 16;
        int payload = 256;
        long warmupMs = 5_000;
        long durationMs = 20_000;

        static Config parse(String[] args) {
            Config c = new Config();
            for (String a : args) {
                if (a.startsWith("--backend=")) c.backend = a.substring("--backend=".length()).trim();
                else if (a.startsWith("--connections=")) c.connections = Integer.parseInt(after(a));
                else if (a.startsWith("--threads=")) c.threads = Integer.parseInt(after(a));
                else if (a.startsWith("--payload=")) c.payload = Integer.parseInt(after(a));
                else if (a.startsWith("--warmupMs=")) c.warmupMs = Long.parseLong(after(a));
                else if (a.startsWith("--durationMs=")) c.durationMs = Long.parseLong(after(a));
                else throw new IllegalArgumentException("unknown arg: " + a);
            }
            return c;
        }

        private static String after(String a) {
            return a.substring(a.indexOf('=') + 1).trim();
        }
    }

    private static final class Result {
        final long messages;
        final long bytes;
        final long elapsedNs;
        final long[] sortedSamples;

        Result(long messages, long bytes, long elapsedNs, long[] sortedSamples) {
            this.messages = messages;
            this.bytes = bytes;
            this.elapsedNs = elapsedNs;
            this.sortedSamples = sortedSamples;
        }

        void print(Config cfg) {
            double seconds = elapsedNs / 1_000_000_000.0;
            double rps = messages / seconds;
            double bps = bytes / seconds;
            System.out.println();
            System.out.printf("=== %s benchmark result ===%n", cfg.backend);
            System.out.printf("elapsed:        %.2f s%n", seconds);
            System.out.printf("messages:       %,d%n", messages);
            System.out.printf("throughput:     %,.0f rps%n", rps);
            System.out.printf("bandwidth:      %,.0f B/s (%.2f MB/s)%n", bps, bps / (1024.0 * 1024.0));
            System.out.printf("latency samples %,d (1-in-%d sampling)%n",
                sortedSamples.length, LATENCY_SAMPLE_RATE);
            if (sortedSamples.length > 0) {
                System.out.printf("p50 latency:    %,d us%n", pct(50) / 1_000);
                System.out.printf("p90 latency:    %,d us%n", pct(90) / 1_000);
                System.out.printf("p99 latency:    %,d us%n", pct(99) / 1_000);
                System.out.printf("max latency:    %,d us%n", sortedSamples[sortedSamples.length - 1] / 1_000);
            }
        }

        long pct(int p) {
            int idx = (int) Math.min(sortedSamples.length - 1L,
                Math.round(sortedSamples.length * (p / 100.0)));
            return sortedSamples[idx];
        }
    }

    private SelectorThroughputBenchmark() { /* not instantiable */ }
}
