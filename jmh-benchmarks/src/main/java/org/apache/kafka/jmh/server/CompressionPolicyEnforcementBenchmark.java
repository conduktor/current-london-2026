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
package org.apache.kafka.jmh.server;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.storage.internals.log.CompressionPolicy;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Micro-benchmark for the broker-side `compression.policy` enforcement loop in
 * `KafkaApis.handleProduceRequest`.
 *
 * The production loop in Scala is:
 * {@code
 *   val policy = replicaManager.compressionPolicy(tp)
 *   if (policy eq CompressionPolicy.NONE) return
 *   records.batches.forEach(batch =>
 *     if (policy.isViolatedBy(batch.compressionType)) throw new InvalidRecordException(...))
 * }
 *
 * This benchmark mirrors that shape in Java to characterise the steady-state cost of two scenarios
 * called out in the PROMPT.md stretch backlog:
 *
 *  1. Fast path: `compression.policy=none` (the default for every existing topic) — the call must
 *     return without inspecting any batch. The bench pins that this branch is essentially free
 *     regardless of how many batches the produce request carries.
 *
 *  2. Hot path: `compression.policy=required` with compressed batches, and
 *     `compression.policy=forbidden` with uncompressed batches — these scan every batch in the
 *     produce request. The cost should scale linearly with `batchCount` and stay in the low-ns
 *     range per batch.
 *
 * The benchmark intentionally does NOT measure the rejection (throw) path: an exception is not
 * the steady-state cost we are trying to characterise; rejection is documented to be rare by
 * design ("policy violation" implies operator-misconfigured producers, not normal traffic).
 *
 * The state matrix exposes `batchCount` as the only varying axis, with 4 representative points
 * spanning a single batch (tiny produce request) to 200 (a saturated produce request from a
 * batched producer).
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class CompressionPolicyEnforcementBenchmark {

    @Param({"1", "10", "50", "200"})
    private int batchCount;

    private MemoryRecords compressedBatches;
    private MemoryRecords uncompressedBatches;

    @Setup
    public void setup() {
        compressedBatches = buildBatches(Compression.lz4().build(), batchCount);
        uncompressedBatches = buildBatches(Compression.NONE, batchCount);
    }

    /**
     * Fast path: `compression.policy=none`. The enforcement call must return immediately without
     * walking the batches iterator. We pass the uncompressed payload to make the bench symmetric
     * with the REQUIRED/FORBIDDEN benches (same heap layout, same record count) and to prove the
     * `eq NONE` short-circuit is what dominates — not whether the records happen to be compressed.
     */
    @Benchmark
    public boolean fastPathNonePolicy() {
        return enforce(CompressionPolicy.NONE, uncompressedBatches);
    }

    /**
     * Hot path: `compression.policy=required` with already-compressed batches (the steady-state
     * accept path). The loop visits every batch, reads `batch.compressionType()`, and the policy
     * returns `false` (no violation) for every batch.
     */
    @Benchmark
    public boolean requiredPolicyHotPath() {
        return enforce(CompressionPolicy.REQUIRED, compressedBatches);
    }

    /**
     * Hot path: `compression.policy=forbidden` with uncompressed batches (the steady-state accept
     * path). Mirror image of {@link #requiredPolicyHotPath()} — same loop shape, same per-batch
     * work, just a different policy/compression combination on the accept side.
     */
    @Benchmark
    public boolean forbiddenPolicyHotPath() {
        return enforce(CompressionPolicy.FORBIDDEN, uncompressedBatches);
    }

    /**
     * Inlined Java mirror of the production Scala loop in `KafkaApis.enforceCompressionPolicy`.
     * Returns true on accept, false on reject (the prod code throws on reject; the bench does
     * not exercise the throw path — see class Javadoc).
     */
    private static boolean enforce(CompressionPolicy policy, MemoryRecords records) {
        if (policy == CompressionPolicy.NONE) {
            return true;
        }
        for (RecordBatch batch : records.batches()) {
            CompressionType batchCompression = batch.compressionType();
            if (policy.isViolatedBy(batchCompression)) {
                return false;
            }
        }
        return true;
    }

    private static MemoryRecords buildBatches(Compression compression, int batches) {
        SimpleRecord[] records = new SimpleRecord[batches];
        byte[] payload = "compression-policy-bench".getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < batches; i++) {
            records[i] = new SimpleRecord(100L + i, payload);
        }
        // MemoryRecords.withRecords packs every SimpleRecord into a single RecordBatch when the
        // compression codec is shared. To force `batchCount` distinct batches we concatenate
        // single-record MemoryRecords instead.
        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(estimateSize(compression, payload, batches));
        for (int i = 0; i < batches; i++) {
            MemoryRecords single = MemoryRecords.withRecords(i, compression, records[i]);
            buffer.put(single.buffer().duplicate());
        }
        buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }

    private static int estimateSize(Compression compression, byte[] payload, int batches) {
        // Conservative upper bound: each batch header is ~ 60 bytes, plus the payload, plus a
        // codec-specific overhead. 256 bytes per batch is enough headroom for short payloads,
        // and over-allocating in the bench setup is harmless (the buffer is flipped before use).
        int payloadOverhead = compression.type() == CompressionType.NONE ? payload.length : payload.length + 32;
        return batches * (96 + payloadOverhead);
    }
}
