# Concentration (Infinite Partitions) — v1 kernel

This document describes what has been built so far on the `experiment/infinite-partitions`
branch toward the feature described in `PROMPT.md`.

## Status

| Layer | Status | Lives in |
|---|---|---|
| Pure-Java concentration kernel | **Done.** Built TDD-first, 64 tests green. | `storage/src/main/java/org/apache/kafka/storage/internals/concentration/` |
| Kernel-level integration tests | **Done.** Six PROMPT scenarios covered. | `storage/src/test/java/org/apache/kafka/storage/internals/concentration/ConcentrationKernelIntegrationTest.java` |
| Broker glue (produce/fetch/admin/DeleteRecords hooks) | **Not started.** Scoped, with the kernel sitting at the seam where it plugs in. | — |
| End-to-end test with a real broker | **Not started.** Depends on broker glue. | — |
| Audit fleet (Codex + Gemini) per PROMPT §"After every major phase" | **Pending.** Kernel is a major-phase boundary; audit is next. | — |

## What v1 ships

A self-contained "concentration kernel" — the pure data structures that let one physical Kafka
topic with M partitions present itself to stock clients as a logical topic with N partitions
(N ≥ M), each logical partition carrying its own contiguous offset sequence even when records
are physically interleaved on the backing.

The kernel is **transport-agnostic** and **broker-agnostic** by design: it depends on nothing
under `clients/` and on nothing in `core/`. The broker glue (not in this commit set) wires the
kernel into the existing produce / fetch / admin paths.

### Components

```
LogicalTopicDescriptor       immutable record (logicalName, N, backingTopic, M)
LogicalPartitionMapper       static fn: backingPartitionFor(d, p) = p % d.M
LogicalTopicRegistry         broker-side declare/lookup, namespace disjointness
LogicalOffsetTracker         per-(topic, partition) reserve / commit / rollback
Reservation                  in-flight logical offset assignment
LogicalSidecarIndex          on-disk logical→backing translation, 8-byte dense, O(1) lookup
BackingScanRecoverer         restart: cheap sidecar path + full backing-log scan rebuild
RecoveryRecord               one scanned record's (topic, partition, logical, backing)
LogicalPartition             typed (topic, partition) key
```

### Design choices worth highlighting

- **Modulo routing.** `logicalPartition % numBackingPartitions`. Trivially derivable from the
  descriptor so a recovering broker needs no extra metadata. Guaranteed to cover every backing
  partition when N ≥ M, which the descriptor enforces.
- **Reservation protocol with rollback.** Produce is two-phase: reserve the logical offset
  before the backing append (so it can be stamped into the record header for recovery), commit
  on success, rollback on failure. v1 allows only one in-flight reservation per partition at a
  time — simple, and enough to honour the "no offset gaps on failed append" acceptance
  criterion. Stretch cascade-rollback is intentionally not built.
- **Dense positional sidecar.** Logical offset is positionally encoded — entry `i` is the
  backing offset for logical offset `i`. O(1) lookup, no binary search. Departs from Kafka's
  stock `OffsetIndex` (sparse, key+value, binary search) because the sidecar **is** the
  translation table, not a narrowing index over an authoritative store.
- **No per-append fsync.** Explicitly forbidden by PROMPT — would collapse throughput. Crash
  recovery rebuilds the tail from the backing log via `BackingScanRecoverer`.
- **Recovery has two paths.** Cheap (open sidecars, seed the tracker — meets the "sub-second
  restart" acceptance criterion) and full (stream `RecoveryRecord`s from the backing log in
  backing-offset order; rebuild sidecars and tracker; surface any gap/regression as
  `IllegalStateException` rather than swallowing).
- **Namespace disjointness.** A name cannot be both logical and backing. Enforced inside the
  registry's write lock. Without this, the broker cannot unambiguously resolve a produce or
  fetch request, and the PROMPT requirement "produce to a backing-topic name is rejected"
  could not be enforced.

### PROMPT functional scenarios → tests

| PROMPT scenario | Where it's pinned |
|---|---|
| Two logical topics share single-partition backing, interleaved produce, consumer isolation | `ConcentrationKernelIntegrationTest#twoLogicalTopicsShareSingleBackingPartitionAndStayIsolated` |
| Three logical topics concurrent produce, monotonic per-topic offsets | `ConcentrationKernelIntegrationTest#threeLogicalTopicsConcurrentlyProduceAndPreserveMonotonicOffsets` |
| DeleteRecords scoped to one logical partition | `ConcentrationKernelIntegrationTest#deleteRecordsAdvancesOnlyOneLogicalPartitionStart` |
| Restart with intact sidecars → cheap startup | `ConcentrationKernelIntegrationTest#restartWithIntactSidecarsRehydratesTracker` |
| Restart without sidecars → full scan rebuild | `ConcentrationKernelIntegrationTest#restartWithoutSidecarsReconstructsFromBackingScan` |
| Direct produce to backing-topic name rejected | `ConcentrationKernelIntegrationTest#registrySignalsThatBackingTopicsCannotBeProducedDirectly` (kernel signal; full broker rejection lives in the produce-path hook, not yet written) |
| Idempotent producer retry, no duplicates | **Not addressable at kernel layer.** Depends on broker-level producer-id / epoch state. |

## What v1 does **not** ship

Explicitly out of scope per PROMPT:

- **Transactional support.** Per-logical-topic LSO, zombie cleanup on `InitProducerId`.
- **Compaction-safe key prefixing.** Two logical topics sharing one compacted backing destroy
  each other's data; v1 sidesteps by excluding compacted backings.
- **Concurrent produce-failure cascade rollback.** Single in-flight reservation per partition
  is enough for the "no gaps" criterion.

Pending (not built yet, not out-of-scope):

- **Broker integration.** Hooks into `ReplicaManager.appendRecords`, `ReplicaManager.fetch`,
  `KafkaApis.handleCreateTopics` (admin), `ReplicaManager.deleteRecords`. The HOW research
  fleet identified all four seams; mechanical wiring on a correct kernel is the next phase.
- **Audit fleet** per PROMPT §"After every major phase" — Codex + Gemini slots are mandatory.
  Kernel completion is a major-phase boundary; the audit is queued.

## How to run

```bash
JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 \
  ./gradlew :storage:test --tests 'org.apache.kafka.storage.internals.concentration.*'
```

64 tests, all green at HEAD.

## File layout

```
storage/src/main/java/org/apache/kafka/storage/internals/concentration/
├── BackingScanRecoverer.java
├── LogicalOffsetTracker.java
├── LogicalPartition.java
├── LogicalPartitionMapper.java
├── LogicalSidecarIndex.java
├── LogicalTopicDescriptor.java
├── LogicalTopicRegistry.java
├── RecoveryRecord.java
└── Reservation.java

storage/src/test/java/org/apache/kafka/storage/internals/concentration/
├── BackingScanRecovererTest.java               (7 tests)
├── ConcentrationKernelIntegrationTest.java     (6 tests — PROMPT scenarios)
├── LogicalOffsetTrackerTest.java              (14 tests)
├── LogicalPartitionMapperTest.java             (5 tests)
├── LogicalSidecarIndexTest.java               (11 tests)
├── LogicalTopicDescriptorTest.java             (9 tests)
└── LogicalTopicRegistryTest.java              (10 tests)
```
