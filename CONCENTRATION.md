# Concentration (Infinite Partitions) — v1 kernel

This document describes what has been built so far on the `experiment/infinite-partitions`
branch toward the feature described in `PROMPT.md`.

## Status

| Layer | Status | Lives in |
|---|---|---|
| Pure-Java concentration kernel | **Done.** Built TDD-first, 91 tests green. | `storage/src/main/java/org/apache/kafka/storage/internals/concentration/` |
| `ConcentrationKernel` facade (single broker-facing surface) | **Done.** 21 facade tests. | `storage/src/main/java/.../concentration/ConcentrationKernel.java` |
| Kernel-level integration tests | **Done.** Six PROMPT scenarios covered. | `storage/src/test/java/.../concentration/ConcentrationKernelIntegrationTest.java` |
| Production-readiness audit fixes | **Done.** Four blocking findings closed: facade-vs-close race, descriptorsFor immutability, sidecar-constructor FD leak, unbounded growth (added `removeLogicalPartition`). | — |
| Broker glue (produce/fetch/admin/DeleteRecords hooks) | **Not started.** Kernel facade ready; seam map below. | — |
| End-to-end test with a real broker | **Not started.** Depends on broker glue. | — |
| Audit fleet (Codex + Gemini) per PROMPT §"After every major phase" | **Partially.** In-fleet sub-agents have audited the kernel twice; Codex/Gemini are unreachable from this CLI environment and that limitation is recorded in commit bodies rather than fabricated. | — |

## What v1 ships

A self-contained "concentration kernel" — the pure data structures that let one physical Kafka
topic with M partitions present itself to stock clients as a logical topic with N partitions
(N ≥ M), each logical partition carrying its own contiguous offset sequence even when records
are physically interleaved on the backing.

The kernel is **transport-agnostic** and **broker-agnostic** by design: it depends on nothing
under `clients/` and on nothing in `core/`. The broker glue (not yet committed) wires the
kernel into the existing produce / fetch / admin paths via the `ConcentrationKernel` facade.

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
ConcentrationKernel          facade — single broker-facing surface, lifecycle-managed
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
- **Atomic produce commit.** `ConcentrationKernel.commitProduce(reservation, backingOffset)`
  persists the sidecar entry AND commits the tracker reservation as one operation; on any
  IOException or RuntimeException from sidecar.append, the tracker reservation is rolled back
  before the exception is rethrown. Honours the "no offset gaps" invariant at the boundary
  where the broker meets the kernel.
- **Bounded growth via `removeLogicalPartition`.** Both `ConcentrationKernel.sidecars` and
  `LogicalOffsetTracker.states` only grow over the broker's lifetime without an explicit
  teardown API. `ConcentrationKernel.removeLogicalPartition(topic, partition)` closes the
  sidecar handle, drops the tracker state (under its partition lock so any in-flight
  reservation has finished by then), and deletes the on-disk sidecar file. Caller contract:
  the broker must have stopped serving the partition first.

### PROMPT functional scenarios → tests

| PROMPT scenario | Where it's pinned |
|---|---|
| Two logical topics share single-partition backing, interleaved produce, consumer isolation | `ConcentrationKernelIntegrationTest#twoLogicalTopicsShareSingleBackingPartitionAndStayIsolated` |
| Three logical topics concurrent produce, monotonic per-topic offsets | `ConcentrationKernelIntegrationTest#threeLogicalTopicsConcurrentlyProduceAndPreserveMonotonicOffsets` |
| DeleteRecords scoped to one logical partition | `ConcentrationKernelIntegrationTest#deleteRecordsAdvancesOnlyOneLogicalPartitionStart` + `ConcentrationKernelTest#advanceStartOffsetMovesLowWaterOnlyForOneLogicalPartition` |
| Restart with intact sidecars → cheap startup | `ConcentrationKernelIntegrationTest#restartWithIntactSidecarsRehydratesTracker` + `ConcentrationKernelTest#recoveryFromSidecarsRebuildsTrackerStateAfterRestart` |
| Restart without sidecars → full scan rebuild | `ConcentrationKernelIntegrationTest#restartWithoutSidecarsReconstructsFromBackingScan` + `ConcentrationKernelTest#recoveryFromBackingScanReplaysHeadersIntoSidecarsAndTracker` |
| Direct produce to backing-topic name rejected | `ConcentrationKernelTest#directProduceToBackingTopicNameIsSignalled` (kernel signal); full broker rejection in `KafkaApis.handleProduceRequest` — **not yet wired** |
| Idempotent producer retry, no duplicates | **Not addressable at kernel layer.** Depends on broker-level producer-id / epoch state preserved across the logical→physical translation. The kernel does not bypass `analyzeAndValidateProducerState` because the kernel is not on the producer-state codepath at all; idempotence is preserved by virtue of running before the kernel-driven offset assignment. |

## What v1 does **not** ship

Explicitly out of scope per PROMPT (stretch goals):

- **Transactional support.** Per-logical-topic LSO, zombie cleanup on `InitProducerId`.
- **Compaction-safe key prefixing.** Two logical topics sharing one compacted backing destroy
  each other's data; v1 sidesteps by excluding compacted backings.
- **Concurrent produce-failure cascade rollback.** Single in-flight reservation per partition
  is enough for the "no gaps" criterion.

Pending broker integration (not started, not out-of-scope, the remaining v1 work):

### Broker-integration seam map

Each PROMPT acceptance criterion that requires broker-side wiring, with the exact call site:

1. **Backing-topic produce rejection** —
   `core/src/main/scala/kafka/server/KafkaApis.scala:378` (`handleProduceRequest`),
   around line 399 inside `produceRequest.data.topicData.forEach { … }`. Inject:
   `kernel.isBackingTopic(topicPartition.topic())` → if true, populate
   `invalidRequestResponses` with `Errors.INVALID_TOPIC_EXCEPTION` and skip the partition.

2. **Logical→backing routing + per-logical-topic offset assignment** —
   `core/src/main/scala/kafka/server/KafkaApis.scala:378` (after step 1): rewrite each
   inbound `TopicPartition` whose name is registered as logical to the resolved backing
   `(backingTopic, kernel.backingPartitionFor(logicalTopic, logicalPartition))`. Reserve a
   logical offset via `kernel.reserveProduce` and stamp it into each record's headers before
   handing off to `ReplicaManager.appendRecords`. On the append-success callback, call
   `kernel.commitProduce(reservation, returnedBackingOffset)`; on failure,
   `kernel.rollbackProduce`. Stock client sees the assigned logical offset in
   `PartitionResponse.baseOffset`.

3. **Fetch translation** —
   `core/src/main/scala/kafka/server/KafkaApis.scala` (`handleFetchRequest`) and
   `core/src/main/scala/kafka/server/ReplicaManager.scala` (`fetchMessages`). For each
   inbound fetch on a logical topic: translate `(logicalTopic, logicalPartition, fetchOffset)`
   to `(backingTopic, backingPartition, backingOffset)` via
   `kernel.resolveBackingOffset(...)`; perform the backing read; on the way back,
   filter records to those carrying the matching logical-topic header and rewrite each
   record's offset to its logical value. The filter step is the v1 cost — records of other
   logical topics on the same backing partition are dropped on the read path.

4. **DeleteRecords on a logical topic** —
   `core/src/main/scala/kafka/server/KafkaApis.scala` (`handleDeleteRecordsRequest`).
   For partitions whose topic is logical, call
   `kernel.advanceStartOffset(logicalTopic, logicalPartition, newStart)` and bypass the
   `ReplicaManager.deleteRecords` call entirely — the backing log is not truncated.

5. **Recovery wiring at broker startup** —
   `core/src/main/scala/kafka/server/BrokerServer.scala` (broker startup sequence,
   after `LogManager.startup`). Construct one `ConcentrationKernel` per broker, point it at
   `<logDirs(0)>/_concentration_sidecars/`, and call `kernel.recoverFromSidecars(declaredPartitions)`.
   If sidecars are missing/corrupt, fall back to `kernel.recoverFromBackingScan` over a
   `RecoveryRecord` stream built by reading the backing log's record headers in backing-offset
   order.

6. **Admin: declare a logical topic** —
   Either extend `CreateTopicsRequest` with a `(logical, N, backing, M)` variant, or add a
   dedicated KIP-style RPC. Either way, the broker controller persists the descriptor in a
   KRaft metadata record (`LogicalTopicRecord`) so it survives controller restarts; followers
   replay the record and call `kernel.declare(descriptor)` to populate the in-memory registry.
   This is the largest unimplemented chunk because it requires a metadata-record schema and
   KRaft replay logic. See the `core/src/main/scala/kafka/server/metadata/` package for the
   pattern used by other records (`TopicRecord`, `PartitionRecord`).

7. **Producer-id / epoch preservation across translation** —
   No new code needed if (2) is implemented correctly: the existing
   `analyzeAndValidateProducerState` in `UnifiedLog.appendAsLeader` (around line 1021) runs
   on the backing partition's `ProducerStateManager` regardless of whether the produce
   originated as logical or direct, and idempotence is preserved because the producer-id /
   epoch / sequence triple is record-level and travels through the translation unchanged.

### Constructor injection sites for the kernel

When you add `ConcentrationKernel` to `KafkaApis`, three call sites need updating:
- `core/src/main/scala/kafka/server/BrokerServer.scala:448` — production construction
- `core/src/main/java/kafka/server/builders/KafkaApisBuilder.java:202` — builder
- `core/src/test/scala/unit/kafka/server/KafkaApisTest.scala:189` — test construction

## How to run

```bash
JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 \
  ./gradlew :storage:test --tests 'org.apache.kafka.storage.internals.concentration.*'
```

91 tests, all green at HEAD.

## File layout

```
storage/src/main/java/org/apache/kafka/storage/internals/concentration/
├── BackingScanRecoverer.java
├── ConcentrationKernel.java
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
├── ConcentrationKernelTest.java               (21 tests — facade contract)
├── LogicalOffsetTrackerTest.java              (18 tests)
├── LogicalPartitionMapperTest.java             (5 tests)
├── LogicalSidecarIndexTest.java               (14 tests)
├── LogicalTopicDescriptorTest.java            (10 tests)
└── LogicalTopicRegistryTest.java              (10 tests)
```

## Production-readiness honest assessment

The kernel itself is production-shape: thread-safe, lifecycle-managed, fail-loud on corruption,
no swallowed exceptions, no per-append fsync, dense O(1) sidecar lookup, bounded growth.
It has been audited three times by in-fleet sub-agents. Audit fixes landed so far:

- `volatile` on the tracker's lock-free reader fields (initial audit)
- File-descriptor leak fix in `BackingScanRecoverer.recoverFromScan` (initial audit)
- Swap from `RuntimeException("CorruptIndexException")` to the actual
  `org.apache.kafka.storage.internals.log.CorruptIndexException` type (initial audit)
- Race between `sidecarFor()`'s slow-path insert and `close()`'s iterate-and-clear, fixed via
  double-checked locking on the kernel monitor (second audit)
- `descriptorsFor` immutability via `List.copyOf` rather than a leaky `Collection` view
  (second audit)
- `LogicalSidecarIndex` constructor FD leak when `CorruptIndexException` or `readEntryAt`
  throws after `new RandomAccessFile`, fixed with try/close/rethrow + addSuppressed (third
  audit; regression-pinned by `constructorFailureReleasesFileDescriptor` hammering 2000x)
- Unbounded growth of the `sidecars` and `states` maps, fixed with
  `ConcentrationKernel.removeLogicalPartition` (third audit)

What the kernel does **not** yet give you is an end-to-end broker that stock clients can
produce to. That work — wiring per the seam map above — is multi-week per PROMPT's own
warning ("multi-week with multi-day debugging sessions"), and `v1 must stay narrow or it
does not ship`. The next focused commits will land the broker hooks one at a time, starting
with the smallest standalone wiring (the backing-topic-rejection check at hook point 1),
which exercises one PROMPT acceptance criterion in isolation without touching the metadata
schema or the fetch translation.

Codex and Gemini consultations are mandated by PROMPT §"After every major phase". Neither
external app is reachable from this CLI environment, and that limitation is recorded plainly
in each commit body rather than fabricated. When the work moves to a host where those apps
are reachable, the audit fleet for the kernel should be re-run and any findings landed as
follow-up commits before the broker glue lands on top.
