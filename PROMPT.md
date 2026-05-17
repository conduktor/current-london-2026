# Infinite Partitions (Concentration)

## Goal
A logical topic with N partitions backed by a physical topic with M partitions (M ≪ N). Stock clients produce and consume the logical topic transparently. v1 covers **produce + fetch + per-logical-topic offset sequence only** — skip transactions and compaction. Zero changes under `clients/`.

## Constraints (do not violate)
- **Do not modify anything under `clients/`.** Stock producers and consumers must work against the logical topic without recompilation.
- v1 scope: **non-transactional, non-compacted backings only.** The two hardest features in the deck (per-logical-topic LSO + zombie-transaction cleanup; compaction key prefixing) are explicitly **out of scope**.
- Each logical topic gets its own contiguous offset sequence (0, 1, 2…), even though records are physically interleaved on the backing partition.
- Do not fsync per-append on small segments — the deck records throughput collapsing from 1000+ msg/s to ~144 msg/s.

## Minimum viable outcome
1. Admin command to declare a logical topic: `(logical_name, N, backing_topic, M)`.
2. Produce: route logical partition → backing partition via a deterministic mapping, assign a per-logical-topic offset, persist a sidecar index entry `(logical_topic, logical_offset) → (backing_partition, backing_offset)`.
3. Fetch: translate the logical fetch offset → backing position via the index; read; rewrite the response so records carry logical offsets.
4. Broker restart with no index file → reconstruct by scanning the backing log; no data loss; restart with index → cheap startup.

## Stretch (do **not** attempt in v1)
- Transactional support: per-logical-topic LSO tracking; zombie transaction cleanup on `InitProducerId` with epoch bump.
- Compaction-safe key prefixing (`logicalTopic\0<key>` on write, strip on fetch).
- Concurrent produce-failure cascade rollback of dependent reservations.
- Sub-second restart via persisted-index recovery.

## Careful
- The deck explicitly describes this as multi-week with multi-day debugging sessions. v1 must stay narrow or it does not ship.
- Per-batch fsync kills throughput. Batch writes and respect the segment's existing flush policy.
- Two logical topics over the same compacted backing destroy each other's data because the same key collides. v1 sidesteps by excluding compacted backings — do **not** quietly attempt to "make it work" for compacted topics.
- A COMMIT on the backing partition commits across every logical topic on that partition. v1 sidesteps by excluding transactional backings — do **not** add per-logical-topic LSO machinery in v1.

## Lessons already known (don't rediscover)
- Kafka's existing `.index` and `.timeindex` files exist for very good reasons. The custom index should follow their shape, not invent a new one.

## Acceptance criteria (v1 scope)
- Logical offsets are contiguous starting at zero for each logical topic-partition, independent of backing layout. Producer callbacks and consumer records receive logical offsets, never physical.
- Broker restart with intact durable index → sub-second offset-tracker rebuild from the sidecar file. Restart without index → full log scan to rebuild the mapping; no data loss in either case.
- DeleteRecords on a logical topic advances only that logical partition's start offset. The backing log is not truncated; sibling logical topics on the same backing partition retain their full readable ranges.
- Logical-offset reservation is rollbackable: if a physical append fails, the reservation is not committed, and subsequent appends do not leave offset gaps.
- Idempotent producer state (producer id, epoch, sequence) is preserved across the logical/physical translation; idempotence validation is not bypassed.
- Direct produce to a backing topic by physical name (bypassing logical-topic routing) is rejected by the broker; logical topics are the sole produce path for concentrated data.
- Records from two logical topics interleaved on the same backing partition are addressable independently — a consumer of logical topic A never receives records from logical topic B, regardless of physical interleaving.

## Functional test scenarios (v1 scope)
- Two logical topics share a single-partition backing. A producer writes 1000 records to each, interleaved round-robin. A consumer reading logical topic A from offset 0 receives A's 1000 records with offsets 0..999 in order and zero records from logical topic B.
- Three logical topics (A, B, C) share one backing partition with concurrent producers. Each consumer sees only its own topic's records with monotonic logical offsets; no offset reordering or duplication within a logical partition.
- A logical topic accumulates 100 records. DeleteRecords advances its start offset to 50; a sibling logical topic on the same backing still reads its full range; the backing log is not truncated.
- Broker restart with intact index → sub-second startup; restart without index → reconstruction by physical-log scan; data is accessible in both cases with correct logical offsets.
- A produce request targeting a backing topic by physical name is rejected with an unambiguous error; no records are appended to the backing.
- An idempotent producer experiences a transient network failure and retries a batch; the logical topic does not receive duplicate records, and the logical offset sequence remains contiguous.

## When stuck (escape hatch)
If you find yourself looping on the same dead-end — the same error reappearing, the same refactor reverted, no measurable progress over several attempts — you may consult `/home/florent/ivy-trunk` read-only **for inspiration only**. Read enough to understand the shape of a solution, then close the file and write your own from scratch in this repo's idiom. **No copy-paste. No transliteration. No "I'll just adapt this block."** The commit that follows the consultation must include in its message a one-liner of the form: `Stuck in local minimum on <one-line description of the dead-end>; consulted ivy-trunk for inspiration; took away <one-sentence insight>; moved on.` If you cannot honestly write that sentence, you copied — revert and try again. The escape hatch exists to break unproductive loops, not to import an implementation.
