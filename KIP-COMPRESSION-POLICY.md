# KIP-XXXX: Server-side `compression.policy` topic config

| Field | Value |
| --- | --- |
| Status | Draft (experimental branch `experiment/compression-policy`) |
| Discussion thread | n/a |
| Vote thread | n/a |
| JIRA | n/a |
| Released in | n/a |

> **Editor's note.** This document is a KIP-style design doc, not a submitted
> KIP. It is co-located with `PROMPT.md` to be the durable reference for the
> design once the spec gets formally proposed. References to file paths in
> this branch are listed in `## Implementation map` at the bottom.

## Motivation

Operators on shared multi-tenant Kafka clusters routinely run into one
producer team writing uncompressed payloads — by accident, by misconfiguration,
or because their client framework defaulted to `compression.type=none`. The
downstream cost is paid by the cluster: storage bytes, replication bandwidth,
broker fetch throughput, and tiered-storage egress. Today the cluster has no
authoritative way to refuse such batches at the boundary; the closest tools
all have material gaps:

- **`compression.type` topic config (broker-side recompression).** Recompresses
  inbound batches on the broker. Pays the CPU cost on the broker, not the
  producer, and silently masks the producer's misconfiguration. Operators are
  paying for someone else's mistake; the producer never learns it is misbehaving.
- **`ProducePolicy` Java SPI** (KIP-201, never landed). A pluggable policy
  loaded by the broker. Requires shipping a jar to every broker, restart to
  pick up changes, opaque to admin tooling — it is invisible through
  `describeConfigs`. Operationally fragile.
- **`MessageMaxBytes`** can be lowered to force producers to compress, but it
  rejects every batch above the threshold (including legitimately small
  uncompressed batches and legitimately large compressed ones). Wrong tool.
- **Out-of-band scanning** (e.g. running a consumer that flags uncompressed
  batches and pages the producer team). Lagging by design and does not protect
  the cluster — by the time the page fires, the bytes have already landed.

What is missing is a first-class, declarative, per-topic admission rule on the
broker that **rejects** uncompressed batches at the produce path, surfaces a
clean non-retriable error to the client, and is fully visible through standard
admin APIs. That is the gap this KIP fills.

### Non-goals

- This KIP does **not** propose a "minimum compression ratio" check or a
  specific codec requirement (`require gzip`, `require zstd`). It draws two
  hard lines — "any compression" (`required`) and "no compression" (`forbidden`).
  A codec allow-list is sketched in `## Future Work` as a backwards-compatible
  extension.
- This KIP does **not** modify the `clients/` module. Producers continue to
  build batches exactly as they do today; the policy is enforced server-side and
  surfaced through the existing `INVALID_RECORD` error code.
- This KIP does **not** enforce the policy on internal appends (replication,
  transaction-state, group-coordinator). Those paths bypass the check by
  construction — see `## Proposed changes` § enforcement scope.
- This KIP does **not** introduce a new wire-protocol error code, request type,
  or schema. Compatibility is preserved by reusing `INVALID_RECORD` (error code
  87, already in every supported produce-protocol version).

## Public interfaces

### New topic config

A single new topic-level config is added. There are **no** new broker configs,
**no** new request types, and **no** new wire-protocol fields.

| Name | `compression.policy` |
| --- | --- |
| Type | `STRING` |
| Default | `"none"` |
| Valid values | `"none"`, `"required"`, `"forbidden"` |
| Importance | `MEDIUM` |
| Server default override | none (this config has no `compression.policy.default` cluster-level fallback) |
| Dynamic | yes (settable via `incrementalAlterConfigs`) |
| Doc | "Server-side policy for the compression of producer batches. `none` (default) preserves vanilla behaviour. `required` rejects produce requests whose batches carry `compression.type=none`; `forbidden` is the mirror image and rejects produce requests whose batches carry any non-`none` `compression.type`. Both rejections surface as INVALID_RECORD on a per-partition basis. Enforced in the produce request handler, so replication, transaction-state, and group-coordinator appends bypass the check by construction." |

The config key is the bare string `"compression.policy"` — the same string an
operator passes to `kafka-configs.sh --add-config` or a programmatic client
passes to `NewTopic.configs(Map.of("compression.policy", "required"))`. The
public Java constant `COMPRESSION_POLICY_CONFIG` lives in
`org.apache.kafka.storage.internals.log.LogConfig` (server-side, storage
module).

This MVP deliberately does **not** mirror the constant onto the public
`org.apache.kafka.common.config.TopicConfig` surface, even though every
other dynamic topic config (`cleanup.policy`, `compression.type`,
`remote.storage.enable`, …) is exposed there. That mirroring requires
modifying `clients/`, which this KIP's non-goals forbid (see Non-goals
above). The operational consequence is that programmatic clients that want a
compile-time-checked constant must reference `LogConfig.COMPRESSION_POLICY_CONFIG`
(which pulls in the `storage` module) instead of `TopicConfig.COMPRESSION_POLICY_CONFIG`.
For the string-based admin tooling path (`kafka-configs.sh`, AdminClient
`NewTopic.configs(...)`, AlterConfig with `ConfigEntry(name, value)`) this is
a no-op — no constant is required. Adding a one-line
`TopicConfig.COMPRESSION_POLICY_CONFIG = "compression.policy"` once the
non-goal is lifted is listed in Future Work as a strictly additive,
backwards-compatible change.

### Error code

Violations surface to clients as the existing `INVALID_RECORD` error code
(numeric id `87`, class `InvalidRecordException`), per-partition, inside the
existing `ProduceResponse` shape. No new error code is introduced.

### CLI / admin tooling

`kafka-configs.sh` and `kafka-topics.sh` need **no** new flags. The new config
is recognised through the standard topic-config plumbing:

```bash
# Set
kafka-configs.sh --bootstrap-server $BS --entity-type topics \
  --entity-name orders --alter --add-config compression.policy=required

# Inspect
kafka-configs.sh --bootstrap-server $BS --entity-type topics \
  --entity-name orders --describe

# Remove (returns the topic to the default "none")
kafka-configs.sh --bootstrap-server $BS --entity-type topics \
  --entity-name orders --alter --delete-config compression.policy
```

## Proposed changes

### Enforcement placement

The check runs in **`KafkaApis.handleProduceRequest`**, the broker entry point
for the produce wire protocol. Concretely, immediately after the existing
`ProduceRequest.validateRecords(...)` call (the in-clients structural check
for the records buffer):

1. Look up the topic's `CompressionPolicy` via
   `ReplicaManager.compressionPolicy(topicPartition)`.
2. Ask the policy whether the batch's `compressionType` violates it. The
   check is `policy.isViolatedBy(batch.compressionType)` — `REQUIRED`
   rejects `NONE` codecs, `FORBIDDEN` rejects any non-`NONE` codec, and the
   default `NONE` policy never reports a violation. On violation, throw
   `InvalidRecordException("<reason>")`. The existing per-partition
   `try { ... } catch { ApiException => Errors.forException(e) }` block
   handles the conversion to `INVALID_RECORD` in the partition response.

This is the **only** new code path. The rest of the produce pipeline
(`ReplicaManager.appendRecords`, `Partition.appendRecordsToLeader`,
`UnifiedLog`, `LogValidator`) is untouched.

#### Why here and not deeper

Putting the check in `KafkaApis` has three load-bearing properties that a
deeper placement would lose:

- **Per-partition error isolation comes for free.** The handler already runs
  one `try / catch` per topic-partition entry. A partition rejected by the
  policy is excluded from the subsequent `handleProduceAppend` call, while
  sibling partitions in the same request continue on the normal path. This is
  the contract clients already expect and rely on for partial success.
- **Internal appends bypass naturally.** Replication, transaction-state, and
  group-coordinator appends do not go through `handleProduceRequest`; they call
  the lower-level `appendRecords` / `appendToLocalLog` paths directly. Without
  any explicit `OriginType` check, the policy applies only to client traffic.
- **No allocation on the cold path.** The lookup returns an enum singleton; the
  fast path is a hash lookup, an instance check, and a singleton compare. No
  `Option` is materialised, no boxing — see `ReplicaManager.compressionPolicy`
  in the implementation map.

A storage-layer placement (in `LogValidator` or `UnifiedLog`) was rejected;
see `## Rejected alternatives`.

### Enforcement scope

| Origin | Enforced? | Why |
| --- | --- | --- |
| Stock `KafkaProducer` over wire protocol | **yes** | The motivating case. |
| Idempotent producer (`enable.idempotence=true`) | **yes** | Same `handleProduceRequest` codepath; the producer's PID/Epoch/Sequence header changes nothing on the policy axis. Pinned by `testIdempotentProducerHonoursCompressionPolicy`. |
| Transactional producer (`transactional.id` set) | **yes** | Same codepath as idempotent. Not covered by an integration test in this branch — see Test Plan. |
| Inter-broker replication (`FETCH_FOLLOWER` → log append) | **no** | Does not flow through `handleProduceRequest`. Already-stored batches must remain replicable even if the policy is enabled after they were written. |
| Transaction-state appends (`__transaction_state`) | **no** | Internal broker traffic, same reasoning as replication. |
| Group-coordinator appends (`__consumer_offsets`) | **no** | Internal broker traffic. |
| Tiered-storage remote read materialisation | **no** | Read path, not write path. |

### Config registration

`compression.policy` is registered exactly like any other topic config:

- Added to `LogConfig.SERVER_CONFIG_DEF` and the per-topic `LogConfigDef`.
- The valid-value validator (`ConfigDef.ValidString.in("none", "required", "forbidden")`)
  rejects unknown values at **both** the CreateTopic and AlterConfig boundary.
  An unknown value fails the Admin API future with
  `InvalidConfigurationException`; the topic is not created (or not altered).
- The config is in `CONFIGS_WITH_NO_SERVER_DEFAULTS`. This KIP deliberately
  does **not** introduce a cluster-level fallback (`compression.policy.default`).
  Doing so would make it possible to flip the policy for every topic on a
  cluster with one config change, which is the wrong default for a
  multi-tenant cluster and is the kind of change that demands its own KIP.
  Treated as Future Work.
- The config is round-trippable through `describeConfigs` (pinned by
  `testDescribeConfigsRoundTripsCompressionPolicy`).
- The config supports `incrementalAlterConfigs` `SET` and `DELETE` ops
  (pinned by `testReverseAlterRelaxesRequiredBackToNone` and
  `testAlterConfigDeleteResetsCompressionPolicyToDefault`).
- The config respects `validateOnly=true` (pinned by
  `testValidateOnlyAlterConfigRejectsBadValueAndPreservesState`).

### `CompressionPolicy` enum

A new enum `org.apache.kafka.storage.internals.log.CompressionPolicy` carries
the three valid values:

```java
public enum CompressionPolicy {
    NONE("none")          { public boolean isViolatedBy(CompressionType c) { return false; } },
    REQUIRED("required")  { public boolean isViolatedBy(CompressionType c) { return c == CompressionType.NONE; } },
    FORBIDDEN("forbidden"){ public boolean isViolatedBy(CompressionType c) { return c != CompressionType.NONE; } };

    public String value();                                   // "none" / "required" / "forbidden"
    public abstract boolean isViolatedBy(CompressionType);   // policy check
    public static List<String> names();                      // for the validator
    public static CompressionPolicy forName(String);         // parse from config
}
```

The enum lives under `storage/internals/log` because it is a server-side concept
keyed off the topic config. It is **internal** (not under
`org.apache.kafka.common`); clients have no reason to depend on it.

## Compatibility, deprecation, and migration plan

### For existing topics

Every topic created before this KIP lands has no `compression.policy` override,
so it inherits the default `"none"`. The produce path for those topics is
**byte-for-byte identical** to vanilla Kafka — the enforcement check is gated on
the policy being `REQUIRED`, and the policy lookup short-circuits to the enum
singleton `NONE` for any partition whose log config does not carry an override.
No data migration is required.

### For existing clients

No client recompile, no client config change. Stock `KafkaProducer` instances
keep working exactly as they did before:

- Against a topic that did not opt into the policy: identical behaviour.
- Against a topic with `compression.policy=required`: a producer configured
  with `compression.type=none` will see its first batch fail with
  `InvalidRecordException` (non-retriable). The operator's playbook is to set
  `compression.type=lz4` (or any non-none codec) on the producer and resume.
- Against a topic with `compression.policy=forbidden`: the mirror of the
  `required` case. A producer configured with any non-`none` codec will see
  its first batch fail with `InvalidRecordException` (non-retriable). The
  operator's playbook is to set `compression.type=none` on the producer.

In all cases the error message names the topic-partition, the configured
policy, and the observed batch compression — sufficient for self-service.

### For existing brokers

No change to inter-broker protocols. Mixed-version clusters during upgrade are
safe:

- A broker that knows about `compression.policy` will recognise and apply it.
- A broker that does not know about `compression.policy` will treat the
  config as unknown (the existing handling for forward-unknown topic configs
  is to log a warning and ignore). The topic still functions; it simply does
  not enforce the policy on that broker.
- During upgrade, the topic's actual behaviour on a per-partition basis depends
  on which broker is the leader. After the cluster is fully upgraded, the
  policy applies uniformly.

This is the same compatibility model every previous dynamic topic config has
followed (e.g. `remote.storage.enable`, `delete.retention.ms`).

### Deprecation

Nothing is deprecated by this KIP.

## Rejected alternatives

### Storage-layer placement (`LogValidator` / `UnifiedLog`)

An earlier iteration of this work placed the check in `LogValidator.validateMessagesAndAssignOffsets`,
on the theory that "rejecting on append" is closer to the storage primitive.
Rejected for three concrete reasons:

1. **Internal-append bypass became a per-callsite OriginType check.** Every
   internal append path (replication, transaction-state, group-coordinator)
   had to be plumbed with an "is this client traffic?" predicate, since the
   storage layer is also the path taken by those callers. The handler-level
   placement gets this for free because it only sits on the client path.
2. **Per-partition error isolation became artificial.** `LogValidator` is
   invoked once per partition by the append path, but the error has to travel
   back up through `appendRecords` → `handleProduceAppend` → `handleProduceRequest`
   to land in the right partition's `PartitionResponse`. The handler-level
   placement lands the error directly in the per-partition `try / catch` block
   that already exists for `validateRecords` failures, with no plumbing.
3. **It put the contract in the wrong layer.** The policy is a
   *client-facing admission rule*, not a *log-format invariant*. Mixing the
   two layers made future evolution (a codec allow-list, a producer-id
   allow-list, a rate-limit policy) much harder.

The branch contains the commit that reverts the storage-layer placement
(`f631245903 — core: revert UnifiedLog placement of compression.policy check`)
as a tombstone.

### `ProducePolicy` Java SPI (KIP-201)

KIP-201 proposed a pluggable `org.apache.kafka.server.policy.ProducePolicy`
broker SPI as the long-term home for this kind of admission rule. It has not
landed in over five years for reasons that apply here too:

- Adds a jar-deployment surface to every broker.
- Opaque through `describeConfigs` / `kafka-configs.sh`; operators cannot
  see whether a topic is policy-protected.
- Each policy needs its own bespoke config surface; no standardisation.
- Rolling a policy change requires bouncing brokers.

A topic-config-shaped solution is strictly less powerful than a pluggable SPI,
but it covers the concrete operational need (force compression on a per-topic
basis) and is fully introspectable through standard tooling. It also does not
preclude a future `ProducePolicy` KIP from landing — that work could read this
same `compression.policy` topic config as one of its inputs.

### Tighten `compression.type` semantics instead

`compression.type` already exists and supports a per-topic codec. We considered
adding a special value (e.g. `compression.type=any-but-none`) instead of a new
config. Rejected: `compression.type` has long-standing recompression semantics
(the broker rewrites batches into the configured codec). Bolting an
admission-rule semantic onto the same key would overload one config with two
unrelated jobs and break the principle of least surprise for every operator
who already knows what `compression.type` means. A separate, narrowly-scoped
config is cleaner.

### Reuse `min.compression.ratio` ideas

A "minimum compression ratio" check is appealing on paper but requires the
broker to actually decompress and measure the batch — paying the cost the
policy is supposed to *avoid*. It is fundamentally incompatible with the
"refuse at the boundary" model.

## Test plan

The implementation in this branch is covered by three test suites:

| Layer | File | What it pins |
| --- | --- | --- |
| Unit (config) | `LogConfigTest` | Config name registered; default is `none`; valid values accepted; invalid values rejected with `ConfigException`. |
| Unit (handler) | `KafkaApisTest` (`testCompressionPolicy*`) | Default policy lets uncompressed through; required rejects uncompressed with `INVALID_RECORD`; required accepts compressed; forbidden rejects compressed with `INVALID_RECORD` (and increments the rejection meter); forbidden accepts uncompressed; mixed-topic per-partition shape with `ArgumentCaptor` proving the rejected partition is excluded from `handleProduceAppend`; downstream `CORRUPT_MESSAGE` is not masked; the per-topic and all-topics `BatchesRejectedByCompressionPolicyPerSec` meter increments on rejection and stays flat on acceptance. |
| Integration | `CompressionPolicyIntegrationTest` | End-to-end against a real broker: required/lz4/open topic shape, mixed-topic per-partition end-to-end, CreateTopic + AlterConfig API-boundary rejection of unknown values, full compression-type matrix (gzip/snappy/lz4/zstd all pass under `required`; all four are rejected under `forbidden`), AlterConfig reverse-flip and DELETE-reset lifecycle, `describeConfigs` round-trip, `validateOnly` rejection preserves state, idempotent producer honours the policy. |

Coverage gaps deliberately left for future work, not blockers for the MVP:

- **Transactional producer** integration test. Same codepath as idempotent
  and unit-tested implicitly through `KafkaApisTest`, but a real end-to-end
  test would require the `__transaction_state` topic and a heavier harness.
- **Multi-broker replicated-topic** integration test. The policy applies at
  the leader, so a replicated topic with RF>1 would only confirm that the
  leader's broker rejects — no new wiring is exercised. Future work if a
  regression makes the case load-bearing.
- **JMH micro-benchmark** for the hot-path lookup. The implementation is
  designed to be allocation-free (see `## Proposed changes` § enforcement
  placement, last bullet), and a JMH bench would pin a regression budget.

## Future work

Strictly orthogonal extensions, each of which would be its own KIP:

0. **Mirror the constant onto `TopicConfig`.** Add
   `public static final String COMPRESSION_POLICY_CONFIG = "compression.policy"`
   plus its `_DOC` companion to `org.apache.kafka.common.config.TopicConfig`,
   matching the convention every other dynamic topic config follows. The MVP
   omits this only because the spec forbids `clients/` modifications; the
   change itself is a one-line additive constant with no behavioural impact.
1. **Codec allow-list** (`compression.policy=gzip,zstd`). Extend the
   `compression.policy` config to accept a comma-separated codec list in
   addition to the three keyword values (`none` / `required` / `forbidden`).
   `CompressionPolicy` would resolve at config-load time into a
   `Set<CompressionType>` of allowed codecs; the `isViolatedBy` check becomes
   `!allowed.contains(batchCompression)`. The `INVALID_RECORD` error code is
   reused. Fully backwards compatible with the three keyword values defined
   by this KIP (treat them as the degenerate sets `{any}`, `{any non-NONE}`,
   and `{NONE}` respectively). This requires turning `CompressionPolicy`
   from a closed enum into a value class with a `kind` + `allowedCodecs`
   field, and replacing the `ValidString.in(...)` validator with a custom
   `ConfigDef.Validator` implementation.
2. **Cluster-level default** (`compression.policy.default` broker config).
   Lets an operator turn enforcement on cluster-wide and opt individual
   topics out, instead of opting in. Deliberately left out of MVP because
   it changes the default-deny / default-allow choice for every topic on
   a cluster.
3. **`ProducePolicy` SPI revival** (KIP-201 successor). If the community
   eventually lands a pluggable SPI, the in-handler check here can be
   trivially re-expressed as the built-in default policy.

Items closed since the initial KIP-MVP cut (no longer Future Work):

- **Metric for rejected batches.** A per-topic + all-topics yammer Meter
  `kafka.server:type=BrokerTopicMetrics,name=BatchesRejectedByCompressionPolicyPerSec`
  is registered alongside the existing peers (`InvalidMagicNumberRecordsPerSec`,
  `InvalidMessageCrcRecordsPerSec`, etc.) and marked on the violation branch
  of `KafkaApis.enforceCompressionPolicy`, before throwing
  `InvalidRecordException`. Pinned by `KafkaApisTest`. Existing JMX /
  Prometheus scrapers pick it up with no config change.
- **`compression.policy=forbidden`.** The mirror image of `required`: any
  batch arriving with `compression.type` other than `none` is rejected with
  `INVALID_RECORD` at the broker handler. Use case: topics whose producers
  must send uncompressed batches (e.g. downstream tooling that reads the
  on-disk batch format directly without per-codec decompression). Caveat:
  this policy gates the *entry* codec only. The separate, pre-existing
  topic-level `compression.type` config can still recompress on disk
  (`compression.type=gzip` will rewrite incoming uncompressed batches as
  gzip on the way to the log). Operators who need raw on-disk bytes must
  set both `compression.policy=forbidden` and `compression.type=producer`
  (or `compression.type=uncompressed`).

## Implementation map

For reviewers reading the diff in this branch:

- Enum: `storage/src/main/java/org/apache/kafka/storage/internals/log/CompressionPolicy.java`
- Config registration: `storage/src/main/java/org/apache/kafka/storage/internals/log/LogConfig.java` (the `COMPRESSION_POLICY_CONFIG` constant, the `LogConfigDef.define(...)` line, the `CONFIGS_WITH_NO_SERVER_DEFAULTS` set, and the constructor assignment).
- Hot-path lookup: `core/src/main/scala/kafka/server/ReplicaManager.scala` (`compressionPolicy(topicPartition)`).
- Enforcement: `core/src/main/scala/kafka/server/KafkaApis.scala` (`enforceCompressionPolicy` invoked from `handleProduceRequest` immediately after `validateRecords`).
- Unit tests: `core/src/test/scala/unit/kafka/log/LogConfigTest.scala`, `core/src/test/scala/unit/kafka/server/KafkaApisTest.scala`.
- Integration tests: `core/src/test/scala/integration/kafka/api/CompressionPolicyIntegrationTest.scala`.

The branch is `experiment/compression-policy`. The spec it was built from is
`PROMPT.md` at the repo root.
