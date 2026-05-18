# Compression Policy

## Goal
Reject `compression.type=none` produces to topics configured with `compression.policy=required`, per-partition. Zero changes under `clients/`.

## Constraints (do not violate)
- **Do not modify anything under `clients/`.** Stock producers and consumers must continue to work without recompilation.
- Per-partition error semantics: a multi-partition produce request must fail only the offending partitions; the rest succeed.
- Topic without the config → no behaviour change.

## Minimum viable outcome
1. Register `compression.policy` as a recognised topic config server-side (in `storage`/`core`, not `clients`).
2. In the **produce request handler — `KafkaApis.handleProduceRequest`** — right next to the existing `ProduceRequest.validateRecords(...)` call, inspect each batch's `CompressionType` and reject with `INVALID_RECORD` per offending partition. The check happens in the request handler, *before* the records reach the replication / log-append layers.
3. Produce with `compression.type=lz4` (or any non-NONE) → succeeds.

## Stretch (only after the minimum lands)
- Additional values: `optional`, `forbidden`, allow-list of codecs.
- Metric for rejected batches.

## Careful
- **The check belongs in the produce request handler — `KafkaApis.handleProduceRequest` — adjacent to the existing `ProduceRequest.validateRecords(...)` call.** That site already converts `ApiException` into a per-partition `PartitionResponse` via `Errors.forException(e)`, which is exactly the per-partition error shape this feature requires.
- **Do NOT put the check in any of the following — those are too deep and bypass the existing per-partition error shaping in the handler:**
  - `UnifiedLog.analyzeAndValidateRecords` (or anywhere else in `UnifiedLog`)
  - `Partition.appendRecordsToLeader`
  - `ReplicaManager.appendRecords` / `handleProduceAppend` / `appendToLocalLog`
  - `LogValidator` or any storage-layer validator
- `ProduceRequest.validateRecords()` lives in `clients/` and only checks API version + record format. **You may not modify it** (clients/ is off-limits). Add the new check **immediately after** the call to `ProduceRequest.validateRecords(...)` in `KafkaApis`, inside the same per-partition `try { ... } catch { case e: ApiException => ... }` block. Throwing an `InvalidRecordException` from there flows naturally into the existing `invalidRequestResponses` map.
- Topic config is reached via the replica/log manager in `KafkaApis` (`replicaManager.getLogConfig(tp)` or equivalent). If no `LogConfig` is available for a partition at that point (topic just created, race), the existing path proceeds unchanged — do not invent a synchronous lookup.
- The LLM's first instinct is to fail the whole request. That's wrong: produce responses are per-partition and clients depend on partial success.

## Lessons already known (don't rediscover)
- Kafka already has `CreateTopicPolicy` and `AlterConfigPolicy` SPIs — there's just no `ProducePolicy`. Same shape if you need one.

## Acceptance criteria
- Topic config absent or default → behaviour identical to vanilla Kafka, with zero validation overhead on the produce path.
- The config is registered server-side only; no client recompilation is required to consume topics with the config set or unset.
- A single produce request targeting multiple partitions across multiple topics is evaluated per-partition: only batches that violate their topic's policy are rejected; the rest succeed in the same response.
- The error returned for an offending partition is `INVALID_RECORD` (or the standard partition-scoped record-validation error code).
- Validation runs in `KafkaApis.handleProduceRequest`, immediately after the existing `ProduceRequest.validateRecords(...)` call, with no allocation on the fast path when the config is unset (default = `none`). The storage layer (`UnifiedLog`, `Partition`, `ReplicaManager`) is **not** modified for this check.
- The config accepts a small, validated value set (e.g., `none` (default), `required`); unknown values are rejected at CreateTopic / AlterConfig time with a clear error.

## Functional test scenarios
- A topic with the policy set to `required` rejects a produce containing a NONE-compressed batch with `INVALID_RECORD`; the producer sees a per-partition error and can retry with compression enabled.
- The same topic accepts a produce containing an LZ4 (or any non-NONE) batch with offsets assigned normally.
- A topic without the policy set behaves exactly like vanilla Kafka: NONE-compressed batches succeed.
- A produce request targeting partitions of two topics — one with the policy required and one without — returns `SUCCESS` for the unconfigured topic's partitions and `INVALID_RECORD` only for the configured topic's partitions, in a single response.
- A produce request to a configured topic where the batch is correctly compressed but one record inside the batch is corrupted hits the existing validation path; the compression-policy check does not change the error reported in that case.
- CreateTopic with a malformed `compression.policy` value (e.g., `yes`, `123`) is rejected with a configuration error and the topic is not created.

## When stuck (escape hatch)
If you find yourself looping on the same dead-end — the same error reappearing, the same refactor reverted, no measurable progress over several attempts — the order of recourse is fixed:

1. **First, ask Codex** (OpenAI's app) for a second opinion on the specific blocker. Frame the question sharply: what you tried, what went wrong, what you suspect. Take its answer seriously and try the direction it suggests.
2. **If still stuck after Codex, ask Gemini** (Google's app) for an independent third opinion. Compare the two — divergence is informative; convergence is reassuring. Try the most credible direction.
3. **Only if you remain stuck after both Codex and Gemini**, you may consult `/home/florent/ivy-trunk` read-only **for inspiration only**. Read enough to understand the shape of a solution, then close the file and write your own from scratch in this repo's idiom. **No copy-paste. No transliteration. No "I'll just adapt this block."**

The commit that follows an ivy-trunk consultation must include in its message a one-liner of the form: `Stuck in local minimum on <one-line description>; Codex suggested <X>; Gemini suggested <Y>; consulted ivy-trunk for inspiration; took away <one-sentence insight>; moved on.` If you cannot honestly write that sentence, you copied — revert and try again. The escape hatch exists to break unproductive loops, not to import an implementation.

## Working mode: full autonomy with sub-agent verification
You must drive this project to completion in **total autonomy**. No human babysitting, no mid-flight permission requests, no "should I continue?" check-ins. Plan, decide, implement, test, iterate, ship. The acceptance criteria and functional test scenarios above are your contract — done means they all pass.

**At the start of every task, fan out a fleet of sub-agents to figure out HOW to do it.** Tailor the fleet to the task — e.g., one sub-agent to map the affected code paths, one to identify the right Kafka APIs and existing patterns to reuse, one to surface edge cases and prior art in this repo, one to enumerate known pitfalls and lessons-already-known. Synthesize their findings before writing your first line of implementation code; do not skip this step because the task "looks simple."

**After every major phase** (initial planning closed; minimum viable outcome reached; each stretch item considered; before merge), fan out another fleet of sub-agents to **audit what happened**: spec compliance against the acceptance criteria and functional test scenarios above, test coverage, regressions in adjacent code, performance impact, edge cases missed, security/authorisation correctness. **Two slots in this audit fleet are mandatory**: one sub-agent consults **Codex** (OpenAI's app), one sub-agent consults **Gemini** (Google's app). Feed each the diff and a sharp question. Take their feedback seriously — integrate what improves the work, push back in writing on what doesn't, never silently ignore.

You may also consult Codex on demand mid-phase when choosing between two non-trivial design options.

Record each LLM consultation briefly in the commit body: `Codex: asked <one-line question>; took <one-line takeaway>. Gemini: asked <one-line question>; took <one-line takeaway>.` Never skip the audit because you "feel done."
