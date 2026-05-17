# Compression Policy

## Goal
Reject `compression.type=none` produces to topics configured with `compression.policy=required`, per-partition. Zero changes under `clients/`.

## Constraints (do not violate)
- **Do not modify anything under `clients/`.** Stock producers and consumers must continue to work without recompilation.
- Per-partition error semantics: a multi-partition produce request must fail only the offending partitions; the rest succeed.
- Topic without the config → no behaviour change.

## Minimum viable outcome
1. Register `compression.policy` as a recognised topic config server-side (in `storage`/`core`, not `clients`).
2. In the produce handler, after the existing `validateRecords()` step, inspect each batch's `CompressionType` and reject with `INVALID_RECORD` per offending partition.
3. Produce with `compression.type=lz4` (or any non-NONE) → succeeds.

## Stretch (only after the minimum lands)
- Additional values: `optional`, `forbidden`, allow-list of codecs.
- Metric for rejected batches.

## Careful
- `validateRecords()` only checks API version + record format today. Extend it with a topic-config parameter, or add the check directly after — both are fine.
- The LLM's first instinct is to fail the whole request. That's wrong: produce responses are per-partition and clients depend on partial success.

## Lessons already known (don't rediscover)
- Kafka already has `CreateTopicPolicy` and `AlterConfigPolicy` SPIs — there's just no `ProducePolicy`. Same shape if you need one.

## Acceptance criteria
- Topic config absent or default → behaviour identical to vanilla Kafka, with zero validation overhead on the produce path.
- The config is registered server-side only; no client recompilation is required to consume topics with the config set or unset.
- A single produce request targeting multiple partitions across multiple topics is evaluated per-partition: only batches that violate their topic's policy are rejected; the rest succeed in the same response.
- The error returned for an offending partition is `INVALID_RECORD` (or the standard partition-scoped record-validation error code).
- Validation runs after the existing API-version and record-format checks, on the same path, with no allocation on the fast path when the config is unset.
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

## Working mode: full autonomy with Codex review
You must drive this project to completion in **total autonomy**. No human babysitting, no mid-flight permission requests, no "should I continue?" check-ins. Plan, decide, implement, test, iterate, ship. The acceptance criteria and functional test scenarios above are your contract — done means they all pass.

**Regularly solicit Codex (OpenAI's app) for a second opinion.** At minimum, consult Codex at: (a) end of initial planning, before the first implementation commit; (b) when choosing between two non-trivial design options; (c) after each milestone (minimum viable outcome reached; each stretch item considered); (d) whenever you're blocked for more than one iteration on the same surface. Present Codex with the relevant context and a sharp question. **Take its feedback seriously** — integrate what improves the work, push back in writing on what doesn't, never silently ignore. Record each consultation briefly in the commit body: `Codex review: asked <one-line question>; took <one-line takeaway>; rejected <one-line dissent> because <reason>.`
