# Topic Views

## Goal
A read-only virtual topic backed by a physical topic + CEL predicate. Consumers see only matching records at the source offsets. All filtering happens in the fetch path, broker-side. Zero changes under `clients/`.

## Constraints (do not violate)
- **Do not modify anything under `clients/`.** Stock consumers issue normal `Fetch` requests; filtering is entirely server-side.
- Views are **read-only**: reject produce at the produce-handler entry point, **before** backing-topic resolution.
- Sandbox the CEL predicate: AST depth + node limits, cost cap, **no `matches()`** on the hot path (catastrophic backtracking risk).
- Offsets returned are the source offsets, sparse (not renumbered).

## Minimum viable outcome
1. View declaration: `(view_name, backing_topic, cel_predicate)` — admin command or topic-level config is fine.
2. Fetch handler: when the requested topic is a view, fetch from the backing topic, filter records server-side, return at source offsets.
3. Produce to a view → `INVALID_REQUEST`, checked **before** the view is resolved to its backing topic.
4. When all records in a fetched batch fail the predicate: emit a header-only v2 batch (zero records, source offset range) so the consumer advances instead of looping.

## Stretch (only after the minimum lands)
- READ_COMMITTED isolation: emit COMMIT/ABORT control batches as empty batches so LSO advances.
- View management via an internal compacted topic (hot-reload, mirroring the CEL rules approach).
- View-of-view composition.

## Careful
- Empty filtered response → consumer re-fetches the same offset → infinite loop. The header-only v2 batch is the fix and is counter-intuitive.
- The produce check must happen **before** backing-topic resolution. The LLM forgets this every time because the view resolves to a real topic and the produce path happily writes to it.
- Reject `matches()` regexes at compile time, not at evaluation time.

## Lessons already known (don't rediscover)
- COMMIT/ABORT control batches contain no user data, so they have nothing to predicate against — but they must still be emitted as empty batches or READ_COMMITTED isolation silently breaks. (Stretch scope, mentioned here so it's not re-derived later.)
- This composes with multi-tenancy to give cross-tenant data sharing for free (Slide 8). Out of scope for this worktree but keep the surface clean.

## Acceptance criteria
- View topics declare an offset mode that exposes source offsets sparse — gaps appear where records did not match. Attempting to renumber offsets is rejected at view creation time.
- A fetch from a view returns records at their source offsets with gaps where non-matching records exist; offsets are never renumbered or compacted.
- When a predicate evaluates to a non-boolean result at runtime, the record is silently skipped; the boolean return type is enforced at compile time.
- Predicates reject comprehension macros and looping constructs (`exists()`, `filter()`, `map()`) at creation time; no runtime exponential or backtracking expressions are possible.
- Predicate evaluation has bounded cost: AST node limits, no catastrophic-backtracking regexes (`matches()` rejected at compile time), cost-per-record cap; oversized payloads (depth, node count, string length) cause record skips rather than broker stalls.
- Produce to a view returns `INVALID_REQUEST` before the backing topic is resolved; views accept reads only.
- When a fetch spans a source batch where 100% of records are filtered out, a header-only batch is emitted covering the source offset range so the consumer offset advances without re-querying the same range.
- READ_COMMITTED view consumers observe correct isolation: COMMIT/ABORT control batches are emitted as empty batches so the consumer's LSO advances.

## Functional test scenarios
- A view over a source topic with predicate `body.color == 'red'` receives records of mixed colours. The consumer sees only red-car records at their native source offsets (e.g., 0, 5, 9 with gaps). The next fetch advances past the gap.
- A view with a predicate referencing headers and nested JSON body correctly filters multi-level structures; body-only predicates ignore malformed or oversized headers without crashing the fetch.
- A source batch of 50 records yielding zero matches for the view predicate emits a header-only batch covering offsets 100–149; the view consumer position advances to 150 without re-fetching 100–149.
- A predicate referencing invalid UTF-8 headers, duplicate object keys, oversized JSON strings, or out-of-long-range integers silently skips those records without hiding valid records from unrelated predicates.
- Predicate validation rejects comprehension expressions, unsafe numeric literals (precision-loss territory), and dynamic-evaluation constructs at creation time with clear error messages.
- A READ_COMMITTED view consumer observes correct isolation: ABORT control batches suppress emitted data; COMMIT/ABORT boundaries advance the consumer's LSO.
- Concurrent predicate evaluation on multiple threads over many iterations produces consistent true/false results for a fixed record — compiled predicates are thread-safe.

## When stuck (escape hatch)
If you find yourself looping on the same dead-end — the same error reappearing, the same refactor reverted, no measurable progress over several attempts — the order of recourse is fixed:

1. **First, ask Codex** (OpenAI's app) for a second opinion on the specific blocker. Frame the question sharply: what you tried, what went wrong, what you suspect. Take its answer seriously and try the direction it suggests.
2. **Only if you remain stuck after Codex's input**, you may consult `/home/florent/ivy-trunk` read-only **for inspiration only**. Read enough to understand the shape of a solution, then close the file and write your own from scratch in this repo's idiom. **No copy-paste. No transliteration. No "I'll just adapt this block."**

The commit that follows an ivy-trunk consultation must include in its message a one-liner of the form: `Stuck in local minimum on <one-line description>; Codex suggested <X> which <was/wasn't> enough; consulted ivy-trunk for inspiration; took away <one-sentence insight>; moved on.` If you cannot honestly write that sentence, you copied — revert and try again. The escape hatch exists to break unproductive loops, not to import an implementation.

## Working mode: full autonomy with Codex review
You must drive this project to completion in **total autonomy**. No human babysitting, no mid-flight permission requests, no "should I continue?" check-ins. Plan, decide, implement, test, iterate, ship. The acceptance criteria and functional test scenarios above are your contract — done means they all pass.

**Regularly solicit Codex (OpenAI's app) for a second opinion.** At minimum, consult Codex at: (a) end of initial planning, before the first implementation commit; (b) when choosing between two non-trivial design options; (c) after each milestone (minimum viable outcome reached; each stretch item considered); (d) whenever you're blocked for more than one iteration on the same surface. Present Codex with the relevant context and a sharp question. **Take its feedback seriously** — integrate what improves the work, push back in writing on what doesn't, never silently ignore. Record each consultation briefly in the commit body: `Codex review: asked <one-line question>; took <one-line takeaway>; rejected <one-line dissent> because <reason>.`
