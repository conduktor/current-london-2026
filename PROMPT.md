# Generic CEL Rules on All Inputs

## Goal
A single broker-side rule engine that evaluates CEL `DENY` rules against any request, hot-reloaded from the `__governance` compacted topic. Zero changes under `clients/`.

## Constraints (do not violate)
- **Do not modify anything under `clients/`.** Rules are written as JSON records to a normal compacted topic — any stock producer can author them.
- Bootstrap-safe: the broker's own read of `__governance` must not traverse `KafkaApis.handle()`, or you deadlock on startup. In the implementation, the broker reads the local `__governance` log directly via `ReplicaManager.getLog` (no consumer, no network round-trip), so no rule can deny it. Operator-tooling consumers of `__governance` are gated by the privileged-listener flag plus enrolment in the `governance.bypass.principals` allow-list — the bypass is principal-based, not client-id-based; the internal-reader client-id prefix is diagnostic only.
- Hot reload: atomic `RuleSet` swap on topic consumption; no broker restart.
- Fast path: zero cost when no DENY rule targets the request's API key.

## Minimum viable outcome
1. JSON rule schema: `{apiKeys: [...], action: "DENY", when: "<CEL expression>", errorCode: <int>}`.
2. CEL compiled once at rule load; cache per-API field extractors so `request.*` works generically.
3. Single interception point at the top of `KafkaApis.handle()`: bitset check on active deny-targeting API keys → if hit, evaluate matching rules → on DENY, short-circuit with the configured error code. Two paths reach the controller without traversing `KafkaApis.handle()` and are by design out of scope for the engine: (a) admin clients connected via `bootstrap.controllers` (KIP-1003), and (b) broker-internal forwarding from `AutoTopicCreationManager` for `CREATE_TOPICS` triggered by METADATA / FIND_COORDINATOR auto-creation. Operators relying on `CREATE_TOPICS` rules must set `auto.create.topics.enable=false` to close path (b); path (a) is operator-only by construction.
4. Internal `__governance` load path: the broker's bootstrap reads the local `__governance` log directly via `ReplicaManager.getLog` (no consumer, no `KafkaApis.handle()` traversal); steady-state reload then runs on the scheduler. Atomic `RuleSet` swap publishes through an `AtomicReference`.

## Stretch (only after the minimum lands)
- `ALLOW` and `FILTER` actions.
- `principal.*`, `session.*`, `client.*` evaluation contexts.
- Per-rule metrics + audit log.
- Bootstrap-load of N rules before accepting client connections.

## Careful
- Field extractors must be auto-generated per Kafka API type — do not hand-code them.
- The broker's local-log read of `__governance` must not traverse `KafkaApis.handle()` — drive it via `ReplicaManager.getLog` directly. Any operator-driven consumer of `__governance` must arrive on the privileged listener AND its peer principal must be enrolled in `governance.bypass.principals`, or rules block the very read that loads them. The client-id prefix used by the internal reader is diagnostic only and is NOT authoritative for the bypass.
- Don't pay the cost of context construction for requests with no matching rules. Bitset first; full context only on hit.

## Lessons already known (don't rediscover)
- The fast path matters more than the slow path. Bitset of active deny-targeting API keys is O(1).
- Self-referential bootstrap deadlock is a one-bite-fits-all trap. The broker reads `__governance` directly from the local log (no consumer, no `KafkaApis.handle()` traversal); operator-tooling consumers are bypassed only when authenticated on the privileged listener AND present in the `governance.bypass.principals` allow-list. Client-id sentinels are diagnostic; they are NOT the authoritative bypass.
- Compression policy from the sibling worktree can ultimately be expressed as a single CEL rule, but for v1 keep them independent.

## Acceptance criteria
- Rule reload is atomic — concurrent readers always observe one of the fully-published RuleSets, never null or partially-constructed state.
- The broker's own startup read of `__governance` does not traverse `KafkaApis.handle()` at all (direct local-log read via `ReplicaManager.getLog`) and is therefore not subject to rule evaluation even when a deny-all rule applies to the API. Operator-tooling consumers of `__governance` are exempt only when authenticated on the privileged listener AND their peer principal is enrolled in `governance.bypass.principals`; the internal-reader client-id prefix is diagnostic and NOT the authoritative bypass.
- When no DENY rule targets a given API key, evaluation is skipped entirely — no field extraction, no principal-attribute resolution, no allocation on the fast path.
- Rule update via tombstone is idempotent — tombstoning an absent rule succeeds, consistent with at-least-once delivery semantics from the rules topic.
- Broker bootstrap drains all existing rules from the rules topic before accepting client connections.
- Rule envelope rejection (invalid CEL, schema mismatch) preserves the previous good RuleSet; partial batch failures do not corrupt loaded state.
- Multiple rules targeting the same API key evaluate in declared order; the first matching DENY short-circuits subsequent evaluations and returns its configured error code.

## Functional test scenarios
- A DENY rule targeting CreateTopics with a CEL expression checking topic-name prefix is published. Three brokers in a cluster converge on the same offset and enforce the rule on the same name. The rule is then tombstoned; all three brokers converge and stop enforcing it without restart.
- A good DENY rule is published; then a malformed rule (referencing a non-existent field) lands in the same batch. The loader rejects the malformed envelope per-record; the previously-good rule remains active with no corruption.
- A DENY rule whose CEL expression matches all principals and all topics is written to `__governance`. The broker's own startup read of `__governance` (direct local-log via `ReplicaManager.getLog`, no `KafkaApis.handle()` traversal) continues to load that rule and any later tombstone without being denied. An operator-tooling consumer of `__governance` connected on the privileged listener and enrolled in `governance.bypass.principals` also continues to fetch; other clients (including the same operator if not on the privileged listener) are immediately blocked.
- Multiple rules target the same API key; the first matching DENY produces the configured error code, and subsequent rules are not evaluated.
- A rule is published targeting Fetch only; a client sends Metadata. The metadata request returns ALLOW without invoking the field extractor or principal-attribute provider, even though the ruleset is non-empty.

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
