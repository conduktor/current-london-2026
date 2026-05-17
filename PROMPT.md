# Multi-Tenancy

## Goal
Tenant `acme` produces/consumes topic `orders`; the broker stores `acme.orders`; the client never sees the prefix. v1 covers Produce / Fetch / Metadata / CreateTopics / DeleteTopics / ListTopics only. Zero changes under `clients/`.

## Constraints (do not violate)
- **Do not modify anything under `clients/`.** Stock producers/consumers connect with SASL and see a pristine cluster.
- Tenant is resolved once at SASL handshake (per-listener config mapping) and embedded in `KafkaPrincipal` for the connection lifetime as `__tenant_<id>.<user>`.
- `__consumer_offsets` and `__transaction_state` pass through unchanged (they key by tenant-scoped identifiers, not by visible topic name).
- Block silent pollution: privileged caller on a tenant-bound listener without a tenant principal → reject the request rather than rewrite into the tenant namespace.

## Minimum viable outcome
1. SASL handshake hook → resolve tenant from per-listener config → encode in principal.
2. Inline logical→physical rewrite on the way **in** for: Produce, Fetch, Metadata, CreateTopics, DeleteTopics, ListTopics.
3. Inline physical→logical rewrite on the way **out**, including in error responses (so `TopicAuthorizationException` never leaks the prefix).
4. Two tenants both creating `orders` end up with independent physical topics `acme.orders` and `beta.orders`.

## Stretch (only after the minimum lands)
- Group coordinator + transaction coordinator rewrites (consumer groups, transactional IDs).
- Token-based tenant resolution backed by `__ivy_identity_tokens`.
- All remaining request handlers (AlterConfigs, DescribeAcls, etc.).
- Revocation within 30 s on token tombstone.

## Careful
- Do **not** try a clean interceptor/filter pattern. Authorization needs the physical name mid-handler; error messages need the logical name; coordinator lookups need both. This forces inline rewriting in each handler.
- Rewrite error responses too — otherwise the physical prefix leaks.
- Super-users on tenant-bound listeners silently pollute the tenant namespace unless explicitly guarded.
- An Authorizer alone is not enough: it gives isolation but not name transparency.

## Lessons already known (don't rediscover)
- Tests with plain names (`my-topic` instead of `acme.my-topic`) hit `TopicAuthorizationException`. Provide a `BROKER_NAMESPACE_ENFORCE=false` test profile bypass.
- The privileged-on-tenant-listener trap fails silently as data corruption, not as a crash. Explicit guard required.

## Acceptance criteria
- Tenant assignment is resolved and embedded in the KafkaPrincipal at SASL handshake time using a standardised format (`__tenant_<id>.<user>`), and remains immutable for the connection lifetime.
- Two tenants connecting via the same listener or via different tenant-mapped listeners and creating a topic with the same logical name end up with independent physical topics with distinct topic IDs and no collision in metadata or storage.
- All user-visible responses (Metadata, DescribeTopics, ListTopics, DescribeConfigs, DescribeAcls) strip the physical prefix; tenants see only the logical name they provided. Internal / inter-broker listeners see both.
- Error messages in client responses never contain the physical prefix; authorization failures, missing-topic errors, and invalid-operation errors all report the logical name.
- Consumer group IDs and transactional IDs are rewritten using the same prefix mechanism as topic names; groups and transactions are isolated per tenant despite sharing physical coordinator state.
- Internal Kafka topics (`__consumer_offsets`, `__transaction_state`, share-group state) are never tenant-prefixed, even when accessed through a tenant-bound listener.
- A privileged principal (super.user, inter-broker) on a tenant-bound listener without a `__tenant_` prefix in its identity is rejected when attempting to operate on a tenant namespace; admin operations on tenant resources via a cluster-wide listener (explicitly naming `__tenant_*`) remain allowed.
- Resource limits, ACLs, and quotas are evaluated against the physical (prefixed) name during enforcement, but reported to clients and administrative tooling against the logical name.

## Functional test scenarios
- Two tenants connect in parallel via distinct tenant-mapped listeners; each issues CreateTopic for `orders`; both succeed; ListTopics for each tenant returns only their own `orders`; an internal listener shows two distinct physical topics with different topic IDs.
- Tenant A produces and consumes via Produce, Fetch, and ListOffsets; logical offsets are tenant-local and independent of another tenant's traffic on the same backing. DeleteRecords from tenant A advances only its logical low-water mark.
- Consumer groups and transactional IDs created by tenants are stored under their physical names and remain invisible to other tenants; OffsetCommit/FetchOffsets and InitProducerId/AddPartitionsToTxn succeed independently per tenant with the same external group or transactional ID.
- DescribeConfigs and AlterConfigs hide tenant-internal metadata fields from tenant clients; only logical topic names are visible; attempts to query by physical name from a tenant listener are rejected.
- A privileged caller on a non-tenant listener explicitly requesting `--describe --group __tenant_acme.foo` (pre-prefixed) succeeds and shows the tenant's group state; the same caller on a tenant-bound listener attempting to create a consumer group without a tenant principal prefix is rejected with an explicit error.
- An authorization failure on a tenant-bound listener (e.g., a tenant attempting to access another tenant's data via a guessed physical name) returns an error whose message references only the logical name the tenant could legitimately use, not the physical one.

## When stuck (escape hatch)
If you find yourself looping on the same dead-end — the same error reappearing, the same refactor reverted, no measurable progress over several attempts — the order of recourse is fixed:

1. **First, ask Codex** (OpenAI's app) for a second opinion on the specific blocker. Frame the question sharply: what you tried, what went wrong, what you suspect. Take its answer seriously and try the direction it suggests.
2. **Only if you remain stuck after Codex's input**, you may consult `/home/florent/ivy-trunk` read-only **for inspiration only**. Read enough to understand the shape of a solution, then close the file and write your own from scratch in this repo's idiom. **No copy-paste. No transliteration. No "I'll just adapt this block."**

The commit that follows an ivy-trunk consultation must include in its message a one-liner of the form: `Stuck in local minimum on <one-line description>; Codex suggested <X> which <was/wasn't> enough; consulted ivy-trunk for inspiration; took away <one-sentence insight>; moved on.` If you cannot honestly write that sentence, you copied — revert and try again. The escape hatch exists to break unproductive loops, not to import an implementation.

## Working mode: full autonomy with Codex review
You must drive this project to completion in **total autonomy**. No human babysitting, no mid-flight permission requests, no "should I continue?" check-ins. Plan, decide, implement, test, iterate, ship. The acceptance criteria and functional test scenarios above are your contract — done means they all pass.

**Regularly solicit Codex (OpenAI's app) for a second opinion.** At minimum, consult Codex at: (a) end of initial planning, before the first implementation commit; (b) when choosing between two non-trivial design options; (c) after each milestone (minimum viable outcome reached; each stretch item considered); (d) whenever you're blocked for more than one iteration on the same surface. Present Codex with the relevant context and a sharp question. **Take its feedback seriously** — integrate what improves the work, push back in writing on what doesn't, never silently ignore. Record each consultation briefly in the commit body: `Codex review: asked <one-line question>; took <one-line takeaway>; rejected <one-line dissent> because <reason>.`
