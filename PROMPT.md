# HTTP / WebSocket / SSE

## Goal
`POST /v1/topics/{topic}/records` and `GET /v1/topics/{topic}/records?partition=X&offset=Y` translate to `ProduceRequest` / `FetchRequest` and go through the same `RequestChannel`, authorization and quotas as the binary protocol. v1 is **HTTP only** — WebSocket and SSE are stretch. Zero changes under `clients/`.

## Constraints (do not violate)
- **Do not modify anything under `clients/`.** The new endpoints are a parallel wire format, not a client API.
- HTTP requests are translated to existing Kafka request objects and submitted to the existing `RequestChannel`. No new authz or quota path.
- Multi-partition partial failure → HTTP **207 Multi-Status** with per-partition JSON results.
- Record value serialisation chain: null → content-type header → UTF-8 validity → base64 fallback.

## Minimum viable outcome
1. Embedded HTTP listener inside the broker process on a configurable port.
2. `POST /v1/topics/{topic}/records` → build `ProduceRequest` → `RequestChannel` → JSON response (or 207 on partial failure).
3. `GET /v1/topics/{topic}/records?partition=X&offset=Y[&max_bytes=...]` → build `FetchRequest` → JSON response with the records and the next-offset link.
4. ACL deny → HTTP 403; quota exceeded → HTTP 429 with `Retry-After`.

## Stretch (only after the minimum lands)
- WebSocket subscribe with credit-based flow control.
- SSE live tail with `?from=earliest`.
- Full HATEOAS expansion (links for partition metadata, topic metadata, next/previous offsets).

## Careful
- HTTP has no native partial-success code. 200 lies about failures, 500 lies about successes — 207 (WebDAV) is the only correct answer.
- Record values are bytes, not strings. The serialisation chain has four edge cases; get it right once and reuse it everywhere.
- The whole point of this feature is to share `RequestChannel`, authz, quotas, and replication with the binary protocol. **Do not** reimplement any of those server-side — translate and submit.

## Lessons already known (don't rediscover)
- Confluent REST Proxy is a separate process and can't share auth, quotas, or replication. Living inside the broker is the entire value proposition; do not drift away from that.

## Acceptance criteria
- HTTP requests share the broker's authorization path via the existing RequestChannel and KafkaApis; the same authorizer evaluates permissions as for the binary protocol — no separate HTTP authz code. Authorization failures return 403.
- Multi-partition produce or fetch with heterogeneous results (one partition succeeds, another offline) returns HTTP 207 Multi-Status with per-partition status and error codes in the JSON body. Pure success → 200; uniform failure → an appropriate error status; mixed → always 207.
- Quota exhaustion on HTTP produce returns 200 with a `Retry-After` header (in seconds, ceiling of the throttle delay). The response indicates the request was accepted; the header signals slow-down before the next call. 429 is reserved for the WebSocket pre-flight throttle, not the HTTP produce path.
- Record value serialisation follows a four-step chain: `null` → `{type: NULL}`; content-type `application/json` → `{type: JSON, data: parsed}`; valid UTF-8 without disallowed control characters → `{type: STRING, data: decoded}`; otherwise → `{type: BINARY, data: base64}`.
- WebSocket subscriptions enforce credit-based backpressure: subscribe with N initial credits → exactly N messages delivered → client must grant additional credits to continue. Zero credits → no delivery. A fast producer cannot cause unbounded server buffering.
- SSE live tail (`?from=earliest`) streams all historical records then continues streaming new records as they arrive, without the client reconnecting. The transition from replay to live is seamless.
- Error response shape is consistent: every error response includes `errorCode` and `errorMessage` fields. Status codes carrying `Retry-After` are 503 / 504 / throttle paths. 403, 404, 400 do not carry `Retry-After`.
- Fetch responses include HATEOAS links in the JSON body — each partition result carries a self-link and the response root carries `next`, `previous`, `first`, `last` cursors. Clients follow links via opaque cursors, not URL templating.

## Functional test scenarios
- A POST to `/v1/topics/{topic}/records` targeting three partitions, where partition 0 succeeds with an offset assigned, partition 1 is offline, and partition 2 succeeds, returns HTTP 207 with a JSON body containing three partition entries, each with partition index, offset (or null), and errorCode.
- A WebSocket subscribe with `initialCredits=5` against a queue holding 20 messages delivers exactly 5 messages then pauses; granting 10 more credits delivers the next 10; without additional credits, no further messages flow.
- A produce request exceeding the configured `producer_byte_rate` quota returns 200 with the offset array populated and a `Retry-After: 2` (or appropriate ceiling) header.
- A GET `/v1/topics/{topic}/records?from=earliest` without a recognised client identity returns 403 when the anonymous principal lacks READ ACL for the topic; granting the ACL to the anonymous principal allows the same request to succeed.
- A GET `/v1/topics/{topic}/records` with `Accept: application/hal+json` returns a JSON page containing `_links` with `self`, `first`, `next`, `last` cursors; fetching the next link via cursor (not URL construction) returns the next page.
- An SSE `/v1/topics/{topic}/records?from=earliest` streams the full backlog and transitions without reconnection to streaming new records as they are produced.

## When stuck (escape hatch)
If you find yourself looping on the same dead-end — the same error reappearing, the same refactor reverted, no measurable progress over several attempts — you may consult `/home/florent/ivy-trunk` read-only **for inspiration only**. Read enough to understand the shape of a solution, then close the file and write your own from scratch in this repo's idiom. **No copy-paste. No transliteration. No "I'll just adapt this block."** The commit that follows the consultation must include in its message a one-liner of the form: `Stuck in local minimum on <one-line description of the dead-end>; consulted ivy-trunk for inspiration; took away <one-sentence insight>; moved on.` If you cannot honestly write that sentence, you copied — revert and try again. The escape hatch exists to break unproductive loops, not to import an implementation.
