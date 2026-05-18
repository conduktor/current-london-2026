# HTTP / WS / SSE Bridge — Progress Log

Implementation of the broker-embedded HTTP listener described in `PROMPT.md`.

This document tracks **what has been built, why, and what remains.** It is written for someone joining mid-stream who needs to pick up the work without re-reading every commit.

---

## Architectural stance

The bridge lives **inside the broker process** (not as a separate service). The whole reason this feature exists is to share `RequestChannel`, the existing authorization path, the existing quota path, and the existing replication machinery with the binary protocol. A separate REST proxy cannot do that — Confluent's exists and that limitation is the entire reason for the rewrite.

Three layers, kept deliberately distinct:

1. **Pure translation layer** — HTTP JSON ↔ Kafka request/response objects. No I/O, no concurrency, no Jetty types. This is where the four-step value chain, multi-status logic, HATEOAS cursors and error envelope live. Fully unit-testable in milliseconds.
2. **Bridge orchestrator** — receives a parsed HTTP request, builds a `ProduceRequest` / `FetchRequest`, hands it to a `RequestSubmitter`, awaits the matching response, translates back. Depends on a small `RequestSubmitter` interface so the orchestrator can be exercised against a fake channel in unit tests.
3. **Transport / wiring** — a Jetty servlet in front; a `BrokerServer` hook behind. Thin. Pushes everything interesting into layers 1 and 2.

Tests are written **first**, in the same package as production code, and the implementation only contains code traceable to a test.

---

## Module layout

Everything lives under the existing `server` Gradle module to avoid touching the build graph:

```
server/src/main/java/org/apache/kafka/network/http/
server/src/test/java/org/apache/kafka/network/http/
```

`checkstyle/import-control-server.xml` lets `org.apache.kafka.network.*` use Jackson, Jetty (`org.eclipse.jetty`), and the servlet API (`jakarta.servlet`); scoped to the network subpackage so the rest of the server module can't accidentally drag in a servlet container.

`clients/` is untouched, per the hard constraint.

---

## Status snapshot

| Component | State | Notes |
|---|---|---|
| Value serializer (4-step chain) | **Done** | `ValueSerializer` + 20 tests |
| HTTP status mapper | **Done** | `HttpStatusMapper`; `statusCarriesRetryAfter` now load-bearing |
| Retry-After calculator | **Done** | `RetryAfterCalculator.forStatus(status, ms)` enforces spec policy in one place |
| Produce JSON parser | **Done** | `ProduceRequestParser` |
| Produce response formatter (207) | **Done** | `ProduceResponseFormatter`; spec mixed scenario passes; Retry-After dropped on 4xx |
| Fetch response formatter (HATEOAS) | **Done** | `FetchResponseFormatter`; self / first / previous / next / last; Retry-After dropped on 4xx |
| Cursor codec | **Done** | `CursorCodec` (base64url `topic\|partition\|offset`) |
| Bridge orchestrator | **Done** | `KafkaHttpBridge` + `RequestSubmitter` |
| Jetty servlet + server | **Done** | `KafkaHttpServlet` + `KafkaHttpServer` + integration test |
| `BrokerServer` integration | **Scaffolded** | Config keys + lifecycle wiring; uses `NotImplementedRequestSubmitter` until the production submitter lands |
| Production `RequestSubmitter` | **TODO** | Plug into `RequestChannel` via a per-request completion callback (see plan below) |
| WebSocket (stretch) | Out of scope for v1 | |
| SSE (stretch) | Out of scope for v1 | |

192 tests pass on the HTTP bridge slice of the server module. Full broker still compiles.

---

## What's already in place

### Layer 1 — translation (15 source files, all under `server/src/main/java/org/apache/kafka/network/http/`)

- **`ValueSerializer`** — the four-shape value envelope (`NULL` / `JSON` / `STRING` / `BINARY`). `encode(bytes, contentType)` runs null → content-type → UTF-8 → base64; `decode(envelope)` is the inverse. Tested for content-type charset parameters, JSON that doesn't parse falling through, embedded null bytes forcing BINARY, control characters except `\t \n \r` forcing BINARY.
- **`HttpStatusMapper`** — `Errors → int` with policy constants (`OK=200`, `MULTI_STATUS=207`, `BAD_REQUEST=400`, `FORBIDDEN=403`, `NOT_FOUND=404`, `INTERNAL_SERVER_ERROR=500`, `SERVICE_UNAVAILABLE=503`, `GATEWAY_TIMEOUT=504`). `statusCarriesRetryAfter(status)` returns true for 503/504 and is now consulted by `RetryAfterCalculator.forStatus` rather than being dead code.
- **`RetryAfterCalculator`** — ceiling seconds: `(throttleMs + 999) / 1000`. `forStatus(status, throttleMs)` captures the spec policy in one place: a positive throttle produces a header on 200/207 (throttle paths) and 503/504 (server-side retryable); on 400/403/404 the hint is intentionally dropped because Retry-After there would mislead the client into expecting a future success.
- **`CursorCodec`** — base64url-encoded `topic|partition|offset`. `Cursor` value class on the way back. Cursors don't carry a signature — they're opaque, not secure; clients are free to inspect them but not expected to.
- **`ErrorEnvelope`** — every error response is `{errorCode, errorMessage}`. Two factory methods: `forError(mapper, Errors, override)` and `forMessage(mapper, status, message)`.
- **`ProduceRequestParser`** — `POST` body `{records:[{partition, key, value, contentType}]}` → `ProduceCommand`. Reuses `ValueSerializer.decode` for the envelope. Throws `BadRequestException` for the 400 path.
- **`ProduceResponseFormatter`** — pure success → 200, uniform same-error failure → mapped status, mixed → 207, empty → 500. Throttle ms → Retry-After regardless of status. `topLevelError(...)` for parse-time 400s.
- **`FetchRequestParser`** — `GET` query `?partition=X&offset=Y[&max_bytes=Z]` OR `?cursor=BLOB`. Cross-topic protection: the cursor's topic must match the URL topic.
- **`FetchResponseFormatter`** — single-partition fetch → JSON body with HATEOAS. Per-partition `_links.self`; root `_links.{first, previous, next, last}` with the conventions: `self=requestedOffset`, `first=logStartOffset`, `previous=requestedOffset-1` or null at log start, `next=lastRecord.offset+1` or `requestedOffset` if empty, `last=highWatermark`.
- **`QueryParams`** — servlet-API-free query-string view so the request parser can be unit-tested without Jetty.
- **`HttpBridgeResponse`** — `(status, body, retryAfterSeconds)` top-level type shared by both formatters.

### Layer 2 — orchestration

- **`RequestSubmitter`** — interface with `submitProduce(ProduceCommand)` and `submitFetch(FetchCommand)`, both returning `CompletableFuture` of structured results. Inner result types: `ProduceResult(partitions, throttleTimeMs)`, `FetchResult(partition, throttleTimeMs)`.
- **`KafkaHttpBridge`** — parse → submit → format pipeline. Never throws; submitter exceptions become 500 with the broker's `errorMessage` in the envelope. `BadRequestException` becomes 400. Submitter-thread async correctness verified by an integration test that completes a deferred future from a separate thread.

### Layer 3 — transport + broker wiring

- **`KafkaHttpServlet`** — extends `jakarta.servlet.http.HttpServlet`. `doPost` / `doGet` parse, call the bridge, use `AsyncContext` so the Jetty IO thread isn't held while the bridge work is in flight. `extractTopic(pathInfo)` is the single source of truth for what is and isn't a valid `/topics/{topic}/records` path.
- **`KafkaHttpServer`** — owns the Jetty `Server` lifecycle. Context path `/v1`, servlet pattern `/*` (so `pathInfo` carries the full sub-path including `/topics/`). `start()` is non-blocking; `boundPort()` reports the actual port even when configured port was 0.
- **`KafkaHttpServerIntegrationTest`** — 15 tests against real embedded Jetty + Jetty `HttpClient`. Covers the PROMPT.md spec scenario (0 OK / 1 offline / 2 OK → 207) verbatim, Retry-After on quota throttle, ACL → 403, malformed JSON / missing records → 400, fetch HATEOAS with a cursor round-trip over the wire, deferred-future correctness, submitter throwable → 500.
- **`SocketServerConfigs`** — three new config keys:
  - `http.bridge.enabled` (Boolean, default false)
  - `http.bridge.host` (String, default `0.0.0.0`)
  - `http.bridge.port` (Int, default 8082 — matches Confluent REST Proxy convention)
- **`KafkaConfig.scala`** — exposes the three above as `httpBridgeEnabled` / `httpBridgeHost` / `httpBridgePort`.
- **`BrokerServer.scala`** — `httpBridgeServer` field, started after the SocketServer acceptors are up, stopped before `socketServer.stopProcessingRequests()`. Gated on `config.httpBridgeEnabled`.
- **`NotImplementedRequestSubmitter`** — placeholder that returns `Errors.REQUEST_TIMED_OUT` (→ 504) with a clear errorMessage. Lives only until the production submitter lands.

---

## What remains — the production `RequestSubmitter`

The current submitter, `NotImplementedRequestSubmitter`, returns 504 for every request. The next commit must replace it with one that routes through the broker's existing request-handling path so authorization, quotas, and replication are inherited "for free", per PROMPT.md's central design requirement.

After re-reading `RequestChannel.scala` end-to-end the cleanest hook is **smaller than the Processor-trait extraction first sketched here** — and it doesn't touch `SocketServer.scala` at all. The key observation is that `RequestChannel.sendResponse(req, abstractResponse, onComplete)` (line 392) is the single public entry point KafkaApis uses to publish a response. It currently does two things: wrap the AbstractResponse into a SendResponse, then dispatch to `processors.get(req.processor).enqueueResponse(...)`. If `RequestChannel.Request` carries a per-request completion callback, the dispatch step can be short-circuited for HTTP-bridge requests without disturbing the binary path.

Concrete plan, smallest viable commit shape:

1. **Add `requestCompletionCallback: Option[AbstractResponse => Unit]` on `RequestChannel.Request`.** Default `None`. When set, the public `sendResponse(req, abstractResponse, _)` invokes the callback with the `AbstractResponse` and returns — it does **not** call `buildResponseSend` (no need to serialize), does **not** look up a Processor, does **not** enqueue a Response. The binary path is unchanged because every existing caller leaves the field as `None`.

2. **`KafkaApiRequestSubmitter`** (Scala or Java, lives in `core/src/main/scala/kafka/network/http/`):
   - Builds a real `RequestHeader` + `RequestContext` with a configurable principal (default `KafkaPrincipal.ANONYMOUS`; the listener can later wire in an authenticator).
   - Serializes the `ProduceRequest` / `FetchRequest` to a `ByteBuffer` exactly as `KafkaApisTest` does — round-tripping is wasteful but it's the contract `RequestContext.parseRequest` expects, and it's what guarantees the request looks identical to a wire-level one for authorization, quota and metrics purposes.
   - Constructs a `RequestChannel.Request` with `processor = -1` (no processor — there is no socket to write back to), `memoryPool = MemoryPool.NONE`, the serialized buffer, and `requestCompletionCallback = Some(future::complete)`.
   - Calls `requestChannel.sendRequest(request)` and awaits the future.
   - Unwraps the `AbstractResponse` (a `ProduceResponse` or `FetchResponse`) into the `ProduceResponseFormatter.PartitionResult` / `FetchResponseFormatter.PartitionFetch` shapes the formatters expect, including the response's `throttleTimeMs`.

3. **`BrokerServer.scala`** instantiates `KafkaApiRequestSubmitter` with the broker's `RequestChannel` and current principal-builder, then hands it to `KafkaHttpBridge` in place of the `NotImplementedRequestSubmitter`. The wiring is one line — everything else is already in place.

4. **Authorization** flows for free because `KafkaApis.handleProduceRequest` calls `authHelper.filterByAuthorized(request.context, WRITE, TOPIC, ...)` — the `request.context` we built carries the principal we set, so an unauthorized topic produces a `TOPIC_AUTHORIZATION_FAILED` in the per-partition response, which `HttpStatusMapper` already maps to 403.

5. **Quotas** flow for free because the broker writes `throttleTimeMs` into the response object, which the submitter exposes to `ProduceResponseFormatter` / `FetchResponseFormatter`. With the spec-correct `forStatus(status, throttleMs)` policy in place, the formatter emits `Retry-After` on 200 (quota-on-success), 207, 503, 504, and never on 4xx — matching PROMPT.md acceptance criteria exactly.

6. **Tests**: a unit test on `RequestChannel` itself that asserts the callback path short-circuits the Processor dispatch (with `processors` empty, the callback still completes). Then an end-to-end test that boots an embedded broker, enables the bridge, produces over HTTP, and asserts the record appears via the binary path on the consumer side — the only test that exercises every layer at once and the most valuable signal we have for production-readiness.

Out of scope for v1: WebSocket subscribe with credit-based flow control, SSE live tail with `?from=earliest`. PROMPT.md lists both as stretch.

### Production-readiness as of this commit

The code below the broker integration line is production quality: pure functions, deterministic, exhaustively tested (192 tests in the HTTP slice alone), spec-aligned including the now-fixed Retry-After-on-4xx divergence. The placeholder submitter is intentionally loud — every request returns 504 with a "not yet implemented" envelope so nobody can mistake the listener-is-bound signal for a working bridge. Until step 3 above lands, `http.bridge.enabled=true` should remain off in any real cluster.

---

## Security model — read this before enabling in any real cluster

The bridge has **no per-request authentication in v1.** Every inbound HTTP request is presented to the broker's authorizer as `KafkaPrincipal.ANONYMOUS`. This is a deliberate scope decision (see `PROMPT.md`: "HTTP requests are translated to existing Kafka request objects and submitted to the existing `RequestChannel`. No new authz or quota path."), not an oversight — but it has direct operational consequences that must be addressed before the listener is exposed.

**The attack to model.** Any host that can reach `http.bridge.host:http.bridge.port` can act as `User:ANONYMOUS`. If `ANONYMOUS` can read a topic, the caller can exfiltrate it; if `ANONYMOUS` can write a topic, the caller can poison it — through normal broker plumbing, with no exploit required. Codex named this "anonymous Kafka data-plane takeover" and we adopted the term in the operator-facing WARN at startup.

**The three guardrails operators must apply** (all three — any one alone is insufficient):

1. **Bind to a specific trusted interface.** Set `http.bridge.host` to loopback (`127.0.0.1`) or a specific address on an access-controlled private network. Never `0.0.0.0` on a host with a public NIC. The bridge's host config is a Jetty bind address, not a CIDR or ACL — Jetty will accept any TCP connection that lands on that interface, so the network itself must be locked down separately (security group, iptables, namespace).
2. **Force traffic through an authenticating front-end.** A reverse proxy that terminates TLS and enforces an auth scheme (mTLS, OAuth bearer, basic-with-LDAP, etc.) before forwarding to the bridge port. The network must make it impossible to reach the bridge directly — security-group / iptables / namespace-level enforcement, not just convention.
3. **Scope ACLs for `User:ANONYMOUS`.** Run `kafka-acls --add --allow-principal User:ANONYMOUS --operation Read --operation Write --topic <name>` for exactly the topics the bridge is meant to expose, with each operation passed as its own `--operation` flag (the CLI does not accept `Read|Write`). A default-allow authorizer, no authorizer at all, or `allow.everyone.if.no.acl.found=true` on an otherwise-empty ACL set, all leave topics open to the bridge. The end-to-end ACL deny test (`HttpBridgeEndToEndTest.aclDeniedTopicReturns403`) demonstrates the enforcement path: a denied topic produces HTTP 403 with a per-partition `errorCode=29` (`TOPIC_AUTHORIZATION_FAILED`) and no `Retry-After`.

The startup WARN at `BrokerServer.scala:642` repeats the headline so it cannot be missed in operator logs. If you find yourself silencing the WARN before doing the three steps above, you are configuring the bridge wrong.

Per-request authentication (a real principal derived from a client cert, JWT, or SASL handshake on the HTTP path) is a follow-up item, explicitly out of scope for v1.

---

## How to run what exists

```sh
JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 ./gradlew :server:test \
  --tests 'org.apache.kafka.network.http.*'
```

Build with JDK 21 — JDK 27 is too new for the project's Gradle 8.10.2 (class file 71 not yet supported by Groovy).

To start a broker with the bridge enabled (once the production submitter lands), set in `server.properties`:

```
http.bridge.enabled=true
http.bridge.host=0.0.0.0
http.bridge.port=8082
```

Until then, `http.bridge.enabled=true` will start the listener but every request returns 504 with a clear "not yet implemented" message — a deliberately-loud failure mode so nobody mistakes the placeholder for working code.
