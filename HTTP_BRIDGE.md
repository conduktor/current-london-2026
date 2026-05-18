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
| HTTP status mapper | **Done** | `HttpStatusMapper`; Retry-After policy for 503/504 |
| Retry-After calculator | **Done** | `RetryAfterCalculator`; ceiling seconds |
| Produce JSON parser | **Done** | `ProduceRequestParser` |
| Produce response formatter (207) | **Done** | `ProduceResponseFormatter`; spec mixed scenario passes |
| Fetch response formatter (HATEOAS) | **Done** | `FetchResponseFormatter`; self / first / previous / next / last |
| Cursor codec | **Done** | `CursorCodec` (base64url `topic\|partition\|offset`) |
| Bridge orchestrator | **Done** | `KafkaHttpBridge` + `RequestSubmitter` |
| Jetty servlet + server | **Done** | `KafkaHttpServlet` + `KafkaHttpServer` + integration test |
| `BrokerServer` integration | **Scaffolded** | Config keys + lifecycle wiring; uses `NotImplementedRequestSubmitter` until the production submitter lands |
| Production `RequestSubmitter` | **TODO** | Plug into `RequestChannel` / `KafkaApis` via a custom `Processor` |
| WebSocket (stretch) | Out of scope for v1 | |
| SSE (stretch) | Out of scope for v1 | |

176 tests pass on the server module. Full broker still compiles.

---

## What's already in place

### Layer 1 — translation (15 source files, all under `server/src/main/java/org/apache/kafka/network/http/`)

- **`ValueSerializer`** — the four-shape value envelope (`NULL` / `JSON` / `STRING` / `BINARY`). `encode(bytes, contentType)` runs null → content-type → UTF-8 → base64; `decode(envelope)` is the inverse. Tested for content-type charset parameters, JSON that doesn't parse falling through, embedded null bytes forcing BINARY, control characters except `\t \n \r` forcing BINARY.
- **`HttpStatusMapper`** — `Errors → int` with policy constants (`OK=200`, `MULTI_STATUS=207`, `BAD_REQUEST=400`, `FORBIDDEN=403`, `NOT_FOUND=404`, `INTERNAL_SERVER_ERROR=500`, `SERVICE_UNAVAILABLE=503`, `GATEWAY_TIMEOUT=504`). `statusCarriesRetryAfter(status)` returns true only for 503/504. The quota path on 200 sets Retry-After explicitly.
- **`RetryAfterCalculator`** — ceiling seconds: `(throttleMs + 999) / 1000`. Zero throttle returns zero (no header).
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

The shape of the work, based on a deep read of `RequestChannel.scala` (line 419+) and `SocketServer.scala` (line 795+):

1. **Register a synthetic `Processor`** with the broker's `RequestChannel`. The `Processor` class in `SocketServer.scala` is `private[kafka]` and tied to a real `Selector`/`ConnectionQuotas`; a clean approach is to extract a small `Processor` trait (id, enqueueResponse, responseQueueSize) and have both the existing concrete `Processor` and a new `HttpBridgeProcessor` implement it. Then `RequestChannel.addProcessor` accepts the trait.
2. **`HttpBridgeProcessor`** holds a `ConcurrentHashMap<connectionId, CompletableFuture<AbstractResponse>>`. Its `enqueueResponse(response)` looks up the connection ID on `response.request.context.connectionId` and completes the future with the response's `AbstractResponse`. Each HTTP request gets a unique connection ID (UUID is fine — it's only used for response correlation).
3. **`KafkaApiRequestSubmitter`** (the production implementation):
   - Builds a real `RequestHeader` + `RequestContext` (with a configurable principal — probably `KafkaPrincipal.ANONYMOUS` by default; the listener can later be tied to an authenticator).
   - Serializes a `ProduceRequest` / `FetchRequest` to a `ByteBuffer` (parallel to what `KafkaApisTest` does).
   - Constructs a `RequestChannel.Request` with `processor = httpBridgeProcessor.id`, then `requestChannel.sendRequest(request)`.
   - Awaits the matching future, unwraps the `ProduceResponse` / `FetchResponse` into the `ProduceResponseFormatter.PartitionResult` / `FetchResponseFormatter.PartitionFetch` shapes the formatters expect.
4. **Authorization** flows for free because `KafkaApis.handleProduceRequest` calls `authHelper.filterByAuthorized(request.context, WRITE, TOPIC, ...)` — the `request.context` we built carries the principal we set.
5. **Quotas** flow for free because the response object carries `throttleTimeMs`; the existing `ProduceResponseFormatter` already turns positive throttle into a Retry-After header.

Out of scope for v1: WebSocket subscribe with credit-based flow control, SSE live tail with `?from=earliest`. PROMPT.md lists both as stretch.

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
