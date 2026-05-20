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
| Value serializer (4-step chain) | **Done** | `ValueSerializer` |
| HTTP status mapper | **Done** | `HttpStatusMapper`; `statusCarriesRetryAfter` consulted by the calculator, no dead code |
| Retry-After calculator | **Done** | `RetryAfterCalculator.forStatus(status, ms)` enforces spec policy in one place (yes on 200 / 207 / 503 / 504; no on 400 / 403 / 404) |
| Produce JSON parser | **Done** | `ProduceRequestParser` |
| Produce response formatter (207) | **Done** | `ProduceResponseFormatter`; spec mixed scenario passes; consistent `{topic, results:[...]}` body for 200 / 207 / uniform-failure |
| Fetch response formatter (HATEOAS) | **Done** | `FetchResponseFormatter`; self / first / previous / next / last; Retry-After dropped on 4xx |
| Content-type negotiation | **Done** | `ContentTypeNegotiator`; HAL+JSON when listed at any q-weight; SSE wins over JSON / HAL when present |
| Cursor codec | **Done** | `CursorCodec` (base64url `topic\|partition\|offset`); cursors must match the URL topic |
| Bridge orchestrator | **Done** | `KafkaHttpBridge` + `RequestSubmitter` |
| Jetty servlet + server | **Done** | `KafkaHttpServlet` + `KafkaHttpServer` |
| Production `RequestSubmitter` | **Done** | `KafkaApiRequestSubmitter` routes HTTP through `RequestChannel` → `KafkaApis`; authorization, quotas, replication inherited from the binary path |
| `BrokerServer` integration | **Done** | Wired behind `http.bridge.enabled` (default `false`); operator-facing WARN + runbook describe the ANONYMOUS threat model |
| SSE live tail | **Done** | `SseStreamer` recursive long-poll; `id: <offset>` + `data:` per record; `event: error` on partition errors; real-broker test covers replay → live boundary; the WHATWG `Last-Event-ID` reconnect header is deliberately ignored — resume must use the `cursor=` query parameter (browser `EventSource` clients that auto-reconnect on Last-Event-ID alone will replay from initial cursor); SSE responses emit `X-Accel-Buffering: no` so nginx-family reverse proxies do not buffer the stream into a fixed-size lump (operators using non-nginx proxies still need to disable response buffering server-side) |
| End-to-end (real broker) | **Done** | `HttpBridgeEndToEndTest`: HTTP-produce → binary-consume round-trip, mixed-partition 207, ACL deny on POST → 403 (FS4 write-side), ACL deny on GET → 403 (FS4 fetch-side), SSE replay→live, HAL+JSON cursor follow across pages with offset-contiguity assertion (FS5), quota-throttle → 200 + Retry-After (FS3 / AC3) |
| Operator security guidance | **Done** | Startup WARN names the ANONYMOUS-principal threat and points at the runbook; the "anonymous Kafka data-plane takeover" phrasing is preserved in the `BrokerServer` threat-model comments and `HostBindingValidator` Javadoc for the source-reading audience; `HTTP_BRIDGE.md` Security model section ships the three-guardrail runbook |
| Admission control | **Done** | Three admission caps in front of the broker — request body byte cap → 413, broker-side request timeout → 504, concurrent-SSE-stream cap → 429 + Retry-After — plus a threading-isolation invariant that dispatches HTTP completion writes to Jetty's server thread pool so a slow HTTP client cannot pin a broker request-handler thread. See "Admission control and thread isolation" below |
| HTTP-level metrics | **Done** | `HttpBridgeMetrics` exposes per-operation latency histograms, per-(operation, status-class) response meters, 413/429 rejection meters, an SSE-opened meter, and a live SSE-concurrency gauge under `kafka.network.http:type=BridgeMetrics`. Operator-graphable independently of `RequestChannel`. See "Bridge metrics (JMX)" below |
| WebSocket subscribe (credit-gated) | **Done** | `KafkaWebSocketEndpoint` + `WsStreamer` + `WsSubscribeMessageParser` + `WsStreamLimiter`; PROMPT.md AC5 / FS2 — subscribe with N initial credits delivers exactly N records then pauses; client flow grant of M delivers exactly M more; cap on concurrent subscriptions refuses additional upgrades with HTTP 429 (PROMPT.md line 34: "429 is reserved for the WebSocket pre-flight throttle"). Real-broker end-to-end test exercises the credit-flow contract against a live KRaft broker |

423 unit and embedded-Jetty tests pass in the `server` HTTP bridge slice — including dedicated coverage for the four admission-control caps (`BoundedRequestBodyTest`, `KafkaHttpBridgeTest` timeout cases, `SseStreamLimiterTest`, `WsStreamLimiterTest`, plus the `produceWithOversizedBodyReturns413`, `sseReturns429WhenConcurrentStreamCapReached`, and `wsReturns429WhenSubscriptionCapReached` integration tests), the new HTTP-level metrics layer (`HttpBridgeMetricsTest` plus six wiring tests in `KafkaHttpServerIntegrationTest` covering both SSE and WS counters/gauges), the WebSocket frame parser (`WsSubscribeMessageParserTest`), the credit-gated streamer (`WsStreamerTest`), and the WebSocket session endpoint (`KafkaWebSocketEndpointTest`) — plus eight real-broker end-to-end tests in `core`. Full broker still compiles. Run them with:

```sh
JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 ./gradlew \
  :server:test --tests 'org.apache.kafka.network.http.*' \
  :core:test --tests 'kafka.network.http.HttpBridgeEndToEndTest'
```

---

## What's already in place

### Layer 1 — translation (all under `server/src/main/java/org/apache/kafka/network/http/`)

- **`ValueSerializer`** — the four-shape value envelope (`NULL` / `JSON` / `STRING` / `BINARY`). `encode(bytes, contentType)` runs null → content-type → UTF-8 → base64; `decode(envelope)` is the inverse. Tested for content-type charset parameters, JSON that doesn't parse falling through, embedded null bytes forcing BINARY, control characters except `\t \n \r` forcing BINARY.
- **`HttpStatusMapper`** — `Errors → int` with policy constants (`OK=200`, `MULTI_STATUS=207`, `BAD_REQUEST=400`, `FORBIDDEN=403`, `NOT_FOUND=404`, `INTERNAL_SERVER_ERROR=500`, `SERVICE_UNAVAILABLE=503`, `GATEWAY_TIMEOUT=504`). `statusCarriesRetryAfter(status)` returns true for 503/504 and is now consulted by `RetryAfterCalculator.forStatus` rather than being dead code.
- **`RetryAfterCalculator`** — ceiling seconds: `(throttleMs + 999) / 1000`. `forStatus(status, throttleMs)` captures the spec policy in one place: a positive throttle produces a header on 200/207 (throttle paths) and 503/504 (server-side retryable); on 400/403/404 the hint is intentionally dropped because Retry-After there would mislead the client into expecting a future success.
- **`CursorCodec`** — base64url-encoded `topic|partition|offset`. `Cursor` value class on the way back. Cursors don't carry a signature — they're opaque, not secure; clients are free to inspect them but not expected to.
- **`ErrorEnvelope`** — every error response is `{errorCode, errorMessage}`. Two factory methods: `forError(mapper, Errors, override)` and `forMessage(mapper, status, message)`.
- **`ProduceRequestParser`** — `POST` body `{records:[{partition, key, value, contentType}]}` → `ProduceCommand`. Reuses `ValueSerializer.decode` for the envelope. Throws `BadRequestException` for the 400 path.
- **`ProduceResponseFormatter`** — pure success → 200, uniform same-error failure → mapped status, mixed → 207, empty → 500. Throttle ms → Retry-After regardless of status. `topLevelError(...)` for parse-time 400s.
- **`FetchRequestParser`** — `GET` query `?partition=X&offset=Y[&max_bytes=Z]`, OR `?partition=X&from=earliest[&max_bytes=Z]` (resolves to offset 0 and enables `out_of_range` retry at `logStartOffset`; `from=latest` is intentionally not supported in v1 — see `readStartOffset` Javadoc), OR `?cursor=BLOB` (mutually exclusive with `partition` / `offset` / `from`; the cursor's topic must match the URL topic).
- **`FetchResponseFormatter`** — single-partition fetch → JSON body with HATEOAS. Per-partition `_links.self`; root `_links.{self, first, previous, next, last}` with the conventions: `self=requestedOffset`, `first=logStartOffset`, `previous=requestedOffset-1` or null at log start, `next=lastRecord.offset+1` or `requestedOffset` if empty, `last=highWatermark`. **Wire-side amplification:** `max_bytes` is a *Kafka fetch* cap, not an HTTP response cap. For binary payloads, `ValueSerializer.encode` base64-encodes each record (4/3 inflation, rounded up) and Jackson holds both the raw bytes and the encoded string transiently — total heap during response assembly is roughly `2.6 × max_bytes`. Size `max_bytes` against broker heap budget, not against on-disk record count.
- **`QueryParams`** — servlet-API-free query-string view so the request parser can be unit-tested without Jetty.
- **`HttpBridgeResponse`** — `(status, body, retryAfterSeconds)` top-level type shared by both formatters.

### Layer 2 — orchestration

- **`RequestSubmitter`** — interface with `submitProduce(ProduceCommand)` and `submitFetch(FetchCommand)`, both returning `CompletableFuture` of structured results. Inner result types: `ProduceResult(partitions, throttleTimeMs)`, `FetchResult(partition, throttleTimeMs)`.
- **`KafkaHttpBridge`** — parse → submit → format pipeline. Never throws; submitter exceptions become 500 with the broker's `errorMessage` in the envelope. `BadRequestException` becomes 400. Submitter-thread async correctness verified by an integration test that completes a deferred future from a separate thread.

### Layer 3 — transport + broker wiring

- **`KafkaHttpServlet`** — extends `jakarta.servlet.http.HttpServlet`. `doPost` / `doGet` parse, call the bridge, use `AsyncContext` so the Jetty IO thread isn't held while the bridge work is in flight. `extractTopic(pathInfo)` is the single source of truth for what is and isn't a valid `/topics/{topic}/records` path.
- **`KafkaHttpServer`** — owns the Jetty `Server` lifecycle. Context path `/v1`, servlet pattern `/*` (so `pathInfo` carries the full sub-path including `/topics/`). `start()` is non-blocking; `boundPort()` reports the actual port even when configured port was 0.
- **`KafkaHttpServerIntegrationTest`** — 65 tests against real embedded Jetty + Jetty `HttpClient` + Jetty `WebSocketClient`. Covers the PROMPT.md spec scenario (0 OK / 1 offline / 2 OK → 207) verbatim, Retry-After on quota throttle, ACL → 403, malformed JSON / missing records → 400, fetch HATEOAS with a cursor round-trip over the wire, deferred-future correctness, submitter throwable → 500, admission-control coverage (`produceWithOversizedBodyReturns413`, `sseReturns429WhenConcurrentStreamCapReached`, `wsReturns429WhenSubscriptionCapReached`), SSE replay-then-live plus partition-error coverage, and the WebSocket happy path including `wsSubscribeDeliversInitialCreditsThenFlowDeliversMore` (PROMPT.md FS2) and `wsClosesWith1003WhenFirstFrameIsNotSubscribe`.
- **`SocketServerConfigs`** — eight config keys for the bridge:
  - `http.bridge.enabled` (Boolean, default false)
  - `http.bridge.host` (String, default `127.0.0.1` — loopback only; see Security model)
  - `http.bridge.port` (Int, default 8082 — matches Confluent REST Proxy convention)
  - `http.bridge.max.request.body.bytes` (Int, default 1048576 — request bodies past this cap are rejected with `413 Payload Too Large` before the body is fully read into memory)
  - `http.bridge.request.timeout.ms` (Int, default 30000 — broker-side wait cap on a single produce/fetch; on expiry the bridge emits `504 Gateway Timeout` with `Retry-After` so a dropped `RequestChannel` response cannot orphan the HTTP client indefinitely)
  - `http.bridge.max.concurrent.sse.streams` (Int, default 100 — additional SSE streams beyond this cap are refused with `429 Too Many Requests` + `Retry-After: 5` so a runaway client cannot exhaust the Jetty server thread pool; set to 0 to disable SSE entirely while leaving produce / fetch / WebSocket subscribe open)
  - `http.bridge.max.concurrent.ws.subscriptions` (Int, default 100 — additional WebSocket upgrades beyond this cap are refused at the handshake with `429 Too Many Requests` + `Retry-After: 5`; set to 0 to disable WebSocket subscribe entirely while leaving produce / fetch / SSE open)
  - `http.bridge.shutdown.grace.ms` (Int, default 5000 — grace period during broker shutdown for in-flight HTTP requests and active SSE/WS streams to drain before `Server.stop()` joins the Jetty thread pool; `0` is the legacy ungraceful path used by tests)
- **`KafkaConfig.scala`** — exposes all eight as `httpBridgeEnabled` / `httpBridgeHost` / `httpBridgePort` / `httpBridgeMaxRequestBodyBytes` / `httpBridgeRequestTimeoutMs` / `httpBridgeMaxConcurrentSseStreams` / `httpBridgeMaxConcurrentWsSubscriptions` / `httpBridgeShutdownGraceMs`.
- **`BrokerServer.scala`** — `httpBridgeServer` field, started after the SocketServer acceptors are up, stopped before `socketServer.stopProcessingRequests()`. Gated on `config.httpBridgeEnabled`.
- **`KafkaApiRequestSubmitter`** (Scala, `core/src/main/scala/kafka/network/http/`) — the production submitter. Builds a `RequestContext` with `KafkaPrincipal.ANONYMOUS`, serializes the request to a `ByteBuffer`, hands it to `RequestChannel.sendRequest` with a per-request completion callback, awaits the `AbstractResponse`, and projects it back into the formatter input shape. The mechanism is described in detail in "How the broker integration works" below.

### WebSocket subscribe (credit-gated live tail)

The WebSocket path is the explicit-backpressure counterpart to SSE. Where SSE delivers records implicitly (one fetch per delivered record, gated by socket write completion), WebSocket delivers them under a client-controlled credit budget: subscribe with N initial credits, receive exactly N records, then pause until the client sends a `flow` message granting more. This is the PROMPT.md AC5 / FS2 contract.

- **`WsSubscribeMessageParser`** — parses the two client-to-server text frames:
  - `{"type":"subscribe","partition":<int>,"offset":<long>,"initialCredits":<int>[,"maxBytes":<int>]}` — first frame on the connection, sets the partition + offset + initial credit budget; the topic is taken from the URL path, not the body, so it cannot drift between the upgrade route and the subscribe command.
  - `{"type":"flow","credits":<int>}` — every subsequent frame; positive integer required.

  A protocol violation throws `BadMessageException`, which the endpoint maps to a WebSocket close with status `1003` (`BAD_DATA`) and an `{type:"error",errorCode:"BAD_MESSAGE",errorMessage:...}` envelope on the same socket.
- **`WsStreamer`** — drives the credit-gated drain. Single-flight dispatch on the supplied executor (Jetty's server thread pool, same as the SSE path), single-fetch carry-over buffer, atomic-CAS credit accounting. The streamer deliberately does not depend on Jetty types — it talks to a `FrameSink` interface that the endpoint wraps around the live `Session` — so the credit-accounting logic is fully unit-testable without standing up an HTTP server. A `WsStreamer.start(...)` accepts a `WsStreamLimiter.Token` and takes ownership: the streamer's `close()` releases the slot, so a misbehaving client that drops the socket does not leak a subscription slot.
- **`WsStreamLimiter`** — the admission cap, separate from `SseStreamLimiter` because the two paths have different per-connection cost profiles (WS subscriptions can be far longer-lived than SSE streams under steady credit-grant traffic). `Token` is `AutoCloseable` and idempotent; a negative cap fails fast at construction; a zero cap is a valid degenerate config (every WS upgrade refused, but produce / fetch / SSE stay open).
- **`KafkaWebSocketEndpoint`** — the Jetty 12 `Session.Listener.AutoDemanding` implementation. One per upgraded connection. Owns the protocol state machine (OPEN → SUBSCRIBED → CLOSED), the limiter token (until ownership transfers to the streamer at the first valid subscribe frame), and the mapping from streamer-level failures to WebSocket close codes (`1003` for protocol violations, `1011` for internal errors, `1000` for normal closure). The class is intentionally `public final` — Jetty's listener-method dispatch in `JettyWebSocketFrameHandlerFactory` resolves handlers through `MethodHandles.Lookup.unreflect()` from a different package and that lookup requires public class access, otherwise the upgrade attempt fails with `IllegalAccessException` surfaced as HTTP 500.
- **`KafkaHttpServer` upgrade wiring** — `JettyWebSocketServletContainerInitializer.configure(context, ...)` installs the WS filter on the same `ServletContextHandler` that hosts the produce / fetch servlet. The path spec is `"uri-template|/topics/{topic}/subscribe"`; the explicit `uri-template|` prefix is load-bearing because Jetty's `WebSocketMappings.parsePathSpec` routes any spec starting with `/` through `ServletPathSpec`, which treats `{topic}` as a literal segment — so the bare form would match the literal string `"{topic}"` and every real topic would 404. The upgrade-time creator extracts the topic, calls `wsLimiter.tryAcquire()`, and either returns a constructed endpoint (which now owns the token) or sends `429 + Retry-After: 5`. Construction is wrapped in `try/catch` so any throw between `tryAcquire()` and the returned endpoint releases the limiter slot before propagating; the `WsSubscriptionsOpened` meter is incremented after successful construction so accepted + rejected always sum to exactly the offered upgrade load.

---

## How the broker integration works

The `RequestChannel` hook is the load-bearing piece of the bridge — it is what lets HTTP requests inherit the binary protocol's authorization, quota, and replication paths without a parallel implementation.

The mechanism, end-to-end:

1. **`RequestChannel.Request` carries an optional `requestCompletionCallback: Option[AbstractResponse => Unit]`.** The public `sendResponse(req, abstractResponse, _)` checks this first: when set, it invokes the callback with the `AbstractResponse` and returns. It does not call `buildResponseSend` (no serialization needed — the bridge owns the wire format), does not look up a Processor, does not enqueue a Response. Every binary-path caller leaves the field as `None`, so that flow is unchanged.

2. **`KafkaApiRequestSubmitter`** (in `core/src/main/scala/kafka/network/http/`) builds a real `RequestHeader` + `RequestContext` with `KafkaPrincipal.ANONYMOUS` and the inter-broker listener name, serializes the `ProduceRequest` / `FetchRequest` to a `ByteBuffer` (the round-trip is necessary — `RequestContext.parseRequest` is what authorization, quota and metrics machinery downstream all consume), constructs a `RequestChannel.Request` with `processor = -1`, `memoryPool = MemoryPool.NONE`, and `requestCompletionCallback = Some(future::complete)`, then calls `requestChannel.sendRequest(...)` and awaits the future. The unwrap projects `ProduceResponse` / `FetchResponse` into `ProduceResponseFormatter.PartitionResult` / `FetchResponseFormatter.PartitionFetch`, including `throttleTimeMs`.

3. **`BrokerServer.scala`** wires the submitter behind `config.httpBridgeEnabled` and starts the Jetty `KafkaHttpServer` after the SocketServer acceptors are up.

What this buys, for free:

- **Authorization** — `KafkaApis.handleProduceRequest` and `handleFetchRequest` both call `authHelper.filterByAuthorized(request.context, ...)`. The `request.context` carries the principal the submitter set (`ANONYMOUS` in v1); a denied topic produces a `TOPIC_AUTHORIZATION_FAILED` in the per-partition response, mapped to HTTP 403 by `HttpStatusMapper`. Demonstrated by `HttpBridgeEndToEndTest.aclDeniedTopicReturns403` against a real KRaft cluster with a custom `StandardAuthorizer` subclass.
- **Quotas** — the broker writes `throttleTimeMs` into the response object; the submitter forwards it to the formatter, which renders `Retry-After` on the spec-allowed statuses (200 / 207 / 503 / 504) and drops it on 400 / 403 / 404 (`RetryAfterCalculator.forStatus`).
- **Replication** — partitions go through the normal `ReplicaManager.appendRecords` path; min-ISR, acks=all, and replica fetch behaviour are inherited unchanged.

### Admission control and thread isolation

The authorization story (Security model section) protects topic data from a caller who can already reach the bridge port. A second class of failure — *availability* — is what a real operator hits first: a single oversized POST, a hung produce that never wakes, a forgotten SSE consumer that keeps tying up Jetty threads, a runaway WebSocket subscribe loop that opens arbitrarily many upgrades, or a slow HTTP client that pins a broker request-handler thread. The bridge ships four admission caps plus one threading-isolation invariant so each of those becomes a graceful HTTP response — or, for the threading case, simply does not happen — instead of broker-wide degradation.

**Admission caps (client-visible HTTP status codes):**

1. **Request body size cap → `413 Payload Too Large`.** `http.bridge.max.request.body.bytes` (default 1 MiB) is enforced in `KafkaHttpServlet.doPost` by wrapping the servlet input stream in `BoundedRequestBody` *before* handing it to Jackson. Jackson's `readTree` consumes the entire stream into memory, and Jetty's default `HttpConfiguration` has no body-size limit of its own — without this wrapper a single multi-GiB POST can OOM the broker JVM before any Kafka admission control runs. Past the cap the wrapper throws `BodyTooLargeException` (which extends `IOException` so Jackson surfaces it cleanly), the servlet emits `413 Payload Too Large` with the `{errorCode, errorMessage}` envelope, and the connection is not held open buffering the rest of the body.
2. **Broker-side request timeout → `504 Gateway Timeout` (+ `Retry-After`).** `http.bridge.request.timeout.ms` (default 30 s) wraps the produce/fetch future inside `KafkaHttpBridge` with `CompletableFuture.orTimeout`. The motivation is the `RequestChannel` short-circuit path used by `KafkaApiRequestSubmitter` (`processor = -1`, `requestCompletionCallback = Some(future::complete)`): the 3-argument `sendResponse` honours the callback, but if any future refactor or unusual KafkaApis path takes the 1-argument `sendResponse(Response)` instead, that path looks up `processors.get(-1)`, finds nothing, and drops the response silently. The timeout is the safety net so the HTTP client always gets an answer; on expiry the bridge emits `504 Gateway Timeout` with `Retry-After`. The timeout is a `long` parameter on the bridge constructor: production wiring passes the config value; tests pass `0L` (sentinel for "no timeout") so they remain deterministic.
3. **Concurrent SSE stream cap → `429 Too Many Requests` (+ `Retry-After: 5`).** `http.bridge.max.concurrent.sse.streams` (default 100) is enforced by `SseStreamLimiter`, an `AtomicInteger`-backed CAS gate. `KafkaHttpServlet.doGet` calls `sseLimiter.tryAcquire()` *before* `startAsync()` so a rejected stream is a one-shot 429, not a half-opened event-stream that immediately closes. The `Token` is `AutoCloseable` and idempotent — `SseStreamer.closeStream` calls `token.close()` exactly once even when triggered from multiple terminal paths. A negative cap fails fast at construction; a zero cap is a valid degenerate config (every SSE acquire refused while produce/fetch stay open).
4. **Concurrent WebSocket subscription cap → `429 Too Many Requests` (+ `Retry-After: 5`).** `http.bridge.max.concurrent.ws.subscriptions` (default 100) is enforced by `WsStreamLimiter`. The upgrade-time creator calls `wsLimiter.tryAcquire()` *before* constructing `KafkaWebSocketEndpoint` so a refused upgrade never enters the subscribe protocol against a connection that would immediately close. The status is `429`, matching the SSE limiter's shape so both streaming admission gates report the same surface to clients and dashboards — and matching PROMPT.md line 34 directly ("429 is reserved for the WebSocket pre-flight throttle, not the HTTP produce path"). The separate `503 Service Unavailable` path is reserved for the broker-shutdown window (see the `Connection: close` + `Retry-After: 1` branches in `KafkaHttpServer.createWsEndpoint`). Token ownership transfers to the endpoint at upgrade time and on to `WsStreamer` at the first valid `subscribe` frame; both holders' `close()` paths are idempotent so cap accounting cannot drift on errors / overlapping teardown.

**Thread isolation (no client-visible status — internal invariant):**

The submitter future is completed on the broker's request-handler thread (via `RequestChannel.Request.requestCompletionCallback`), the same thread pool that services the binary Kafka protocol. Running a slow socket write there pins a Kafka API handler on slow-client I/O — for SSE the pin is indefinite, since every fetch turn re-enters `handleFetchResult` on whichever handler thread happened to complete that round. `KafkaHttpServer` passes `jetty.getThreadPool()` (an `Executor`) into the servlet at construction; `KafkaHttpServlet.doPost` / `doGet` switch their `.whenComplete(...)` calls to `.whenCompleteAsync(..., httpExecutor)`, and `SseStreamer.scheduleNextFetch` does the same so the recursive long-poll runs on Jetty's server thread pool. The broker handler thread returns to `RequestChannel` the moment the response future resolves; this isolation has no operator-visible toggle because there is no reason to disable it.

### Bridge metrics (JMX)

`HttpBridgeMetrics` registers a deliberately small set of HTTP-layer Yammer metrics under the MBean namespace `kafka.network.http:type=BridgeMetrics`, alongside the broker's existing `kafka.network:*` family. The set is sized for what an operator would put on an alert; access logs cover everything else.

| MBean `name=` | Type | Tags | What it measures |
|---|---|---|---|
| `RequestLatencyMs` | Histogram | `operation={Produce,Fetch}` | End-to-end servlet latency in ms (entry of `doPost`/`doGet` → completion of the async write). Implementation uses Yammer's biased (exponentially-decaying) reservoir, so percentiles are recency-weighted rather than a strict windowed aggregate — a quiet period keeps the most recent samples until new ones arrive, and a short burst dominates `p99` until decay catches up. Alert thresholds should be calibrated on the same reservoir behaviour the broker's existing Kafka request metrics use. SSE-stream lifetimes are intentionally excluded — see below. |
| `ResponseCount` | Meter | `operation`, `statusClass={2xx,4xx,5xx,other}` | Rate of HTTP responses bucketed by family. The 5xx bucket aggregates 500 (internal bridge error), 503 (broker unavailable), and 504 (request timeout); each subtype is uniquely distinguishable in bridge logs so the meter is not subdivided further. |
| `RejectedOversizedBody` | Meter | — | Bridge-side 413 rejections at the `BoundedRequestBody` cap. Operator-actionable signal that traffic is bumping `http.bridge.max.request.body.bytes`. |
| `RejectedAtSseCap` | Meter | — | Bridge-side 429 rejections at the SSE admission gate. Pairs with `ActiveSseStreams` to diagnose whether the cap or the consumers is the bottleneck. |
| `SseStreamsOpened` | Meter | — | Rate of *accepted* SSE subscriptions. Recorded once per stream right after `startAsync` succeeds; the stream lifetime is intentionally NOT counted in `ResponseCount` or `RequestLatencyMs` because a stream has no terminal status and runs for hours. |
| `ActiveSseStreams` | Gauge | — | Live concurrent SSE count, read lock-free from `SseStreamLimiter`. |
| `RejectedAtWsCap` | Meter | — | Bridge-side 429 rejections at the WebSocket upgrade admission gate. Matches the SSE 429 shape so both streaming admission gates report the same surface to clients and dashboards (PROMPT.md line 34: "429 is reserved for the WebSocket pre-flight throttle"). The bridge's `503 Service Unavailable` is reserved for the broker-shutdown window, not the concurrency cap. |
| `WsSubscriptionsOpened` | Meter | — | Rate of *accepted* WebSocket subscriptions. Recorded once per upgrade right after `KafkaWebSocketEndpoint` construction succeeds, so accepted + rejected sum to exactly the offered upgrade load — a half-constructed endpoint that never reaches the client is neither. |
| `ActiveWsSubscriptions` | Gauge | — | Live concurrent WebSocket-subscription count, read lock-free from `WsStreamLimiter`. |

**JMX exporter setup.** Bridge metrics live in the new `kafka.network.http` JMX domain, deliberately distinct from `kafka.network` so they can be alerted on independently of the binary protocol's `RequestChannel` surface. Existing Prometheus JMX-exporter / Datadog Kafka-integration configs that pattern-match only `kafka.network<type=...>` will silently skip the bridge metrics — the domain is a different string, not a child namespace. Add a sibling pattern matching `kafka.network.http<type=BridgeMetrics, name=(.+)>` (or the equivalent in the exporter's config syntax) to expose the bridge alongside the existing broker metrics.

`KafkaHttpServer` is one-shot for metrics-lifecycle reasons: the constructor registers all of the above into the global Yammer registry; `stop()` unregisters them unconditionally (even if `start()` never ran or threw partway), and a same-instance `start()` after `stop()` is refused with `IllegalStateException`. Constructor registration itself is wrapped in a best-effort rollback so a partial-init never leaks names that would block a subsequent `KafkaHttpServer` construction in the same JVM.

Recording is guarded by `volatile boolean closed` — a late callback firing during broker shutdown reads `closed == true` and silently returns, satisfying the bridge's never-throws contract. The check-then-act window with `close()` is harmless: Yammer's `Meter.mark()` / `Histogram.update()` operate on internal atomic counters and remain valid after the metric has been removed from the registry map; the update simply ceases to be visible via JMX, never throws.

### Read consistency and write delivery semantics

Two intentional architectural choices about transactional and idempotent semantics. Neither is a bug; both are visible at the HTTP wire so callers can reason about them.

- **Isolation level on fetch / SSE / WS is `READ_UNCOMMITTED`.** The bridge constructs every fetch via `FetchRequest.Builder.forConsumer(...)`, which defaults to `IsolationLevel.READ_UNCOMMITTED`. The broker therefore returns records from open and aborted transactions alongside committed ones, with one bridge-specific guarantee: the per-batch `isControlBatch()` scrub in `KafkaApiRequestSubmitter.extractRecords` strips transaction commit / abort *markers* (producer id and marker payload would otherwise leak as if they were user records). What the bridge does **not** do is filter aborted *data* records — a record that was produced inside a transaction and later aborted will still be visible to HTTP / SSE / WS consumers. Callers that need committed-only reads should consume the topic with a standard Kafka client at `IsolationLevel.READ_COMMITTED`; the bridge has no `?isolation=committed` opt-in in v1. The standard Java consumer's `ConsumerRecord` exposes no transactional flag either, so an in-bridge `READ_COMMITTED` toggle is the only forward path — out of scope for v1.
- **HTTP produce is non-idempotent and non-transactional.** Every `POST /topics/{topic}/records` becomes one `ProduceRequest` built with `NO_PRODUCER_ID` / `NO_PRODUCER_EPOCH`; sequence numbers are not threaded across calls because HTTP is stateless and there is no per-caller producer-id allocator. The broker therefore treats each HTTP produce as a fresh write — a client that retries on a 5xx / network error must accept that the original request may have committed before the retry, i.e. the bridge offers *at-least-once* on the write side. A client that needs exactly-once (idempotent dedup) or transactional atomicity across multiple partitions / topics must use a standard Kafka producer with `enable.idempotence=true` and / or `transactional.id=...`; that path is intentionally out of scope for v1 because the producer-id lifecycle is incompatible with stateless request-response.
- **HTTP produce / fetch never triggers auto-topic-creation.** `auto.create.topics.enable=true` on the broker creates topics only on `MetadataRequest`; the bridge synthesises `ProduceRequest` / `FetchRequest` directly via `KafkaApiRequestSubmitter` and never sends a `Metadata` round-trip. A `POST` or `GET` against a topic that does not exist therefore returns `404 UNKNOWN_TOPIC_OR_PARTITION` regardless of the cluster-side `auto.create.topics.enable` setting. Operators who want topics to materialise on first write must either pre-create them out-of-band (`kafka-topics --create`) or use a standard Kafka producer / consumer that does its own `Metadata` fetch as part of the protocol handshake.

### What is explicitly **not** in v1

- **Per-request authentication on the HTTP path.** Every request runs as `KafkaPrincipal.ANONYMOUS`. See the Security model section above — this is the project's largest production-readiness caveat and the operator-facing WARN + runbook are the v1 mitigation.
- **Committed-only reads (`READ_COMMITTED` isolation).** The fetch path is hardcoded to `READ_UNCOMMITTED`; aborted records remain visible to HTTP / SSE / WS consumers. See "Read consistency and write delivery semantics" above. A standard Java consumer at `READ_COMMITTED` is the v1 workaround.
- **Idempotent / transactional HTTP produce.** Every HTTP `POST` is an independent, non-idempotent write — retries can duplicate. See "Read consistency and write delivery semantics" above. A standard Java producer with `enable.idempotence=true` (and optionally `transactional.id=...`) is the v1 workaround.

### Production-readiness summary

The pure layer (`server/src/main/java/org/apache/kafka/network/http/`) is exhaustively unit-tested — 423 tests covering content-type negotiation, cursor encoding/decoding, request parsing edge cases (HTTP + WS subscribe/flow), response formatting policy, Retry-After policy, error envelope shape, SSE framing, WebSocket credit-gated dispatch, the four admission-control caps (body-size, request-timeout, concurrent-SSE, concurrent-WS) plus the off-handler-thread dispatch invariant, the HTTP-level metrics layer (per-operation latency histograms, per-status-class meters, SSE / WS open / reject meters, active-SSE and active-WS gauges — registered/unregistered cleanly across `KafkaHttpServer` lifecycles and stress-tested under concurrent close-vs-record), and the WebSocket session endpoint (state machine, token ownership transfer, idempotent teardown). The broker integration is covered by eight real-broker end-to-end tests in `core/src/test/scala/integration/kafka/network/http/HttpBridgeEndToEndTest.scala` that boot a KRaft cluster via `KafkaClusterTestKit`, enable the bridge, and verify produce/fetch round-trips, mixed-partition 207, ACL-denied 403 on both POST (write) and GET (fetch) paths, SSE replay-then-live continuity, HAL+JSON cursor follow across pages with offset-contiguity assertion, quota-throttle → 200 + `Retry-After` against a tight `producer_byte_rate` quota on `User:ANONYMOUS`, and the WebSocket credit-gated subscribe contract end-to-end (initialCredits=5 → exactly 5 records → flow(10) → exactly 15 total against a live KRaft broker).

`http.bridge.enabled=true` is safe to set in any cluster where the operator has followed the Security model section above — bound interface, fronting auth proxy, scoped ACLs. The startup WARN repeats the requirement in operator logs.

---

## Security model — read this before enabling in any real cluster

The bridge has **no per-request authentication in v1.** Every inbound HTTP request is presented to the broker's authorizer as `KafkaPrincipal.ANONYMOUS`. This is a deliberate scope decision (see `PROMPT.md`: "HTTP requests are translated to existing Kafka request objects and submitted to the existing `RequestChannel`. No new authz or quota path."), not an oversight — but it has direct operational consequences that must be addressed before the listener is exposed.

**The attack to model.** Any host that can reach `http.bridge.host:http.bridge.port` can act as `User:ANONYMOUS`. If `ANONYMOUS` can read a topic, the caller can exfiltrate it; if `ANONYMOUS` can write a topic, the caller can poison it — through normal broker plumbing, with no exploit required. Codex named this "anonymous Kafka data-plane takeover" and we carry the phrase in the `BrokerServer` threat-model comments and `HostBindingValidator` Javadoc — the operator-facing WARN at startup keeps the same threat in plainer language (every request runs as `KafkaPrincipal.ANONYMOUS`, any reachable host can act on every topic that principal can access) and points at this Security model section for the rest.

**The three guardrails operators must apply** (all three — any one alone is insufficient):

1. **Bind to a specific trusted interface.** Set `http.bridge.host` to loopback (`127.0.0.1`) or a specific address on an access-controlled private network. Never `0.0.0.0` on a host with a public NIC. The bridge's host config is a Jetty bind address, not a CIDR or ACL — Jetty will accept any TCP connection that lands on that interface, so the network itself must be locked down separately (security group, iptables, namespace).
2. **Force traffic through an authenticating front-end.** A reverse proxy that terminates TLS and enforces an auth scheme (mTLS, OAuth bearer, basic-with-LDAP, etc.) before forwarding to the bridge port. The network must make it impossible to reach the bridge directly — security-group / iptables / namespace-level enforcement, not just convention.
3. **Scope ACLs for `User:ANONYMOUS`.** Run `kafka-acls --add --allow-principal User:ANONYMOUS --operation Read --operation Write --topic <name>` for exactly the topics the bridge is meant to expose, with each operation passed as its own `--operation` flag (the CLI does not accept `Read|Write`). A default-allow authorizer, no authorizer at all, or `allow.everyone.if.no.acl.found=true` on an otherwise-empty ACL set, all leave topics open to the bridge. Two end-to-end ACL deny tests demonstrate the enforcement path against a real broker: `HttpBridgeEndToEndTest.aclDeniedTopicReturns403` covers the POST / write side (denied WRITE → HTTP 403 with per-partition `errorCode=29` `TOPIC_AUTHORIZATION_FAILED` and no `Retry-After`); `HttpBridgeEndToEndTest.aclDeniedFetchReturns403` covers the GET / read side (denied READ → HTTP 403 with the same envelope shape and no `Retry-After`).

The startup WARN in `BrokerServer.scala` (search for `warn("HTTP bridge: every request runs as KafkaPrincipal.ANONYMOUS`) repeats the headline so it cannot be missed in operator logs. If you find yourself silencing the WARN before doing the three steps above, you are configuring the bridge wrong.

**Availability is a separate threat dimension and the bridge does not protect it per source.** The SSE/WS concurrent-stream caps (`http.bridge.max.concurrent.sse.streams`, `http.bridge.max.concurrent.ws.subscriptions`) are global counters, not per-IP. The broker's request-quota bucket is also effectively shared, because every bridge request runs as `User:ANONYMOUS` — all bridge traffic competes for one principal's quota. A single misbehaving caller can saturate either dimension and starve every other client of the bridge. Per-source rate limiting and per-IP connection counting are the fronting reverse proxy's responsibility — the same proxy that performs authentication in guardrail #2 above. Do not expose the bridge port without one.

**Browser-origin CSRF defence is also deferred to the fronting proxy.** The WebSocket upgrade handler does not inspect the `Origin` request header (RFC 6455 §10.2), so a browser tab on the open internet can speculatively open `ws://bridge/v1/topics/{topic}/subscribe` from any HTTPS page if it can reach the bridge directly. The HTTP produce endpoint rejects request bodies whose `Content-Type` is not `application/json` with `415 Unsupported Media Type`, which closes the cross-origin form-POST CSRF primitive at the bridge boundary — a browser form with `enctype="text/plain"` can submit cross-origin without a CORS preflight, but the bridge will refuse it before reading the body. The WS upgrade path is browser-reachable without an equivalent in-bridge defence: `Origin`-allowlist enforcement, `Sec-Fetch-Site` checking, and any CSRF-token scheme belong on the reverse proxy that already terminates TLS and authenticates clients in guardrail #2. The bridge cannot distinguish a browser-origin from a service-to-service caller once both have reached its port, because neither has been authenticated yet.

Per-request authentication (a real principal derived from a client cert, JWT, or SASL handshake on the HTTP path) is a follow-up item, explicitly out of scope for v1.

---

## How to run what exists

Unit suite (fast, runs in seconds):

```sh
JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 ./gradlew :server:test \
  --tests 'org.apache.kafka.network.http.*'
```

Real-broker end-to-end suite (boots a KRaft cluster per test):

```sh
JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 ./gradlew :core:test \
  --tests 'kafka.network.http.HttpBridgeEndToEndTest'
```

Build with JDK 21 — JDK 27 is too new for the project's Gradle 8.10.2 (class file 71 not yet supported by Groovy).

To start a broker with the bridge enabled, set in `server.properties`:

```
http.bridge.enabled=true
http.bridge.host=127.0.0.1
http.bridge.port=8082
# Admission-control caps (defaults shown):
http.bridge.max.request.body.bytes=1048576
http.bridge.request.timeout.ms=30000
http.bridge.max.concurrent.sse.streams=100
http.bridge.max.concurrent.ws.subscriptions=100
```

The example mirrors the config defaults (`127.0.0.1`, loopback only; 1 MiB body cap; 30 s request timeout; 100 concurrent SSE streams; 100 concurrent WebSocket subscriptions). To make the bridge reachable from other hosts, change `http.bridge.host` — but read the Security model section above first, and expect a secondary WARN in the broker log whenever the host is set to a wildcard (`0.0.0.0` or `::`). The admission caps are sized for typical produce/fetch use; raise them deliberately if a workload genuinely needs more, and remember that `http.bridge.max.concurrent.sse.streams` and `http.bridge.max.concurrent.ws.subscriptions` both share the Jetty thread pool with the produce / fetch path.
