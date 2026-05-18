# HTTP / WS / SSE Bridge — Progress Log

Implementation of the broker-embedded HTTP listener described in `PROMPT.md`.

This document tracks **what has been built, why, and what remains.** It is written for someone joining mid-stream who needs to pick up the work without re-reading every commit.

---

## Architectural stance

The bridge lives **inside the broker process** (not as a separate service). The whole reason this feature exists is to share `RequestChannel`, the existing authorization path, the existing quota path, and the existing replication machinery with the binary protocol. A separate REST proxy cannot do that — Confluent's exists and that limitation is the entire reason for the rewrite.

Three layers, kept deliberately distinct:

1. **Pure translation layer** — HTTP JSON ↔ Kafka request/response objects. No I/O, no concurrency, no Jetty types. This is where the four-step value chain, multi-status logic, HATEOAS cursors and error envelope live. Fully unit-testable in milliseconds.
2. **Bridge orchestrator** — receives a parsed HTTP request, builds a `ProduceRequest` / `FetchRequest`, hands it to the existing `RequestChannel`, awaits the matching `Response`, translates back. Depends on a small `RequestSubmitter` interface so the orchestrator can be exercised against a fake channel in unit tests.
3. **Transport / wiring** — a Jetty handler in front; a `BrokerServer` hook behind. Thin. Pushes everything interesting into layers 1 and 2.

Tests are written **first**, in the same package as production code, and the implementation only contains code traceable to a test.

---

## Module layout

Everything lives under the existing `server` Gradle module to avoid touching the build graph:

```
server/src/main/java/org/apache/kafka/network/http/
server/src/test/java/org/apache/kafka/network/http/
```

The `org.apache.kafka.network.*` package is already authorised to use Jackson in `checkstyle/import-control-server.xml`; subpackages inherit, so no checkstyle change was needed for layer 1. New transport-layer code may need explicit Jetty entries when wiring lands.

`clients/` is untouched, per the hard constraint.

---

## Status snapshot

| Component | State | Notes |
|---|---|---|
| Value serializer (4-step chain) | **Done** | 20 tests, all green |
| HTTP status mapper | In progress | next |
| Retry-After calculator | Pending | |
| Produce JSON parser | Pending | |
| Produce response formatter (207) | Pending | |
| Fetch response formatter (HATEOAS) | Pending | |
| Cursor codec | Pending | |
| Bridge orchestrator | Pending | uses fake `RequestSubmitter` |
| Jetty handler | Pending | |
| `BrokerServer` integration | Pending | gated by config flag |
| WebSocket (stretch) | Out of scope for v1 | |
| SSE (stretch) | Out of scope for v1 | |

---

## What's already in place

### `ValueSerializer` — the four-shape value envelope

Source: `server/src/main/java/org/apache/kafka/network/http/ValueSerializer.java`
Tests: `server/src/test/java/org/apache/kafka/network/http/ValueSerializerTest.java` (20 tests)

Pure, allocation-light translator between raw record bytes and the JSON envelope `{type, data}` used in HTTP payloads.

**Output side — `encode(bytes, contentType)`:**

| Input | Output |
|---|---|
| `bytes == null` | `{ type: NULL, data: null }` |
| `contentType` ≈ `application/json` and parseable | `{ type: JSON, data: <parsed node> }` |
| Valid UTF-8 with no disallowed C0 control chars (`\t \n \r` allowed) | `{ type: STRING, data: "<decoded>" }` |
| Anything else | `{ type: BINARY, data: "<base64>" }` |

Notable behaviours captured by tests:
- `application/json; charset=utf-8` is recognised as JSON (parameter is stripped).
- If `content-type` claims JSON but the payload doesn't parse, the chain falls through — the bytes are still served, just as `STRING` or `BINARY` rather than misadvertised `JSON`.
- Empty bytes encode as an empty `STRING`.
- A null byte anywhere in the payload kicks it down to `BINARY` (it isn't valid in a JSON string).

**Input side — `decode(envelope)`:**

| `type` | Output bytes |
|---|---|
| `NULL` | `null` |
| `JSON` | canonical UTF-8 serialisation of `data` |
| `STRING` | `data.getBytes(UTF-8)` |
| `BINARY` | base64-decoded `data` |

Malformed envelopes throw `IllegalArgumentException` with a message that says exactly what was wrong (missing `type`, unknown `type`, invalid base64, missing `data`, …).

**`contentTypeFor(envelope)`** returns `"application/json"` for JSON envelopes so the produce path can attach a content-type header to the record — this is what lets the symmetric output chain pick the JSON branch on read-back. Other types return `null` (no header attached).

---

## What's coming next (TDD order)

1. **`HttpStatusMapper`** — `Errors → int` mapping. Drives 403/404/400/503/etc. Keep simple: a small switch. Tested with one assertion per important error code.
2. **`RetryAfterCalculator`** — `long throttleMs → int seconds`, ceiling. Used for the produce quota path where the response stays `200` but carries `Retry-After`.
3. **`ProduceJsonRequest` parser** — incoming `POST` body → an internal `ProduceCommand` data class (records grouped by partition, with key/value bytes and content-type headers already resolved by the value serializer). Acks and timeout from headers.
4. **`ProduceJsonResponse` formatter** — `ProduceResponse` → JSON. Heterogeneous-partition handling: pure success → `200`, mixed → `207`, uniform failure → mapped error status.
5. **`FetchJsonResponse` formatter** — `FetchResponse` → JSON with per-partition `_links.self` and root-level `_links.{first, previous, next, last}` cursors.
6. **`CursorCodec`** — opaque base64url cursor that hides `(topic, partition, offset)` so clients follow links rather than building URLs.
7. **`KafkaHttpBridge`** — orchestrator. Depends on a `RequestSubmitter` interface (the real one wraps `RequestChannel.sendRequest` + a custom response capture; the fake one just stages responses for the test).
8. **Jetty handler** — single handler, two routes (`POST` and `GET` under `/v1/topics/{topic}/records`).
9. **`BrokerServer` hook** — config keys (`http.bridge.enabled`, `http.bridge.listener`), start/stop alongside `socketServer`.

For every step, the checkpoint discipline applies: clean code, tests that reflect intent (not implementation), no duplication, integration tests where boundaries are crossed, local conventions matched.

---

## How to run what exists

```sh
JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 ./gradlew :server:test \
  --tests 'org.apache.kafka.network.http.*'
```

Build with JDK 21 — JDK 27 is too new for the project's Gradle 8.10.2 (class file 71 not yet supported by Groovy).
