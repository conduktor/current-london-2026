# io_uring networking for the Kafka broker (experimental)

This package is the v1 implementation of the io_uring network backend described
in `PROMPT.md` at the repository root. It is invoked behind a new broker-wide
configuration knob (`socket.selector.implementation`), and falls back to the
historical NIO selector for any listener that cannot use io_uring.

## Scope (v1)

- Linux only — the platform probe transparently falls back on other OSes.
- PLAINTEXT listeners only — SSL / SASL_PLAINTEXT / SASL_SSL listeners
  transparently fall back to NIO regardless of the operator's request.
- Inside the broker (`server/` + `core/`) only — no changes under `clients/`.
- Netty 4.2.x io_uring transport (GA, not incubator).
- No virtual threads.

## What is in place today

| Class                            | Responsibility                                                                                                                                                                                              | Status         |
|----------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|
| `SelectorImplementation`         | Enum + lenient parser for the operator-facing config value.                                                                                                                                                 | Done (tested). |
| `IoUringSupport`                 | Reflective platform probe; loads on any JVM. Reports `isLinux`, `isAvailable`, and a reason.                                                                                                                | Done (tested). |
| `BrokerSelector`                 | Internal interface extending `org.apache.kafka.common.network.Selectable` + 7 broker methods.                                                                                                               | Done.          |
| `NioBrokerSelector`              | Pure pass-through adapter over the historical `Selector`. Anchors the NIO baseline.                                                                                                                         | Done (tested). |
| `BrokerSelectorFactory`          | Pure decision function: `(requested, protocol, available) → effective implementation`.                                                                                                                      | Done (tested). |
| `socket.selector.implementation` | Broker config knob, default `auto`. Registered in `SocketServerConfigs.CONFIG_DEF` and exposed on `KafkaConfig` as `socketSelectorImplementation: SelectorImplementation`.                                  | Done.          |
| `IoUringSelector`                | Channel-management core: bind, accept queue, completed receives/sends, idle expiry, mute/unmute via `KafkaChannelMuteBridge`, closingChannels lifecycle aligned with NIO (FIN drain, eviction notification). | Done (tested). |
| `IoUringServerListener`          | Netty `ServerBootstrap` over `IoUringServerSocketChannel` with `SO_REUSEPORT`, `TCP_NODELAY`, `SO_KEEPALIVE`, operator-tunable `SO_SNDBUF` / `SO_RCVBUF` matching the NIO Acceptor.                          | Done (tested). |
| `IoUringTransportLayer`          | Bridges `KafkaChannel` to a Netty `IoUringSocketChannel`: bounded inbound queue (releases on close), backpressure via Netty `autoRead`, async write failures relayed as `IOException` on the next `write`.  | Done (tested). |
| `IoUringPlaintextAuthenticator`  | PLAINTEXT authenticator returning `KafkaPrincipal.ANONYMOUS` via the configured principal builder; matches NIO behavior.                                                                                    | Done (tested). |
| `IoUringChannelMetadataRegistry` | Per-channel metadata bag used by the principal-builder path.                                                                                                                                                | Done (tested). |
| `StubSocketChannel`              | Minimal NIO `SocketChannel` view exposing remote/local addresses, used by `KafkaChannel.socket()` callers.                                                                                                  | Done (tested). |
| `NoopSelectionKey`               | Stable `SelectionKey` shim — `interestOps` cycles through `OP_READ` to drive `autoRead` on/off.                                                                                                             | Done (tested). |
| `KafkaChannelMuteBridge`         | Lives under `org.apache.kafka.common.network` (same package as `KafkaChannel`) to expose the package-private `mute()` / `maybeUnmute()` state-machine transitions to the broker selector.                   | Done (tested). |
| `SocketServer.scala` Processor   | `buildIoBundle()` resolves the effective selector per Processor, constructs an `IoUringSelector` + `IoUringServerListener` for io_uring listeners, falls back to the NIO `Selector` otherwise.              | Done.          |

All unit/integration tests under `server/src/test/java/org/apache/kafka/network/iouring/`
are green under JDK 21. The `IoUringSelectorTest`, `IoUringTransportLayerTest`,
and `IoUringServerListenerIT` suites pin down: bind on a kernel-assigned port,
round-trip a size-prefixed frame end-to-end through the real io_uring kernel
transport, mute/unmute via `KafkaChannelMuteBridge`, single LOCAL_CLOSE
notification deferred to eviction, async write failure surfaced as `IOException`,
ByteBuf release on every error/close path, and the operator-configured TCP
options reaching the bootstrap.

## What is intentionally not yet in place

1. The 10k-connection PLAINTEXT benchmark write-up vs. the NIO baseline
   (PROMPT.md "minimum viable outcome #4" — pending).
2. SSL / SASL stretch items (PROMPT.md "Stretch" — explicit non-goal for v1).

## Architecture sketch for `IoUringSelector`

Adopting an already-accepted JDK `SocketChannel` into Netty's io_uring transport
is not supported by Netty 4.2 — every FD-adopting constructor of
`IoUringSocketChannel` takes a package-private `LinuxSocket`. The Acceptor →
Processor handoff path therefore cannot be reused unchanged.

The intended seam (subject to the in-flight architectural validation):

- For listeners resolved to `IO_URING`, **each `Processor` constructs an
  `IoUringSelector` that binds the listener address itself** via a Netty
  `ServerBootstrap` over `IoUringServerSocketChannel` with `SO_REUSEPORT`.
  The Linux kernel shards accepts across the per-Processor sockets.
- The classic per-listener `Acceptor` is short-circuited (its NIO accept loop
  does not run) for io_uring-backed listeners — but it still owns the
  bind-time configuration, connection quotas, and listener metrics that are
  not Selector-local.
- Accepted `IoUringSocketChannel` instances are enqueued (by a Netty handler
  on the io_uring event loop) onto a `ConcurrentLinkedQueue<Conn>` drained
  inside `IoUringSelector.poll()` on the Processor thread. There each
  connection is wrapped as a `KafkaChannel` whose `TransportLayer` reads/writes
  through the Netty channel, and registered into the Selector's internal
  channel map.
- `Processor.configureNewConnections()` remains a no-op for io_uring listeners
  (its `newConnections` JDK-channel queue stays empty for those Processors).
- Mute/unmute maps to `channel.config().setAutoRead(false/true)`.

This places the entire io_uring lifecycle inside the `BrokerSelector` contract
the Processor already speaks to — and the Processor stays oblivious to Netty.

The two architectural risks being verified before code lands:

1. Whether Netty 4.2's io_uring transport actually exposes a `SO_REUSEPORT`
   channel option — the epoll transport does (`EpollChannelOption.SO_REUSEPORT`);
   the io_uring transport needs the analog to make the per-Processor model
   viable.
2. Whether Linux's accept distribution across SO_REUSEPORT sockets is fair
   enough in practice to not require an explicit accept-handler at the Acceptor.

## Escape hatch

The branch follows PROMPT.md's required escape-hatch order: Codex → Gemini →
ivy-trunk. Consultations and design references are recorded in commit messages
when used; this file is updated when a decision becomes load-bearing on code
that has merged.

## Configuration reference

```
# Broker config (server.properties)
socket.selector.implementation = auto    # default; picks io_uring on Linux, else nio
                                = nio    # force the historical NIO selector
                                = io_uring  # force io_uring (PLAINTEXT only; non-PLAINTEXT listeners still use NIO)
```

The setting is broker-wide. A per-listener override is honoured via
`listener.name.<name>.socket.selector.implementation` — the per-listener value,
if set, takes precedence over the broker-wide value for that listener only.

Misuse modes and the broker's response:

| Operator request                                  | Listener security protocol           | Platform / native-lib state       | Broker behaviour                                                                                                                                                                  |
|---------------------------------------------------|--------------------------------------|-----------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| value not in `{nio, io_uring, auto}`              | any                                  | any                               | `ConfigException` at startup, the invalid token surfaced.                                                                                                                         |
| `nio`                                             | any                                  | any                               | NIO. Always wins; explicit `nio` is never escalated.                                                                                                                              |
| `auto` (default)                                  | PLAINTEXT                            | Linux + io_uring native lib found | io_uring.                                                                                                                                                                         |
| `auto`                                            | PLAINTEXT                            | any other (non-Linux, no kernel)  | Silent NIO fallback. **No warn spam**, one `INFO` log per listener at start (`F-INT-LOG`) names the resolved backend.                                                             |
| `auto`                                            | SSL / SASL_PLAINTEXT / SASL_SSL      | any                               | Silent NIO fallback. v1 PLAINTEXT-only contract.                                                                                                                                  |
| `io_uring` (explicit)                             | PLAINTEXT                            | Linux + native lib found          | io_uring.                                                                                                                                                                         |
| `io_uring` (explicit)                             | PLAINTEXT                            | Linux but native lib missing      | **Hard fail at broker start** — `IllegalStateException` from `BrokerSelectorFactory.resolve`, message names `io_uring` and the `IoUringSupport.unavailabilityReason()`.            |
| `io_uring` (explicit)                             | PLAINTEXT                            | non-Linux                         | **Hard fail at broker start** — same as above; the operator asked for io_uring explicitly, silently downgrading would hide configuration drift.                                   |
| `io_uring` (explicit)                             | SSL / SASL_PLAINTEXT / SASL_SSL      | any                               | Silent NIO fallback for that listener. Other PLAINTEXT listeners on the same broker still use io_uring per the rules above.                                                       |

Operator-observable log line on every listener start (since `F-INT-LOG`):

```
INFO  [SocketServer brokerId=…] Listener PLAINTEXT://0.0.0.0:9092 resolved to io_uring backend
INFO  [SocketServer brokerId=…] Listener SSL://0.0.0.0:9093 resolved to nio backend (PLAINTEXT-only contract for io_uring v1)
```

## Native library dependencies

io_uring is reached through Netty 4.2.x's `IoUringServerSocketChannel` /
`IoUringSocketChannel`. Three Maven artifacts are involved:

| Artifact                                                                                                                | Classpath role  | Required to compile? | Required to run io_uring? |
|-------------------------------------------------------------------------------------------------------------------------|-----------------|----------------------|---------------------------|
| `io.netty:netty-transport-classes-io_uring`                                                                             | `implementation` (compile + runtime) | yes              | yes                       |
| `io.netty:netty-transport-native-io_uring` (classifier `linux-x86_64`)                                                  | `runtimeOnly`                        | no               | yes (on x86_64 Linux)     |
| `io.netty:netty-transport-native-io_uring` (classifier `linux-aarch_64`)                                                | `runtimeOnly`                        | no               | yes (on aarch_64 Linux)   |

The classes artifact is on the compile classpath; the native artifacts are
`runtimeOnly` because (a) they are classifier-bound and would break compilation
on macOS / Windows / FreeBSD CI machines, and (b) every reference to the native
classes is gated behind `IoUringSupport.isAvailable()`, which performs a
reflective probe on first use. Both Linux native classifiers ship together so a
single Kafka tarball boots on both Intel/AMD x86_64 and ARM64 (Graviton,
Ampere, Apple-Silicon-Linux) brokers; Netty's `NativeLibraryLoader` picks the
matching `.so` from `os.arch` at startup. `LICENSE-binary` enumerates the seven
Netty artifacts the broker now bundles (Apache 2.0).

If either the classes artifact or the native artifact for the current
architecture is missing at runtime, `IoUringSupport.computeProbe()` catches the
resulting `LinkageError` / `ClassNotFoundException` / `UnsatisfiedLinkError`,
caches `isAvailable = false`, and surfaces a one-line reason via
`unavailabilityReason()`. The resolution table above then applies — `auto`
silently uses NIO, explicit `io_uring` aborts startup with the reason in the
error message. This is the only place a stripped-down deployment image (e.g.,
a custom Docker base that excludes one classifier) is observed by the operator.
