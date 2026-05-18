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

| Class                       | Responsibility                                                                                   | Status          |
|-----------------------------|--------------------------------------------------------------------------------------------------|-----------------|
| `SelectorImplementation`    | Enum + lenient parser for the operator-facing config value.                                      | Done (tested).  |
| `IoUringSupport`            | Reflective platform probe; loads on any JVM. Reports `isLinux`, `isAvailable`, and a reason.     | Done (tested).  |
| `BrokerSelector`            | Internal interface extending `org.apache.kafka.common.network.Selectable` + 7 broker methods.    | Done.           |
| `NioBrokerSelector`         | Pure pass-through adapter over the historical `Selector`. Anchors the NIO baseline.              | Done (tested).  |
| `BrokerSelectorFactory`     | Pure decision function: `(requested, protocol, available) → effective implementation`.           | Done (tested).  |
| `socket.selector.implementation` | Broker config knob, default `auto`. Registered in `SocketServerConfigs.CONFIG_DEF`.          | Done.           |

The 22 unit/integration tests under `server/src/test/java/org/apache/kafka/network/iouring/`
are all green under JDK 21. `NioBrokerSelectorTest` opens a real loopback TCP
connection and exercises register / round-trip / mute / unmute / local close /
LRU candidate / close-all / unwrap, which has already pinned down two
non-obvious `Selector` quirks (no disconnect notification on local close;
`lowestPriorityChannel()` falls back to a registered channel when idle tracking
is off) that any future io_uring backend must match.

## What is intentionally not yet in place

The next commit set will add:

1. Netty 4.2.12.Final dependencies (`netty-transport-classes-io_uring` plus the
   classifier-bound native artifact) under the `server` module. The jars are
   already in the local Gradle cache; the wiring is the gating change.
2. `KafkaConfig.scala` getter `socketSelectorImplementation: SelectorImplementation`.
3. `IoUringSelector` — the real Netty-backed implementation. Architecture
   sketched below.
4. `Processor.createSelector` (`core/src/main/scala/kafka/network/SocketServer.scala`)
   plumbed through `BrokerSelectorFactory.resolve`.
5. The 10k-connection PLAINTEXT benchmark and its write-up.

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

Misuse modes and the broker's response:

- Value not in `{nio, io_uring, auto}` → `ConfigException` at startup with the
  invalid token surfaced.
- `io_uring` on a non-Linux host or a host without the Netty native library →
  for `auto`, transparently falls back to NIO; for explicit `io_uring`, fails
  fast with a message that names the missing capability.
- `io_uring` on an SSL / SASL_PLAINTEXT / SASL_SSL listener → that listener
  transparently uses NIO. (Lifted in a follow-up version.)
