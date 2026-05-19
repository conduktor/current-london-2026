# Round 17 — Four-agent adversarial sweep (2026-05-19, post-B-S1/B-S2/B-T2/B-T3 fixes)

Branch: `experiment/io-uring` at `aa75273bd1`. Audit posture: read-only, ambitious adversary. No external AI used.

Four parallel agents covering: **Lifecycle/shutdown**, **Concurrency/races**, **Observability/metrics**, **Integration boundaries**.

## Cross-agent consolidated BLOCKERs (post-Round-16)

### Code correctness

**B-17-1 (Lifecycle agent + matches Round-16 B-S3) — onAccept races close(), leaking Netty channel + transport.**
`IoUringSelector.java:314-366` (onAccept) vs `:1001-1023` (close).
Event loop reads `closed=false` → Processor `close()` drains every map → event loop continues to `nettyChannels.put(id, …)` / `pendingAccepts.offer(channel)` AFTER drain. Channel is never closed, transport leaks pooled-direct buffers per racing accept. **Fix:** re-check `closed` after publishing in `onAccept`; self-cleanup on detected close.

**B-17-2 (Concurrency agent B2 + Round-16 "Other CRITICAL") — autoRead read-modify-write race between Processor and event loop.**
`IoUringTransportLayer.java:308-311` (addInterestOps) vs `:239-241` (offerInbound watermark gate). Non-atomic check-then-act on `isAutoRead()` can leave the gate disabled while bytes pile past HIGH. Defeats the direct-memory bound under burst. **Fix is non-trivial** — needs a guarded transition or atomic state.

**B-17-3 (Lifecycle agent) — `IoUringSupport.computeProbe` catches only `ReflectiveOperationException`, not `LinkageError`/`UnsatisfiedLinkError`/`ExceptionInInitializerError`.**
`IoUringSupport.java:74-91`. On a Linux host with the netty-io_uring jar present but native lib unmatched (musl, old glibc), the static initializer throws an `Error` that escapes `computeProbe`, manifesting as `NoClassDefFoundError: Could not initialize class IoUringSupport` on every subsequent access. NIO fallback fails open. **Tiny fix:** `catch (ReflectiveOperationException | LinkageError e)`.

**B-17-4 (Lifecycle agent C-17-1, matches Round-16 B-S5) — `serverChannel.close().sync()` has no timeout.**
`IoUringServerListener.java:232`. Wedged event loop ⇒ broker `shutdown()` blocks indefinitely. **Tiny fix:** swap to `.awaitUninterruptibly(SHUTDOWN_TIMEOUT_MS, MILLISECONDS)`; `shutdownGracefully` already enforces its own bound.

**B-17-5 (Lifecycle agent B-17-2) — `IoUringServerListener.start()` partial-failure leaves the LISTEN socket bound.**
`IoUringServerListener.java:191-204`. If `boundPort` extraction throws (NPE/CCE on `localAddress()`), `serverChannel` is bound and non-null but `started=false` ⇒ subsequent `close()` skips it; port stays in LISTEN ~2s. **Tiny fix:** close `serverChannel` explicitly in catch blocks; set `started=true` last.

### Observability (BLOCKER per PROMPT.md production-grade contract)

**B-OBS1..B-OBS8** — `IoUringSelector` registers ZERO Kafka `Sensor`s. Missing:
- `io-wait-ratio` → `NetworkProcessorAvgIdlePercent=0` on io_uring listeners ⇒ saturation alert fires constantly, autoscaler over-provisions.
- `incoming-byte-rate` / `outgoing-byte-rate` (per-listener AND per-client-id).
- `connection-count` gauge (active connections per listener invisible).
- `connections-created-rate` / `connection-close-rate` (reconnect-storm alert dark).
- `successful-authentication-rate` (PLAINTEXT ANONYMOUS auth invisible).
- `request-rate`, `request-size-{avg,max}`, `response-rate`, `network-io-rate` (capacity planning blind).
- `expired-connection-rate` (idle eviction invisible — `connections.max.idle.ms` mis-tuning undetectable).
- Per-connection sensors (`maybeRegisterConnectionMetrics`) absent (low impact on broker because `metricsPerConnection=false` default).

This is the single largest production-grade gap. Whole operator-observability surface is missing for io_uring listeners.

### Integration test coverage

**B-T1 / C-1 (Integration agent) — Zero ITs wire `IoUringSelector` through `SocketServer.Processor`.**
`SocketServerTest` is hardcoded to `NioBrokerSelector`. Existing `IoUringServerListenerIT` drives the selector directly with a `Socket`, never crosses `applyConnectionQuotasForNewlyAcceptedChannels`, `processCompletedReceives`, `processDisconnected`, `closeExcessConnections`. Every per-Processor sharding bug, quota-rollback bug, listener-start-ordering bug, and shutdown-ordering bug is **unobserved by CI**.

## CRITICALs (high impact, not BLOCKER)

- **C-17-Concurrency-C1**: `inboundBytes` can go transiently negative because `offerInbound` calls `inboundBytes.addAndGet(size)` AFTER `inbound.offer(buf)`. Reorder to increment-before-offer so a draining reader can never see queue bytes uncounted.
- **C-OBS3 (logging asymmetry)**: idle expiry on io_uring is silent (no DEBUG, no metric). SREs grepping logs see nothing.
- **C-17-Lifecycle-C2 (authenticator leak)**: if `KafkaChannel` constructor throws in `onAccept`, the per-accept `PlaintextAuthenticator` is orphaned with no `close()`. Wrap the construction in try-catch with `authenticator.close()` + `transport.close()`.

## CONCERNs

- `idGen` wraparound: at Int.MAX_VALUE, `nettyChannels.put` could clobber prior entry. Round-16 B-S8 — probability is extremely low but the invariant is implicit; NIO has the same bound. Defer.
- `failedSends.remove(Object)` invariant brittleness — Round-16 B-S9. Consider `LinkedHashSet<String>`.
- `pendingWriteBytes` / `asyncWriteFailure` never reset on transport `close()` — Round-16 B-S6. Counters strand. Low impact (test/diagnostic).
- `selector.close()` doesn't release queued `NetworkReceive`s in `completedReceives` — Round-16 B-S7. Parity with NIO; low impact.
- First-read latency carries one extra poll iteration via `justAccepted` gate. Acceptable for v1.
- `acceptorBlockedPercentMeter` is dead code on io_uring path (Processor uses non-blocking `tryInc`). Either remove from constructor or document.

## Verified-safe (don't re-audit)

- `pendingWriteBytes` / `asyncWriteFailure` ordering: B-S2 fix at `:480-494` is correct, no false-success window.
- `wakeup` semaphore: every event-loop callback releases a permit; no lost-wakeup.
- `offerInbound` close-race: double-check pattern with self-drain closes the window.
- `close(id)`: cleans both `channels` AND `closingChannels`; `failedSends.remove(id)` too.
- Selectable contract parity (mute/unmute/close/lowestPriorityChannel/etc.): all correctly wired.
- `closeAll` ordering: listener → selector is correct.
- PrincipalBuilder / ChannelBuilders.channelBuilderConfigs equivalence: round 5 fix is still right.
- ClientInformation.EMPTY at accept: round 12 fix is still right.
- Bind deferred to `Acceptor.start()`: round 13 fix is still right.

## Recommended sequencing — next 30m

**Tiny + surgical (target THIS turn, one commit):**
1. **B-17-3** computeProbe LinkageError — 5 min
2. **B-17-4** serverChannel.close() timeout — 5 min
3. **B-17-5** start() partial-failure cleanup — 10 min
4. **C-17-Concurrency-C1** inboundBytes increment-before-offer — 5 min

**Medium (next /loop firing, separate commit):**
5. **B-17-1** onAccept race — needs careful test + cleanup branch
6. **C-17-Lifecycle-C2** authenticator leak guard

**Larger initiatives (own task, can't fit a single turn):**
7. **B-17-2** autoRead atomic gate (needs design)
8. **B-OBS1..8** SelectorMetrics wiring (separate multi-commit initiative)
9. **B-T1 / C-1** real SocketServer Processor IT
