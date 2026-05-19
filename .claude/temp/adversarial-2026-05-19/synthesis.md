# Round 16 — Four-agent adversarial sweep (2026-05-19)

Branch: `experiment/io-uring`. Audit posture: read-only, ambitious adversary. No external AI (codex/gemini) used.

Four parallel agents, ~67 findings total, ~16 BLOCKERs after dedup.

## Cross-agent consolidated BLOCKER list (top of stack)

### Code correctness (selector + transport + lifecycle)

**B-S1** Remote disconnects always emit `ChannelState.LOCAL_CLOSE` instead of `channel.state()` — `IoUringSelector.java:658` (`drainClosingChannels`). NIO does `disconnected.put(id, channel.state())`. Misclassifies every peer-FIN, breaks every connection-cause dashboard. **Tiny, clean fix.**

**B-S2** Async write listener decrements `pendingWriteBytes` BEFORE setting `asyncWriteFailure` — `IoUringTransportLayer.java:480-504`. Brief window where `hasPendingWrites()` returns false on a failed write → `KafkaChannel.maybeCompleteSend()` clears the send → broker emits `RESPONSE_SENT` for a response the kernel rejected. **Tiny ordering swap.**

**B-S3** `onAccept` vs `close()` race — `IoUringSelector.java:314-366` vs `:994-1016`. Pass-the-closed-check on event-loop, then close() drains, then the accept body populates `nettyChannels`/`pendingAccepts` — channel leaks (direct memory + transport queue + Netty channel). Agent 4's BLOCKER 2 is the same race seen from the lifecycle side.

**B-S4** Selector close does NOT shut the event-loop group — that lifecycle lives on `IoUringServerListener` (per its own javadoc). If the listener is leaked or close-ordering inverts, ring fd + event-loop thread + accepted children survive.

**B-S5** `serverChannel.close().sync()` in `IoUringServerListener.close():232` has no timeout. With the production hang from task #69, broker shutdown deadlocks here. `syncUninterruptibly()` on the event-loop shutdown also swallows interrupt.

**B-S6** `pendingWriteBytes` and `asyncWriteFailure` never reset on `IoUringTransportLayer.close()` — counters strand; async failure recorded but never observed.

**B-S7** `IoUringSelector.close()` doesn't release queued `NetworkReceive`s in `completedReceives` — every receive whose ByteBuffer came from `MemoryPool` leaks pool quota.

**B-S8** `nettyChannels.put(id, ...)` silently clobbers prior entry on `idGen` wrap (2.1B accepts) — NIO throws `IllegalStateException` here. Silent leak of prior KafkaChannel + Netty channel.

**B-S9** `failedSends` uses `List.remove(Object)` — duplicate id entries linger, causing stale disconnect emission → connection-quota underflow.

### Observability (metrics parity with NIO)

**B-M1..B-M15** **The io_uring transport emits ZERO `SelectorMetrics` sensors**: `io-wait-ratio`, `io-ratio`, `io-time-ns-*`, `io-wait-time-ns-*`, `connection-count`, `connections-created-*`, `connection-close-*`, `incoming-byte-*`, `outgoing-byte-*`, `request-rate`, `request-size-*`, `response-rate`, `network-io-rate`, `select-rate`, `successful-authentication-rate` (PLAINTEXT auth invisible). Per-listener dashboards & capacity-planning alerts (`NetworkProcessorAvgIdlePercent` is 0.0 on io_uring) all wrong. `IoUringChannelMetadataRegistry`'s own javadoc admits a known gap. This is the single largest gap on the branch — entire operator-observability surface is missing.

### Test coverage (gaps)

**B-T1** No end-to-end test that wires `IoUringSelector` through real `SocketServer` + `Processor` + `Acceptor`. Zero io_uring references under `core/src/test/`. Every existing test drives the selector directly, bypassing the `Selectable` contract used in production.

**B-T2** `MultiClientSustainedTrafficIT.trackStall` returns `-1` and `awaitOrDumpOnStall` returns silently on detected stall — failure surfaces only via trip-count assertion, no explicit `fail("stall detected")`. Diagnostic intent is advisory, not load-bearing.

**B-T3** `IoUringTransportLayerTest.writeWakeCallbackFires` uses `EmbeddedChannel` (synchronous) — does NOT exercise cross-thread wake. The v10 regression class can re-enter silently.

## Other CRITICAL findings (no BLOCKER tag but high impact)

- Watermark gate races: `setAutoRead` called from BOTH Processor and event-loop threads → wrong state can stick → direct-memory exhaustion path the gate exists to prevent.
- `drainClosingChannels` busy-drains up to 1 MiB on one channel before yielding — fairness violation under disconnect storm.
- `transferFrom(FileChannel)` is untested and uses heap allocate + copy — Fetch path quality.
- `IoUringSupport.computeProbe` catches `ReflectiveOperationException` only, not `LinkageError`/`Error` — broker crash on Linux + missing native lib instead of NIO fallback.
- `IoUringTransportLayer.close()` race with `offerInbound` re-pollutes `inboundBytes` after drain.

## Recommended sequencing (next 30 min — before next /loop firing)

1. **B-S1** disconnect state — 5 minutes, surgical
2. **B-S2** async write listener ordering — 5 minutes, surgical
3. **B-T2** MultiClientSustainedTrafficIT explicit-fail + mute/unmute (also closes task #69) — 30 minutes
4. **B-T3** real cross-thread wake test for `writeWakeCallback` — 30 minutes

Items 1-3 form a tight commit; item 4 a second commit. The metrics gap (B-M1..15) is a separate, larger initiative.
