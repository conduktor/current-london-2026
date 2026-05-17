# Networking: io_uring

## Goal
A Linux PLAINTEXT listener that runs through Netty's `IoUringEventLoopGroup` behind Kafka's existing `Selectable` interface, with a graceful NIO fallback on non-Linux and on other security protocols. Zero changes under `clients/`.

## Constraints (do not violate)
- **Do not modify anything under `clients/`.** The selector lives broker-side; clients connect as normal.
- v1: **PLAINTEXT only.** SSL / SASL_PLAINTEXT / SASL_SSL fall back to NIO.
- Non-Linux: graceful NIO fallback (compile-time detection or runtime probe, not a crash).
- Use **Netty** for everything. Do **not** build a separate Panama FFI path — the deck team tried it and abandoned it.
- Do **not** reach for virtual threads. They're empirically worse above 50 connections (per-task work ~1 μs is below scheduling cost). The bottleneck is kernel-side, not userspace.

## Minimum viable outcome
1. Wrap `IoUringEventLoopGroup` to satisfy Kafka's `Selectable` interface.
2. Bridge Netty's event loop ↔ Kafka's Processor thread via a `ConcurrentLinkedQueue` (one bridge thread per Processor).
3. Broker config `socket.selector.implementation = io_uring | nio | auto` (default `auto`: io_uring on Linux + PLAINTEXT, NIO otherwise).
4. PLAINTEXT 10k-connection benchmark vs the NIO baseline. Numbers in a short note in the worktree — direction matters more than chasing the deck's exact percentages.

## Stretch (only after the minimum lands)
- SSL, SASL_PLAINTEXT, SASL_SSL via Netty SSL/SASL handlers.
- TLS offload tuning.
- Per-Processor pinning and event-loop sizing knobs.

## Careful
- Netty's event loop runs on its own thread; Kafka's Processor expects to drive I/O from its own thread. Without the bridge queue, you deadlock.
- Measure before optimising. Userspace cleverness made things worse for the deck team — the work is in the kernel.

## Lessons already known (don't rediscover)
- Two selector implementations doubles the bug surface. One backend (Netty), one fallback (NIO). Stop there.
- Virtual threads are a trap at this granularity. Skip them.

## Acceptance criteria
- Linux platform detection is automatic at initialisation; the io_uring selector throws with a clear error message if invoked on a non-Linux OS or a kernel without io_uring support. Selection of NIO vs io_uring happens at broker startup, not per-request.
- PLAINTEXT is the primary supported security protocol on the io_uring path. SSL, SASL_PLAINTEXT, and SASL_SSL are supported via the Netty handler pipeline; protocols not yet verified must fall back to NIO rather than fail.
- Cross-thread signalling between the Netty event loop and the Kafka Processor thread uses ConcurrentLinkedQueues — at minimum one for completed-receive events and one for disconnect events. There is one event-loop + queue pair per Selectable instance (per Processor).
- Virtual threads are not used anywhere in the completion-processing path.
- Idle-connection expiry on the io_uring path matches the NIO baseline: the same `connectionsMaxIdleNanos` parameter is respected, `ChannelState.EXPIRED` is emitted when the threshold is exceeded, and no busy-loops or spurious wakeups occur.
- Mute / unmute semantics match NIO: muting suppresses read-event delivery from the event loop; unmuting resumes delivery; toggling cycles cleanly without leaks or deadlocks.

## Functional test scenarios
- On a Linux host with io_uring support, the broker starts with the io_uring selector active; a PLAINTEXT client connects, registers, and exchanges a size-prefixed message round-trip; the selector reports completed sends and completed receives correctly; no spurious disconnects or idle expirations occur during steady-state traffic.
- A multi-protocol harness exercises PLAINTEXT, SSL, SASL_PLAINTEXT, and SASL_SSL against the io_uring backend on Linux; all handshakes complete and payloads transit correctly (or unsupported combinations fall back to NIO without crashing).
- On macOS or Windows (or any environment without io_uring), broker startup either throws with a clear unsupported-platform message or selects NIO automatically; no warning spam, no crash, no degraded behaviour.
- A short idle-timeout configuration (e.g., 100 ms) is applied to multiple registered connections; with no traffic, only the idle connections appear in `selector.disconnected()`; active connections that exchanged traffic remain connected.
- A connection is muted; the peer sends data; the next selector poll returns no read events. The connection is then unmuted; the next poll exposes the data. Mute / unmute cycles repeatedly without resource leaks.

## When stuck (escape hatch)
If you find yourself looping on the same dead-end — the same error reappearing, the same refactor reverted, no measurable progress over several attempts — the order of recourse is fixed:

1. **First, ask Codex** (OpenAI's app) for a second opinion on the specific blocker. Frame the question sharply: what you tried, what went wrong, what you suspect. Take its answer seriously and try the direction it suggests.
2. **If still stuck after Codex, ask Gemini** (Google's app) for an independent third opinion. Compare the two — divergence is informative; convergence is reassuring. Try the most credible direction.
3. **Only if you remain stuck after both Codex and Gemini**, you may consult `/home/florent/ivy-trunk` read-only **for inspiration only**. Read enough to understand the shape of a solution, then close the file and write your own from scratch in this repo's idiom. **No copy-paste. No transliteration. No "I'll just adapt this block."**

The commit that follows an ivy-trunk consultation must include in its message a one-liner of the form: `Stuck in local minimum on <one-line description>; Codex suggested <X>; Gemini suggested <Y>; consulted ivy-trunk for inspiration; took away <one-sentence insight>; moved on.` If you cannot honestly write that sentence, you copied — revert and try again. The escape hatch exists to break unproductive loops, not to import an implementation.

## Working mode: full autonomy with sub-agent verification
You must drive this project to completion in **total autonomy**. No human babysitting, no mid-flight permission requests, no "should I continue?" check-ins. Plan, decide, implement, test, iterate, ship. The acceptance criteria and functional test scenarios above are your contract — done means they all pass.

**At the start of every task, fan out a fleet of sub-agents to figure out HOW to do it.** Tailor the fleet to the task — e.g., one sub-agent to map the affected code paths, one to identify the right Kafka APIs and existing patterns to reuse, one to surface edge cases and prior art in this repo, one to enumerate known pitfalls and lessons-already-known. Synthesize their findings before writing your first line of implementation code; do not skip this step because the task "looks simple."

**After every major phase** (initial planning closed; minimum viable outcome reached; each stretch item considered; before merge), fan out another fleet of sub-agents to **audit what happened**: spec compliance against the acceptance criteria and functional test scenarios above, test coverage, regressions in adjacent code, performance impact, edge cases missed, security/authorisation correctness. **Two slots in this audit fleet are mandatory**: one sub-agent consults **Codex** (OpenAI's app), one sub-agent consults **Gemini** (Google's app). Feed each the diff and a sharp question. Take their feedback seriously — integrate what improves the work, push back in writing on what doesn't, never silently ignore.

You may also consult Codex on demand mid-phase when choosing between two non-trivial design options.

Record each LLM consultation briefly in the commit body: `Codex: asked <one-line question>; took <one-line takeaway>. Gemini: asked <one-line question>; took <one-line takeaway>.` Never skip the audit because you "feel done."
