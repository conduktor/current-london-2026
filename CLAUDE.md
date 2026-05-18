# Project: Kafka multi-tenancy fork

This is a fork of Apache Kafka being modified to add tenant-aware topic
namespacing. The contract is in `PROMPT.md` at the repo root — read it before
making changes.

## Hard constraints (do not violate)

- **Do not modify anything under `clients/`.** Stock producers and consumers must
  connect with SASL and see a pristine cluster. All tenant logic lives in
  `server/`, `core/`, or new modules.
- Tenant identity is encoded in `KafkaPrincipal` as `__tenant_<id>.<user>` and is
  immutable for the connection lifetime.
- Kafka-internal topics (`__consumer_offsets`, `__transaction_state`,
  `__share_group_state`) are never tenant-prefixed.
- A privileged caller on a tenant-bound listener without a `__tenant_` prefix
  must be **rejected**, never silently rewritten into the tenant namespace.

## Discipline at every important step

Before declaring a step done, verify each of these — in parallel where possible:

1. **The code is clean.** No dead code, no half-implementations, no leftover
   imports, no speculative abstractions. Every changed line traces directly to a
   `PROMPT.md` acceptance criterion or to making a test pass.
2. **The tests faithfully express the intent.** Read each test back and ask:
   "if this passes, what does that actually prove?" If the answer is "the code
   compiles" or "a method got called", strengthen the assertion or delete the
   test.
3. **No duplication.** Look for repeated rewrite logic, repeated config
   parsing, repeated topic-name extraction. Extract once; reuse everywhere.
4. **Integration tests are not optional.** Unit tests pin units; integration
   tests pin behaviour against real Kafka request/response shapes. For each
   acceptance criterion in `PROMPT.md`, there should be at least one test that
   exercises the actual handler path (mocked dependencies are fine; mocked
   intent is not).
5. **Match local conventions.** Java 21, Apache license header, package layout
   under `org.apache.kafka.server.tenant`, JUnit 5, the `assertEquals`/
   `assertThrows` style already used in `server/src/test/java/...`.

These checks compose — run them after each unit of work, not just at the end.

## Working mode

`PROMPT.md` mandates full autonomy with sub-agent verification:

- **At the start of every phase**, fan out a fleet of sub-agents (via the Agent
  tool) to figure out HOW: map affected code paths, identify reusable Kafka
  APIs, surface edge cases, enumerate known pitfalls.
- **After every major phase**, fan out an audit fleet: spec compliance, test
  coverage, regressions, edge cases, security correctness.
- Use the **Plan** subagent for design questions, **Explore** for read-only
  surveys, and **general-purpose** for multi-step research.

## Build & test

Use JDK 21 (the repo's Gradle does not yet support JDK 27):

```bash
JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 ./gradlew \
  :server:test --tests 'org.apache.kafka.server.tenant.*'
```

## When stuck

Follow the escape hatch in `PROMPT.md` §"When stuck": Codex first, then Gemini,
then `/home/florent/ivy-trunk` as last resort — and only for inspiration, never
copy-paste. Record the consultation in the commit body in the prescribed form.
