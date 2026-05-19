You are a senior systems engineer auditing a feature branch.

Context: this is the experiment/io-uring branch of Apache Kafka. PROMPT.md asks for an io_uring-based broker-side selector that plugs into Kafka via a new `socket.selector.implementation=auto|nio|io_uring` config knob. v1 scope: Linux + PLAINTEXT only. No changes under clients/. Use Netty (not Panama FFI). Do not reach for virtual threads.

The two most recent commits (HEAD and HEAD~1) fix two BLOCKERs from a prior Codex audit:

  HEAD: d85e5fa6fc — "Defer io_uring LISTEN socket bind from Processor constructor to Acceptor.start()"
  HEAD~1: beb52b45f4 — "Fix mute/unmute race that bypasses the io_uring inbound watermark gate"

Please do two things:

1. Verify those two fixes are correct, complete, and don't regress anything else. Look at the actual code:
   - server/src/main/java/org/apache/kafka/network/iouring/IoUringServerListener.java
   - server/src/main/java/org/apache/kafka/network/iouring/IoUringTransportLayer.java
   - server/src/test/java/org/apache/kafka/network/iouring/IoUringTransportLayerTest.java
   - server/src/test/java/org/apache/kafka/network/iouring/IoUringServerListenerIT.java
   - server/src/test/java/org/apache/kafka/network/iouring/bench/SelectorThroughputBenchmark.java
   - core/src/main/scala/kafka/network/SocketServer.scala (around Processor.start, Acceptor.start, enableRequestProcessing)

2. Do a fresh end-to-end audit of the cumulative io_uring path. Files to read:
   - server/src/main/java/org/apache/kafka/network/iouring/*.java
   - server/src/test/java/org/apache/kafka/network/iouring/*.java
   - core/src/main/scala/kafka/network/SocketServer.scala (io_uring sections)
   - PROMPT.md (the contract)

I want PRODUCTION-GRADE judgment, not "happy path works". Look for:
  - DoS surfaces (direct-memory leaks, slowloris, pipelined-writes, accept floods)
  - Lifecycle issues (close ordering, shutdown races, half-open / closingChannels)
  - Quota and ConnectionQuotas correctness
  - Metrics parity with NIO
  - Authorizer/principal-builder contract parity
  - Edge cases in mute/unmute, MemoryPool self-mute, watermark gates
  - Concurrency bugs in Netty event-loop / Processor handoff
  - Wire-protocol behavior parity with NIO

Output: 'PRODUCTION-GRADE FOR V1 (PLAINTEXT)' or 'NOT PRODUCTION-GRADE' with a specific BLOCKER list. Distinguish BLOCKER (must fix before v1 ships) from CORRECTNESS (should fix soon, but v1 can ship) from POLISH. Be terse.
