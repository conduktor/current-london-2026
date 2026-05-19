/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.network.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Jetty 12 native {@code Session.Listener.AutoDemanding} for the {@code /v1/topics/{topic}/subscribe}
 * WebSocket endpoint. One instance per upgraded connection; ownership of the
 * {@link WsStreamLimiter.Token} (acquired by the upgrade creator) and the streamer's lifetime are
 * tied to the lifetime of this listener.
 *
 * <p><strong>Protocol state machine.</strong> Jetty serialises message callbacks per session, so we
 * can model the state transitions without explicit locking:
 * <ul>
 *   <li>{@code OPEN → SUBSCRIBED}: the first text frame must be a valid {@code subscribe} message;
 *       a {@link WsStreamer} is constructed (taking ownership of the limiter token) and the initial
 *       credit budget is delivered.</li>
 *   <li>{@code SUBSCRIBED → SUBSCRIBED}: every subsequent text frame must be a {@code flow} message;
 *       its credit value is forwarded to {@code streamer.grantCredits()}.</li>
 *   <li>{@code * → CLOSED}: any protocol violation closes the session with status
 *       {@link StatusCode#BAD_DATA 1003} ("Unsupported Data") and releases the limiter token. A
 *       transport-level failure surfaces as {@code onWebSocketError} and follows the same release
 *       path; idempotent so error+close together still releases exactly once.</li>
 * </ul>
 *
 * <p><strong>Why state is held here and not in {@link WsStreamer}.</strong> The streamer is
 * deliberately ignorant of the connection's protocol — it knows only "deliver records subject to
 * credit". The endpoint owns the protocol because (a) it must reject ill-formed frames *before* a
 * streamer is constructed (otherwise a bad first frame would leak a streamer), and (b) it must
 * map streamer-level send failures to WebSocket close codes.
 *
 * <p>Class visibility: although only {@link KafkaHttpServer} constructs it, this class must be
 * {@code public} because Jetty's listener-method dispatch in {@code JettyWebSocketFrameHandlerFactory}
 * uses {@code MethodHandles.Lookup.unreflect()} from a different package — that lookup requires public
 * class access, otherwise the upgrade attempt fails with an {@code IllegalAccessException} surfaced as
 * HTTP 500. Constructor is package-private to keep instantiation under the bridge's control.
 */
public final class KafkaWebSocketEndpoint implements Session.Listener.AutoDemanding {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaWebSocketEndpoint.class);

    /**
     * Idle timeout for an upgraded WS subscription. Live-tail can have long quiet periods between
     * records on a slow topic, so the default 30s Jetty idle timeout is too aggressive — quiet
     * subscriptions would be torn down even though both sides are healthy. Five minutes gives the
     * scheduler enough slack for live-tail while still reclaiming truly dead connections; the
     * concurrent-subscription cap remains the load-bearing defence against runaway clients.
     */
    private static final Duration IDLE_TIMEOUT = Duration.ofMinutes(5);

    /**
     * Tight idle timeout enforced from the moment the connection opens until the first <em>positive</em>
     * credit grant. The limiter slot is acquired at the upgrade handshake (before the streamer exists),
     * so a client that opens the WS and never speaks — or subscribes with {@code credits=0} and then
     * never grants flow — would otherwise pin the slot for the full {@link #IDLE_TIMEOUT} (5 min).
     * 30 seconds is more than enough for a real client to land its subscribe + first credit grant after
     * the upgrade completes; tightening this is the difference between "limiter slot held for 30s
     * before the connection drops" and "5 min" under an open-and-sit attack pattern.
     *
     * <p>Why gated on credits, not on subscribe arrival: a subscribe-with-credits=0 frame issues no
     * broker fetch, so it has the same DoS profile as an unopened connection — just a held slot with
     * no data flowing. Relaxing to {@link #IDLE_TIMEOUT} only once a positive credit grant exists
     * keeps the "subscribe-zero-then-flow-later" idiom available to a well-behaved client (it has
     * 30s to grant credit) while denying the slot-pinning escape hatch.
     *
     * <p><strong>Why a paired explicit deadline (Wave 41 axis AAA).</strong> The Jetty idle timeout
     * is a <em>byte-level</em> watchdog: {@code AbstractEndPoint.fill()} calls {@code notIdle()} on
     * every inbound read, so a client that sends a 2-byte WebSocket Ping (opcode 0x9) every 25s keeps
     * the watchdog alive indefinitely while never sending a subscribe frame. Pings are auto-handled by
     * Jetty (the bridge never sees them) and they cost the attacker nothing — a single attacker can
     * pin every {@link WsStreamLimiter} slot until {@link #IDLE_TIMEOUT}. The fix is to <em>also</em>
     * arm an explicit one-shot deadline at {@link #onWebSocketOpen} that fires regardless of inbound
     * byte activity: see {@link #scheduleSubscribeDeadline()} / {@link #onSubscribeDeadlineFired()}.
     * The Jetty idle timeout remains as defence-in-depth for the bytes-not-arriving case.
     */
    private static final Duration PRE_SUBSCRIBE_IDLE_TIMEOUT = Duration.ofSeconds(30);

    /**
     * Hard cap on queued outgoing frames per session. The credit-gated subscribe protocol is the
     * primary backpressure mechanism — but it relies on the client granting credit only when it's
     * ready to consume. A misbehaving client that grants a huge credit budget then stops reading
     * from its socket would otherwise let Jetty's default-unbounded outbound queue grow without
     * limit (Jetty 12's {@code MaxOutgoingFrames} defaults to {@code -1}). With this cap, the
     * underlying writer surfaces a send failure once the queue is full, the streamer treats that
     * as a fatal stream error, and the subscription is torn down — bounding per-session memory
     * regardless of client misuse. 1024 frames leaves plenty of headroom for normal bursty
     * delivery without ever growing unboundedly.
     */
    private static final int MAX_OUTGOING_FRAMES = 1024;

    private final String topic;
    private final RequestSubmitter submitter;
    private final ObjectMapper mapper;
    private final Executor httpExecutor;

    /**
     * Limiter token held until the endpoint terminates. Ownership transfers to the {@link WsStreamer}
     * the moment the streamer is constructed (so that the streamer's close also releases the slot);
     * before that point, we own it directly. {@code released} guards against double-release.
     */
    private final WsStreamLimiter.Token token;
    private final AtomicBoolean released = new AtomicBoolean(false);

    /**
     * One-shot guard: the idle timeout is relaxed from {@link #PRE_SUBSCRIBE_IDLE_TIMEOUT} to
     * {@link #IDLE_TIMEOUT} exactly once, when the first positive credit grant arrives (either an
     * initial {@code subscribe} with non-zero credits, or the first {@code flow} after a zero-credit
     * subscribe). Subsequent flow grants are no-ops on the timeout.
     */
    private final AtomicBoolean idleTimeoutRelaxed = new AtomicBoolean(false);

    /**
     * Shared registry of live endpoints owned by the surrounding {@link KafkaHttpServer}. The endpoint
     * adds itself in {@link #onWebSocketOpen} (which Jetty serialises per session) and removes itself
     * in {@link #tearDown}. The server walks this registry at the start of {@code stop()} to send each
     * live peer an RFC 6455 §5.5.1 close frame with {@link StatusCode#SHUTDOWN 1001} before the connector
     * force-closes the underlying socket. Without this, a planned broker restart leaves every connected
     * WS client observing close code 1006 (abnormal closure) instead of 1001 (going away) — monitoring
     * dashboards then conflate orderly restarts with transport failures. The registry is a
     * {@code ConcurrentHashMap}-backed set so concurrent add/remove on Jetty's I/O threads does not race
     * the shutdown walk on the broker's stop thread.
     */
    private final Set<KafkaWebSocketEndpoint> activeSessions;

    private volatile Session session;
    private volatile WsStreamer streamer;
    private volatile boolean closed;

    KafkaWebSocketEndpoint(String topic, RequestSubmitter submitter, ObjectMapper mapper,
                           WsStreamLimiter.Token token, Executor httpExecutor,
                           Set<KafkaWebSocketEndpoint> activeSessions) {
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
        this.submitter = Objects.requireNonNull(submitter, "submitter must not be null");
        this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
        this.token = Objects.requireNonNull(token, "token must not be null");
        this.httpExecutor = Objects.requireNonNull(httpExecutor, "httpExecutor must not be null");
        this.activeSessions = Objects.requireNonNull(activeSessions, "activeSessions must not be null");
    }

    @Override
    public void onWebSocketOpen(Session session) {
        this.session = session;
        // Register before any blocking work so any stop() whose snapshot is taken AFTER this point
        // observes the entry and delivers the 1001 close frame. The complementary window — stop()'s
        // snapshot taken between KafkaHttpServer.createWebSocket returning the endpoint and Jetty
        // invoking onWebSocketOpen here — is closed by the double-checked {@code shuttingDown} flag in
        // KafkaHttpServer.createWebSocket (before and after tryAcquire), which converts that race into
        // a clean 503 instead of letting an unregistered session slip through. Jetty serialises
        // lifecycle callbacks per session, so the matching remove() in tearDown() cannot reorder
        // before this add().
        activeSessions.add(this);
        // Start with the tight pre-subscribe timeout; handleFirstFrame relaxes to IDLE_TIMEOUT once the
        // subscribe frame has been validated and the streamer is constructed.
        session.setIdleTimeout(PRE_SUBSCRIBE_IDLE_TIMEOUT);
        // Cap the inbound text size so a hostile client cannot stream a multi-megabyte subscribe
        // frame into our heap. Subscribe/flow JSONs are tens of bytes; 8 KiB is loose but cheap.
        session.setMaxTextMessageSize(8 * 1024);
        // The bridge has no binary message sink — Jetty drops binary frames silently — but its default
        // 64 KiB reassembly cap still lets a hostile client park up to that ceiling of transient heap
        // per session before the data is discarded. Align the binary cap with the text cap so the
        // inbound heap ceiling does not differ by frame type.
        session.setMaxBinaryMessageSize(8 * 1024);
        // Per-frame cap that complements the per-message caps above. Without this, a single 64 KiB
        // text frame is parsed up to Jetty's default 64 KiB before the 8 KiB message cap fires; with
        // this set, oversized frames are rejected at the parser instead of after full reassembly.
        session.setMaxFrameSize(8 * 1024);
        // Defence-in-depth backstop for the credit protocol. See MAX_OUTGOING_FRAMES javadoc — this
        // is what stops a slow-reader-with-large-credit-grant from accumulating an unbounded outbound
        // queue inside Jetty.
        session.setMaxOutgoingFrames(MAX_OUTGOING_FRAMES);
        // Wave 41 axis AAA: arm an explicit subscribe deadline that is independent of the Jetty
        // byte-level idle timeout. A hostile client that sends only WebSocket Pings keeps Jetty's
        // notIdle() ticking forever; without this scheduler-driven deadline the PRE_SUBSCRIBE
        // watchdog never fires and the limiter slot is pinned for the full IDLE_TIMEOUT. See the
        // javadoc on PRE_SUBSCRIBE_IDLE_TIMEOUT for the full attack model.
        scheduleSubscribeDeadline();
    }

    /**
     * Wave 41 axis AAA: schedule the explicit subscribe deadline. Dispatches a one-shot runnable via
     * the {@code httpExecutor} after {@link #PRE_SUBSCRIBE_IDLE_TIMEOUT}; the runnable invokes
     * {@link #onSubscribeDeadlineFired()}, which is a no-op if positive credits have already been
     * granted or the endpoint has already torn down.
     *
     * <p>The pattern matches {@link WsStreamer#scheduleDrainAfter(long)}: the same JDK static delayer
     * fires the timer, then dispatches to the Jetty thread pool. The {@code .exceptionally} handler
     * catches an uncaught throw from inside {@link #onSubscribeDeadlineFired()} (the action is already
     * fully defensive, so this is a future-proofing safety net, not a routine path). A
     * {@code RejectedExecutionException} raised when the delayer eventually submits to a stopped
     * {@code httpExecutor} surfaces inside the JDK delayer thread, not through the dependent future —
     * the deadline simply never enforces in that race, and the Jetty idle timeout remains the
     * defence-in-depth signal.
     */
    private void scheduleSubscribeDeadline() {
        try {
            CompletableFuture.runAsync(this::onSubscribeDeadlineFired,
                CompletableFuture.delayedExecutor(
                    PRE_SUBSCRIBE_IDLE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS, httpExecutor))
                .exceptionally(t -> {
                    LOG.debug("WS subscribe-deadline handler threw on {}: {}", topic,
                        t == null ? "null" : t.toString());
                    return null;
                });
        } catch (RuntimeException e) {
            // The synchronous dispatch leg (queueing into the static delayer) is virtually never
            // reachable, but if it ever throws we lose the deadline. Log so a regression is
            // diagnosable; the Jetty idle timeout remains as defence-in-depth.
            LOG.warn("WS subscribe-deadline schedule failed on {}", topic, e);
        }
    }

    /**
     * Wave 41 axis AAA: deadline-fire handler. Closes the session with {@link StatusCode#POLICY_VIOLATION
     * 1008} when the explicit subscribe deadline elapses without a positive credit grant having arrived.
     * Package-private so unit tests can simulate the deadline firing without waiting for real time —
     * production callers go through {@link #scheduleSubscribeDeadline()}.
     *
     * <p>No-op if either of:
     * <ul>
     *   <li>{@code idleTimeoutRelaxed} is set — the steady-state path is already in force and the
     *       Jetty idle timeout (5 min) is now the controlling watchdog;</li>
     *   <li>{@code closed} is set — teardown already completed for some other reason.</li>
     * </ul>
     * Both checks are race-tolerant: a concurrent positive credit grant that flips
     * {@code idleTimeoutRelaxed} between the check and the close is also harmless because
     * {@code tearDown()} is idempotent and the broker has nothing in flight on a session that has
     * yet to issue its first fetch.
     */
    void onSubscribeDeadlineFired() {
        if (closed || idleTimeoutRelaxed.get()) {
            return;
        }
        LOG.debug("WS subscribe deadline elapsed on {} — closing 1008", topic);
        Session s = this.session;
        if (s != null) {
            sendErrorEnvelope("SUBSCRIBE_DEADLINE",
                "no positive credits granted within subscribe deadline");
            try {
                s.close(StatusCode.POLICY_VIOLATION, "subscribe deadline elapsed", Callback.NOOP);
            } catch (RuntimeException e) {
                LOG.debug("close(1008) failed on {}: {}", topic, e.toString());
            }
        }
        tearDown();
    }

    @Override
    public void onWebSocketText(String message) {
        if (closed) {
            // A frame arrived after we already torn down. Drop silently — Jetty will deliver the
            // close to the client; nothing more for us to do.
            return;
        }
        try {
            WsSubscribeMessageParser.WsClientMessage msg = WsSubscribeMessageParser.parse(message);
            if (streamer == null) {
                handleFirstFrame(msg);
            } else {
                handleFlowFrame(msg);
            }
        } catch (WsSubscribeMessageParser.BadMessageException e) {
            LOG.debug("WS protocol violation on {}: {}", topic, e.getMessage());
            closeWithProtocolError(e.getMessage());
        } catch (RuntimeException e) {
            // Anything else (the streamer's start path can throw if a fetch submission blows up at
            // construction time, for example) is treated as an internal error. Close 1011 so the
            // client distinguishes "I did something wrong" (1003) from "server failed" (1011).
            // Log the throwable server-side; never pass e.getMessage() into the close reason or
            // error envelope — stock JDK messages (e.g. NPE "Cannot invoke X.y() because z is null")
            // leak broker class/field names to any subscribed client.
            LOG.warn("WS unexpected failure handling frame on {}", topic, e);
            closeWithInternalError(null);
        }
    }

    private void handleFirstFrame(WsSubscribeMessageParser.WsClientMessage msg) {
        if (!(msg instanceof WsSubscribeMessageParser.WsSubscribeCommand)) {
            // The state machine says the first frame must be subscribe; a flow frame here is a
            // client bug. Don't try to recover — close 1003 with a descriptive envelope.
            throw new WsSubscribeMessageParser.BadMessageException(
                "first WebSocket frame must be 'subscribe', got 'flow'");
        }
        WsSubscribeMessageParser.WsSubscribeCommand sub =
            (WsSubscribeMessageParser.WsSubscribeCommand) msg;
        // Transfer ownership of the limiter token to the streamer. From this point onwards the
        // streamer's close() is responsible for releasing the slot; our own release path becomes
        // a no-op because the token is idempotent.
        streamer = WsStreamer.start(new SessionFrameSink(), submitter, mapper,
            topic, sub.partition(), sub.offset(), sub.maxBytes(), sub.initialCredits(),
            token, httpExecutor);
        // Relax the idle timeout from the tight pre-subscribe window to the steady-state value ONLY when
        // the subscribe brings positive initial credits with it. A subscribe-with-credits=0 frame triggers
        // no broker fetch, so it must stay under the tight 30s watchdog — otherwise it pins the limiter
        // slot for the full IDLE_TIMEOUT (5 min) with no data flowing. The first positive flow grant in
        // handleFlowFrame() picks up the relax for the zero-credit-then-flow-later idiom.
        maybeRelaxIdleTimeout(sub.initialCredits());
    }

    private void handleFlowFrame(WsSubscribeMessageParser.WsClientMessage msg) {
        if (!(msg instanceof WsSubscribeMessageParser.WsFlowCommand)) {
            // After subscribe, only flow is valid. A second subscribe is a state-machine violation —
            // multiplexing two streams on one connection isn't supported.
            throw new WsSubscribeMessageParser.BadMessageException(
                "after 'subscribe', subsequent frames must be 'flow', got 'subscribe'");
        }
        WsSubscribeMessageParser.WsFlowCommand flow = (WsSubscribeMessageParser.WsFlowCommand) msg;
        streamer.grantCredits(flow.credits());
        // Picks up the relax for the subscribe-zero-then-flow-later idiom: a well-behaved client that
        // subscribed with credits=0 has up to PRE_SUBSCRIBE_IDLE_TIMEOUT (30s) to send the first positive
        // flow grant, at which point the watchdog drops back to IDLE_TIMEOUT for steady-state operation.
        maybeRelaxIdleTimeout(flow.credits());
    }

    /**
     * Relax the idle timeout from {@link #PRE_SUBSCRIBE_IDLE_TIMEOUT} to {@link #IDLE_TIMEOUT} on the
     * first positive credit grant. Subsequent calls (or any call with non-positive credits) are no-ops.
     * The CAS ensures the timeout is set exactly once: a benign client that subscribes with credits=10
     * and then grants 5 more later sets the timeout once and never touches it again, matching the
     * pre-fix behaviour for that path.
     */
    private void maybeRelaxIdleTimeout(int credits) {
        if (credits > 0 && idleTimeoutRelaxed.compareAndSet(false, true)) {
            Session s = this.session;
            if (s != null) {
                s.setIdleTimeout(IDLE_TIMEOUT);
            }
        }
    }

    @Override
    public void onWebSocketError(Throwable cause) {
        // Transport-level failure (write failure, idle timeout, etc.). Jetty MAY still deliver
        // onWebSocketClose afterwards, but is allowed not to — our cleanup must be idempotent.
        LOG.debug("WS error on {}: {}", topic, cause.toString());
        tearDown();
    }

    @Override
    public void onWebSocketClose(int statusCode, String reason) {
        if (LOG.isDebugEnabled()) {
            // The close-frame reason is the remote peer's RFC 6455 §5.5.1 payload — up to 123 UTF-8 bytes of
            // arbitrary client-supplied data. Without sanitisation a peer that sends `reason="\nWARN forged"`
            // forges a log line on every log aggregator that line-splits; operators routinely enable DEBUG
            // under incident response so the "DEBUG-gated" mitigation is not sufficient. Mirror the policy
            // applied after Wave 26 to the topic-name slot: never let an untrusted byte sequence land in a
            // logger format argument regardless of level. {@code sanitizeShortPreview} substitutes '?' for
            // C0 controls + DEL and truncates above 32 chars.
            LOG.debug("WS close on {} status={} reason={}", topic, statusCode,
                WsSubscribeMessageParser.sanitizeShortPreview(reason));
        }
        tearDown();
    }

    /**
     * Tear everything down: close the streamer (which releases its copy of the token + closes the
     * underlying sink) if one was started; otherwise release the token directly. Idempotent because
     * both Token.close() and our own {@code released} flag are.
     */
    private void tearDown() {
        closed = true;
        WsStreamer s = streamer;
        if (s != null) {
            try {
                s.close();
            } catch (RuntimeException e) {
                LOG.debug("WsStreamer.close() raised on teardown of {}: {}", topic, e.toString());
            }
        }
        // Always also release our own reference to the token — Token.close() is idempotent so this
        // is safe even when the streamer has already released. If no streamer was started, this is
        // the only release path and it's load-bearing.
        if (released.compareAndSet(false, true)) {
            token.close();
        }
        // Drop from the shutdown registry last — once tearDown has reached this point the session has
        // no further interaction with the server. remove() on a ConcurrentHashMap-backed set is
        // idempotent so a concurrent closeForShutdown() walking the snapshot cannot trip on this.
        activeSessions.remove(this);
    }

    /**
     * Initiated by {@link KafkaHttpServer#stop()} at the start of the graceful-shutdown window. Sends an
     * RFC 6455 §5.5.1 close frame with {@link StatusCode#SHUTDOWN 1001} ("Going Away") so peers can
     * distinguish a planned broker restart from a transport failure ({@link StatusCode#ABNORMAL 1006},
     * which is what Jetty's force-close on connector stop would otherwise deliver). After the frame is
     * dispatched we tear down our own state — the streamer's close releases the limiter token and the
     * outbound queue is drained inside the {@code Server.stop()} grace window.
     *
     * <p>Idempotent: a session that has already closed (via client disconnect, error, or a prior
     * shutdown walk) is a no-op. Called on the broker's stop thread, not Jetty's I/O thread, so the
     * session field is read once and the Jetty-side serialisation guarantees of {@code session.close}
     * are still respected (it queues the close frame on the session's strand).
     */
    void closeForShutdown() {
        Session s = this.session;
        if (s != null && s.isOpen()) {
            try {
                s.close(StatusCode.SHUTDOWN, "broker shutting down", Callback.NOOP);
            } catch (RuntimeException e) {
                LOG.debug("close(1001) failed on {}: {}", topic, e.toString());
            }
        }
        tearDown();
    }

    private void closeWithProtocolError(String reason) {
        Session s = this.session;
        if (s != null) {
            sendErrorEnvelope("BAD_MESSAGE", reason);
            try {
                s.close(StatusCode.BAD_DATA, truncateReason(reason), Callback.NOOP);
            } catch (RuntimeException e) {
                LOG.debug("close(1003) failed on {}: {}", topic, e.toString());
            }
        }
        tearDown();
    }

    private void closeWithInternalError(String reason) {
        Session s = this.session;
        if (s != null) {
            sendErrorEnvelope("INTERNAL", reason);
            try {
                s.close(StatusCode.SERVER_ERROR, truncateReason(reason), Callback.NOOP);
            } catch (RuntimeException e) {
                LOG.debug("close(1011) failed on {}: {}", topic, e.toString());
            }
        }
        tearDown();
    }

    private void sendErrorEnvelope(String code, String message) {
        Session s = this.session;
        if (s == null) {
            return;
        }
        try {
            ObjectNode env = mapper.createObjectNode();
            env.put("type", "error");
            env.put("errorCode", code);
            env.put("errorMessage", message == null ? "" : message);
            s.sendText(mapper.writeValueAsString(env), Callback.NOOP);
        } catch (Exception e) {
            // Sending the error envelope is best-effort; the close frame is the load-bearing signal.
            LOG.debug("failed to emit error envelope on {}: {}", topic, e.toString());
        }
    }

    /**
     * RFC 6455 §5.5.1 caps the close-frame reason at 123 UTF-8 bytes. We trim more defensively at
     * {@value #MAX_REASON_BYTES} to leave headroom. Every caller today passes ASCII (parser literals,
     * {@code Errors.name()}, the Wave 6 {@code sanitizeShortPreview} which ?-substitutes non-ASCII),
     * so this never trims for the current call set. The byte-budget walk is for future Unicode
     * callers: a {@code String.length()}-based trim would cut a non-BMP code point into 1-3 of its
     * 4 UTF-8 bytes, and Jetty's writer would either reject the frame or emit a payload that fails
     * RFC 6455 §8.1's UTF-8 validity requirement on the peer side.
     */
    static String truncateReason(String reason) {
        if (reason == null) {
            return "";
        }
        byte[] bytes = reason.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= MAX_REASON_BYTES) {
            return reason;
        }
        int limit = MAX_REASON_BYTES;
        // Step back over UTF-8 continuation bytes (10xxxxxx) so the kept prefix ends at a code-point
        // boundary. Worst case is 3 steps back (4-byte code point with 3 continuation bytes).
        while (limit > 0 && (bytes[limit] & 0xC0) == 0x80) {
            limit--;
        }
        return new String(bytes, 0, limit, StandardCharsets.UTF_8);
    }

    static final int MAX_REASON_BYTES = 100;

    /**
     * Adapter that lets the streamer write to the Jetty Session without depending on Jetty types.
     * Each {@code sendText} hands Jetty a fresh {@link Callback} whose {@code failed} path tears the
     * subscription down. The credit gate is the primary backpressure for the fast-producer case;
     * the explicit {@link #MAX_OUTGOING_FRAMES} cap installed in {@link #onWebSocketOpen} bounds
     * queued frames for the slow-consumer-with-large-credit case. When that cap is exceeded Jetty
     * does NOT throw from {@code sendText} — it signals via {@code Callback.failed(...)} (typically
     * {@link java.nio.channels.WritePendingException}). Routing that failure into {@link #tearDown}
     * is what actually closes the stream when the queue fills; with {@code Callback.NOOP} the
     * overflow would be silently dropped while credit/offset accounting kept advancing.
     */
    private final class SessionFrameSink implements WsStreamer.FrameSink {
        @Override
        public void sendText(String text) {
            Session s = KafkaWebSocketEndpoint.this.session;
            if (s == null || !s.isOpen()) {
                throw new IllegalStateException("WS session closed");
            }
            s.sendText(text, Callback.from(
                () -> {
                    // succeeded — nothing to do; the streamer already accounted for credit.
                },
                cause -> {
                    // Failed sends mean the outbound queue is full (over the cap), the socket is
                    // gone, or the peer's TCP buffers are stuck. Any of those is terminal for this
                    // subscription — tear down so credit/offset accounting cannot drift further
                    // past frames that never reached the wire. tearDown() is idempotent so a
                    // burst of failed callbacks collapses into a single close.
                    LOG.debug("WS sendText failed on {}: {}", topic, cause == null ? "null" : cause.toString());
                    tearDown();
                }
            ));
        }

        @Override
        public void close() {
            Session s = KafkaWebSocketEndpoint.this.session;
            if (s != null && s.isOpen()) {
                try {
                    s.close();
                } catch (RuntimeException e) {
                    LOG.debug("FrameSink.close() failed on {}: {}", topic, e.toString());
                }
            }
        }

        @Override
        public void close(int statusCode, String reason) {
            // Streamer-initiated failure path. The error envelope was already sent as a text frame; the close
            // frame carries the RFC 6455 code (typically 1011 Server Error) which clients use for retry/back-off
            // decisions distinct from a clean 1000 Normal Closure. Reason text is bounded by RFC 6455 (123 bytes
            // after UTF-8 encoding) — defer to the shared truncator that the endpoint's own close paths use.
            Session s = KafkaWebSocketEndpoint.this.session;
            if (s != null && s.isOpen()) {
                try {
                    s.close(statusCode, truncateReason(reason), Callback.NOOP);
                } catch (RuntimeException e) {
                    LOG.debug("FrameSink.close({}) failed on {}: {}", statusCode, topic, e.toString());
                }
            }
        }

        @Override
        public boolean isOpen() {
            Session s = KafkaWebSocketEndpoint.this.session;
            return s != null && s.isOpen();
        }
    }
}
