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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executor;
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

    private volatile Session session;
    private volatile WsStreamer streamer;
    private volatile boolean closed;

    KafkaWebSocketEndpoint(String topic, RequestSubmitter submitter, ObjectMapper mapper,
                           WsStreamLimiter.Token token, Executor httpExecutor) {
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
        this.submitter = Objects.requireNonNull(submitter, "submitter must not be null");
        this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
        this.token = Objects.requireNonNull(token, "token must not be null");
        this.httpExecutor = Objects.requireNonNull(httpExecutor, "httpExecutor must not be null");
    }

    @Override
    public void onWebSocketOpen(Session session) {
        this.session = session;
        session.setIdleTimeout(IDLE_TIMEOUT);
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
            LOG.debug("WS close on {} status={} reason={}", topic, statusCode, reason);
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

    /** Close reason strings are limited to 123 bytes by RFC 6455. Trim defensively. */
    private static String truncateReason(String reason) {
        if (reason == null) {
            return "";
        }
        if (reason.length() <= 100) {
            return reason;
        }
        return reason.substring(0, 100);
    }

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
        public boolean isOpen() {
            Session s = KafkaWebSocketEndpoint.this.session;
            return s != null && s.isOpen();
        }
    }
}
