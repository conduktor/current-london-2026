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
            LOG.warn("WS unexpected failure handling frame on {}", topic, e);
            closeWithInternalError(e.getMessage());
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
     * sendText uses {@code Callback.NOOP} — fire-and-forget — because the streamer's credit gating
     * already bounds the in-flight queue per subscription, and Jetty's own outgoing-frame queue
     * (capped by {@code setMaxOutgoingFrames}) backstops any pathology.
     */
    private final class SessionFrameSink implements WsStreamer.FrameSink {
        @Override
        public void sendText(String text) {
            Session s = KafkaWebSocketEndpoint.this.session;
            if (s == null || !s.isOpen()) {
                throw new IllegalStateException("WS session closed");
            }
            s.sendText(text, Callback.NOOP);
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
