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

import org.apache.kafka.common.internals.Topic;

import org.eclipse.jetty.ee10.servlet.ErrorHandler;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletContextRequest;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.ee10.websocket.server.config.JettyWebSocketServletContainerInitializer;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpHeaderValue;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Wraps a Jetty {@link Server} hosting {@link KafkaHttpServlet} at {@code /v1/topics/*}. The broker constructs one of
 * these and drives its lifecycle alongside its existing {@code SocketServer}. {@link #start()} is non-blocking;
 * {@link #boundPort()} returns the actual port once started (useful when the configured port was {@code 0}).
 *
 * <p>The class is intentionally minimal: it knows nothing about Kafka configuration parsing — the caller passes the
 * listener host/port directly. Plumbing the values out of {@code KafkaConfig} is the broker integration's job, not
 * this class's.
 *
 * <p><strong>Lifecycle is one-shot.</strong> An instance progresses through {@code constructed → started → stopped}.
 * After {@link #stop()} the {@link HttpBridgeMetrics} owned by this server are unregistered from the global Yammer
 * registry; calling {@link #start()} again on the same instance would wire the servlet to a closed metrics object
 * and silently drop every recording, so callers that need to restart the bridge must construct a new instance.
 */
public final class KafkaHttpServer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaHttpServer.class);

    private static final String CONTEXT_PATH = "/v1";
    // Servlet pattern is "/*" so the servlet receives the full sub-path (e.g. "/topics/orders/records") via
    // HttpServletRequest#getPathInfo(). KafkaHttpServlet.extractTopic() then parses the topic out of that pathInfo.
    // If we mounted the servlet at "/topics/*", Jetty would strip "/topics" before exposing pathInfo, breaking the
    // expected layout of /topics/{topic}/records that extractTopic enforces.
    private static final String SERVLET_PATTERN = "/*";
    // WebSocket upgrade mapping for /v1/topics/{topic}/subscribe. The path is context-relative — the WS filter
    // installed by JettyWebSocketServletContainerInitializer is scoped to this ServletContextHandler, so
    // /topics/{topic}/subscribe under the /v1 context resolves to /v1/topics/{topic}/subscribe on the wire.
    // The "uri-template|" prefix is load-bearing: WebSocketMappings.parsePathSpec routes any spec starting with
    // "/" through ServletPathSpec, which treats "{topic}" as a literal segment rather than a named capture. The
    // explicit prefix forces a UriTemplatePathSpec so "{topic}" actually matches a topic segment.
    private static final String WS_PATH_SPEC = "uri-template|/topics/{topic}/subscribe";

    private final String host;
    private final int port;
    private final KafkaHttpBridge bridge;
    private final RequestSubmitter submitter;
    private final ObjectMapper mapper;
    private final int maxRequestBodyBytes;
    private final SseStreamLimiter sseLimiter;
    private final WsStreamLimiter wsLimiter;
    private final HttpBridgeMetrics metrics;
    // Jetty stop timeout. >0 enables Graceful.shutdown — connectors stop accepting new connections, in-flight
    // requests get this long to drain, and only then are sockets forcibly closed. With 0 (the legacy behaviour
    // and the test-harness default) stop() returns near-instantly and SSE/WS clients see a TCP reset mid-stream
    // instead of an orderly close. Production wiring (BrokerServer) passes the configured value.
    private final long shutdownGraceMs;

    // volatile: written inside synchronized start()/stop(), read by boundPort() without holding the lock.
    // The synchronized writer publishes through the monitor, but unsynchronized readers (test threads and
    // anyone observing the boot state via the accessor below) need a happens-before edge of their own.
    // Without volatile the JMM permits the reader to observe -1 indefinitely on a weak memory architecture,
    // even after start() has returned. Same publication pattern as RequestChannel.requestCompletionCallback.
    private volatile Server server;
    private volatile int boundPort = -1;

    /**
     * Legacy 8-arg constructor preserved for tests that explicitly want a fast, ungraceful teardown
     * (no in-flight drain). New production call sites should pass {@code shutdownGraceMs} via the
     * 9-arg overload below.
     */
    public KafkaHttpServer(String host, int port, KafkaHttpBridge bridge, RequestSubmitter submitter,
                           ObjectMapper mapper, int maxRequestBodyBytes, int maxConcurrentSseStreams,
                           int maxConcurrentWsSubscriptions) {
        this(host, port, bridge, submitter, mapper, maxRequestBodyBytes,
            maxConcurrentSseStreams, maxConcurrentWsSubscriptions, 0L);
    }

    public KafkaHttpServer(String host, int port, KafkaHttpBridge bridge, RequestSubmitter submitter,
                           ObjectMapper mapper, int maxRequestBodyBytes, int maxConcurrentSseStreams,
                           int maxConcurrentWsSubscriptions, long shutdownGraceMs) {
        this.host = Objects.requireNonNull(host, "host must not be null");
        this.port = port;
        this.bridge = Objects.requireNonNull(bridge, "bridge must not be null");
        this.submitter = Objects.requireNonNull(submitter, "submitter must not be null");
        this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
        if (maxRequestBodyBytes < 0) {
            throw new IllegalArgumentException("maxRequestBodyBytes must be non-negative, got " + maxRequestBodyBytes);
        }
        if (shutdownGraceMs < 0L) {
            throw new IllegalArgumentException("shutdownGraceMs must be non-negative, got " + shutdownGraceMs);
        }
        this.maxRequestBodyBytes = maxRequestBodyBytes;
        this.shutdownGraceMs = shutdownGraceMs;
        this.sseLimiter = new SseStreamLimiter(maxConcurrentSseStreams);
        this.wsLimiter = new WsStreamLimiter(maxConcurrentWsSubscriptions);
        // Construct metrics here (not in start()) so the gauges for ActiveSseStreams / ActiveWsSubscriptions are
        // registered as soon as the server object exists and a JMX scrape against a stopped broker doesn't briefly
        // show "metric missing". The Yammer registry is global; stop() must call metrics.close() so the next
        // KafkaHttpServer constructed in the same JVM does not trip "duplicate metric name". (This instance itself
        // is one-shot — see class javadoc — so a same-instance start() after stop() is rejected, not re-registered.)
        this.metrics = new HttpBridgeMetrics(sseLimiter, wsLimiter);
    }

    public synchronized void start() throws Exception {
        if (server != null) {
            throw new IllegalStateException("server already started");
        }
        if (metrics.isClosed()) {
            // KafkaHttpServer is one-shot (see class javadoc): once stop() has run the metrics object is dead and a
            // re-start would silently drop every recording. Fail loudly so the misuse is visible at the boot path.
            throw new IllegalStateException("server has already been stopped; construct a new KafkaHttpServer to restart");
        }

        Server jetty = new Server();
        // Set BEFORE jetty.start() so Server.doStop() picks it up. With shutdownGraceMs > 0, Server.doStop
        // calls Graceful.shutdown(this) on every Graceful bean (the ServerConnector stops accepting new
        // connections, the ServletContextHandler refuses new requests) and waits up to this timeout for the
        // returned future to complete before forcing connectors closed. SSE/WS clients with no other escape
        // from the long-poll thereby observe an orderly TCP close (FIN) rather than a reset (RST) when the
        // broker restarts. shutdownGraceMs == 0 keeps the legacy ungraceful behaviour, which the test harness
        // relies on for fast teardown.
        jetty.setStopTimeout(shutdownGraceMs);
        // Suppress the "Server: Jetty(<version>)" and "X-Powered-By" response headers. Jetty's HttpConfiguration
        // defaults emit the version on every response, which violates the bridge's no-fingerprint contract — the
        // same contract the Server-level CoreJsonErrorHandler below was added to enforce for error bodies. Sealing
        // the body without sealing the header would leave the leak fully open via a trivial HEAD on /v1.
        HttpConfiguration httpConfig = new HttpConfiguration();
        httpConfig.setSendServerVersion(false);
        httpConfig.setSendXPoweredBy(false);
        ServerConnector connector = new ServerConnector(jetty, new HttpConnectionFactory(httpConfig));
        connector.setHost(host);
        connector.setPort(port);
        // Pin the connector idle timeout explicitly instead of relying on Jetty's default (30s today). The idle timeout
        // bounds how long a TCP connection sits with no I/O before Jetty closes it — important for two reasons:
        // (1) it backstops the CONNECT/keep-alive smuggling defence (a malicious or buggy client that opens a socket
        // and never sends a request would otherwise pin an FD for the whole default window), and (2) it interacts with
        // SSE's 500 ms heartbeat — the heartbeat must beat the connector timeout, so pinning the connector value here
        // documents the contract instead of leaving it depending on whichever default Jetty 12.x ships.
        // 30 s is comfortable for SSE (heartbeats fire every quiet-fetch iteration, well inside this window) and
        // tight enough that an idle-attack waste of FDs is bounded. WS upgrades replace this timeout with the
        // per-session idle (see KafkaWebSocketEndpoint.IDLE_TIMEOUT / PRE_SUBSCRIBE_IDLE_TIMEOUT), so this value does
        // not affect long-running subscriptions.
        connector.setIdleTimeout(30_000L);
        jetty.addConnector(connector);

        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath(CONTEXT_PATH);
        // Render any container-level sendError(code, msg) as the bridge's {errorCode, errorMessage} JSON envelope —
        // matching the shape KafkaHttpServlet.writeEnvelope produces. The load-bearing case is the WebSocket upgrade
        // path below: JettyServerUpgradeResponse.sendError delegates to HttpServletResponse.sendError, which without
        // this handler renders Jetty's stock HTML error page. A client following the bridge's documented contract
        // ({errorCode, errorMessage} on every error) would have to special-case WS upgrade rejections — and a strict
        // JSON parser would crash on the HTML. The handler also covers any future code path that calls sendError
        // through the servlet context, so the envelope stays consistent without per-call-site discipline.
        context.setErrorHandler(new JsonErrorHandler(mapper));
        // jetty.getThreadPool() returns the bound ThreadPool (Executor) — pass it to the servlet so async-completion
        // writes happen on Jetty I/O threads rather than the broker request-handler thread that completes the
        // submitter future. Without this, a slow HTTP client can pin a broker handler thread on a socket write.
        java.util.concurrent.Executor httpExecutor = jetty.getThreadPool();
        ServletHolder holder = new ServletHolder(
            new KafkaHttpServlet(bridge, submitter, mapper, maxRequestBodyBytes, sseLimiter, httpExecutor, metrics));
        holder.setAsyncSupported(true);
        context.addServlet(holder, SERVLET_PATTERN);

        // Install the Jetty 12 native WebSocket container alongside the servlet. The container initializer
        // registers a filter ahead of the servlet that intercepts upgrade requests against
        // /v1/topics/{topic}/subscribe and routes them to KafkaWebSocketEndpoint via the creator below.
        // Non-upgrade requests against the same path fall through to the servlet, which returns 404 (the
        // servlet's extractTopic enforces /topics/{topic}/records — /subscribe does not match).
        JettyWebSocketServletContainerInitializer.configure(context, (servletContext, container) -> {
            // Upgrade-time admission gate: extract the topic, acquire a limiter slot, and either return a
            // freshly-constructed endpoint (counts as one accepted subscription) or send a 429 (counts as
            // a cap rejection). The endpoint owns the token from that point onward; cleanup in onClose/onError
            // is idempotent.
            container.addMapping(WS_PATH_SPEC, (req, resp) -> {
                // Strip any negotiated WebSocket extensions before the upgrade response is sent. Jetty 12 registers
                // `permessage-deflate` in the default ExtensionRegistry and negotiates it whenever a client offers
                // `Sec-WebSocket-Extensions: permessage-deflate` — even when the application code is unaware. The
                // 8 KiB inbound caps in KafkaWebSocketEndpoint apply to the DECOMPRESSED message, so heap pressure
                // is bounded, but the bridge has no protocol need for compression (frames are tiny JSON records,
                // not bulk payloads) and silently negotiating it: (a) adds a per-session zlib state machine that
                // isn't required by the wire spec we publish, (b) creates a CPU-cost surface where a small
                // compressed frame expands into more decompression work, (c) makes the negotiated handshake
                // depend on which Jetty patch version ships which extensions by default, which is exactly the
                // kind of implementation drift Wave 23 axis G called out. Pin the contract: no extensions.
                resp.setExtensions(java.util.Collections.emptyList());

                String topic = extractSubscribeTopic(req.getRequestPath(), context.getContextPath());
                if (topic == null) {
                    // The path-spec was already matched by the WS filter, so this branch should be unreachable
                    // in practice — but defending against future spec changes (e.g. trailing slashes) by
                    // returning a sane error is cheap insurance.
                    // sendError → HttpServletResponse.sendError → JsonErrorHandler renders the bridge's
                    // {errorCode, errorMessage} envelope. See setErrorHandler() above.
                    resp.sendError(404, "topic path did not match /v1/topics/{topic}/subscribe");
                    return null;
                }
                WsStreamLimiter.Token token = wsLimiter.tryAcquire();
                if (token == null) {
                    metrics.recordWsCapRejection();
                    // PROMPT.md AC: "429 is reserved for the WebSocket pre-flight throttle, not the HTTP
                    // produce path." Refusing the upgrade at the concurrency cap is exactly that pre-flight
                    // throttle, so the contract is 429 + Retry-After — matching the SSE limiter's shape so
                    // both streaming admission gates report the same surface to clients and dashboards.
                    // Retry-After must be set BEFORE sendError — sendError commits the response headers when
                    // the configured ErrorHandler runs, and headers added after commit are dropped.
                    resp.setHeader("Retry-After", "5");
                    resp.sendError(HttpStatusMapper.TOO_MANY_REQUESTS,
                        "WebSocket subscription cap reached; try again shortly");
                    return null;
                }
                // Wrap endpoint construction so a throw between tryAcquire() and the returned endpoint does not
                // leak the limiter slot. The token is meant to transfer to the endpoint (and from there to the
                // streamer); if construction fails we must release it before propagating the failure as 500.
                // Also: record the "opened" meter only after successful construction so accepted+rejected meters
                // sum to exactly the offered load — a half-constructed endpoint that never reaches the client
                // is neither.
                KafkaWebSocketEndpoint endpoint;
                try {
                    endpoint = new KafkaWebSocketEndpoint(topic, submitter, mapper, token, httpExecutor);
                } catch (RuntimeException e) {
                    token.close();
                    throw e;
                }
                metrics.recordWsSubscriptionOpened();
                return endpoint;
            });
        });

        // Server-level (core) handler for errors that escape the servlet context entirely:
        //   - URI rejections by Jetty's HTTP parser (CRLF in headers, ambiguous %2F, path traversal, control chars)
        //   - Requests to paths outside the /v1 context (no ServletContextHandler matched)
        //   - Bad-message errors raised before request dispatch
        // The context-level JsonErrorHandler above only fires for errors dispatched *inside* the servlet context
        // (servlet sendError, WS upgrade rejection). Without this Server-level handler, those earlier rejections
        // emit Jetty's stock HTML error page — including the "Powered by Jetty <version>" footer — which violates
        // PROMPT.md "every error response includes errorCode and errorMessage fields" and leaks the server fingerprint.
        jetty.setErrorHandler(new CoreJsonErrorHandler(mapper));
        // Pre-dispatch CONNECT guard. Jetty 12.0.25's HttpConnection.HttpStreamOverHTTP1.headerComplete()
        // hardcodes "method == CONNECT" as always persistent (overriding even Connection: close from the
        // client). Without an explicit guard, a CONNECT request to the bridge falls through to the 404
        // Server-level handler but leaves the TCP socket open for the full ServerConnector idle timeout
        // (30s default). An attacker firing N CONNECTs pins N file descriptors. Intercept CONNECT before
        // dispatch, emit the bridge's 405 envelope, and force-close the connection.
        Handler.Wrapper connectGuard = new ConnectMethodGuard(mapper, context);
        jetty.setHandler(connectGuard);
        jetty.start();

        this.server = jetty;
        this.boundPort = connector.getLocalPort();
        LOG.info("Kafka HTTP bridge listening on {}:{}", host, boundPort);
    }

    public synchronized void stop() throws Exception {
        try {
            if (server != null) {
                try {
                    server.stop();
                } catch (TimeoutException e) {
                    // Jetty 12's Server.doStop runs Graceful.shutdown(this).get(stopTimeout, MS) BEFORE
                    // it forcibly stops connectors and the handler tree. The throwable from get() is
                    // accumulated and rethrown at the end of doStop — but by then connectors are already
                    // down. A TimeoutException here therefore means "a stream did not drain inside the
                    // configured graceMs window and was force-closed", which is the documented contract
                    // of http.bridge.shutdown.grace.ms, not a shutdown failure. Downgrade to a warning so
                    // the broker's stop sequence can continue past the bridge.
                    LOG.warn("HTTP bridge did not drain all in-flight streams within {}ms; force-closed",
                        shutdownGraceMs);
                }
            }
        } finally {
            server = null;
            boundPort = -1;
            // Unregister metrics unconditionally — they are constructed in the KafkaHttpServer constructor, so a
            // never-started or start-failed server still has live entries in the global Yammer registry. Closing
            // only inside the `server != null` branch would strand those entries forever and break a subsequent
            // KafkaHttpServer in the same JVM (test harness or BrokerServer restart) with "duplicate metric name".
            // close() is idempotent so repeated stop() calls are safe.
            metrics.close();
        }
    }

    public int boundPort() {
        return boundPort;
    }

    /**
     * Pull the topic out of a WebSocket upgrade request path of the form {@code <contextPath>/topics/{topic}/subscribe}.
     * Returns {@code null} if the shape doesn't match — the caller emits 404 in that case rather than guessing. The
     * Jetty WS filter already path-matched against {@link #WS_PATH_SPEC}, so this is defence-in-depth, not the primary
     * validation.
     *
     * <p>The topic name is also checked against {@link Topic#isValid}. See {@link KafkaHttpServlet#extractTopic} for
     * the full reasoning — same defence: rejecting decoded ASCII control bytes here keeps {@code %0A}-bearing topic
     * names out of every WS-side {@code LOG.warn("…for {}", topic, …)} site (log-forging primitive) and removes the
     * bridge's silent dependency on broker-side topic-name validation.
     */
    static String extractSubscribeTopic(String requestPath, String contextPath) {
        if (requestPath == null) {
            return null;
        }
        String inner = requestPath;
        if (contextPath != null && !contextPath.isEmpty() && inner.startsWith(contextPath)) {
            inner = inner.substring(contextPath.length());
        }
        final String prefix = "/topics/";
        final String suffix = "/subscribe";
        if (!inner.startsWith(prefix) || !inner.endsWith(suffix)) {
            return null;
        }
        String topic = inner.substring(prefix.length(), inner.length() - suffix.length());
        if (topic.isEmpty() || topic.indexOf('/') >= 0 || !Topic.isValid(topic)) {
            return null;
        }
        return topic;
    }

    /**
     * ErrorHandler that emits the bridge's canonical {@code {errorCode, errorMessage}} JSON envelope for any
     * {@code sendError(code, message)} that reaches the servlet context. Mirrors
     * {@link KafkaHttpServlet}'s {@code writeEnvelope} so a client that consumes the bridge's HTTP API sees a single
     * error shape regardless of whether the failure originated in the servlet, the WebSocket upgrade gate, or a
     * future code path that hits the container's error path.
     *
     * <p>The handler overrides {@code generateAcceptableResponse} rather than the public {@code handle} so it picks
     * up any future Jetty plumbing improvements around error dispatch (forwarding, error-page semantics) for free.
     */
    static final class JsonErrorHandler extends ErrorHandler {

        private final ObjectMapper mapper;

        JsonErrorHandler(ObjectMapper mapper) {
            this.mapper = mapper;
        }

        /**
         * Mirrors the override on {@link CoreJsonErrorHandler}: the parent {@code ErrorHandler.handle} short-circuits
         * to {@code callback.succeeded()} with no body when {@code errorPageForMethod} returns false, and the default
         * implementation only returns true for {@code {GET, POST, HEAD}}. A lowercase method like {@code get} is
         * rejected by {@code HttpServlet.service} (RFC 9110: method names are case-sensitive) which then calls
         * {@code sendError(501)} — that dispatch routes through this servlet-context handler. Without the override,
         * the parent would skip {@link #generateAcceptableResponse} and emit an empty body, breaking the
         * "every error response includes errorCode and errorMessage" contract for any non-{GET,POST,HEAD} method.
         */
        @Override
        public boolean errorPageForMethod(String method) {
            return true;
        }

        @Override
        protected void generateAcceptableResponse(ServletContextRequest request,
                                                  HttpServletRequest req,
                                                  HttpServletResponse resp,
                                                  int code,
                                                  String message) throws IOException {
            // Defensive: if some upstream filter already committed the response, all we can do is stop. Writing
            // again would corrupt the stream (HTTP/1.1) or throw (HTTP/2). The status code is preserved.
            if (resp.isCommitted()) {
                return;
            }
            resp.setStatus(code);
            resp.setContentType(ContentTypeNegotiator.APPLICATION_JSON);
            byte[] payload = mapper.writeValueAsBytes(ErrorEnvelope.forMessage(mapper, code, message));
            resp.setContentLength(payload.length);
            resp.getOutputStream().write(payload);
        }
    }

    /**
     * Pre-dispatch guard for HTTP CONNECT. Jetty 12.0.25's {@code HttpStreamOverHTTP1.headerComplete()}
     * unconditionally marks the connection persistent when the request method is CONNECT — overriding even
     * {@code Connection: close} from the client. The bridge does not implement any tunnel, so a CONNECT
     * request falls through to the 404 path, but the TCP socket is then held alive for the
     * {@code ServerConnector} idle timeout (30s default). An attacker firing CONNECT to the bridge can pin
     * file descriptors at attack-stream rate × 30s.
     *
     * <p>This wrapper intercepts CONNECT before any handler dispatch, emits the bridge's canonical
     * {@code {errorCode, errorMessage}} envelope with status 405, and adds {@code Connection: close} so
     * the underlying HTTP stream closes the socket immediately after the response is flushed. Verified
     * against Jetty 12.0.25 by raw-socket probe: with the guard installed the response carries
     * {@code Connection: close} and the server FIN closes the socket inside one request lifetime.
     */
    static final class ConnectMethodGuard extends Handler.Wrapper {

        private final ObjectMapper mapper;

        ConnectMethodGuard(ObjectMapper mapper, Handler next) {
            super(next);
            this.mapper = mapper;
        }

        @Override
        public boolean handle(Request request, Response response, Callback callback) throws Exception {
            if (HttpMethod.CONNECT.is(request.getMethod())) {
                response.setStatus(HttpStatusMapper.METHOD_NOT_ALLOWED);
                HttpFields.Mutable headers = response.getHeaders();
                headers.put(HttpHeader.CONTENT_TYPE, ContentTypeNegotiator.APPLICATION_JSON);
                // RFC 9110: a 405 response MUST generate an Allow header listing the methods that are allowed.
                headers.put(HttpHeader.ALLOW, "GET, HEAD, OPTIONS, POST");
                // Force-close the connection. The Jetty HTTP/1 generator special-cases CONNECT as always
                // persistent unless the response explicitly carries Connection: close; setting it here is the
                // only knob that actually causes the socket to be closed after the response is written.
                headers.put(HttpHeader.CONNECTION, HttpHeaderValue.CLOSE.asString());
                byte[] payload = mapper.writeValueAsBytes(
                    ErrorEnvelope.forMessage(mapper, HttpStatusMapper.METHOD_NOT_ALLOWED,
                        "method not allowed"));
                headers.put(HttpHeader.CONTENT_LENGTH, Integer.toString(payload.length));
                response.write(true, ByteBuffer.wrap(payload), callback);
                return true;
            }
            return super.handle(request, response, callback);
        }
    }

    /**
     * Server-level error handler that emits the {@code {errorCode, errorMessage}} JSON envelope for failures that
     * never reach the servlet context — URI parser rejections (CRLF, {@code %2F}, control chars), bad-message errors,
     * and requests to paths outside {@code /v1} that no {@link ServletContextHandler} matched.
     *
     * <p>This complements {@link JsonErrorHandler} above: the servlet handler covers in-context dispatches
     * ({@code sendError} from the servlet or the WS upgrade gate), and this one covers everything that fails before
     * dispatch begins. Without the Server-level handler, those earlier rejections fall through to Jetty's default
     * error renderer — which emits an HTML body that includes the Jetty version string, simultaneously breaking the
     * bridge's documented error shape and leaking the server fingerprint.
     *
     * <p>Override target is {@code generateResponse} (not {@code generateAcceptableResponse}) so we bypass Jetty's
     * built-in content negotiation entirely: every error response is JSON regardless of the client's {@code Accept}
     * header. A client that asks for {@code text/html} on an SSE URL still gets the JSON envelope, which is the only
     * shape the bridge documents.
     */
    static final class CoreJsonErrorHandler extends org.eclipse.jetty.server.handler.ErrorHandler {

        private final ObjectMapper mapper;

        CoreJsonErrorHandler(ObjectMapper mapper) {
            this.mapper = mapper;
            // Defence in depth: even if a future change accidentally routes through generateAcceptableResponse,
            // these flags keep the rendered output free of Jetty's stack traces, exception chain, and message-in-title
            // markup. The bridge's envelope is the only thing a client should ever see.
            setShowStacks(false);
            setShowCauses(false);
            setShowMessageInTitle(false);
            setDefaultResponseMimeType(ContentTypeNegotiator.APPLICATION_JSON);
        }

        /**
         * Jetty's parent {@code ErrorHandler.handle} consults {@code errorPageForMethod} before calling
         * {@code generateResponse}; the default implementation returns true only for {@code {GET, POST, HEAD}} via a
         * hardcoded {@code ERROR_METHODS} set, and any other method (PUT/DELETE/PATCH/OPTIONS/TRACE/CONNECT) is
         * short-circuited to {@code callback.succeeded()} with no body. A {@code PUT /v1/topics/foo%2Fbar} that
         * Jetty's URI compliance rejects pre-dispatch would emit an empty body, breaking the bridge's
         * "every error response includes errorCode and errorMessage" contract for any non-{GET,POST,HEAD} caller.
         * Returning true for every method routes all pre-dispatch rejections through {@link #generateResponse} below.
         */
        @Override
        public boolean errorPageForMethod(String method) {
            return true;
        }

        @Override
        protected void generateResponse(Request request, Response response, int code, String message,
                                        Throwable cause, Callback callback) throws IOException {
            // Mirror the servlet-side guard: if the response is already committed there's nothing useful we can do
            // (writing again would corrupt HTTP/1.1 framing or throw on HTTP/2). The status code is preserved.
            if (response.isCommitted()) {
                callback.succeeded();
                return;
            }
            response.setStatus(code);
            response.getHeaders().put(HttpHeader.CONTENT_TYPE, ContentTypeNegotiator.APPLICATION_JSON);
            byte[] payload = mapper.writeValueAsBytes(ErrorEnvelope.forMessage(mapper, code, message));
            response.write(true, ByteBuffer.wrap(payload), callback);
        }
    }
}
