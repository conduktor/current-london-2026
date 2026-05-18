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

import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

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

    private final String host;
    private final int port;
    private final KafkaHttpBridge bridge;
    private final RequestSubmitter submitter;
    private final ObjectMapper mapper;
    private final int maxRequestBodyBytes;
    private final SseStreamLimiter sseLimiter;
    private final HttpBridgeMetrics metrics;

    private Server server;
    private int boundPort = -1;

    public KafkaHttpServer(String host, int port, KafkaHttpBridge bridge, RequestSubmitter submitter,
                           ObjectMapper mapper, int maxRequestBodyBytes, int maxConcurrentSseStreams) {
        this.host = Objects.requireNonNull(host, "host must not be null");
        this.port = port;
        this.bridge = Objects.requireNonNull(bridge, "bridge must not be null");
        this.submitter = Objects.requireNonNull(submitter, "submitter must not be null");
        this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
        if (maxRequestBodyBytes < 0) {
            throw new IllegalArgumentException("maxRequestBodyBytes must be non-negative, got " + maxRequestBodyBytes);
        }
        this.maxRequestBodyBytes = maxRequestBodyBytes;
        this.sseLimiter = new SseStreamLimiter(maxConcurrentSseStreams);
        // Construct metrics here (not in start()) so the gauge for ActiveSseStreams is registered as soon as the
        // server object exists and a JMX scrape against a stopped broker doesn't briefly show "metric missing".
        // The Yammer registry is global; stop() must call metrics.close() so the next KafkaHttpServer constructed
        // in the same JVM does not trip "duplicate metric name". (This instance itself is one-shot — see class
        // javadoc — so a same-instance start() after stop() is rejected, not re-registered.)
        this.metrics = new HttpBridgeMetrics(sseLimiter);
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
        ServerConnector connector = new ServerConnector(jetty);
        connector.setHost(host);
        connector.setPort(port);
        jetty.addConnector(connector);

        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath(CONTEXT_PATH);
        // jetty.getThreadPool() returns the bound ThreadPool (Executor) — pass it to the servlet so async-completion
        // writes happen on Jetty I/O threads rather than the broker request-handler thread that completes the
        // submitter future. Without this, a slow HTTP client can pin a broker handler thread on a socket write.
        java.util.concurrent.Executor httpExecutor = jetty.getThreadPool();
        ServletHolder holder = new ServletHolder(
            new KafkaHttpServlet(bridge, submitter, mapper, maxRequestBodyBytes, sseLimiter, httpExecutor, metrics));
        holder.setAsyncSupported(true);
        context.addServlet(holder, SERVLET_PATTERN);

        jetty.setHandler(context);
        jetty.start();

        this.server = jetty;
        this.boundPort = connector.getLocalPort();
        LOG.info("Kafka HTTP bridge listening on {}:{}", host, boundPort);
    }

    public synchronized void stop() throws Exception {
        try {
            if (server != null) {
                server.stop();
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
}
