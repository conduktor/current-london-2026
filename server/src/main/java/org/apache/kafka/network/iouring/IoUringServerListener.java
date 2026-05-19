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
package org.apache.kafka.network.iouring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.unix.UnixChannelOption;
import io.netty.channel.uring.IoUringIoHandler;
import io.netty.channel.uring.IoUringServerSocketChannel;
import io.netty.util.ReferenceCountUtil;

/**
 * Binds an {@code IoUringServerSocketChannel} on the listener address and wires every
 * accepted child channel into an {@link IoUringSelector}.
 *
 * <h3>What this class owns</h3>
 * <ul>
 *   <li>A single-thread Netty {@code MultiThreadIoEventLoopGroup} backed by
 *       {@code IoUringIoHandler} — the kernel-side ring driver.</li>
 *   <li>The bound {@link IoUringServerSocketChannel} on the listener port. The
 *       {@link UnixChannelOption#SO_REUSEPORT} flag is set so multiple per-Processor
 *       listeners can share the same operator-configured port — the kernel does the
 *       accept-sharding.</li>
 *   <li>A child-channel pipeline that pushes every event back into the supplied
 *       {@link IoUringSelector} via its four package-private hooks.</li>
 * </ul>
 *
 * <h3>What this class does NOT own</h3>
 * The {@link IoUringSelector} — the Processor that owns the selector also owns this
 * listener. {@link #close()} closes the server socket and shuts the event loop down, but
 * does <em>not</em> close the selector; that lifecycle stays with the Processor.
 *
 * <h3>Lifecycle</h3>
 * Construction is cheap and does <strong>not</strong> open a kernel socket — it only
 * validates inputs, creates the io_uring event-loop group, and stores the bootstrap
 * configuration. The actual {@code bind(2)} happens in {@link #start()}.
 *
 * <p>Splitting construction from bind matches NIO's deferred-open contract
 * ({@code Acceptor.start()} opens its {@code ServerSocketChannel}, the constructor
 * does not — see {@code SocketServer.scala:508-520}). Kafka starts acceptors before
 * authorizer initialization finishes; without this split, the io_uring listener would
 * accept TCP connections in the Processor constructor — i.e. before
 * {@code SocketServer.enableRequestProcessing} resolves and before the broker is
 * actually ready to handle requests. External tooling that probes the listener port
 * as a readiness signal would observe a misleading "ready" before request processing
 * is enabled or after authorizer startup fails.
 *
 * <h3>Thread safety</h3>
 * {@link #start()} binds synchronously and may block briefly; it is not safe to call
 * concurrently with itself but is safe to call once per instance. {@link #close()} is
 * idempotent and safe to call from any thread; subsequent calls return immediately.
 * The internal Netty handler runs entirely on the event-loop thread and is the only
 * thread that touches the selector's event-loop callbacks.
 */
public final class IoUringServerListener implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(IoUringServerListener.class);

    /** Netty's quiet-period for shutdownGracefully — kept short so broker shutdown isn't slow. */
    private static final long SHUTDOWN_QUIET_MS = 100;
    /** Netty's overall shutdown deadline. */
    private static final long SHUTDOWN_TIMEOUT_MS = 5_000;

    private final InetSocketAddress bindAddress;
    private final IoUringSelector selector;
    private final int soBacklog;
    private final int sendBufferSize;
    private final int receiveBufferSize;
    private final EventLoopGroup eventLoopGroup;

    private volatile Channel serverChannel;
    private volatile int boundPort = -1;
    private volatile boolean started;
    private volatile boolean closed;

    /** Sentinel meaning "leave the OS default in place" — matches {@code Selectable.USE_DEFAULT_BUFFER_SIZE}. */
    public static final int USE_DEFAULT_BUFFER_SIZE = -1;

    public IoUringServerListener(InetSocketAddress bindAddress, IoUringSelector selector) {
        this(bindAddress, selector, /*soBacklog*/ 128, USE_DEFAULT_BUFFER_SIZE, USE_DEFAULT_BUFFER_SIZE);
    }

    public IoUringServerListener(InetSocketAddress bindAddress, IoUringSelector selector, int soBacklog) {
        this(bindAddress, selector, soBacklog, USE_DEFAULT_BUFFER_SIZE, USE_DEFAULT_BUFFER_SIZE);
    }

    /**
     * @param sendBufferSize    {@code SO_SNDBUF} applied to every accepted child channel, or
     *                          {@link #USE_DEFAULT_BUFFER_SIZE} to leave the OS default in place
     *                          ({@code socket.send.buffer.bytes} on the NIO path).
     * @param receiveBufferSize {@code SO_RCVBUF} applied to the listening socket so newly accepted
     *                          children inherit it via standard TCP semantics, or
     *                          {@link #USE_DEFAULT_BUFFER_SIZE} for the OS default
     *                          ({@code socket.receive.buffer.bytes} on the NIO path).
     */
    public IoUringServerListener(InetSocketAddress bindAddress,
                                 IoUringSelector selector,
                                 int soBacklog,
                                 int sendBufferSize,
                                 int receiveBufferSize) {
        this.bindAddress = Objects.requireNonNull(bindAddress, "bindAddress");
        this.selector = Objects.requireNonNull(selector, "selector");
        this.soBacklog = soBacklog;
        this.sendBufferSize = sendBufferSize;
        this.receiveBufferSize = receiveBufferSize;
        if (!IoUringSupport.isAvailable()) {
            throw new IllegalStateException(
                "io_uring is not available on this host: " + IoUringSupport.unavailabilityReason());
        }
        // The event-loop group is created up front so io_uring availability is validated at
        // construction time (rather than deferred to start()). Binding the LISTEN socket — the
        // operation that makes the broker visible on the network — is what start() handles.
        this.eventLoopGroup = new MultiThreadIoEventLoopGroup(1, IoUringIoHandler.newFactory());
    }

    /**
     * Open the LISTEN socket on {@code bindAddress} and start accepting connections. Must be
     * called exactly once per instance, after construction. See the class Javadoc on why
     * binding is deferred from the constructor.
     */
    public void start() {
        if (closed) {
            throw new IllegalStateException("io_uring listener was closed before start()");
        }
        if (started) {
            throw new IllegalStateException("io_uring listener was already started");
        }
        try {
            // Mirror NIO Acceptor.configureAcceptedSocketChannel (SocketServer.scala:740-746):
            // every accepted broker connection gets TCP_NODELAY=true and SO_KEEPALIVE=true.
            // TCP_NODELAY disables Nagle's algorithm — critical for Kafka request/response
            // latency since requests are batched at the producer/consumer layer and Nagle
            // would add hundreds of microseconds on every flush. SO_KEEPALIVE detects half-open
            // connections so the broker doesn't keep a dead peer slot indefinitely.
            // SO_SNDBUF is applied per-child when the operator overrides the default; SO_RCVBUF
            // is applied to the LISTEN socket (children inherit via TCP) matching how NIO sets
            // it on serverSocket.openServerSocket(..., recvBufferSize) at SocketServer.scala:667.
            ServerBootstrap bootstrap = new ServerBootstrap()
                .group(eventLoopGroup)
                .channel(IoUringServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, soBacklog)
                .option(UnixChannelOption.SO_REUSEPORT, true)
                .childOption(ChannelOption.AUTO_READ, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ch.pipeline().addLast(new ChildHandler(selector));
                    }
                });
            if (sendBufferSize != USE_DEFAULT_BUFFER_SIZE) {
                bootstrap.childOption(ChannelOption.SO_SNDBUF, sendBufferSize);
            }
            if (receiveBufferSize != USE_DEFAULT_BUFFER_SIZE) {
                // SO_RCVBUF on the LISTEN socket: TCP semantics propagate it to every accepted
                // child at the moment of accept. Setting it per-child would be ineffective
                // because the OS sizes the receive buffer at accept time, not afterwards.
                bootstrap.option(ChannelOption.SO_RCVBUF, receiveBufferSize);
            }

            ChannelFuture future = bootstrap.bind(bindAddress).sync();
            this.serverChannel = future.channel();
            // Port extraction is the last step that can fail post-bind. If localAddress() ever
            // returns null (channel torn down between bind() and getPort()) or the cast trips a
            // ClassCastException on a non-Inet address, the catch (RuntimeException) below must
            // tear down the bound LISTEN socket — otherwise the port stays in LISTEN until the
            // event loop is GC'd and external readiness probes see a phantom "ready" broker.
            this.boundPort = ((InetSocketAddress) serverChannel.localAddress()).getPort();
            // started=true must be the last line of the happy path. If anything above this throws,
            // we run the partial-failure cleanup in the catch blocks instead of leaving the
            // listener half-initialized.
            this.started = true;
            log.info("io_uring listener bound to {} (port {}, sendBufferSize={}, receiveBufferSize={})",
                bindAddress, boundPort, sendBufferSize, receiveBufferSize);
        } catch (InterruptedException ie) {
            cleanupAfterFailedStart();
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while binding io_uring listener", ie);
        } catch (Throwable t) {
            // Catch Throwable rather than RuntimeException: Netty's ChannelFuture.sync() uses
            // PlatformDependent.throwException to sneakily rethrow the failure cause without
            // declaring it. A bind failure surfaces as a checked java.net.BindException at
            // runtime — slipping past `catch (RuntimeException)` and skipping
            // cleanupAfterFailedStart entirely. Pre-fix, every failed-bind leaked the io_uring
            // event-loop group and ring file descriptor for the JVM's lifetime. Other Errors
            // (NoClassDefFoundError from a missing native, LinkageError mid-init) follow the
            // same path. VirtualMachineError is re-thrown to preserve JVM crash-fast semantics.
            cleanupAfterFailedStart();
            if (t instanceof VirtualMachineError) {
                throw (VirtualMachineError) t;
            }
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            }
            if (t instanceof Error) {
                throw (Error) t;
            }
            // Checked exception (e.g. BindException sneakily thrown by sync()): wrap and rethrow
            // so the caller sees a meaningful failure, not just a generic Throwable.
            throw new IllegalStateException("Failed to bind io_uring listener to " + bindAddress, t);
        }
    }

    /**
     * Releases all resources held by a failed {@link #start()} attempt: the bound LISTEN socket
     * (if {@code bind()} succeeded but a later step threw), and the io_uring event-loop group.
     * Sets {@code closed=true} so a subsequent {@link #close()} is a no-op and a re-{@code start()}
     * fails with "was closed before start()" instead of silently re-using a torn-down event loop.
     *
     * <p>The event-loop shutdown is awaited synchronously (matching {@link #close()}). Without the
     * await, {@code shutdownGracefully} runs asynchronously and {@code start()} returns while the
     * io_uring ring file descriptor and the event-loop thread are still alive — that breaks tight
     * rebind paths (a follow-up Acceptor.start() racing the previous loop's tear-down) and lets a
     * thread plus ring fd outlive the constructor on what is supposed to be a failed-cleanup
     * return. Bound the wait the same way close() does: SHUTDOWN_QUIET_MS + SHUTDOWN_TIMEOUT_MS,
     * then proceed regardless so a wedged loop cannot block the start() caller indefinitely.
     */
    private void cleanupAfterFailedStart() {
        if (serverChannel != null) {
            try {
                // Use awaitUninterruptibly with a tight bound — we're already on an error path
                // and must not block start() indefinitely if the event loop is wedged.
                serverChannel.close().awaitUninterruptibly(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (Exception suppress) {
                log.debug("error closing io_uring server channel during failed-start cleanup", suppress);
            }
        }
        long shutdownBudgetMs = SHUTDOWN_QUIET_MS + SHUTDOWN_TIMEOUT_MS;
        boolean shutdownInTime = eventLoopGroup
            .shutdownGracefully(SHUTDOWN_QUIET_MS, SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)
            .awaitUninterruptibly(shutdownBudgetMs, TimeUnit.MILLISECONDS);
        if (!shutdownInTime) {
            log.warn("io_uring event-loop group did not shut down within {}ms during failed-start " +
                "cleanup; leaving it detached so the caller's start() throw isn't blocked", shutdownBudgetMs);
        }
        closed = true;
    }

    /**
     * Returns the actually bound port. Caller must have invoked {@link #start()} first;
     * before start the listener has no kernel socket and no port.
     */
    public int boundPort() {
        if (!started) {
            throw new IllegalStateException("io_uring listener has not been started; call start() first");
        }
        return boundPort;
    }

    /** The selector that channels are wired to. Returned for symmetry; the caller already has it. */
    public IoUringSelector selector() {
        return selector;
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        // If start() never ran (broker shutdown between construct and start), there's no
        // serverChannel to close — just shut the event-loop group down so the io_uring ring
        // file descriptors are released.
        if (serverChannel != null) {
            try {
                // Bound the close() with awaitUninterruptibly(timeout). sync() blocks forever
                // if the event loop is wedged (deadlocked native code, runaway handler) and
                // that turns into an unkillable broker shutdown — operators must SIGKILL.
                // shutdownGracefully below already enforces its own bound, so the listener
                // shutdown path now has a definite upper time bound regardless of loop health.
                boolean closedInTime = serverChannel.close()
                    .awaitUninterruptibly(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                if (!closedInTime) {
                    log.warn("io_uring server channel did not close within {}ms; forcing event-loop shutdown",
                        SHUTDOWN_TIMEOUT_MS);
                }
            } catch (Exception e) {
                log.debug("error closing io_uring server channel", e);
            }
        }
        // C-18-L1: bound the event-loop shutdown wait the same way serverChannel.close() is
        // bounded above. shutdownGracefully's own Future respects the SHUTDOWN_TIMEOUT_MS
        // deadline, but syncUninterruptibly() does NOT — if the io_uring event-loop thread
        // is wedged in a kernel syscall (a known io_uring failure mode under low-memory
        // submission-queue pressure), syncUninterruptibly blocks forever, defeating the
        // explicit timeout passed to shutdownGracefully. awaitUninterruptibly(timeout)
        // gives the loop the same wall-clock budget — twice the shutdownGracefully
        // deadline as headroom for the quiet period — then proceeds regardless so a wedged
        // event loop does not turn into an unkillable broker shutdown.
        long shutdownBudgetMs = SHUTDOWN_QUIET_MS + SHUTDOWN_TIMEOUT_MS;
        boolean shutdownInTime = eventLoopGroup
            .shutdownGracefully(SHUTDOWN_QUIET_MS, SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)
            .awaitUninterruptibly(shutdownBudgetMs, TimeUnit.MILLISECONDS);
        if (!shutdownInTime) {
            log.warn("io_uring event-loop group did not shut down within {}ms; leaving it detached " +
                "to avoid blocking the broker shutdown thread on a wedged event loop", shutdownBudgetMs);
        }
    }

    /**
     * The per-child-channel Netty handler. One instance is added to every accepted
     * channel's pipeline and forwards each event-loop event to the corresponding
     * package-private hook on {@link IoUringSelector}.
     */
    private static final class ChildHandler extends ChannelInboundHandlerAdapter {

        private final IoUringSelector selector;

        ChildHandler(IoUringSelector selector) {
            this.selector = selector;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            InetSocketAddress remote = (InetSocketAddress) ctx.channel().remoteAddress();
            InetSocketAddress local = (InetSocketAddress) ctx.channel().localAddress();
            selector.onAccept(ctx.channel(), remote, local);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof ByteBuf) {
                selector.onRead(ctx.channel(), (ByteBuf) msg);
            } else {
                // io_uring always delivers ByteBuf for inbound bytes; anything else is unexpected.
                ReferenceCountUtil.release(msg);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            selector.onDisconnect(ctx.channel());
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) {
            selector.onWritabilityChanged(ctx.channel());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.debug("io_uring child channel raised an exception; closing", cause);
            ctx.close();
        }
    }
}
