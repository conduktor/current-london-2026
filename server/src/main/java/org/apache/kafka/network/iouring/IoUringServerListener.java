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
 * <h3>Thread safety</h3>
 * Construction binds synchronously and may block briefly. {@link #close()} is
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

    private final EventLoopGroup eventLoopGroup;
    private final Channel serverChannel;
    private final IoUringSelector selector;
    private final int boundPort;
    private volatile boolean closed;

    public IoUringServerListener(InetSocketAddress bindAddress, IoUringSelector selector) {
        this(bindAddress, selector, /*soBacklog*/ 128);
    }

    public IoUringServerListener(InetSocketAddress bindAddress, IoUringSelector selector, int soBacklog) {
        Objects.requireNonNull(bindAddress, "bindAddress");
        this.selector = Objects.requireNonNull(selector, "selector");
        if (!IoUringSupport.isAvailable()) {
            throw new IllegalStateException(
                "io_uring is not available on this host: " + IoUringSupport.unavailabilityReason());
        }

        this.eventLoopGroup = new MultiThreadIoEventLoopGroup(1, IoUringIoHandler.newFactory());

        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                .group(eventLoopGroup)
                .channel(IoUringServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, soBacklog)
                .option(UnixChannelOption.SO_REUSEPORT, true)
                .childOption(ChannelOption.AUTO_READ, true)
                .childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ch.pipeline().addLast(new ChildHandler(selector));
                    }
                });

            ChannelFuture future = bootstrap.bind(bindAddress).sync();
            this.serverChannel = future.channel();
            this.boundPort = ((InetSocketAddress) serverChannel.localAddress()).getPort();
            log.info("io_uring listener bound to {} (port {})", bindAddress, boundPort);
        } catch (InterruptedException ie) {
            eventLoopGroup.shutdownGracefully();
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while binding io_uring listener", ie);
        } catch (RuntimeException e) {
            eventLoopGroup.shutdownGracefully();
            throw e;
        }
    }

    /** Returns the actually bound port — useful when the caller passed port 0. */
    public int boundPort() {
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
        try {
            serverChannel.close().sync();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.debug("error closing io_uring server channel", e);
        }
        eventLoopGroup.shutdownGracefully(SHUTDOWN_QUIET_MS, SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)
            .syncUninterruptibly();
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
