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

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.unix.UnixChannelOption;
import io.netty.channel.uring.IoUringIoHandler;
import io.netty.channel.uring.IoUringServerSocketChannel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Architectural prerequisite for the per-Processor io_uring binding model:
 * proves the local kernel + Netty 4.2.x io_uring transport actually shard
 * accepts across two {@code IoUringServerSocketChannel}s bound to the same
 * port with {@code SO_REUSEPORT}.
 *
 * <p>If this test passes, the architecture described in the package README
 * (each Processor owns its own io_uring listener bound to the listener port
 * with {@code SO_REUSEPORT=true}; the kernel load-balances accepts) is sound
 * on the host running the tests. If it fails, the IoUringSelector model must
 * change before implementation begins.
 *
 * <p>Skipped transparently when {@link IoUringSupport#isAvailable()} is false
 * — this test never gates CI on a non-Linux machine.
 *
 * <p>Why this test must exist before IoUringSelector is written: the agent
 * report on the architectural seam (recorded in commit 75daf00832's review)
 * identified two unknowns the test resolves in one shot —
 * <ol>
 *   <li>Whether Netty 4.2.12 honours {@code UnixChannelOption.SO_REUSEPORT}
 *       on the {@code IoUringServerSocketChannel}.</li>
 *   <li>Whether Linux's accept-distribution across SO_REUSEPORT sockets
 *       is even enough that no explicit accept-handler at the Acceptor is
 *       needed.</li>
 * </ol>
 */
class IoUringPortShardingIT {

    private static final int CLIENT_CONNECTIONS = 200;
    private static final int CONNECT_TIMEOUT_MS = 5_000;
    private static final long ACCEPT_DEADLINE_MS = 10_000;

    @Test
    void twoIoUringServersOnSamePortShareAcceptsViaSoReuseport() throws Exception {
        assumeTrue(IoUringSupport.isAvailable(),
            "io_uring is not available on this host (" + IoUringSupport.unavailabilityReason() + "); skipping");

        AtomicInteger acceptsA = new AtomicInteger();
        AtomicInteger acceptsB = new AtomicInteger();

        EventLoopGroup bossA = newGroup();
        EventLoopGroup workerA = newGroup();
        EventLoopGroup bossB = newGroup();
        EventLoopGroup workerB = newGroup();

        try {
            // Bind A on an ephemeral port. The kernel picks an unused port for us, and
            // SO_REUSEPORT is set on A from the start so B can join with the same port.
            Channel serverA = bind(bossA, workerA, 0, acceptsA);
            int port = ((InetSocketAddress) serverA.localAddress()).getPort();
            assertTrue(port > 0, "kernel must hand out a real ephemeral port");

            // Bind B on the exact port A is on. This only succeeds if SO_REUSEPORT
            // is honoured by the io_uring transport — otherwise EADDRINUSE.
            Channel serverB = bind(bossB, workerB, port, acceptsB);
            assertEquals(port, ((InetSocketAddress) serverB.localAddress()).getPort());

            try {
                openClientConnections(port, CLIENT_CONNECTIONS);
                waitForAccepts(acceptsA, acceptsB, CLIENT_CONNECTIONS, ACCEPT_DEADLINE_MS);
            } finally {
                serverB.close().sync();
                serverA.close().sync();
            }

            int total = acceptsA.get() + acceptsB.get();
            assertEquals(CLIENT_CONNECTIONS, total,
                "every client connect must land on exactly one server (acceptsA=" + acceptsA + ", acceptsB=" + acceptsB + ")");
            assertTrue(acceptsA.get() > 0,
                "kernel must shard at least one accept to server A (got " + acceptsA + " / " + acceptsB + ")");
            assertTrue(acceptsB.get() > 0,
                "kernel must shard at least one accept to server B (got " + acceptsA + " / " + acceptsB + ")");
        } finally {
            shutdownGracefully(bossA, workerA, bossB, workerB);
        }
    }

    private static EventLoopGroup newGroup() {
        // Single-threaded group: matches the per-Processor model in production where the
        // Processor thread IS the io_uring event-loop thread (no cross-thread hop).
        return new MultiThreadIoEventLoopGroup(1, IoUringIoHandler.newFactory());
    }

    private static Channel bind(EventLoopGroup boss, EventLoopGroup worker, int port,
                                AtomicInteger acceptCounter) throws InterruptedException {
        return new ServerBootstrap()
            .group(boss, worker)
            .channel(IoUringServerSocketChannel.class)
            .option(UnixChannelOption.SO_REUSEPORT, true)
            .childHandler(new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel ch) {
                    acceptCounter.incrementAndGet();
                    // Discard everything quickly so client writes never back up.
                    ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                        @Override
                        public void channelRead(ChannelHandlerContext ctx, Object msg) {
                            io.netty.util.ReferenceCountUtil.release(msg);
                        }
                    });
                }
            })
            .bind("127.0.0.1", port).sync().channel();
    }

    private static void openClientConnections(int port, int count) throws Exception {
        // Drive connects from a separate thread so server event loops can drain accepts
        // in parallel. We hold the sockets open until both servers have counted everything;
        // closing too eagerly can race with the accept side and starve one of the rings.
        Socket[] sockets = new Socket[count];
        try {
            for (int i = 0; i < count; i++) {
                Socket s = new Socket();
                s.connect(new InetSocketAddress("127.0.0.1", port), CONNECT_TIMEOUT_MS);
                sockets[i] = s;
            }
        } finally {
            for (Socket s : sockets) {
                if (s != null) {
                    try {
                        s.close();
                    } catch (Exception ignored) { /* noop */ }
                }
            }
        }
    }

    private static void waitForAccepts(AtomicInteger a, AtomicInteger b, int target, long deadlineMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + deadlineMs;
        while (a.get() + b.get() < target && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
    }

    private static void shutdownGracefully(EventLoopGroup... groups) {
        for (EventLoopGroup g : groups) {
            try {
                g.shutdownGracefully(0, 200, java.util.concurrent.TimeUnit.MILLISECONDS).sync();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
