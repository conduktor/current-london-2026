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

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Set;

/**
 * The minimum {@link SocketChannel} stub needed to wrap a Netty {@code IoUringSocketChannel}
 * inside Kafka's {@code KafkaChannel}.
 *
 * <p>{@code KafkaChannel} (which we cannot modify — it lives in {@code clients/}) reads from
 * its {@code transportLayer.socketChannel()} in exactly four places:
 *
 * <ul>
 *   <li>{@code finishConnect()} — {@code socketChannel().getRemoteAddress()}</li>
 *   <li>{@code socketAddress()} — {@code socketChannel().socket().getInetAddress()}</li>
 *   <li>{@code socketPort()} — {@code socketChannel().socket().getPort()}</li>
 *   <li>{@code socketDescription()} — {@code socketChannel().socket().getInetAddress()} +
 *       {@code getLocalAddress()}</li>
 * </ul>
 *
 * <p>All four read addresses that were captured when the Netty channel was accepted. This
 * stub serves only those reads. Every other {@code SocketChannel} method throws
 * {@link UnsupportedOperationException} so that any unexpected call site fails loudly
 * rather than silently returning wrong data.
 *
 * <p>The stub deliberately does <em>not</em> register itself with a JDK {@code Selector};
 * the io_uring backend drives I/O autonomously and never goes through {@code SelectorProvider}.
 */
final class StubSocketChannel extends SocketChannel {

    private final SocketAddress remote;
    private final SocketAddress local;
    private final Socket socket;

    StubSocketChannel(SocketAddress remote, SocketAddress local) {
        super(SelectorProvider.provider());
        this.remote = remote;
        this.local = local;
        this.socket = new StubSocket(remote, local);
    }

    @Override
    public Socket socket() {
        return socket;
    }

    @Override
    public boolean isConnected() {
        return true;
    }

    @Override
    public boolean isConnectionPending() {
        return false;
    }

    @Override
    public SocketAddress getRemoteAddress() {
        return remote;
    }

    @Override
    public SocketAddress getLocalAddress() {
        return local;
    }

    @Override
    public SocketChannel bind(SocketAddress localAddress) {
        throw unsupported("bind");
    }

    @Override
    public <T> SocketChannel setOption(SocketOption<T> name, T value) {
        throw unsupported("setOption");
    }

    @Override
    public <T> T getOption(SocketOption<T> name) {
        throw unsupported("getOption");
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
        throw unsupported("supportedOptions");
    }

    @Override
    public SocketChannel shutdownInput() {
        throw unsupported("shutdownInput");
    }

    @Override
    public SocketChannel shutdownOutput() {
        throw unsupported("shutdownOutput");
    }

    @Override
    public boolean connect(SocketAddress remoteAddress) {
        throw unsupported("connect");
    }

    @Override
    public boolean finishConnect() {
        throw unsupported("finishConnect");
    }

    @Override
    public int read(ByteBuffer dst) {
        throw unsupported("read");
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) {
        throw unsupported("read[]");
    }

    @Override
    public int write(ByteBuffer src) {
        throw unsupported("write");
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) {
        throw unsupported("write[]");
    }

    @Override
    protected void implCloseSelectableChannel() {
        // io_uring channels are closed via the Netty channel directly, never through this stub.
    }

    @Override
    protected void implConfigureBlocking(boolean block) {
        throw unsupported("configureBlocking");
    }

    private static UnsupportedOperationException unsupported(String op) {
        return new UnsupportedOperationException(
            "StubSocketChannel exists only to satisfy KafkaChannel's remote-address reads; "
                + "operation '" + op + "' is not supported on an io_uring-backed channel");
    }

    /** A minimal {@link Socket} surfacing the cached remote/local addresses. */
    private static final class StubSocket extends Socket {
        private final SocketAddress remote;
        private final SocketAddress local;

        StubSocket(SocketAddress remote, SocketAddress local) {
            this.remote = remote;
            this.local = local;
        }

        @Override
        public InetAddress getInetAddress() {
            return remote instanceof java.net.InetSocketAddress
                ? ((java.net.InetSocketAddress) remote).getAddress()
                : null;
        }

        @Override
        public int getPort() {
            return remote instanceof java.net.InetSocketAddress
                ? ((java.net.InetSocketAddress) remote).getPort()
                : -1;
        }

        @Override
        public SocketAddress getRemoteSocketAddress() {
            return remote;
        }

        @Override
        public SocketAddress getLocalSocketAddress() {
            return local;
        }

        @Override
        public InetAddress getLocalAddress() {
            return local instanceof java.net.InetSocketAddress
                ? ((java.net.InetSocketAddress) local).getAddress()
                : null;
        }

        @Override
        public int getLocalPort() {
            return local instanceof java.net.InetSocketAddress
                ? ((java.net.InetSocketAddress) local).getPort()
                : -1;
        }

        @Override
        public boolean isConnected() {
            return true;
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() throws IOException {
            // Closing is the Netty channel's job; this stub holds no native resources.
        }
    }
}
