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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for {@link StubSocketChannel}.
 *
 * <p>The stub exists only to satisfy KafkaChannel's four reads off
 * {@code transportLayer.socketChannel()} (and its socket): remote {@link InetAddress},
 * remote port, local address, and remote address. Every other SocketChannel method must
 * either be safely defaulted (e.g. blocking-mode getters) or throw to make any unexpected
 * usage loud rather than silent.
 */
class StubSocketChannelTest {

    private static final InetSocketAddress REMOTE = new InetSocketAddress("198.51.100.7", 51234);
    private static final InetSocketAddress LOCAL = new InetSocketAddress("203.0.113.1", 9092);

    @Test
    void socketReturnsCachedRemoteAddressAndPort() {
        StubSocketChannel ch = new StubSocketChannel(REMOTE, LOCAL);
        assertEquals(REMOTE.getAddress(), ch.socket().getInetAddress(),
            "KafkaChannel.socketAddress() reads getInetAddress() — must be the remote, not local");
        assertEquals(REMOTE.getPort(), ch.socket().getPort(),
            "KafkaChannel.socketPort() reads getPort() — must be the remote port");
    }

    @Test
    void socketReturnsCachedLocalAddress() {
        StubSocketChannel ch = new StubSocketChannel(REMOTE, LOCAL);
        assertEquals(LOCAL, ch.socket().getLocalSocketAddress(),
            "KafkaChannel.socketDescription() reads getLocalSocketAddress() — must be the broker's bound address");
    }

    @Test
    void getRemoteAddressReturnsTheCachedRemote() throws Exception {
        StubSocketChannel ch = new StubSocketChannel(REMOTE, LOCAL);
        assertEquals(REMOTE, ch.getRemoteAddress(),
            "KafkaChannel.finishConnect() reads getRemoteAddress() — must be the remote SocketAddress");
    }

    @Test
    void getLocalAddressReturnsTheCachedLocal() throws Exception {
        StubSocketChannel ch = new StubSocketChannel(REMOTE, LOCAL);
        assertEquals(LOCAL, ch.getLocalAddress());
    }

    @Test
    void isConnectedReportsTrueForAcceptedChannel() {
        // The stub only ever models an already-accepted server-side channel, so this is true
        // until close() — matches the lifetime semantics that KafkaChannel expects.
        StubSocketChannel ch = new StubSocketChannel(REMOTE, LOCAL);
        assertTrue(ch.isConnected());
        assertFalse(ch.isConnectionPending(), "server-side accepted channels are never pending");
    }

    @Test
    void socketInstanceIsCachedAndIdentityStable() {
        // KafkaChannel reads .socket() multiple times; we cache one Socket instance to keep
        // identity stable in case anything keys off it.
        StubSocketChannel ch = new StubSocketChannel(REMOTE, LOCAL);
        assertSame(ch.socket(), ch.socket());
        assertNotNull(ch.socket());
    }

    @Test
    void nonGetterOperationsThrowToMakeMisuseLoud() {
        // Anything beyond the four read sites in KafkaChannel is unexpected. If something
        // ever does call read/write/bind/connect/shutdown on this stub, we want the broker
        // to fail loudly rather than silently behaving wrong.
        StubSocketChannel ch = new StubSocketChannel(REMOTE, LOCAL);
        assertThrows(UnsupportedOperationException.class, () -> ch.read(java.nio.ByteBuffer.allocate(1)));
        assertThrows(UnsupportedOperationException.class, () -> ch.write(java.nio.ByteBuffer.allocate(1)));
        assertThrows(UnsupportedOperationException.class, () -> ch.bind(LOCAL));
        assertThrows(UnsupportedOperationException.class, () -> ch.connect(REMOTE));
        assertThrows(UnsupportedOperationException.class, () -> ch.shutdownInput());
        assertThrows(UnsupportedOperationException.class, () -> ch.shutdownOutput());
        assertThrows(UnsupportedOperationException.class, () -> ch.finishConnect());
    }

    @Test
    void implementsSocketChannelSoKafkaChannelCanUseItUnchanged() {
        // Confirms the assignment SocketChannel sc = transportLayer.socketChannel() compiles
        // — the whole point of this stub.
        SocketChannel ch = new StubSocketChannel(REMOTE, LOCAL);
        assertNotNull(ch);
    }
}
