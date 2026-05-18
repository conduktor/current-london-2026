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

import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import io.netty.channel.embedded.EmbeddedChannel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IoUringPlaintextAuthenticatorTest {

    private EmbeddedChannel channel;
    private IoUringTransportLayer layer;
    private IoUringPlaintextAuthenticator auth;

    @AfterEach
    void tearDown() {
        if (auth != null) auth.close();
        if (layer != null) layer.close();
        if (channel != null && channel.isOpen()) channel.close();
    }

    @Test
    void completeImmediatelyAndAuthenticateIsNoop() throws Exception {
        channel = new EmbeddedChannel();
        layer = new IoUringTransportLayer(channel,
            new InetSocketAddress("198.51.100.7", 1234),
            new InetSocketAddress("203.0.113.1", 9092));
        auth = new IoUringPlaintextAuthenticator(layer, ListenerName.normalised("PLAINTEXT"));

        auth.authenticate(); // no exception
        assertTrue(auth.complete(), "PLAINTEXT auth is always complete");
    }

    @Test
    void principalIsAnonymousWithTheDefaultBuilder() {
        channel = new EmbeddedChannel();
        layer = new IoUringTransportLayer(channel,
            new InetSocketAddress("198.51.100.7", 1234),
            new InetSocketAddress("203.0.113.1", 9092));
        auth = new IoUringPlaintextAuthenticator(layer, ListenerName.normalised("PLAINTEXT"));

        KafkaPrincipal p = auth.principal();
        assertEquals(KafkaPrincipal.ANONYMOUS, p,
            "PLAINTEXT must yield ANONYMOUS — the broker's authorizer keys off this");
    }
}
