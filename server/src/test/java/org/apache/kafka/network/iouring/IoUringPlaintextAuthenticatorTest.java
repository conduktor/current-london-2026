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

import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.PlaintextAuthenticationContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;

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
        auth = new IoUringPlaintextAuthenticator(layer, ListenerName.normalised("PLAINTEXT"), Collections.emptyMap());

        auth.authenticate(); // no exception
        assertTrue(auth.complete(), "PLAINTEXT auth is always complete");
    }

    @Test
    void principalIsAnonymousWithTheDefaultBuilder() {
        channel = new EmbeddedChannel();
        layer = new IoUringTransportLayer(channel,
            new InetSocketAddress("198.51.100.7", 1234),
            new InetSocketAddress("203.0.113.1", 9092));
        auth = new IoUringPlaintextAuthenticator(layer, ListenerName.normalised("PLAINTEXT"), Collections.emptyMap());

        KafkaPrincipal p = auth.principal();
        assertEquals(KafkaPrincipal.ANONYMOUS, p,
            "PLAINTEXT must yield ANONYMOUS — the broker's authorizer keys off this");
    }

    @Test
    void customPrincipalBuilderClassIsHonored() {
        // Without threading the broker configs through, the io_uring path would silently
        // fall back to DefaultKafkaPrincipalBuilder and emit ANONYMOUS regardless of what
        // the user set principal.builder.class to — diverging from NIO PLAINTEXT on the
        // same broker. The configs map is the only signal the user gave us; if we lose
        // it we lose user-defined authorization semantics on PLAINTEXT.
        channel = new EmbeddedChannel();
        layer = new IoUringTransportLayer(channel,
            new InetSocketAddress("198.51.100.7", 1234),
            new InetSocketAddress("203.0.113.1", 9092));
        Map<String, Object> configs = Collections.singletonMap(
            BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, IpPrincipalBuilder.class);
        auth = new IoUringPlaintextAuthenticator(layer, ListenerName.normalised("PLAINTEXT"), configs);

        KafkaPrincipal p = auth.principal();
        assertEquals("User", p.getPrincipalType());
        assertEquals("198.51.100.7", p.getName(),
            "custom builder must be loaded from configs and applied — otherwise io_uring " +
            "PLAINTEXT silently diverges from NIO PLAINTEXT authorization");
    }

    /** Test-only builder that derives the principal name from the remote IP. */
    public static final class IpPrincipalBuilder implements KafkaPrincipalBuilder {
        @Override
        public KafkaPrincipal build(AuthenticationContext context) {
            if (context instanceof PlaintextAuthenticationContext) {
                return new KafkaPrincipal(KafkaPrincipal.USER_TYPE,
                    ((PlaintextAuthenticationContext) context).clientAddress().getHostAddress());
            }
            return KafkaPrincipal.ANONYMOUS;
        }
    }
}
