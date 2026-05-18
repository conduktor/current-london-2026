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

import org.apache.kafka.common.network.Authenticator;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.common.security.auth.PlaintextAuthenticationContext;
import org.apache.kafka.common.utils.Utils;

import java.io.Closeable;
import java.net.InetAddress;
import java.util.Map;
import java.util.Optional;

/**
 * The PLAINTEXT {@link Authenticator} the io_uring backend wraps around every accepted
 * channel.
 *
 * <p>Mirrors the private {@code PlaintextChannelBuilder.PlaintextAuthenticator} in
 * {@code clients/} — that class is package-private and cannot be reused from {@code server/}.
 * The behaviour is identical:
 *
 * <ul>
 *   <li>{@link #authenticate()} is a no-op (PLAINTEXT has no handshake).</li>
 *   <li>{@link #complete()} is always true.</li>
 *   <li>{@link #principal()} reads the remote {@link InetAddress} off the transport layer
 *       and calls the configured {@link KafkaPrincipalBuilder}. With the default builder
 *       this returns {@link KafkaPrincipal#ANONYMOUS}.</li>
 * </ul>
 */
final class IoUringPlaintextAuthenticator implements Authenticator {

    private final IoUringTransportLayer transportLayer;
    private final KafkaPrincipalBuilder principalBuilder;
    private final ListenerName listenerName;

    IoUringPlaintextAuthenticator(IoUringTransportLayer transportLayer, ListenerName listenerName,
                                  Map<String, ?> configs) {
        this.transportLayer = transportLayer;
        this.listenerName = listenerName;
        // Match PlaintextChannelBuilder.PlaintextAuthenticator: the broker's parsed configs
        // are required so a user-configured PRINCIPAL_BUILDER_CLASS_CONFIG is honored. Passing
        // an empty map silently falls back to DefaultKafkaPrincipalBuilder, which is a
        // semantic divergence from the NIO path on the same broker.
        this.principalBuilder = ChannelBuilders.createPrincipalBuilder(configs, null, null);
    }

    @Override
    public void authenticate() {
        // PLAINTEXT: no handshake.
    }

    @Override
    public KafkaPrincipal principal() {
        InetAddress clientAddress = transportLayer.socketChannel().socket().getInetAddress();
        return principalBuilder.build(new PlaintextAuthenticationContext(clientAddress, listenerName.value()));
    }

    @Override
    public Optional<KafkaPrincipalSerde> principalSerde() {
        return principalBuilder instanceof KafkaPrincipalSerde
            ? Optional.of((KafkaPrincipalSerde) principalBuilder)
            : Optional.empty();
    }

    @Override
    public boolean complete() {
        return true;
    }

    @Override
    public void close() {
        if (principalBuilder instanceof Closeable) {
            Utils.closeQuietly((Closeable) principalBuilder, "principal builder");
        }
    }
}
