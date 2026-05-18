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

import org.apache.kafka.common.network.ChannelMetadataRegistry;
import org.apache.kafka.common.network.CipherInformation;
import org.apache.kafka.common.network.ClientInformation;

/**
 * Per-channel metadata holder used by {@link IoUringSelector}.
 *
 * <p>The NIO {@code Selector} ties its inner-class registry to its own metrics sensors so
 * {@code connectionsByCipher} / {@code connectionsByClient} sensors track active counts.
 * v1 of the io_uring backend deliberately does not wire those broker-wide gauges — the
 * sensors live on {@code Selector} and re-creating them would mean leaking
 * implementation-private state out of {@code clients/}. We hold the last-registered
 * values so {@code KafkaChannel.channelMetadataRegistry().clientInformation()} returns
 * sensible data to the request-handling path; the matching metrics will land alongside
 * the broker's broader telemetry wiring (separate follow-up).
 */
final class IoUringChannelMetadataRegistry implements ChannelMetadataRegistry {

    private CipherInformation cipherInformation;
    private ClientInformation clientInformation;

    @Override
    public void registerCipherInformation(CipherInformation cipherInformation) {
        this.cipherInformation = cipherInformation;
    }

    @Override
    public CipherInformation cipherInformation() {
        return cipherInformation;
    }

    @Override
    public void registerClientInformation(ClientInformation clientInformation) {
        this.clientInformation = clientInformation;
    }

    @Override
    public ClientInformation clientInformation() {
        return clientInformation;
    }

    @Override
    public void close() {
        cipherInformation = null;
        clientInformation = null;
    }
}
