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

import org.apache.kafka.common.network.ChannelState;
import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Selector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Pass-through {@link BrokerSelector} backed by the production Java-NIO {@link Selector}.
 *
 * <p>Exists so the broker's {@code Processor} can talk to a single
 * {@link BrokerSelector} type regardless of which I/O backend is active. Holds no state
 * beyond a reference to the wrapped Selector — every call delegates.
 */
public final class NioBrokerSelector implements BrokerSelector {

    private final Selector delegate;

    public NioBrokerSelector(Selector delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    /** Visible to broker-side code that still needs the concrete Selector (e.g. test hooks). */
    public Selector unwrap() {
        return delegate;
    }

    @Override
    public void register(String id, SocketChannel socketChannel) throws IOException {
        Objects.requireNonNull(socketChannel, "socketChannel");
        delegate.register(id, socketChannel);
    }

    @Override
    public List<KafkaChannel> channels() {
        return delegate.channels();
    }

    @Override
    public KafkaChannel channel(String id) {
        return delegate.channel(id);
    }

    @Override
    public KafkaChannel closingChannel(String id) {
        return delegate.closingChannel(id);
    }

    @Override
    public KafkaChannel lowestPriorityChannel() {
        return delegate.lowestPriorityChannel();
    }

    @Override
    public void clearCompletedReceives() {
        delegate.clearCompletedReceives();
    }

    @Override
    public void clearCompletedSends() {
        delegate.clearCompletedSends();
    }

    // ---- Selectable ----

    @Override
    public void connect(String id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException {
        delegate.connect(id, address, sendBufferSize, receiveBufferSize);
    }

    @Override
    public void wakeup() {
        delegate.wakeup();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void close(String id) {
        delegate.close(id);
    }

    @Override
    public void send(NetworkSend send) {
        delegate.send(send);
    }

    @Override
    public void poll(long timeout) throws IOException {
        delegate.poll(timeout);
    }

    @Override
    public List<NetworkSend> completedSends() {
        return delegate.completedSends();
    }

    @Override
    public Collection<NetworkReceive> completedReceives() {
        return delegate.completedReceives();
    }

    @Override
    public Map<String, ChannelState> disconnected() {
        return delegate.disconnected();
    }

    @Override
    public List<String> connected() {
        return delegate.connected();
    }

    @Override
    public void mute(String id) {
        delegate.mute(id);
    }

    @Override
    public void unmute(String id) {
        delegate.unmute(id);
    }

    @Override
    public void muteAll() {
        delegate.muteAll();
    }

    @Override
    public void unmuteAll() {
        delegate.unmuteAll();
    }

    @Override
    public boolean isChannelReady(String id) {
        return delegate.isChannelReady(id);
    }
}
