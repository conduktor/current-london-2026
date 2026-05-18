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

import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.network.Selectable;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.List;

/**
 * Broker-side facade over {@link Selectable}. Captures the additional methods that the
 * {@code kafka.network.Processor} relies on beyond the public {@link Selectable} interface,
 * so an alternative I/O backend (Netty io_uring) can be plugged in without modifying
 * {@code clients/}.
 *
 * <p>The contract is identical to {@code org.apache.kafka.common.network.Selector} for the
 * methods listed below — see that class for invariants. Implementations are not required to
 * be thread-safe; each instance is owned by a single Processor thread.
 */
public interface BrokerSelector extends Selectable, AutoCloseable {

    /**
     * Register an already-accepted server-side socket channel with the selector. The selector
     * takes ownership of the channel; subsequent I/O is driven through {@link #poll(long)}.
     *
     * @throws IllegalStateException if a channel with this id is already registered or closing
     */
    void register(String id, SocketChannel socketChannel) throws IOException;

    /** All channels currently registered with the selector (including closing channels in some backends). */
    List<KafkaChannel> channels();

    /** Look up an active channel by id, or {@code null} if none. */
    KafkaChannel channel(String id);

    /** Look up a channel that is in the process of being closed by id, or {@code null} if none. */
    KafkaChannel closingChannel(String id);

    /**
     * The channel with the lowest priority (typically least-recently-used), used by the broker
     * to make room when global connection quotas are exceeded.
     *
     * @return the lowest-priority channel, or {@code null} if no eligible channel exists
     */
    KafkaChannel lowestPriorityChannel();

    /**
     * Discard the receive-completion events produced by the most recent {@link #poll(long)}.
     * Called by the broker once it has handed those receives off to request processing.
     */
    void clearCompletedReceives();

    /**
     * Discard the send-completion events produced by the most recent {@link #poll(long)}.
     * Called by the broker once it has handed those sends off to response processing.
     */
    void clearCompletedSends();

    @Override
    void close();
}
