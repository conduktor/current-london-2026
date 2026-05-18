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

import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

/**
 * Inert {@link SelectionKey} that {@code IoUringTransportLayer.selectionKey()} returns.
 *
 * <p>The io_uring backend never registers a {@link SelectableChannel} with a JDK
 * {@link Selector}; reads and writes are driven by the Netty {@code IoUringEventLoop}.
 * KafkaChannel still exposes a {@code selectionKey()} accessor (and the historical NIO
 * {@code Selector} consumes the returned key as a map/set identity token plus
 * {@code cancel()}/{@code attach(null)}). For the io_uring path nothing outside our
 * package ever derefs the returned key, but we keep the contract honest by returning a
 * key whose state can be inspected without crashing: interest-ops round-trip in memory,
 * readiness is always zero (no JDK selector to observe it), and the key cleanly flips
 * invalid on {@link #cancel()}.
 *
 * <p>Not thread-safe. Each KafkaChannel owns one key, manipulated only on the Processor
 * thread.
 */
final class NoopSelectionKey extends SelectionKey {

    private int interestOps;
    private boolean valid = true;

    @Override
    public SelectableChannel channel() {
        return null;
    }

    @Override
    public Selector selector() {
        return null;
    }

    @Override
    public boolean isValid() {
        return valid;
    }

    @Override
    public void cancel() {
        valid = false;
    }

    @Override
    public int interestOps() {
        return interestOps;
    }

    @Override
    public SelectionKey interestOps(int ops) {
        this.interestOps = ops;
        return this;
    }

    @Override
    public int readyOps() {
        return 0;
    }
}
