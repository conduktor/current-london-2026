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
package org.apache.kafka.common.network;

/**
 * Same-package bridge that exposes {@link KafkaChannel#mute()} and
 * {@link KafkaChannel#maybeUnmute()} to broker-side selector implementations.
 *
 * <p>The methods are package-private in {@code clients/}; this class lives under
 * {@code server/} but in the same package so it can call them. The PROMPT constraint
 * forbids editing anything under {@code clients/}, so this is the only way for the
 * io_uring selector to drive {@link KafkaChannel}'s mute state machine. The NIO
 * {@code Selector} drives the same state machine via the package-private calls
 * directly — see {@code clients/.../Selector.java#mute(KafkaChannel)} and
 * {@code unmute(KafkaChannel)}.
 *
 * <p>This is a thin pass-through, no additional logic. State transitions, OP_READ
 * interest changes, and autoRead flipping are all preserved exactly as the NIO
 * baseline performs them.
 */
public final class KafkaChannelMuteBridge {

    private KafkaChannelMuteBridge() {
    }

    /**
     * Transition {@code channel} into the {@code MUTED} mute state if currently
     * {@code NOT_MUTED}. Mirrors {@code Selector.mute(KafkaChannel)} step 1.
     */
    public static void mute(KafkaChannel channel) {
        channel.mute();
    }

    /**
     * Attempt to leave the {@code MUTED} state. Returns true if the channel is
     * {@code NOT_MUTED} after the call (either it was already there, or this call
     * moved it). Mirrors {@code Selector.unmute(KafkaChannel)} step 1.
     */
    public static boolean maybeUnmute(KafkaChannel channel) {
        return channel.maybeUnmute();
    }
}
