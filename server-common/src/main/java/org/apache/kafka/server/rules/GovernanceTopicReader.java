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
package org.apache.kafka.server.rules;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Drives the {@link GovernanceLoader} from a Kafka {@link Consumer}.
 *
 * <p>The reader has three modes of operation:
 *
 * <ul>
 *   <li>{@link #pollOnce()} — one poll + apply + commit cycle. The lowest-level
 *       hook, useful for tests and for callers that own the polling loop.</li>
 *   <li>{@link #drainToEnd()} — synchronous bootstrap. Polls until every assigned
 *       partition's position has caught up with the end offset at the moment
 *       drain started, then commits once and returns. The broker calls this
 *       before opening its SocketServer to satisfy PROMPT.md's
 *       "drain all existing rules from the rules topic before accepting
 *       client connections" criterion.</li>
 *   <li>{@link #runLoop()} — steady-state. Calls {@code pollOnce()} until
 *       {@link #close()} is invoked. The consumer's {@code wakeup()} is used
 *       to break out of a long poll cleanly.</li>
 * </ul>
 *
 * <p>The reader does not own consumer setup: it expects the caller to have
 * configured the consumer with the broker's sentinel client-id (see
 * {@link GovernanceTopic#readerClientId}) and to have assigned (or subscribed)
 * the {@link GovernanceTopic#NAME} partitions before any of these methods are
 * called. That keeps the reader testable with {@link
 * org.apache.kafka.clients.consumer.MockConsumer} and doesn't entangle it with
 * group-coordinator semantics that don't apply to a broker-internal compacted
 * topic.
 */
public final class GovernanceTopicReader implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(GovernanceTopicReader.class);

    private final Consumer<String, byte[]> consumer;
    private final GovernanceLoader loader;
    private final Duration pollTimeout;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public GovernanceTopicReader(Consumer<String, byte[]> consumer,
                                 GovernanceLoader loader,
                                 Duration pollTimeout) {
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        this.loader = Objects.requireNonNull(loader, "loader");
        this.pollTimeout = Objects.requireNonNull(pollTimeout, "pollTimeout");
    }

    /**
     * One poll-apply-commit cycle. Returns the number of records consumed.
     * Empty polls still commit so that engine.active() reflects the loader's
     * current accumulated state.
     */
    public int pollOnce() {
        ConsumerRecords<String, byte[]> records = consumer.poll(pollTimeout);
        for (ConsumerRecord<String, byte[]> r : records) {
            loader.apply(r.key(), r.value());
        }
        loader.commit();
        return records.count();
    }

    /**
     * Synchronously drain every record currently in the topic, then commit.
     *
     * <p>"Currently in the topic" is defined as the {@code endOffsets} of the
     * assigned partitions at the moment this method starts. Records written
     * after that point may or may not be picked up here; they'll be picked
     * up by the next {@link #pollOnce()} or {@link #runLoop()}.
     *
     * <p>The single commit at the end is intentional: bootstrap should publish
     * one fully-formed {@link RuleSet} rather than a stream of intermediate
     * states. The request path therefore sees either the empty bootstrap state
     * or the fully-drained state, never anything in between.
     */
    public void drainToEnd() {
        Set<TopicPartition> assigned = consumer.assignment();
        if (assigned.isEmpty()) {
            LOG.debug("drainToEnd: consumer has no assigned partitions; nothing to drain");
            loader.commit();
            return;
        }
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assigned);
        while (!caughtUp(assigned, endOffsets)) {
            ConsumerRecords<String, byte[]> records = consumer.poll(pollTimeout);
            if (records.isEmpty() && caughtUp(assigned, endOffsets)) {
                break;
            }
            for (ConsumerRecord<String, byte[]> r : records) {
                loader.apply(r.key(), r.value());
            }
            // Defensive: if poll() returns empty before we're caught up
            // (e.g. broker not responsive yet), we'll loop again. To avoid
            // a tight spin, the consumer's poll timeout already throttles us.
        }
        loader.commit();
    }

    private boolean caughtUp(Set<TopicPartition> assigned, Map<TopicPartition, Long> endOffsets) {
        for (TopicPartition tp : assigned) {
            Long end = endOffsets.get(tp);
            if (end == null) {
                continue;
            }
            long pos;
            try {
                pos = consumer.position(tp);
            } catch (Exception e) {
                return false;
            }
            if (pos < end) {
                return false;
            }
        }
        return true;
    }

    /**
     * Steady-state polling loop. Returns when {@link #close()} is called
     * (which raises {@link WakeupException} on the consumer to break the
     * current poll).
     */
    public void runLoop() {
        while (running.get()) {
            try {
                pollOnce();
            } catch (WakeupException e) {
                // close() was called — exit cleanly.
                return;
            } catch (RuntimeException e) {
                // Non-fatal: log and continue. The loader's working state is
                // unaffected. A persistent failure (e.g. authorisation revoked)
                // will manifest as a flood of warns — the right place to alert.
                //
                // R23 #222: e.toString() can embed wire-derived attacker bytes
                // — e.g. an UnknownTopicOrPartitionException naming an
                // attacker-controlled topic, an AuthorizationException naming
                // an attacker-controlled principal, or a SerializationException
                // wrapping a hostile payload. Sanitise through LogSafe so a
                // poisoned message cannot inject CR/LF/control bytes into the
                // operator's SLF4J line.
                LOG.warn("governance reader poll failed: {}", LogSafe.sanitize(e.toString()));
            }
        }
    }

    /**
     * Signal {@link #runLoop()} to exit. Idempotent.
     */
    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            consumer.wakeup();
        }
    }

    /** Visible for tests: assigned partitions on the underlying consumer. */
    Set<TopicPartition> assignment() {
        return Collections.unmodifiableSet(consumer.assignment());
    }
}
