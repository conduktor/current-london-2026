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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link GovernanceTopicReader} — drives the {@link GovernanceLoader}
 * from a {@code Consumer<String, byte[]>}.
 *
 * <p>All tests use {@link MockConsumer} so they exercise the real wiring
 * (subscribe → poll → loader.apply → loader.commit) without spinning a broker.
 */
public class GovernanceTopicReaderTest {

    private static final TopicPartition TP0 = new TopicPartition(GovernanceTopic.NAME, 0);

    private static byte[] envelope(String when, ApiKeys key, int errorCode) {
        String json = "{\"apiKeys\":[\"" + key.name() + "\"],\"action\":\"DENY\","
            + "\"when\":\"" + when + "\",\"errorCode\":" + errorCode + "}";
        return json.getBytes(StandardCharsets.UTF_8);
    }

    private static MockConsumer<String, byte[]> newConsumer() {
        MockConsumer<String, byte[]> c = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        c.assign(Collections.singleton(TP0));
        Map<TopicPartition, Long> begin = new HashMap<>();
        begin.put(TP0, 0L);
        c.updateBeginningOffsets(begin);
        return c;
    }

    @Test
    public void pollOnceAppliesRecordsAndCommits() {
        MockConsumer<String, byte[]> consumer = newConsumer();
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        GovernanceTopicReader reader = new GovernanceTopicReader(consumer, loader, Duration.ofMillis(10));
        consumer.addRecord(new ConsumerRecord<>(GovernanceTopic.NAME, 0, 0L, "r1",
            envelope("true", ApiKeys.METADATA, 7)));
        int n = reader.pollOnce();
        assertEquals(1, n);
        RuleDecision d = engine.evaluate(ApiKeys.METADATA, "c", Collections::emptyMap);
        assertTrue(d.denied());
        assertEquals("r1", d.denyingRuleId());
    }

    @Test
    public void tombstonePropagatesThroughReader() {
        MockConsumer<String, byte[]> consumer = newConsumer();
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        GovernanceTopicReader reader = new GovernanceTopicReader(consumer, loader, Duration.ofMillis(10));
        consumer.addRecord(new ConsumerRecord<>(GovernanceTopic.NAME, 0, 0L, "r1",
            envelope("true", ApiKeys.METADATA, 7)));
        consumer.addRecord(new ConsumerRecord<>(GovernanceTopic.NAME, 0, 1L, "r1", null));
        reader.pollOnce();
        assertSame(RuleDecision.ALLOW,
            engine.evaluate(ApiKeys.METADATA, "c", Collections::emptyMap));
    }

    @Test
    public void emptyPollStillCommits() {
        // No records returned by poll → loader.commit() still runs, installing
        // a fresh RuleSet (which is identical to the previous one in content
        // but still a valid install).
        MockConsumer<String, byte[]> consumer = newConsumer();
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        GovernanceTopicReader reader = new GovernanceTopicReader(consumer, loader, Duration.ofMillis(10));
        int n = reader.pollOnce();
        assertEquals(0, n);
        // engine.active() is some non-null RuleSet (EMPTY or otherwise valid).
        assertEquals(0, engine.active().size());
    }

    @Test
    public void drainToEndAppliesAllExistingAndStops() {
        // Bootstrap-drain: read every record currently in the topic, commit,
        // and return — do not block forever waiting for more records.
        MockConsumer<String, byte[]> consumer = newConsumer();
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        GovernanceTopicReader reader = new GovernanceTopicReader(consumer, loader, Duration.ofMillis(10));

        Map<TopicPartition, Long> ends = new HashMap<>();
        ends.put(TP0, 3L);
        consumer.updateEndOffsets(ends);
        consumer.addRecord(new ConsumerRecord<>(GovernanceTopic.NAME, 0, 0L, "r1",
            envelope("true", ApiKeys.METADATA, 11)));
        consumer.addRecord(new ConsumerRecord<>(GovernanceTopic.NAME, 0, 1L, "r2",
            envelope("true", ApiKeys.FETCH, 22)));
        consumer.addRecord(new ConsumerRecord<>(GovernanceTopic.NAME, 0, 2L, "r3",
            envelope("true", ApiKeys.CREATE_TOPICS, 33)));

        reader.drainToEnd();

        assertEquals(3, engine.active().size());
        assertTrue(engine.evaluate(ApiKeys.METADATA, "c", Collections::emptyMap).denied());
        assertTrue(engine.evaluate(ApiKeys.FETCH, "c", Collections::emptyMap).denied());
        assertTrue(engine.evaluate(ApiKeys.CREATE_TOPICS, "c", Collections::emptyMap).denied());
    }

    @Test
    public void runLoopProcessesBatchesUntilClosed() throws InterruptedException {
        MockConsumer<String, byte[]> consumer = newConsumer();
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        GovernanceTopicReader reader = new GovernanceTopicReader(consumer, loader, Duration.ofMillis(5));

        consumer.addRecord(new ConsumerRecord<>(GovernanceTopic.NAME, 0, 0L, "r1",
            envelope("true", ApiKeys.METADATA, 7)));

        Thread t = new Thread(reader::runLoop, "test-governance-reader");
        t.start();
        // Wait for the rule to take effect — generous timeout.
        long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
        while (System.nanoTime() < deadline) {
            if (engine.evaluate(ApiKeys.METADATA, "c", Collections::emptyMap).denied()) {
                break;
            }
            Thread.sleep(5);
        }
        assertTrue(engine.evaluate(ApiKeys.METADATA, "c", Collections::emptyMap).denied(),
            "runLoop should have applied the record");

        reader.close();
        t.join(2_000);
        assertTrue(!t.isAlive(), "runLoop must exit on close()");
    }

    @Test
    public void badEnvelopeDoesNotHaltRunLoop() throws InterruptedException {
        MockConsumer<String, byte[]> consumer = newConsumer();
        RuleEngine engine = new RuleEngine();
        GovernanceLoader loader = new GovernanceLoader(engine);
        GovernanceTopicReader reader = new GovernanceTopicReader(consumer, loader, Duration.ofMillis(5));

        consumer.addRecord(new ConsumerRecord<>(GovernanceTopic.NAME, 0, 0L, "bad",
            "not json".getBytes(StandardCharsets.UTF_8)));
        consumer.addRecord(new ConsumerRecord<>(GovernanceTopic.NAME, 0, 1L, "good",
            envelope("true", ApiKeys.METADATA, 99)));

        Thread t = new Thread(reader::runLoop, "test-governance-reader");
        t.start();
        long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
        while (System.nanoTime() < deadline) {
            RuleDecision d = engine.evaluate(ApiKeys.METADATA, "c", Collections::emptyMap);
            if (d.denied() && "good".equals(d.denyingRuleId())) {
                break;
            }
            Thread.sleep(5);
        }
        RuleDecision d = engine.evaluate(ApiKeys.METADATA, "c", Collections::emptyMap);
        assertTrue(d.denied());
        assertEquals("good", d.denyingRuleId());
        reader.close();
        t.join(2_000);
    }
}
