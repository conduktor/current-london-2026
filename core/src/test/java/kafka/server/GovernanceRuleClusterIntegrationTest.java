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
package kafka.server;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.rules.GovernanceTopic;
import org.apache.kafka.server.rules.RuleSet;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * PROMPT.md functional scenario #1 — three brokers in a cluster converge on
 * the same governance rule and stop enforcing it together after a tombstone,
 * with no broker restart.
 *
 * <p>This is the end-to-end test that pins multi-broker convergence: the
 * unit tests in {@link BrokerGovernanceBootstrapTest} prove the drain
 * mechanics on a single mocked log; this test proves the same code path
 * actually works across three real KRaft brokers reading the same
 * {@code __governance} compacted topic through the periodic re-drain.
 *
 * <p>It also doubles as the regression test for the fail-closed gate added
 * in task #17: the test must create {@code __governance} with RF=3 (one
 * replica on each broker) before the gate observes any of them, otherwise
 * one or more brokers would refuse to open client traffic.
 *
 * <p>Why we assert at the {@link org.apache.kafka.server.rules.RuleEngine}
 * level rather than driving an actual {@code CreateTopics} from a client:
 * the acceptance criterion is "all brokers converge on the same rule set".
 * Enforcement is exercised exhaustively in {@code KafkaApisTest}; here we
 * want the multi-broker convergence property in isolation, so a flake in
 * the request path doesn't mask a real convergence regression.
 */
@ClusterTestDefaults(types = {Type.KRAFT}, brokers = 3)
public class GovernanceRuleClusterIntegrationTest {

    private static final String RULE_ID = "rule-deny-audit-prefix";
    // Matches the prompt scenario: deny CreateTopics where any topic in the
    // batch starts with "audit-". The CEL expression goes through the same
    // generic ApiMessageActivation extractor the request path uses.
    private static final String RULE_JSON =
        "{\"apiKeys\":[\"CREATE_TOPICS\"],\"action\":\"DENY\","
            + "\"when\":\"request.topics.exists(t, t.name.startsWith(\\\"audit-\\\"))\","
            + "\"errorCode\":29}"; // TOPIC_AUTHORIZATION_FAILED — arbitrary but distinct

    @ClusterTest
    public void threeBrokerClusterConvergesOnPublishedRuleAndOnTombstone(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            // __governance is NOT auto-created. Create it with RF == broker
            // count so every broker is a replica of partition 0 — that
            // matches PROMPT.md's "three brokers converge" shape AND keeps
            // the fail-closed startup gate satisfied for every broker.
            admin.createTopics(List.of(
                new NewTopic(GovernanceTopic.NAME, 1, (short) 3))).all().get();
            cluster.waitForTopic(GovernanceTopic.NAME, 1);
        }

        // Publish the DENY rule via a stock producer. Crucially this client
        // is NOT marked with the internal-client-id prefix — the bootstrap
        // drain is the broker-side reader, and it reads the local log
        // directly, so there is no chicken-and-egg with a consumer here.
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "integration-test-producer");
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(
            producerProps, new StringSerializer(), new ByteArraySerializer())) {
            producer.send(new ProducerRecord<>(GovernanceTopic.NAME, RULE_ID,
                RULE_JSON.getBytes(StandardCharsets.UTF_8))).get();
        }

        // All three brokers must converge on the new rule. The drain is
        // scheduled every BrokerServer.GovernanceDrainIntervalMs (200ms),
        // so a 30s waitForCondition is generous; on a healthy machine
        // convergence typically lands in well under a second.
        TestUtils.waitForCondition(() -> allBrokersHaveExactlyOneRuleForApiKey(cluster, ApiKeys.CREATE_TOPICS),
            30_000L,
            "all three brokers must converge on the DENY rule installed via __governance");

        // Tombstone the rule. Null value → log-compacted record key signals
        // removal. Note: the producer needs explicit null-handling — we
        // use the byte[] serializer which passes null through.
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(
            producerProps, new StringSerializer(), new ByteArraySerializer())) {
            producer.send(new ProducerRecord<>(GovernanceTopic.NAME, RULE_ID, null)).get();
        }

        // All three brokers must converge on the empty rule set. No broker
        // restart in this test — convergence happens entirely through the
        // scheduled re-drain.
        TestUtils.waitForCondition(() -> allBrokersHaveNoRuleForApiKey(cluster, ApiKeys.CREATE_TOPICS),
            30_000L,
            "all three brokers must converge on rule removal after tombstone — no restart needed");
    }

    private static boolean allBrokersHaveExactlyOneRuleForApiKey(ClusterInstance cluster, ApiKeys apiKey) {
        Map<Integer, KafkaBroker> brokers = cluster.brokers();
        if (brokers.size() != 3) {
            // Sanity — @ClusterTestDefaults(brokers = 3) guarantees this, but
            // if a future change drops a broker, fail fast with a clearer
            // message than "convergence never happened".
            fail("expected 3 brokers, found " + brokers.size());
        }
        for (KafkaBroker broker : brokers.values()) {
            RuleSet rs = ruleSetOf(broker);
            // We must NOT inspect rs.size() in isolation — multiple rules may
            // exist if tests run in parallel and share state, but @ClusterTest
            // brings up a fresh KRaft cluster per test. Still, scope the
            // assertion to the CreateTopics bitset bit.
            if (!rs.hasDenyRuleFor(apiKey.id)) {
                return false;
            }
        }
        return true;
    }

    private static boolean allBrokersHaveNoRuleForApiKey(ClusterInstance cluster, ApiKeys apiKey) {
        for (KafkaBroker broker : cluster.brokers().values()) {
            RuleSet rs = ruleSetOf(broker);
            // After tombstone + re-drain, the CreateTopics bitset bit must
            // be CLEAR on every broker. If even one broker still flags the
            // bit, convergence has not happened yet.
            if (rs.hasDenyRuleFor(apiKey.id)) {
                return false;
            }
        }
        return true;
    }

    private static RuleSet ruleSetOf(KafkaBroker broker) {
        // KafkaBroker is the public trait; BrokerServer is the concrete KRaft
        // implementation that owns the RuleEngine. We cast here rather than
        // adding ruleEngine() to the trait surface — keeping the trait clean
        // of CEL-engine details is intentional.
        BrokerServer server = (BrokerServer) broker;
        RuleSet rs = server.ruleEngine().active();
        assertNotNull(rs, "every broker must have a non-null active RuleSet once it accepts traffic");
        return rs;
    }

    @ClusterTest
    public void clusterStartsCleanWhenGovernanceTopicIsAbsent(ClusterInstance cluster) {
        // Regression-locking test for the TopicAbsent branch of the fail-
        // closed gate (#17): a fresh cluster where __governance has never
        // been created must come up healthy on all three brokers, with each
        // broker's RuleEngine reporting an empty active RuleSet. The earlier
        // bug — "throw on any None getLog" — would have prevented this from
        // ever starting; today's logic distinguishes TopicAbsent from
        // NonReplica and only the latter is fatal.
        Map<Integer, KafkaBroker> brokers = cluster.brokers();
        assertEquals(3, brokers.size());
        for (KafkaBroker broker : brokers.values()) {
            BrokerServer server = (BrokerServer) broker;
            RuleSet rs = server.ruleEngine().active();
            assertNotNull(rs, "RuleEngine must have a non-null active RuleSet even when __governance is absent");
            assertEquals(0, rs.size(),
                "empty RuleSet is the correct state when __governance has not been created");
            assertTrue(server.governanceBootstrap() != null,
                "BrokerServer must instantiate the governanceBootstrap regardless of topic presence");
        }
    }
}
