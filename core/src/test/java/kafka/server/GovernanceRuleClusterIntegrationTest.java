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
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.rules.GovernanceTopic;
import org.apache.kafka.server.rules.RuleDecision;
import org.apache.kafka.server.rules.RuleEngine;
import org.apache.kafka.server.rules.RuleSet;
import org.apache.kafka.server.rules.extract.ApiMessageActivation;

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
@ClusterTestDefaults(types = {Type.KRAFT}, brokers = 3, serverProperties = {
    // governance.bypass.principals MUST be non-empty at broker startup
    // (BrokerServer.startup → ServerConfigs.GOVERNANCE_BYPASS_PRINCIPALS_CONFIG
    // empty-check rejects). PLAINTEXT inter-broker traffic in
    // KafkaClusterTestKit authenticates as User:ANONYMOUS, which must be on
    // the allow-list so replica fetchers, the __governance reader, and KRaft
    // metadata fetches are not subject to operator CEL rules.
    @ClusterConfigProperty(key = ServerConfigs.GOVERNANCE_BYPASS_PRINCIPALS_CONFIG, value = "User:ANONYMOUS")
})
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
            //
            // cleanup.policy=compact is REQUIRED by the operator contract
            // documented in PROMPT.md and enforced by
            // BrokerServer.requireGovernanceTopicCompactPolicy at every
            // broker startup. Without it, the topic would inherit the broker
            // default ("delete"), records would age out by retention.ms, and
            // a broker restart would refuse to open client traffic. The gate
            // does not fire in this test because the topic is created after
            // all three brokers are already running — but pinning the policy
            // here keeps the test honest to the documented contract and
            // safe against any future variant that adds a restart step.
            NewTopic governance = new NewTopic(GovernanceTopic.NAME, 1, (short) 3)
                .configs(Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT));
            admin.createTopics(List.of(governance)).all().get();
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
        //
        // The convergence predicate is intentionally a full end-to-end
        // semantic check (Codex P1 audit follow-up): we don't just verify
        // the bitset bit is set for CREATE_TOPICS on every broker — we
        // build an activation that actually matches the rule's `when`
        // expression and call RuleEngine.evaluate() on every broker. The
        // assertion is:
        //   - For an audit-prefixed topic name:  denied, rule id RULE_ID,
        //     error code 29 — on every broker.
        //   - For a non-prefixed topic name:    ALLOW — on every broker.
        // A regression that left some broker with a wrong rule (different
        // error code, different id, or a rule that fires on the wrong
        // expression) would pass the bitset-only check but fail here.
        TestUtils.waitForCondition(() -> allBrokersConvergedOnAuditPrefixDeny(cluster),
            30_000L,
            "all three brokers must converge on the DENY rule installed via __governance — " +
                "audit-prefixed CreateTopics must be denied with rule id '" + RULE_ID +
                "' and error code 29, and non-matching CreateTopics must be allowed");

        // Tombstone the rule. Null value → log-compacted record key signals
        // removal. Note: the producer needs explicit null-handling — we
        // use the byte[] serializer which passes null through.
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(
            producerProps, new StringSerializer(), new ByteArraySerializer())) {
            producer.send(new ProducerRecord<>(GovernanceTopic.NAME, RULE_ID, null)).get();
        }

        // All three brokers must converge on the empty rule set. No broker
        // restart in this test — convergence happens entirely through the
        // scheduled re-drain. Again, semantic check: even the previously-
        // matching audit-prefixed activation must now ALLOW on every broker.
        TestUtils.waitForCondition(() -> allBrokersAllowAuditPrefixAfterTombstone(cluster),
            30_000L,
            "all three brokers must converge on rule removal after tombstone — " +
                "audit-prefixed CreateTopics must be ALLOWED again (rule gone), no restart needed");
    }

    /**
     * Semantic convergence predicate (Codex P1 follow-up): every broker must
     *   1. have the bitset bit set for CREATE_TOPICS (fast-path engagement);
     *   2. DENY an audit-prefixed CreateTopics with the exact rule id and
     *      error code we published — proves the rule's content (not just
     *      "some" deny rule) converged byte-for-byte;
     *   3. ALLOW a non-prefixed CreateTopics — proves the rule's `when`
     *      expression actually distinguishes inputs and isn't a "deny all"
     *      that happens to also catch the audit case.
     */
    private static boolean allBrokersConvergedOnAuditPrefixDeny(ClusterInstance cluster) {
        Map<Integer, KafkaBroker> brokers = cluster.brokers();
        if (brokers.size() != 3) {
            fail("expected 3 brokers, found " + brokers.size());
        }
        for (KafkaBroker broker : brokers.values()) {
            BrokerServer server = (BrokerServer) broker;
            RuleEngine engine = server.ruleEngine();
            RuleSet rs = engine.active();
            assertNotNull(rs, "every broker must have a non-null active RuleSet once it accepts traffic");
            // Fast-path bit must be set first.
            if (!rs.hasDenyRuleFor(ApiKeys.CREATE_TOPICS.id)) {
                return false;
            }
            // Matching activation: an audit-prefixed topic must be denied
            // with the exact rule id and error code we published.
            RuleDecision matching = engine.evaluate(
                ApiKeys.CREATE_TOPICS, "client-x", false,
                () -> buildCreateTopicsActivation("audit-events"));
            if (!matching.denied()) return false;
            if (!RULE_ID.equals(matching.denyingRuleId())) return false;
            if (matching.errorCode() != 29) return false;
            // Non-matching activation: a plain topic must be allowed. If a
            // regression installed a "deny all CREATE_TOPICS" rule that
            // happens to match the audit case too, this catches it.
            RuleDecision nonMatching = engine.evaluate(
                ApiKeys.CREATE_TOPICS, "client-x", false,
                () -> buildCreateTopicsActivation("orders"));
            if (nonMatching.denied()) return false;
        }
        return true;
    }

    /**
     * Post-tombstone semantic predicate: even the previously-matching
     * audit-prefixed activation must now be ALLOWED on every broker. A
     * bitset-only check could pass here while a stale rule still fires
     * under a different code path; this catches it.
     */
    private static boolean allBrokersAllowAuditPrefixAfterTombstone(ClusterInstance cluster) {
        for (KafkaBroker broker : cluster.brokers().values()) {
            BrokerServer server = (BrokerServer) broker;
            RuleEngine engine = server.ruleEngine();
            RuleSet rs = engine.active();
            assertNotNull(rs);
            // Bitset bit must be clear AND a previously-matching activation
            // must now ALLOW. Either failure means convergence has not landed.
            if (rs.hasDenyRuleFor(ApiKeys.CREATE_TOPICS.id)) return false;
            RuleDecision d = engine.evaluate(
                ApiKeys.CREATE_TOPICS, "client-x", false,
                () -> buildCreateTopicsActivation("audit-events"));
            if (d.denied()) return false;
        }
        return true;
    }

    /**
     * Build a CEL activation map that matches the shape the broker actually
     * produces on the request path. Going through {@link ApiMessageActivation}
     * (not a hand-rolled Map) guarantees the test sees the same envelope and
     * the same field-name discipline a real CreateTopics request would; if a
     * field-extractor regression broke the broker, this assertion would catch
     * it too.
     */
    private static Map<String, Object> buildCreateTopicsActivation(String topicName) {
        CreatableTopicCollection topics = new CreatableTopicCollection();
        topics.add(new CreatableTopic().setName(topicName));
        CreateTopicsRequestData data = new CreateTopicsRequestData().setTopics(topics);
        return ApiMessageActivation.requestActivation(data);
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
