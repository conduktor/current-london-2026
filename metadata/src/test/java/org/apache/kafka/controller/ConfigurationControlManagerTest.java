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

package org.apache.kafka.controller;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.metadata.KafkaConfigSchema;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.EligibleLeaderReplicasVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.config.ConfigSynonym;
import org.apache.kafka.server.policy.AlterConfigPolicy;
import org.apache.kafka.server.policy.AlterConfigPolicy.RequestMetadata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.APPEND;
import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.DELETE;
import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SUBTRACT;
import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import static org.apache.kafka.common.metadata.MetadataRecordType.CONFIG_RECORD;
import static org.apache.kafka.server.config.ConfigSynonym.HOURS_TO_MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class ConfigurationControlManagerTest {

    static final Map<ConfigResource.Type, ConfigDef> CONFIGS = new HashMap<>();

    static {
        CONFIGS.put(BROKER, new ConfigDef().
            define("foo.bar", ConfigDef.Type.LIST, "1", ConfigDef.Importance.HIGH, "foo bar").
            define("baz", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "baz").
            define("quux", ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "quux").
            define(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
                ConfigDef.Type.INT, "1", ConfigDef.Importance.HIGH, "min.isr"));

        CONFIGS.put(TOPIC, new ConfigDef().
            define("abc", ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "abc").
            define("def", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "def").
            define("ghi", ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH, "ghi").
            define("quuux", ConfigDef.Type.LONG, ConfigDef.Importance.HIGH, "quux").
            define(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, ""));
    }

    public static final Map<String, List<ConfigSynonym>> SYNONYMS = new HashMap<>();

    static {
        SYNONYMS.put("abc", Collections.singletonList(new ConfigSynonym("foo.bar")));
        SYNONYMS.put("def", Collections.singletonList(new ConfigSynonym("baz")));
        SYNONYMS.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
            Collections.singletonList(new ConfigSynonym(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG)));
        SYNONYMS.put("quuux", Collections.singletonList(new ConfigSynonym("quux", HOURS_TO_MILLISECONDS)));
        SYNONYMS.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Collections.singletonList(new ConfigSynonym(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG)));
    }

    static final KafkaConfigSchema SCHEMA = new KafkaConfigSchema(CONFIGS, SYNONYMS);

    static final ConfigResource BROKER0 = new ConfigResource(BROKER, "0");
    static final ConfigResource MYTOPIC = new ConfigResource(TOPIC, "mytopic");

    static class TestExistenceChecker implements Consumer<ConfigResource> {
        static final TestExistenceChecker INSTANCE = new TestExistenceChecker();

        @Override
        public void accept(ConfigResource resource) {
            if (!resource.name().startsWith("Existing")) {
                throw new UnknownTopicOrPartitionException("Unknown resource.");
            }
        }
    }

    @SuppressWarnings("unchecked")
    static <A, B> Map<A, B> toMap(Entry... entries) {
        Map<A, B> map = new LinkedHashMap<>();
        for (Entry<A, B> entry : entries) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    static <A, B> Entry<A, B> entry(A a, B b) {
        return new SimpleImmutableEntry<>(a, b);
    }

    @Test
    public void testReplay() throws Exception {
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setKafkaConfigSchema(SCHEMA).
            build();
        assertEquals(Collections.emptyMap(), manager.getConfigs(BROKER0));
        manager.replay(new ConfigRecord().
            setResourceType(BROKER.id()).setResourceName("0").
            setName("foo.bar").setValue("1,2"));
        assertEquals(Collections.singletonMap("foo.bar", "1,2"),
            manager.getConfigs(BROKER0));
        manager.replay(new ConfigRecord().
            setResourceType(BROKER.id()).setResourceName("0").
            setName("foo.bar").setValue(null));
        assertEquals(Collections.emptyMap(), manager.getConfigs(BROKER0));
        manager.replay(new ConfigRecord().
            setResourceType(TOPIC.id()).setResourceName("mytopic").
            setName("abc").setValue("x,y,z"));
        manager.replay(new ConfigRecord().
            setResourceType(TOPIC.id()).setResourceName("mytopic").
            setName("def").setValue("blah"));
        assertEquals(toMap(entry("abc", "x,y,z"), entry("def", "blah")),
            manager.getConfigs(MYTOPIC));
        assertEquals("x,y,z", manager.getTopicConfig(MYTOPIC.name(), "abc").value());
    }

    @Test
    public void testIncrementalAlterConfigs() {
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setKafkaConfigSchema(SCHEMA).
            build();

        ControllerResult<Map<ConfigResource, ApiError>> result = manager.
            incrementalAlterConfigs(toMap(entry(BROKER0, toMap(
                entry("baz", entry(SUBTRACT, "abc")),
                entry("quux", entry(SET, "abc")))),
                entry(MYTOPIC, toMap(entry("abc", entry(APPEND, "123"))))),
                true);

        assertEquals(ControllerResult.atomicOf(Collections.singletonList(new ApiMessageAndVersion(
                new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
                    setName("abc").setValue("123"), CONFIG_RECORD.highestSupportedVersion())),
                toMap(entry(BROKER0, new ApiError(Errors.INVALID_CONFIG,
                            "Can't SUBTRACT to key baz because its type is not LIST.")),
                    entry(MYTOPIC, ApiError.NONE))), result);

        RecordTestUtils.replayAll(manager, result.records());

        assertEquals(ControllerResult.atomicOf(Collections.singletonList(new ApiMessageAndVersion(
                new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
                    setName("abc").setValue(null), CONFIG_RECORD.highestSupportedVersion())),
                toMap(entry(MYTOPIC, ApiError.NONE))),
            manager.incrementalAlterConfigs(toMap(entry(MYTOPIC, toMap(
                entry("abc", entry(DELETE, "xyz"))))),
                true));
    }

    @Test
    public void testIncrementalAlterConfig() {
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setKafkaConfigSchema(SCHEMA).
            build();
        Map<String, Entry<AlterConfigOp.OpType, String>> keyToOps = toMap(entry("abc", entry(APPEND, "123")));

        ControllerResult<ApiError> result = manager.
            incrementalAlterConfig(MYTOPIC, keyToOps, true);

        assertEquals(ControllerResult.atomicOf(Collections.singletonList(new ApiMessageAndVersion(
                new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
                    setName("abc").setValue("123"), CONFIG_RECORD.highestSupportedVersion())),
            ApiError.NONE), result);

        RecordTestUtils.replayAll(manager, result.records());

        assertEquals(ControllerResult.atomicOf(Collections.singletonList(new ApiMessageAndVersion(
                    new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
                        setName("abc").setValue(null), CONFIG_RECORD.highestSupportedVersion())),
                ApiError.NONE),
            manager.incrementalAlterConfig(MYTOPIC, toMap(entry("abc", entry(DELETE, "xyz"))), true));
    }

    @Test
    public void testIncrementalAlterMultipleConfigValues() {
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setKafkaConfigSchema(SCHEMA).
            build();

        ControllerResult<Map<ConfigResource, ApiError>> result = manager.
            incrementalAlterConfigs(toMap(entry(MYTOPIC, toMap(entry("abc", entry(APPEND, "123,456,789"))))), true);

        assertEquals(ControllerResult.atomicOf(Collections.singletonList(new ApiMessageAndVersion(
                new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
                    setName("abc").setValue("123,456,789"), CONFIG_RECORD.highestSupportedVersion())),
                toMap(entry(MYTOPIC, ApiError.NONE))), result);

        RecordTestUtils.replayAll(manager, result.records());

        // It's ok for the appended value to be already present
        result = manager
            .incrementalAlterConfigs(toMap(entry(MYTOPIC, toMap(entry("abc", entry(APPEND, "123,456"))))), true);
        assertEquals(
            ControllerResult.atomicOf(Collections.emptyList(), toMap(entry(MYTOPIC, ApiError.NONE))),
            result
        );
        RecordTestUtils.replayAll(manager, result.records());

        result = manager
            .incrementalAlterConfigs(toMap(entry(MYTOPIC, toMap(entry("abc", entry(SUBTRACT, "123,456"))))), true);
        assertEquals(ControllerResult.atomicOf(Collections.singletonList(new ApiMessageAndVersion(
                new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
                    setName("abc").setValue("789"), CONFIG_RECORD.highestSupportedVersion())),
                toMap(entry(MYTOPIC, ApiError.NONE))),
                result);
        RecordTestUtils.replayAll(manager, result.records());

        // It's ok for the deleted value not to be present
        result = manager
            .incrementalAlterConfigs(toMap(entry(MYTOPIC, toMap(entry("abc", entry(SUBTRACT, "123456"))))), true);
        assertEquals(
            ControllerResult.atomicOf(Collections.emptyList(), toMap(entry(MYTOPIC, ApiError.NONE))),
            result
        );
        RecordTestUtils.replayAll(manager, result.records());

        assertEquals("789", manager.getConfigs(MYTOPIC).get("abc"));
    }

    @Test
    public void testIncrementalAlterConfigsWithoutExistence() {
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setKafkaConfigSchema(SCHEMA).
            setExistenceChecker(TestExistenceChecker.INSTANCE).
            build();
        ConfigResource existingTopic = new ConfigResource(TOPIC, "ExistingTopic");

        ControllerResult<Map<ConfigResource, ApiError>> result = manager.
            incrementalAlterConfigs(toMap(entry(BROKER0, toMap(
                entry("quux", entry(SET, "1")))),
                entry(existingTopic, toMap(entry("def", entry(SET, "newVal"))))),
                false);

        assertEquals(ControllerResult.atomicOf(Collections.singletonList(new ApiMessageAndVersion(
                new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("ExistingTopic").
                    setName("def").setValue("newVal"), CONFIG_RECORD.highestSupportedVersion())),
            toMap(entry(BROKER0, new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION,
                    "Unknown resource.")),
                entry(existingTopic, ApiError.NONE))), result);
    }

    private static class MockAlterConfigsPolicy implements AlterConfigPolicy {
        private final List<RequestMetadata> expecteds;
        private final AtomicLong index = new AtomicLong(0);

        MockAlterConfigsPolicy(List<RequestMetadata> expecteds) {
            this.expecteds = expecteds;
        }

        @Override
        public void validate(RequestMetadata actual) throws PolicyViolationException {
            long curIndex = index.getAndIncrement();
            if (curIndex >= expecteds.size()) {
                throw new PolicyViolationException("Unexpected config alteration: index " +
                    "out of range at " + curIndex);
            }
            RequestMetadata expected = expecteds.get((int) curIndex);
            if (!expected.equals(actual)) {
                throw new PolicyViolationException("Expected: " + expected +
                    ". Got: " + actual);
            }
        }

        @Override
        public void close() throws Exception {
            // nothing to do
        }

        @Override
        public void configure(Map<String, ?> configs) {
            // nothing to do
        }
    }

    @Test
    public void testIncrementalAlterConfigsWithPolicy() {
        MockAlterConfigsPolicy policy = new MockAlterConfigsPolicy(asList(
            new RequestMetadata(MYTOPIC, Collections.emptyMap()),
            new RequestMetadata(BROKER0, toMap(
                entry("foo.bar", "123"),
                entry("quux", "456"),
                entry("broker.config.to.remove", null)))));
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setKafkaConfigSchema(SCHEMA).
            setAlterConfigPolicy(Optional.of(policy)).
            build();
        // Existing configs should not be passed to the policy
        manager.replay(new ConfigRecord().setResourceType(BROKER.id()).setResourceName("0").
                setName("broker.config").setValue("123"));
        manager.replay(new ConfigRecord().setResourceType(TOPIC.id()).setResourceName(MYTOPIC.name()).
                setName("topic.config").setValue("123"));
        manager.replay(new ConfigRecord().setResourceType(BROKER.id()).setResourceName("0").
                setName("broker.config.to.remove").setValue("123"));
        assertEquals(ControllerResult.atomicOf(asList(new ApiMessageAndVersion(
                new ConfigRecord().setResourceType(BROKER.id()).setResourceName("0").
                    setName("foo.bar").setValue("123"), CONFIG_RECORD.highestSupportedVersion()), new ApiMessageAndVersion(
                                new ConfigRecord().setResourceType(BROKER.id()).setResourceName("0").
                                        setName("quux").setValue("456"), CONFIG_RECORD.highestSupportedVersion()), new ApiMessageAndVersion(
                                            new ConfigRecord().setResourceType(BROKER.id()).setResourceName("0").
                                                    setName("broker.config.to.remove").setValue(null), CONFIG_RECORD.highestSupportedVersion())
                ),
                toMap(entry(MYTOPIC, new ApiError(Errors.POLICY_VIOLATION,
                    "Expected: AlterConfigPolicy.RequestMetadata(resource=ConfigResource(" +
                    "type=TOPIC, name='mytopic'), configs={}). Got: " +
                    "AlterConfigPolicy.RequestMetadata(resource=ConfigResource(" +
                    "type=TOPIC, name='mytopic'), configs={foo.bar=123})")),
                entry(BROKER0, ApiError.NONE))),
            manager.incrementalAlterConfigs(toMap(entry(MYTOPIC, toMap(
                entry("foo.bar", entry(SET, "123")))),
                entry(BROKER0, toMap(
                        entry("foo.bar", entry(SET, "123")),
                        entry("quux", entry(SET, "456")),
                        entry("broker.config.to.remove", entry(DELETE, null))
                ))),
                true));
    }

    private static class CheckForNullValuesPolicy implements AlterConfigPolicy {
        @Override
        public void validate(RequestMetadata actual) throws PolicyViolationException {
            actual.configs().forEach((key, value) -> {
                if (value == null) {
                    throw new PolicyViolationException("Legacy Alter Configs should not see null values");
                }
            });
        }

        @Override
        public void close() throws Exception {
            // empty
        }

        @Override
        public void configure(Map<String, ?> configs) {
            // empty
        }
    }

    @Test
    public void testLegacyAlterConfigs() {
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setKafkaConfigSchema(SCHEMA).
            setAlterConfigPolicy(Optional.of(new CheckForNullValuesPolicy())).
            build();
        List<ApiMessageAndVersion> expectedRecords1 = asList(
            new ApiMessageAndVersion(new ConfigRecord().
                setResourceType(TOPIC.id()).setResourceName("mytopic").
                setName("abc").setValue("456"), CONFIG_RECORD.highestSupportedVersion()),
            new ApiMessageAndVersion(new ConfigRecord().
                setResourceType(TOPIC.id()).setResourceName("mytopic").
                setName("def").setValue("901"), CONFIG_RECORD.highestSupportedVersion()));
        assertEquals(ControllerResult.atomicOf(
                expectedRecords1, toMap(entry(MYTOPIC, ApiError.NONE))),
            manager.legacyAlterConfigs(
                toMap(entry(MYTOPIC, toMap(entry("abc", "456"), entry("def", "901")))),
                true));
        for (ApiMessageAndVersion message : expectedRecords1) {
            manager.replay((ConfigRecord) message.message());
        }
        assertEquals(ControllerResult.atomicOf(Collections.singletonList(
            new ApiMessageAndVersion(
                new ConfigRecord()
                    .setResourceType(TOPIC.id())
                    .setResourceName("mytopic")
                    .setName("abc")
                    .setValue(null),
                CONFIG_RECORD.highestSupportedVersion())),
            toMap(entry(MYTOPIC, ApiError.NONE))),
            manager.legacyAlterConfigs(toMap(entry(MYTOPIC, toMap(entry("def", "901")))),
                true));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testMaybeGenerateElrSafetyRecords(boolean setStaticConfig) {
        ConfigurationControlManager.Builder builder = new ConfigurationControlManager.Builder().
            setKafkaConfigSchema(SCHEMA);
        if (setStaticConfig) {
            builder.setStaticConfig(Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2"));
        }
        ConfigurationControlManager manager = builder.build();
        Map<String, Entry<AlterConfigOp.OpType, String>> keyToOps =
            toMap(entry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, entry(SET, "3")));
        ConfigResource brokerConfigResource = new ConfigResource(ConfigResource.Type.BROKER, "1");
        ControllerResult<ApiError> result = manager.incrementalAlterConfig(brokerConfigResource, keyToOps, true);
        assertEquals(Collections.emptySet(), manager.brokersWithConfigs());

        assertEquals(ControllerResult.atomicOf(Collections.singletonList(new ApiMessageAndVersion(
            new ConfigRecord().setResourceType(BROKER.id()).setResourceName("1").
                setName(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).setValue("3"), (short) 0)),
            ApiError.NONE), result);

        RecordTestUtils.replayAll(manager, result.records());
        assertEquals(Set.of(1), manager.brokersWithConfigs());

        List<ApiMessageAndVersion> records = new ArrayList<>();
        String effectiveMinInsync = setStaticConfig ? "2" : "1";
        assertEquals("Generating cluster-level min.insync.replicas of " +
            effectiveMinInsync + ". Removing broker-level min.insync.replicas " +
            "for brokers: 1.", manager.maybeGenerateElrSafetyRecords(records));

        assertEquals(Arrays.asList(new ApiMessageAndVersion(
            new ConfigRecord().
                setResourceType(BROKER.id()).
                setResourceName("").
                setName(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).
                setValue(effectiveMinInsync), (short) 0),
            new ApiMessageAndVersion(new ConfigRecord().
                setResourceType(BROKER.id()).
                setResourceName("1").
                setName(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).
                setValue(null), (short) 0)),
            records);
        RecordTestUtils.replayAll(manager, records);
        assertEquals(Collections.emptySet(), manager.brokersWithConfigs());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testRejectMinIsrChangeWhenElrEnabled(boolean removal) {
        FeatureControlManager featureManager = new FeatureControlManager.Builder().
            setQuorumFeatures(new QuorumFeatures(0,
                QuorumFeatures.defaultSupportedFeatureMap(true),
                Collections.emptyList())).
            build();
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setStaticConfig(Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")).
            setFeatureControl(featureManager).
            setKafkaConfigSchema(SCHEMA).
            build();
        ControllerResult<ApiError> result = manager.updateFeatures(
            Collections.singletonMap(EligibleLeaderReplicasVersion.FEATURE_NAME,
                EligibleLeaderReplicasVersion.ELRV_1.featureLevel()),
            Collections.singletonMap(EligibleLeaderReplicasVersion.FEATURE_NAME,
                FeatureUpdate.UpgradeType.UPGRADE),
            false);
        assertNotNull(result.response());
        assertEquals(Errors.NONE, result.response().error());
        RecordTestUtils.replayAll(manager, result.records());
        RecordTestUtils.replayAll(featureManager, result.records());

        // Broker level update is not allowed.
        result = manager.incrementalAlterConfig(new ConfigResource(ConfigResource.Type.BROKER, "1"),
            toMap(entry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
                removal ? entry(DELETE, null) : entry(SET, "3"))),
            true);
        assertEquals(Errors.INVALID_CONFIG, result.response().error());
        assertEquals("Broker-level min.insync.replicas cannot be altered while ELR is enabled.",
            result.response().message());

        // Cluster level removal is not allowed.
        result = manager.incrementalAlterConfig(new ConfigResource(ConfigResource.Type.BROKER, ""),
            toMap(entry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
                removal ? entry(DELETE, null) : entry(SET, "3"))),
            true);
        if (removal) {
            assertEquals(Errors.INVALID_CONFIG, result.response().error());
            assertEquals("Cluster-level min.insync.replicas cannot be removed while ELR is enabled.",
                    result.response().message());
        } else {
            assertEquals(Errors.NONE, result.response().error());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testElrUpgrade(boolean isMetadataVersionElrEnabled) {
        FeatureControlManager featureManager = new FeatureControlManager.Builder().
            setQuorumFeatures(new QuorumFeatures(0,
                QuorumFeatures.defaultSupportedFeatureMap(true),
                Collections.emptyList())).
            setMetadataVersion(isMetadataVersionElrEnabled ? MetadataVersion.IBP_4_0_IV1 : MetadataVersion.IBP_4_0_IV0).
            build();
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setStaticConfig(Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")).
            setFeatureControl(featureManager).
            setKafkaConfigSchema(SCHEMA).
            build();
        assertFalse(featureManager.isElrFeatureEnabled());
        ControllerResult<ApiError> result = manager.updateFeatures(
            Collections.singletonMap(EligibleLeaderReplicasVersion.FEATURE_NAME,
                EligibleLeaderReplicasVersion.ELRV_1.featureLevel()),
            Collections.singletonMap(EligibleLeaderReplicasVersion.FEATURE_NAME,
                FeatureUpdate.UpgradeType.UPGRADE),
            false);
        assertNotNull(result.response());
        if (isMetadataVersionElrEnabled) {
            assertEquals(Errors.NONE, result.response().error());
            RecordTestUtils.replayAll(manager, result.records());
            RecordTestUtils.replayAll(featureManager, result.records());
            assertTrue(featureManager.isElrFeatureEnabled());
        } else {
            assertEquals(Errors.INVALID_UPDATE_VERSION, result.response().error());
        }
    }

    /**
     * R40b: the precondition map on {@code incrementalAlterConfigs} closes the R38 TOCTOU residual.
     * Scenario simulated here:
     *   1. Broker preflight sees view backing = "B_old" and authorizes READ on it.
     *   2. Between preflight and controller commit, the backing is rebound to "B_new" via a
     *      separate alter (the in-test {@code replay} of a ConfigRecord stands in for that
     *      concurrent commit landing first in the event loop).
     *   3. The original alter — a predicate-only mutation — must now be rejected because the
     *      controller's current backing no longer matches what the broker authorized against.
     */
    @Test
    public void testR40bPredicateOnlyAlterRejectedWhenBackingRaced() {
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setKafkaConfigSchema(SCHEMA).
            build();

        manager.replay(new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
            setName("view.backing.topic").setValue("B_new"));

        Map<ConfigResource, Map<String, String>> preconditions =
            toMap(entry(MYTOPIC, toMap(entry("view.backing.topic", "B_old"))));
        ControllerResult<Map<ConfigResource, ApiError>> result = manager.incrementalAlterConfigs(
            toMap(entry(MYTOPIC, toMap(entry("view.cel.predicate", entry(SET, "true"))))),
            preconditions,
            false);

        assertEquals(Collections.emptyList(), result.records(),
            "no records should be emitted when the precondition fails");
        ApiError err = result.response().get(MYTOPIC);
        assertEquals(Errors.INVALID_REQUEST, err.error());
        assertTrue(err.message().contains("view.backing.topic"),
            "error message must name the failing key, got: " + err.message());
    }

    @Test
    public void testR40bPredicateOnlyAlterAdmittedWhenBackingMatches() {
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setKafkaConfigSchema(SCHEMA).
            build();
        manager.replay(new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
            setName("view.backing.topic").setValue("B_old"));

        Map<ConfigResource, Map<String, String>> preconditions =
            toMap(entry(MYTOPIC, toMap(entry("view.backing.topic", "B_old"))));
        ControllerResult<Map<ConfigResource, ApiError>> result = manager.incrementalAlterConfigs(
            toMap(entry(MYTOPIC, toMap(entry("view.cel.predicate", entry(SET, "true"))))),
            preconditions,
            false);

        assertEquals(ApiError.NONE, result.response().get(MYTOPIC));
        assertEquals(1, result.records().size());
        ConfigRecord emitted = (ConfigRecord) result.records().get(0).message();
        assertEquals("view.cel.predicate", emitted.name());
        assertEquals("true", emitted.value());
    }

    @Test
    public void testR40bLegacyAlterRejectedWhenBackingRaced() {
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setKafkaConfigSchema(SCHEMA).
            build();
        manager.replay(new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
            setName("view.backing.topic").setValue("B_new"));

        Map<ConfigResource, Map<String, String>> preconditions =
            toMap(entry(MYTOPIC, toMap(entry("view.backing.topic", "B_old"))));
        ControllerResult<Map<ConfigResource, ApiError>> result = manager.legacyAlterConfigs(
            toMap(entry(MYTOPIC, toMap(
                entry("view.backing.topic", "B_new"),
                entry("view.cel.predicate", "true")))),
            preconditions,
            false);

        assertEquals(Collections.emptyList(), result.records());
        assertEquals(Errors.INVALID_REQUEST, result.response().get(MYTOPIC).error());
    }

    /**
     * Precondition pinned at the absent-key value (null) — exercises the {@code null}-vs-string
     * matching branch of {@link ConfigurationControlManager#checkPreconditions} so a future change
     * that conflates "no precondition" with "expect null" gets caught here.
     */
    @Test
    public void testR40bAbsentBackingPrecondition() {
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setKafkaConfigSchema(SCHEMA).
            build();

        Map<String, String> expected = new HashMap<>();
        expected.put("view.backing.topic", null);
        Map<ConfigResource, Map<String, String>> preconditions = toMap(entry(MYTOPIC, expected));

        ControllerResult<Map<ConfigResource, ApiError>> ok = manager.incrementalAlterConfigs(
            toMap(entry(MYTOPIC, toMap(entry("def", entry(SET, "x"))))),
            preconditions,
            false);
        assertEquals(ApiError.NONE, ok.response().get(MYTOPIC),
            "absent-backing precondition should pass when no backing is set");

        manager.replay(new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
            setName("view.backing.topic").setValue("B_set"));

        ControllerResult<Map<ConfigResource, ApiError>> nok = manager.incrementalAlterConfigs(
            toMap(entry(MYTOPIC, toMap(entry("def", entry(SET, "y"))))),
            preconditions,
            false);
        assertEquals(Errors.INVALID_REQUEST, nok.response().get(MYTOPIC).error(),
            "absent-backing precondition should fail once a backing is set");
    }

    /**
     * R53 (Codex Finding): view-ness is immutable after topic creation. An
     * incrementalAlterConfigs that sets {@code view.backing.topic} on a topic which is not
     * already a view must be rejected with INVALID_CONFIG. The exploit chain Codex traced:
     * AddPartitionsToTxn(T-0) is accepted while T is a regular topic, an alter then sets
     * view.backing.topic on T, EndTxn issues markers for T-0, the broker rejects with
     * INVALID_TOPIC_EXCEPTION, and TransactionMarkerRequestCompletionHandler throws
     * IllegalStateException in its default case — wedging the transaction and blocking
     * READ_COMMITTED consumers at the LSO.
     */
    @Test
    public void testR53RejectIncrementalAlterTurningRegularTopicIntoView() {
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setKafkaConfigSchema(SCHEMA).
            build();

        // mytopic exists as a regular topic (no view.backing.topic). Seed via a non-view config.
        manager.replay(new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
            setName("def").setValue("regular"));

        ControllerResult<Map<ConfigResource, ApiError>> result = manager.incrementalAlterConfigs(
            toMap(entry(MYTOPIC, toMap(entry("view.backing.topic", entry(SET, "some_backing"))))),
            false);

        assertEquals(Collections.emptyList(), result.records(),
            "no records should be emitted when the conversion is rejected");
        ApiError err = result.response().get(MYTOPIC);
        assertEquals(Errors.INVALID_CONFIG, err.error());
        assertTrue(err.message().contains("immutable"),
            "error message must explain the view-ness immutability rule, got: " + err.message());
        assertTrue(err.message().contains("mytopic"),
            "error message must name the topic being converted, got: " + err.message());
    }

    /**
     * R53 companion: the legacy AlterConfigs full-replace path is the same threat surface — a
     * legacy alter that includes view.backing.topic on a topic that did not previously have it
     * must also be rejected.
     */
    @Test
    public void testR53RejectLegacyAlterTurningRegularTopicIntoView() {
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setKafkaConfigSchema(SCHEMA).
            build();

        manager.replay(new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
            setName("def").setValue("regular"));

        ControllerResult<Map<ConfigResource, ApiError>> result = manager.legacyAlterConfigs(
            toMap(entry(MYTOPIC, toMap(entry("view.backing.topic", "some_backing")))),
            false);

        assertEquals(Collections.emptyList(), result.records());
        ApiError err = result.response().get(MYTOPIC);
        assertEquals(Errors.INVALID_CONFIG, err.error());
        assertTrue(err.message().contains("immutable"),
            "legacy alter rejection must also explain immutability, got: " + err.message());
    }

    /**
     * R53 negative: altering an existing view (view.backing.topic already set) to point to a
     * different backing must still succeed. Without this case, a regression that blanket-rejects
     * any view.backing.topic SET would pass the rejection tests above.
     */
    @Test
    public void testR53AllowRebindOnExistingView() {
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setKafkaConfigSchema(SCHEMA).
            build();
        // mytopic was created as a view (has view.backing.topic = B_old).
        manager.replay(new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
            setName("view.backing.topic").setValue("B_old"));

        ControllerResult<Map<ConfigResource, ApiError>> result = manager.incrementalAlterConfigs(
            toMap(entry(MYTOPIC, toMap(entry("view.backing.topic", entry(SET, "B_new"))))),
            false);

        assertEquals(ApiError.NONE, result.response().get(MYTOPIC),
            "view-to-view rebind on an existing view must remain allowed");
        assertEquals(1, result.records().size());
        ConfigRecord emitted = (ConfigRecord) result.records().get(0).message();
        assertEquals("view.backing.topic", emitted.name());
        assertEquals("B_new", emitted.value());
    }

    /**
     * R53 negative: a create-time alter (newlyCreatedResource=true path) which establishes
     * view.backing.topic as part of topic creation must NOT trip the immutability check —
     * that path is the supported "create the topic as a view" flow used by createTopics.
     */
    @Test
    public void testR53AllowViewBackingAtCreate() {
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setKafkaConfigSchema(SCHEMA).
            build();

        // Use the internal createTopicConfigs path: newlyCreatedResource=true via the
        // incrementalAlterConfigs(newlyCreatedResource=true) overload.
        ControllerResult<Map<ConfigResource, ApiError>> result = manager.incrementalAlterConfigs(
            toMap(entry(MYTOPIC, toMap(entry("view.backing.topic", entry(SET, "fresh_backing"))))),
            true);

        assertEquals(ApiError.NONE, result.response().get(MYTOPIC),
            "create-time view.backing.topic must remain allowed");
    }

    /**
     * R55 (Codex Finding): view-ness is immutable in BOTH directions. R53 covers regular→view;
     * R55 covers view→regular. A legacy AlterConfigs full-replace that omits all view.* keys on
     * an existing view generates implicit-DELETE records for the omitted keys (see
     * legacyAlterConfigResource at CCM:633-642). The LogConfig all-or-none invariant permits
     * present=0 as a valid "regular topic" post-state, so the schema layer does not catch the
     * full-strip. Without R55, KafkaApis.isViewTopic would flip to false: produces would stop
     * hitting the read-only rejection and fetches would stop redirecting to the backing topic,
     * letting a principal who could not READ the backing serve attacker-controlled records to
     * consumers who still hold READ on the view.
     */
    @Test
    public void testR55RejectLegacyAlterStrippingViewKeysByOmission() {
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setKafkaConfigSchema(SCHEMA).
            build();

        // Seed mytopic as a complete view: all three view.* keys set.
        manager.replay(new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
            setName("view.backing.topic").setValue("B"));
        manager.replay(new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
            setName("view.cel.predicate").setValue("true"));
        manager.replay(new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
            setName("view.offset.mode").setValue("sparse"));

        // Legacy alter with only a non-view key. Full-replace semantics generate implicit
        // deletes for the omitted view.* keys.
        ControllerResult<Map<ConfigResource, ApiError>> result = manager.legacyAlterConfigs(
            toMap(entry(MYTOPIC, toMap(entry("retention.ms", "60000")))),
            false);

        assertEquals(Collections.emptyList(), result.records(),
            "no records should be emitted when the view→regular strip is rejected");
        ApiError err = result.response().get(MYTOPIC);
        assertEquals(Errors.INVALID_CONFIG, err.error());
        assertTrue(err.message().contains("immutable"),
            "error message must explain the view-ness immutability rule, got: " + err.message());
        assertTrue(err.message().contains("mytopic"),
            "error message must name the topic being stripped, got: " + err.message());
    }

    /**
     * R55: the same threat via the IncrementalAlterConfigs surface. Explicit DELETE on
     * view.backing.topic alone (predicate + offsetMode would then be partial, but the post-state
     * check sees backing absent so willBeView=false) — or DELETE on all three keys in one
     * request — must be rejected. The single-key DELETE case is the cleaner attacker shape: one
     * operation, one round-trip, no partial-state window.
     */
    @Test
    public void testR55RejectIncrementalDeleteOfViewBacking() {
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setKafkaConfigSchema(SCHEMA).
            build();

        manager.replay(new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
            setName("view.backing.topic").setValue("B"));
        manager.replay(new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
            setName("view.cel.predicate").setValue("true"));
        manager.replay(new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
            setName("view.offset.mode").setValue("sparse"));

        ControllerResult<Map<ConfigResource, ApiError>> result = manager.incrementalAlterConfigs(
            toMap(entry(MYTOPIC, toMap(entry("view.backing.topic", entry(DELETE, ""))))),
            false);

        assertEquals(Collections.emptyList(), result.records());
        ApiError err = result.response().get(MYTOPIC);
        assertEquals(Errors.INVALID_CONFIG, err.error());
        assertTrue(err.message().contains("immutable"),
            "error message must explain the view-ness immutability rule, got: " + err.message());
    }

    /**
     * R55: incremental DELETE on all three view keys in one atomic request must also be
     * rejected. Without this case, a regression that only blocked single-key DELETE would still
     * let an attacker batch-delete all three in one shot.
     */
    @Test
    public void testR55RejectIncrementalDeleteOfAllThreeViewKeys() {
        ConfigurationControlManager manager = new ConfigurationControlManager.Builder().
            setKafkaConfigSchema(SCHEMA).
            build();

        manager.replay(new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
            setName("view.backing.topic").setValue("B"));
        manager.replay(new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
            setName("view.cel.predicate").setValue("true"));
        manager.replay(new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
            setName("view.offset.mode").setValue("sparse"));

        ControllerResult<Map<ConfigResource, ApiError>> result = manager.incrementalAlterConfigs(
            toMap(entry(MYTOPIC, toMap(
                entry("view.backing.topic", entry(DELETE, "")),
                entry("view.cel.predicate", entry(DELETE, "")),
                entry("view.offset.mode", entry(DELETE, ""))))),
            false);

        assertEquals(Collections.emptyList(), result.records());
        ApiError err = result.response().get(MYTOPIC);
        assertEquals(Errors.INVALID_CONFIG, err.error());
        assertTrue(err.message().contains("immutable"),
            "error message must explain the view-ness immutability rule, got: " + err.message());
    }
}
