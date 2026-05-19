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

import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.metadata.ClearElrRecord;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.KafkaConfigSchema;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.EligibleLeaderReplicasVersion;
import org.apache.kafka.server.mutable.BoundedList;
import org.apache.kafka.server.policy.AlterConfigPolicy;
import org.apache.kafka.server.policy.AlterConfigPolicy.RequestMetadata;
import org.apache.kafka.server.views.ViewTopicConfig;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineHashSet;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.APPEND;
import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;
import static org.apache.kafka.common.config.TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG;
import static org.apache.kafka.common.metadata.MetadataRecordType.CONFIG_RECORD;
import static org.apache.kafka.common.protocol.Errors.INVALID_CONFIG;
import static org.apache.kafka.common.protocol.Errors.INVALID_REQUEST;
import static org.apache.kafka.controller.QuorumController.MAX_RECORDS_PER_USER_OP;


public class ConfigurationControlManager {
    public static final ConfigResource DEFAULT_NODE = new ConfigResource(Type.BROKER, "");

    private final Logger log;
    private final SnapshotRegistry snapshotRegistry;
    private final KafkaConfigSchema configSchema;
    private final Consumer<ConfigResource> existenceChecker;
    private final Optional<AlterConfigPolicy> alterConfigPolicy;
    private final ConfigurationValidator validator;
    private final TimelineHashMap<ConfigResource, TimelineHashMap<String, String>> configData;
    private final TimelineHashSet<Integer> brokersWithConfigs;
    private final Map<String, Object> staticConfig;
    private final ConfigResource currentController;
    private final FeatureControlManager featureControl;

    /**
     * R44 (Codex Finding): partition-count lookup wired post-construction by
     * {@link QuorumController} after {@link ReplicationControlManager} is built. Used inside
     * {@link #validateAlterConfig} to refuse alter operations that would leave a topic with
     * {@code view.backing.topic} set to a topic whose partition count does not match this
     * topic's. The default no-op lookup means the check is silently skipped if the
     * controller wiring forgets to call {@link #setTopicPartitionCountLookup} — the create
     * and CreatePartitions guards in {@link ReplicationControlManager} still hold even in
     * that degraded mode, but production correctness depends on the wiring being present.
     */
    private volatile Function<String, OptionalInt> topicPartitionCountLookup = __ -> OptionalInt.empty();

    static class Builder {
        private LogContext logContext = null;
        private SnapshotRegistry snapshotRegistry = null;
        private KafkaConfigSchema configSchema = null;
        private Consumer<ConfigResource> existenceChecker = __ -> { };
        private Optional<AlterConfigPolicy> alterConfigPolicy = Optional.empty();
        private ConfigurationValidator validator = ConfigurationValidator.NO_OP;
        private Map<String, Object> staticConfig = Collections.emptyMap();
        private int nodeId = 0;
        private FeatureControlManager featureControl = null;

        Builder setLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        Builder setSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        Builder setKafkaConfigSchema(KafkaConfigSchema configSchema) {
            this.configSchema = configSchema;
            return this;
        }

        Builder setExistenceChecker(Consumer<ConfigResource> existenceChecker) {
            this.existenceChecker = existenceChecker;
            return this;
        }

        Builder setAlterConfigPolicy(Optional<AlterConfigPolicy> alterConfigPolicy) {
            this.alterConfigPolicy = alterConfigPolicy;
            return this;
        }

        Builder setValidator(ConfigurationValidator validator) {
            this.validator = validator;
            return this;
        }

        Builder setStaticConfig(Map<String, Object> staticConfig) {
            this.staticConfig = staticConfig;
            return this;
        }

        Builder setNodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        Builder setFeatureControl(FeatureControlManager featureControl) {
            this.featureControl = featureControl;
            return this;
        }

        ConfigurationControlManager build() {
            if (logContext == null) logContext = new LogContext();
            if (snapshotRegistry == null) snapshotRegistry = new SnapshotRegistry(logContext);
            if (configSchema == null) {
                throw new RuntimeException("You must set the configSchema.");
            }
            if (featureControl == null) {
                featureControl = new FeatureControlManager.Builder().build();
            }
            return new ConfigurationControlManager(
                logContext,
                snapshotRegistry,
                configSchema,
                existenceChecker,
                alterConfigPolicy,
                validator,
                staticConfig,
                nodeId,
                featureControl);
        }
    }

    private ConfigurationControlManager(LogContext logContext,
            SnapshotRegistry snapshotRegistry,
            KafkaConfigSchema configSchema,
            Consumer<ConfigResource> existenceChecker,
            Optional<AlterConfigPolicy> alterConfigPolicy,
            ConfigurationValidator validator,
            Map<String, Object> staticConfig,
            int nodeId,
            FeatureControlManager featureControl
    ) {
        this.log = logContext.logger(ConfigurationControlManager.class);
        this.snapshotRegistry = snapshotRegistry;
        this.configSchema = configSchema;
        this.existenceChecker = existenceChecker;
        this.alterConfigPolicy = alterConfigPolicy;
        this.validator = validator;
        this.configData = new TimelineHashMap<>(snapshotRegistry, 0);
        this.brokersWithConfigs = new TimelineHashSet<>(snapshotRegistry, 0);
        this.staticConfig = Collections.unmodifiableMap(new HashMap<>(staticConfig));
        this.currentController = new ConfigResource(Type.BROKER, Integer.toString(nodeId));
        this.featureControl = featureControl;
    }

    SnapshotRegistry snapshotRegistry() {
        return snapshotRegistry;
    }

    /**
     * R44 (Codex Finding): wire the partition-count lookup from {@link ReplicationControlManager}
     * after the controller's manager graph has been constructed. Idempotent; later calls
     * overwrite the previous lookup (used in tests). Must be called from inside the controller
     * event loop or before any alter request is processed; the lookup itself is invoked inside
     * the event loop in {@link #validateAlterConfig}, so the {@link Function} implementation
     * must read state that is consistent with the event-loop timeline.
     */
    public void setTopicPartitionCountLookup(Function<String, OptionalInt> lookup) {
        this.topicPartitionCountLookup = Objects.requireNonNull(lookup,
            "topicPartitionCountLookup must not be null");
    }

    /**
     * Determine the result of applying a batch of incremental configuration changes.  Note
     * that this method does not change the contents of memory.  It just generates a
     * result, that you can replay later if you wish using replay().
     *
     * Note that there can only be one result per ConfigResource.  So if you try to modify
     * several keys and one modification fails, the whole ConfigKey fails and nothing gets
     * changed.
     *
     * @param configChanges     Maps each resource to a map from config keys to
     *                          operation data.
     * @return                  The result.
     */
    ControllerResult<Map<ConfigResource, ApiError>> incrementalAlterConfigs(
        Map<ConfigResource, Map<String, Entry<OpType, String>>> configChanges,
        boolean newlyCreatedResource
    ) {
        return incrementalAlterConfigs(configChanges, Collections.emptyMap(), newlyCreatedResource);
    }

    /**
     * R40b: variant accepting per-resource preconditions. For each entry in
     * {@code expectedConfigValues}, the current value of that config key in {@link #configData}
     * (read inside the controller event loop, atomically) must equal the expected value, otherwise
     * the resource's mutations are rejected with {@code INVALID_REQUEST}. The check is per-key:
     * an expected value of {@code null} matches an absent key; any non-null expected value must
     * match the stored string exactly. Pass {@link Collections#emptyMap()} when no precondition is
     * required.
     *
     * <p>This closes the R38 TOCTOU residual: the broker-side preflight reads the current view
     * backing through {@code metadataCache}, which can race a concurrent rebind committed by a
     * different principal between the preflight authz check and the controller apply. By
     * threading the expected backing through as a precondition, the controller event-loop
     * commit-time view of {@code configData} is what is actually authorized against — there is
     * no window where the alter lands on a backing the broker never saw.
     */
    ControllerResult<Map<ConfigResource, ApiError>> incrementalAlterConfigs(
        Map<ConfigResource, Map<String, Entry<OpType, String>>> configChanges,
        Map<ConfigResource, Map<String, String>> expectedConfigValues,
        boolean newlyCreatedResource
    ) {
        List<ApiMessageAndVersion> outputRecords =
                BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        Map<ConfigResource, ApiError> outputResults = new HashMap<>();
        for (Entry<ConfigResource, Map<String, Entry<OpType, String>>> resourceEntry :
                configChanges.entrySet()) {
            ConfigResource resource = resourceEntry.getKey();
            ApiError preconditionError = checkPreconditions(resource,
                expectedConfigValues.getOrDefault(resource, Collections.emptyMap()));
            if (preconditionError.isFailure()) {
                outputResults.put(resource, preconditionError);
                continue;
            }
            ApiError apiError = incrementalAlterConfigResource(resource,
                resourceEntry.getValue(),
                newlyCreatedResource,
                outputRecords);
            outputResults.put(resource, apiError);
        }
        outputRecords.addAll(createClearElrRecordsAsNeeded(outputRecords));
        return ControllerResult.atomicOf(outputRecords, outputResults);
    }

    private ApiError checkPreconditions(ConfigResource resource, Map<String, String> expected) {
        if (expected.isEmpty()) return ApiError.NONE;
        TimelineHashMap<String, String> current = configData.get(resource);
        for (Entry<String, String> e : expected.entrySet()) {
            String got = current == null ? null : current.get(e.getKey());
            if (!Objects.equals(got, e.getValue())) {
                return new ApiError(INVALID_REQUEST,
                    "Precondition failed for " + resource + ": expected " + e.getKey() +
                    "=" + e.getValue() + " but current value is " + got + ".");
            }
        }
        return ApiError.NONE;
    }

    List<ApiMessageAndVersion> createClearElrRecordsAsNeeded(List<ApiMessageAndVersion> input) {
        if (!featureControl.isElrFeatureEnabled()) {
            return Collections.emptyList();
        }
        List<ApiMessageAndVersion> output = new ArrayList<>();
        for (ApiMessageAndVersion messageAndVersion : input) {
            if (messageAndVersion.message().apiKey() == CONFIG_RECORD.id()) {
                ConfigRecord record = (ConfigRecord) messageAndVersion.message();
                if (record.name().equals(MIN_IN_SYNC_REPLICAS_CONFIG)) {
                    if (Type.forId(record.resourceType()) == Type.TOPIC) {
                        output.add(new ApiMessageAndVersion(
                            new ClearElrRecord().
                                setTopicName(record.resourceName()), (short) 0));
                    } else {
                        output.add(new ApiMessageAndVersion(new ClearElrRecord(), (short) 0));
                    }
                }
            }
        }
        return output;
    }

    ControllerResult<ApiError> incrementalAlterConfig(
        ConfigResource configResource,
        Map<String, Entry<OpType, String>> keyToOps,
        boolean newlyCreatedResource
    ) {
        List<ApiMessageAndVersion> outputRecords =
                BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        ApiError apiError = incrementalAlterConfigResource(configResource,
            keyToOps,
            newlyCreatedResource,
            outputRecords);

        outputRecords.addAll(createClearElrRecordsAsNeeded(outputRecords));
        return ControllerResult.atomicOf(outputRecords, apiError);
    }

    private ApiError incrementalAlterConfigResource(
        ConfigResource configResource,
        Map<String, Entry<OpType, String>> keysToOps,
        boolean newlyCreatedResource,
        List<ApiMessageAndVersion> outputRecords
    ) {
        List<ApiMessageAndVersion> newRecords = new ArrayList<>();
        for (Entry<String, Entry<OpType, String>> keysToOpsEntry : keysToOps.entrySet()) {
            String key = keysToOpsEntry.getKey();
            String currentValue = null;
            TimelineHashMap<String, String> currentConfigs = configData.get(configResource);
            if (currentConfigs != null) {
                currentValue = currentConfigs.get(key);
            }
            String newValue = currentValue;
            Entry<OpType, String> opTypeAndNewValue = keysToOpsEntry.getValue();
            OpType opType = opTypeAndNewValue.getKey();
            String opValue = opTypeAndNewValue.getValue();
            switch (opType) {
                case SET:
                    newValue = opValue;
                    break;
                case DELETE:
                    newValue = null;
                    break;
                case APPEND:
                case SUBTRACT:
                    if (!configSchema.isSplittable(configResource.type(), key)) {
                        return new ApiError(
                            INVALID_CONFIG, "Can't " + opType + " to " +
                            "key " + key + " because its type is not LIST.");
                    }
                    List<String> oldValueList = getParts(newValue, key, configResource);
                    if (opType == APPEND) {
                        for (String value : opValue.split(",")) {
                            if (!oldValueList.contains(value)) {
                                oldValueList.add(value);
                            }
                        }
                    } else {
                        for (String value : opValue.split(",")) {
                            oldValueList.remove(value);
                        }
                    }
                    newValue = String.join(",", oldValueList);
                    break;
            }
            if (!Objects.equals(currentValue, newValue) || configResource.type().equals(Type.BROKER)) {
                // KAFKA-14136 We need to generate records even if the value is unchanged to trigger reloads on the brokers
                newRecords.add(new ApiMessageAndVersion(new ConfigRecord().
                    setResourceType(configResource.type().id()).
                    setResourceName(configResource.name()).
                    setName(key).
                    setValue(newValue), (short) 0));
            }
        }
        ApiError error = validateAlterConfig(configResource, newRecords, Collections.emptyList(), newlyCreatedResource);
        if (error.isFailure()) {
            return error;
        }
        outputRecords.addAll(newRecords);
        return ApiError.NONE;
    }

    private ApiError validateAlterConfig(
        ConfigResource configResource,
        List<ApiMessageAndVersion> recordsExplicitlyAltered,
        List<ApiMessageAndVersion> recordsImplicitlyDeleted,
        boolean newlyCreatedResource
    ) {
        Map<String, String> allConfigs = new HashMap<>();
        Map<String, String> existingConfigsMap = new HashMap<>();
        Map<String, String> alteredConfigsForAlterConfigPolicyCheck = new HashMap<>();
        TimelineHashMap<String, String> existingConfigsSnapshot = configData.get(configResource);
        if (existingConfigsSnapshot != null) {
            allConfigs.putAll(existingConfigsSnapshot);
            existingConfigsMap.putAll(existingConfigsSnapshot);
        }
        for (ApiMessageAndVersion newRecord : recordsExplicitlyAltered) {
            ConfigRecord configRecord = (ConfigRecord) newRecord.message();
            if (isDisallowedBrokerMinIsrTransition(configRecord)) {
                return DISALLOWED_BROKER_MIN_ISR_TRANSITION_ERROR;
            } else if (isDisallowedClusterMinIsrTransition(configRecord)) {
                return DISALLOWED_CLUSTER_MIN_ISR_REMOVAL_ERROR;
            } else if (configRecord.value() == null) {
                allConfigs.remove(configRecord.name());
            } else {
                allConfigs.put(configRecord.name(), configRecord.value());
            }
            alteredConfigsForAlterConfigPolicyCheck.put(configRecord.name(), configRecord.value());
        }
        for (ApiMessageAndVersion recordImplicitlyDeleted : recordsImplicitlyDeleted) {
            ConfigRecord configRecord = (ConfigRecord) recordImplicitlyDeleted.message();
            if (isDisallowedBrokerMinIsrTransition(configRecord)) {
                return DISALLOWED_BROKER_MIN_ISR_TRANSITION_ERROR;
            } else if (isDisallowedClusterMinIsrTransition(configRecord)) {
                return DISALLOWED_CLUSTER_MIN_ISR_REMOVAL_ERROR;
            } else {
                allConfigs.remove(configRecord.name());
            }
            // As per KAFKA-14195, do not include implicit deletions caused by using the legacy AlterConfigs API
            // in the list passed to the policy in order to maintain backwards compatibility
        }
        try {
            validator.validate(configResource, allConfigs, existingConfigsMap);
            if (!newlyCreatedResource) {
                existenceChecker.accept(configResource);
            }
            // R44 (Codex Finding): for alter operations on an existing topic that leave
            // view.backing.topic set in the POST-state, require the view topic and its backing
            // to have the same partition count. The fetch redirect maps (view, p) → (backing, p)
            // on partition index alone; a mismatch silently drops backing partitions whose
            // index ≥ view.parts.size (data loss for view consumers) or strands view partitions
            // whose index ≥ backing.parts.size (perpetual UNKNOWN_TOPIC_OR_PARTITION, wedged
            // consumer groups). The createTopic path enforces the same invariant inline; this
            // covers the rebind / turn-existing-topic-into-view paths reachable via
            // IncrementalAlterConfigs and legacy AlterConfigs. We skip the check when the
            // resource is newly created — at that point the view topic is not yet in the
            // partition registry, and the createTopic-side guard is authoritative. We also
            // skip if the lookup is the default no-op (lookup never returns a present count
            // for the view itself), which preserves test-context fallthrough where the
            // ReplicationControlManager is not wired in.
            if (!newlyCreatedResource && configResource.type() == Type.TOPIC) {
                String backing = allConfigs.get(ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG);
                String existingBacking = existingConfigsMap.get(ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG);
                boolean wasView = existingBacking != null && !existingBacking.trim().isEmpty();
                boolean willBeView = backing != null && !backing.trim().isEmpty();
                // R53 (Codex Finding): view-ness is immutable after topic creation. Reject
                // AlterConfigs that transitions a non-view topic into a view by setting
                // view.backing.topic. The exploit chain Codex traced: AddPartitionsToTxn(T-0) is
                // accepted while T is a regular topic (txnMetadata.topicPartitions includes T-0),
                // an alter then sets view.backing.topic on T (this path), EndTxn issues markers
                // for T-0, the broker classifies T as a view at marker dispatch time and rejects
                // with INVALID_TOPIC_EXCEPTION (KafkaApis.handleWriteTxnMarkers), and
                // TransactionMarkerRequestCompletionHandler has no case for INVALID_TOPIC_EXCEPTION
                // — it falls into the default branch and throws IllegalStateException. The marker
                // for T-0 is then never acknowledged or cancelled, leaving the transaction wedged
                // and any READ_COMMITTED consumer on the backing partition blocked at the LSO.
                // Separate but reinforcing concern: any data already written to T's log (when T
                // was a regular topic) becomes orphaned after conversion, because consumer reads
                // on the view redirect to the backing topic and never observe the pre-conversion
                // log. We allow create-time view configs (newlyCreatedResource above) and
                // view-to-view changes (predicate/backing rebind where wasView==true).
                if (willBeView && !wasView) {
                    throw new ConfigException(
                        "Cannot set " + ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG +
                        " on existing non-view topic '" + configResource.name() + "': " +
                        "view-ness is immutable after topic creation. Create the topic with " +
                        "view configs at create time, or alter an existing view's predicate or " +
                        "backing. Converting a regular topic into a view would wedge any " +
                        "in-flight transactions (the broker rejects markers on views with " +
                        "INVALID_TOPIC_EXCEPTION, blocking READ_COMMITTED consumers at the LSO) " +
                        "and orphan any data already present in the topic's log (consumer reads " +
                        "redirect to the backing topic).");
                }
                // R55 (Codex Finding): view-ness is immutable in BOTH directions. R53 above
                // rejects regular→view; this branch rejects view→regular. Without it, a
                // principal holding ALTER_CONFIGS on a view V (but not READ on its backing B)
                // can strip view-ness via either of two paths:
                //   (a) legacy AlterConfigs full-replace omitting all view.* keys. Legacy
                //       overwrites all configs, so omitted keys become implicit DELETEs
                //       (CCM.legacyAlterConfigResource at lines 633-642). The all-or-none
                //       invariant in LogConfig.validateViewConfigs permits present=0 as a
                //       valid "regular topic" post-state, so partial-strip detection at the
                //       schema layer does not catch full-strip.
                //   (b) IncrementalAlterConfigs DELETE op on view.backing.topic. The
                //       per-key DELETE sets newValue=null (CCM.incrementalAlterConfigResource
                //       at line 355), and the LogConfig all-or-none check would only reject if
                //       a partial subset remained — so attacker DELETEs the backing key (or
                //       all three keys) in one shot to land on the valid present=0 state.
                // Once stripped, broker classification flips: KafkaApis.isViewTopic at
                // KafkaApis:565 requires all three view configs (via TopicViewConfigs.fromMap),
                // so a topic with present=0 is no longer a view. Subsequent fetches stop
                // redirecting to the backing topic (KafkaApis:828 path) and produces stop
                // hitting the read-only rejection (KafkaApis:434, :540). The principal — who
                // could not READ B — now controls a writable local log that consumers holding
                // READ on V still believe is the filtered view feed, opening a path to
                // attacker-controlled records being served as the view. Reject the conversion
                // here so the create path remains the only place view-ness is established or
                // torn down. The auth check at ControllerApis.handleLegacyAlterConfigs only
                // computes currentBackingDenied when the submitted map contains a view key by
                // name (legacyTouchesAnyViewConfig at ControllerApis:956), so omission-based
                // mutations bypass the broker-side READ-on-backing check — this controller
                // gate is what actually closes the exploit.
                if (wasView && !willBeView) {
                    throw new ConfigException(
                        "Cannot remove " + ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG +
                        " from existing view topic '" + configResource.name() + "': " +
                        "view-ness is immutable after topic creation. Delete and recreate " +
                        "the topic if you need to convert a view into a regular topic. " +
                        "Stripping view configs would let a principal who cannot READ the " +
                        "backing topic produce attacker-controlled records to consumers " +
                        "that still hold READ on the view (the read-only rejection and " +
                        "fetch-redirect both key on view classification, which collapses " +
                        "once any of the three view configs is removed).");
                }
                if (backing != null) {
                    String trimmedBacking = backing.trim();
                    if (!trimmedBacking.isEmpty()) {
                        OptionalInt viewParts = topicPartitionCountLookup.apply(configResource.name());
                        if (viewParts.isPresent()) {
                            OptionalInt backingParts = topicPartitionCountLookup.apply(trimmedBacking);
                            if (!backingParts.isPresent()) {
                                throw new ConfigException(
                                    "Cannot set " + ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG +
                                    "='" + trimmedBacking + "' on topic '" + configResource.name() +
                                    "': backing topic '" + trimmedBacking + "' does not exist.");
                            }
                            if (viewParts.getAsInt() != backingParts.getAsInt()) {
                                throw new ConfigException(
                                    "Cannot set " + ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG +
                                    "='" + trimmedBacking + "' on topic '" + configResource.name() +
                                    "': view has " + viewParts.getAsInt() + " partition(s) but " +
                                    "backing topic '" + trimmedBacking + "' has " +
                                    backingParts.getAsInt() + " partition(s). A view must have " +
                                    "the same partition count as its backing — the fetch redirect " +
                                    "maps (view, p) → (backing, p) on partition index alone.");
                            }
                        }
                    }
                }
            }
            if (alterConfigPolicy.isPresent()) {
                alterConfigPolicy.get().validate(new RequestMetadata(configResource, alteredConfigsForAlterConfigPolicyCheck));
            }
        } catch (ConfigException e) {
            return new ApiError(INVALID_CONFIG, e.getMessage());
        } catch (Throwable e) {
            // return the corresponding API error, but emit the stack trace first if it is an unknown server error
            ApiError apiError = ApiError.fromThrowable(e);
            if (apiError.error() == Errors.UNKNOWN_SERVER_ERROR) {
                log.error("Unknown server error validating Alter Configs", e);
            }
            return apiError;
        }
        return ApiError.NONE;
    }

    private static final ApiError DISALLOWED_BROKER_MIN_ISR_TRANSITION_ERROR =
        new ApiError(INVALID_CONFIG, "Broker-level " + MIN_IN_SYNC_REPLICAS_CONFIG +
            " cannot be altered while ELR is enabled.");

    private static final ApiError DISALLOWED_CLUSTER_MIN_ISR_REMOVAL_ERROR =
        new ApiError(INVALID_CONFIG, "Cluster-level " + MIN_IN_SYNC_REPLICAS_CONFIG +
            " cannot be removed while ELR is enabled.");

    boolean isDisallowedBrokerMinIsrTransition(ConfigRecord configRecord) {
        if (configRecord.name().equals(MIN_IN_SYNC_REPLICAS_CONFIG) &&
                configRecord.resourceType() == BROKER.id() &&
                !configRecord.resourceName().isEmpty()) {
            if (featureControl.isElrFeatureEnabled()) {
                return true;
            }
        }
        return false;
    }

    boolean isDisallowedClusterMinIsrTransition(ConfigRecord configRecord) {
        if (configRecord.name().equals(MIN_IN_SYNC_REPLICAS_CONFIG) &&
                configRecord.resourceType() == BROKER.id() &&
                configRecord.resourceName().isEmpty() &&
                configRecord.value() == null) {
            if (featureControl.isElrFeatureEnabled()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Determine the result of applying a batch of legacy configuration changes.  Note
     * that this method does not change the contents of memory.  It just generates a
     * result, that you can replay later if you wish using replay().
     *
     * @param newConfigs        The new configurations to install for each resource.
     *                          All existing configurations will be overwritten.
     * @return                  The result.
     */
    ControllerResult<Map<ConfigResource, ApiError>> legacyAlterConfigs(
        Map<ConfigResource, Map<String, String>> newConfigs,
        boolean newlyCreatedResource
    ) {
        return legacyAlterConfigs(newConfigs, Collections.emptyMap(), newlyCreatedResource);
    }

    /**
     * R40b: legacy variant of
     * {@link #incrementalAlterConfigs(Map, Map, boolean)} with per-resource preconditions.
     * Same semantics: each entry in {@code expectedConfigValues} is compared against
     * {@link #configData} atomically; a resource whose precondition fails gets
     * {@code INVALID_REQUEST} and its mutations are skipped.
     */
    ControllerResult<Map<ConfigResource, ApiError>> legacyAlterConfigs(
        Map<ConfigResource, Map<String, String>> newConfigs,
        Map<ConfigResource, Map<String, String>> expectedConfigValues,
        boolean newlyCreatedResource
    ) {
        List<ApiMessageAndVersion> outputRecords =
                BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        Map<ConfigResource, ApiError> outputResults = new HashMap<>();
        for (Entry<ConfigResource, Map<String, String>> resourceEntry :
            newConfigs.entrySet()) {
            ConfigResource resource = resourceEntry.getKey();
            ApiError preconditionError = checkPreconditions(resource,
                expectedConfigValues.getOrDefault(resource, Collections.emptyMap()));
            if (preconditionError.isFailure()) {
                outputResults.put(resource, preconditionError);
                continue;
            }
            legacyAlterConfigResource(resource,
                resourceEntry.getValue(),
                newlyCreatedResource,
                outputRecords,
                outputResults);
        }
        outputRecords.addAll(createClearElrRecordsAsNeeded(outputRecords));
        return ControllerResult.atomicOf(outputRecords, outputResults);
    }

    private void legacyAlterConfigResource(ConfigResource configResource,
                                           Map<String, String> newConfigs,
                                           boolean newlyCreatedResource,
                                           List<ApiMessageAndVersion> outputRecords,
                                           Map<ConfigResource, ApiError> outputResults) {
        List<ApiMessageAndVersion> recordsExplicitlyAltered = new ArrayList<>();
        Map<String, String> currentConfigs = configData.get(configResource);
        if (currentConfigs == null) {
            currentConfigs = Collections.emptyMap();
        }
        for (Entry<String, String> entry : newConfigs.entrySet()) {
            String key = entry.getKey();
            String newValue = entry.getValue();
            String currentValue = currentConfigs.get(key);
            if (!Objects.equals(currentValue, newValue) || configResource.type().equals(Type.BROKER)) {
                // KAFKA-14136 We need to generate records even if the value is unchanged to trigger reloads on the brokers
                recordsExplicitlyAltered.add(new ApiMessageAndVersion(new ConfigRecord().
                    setResourceType(configResource.type().id()).
                    setResourceName(configResource.name()).
                    setName(key).
                    setValue(newValue), (short) 0));
            }
        }
        List<ApiMessageAndVersion> recordsImplicitlyDeleted = new ArrayList<>();
        for (String key : currentConfigs.keySet()) {
            if (!newConfigs.containsKey(key)) {
                recordsImplicitlyDeleted.add(new ApiMessageAndVersion(new ConfigRecord().
                    setResourceType(configResource.type().id()).
                    setResourceName(configResource.name()).
                    setName(key).
                    setValue(null), (short) 0));
            }
        }
        ApiError error = validateAlterConfig(configResource, recordsExplicitlyAltered, recordsImplicitlyDeleted, newlyCreatedResource);
        if (error.isFailure()) {
            outputResults.put(configResource, error);
            return;
        }
        outputRecords.addAll(recordsExplicitlyAltered);
        outputRecords.addAll(recordsImplicitlyDeleted);
        outputResults.put(configResource, ApiError.NONE);
    }

    private List<String> getParts(String value, String key, ConfigResource configResource) {
        if (value == null) {
            value = configSchema.getDefault(configResource.type(), key);
        }
        List<String> parts = new ArrayList<>();
        if (value == null) {
            return parts;
        }
        String[] splitValues = value.split(",");
        for (String splitValue : splitValues) {
            if (!splitValue.isEmpty()) {
                parts.add(splitValue);
            }
        }
        return parts;
    }

    /**
     * Apply a configuration record to the in-memory state.
     *
     * @param record            The ConfigRecord.
     */
    public void replay(ConfigRecord record) {
        Type type = Type.forId(record.resourceType());
        ConfigResource configResource = new ConfigResource(type, record.resourceName());
        TimelineHashMap<String, String> configs = configData.get(configResource);
        if (configs == null) {
            configs = new TimelineHashMap<>(snapshotRegistry, 0);
            configData.put(configResource, configs);
            if (configResource.type().equals(BROKER) && !configResource.name().isEmpty()) {
                brokersWithConfigs.add(Integer.parseInt(configResource.name()));
            }
        }
        if (record.value() == null) {
            configs.remove(record.name());
        } else {
            configs.put(record.name(), record.value());
        }
        if (configs.isEmpty()) {
            configData.remove(configResource);
            if (configResource.type().equals(BROKER) && !configResource.name().isEmpty()) {
                brokersWithConfigs.remove(Integer.parseInt(configResource.name()));
            }
        }
        if (configSchema.isSensitive(record)) {
            log.info("Replayed ConfigRecord for {} which set configuration {} to {}",
                    configResource, record.name(), Password.HIDDEN);
        } else {
            log.info("Replayed ConfigRecord for {} which set configuration {} to {}",
                    configResource, record.name(), record.value());
        }
    }

    // VisibleForTesting
    Map<String, String> getConfigs(ConfigResource configResource) {
        Map<String, String> map = configData.get(configResource);
        if (map == null) {
            return Collections.emptyMap();
        } else {
            return Collections.unmodifiableMap(new HashMap<>(map));
        }
    }

    /**
     * Get the config value for the given topic and given config key.
     * The check order is:
     *   1. dynamic topic overridden configs
     *   2. dynamic node overridden configs
     *   3. dynamic cluster overridden configs
     *   4. static configs
     * If the config value is not found, return null.
     *
     * @param topicName            The topic name for the config.
     * @param configKey            The key for the config.
     * @return the config value for the provided config key in the topic
     */
    ConfigEntry getTopicConfig(String topicName, String configKey) throws NoSuchElementException {
        return configSchema.resolveEffectiveTopicConfig(configKey,
            staticConfig,
            clusterConfig(),
            currentControllerConfig(), currentTopicConfig(topicName));
    }

    public Map<ConfigResource, ResultOrError<Map<String, String>>> describeConfigs(
            long lastCommittedOffset, Map<ConfigResource, Collection<String>> resources) {
        Map<ConfigResource, ResultOrError<Map<String, String>>> results = new HashMap<>();
        for (Entry<ConfigResource, Collection<String>> resourceEntry : resources.entrySet()) {
            ConfigResource resource = resourceEntry.getKey();
            try {
                validator.validate(resource);
            } catch (Throwable e) {
                results.put(resource, new ResultOrError<>(ApiError.fromThrowable(e)));
                continue;
            }
            Map<String, String> foundConfigs = new HashMap<>();
            TimelineHashMap<String, String> configs =
                configData.get(resource, lastCommittedOffset);
            if (configs != null) {
                Collection<String> targetConfigs = resourceEntry.getValue();
                if (targetConfigs.isEmpty()) {
                    for (Entry<String, String> entry : configs.entrySet(lastCommittedOffset)) {
                        foundConfigs.put(entry.getKey(), entry.getValue());
                    }
                } else {
                    for (String key : targetConfigs) {
                        String value = configs.get(key, lastCommittedOffset);
                        if (value != null) {
                            foundConfigs.put(key, value);
                        }
                    }
                }
            }
            results.put(resource, new ResultOrError<>(foundConfigs));
        }
        return results;
    }

    void deleteTopicConfigs(String name) {
        configData.remove(new ConfigResource(Type.TOPIC, name));
    }

    int getStaticallyConfiguredMinInsyncReplicas() {
        return configSchema.getStaticallyConfiguredMinInsyncReplicas(staticConfig);
    }

    /**
     * Generate any configuration records that are needed to make it safe to enable ELR.
     * Specifically, we need to remove all cluster-level configurations for min.insync.replicas,
     * and create a cluster-level configuration for min.insync.replicas. It is always safe to call
     * this function if ELR is already enabled; it will simply do nothing if the necessary
     * configurations already exist.
     *
     * @param outputRecords     A list to add the new records to.
     *
     * @return                  The log message to generate.
     */
    String maybeGenerateElrSafetyRecords(List<ApiMessageAndVersion> outputRecords) {
        StringBuilder bld = new StringBuilder();
        String prefix = "";
        if (!clusterConfig().containsKey(MIN_IN_SYNC_REPLICAS_CONFIG)) {
            int minInsyncReplicas = configSchema.getStaticallyConfiguredMinInsyncReplicas(staticConfig);
            outputRecords.add(new ApiMessageAndVersion(
                new ConfigRecord().
                    setResourceType(BROKER.id()).
                    setResourceName("").
                    setName(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).
                    setValue(Integer.toString(minInsyncReplicas)),
                CONFIG_RECORD.highestSupportedVersion()));
            bld.append("Generating cluster-level ").append(MIN_IN_SYNC_REPLICAS_CONFIG).
                append(" of ").append(minInsyncReplicas);
            prefix = ". ";
        }
        prefix = prefix + "Removing broker-level " + MIN_IN_SYNC_REPLICAS_CONFIG + " for brokers: ";
        for (Integer brokerId : brokersWithConfigs) {
            ConfigResource configResource = new ConfigResource(BROKER, brokerId.toString());
            Map<String, String> configs = configData.get(configResource);
            if (configs.containsKey(MIN_IN_SYNC_REPLICAS_CONFIG)) {
                outputRecords.add(new ApiMessageAndVersion(
                    new ConfigRecord().setResourceType(BROKER.id()).setResourceName(configResource.name()).
                        setName(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).setValue(null),
                    CONFIG_RECORD.highestSupportedVersion()));
                bld.append(prefix).append(brokerId);
                prefix = ", ";
            }
        }
        if (bld.isEmpty()) {
            return "";
        } else {
            bld.append(".");
            return bld.toString();
        }
    }

    /**
     * Update a Kafka feature, generating any configuration changes that are required.
     *
     * @param updates       The user-requested updates.
     * @param upgradeTypes  The user-requested upgrade types.
     * @param validateOnly  True if we should validate the request but not make changes.
     *
     * @return              The result.
     */
    ControllerResult<ApiError> updateFeatures(
        Map<String, Short> updates,
        Map<String, FeatureUpdate.UpgradeType> upgradeTypes,
        boolean validateOnly
    ) {
        ControllerResult<ApiError> result = featureControl.updateFeatures(updates, upgradeTypes, validateOnly);
        if (result.response().isSuccess() &&
            !validateOnly &&
            updates.getOrDefault(EligibleLeaderReplicasVersion.FEATURE_NAME, (short) 0) > 0
        ) {
            List<ApiMessageAndVersion> records = BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
            String logMessage = maybeGenerateElrSafetyRecords(records);
            if (!logMessage.isEmpty()) {
                log.info("{}", logMessage);
            }
            records.addAll(result.records());
            return ControllerResult.atomicOf(records, ApiError.NONE);
        }
        return result;
    }

    /**
     * Check if this topic has "unclean.leader.election.enable" set to true.
     *
     * @param topicName            The topic name for the config.
     * @return true if this topic has uncleanLeaderElection enabled
     */
    boolean uncleanLeaderElectionEnabledForTopic(String topicName) {
        String uncleanLeaderElection = getTopicConfig(topicName, UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG).value();
        if (!uncleanLeaderElection.isEmpty()) {
            return Boolean.parseBoolean(uncleanLeaderElection);
        }
        return false;
    }

    Map<String, ConfigEntry> computeEffectiveTopicConfigs(Map<String, String> creationConfigs) {
        return configSchema.resolveEffectiveTopicConfigs(staticConfig, clusterConfig(),
            currentControllerConfig(), creationConfigs);
    }

    Map<String, String> clusterConfig() {
        Map<String, String> result = configData.get(DEFAULT_NODE);
        return (result == null) ? Collections.emptyMap() : result;
    }

    Map<String, String> currentControllerConfig() {
        Map<String, String> result = configData.get(currentController);
        return (result == null) ? Collections.emptyMap() : result;
    }

    Map<String, String> currentTopicConfig(String topicName) {
        Map<String, String> result = configData.get(new ConfigResource(Type.TOPIC, topicName));
        return (result == null) ? Collections.emptyMap() : result;
    }

    /**
     * Return the names of topics whose `view.backing.topic` config currently points to
     * {@code backingTopicName}. The lookup runs inside the controller event loop against
     * the live, atomic {@link #configData} state.
     *
     * <p>R39 (Codex Finding #1): {@link ReplicationControlManager#deleteTopic} uses this
     * to refuse deletion of a topic that is the active backing of any view. {@code ViewSpec}
     * stores the backing by NAME only — without this check, a principal with DELETE on the
     * backing could delete it, then a separate principal with CREATE on TOPIC could create
     * a different sensitive topic that happens to reuse the same name, and the original
     * view's fetch path would silently re-bind to the new topic. That bypasses the
     * round-23 view-create READ-on-backing gate, since the bypass route never executes a
     * view-create at all.
     */
    Set<String> topicsReferencingBackingTopic(String backingTopicName) {
        if (backingTopicName == null) return Collections.emptySet();
        Set<String> result = new HashSet<>();
        for (Entry<ConfigResource, TimelineHashMap<String, String>> entry : configData.entrySet()) {
            ConfigResource resource = entry.getKey();
            if (resource.type() != Type.TOPIC) continue;
            String configured = entry.getValue().get(ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG);
            if (configured != null && backingTopicName.equals(configured.trim())) {
                result.add(resource.name());
            }
        }
        return result;
    }

    // Visible to test
    TimelineHashSet<Integer> brokersWithConfigs() {
        return brokersWithConfigs;
    }
}
