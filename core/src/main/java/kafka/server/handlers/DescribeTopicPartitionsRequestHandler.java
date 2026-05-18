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

package kafka.server.handlers;

import kafka.network.RequestChannel;
import kafka.server.AuthHelper;
import kafka.server.KafkaConfig;
import kafka.server.metadata.KRaftMetadataCache;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.Cursor;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeTopicPartitionsRequest;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.storage.internals.concentration.ConcentrationKernel;
import org.apache.kafka.storage.internals.concentration.LogicalTopicDescriptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import scala.jdk.javaapi.CollectionConverters;

import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;

public class DescribeTopicPartitionsRequestHandler {
    KRaftMetadataCache metadataCache;
    AuthHelper authHelper;
    KafkaConfig config;
    ConcentrationKernel concentrationKernel;

    public DescribeTopicPartitionsRequestHandler(
        KRaftMetadataCache metadataCache,
        AuthHelper authHelper,
        KafkaConfig config,
        ConcentrationKernel concentrationKernel
    ) {
        this.metadataCache = metadataCache;
        this.authHelper = authHelper;
        this.config = config;
        this.concentrationKernel = concentrationKernel;
    }

    public DescribeTopicPartitionsResponseData handleDescribeTopicPartitionsRequest(RequestChannel.Request abstractRequest) {
        DescribeTopicPartitionsRequestData request = ((DescribeTopicPartitionsRequest) abstractRequest.loggableRequest()).data();
        boolean fetchAllTopics = request.topics().isEmpty();
        DescribeTopicPartitionsRequestData.Cursor cursor = request.cursor();
        String cursorTopicName = cursor != null ? cursor.topicName() : "";

        Set<String> topics = collectCandidateTopics(request, fetchAllTopics, cursor, cursorTopicName);

        if (cursor != null && cursor.partitionIndex() < 0) {
            // The partition id in cursor must be valid.
            throw new InvalidRequestException("DescribeTopicPartitionsRequest cursor partition must be valid: " + cursor);
        }

        Set<DescribeTopicPartitionsResponseTopic> unauthorizedForDescribeTopicMetadata = new HashSet<>();
        List<String> physicalTopics = new ArrayList<>();
        List<String> logicalTopics = new ArrayList<>();
        partitionAuthorizedTopics(abstractRequest, topics, fetchAllTopics,
            unauthorizedForDescribeTopicMetadata, physicalTopics, logicalTopics);

        ListenerName listenerName = abstractRequest.context().listenerName;
        int maxPartitions = Math.max(Math.min(config.maxRequestPartitionSizeLimit(), request.responsePartitionLimit()), 1);

        DescribeTopicPartitionsResponseData response = describePhysicalTopics(
            physicalTopics, listenerName, cursor, cursorTopicName, maxPartitions, fetchAllTopics);

        if (response.nextCursor() == null) {
            int used = 0;
            for (DescribeTopicPartitionsResponseTopic t : response.topics()) {
                used += t.partitions().size();
            }
            appendLogicalTopics(response, logicalTopics, listenerName, cursor, cursorTopicName,
                maxPartitions - used, fetchAllTopics);
        }

        // get topic authorized operations
        response.topics().forEach(topicData ->
            topicData.setTopicAuthorizedOperations(authHelper.authorizedOperations(abstractRequest, new Resource(TOPIC, topicData.name()))));

        response.topics().addAll(unauthorizedForDescribeTopicMetadata);
        return response;
    }

    private Set<String> collectCandidateTopics(
        DescribeTopicPartitionsRequestData request,
        boolean fetchAllTopics,
        DescribeTopicPartitionsRequestData.Cursor cursor,
        String cursorTopicName
    ) {
        Set<String> topics = new HashSet<>();
        if (fetchAllTopics) {
            CollectionConverters.asJavaCollection(metadataCache.getAllTopics()).forEach(topicName -> {
                if (topicName.compareTo(cursorTopicName) >= 0) {
                    topics.add(topicName);
                }
            });
            // Logical topics live entirely in broker config — the KRaft metadata cache knows nothing
            // about them — so listTopics() / DescribeTopicPartitions(all) would miss them without
            // this overlay. Mirrors the overlay on the METADATA path (KafkaApis.scala).
            if (concentrationKernel != null) {
                concentrationKernel.allLogicalTopicNames().forEach(topicName -> {
                    if (topicName.compareTo(cursorTopicName) >= 0) {
                        topics.add(topicName);
                    }
                });
            }
        } else {
            request.topics().forEach(topic -> {
                String topicName = topic.name();
                if (topicName.compareTo(cursorTopicName) >= 0) {
                    topics.add(topicName);
                }
            });
            if (cursor != null && !topics.contains(cursor.topicName())) {
                // The topic in cursor must be included in the topic list if provided.
                throw new InvalidRequestException("DescribeTopicPartitionsRequest topic list should contain the cursor topic: " + cursor.topicName());
            }
        }
        return topics;
    }

    /**
     * Filter by DESCRIBE authorization, append authorization-failure stubs for explicitly named
     * topics that fail authorization (matches stock semantics), and split the authorized topics
     * into physical vs logical buckets. The physical bucket is the normal metadata-cache flow;
     * the logical bucket is synthesized from backing-partition leadership.
     */
    private void partitionAuthorizedTopics(
        RequestChannel.Request abstractRequest,
        Set<String> topics,
        boolean fetchAllTopics,
        Set<DescribeTopicPartitionsResponseTopic> unauthorizedForDescribeTopicMetadata,
        List<String> physicalTopics,
        List<String> logicalTopics
    ) {
        topics.stream().sorted().forEach(topicName -> {
            boolean isAuthorized = authHelper.authorize(
                abstractRequest.context(), DESCRIBE, TOPIC, topicName, true, true, 1);
            if (!isAuthorized) {
                if (!fetchAllTopics) {
                    // We should not return topicId when on unauthorized error, so we return zero uuid.
                    unauthorizedForDescribeTopicMetadata.add(describeTopicPartitionsResponseTopic(
                        Errors.TOPIC_AUTHORIZATION_FAILED, topicName, Uuid.ZERO_UUID, false,
                        Collections.emptyList()));
                }
                return;
            }
            if (concentrationKernel != null && concentrationKernel.isLogicalTopic(topicName)) {
                logicalTopics.add(topicName);
            } else {
                physicalTopics.add(topicName);
            }
        });
    }

    /**
     * Delegate the physical bucket to the unchanged metadata-cache path. If the cursor points
     * into the logical phase, the physical phase is skipped entirely so the response resumes
     * exactly where the previous call stopped.
     */
    private DescribeTopicPartitionsResponseData describePhysicalTopics(
        List<String> physicalTopics,
        ListenerName listenerName,
        DescribeTopicPartitionsRequestData.Cursor cursor,
        String cursorTopicName,
        int maxPartitions,
        boolean fetchAllTopics
    ) {
        boolean cursorIsLogical = cursor != null && concentrationKernel != null
            && concentrationKernel.isLogicalTopic(cursorTopicName);
        if (cursorIsLogical) {
            return new DescribeTopicPartitionsResponseData();
        }
        return metadataCache.getTopicMetadataForDescribeTopicResponse(
            CollectionConverters.asScala(physicalTopics.iterator()),
            listenerName,
            (String topicName) -> topicName.equals(cursorTopicName) ? cursor.partitionIndex() : 0,
            maxPartitions,
            fetchAllTopics
        );
    }

    /**
     * Synthesize {@link DescribeTopicPartitionsResponseTopic} entries for logical topics that
     * appear after the physical phase finished without hitting the partition budget. Mirrors
     * {@code KafkaApis.synthesizeLogicalTopicMetadata} but for the
     * {@code DescribeTopicPartitions} response shape and with per-partition pagination so
     * large logical topics don't have to come back in one response.
     */
    private void appendLogicalTopics(
        DescribeTopicPartitionsResponseData response,
        List<String> logicalTopics,
        ListenerName listenerName,
        DescribeTopicPartitionsRequestData.Cursor cursor,
        String cursorTopicName,
        int remainingBudget,
        boolean ignoreTopicsWithExceptions
    ) {
        int remaining = remainingBudget;
        for (String logicalTopic : logicalTopics) {
            if (remaining <= 0) {
                response.setNextCursor(new Cursor()
                    .setTopicName(logicalTopic)
                    .setPartitionIndex(0));
                return;
            }
            int startPartition = logicalTopic.equals(cursorTopicName) && cursor != null
                ? cursor.partitionIndex() : 0;
            SynthesisResult synth = synthesizeLogicalDescribe(
                logicalTopic, listenerName, startPartition, remaining);
            if (synth == null) {
                if (!ignoreTopicsWithExceptions) {
                    response.topics().add(describeTopicPartitionsResponseTopic(
                        Errors.UNKNOWN_TOPIC_OR_PARTITION, logicalTopic, Uuid.ZERO_UUID, false,
                        Collections.emptyList()));
                }
                continue;
            }
            response.topics().add(synth.topic);
            remaining -= synth.topic.partitions().size();
            if (synth.nextPartitionIndex >= 0) {
                response.setNextCursor(new Cursor()
                    .setTopicName(logicalTopic)
                    .setPartitionIndex(synth.nextPartitionIndex));
                return;
            }
        }
    }

    /**
     * Build a single logical-topic response by pulling backing partition leadership from the
     * KRaft cache and rewriting partitionIndex from backing index to logical index. Returns
     * {@code null} if the logical topic's descriptor races out from under us (declared then
     * removed between {@code isLogicalTopic()} and {@code describe()}).
     */
    private SynthesisResult synthesizeLogicalDescribe(
        String logicalTopic,
        ListenerName listenerName,
        int startPartition,
        int remainingBudget
    ) {
        Optional<LogicalTopicDescriptor> descriptorOpt = concentrationKernel.describe(logicalTopic);
        if (descriptorOpt.isEmpty()) {
            return null;
        }
        LogicalTopicDescriptor descriptor = descriptorOpt.get();
        int totalPartitions = descriptor.numLogicalPartitions();
        Uuid topicId = concentrationKernel.logicalTopicId(logicalTopic);

        if (startPartition >= totalPartitions) {
            DescribeTopicPartitionsResponseTopic empty = describeTopicPartitionsResponseTopic(
                Errors.NONE, logicalTopic, topicId, false, Collections.emptyList());
            return new SynthesisResult(empty, -1);
        }

        // Ask the metadata cache for all backing partitions in one shot (Integer.MAX_VALUE
        // budget) so we can index by backing partition id and remap. Logical pagination is
        // applied to the logical-partition space, not the backing one — pagination at the
        // backing layer would interact badly with the logical→backing fan-out.
        String backingTopic = descriptor.backingTopic();
        DescribeTopicPartitionsResponseData backingResp = metadataCache.getTopicMetadataForDescribeTopicResponse(
            CollectionConverters.asScala(Collections.singletonList(backingTopic).iterator()),
            listenerName,
            (String t) -> 0,
            Integer.MAX_VALUE,
            true  // ignoreTopicsWithExceptions — backing-missing surfaces as LEADER_NOT_AVAILABLE below
        );

        if (backingResp.topics().isEmpty()) {
            // Backing topic absent from the metadata cache (still being created, or operator
            // misconfiguration). Retriable so stock callers keep re-asking until the backing
            // materialises — matches synthesizeLogicalTopicMetadata.
            DescribeTopicPartitionsResponseTopic err = describeTopicPartitionsResponseTopic(
                Errors.LEADER_NOT_AVAILABLE, logicalTopic, topicId, false, Collections.emptyList());
            return new SynthesisResult(err, -1);
        }

        DescribeTopicPartitionsResponseTopic backingTopicResp = backingResp.topics().iterator().next();
        Map<Integer, DescribeTopicPartitionsResponsePartition> backingByIndex = new HashMap<>();
        for (DescribeTopicPartitionsResponsePartition p : backingTopicResp.partitions()) {
            backingByIndex.put(p.partitionIndex(), p);
        }

        int upper = Math.min(totalPartitions, startPartition + remainingBudget);
        List<DescribeTopicPartitionsResponsePartition> logicalPartitions = new ArrayList<>(upper - startPartition);
        for (int lp = startPartition; lp < upper; lp++) {
            int backingIdx = concentrationKernel.backingPartitionFor(logicalTopic, lp);
            DescribeTopicPartitionsResponsePartition bp = backingByIndex.get(backingIdx);
            if (bp == null) {
                // Concentration config promised more backing partitions than the real backing
                // topic has. Surface per-partition so the rest of the logical topic remains
                // useful.
                logicalPartitions.add(new DescribeTopicPartitionsResponsePartition()
                    .setPartitionIndex(lp)
                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
            } else {
                logicalPartitions.add(new DescribeTopicPartitionsResponsePartition()
                    .setErrorCode(bp.errorCode())
                    .setPartitionIndex(lp)
                    .setLeaderId(bp.leaderId())
                    .setLeaderEpoch(bp.leaderEpoch())
                    .setReplicaNodes(new ArrayList<>(bp.replicaNodes()))
                    .setIsrNodes(new ArrayList<>(bp.isrNodes()))
                    .setEligibleLeaderReplicas(new ArrayList<>(bp.eligibleLeaderReplicas()))
                    .setLastKnownElr(new ArrayList<>(bp.lastKnownElr()))
                    .setOfflineReplicas(new ArrayList<>(bp.offlineReplicas())));
            }
        }
        int nextIdx = upper < totalPartitions ? upper : -1;

        DescribeTopicPartitionsResponseTopic synth = describeTopicPartitionsResponseTopic(
            Errors.NONE, logicalTopic, topicId, false, logicalPartitions);
        return new SynthesisResult(synth, nextIdx);
    }

    private DescribeTopicPartitionsResponseTopic describeTopicPartitionsResponseTopic(
        Errors error,
        String topic,
        Uuid topicId,
        Boolean isInternal,
        List<DescribeTopicPartitionsResponsePartition> partitionData
    ) {
        return new DescribeTopicPartitionsResponseTopic()
            .setErrorCode(error.code())
            .setName(topic)
            .setTopicId(topicId)
            .setIsInternal(isInternal)
            .setPartitions(partitionData);
    }

    private static final class SynthesisResult {
        final DescribeTopicPartitionsResponseTopic topic;
        final int nextPartitionIndex;

        SynthesisResult(DescribeTopicPartitionsResponseTopic topic, int nextPartitionIndex) {
            this.topic = topic;
            this.nextPartitionIndex = nextPartitionIndex;
        }
    }
}
