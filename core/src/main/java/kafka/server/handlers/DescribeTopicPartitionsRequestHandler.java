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
        List<String> authorizedSorted = collectAuthorizedSorted(
            abstractRequest, topics, fetchAllTopics, unauthorizedForDescribeTopicMetadata);

        ListenerName listenerName = abstractRequest.context().listenerName;
        int maxPartitions = Math.max(Math.min(config.maxRequestPartitionSizeLimit(), request.responsePartitionLimit()), 1);

        DescribeTopicPartitionsResponseData response = buildResponse(
            authorizedSorted, listenerName, cursor, cursorTopicName, maxPartitions, fetchAllTopics);

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
     * topics that fail authorization (matches stock semantics), and return the authorized topics
     * as a single alphabetically-sorted list (physical and logical interleaved). The interleaving
     * is critical: DescribeTopicPartitions cursor pagination relies on the response being in
     * topic-name order — bucketing physical-then-logical would silently drop logical topics that
     * sort between two physical topics on a paged request.
     */
    private List<String> collectAuthorizedSorted(
        RequestChannel.Request abstractRequest,
        Set<String> topics,
        boolean fetchAllTopics,
        Set<DescribeTopicPartitionsResponseTopic> unauthorizedForDescribeTopicMetadata
    ) {
        List<String> authorized = new ArrayList<>();
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
            authorized.add(topicName);
        });
        return authorized;
    }

    /**
     * Walk the authorized topics in alphabetical order, dispatching per topic to either the
     * KRaft metadata cache (physical) or the kernel-based synthesizer (logical), sharing a
     * single partition budget. Per-topic dispatch is functionally equivalent to a batched
     * cache call for physical-only workloads (the cache iterates topics in the input order and
     * applies the same budget arithmetic), but it lets us interleave logical synthesis without
     * disturbing the cursor contract.
     */
    private DescribeTopicPartitionsResponseData buildResponse(
        List<String> authorizedSorted,
        ListenerName listenerName,
        DescribeTopicPartitionsRequestData.Cursor cursor,
        String cursorTopicName,
        int maxPartitions,
        boolean fetchAllTopics
    ) {
        DescribeTopicPartitionsResponseData response = new DescribeTopicPartitionsResponseData();
        int remaining = maxPartitions;
        for (String topicName : authorizedSorted) {
            if (remaining <= 0) {
                response.setNextCursor(new Cursor()
                    .setTopicName(topicName)
                    .setPartitionIndex(0));
                return response;
            }
            boolean isLogical = concentrationKernel != null && concentrationKernel.isLogicalTopic(topicName);
            int delta = isLogical
                ? appendLogicalTopic(response, topicName, listenerName, cursor, cursorTopicName, remaining, fetchAllTopics)
                : appendPhysicalTopic(response, topicName, listenerName, cursor, cursorTopicName, remaining, fetchAllTopics);
            if (delta < 0) {
                // Topic emitted a nextCursor — pagination stops here.
                return response;
            }
            remaining -= delta;
        }
        return response;
    }

    /**
     * Dispatch a single physical topic to the KRaft metadata cache using a single-element list.
     * Returns the number of partitions added on success, or -1 if the cache emitted a nextCursor
     * (mid-topic break), signalling the caller to stop the walk.
     */
    private int appendPhysicalTopic(
        DescribeTopicPartitionsResponseData response,
        String topicName,
        ListenerName listenerName,
        DescribeTopicPartitionsRequestData.Cursor cursor,
        String cursorTopicName,
        int remainingBudget,
        boolean fetchAllTopics
    ) {
        DescribeTopicPartitionsResponseData oneTopic = metadataCache.getTopicMetadataForDescribeTopicResponse(
            CollectionConverters.asScala(Collections.singletonList(topicName).iterator()),
            listenerName,
            (String t) -> t.equals(cursorTopicName) && cursor != null ? cursor.partitionIndex() : 0,
            remainingBudget,
            fetchAllTopics
        );
        int added = 0;
        for (DescribeTopicPartitionsResponseTopic t : oneTopic.topics()) {
            // DescribeTopicPartitionsResponseTopic is an ImplicitLinkedHashMultiCollection.Element
            // and carries intrusive prev/next pointers, so an element still linked in oneTopic.topics()
            // silently fails to insert into response.topics(). Duplicate to get an unlinked copy.
            response.topics().add(t.duplicate());
            added += t.partitions().size();
        }
        if (oneTopic.nextCursor() != null) {
            response.setNextCursor(oneTopic.nextCursor());
            return -1;
        }
        return added;
    }

    /**
     * Synthesize a single logical topic and append it. Returns the number of partitions added,
     * or -1 if the topic emitted a nextCursor (mid-topic break).
     */
    private int appendLogicalTopic(
        DescribeTopicPartitionsResponseData response,
        String topicName,
        ListenerName listenerName,
        DescribeTopicPartitionsRequestData.Cursor cursor,
        String cursorTopicName,
        int remainingBudget,
        boolean fetchAllTopics
    ) {
        int startPartition = topicName.equals(cursorTopicName) && cursor != null
            ? cursor.partitionIndex() : 0;
        SynthesisResult synth = synthesizeLogicalDescribe(
            topicName, listenerName, startPartition, remainingBudget);
        if (synth == null) {
            if (!fetchAllTopics) {
                response.topics().add(describeTopicPartitionsResponseTopic(
                    Errors.UNKNOWN_TOPIC_OR_PARTITION, topicName, Uuid.ZERO_UUID, false,
                    Collections.emptyList()));
            }
            return 0;
        }
        response.topics().add(synth.topic);
        if (synth.nextPartitionIndex >= 0) {
            response.setNextCursor(new Cursor()
                .setTopicName(topicName)
                .setPartitionIndex(synth.nextPartitionIndex));
            return -1;
        }
        return synth.topic.partitions().size();
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
