/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.coordinator.transaction.{InitProducerIdResult, TransactionCoordinator}
import kafka.network.RequestChannel
import kafka.server.QuotaFactory.{QuotaManagers, UNBOUNDED_QUOTA}
import kafka.server.handlers.DescribeTopicPartitionsRequestHandler
import kafka.server.metadata.{ConfigRepository, KRaftMetadataCache}
import kafka.server.share.SharePartitionManager
import kafka.utils.Logging
import org.apache.kafka.admin.AdminUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.EndpointType
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.acl.AclOperation._
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic.{GROUP_METADATA_TOPIC_NAME, SHARE_GROUP_STATE_TOPIC_NAME, TRANSACTION_STATE_TOPIC_NAME, isInternal}
import org.apache.kafka.common.internals.{FatalExitError, Topic}
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.{AddPartitionsToTxnResult, AddPartitionsToTxnResultCollection}
import org.apache.kafka.common.message.DeleteRecordsResponseData.{DeleteRecordsPartitionResult, DeleteRecordsTopicResult}
import org.apache.kafka.common.message.ListClientMetricsResourcesResponseData.ClientMetricsResource
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.message.ListOffsetsResponseData.{ListOffsetsPartitionResponse, ListOffsetsTopicResponse}
import org.apache.kafka.common.message.MetadataResponseData.{MetadataResponsePartition, MetadataResponseTopic}
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.{EpochEndOffset, OffsetForLeaderTopicResult, OffsetForLeaderTopicResultCollection}
import org.apache.kafka.common.message._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, ApiMessage, Errors}
import org.apache.kafka.common.record._
import org.apache.kafka.common.replica.ClientMetadata
import org.apache.kafka.common.replica.ClientMetadata.DefaultClientMetadata
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType._
import org.apache.kafka.common.resource.{Resource, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.security.token.delegation.{DelegationToken, TokenInformation}
import org.apache.kafka.common.utils.{BufferSupplier, ProducerIdAndEpoch, Time}
import org.apache.kafka.common.{Node, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.coordinator.group.{Group, GroupCoordinator}
import org.apache.kafka.coordinator.share.ShareCoordinator
import org.apache.kafka.server.ClientMetricsManager
import org.apache.kafka.server.authorizer._
import org.apache.kafka.server.common.{GroupVersion, RequestLocal, TransactionVersion}
import org.apache.kafka.server.share.context.ShareFetchContext
import org.apache.kafka.server.share.{ErroneousAndValidPartitionData, SharePartitionKey}
import org.apache.kafka.server.share.acknowledge.ShareAcknowledgementBatch
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams, FetchPartitionData}
import org.apache.kafka.server.views.{ViewFilter, ViewMetrics, ViewRegistry, ViewSpec}
import org.apache.kafka.storage.internals.log.AppendOrigin
import org.apache.kafka.storage.log.metrics.BrokerTopicStats

import java.time.Duration
import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap}
import java.util.stream.Collectors
import java.util.{Collections, Optional}
import scala.annotation.nowarn
import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, Seq, Set, mutable}
import scala.jdk.CollectionConverters._

/**
 * Logic to handle the various Kafka requests
 */
class KafkaApis(val requestChannel: RequestChannel,
                val forwardingManager: ForwardingManager,
                val replicaManager: ReplicaManager,
                val groupCoordinator: GroupCoordinator,
                val txnCoordinator: TransactionCoordinator,
                val shareCoordinator: Option[ShareCoordinator],
                val autoTopicCreationManager: AutoTopicCreationManager,
                val brokerId: Int,
                val config: KafkaConfig,
                val configRepository: ConfigRepository,
                val metadataCache: MetadataCache,
                val metrics: Metrics,
                val authorizer: Option[Authorizer],
                val quotas: QuotaManagers,
                val fetchManager: FetchManager,
                val sharePartitionManager: SharePartitionManager,
                brokerTopicStats: BrokerTopicStats,
                val clusterId: String,
                time: Time,
                val tokenManager: DelegationTokenManager,
                val apiVersionManager: ApiVersionManager,
                val clientMetricsManager: ClientMetricsManager
) extends ApiRequestHandler with Logging {

  type FetchResponseStats = Map[TopicPartition, RecordValidationStats]
  this.logIdent = "[KafkaApi-%d] ".format(brokerId)
  // One registry per broker: caches compiled view predicates so the fetch hot path never re-parses
  // CEL text. The config source closes over `configRepository`, which is the broker's live config
  // snapshot. Misses are cached too — a fetch against a regular topic never re-queries config.
  // package-private because the broker wiring layer (config change notifications) needs to call
  // `invalidate(name)` on it; nothing outside the broker should hold a reference.
  // The metrics instance is per-KafkaApis (per-broker) — Yammer registers in a global singleton
  // registry, so two ViewMetrics in the same JVM would double-register. KafkaApis is per-broker;
  // shutdown via close() removes the meters so the next broker in the same JVM (mainly test
  // harness reuse) gets clean readings.
  private[server] val viewMetrics: ViewMetrics = new ViewMetrics()
  private[server] val viewRegistry: ViewRegistry =
    new ViewRegistry((name: String) => topicViewConfigsFor(name), viewMetrics)
  val configHelper = new ConfigHelper(metadataCache, config, configRepository)
  val authHelper = new AuthHelper(authorizer)
  val requestHelper = new RequestHandlerHelper(requestChannel, quotas, time)
  val aclApis = new AclApis(authHelper, authorizer, requestHelper, "broker", config)
  val configManager = new ConfigAdminManager(brokerId, config, configRepository)
  val describeTopicPartitionsRequestHandler : Option[DescribeTopicPartitionsRequestHandler] = metadataCache match {
    case kRaftMetadataCache: KRaftMetadataCache =>
      Some(new DescribeTopicPartitionsRequestHandler(kRaftMetadataCache, authHelper, config))
    case _ => None
  }

  def close(): Unit = {
    aclApis.close()
    // Deregister view-feature meters from the Yammer singleton so a follow-up broker in the same
    // JVM (test harness reuse) does not see doubled readings.
    viewMetrics.close()
    info("Shutdown complete.")
  }

  private def forwardToController(request: RequestChannel.Request): Unit = {
    def responseCallback(responseOpt: Option[AbstractResponse]): Unit = {
      responseOpt match {
        case Some(response) => requestHelper.sendForwardedResponse(request, response)
        case None => handleInvalidVersionsDuringForwarding(request)
      }
    }

    forwardingManager.forwardRequest(request, responseCallback)
  }

  private def handleInvalidVersionsDuringForwarding(request: RequestChannel.Request): Unit = {
    info(s"The client connection will be closed due to controller responded " +
      s"unsupported version exception during $request forwarding. " +
      s"This could happen when the controller changed after the connection was established.")
    requestChannel.closeConnection(request, Collections.emptyMap())
  }

  /**
   * Top-level method that handles all requests and multiplexes to the right api
   */
  override def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    def handleError(e: Throwable): Unit = {
      error(s"Unexpected error handling request ${request.requestDesc(true)} " +
        s"with context ${request.context}", e)
      requestHelper.handleError(request, e)
    }

    try {
      trace(s"Handling request:${request.requestDesc(true)} from connection ${request.context.connectionId};" +
        s"securityProtocol:${request.context.securityProtocol},principal:${request.context.principal}")

      if (!apiVersionManager.isApiEnabled(request.header.apiKey, request.header.apiVersion)) {
        // The socket server will reject APIs which are not exposed in this scope and close the connection
        // before handing them to the request handler, so this path should not be exercised in practice
        throw new IllegalStateException(s"API ${request.header.apiKey} with version ${request.header.apiVersion} is not enabled")
      }

      request.header.apiKey match {
        case ApiKeys.PRODUCE => handleProduceRequest(request, requestLocal)
        case ApiKeys.FETCH => handleFetchRequest(request)
        case ApiKeys.LIST_OFFSETS => handleListOffsetRequest(request)
        case ApiKeys.METADATA => handleTopicMetadataRequest(request)
        case ApiKeys.OFFSET_COMMIT => handleOffsetCommitRequest(request, requestLocal).exceptionally(handleError)
        case ApiKeys.OFFSET_FETCH => handleOffsetFetchRequest(request).exceptionally(handleError)
        case ApiKeys.FIND_COORDINATOR => handleFindCoordinatorRequest(request)
        case ApiKeys.JOIN_GROUP => handleJoinGroupRequest(request, requestLocal).exceptionally(handleError)
        case ApiKeys.HEARTBEAT => handleHeartbeatRequest(request).exceptionally(handleError)
        case ApiKeys.LEAVE_GROUP => handleLeaveGroupRequest(request).exceptionally(handleError)
        case ApiKeys.SYNC_GROUP => handleSyncGroupRequest(request, requestLocal).exceptionally(handleError)
        case ApiKeys.DESCRIBE_GROUPS => handleDescribeGroupsRequest(request).exceptionally(handleError)
        case ApiKeys.LIST_GROUPS => handleListGroupsRequest(request).exceptionally(handleError)
        case ApiKeys.SASL_HANDSHAKE => handleSaslHandshakeRequest(request)
        case ApiKeys.API_VERSIONS => handleApiVersionsRequest(request)
        case ApiKeys.CREATE_TOPICS => forwardToController(request)
        case ApiKeys.DELETE_TOPICS => forwardToController(request)
        case ApiKeys.DELETE_RECORDS => handleDeleteRecordsRequest(request)
        case ApiKeys.INIT_PRODUCER_ID => handleInitProducerIdRequest(request, requestLocal)
        case ApiKeys.OFFSET_FOR_LEADER_EPOCH => handleOffsetForLeaderEpochRequest(request)
        case ApiKeys.ADD_PARTITIONS_TO_TXN => handleAddPartitionsToTxnRequest(request, requestLocal)
        case ApiKeys.ADD_OFFSETS_TO_TXN => handleAddOffsetsToTxnRequest(request, requestLocal)
        case ApiKeys.END_TXN => handleEndTxnRequest(request, requestLocal)
        case ApiKeys.WRITE_TXN_MARKERS => handleWriteTxnMarkersRequest(request, requestLocal)
        case ApiKeys.TXN_OFFSET_COMMIT => handleTxnOffsetCommitRequest(request, requestLocal).exceptionally(handleError)
        case ApiKeys.DESCRIBE_ACLS => handleDescribeAcls(request)
        case ApiKeys.CREATE_ACLS => forwardToController(request)
        case ApiKeys.DELETE_ACLS => forwardToController(request)
        case ApiKeys.ALTER_CONFIGS => handleAlterConfigsRequest(request)
        case ApiKeys.DESCRIBE_CONFIGS => handleDescribeConfigsRequest(request)
        case ApiKeys.ALTER_REPLICA_LOG_DIRS => handleAlterReplicaLogDirsRequest(request)
        case ApiKeys.DESCRIBE_LOG_DIRS => handleDescribeLogDirsRequest(request)
        case ApiKeys.SASL_AUTHENTICATE => handleSaslAuthenticateRequest(request)
        case ApiKeys.CREATE_PARTITIONS => forwardToController(request)
        // Create, renew and expire DelegationTokens must first validate that the connection
        // itself is not authenticated with a delegation token before maybeForwardToController.
        case ApiKeys.CREATE_DELEGATION_TOKEN => handleCreateTokenRequest(request)
        case ApiKeys.RENEW_DELEGATION_TOKEN => handleRenewTokenRequest(request)
        case ApiKeys.EXPIRE_DELEGATION_TOKEN => handleExpireTokenRequest(request)
        case ApiKeys.DESCRIBE_DELEGATION_TOKEN => handleDescribeTokensRequest(request)
        case ApiKeys.DELETE_GROUPS => handleDeleteGroupsRequest(request, requestLocal).exceptionally(handleError)
        case ApiKeys.ELECT_LEADERS => forwardToController(request)
        case ApiKeys.INCREMENTAL_ALTER_CONFIGS => handleIncrementalAlterConfigsRequest(request)
        case ApiKeys.ALTER_PARTITION_REASSIGNMENTS => forwardToController(request)
        case ApiKeys.LIST_PARTITION_REASSIGNMENTS => forwardToController(request)
        case ApiKeys.OFFSET_DELETE => handleOffsetDeleteRequest(request, requestLocal).exceptionally(handleError)
        case ApiKeys.DESCRIBE_CLIENT_QUOTAS => handleDescribeClientQuotasRequest(request)
        case ApiKeys.ALTER_CLIENT_QUOTAS => forwardToController(request)
        case ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS => handleDescribeUserScramCredentialsRequest(request)
        case ApiKeys.ALTER_USER_SCRAM_CREDENTIALS => forwardToController(request)
        case ApiKeys.UPDATE_FEATURES => forwardToController(request)
        case ApiKeys.DESCRIBE_CLUSTER => handleDescribeCluster(request)
        case ApiKeys.DESCRIBE_PRODUCERS => handleDescribeProducersRequest(request)
        case ApiKeys.UNREGISTER_BROKER => forwardToController(request)
        case ApiKeys.DESCRIBE_TRANSACTIONS => handleDescribeTransactionsRequest(request)
        case ApiKeys.LIST_TRANSACTIONS => handleListTransactionsRequest(request)
        case ApiKeys.DESCRIBE_QUORUM => forwardToController(request)
        case ApiKeys.CONSUMER_GROUP_HEARTBEAT => handleConsumerGroupHeartbeat(request).exceptionally(handleError)
        case ApiKeys.CONSUMER_GROUP_DESCRIBE => handleConsumerGroupDescribe(request).exceptionally(handleError)
        case ApiKeys.DESCRIBE_TOPIC_PARTITIONS => handleDescribeTopicPartitionsRequest(request)
        case ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS => handleGetTelemetrySubscriptionsRequest(request)
        case ApiKeys.PUSH_TELEMETRY => handlePushTelemetryRequest(request)
        case ApiKeys.LIST_CLIENT_METRICS_RESOURCES => handleListClientMetricsResources(request)
        case ApiKeys.ADD_RAFT_VOTER => forwardToController(request)
        case ApiKeys.REMOVE_RAFT_VOTER => forwardToController(request)
        case ApiKeys.SHARE_GROUP_HEARTBEAT => handleShareGroupHeartbeat(request).exceptionally(handleError)
        case ApiKeys.SHARE_GROUP_DESCRIBE => handleShareGroupDescribe(request).exceptionally(handleError)
        case ApiKeys.SHARE_FETCH => handleShareFetchRequest(request)
        case ApiKeys.SHARE_ACKNOWLEDGE => handleShareAcknowledgeRequest(request)
        case ApiKeys.INITIALIZE_SHARE_GROUP_STATE => handleInitializeShareGroupStateRequest(request)
        case ApiKeys.READ_SHARE_GROUP_STATE => handleReadShareGroupStateRequest(request)
        case ApiKeys.WRITE_SHARE_GROUP_STATE => handleWriteShareGroupStateRequest(request)
        case ApiKeys.DELETE_SHARE_GROUP_STATE => handleDeleteShareGroupStateRequest(request)
        case ApiKeys.READ_SHARE_GROUP_STATE_SUMMARY => handleReadShareGroupStateSummaryRequest(request)
        case _ => throw new IllegalStateException(s"No handler for request api key ${request.header.apiKey}")
      }
    } catch {
      case e: FatalExitError => throw e
      case e: Throwable => handleError(e)
    } finally {
      // try to complete delayed action. In order to avoid conflicting locking, the actions to complete delayed requests
      // are kept in a queue. We add the logic to check the ReplicaManager queue at the end of KafkaApis.handle() and the
      // expiration thread for certain delayed operations (e.g. DelayedJoin)
      // Delayed fetches are also completed by ReplicaFetcherThread.
      replicaManager.tryCompleteActions()
      // The local completion time may be set while processing the request. Only record it if it's unset.
      if (request.apiLocalCompleteTimeNanos < 0)
        request.apiLocalCompleteTimeNanos = time.nanoseconds
    }
  }

  override def tryCompleteActions(): Unit = {
    replicaManager.tryCompleteActions()
  }

  /**
   * Handle an offset commit request
   */
  def handleOffsetCommitRequest(
    request: RequestChannel.Request,
    requestLocal: RequestLocal
  ): CompletableFuture[Unit] = {
    val offsetCommitRequest = request.body[OffsetCommitRequest]

    // Reject the request if not authorized to the group
    if (!authHelper.authorize(request.context, READ, GROUP, offsetCommitRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, offsetCommitRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      val authorizedTopics = authHelper.filterByAuthorized(
        request.context,
        READ,
        TOPIC,
        offsetCommitRequest.data.topics.asScala
      )(_.name)

      val responseBuilder = new OffsetCommitResponse.Builder()
      val authorizedTopicsRequest = new mutable.ArrayBuffer[OffsetCommitRequestData.OffsetCommitRequestTopic]()
      offsetCommitRequest.data.topics.forEach { topic =>
        if (!authorizedTopics.contains(topic.name)) {
          // If the topic is not authorized, we add the topic and all its partitions
          // to the response with TOPIC_AUTHORIZATION_FAILED.
          responseBuilder.addPartitions[OffsetCommitRequestData.OffsetCommitRequestPartition](
            topic.name, topic.partitions, _.partitionIndex, Errors.TOPIC_AUTHORIZATION_FAILED)
        } else if (!metadataCache.contains(topic.name)) {
          // If the topic is unknown, we add the topic and all its partitions
          // to the response with UNKNOWN_TOPIC_OR_PARTITION.
          responseBuilder.addPartitions[OffsetCommitRequestData.OffsetCommitRequestPartition](
            topic.name, topic.partitions, _.partitionIndex, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        } else {
          // Otherwise, we check all partitions to ensure that they all exist.
          val topicWithValidPartitions = new OffsetCommitRequestData.OffsetCommitRequestTopic().setName(topic.name)
          val isView = isViewTopic(topic.name)

          topic.partitions.forEach { partition =>
            if (metadataCache.getLeaderAndIsr(topic.name, partition.partitionIndex).nonEmpty) {
              // Normalize committedLeaderEpoch on view partitions: views have no client-validatable
              // epoch ledger (see handleOffsetForLeaderEpochRequest), so accepting a non-(-1) value
              // would persist a backing-derived or stale epoch under the view name in
              // __consumer_offsets. On the next session start, OffsetFetch would replay it into
              // SubscriptionState.position.offsetEpoch → AWAIT_VALIDATION → OFLE → infinite retry.
              // The OffsetFetch response scrub at handleOffsetFetchRequest's allOf callback already
              // catches replay; this request-side normalization closes the write side so the
              // poisoned value never reaches the offset store in the first place.
              if (isView) partition.setCommittedLeaderEpoch(-1)
              topicWithValidPartitions.partitions.add(partition)
            } else {
              responseBuilder.addPartition(topic.name, partition.partitionIndex, Errors.UNKNOWN_TOPIC_OR_PARTITION)
            }
          }

          if (!topicWithValidPartitions.partitions.isEmpty) {
            authorizedTopicsRequest += topicWithValidPartitions
          }
        }
      }

      if (authorizedTopicsRequest.isEmpty) {
        requestHelper.sendMaybeThrottle(request, responseBuilder.build())
        CompletableFuture.completedFuture(())
      } else {
        // For version > 0, store offsets in Coordinator.
        commitOffsetsToCoordinator(
          request,
          offsetCommitRequest,
          authorizedTopicsRequest,
          responseBuilder,
          requestLocal
        )
      }
    }
  }

  private def commitOffsetsToCoordinator(
    request: RequestChannel.Request,
    offsetCommitRequest: OffsetCommitRequest,
    authorizedTopicsRequest: mutable.ArrayBuffer[OffsetCommitRequestData.OffsetCommitRequestTopic],
    responseBuilder: OffsetCommitResponse.Builder,
    requestLocal: RequestLocal
  ): CompletableFuture[Unit] = {
    val offsetCommitRequestData = new OffsetCommitRequestData()
      .setGroupId(offsetCommitRequest.data.groupId)
      .setMemberId(offsetCommitRequest.data.memberId)
      .setGenerationIdOrMemberEpoch(offsetCommitRequest.data.generationIdOrMemberEpoch)
      .setRetentionTimeMs(offsetCommitRequest.data.retentionTimeMs)
      .setGroupInstanceId(offsetCommitRequest.data.groupInstanceId)
      .setTopics(authorizedTopicsRequest.asJava)

    groupCoordinator.commitOffsets(
      request.context,
      offsetCommitRequestData,
      requestLocal.bufferSupplier
    ).handle[Unit] { (results, exception) =>
      if (exception != null) {
        requestHelper.sendMaybeThrottle(request, offsetCommitRequest.getErrorResponse(exception))
      } else {
        requestHelper.sendMaybeThrottle(request, responseBuilder.merge(results).build())
      }
    }
  }

  case class LeaderNode(leaderId: Int, leaderEpoch: Int, node: Option[Node])

  private def getCurrentLeader(tp: TopicPartition, ln: ListenerName): LeaderNode = {
    val partitionInfoOrError = replicaManager.getPartitionOrError(tp)
    val (leaderId, leaderEpoch) = partitionInfoOrError match {
      case Right(x) =>
        (x.leaderReplicaIdOpt.getOrElse(-1), x.getLeaderEpoch)
      case Left(x) =>
        debug(s"Unable to retrieve local leaderId and Epoch with error $x, falling back to metadata cache")
        metadataCache.getLeaderAndIsr(tp.topic, tp.partition) match {
          case Some(pinfo) => (pinfo.leader(), pinfo.leaderEpoch())
          case None => (-1, -1)
        }
    }
    LeaderNode(leaderId, leaderEpoch, metadataCache.getAliveBrokerNode(leaderId, ln))
  }

  /**
   * Handle a produce request
   */
  def handleProduceRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val produceRequest = request.body[ProduceRequest]

    if (RequestUtils.hasTransactionalRecords(produceRequest)) {
      val isAuthorizedTransactional = produceRequest.transactionalId != null &&
        authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, produceRequest.transactionalId)
      if (!isAuthorizedTransactional) {
        requestHelper.sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
        return
      }
    }

    val unauthorizedTopicResponses = mutable.Map[TopicPartition, PartitionResponse]()
    val nonExistingTopicResponses = mutable.Map[TopicPartition, PartitionResponse]()
    val invalidRequestResponses = mutable.Map[TopicPartition, PartitionResponse]()
    val authorizedRequestInfo = mutable.Map[TopicPartition, MemoryRecords]()
    // cache the result to avoid redundant authorization calls
    val authorizedTopics = authHelper.filterByAuthorized(request.context, WRITE, TOPIC,
      produceRequest.data().topicData().asScala)(_.name())

    produceRequest.data.topicData.forEach(topic => topic.partitionData.forEach { partition =>
      val topicPartition = new TopicPartition(topic.name, partition.index)
      // This caller assumes the type is MemoryRecords and that is true on current serialization
      // We cast the type to avoid causing big change to code base.
      // https://issues.apache.org/jira/browse/KAFKA-10698
      val memoryRecords = partition.records.asInstanceOf[MemoryRecords]
      if (!authorizedTopics.contains(topicPartition.topic))
        unauthorizedTopicResponses += topicPartition -> new PartitionResponse(Errors.TOPIC_AUTHORIZATION_FAILED)
      else if (!metadataCache.contains(topicPartition))
        nonExistingTopicResponses += topicPartition -> new PartitionResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION)
      else if (isViewTopic(topicPartition.topic))
        // Views are read-only. PROMPT.md mandates this rejection happens before any backing-topic
        // resolution — produce never resolves to the backing topic, so positioning the check here,
        // before validateRecords (which has no backing-topic awareness anyway), satisfies that.
        invalidRequestResponses += topicPartition -> new PartitionResponse(
          Errors.INVALID_REQUEST,
          s"Cannot produce to view topic '${topicPartition.topic}': views are read-only.")
      else
        try {
          ProduceRequest.validateRecords(request.header.apiVersion, memoryRecords)
          authorizedRequestInfo += (topicPartition -> memoryRecords)
        } catch {
          case e: ApiException =>
            invalidRequestResponses += topicPartition -> new PartitionResponse(Errors.forException(e))
        }
    })

    // the callback for sending a produce response
    // The construction of ProduceResponse is able to accept auto-generated protocol data so
    // KafkaApis#handleProduceRequest should apply auto-generated protocol to avoid extra conversion.
    // https://issues.apache.org/jira/browse/KAFKA-10730
    @nowarn("cat=deprecation")
    def sendResponseCallback(responseStatus: Map[TopicPartition, PartitionResponse]): Unit = {
      val mergedResponseStatus = responseStatus ++ unauthorizedTopicResponses ++ nonExistingTopicResponses ++ invalidRequestResponses
      var errorInResponse = false

      val nodeEndpoints = new mutable.HashMap[Int, Node]
      mergedResponseStatus.foreachEntry { (topicPartition, status) =>
        if (status.error != Errors.NONE) {
          errorInResponse = true
          debug("Produce request with correlation id %d from client %s on partition %s failed due to %s".format(
            request.header.correlationId,
            request.header.clientId,
            topicPartition,
            status.error.exceptionName))

          if (request.header.apiVersion >= 10) {
            status.error match {
              case Errors.NOT_LEADER_OR_FOLLOWER =>
                val leaderNode = getCurrentLeader(topicPartition, request.context.listenerName)
                leaderNode.node.foreach { node =>
                  nodeEndpoints.put(node.id(), node)
                }
                status.currentLeader
                  .setLeaderId(leaderNode.leaderId)
                  .setLeaderEpoch(leaderNode.leaderEpoch)
                case _ =>
            }
          }
        }
      }

      // Record both bandwidth and request quota-specific values and throttle by muting the channel if any of the quotas
      // have been violated. If both quotas have been violated, use the max throttle time between the two quotas. Note
      // that the request quota is not enforced if acks == 0.
      val timeMs = time.milliseconds()
      val requestSize = request.sizeInBytes
      val bandwidthThrottleTimeMs = quotas.produce.maybeRecordAndGetThrottleTimeMs(request, requestSize, timeMs)
      val requestThrottleTimeMs =
        if (produceRequest.acks == 0) 0
        else quotas.request.maybeRecordAndGetThrottleTimeMs(request, timeMs)
      val maxThrottleTimeMs = Math.max(bandwidthThrottleTimeMs, requestThrottleTimeMs)
      if (maxThrottleTimeMs > 0) {
        request.apiThrottleTimeMs = maxThrottleTimeMs
        if (bandwidthThrottleTimeMs > requestThrottleTimeMs) {
          requestHelper.throttle(quotas.produce, request, bandwidthThrottleTimeMs)
        } else {
          requestHelper.throttle(quotas.request, request, requestThrottleTimeMs)
        }
      }

      // Send the response immediately. In case of throttling, the channel has already been muted.
      if (produceRequest.acks == 0) {
        // no operation needed if producer request.required.acks = 0; however, if there is any error in handling
        // the request, since no response is expected by the producer, the server will close socket server so that
        // the producer client will know that some error has happened and will refresh its metadata
        if (errorInResponse) {
          val exceptionsSummary = mergedResponseStatus.map { case (topicPartition, status) =>
            topicPartition -> status.error.exceptionName
          }.mkString(", ")
          info(
            s"Closing connection due to error during produce request with correlation id ${request.header.correlationId} " +
              s"from client id ${request.header.clientId} with ack=0\n" +
              s"Topic and partition to exceptions: $exceptionsSummary"
          )
          requestChannel.closeConnection(request, new ProduceResponse(mergedResponseStatus.asJava).errorCounts)
        } else {
          // Note that although request throttling is exempt for acks == 0, the channel may be throttled due to
          // bandwidth quota violation.
          requestHelper.sendNoOpResponseExemptThrottle(request)
        }
      } else {
        requestChannel.sendResponse(request, new ProduceResponse(mergedResponseStatus.asJava, maxThrottleTimeMs, nodeEndpoints.values.toList.asJava), None)
      }
    }

    def processingStatsCallback(processingStats: FetchResponseStats): Unit = {
      processingStats.foreachEntry { (tp, info) =>
        updateRecordConversionStats(request, tp, info)
      }
    }

    if (authorizedRequestInfo.isEmpty)
      sendResponseCallback(Map.empty)
    else {
      val internalTopicsAllowed = request.header.clientId == AdminUtils.ADMIN_CLIENT_ID
      val transactionSupportedOperation = AddPartitionsToTxnManager.produceRequestVersionToTransactionSupportedOperation(request.header.apiVersion())
      // call the replica manager to append messages to the replicas
      replicaManager.handleProduceAppend(
        timeout = produceRequest.timeout.toLong,
        requiredAcks = produceRequest.acks,
        internalTopicsAllowed = internalTopicsAllowed,
        transactionalId = produceRequest.transactionalId,
        entriesPerPartition = authorizedRequestInfo,
        responseCallback = sendResponseCallback,
        recordValidationStatsCallback = processingStatsCallback,
        requestLocal = requestLocal,
        transactionSupportedOperation = transactionSupportedOperation)

      // if the request is put into the purgatory, it will have a held reference and hence cannot be garbage collected;
      // hence we clear its data here in order to let GC reclaim its memory since it is already appended to log
      produceRequest.clearPartitionRecords()
    }
  }

  // A topic is a view iff all three view configs are present and non-blank in its Properties.
  // We go through TopicViewConfigs.fromMap (the same path used by the view runtime) so the
  // all-or-none rule lives in exactly one place. Produce-rejection deliberately uses this
  // light-touch check instead of viewRegistry.viewFor — produce never needs the compiled
  // predicate, and skipping compilation here keeps a malformed predicate from masking the
  // read-only intent of the topic.
  private[server] def isViewTopic(topicName: String): Boolean =
    topicViewConfigsFor(topicName).isPresent

  // The config source threaded into viewRegistry. Lives here (not in ViewRegistry) so that the
  // Properties → Map conversion stays in Scala and the registry stays Properties-free.
  private def topicViewConfigsFor(topicName: String): Optional[ViewRegistry.TopicViewConfigs] = {
    val props = configRepository.topicConfig(topicName)
    if (props == null || props.isEmpty) return Optional.empty[ViewRegistry.TopicViewConfigs]()
    val map = new util.HashMap[String, String]()
    val it = props.stringPropertyNames().iterator()
    while (it.hasNext) {
      val k = it.next()
      val v = props.getProperty(k)
      if (v != null) map.put(k, v)
    }
    ViewRegistry.TopicViewConfigs.fromMap(map)
  }

  /**
   * Handle a fetch request
   */
  def handleFetchRequest(request: RequestChannel.Request): Unit = {
    val versionId = request.header.apiVersion
    val clientId = request.header.clientId
    val fetchRequest = request.body[FetchRequest]
    val topicNames =
      if (fetchRequest.version() >= 13)
        metadataCache.topicIdsToNames()
      else
        Collections.emptyMap[Uuid, String]()

    val fetchData = fetchRequest.fetchData(topicNames)
    val forgottenTopics = fetchRequest.forgottenTopics(topicNames)

    val fetchContext = fetchManager.newContext(
      fetchRequest.version,
      fetchRequest.metadata,
      fetchRequest.isFromFollower,
      fetchData,
      forgottenTopics,
      topicNames)

    val erroneous = mutable.ArrayBuffer[(TopicIdPartition, FetchResponseData.PartitionData)]()
    val interesting = mutable.ArrayBuffer[(TopicIdPartition, FetchRequest.PartitionData)]()
    if (fetchRequest.isFromFollower) {
      // The follower must have ClusterAction on ClusterResource in order to fetch partition data.
      if (authHelper.authorize(request.context, CLUSTER_ACTION, CLUSTER, CLUSTER_NAME)) {
        fetchContext.foreachPartition { (topicIdPartition, data) =>
          if (topicIdPartition.topic == null)
            erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_ID)
          else if (!metadataCache.contains(topicIdPartition.topicPartition))
            erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
          else
            interesting += topicIdPartition -> data
        }
      } else {
        fetchContext.foreachPartition { (topicIdPartition, _) =>
          erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.TOPIC_AUTHORIZATION_FAILED)
        }
      }
    } else {
      // Regular Kafka consumers need READ permission on each partition they are fetching.
      val partitionDatas = new mutable.ArrayBuffer[(TopicIdPartition, FetchRequest.PartitionData)]
      fetchContext.foreachPartition { (topicIdPartition, partitionData) =>
        if (topicIdPartition.topic == null)
          erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_ID)
        else
          partitionDatas += topicIdPartition -> partitionData
      }
      val authorizedTopics = authHelper.filterByAuthorized(request.context, READ, TOPIC, partitionDatas)(_._1.topicPartition.topic)
      partitionDatas.foreach { case (topicIdPartition, data) =>
        if (!authorizedTopics.contains(topicIdPartition.topic))
          erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.TOPIC_AUTHORIZATION_FAILED)
        else if (!metadataCache.contains(topicIdPartition.topicPartition))
          erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        else
          interesting += topicIdPartition -> data
      }
    }

    // View redirection: any partition whose topic is configured as a view gets rewritten to its
    // backing topic for the actual fetch. The mapping is recorded here so the response callback
    // can filter records and key the response back at the view. Follower fetches are skipped on
    // purpose — views are a consumer-side concept; replicas mirror the physical (backing) topic.
    //
    // AUTHORIZATION MODEL: the READ-on-TOPIC check above (line ~625) ran against the view's
    // topic name. The redirect to the backing topic happens HERE, after the ACL decision; the
    // backing topic's ACLs are deliberately NOT re-checked on the redirect path. This is the
    // intended behavior: views exist to expose a filtered projection of a sensitive backing
    // topic to consumers who should not be able to read the unfiltered stream. Operators must
    // therefore treat the choice of view.cel.predicate as a security-relevant decision — a
    // permissive predicate effectively grants READ on the matching subset of the backing topic.
    // See ViewTopicConfig javadoc for the full model. Do NOT add a second READ check against
    // the backing topic here without first reviewing the threat model: it would defeat the
    // primary use case of views (selective READ access). Restrictions on view consumers reading
    // the backing topic *directly* are still enforced by the backing topic's own ACLs.
    //
    // Single-view-per-backing-partition-per-request invariant: if a single FetchRequest contains
    // two views over the SAME backing partition, the second is rejected with INVALID_REQUEST. The
    // honest answer here is that the replica fetch API is keyed by TopicIdPartition, so two views
    // over one backing partition would either collide on that key (silently dropping one) or
    // require two independent fetches at potentially different offsets — neither is something the
    // current fetch path can express. Surface the collision loudly to the operator instead of
    // silently mis-attributing records. Lift this restriction (and add proper backing-fetch
    // fan-out) only if operational demand emerges; today no consumer subscribes to two views over
    // one backing partition in one FetchRequest.
    //
    // The same invariant must also hold for the asymmetric case where ONE view AND its backing
    // are requested in a single FetchRequest. Otherwise `rewritten` would contain duplicate
    // backingTpId entries (one from the view's redirect, one from the direct backing fetch),
    // which would either be deduped by the replica layer (silently dropping data) or read twice,
    // and the response callback would apply the view filter to the consumer's direct backing
    // fetch — leaking the view's identity into a fetch the consumer never asked to be filtered.
    // We pre-classify entries into "direct" vs "view" in one pass, then route in a second pass
    // so the order in which the view and its backing appear in the FetchRequest doesn't matter.
    val viewRewrites = mutable.Map[TopicIdPartition, (TopicIdPartition, ViewSpec)]()
    if (!fetchRequest.isFromFollower && interesting.nonEmpty) {
      // First pass: classify each interesting entry. Outcomes per entry:
      //   - Right(Some(spec))    — entry is a view; route via redirect
      //   - Right(None)          — entry is a direct (non-view) fetch
      //   - Left(exception)      — viewFor blew up; reject with INVALID_REQUEST
      // We materialize the classification map so the second pass can detect collisions between
      // views' backing TpIds and other entries' direct TpIds regardless of request order.
      val classified = new mutable.ArrayBuffer[(TopicIdPartition, FetchRequest.PartitionData, Either[Exception, Optional[ViewSpec]])](interesting.size)
      // Keyed by TopicPartition (name+partition), not TopicIdPartition, so the collision check
      // works for Fetch v12 and older. v12's wire format has no topic ID field, so its entries
      // arrive with Uuid.ZERO_UUID (see FetchRequest.fetchData), while the view-redirect path
      // builds backingTpId via metadataCache.getTopicId(...) which returns the real Uuid. A
      // TopicIdPartition-keyed set would compare unequal between those two and silently miss the
      // collision — letting the dispatch hit the "duplicate entries by name" failure mode this
      // pass is meant to prevent. Topic NAMES are unique within the broker at any given moment,
      // so the (name, partition) tuple is the right collision key regardless of fetch version.
      val directTpIds = mutable.Set[TopicPartition]()
      interesting.foreach { case (tpId, data) =>
        val result: Either[Exception, Optional[ViewSpec]] = try {
          Right(viewRegistry.viewFor(tpId.topic))
        } catch {
          case e: Exception => Left(e)
        }
        classified += ((tpId, data, result))
        result match {
          case Right(opt) if !opt.isPresent => directTpIds.add(tpId.topicPartition)
          case _ => // view, or threw — not a direct fetch
        }
      }

      // Second pass: route. Reuses `viewRewrites` for the multi-view-same-backing detection (unchanged
      // semantics) and adds a directTpIds membership check for the view-and-backing-same-request case.
      val rewritten = new mutable.ArrayBuffer[(TopicIdPartition, FetchRequest.PartitionData)](interesting.size)
      classified.foreach { case (viewTpId, data, classification) =>
        classification match {
          case Left(e) =>
            // A malformed view config (predicate compile failure, self-loop) reaching this point
            // means LogConfig validation was bypassed somehow. Fail the fetch cleanly instead of
            // letting the exception escape. WARN (not ERROR) because broker liveness is fine —
            // only this one view is broken, and operators get the same actionable signal.
            warn(s"Failed to load view spec for topic ${viewTpId.topic}; rejecting fetch with INVALID_REQUEST", e)
            erroneous += viewTpId -> FetchResponse.partitionResponse(viewTpId, Errors.INVALID_REQUEST)
          case Right(maybeSpec) if maybeSpec.isPresent =>
            val spec = maybeSpec.get()
            val backingName = spec.backingTopic()
            val backingTp = new TopicPartition(backingName, viewTpId.partition)
            if (!metadataCache.contains(backingTp)) {
              // The view points at a non-existent backing topic in cluster metadata (typo in
              // config, or the backing topic was deleted out from under the view). Surface that
              // as UNKNOWN_TOPIC_OR_PARTITION keyed at the *view* so the consumer does not learn
              // about the backing topic name.
              erroneous += viewTpId -> FetchResponse.partitionResponse(viewTpId, Errors.UNKNOWN_TOPIC_OR_PARTITION)
            } else if (replicaManager.getPartitionOrError(backingTp).isLeft) {
              // The backing topic exists in cluster metadata but this broker is not a replica of
              // backing partition N. We reached this broker because it is the leader of view
              // partition N; without aligned partition assignment we cannot read backing N
              // locally and PROMPT.md does not call for cross-broker proxying (see ViewSpec
              // javadoc — "Partition-assignment constraint"). Fail-fast at routing time with
              // UNKNOWN_TOPIC_OR_PARTITION keyed at the view; the alternative is letting the
              // fetch reach replicaManager.fetchMessages where it would surface as
              // NOT_LEADER_OR_FOLLOWER and trigger client metadata-refresh-retry loops (the view
              // leader hasn't moved, so the consumer would hammer this broker forever). The WARN
              // makes the misconfig visible to operators in the broker log.
              warn(s"Fetch rejected: view ${viewTpId.topic} redirects to backing partition $backingTp, " +
                s"but this broker is not a replica of the backing partition. View and backing must " +
                s"share the same replica assignment (see ViewSpec javadoc). Returning " +
                s"UNKNOWN_TOPIC_OR_PARTITION keyed at the view.")
              erroneous += viewTpId -> FetchResponse.partitionResponse(viewTpId, Errors.UNKNOWN_TOPIC_OR_PARTITION)
            } else {
              val backingTpId = new TopicIdPartition(metadataCache.getTopicId(backingName), backingTp)
              if (directTpIds.contains(backingTp)) {
                // The view redirects to a backing TpId that is ALSO requested directly in this same
                // FetchRequest. Routing both would produce duplicate backingTpId entries in `rewritten`
                // (the replica layer is keyed by TopicIdPartition) and the response callback would apply
                // the view filter to the direct fetch's data — silently filtering bytes the consumer
                // explicitly asked for unfiltered. Reject the VIEW side (not the direct backing fetch:
                // the consumer holds READ on the backing topic and is entitled to raw bytes), keyed at
                // the view's topic so the consumer sees the rejection against the name it asked for.
                warn(s"Fetch rejected: view ${viewTpId.topic} redirects to backing partition $backingTp " +
                  s"which is also a direct fetch target in the same FetchRequest. The replica fetch API " +
                  s"cannot route both; failing the view with INVALID_REQUEST. Issue the view in a separate " +
                  s"fetch session.")
                erroneous += viewTpId -> FetchResponse.partitionResponse(viewTpId, Errors.INVALID_REQUEST)
              } else if (viewRewrites.contains(backingTpId)) {
                // Collision: another view over the same backing partition is already in this request.
                // Don't append a duplicate entry to `rewritten` (that would either be deduped by the
                // replica layer, silently dropping the first fetch's data, or read the backing twice
                // — both wrong). Reject the colliding view keyed at the *view*'s topic so the consumer
                // sees a clean error against the name it asked for.
                val firstViewTpId = viewRewrites(backingTpId)._1
                warn(s"Fetch rejected: view ${viewTpId.topic} and view ${firstViewTpId.topic} both target backing " +
                  s"partition $backingTp in one FetchRequest. The replica fetch API cannot dispatch this; " +
                  s"failing the second view with INVALID_REQUEST. Issue the views in separate fetch sessions.")
                erroneous += viewTpId -> FetchResponse.partitionResponse(viewTpId, Errors.INVALID_REQUEST)
              } else {
                // Leader-epoch boundary (KIP-595): the consumer's `currentLeaderEpoch` reflects the
                // VIEW's leader-epoch — view and backing topics maintain independent epoch ledgers
                // (a view-leader change does not bump the backing's epoch, and vice versa). Passing
                // `data` through unchanged would let the replica layer validate the view's epoch
                // against the BACKING partition's epoch, which (except by coincidence at epoch 0)
                // surfaces FENCED_LEADER_EPOCH or UNKNOWN_LEADER_EPOCH on every modern-consumer
                // fetch and breaks the feature wholesale.
                //
                // We validate the consumer's epoch against the VIEW partition here, then strip
                // both the leader-epoch fields before redirecting so the backing fetch sees an
                // epoch-less request. The view-partition `localLogWithEpochOrThrow` enforces the
                // same fenced/unknown/not-leader semantics a consumer would see against any other
                // topic, keyed at the view's TopicIdPartition so the error is attributed correctly.
                //
                // `lastFetchedEpoch` is stripped on the same logic: it carries the view-epoch from
                // which the consumer last received a record (KIP-320 divergence detection), which
                // would also mis-validate against the backing's epoch history. View consumers
                // therefore lose KIP-320 divergence detection on the backing path; the view leader
                // itself rarely diverges (it is a passive predicate, not a replicated state
                // machine), so the practical exposure is small. Document this in ViewSpec javadoc
                // if the gap matters operationally.
                //
                // `viewFetchOnlyLeader` mirrors `FetchParams.fetchOnlyLeader()` for consumer
                // fetches: pre-v11 consumers cannot read from followers (no clientMetadata path);
                // v11+ consumers may, so don't require leader. Follower fetches are excluded by
                // the enclosing `!fetchRequest.isFromFollower` guard.
                val viewFetchOnlyLeader = versionId < 11
                val epochError: Errors = replicaManager.getPartitionOrError(viewTpId.topicPartition) match {
                  case Left(error) => error
                  case Right(viewPartition) =>
                    try {
                      viewPartition.localLogWithEpochOrThrow(data.currentLeaderEpoch, viewFetchOnlyLeader)
                      Errors.NONE
                    } catch {
                      case e: ApiException => Errors.forException(e)
                    }
                }
                if (epochError != Errors.NONE) {
                  erroneous += viewTpId -> FetchResponse.partitionResponse(viewTpId, epochError)
                } else {
                  val backingData = new FetchRequest.PartitionData(
                    backingTpId.topicId,
                    data.fetchOffset,
                    data.logStartOffset,
                    data.maxBytes,
                    Optional.empty[Integer](),
                    Optional.empty[Integer]())
                  rewritten += backingTpId -> backingData
                  viewRewrites.put(backingTpId, (viewTpId, spec))
                }
              }
            }
          case Right(_) =>
            // Not a view — pass through as a direct fetch.
            rewritten += viewTpId -> data
        }
      }
      interesting.clear()
      interesting ++= rewritten
    }

    def maybeDownConvertStorageError(error: Errors): Errors = {
      // If consumer sends FetchRequest V5 or earlier, the client library is not guaranteed to recognize the error code
      // for KafkaStorageException. In this case the client library will translate KafkaStorageException to
      // UnknownServerException which is not retriable. We can ensure that consumer will update metadata and retry
      // by converting the KafkaStorageException to NotLeaderOrFollowerException in the response if FetchRequest version <= 5
      if (error == Errors.KAFKA_STORAGE_ERROR && versionId <= 5) {
        Errors.NOT_LEADER_OR_FOLLOWER
      } else {
        error
      }
    }

    // Rewrite a single (backingTpId, FetchPartitionData) into its view form: filter records via
    // the view's predicate and re-key at the view's TopicIdPartition. Errors are passed through
    // unfiltered (only the key changes) so the consumer sees the failure under the topic name it
    // asked for, not the backing topic.
    //
    // PROMPT.md: "all filtering broker-side". The local-disk fetch path returns FileRecords for
    // zero-copy network transfer; we cannot pass those through (the predicate has not run) so we
    // materialize them into MemoryRecords first. This intentionally gives up zero-copy on the
    // view path — that is the cost of broker-side filtering and is what views opt into. Any
    // other Records subtype the broker may grow in future (e.g. tier-storage shapes that have not
    // materialized into MemoryRecords/FileRecords by the time the fetch callback fires, or
    // unaligned snapshot records that have leaked outside the raft path) is rejected with
    // KAFKA_STORAGE_ERROR rather than passed through: leaking unfiltered backing-topic bytes to
    // a consumer that only has READ on the view would defeat the whole feature. Today both
    // RemoteLogManager and the in-memory replication paths land as MemoryRecords pre-callback,
    // so the fallback is defensive — but it must stay because a future read-path can ship a new
    // Records subclass and silently break the view contract otherwise.
    //
    // The FileRecords slurp and the filterTo decompression staging both go through this supplier
    // so buffers are reused across all view partitions in a single fetch callback. The
    // destination buffer that backs the filtered MemoryRecords cannot be pooled — it has to
    // survive past this method until the response is serialized on the network thread — so peak
    // per-partition allocation is still 1× input size plus the slurp buffer's high-water mark.
    // What this supplier does eliminate is the per-batch decompression-staging churn inside
    // filterTo (significant for compressed batches, which dominate production fetches) and the
    // per-partition cost of re-allocating the FileRecords slurp buffer when one FetchRequest
    // touches several view partitions.
    def applyViewFilter(backingTpId: TopicIdPartition,
                        data: FetchPartitionData,
                        viewTpId: TopicIdPartition,
                        spec: ViewSpec,
                        bufferSupplier: BufferSupplier): (TopicIdPartition, FetchPartitionData) = {
      if (data.error != Errors.NONE) {
        // Even on a backing-side error, scrub the two response fields that would otherwise
        // wedge or mislead a view consumer:
        //  - divergingEpoch: today no error-producing LogReadResult carries a non-empty
        //    marker (the replica layer only computes it on successful reads through
        //    Partition.readRecords), but defense-in-depth keeps the backing epoch out of the
        //    view-named response if a future path combines an error with a divergence marker.
        //  - preferredReadReplica: ReplicaManager computes PRR against the BACKING tp's replica
        //    set (the fetch is redirected). On an error response the suggestion would still
        //    point at a backing replica that may not exist in the view's replica set, wedging
        //    rack-aware consumers exactly as in the success path. Same justification as the
        //    main applyViewFilter return below — the prior fix (e2e7a0d048) covered both
        //    Right(records) and Left(err) but missed this early-error return.
        // highWatermark/logStartOffset/abortedTransactions pass through (source-sparse offsets
        // are inherent to the view design per PROMPT.md).
        return (viewTpId, new FetchPartitionData(
          data.error,
          data.highWatermark,
          data.logStartOffset,
          MemoryRecords.EMPTY,
          Optional.empty[FetchResponseData.EpochEndOffset](),
          data.lastStableOffset,
          data.abortedTransactions,
          java.util.OptionalInt.empty(),
          data.isReassignmentFetch))
      }
      val filtered: Either[Errors, MemoryRecords] = data.records match {
        case mr: MemoryRecords =>
          Right(ViewFilter.apply(spec.predicate(), mr, viewTpId.partition, viewMetrics, bufferSupplier))
        case fr: FileRecords =>
          // Slurp the on-disk slice into a heap buffer and reuse the MemoryRecords filter. The
          // buffer comes from the per-callback supplier so subsequent view partitions in the same
          // fetch can reuse it. Empty slices are short-circuited because ByteBuffer.allocate(0) +
          // readInto on a closed/empty FileRecords would still hit the channel.
          val size = fr.sizeInBytes()
          if (size == 0) Right(MemoryRecords.EMPTY)
          else {
            val buffer = bufferSupplier.get(size)
            try {
              // GrowableBufferSupplier may return a buffer larger than `size`; constrain to
              // exactly `size` so readFully fills the whole region and readableRecords sees the
              // right limit.
              buffer.clear()
              buffer.limit(size)
              fr.readInto(buffer, 0)
              val materialized = MemoryRecords.readableRecords(buffer)
              Right(ViewFilter.apply(spec.predicate(), materialized, viewTpId.partition, viewMetrics, bufferSupplier))
            } catch {
              case e: java.io.IOException =>
                error(s"View fetch for ${viewTpId.topic} failed to materialize FileRecords for filtering; " +
                  s"returning KAFKA_STORAGE_ERROR.", e)
                Left(Errors.KAFKA_STORAGE_ERROR)
            } finally {
              // Safe to release: filterTo has finished iterating the materialized records (the
              // filtered output lives in ViewFilter's destination buffer, not in `buffer`).
              bufferSupplier.release(buffer)
            }
          }
        case _ =>
          warn(s"View fetch for ${viewTpId.topic} received records of type ${data.records.getClass.getSimpleName} " +
            s"which cannot be filtered in place; failing with KAFKA_STORAGE_ERROR to avoid leaking unfiltered records.")
          Left(Errors.KAFKA_STORAGE_ERROR)
      }
      // `divergingEpoch` (KIP-320 truncation marker) is rewritten to empty on the view path. Two
      // reasons stacked: (1) the field carries (epoch, end_offset) from the BACKING topic's
      // leader-epoch ledger — same backing-vs-view cross-ledger hazard that wedges the consumer
      // via partition_leader_epoch (cf. ViewFilter's strip). (2) The replica layer should not be
      // computing a divergingEpoch in the first place: we already strip both `currentLeaderEpoch`
      // and `lastFetchedEpoch` from the request before redirecting to the backing (cf. line 805
      // 'val backingData = new FetchRequest.PartitionData(...)' construction), so
      // Partition.fetchRecords cannot enter its `if (fetchEpochOpt.isPresent)` epoch-divergence
      // branch (Partition.scala:1538). If the request-side strip ever regresses (or if some
      // future fetch path computes divergingEpoch without consulting lastFetchedEpoch), this
      // response-side strip ensures the backing epoch still cannot leak under the view name.
      // Consumers don't read divergingEpoch today (only the Raft client and the broker-internal
      // AbstractFetcherThread follower-replication path do), but defense-in-depth keeps the view
      // contract intact if a future consumer KIP starts consuming the field.
      val safeDivergingEpoch = Optional.empty[FetchResponseData.EpochEndOffset]()
      // Clear preferredReadReplica. ReplicaManager computes PRR against the BACKING tp's replica
      // set (the fetch is redirected to the backing): if the backing has replicas {1,3} and the
      // view has replicas {1,2}, a rack-aware backing fetch on broker 1 can suggest broker 3 as
      // the preferred read replica. The client then retries the VIEW partition against broker 3,
      // which is not a view replica and cannot serve it — wedging the consumer until the PRR
      // expires. Stripping PRR on view responses costs follower-read latency on view-only consumer
      // groups (acceptable: today the view leader is required to be a backing replica, so the
      // leader-served path already works) and is correct: a precise translation would require
      // intersecting backing and view replica sets, which we do not have here because view
      // replicas are not threaded into applyViewFilter.
      val safePreferredReadReplica = java.util.OptionalInt.empty()
      filtered match {
        case Right(records) =>
          (viewTpId, new FetchPartitionData(
            data.error,
            data.highWatermark,
            data.logStartOffset,
            records,
            safeDivergingEpoch,
            data.lastStableOffset,
            data.abortedTransactions,
            safePreferredReadReplica,
            data.isReassignmentFetch))
        case Left(err) =>
          (viewTpId, new FetchPartitionData(
            err,
            data.highWatermark,
            data.logStartOffset,
            MemoryRecords.EMPTY,
            safeDivergingEpoch,
            data.lastStableOffset,
            data.abortedTransactions,
            safePreferredReadReplica,
            data.isReassignmentFetch))
      }
    }

    // the callback for process a fetch response, invoked before throttling
    def processResponseCallback(responsePartitionData: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      val translated: Seq[(TopicIdPartition, FetchPartitionData)] =
        if (viewRewrites.isEmpty) responsePartitionData
        else {
          // One supplier shared across every view partition in this callback. GrowableBufferSupplier
          // keeps a single buffer that grows monotonically: cheap on single-view fetches (one alloc),
          // amortizing across multi-view fetches (reuse). Closed in `finally` so the slot doesn't
          // pin a buffer past the response build, which would defeat the point of pooling.
          val bufferSupplier: BufferSupplier = new BufferSupplier.GrowableBufferSupplier()
          try {
            responsePartitionData.map { case (backingTpId, data) =>
              viewRewrites.get(backingTpId) match {
                case Some((viewTpId, spec)) => applyViewFilter(backingTpId, data, viewTpId, spec, bufferSupplier)
                case None => (backingTpId, data)
              }
            }
          } finally {
            bufferSupplier.close()
          }
        }
      val partitions = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
      val reassigningPartitions = mutable.Set[TopicIdPartition]()
      val nodeEndpoints = new mutable.HashMap[Int, Node]
      translated.foreach { case (tp, data) =>
        val abortedTransactions = data.abortedTransactions.orElse(null)
        val lastStableOffset: Long = data.lastStableOffset.orElse(FetchResponse.INVALID_LAST_STABLE_OFFSET)
        if (data.isReassignmentFetch) reassigningPartitions.add(tp)
        val partitionData = new FetchResponseData.PartitionData()
          .setPartitionIndex(tp.partition)
          .setErrorCode(maybeDownConvertStorageError(data.error).code)
          .setHighWatermark(data.highWatermark)
          .setLastStableOffset(lastStableOffset)
          .setLogStartOffset(data.logStartOffset)
          .setAbortedTransactions(abortedTransactions)
          .setRecords(data.records)
          .setPreferredReadReplica(data.preferredReadReplica.orElse(FetchResponse.INVALID_PREFERRED_REPLICA_ID))

        if (versionId >= 16) {
          data.error match {
            case Errors.NOT_LEADER_OR_FOLLOWER | Errors.FENCED_LEADER_EPOCH =>
              val leaderNode = getCurrentLeader(tp.topicPartition(), request.context.listenerName)
              leaderNode.node.foreach { node =>
                nodeEndpoints.put(node.id(), node)
              }
              partitionData.currentLeader()
                .setLeaderId(leaderNode.leaderId)
                .setLeaderEpoch(leaderNode.leaderEpoch)
            case _ =>
          }
        }

        data.divergingEpoch.ifPresent(partitionData.setDivergingEpoch(_))
        partitions.put(tp, partitionData)
      }
      erroneous.foreach { case (tp, data) => partitions.put(tp, data) }

      def recordBytesOutMetric(fetchResponse: FetchResponse): Unit = {
        // record the bytes out metrics only when the response is being sent
        fetchResponse.data.responses.forEach { topicResponse =>
          topicResponse.partitions.forEach { data =>
            // If the topic name was not known, we will have no bytes out.
            if (topicResponse.topic != null) {
              val tp = new TopicIdPartition(topicResponse.topicId, new TopicPartition(topicResponse.topic, data.partitionIndex))
              brokerTopicStats.updateBytesOut(tp.topic, fetchRequest.isFromFollower, reassigningPartitions.contains(tp), FetchResponse.recordsSize(data))
            }
          }
        }
      }

      if (fetchRequest.isFromFollower) {
        // We've already evaluated against the quota and are good to go. Just need to record it now.
        val fetchResponse = fetchContext.updateAndGenerateResponseData(partitions, Seq.empty.asJava)
        val responseSize = KafkaApis.sizeOfThrottledPartitions(versionId, fetchResponse, quotas.leader)
        quotas.leader.record(responseSize)
        val responsePartitionsSize = fetchResponse.data().responses().stream().mapToInt(_.partitions().size()).sum()
        trace(s"Sending Fetch response with partitions.size=$responsePartitionsSize, " +
          s"metadata=${fetchResponse.sessionId}")
        recordBytesOutMetric(fetchResponse)
        requestHelper.sendResponseExemptThrottle(request, fetchResponse)
      } else {
        // Record both bandwidth and request quota-specific values and throttle by muting the channel if any of the
        // quotas have been violated. If both quotas have been violated, use the max throttle time between the two
        // quotas. When throttled, we unrecord the recorded bandwidth quota value.
        val responseSize = fetchContext.getResponseSize(partitions, versionId)
        val timeMs = time.milliseconds()
        val requestThrottleTimeMs = quotas.request.maybeRecordAndGetThrottleTimeMs(request, timeMs)
        val bandwidthThrottleTimeMs = quotas.fetch.maybeRecordAndGetThrottleTimeMs(request, responseSize, timeMs)

        val maxThrottleTimeMs = math.max(bandwidthThrottleTimeMs, requestThrottleTimeMs)
        val fetchResponse = if (maxThrottleTimeMs > 0) {
          request.apiThrottleTimeMs = maxThrottleTimeMs
          // Even if we need to throttle for request quota violation, we should "unrecord" the already recorded value
          // from the fetch quota because we are going to return an empty response.
          quotas.fetch.unrecordQuotaSensor(request, responseSize, timeMs)
          if (bandwidthThrottleTimeMs > requestThrottleTimeMs) {
            requestHelper.throttle(quotas.fetch, request, bandwidthThrottleTimeMs)
          } else {
            requestHelper.throttle(quotas.request, request, requestThrottleTimeMs)
          }
          // If throttling is required, return an empty response.
          fetchContext.getThrottledResponse(maxThrottleTimeMs, nodeEndpoints.values.toSeq.asJava)
        } else {
          // Get the actual response. This will update the fetch context.
          val fetchResponse = fetchContext.updateAndGenerateResponseData(partitions, nodeEndpoints.values.toSeq.asJava)
          val responsePartitionsSize = fetchResponse.data().responses().stream().mapToInt(_.partitions().size()).sum()
          trace(s"Sending Fetch response with partitions.size=$responsePartitionsSize, " +
            s"metadata=${fetchResponse.sessionId}")
          fetchResponse
        }

        recordBytesOutMetric(fetchResponse)
        // Send the response immediately.
        requestChannel.sendResponse(request, fetchResponse, None)
      }
    }

    if (interesting.isEmpty) {
      processResponseCallback(Seq.empty)
    } else {
      // for fetch from consumer, cap fetchMaxBytes to the maximum bytes that could be fetched without being throttled given
      // no bytes were recorded in the recent quota window
      // trying to fetch more bytes would result in a guaranteed throttling potentially blocking consumer progress
      val maxQuotaWindowBytes = if (fetchRequest.isFromFollower)
        Int.MaxValue
      else
        quotas.fetch.getMaxValueInQuotaWindow(request.session, clientId).toInt

      val fetchMaxBytes = Math.min(Math.min(fetchRequest.maxBytes, config.fetchMaxBytes), maxQuotaWindowBytes)
      val fetchMinBytes = Math.min(fetchRequest.minBytes, fetchMaxBytes)

      val clientMetadata: Optional[ClientMetadata] = if (versionId >= 11) {
        // Fetch API version 11 added preferred replica logic
        Optional.of(new DefaultClientMetadata(
          fetchRequest.rackId,
          clientId,
          request.context.clientAddress,
          request.context.principal,
          request.context.listenerName.value))
      } else {
        Optional.empty()
      }

      val params = new FetchParams(
        versionId,
        fetchRequest.replicaId,
        fetchRequest.replicaEpoch,
        fetchRequest.maxWait,
        fetchMinBytes,
        fetchMaxBytes,
        FetchIsolation.of(fetchRequest),
        clientMetadata
      )

      // call the replica manager to fetch messages from the local replica
      replicaManager.fetchMessages(
        params = params,
        fetchInfos = interesting,
        quota = replicationQuota(fetchRequest),
        responseCallback = processResponseCallback,
      )
    }
  }

  def replicationQuota(fetchRequest: FetchRequest): ReplicaQuota =
    if (fetchRequest.isFromFollower) quotas.leader else UNBOUNDED_QUOTA

  def handleListOffsetRequest(request: RequestChannel.Request): Unit = {
    val correlationId = request.header.correlationId
    val clientId = request.header.clientId
    val offsetRequest = request.body[ListOffsetsRequest]
    val version = request.header.apiVersion

    def buildErrorResponse(e: Errors, partition: ListOffsetsPartition): ListOffsetsPartitionResponse = {
      new ListOffsetsPartitionResponse()
        .setPartitionIndex(partition.partitionIndex)
        .setErrorCode(e.code)
        .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
        .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)
    }

    val (authorizedRequestInfo, unauthorizedRequestInfo) = authHelper.partitionSeqByAuthorized(request.context,
        DESCRIBE, TOPIC, offsetRequest.topics.asScala.toSeq)(_.name)

    val unauthorizedResponseStatus = unauthorizedRequestInfo.map(topic =>
      new ListOffsetsTopicResponse()
        .setName(topic.name)
        .setPartitions(topic.partitions.asScala.map(partition =>
          buildErrorResponse(Errors.TOPIC_AUTHORIZATION_FAILED, partition)).asJava)
    )

    def sendResponseCallback(response: Seq[ListOffsetsTopicResponse]): Unit = {
      val mergedResponses = response ++ unauthorizedResponseStatus
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new ListOffsetsResponse(new ListOffsetsResponseData()
          .setThrottleTimeMs(requestThrottleMs)
          .setTopics(mergedResponses.asJava)))
    }

    // View-rewrite pass for ListOffsets. After authorization, each view topic in the request is
    // rewritten to its backing topic so replicaManager.fetchOffset queries the actual on-disk
    // log (the view's placeholder log is empty). The response is translated back to view names
    // before being returned to the consumer, so the view name never leaks the backing name.
    //
    // Mirrors the rewrite logic in handleFetchRequest (collision detection on view+backing in
    // one request, on two views sharing one backing, and on malformed view configs). Keyed by
    // topic NAME rather than TopicIdPartition because ListOffsetsRequest is name-keyed and the
    // partition count of a view always matches its backing (enforced at view-creation time).
    val backingToView = mutable.LinkedHashMap[String, String]() // backingName → viewName, response translation
    val viewRewriteResponses = mutable.ArrayBuffer[ListOffsetsTopicResponse]()
    val rewrittenAuthorizedInfo = mutable.ArrayBuffer[ListOffsetsTopic]()
    // Per-view, per-partition epoch errors. Used when only SOME partitions of a view fail
    // currentLeaderEpoch validation — we still route the surviving partitions through the
    // replica layer, then splice these per-partition error responses back into the view's
    // ListOffsetsTopicResponse before sending. Keyed by view name (not backing) so the splice
    // happens after backing→view translation in viewAwareSendResponseCallback.
    val viewPartialPartitionErrors = mutable.LinkedHashMap[String, mutable.ArrayBuffer[ListOffsetsPartitionResponse]]()

    if (authorizedRequestInfo.nonEmpty) {
      // First pass: classify each authorized topic — view (with spec) / direct / malformed view.
      val classified = authorizedRequestInfo.map { topic =>
        val result: Either[Exception, Optional[ViewSpec]] = try {
          Right(viewRegistry.viewFor(topic.name))
        } catch {
          case e: Exception => Left(e)
        }
        (topic, result)
      }

      // Coalesce same-name view (or malformed-view) entries: the wire protocol only forbids
      // duplicate (topic-name, partition-index) PAIRS (caught request-side by
      // duplicatePartitions(), see ListOffsetsRequest.java:127), not duplicate topic names.
      // Without this merge, two ListOffsetsTopic entries naming the same view would either
      // (a) trip the "different view sharing same backing" guard below with a misleading
      //     error message that rejects the second entry's partitions, or
      // (b) synthesize two ListOffsetsTopicResponse entries with the same name (one per
      //     entry), which a consumer parses as just the first.
      // Non-view entries are left untouched: the downstream replica layer handles duplicate-name
      // direct topics with the same semantics it did before the view rewrite was added.
      val classifiedCoalesced: Seq[(ListOffsetsTopic, Either[Exception, Optional[ViewSpec]])] = {
        val viewByName = mutable.LinkedHashMap[String, (ListOffsetsTopic, Either[Exception, Optional[ViewSpec]])]()
        val nonViewEntries = mutable.ArrayBuffer[(ListOffsetsTopic, Either[Exception, Optional[ViewSpec]])]()
        classified.foreach { entry =>
          val (topic, result) = entry
          val isView = result match {
            case Right(opt) => opt.isPresent
            case Left(_)    => true
          }
          if (isView) {
            viewByName.get(topic.name) match {
              case Some((existing, existingResult)) =>
                val mergedPartitions = new util.ArrayList[ListOffsetsPartition](existing.partitions)
                mergedPartitions.addAll(topic.partitions)
                viewByName.put(topic.name,
                  (new ListOffsetsTopic().setName(topic.name).setPartitions(mergedPartitions),
                    existingResult))
              case None =>
                viewByName.put(topic.name, entry)
            }
          } else {
            nonViewEntries += entry
          }
        }
        (viewByName.values ++ nonViewEntries).toSeq
      }

      val directNames: Set[String] = classifiedCoalesced.collect {
        case (topic, Right(opt)) if !opt.isPresent => topic.name
      }.toSet

      def rejectAllPartitions(topic: ListOffsetsTopic, err: Errors): Unit = {
        viewRewriteResponses += new ListOffsetsTopicResponse()
          .setName(topic.name)
          .setPartitions(topic.partitions.asScala.map(p => buildErrorResponse(err, p)).asJava)
      }

      classifiedCoalesced.foreach {
        case (topic, Left(e)) =>
          // A malformed view config (predicate compile failure, self-loop) reaching this point
          // means LogConfig validation was bypassed somehow. Mirrors handleFetchRequest.
          warn(s"Failed to load view spec for topic ${topic.name}; rejecting ListOffsets with INVALID_REQUEST", e)
          rejectAllPartitions(topic, Errors.INVALID_REQUEST)

        case (topic, Right(maybeSpec)) if maybeSpec.isPresent =>
          val spec = maybeSpec.get()
          val backingName = spec.backingTopic()
          // Backing must exist for every requested partition.
          val anyMissing = topic.partitions.asScala.exists { p =>
            !metadataCache.contains(new TopicPartition(backingName, p.partitionIndex))
          }
          // Every requested backing partition must also be local to this broker. If a backing
          // partition lives on a different broker, replicaManager.fetchOffset would return
          // NOT_LEADER_OR_FOLLOWER and the consumer would loop against the view's leader
          // (see ViewSpec javadoc "Partition-assignment constraint" and the matching guard in
          // handleFetchRequest). Fail-fast at routing time keyed at the view.
          lazy val anyNonLocal = topic.partitions.asScala.exists { p =>
            replicaManager.getPartitionOrError(new TopicPartition(backingName, p.partitionIndex)).isLeft
          }
          if (anyMissing) {
            // Mirror handleFetchRequest: surface UNKNOWN_TOPIC_OR_PARTITION keyed at the view
            // so the consumer never learns the backing topic name.
            rejectAllPartitions(topic, Errors.UNKNOWN_TOPIC_OR_PARTITION)
          } else if (anyNonLocal) {
            warn(s"ListOffsets rejected: view ${topic.name} redirects to backing topic $backingName, " +
              s"but this broker is not a replica of every requested backing partition. View and " +
              s"backing must share the same replica assignment (see ViewSpec javadoc). Returning " +
              s"UNKNOWN_TOPIC_OR_PARTITION keyed at the view.")
            rejectAllPartitions(topic, Errors.UNKNOWN_TOPIC_OR_PARTITION)
          } else if (directNames.contains(backingName)) {
            warn(s"ListOffsets rejected: view ${topic.name} redirects to backing topic $backingName " +
              s"which is also a direct entry in the same ListOffsetsRequest; failing the view with " +
              s"INVALID_REQUEST. Issue the view in a separate ListOffsets request.")
            rejectAllPartitions(topic, Errors.INVALID_REQUEST)
          } else if (backingToView.contains(backingName)) {
            val firstView = backingToView(backingName)
            warn(s"ListOffsets rejected: view ${topic.name} and view $firstView both target backing topic " +
              s"$backingName in one ListOffsetsRequest; failing the second view with INVALID_REQUEST.")
            rejectAllPartitions(topic, Errors.INVALID_REQUEST)
          } else {
            // Leader-epoch boundary (KIP-595): each ListOffsetsPartition.currentLeaderEpoch
            // reflects the VIEW's leader-epoch ledger, which evolves independently of the
            // backing's (a view-leader change does not bump the backing's epoch and vice versa).
            // Forwarding the partitions unchanged would let replicaManager.fetchOffset validate
            // the view's epoch against the backing partition's epoch and surface FENCED/UNKNOWN
            // on every modern-client ListOffsets call.
            //
            // Validate per-partition against the VIEW partition's local log, then build a
            // rewritten ListOffsetsTopic that points at the backing topic but strips every
            // partition's currentLeaderEpoch (UNKNOWN_EPOCH = -1) so the backing fetchOffset
            // sees an epoch-less request. Partitions whose epoch fails validation are surfaced
            // as per-partition errors against the VIEW name (FENCED_LEADER_EPOCH /
            // UNKNOWN_LEADER_EPOCH / NOT_LEADER_OR_FOLLOWER) via viewPartialPartitionErrors;
            // the surviving partitions go through the storage layer normally.
            //
            // The view-leader check runs even when the client omits the epoch
            // (UNKNOWN_EPOCH = -1): otherwise a non-view-leader replica could still
            // serve ListOffsets via the backing redirect, hiding stale client metadata.
            // `localLogWithEpochOrThrow(Optional.empty(), requireLeader=true)` enforces the
            // is-leader predicate without epoch validation.
            //
            // `fetchOnlyFromLeader` mirrors `ReplicaManager.fetchOffset`'s own decision
            // (`replicaId != DEBUGGING_REPLICA_ID`); debug-replica requests bypass leader
            // enforcement so they can poke at follower replicas, matching upstream.
            //
            // Duplicate view partitions are emitted to the client as INVALID_REQUEST,
            // matching upstream's `replicaManager.fetchOffset` behaviour for duplicates,
            // and we collapse the duplicate to a single response entry to keep the
            // protocol-mandated "one partition-response per partition-index" invariant
            // through the splice path.
            val viewDuplicatePartitionIndexes: Set[Int] =
              offsetRequest.duplicatePartitions().asScala
                .iterator
                .filter(_.topic == topic.name)
                .map(_.partition)
                .toSet
            val processedPartitionIndexes = mutable.Set[Int]()
            val fetchOnlyFromLeader = offsetRequest.replicaId != ListOffsetsRequest.DEBUGGING_REPLICA_ID
            val validatedPartitions = new util.ArrayList[ListOffsetsPartition]()
            val perPartitionErrors = mutable.ArrayBuffer[ListOffsetsPartitionResponse]()
            topic.partitions.asScala.foreach { p =>
              if (!processedPartitionIndexes.add(p.partitionIndex)) {
                // Already handled (first occurrence won). Drop this copy on the floor so the
                // response carries exactly one entry for this partition index — same shape as
                // the replica-manager's duplicate handling for ordinary topics.
              } else if (viewDuplicatePartitionIndexes.contains(p.partitionIndex)) {
                perPartitionErrors += buildErrorResponse(Errors.INVALID_REQUEST, p)
              } else {
                val epochOpt: Optional[Integer] =
                  if (p.currentLeaderEpoch == ListOffsetsResponse.UNKNOWN_EPOCH) Optional.empty()
                  else Optional.of(Integer.valueOf(p.currentLeaderEpoch))
                val epochError: Errors =
                  replicaManager.getPartitionOrError(new TopicPartition(topic.name, p.partitionIndex)) match {
                    case Left(error) => error
                    case Right(viewPartition) =>
                      try {
                        viewPartition.localLogWithEpochOrThrow(epochOpt, fetchOnlyFromLeader)
                        Errors.NONE
                      } catch {
                        case e: ApiException => Errors.forException(e)
                      }
                  }
                if (epochError != Errors.NONE) {
                  perPartitionErrors += buildErrorResponse(epochError, p)
                } else {
                  validatedPartitions.add(new ListOffsetsPartition()
                    .setPartitionIndex(p.partitionIndex)
                    .setCurrentLeaderEpoch(ListOffsetsResponse.UNKNOWN_EPOCH)
                    .setTimestamp(p.timestamp))
                }
              }
            }
            if (perPartitionErrors.nonEmpty && validatedPartitions.isEmpty) {
              // All partitions failed epoch validation → synthesise the view response directly,
              // skip the replica layer entirely.
              viewRewriteResponses += new ListOffsetsTopicResponse()
                .setName(topic.name)
                .setPartitions(perPartitionErrors.asJava)
            } else {
              if (perPartitionErrors.nonEmpty) {
                viewPartialPartitionErrors.getOrElseUpdate(topic.name,
                  mutable.ArrayBuffer[ListOffsetsPartitionResponse]()) ++= perPartitionErrors
              }
              rewrittenAuthorizedInfo += new ListOffsetsTopic()
                .setName(backingName)
                .setPartitions(validatedPartitions)
              backingToView.put(backingName, topic.name)
            }
          }

        case (topic, Right(_)) =>
          // Non-view — pass through unchanged.
          rewrittenAuthorizedInfo += topic
      }
    }

    // The duplicate-partitions set is keyed by the names the client sent. Rewrite any (view, p)
    // entries to (backing, p) so replicaManager's duplicate check finds them under the rewritten
    // request shape.
    val rewrittenDuplicates: collection.Set[TopicPartition] = if (backingToView.isEmpty) {
      offsetRequest.duplicatePartitions().asScala
    } else {
      val viewToBacking: Map[String, String] = backingToView.map { case (k, v) => v -> k }.toMap
      offsetRequest.duplicatePartitions().asScala.map { tp =>
        viewToBacking.get(tp.topic) match {
          case Some(backing) => new TopicPartition(backing, tp.partition)
          case None => tp
        }
      }
    }

    // Wrap the original sendResponseCallback so we (a) translate backing names back to view names,
    // (b) splice in per-partition epoch errors for views whose other partitions reached the
    // storage layer, (c) STRIP the backing partition's leader_epoch off each re-keyed partition
    // response, and (d) append any view-rewrite rejection responses we built above. The original
    // callback also tacks on the topic-authorization-failure responses.
    //
    // The splice keeps the protocol invariant of one ListOffsetsTopicResponse per topic name —
    // duplicate-name entries would be ambiguous to the consumer (only the first parsed wins).
    //
    // Why strip leader_epoch: `ReplicaManager.fetchOffset` sets `LeaderEpoch` on each
    // `ListOffsetsPartitionResponse` from the BACKING partition's leader-epoch ledger (see
    // ReplicaManager.scala:1450). The view's ledger evolves independently and is generally LOWER
    // than the backing's (the backing receives records continuously while the view receives
    // none). Returning the backing epoch under the view's name would feed
    // `Metadata.updateLastSeenEpochIfNewer(viewPartition, backingEpoch)` on the consumer side
    // (see OffsetFetcherUtils.java:386), poisoning the consumer's view-side `lastSeenEpoch` with
    // a backing-side value. The next ListOffsets / Fetch the consumer sends would carry
    // `currentLeaderEpoch = backingEpoch` against the view's actual (lower) leader epoch,
    // tripping `localLogWithEpochOrThrow` into `UNKNOWN_LEADER_EPOCH` (the request's epoch is
    // ahead of the partition's). Combined with the consumer's metadata refresh returning the
    // unchanged (lower) view leader epoch, the consumer would wedge in retry indefinitely.
    //
    // Setting `LeaderEpoch = UNDEFINED_EPOCH (-1)` is the protocol's "no useful epoch info"
    // sentinel and is below any non-negative tracked epoch, so
    // `updateLastSeenEpochIfNewer` is a no-op for it and the consumer keeps the legitimate
    // view-side epoch it last observed via metadata. KIP-320 truncation detection for view
    // consumers is already cross-ledger and out-of-scope per PROMPT.md; stripping here makes
    // the response shape consistent with the consumer's view-leader-epoch state machine.
    def viewAwareSendResponseCallback(response: Seq[ListOffsetsTopicResponse]): Unit = {
      val translated: Seq[ListOffsetsTopicResponse] =
        if (backingToView.isEmpty) response
        else response.map { topicResp =>
          backingToView.get(topicResp.name) match {
            case Some(viewName) =>
              val stripped = topicResp.partitions.asScala.map { p =>
                new ListOffsetsPartitionResponse()
                  .setPartitionIndex(p.partitionIndex)
                  .setErrorCode(p.errorCode)
                  .setTimestamp(p.timestamp)
                  .setOffset(p.offset)
                  .setLeaderEpoch(ListOffsetsResponse.UNKNOWN_EPOCH)
              }
              val combined = new util.ArrayList[ListOffsetsPartitionResponse](stripped.asJava)
              viewPartialPartitionErrors.get(viewName).foreach(errs => combined.addAll(errs.asJava))
              new ListOffsetsTopicResponse().setName(viewName).setPartitions(combined)
            case None => topicResp
          }
        }
      sendResponseCallback(translated ++ viewRewriteResponses)
    }

    if (rewrittenAuthorizedInfo.isEmpty) {
      // Either no authorized topics at all, or every authorized topic was a view that we rejected.
      // Either way we have nothing live to ask the replica manager.
      viewAwareSendResponseCallback(Seq.empty)
    } else {
      replicaManager.fetchOffset(rewrittenAuthorizedInfo.toSeq, rewrittenDuplicates.toSet,
        offsetRequest.isolationLevel(), offsetRequest.replicaId(), clientId, correlationId, version,
        buildErrorResponse, viewAwareSendResponseCallback, offsetRequest.timeoutMs())
    }
  }

  private def metadataResponseTopic(error: Errors,
                                    topic: String,
                                    topicId: Uuid,
                                    isInternal: Boolean,
                                    partitionData: util.List[MetadataResponsePartition]): MetadataResponseTopic = {
    new MetadataResponseTopic()
      .setErrorCode(error.code)
      .setName(topic)
      .setTopicId(topicId)
      .setIsInternal(isInternal)
      .setPartitions(partitionData)
  }

  private def getTopicMetadata(
    request: RequestChannel.Request,
    fetchAllTopics: Boolean,
    allowAutoTopicCreation: Boolean,
    topics: Set[String],
    listenerName: ListenerName,
    errorUnavailableEndpoints: Boolean,
    errorUnavailableListeners: Boolean
  ): Seq[MetadataResponseTopic] = {
    val topicResponses = metadataCache.getTopicMetadata(topics, listenerName,
      errorUnavailableEndpoints, errorUnavailableListeners)

    if (topics.isEmpty || topicResponses.size == topics.size || fetchAllTopics) {
      topicResponses
    } else {
      val nonExistingTopics = topics.diff(topicResponses.map(_.name).toSet)
      val nonExistingTopicResponses = if (allowAutoTopicCreation) {
        val controllerMutationQuota = quotas.controllerMutation.newPermissiveQuotaFor(request)
        autoTopicCreationManager.createTopics(nonExistingTopics, controllerMutationQuota, Some(request.context))
      } else {
        nonExistingTopics.map { topic =>
          val error = try {
            Topic.validate(topic)
            Errors.UNKNOWN_TOPIC_OR_PARTITION
          } catch {
            case _: InvalidTopicException =>
              Errors.INVALID_TOPIC_EXCEPTION
          }

          metadataResponseTopic(
            error,
            topic,
            metadataCache.getTopicId(topic),
            Topic.isInternal(topic),
            util.Collections.emptyList()
          )
        }
      }

      topicResponses ++ nonExistingTopicResponses
    }
  }

  def handleTopicMetadataRequest(request: RequestChannel.Request): Unit = {
    val metadataRequest = request.body[MetadataRequest]
    val requestVersion = request.header.apiVersion

    // Topic IDs are not supported for versions 10 and 11. Topic names can not be null in these versions.
    if (!metadataRequest.isAllTopics) {
      metadataRequest.data.topics.forEach{ topic =>
        if (topic.name == null && metadataRequest.version < 12) {
          throw new InvalidRequestException(s"Topic name can not be null for version ${metadataRequest.version}")
        } else if (topic.topicId != Uuid.ZERO_UUID && metadataRequest.version < 12) {
          throw new InvalidRequestException(s"Topic IDs are not supported in requests for version ${metadataRequest.version}")
        }
      }
    }

    // Check if topicId is presented firstly.
    val topicIds = metadataRequest.topicIds.asScala.toSet.filterNot(_ == Uuid.ZERO_UUID)
    val useTopicId = topicIds.nonEmpty

    // Only get topicIds and topicNames when supporting topicId
    val unknownTopicIds = topicIds.filter(metadataCache.getTopicName(_).isEmpty)
    val knownTopicNames = topicIds.flatMap(metadataCache.getTopicName)

    val unknownTopicIdsTopicMetadata = unknownTopicIds.map(topicId =>
        metadataResponseTopic(Errors.UNKNOWN_TOPIC_ID, null, topicId, isInternal = false, util.Collections.emptyList())).toSeq

    val topics = if (metadataRequest.isAllTopics)
      metadataCache.getAllTopics()
    else if (useTopicId)
      knownTopicNames
    else
      metadataRequest.topics.asScala.toSet

    val authorizedForDescribeTopics = authHelper.filterByAuthorized(request.context, DESCRIBE, TOPIC,
      topics, logIfDenied = !metadataRequest.isAllTopics)(identity)
    var (authorizedTopics, unauthorizedForDescribeTopics) = topics.partition(authorizedForDescribeTopics.contains)
    var unauthorizedForCreateTopics = Set[String]()

    if (authorizedTopics.nonEmpty) {
      val nonExistingTopics = authorizedTopics.filterNot(metadataCache.contains)
      if (metadataRequest.allowAutoTopicCreation && config.autoCreateTopicsEnable && nonExistingTopics.nonEmpty) {
        if (!authHelper.authorize(request.context, CREATE, CLUSTER, CLUSTER_NAME, logIfDenied = false)) {
          val authorizedForCreateTopics = authHelper.filterByAuthorized(request.context, CREATE, TOPIC,
            nonExistingTopics)(identity)
          unauthorizedForCreateTopics = nonExistingTopics.diff(authorizedForCreateTopics)
          authorizedTopics = authorizedTopics.diff(unauthorizedForCreateTopics)
        }
      }
    }

    val unauthorizedForCreateTopicMetadata = unauthorizedForCreateTopics.map(topic =>
      // Set topicId to zero since we will never create topic which topicId
      metadataResponseTopic(Errors.TOPIC_AUTHORIZATION_FAILED, topic, Uuid.ZERO_UUID, isInternal(topic), util.Collections.emptyList()))

    // do not disclose the existence of topics unauthorized for Describe, so we've not even checked if they exist or not
    val unauthorizedForDescribeTopicMetadata =
      // In case of all topics, don't include topics unauthorized for Describe
      if ((requestVersion == 0 && (metadataRequest.topics == null || metadataRequest.topics.isEmpty)) || metadataRequest.isAllTopics)
        Set.empty[MetadataResponseTopic]
      else if (useTopicId) {
        // Topic IDs are not considered sensitive information, so returning TOPIC_AUTHORIZATION_FAILED is OK
        unauthorizedForDescribeTopics.map(topic =>
          metadataResponseTopic(Errors.TOPIC_AUTHORIZATION_FAILED, null, metadataCache.getTopicId(topic), isInternal = false, util.Collections.emptyList()))
      } else {
        // We should not return topicId when on unauthorized error, so we return zero uuid.
        unauthorizedForDescribeTopics.map(topic =>
          metadataResponseTopic(Errors.TOPIC_AUTHORIZATION_FAILED, topic, Uuid.ZERO_UUID, isInternal = false, util.Collections.emptyList()))
      }

    // In version 0, we returned an error when brokers with replicas were unavailable,
    // while in higher versions we simply don't include the broker in the returned broker list
    val errorUnavailableEndpoints = requestVersion == 0
    // In versions 5 and below, we returned LEADER_NOT_AVAILABLE if a matching listener was not found on the leader.
    // From version 6 onwards, we return LISTENER_NOT_FOUND to enable diagnosis of configuration errors.
    val errorUnavailableListeners = requestVersion >= 6

    val allowAutoCreation = config.autoCreateTopicsEnable && metadataRequest.allowAutoTopicCreation && !metadataRequest.isAllTopics
    val topicMetadata = getTopicMetadata(request, metadataRequest.isAllTopics, allowAutoCreation, authorizedTopics,
      request.context.listenerName, errorUnavailableEndpoints, errorUnavailableListeners)

    var clusterAuthorizedOperations = Int.MinValue // Default value in the schema
    if (requestVersion >= 8) {
      // get cluster authorized operations
      if (requestVersion <= 10) {
        if (metadataRequest.data.includeClusterAuthorizedOperations) {
          if (authHelper.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME))
            clusterAuthorizedOperations = authHelper.authorizedOperations(request, Resource.CLUSTER)
          else
            clusterAuthorizedOperations = 0
        }
      }

      // get topic authorized operations
      if (metadataRequest.data.includeTopicAuthorizedOperations) {
        def setTopicAuthorizedOperations(topicMetadata: Seq[MetadataResponseTopic]): Unit = {
          topicMetadata.foreach { topicData =>
            topicData.setTopicAuthorizedOperations(authHelper.authorizedOperations(request, new Resource(ResourceType.TOPIC, topicData.name)))
          }
        }
        setTopicAuthorizedOperations(topicMetadata)
      }
    }

    val completeTopicMetadata =  unknownTopicIdsTopicMetadata ++
      topicMetadata ++ unauthorizedForCreateTopicMetadata ++ unauthorizedForDescribeTopicMetadata

    val brokers = metadataCache.getAliveBrokerNodes(request.context.listenerName)

    trace("Sending topic metadata %s and brokers %s for correlation id %d to client %s".format(completeTopicMetadata.mkString(","),
      brokers.mkString(","), request.header.correlationId, request.header.clientId))
    val controllerId = {
      metadataCache.getControllerId.flatMap {
        case ZkCachedControllerId(id) => Some(id)
        case KRaftCachedControllerId(_) => metadataCache.getRandomAliveBrokerId
      }
    }

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
       MetadataResponse.prepareResponse(
         requestVersion,
         requestThrottleMs,
         brokers.toList.asJava,
         clusterId,
         controllerId.getOrElse(MetadataResponse.NO_CONTROLLER_ID),
         completeTopicMetadata.asJava,
         clusterAuthorizedOperations
      ))
  }

  def handleDescribeTopicPartitionsRequest(request: RequestChannel.Request): Unit = {
    describeTopicPartitionsRequestHandler match {
      case Some(handler) => {
        val response = handler.handleDescribeTopicPartitionsRequest(request)
        trace("Sending topic partitions metadata %s for correlation id %d to client %s".format(response.topics().asScala.mkString(","),
          request.header.correlationId, request.header.clientId))

        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
          response.setThrottleTimeMs(requestThrottleMs)
          new DescribeTopicPartitionsResponse(response)
        })
      }
      case None => {
        requestHelper.sendMaybeThrottle(request, request.body[DescribeTopicPartitionsRequest].getErrorResponse(Errors.UNSUPPORTED_VERSION.exception))
      }
    }
  }

  /**
   * Handle an offset fetch request
   */
  def handleOffsetFetchRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val offsetFetchRequest = request.body[OffsetFetchRequest]
    val groups = offsetFetchRequest.groups()
    val requireStable = offsetFetchRequest.requireStable()

    val futures = new mutable.ArrayBuffer[CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]](groups.size)
    groups.forEach { groupOffsetFetch =>
      val isAllPartitions = groupOffsetFetch.topics == null
      if (!authHelper.authorize(request.context, DESCRIBE, GROUP, groupOffsetFetch.groupId)) {
        futures += CompletableFuture.completedFuture(OffsetFetchResponse.groupError(
          groupOffsetFetch,
          Errors.GROUP_AUTHORIZATION_FAILED,
          request.header.apiVersion()
        ))
      } else if (isAllPartitions) {
        futures += fetchAllOffsetsForGroup(
          request.context,
          groupOffsetFetch,
          requireStable
        )
      } else {
        futures += fetchOffsetsForGroup(
          request.context,
          groupOffsetFetch,
          requireStable
        )
      }
    }

    CompletableFuture.allOf(futures.toArray: _*).handle[Unit] { (_, _) =>
      val groupResponses = new ArrayBuffer[OffsetFetchResponseData.OffsetFetchResponseGroup](futures.size)
      futures.foreach(future => groupResponses += future.get())
      // Strip any backing-topic leader_epoch that was previously committed under a view topic name.
      // The view fetch path now scrubs partition_leader_epoch from records (see ViewFilter), but
      // consumers that committed offsets BEFORE that fix landed have backing-derived leader_epoch
      // values sitting in __consumer_offsets under view-topic names. On the next session start,
      // OffsetFetch would replay those epochs into SubscriptionState.position.offsetEpoch ->
      // AWAIT_VALIDATION on the view partition -> OFLE request to validate -> our INVALID_REQUEST
      // rejection (view topics have no epoch ledger queryable by a backing-derived epoch) ->
      // partition stays in partitionsToRetry forever, non-fetchable. Normalizing the field to
      // CommittedLeaderEpoch=-1 (the protocol's "no epoch known" sentinel, also the field's
      // default value per OffsetFetchResponse.json) makes the consumer skip validation entirely
      // on view partitions, which matches the design of views: they have no client-validatable
      // epoch ledger. This is response-side cleanse only — it handles pre-fix poisoned commits
      // AND any future consumer that somehow submits a non-(-1) epoch on OffsetCommit for a view.
      groupResponses.foreach { groupResponse =>
        val topics = groupResponse.topics
        if (topics != null) {
          topics.forEach { topicResponse =>
            if (topicResponse.name != null && isViewTopic(topicResponse.name)) {
              topicResponse.partitions.forEach { partitionResponse =>
                partitionResponse.setCommittedLeaderEpoch(-1)
              }
            }
          }
        }
      }
      requestHelper.sendMaybeThrottle(request, new OffsetFetchResponse(groupResponses.asJava, request.context.apiVersion))
    }
  }

  private def fetchAllOffsetsForGroup(
    requestContext: RequestContext,
    groupFetchRequest: OffsetFetchRequestData.OffsetFetchRequestGroup,
    requireStable: Boolean
  ): CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup] = {
    groupCoordinator.fetchAllOffsets(
      requestContext,
      groupFetchRequest,
      requireStable
    ).handle[OffsetFetchResponseData.OffsetFetchResponseGroup] { (groupFetchResponse, exception) =>
      if (exception != null) {
        OffsetFetchResponse.groupError(
          groupFetchRequest,
          Errors.forException(exception),
          requestContext.apiVersion()
        )
      } else if (groupFetchResponse.errorCode() != Errors.NONE.code) {
        groupFetchResponse
      } else {
        // Clients are not allowed to see offsets for topics that are not authorized for Describe.
        val (authorizedOffsets, _) = authHelper.partitionSeqByAuthorized(
          requestContext,
          DESCRIBE,
          TOPIC,
          groupFetchResponse.topics.asScala
        )(_.name)
        groupFetchResponse.setTopics(authorizedOffsets.asJava)
      }
    }
  }

  private def fetchOffsetsForGroup(
    requestContext: RequestContext,
    groupFetchRequest: OffsetFetchRequestData.OffsetFetchRequestGroup,
    requireStable: Boolean
  ): CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup] = {
    // Clients are not allowed to see offsets for topics that are not authorized for Describe.
    val (authorizedTopics, unauthorizedTopics) = authHelper.partitionSeqByAuthorized(
      requestContext,
      DESCRIBE,
      TOPIC,
      groupFetchRequest.topics.asScala
    )(_.name)

    groupCoordinator.fetchOffsets(
      requestContext,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId(groupFetchRequest.groupId)
        .setMemberId(groupFetchRequest.memberId)
        .setMemberEpoch(groupFetchRequest.memberEpoch)
        .setTopics(authorizedTopics.asJava),
      requireStable
    ).handle[OffsetFetchResponseData.OffsetFetchResponseGroup] { (groupFetchResponse, exception) =>
      if (exception != null) {
        OffsetFetchResponse.groupError(
          groupFetchRequest,
          Errors.forException(exception),
          requestContext.apiVersion()
        )
      } else if (groupFetchResponse.errorCode() != Errors.NONE.code) {
        groupFetchResponse
      } else {
        val topics = new util.ArrayList[OffsetFetchResponseData.OffsetFetchResponseTopics](
          groupFetchResponse.topics.size + unauthorizedTopics.size
        )
        topics.addAll(groupFetchResponse.topics)
        unauthorizedTopics.foreach { topic =>
          val topicResponse = new OffsetFetchResponseData.OffsetFetchResponseTopics().setName(topic.name)
          topic.partitionIndexes.forEach { partitionIndex =>
            topicResponse.partitions.add(new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(partitionIndex)
              .setCommittedOffset(-1)
              .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code))
          }
          topics.add(topicResponse)
        }
        groupFetchResponse.setTopics(topics)
      }
    }
  }

  def handleFindCoordinatorRequest(request: RequestChannel.Request): Unit = {
    val version = request.header.apiVersion
    if (version < 4) {
      handleFindCoordinatorRequestLessThanV4(request)
    } else {
      handleFindCoordinatorRequestV4AndAbove(request)
    }
  }

  private def handleFindCoordinatorRequestV4AndAbove(request: RequestChannel.Request): Unit = {
    val findCoordinatorRequest = request.body[FindCoordinatorRequest]

    val coordinators = findCoordinatorRequest.data.coordinatorKeys.asScala.map { key =>
      val (error, node) = getCoordinator(request, findCoordinatorRequest.data.keyType, key)
      new FindCoordinatorResponseData.Coordinator()
        .setKey(key)
        .setErrorCode(error.code)
        .setHost(node.host)
        .setNodeId(node.id)
        .setPort(node.port)
    }
    def createResponse(requestThrottleMs: Int): AbstractResponse = {
      val response = new FindCoordinatorResponse(
              new FindCoordinatorResponseData()
                .setCoordinators(coordinators.asJava)
                .setThrottleTimeMs(requestThrottleMs))
      trace("Sending FindCoordinator response %s for correlation id %d to client %s."
              .format(response, request.header.correlationId, request.header.clientId))
      response
    }
    requestHelper.sendResponseMaybeThrottle(request, createResponse)
  }

  private def handleFindCoordinatorRequestLessThanV4(request: RequestChannel.Request): Unit = {
    val findCoordinatorRequest = request.body[FindCoordinatorRequest]

    val (error, node) = getCoordinator(request, findCoordinatorRequest.data.keyType, findCoordinatorRequest.data.key)
    def createResponse(requestThrottleMs: Int): AbstractResponse = {
      val responseBody = new FindCoordinatorResponse(
          new FindCoordinatorResponseData()
            .setErrorCode(error.code)
            .setErrorMessage(error.message())
            .setNodeId(node.id)
            .setHost(node.host)
            .setPort(node.port)
            .setThrottleTimeMs(requestThrottleMs))
      trace("Sending FindCoordinator response %s for correlation id %d to client %s."
        .format(responseBody, request.header.correlationId, request.header.clientId))
      responseBody
    }
    if (error == Errors.NONE) {
      requestHelper.sendResponseMaybeThrottle(request, createResponse)
    } else {
      requestHelper.sendErrorResponseMaybeThrottle(request, error.exception)
    }
  }

  private def getCoordinator(request: RequestChannel.Request, keyType: Byte, key: String): (Errors, Node) = {
    if (keyType == CoordinatorType.GROUP.id &&
        !authHelper.authorize(request.context, DESCRIBE, GROUP, key))
      (Errors.GROUP_AUTHORIZATION_FAILED, Node.noNode)
    else if (keyType == CoordinatorType.TRANSACTION.id &&
        !authHelper.authorize(request.context, DESCRIBE, TRANSACTIONAL_ID, key))
      (Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED, Node.noNode)
    else if (keyType == CoordinatorType.SHARE.id && request.context.apiVersion < 6)
      (Errors.INVALID_REQUEST, Node.noNode)
    else {
      if (keyType == CoordinatorType.SHARE.id) {
        authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
        if (shareCoordinator.isEmpty) {
          return (Errors.INVALID_REQUEST, Node.noNode)
        }
        try {
          SharePartitionKey.validate(key)
        } catch {
          case e: IllegalArgumentException =>
            error(s"Share coordinator key is invalid", e)
            (Errors.INVALID_REQUEST, Node.noNode())
        }
      }
      val (partition, internalTopicName) = CoordinatorType.forId(keyType) match {
        case CoordinatorType.GROUP =>
          (groupCoordinator.partitionFor(key), GROUP_METADATA_TOPIC_NAME)

        case CoordinatorType.TRANSACTION =>
          (txnCoordinator.partitionFor(key), TRANSACTION_STATE_TOPIC_NAME)

        case CoordinatorType.SHARE =>
          (shareCoordinator.foreach(coordinator => coordinator.partitionFor(SharePartitionKey.getInstance(key))), SHARE_GROUP_STATE_TOPIC_NAME)
      }

      val topicMetadata = metadataCache.getTopicMetadata(Set(internalTopicName), request.context.listenerName)

      if (topicMetadata.headOption.isEmpty) {
        val controllerMutationQuota = quotas.controllerMutation.newPermissiveQuotaFor(request)
        autoTopicCreationManager.createTopics(Seq(internalTopicName).toSet, controllerMutationQuota, None)
        (Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
      } else {
        if (topicMetadata.head.errorCode != Errors.NONE.code) {
          (Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
        } else {
          val coordinatorEndpoint = topicMetadata.head.partitions.asScala
            .find(_.partitionIndex == partition)
            .filter(_.leaderId != MetadataResponse.NO_LEADER_ID)
            .flatMap(metadata => metadataCache.
                getAliveBrokerNode(metadata.leaderId, request.context.listenerName))

          coordinatorEndpoint match {
            case Some(endpoint) =>
              (Errors.NONE, endpoint)
            case _ =>
              (Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
          }
        }
      }
    }
  }

  def handleDescribeGroupsRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val describeRequest = request.body[DescribeGroupsRequest]
    val includeAuthorizedOperations = describeRequest.data.includeAuthorizedOperations
    val response = new DescribeGroupsResponseData()
    val authorizedGroups = new ArrayBuffer[String]()

    describeRequest.data.groups.forEach { groupId =>
      if (!authHelper.authorize(request.context, DESCRIBE, GROUP, groupId)) {
        response.groups.add(DescribeGroupsResponse.groupError(
          groupId,
          Errors.GROUP_AUTHORIZATION_FAILED
        ))
      } else {
        authorizedGroups += groupId
      }
    }

    groupCoordinator.describeGroups(
      request.context,
      authorizedGroups.asJava
    ).handle[Unit] { (results, exception) =>
      if (exception != null) {
        requestHelper.sendMaybeThrottle(request, describeRequest.getErrorResponse(exception))
      } else {
        if (request.header.apiVersion >= 3 && includeAuthorizedOperations) {
          results.forEach { groupResult =>
            if (groupResult.errorCode == Errors.NONE.code) {
              groupResult.setAuthorizedOperations(authHelper.authorizedOperations(
                request,
                new Resource(ResourceType.GROUP, groupResult.groupId)
              ))
            }
          }
        }

        if (response.groups.isEmpty) {
          // If the response is empty, we can directly reuse the results.
          response.setGroups(results)
        } else {
          // Otherwise, we have to copy the results into the existing ones.
          response.groups.addAll(results)
        }

        requestHelper.sendMaybeThrottle(request, new DescribeGroupsResponse(response))
      }
    }
  }

  def handleListGroupsRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val listGroupsRequest = request.body[ListGroupsRequest]
    val hasClusterDescribe = authHelper.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME, logIfDenied = false)

    groupCoordinator.listGroups(
      request.context,
      listGroupsRequest.data
    ).handle[Unit] { (response, exception) =>
      if (exception != null) {
        requestHelper.sendMaybeThrottle(request, listGroupsRequest.getErrorResponse(exception))
      } else {
        val listGroupsResponse = if (hasClusterDescribe) {
          // With describe cluster access all groups are returned. We keep this alternative for backward compatibility.
          new ListGroupsResponse(response)
        } else {
          // Otherwise, only groups with described group are returned.
          val authorizedGroups = response.groups.asScala.filter { group =>
            authHelper.authorize(request.context, DESCRIBE, GROUP, group.groupId, logIfDenied = false)
          }
          new ListGroupsResponse(response.setGroups(authorizedGroups.asJava))
        }
        requestHelper.sendMaybeThrottle(request, listGroupsResponse)
      }
    }
  }

  def handleJoinGroupRequest(
    request: RequestChannel.Request,
    requestLocal: RequestLocal
  ): CompletableFuture[Unit] = {
    val joinGroupRequest = request.body[JoinGroupRequest]

    if (!authHelper.authorize(request.context, READ, GROUP, joinGroupRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, joinGroupRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      groupCoordinator.joinGroup(
        request.context,
        joinGroupRequest.data,
        requestLocal.bufferSupplier
      ).handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, joinGroupRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, new JoinGroupResponse(response, request.context.apiVersion))
        }
      }
    }
  }

  def handleSyncGroupRequest(
    request: RequestChannel.Request,
    requestLocal: RequestLocal
  ): CompletableFuture[Unit] = {
    val syncGroupRequest = request.body[SyncGroupRequest]

    if (!syncGroupRequest.areMandatoryProtocolTypeAndNamePresent()) {
      // Starting from version 5, ProtocolType and ProtocolName fields are mandatory.
      requestHelper.sendMaybeThrottle(request, syncGroupRequest.getErrorResponse(Errors.INCONSISTENT_GROUP_PROTOCOL.exception))
      CompletableFuture.completedFuture[Unit](())
    } else if (!authHelper.authorize(request.context, READ, GROUP, syncGroupRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, syncGroupRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      groupCoordinator.syncGroup(
        request.context,
        syncGroupRequest.data,
        requestLocal.bufferSupplier
      ).handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, syncGroupRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, new SyncGroupResponse(response))
        }
      }
    }
  }

  def handleDeleteGroupsRequest(
    request: RequestChannel.Request,
    requestLocal: RequestLocal
  ): CompletableFuture[Unit] = {
    val deleteGroupsRequest = request.body[DeleteGroupsRequest]
    val groups = deleteGroupsRequest.data.groupsNames.asScala.distinct

    val (authorizedGroups, unauthorizedGroups) =
      authHelper.partitionSeqByAuthorized(request.context, DELETE, GROUP, groups)(identity)

    groupCoordinator.deleteGroups(
      request.context,
      authorizedGroups.toList.asJava,
      requestLocal.bufferSupplier
    ).handle[Unit] { (results, exception) =>
      val response = new DeleteGroupsResponseData()

      if (exception != null) {
        val error = Errors.forException(exception)
        authorizedGroups.foreach { groupId =>
          response.results.add(new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId(groupId)
            .setErrorCode(error.code))
        }
      } else {
        response.setResults(results)
      }

      unauthorizedGroups.foreach { groupId =>
        response.results.add(new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId(groupId)
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code))
      }

      requestHelper.sendMaybeThrottle(request, new DeleteGroupsResponse(response))
    }
  }

  def handleHeartbeatRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val heartbeatRequest = request.body[HeartbeatRequest]

    if (!authHelper.authorize(request.context, READ, GROUP, heartbeatRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, heartbeatRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      groupCoordinator.heartbeat(
        request.context,
        heartbeatRequest.data
      ).handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, heartbeatRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, new HeartbeatResponse(response))
        }
      }
    }
  }

  def handleLeaveGroupRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val leaveGroupRequest = request.body[LeaveGroupRequest]

    if (!authHelper.authorize(request.context, READ, GROUP, leaveGroupRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, leaveGroupRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      groupCoordinator.leaveGroup(
        request.context,
        leaveGroupRequest.normalizedData()
      ).handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, leaveGroupRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, new LeaveGroupResponse(response, leaveGroupRequest.version))
        }
      }
    }
  }

  def handleSaslHandshakeRequest(request: RequestChannel.Request): Unit = {
    val responseData = new SaslHandshakeResponseData().setErrorCode(Errors.ILLEGAL_SASL_STATE.code)
    requestHelper.sendResponseMaybeThrottle(request, _ => new SaslHandshakeResponse(responseData))
  }

  def handleSaslAuthenticateRequest(request: RequestChannel.Request): Unit = {
    val responseData = new SaslAuthenticateResponseData()
      .setErrorCode(Errors.ILLEGAL_SASL_STATE.code)
      .setErrorMessage("SaslAuthenticate request received after successful authentication")
    requestHelper.sendResponseMaybeThrottle(request, _ => new SaslAuthenticateResponse(responseData))
  }

  def handleApiVersionsRequest(request: RequestChannel.Request): Unit = {
    // Note that broker returns its full list of supported ApiKeys and versions regardless of current
    // authentication state (e.g., before SASL authentication on an SASL listener, do note that no
    // Kafka protocol requests may take place on an SSL listener before the SSL handshake is finished).
    // If this is considered to leak information about the broker version a workaround is to use SSL
    // with client authentication which is performed at an earlier stage of the connection where the
    // ApiVersionRequest is not available.
    def createResponseCallback(requestThrottleMs: Int): ApiVersionsResponse = {
      val apiVersionRequest = request.body[ApiVersionsRequest]
      if (apiVersionRequest.hasUnsupportedRequestVersion) {
        apiVersionRequest.getErrorResponse(requestThrottleMs, Errors.UNSUPPORTED_VERSION.exception)
      } else if (!apiVersionRequest.isValid) {
        apiVersionRequest.getErrorResponse(requestThrottleMs, Errors.INVALID_REQUEST.exception)
      } else {
        apiVersionManager.apiVersionResponse(requestThrottleMs, request.header.apiVersion() < 4)
      }
    }
    requestHelper.sendResponseMaybeThrottle(request, createResponseCallback)
  }

  def handleDeleteRecordsRequest(request: RequestChannel.Request): Unit = {
    val deleteRecordsRequest = request.body[DeleteRecordsRequest]

    val unauthorizedTopicResponses = mutable.Map[TopicPartition, DeleteRecordsPartitionResult]()
    val nonExistingTopicResponses = mutable.Map[TopicPartition, DeleteRecordsPartitionResult]()
    val authorizedForDeleteTopicOffsets = mutable.Map[TopicPartition, Long]()
    // Views are read-only (PROMPT.md): produce rejects upfront, DeleteRecords must too,
    // otherwise the request would resolve to the view's local log (no records) and either
    // no-op or return misleading low-watermark values without surfacing that the operation
    // is structurally disallowed. We use the same light-touch `isViewTopic` check as produce
    // — config-only, no predicate compilation — so a malformed predicate cannot mask the
    // read-only intent of the topic. INVALID_REQUEST matches the produce-side error code
    // for symmetry.
    val viewTopicResponses = mutable.Map[TopicPartition, DeleteRecordsPartitionResult]()

    val topics = deleteRecordsRequest.data.topics.asScala
    val authorizedTopics = authHelper.filterByAuthorized(request.context, DELETE, TOPIC, topics)(_.name)
    val deleteTopicPartitions = topics.flatMap { deleteTopic =>
      deleteTopic.partitions.asScala.map { deletePartition =>
        new TopicPartition(deleteTopic.name, deletePartition.partitionIndex) -> deletePartition.offset
      }
    }
    for ((topicPartition, offset) <- deleteTopicPartitions) {
      if (!authorizedTopics.contains(topicPartition.topic))
        unauthorizedTopicResponses += topicPartition -> new DeleteRecordsPartitionResult()
          .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
          .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
      else if (!metadataCache.contains(topicPartition))
        nonExistingTopicResponses += topicPartition -> new DeleteRecordsPartitionResult()
          .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
          .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
      else if (isViewTopic(topicPartition.topic))
        viewTopicResponses += topicPartition -> new DeleteRecordsPartitionResult()
          .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
          .setErrorCode(Errors.INVALID_REQUEST.code)
      else
        authorizedForDeleteTopicOffsets += (topicPartition -> offset)
    }

    // the callback for sending a DeleteRecordsResponse
    def sendResponseCallback(authorizedTopicResponses: Map[TopicPartition, DeleteRecordsPartitionResult]): Unit = {
      val mergedResponseStatus = authorizedTopicResponses ++ unauthorizedTopicResponses ++ nonExistingTopicResponses ++ viewTopicResponses
      mergedResponseStatus.foreachEntry { (topicPartition, status) =>
        if (status.errorCode != Errors.NONE.code) {
          debug("DeleteRecordsRequest with correlation id %d from client %s on partition %s failed due to %s".format(
            request.header.correlationId,
            request.header.clientId,
            topicPartition,
            Errors.forCode(status.errorCode).exceptionName))
        }
      }

      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new DeleteRecordsResponse(new DeleteRecordsResponseData()
          .setThrottleTimeMs(requestThrottleMs)
          .setTopics(new DeleteRecordsResponseData.DeleteRecordsTopicResultCollection(mergedResponseStatus.groupBy(_._1.topic).map { case (topic, partitionMap) =>
            new DeleteRecordsTopicResult()
              .setName(topic)
              .setPartitions(new DeleteRecordsResponseData.DeleteRecordsPartitionResultCollection(partitionMap.map { case (topicPartition, partitionResult) =>
                new DeleteRecordsPartitionResult().setPartitionIndex(topicPartition.partition)
                  .setLowWatermark(partitionResult.lowWatermark)
                  .setErrorCode(partitionResult.errorCode)
              }.toList.asJava.iterator()))
          }.toList.asJava.iterator()))))
    }

    if (authorizedForDeleteTopicOffsets.isEmpty)
      sendResponseCallback(Map.empty)
    else {
      // call the replica manager to append messages to the replicas
      replicaManager.deleteRecords(
        deleteRecordsRequest.data.timeoutMs.toLong,
        authorizedForDeleteTopicOffsets,
        sendResponseCallback)
    }
  }

  def handleInitProducerIdRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val initProducerIdRequest = request.body[InitProducerIdRequest]
    val transactionalId = initProducerIdRequest.data.transactionalId

    if (transactionalId != null) {
      if (!authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, transactionalId)) {
        requestHelper.sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
        return
      }
    } else if (!authHelper.authorize(request.context, IDEMPOTENT_WRITE, CLUSTER, CLUSTER_NAME, true, false)
        && !authHelper.authorizeByResourceType(request.context, AclOperation.WRITE, ResourceType.TOPIC)) {
      requestHelper.sendErrorResponseMaybeThrottle(request, Errors.CLUSTER_AUTHORIZATION_FAILED.exception)
      return
    }

    def sendResponseCallback(result: InitProducerIdResult): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val finalError =
          if (initProducerIdRequest.version < 4 && result.error == Errors.PRODUCER_FENCED) {
            // For older clients, they could not understand the new PRODUCER_FENCED error code,
            // so we need to return the INVALID_PRODUCER_EPOCH to have the same client handling logic.
            Errors.INVALID_PRODUCER_EPOCH
          } else {
            result.error
          }
        val responseData = new InitProducerIdResponseData()
          .setProducerId(result.producerId)
          .setProducerEpoch(result.producerEpoch)
          .setThrottleTimeMs(requestThrottleMs)
          .setErrorCode(finalError.code)
        val responseBody = new InitProducerIdResponse(responseData)
        trace(s"Completed $transactionalId's InitProducerIdRequest with result $result from client ${request.header.clientId}.")
        responseBody
      }
      requestHelper.sendResponseMaybeThrottle(request, createResponse)
    }

    val producerIdAndEpoch = (initProducerIdRequest.data.producerId, initProducerIdRequest.data.producerEpoch) match {
      case (RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH) => Right(None)
      case (RecordBatch.NO_PRODUCER_ID, _) | (_, RecordBatch.NO_PRODUCER_EPOCH) => Left(Errors.INVALID_REQUEST)
      case (_, _) => Right(Some(new ProducerIdAndEpoch(initProducerIdRequest.data.producerId, initProducerIdRequest.data.producerEpoch)))
    }

    producerIdAndEpoch match {
      case Right(producerIdAndEpoch) => txnCoordinator.handleInitProducerId(transactionalId, initProducerIdRequest.data.transactionTimeoutMs,
        producerIdAndEpoch, sendResponseCallback, requestLocal)
      case Left(error) => requestHelper.sendErrorResponseMaybeThrottle(request, error.exception)
    }
  }

  def handleEndTxnRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val endTxnRequest = request.body[EndTxnRequest]
    val transactionalId = endTxnRequest.data.transactionalId

    if (authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, transactionalId)) {
      def sendResponseCallback(error: Errors, newProducerId: Long, newProducerEpoch: Short): Unit = {
        def createResponse(requestThrottleMs: Int): AbstractResponse = {
          val finalError =
            if (endTxnRequest.version < 2 && error == Errors.PRODUCER_FENCED) {
              // For older clients, they could not understand the new PRODUCER_FENCED error code,
              // so we need to return the INVALID_PRODUCER_EPOCH to have the same client handling logic.
              Errors.INVALID_PRODUCER_EPOCH
            } else {
              error
            }
          val responseBody = new EndTxnResponse(new EndTxnResponseData()
            .setErrorCode(finalError.code)
            .setProducerId(newProducerId)
            .setProducerEpoch(newProducerEpoch)
            .setThrottleTimeMs(requestThrottleMs))
          trace(s"Completed ${endTxnRequest.data.transactionalId}'s EndTxnRequest " +
            s"with committed: ${endTxnRequest.data.committed}, " +
            s"errors: $error from client ${request.header.clientId}.")
          responseBody
        }
        requestHelper.sendResponseMaybeThrottle(request, createResponse)
      }

      txnCoordinator.handleEndTransaction(endTxnRequest.data.transactionalId,
        endTxnRequest.data.producerId,
        endTxnRequest.data.producerEpoch,
        endTxnRequest.result(),
        TransactionVersion.transactionVersionForEndTxn(endTxnRequest),
        sendResponseCallback,
        requestLocal)
    } else
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new EndTxnResponse(new EndTxnResponseData()
            .setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code)
            .setThrottleTimeMs(requestThrottleMs))
      )
  }

  def handleWriteTxnMarkersRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    // We are checking for AlterCluster permissions first. If it is not present, we are authorizing cluster operation
    // The latter will throw an exception if it is denied.
    if (!authHelper.authorize(request.context, ALTER, CLUSTER, CLUSTER_NAME, logIfDenied = false)) {
      authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    }
    val writeTxnMarkersRequest = request.body[WriteTxnMarkersRequest]
    val errors = new ConcurrentHashMap[java.lang.Long, util.Map[TopicPartition, Errors]]()
    val markers = writeTxnMarkersRequest.markers
    val numAppends = new AtomicInteger(markers.size)

    if (numAppends.get == 0) {
      requestHelper.sendResponseExemptThrottle(request, new WriteTxnMarkersResponse(errors))
      return
    }

    def updateErrors(producerId: Long, currentErrors: ConcurrentHashMap[TopicPartition, Errors]): Unit = {
      val previousErrors = errors.putIfAbsent(producerId, currentErrors)
      if (previousErrors != null)
        previousErrors.putAll(currentErrors)
    }

    /**
      * This is the call back invoked when a log append of transaction markers succeeds. This can be called multiple
      * times when handling a single WriteTxnMarkersRequest because there is one append per TransactionMarker in the
      * request, so there could be multiple appends of markers to the log. The final response will be sent only
      * after all appends have returned.
      */
    def maybeSendResponseCallback(producerId: Long, result: TransactionResult, currentErrors: ConcurrentHashMap[TopicPartition, Errors]): Unit = {
      trace(s"End transaction marker append for producer id $producerId completed with status: $currentErrors")
      updateErrors(producerId, currentErrors)

      def maybeSendResponse(): Unit = {
        if (numAppends.decrementAndGet() == 0) {
          requestHelper.sendResponseExemptThrottle(request, new WriteTxnMarkersResponse(errors))
        }
      }

      // The new group coordinator uses GroupCoordinator#completeTransaction so we do
      // not need to call GroupCoordinator#onTransactionCompleted here.
      if (config.isNewGroupCoordinatorEnabled) {
        maybeSendResponse()
        return
      }

      val successfulOffsetsPartitions = currentErrors.asScala.filter { case (topicPartition, error) =>
        topicPartition.topic == GROUP_METADATA_TOPIC_NAME && error == Errors.NONE
      }.keys

      // If no end transaction marker has been written to a __consumer_offsets partition, we do not
      // need to call GroupCoordinator#onTransactionCompleted.
      if (successfulOffsetsPartitions.isEmpty) {
        maybeSendResponse()
        return
      }

      // Otherwise, we call GroupCoordinator#onTransactionCompleted to materialize the offsets
      // into the cache and we wait until the meterialization is completed.
      groupCoordinator.onTransactionCompleted(producerId, successfulOffsetsPartitions.asJava, result).whenComplete { (_, exception) =>
        if (exception != null) {
          error(s"Received an exception while trying to update the offsets cache on transaction marker append", exception)
          val updatedErrors = new ConcurrentHashMap[TopicPartition, Errors]()
          successfulOffsetsPartitions.foreach(updatedErrors.put(_, Errors.UNKNOWN_SERVER_ERROR))
          updateErrors(producerId, updatedErrors)
        }
        maybeSendResponse()
      }
    }

    // TODO: The current append API makes doing separate writes per producerId a little easier, but it would
    // be nice to have only one append to the log. This requires pushing the building of the control records
    // into Log so that we only append those having a valid producer epoch, and exposing a new appendControlRecord
    // API in ReplicaManager. For now, we've done the simpler approach
    var skippedMarkers = 0
    for (marker <- markers.asScala) {
      val producerId = marker.producerId
      val partitionsWithCompatibleMessageFormat = new mutable.ArrayBuffer[TopicPartition]

      val currentErrors = new ConcurrentHashMap[TopicPartition, Errors]()
      marker.partitions.forEach { partition =>
        if (isViewTopic(partition.topic))
          // Views are read-only (PROMPT.md). The txn coordinator already cannot persist a view tp
          // into transaction state (testAddPartitionsToTxnOnViewTopicIsRejectedAsInvalidTopic), so
          // a well-behaved cluster will never reach this code with a view partition. Defense in
          // depth for the inter-broker / cluster-authorized path: a misrouted or replayed marker
          // request must never cause `replicaManager.appendRecords` below to write an
          // EndTxnMarker control batch into the view's local placeholder log. Surface
          // INVALID_TOPIC_EXCEPTION so the bug is visible upstream rather than silently leaving
          // operator-visible artifacts under the view name.
          currentErrors.put(partition, Errors.INVALID_TOPIC_EXCEPTION)
        else replicaManager.onlinePartition(partition) match {
          case Some(_)  =>
            partitionsWithCompatibleMessageFormat += partition
          case None =>
            currentErrors.put(partition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        }
      }

      if (!currentErrors.isEmpty)
        updateErrors(producerId, currentErrors)

      if (partitionsWithCompatibleMessageFormat.isEmpty) {
        numAppends.decrementAndGet()
        skippedMarkers += 1
      } else {
        val controlRecordType = marker.transactionResult match {
          case TransactionResult.COMMIT => ControlRecordType.COMMIT
          case TransactionResult.ABORT => ControlRecordType.ABORT
        }

        val markerResults = new ConcurrentHashMap[TopicPartition, Errors]()
        val numPartitions = new AtomicInteger(partitionsWithCompatibleMessageFormat.size)
        def addResultAndMaybeComplete(partition: TopicPartition, error: Errors): Unit = {
          markerResults.put(partition, error)
          // We should only call maybeSendResponseCallback once per marker. Otherwise, it causes sending the response
          // prematurely.
          if (numPartitions.decrementAndGet() == 0) {
            maybeSendResponseCallback(producerId, marker.transactionResult, markerResults)
          }
        }

        val controlRecords = mutable.Map.empty[TopicPartition, MemoryRecords]
        partitionsWithCompatibleMessageFormat.foreach { partition =>
          if (groupCoordinator.isNewGroupCoordinator && partition.topic == GROUP_METADATA_TOPIC_NAME) {
            // When the new group coordinator is used, writing the end marker is fully delegated
            // to the group coordinator.
            groupCoordinator.completeTransaction(
              partition,
              marker.producerId,
              marker.producerEpoch,
              marker.coordinatorEpoch,
              marker.transactionResult,
              Duration.ofMillis(config.requestTimeoutMs.toLong)
            ).whenComplete { (_, exception) =>
              val error = if (exception == null) {
                Errors.NONE
              } else {
                Errors.forException(exception) match {
                  case Errors.COORDINATOR_NOT_AVAILABLE | Errors.COORDINATOR_LOAD_IN_PROGRESS | Errors.NOT_COORDINATOR =>
                    // The transaction coordinator does not expect those errors so we translate them
                    // to NOT_LEADER_OR_FOLLOWER to signal to it that the coordinator is not ready yet.
                    Errors.NOT_LEADER_OR_FOLLOWER
                  case error =>
                    error
                }
              }
              addResultAndMaybeComplete(partition, error)
            }
          } else {
            // Otherwise, the regular appendRecords path is used for all the non __consumer_offsets
            // partitions or for all partitions when the new group coordinator is disabled.
            controlRecords += partition -> MemoryRecords.withEndTransactionMarker(
              producerId,
              marker.producerEpoch,
              new EndTransactionMarker(controlRecordType, marker.coordinatorEpoch)
            )
          }
        }

        if (controlRecords.nonEmpty) {
          replicaManager.appendRecords(
            timeout = config.requestTimeoutMs.toLong,
            requiredAcks = -1,
            internalTopicsAllowed = true,
            origin = AppendOrigin.COORDINATOR,
            entriesPerPartition = controlRecords,
            requestLocal = requestLocal,
            responseCallback = errors => {
              errors.foreachEntry { (tp, partitionResponse) =>
                addResultAndMaybeComplete(tp, partitionResponse.error)
              }
            }
          )
        }
      }
    }

    // No log appends were written as all partitions had incorrect log format
    // so we need to send the error response
    if (skippedMarkers == markers.size)
      requestHelper.sendResponseExemptThrottle(request, new WriteTxnMarkersResponse(errors))
  }

  def handleAddPartitionsToTxnRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val addPartitionsToTxnRequest =
      if (request.context.apiVersion() < 4)
        request.body[AddPartitionsToTxnRequest].normalizeRequest()
      else
        request.body[AddPartitionsToTxnRequest]
    val version = addPartitionsToTxnRequest.version
    val responses = new AddPartitionsToTxnResultCollection()
    val partitionsByTransaction = addPartitionsToTxnRequest.partitionsByTransaction()

    // Newer versions of the request should only come from other brokers.
    if (version >= 4) authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)

    // V4 requests introduced batches of transactions. We need all transactions to be handled before sending the
    // response so there are a few differences in handling errors and sending responses.
    def createResponse(requestThrottleMs: Int): AbstractResponse = {
      if (version < 4) {
        // There will only be one response in data. Add it to the response data object.
        val data = new AddPartitionsToTxnResponseData()
        responses.forEach { result =>
          data.setResultsByTopicV3AndBelow(result.topicResults())
          data.setThrottleTimeMs(requestThrottleMs)
        }
        new AddPartitionsToTxnResponse(data)
      } else {
        new AddPartitionsToTxnResponse(new AddPartitionsToTxnResponseData().setThrottleTimeMs(requestThrottleMs).setResultsByTransaction(responses))
      }
    }

    val txns = addPartitionsToTxnRequest.data.transactions
    def addResultAndMaybeSendResponse(result: AddPartitionsToTxnResult): Unit = {
      val canSend = responses.synchronized {
        responses.add(result)
        responses.size == txns.size
      }
      if (canSend) {
        requestHelper.sendResponseMaybeThrottle(request, createResponse)
      }
    }

    txns.forEach { transaction =>
      val transactionalId = transaction.transactionalId

      if (transactionalId == null)
        throw new InvalidRequestException("Transactional ID can not be null in request.")

      val partitionsToAdd = partitionsByTransaction.get(transactionalId).asScala

      // Versions < 4 come from clients and must be authorized to write for the given transaction and for the given topics.
      if (version < 4 && !authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, transactionalId)) {
        addResultAndMaybeSendResponse(addPartitionsToTxnRequest.errorResponseForTransaction(transactionalId, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED))
      } else {
        val unauthorizedTopicErrors = mutable.Map[TopicPartition, Errors]()
        val nonExistingTopicErrors = mutable.Map[TopicPartition, Errors]()
        val authorizedPartitions = mutable.Set[TopicPartition]()

        // Only request versions less than 4 need write authorization since they come from clients.
        val authorizedTopics =
          if (version < 4)
            authHelper.filterByAuthorized(request.context, WRITE, TOPIC, partitionsToAdd.filterNot(tp => Topic.isInternal(tp.topic)))(_.topic)
          else
            partitionsToAdd.map(_.topic).toSet
        for (topicPartition <- partitionsToAdd) {
          if (!authorizedTopics.contains(topicPartition.topic))
            unauthorizedTopicErrors += topicPartition -> Errors.TOPIC_AUTHORIZATION_FAILED
          else if (!metadataCache.contains(topicPartition))
            nonExistingTopicErrors += topicPartition -> Errors.UNKNOWN_TOPIC_OR_PARTITION
          else if (isViewTopic(topicPartition.topic))
            // Views are read-only (PROMPT.md, see handleProduceRequest). Reject AddPartitionsToTxn
            // before the txn coordinator persists the view tp into transaction state — otherwise a
            // subsequent WriteTxnMarkers would attempt to append EndTxnMarker control records to the
            // view's local placeholder log, violating the read-only invariant. Same INVALID_TOPIC_EXCEPTION
            // shape as the produce-side rejection so the client error model is uniform.
            unauthorizedTopicErrors += topicPartition -> Errors.INVALID_TOPIC_EXCEPTION
          else
            authorizedPartitions.add(topicPartition)
        }

        if (unauthorizedTopicErrors.nonEmpty || nonExistingTopicErrors.nonEmpty) {
          // Any failed partition check causes the entire transaction to fail. We send the appropriate error codes for the
          // partitions which failed, and an 'OPERATION_NOT_ATTEMPTED' error code for the partitions which succeeded
          // the authorization check to indicate that they were not added to the transaction.
          val partitionErrors = unauthorizedTopicErrors ++ nonExistingTopicErrors ++
            authorizedPartitions.map(_ -> Errors.OPERATION_NOT_ATTEMPTED)
          addResultAndMaybeSendResponse(AddPartitionsToTxnResponse.resultForTransaction(transactionalId, partitionErrors.asJava))
        } else {
          def sendResponseCallback(error: Errors): Unit = {
            val finalError = {
              if (version < 2 && error == Errors.PRODUCER_FENCED) {
                // For older clients, they could not understand the new PRODUCER_FENCED error code,
                // so we need to return the old INVALID_PRODUCER_EPOCH to have the same client handling logic.
                Errors.INVALID_PRODUCER_EPOCH
              } else {
                error
              }
            }
            addResultAndMaybeSendResponse(addPartitionsToTxnRequest.errorResponseForTransaction(transactionalId, finalError))
          }

          if (!transaction.verifyOnly) {
            txnCoordinator.handleAddPartitionsToTransaction(transactionalId,
              transaction.producerId,
              transaction.producerEpoch,
              authorizedPartitions,
              sendResponseCallback,
              TransactionVersion.transactionVersionForAddPartitionsToTxn(addPartitionsToTxnRequest),
              requestLocal)
          } else {
            txnCoordinator.handleVerifyPartitionsInTransaction(transactionalId,
              transaction.producerId,
              transaction.producerEpoch,
              authorizedPartitions,
              addResultAndMaybeSendResponse)
          }
        }
      }
    }
  }

  def handleAddOffsetsToTxnRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val addOffsetsToTxnRequest = request.body[AddOffsetsToTxnRequest]
    val transactionalId = addOffsetsToTxnRequest.data.transactionalId
    val groupId = addOffsetsToTxnRequest.data.groupId
    val offsetTopicPartition = new TopicPartition(GROUP_METADATA_TOPIC_NAME, groupCoordinator.partitionFor(groupId))

    if (!authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, transactionalId))
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AddOffsetsToTxnResponse(new AddOffsetsToTxnResponseData()
          .setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code)
          .setThrottleTimeMs(requestThrottleMs)))
    else if (!authHelper.authorize(request.context, READ, GROUP, groupId))
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AddOffsetsToTxnResponse(new AddOffsetsToTxnResponseData()
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)
          .setThrottleTimeMs(requestThrottleMs))
      )
    else {
      def sendResponseCallback(error: Errors): Unit = {
        def createResponse(requestThrottleMs: Int): AbstractResponse = {
          val finalError =
            if (addOffsetsToTxnRequest.version < 2 && error == Errors.PRODUCER_FENCED) {
              // For older clients, they could not understand the new PRODUCER_FENCED error code,
              // so we need to return the old INVALID_PRODUCER_EPOCH to have the same client handling logic.
              Errors.INVALID_PRODUCER_EPOCH
            } else {
              error
            }

          val responseBody: AddOffsetsToTxnResponse = new AddOffsetsToTxnResponse(
            new AddOffsetsToTxnResponseData()
              .setErrorCode(finalError.code)
              .setThrottleTimeMs(requestThrottleMs))
          trace(s"Completed $transactionalId's AddOffsetsToTxnRequest for group $groupId on partition " +
            s"$offsetTopicPartition: errors: $error from client ${request.header.clientId}")
          responseBody
        }
        requestHelper.sendResponseMaybeThrottle(request, createResponse)
      }

      txnCoordinator.handleAddPartitionsToTransaction(transactionalId,
        addOffsetsToTxnRequest.data.producerId,
        addOffsetsToTxnRequest.data.producerEpoch,
        Set(offsetTopicPartition),
        sendResponseCallback,
        TransactionVersion.TV_0, // This request will always come from the client not using TV 2.
        requestLocal)
    }
  }

  def handleTxnOffsetCommitRequest(
    request: RequestChannel.Request,
    requestLocal: RequestLocal
  ): CompletableFuture[Unit] = {
    val txnOffsetCommitRequest = request.body[TxnOffsetCommitRequest]

    def sendResponse(response: TxnOffsetCommitResponse): Unit = {
      // We need to replace COORDINATOR_LOAD_IN_PROGRESS with COORDINATOR_NOT_AVAILABLE
      // for older producer client from 0.11 to prior 2.0, which could potentially crash due
      // to unexpected loading error. This bug is fixed later by KAFKA-7296. Clients using
      // txn commit protocol >= 2 (version 2.3 and onwards) are guaranteed to have
      // the fix to check for the loading error.
      if (txnOffsetCommitRequest.version < 2) {
        response.data.topics.forEach { topic =>
          topic.partitions.forEach { partition =>
            if (partition.errorCode == Errors.COORDINATOR_LOAD_IN_PROGRESS.code) {
              partition.setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code)
            }
          }
        }
      }

      requestHelper.sendMaybeThrottle(request, response)
    }

    // authorize for the transactionalId and the consumer group. Note that we skip producerId authorization
    // since it is implied by transactionalId authorization
    if (!authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, txnOffsetCommitRequest.data.transactionalId)) {
      sendResponse(txnOffsetCommitRequest.getErrorResponse(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else if (!authHelper.authorize(request.context, READ, GROUP, txnOffsetCommitRequest.data.groupId)) {
      sendResponse(txnOffsetCommitRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      val authorizedTopics = authHelper.filterByAuthorized(
        request.context,
        READ,
        TOPIC,
        txnOffsetCommitRequest.data.topics.asScala
      )(_.name)

      val responseBuilder = new TxnOffsetCommitResponse.Builder()
      val authorizedTopicCommittedOffsets = new mutable.ArrayBuffer[TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic]()
      txnOffsetCommitRequest.data.topics.forEach { topic =>
        if (!authorizedTopics.contains(topic.name)) {
          // If the topic is not authorized, we add the topic and all its partitions
          // to the response with TOPIC_AUTHORIZATION_FAILED.
          responseBuilder.addPartitions[TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition](
            topic.name, topic.partitions, _.partitionIndex, Errors.TOPIC_AUTHORIZATION_FAILED)
        } else if (!metadataCache.contains(topic.name)) {
          // If the topic is unknown, we add the topic and all its partitions
          // to the response with UNKNOWN_TOPIC_OR_PARTITION.
          responseBuilder.addPartitions[TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition](
            topic.name, topic.partitions, _.partitionIndex, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        } else {
          // Otherwise, we check all partitions to ensure that they all exist.
          val topicWithValidPartitions = new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic().setName(topic.name)
          val isView = isViewTopic(topic.name)

          topic.partitions.forEach { partition =>
            if (metadataCache.getLeaderAndIsr(topic.name, partition.partitionIndex).nonEmpty) {
              // Mirror the OffsetCommit-side committedLeaderEpoch normalization on the
              // transactional path so a view tp never has a non-(-1) epoch persisted by either
              // commit shape. Same rationale: views have no client-validatable epoch ledger.
              if (isView) partition.setCommittedLeaderEpoch(-1)
              topicWithValidPartitions.partitions.add(partition)
            } else {
              responseBuilder.addPartition(topic.name, partition.partitionIndex, Errors.UNKNOWN_TOPIC_OR_PARTITION)
            }
          }

          if (!topicWithValidPartitions.partitions.isEmpty) {
            authorizedTopicCommittedOffsets += topicWithValidPartitions
          }
        }
      }

      if (authorizedTopicCommittedOffsets.isEmpty) {
        sendResponse(responseBuilder.build())
        CompletableFuture.completedFuture[Unit](())
      } else {
        val txnOffsetCommitRequestData = new TxnOffsetCommitRequestData()
          .setGroupId(txnOffsetCommitRequest.data.groupId)
          .setMemberId(txnOffsetCommitRequest.data.memberId)
          .setGenerationId(txnOffsetCommitRequest.data.generationId)
          .setGroupInstanceId(txnOffsetCommitRequest.data.groupInstanceId)
          .setProducerEpoch(txnOffsetCommitRequest.data.producerEpoch)
          .setProducerId(txnOffsetCommitRequest.data.producerId)
          .setTransactionalId(txnOffsetCommitRequest.data.transactionalId)
          .setTopics(authorizedTopicCommittedOffsets.asJava)

        groupCoordinator.commitTransactionalOffsets(
          request.context,
          txnOffsetCommitRequestData,
          requestLocal.bufferSupplier
        ).handle[Unit] { (response, exception) =>
          if (exception != null) {
            sendResponse(txnOffsetCommitRequest.getErrorResponse(exception))
          } else {
            sendResponse(responseBuilder.merge(response).build())
          }
        }
      }
    }
  }

  def handleDescribeAcls(request: RequestChannel.Request): Unit = {
    aclApis.handleDescribeAcls(request)
  }

  def handleOffsetForLeaderEpochRequest(request: RequestChannel.Request): Unit = {
    val offsetForLeaderEpoch = request.body[OffsetsForLeaderEpochRequest]
    val topics = offsetForLeaderEpoch.data.topics.asScala.toSeq

    // The OffsetsForLeaderEpoch API was initially only used for inter-broker communication and required
    // cluster permission. With KIP-320, the consumer now also uses this API to check for log truncation
    // following a leader change, so we also allow topic describe permission.
    val (authorizedTopics, unauthorizedTopics) =
      if (authHelper.authorize(request.context, CLUSTER_ACTION, CLUSTER, CLUSTER_NAME, logIfDenied = false))
        (topics, Seq.empty[OffsetForLeaderTopic])
      else authHelper.partitionSeqByAuthorized(request.context, DESCRIBE, TOPIC, topics)(_.topic)

    // View topics have no client-validatable leader-epoch ledger of their own:
    //
    //   1. (epoch, end_offset) returned by `replicaManager.lastOffsetForLeaderEpoch` reflects
    //      the VIEW partition's own local log, which never receives records directly —
    //      produce to a view is rejected per PROMPT.md, and the records a view consumer reads
    //      carry epoch-stripped batch headers (partition_leader_epoch = NO_PARTITION_LEADER_EPOCH;
    //      see ViewFilter). Asking the replica layer for a view-side (epoch, offset) lookup
    //      either returns UNDEFINED or is meaningless. Forwarding to the backing does not help:
    //      backing epochs are not interchangeable with view epochs (independent ledgers, KIP-595).
    //
    //   2. Inter-broker followers never replicate from a view leader (views are read-only virtual
    //      topics with no records of their own). A follower OFLE on a view is always a bug; it
    //      gets `INVALID_REQUEST` so it fails loudly.
    //
    //   3. Consumer-side OFLE on a view requires care. The original implementation returned
    //      `INVALID_REQUEST + UNDEFINED_EPOCH + UNDEFINED_EPOCH_OFFSET` for all callers, which
    //      worked safely IF the consumer never sent OFLE on a view partition. The three
    //      epoch-scrubbing fixes upstream make that the steady-state expectation: ViewFilter
    //      strips `partition_leader_epoch` from every batch header, divergingEpoch is rewritten
    //      to empty in applyViewFilter, and OffsetFetch normalizes CommittedLeaderEpoch to -1
    //      for view topics — so `position.offsetEpoch` stays empty on view partitions and OFLE
    //      is never sent (cf. OffsetsForLeaderEpochUtils.prepareRequest, which short-circuits
    //      when offsetEpoch is empty). But for an upgrade rollout where a consumer was running
    //      *before* those scrubs landed, an in-memory poisoned `offsetEpoch` can still trigger
    //      OFLE on a view. With INVALID_REQUEST the consumer falls into
    //      OffsetsForLeaderEpochUtils.handleResponse's `default:` case, leaves the partition in
    //      `partitionsToRetry`, and re-sends forever — the partition is silently wedged in
    //      AWAIT_VALIDATION. Returning `NONE + (leaderEpoch=0, endOffset=Long.MAX_VALUE)`
    //      instead drives SubscriptionState.maybeCompleteValidation into its `completeValidation()`
    //      branch (endOffset >= currentPosition.offset, and neither field is UNDEFINED), so the
    //      consumer exits AWAIT_VALIDATION cleanly without entering the truncation-reset path.
    //      The synthetic values are safe because the consumer's only consumer of the returned
    //      EpochEndOffset is `maybeCompleteValidation`; nothing else reads them (cf.
    //      OffsetForEpochResult.endOffsets in OffsetsForLeaderEpochUtils).
    //
    // Note: KIP-320 truncation detection is intrinsically degraded on view consumers — a view
    // has no end-offset ledger to validate against — which is an acceptable trade-off for the
    // topic-views feature (documented as out-of-scope for stretch goals in PROMPT.md). The
    // consumer-side recovery here trades "wedged AWAIT_VALIDATION" for "no truncation detection
    // on view partitions"; the latter is consistent with the design intent.
    val isConsumerRequest = offsetForLeaderEpoch.replicaId == OffsetsForLeaderEpochRequest.CONSUMER_REPLICA_ID
    val (viewTopics, nonViewTopics) = authorizedTopics.partition(t => isViewTopic(t.topic))
    val endOffsetsForViewPartitions = viewTopics.map { offsetForLeaderTopic =>
      val partitions = offsetForLeaderTopic.partitions.asScala.map { offsetForLeaderPartition =>
        if (isConsumerRequest) {
          // Synthetic non-wedging response — see point (3) above for the consumer-side rationale.
          // leaderEpoch=0 and endOffset=Long.MaxValue together defeat the UNDEFINED-sentinel
          // check AND the `endOffset < currentPosition.offset` truncation check, so
          // maybeCompleteValidation falls through to completeValidation() unconditionally.
          new EpochEndOffset()
            .setPartition(offsetForLeaderPartition.partition)
            .setErrorCode(Errors.NONE.code)
            .setLeaderEpoch(0)
            .setEndOffset(Long.MaxValue)
        } else {
          // Follower / inter-broker OFLE on a view is never a healthy code path — point (2).
          new EpochEndOffset()
            .setPartition(offsetForLeaderPartition.partition)
            .setErrorCode(Errors.INVALID_REQUEST.code)
            .setLeaderEpoch(OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH)
            .setEndOffset(OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET)
        }
      }
      new OffsetForLeaderTopicResult()
        .setTopic(offsetForLeaderTopic.topic)
        .setPartitions(partitions.toList.asJava)
    }

    val endOffsetsForAuthorizedPartitions = replicaManager.lastOffsetForLeaderEpoch(nonViewTopics)
    val endOffsetsForUnauthorizedPartitions = unauthorizedTopics.map { offsetForLeaderTopic =>
      val partitions = offsetForLeaderTopic.partitions.asScala.map { offsetForLeaderPartition =>
        new EpochEndOffset()
          .setPartition(offsetForLeaderPartition.partition)
          .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
      }

      new OffsetForLeaderTopicResult()
        .setTopic(offsetForLeaderTopic.topic)
        .setPartitions(partitions.toList.asJava)
    }

    val endOffsetsForAllTopics = new OffsetForLeaderTopicResultCollection(
      (endOffsetsForAuthorizedPartitions ++ endOffsetsForViewPartitions ++ endOffsetsForUnauthorizedPartitions)
        .asJava.iterator
    )

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new OffsetsForLeaderEpochResponse(new OffsetForLeaderEpochResponseData()
        .setThrottleTimeMs(requestThrottleMs)
        .setTopics(endOffsetsForAllTopics)))
  }

  def handleAlterConfigsRequest(request: RequestChannel.Request): Unit = {
    val original = request.body[AlterConfigsRequest]
    val preprocessingResponses = configManager.preprocess(original.data())
    val remaining = ConfigAdminManager.copyWithoutPreprocessed(original.data(), preprocessingResponses)
    def sendResponse(secondPart: Option[ApiMessage]): Unit = {
      secondPart match {
        case Some(result: AlterConfigsResponseData) =>
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new AlterConfigsResponse(ConfigAdminManager.reassembleLegacyResponse(
              original.data(),
              preprocessingResponses,
              result).setThrottleTimeMs(requestThrottleMs)))
        case _ => handleInvalidVersionsDuringForwarding(request)
      }
    }
    if (remaining.resources().isEmpty) {
      sendResponse(Some(new AlterConfigsResponseData()))
    } else {
      forwardingManager.forwardRequest(request,
        new AlterConfigsRequest(remaining, request.header.apiVersion()),
        response => sendResponse(response.map(_.data())))
    }
  }

  def handleIncrementalAlterConfigsRequest(request: RequestChannel.Request): Unit = {
    val original = request.body[IncrementalAlterConfigsRequest]
    val preprocessingResponses = configManager.preprocess(original.data(),
      (rType, rName) => authHelper.authorize(request.context, ALTER_CONFIGS, rType, rName))
    val remaining = ConfigAdminManager.copyWithoutPreprocessed(original.data(), preprocessingResponses)

    def sendResponse(secondPart: Option[ApiMessage]): Unit = {
      secondPart match {
        case Some(result: IncrementalAlterConfigsResponseData) =>
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new IncrementalAlterConfigsResponse(ConfigAdminManager.reassembleIncrementalResponse(
              original.data(),
              preprocessingResponses,
              result).setThrottleTimeMs(requestThrottleMs)))
        case _ => handleInvalidVersionsDuringForwarding(request)
      }
    }

    if (remaining.resources().isEmpty) {
      sendResponse(Some(new IncrementalAlterConfigsResponseData()))
    } else {
      forwardingManager.forwardRequest(request,
        new IncrementalAlterConfigsRequest(remaining, request.header.apiVersion()),
        response => sendResponse(response.map(_.data())))
    }
  }

  def handleDescribeConfigsRequest(request: RequestChannel.Request): Unit = {
    val responseData = configHelper.handleDescribeConfigsRequest(request, authHelper)
    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new DescribeConfigsResponse(responseData.setThrottleTimeMs(requestThrottleMs)))
  }

  def handleAlterReplicaLogDirsRequest(request: RequestChannel.Request): Unit = {
    val alterReplicaDirsRequest = request.body[AlterReplicaLogDirsRequest]
    if (authHelper.authorize(request.context, ALTER, CLUSTER, CLUSTER_NAME)) {
      // Views have no on-disk storage of their own (the placeholder log is never written to). Moving
      // it between log dirs is meaningless and would leave operator-visible artifacts under the
      // view name that could later be mistaken for real data. Fail loudly on view partitions and
      // pass the rest through to the replica manager.
      val (viewPartitions, nonViewPartitions) = alterReplicaDirsRequest.partitionDirs.asScala.toMap.partition {
        case (tp, _) => isViewTopic(tp.topic)
      }
      val viewResults: Map[TopicPartition, Errors] = viewPartitions.map { case (tp, _) => tp -> Errors.INVALID_TOPIC_EXCEPTION }
      val result = replicaManager.alterReplicaLogDirs(nonViewPartitions) ++ viewResults
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AlterReplicaLogDirsResponse(new AlterReplicaLogDirsResponseData()
          .setResults(result.groupBy(_._1.topic).map {
            case (topic, errors) => new AlterReplicaLogDirsResponseData.AlterReplicaLogDirTopicResult()
              .setTopicName(topic)
              .setPartitions(errors.map {
                case (tp, error) => new AlterReplicaLogDirsResponseData.AlterReplicaLogDirPartitionResult()
                  .setPartitionIndex(tp.partition)
                  .setErrorCode(error.code)
              }.toList.asJava)
          }.toList.asJava)
          .setThrottleTimeMs(requestThrottleMs)))
    } else {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        alterReplicaDirsRequest.getErrorResponse(requestThrottleMs, Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    }
  }

  def handleDescribeLogDirsRequest(request: RequestChannel.Request): Unit = {
    val describeLogDirsDirRequest = request.body[DescribeLogDirsRequest]
    val (logDirInfos, error) = {
      if (authHelper.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME)) {
        val partitions =
          if (describeLogDirsDirRequest.isAllTopicPartitions)
            replicaManager.logManager.allLogs.map(_.topicPartition).toSet
          else
            describeLogDirsDirRequest.data.topics.asScala.flatMap(
              logDirTopic => logDirTopic.partitions.asScala.map(partitionIndex =>
                new TopicPartition(logDirTopic.topic, partitionIndex))).toSet

        (replicaManager.describeLogDirs(partitions), Errors.NONE)
      } else {
        (List.empty[DescribeLogDirsResponseData.DescribeLogDirsResult], Errors.CLUSTER_AUTHORIZATION_FAILED)
      }
    }
    requestHelper.sendResponseMaybeThrottle(request, throttleTimeMs => new DescribeLogDirsResponse(new DescribeLogDirsResponseData()
      .setThrottleTimeMs(throttleTimeMs)
      .setResults(logDirInfos.asJava)
      .setErrorCode(error.code)))
  }

  def handleCreateTokenRequest(request: RequestChannel.Request): Unit = {
    val createTokenRequest = request.body[CreateDelegationTokenRequest]

    val requester = request.context.principal
    val ownerPrincipalName = createTokenRequest.data.ownerPrincipalName
    val owner = if (ownerPrincipalName == null || ownerPrincipalName.isEmpty) {
      request.context.principal
    } else {
      new KafkaPrincipal(createTokenRequest.data.ownerPrincipalType, ownerPrincipalName)
    }
    val renewerList = createTokenRequest.data.renewers.asScala.toList.map(entry =>
      new KafkaPrincipal(entry.principalType, entry.principalName))

    if (!allowTokenRequests(request)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        CreateDelegationTokenResponse.prepareResponse(request.context.requestVersion, requestThrottleMs,
          Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, owner, requester))
    } else if (!owner.equals(requester) && !authHelper.authorize(request.context, CREATE_TOKENS, USER, owner.toString)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        CreateDelegationTokenResponse.prepareResponse(request.context.requestVersion, requestThrottleMs,
          Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED, owner, requester))
    } else if (renewerList.exists(principal => principal.getPrincipalType != KafkaPrincipal.USER_TYPE)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        CreateDelegationTokenResponse.prepareResponse(request.context.requestVersion, requestThrottleMs,
          Errors.INVALID_PRINCIPAL_TYPE, owner, requester))
    } else {
      forwardToController(request)
    }
  }

  def handleExpireTokenRequest(request: RequestChannel.Request): Unit = {
    if (!allowTokenRequests(request)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new ExpireDelegationTokenResponse(
          new ExpireDelegationTokenResponseData()
              .setThrottleTimeMs(requestThrottleMs)
              .setErrorCode(Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED.code)
              .setExpiryTimestampMs(DelegationTokenManager.ErrorTimestamp)))
    } else {
      forwardToController(request)
    }
  }

  def handleRenewTokenRequest(request: RequestChannel.Request): Unit = {
    if (!allowTokenRequests(request)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new RenewDelegationTokenResponse(
          new RenewDelegationTokenResponseData()
            .setThrottleTimeMs(requestThrottleMs)
            .setErrorCode(Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED.code)
            .setExpiryTimestampMs(DelegationTokenManager.ErrorTimestamp)))
    } else {
      forwardToController(request)
    }
  }

  def handleDescribeTokensRequest(request: RequestChannel.Request): Unit = {
    val describeTokenRequest = request.body[DescribeDelegationTokenRequest]

    // the callback for sending a describe token response
    def sendResponseCallback(error: Errors, tokenDetails: List[DelegationToken]): Unit = {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new DescribeDelegationTokenResponse(request.context.requestVersion(), requestThrottleMs, error, tokenDetails.asJava))
      trace("Sending describe token response for correlation id %d to client %s."
        .format(request.header.correlationId, request.header.clientId))
    }

    if (!allowTokenRequests(request))
      sendResponseCallback(Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, List.empty)
    else if (!config.tokenAuthEnabled)
      sendResponseCallback(Errors.DELEGATION_TOKEN_AUTH_DISABLED, List.empty)
    else {
      val requestPrincipal = request.context.principal

      if (describeTokenRequest.ownersListEmpty()) {
        sendResponseCallback(Errors.NONE, List())
      }
      else {
        val owners = if (describeTokenRequest.data.owners == null)
          None
        else
          Some(describeTokenRequest.data.owners.asScala.map(p => new KafkaPrincipal(p.principalType(), p.principalName)).toList)
        def authorizeToken(tokenId: String) = authHelper.authorize(request.context, DESCRIBE, DELEGATION_TOKEN, tokenId)
        def authorizeRequester(owner: KafkaPrincipal) = authHelper.authorize(request.context, DESCRIBE_TOKENS, USER, owner.toString)
        def eligible(token: TokenInformation) = DelegationTokenManager
          .filterToken(requestPrincipal, owners, token, authorizeToken, authorizeRequester)
        val tokens =  tokenManager.getTokens(eligible)
        sendResponseCallback(Errors.NONE, tokens)
      }
    }
  }

  def allowTokenRequests(request: RequestChannel.Request): Boolean = {
    val protocol = request.context.securityProtocol
    if (request.context.principal.tokenAuthenticated ||
      protocol == SecurityProtocol.PLAINTEXT ||
      // disallow requests from 1-way SSL
      (protocol == SecurityProtocol.SSL && request.context.principal == KafkaPrincipal.ANONYMOUS))
      false
    else
      true
  }

  def handleOffsetDeleteRequest(
    request: RequestChannel.Request,
    requestLocal: RequestLocal
  ): CompletableFuture[Unit] = {
    val offsetDeleteRequest = request.body[OffsetDeleteRequest]

    if (!authHelper.authorize(request.context, DELETE, GROUP, offsetDeleteRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, offsetDeleteRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      val authorizedTopics = authHelper.filterByAuthorized(
        request.context,
        READ,
        TOPIC,
        offsetDeleteRequest.data.topics.asScala
      )(_.name)

      val responseBuilder = new OffsetDeleteResponse.Builder
      val authorizedTopicPartitions = new OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection()
      offsetDeleteRequest.data.topics.forEach { topic =>
        if (!authorizedTopics.contains(topic.name)) {
          // If the topic is not authorized, we add the topic and all its partitions
          // to the response with TOPIC_AUTHORIZATION_FAILED.
          responseBuilder.addPartitions[OffsetDeleteRequestData.OffsetDeleteRequestPartition](
            topic.name, topic.partitions, _.partitionIndex, Errors.TOPIC_AUTHORIZATION_FAILED)
        } else if (!metadataCache.contains(topic.name)) {
          // If the topic is unknown, we add the topic and all its partitions
          // to the response with UNKNOWN_TOPIC_OR_PARTITION.
          responseBuilder.addPartitions[OffsetDeleteRequestData.OffsetDeleteRequestPartition](
            topic.name, topic.partitions, _.partitionIndex, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        } else {
          // Otherwise, we check all partitions to ensure that they all exist.
          val topicWithValidPartitions = new OffsetDeleteRequestData.OffsetDeleteRequestTopic().setName(topic.name)

          topic.partitions.forEach { partition =>
            if (metadataCache.getLeaderAndIsr(topic.name, partition.partitionIndex).nonEmpty) {
              topicWithValidPartitions.partitions.add(partition)
            } else {
              responseBuilder.addPartition(topic.name, partition.partitionIndex, Errors.UNKNOWN_TOPIC_OR_PARTITION)
            }
          }

          if (!topicWithValidPartitions.partitions.isEmpty) {
            authorizedTopicPartitions.add(topicWithValidPartitions)
          }
        }
      }

      val offsetDeleteRequestData = new OffsetDeleteRequestData()
        .setGroupId(offsetDeleteRequest.data.groupId)
        .setTopics(authorizedTopicPartitions)

      groupCoordinator.deleteOffsets(
        request.context,
        offsetDeleteRequestData,
        requestLocal.bufferSupplier
      ).handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, offsetDeleteRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, responseBuilder.merge(response).build())
        }
      }
    }
  }

  def handleDescribeClientQuotasRequest(request: RequestChannel.Request): Unit = {
    val describeClientQuotasRequest = request.body[DescribeClientQuotasRequest]

    if (!authHelper.authorize(request.context, DESCRIBE_CONFIGS, CLUSTER, CLUSTER_NAME)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        describeClientQuotasRequest.getErrorResponse(requestThrottleMs, Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    } else {
      val result = metadataCache.asInstanceOf[KRaftMetadataCache].describeClientQuotas(describeClientQuotasRequest.data())
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
        result.setThrottleTimeMs(requestThrottleMs)
        new DescribeClientQuotasResponse(result)
      })
    }
  }

  def handleDescribeUserScramCredentialsRequest(request: RequestChannel.Request): Unit = {
    val describeUserScramCredentialsRequest = request.body[DescribeUserScramCredentialsRequest]

    if (!authHelper.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        describeUserScramCredentialsRequest.getErrorResponse(requestThrottleMs, Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    } else {
      val result = metadataCache.asInstanceOf[KRaftMetadataCache].describeScramCredentials(describeUserScramCredentialsRequest.data())
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new DescribeUserScramCredentialsResponse(result.setThrottleTimeMs(requestThrottleMs)))
    }
  }

  def handleDescribeCluster(request: RequestChannel.Request): Unit = {
    val response = authHelper.computeDescribeClusterResponse(
      request,
      EndpointType.BROKER,
      clusterId,
      () => {
        val brokers = new DescribeClusterResponseData.DescribeClusterBrokerCollection()
        val describeClusterRequest = request.body[DescribeClusterRequest]
        metadataCache.getBrokerNodes(request.context.listenerName).foreach { node =>
          if (!node.isFenced || describeClusterRequest.data().includeFencedBrokers()) {
          brokers.add(new DescribeClusterResponseData.DescribeClusterBroker().
            setBrokerId(node.id).
            setHost(node.host).
            setPort(node.port).
            setRack(node.rack).
            setIsFenced(node.isFenced))
          }
        }
        brokers
      },
      () => {
        metadataCache.getControllerId match {
          case Some(value) =>
            value match {
              case ZkCachedControllerId (id) => id
              case KRaftCachedControllerId (_) => metadataCache.getRandomAliveBrokerId.getOrElse(- 1)
            }
          case None => -1
        }
      }
    )
    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new DescribeClusterResponse(response.setThrottleTimeMs(requestThrottleMs)))
  }

  def handleDescribeProducersRequest(request: RequestChannel.Request): Unit = {
    val describeProducersRequest = request.body[DescribeProducersRequest]

    def partitionError(
      topicPartition: TopicPartition,
      apiError: ApiError
    ): DescribeProducersResponseData.PartitionResponse = {
      new DescribeProducersResponseData.PartitionResponse()
        .setPartitionIndex(topicPartition.partition)
        .setErrorCode(apiError.error.code)
        .setErrorMessage(apiError.message)
    }

    val response = new DescribeProducersResponseData()
    describeProducersRequest.data.topics.forEach { topicRequest =>
      val topicResponse = new DescribeProducersResponseData.TopicResponse()
        .setName(topicRequest.name)

      val invalidTopicError = checkValidTopic(topicRequest.name)

      val topicError = invalidTopicError.orElse {
        if (!authHelper.authorize(request.context, READ, TOPIC, topicRequest.name)) {
          Some(new ApiError(Errors.TOPIC_AUTHORIZATION_FAILED))
        } else if (!metadataCache.contains(topicRequest.name))
          Some(new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION))
        else if (isViewTopic(topicRequest.name))
          // Views have no producers (produce is rejected before backing resolution). Reading the
          // view's local placeholder log producer-state-snapshot would either return an empty
          // result (today) or expose internal-control-record producer IDs if any txn-coordinator
          // bug ever leaked one. Fail fast with INVALID_TOPIC_EXCEPTION to make the read-only
          // contract uniform across describe-style APIs.
          Some(new ApiError(Errors.INVALID_TOPIC_EXCEPTION))
        else {
          None
        }
      }

      topicRequest.partitionIndexes.forEach { partitionId =>
        val topicPartition = new TopicPartition(topicRequest.name, partitionId)
        val partitionResponse = topicError match {
          case Some(error) => partitionError(topicPartition, error)
          case None => replicaManager.activeProducerState(topicPartition)
        }
        topicResponse.partitions.add(partitionResponse)
      }

      response.topics.add(topicResponse)
    }

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new DescribeProducersResponse(response.setThrottleTimeMs(requestThrottleMs)))
  }

  private def checkValidTopic(topic: String): Option[ApiError] = {
    try {
      Topic.validate(topic)
      None
    } catch {
      case e: Throwable => Some(ApiError.fromThrowable(e))
    }
  }

  def handleDescribeTransactionsRequest(request: RequestChannel.Request): Unit = {
    val describeTransactionsRequest = request.body[DescribeTransactionsRequest]
    val response = new DescribeTransactionsResponseData()

    describeTransactionsRequest.data.transactionalIds.forEach { transactionalId =>
      val transactionState = if (!authHelper.authorize(request.context, DESCRIBE, TRANSACTIONAL_ID, transactionalId)) {
        new DescribeTransactionsResponseData.TransactionState()
          .setTransactionalId(transactionalId)
          .setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code)
      } else {
        txnCoordinator.handleDescribeTransactions(transactionalId)
      }

      // Include only partitions which the principal is authorized to describe
      val topicIter = transactionState.topics.iterator()
      while (topicIter.hasNext) {
        val topic = topicIter.next().topic
        if (!authHelper.authorize(request.context, DESCRIBE, TOPIC, topic)) {
          topicIter.remove()
        }
      }
      response.transactionStates.add(transactionState)
    }

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new DescribeTransactionsResponse(response.setThrottleTimeMs(requestThrottleMs)))
  }

  def handleListTransactionsRequest(request: RequestChannel.Request): Unit = {
    val listTransactionsRequest = request.body[ListTransactionsRequest]
    val filteredProducerIds = listTransactionsRequest.data.producerIdFilters.asScala.map(Long.unbox).toSet
    val filteredStates = listTransactionsRequest.data.stateFilters.asScala.toSet
    val durationFilter = listTransactionsRequest.data.durationFilter()
    val response = txnCoordinator.handleListTransactions(filteredProducerIds, filteredStates, durationFilter)

    // The response should contain only transactionalIds that the principal
    // has `Describe` permission to access.
    val transactionStateIter = response.transactionStates.iterator()
    while (transactionStateIter.hasNext) {
      val transactionState = transactionStateIter.next()
      if (!authHelper.authorize(request.context, DESCRIBE, TRANSACTIONAL_ID, transactionState.transactionalId)) {
        transactionStateIter.remove()
      }
    }

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new ListTransactionsResponse(response.setThrottleTimeMs(requestThrottleMs)))
  }

  private def groupVersion(): GroupVersion = {
    GroupVersion.fromFeatureLevel(metadataCache.features.finalizedFeatures.getOrDefault(GroupVersion.FEATURE_NAME, 0.toShort))
  }

  def isConsumerGroupProtocolEnabled(): Boolean = {
    groupCoordinator.isNewGroupCoordinator &&
      config.groupCoordinatorRebalanceProtocols.contains(Group.GroupType.CONSUMER) &&
      groupVersion().isConsumerRebalanceProtocolSupported
  }

  def handleConsumerGroupHeartbeat(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val consumerGroupHeartbeatRequest = request.body[ConsumerGroupHeartbeatRequest]

    if (!isConsumerGroupProtocolEnabled()) {
      // The API is not supported by the "old" group coordinator (the default). If the
      // new one is not enabled, we fail directly here.
      requestHelper.sendMaybeThrottle(request, consumerGroupHeartbeatRequest.getErrorResponse(Errors.UNSUPPORTED_VERSION.exception))
      CompletableFuture.completedFuture[Unit](())
    } else if (!authHelper.authorize(request.context, READ, GROUP, consumerGroupHeartbeatRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, consumerGroupHeartbeatRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      if (consumerGroupHeartbeatRequest.data.subscribedTopicNames != null &&
        !consumerGroupHeartbeatRequest.data.subscribedTopicNames.isEmpty) {
        // Check the authorization if the subscribed topic names are provided.
        // Clients are not allowed to see topics that are not authorized for Describe.
        val subscribedTopicSet = consumerGroupHeartbeatRequest.data.subscribedTopicNames.asScala.toSet
        val authorizedTopics = authHelper.filterByAuthorized(request.context, DESCRIBE, TOPIC,
          subscribedTopicSet)(identity)
        if (authorizedTopics.size < subscribedTopicSet.size) {
          val responseData = new ConsumerGroupHeartbeatResponseData()
            .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
          requestHelper.sendMaybeThrottle(request, new ConsumerGroupHeartbeatResponse(responseData))
          return CompletableFuture.completedFuture[Unit](())
        }
      }

      groupCoordinator.consumerGroupHeartbeat(
        request.context,
        consumerGroupHeartbeatRequest.data
      ).handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, consumerGroupHeartbeatRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, new ConsumerGroupHeartbeatResponse(response))
        }
      }
    }
  }

  def handleConsumerGroupDescribe(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val consumerGroupDescribeRequest = request.body[ConsumerGroupDescribeRequest]
    val includeAuthorizedOperations = consumerGroupDescribeRequest.data.includeAuthorizedOperations

    if (!isConsumerGroupProtocolEnabled()) {
      // The API is not supported by the "old" group coordinator (the default). If the
      // new one is not enabled, we fail directly here.
      requestHelper.sendMaybeThrottle(request, request.body[ConsumerGroupDescribeRequest].getErrorResponse(Errors.UNSUPPORTED_VERSION.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      val response = new ConsumerGroupDescribeResponseData()

      val authorizedGroups = new ArrayBuffer[String]()
      consumerGroupDescribeRequest.data.groupIds.forEach { groupId =>
        if (!authHelper.authorize(request.context, DESCRIBE, GROUP, groupId)) {
          response.groups.add(new ConsumerGroupDescribeResponseData.DescribedGroup()
            .setGroupId(groupId)
            .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)
          )
        } else {
          authorizedGroups += groupId
        }
      }

      groupCoordinator.consumerGroupDescribe(
        request.context,
        authorizedGroups.asJava
      ).handle[Unit] { (results, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, consumerGroupDescribeRequest.getErrorResponse(exception))
        } else {
          if (includeAuthorizedOperations) {
            results.forEach { groupResult =>
              if (groupResult.errorCode == Errors.NONE.code) {
                groupResult.setAuthorizedOperations(authHelper.authorizedOperations(
                  request,
                  new Resource(ResourceType.GROUP, groupResult.groupId)
                ))
              }
            }
          }

          if (response.groups.isEmpty) {
            // If the response is empty, we can directly reuse the results.
            response.setGroups(results)
          } else {
            // Otherwise, we have to copy the results into the existing ones.
            response.groups.addAll(results)
          }

          // Clients are not allowed to see topics that are not authorized for Describe.
          if (!authorizer.isEmpty) {
            val topicsToCheck = response.groups.stream()
              .flatMap(group => group.members.stream)
              .flatMap(member => util.stream.Stream.of(member.assignment, member.targetAssignment))
              .flatMap(assignment => assignment.topicPartitions.stream)
              .map(topicPartition => topicPartition.topicName)
              .collect(Collectors.toSet[String])
              .asScala
            val authorizedTopics = authHelper.filterByAuthorized(request.context, DESCRIBE, TOPIC,
              topicsToCheck)(identity)
            val updatedGroups = response.groups.stream().map { group =>
              val hasUnauthorizedTopic = group.members.stream()
                .flatMap(member => util.stream.Stream.of(member.assignment, member.targetAssignment))
                .flatMap(assignment => assignment.topicPartitions.stream())
                .anyMatch(tp => !authorizedTopics.contains(tp.topicName))

              if (hasUnauthorizedTopic) {
                new ConsumerGroupDescribeResponseData.DescribedGroup()
                  .setGroupId(group.groupId)
                  .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
                  .setErrorMessage("The group has described topic(s) that the client is not authorized to describe.")
                  .setMembers(List.empty.asJava)
              } else {
                group
              }
            }.collect(Collectors.toList[ConsumerGroupDescribeResponseData.DescribedGroup])
            response.setGroups(updatedGroups)
          }

          requestHelper.sendMaybeThrottle(request, new ConsumerGroupDescribeResponse(response))
        }
      }
    }

  }

  def handleGetTelemetrySubscriptionsRequest(request: RequestChannel.Request): Unit = {
    val subscriptionRequest = request.body[GetTelemetrySubscriptionsRequest]
    try {
      requestHelper.sendMaybeThrottle(request, clientMetricsManager.processGetTelemetrySubscriptionRequest(subscriptionRequest, request.context))
    } catch {
      case _: Exception =>
        requestHelper.sendMaybeThrottle(request, subscriptionRequest.getErrorResponse(Errors.INVALID_REQUEST.exception))
    }
  }

  private def handlePushTelemetryRequest(request: RequestChannel.Request): Unit = {
    val pushTelemetryRequest = request.body[PushTelemetryRequest]
    try {
      requestHelper.sendMaybeThrottle(request, clientMetricsManager.processPushTelemetryRequest(pushTelemetryRequest, request.context))
    } catch {
      case _: Exception =>
        requestHelper.sendMaybeThrottle(request, pushTelemetryRequest.getErrorResponse(Errors.INVALID_REQUEST.exception))
    }
  }

  def handleListClientMetricsResources(request: RequestChannel.Request): Unit = {
    val listClientMetricsResourcesRequest = request.body[ListClientMetricsResourcesRequest]

    if (!authHelper.authorize(request.context, DESCRIBE_CONFIGS, CLUSTER, CLUSTER_NAME)) {
      requestHelper.sendMaybeThrottle(request, listClientMetricsResourcesRequest.getErrorResponse(Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    } else {
      val data = new ListClientMetricsResourcesResponseData().setClientMetricsResources(
        clientMetricsManager.listClientMetricsResources.stream.map(
          name => new ClientMetricsResource().setName(name)).toList)
      requestHelper.sendMaybeThrottle(request, new ListClientMetricsResourcesResponse(data))
    }
  }

  def handleShareGroupHeartbeat(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val shareGroupHeartbeatRequest = request.body[ShareGroupHeartbeatRequest]

    if (!isShareGroupProtocolEnabled) {
      requestHelper.sendMaybeThrottle(request, shareGroupHeartbeatRequest.getErrorResponse(Errors.UNSUPPORTED_VERSION.exception))
      CompletableFuture.completedFuture[Unit](())
    } else if (!authHelper.authorize(request.context, READ, GROUP, shareGroupHeartbeatRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, shareGroupHeartbeatRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      groupCoordinator.shareGroupHeartbeat(
        request.context,
        shareGroupHeartbeatRequest.data,
      ).handle[Unit] { (response, exception) =>

        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, shareGroupHeartbeatRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, new ShareGroupHeartbeatResponse(response))
        }
      }
    }
  }

  def handleShareGroupDescribe(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val shareGroupDescribeRequest = request.body[ShareGroupDescribeRequest]
    val includeAuthorizedOperations = shareGroupDescribeRequest.data.includeAuthorizedOperations

    if (!isShareGroupProtocolEnabled) {
      requestHelper.sendMaybeThrottle(request, shareGroupDescribeRequest.getErrorResponse(Errors.UNSUPPORTED_VERSION.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      val response = new ShareGroupDescribeResponseData()

      val authorizedGroups = new ArrayBuffer[String]()
      shareGroupDescribeRequest.data.groupIds.forEach { groupId =>
        if (!authHelper.authorize(request.context, DESCRIBE, GROUP, groupId)) {
          response.groups.add(new ShareGroupDescribeResponseData.DescribedGroup()
            .setGroupId(groupId)
            .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)
          )
        } else {
          authorizedGroups += groupId
        }
      }

      groupCoordinator.shareGroupDescribe(
        request.context,
        authorizedGroups.asJava
      ).handle[Unit] { (results, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, shareGroupDescribeRequest.getErrorResponse(exception))
        } else {
          if (includeAuthorizedOperations) {
            results.forEach { groupResult =>
              if (groupResult.errorCode == Errors.NONE.code) {
                groupResult.setAuthorizedOperations(authHelper.authorizedOperations(
                  request,
                  new Resource(ResourceType.GROUP, groupResult.groupId)
                ))
              }
            }
          }

          if (response.groups.isEmpty) {
            // If the response is empty, we can directly reuse the results.
            response.setGroups(results)
          } else {
            // Otherwise, we have to copy the results into the existing ones.
            response.groups.addAll(results)
          }

          requestHelper.sendMaybeThrottle(request, new ShareGroupDescribeResponse(response))
        }
      }
    }
  }

  /**
   * Handle a shareFetch request
   */
  def handleShareFetchRequest(request: RequestChannel.Request): Unit = {
    val shareFetchRequest = request.body[ShareFetchRequest]

    if (!isShareGroupProtocolEnabled) {
      requestHelper.sendMaybeThrottle(request, shareFetchRequest.getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, Errors.UNSUPPORTED_VERSION.exception))
      return
    }

    val groupId = shareFetchRequest.data.groupId

    // Share Fetch needs permission to perform the READ action on the named group resource (groupId)
    if (!authHelper.authorize(request.context, READ, GROUP, groupId)) {
      requestHelper.sendMaybeThrottle(request, shareFetchRequest.getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, Errors.GROUP_AUTHORIZATION_FAILED.exception))
      return
    }

    val memberId = shareFetchRequest.data.memberId
    val shareSessionEpoch = shareFetchRequest.data.shareSessionEpoch

    def isAcknowledgeDataPresentInFetchRequest: Boolean = {
      shareFetchRequest.data.topics.asScala
        .flatMap(t => t.partitions().asScala)
        .exists(partition => partition.acknowledgementBatches != null && !partition.acknowledgementBatches.isEmpty)
    }

    val isAcknowledgeDataPresent = isAcknowledgeDataPresentInFetchRequest
    val topicIdNames = metadataCache.topicIdsToNames()

    val shareFetchData = shareFetchRequest.shareFetchData(topicIdNames)
    val forgottenTopics = shareFetchRequest.forgottenTopics(topicIdNames)

    val newReqMetadata: ShareRequestMetadata = new ShareRequestMetadata(Uuid.fromString(memberId), shareSessionEpoch)
    var shareFetchContext: ShareFetchContext = null

    try {
      // Creating the shareFetchContext for Share Session Handling. if context creation fails, the request is failed directly here.
      shareFetchContext = sharePartitionManager.newContext(groupId, shareFetchData, forgottenTopics, newReqMetadata, isAcknowledgeDataPresent)
    } catch {
      case e: Exception =>
        requestHelper.sendMaybeThrottle(request, shareFetchRequest.getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, e))
        return
    }

    val erroneousAndValidPartitionData: ErroneousAndValidPartitionData = shareFetchContext.getErroneousAndValidTopicIdPartitions
    val topicIdPartitionSeq: mutable.Set[TopicIdPartition] = mutable.Set()
    erroneousAndValidPartitionData.erroneous.forEach {
      case(tp, _) => if (!topicIdPartitionSeq.contains(tp)) topicIdPartitionSeq += tp
    }
    erroneousAndValidPartitionData.validTopicIdPartitions.forEach {
      case(tp, _) => if (!topicIdPartitionSeq.contains(tp)) topicIdPartitionSeq += tp
    }
    shareFetchData.forEach {
      case(tp, _) => if (!topicIdPartitionSeq.contains(tp)) topicIdPartitionSeq += tp
    }

    // Kafka share consumers need READ permission on each topic they are fetching.
    val authorizedTopics = authHelper.filterByAuthorized(
      request.context,
      READ,
      TOPIC,
      topicIdPartitionSeq
    )(_.topicPartition.topic)

    // Variable to store the topic partition wise result of piggybacked acknowledgements.
    var acknowledgeResult: CompletableFuture[Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]] =
      CompletableFuture.completedFuture(mutable.Map.empty)

    // Handling the Acknowledgements from the ShareFetchRequest If this check is true, we are sure that this is not an
    // Initial ShareFetch Request, otherwise the request would have been invalid.
    if (isAcknowledgeDataPresent) {
      val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()
      val acknowledgementDataFromRequest = getAcknowledgeBatchesFromShareFetchRequest(shareFetchRequest, topicIdNames, erroneous)
      acknowledgeResult = handleAcknowledgements(
        acknowledgementDataFromRequest,
        erroneous,
        sharePartitionManager,
        authorizedTopics,
        groupId,
        memberId,
      )
    }

    // Handling the Fetch from the ShareFetchRequest.
    // Variable to store the topic partition wise result of fetching.
    val fetchResult: CompletableFuture[Map[TopicIdPartition, ShareFetchResponseData.PartitionData]] =
      handleFetchFromShareFetchRequest(
      request,
      erroneousAndValidPartitionData,
      sharePartitionManager,
      authorizedTopics
    )

    def combineShareFetchAndShareAcknowledgeResponses(fetchResult: CompletableFuture[Map[TopicIdPartition, ShareFetchResponseData.PartitionData]],
                                                      acknowledgeResult: CompletableFuture[Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]],
                                                     ): CompletableFuture[ShareFetchResponse] = {

      fetchResult.thenCombine(acknowledgeResult,
        (fetchMap: Map[TopicIdPartition, ShareFetchResponseData.PartitionData],
          acknowledgeMap: Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]) => {
          val shareFetchResponse = processShareFetchResponse(fetchMap, request, topicIdNames, shareFetchContext)
          // The outer map has topicId as the key and the inner map has partitionIndex as the key.
          val topicPartitionAcknowledgements: mutable.Map[Uuid, mutable.Map[Int, Short]] = mutable.Map()
          if (acknowledgeMap != null && acknowledgeMap.nonEmpty) {
            acknowledgeMap.foreach { case (tp, partitionData) =>
              topicPartitionAcknowledgements.get(tp.topicId) match {
                case Some(subMap) =>
                  subMap += tp.partition -> partitionData.errorCode
                case None =>
                  val partitionAcknowledgementsMap: mutable.Map[Int, Short] = mutable.Map()
                  partitionAcknowledgementsMap += tp.partition -> partitionData.errorCode
                  topicPartitionAcknowledgements += tp.topicId -> partitionAcknowledgementsMap
              }
            }
          }

          shareFetchResponse.data.responses.forEach{ topic =>
            val topicId = topic.topicId
            topicPartitionAcknowledgements.get(topicId) match {
              case Some(subMap) =>
                topic.partitions.forEach { partition =>
                  subMap.get(partition.partitionIndex) match {
                    case Some(value) =>
                      partition.setAcknowledgeErrorCode(value)
                      // Delete the element.
                      subMap.remove(partition.partitionIndex)
                    case None =>
                  }
                }
                // Add the remaining acknowledgements.
                subMap.foreach { case (partitionIndex, value) =>
                  val fetchPartitionData = new ShareFetchResponseData.PartitionData()
                    .setPartitionIndex(partitionIndex)
                    .setErrorCode(Errors.NONE.code)
                    .setAcknowledgeErrorCode(value)
                  topic.partitions.add(fetchPartitionData)
                }
                topicPartitionAcknowledgements.remove(topicId)
              case None =>
            }
          }
          // Add the remaining acknowledgements.
          topicPartitionAcknowledgements.foreach { case (topicId, subMap) =>
            val topicData = new ShareFetchResponseData.ShareFetchableTopicResponse()
              .setTopicId(topicId)
            subMap.foreach { case (partitionIndex, value) =>
              val fetchPartitionData = new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(partitionIndex)
                .setErrorCode(Errors.NONE.code)
                .setAcknowledgeErrorCode(value)
              topicData.partitions.add(fetchPartitionData)
            }
            shareFetchResponse.data.responses.add(topicData)
          }

          if (shareSessionEpoch == ShareRequestMetadata.FINAL_EPOCH) {
            sharePartitionManager.releaseSession(groupId, memberId).
              whenComplete((releaseAcquiredRecordsData, throwable) =>
                if (throwable != null) {
                  error(s"Releasing share session close with correlation from client ${request.header.clientId}  " +
                    s"failed with error ${throwable.getMessage}")
                } else {
                  info(s"Releasing share session close $releaseAcquiredRecordsData succeeded")
                }
              )
          }
          shareFetchResponse
        })
    }

    // Send the response once the future completes.
    combineShareFetchAndShareAcknowledgeResponses(fetchResult, acknowledgeResult).handle[Unit] {(result, exception) =>
      if (exception != null) {
        requestHelper.sendMaybeThrottle(request, shareFetchRequest.getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, exception))
      } else {
        requestChannel.sendResponse(request, result, None)
      }
    }
  }

  // Visible for Testing
  def handleFetchFromShareFetchRequest(request: RequestChannel.Request,
                                       erroneousAndValidPartitionData: ErroneousAndValidPartitionData,
                                       sharePartitionManagerInstance: SharePartitionManager,
                                       authorizedTopics: Set[String]
                                      ): CompletableFuture[Map[TopicIdPartition, ShareFetchResponseData.PartitionData]] = {

    val erroneous = mutable.Map.empty[TopicIdPartition, ShareFetchResponseData.PartitionData]
    erroneousAndValidPartitionData.erroneous.forEach { (topicIdPartition, partitionData) => erroneous.put(topicIdPartition, partitionData) }

    val interestedWithMaxBytes = new util.LinkedHashMap[TopicIdPartition, Integer]

    erroneousAndValidPartitionData.validTopicIdPartitions.forEach { case (topicIdPartition, sharePartitionData) =>
      if (!authorizedTopics.contains(topicIdPartition.topicPartition.topic))
        erroneous += topicIdPartition -> ShareFetchResponse.partitionResponse(topicIdPartition, Errors.TOPIC_AUTHORIZATION_FAILED)
      else if (!metadataCache.contains(topicIdPartition.topicPartition))
        erroneous += topicIdPartition -> ShareFetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
      else if (isViewTopic(topicIdPartition.topicPartition.topic))
        // Views are read-only (PROMPT.md). Share-fetch bypasses the regular fetch path's view
        // rewrite (KafkaApis.handleFetchRequest → applyViewFilter): SharePartitionManager reads
        // the view's local placeholder log directly via ReplicaManager.readFromLog
        // (DelayedShareFetch / ShareFetchUtils), which would either return an empty result (the
        // happy case) or surface stale local records / control batches under the view name if any
        // ever leaked through other paths. Fail fast with INVALID_TOPIC_EXCEPTION, mirroring the
        // DescribeProducers treatment above, until share-fetch grows its own view-aware path.
        erroneous += topicIdPartition -> ShareFetchResponse.partitionResponse(topicIdPartition, Errors.INVALID_TOPIC_EXCEPTION)
      else
        interestedWithMaxBytes.put(topicIdPartition, sharePartitionData.maxBytes)
    }

    val shareFetchRequest = request.body[ShareFetchRequest]

    val clientId = request.header.clientId
    val versionId = request.header.apiVersion
    val groupId = shareFetchRequest.data.groupId

    if (interestedWithMaxBytes.isEmpty) {
      CompletableFuture.completedFuture(erroneous)
    } else {
      // for share fetch from consumer, cap fetchMaxBytes to the maximum bytes that could be fetched without being
      // throttled given no bytes were recorded in the recent quota window. Trying to fetch more bytes would result
      // in a guaranteed throttling potentially blocking consumer progress.
      val maxQuotaWindowBytes = quotas.fetch.getMaxValueInQuotaWindow(request.session, clientId).toInt

      val fetchMaxBytes = Math.min(Math.min(shareFetchRequest.maxBytes, config.fetchMaxBytes), maxQuotaWindowBytes)
      val fetchMinBytes = Math.min(shareFetchRequest.minBytes, fetchMaxBytes)

      val clientMetadata: Optional[ClientMetadata] =
        Optional.of(new DefaultClientMetadata(
          CommonClientConfigs.DEFAULT_CLIENT_RACK,
          clientId,
          request.context.clientAddress,
          request.context.principal,
          request.context.listenerName.value))

      val params = new FetchParams(
        versionId,
        FetchRequest.CONSUMER_REPLICA_ID,
        -1,
        shareFetchRequest.maxWait,
        fetchMinBytes,
        fetchMaxBytes,
        FetchIsolation.HIGH_WATERMARK,
        clientMetadata,
        true
      )

      // call the share partition manager to fetch messages from the local replica.
      sharePartitionManagerInstance.fetchMessages(
        groupId,
        shareFetchRequest.data.memberId,
        params,
        interestedWithMaxBytes
      ).thenApply{ result =>
        val combinedResult = mutable.Map.empty[TopicIdPartition, ShareFetchResponseData.PartitionData]
        result.asScala.foreach { case (tp, data) =>
          combinedResult += (tp -> data)
        }
        erroneous.foreach { case (tp, data) =>
          combinedResult += (tp -> data)
        }
        combinedResult.toMap
      }
    }
  }

  // Visible for Testing
  def handleAcknowledgements(acknowledgementData: mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]],
                             erroneous: mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData],
                             sharePartitionManagerInstance: SharePartitionManager,
                             authorizedTopics: Set[String],
                             groupId: String,
                             memberId: String): CompletableFuture[Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]] = {

    val erroneousTopicIdPartitions = validateAcknowledgementBatches(acknowledgementData, erroneous)
    erroneousTopicIdPartitions.foreach(tp => acknowledgementData.remove(tp))

    val interested = mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]]()
    val emptyAcknowledgements = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    acknowledgementData.foreach{
      case (topicIdPartition: TopicIdPartition, acknowledgeBatches: util.List[ShareAcknowledgementBatch]) =>
        if (!authorizedTopics.contains(topicIdPartition.topicPartition.topic))
          erroneous += topicIdPartition ->
            ShareAcknowledgeResponse.partitionResponse(topicIdPartition, Errors.TOPIC_AUTHORIZATION_FAILED)
        else if (!metadataCache.contains(topicIdPartition.topicPartition))
          erroneous += topicIdPartition ->
            ShareAcknowledgeResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        else if (isViewTopic(topicIdPartition.topicPartition.topic))
          // Symmetric to the SHARE_FETCH rejection above: a client that somehow obtained share
          // partition state for a view tp (it cannot, post-fix) must still be unable to acknowledge
          // it. Reject up front so SharePartitionManager.acknowledge never sees a view tp.
          erroneous += topicIdPartition ->
            ShareAcknowledgeResponse.partitionResponse(topicIdPartition, Errors.INVALID_TOPIC_EXCEPTION)
        else if (acknowledgeBatches.size() == 0)
          emptyAcknowledgements += topicIdPartition ->
            ShareAcknowledgeResponse.partitionResponse(topicIdPartition, Errors.NONE)
        else {
          interested += topicIdPartition -> acknowledgeBatches
        }
    }

    if (interested.isEmpty) {
      // Combine erroneous + emptyAcknowledgements in the short-circuit return too. Before
      // the view-aware rejection added empty-batch tps to `emptyAcknowledgements`, this
      // path was only reachable when ALL tps were erroneous and there was nothing else to
      // return. After the rejection, a request with a single view-tp ack (now erroneous)
      // and any number of non-view empty-batch acks can land here — and the empty-batch
      // responses must not be silently dropped, or the client retries forever waiting on
      // a response for tps it just acknowledged.
      val combined = mutable.Map.empty[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]
      erroneous.foreach { case (tp, data) => combined += (tp -> data) }
      emptyAcknowledgements.foreach { case (tp, data) => combined += (tp -> data) }
      CompletableFuture.completedFuture(combined.toMap)
    } else {
      // call the share partition manager to acknowledge messages in the share partition
      sharePartitionManagerInstance.acknowledge(
        memberId,
        groupId,
        interested.asJava
      ).thenApply{ result =>
        val combinedResult = mutable.Map.empty[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]
        result.asScala.foreach{ case (tp, data) =>
          combinedResult += (tp -> data)
        }
        erroneous.foreach{ case (tp, data) =>
          combinedResult += (tp -> data)
        }
        emptyAcknowledgements.foreach{ case (tp, data) =>
          combinedResult += (tp -> data)
        }
        combinedResult.toMap
      }
    }
  }

  def handleShareAcknowledgeRequest(request: RequestChannel.Request): Unit = {
    val shareAcknowledgeRequest = request.body[ShareAcknowledgeRequest]

    if (!isShareGroupProtocolEnabled) {
      requestHelper.sendMaybeThrottle(request,
        shareAcknowledgeRequest.getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, Errors.UNSUPPORTED_VERSION.exception))
      return
    }

    val groupId = shareAcknowledgeRequest.data.groupId

    // Share Acknowledge needs permission to perform READ action on the named group resource (groupId)
    if (!authHelper.authorize(request.context, READ, GROUP, groupId)) {
      requestHelper.sendMaybeThrottle(request,
        shareAcknowledgeRequest.getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, Errors.GROUP_AUTHORIZATION_FAILED.exception))
      return
    }

    val memberId = shareAcknowledgeRequest.data.memberId
    val shareSessionEpoch = shareAcknowledgeRequest.data.shareSessionEpoch
    val newReqMetadata: ShareRequestMetadata = new ShareRequestMetadata(Uuid.fromString(memberId), shareSessionEpoch)

    try {
      // Updating the cache for Share Session Handling
      sharePartitionManager.acknowledgeSessionUpdate(groupId, newReqMetadata)
    } catch {
      case e: Exception =>
        requestHelper.sendMaybeThrottle(request, shareAcknowledgeRequest.getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, e))
        return
    }

    val topicIdPartitionSeq: mutable.Set[TopicIdPartition] = mutable.Set()
    val shareAcknowledgeData = shareAcknowledgeRequest.data

    val topicIdNames = metadataCache.topicIdsToNames()

    shareAcknowledgeData.topics.forEach{ topic =>
      topic.partitions.forEach{ partition =>
        val topicIdPartition = new TopicIdPartition(topic.topicId,
          new TopicPartition(topicIdNames.get(topic.topicId), partition.partitionIndex))
        topicIdPartitionSeq += topicIdPartition
      }
    }

    val authorizedTopics = authHelper.filterByAuthorized(
      request.context,
      READ,
      TOPIC,
      topicIdPartitionSeq
    )(_.topicPartition.topic)

    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()
    val acknowledgementDataFromRequest = getAcknowledgeBatchesFromShareAcknowledgeRequest(shareAcknowledgeRequest, topicIdNames, erroneous)
    handleAcknowledgements(acknowledgementDataFromRequest, erroneous, sharePartitionManager, authorizedTopics, groupId, memberId)
      .handle[Unit] {(result, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, shareAcknowledgeRequest.getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, exception))
        } else {
          if (shareSessionEpoch == ShareRequestMetadata.FINAL_EPOCH) {
            sharePartitionManager.releaseSession(groupId, memberId).
              whenComplete{ (releaseAcquiredRecordsData, throwable) =>
                if (throwable != null) {
                  debug(s"Releasing share session close with correlation from client ${request.header.clientId}  " +
                    s"failed with error ${throwable.getMessage}")
                } else {
                  info(s"Releasing share session close $releaseAcquiredRecordsData succeeded")
                }
              }
          }
          requestHelper.sendMaybeThrottle(request, processShareAcknowledgeResponse(result, request))
        }
      }
  }

  def handleInitializeShareGroupStateRequest(request: RequestChannel.Request): Unit = {
    val initializeShareGroupStateRequest = request.body[InitializeShareGroupStateRequest]
    // TODO: Implement the InitializeShareGroupStateRequest handling
    requestHelper.sendMaybeThrottle(request, initializeShareGroupStateRequest.getErrorResponse(Errors.UNSUPPORTED_VERSION.exception))
    CompletableFuture.completedFuture[Unit](())
  }

  def handleReadShareGroupStateRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val readShareGroupStateRequest = request.body[ReadShareGroupStateRequest]

    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)

    shareCoordinator match {
      case None => requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        readShareGroupStateRequest.getErrorResponse(requestThrottleMs,
          new ApiException("Share coordinator is not enabled.")))
        CompletableFuture.completedFuture[Unit](())
      case Some(coordinator) => coordinator.readState(request.context, readShareGroupStateRequest.data)
        .handle[Unit] { (response, exception) =>
          if (exception != null) {
            requestHelper.sendMaybeThrottle(request, readShareGroupStateRequest.getErrorResponse(exception))
          } else {
            requestHelper.sendMaybeThrottle(request, new ReadShareGroupStateResponse(response))
          }
        }
    }
  }

  def handleWriteShareGroupStateRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val writeShareRequest = request.body[WriteShareGroupStateRequest]

    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)

    shareCoordinator match {
      case None => requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        writeShareRequest.getErrorResponse(requestThrottleMs,
          new ApiException("Share coordinator is not enabled.")))
        CompletableFuture.completedFuture[Unit](())
      case Some(coordinator) => coordinator.writeState(request.context, writeShareRequest.data)
        .handle[Unit] { (response, exception) =>
          if (exception != null) {
            requestHelper.sendMaybeThrottle(request, writeShareRequest.getErrorResponse(exception))
          } else {
            requestHelper.sendMaybeThrottle(request, new WriteShareGroupStateResponse(response))
          }
        }
    }
  }

  def handleDeleteShareGroupStateRequest(request: RequestChannel.Request): Unit = {
    val deleteShareGroupStateRequest = request.body[DeleteShareGroupStateRequest]
    // TODO: Implement the DeleteShareGroupStateRequest handling
    requestHelper.sendMaybeThrottle(request, deleteShareGroupStateRequest.getErrorResponse(Errors.UNSUPPORTED_VERSION.exception))
    CompletableFuture.completedFuture[Unit](())
  }

  def handleReadShareGroupStateSummaryRequest(request: RequestChannel.Request): Unit = {
    val readShareGroupStateSummaryRequest = request.body[ReadShareGroupStateSummaryRequest]
    // TODO: Implement the ReadShareGroupStateSummaryRequest handling
    requestHelper.sendMaybeThrottle(request, readShareGroupStateSummaryRequest.getErrorResponse(Errors.UNSUPPORTED_VERSION.exception))
    CompletableFuture.completedFuture[Unit](())
  }

  // Visible for Testing
  def getAcknowledgeBatchesFromShareAcknowledgeRequest(shareAcknowledgeRequest: ShareAcknowledgeRequest,
                                                       topicIdNames: util.Map[Uuid, String],
                                                       erroneous: mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]
                                                      ): mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]] = {
    val acknowledgeBatchesMap = mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]]()
    shareAcknowledgeRequest.data().topics().forEach{ topic =>
      if (!topicIdNames.containsKey(topic.topicId)) {
        topic.partitions.forEach{ case partition: ShareAcknowledgeRequestData.AcknowledgePartition =>
          val topicIdPartition = new TopicIdPartition(
            topic.topicId,
            new TopicPartition(null, partition.partitionIndex))
          erroneous +=
            topicIdPartition -> ShareAcknowledgeResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_ID)
        }
      } else {
        topic.partitions().forEach{ partition =>
          val topicIdPartition = new TopicIdPartition(
            topic.topicId(),
            new TopicPartition(topicIdNames.get(topic.topicId()), partition.partitionIndex())
          )
          val acknowledgeBatches = new util.ArrayList[ShareAcknowledgementBatch]()
          partition.acknowledgementBatches().forEach{ batch =>
            acknowledgeBatches.add(new ShareAcknowledgementBatch(
              batch.firstOffset(),
              batch.lastOffset(),
              batch.acknowledgeTypes()
            ))
          }
          acknowledgeBatchesMap += topicIdPartition -> acknowledgeBatches
        }
      }
    }
    acknowledgeBatchesMap
  }

  // Visible for Testing
  def getAcknowledgeBatchesFromShareFetchRequest(shareFetchRequest: ShareFetchRequest,
                                                 topicIdNames: util.Map[Uuid, String],
                                                 erroneous: mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]
                                                ): mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]] = {

    val acknowledgeBatchesMap = mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]]()
    shareFetchRequest.data().topics().forEach{ topic =>
      if (!topicIdNames.containsKey(topic.topicId)) {
        topic.partitions.forEach{ partition: ShareFetchRequestData.FetchPartition =>
          val topicIdPartition = new TopicIdPartition(
            topic.topicId,
            new TopicPartition(null, partition.partitionIndex))
          erroneous +=
            topicIdPartition -> ShareAcknowledgeResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_ID)
        }
      } else {
        topic.partitions().forEach { partition =>
          val topicIdPartition = new TopicIdPartition(
            topic.topicId(),
            new TopicPartition(topicIdNames.get(topic.topicId()), partition.partitionIndex())
          )
          val acknowledgeBatches = new util.ArrayList[ShareAcknowledgementBatch]()
          partition.acknowledgementBatches().forEach{ batch =>
            acknowledgeBatches.add(new ShareAcknowledgementBatch(
              batch.firstOffset(),
              batch.lastOffset(),
              batch.acknowledgeTypes()
            ))
          }
          acknowledgeBatchesMap += topicIdPartition -> acknowledgeBatches
        }
      }
    }
    acknowledgeBatchesMap
  }

  // the callback for processing a share acknowledge response, invoked before throttling
  def processShareAcknowledgeResponse(responseAcknowledgeData: Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData],
                                      request: RequestChannel.Request): ShareAcknowledgeResponse = {
    val partitions = new util.LinkedHashMap[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]
    val nodeEndpoints = new mutable.HashMap[Int, Node]
    responseAcknowledgeData.foreach{ case(tp, partitionData) =>
      partitionData.errorCode() match {
        case errCode if errCode == Errors.NOT_LEADER_OR_FOLLOWER.code | errCode == Errors.FENCED_LEADER_EPOCH.code =>
          val leaderNode = getCurrentLeader(tp.topicPartition(), request.context.listenerName)
          leaderNode.node.foreach { node =>
            nodeEndpoints.put(node.id(), node)
          }
          partitionData.currentLeader()
            .setLeaderId(leaderNode.leaderId)
            .setLeaderEpoch(leaderNode.leaderEpoch)
        case _ =>
      }
      partitions.put(tp, partitionData)
    }

    ShareAcknowledgeResponse.of(
      Errors.NONE,
      0,
      partitions,
      nodeEndpoints.values.toList.asJava
    )
  }

  // Visible for Testing
  def validateAcknowledgementBatches(acknowledgementDataFromRequest: mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]],
                                     erroneous: mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]
                                    ): mutable.Set[TopicIdPartition] = {
    val erroneousTopicIdPartitions: mutable.Set[TopicIdPartition] = mutable.Set.empty[TopicIdPartition]

    acknowledgementDataFromRequest.foreach { case (tp: TopicIdPartition, acknowledgeBatches: util.List[ShareAcknowledgementBatch]) =>
      var prevEndOffset = -1L
      var isErroneous = false
      acknowledgeBatches.forEach { batch =>
        if (!isErroneous) {
          if (batch.firstOffset > batch.lastOffset) {
            erroneous += tp -> ShareAcknowledgeResponse.partitionResponse(tp, Errors.INVALID_REQUEST)
            erroneousTopicIdPartitions.add(tp)
            isErroneous = true
          } else if (batch.firstOffset < prevEndOffset) {
            erroneous += tp -> ShareAcknowledgeResponse.partitionResponse(tp, Errors.INVALID_REQUEST)
            erroneousTopicIdPartitions.add(tp)
            isErroneous = true
          } else if (batch.acknowledgeTypes == null || batch.acknowledgeTypes.isEmpty) {
            erroneous += tp -> ShareAcknowledgeResponse.partitionResponse(tp, Errors.INVALID_REQUEST)
            erroneousTopicIdPartitions.add(tp)
            isErroneous = true
          } else if (batch.acknowledgeTypes.size() > 1 && batch.lastOffset - batch.firstOffset != batch.acknowledgeTypes.size() - 1) {
            erroneous += tp -> ShareAcknowledgeResponse.partitionResponse(tp, Errors.INVALID_REQUEST)
            erroneousTopicIdPartitions.add(tp)
            isErroneous = true
          } else if (batch.acknowledgeTypes.stream().anyMatch(ackType => ackType < 0 || ackType > 3)) {
            erroneous += tp -> ShareAcknowledgeResponse.partitionResponse(tp, Errors.INVALID_REQUEST)
            erroneousTopicIdPartitions.add(tp)
            isErroneous = true
          } else {
            prevEndOffset = batch.lastOffset
          }
        }
      }
    }

    erroneousTopicIdPartitions
  }

  // the callback for processing a share fetch response.
  private def processShareFetchResponse(responsePartitionData: Map[TopicIdPartition, ShareFetchResponseData.PartitionData],
                                        request: RequestChannel.Request,
                                        topicIdNames: util.Map[Uuid, String],
                                        shareFetchContext: ShareFetchContext): ShareFetchResponse = {
    val clientId = request.header.clientId
    val versionId = request.header.apiVersion
    val shareFetchRequest = request.body[ShareFetchRequest]
    val groupId = shareFetchRequest.data.groupId
    val memberId = shareFetchRequest.data.memberId

    val nodeEndpoints = new mutable.HashMap[Int, Node]
    responsePartitionData.foreach { case(tp, partitionData) =>
      partitionData.errorCode match {
        case errCode if errCode == Errors.NOT_LEADER_OR_FOLLOWER.code | errCode == Errors.FENCED_LEADER_EPOCH.code =>
          val leaderNode = getCurrentLeader(tp.topicPartition, request.context.listenerName)
          leaderNode.node.foreach { node =>
            nodeEndpoints.put(node.id, node)
          }
          partitionData.currentLeader
            .setLeaderId(leaderNode.leaderId)
            .setLeaderEpoch(leaderNode.leaderEpoch)
        case _ =>
      }
    }
    val partitions = new util.LinkedHashMap[TopicIdPartition, ShareFetchResponseData.PartitionData](responsePartitionData.asJava)

    var shareFetchResponse: ShareFetchResponse = null

    def createResponse(throttleTimeMs: Int): ShareFetchResponse = {
      val responseData = new util.LinkedHashMap[TopicIdPartition, ShareFetchResponseData.PartitionData]
      shareFetchResponse.data.responses.forEach { topicResponse =>
        topicResponse.partitions.forEach { partitionData =>
          val tp = new TopicIdPartition(topicResponse.topicId, new TopicPartition(topicIdNames.get(topicResponse.topicId),
            partitionData.partitionIndex))
          val error = Errors.forCode(partitionData.errorCode)
          if (error != Errors.NONE)
            debug(s"Share Fetch request with correlation id ${request.header.correlationId} from client $clientId " +
              s"on partition $tp failed due to ${error.exceptionName}")
          responseData.put(tp, getResponsePartitionData(tp, partitionData))
        }
      }

      // Prepare share fetch response
      val response =
        ShareFetchResponse.of(shareFetchResponse.error, throttleTimeMs, responseData, nodeEndpoints.values.toList.asJava)
      // record the bytes out metrics only when the response is being sent.
      response.data.responses.forEach { topicResponse =>
        topicResponse.partitions.forEach { data =>
          // If the topic name was not known, we will have no bytes out.
          if (topicResponse.topicId != null) {
            val tp = new TopicIdPartition(topicResponse.topicId, new TopicPartition(topicIdNames.get(topicResponse.topicId), data.partitionIndex))
            brokerTopicStats.updateBytesOut(tp.topic, false, false, ShareFetchResponse.recordsSize(data))
          }
        }
      }
      response
    }

    // Share Fetch size used to determine throttle time is calculated here.
    // This may be slightly different from the actual response size.
    //
    // Record both bandwidth and request quota-specific values and throttle by muting the channel if any of the
    // quotas have been violated. If both quotas have been violated, use the max throttle time between the two
    // quotas. When throttled, we unrecord the recorded bandwidth quota value.
    val responseSize = shareFetchContext.responseSize(partitions, versionId)
    val timeMs = time.milliseconds()
    val requestThrottleTimeMs = quotas.request.maybeRecordAndGetThrottleTimeMs(request, timeMs)
    val bandwidthThrottleTimeMs = quotas.fetch.maybeRecordAndGetThrottleTimeMs(request, responseSize, timeMs)

    val maxThrottleTimeMs = math.max(bandwidthThrottleTimeMs, requestThrottleTimeMs)
    if (maxThrottleTimeMs > 0) {
      request.apiThrottleTimeMs = maxThrottleTimeMs
      // Even if we need to throttle for request quota violation, we should "unrecord" the already recorded value
      // from the fetch quota because we are going to return an empty response.
      quotas.fetch.unrecordQuotaSensor(request, responseSize, timeMs)
      if (bandwidthThrottleTimeMs > requestThrottleTimeMs) {
        requestHelper.throttle(quotas.fetch, request, bandwidthThrottleTimeMs)
      } else {
        requestHelper.throttle(quotas.request, request, requestThrottleTimeMs)
      }
      // If throttling is required, return an empty response.
      shareFetchResponse = shareFetchContext.throttleResponse(maxThrottleTimeMs)
    } else {
      // Get the actual response. This will update the fetch context.
      shareFetchResponse = shareFetchContext.updateAndGenerateResponseData(groupId, Uuid.fromString(memberId), partitions)
      val responsePartitionsSize = shareFetchResponse.data.responses.stream().mapToInt(_.partitions.size()).sum()
      trace(s"Sending Share Fetch response with partitions size=$responsePartitionsSize")
    }
    createResponse(maxThrottleTimeMs)
  }

  private def getResponsePartitionData(tp: TopicIdPartition,
                                       partitionData: ShareFetchResponseData.PartitionData): ShareFetchResponseData.PartitionData = {
    val records = ShareFetchResponse.recordsOrFail(partitionData)
    new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(tp.partition)
      .setErrorCode(Errors.forCode(partitionData.errorCode).code)
      .setRecords(records)
      .setAcquiredRecords(partitionData.acquiredRecords)
      .setCurrentLeader(partitionData.currentLeader)
  }

  private def isShareGroupProtocolEnabled: Boolean = {
    groupCoordinator.isNewGroupCoordinator && config.shareGroupConfig.isShareGroupEnabled
  }

  private def updateRecordConversionStats(request: RequestChannel.Request,
                                          tp: TopicPartition,
                                          conversionStats: RecordValidationStats): Unit = {
    val conversionCount = conversionStats.numRecordsConverted
    if (conversionCount > 0) {
      request.header.apiKey match {
        case ApiKeys.PRODUCE =>
          brokerTopicStats.topicStats(tp.topic).produceMessageConversionsRate.mark(conversionCount)
          brokerTopicStats.allTopicsStats.produceMessageConversionsRate.mark(conversionCount)
        case ApiKeys.FETCH =>
          brokerTopicStats.topicStats(tp.topic).fetchMessageConversionsRate.mark(conversionCount)
          brokerTopicStats.allTopicsStats.fetchMessageConversionsRate.mark(conversionCount)
        case _ =>
          throw new IllegalStateException("Message conversion info is recorded only for Produce/Fetch requests")
      }
      request.messageConversionsTimeNanos = conversionStats.conversionTimeNanos
    }
    request.temporaryMemoryBytes = conversionStats.temporaryMemoryBytes
  }
}

object KafkaApis {
  // Traffic from both in-sync and out of sync replicas are accounted for in replication quota to ensure total replication
  // traffic doesn't exceed quota.
  // TODO: remove resolvedResponseData method when sizeOf can take a data object.
  private[server] def sizeOfThrottledPartitions(versionId: Short,
                                                unconvertedResponse: FetchResponse,
                                                quota: ReplicationQuotaManager): Int = {
    val responseData = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    unconvertedResponse.data.responses().forEach(topicResponse =>
      topicResponse.partitions().forEach(partition =>
        responseData.put(new TopicIdPartition(topicResponse.topicId, new TopicPartition(topicResponse.topic(), partition.partitionIndex)), partition)))
    FetchResponse.sizeOf(versionId, responseData.entrySet
      .iterator.asScala.filter(element => element.getKey.topicPartition.topic != null && quota.isThrottled(element.getKey.topicPartition)).asJava)
  }
}
