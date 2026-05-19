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

import java.{lang, util}
import java.nio.ByteBuffer
import java.util.{Collections, OptionalLong}
import java.util.Map.Entry
import java.util.concurrent.CompletableFuture
import java.util.function.Consumer
import kafka.network.RequestChannel
import kafka.raft.RaftManager
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.logger.RuntimeLoggerManager
import kafka.server.metadata.KRaftMetadataCache
import kafka.utils.Logging
import org.apache.kafka.clients.admin.{AlterConfigOp, EndpointType}
import org.apache.kafka.common.Uuid.ZERO_UUID
import org.apache.kafka.common.acl.AclOperation.{ALTER, ALTER_CONFIGS, CLUSTER_ACTION, CREATE, CREATE_TOKENS, DELETE, DESCRIBE, DESCRIBE_CONFIGS}
import org.apache.kafka.common.acl.{AclBinding, AclBindingFilter}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.{ApiException, ClusterAuthorizationException, InvalidRequestException, TopicDeletionDisabledException, UnsupportedVersionException}
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.AlterConfigsResponseData.{AlterConfigsResourceResponse => OldAlterConfigsResourceResponse}
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult
import org.apache.kafka.common.message.DeleteTopicsResponseData.{DeletableTopicResult, DeletableTopicResultCollection}
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse
import org.apache.kafka.common.message.{CreateTopicsRequestData, _}
import org.apache.kafka.common.protocol.Errors._
import org.apache.kafka.common.protocol.{ApiKeys, ApiMessage, Errors}
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.common.resource.ResourceType.{CLUSTER, GROUP, TOPIC, USER}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.Uuid
import org.apache.kafka.controller.ControllerRequestContext.requestTimeoutMsToDeadlineNs
import org.apache.kafka.controller.{Controller, ControllerRequestContext}
import org.apache.kafka.image.publisher.ControllerRegistrationsPublisher
import org.apache.kafka.metadata.{BrokerHeartbeatReply, BrokerRegistrationReply}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.{ApiMessageAndVersion, RequestLocal}
import org.apache.kafka.server.tenant.{TenantConfig, TenantNamespace}

import scala.jdk.CollectionConverters._


/**
 * Request handler for Controller APIs
 */
class ControllerApis(
  val requestChannel: RequestChannel,
  val authorizer: Option[Authorizer],
  val quotas: QuotaManagers,
  val time: Time,
  val controller: Controller,
  val raftManager: RaftManager[ApiMessageAndVersion],
  val config: KafkaConfig,
  val clusterId: String,
  val registrationsPublisher: ControllerRegistrationsPublisher,
  val apiVersionManager: ApiVersionManager,
  val metadataCache: KRaftMetadataCache,
  val tenantConfig: TenantConfig = TenantConfig.empty()
) extends ApiRequestHandler with Logging {

  this.logIdent = s"[ControllerApis nodeId=${config.nodeId}] "
  val authHelper = new AuthHelper(authorizer)
  val configHelper = new ConfigHelper(metadataCache, config, metadataCache)
  val requestHelper = new RequestHandlerHelper(requestChannel, quotas, time)
  val runtimeLoggerManager = new RuntimeLoggerManager(config.nodeId, logger.underlying)
  private val aclApis = new AclApis(authHelper, authorizer, requestHelper, "controller", config)

  def isClosed: Boolean = aclApis.isClosed

  def close(): Unit = aclApis.close()

  override def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    try {
      val handlerFuture: CompletableFuture[Unit] = request.header.apiKey match {
        case ApiKeys.FETCH => handleFetch(request)
        case ApiKeys.FETCH_SNAPSHOT => handleFetchSnapshot(request)
        case ApiKeys.CREATE_TOPICS => handleCreateTopics(request)
        case ApiKeys.DELETE_TOPICS => handleDeleteTopics(request)
        case ApiKeys.API_VERSIONS => handleApiVersionsRequest(request)
        case ApiKeys.ALTER_CONFIGS => handleLegacyAlterConfigs(request)
        case ApiKeys.VOTE => handleVote(request)
        case ApiKeys.BEGIN_QUORUM_EPOCH => handleBeginQuorumEpoch(request)
        case ApiKeys.END_QUORUM_EPOCH => handleEndQuorumEpoch(request)
        case ApiKeys.DESCRIBE_QUORUM => handleDescribeQuorum(request)
        case ApiKeys.ALTER_PARTITION => handleAlterPartitionRequest(request)
        case ApiKeys.BROKER_REGISTRATION => handleBrokerRegistration(request)
        case ApiKeys.BROKER_HEARTBEAT => handleBrokerHeartBeatRequest(request)
        case ApiKeys.UNREGISTER_BROKER => handleUnregisterBroker(request)
        case ApiKeys.ALTER_CLIENT_QUOTAS => handleAlterClientQuotas(request)
        case ApiKeys.INCREMENTAL_ALTER_CONFIGS => handleIncrementalAlterConfigs(request)
        case ApiKeys.ALTER_PARTITION_REASSIGNMENTS => handleAlterPartitionReassignments(request)
        case ApiKeys.LIST_PARTITION_REASSIGNMENTS => handleListPartitionReassignments(request)
        case ApiKeys.ALTER_USER_SCRAM_CREDENTIALS => handleAlterUserScramCredentials(request)
        case ApiKeys.CREATE_DELEGATION_TOKEN => handleCreateDelegationTokenRequest(request)
        case ApiKeys.RENEW_DELEGATION_TOKEN => handleRenewDelegationTokenRequest(request)
        case ApiKeys.EXPIRE_DELEGATION_TOKEN => handleExpireDelegationTokenRequest(request)
        case ApiKeys.ENVELOPE => handleEnvelopeRequest(request, requestLocal)
        case ApiKeys.SASL_HANDSHAKE => handleSaslHandshakeRequest(request)
        case ApiKeys.SASL_AUTHENTICATE => handleSaslAuthenticateRequest(request)
        case ApiKeys.ALLOCATE_PRODUCER_IDS => handleAllocateProducerIdsRequest(request)
        case ApiKeys.CREATE_PARTITIONS => handleCreatePartitions(request)
        case ApiKeys.DESCRIBE_CONFIGS => handleDescribeConfigsRequest(request)
        case ApiKeys.DESCRIBE_ACLS => handleDescribeAclsRequest(request)
        case ApiKeys.CREATE_ACLS => handleCreateAclsRequest(request)
        case ApiKeys.DELETE_ACLS => handleDeleteAclsRequest(request)
        case ApiKeys.ELECT_LEADERS => handleElectLeaders(request)
        case ApiKeys.UPDATE_FEATURES => handleUpdateFeatures(request)
        case ApiKeys.DESCRIBE_CLUSTER => handleDescribeCluster(request)
        case ApiKeys.CONTROLLER_REGISTRATION => handleControllerRegistration(request)
        case ApiKeys.ASSIGN_REPLICAS_TO_DIRS => handleAssignReplicasToDirs(request)
        case ApiKeys.ADD_RAFT_VOTER => handleAddRaftVoter(request)
        case ApiKeys.REMOVE_RAFT_VOTER => handleRemoveRaftVoter(request)
        case ApiKeys.UPDATE_RAFT_VOTER => handleUpdateRaftVoter(request)
        case _ => throw new ApiException(s"Unsupported ApiKey ${request.context.header.apiKey}")
      }

      // This catches exceptions in the future and subsequent completion stages returned by the request handlers.
      handlerFuture.whenComplete { (_, exception) =>
        if (exception != null) {
          // CompletionException does not include the stack frames in its "cause" exception, so we need to
          // log the original exception here
          error(s"Unexpected error handling request ${request.requestDesc(true)} " +
            s"with context ${request.context}", exception)
          requestHelper.handleError(request, exception)
        }
      }
    } catch {
      case e: FatalExitError => throw e
      case t: Throwable =>
        // This catches exceptions in the blocking parts of the request handlers
        error(s"Unexpected error handling request ${request.requestDesc(true)} " +
          s"with context ${request.context}", t)
        requestHelper.handleError(request, t)
    } finally {
      // Only record local completion time if it is unset.
      if (request.apiLocalCompleteTimeNanos < 0) {
        request.apiLocalCompleteTimeNanos = time.nanoseconds
      }
    }
  }

  def handleEnvelopeRequest(request: RequestChannel.Request, requestLocal: RequestLocal): CompletableFuture[Unit] = {
    if (!authHelper.authorize(request.context, CLUSTER_ACTION, CLUSTER, CLUSTER_NAME)) {
      requestHelper.sendErrorResponseMaybeThrottle(request, new ClusterAuthorizationException(
        s"Principal ${request.context.principal} does not have required CLUSTER_ACTION for envelope"))
    } else {
      EnvelopeUtils.handleEnvelopeRequest(request, requestChannel.metrics, handle(_, requestLocal))
    }
    CompletableFuture.completedFuture[Unit](())
  }

  def handleSaslHandshakeRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val responseData = new SaslHandshakeResponseData().setErrorCode(ILLEGAL_SASL_STATE.code)
    requestHelper.sendResponseMaybeThrottle(request, _ => new SaslHandshakeResponse(responseData))
    CompletableFuture.completedFuture[Unit](())
  }

  def handleSaslAuthenticateRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val responseData = new SaslAuthenticateResponseData()
      .setErrorCode(ILLEGAL_SASL_STATE.code)
      .setErrorMessage("SaslAuthenticate request received after successful authentication")
    requestHelper.sendResponseMaybeThrottle(request, _ => new SaslAuthenticateResponse(responseData))
    CompletableFuture.completedFuture[Unit](())
  }

  def handleFetch(request: RequestChannel.Request): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    handleRaftRequest(request, response => new FetchResponse(response.asInstanceOf[FetchResponseData]))
  }

  def handleFetchSnapshot(request: RequestChannel.Request): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    handleRaftRequest(request, response => new FetchSnapshotResponse(response.asInstanceOf[FetchSnapshotResponseData]))
  }

  private def handleDeleteTopics(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val deleteTopicsRequest = request.body[DeleteTopicsRequest]
    val controllerMutationQuota = quotas.controllerMutation.newQuotaFor(request, strictSinceVersion = 5)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      requestTimeoutMsToDeadlineNs(time, deleteTopicsRequest.data.timeoutMs),
      controllerMutationQuotaRecorderFor(controllerMutationQuota))
    val future = deleteTopics(context,
      deleteTopicsRequest.data,
      request.context.apiVersion,
      authHelper.authorize(request.context, DELETE, CLUSTER, CLUSTER_NAME, logIfDenied = false),
      names => authHelper.filterByAuthorized(request.context, DESCRIBE, TOPIC, names)(n => n),
      names => authHelper.filterByAuthorized(request.context, DELETE, TOPIC, names)(n => n))
    future.handle[Unit] { (results, exception) =>
      val response = if (exception != null) {
        deleteTopicsRequest.getErrorResponse(exception)
      } else {
        val responseData = new DeleteTopicsResponseData()
          .setResponses(new DeletableTopicResultCollection(results.iterator))
        new DeleteTopicsResponse(responseData)
      }
      requestHelper.sendResponseMaybeThrottleWithControllerQuota(controllerMutationQuota, request, response)
    }
  }

  def deleteTopics(
    context: ControllerRequestContext,
    request: DeleteTopicsRequestData,
    apiVersion: Int,
    hasClusterAuth: Boolean,
    getDescribableTopics: Iterable[String] => Set[String],
    getDeletableTopics: Iterable[String] => Set[String]
  ): CompletableFuture[util.List[DeletableTopicResult]] = {
    // Check if topic deletion is enabled at all.
    if (!config.deleteTopicEnable) {
      if (apiVersion < 3) {
        throw new InvalidRequestException("Topic deletion is disabled.")
      } else {
        throw new TopicDeletionDisabledException()
      }
    }
    // The first step is to load up the names and IDs that have been provided by the
    // request.  This is a bit messy because we support multiple ways of referring to
    // topics (both by name and by id) and because we need to check for duplicates or
    // other invalid inputs.
    val responses = new util.ArrayList[DeletableTopicResult]
    def appendResponse(name: String, id: Uuid, error: ApiError): Unit = {
      responses.add(new DeletableTopicResult().
        setName(name).
        setTopicId(id).
        setErrorCode(error.error.code).
        setErrorMessage(error.message))
    }
    val providedNames = new util.HashSet[String]
    val duplicateProvidedNames = new util.HashSet[String]
    val providedIds = new util.HashSet[Uuid]
    val duplicateProvidedIds = new util.HashSet[Uuid]
    def addProvidedName(name: String): Unit = {
      if (duplicateProvidedNames.contains(name) || !providedNames.add(name)) {
        duplicateProvidedNames.add(name)
        providedNames.remove(name)
      }
    }
    // Outside-in pollution guard. A caller reaching ControllerApis directly
    // via `bootstrap.controllers` bypasses KafkaApis.handleDeleteTopicsRequest
    // and its name/UUID scrub — they could submit `acme.orders` as a literal
    // name or a tenant topic UUID, and the controller would delete the topic
    // out from under tenant acme. Refuse any tenant-namespaced name (or
    // UUID-resolved name; see below) before forwarding. Principal-aware via
    // `isForeignTenantNamespace`: the legitimate forwarded tenant flow
    // (broker rewrote `orders` → `acme.orders`, envelope carries
    // `__tenant_acme.alice`) passes through untouched.
    val callerTenant = callerTenantFromPrincipal(context.principal.getName)
    def refuseIfTenantPolluting(name: String): Boolean = {
      if (isForeignTenantNamespace(name, callerTenant)) {
        appendResponse(name, ZERO_UUID, new ApiError(INVALID_TOPIC_EXCEPTION,
          "Topic name '" + name + "' is reserved (tenant namespace prefix)"))
        true
      } else false
    }
    request.topicNames.forEach { name =>
      if (!refuseIfTenantPolluting(name)) addProvidedName(name)
    }
    request.topics.forEach {
      topic => if (topic.name == null) {
        if (topic.topicId.equals(ZERO_UUID)) {
          appendResponse(null, ZERO_UUID, new ApiError(INVALID_REQUEST,
            "Neither topic name nor id were specified."))
        } else if (duplicateProvidedIds.contains(topic.topicId) || !providedIds.add(topic.topicId)) {
          duplicateProvidedIds.add(topic.topicId)
          providedIds.remove(topic.topicId)
        }
      } else {
        if (topic.topicId.equals(ZERO_UUID)) {
          if (!refuseIfTenantPolluting(topic.name)) addProvidedName(topic.name)
        } else {
          appendResponse(topic.name, topic.topicId, new ApiError(INVALID_REQUEST,
            "You may not specify both topic name and topic id."))
        }
      }
    }
    // Create error responses for duplicates.
    duplicateProvidedNames.forEach(name => appendResponse(name, ZERO_UUID,
      new ApiError(INVALID_REQUEST, "Duplicate topic name.")))
    duplicateProvidedIds.forEach(id => appendResponse(null, id,
      new ApiError(INVALID_REQUEST, "Duplicate topic id.")))
    // At this point we have all the valid names and IDs that have been provided.
    // However, the Authorizer needs topic names as inputs, not topic IDs.  So
    // we need to resolve all IDs to names.
    val toAuthenticate = new util.HashSet[String]
    toAuthenticate.addAll(providedNames)
    val idToName = new util.HashMap[Uuid, String]
    controller.findTopicNames(context, providedIds).thenCompose { topicNames =>
      topicNames.forEach { (id, nameOrError) =>
        if (nameOrError.isError) {
          appendResponse(null, id, nameOrError.error())
        } else {
          val resolved = nameOrError.result()
          // UUID resolution can surface a tenant-namespaced name even when
          // the caller supplied only the topic id. The bootstrap.controllers
          // bypass exposes this: a cluster-acting principal that knows or
          // guesses a tenant topic's UUID could otherwise delete it through
          // this path. Refuse the resolved name. Principal-aware so the
          // legitimate forwarded tenant delete (envelope carries
          // `__tenant_acme.alice`) passes. Note the response surfaces the
          // ACTUAL `id` (not ZERO_UUID like the named path) so the caller
          // sees which UUID was rejected.
          if (isForeignTenantNamespace(resolved, callerTenant)) {
            appendResponse(resolved, id, new ApiError(INVALID_TOPIC_EXCEPTION,
              "Topic name '" + resolved + "' is reserved (tenant namespace prefix)"))
          } else {
            toAuthenticate.add(resolved)
            idToName.put(id, resolved)
          }
        }
      }
      // Get the list of deletable topics (those we can delete) and the list of describable
      // topics.
      val topicsToAuthenticate = toAuthenticate.asScala
      val (describable, deletable) = if (hasClusterAuth) {
        (topicsToAuthenticate.toSet, topicsToAuthenticate.toSet)
      } else {
        (getDescribableTopics(topicsToAuthenticate), getDeletableTopics(topicsToAuthenticate))
      }
      // For each topic that was provided by ID, check if authentication failed.
      // If so, remove it from the idToName map and create an error response for it.
      val iterator = idToName.entrySet().iterator()
      while (iterator.hasNext) {
        val entry = iterator.next()
        val id = entry.getKey
        val name = entry.getValue
        if (!deletable.contains(name)) {
          if (describable.contains(name)) {
            appendResponse(name, id, new ApiError(TOPIC_AUTHORIZATION_FAILED))
          } else {
            appendResponse(null, id, new ApiError(TOPIC_AUTHORIZATION_FAILED))
          }
          iterator.remove()
        }
      }
      // For each topic that was provided by name, check if authentication failed.
      // If so, create an error response for it. Otherwise, add it to the idToName map.
      controller.findTopicIds(context, providedNames).thenCompose { topicIds =>
        topicIds.forEach { (name, idOrError) =>
          if (!describable.contains(name)) {
            appendResponse(name, ZERO_UUID, new ApiError(TOPIC_AUTHORIZATION_FAILED))
          } else if (idOrError.isError) {
            appendResponse(name, ZERO_UUID, idOrError.error)
          } else if (deletable.contains(name)) {
            val id = idOrError.result()
            if (duplicateProvidedIds.contains(id) || idToName.put(id, name) != null) {
              // This is kind of a weird case: what if we supply topic ID X and also a name
              // that maps to ID X?  In that case, _if authorization succeeds_, we end up
              // here.  If authorization doesn't succeed, we refrain from commenting on the
              // situation since it would reveal topic ID mappings.
              duplicateProvidedIds.add(id)
              idToName.remove(id)
              appendResponse(name, id, new ApiError(INVALID_REQUEST,
                "The provided topic name maps to an ID that was already supplied."))
            }
          } else {
            appendResponse(name, ZERO_UUID, new ApiError(TOPIC_AUTHORIZATION_FAILED))
          }
        }
        // Finally, the idToName map contains all the topics that we are authorized to delete.
        // Perform the deletion and create responses for each one.
        controller.deleteTopics(context, idToName.keySet).thenApply { idToError =>
          idToError.forEach { (id, error) =>
            appendResponse(idToName.get(id), id, error)
          }
          // Shuffle the responses so that users can not use patterns in their positions to
          // distinguish between absent topics and topics we are not permitted to see.
          Collections.shuffle(responses)
          responses
        }
      }
    }
  }

  private def handleCreateTopics(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val createTopicsRequest = request.body[CreateTopicsRequest]
    val controllerMutationQuota = quotas.controllerMutation.newQuotaFor(request, strictSinceVersion = 6)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      requestTimeoutMsToDeadlineNs(time, createTopicsRequest.data.timeoutMs),
      controllerMutationQuotaRecorderFor(controllerMutationQuota))
    // Outside-in pollution guard. A caller reaching ControllerApis directly via
    // `bootstrap.controllers` bypasses KafkaApis.handleCreateTopicsRequest and
    // its name scrub. Refuse any topic whose name lies in a reserved-tenant
    // namespace before the request is handed to the controller, so the
    // metadata log never records a polluting create. Each refused entry
    // surfaces as INVALID_TOPIC_EXCEPTION (the same shape the broker emits at
    // KafkaApis line 347) so a privileged caller sees one refusal per
    // offender instead of a silently-truncated request.
    //
    // Principal-aware: a legitimate forwarded tenant request reaches the
    // controller as `acme.foo` from principal `__tenant_acme.alice`. The
    // broker has already rewritten the name and validated the listener/
    // principal binding (#86, #110), so we use `isForeignTenantNamespace`
    // here — refuse `acme.foo` from `User:admin` (cluster-wide pollution)
    // or from `__tenant_evil.alice` (cross-tenant pollution) but pass
    // through when the caller's own tenant owns the prefix.
    val callerTenant = callerTenantFromPrincipal(request.context.principal.getName)
    val pollutionRejected = new util.ArrayList[CreatableTopicResult]()
    val topicsIter = createTopicsRequest.data.topics().iterator()
    while (topicsIter.hasNext) {
      val t = topicsIter.next()
      if (isForeignTenantNamespace(t.name(), callerTenant)) {
        pollutionRejected.add(new CreatableTopicResult()
          .setName(t.name())
          .setErrorCode(INVALID_TOPIC_EXCEPTION.code)
          .setErrorMessage("Topic name '" + t.name() + "' is reserved (tenant namespace prefix)"))
        topicsIter.remove()
      }
    }
    if (createTopicsRequest.data.topics().isEmpty && !pollutionRejected.isEmpty) {
      val response = new CreateTopicsResponseData()
      val responses = new CreateTopicsResponseData.CreatableTopicResultCollection(pollutionRejected.size)
      pollutionRejected.forEach(r => responses.add(r))
      response.setTopics(responses)
      requestHelper.sendResponseMaybeThrottleWithControllerQuota(controllerMutationQuota, request,
        new CreateTopicsResponse(response))
      return CompletableFuture.completedFuture(())
    }
    val future = createTopics(context,
        createTopicsRequest.data,
        authHelper.authorize(request.context, CREATE, CLUSTER, CLUSTER_NAME, logIfDenied = false),
        names => authHelper.filterByAuthorized(request.context, CREATE, TOPIC, names)(identity),
        names => authHelper.filterByAuthorized(request.context, DESCRIBE_CONFIGS, TOPIC,
            names, logIfDenied = false)(identity))
    future.handle[Unit] { (result, exception) =>
      val response = if (exception != null) {
        createTopicsRequest.getErrorResponse(exception)
      } else {
        if (!pollutionRejected.isEmpty) pollutionRejected.forEach(r => result.topics().add(r))
        new CreateTopicsResponse(result)
      }
      requestHelper.sendResponseMaybeThrottleWithControllerQuota(controllerMutationQuota, request, response)
    }
  }

  private def controllerMutationQuotaRecorderFor(controllerMutationQuota: ControllerMutationQuota) = {
    new Consumer[lang.Integer]() {
      override def accept(permits: lang.Integer): Unit = controllerMutationQuota.record(permits.doubleValue())
    }
  }

  def createTopics(
    context: ControllerRequestContext,
    request: CreateTopicsRequestData,
    hasClusterAuth: Boolean,
    getCreatableTopics: Iterable[String] => Set[String],
    getDescribableTopics: Iterable[String] => Set[String]
  ): CompletableFuture[CreateTopicsResponseData] = {
    val topicNames = new util.HashSet[String]()
    val duplicateTopicNames = new util.HashSet[String]()
    request.topics().forEach { topicData =>
      if (!duplicateTopicNames.contains(topicData.name())) {
        if (!topicNames.add(topicData.name())) {
          topicNames.remove(topicData.name())
          duplicateTopicNames.add(topicData.name())
        }
      }
    }

    val allowedTopicNames = topicNames.asScala.diff(Set(Topic.CLUSTER_METADATA_TOPIC_NAME))

    val authorizedTopicNames = if (hasClusterAuth) {
      allowedTopicNames
    } else {
      getCreatableTopics.apply(allowedTopicNames)
    }
    val describableTopicNames = getDescribableTopics.apply(allowedTopicNames).asJava
    val effectiveRequest = request.duplicate()
    val iterator = effectiveRequest.topics().iterator()
    while (iterator.hasNext) {
      val creatableTopic = iterator.next()
      if (duplicateTopicNames.contains(creatableTopic.name()) ||
          !authorizedTopicNames.contains(creatableTopic.name())) {
        iterator.remove()
      }
    }
    controller.createTopics(context, effectiveRequest, describableTopicNames).thenApply { response =>
      duplicateTopicNames.forEach { name =>
        response.topics().add(new CreatableTopicResult().
          setName(name).
          setErrorCode(INVALID_REQUEST.code).
          setErrorMessage("Duplicate topic name."))
      }
      topicNames.forEach { name =>
        if (name == Topic.CLUSTER_METADATA_TOPIC_NAME) {
          response.topics().add(new CreatableTopicResult().
            setName(name).
            setErrorCode(INVALID_REQUEST.code).
            setErrorMessage(s"Creation of internal topic ${Topic.CLUSTER_METADATA_TOPIC_NAME} is prohibited."))
        } else if (!authorizedTopicNames.contains(name)) {
          response.topics().add(new CreatableTopicResult().
            setName(name).
            setErrorCode(TOPIC_AUTHORIZATION_FAILED.code).
            setErrorMessage("Authorization failed."))
        }
      }
      response
    }
  }

  def handleApiVersionsRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    // Note that controller returns its full list of supported ApiKeys and versions regardless of current
    // authentication state (e.g., before SASL authentication on an SASL listener, do note that no
    // Kafka protocol requests may take place on an SSL listener before the SSL handshake is finished).
    // If this is considered to leak information about the controller version a workaround is to use SSL
    // with client authentication which is performed at an earlier stage of the connection where the
    // ApiVersionRequest is not available.
    val apiVersionRequest = request.body[ApiVersionsRequest]
    if (apiVersionRequest.hasUnsupportedRequestVersion) {
      requestHelper.sendResponseMaybeThrottle(request,
        requestThrottleMs => apiVersionRequest.getErrorResponse(requestThrottleMs, UNSUPPORTED_VERSION.exception))
    } else if (!apiVersionRequest.isValid) {
      requestHelper.sendResponseMaybeThrottle(request,
        requestThrottleMs => apiVersionRequest.getErrorResponse(requestThrottleMs, INVALID_REQUEST.exception))
    } else {
      requestHelper.sendResponseMaybeThrottle(request,
        requestThrottleMs => apiVersionManager.apiVersionResponse(requestThrottleMs, request.header.apiVersion() < 4))
    }
    CompletableFuture.completedFuture[Unit](())
  }

  private def authorizeAlterResource(requestContext: RequestContext,
                                     resource: ConfigResource): ApiError = {
    resource.`type` match {
      case ConfigResource.Type.BROKER | ConfigResource.Type.CLIENT_METRICS =>
        if (authHelper.authorize(requestContext, ALTER_CONFIGS, CLUSTER, CLUSTER_NAME)) {
          new ApiError(NONE)
        } else {
          new ApiError(CLUSTER_AUTHORIZATION_FAILED)
        }
      case ConfigResource.Type.TOPIC =>
        if (authHelper.authorize(requestContext, ALTER_CONFIGS, TOPIC, resource.name)) {
          new ApiError(NONE)
        } else {
          new ApiError(TOPIC_AUTHORIZATION_FAILED)
        }
      case ConfigResource.Type.GROUP =>
        if (authHelper.authorize(requestContext, ALTER_CONFIGS, GROUP, resource.name)) {
          new ApiError(NONE)
        } else {
          new ApiError(GROUP_AUTHORIZATION_FAILED)
        }
      case rt => new ApiError(INVALID_REQUEST, s"Unexpected resource type $rt.")
    }
  }

  def handleLegacyAlterConfigs(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val response = new AlterConfigsResponseData()
    val alterConfigsRequest = request.body[AlterConfigsRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal, OptionalLong.empty())
    val duplicateResources = new util.HashSet[ConfigResource]
    val configChanges = new util.HashMap[ConfigResource, util.Map[String, String]]()
    // Outside-in defence: refuse foreign-tenant TOPIC and GROUP resource names
    // BEFORE they enter `configChanges` and reach the controller. A direct
    // `bootstrap.controllers` admin (KIP-590) reaches this handler without
    // traversing KafkaApis, so the broker-side TOPIC scrub does not run; the
    // GROUP type is unscrubbed even on the broker, so the controller is the
    // sole chokepoint. Same pattern as #117 / #123-#125.
    val callerTenant = callerTenantFromPrincipal(request.context.principal.getName)
    alterConfigsRequest.data.resources.forEach { resource =>
      val configResource = new ConfigResource(
        ConfigResource.Type.forId(resource.resourceType), resource.resourceName())
      val foreignTenantRefusal = foreignTenantConfigRefusal(configResource, callerTenant)
      if (configResource.`type`().equals(ConfigResource.Type.UNKNOWN)) {
        response.responses().add(new OldAlterConfigsResourceResponse().
          setErrorCode(UNSUPPORTED_VERSION.code()).
          setErrorMessage("Unknown resource type " + resource.resourceType() + ".").
          setResourceName(resource.resourceName()).
          setResourceType(resource.resourceType()))
      } else if (foreignTenantRefusal.isDefined) {
        val (errorCode, errorMessage) = foreignTenantRefusal.get
        response.responses().add(new OldAlterConfigsResourceResponse().
          setErrorCode(errorCode).
          setErrorMessage(errorMessage).
          setResourceName(resource.resourceName()).
          setResourceType(resource.resourceType()))
        duplicateResources.add(configResource)
      } else if (!duplicateResources.contains(configResource)) {
        val configs = new util.HashMap[String, String]()
        resource.configs().forEach(config => configs.put(config.name(), config.value()))
        if (configChanges.put(configResource, configs) != null) {
          duplicateResources.add(configResource)
          configChanges.remove(configResource)
          response.responses().add(new OldAlterConfigsResourceResponse().
            setErrorCode(INVALID_REQUEST.code()).
            setErrorMessage("Duplicate resource.").
            setResourceName(resource.resourceName()).
            setResourceType(resource.resourceType()))
        }
      }
    }
    val iterator = configChanges.keySet().iterator()
    while (iterator.hasNext) {
      val resource = iterator.next()
      val apiError = authorizeAlterResource(request.context, resource)
      if (apiError.isFailure) {
        response.responses().add(new OldAlterConfigsResourceResponse().
          setErrorCode(apiError.error().code()).
          setErrorMessage(apiError.message()).
          setResourceName(resource.name()).
          setResourceType(resource.`type`().id()))
        iterator.remove()
      }
    }
    val legacyControllerFuture =
      if (configChanges.isEmpty) {
        // Every resource was refused upstream (UNKNOWN, duplicate, foreign
        // tenant, authz). Skip the controller round-trip entirely so no
        // metadata write is attempted — preserves the per-resource response
        // envelope built above.
        CompletableFuture.completedFuture(
          java.util.Collections.emptyMap[ConfigResource, ApiError]())
      } else {
        controller.legacyAlterConfigs(context, configChanges, alterConfigsRequest.data.validateOnly)
      }
    legacyControllerFuture.handle[Unit] { (controllerResults, exception) =>
      if (exception != null) {
        requestHelper.handleError(request, exception)
      } else {
        controllerResults.forEach((key, value) => response.responses().add(
          new OldAlterConfigsResourceResponse().
            setErrorCode(value.error().code()).
            setErrorMessage(value.message()).
            setResourceName(key.name()).
            setResourceType(key.`type`().id())))
        requestHelper.sendResponseMaybeThrottle(request, throttleMs =>
          new AlterConfigsResponse(response.setThrottleTimeMs(throttleMs)))
      }
    }
  }

  def handleVote(request: RequestChannel.Request): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    handleRaftRequest(request, response => new VoteResponse(response.asInstanceOf[VoteResponseData]))
  }

  def handleBeginQuorumEpoch(request: RequestChannel.Request): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    handleRaftRequest(request, response => new BeginQuorumEpochResponse(response.asInstanceOf[BeginQuorumEpochResponseData]))
  }

  def handleEndQuorumEpoch(request: RequestChannel.Request): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    handleRaftRequest(request, response => new EndQuorumEpochResponse(response.asInstanceOf[EndQuorumEpochResponseData]))
  }

  def handleDescribeQuorum(request: RequestChannel.Request): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, DESCRIBE)
    handleRaftRequest(request, response => new DescribeQuorumResponse(response.asInstanceOf[DescribeQuorumResponseData]))
  }

  def handleElectLeaders(request: RequestChannel.Request): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, ALTER)
    val electLeadersRequest = request.body[ElectLeadersRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      requestTimeoutMsToDeadlineNs(time, electLeadersRequest.data.timeoutMs))
    // Outside-in TOPIC scrub (#128 / F6). A direct `bootstrap.controllers`
    // Admin (KIP-590) reaches this handler without traversing KafkaApis, so
    // the broker-side scrub at handleElectLeadersRequest does not run.
    // Two attack shapes:
    //
    //  1. Named-topic mode (`topicPartitions != null`): a cluster-wide caller
    //     could trigger an UNCLEAN leader election on `acme.orders-0`,
    //     potentially data-losing the tenant. Refuse per-topic, echo the
    //     topic name — INVALID_TOPIC_EXCEPTION on every requested partition.
    //
    //  2. Null-sweep mode (`topicPartitions == null`): the controller
    //     enumerates EVERY topic in topicsByName and elects leaders on each.
    //     A forwarded tenant principal must NEVER null-sweep — tenants are
    //     not cluster admins; their own topics are reachable by naming them.
    //     For a cluster-wide caller, the elections legitimately cover the
    //     whole cluster (the admin has ALTER on CLUSTER, same as the broker
    //     handler's stated rationale) but the response would otherwise
    //     enumerate every tenant topic name back to the caller. Strip
    //     tenant-namespaced entries from the response post-hoc as a name
    //     enumeration defence-in-depth.
    val data = electLeadersRequest.data
    val callerTenant = callerTenantFromPrincipal(request.context.principal.getName)

    if (data.topicPartitions() == null && callerTenant.isDefined) {
      // Tenant principal attempting a cluster-wide sweep. Refuse with a
      // top-level CLUSTER_AUTHORIZATION_FAILED — the legitimate flow is to
      // name the topics explicitly. Tenants don't normally reach this RPC
      // (ELECT_LEADERS is not in TENANT_ALLOWED_APIS on the broker), but a
      // forwarded envelope could carry the principal to bootstrap.controllers.
      val errResp = new ElectLeadersResponseData()
        .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code)
      requestHelper.sendResponseMaybeThrottle(request, throttleMs =>
        new ElectLeadersResponse(errResp.setThrottleTimeMs(throttleMs)))
      return CompletableFuture.completedFuture(())
    }

    val pollutionRejected =
      new util.ArrayList[ElectLeadersResponseData.ReplicaElectionResult]()
    if (data.topicPartitions() != null) {
      val keep = new ElectLeadersRequestData.TopicPartitionsCollection(data.topicPartitions().size)
      data.topicPartitions().forEach { tp =>
        foreignTenantTopicRefusal(tp.topic(), callerTenant) match {
          case Some(message) =>
            val partitionResults = new util.ArrayList[ElectLeadersResponseData.PartitionResult](tp.partitions().size)
            tp.partitions().forEach { p =>
              partitionResults.add(new ElectLeadersResponseData.PartitionResult()
                .setPartitionId(p)
                .setErrorCode(INVALID_TOPIC_EXCEPTION.code)
                .setErrorMessage(message))
            }
            pollutionRejected.add(new ElectLeadersResponseData.ReplicaElectionResult()
              .setTopic(tp.topic())
              .setPartitionResult(partitionResults))
          case None =>
            keep.add(tp.duplicate())
        }
      }
      if (!pollutionRejected.isEmpty) data.setTopicPartitions(keep)
    }

    val electFuture =
      if (data.topicPartitions() != null && data.topicPartitions().isEmpty && !pollutionRejected.isEmpty) {
        // Every requested topic was refused. Do NOT call the controller — an
        // empty TopicPartitions list would otherwise degrade into a NULL
        // sweep on some code paths (the request field is nullable). Skip the
        // round-trip and return only the rejections built above.
        CompletableFuture.completedFuture(new ElectLeadersResponseData())
      } else {
        controller.electLeaders(context, data)
      }
    electFuture.handle[Unit] { (responseData, exception) =>
      if (exception != null) {
        requestHelper.sendResponseMaybeThrottle(request, throttleMs => {
          electLeadersRequest.getErrorResponse(throttleMs, exception)
        })
      } else {
        // Defence-in-depth for the cluster-wide null-sweep case: strip any
        // tenant-namespaced topic from the response so the caller cannot
        // enumerate them. `callerTenant.isDefined && null-sweep` was
        // already short-circuited above; here only the cluster-wide path
        // can reach with a null sweep, but we filter using the SAME
        // structural predicate so combined-mode + split-mode agree.
        if (data.topicPartitions() == null && responseData.replicaElectionResults() != null) {
          val filtered = new util.ArrayList[ElectLeadersResponseData.ReplicaElectionResult](
            responseData.replicaElectionResults().size)
          responseData.replicaElectionResults().forEach { r =>
            if (!isForeignTenantNamespace(r.topic(), callerTenant)) filtered.add(r)
          }
          responseData.setReplicaElectionResults(filtered)
        }
        if (!pollutionRejected.isEmpty) {
          val merged = new util.ArrayList[ElectLeadersResponseData.ReplicaElectionResult](
            responseData.replicaElectionResults().size + pollutionRejected.size)
          merged.addAll(responseData.replicaElectionResults())
          merged.addAll(pollutionRejected)
          responseData.setReplicaElectionResults(merged)
        }
        requestHelper.sendResponseMaybeThrottle(request, throttleMs => {
          new ElectLeadersResponse(responseData.setThrottleTimeMs(throttleMs))
        })
      }
    }
  }

  def handleAlterPartitionRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val alterPartitionRequest = request.body[AlterPartitionRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    val future = controller.alterPartition(context, alterPartitionRequest.data)
    future.handle[Unit] { (result, exception) =>
      val response = if (exception != null) {
        alterPartitionRequest.getErrorResponse(exception)
      } else {
        new AlterPartitionResponse(result)
      }
      requestHelper.sendResponseExemptThrottle(request, response)
    }
  }

  def handleBrokerHeartBeatRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val heartbeatRequest = request.body[BrokerHeartbeatRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      requestTimeoutMsToDeadlineNs(time, config.brokerHeartbeatIntervalMs / 2))
    controller.processBrokerHeartbeat(context, heartbeatRequest.data).handle[Unit] { (reply, e) =>
      def createResponseCallback(requestThrottleMs: Int,
                                 reply: BrokerHeartbeatReply,
                                 e: Throwable): BrokerHeartbeatResponse = {
        if (e != null) {
          new BrokerHeartbeatResponse(new BrokerHeartbeatResponseData().
            setThrottleTimeMs(requestThrottleMs).
            setErrorCode(Errors.forException(e).code))
        } else {
          new BrokerHeartbeatResponse(new BrokerHeartbeatResponseData().
            setThrottleTimeMs(requestThrottleMs).
            setErrorCode(NONE.code).
            setIsCaughtUp(reply.isCaughtUp).
            setIsFenced(reply.isFenced).
            setShouldShutDown(reply.shouldShutDown))
        }
      }
      requestHelper.sendResponseMaybeThrottle(request,
        requestThrottleMs => createResponseCallback(requestThrottleMs, reply, e))
    }
  }

  def handleUnregisterBroker(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val decommissionRequest = request.body[UnregisterBrokerRequest]
    authHelper.authorizeClusterOperation(request, ALTER)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())

    controller.unregisterBroker(context, decommissionRequest.data.brokerId).handle[Unit] { (_, e) =>
      def createResponseCallback(requestThrottleMs: Int,
                                 e: Throwable): UnregisterBrokerResponse = {
        if (e != null) {
          new UnregisterBrokerResponse(new UnregisterBrokerResponseData().
            setThrottleTimeMs(requestThrottleMs).
            setErrorCode(Errors.forException(e).code))
        } else {
          new UnregisterBrokerResponse(new UnregisterBrokerResponseData().
            setThrottleTimeMs(requestThrottleMs))
        }
      }
      requestHelper.sendResponseMaybeThrottle(request,
        requestThrottleMs => createResponseCallback(requestThrottleMs, e))
    }
  }

  private def handleBrokerRegistration(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val registrationRequest = request.body[BrokerRegistrationRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())

    controller.registerBroker(context, registrationRequest.data).handle[Unit] { (reply, e) =>
      def createResponseCallback(requestThrottleMs: Int,
                                 reply: BrokerRegistrationReply,
                                 e: Throwable): BrokerRegistrationResponse = {
        if (e != null) {
          new BrokerRegistrationResponse(new BrokerRegistrationResponseData().
            setThrottleTimeMs(requestThrottleMs).
            setErrorCode(Errors.forException(e).code))
        } else {
          new BrokerRegistrationResponse(new BrokerRegistrationResponseData().
            setThrottleTimeMs(requestThrottleMs).
            setErrorCode(NONE.code).
            setBrokerEpoch(reply.epoch))
        }
      }
      requestHelper.sendResponseMaybeThrottle(request,
        requestThrottleMs => createResponseCallback(requestThrottleMs, reply, e))
    }
  }

  private def handleRaftRequest(request: RequestChannel.Request,
                                buildResponse: ApiMessage => AbstractResponse): CompletableFuture[Unit] = {
    val requestBody = request.body[AbstractRequest]
    val future = raftManager.handleRequest(request.context, request.header, requestBody.data, time.milliseconds())
    future.handle[Unit] { (responseData, exception) =>
      val response = if (exception != null) {
        requestBody.getErrorResponse(exception)
      } else {
        buildResponse(responseData)
      }
      requestHelper.sendResponseExemptThrottle(request, response)
    }
  }

  def handleAlterClientQuotas(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val quotaRequest = request.body[AlterClientQuotasRequest]
    authHelper.authorizeClusterOperation(request, ALTER_CONFIGS)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())

    // Outside-in defence for the USER quota entity. A cluster-wide caller
    // hitting the broker listener or, via KIP-590 `bootstrap.controllers`,
    // the controller listener directly, can otherwise persist a quota record
    // keyed on `__tenant_<id>.<user>`. The broker enforces quotas by the
    // runtime principal name, so such a record DOS's (or, with high values,
    // silently elevates) a tenant principal's traffic with no tenant-side
    // visibility. The metadata layer (ClientQuotaControlManager) writes the
    // quota record unconditionally — controller is SOLE line of defence.
    //
    // Same-tenant exemption: a forwarded tenant principal `__tenant_acme.X`
    // is allowed to set/modify quotas keyed on a user in its own namespace
    // (`__tenant_acme.<user>`); cross-tenant or cluster-wide callers are
    // refused. Mirrors the delegation-token pattern at
    // handleCreateDelegationTokenRequest.
    val callerTenant = callerTenantFromPrincipal(request.context.principal.getName)
    def isForeignTenantUser(name: String): Boolean = {
      if (name == null) return false
      if (!isReservedTenantPrincipalNamespace(name)) return false
      callerTenant match {
        case Some(t) => !name.startsWith(TenantNamespace.PRINCIPAL_PREFIX + t + ".")
        case None => true
      }
    }
    val refused = new util.LinkedHashMap[ClientQuotaEntity, ApiError]()
    val allowed = new util.ArrayList[ClientQuotaAlteration](quotaRequest.entries.size)
    quotaRequest.entries.forEach { alteration =>
      val entity = alteration.entity()
      val userName = entity.entries().get(ClientQuotaEntity.USER)
      if (isForeignTenantUser(userName)) {
        // Empty error message — leaking the physical user name back to the
        // caller would amount to a presence oracle for that tenant principal.
        refused.put(entity, new ApiError(Errors.CLUSTER_AUTHORIZATION_FAILED, ""))
      } else {
        allowed.add(alteration)
      }
    }

    val controllerFuture =
      if (allowed.isEmpty) {
        // Skip the controller round-trip entirely if every entry was refused.
        // Returning an empty-results future preserves the response shape;
        // throttling is still applied below.
        CompletableFuture.completedFuture(
          java.util.Collections.emptyMap[ClientQuotaEntity, ApiError]())
      } else {
        controller.alterClientQuotas(context, allowed, quotaRequest.validateOnly)
      }
    controllerFuture.handle[Unit] { (results, exception) =>
      if (exception != null) {
        requestHelper.handleError(request, exception)
      } else {
        val merged = new util.LinkedHashMap[ClientQuotaEntity, ApiError]()
        merged.putAll(results)
        merged.putAll(refused)
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          AlterClientQuotasResponse.fromQuotaEntities(merged, requestThrottleMs))
      }
    }
  }

  def handleIncrementalAlterConfigs(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val response = new IncrementalAlterConfigsResponseData()
    val alterConfigsRequest = request.body[IncrementalAlterConfigsRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    val duplicateResources = new util.HashSet[ConfigResource]
    val configChanges = new util.HashMap[ConfigResource,
      util.Map[String, Entry[AlterConfigOp.OpType, String]]]()
    val brokerLoggerResponses = new util.ArrayList[AlterConfigsResourceResponse](1)
    // Outside-in defence: refuse foreign-tenant TOPIC and GROUP resource names
    // BEFORE they enter `configChanges` and reach the controller. See the
    // legacy handler above for the full rationale.
    val callerTenant = callerTenantFromPrincipal(request.context.principal.getName)
    alterConfigsRequest.data.resources.forEach { resource =>
      val configResource = new ConfigResource(
        ConfigResource.Type.forId(resource.resourceType), resource.resourceName())
      val foreignTenantRefusal = foreignTenantConfigRefusal(configResource, callerTenant)
      if (configResource.`type`().equals(ConfigResource.Type.BROKER_LOGGER)) {
        val apiError = try {
          runtimeLoggerManager.applyChangesForResource(
            authHelper.authorize(request.context, CLUSTER_ACTION, CLUSTER, CLUSTER_NAME),
            alterConfigsRequest.data().validateOnly(),
            resource)
          ApiError.NONE
        } catch {
          case t: Throwable => ApiError.fromThrowable(t)
        }
        brokerLoggerResponses.add(new AlterConfigsResourceResponse().
          setResourceName(resource.resourceName()).
          setResourceType(resource.resourceType()).
          setErrorCode(apiError.error().code()).
          setErrorMessage(if (apiError.isFailure) apiError.messageWithFallback() else null))
      } else if (configResource.`type`().equals(ConfigResource.Type.UNKNOWN)) {
        response.responses().add(new AlterConfigsResourceResponse().
          setErrorCode(UNSUPPORTED_VERSION.code()).
          setErrorMessage("Unknown resource type " + resource.resourceType() + ".").
          setResourceName(resource.resourceName()).
          setResourceType(resource.resourceType()))
      } else if (foreignTenantRefusal.isDefined) {
        val (errorCode, errorMessage) = foreignTenantRefusal.get
        response.responses().add(new AlterConfigsResourceResponse().
          setErrorCode(errorCode).
          setErrorMessage(errorMessage).
          setResourceName(resource.resourceName()).
          setResourceType(resource.resourceType()))
        duplicateResources.add(configResource)
      } else if (!duplicateResources.contains(configResource)) {
        val altersByName = new util.HashMap[String, Entry[AlterConfigOp.OpType, String]]()
        resource.configs.forEach { config =>
          altersByName.put(config.name, new util.AbstractMap.SimpleEntry[AlterConfigOp.OpType, String](
            AlterConfigOp.OpType.forId(config.configOperation), config.value))
        }
        if (configChanges.put(configResource, altersByName) != null) {
          duplicateResources.add(configResource)
          configChanges.remove(configResource)
          response.responses().add(new AlterConfigsResourceResponse().
            setErrorCode(INVALID_REQUEST.code()).
            setErrorMessage("Duplicate resource.").
            setResourceName(resource.resourceName()).
            setResourceType(resource.resourceType()))
        }
      }
    }
    val iterator = configChanges.keySet().iterator()
    while (iterator.hasNext) {
      val resource = iterator.next()
      val apiError = authorizeAlterResource(request.context, resource)
      if (apiError.isFailure) {
        response.responses().add(new AlterConfigsResourceResponse().
          setErrorCode(apiError.error().code()).
          setErrorMessage(apiError.message()).
          setResourceName(resource.name()).
          setResourceType(resource.`type`().id()))
        iterator.remove()
      }
    }
    val incrementalControllerFuture =
      if (configChanges.isEmpty) {
        // Every resource was refused upstream (BROKER_LOGGER handled inline,
        // UNKNOWN, duplicate, foreign tenant, authz). Skip the controller
        // round-trip so no metadata write is attempted; brokerLoggerResponses
        // are still appended below.
        CompletableFuture.completedFuture(
          java.util.Collections.emptyMap[ConfigResource, ApiError]())
      } else {
        controller.incrementalAlterConfigs(context, configChanges, alterConfigsRequest.data.validateOnly)
      }
    incrementalControllerFuture.handle[Unit] { (controllerResults, exception) =>
      if (exception != null) {
        requestHelper.handleError(request, exception)
      } else {
        controllerResults.forEach((key, value) => response.responses().add(
          new AlterConfigsResourceResponse().
            setErrorCode(value.error().code()).
            setErrorMessage(value.message()).
            setResourceName(key.name()).
            setResourceType(key.`type`().id())))
        brokerLoggerResponses.forEach(r => response.responses().add(r))
        requestHelper.sendResponseMaybeThrottle(request, throttleMs =>
          new IncrementalAlterConfigsResponse(response.setThrottleTimeMs(throttleMs)))
      }
    }
  }

  private def handleCreatePartitions(request: RequestChannel.Request): CompletableFuture[Unit] = {
    def filterAlterAuthorizedTopics(topics: Iterable[String]): Set[String] = {
      authHelper.filterByAuthorized(request.context, ALTER, TOPIC, topics)(n => n)
    }
    val createPartitionsRequest = request.body[CreatePartitionsRequest]
    val controllerMutationQuota = quotas.controllerMutation.newQuotaFor(request, strictSinceVersion = 3)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      requestTimeoutMsToDeadlineNs(time, createPartitionsRequest.data.timeoutMs),
      controllerMutationQuotaRecorderFor(controllerMutationQuota))
    val future = createPartitions(context,
      createPartitionsRequest.data(),
      filterAlterAuthorizedTopics)
    future.handle[Unit] { (responses, exception) =>
      val response = if (exception != null) {
        createPartitionsRequest.getErrorResponse(exception)
      } else {
        val responseData = new CreatePartitionsResponseData().setResults(responses)
        new CreatePartitionsResponse(responseData)
      }
      requestHelper.sendResponseMaybeThrottleWithControllerQuota(controllerMutationQuota, request, response)
    }
  }

  def handleDescribeConfigsRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    // Outside-in pollution guard (#70). A caller reaching ControllerApis
    // directly via `bootstrap.controllers` bypasses the broker-side
    // KafkaApis.handleDescribeConfigsRequest scrub. They could submit
    // `acme.orders` (TOPIC) or `__tenant_acme.G` (GROUP) and obtain either an
    // existence oracle (CONFIG_RESOURCE_NOT_FOUND vs full config) or the
    // tenant's entire topic configuration (retention.ms, segment.bytes,
    // cleanup.policy, ...). Principal-aware via `isForeignTenantNamespace` /
    // caller-tenant carve-out so a forwarded `__tenant_acme.alice` reading
    // its own namespace still passes through. Mirrors the controller-side
    // pattern used by (Incremental)AlterConfigs and the ACL handlers.
    val describeRequest = request.body[DescribeConfigsRequest]
    val callerTenant = callerTenantFromPrincipal(request.context.principal.getName)
    val pollutionRefused = new util.ArrayList[DescribeConfigsResponseData.DescribeConfigsResult]()
    val keep = new util.ArrayList[DescribeConfigsRequestData.DescribeConfigsResource]()
    describeRequest.data.resources.forEach { r =>
      val rt = ConfigResource.Type.forId(r.resourceType)
      if (rt == ConfigResource.Type.TOPIC
          && isForeignTenantNamespace(r.resourceName, callerTenant)) {
        pollutionRefused.add(new DescribeConfigsResponseData.DescribeConfigsResult()
          .setResourceType(r.resourceType)
          .setResourceName(r.resourceName)
          .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
          .setErrorMessage(null)
          .setConfigs(util.Collections.emptyList[DescribeConfigsResponseData.DescribeConfigsResourceResult]))
      } else if (rt == ConfigResource.Type.GROUP
          && isReservedTenantPrincipalNamespace(r.resourceName)
          && !callerTenant.exists(t =>
            r.resourceName.startsWith(TenantNamespace.PRINCIPAL_PREFIX + t + "."))) {
        pollutionRefused.add(new DescribeConfigsResponseData.DescribeConfigsResult()
          .setResourceType(r.resourceType)
          .setResourceName(r.resourceName)
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)
          .setErrorMessage(null)
          .setConfigs(util.Collections.emptyList[DescribeConfigsResponseData.DescribeConfigsResourceResult]))
      } else {
        keep.add(r)
      }
    }
    if (!pollutionRefused.isEmpty) {
      describeRequest.data.setResources(keep)
    }
    val responseData = configHelper.handleDescribeConfigsRequest(request, authHelper)
    if (!pollutionRefused.isEmpty) {
      val merged = new util.ArrayList[DescribeConfigsResponseData.DescribeConfigsResult](
        responseData.results.size + pollutionRefused.size)
      merged.addAll(responseData.results)
      merged.addAll(pollutionRefused)
      responseData.setResults(merged)
    }
    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new DescribeConfigsResponse(responseData.setThrottleTimeMs(requestThrottleMs)))
    CompletableFuture.completedFuture[Unit](())
  }

  def createPartitions(
    context: ControllerRequestContext,
    request: CreatePartitionsRequestData,
    getAlterAuthorizedTopics: Iterable[String] => Set[String]
  ): CompletableFuture[util.List[CreatePartitionsTopicResult]] = {
    val responses = new util.ArrayList[CreatePartitionsTopicResult]()
    val duplicateTopicNames = new util.HashSet[String]()
    val topicNames = new util.HashSet[String]()
    // Outside-in pollution guard (#121 step 3). A caller reaching
    // ControllerApis directly via `bootstrap.controllers` bypasses the
    // KafkaApis.handleCreatePartitionsRequest scrub. They could submit
    // `acme.orders` and add partitions to a tenant's topic — the broker
    // never sees the request, so its `effectiveTenant.isPresent` gate is
    // irrelevant here. Refuse pre-loop. Principal-aware via
    // `isForeignTenantNamespace`: the legitimate forwarded tenant flow
    // (broker rewrote `orders` → `acme.orders`, envelope carries
    // `__tenant_acme.alice`) passes through untouched.
    val callerTenant = callerTenantFromPrincipal(context.principal.getName)
    request.topics().forEach {
      topic =>
        if (isForeignTenantNamespace(topic.name(), callerTenant)) {
          responses.add(new CreatePartitionsTopicResult().
            setName(topic.name()).
            setErrorCode(INVALID_TOPIC_EXCEPTION.code).
            setErrorMessage("Topic name '" + topic.name() + "' is reserved (tenant namespace prefix)"))
        } else if (!topicNames.add(topic.name())) {
          duplicateTopicNames.add(topic.name())
        }
    }
    duplicateTopicNames.forEach { topicName =>
      responses.add(new CreatePartitionsTopicResult().
        setName(topicName).
        setErrorCode(INVALID_REQUEST.code).
        setErrorMessage("Duplicate topic name."))
        topicNames.remove(topicName)
    }
    val authorizedTopicNames = getAlterAuthorizedTopics(topicNames.asScala)
    val topics = new util.ArrayList[CreatePartitionsTopic]
    topicNames.forEach { topicName =>
      if (authorizedTopicNames.contains(topicName)) {
        topics.add(request.topics().find(topicName))
      } else {
        responses.add(new CreatePartitionsTopicResult().
          setName(topicName).
          setErrorCode(TOPIC_AUTHORIZATION_FAILED.code))
      }
    }
    controller.createPartitions(context, topics, request.validateOnly).thenApply { results =>
      results.forEach(response => responses.add(response))
      responses
    }
  }

  def handleControllerRegistration(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val registrationRequest = request.body[ControllerRegistrationRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())

    controller.registerController(context, registrationRequest.data)
      .thenApply[Unit] { _ =>
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          new ControllerRegistrationResponse(new ControllerRegistrationResponseData().
            setThrottleTimeMs(requestThrottleMs)))
      }
  }

  def handleAlterPartitionReassignments(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val alterRequest = request.body[AlterPartitionReassignmentsRequest]
    authHelper.authorizeClusterOperation(request, ALTER)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      requestTimeoutMsToDeadlineNs(time, alterRequest.data.timeoutMs))
    // Outside-in TOPIC scrub (#127 / F4). A direct `bootstrap.controllers`
    // Admin (KIP-590) reaches this handler without traversing KafkaApis, so
    // the broker-side scrub at handleAlterPartitionReassignmentsRequest does
    // not run. A cluster-wide caller could rewire replicas on a tenant topic
    // — or, with `Replicas=null`, silently CANCEL the tenant's in-flight
    // reassignments. Refuse per-topic, echoing the topic name (TOPIC names
    // are public via Metadata; nothing to hide). Same-tenant carve-out via
    // `isForeignTenantNamespace`: a forwarded `__tenant_acme.alice` may
    // still reassign `acme.*` topics.
    val data = alterRequest.data
    val callerTenant = callerTenantFromPrincipal(request.context.principal.getName)
    val pollutionRejected =
      new util.ArrayList[AlterPartitionReassignmentsResponseData.ReassignableTopicResponse]()
    val keep = new util.ArrayList[AlterPartitionReassignmentsRequestData.ReassignableTopic](data.topics().size)
    data.topics().forEach { t =>
      foreignTenantTopicRefusal(t.name(), callerTenant) match {
        case Some(message) =>
          val partResults = new util.ArrayList[AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse](t.partitions().size)
          t.partitions().forEach { p =>
            partResults.add(new AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse()
              .setPartitionIndex(p.partitionIndex())
              .setErrorCode(INVALID_TOPIC_EXCEPTION.code)
              .setErrorMessage(message))
          }
          pollutionRejected.add(new AlterPartitionReassignmentsResponseData.ReassignableTopicResponse()
            .setName(t.name())
            .setPartitions(partResults))
        case None =>
          keep.add(t)
      }
    }
    if (!pollutionRejected.isEmpty) data.setTopics(keep)

    val reassignFuture =
      if (data.topics().isEmpty) {
        // Every topic was refused upstream. Skip the controller round-trip
        // so no metadata write is attempted; the per-topic rejections built
        // above still ship in the response below. Same short-circuit as
        // #117 / #126.
        CompletableFuture.completedFuture(new AlterPartitionReassignmentsResponseData())
      } else {
        controller.alterPartitionReassignments(context, data)
      }
    reassignFuture.thenApply[Unit] { response =>
      if (!pollutionRejected.isEmpty) {
        val merged = new util.ArrayList[AlterPartitionReassignmentsResponseData.ReassignableTopicResponse](
          response.responses.size + pollutionRejected.size)
        merged.addAll(response.responses)
        merged.addAll(pollutionRejected)
        response.setResponses(merged)
      }
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AlterPartitionReassignmentsResponse(response.setThrottleTimeMs(requestThrottleMs)))
    }
  }

  private[server] def handleAlterUserScramCredentials(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val alterRequest = request.body[AlterUserScramCredentialsRequest]
    authHelper.authorizeClusterOperation(request, ALTER)

    // Defence in depth for the identity-laundering guard the broker applies
    // in KafkaApis.handleAlterUserScramCredentialsRequest. The controller is
    // reachable directly (the schema declares listeners=[broker,controller])
    // and via Envelope re-dispatch from any CLUSTER_ACTION holder — both
    // routes bypass the broker-side check. Tenant identity on the controller
    // is derived purely from the principal name (no per-listener binding
    // here). Rule: a caller not within tenant T may not Upsert or Delete
    // SCRAM credentials whose User is in `__tenant_T.*`. Atomic batch
    // refusal mirrors the broker.
    val callerTenant = callerTenantFromPrincipal(request.context.principal.getName)
    def belongsToCallerTenant(name: String): Boolean =
      name != null && callerTenant.isDefined &&
        name.startsWith(TenantNamespace.PRINCIPAL_PREFIX + callerTenant.get + ".")
    def isForeignTenantPrincipal(name: String): Boolean =
      isReservedTenantPrincipalNamespace(name) && !belongsToCallerTenant(name)

    val foreignTenantPrincipal =
      alterRequest.data.upsertions.asScala.exists(u => isForeignTenantPrincipal(u.name)) ||
      alterRequest.data.deletions.asScala.exists(d => isForeignTenantPrincipal(d.name))

    if (foreignTenantPrincipal) {
      val results = new util.ArrayList[AlterUserScramCredentialsResponseData.AlterUserScramCredentialsResult]()
      val seen = new util.HashSet[String]()
      alterRequest.data.deletions.forEach { d =>
        if (seen.add(d.name)) {
          results.add(new AlterUserScramCredentialsResponseData.AlterUserScramCredentialsResult()
            .setUser(d.name)
            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code)
            .setErrorMessage(null))
        }
      }
      alterRequest.data.upsertions.forEach { u =>
        if (seen.add(u.name)) {
          results.add(new AlterUserScramCredentialsResponseData.AlterUserScramCredentialsResult()
            .setUser(u.name)
            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code)
            .setErrorMessage(null))
        }
      }
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AlterUserScramCredentialsResponse(new AlterUserScramCredentialsResponseData()
          .setThrottleTimeMs(requestThrottleMs)
          .setResults(results)))
      CompletableFuture.completedFuture(())
    } else {
      val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
        OptionalLong.empty())
      controller.alterUserScramCredentials(context, alterRequest.data)
        .thenApply[Unit] { response =>
           requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
             new AlterUserScramCredentialsResponse(response.setThrottleTimeMs(requestThrottleMs)))
        }
    }
  }

  // The principal is carried through in the forwarded case.
  // The security protocol in the context is for the current connection (hop)
  // We need to always disallow a tokenAuthenticated principal
  // We need to allow special protocols but only in the forwarded case for testing.
  def allowTokenRequests(request: RequestChannel.Request): Boolean = {
    val protocol = request.context.securityProtocol
    if (request.context.principal.tokenAuthenticated ||
      // We allow forwarded requests to use PLAINTEXT for testing purposes
      (request.isForwarded == false && protocol == SecurityProtocol.PLAINTEXT) ||
      // disallow requests from 1-way SSL
      (request.isForwarded == false && protocol == SecurityProtocol.SSL && request.context.principal == KafkaPrincipal.ANONYMOUS))
      false
    else
      true
  }

  // Defence-in-depth helpers for the identity-laundering guard on the
  // controller listener. The controller has no per-listener tenant binding —
  // the only way a caller's identity can carry a tenant is through the
  // KafkaPrincipal name itself (`__tenant_<id>.<user>`), already validated
  // by TenantPrincipalBuilder on the originating broker/edge listener.
  //
  // The `__tenant_` prefix is RESERVED: ANY name shaped `__tenant_<id>.<x>`
  // with non-empty `<id>` is a reserved-namespace name, regardless of whether
  // `<id>` is currently bound to a broker listener. A structural check is
  // required here for two reasons:
  //
  //  - Pre-binding pollution: an admin on a cluster-wide listener could plant
  //    delegation tokens, SCRAM creds, or ACLs under `__tenant_X.*` BEFORE
  //    tenant X is bound. The newly-bound tenant would inherit the planted
  //    state on first use.
  //  - Split-mode KRaft: a process running with `process.roles=controller`
  //    only has no broker listeners and therefore an empty TenantConfig.
  //    Without a structural check, every controller-side guard collapses,
  //    re-opening the identity-laundering hole this method exists to close.
  //
  // Same-tenant exemption lives in `callerTenantFromPrincipal`: a forwarded
  // tenant principal is recognised structurally from its name, so a tenant
  // owning the namespace it is acting on is exempt from this guard via
  // `belongsToCallerTenant`. Recognition is purely structural (task #114) —
  // no `tenantConfig.allTenants` lookup — so combined-mode and split-mode
  // controllers produce identical decisions.
  private def isReservedTenantPrincipalNamespace(name: String): Boolean = {
    if (name == null) return false
    if (!name.startsWith(TenantNamespace.PRINCIPAL_PREFIX)) return false
    val afterPrefix = name.substring(TenantNamespace.PRINCIPAL_PREFIX.length)
    val dot = afterPrefix.indexOf('.')
    dot > 0
  }

  // Outside-in pollution guard for TOPIC-namespace requests on the controller
  // listener. A cluster-wide admin reaching the CONTROLLER listener directly
  // via `AdminClient.bootstrap.controllers` (KIP-590) skips every broker-side
  // outside-in scrub in KafkaApis, and would otherwise mutate tenant topic
  // state (create/delete/alter configs/reassign/elect/...) by naming
  // `<tenantId>.X` literally.
  //
  // STRUCTURAL CHECK — does not consult `tenantConfig.allTenants`. Task #114:
  // in split-mode KRaft the controller node has no broker listeners and
  // therefore an empty TenantConfig, so any guard keyed on `allTenants` is a
  // no-op and the metadata log silently absorbs cross-tenant creates. The
  // structural rule below works regardless of which tenants are bound on
  // this node: a name `<x>.<rest>` is "in a tenant namespace" iff `<x>` is
  // syntactically a valid tenant id (passes `TenantNamespace.validateTenantId`).
  //
  // Behavioural break for split-mode safety: a privileged caller naming a
  // topic `foo.bar` on the controller listener — where `foo` happens to
  // satisfy the tenant-id charset but is NOT bound to any tenant — is now
  // refused as INVALID_TOPIC_EXCEPTION. The old combined-mode behaviour
  // accepted it (because `foo` was not in `allTenants`). The cost is real
  // (free-dotted names from a cluster admin are off the table when the
  // first segment looks like a tenant id) but the alternative is silently
  // re-opening the entire #102 / #114 hole on every split-mode cluster.
  // Internal topics (`__consumer_offsets` and friends), single-underscore
  // prefixes (`_confluent-*`, Connect configs), and dot-free names remain
  // allowed.
  //
  // Principal-aware: returns true when the topic name lies in a tenant
  // namespace that does NOT belong to the caller. So the legitimate
  // forwarded tenant flow — broker rewrites `foo` → `acme.foo`, envelopes
  // to controller with forwarded principal `__tenant_acme.alice` — passes
  // through untouched while a cluster-wide admin (`callerTenant = None`)
  // or a cross-tenant principal `__tenant_evil.X` attempting `acme.foo` is
  // refused.
  private def isForeignTenantNamespace(name: String, callerTenant: Option[String]): Boolean = {
    if (name == null || Topic.isInternal(name)) return false
    // Single-`_` prefix exempts `_confluent-*`, Connect connector configs,
    // and other operator-internal conventions. `__*` is covered by the
    // `validateTenantId` rejection below (it refuses `__`-prefixed ids).
    if (name.startsWith("_")) return false
    val dot = name.indexOf('.')
    if (dot <= 0) return false
    val prefix = name.substring(0, dot)
    // Tenant-id-shape check. If the prefix would fail at broker startup
    // (charset, length, reserved `__` prefix, contains a dot — impossible
    // here but checked for symmetry), this name is not in any tenant's
    // namespace because no broker could ever be bound to `prefix`.
    try {
      TenantNamespace.validateTenantId(prefix)
    } catch {
      case _: IllegalArgumentException => return false
    }
    // Topic IS in a tenant-shaped namespace. Refuse unless the caller's
    // effective tenant is exactly `prefix`.
    !callerTenant.contains(prefix)
  }

  // Derive the caller's tenant from their principal name. STRUCTURAL — does
  // not consult `tenantConfig.allTenants`. Task #114: in split-mode KRaft the
  // controller's TenantConfig is empty, so the old `allTenants.contains`
  // gate returned None for every forwarded tenant principal and the
  // controller treated the legitimate `__tenant_acme.alice` → `acme.foo`
  // flow as foreign — which made the scrub return TRUE and broke every
  // tenant CreateTopics through the controller.
  //
  // The structural form pairs with `isForeignTenantNamespace`: both check
  // the tenant-id shape independently of `allTenants`, so combined-mode
  // and split-mode produce identical decisions for a given (principal,
  // topic) pair. The only asymmetry that remains — unknown-but-valid
  // tenant ids being treated as tenant-shaped — is documented in
  // `isForeignTenantNamespace`.
  private def callerTenantFromPrincipal(principalName: String): Option[String] = {
    if (principalName == null || !principalName.startsWith(TenantNamespace.PRINCIPAL_PREFIX)) return None
    val afterPrefix = principalName.substring(TenantNamespace.PRINCIPAL_PREFIX.length)
    val dot = afterPrefix.indexOf('.')
    if (dot <= 0) return None
    // Empty-user envelope `__tenant_<id>.` is treated as un-bound: empty
    // user names are forbidden on the encode side (TenantNamespace) so this
    // shape can only arrive via a planted JAAS entry or a metadata-log
    // write. Refusing to recognise it here forces `isForeignTenantNamespace`
    // to treat any tenant-namespaced topic as foreign — closing #103.
    if (dot == afterPrefix.length - 1) return None
    Some(afterPrefix.substring(0, dot))
  }

  // For per-topic outside-in TOPIC scrubs on the controller listener
  // (AlterPartitionReassignments, ElectLeaders, ...). Returns the standard
  // refusal message — formatted ONCE to keep #126-style filter loops in
  // every handler textually identical — when the topic name lies in a
  // foreign tenant's namespace, or `None` when the topic is acceptable for
  // this caller.
  //
  // Same-tenant carve-out lives in `isForeignTenantNamespace`: a forwarded
  // tenant principal `__tenant_<id>.<user>` whose `callerTenant` equals the
  // topic's leading segment passes through untouched. Internal topics,
  // single-`_` prefixes, and dotless names are NOT in any tenant namespace
  // and also pass through.
  private def foreignTenantTopicRefusal(
      topicName: String,
      callerTenant: Option[String]): Option[String] = {
    if (isForeignTenantNamespace(topicName, callerTenant))
      Some(s"Topic name '$topicName' is reserved (tenant namespace prefix)")
    else
      None
  }

  // For AlterConfigs / IncrementalAlterConfigs on the controller listener.
  // Returns `Some((errorCode, errorMessage))` when the resource lies in a
  // foreign tenant's namespace, or `None` when the resource is acceptable for
  // this caller.
  //
  // TOPIC names are <id>.<rest>; GROUP ids are __tenant_<id>.<rest>
  // (TenantNamespace.groupToPhysical uses the principal-prefix form so groups
  // and topics cannot collide in __consumer_offsets). The two checks are
  // therefore SEPARATE: `isForeignTenantNamespace` for TOPIC,
  // `isReservedTenantPrincipalNamespace` (+ caller-tenant carve-out) for
  // GROUP.
  //
  // Error semantics:
  //  - TOPIC → INVALID_TOPIC_EXCEPTION with the echoed name (the topic name
  //    is public via Metadata; nothing to hide).
  //  - GROUP → GROUP_AUTHORIZATION_FAILED with NULL errorMessage. The
  //    physical group id `__tenant_<id>.<rest>` would otherwise serve as a
  //    presence oracle for that tenant's consumer groups.
  //  - BROKER, BROKER_LOGGER, CLIENT_METRICS → no tenant scrub needed (they
  //    are not in any tenant namespace).
  private def foreignTenantConfigRefusal(
      resource: ConfigResource,
      callerTenant: Option[String]): Option[(Short, String)] = {
    resource.`type`() match {
      case ConfigResource.Type.TOPIC if isForeignTenantNamespace(resource.name(), callerTenant) =>
        Some((INVALID_TOPIC_EXCEPTION.code,
          s"Topic name '${resource.name()}' is reserved (tenant namespace prefix)"))
      case ConfigResource.Type.GROUP if isReservedTenantPrincipalNamespace(resource.name()) =>
        val callerOwned = callerTenant.exists(t =>
          resource.name().startsWith(TenantNamespace.PRINCIPAL_PREFIX + t + "."))
        if (callerOwned) None
        else Some((GROUP_AUTHORIZATION_FAILED.code, null))
      case _ => None
    }
  }

  // True iff `principalStr` is in the legacy `User:<name>` form AND the
  // `<name>` portion lies in a reserved tenant principal namespace. ACL bindings
  // serialise the principal as `User:foo`; the tenant-encoded form is
  // `User:__tenant_<id>.<user>`. Mirrors KafkaApis.isReservedUserPrincipalLiteral
  // so the structural check stays in lock-step across broker and controller.
  private def isReservedUserPrincipalLiteral(principalStr: String): Boolean = {
    if (principalStr == null) return false
    val userPrefix = "User:"
    if (!principalStr.startsWith(userPrefix)) return false
    isReservedTenantPrincipalNamespace(principalStr.substring(userPrefix.length))
  }

  // ACL handler outside-in pollution / leak guard for the controller listener.
  //
  // ACL APIs declare `listeners=[broker, controller]` in their schema — so an
  // AdminClient configured with `bootstrap.controllers` reaches CreateAcls /
  // DeleteAcls / DescribeAcls directly on a controller node, completely
  // bypassing KafkaApis.handleCreateAclsRequest / handleDeleteAclsRequest /
  // handleDescribeAcls and their tenant-namespace scrubs (#88, #98, #115).
  // Without these guards a cluster-wide admin reaching the controller could:
  //
  //  - CreateAcls: write a literal ACL keyed by physical name (e.g.
  //    `Topic:acme.orders`) or by tenant principal (e.g. `User:__tenant_acme.bob`),
  //    laundering authority into a tenant's namespace.
  //  - DeleteAcls: yank every ACL of a tenant via an explicit `Topic:acme.orders`
  //    or `User:__tenant_acme.alice` filter — or, via a wildcard filter, get a
  //    free enumeration of tenant ACLs from MatchingAcls.
  //  - DescribeAcls: enumerate tenant principals / consumer-group ids /
  //    transactional ids by filter probe, or get a wildcard dump of every
  //    tenant binding.
  //
  // Each scrub below is principal-aware: a forwarded tenant principal whose
  // `callerTenant` matches the bound prefix passes through (legitimate
  // tenant flow). Refusal is keyed structurally on `__tenant_*` and
  // tenant-id-shape topic prefixes — works in split-mode KRaft (#114).

  // Returns Some(ApiError) for a binding that a non-owning caller must not
  // create. Reuses the structural helpers shared with the topic/group scrubs.
  private def aclTenantPollutionRefusal(
      binding: AclBinding,
      callerTenant: Option[String]): Option[ApiError] = {
    val resource = binding.pattern
    val resourceType = resource.resourceType
    val resourceName = resource.name
    val principalRefused = isReservedUserPrincipalLiteral(binding.entry.principal) &&
      !callerOwnsUserPrincipal(binding.entry.principal, callerTenant)
    val nameRefused = resourceType match {
      case ResourceType.TOPIC =>
        isForeignTenantNamespace(resourceName, callerTenant)
      case ResourceType.GROUP | ResourceType.TRANSACTIONAL_ID | ResourceType.USER =>
        isReservedTenantPrincipalNamespace(resourceName) &&
          !callerOwnsPrincipalNamespaceName(resourceName, callerTenant)
      case _ => false
    }
    if (principalRefused || nameRefused) {
      val what =
        if (nameRefused) "Resource name '" + resourceName + "'"
        else "Principal '" + binding.entry.principal + "'"
      Some(new ApiError(Errors.INVALID_REQUEST,
        what + " is reserved (tenant namespace prefix)"))
    } else None
  }

  // Returns Some(ApiError) for a DeleteAcls filter that EXPLICITLY names a
  // foreign tenant namespace. Wildcard / null-name filters are left alone here
  // (cluster admin reach is legitimate) — they are scrubbed via the postFilter
  // on MatchingAcls below.
  private def aclTenantFilterRefusal(
      filter: AclBindingFilter,
      callerTenant: Option[String]): Option[ApiError] = {
    val pattern = filter.patternFilter
    val resourceName = pattern.name
    val principal = filter.entryFilter.principal
    val nameRefused = resourceName != null && (pattern.resourceType match {
      case ResourceType.TOPIC =>
        isForeignTenantNamespace(resourceName, callerTenant)
      case ResourceType.GROUP | ResourceType.TRANSACTIONAL_ID | ResourceType.USER =>
        isReservedTenantPrincipalNamespace(resourceName) &&
          !callerOwnsPrincipalNamespaceName(resourceName, callerTenant)
      case _ => false
    })
    val principalRefused = principal != null &&
      isReservedUserPrincipalLiteral(principal) &&
      !callerOwnsUserPrincipal(principal, callerTenant)
    if (nameRefused || principalRefused) {
      val what =
        if (nameRefused) "Resource filter '" + resourceName + "'"
        else "Principal filter '" + principal + "'"
      Some(new ApiError(Errors.INVALID_REQUEST,
        what + " names a reserved tenant namespace"))
    } else None
  }

  // Postfilter on DeleteAcls.MatchingAcls and DescribeAcls.Resources: keep a
  // binding only if it does not echo a foreign tenant namespace name or
  // principal. This is the leak guard for wildcard / ResourceType.ANY queries
  // that we intentionally let through.
  private def aclTenantBindingAllowed(
      binding: AclBinding,
      callerTenant: Option[String]): Boolean = {
    val pattern = binding.pattern
    val name = pattern.name
    val byName = pattern.resourceType match {
      case ResourceType.TOPIC =>
        isForeignTenantNamespace(name, callerTenant)
      case ResourceType.GROUP | ResourceType.TRANSACTIONAL_ID | ResourceType.USER =>
        isReservedTenantPrincipalNamespace(name) &&
          !callerOwnsPrincipalNamespaceName(name, callerTenant)
      case _ => false
    }
    val byPrincipal = isReservedUserPrincipalLiteral(binding.entry.principal) &&
      !callerOwnsUserPrincipal(binding.entry.principal, callerTenant)
    !(byName || byPrincipal)
  }

  // Helpers for the same-tenant carve-out. The name `__tenant_<id>.<rest>` is
  // caller-owned iff the caller's effective tenant is `<id>`.
  private def callerOwnsPrincipalNamespaceName(name: String,
                                               callerTenant: Option[String]): Boolean = {
    callerTenant.exists(t => name.startsWith(TenantNamespace.PRINCIPAL_PREFIX + t + "."))
  }

  private def callerOwnsUserPrincipal(principalStr: String,
                                      callerTenant: Option[String]): Boolean = {
    callerTenant.exists(t =>
      principalStr.startsWith("User:" + TenantNamespace.PRINCIPAL_PREFIX + t + "."))
  }

  private[server] def handleCreateAclsRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val callerTenant = callerTenantFromPrincipal(request.context.principal.getName)
    aclApis.handleCreateAcls(request, b => aclTenantPollutionRefusal(b, callerTenant))
  }

  private[server] def handleDeleteAclsRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val callerTenant = callerTenantFromPrincipal(request.context.principal.getName)
    aclApis.handleDeleteAcls(request,
      f => aclTenantFilterRefusal(f, callerTenant),
      b => aclTenantBindingAllowed(b, callerTenant))
  }

  private[server] def handleDescribeAclsRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val callerTenant = callerTenantFromPrincipal(request.context.principal.getName)
    aclApis.handleDescribeAcls(request, b => aclTenantBindingAllowed(b, callerTenant))
  }

  private[server] def handleCreateDelegationTokenRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val createTokenRequest = request.body[CreateDelegationTokenRequest]

    val requester = request.context.principal
    val ownerPrincipalName = createTokenRequest.data.ownerPrincipalName
    val ownerPrincipalType = createTokenRequest.data.ownerPrincipalType
    val owner = if (ownerPrincipalName == null || ownerPrincipalName.isEmpty) {
      request.context.principal
    } else {
      new KafkaPrincipal(ownerPrincipalType, ownerPrincipalName)
    }

    // Defence in depth for the identity-laundering guard the broker applies
    // in KafkaApis.handleCreateTokenRequest. The controller is reachable
    // directly (the schema declares listeners=[broker,controller]) and via
    // Envelope re-dispatch from any CLUSTER_ACTION holder — both routes
    // bypass the broker-side check. Tenant identity on the controller is
    // canonical-from-principal: a caller's principal name `__tenant_<id>.X`
    // is the only way to claim tenant T (TenantPrincipalBuilder rejects
    // cross-listener wrapping). So: if the owner or any renewer is in a
    // KNOWN tenant principal namespace and the caller's own principal is
    // not in that same namespace, refuse the mint.
    val callerTenant = callerTenantFromPrincipal(requester.getName)
    def belongsToCallerTenant(name: String): Boolean =
      name != null && callerTenant.isDefined &&
        name.startsWith(TenantNamespace.PRINCIPAL_PREFIX + callerTenant.get + ".")
    def isForeignTenantPrincipal(name: String): Boolean =
      isReservedTenantPrincipalNamespace(name) && !belongsToCallerTenant(name)
    val renewerNames = createTokenRequest.data.renewers.asScala.map(_.principalName)
    val foreignTenantPrincipal =
      isForeignTenantPrincipal(ownerPrincipalName) ||
      renewerNames.exists(isForeignTenantPrincipal)

    if (!allowTokenRequests(request)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        CreateDelegationTokenResponse.prepareResponse(request.context.requestVersion, requestThrottleMs,
          Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, owner, requester))
      CompletableFuture.completedFuture[Unit](())
    } else if (foreignTenantPrincipal) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        CreateDelegationTokenResponse.prepareResponse(request.context.requestVersion, requestThrottleMs,
          Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED, owner, requester))
      CompletableFuture.completedFuture[Unit](())
    } else if (!owner.equals(requester) &&
      !authHelper.authorize(request.context, CREATE_TOKENS, USER, owner.toString)) {
      // Requester is always allowed to create token for self
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        CreateDelegationTokenResponse.prepareResponse(request.context.requestVersion, requestThrottleMs,
          Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED, owner, requester))
        CompletableFuture.completedFuture[Unit](())
    } else {

      val context = new ControllerRequestContext(request.context.header.data,
        request.context.principal, OptionalLong.empty())

      // Copy the response data to a new response so we can apply the request version
      controller.createDelegationToken(context, createTokenRequest.data)
        .thenApply[Unit] { response =>
           requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
             CreateDelegationTokenResponse.prepareResponse(
               request.context.requestVersion,
               requestThrottleMs,
               Errors.forCode(response.errorCode()),
               new KafkaPrincipal(response.principalType(), response.principalName()),
               new KafkaPrincipal(response.tokenRequesterPrincipalType(), response.tokenRequesterPrincipalName()),
               response.issueTimestampMs(),
               response.expiryTimestampMs(),
               response.maxTimestampMs(),
               response.tokenId(),
               ByteBuffer.wrap(response.hmac())))
      }
    }
  }

  private def handleRenewDelegationTokenRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val renewTokenRequest = request.body[RenewDelegationTokenRequest]

    if (!allowTokenRequests(request)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new RenewDelegationTokenResponse(
          new RenewDelegationTokenResponseData()
              .setThrottleTimeMs(requestThrottleMs)
              .setErrorCode(Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED.code)
              .setExpiryTimestampMs(DelegationTokenManager.ErrorTimestamp)))
        CompletableFuture.completedFuture[Unit](())
    } else {
      val context = new ControllerRequestContext(
        request.context.header.data,
        request.context.principal,
        OptionalLong.empty())
      controller.renewDelegationToken(context, renewTokenRequest.data)
        .thenApply[Unit] { response =>
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new RenewDelegationTokenResponse(response.setThrottleTimeMs(requestThrottleMs)))
      }
    }
  }

  private def handleExpireDelegationTokenRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val expireTokenRequest = request.body[ExpireDelegationTokenRequest]

    if (!allowTokenRequests(request)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new ExpireDelegationTokenResponse(
          new ExpireDelegationTokenResponseData()
              .setThrottleTimeMs(requestThrottleMs)
              .setErrorCode(Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED.code)
              .setExpiryTimestampMs(DelegationTokenManager.ErrorTimestamp)))
      CompletableFuture.completedFuture[Unit](())
    } else {
      val context = new ControllerRequestContext(
        request.context.header.data,
        request.context.principal,
        OptionalLong.empty())
      controller.expireDelegationToken(context, expireTokenRequest.data)
        .thenApply[Unit] { response =>
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new ExpireDelegationTokenResponse(response.setThrottleTimeMs(requestThrottleMs)))
      }
    }
  }

  def handleListPartitionReassignments(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val listRequest = request.body[ListPartitionReassignmentsRequest]
    authHelper.authorizeClusterOperation(request, DESCRIBE)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    // Outside-in TOPIC scrub + response-side enumeration defence (#129 / F5).
    // A direct `bootstrap.controllers` Admin (KIP-590) reaches this handler
    // without traversing KafkaApis, so the broker-side filter in
    // handleListPartitionReassignmentsRequest does not run.
    //
    // Two attack shapes:
    //
    //  1. List-all mode (`topics == null`): the controller enumerates every
    //     in-progress reassignment cluster-wide. A cluster-wide caller would
    //     see `acme.orders`, `beta.events`, ... — a tenant-topic-presence
    //     oracle and a partition-count leak. A forwarded tenant principal
    //     would see other tenants' topics. Filter the response.
    //
    //  2. Per-topic mode (`topics != null`): a cluster-wide or cross-tenant
    //     caller could name a foreign-tenant topic explicitly. Refuse with a
    //     top-level INVALID_TOPIC_EXCEPTION — the response schema has no per-
    //     entry error slot, so partial refusal is not expressible. Top-level
    //     refusal matches F4/AlterPartitionReassignments in spirit (explicit,
    //     not silent) while preserving atomicity.
    //
    // Same-tenant carve-out via `callerTenantFromPrincipal`: a forwarded
    // `__tenant_acme.alice` may name `orders` (LOGICAL) per-topic; we
    // translate to `acme.orders` before forwarding and back on the response.
    val data = listRequest.data
    val callerTenant = callerTenantFromPrincipal(request.context.principal.getName)

    // Per-topic mode pre-checks. Returns Some(errorMessage) on first refusal,
    // or None when the request is safe to forward. Translation of logical→
    // physical names for a tenant caller is performed in-place after the
    // per-entry refusal scan, so a partial mutation cannot occur.
    val perTopicRefusal: Option[String] =
      if (data.topics() == null) None
      else {
        var refusal: Option[String] = None
        val it = data.topics().iterator()
        while (it.hasNext && refusal.isEmpty) {
          val name = it.next().name()
          if (isForeignTenantNamespace(name, callerTenant)) {
            refusal = Some(s"Topic name '$name' is reserved (tenant namespace prefix)")
          }
        }
        refusal
      }

    if (perTopicRefusal.isDefined) {
      val errResp = new ListPartitionReassignmentsResponseData()
        .setErrorCode(INVALID_TOPIC_EXCEPTION.code)
        .setErrorMessage(perTopicRefusal.get)
      requestHelper.sendResponseMaybeThrottle(request, throttleMs =>
        new ListPartitionReassignmentsResponse(errResp.setThrottleTimeMs(throttleMs)))
      return CompletableFuture.completedFuture(())
    }

    // Tenant caller in per-topic mode: rewrite each entry's logical name to
    // its physical form so the controller (which stores physical names)
    // returns matches. A double-prefix or invalid logical form throws
    // InvalidTopicException from TenantNamespace.toPhysical; surface that as
    // a top-level error rather than silently consuming it.
    if (data.topics() != null && callerTenant.isDefined) {
      val tenantId = callerTenant.get
      var rewriteError: Option[String] = None
      val it = data.topics().iterator()
      while (it.hasNext && rewriteError.isEmpty) {
        val t = it.next()
        val logical = t.name()
        if (logical != null && !TenantNamespace.isInternalTopic(logical)) {
          try t.setName(TenantNamespace.toPhysical(tenantId, logical))
          catch {
            case e: org.apache.kafka.common.errors.InvalidTopicException =>
              rewriteError = Some(e.getMessage)
          }
        }
      }
      if (rewriteError.isDefined) {
        val errResp = new ListPartitionReassignmentsResponseData()
          .setErrorCode(INVALID_TOPIC_EXCEPTION.code)
          .setErrorMessage(rewriteError.get)
        requestHelper.sendResponseMaybeThrottle(request, throttleMs =>
          new ListPartitionReassignmentsResponse(errResp.setThrottleTimeMs(throttleMs)))
        return CompletableFuture.completedFuture(())
      }
    }

    controller.listPartitionReassignments(context, data)
      .thenApply[Unit] { response =>
        // Response-side filter. Two cases:
        //
        //  - Tenant caller: keep only entries in this tenant's `<id>.`
        //    namespace AND rewrite back to logical names so the client only
        //    ever sees what it sent (or, in list-all mode, the logical view
        //    of its own namespace).
        //
        //  - Cluster-wide caller in list-all mode: strip any entry whose
        //    leading segment is structurally a valid tenant id. Per-topic
        //    cluster-wide requests have already been refused above if they
        //    named a foreign tenant, so no filtering needed there (every
        //    entry the controller returns matches a name the cluster admin
        //    explicitly asked for, and those names are by definition not
        //    in any tenant namespace).
        if (response.topics() != null) {
          callerTenant match {
            case Some(tenantId) =>
              val filtered = new util.ArrayList[ListPartitionReassignmentsResponseData.OngoingTopicReassignment](
                response.topics().size)
              response.topics().forEach { t =>
                if (TenantNamespace.belongsTo(tenantId, t.name())) {
                  filtered.add(t.setName(TenantNamespace.toLogical(tenantId, t.name())))
                }
              }
              response.setTopics(filtered)
            case None if data.topics() == null =>
              val filtered = new util.ArrayList[ListPartitionReassignmentsResponseData.OngoingTopicReassignment](
                response.topics().size)
              response.topics().forEach { t =>
                if (!isForeignTenantNamespace(t.name(), None)) filtered.add(t)
              }
              response.setTopics(filtered)
            case None => // cluster-wide, per-topic: nothing to strip
          }
        }
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          new ListPartitionReassignmentsResponse(response.setThrottleTimeMs(requestThrottleMs)))
      }
  }

  def handleAllocateProducerIdsRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val allocatedProducerIdsRequest = request.body[AllocateProducerIdsRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
        OptionalLong.empty())
    controller.allocateProducerIds(context, allocatedProducerIdsRequest.data)
      .handle[Unit] { (results, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            results.setThrottleTimeMs(requestThrottleMs)
            new AllocateProducerIdsResponse(results)
          })
        }
      }
  }

  def handleUpdateFeatures(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val updateFeaturesRequest = request.body[UpdateFeaturesRequest]
    authHelper.authorizeClusterOperation(request, ALTER)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.updateFeatures(context, updateFeaturesRequest.data)
      .handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new UpdateFeaturesResponse(response.setThrottleTimeMs(requestThrottleMs)))
        }
      }
  }

  def handleDescribeCluster(request: RequestChannel.Request): CompletableFuture[Unit] = {
    // Nearly all RPCs should check MetadataVersion inside the QuorumController. However, this
    // RPC is consulting a cache which lives outside the QC. So we check MetadataVersion here.
    if (!apiVersionManager.features.metadataVersion().isControllerRegistrationSupported) {
      throw new UnsupportedVersionException("Direct-to-controller communication is not " +
        "supported with the current MetadataVersion.")
    }
    // Unlike on the broker, DESCRIBE_CLUSTER on the controller requires a high level of
    // permissions (ALTER on CLUSTER).
    authHelper.authorizeClusterOperation(request, ALTER)
    val response = authHelper.computeDescribeClusterResponse(
      request,
      EndpointType.CONTROLLER,
      clusterId,
      () => registrationsPublisher.describeClusterControllers(request.context.listenerName()),
      () => raftManager.leaderAndEpoch.leaderId().orElse(-1)
    )
    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new DescribeClusterResponse(response.setThrottleTimeMs(requestThrottleMs)))
    CompletableFuture.completedFuture[Unit](())
  }

  private def handleAssignReplicasToDirs(request: RequestChannel.Request): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    val assignReplicasToDirsRequest = request.body[AssignReplicasToDirsRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.assignReplicasToDirs(context, assignReplicasToDirsRequest.data).thenApply { reply =>
      requestHelper.sendResponseMaybeThrottle(request,
        requestThrottleMs => new AssignReplicasToDirsResponse(reply.setThrottleTimeMs(requestThrottleMs)))
    }
  }

  def handleAddRaftVoter(request: RequestChannel.Request): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, ALTER)
    handleRaftRequest(request, response => new AddRaftVoterResponse(response.asInstanceOf[AddRaftVoterResponseData]))
  }

  def handleRemoveRaftVoter(request: RequestChannel.Request): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, ALTER)
    handleRaftRequest(request, response => new RemoveRaftVoterResponse(response.asInstanceOf[RemoveRaftVoterResponseData]))
  }

  def handleUpdateRaftVoter(request: RequestChannel.Request): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    handleRaftRequest(request, response => new UpdateRaftVoterResponse(response.asInstanceOf[UpdateRaftVoterResponseData]))
  }
}
