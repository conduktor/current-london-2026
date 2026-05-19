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
import org.apache.kafka.common.acl.{AclBinding, AclOperation}
import org.apache.kafka.common.acl.AclOperation._
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic.{GROUP_METADATA_TOPIC_NAME, SHARE_GROUP_STATE_TOPIC_NAME, TRANSACTION_STATE_TOPIC_NAME, isInternal}
import org.apache.kafka.common.internals.{FatalExitError, Topic}
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.{AddPartitionsToTxnResult, AddPartitionsToTxnResultCollection}
import org.apache.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsTopic
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
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType._
import org.apache.kafka.common.resource.{Resource, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.security.token.delegation.{DelegationToken, TokenInformation}
import org.apache.kafka.common.utils.{ProducerIdAndEpoch, Time}
import org.apache.kafka.common.{ElectionType, Node, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.coordinator.group.{Group, GroupCoordinator}
import org.apache.kafka.coordinator.share.ShareCoordinator
import org.apache.kafka.server.ClientMetricsManager
import org.apache.kafka.server.authorizer._
import org.apache.kafka.server.common.{GroupVersion, RequestLocal, TransactionVersion}
import org.apache.kafka.server.share.context.ShareFetchContext
import org.apache.kafka.server.share.{ErroneousAndValidPartitionData, SharePartitionKey}
import org.apache.kafka.server.share.acknowledge.ShareAcknowledgementBatch
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams, FetchPartitionData}
import org.apache.kafka.server.tenant.{TenantConfig, TenantContext, TenantNamespace}
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
                val clientMetricsManager: ClientMetricsManager,
                val tenantConfig: TenantConfig = TenantConfig.empty()
) extends ApiRequestHandler with Logging {

  type FetchResponseStats = Map[TopicPartition, RecordValidationStats]
  this.logIdent = "[KafkaApi-%d] ".format(brokerId)
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
    info("Shutdown complete.")
  }

  // Build the per-request tenant view from the principal (carries `__tenant_<id>.`
  // from SASL handshake) and the listener's configured binding. Returns
  // TenantContext.none() when neither side declares a tenant, so the rewrite
  // paths in the handlers below short-circuit to identity.
  private[server] def tenantContextFor(request: RequestChannel.Request): TenantContext = {
    TenantContext.of(request.context.principal, tenantConfig.boundTenantFor(request.context.listenerName))
  }

  // Outside-in pollution guard, applied by non-tenant handlers (Produce / Fetch
  // / DeleteTopics) before they would otherwise hit replicaManager or forward
  // to the controller. A privileged caller on a non-tenant listener naming
  // `<id>.X` where `<id>` is tenant-shaped directly addresses tenant storage —
  // without this guard they could overwrite, read, or delete tenant data
  // despite the CreateTopics pollution guard refusing to create the same name
  // in the first place. Internal Kafka topics are exempt.
  //
  // STRUCTURAL ITERATION (F7) — when the broker is tenant-aware, classify by
  // tenant-id SHAPE rather than by membership in this broker's locally-bound
  // tenant set. The previous form iterated `tenantConfig.allTenants` and
  // therefore missed the heterogeneous-broker case: broker A configured with
  // `tenant.ids=acme,beta` would accept `gamma.foo` as a plain topic even
  // when `gamma` is bound on broker B and the cluster therefore owns that
  // namespace. The new rule mirrors `065b0d247e` / task #114 on the
  // controller side: `<id>.<rest>` is reserved iff `<id>` is syntactically a
  // valid tenant id per `TenantNamespace.validateTenantId`.
  //
  // The `allTenants.isEmpty` early-out is preserved: a stock-Kafka cluster
  // with zero tenants anywhere on this broker keeps the historical "tenant-
  // looking names are just topic names" behaviour, so a `foo.bar` topic
  // continues to work on plain deployments. The fix changes behaviour only
  // on brokers that already have at least one tenant bound locally — which
  // is precisely where the cross-broker blindspot opens up.
  //
  // Single-`_` prefix and dot-free names are not in any tenant namespace
  // (`_confluent-*`, Connect configs, plain `orders`). Reserved internal
  // topics short-circuit via `Topic.isInternal`.
  private def isReservedTenantNamespace(name: String): Boolean = {
    if (name == null || Topic.isInternal(name)) return false
    if (tenantConfig.allTenants.isEmpty) return false
    if (name.startsWith("_")) return false
    val dot = name.indexOf('.')
    if (dot <= 0) return false
    val prefix = name.substring(0, dot)
    try {
      TenantNamespace.validateTenantId(prefix)
    } catch {
      case _: IllegalArgumentException => return false
    }
    true
  }

  // Whether this Fetch is a *trusted* inter-broker follower fetch.
  //
  // `FetchRequest.isFromFollower` is purely wire-derived (`replicaId >= 0`);
  // it is a CLIENT-controlled boolean. Any caller that already holds
  // `CLUSTER_ACTION` on the cluster (super-users, MirrorMaker service
  // accounts, replica-audit operators) can flip the broker into "follower
  // fetch" mode by setting `replicaId=99` from a cluster-wide listener and
  // by-pass the outside-in pollution guard at line 1349 — `isFromFollower`
  // is the very predicate that turns the guard OFF. The follower-branch
  // CLUSTER_ACTION check at line 1423 then waves the spoof through, and the
  // tenant's physical topic (`acme.orders`) is returned by name.
  //
  // A real inter-broker follower always arrives on `config.interBrokerListenerName`.
  // Pin the trust to that listener: a `replicaId >= 0` fetch on any other
  // listener is treated as a regular consumer fetch (outside-in guard
  // applies, READ ACL required, etc.). Legitimate replication is unaffected
  // because the inter-broker listener is exactly where replicas connect.
  private def isInterBrokerFollowerFetch(request: RequestChannel.Request,
                                         fetchRequest: FetchRequest): Boolean = {
    if (!fetchRequest.isFromFollower) return false
    val ibl = config.interBrokerListenerName
    ibl != null && ibl == request.context.listenerName
  }

  // Sibling of `isInterBrokerFollowerFetch` for the OffsetForLeaderEpoch path.
  //
  // `RemoteLeaderEndPoint.fetchEpochEndOffsets` issues OFLE with
  // `forFollower(topics, brokerConfig.brokerId)` over `config.interBrokerListenerName`
  // every time a follower needs to re-anchor its log against a leader epoch
  // change. The outside-in scrub added by #131 (refuse `acme.orders` from a
  // non-tenant principal) would otherwise refuse the follower-side call with
  // TOPIC_AUTHORIZATION_FAILED for every tenant-prefixed partition, breaking
  // the truncation cycle and silently shrinking the ISR after any leader
  // epoch bump.
  //
  // Same trust model as Fetch: `replicaId >= 0` is a CLIENT-controlled wire
  // value; the only safe witness that the caller is a real replica is the
  // listener the request landed on. CONSUMER_REPLICA_ID (-1) and
  // DEBUGGING_REPLICA_ID (-2) are NOT followers and stay subject to the
  // outside-in guard.
  private def isInterBrokerFollowerOffsetForLeaderEpoch(
      request: RequestChannel.Request,
      offsetForLeaderEpoch: OffsetsForLeaderEpochRequest): Boolean = {
    if (offsetForLeaderEpoch.replicaId < 0) return false
    val ibl = config.interBrokerListenerName
    ibl != null && ibl == request.context.listenerName
  }

  // Outside-in guard for coordinator-keyed namespaces (consumer-group ids and
  // transactional ids). The `__tenant_` prefix is RESERVED by the multi-tenancy
  // fork: ANY name shaped `__tenant_<id>.<x>` with non-empty `<id>` is a
  // reserved-namespace name, regardless of whether `<id>` is currently bound to
  // a listener on this node. Two reasons to keep the check structural rather
  // than tenant-id-aware:
  //
  //  - Pre-binding pollution: an attacker on a cluster-wide listener can plant
  //    offsets / txn state / ACLs / SCRAM creds / delegation tokens under
  //    `__tenant_X.*` BEFORE tenant X is bound. The newly-bound tenant then
  //    inherits the pollution (their first OffsetFetch sees the planted
  //    commits, their CreateTopics conflicts with planted ACLs, etc).
  //  - Split-mode KRaft: a controller node runs with no broker listeners and
  //    therefore an empty TenantConfig. Every controller-side check against
  //    `allTenants` collapses, re-opening the cross-tenant guards on every
  //    forwarded RPC. A structural check fires on the controller too.
  //
  // The narrow lookup lives in `callerTenantFromPrincipal`: only callers whose
  // tenant id is currently bound are recognised as tenant clients and get
  // exempted from this guard via `belongsToCallerTenant`. Unknown-tenant
  // principals are refused; legitimate tenant clients on their own listener
  // are not.
  private def isReservedTenantPrincipalNamespace(name: String): Boolean = {
    if (name == null) return false
    if (!name.startsWith(org.apache.kafka.server.tenant.TenantNamespace.PRINCIPAL_PREFIX)) return false
    val afterPrefix = name.substring(org.apache.kafka.server.tenant.TenantNamespace.PRINCIPAL_PREFIX.length)
    val dot = afterPrefix.indexOf('.')
    dot > 0
  }

  // ACL pattern-aware variants of the two reserved-namespace helpers. #157:
  // `StandardAuthorizer.checkSection` resolves an `AclBinding` against a
  // physical resource name by literal `startsWith` for `PatternType.PREFIXED`
  // and by equality for `LITERAL` (see `StandardAuthorizerData.checkSection`).
  // The plain helpers above only catch the LITERAL case: a PREFIXED ACL with
  // a dotless tenant-id-shape name like `Topic:PREFIXED:acme` passes through
  // both `isReservedTenantNamespace("acme")` (no dot) and the L2 scrub on
  // DescribeAcls/DeleteAcls — and silently grants the planted authority over
  // every tenant-acme topic. Same shape for `Group:PREFIXED:__tenant_acme` on
  // the principal-prefix namespace.
  //
  // The pattern-aware predicate is "does ANY string matching this (name,
  // pattern) pair fall inside the tenant namespace?" For PREFIXED the answer
  // is yes iff the name is a prefix of a known tenant-namespace anchor OR is
  // already in the namespace. For MATCH and ANY the predicate widens to the
  // union of LITERAL and PREFIXED checks (MATCH bindings match both literal
  // and prefix variants of the resource name). UNKNOWN is treated as a no-op
  // — the request will fail downstream on the pattern type anyway.
  //
  // Empty `name` with PREFIXED is the most virulent shape (matches every
  // resource). Refuse it on any non-empty tenant configuration; on an empty
  // tenant set the predicate is a no-op for backward compatibility with
  // stock single-tenant Kafka.
  private def isReservedTenantTopicAclName(name: String, patternType: PatternType): Boolean = {
    if (name == null) return false
    patternType match {
      case PatternType.LITERAL => isReservedTenantNamespace(name)
      case PatternType.PREFIXED => isReservedTenantTopicAclPrefix(name)
      case PatternType.MATCH | PatternType.ANY =>
        isReservedTenantNamespace(name) || isReservedTenantTopicAclPrefix(name)
      case _ => false
    }
  }

  private def isReservedTenantTopicAclPrefix(name: String): Boolean = {
    // PREFIXED on TOPIC: only refuse when at least one tenant id is bound on
    // this node — the LITERAL helper makes the same choice (its short-circuit
    // on empty `allTenants` is what keeps stock Kafka working). The structural
    // pre-binding posture (#102/#114) is supplied by the principal-prefix
    // helper instead, since tenant TOPICS live under `<id>.` and are knowable
    // only from the tenant id table.
    if (tenantConfig.allTenants.isEmpty) return false
    val tenants = tenantConfig.allTenants.iterator()
    while (tenants.hasNext) {
      val t = tenants.next()
      val anchor = t + "."
      // `anchor.startsWith(name)` — e.g. name="ac" with tenant "acme" matches
      // the anchor; planting `Topic:PREFIXED:ac` would grant on every
      // `acme.*` physical topic.
      // `name.startsWith(anchor)` — e.g. name="acme.orders" with PREFIXED
      // already names physical state inside the tenant namespace.
      if (anchor.startsWith(name) || name.startsWith(anchor)) return true
    }
    false
  }

  private def isReservedTenantPrincipalAclName(name: String, patternType: PatternType): Boolean = {
    if (name == null) return false
    patternType match {
      case PatternType.LITERAL => isReservedTenantPrincipalNamespace(name)
      case PatternType.PREFIXED => isReservedTenantPrincipalAclPrefix(name)
      case PatternType.MATCH | PatternType.ANY =>
        isReservedTenantPrincipalNamespace(name) || isReservedTenantPrincipalAclPrefix(name)
      case _ => false
    }
  }

  private def isReservedTenantPrincipalAclPrefix(name: String): Boolean = {
    // STRUCTURAL — does not consult `allTenants` (mirrors
    // `isReservedTenantPrincipalNamespace`'s pre-binding posture). A PREFIXED
    // ACL whose name `p` is either a prefix of `__tenant_` (so it could grow
    // into the namespace when extended) OR already starts with `__tenant_`
    // (already in the namespace) matches some reserved name and must be
    // refused. Empty `p` is caught by `prefix.startsWith("")`.
    val prefix = org.apache.kafka.server.tenant.TenantNamespace.PRINCIPAL_PREFIX
    prefix.startsWith(name) || name.startsWith(prefix)
  }

  // Return the PHYSICAL TopicIdPartition the tenant is allowed to fetch, or
  // None if the topic falls outside the tenant's namespace.
  //
  // For Fetch v13+ the topic name was resolved from the broker's id→name map
  // (metadataCache.topicIdsToNames), so it is already PHYSICAL. The boundary
  // rule is simply: belongsToTenant or internal — anything else is foreign.
  //
  // For Fetch v0-12 the topic name comes from the client wire and is LOGICAL;
  // we translate it via toPhysical and accept if the result lands within the
  // tenant's namespace.
  //
  // The caller surfaces a boundary violation as UNKNOWN_TOPIC_OR_PARTITION so
  // the response never reveals whether the foreign topic exists.
  private def normaliseTenantTopicForFetch(tip: TopicIdPartition,
                                           ctx: TenantContext,
                                           versionId: Int): Option[TopicIdPartition] = {
    if (tip.topic == null) return None
    if (Topic.isInternal(tip.topic)) {
      // Defence-in-depth: a tenant principal must never Fetch a Kafka-internal
      // topic directly. The legitimate path is via the coordinator APIs
      // (OffsetFetch, FindCoordinator, AddPartitionsToTxn, ...) which key by
      // tenant-scoped id and only return that tenant's records. A direct Fetch
      // on `__consumer_offsets` (or friends), if any operator ever grants READ
      // by mistake, would otherwise expose every tenant's commits / txn state.
      // The follower-fetch path is unaffected: an inter-broker listener has no
      // tenant binding, so ctx.effectiveTenant is empty and we keep returning
      // the tip unchanged. Surface as UNKNOWN_TOPIC_OR_PARTITION (via the
      // foreignFetchTips path) so existence cannot be probed.
      return if (ctx.effectiveTenant.isPresent) None else Some(tip)
    }
    if (versionId >= 13) {
      if (ctx.belongsToTenant(tip.topic)) Some(tip) else None
    } else {
      // A v0-12 fetch carrying a reserved-physical-form name (e.g. tenant acme
      // asking for `acme.orders`) is treated as foreign — surfaces as
      // UNKNOWN_TOPIC_OR_PARTITION, identical to any other out-of-namespace
      // probe so it cannot be used to test for the existence of the physical
      // form. An over-long logical name is treated the same way: prefixing it
      // would produce a name the cluster cannot carry, and we want a probe to
      // be indistinguishable from any other miss. A logical name that is not
      // a valid Kafka topic name at all (`""`, `.`, `..`, illegal chars) is
      // also treated as a miss — rewriting would either let the controller
      // reject with the physical form in the error string, or in the auto-
      // create path briefly materialise the malformed name.
      if (ctx.isReservedPhysicalForm(tip.topic)
          || ctx.isOverlongLogicalForm(tip.topic)
          || ctx.isInvalidLogicalForm(tip.topic)) return None
      val physical = ctx.toPhysical(tip.topic)
      if (ctx.belongsToTenant(physical)) {
        if (physical == tip.topic) Some(tip)
        else Some(new TopicIdPartition(tip.topicId, tip.partition, physical))
      } else None
    }
  }

  // The FetchResponse Topic string field is serialized only in v0-12. After we
  // generate the response the topic name on each FetchableTopicResponse mirrors
  // the physical TopicIdPartition key, which would leak `<tenantId>.<topic>` to
  // a v0-12 caller that asked for plain `<topic>`. Rewrite physical → logical
  // here, after recordBytesOutMetric has used the physical name for stats.
  private def rewriteFetchResponseToLogical(resp: FetchResponse, ctx: TenantContext): Unit = {
    if (!ctx.effectiveTenant.isPresent) return
    resp.data.responses.forEach { topicResp =>
      val name = topicResp.topic
      if (name != null && !name.isEmpty) {
        val logical = ctx.toLogical(name)
        if (logical != name) topicResp.setTopic(logical)
      }
    }
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

  // CREATE_TOPICS — Forwarded to the controller. We translate the topic names
  // in the request body to physical before forwarding, and translate them back
  // in the response. Any "unsafe" request — privileged caller on a tenant-bound
  // listener, principal/listener tenant disagreement, or a `__tenant_` prefix
  // arriving on an unbound listener — is refused here rather than forwarded.
  // The controller has no knowledge of listener tenancy and would happily let
  // the caller create topics in any namespace.
  def handleCreateTopicsRequest(request: RequestChannel.Request): Unit = {
    val ctx = tenantContextFor(request)
    if (ctx.isUnsafe) {
      val createReq = request.body[CreateTopicsRequest]
      val results = new CreateTopicsResponseData.CreatableTopicResultCollection()
      createReq.data.topics.forEach(t =>
        results.add(new CreateTopicsResponseData.CreatableTopicResult()
          .setName(t.name)
          .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
          .setErrorMessage(Errors.TOPIC_AUTHORIZATION_FAILED.message)))
      requestChannel.sendResponse(request,
        new CreateTopicsResponse(new CreateTopicsResponseData().setTopics(results)), None)
      return
    }
    if (!ctx.effectiveTenant.isPresent) {
      // Outside-in pollution guard. A privileged caller on an unbound listener
      // could otherwise CreateTopics("acme.foo") literally; tenant acme on its
      // own listener would then see `foo` in ListTopics because the broker
      // cannot tell intent apart from prefix. Refuse any structurally
      // tenant-shaped name (F7: see `isReservedTenantNamespace` — uses
      // TenantNamespace.validateTenantId on the prefix rather than this
      // broker's local `allTenants` snapshot, so a heterogeneous-broker
      // deployment where `gamma` is bound only on broker B still refuses
      // `gamma.foo` on broker A). Internal topics are exempt — they are
      // never tenant-namespaced. The helper short-circuits when
      // `tenantConfig.allTenants` is empty, so a stock Kafka deployment is
      // unaffected.
      val createReq = request.body[CreateTopicsRequest]
      val pollutionRejected = new util.ArrayList[CreateTopicsResponseData.CreatableTopicResult]()
      val forwardable = new CreateTopicsRequestData.CreatableTopicCollection(createReq.data.topics.size)
      createReq.data.topics.forEach { t =>
        val name = t.name
        if (isReservedTenantNamespace(name)) {
          pollutionRejected.add(new CreateTopicsResponseData.CreatableTopicResult()
            .setName(name)
            .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code)
            .setErrorMessage("Topic name '" + name + "' is reserved (tenant namespace prefix)"))
        } else {
          forwardable.add(t.duplicate())
        }
      }
      if (pollutionRejected.isEmpty) {
        forwardToController(request)
        return
      }
      if (forwardable.isEmpty) {
        val responses = new CreateTopicsResponseData.CreatableTopicResultCollection(pollutionRejected.size)
        pollutionRejected.forEach(r => responses.add(r))
        requestChannel.sendResponse(request,
          new CreateTopicsResponse(new CreateTopicsResponseData().setTopics(responses)), None)
        return
      }
      createReq.data.setTopics(forwardable)
      forwardingManager.forwardRequest(request, createReq, {
        case Some(resp: CreateTopicsResponse) =>
          val merged = new CreateTopicsResponseData.CreatableTopicResultCollection(
            resp.data.topics.size + pollutionRejected.size)
          pollutionRejected.forEach(r => merged.add(r))
          resp.data.topics.forEach(r => merged.add(r.duplicate()))
          resp.data.setTopics(merged)
          requestHelper.sendForwardedResponse(request, resp)
        case Some(other) =>
          requestHelper.sendForwardedResponse(request, other)
        case None => handleInvalidVersionsDuringForwarding(request)
      })
      return
    }
    val createReq = request.body[CreateTopicsRequest]
    // Reserved-physical-form guard: refuse to forward `acme.orders` from
    // tenant acme — rewriting would double-prefix into `acme.acme.orders`.
    // Such entries get INVALID_TOPIC_EXCEPTION carrying the logical name the
    // client sent; the rest of the batch is rewritten and forwarded normally.
    val preRejected = new util.ArrayList[CreateTopicsResponseData.CreatableTopicResult]()
    // Map physical → logical so we can rewrite the response, even when an
    // error path returns the physical name (e.g. INVALID_TOPIC_EXCEPTION).
    val physicalToLogical = mutable.Map[String, String]()
    val rewrittenTopics = new CreateTopicsRequestData.CreatableTopicCollection(createReq.data.topics.size)
    createReq.data.topics.forEach { t =>
      val logical = t.name
      if (ctx.isReservedPhysicalForm(logical)) {
        preRejected.add(new CreateTopicsResponseData.CreatableTopicResult()
          .setName(logical)
          .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code)
          .setErrorMessage("Topic name '" + logical + "' is reserved (tenant namespace prefix)"))
      } else if (ctx.isOverlongLogicalForm(logical)) {
        // Refuse here so the rejection message quotes the LOGICAL name. Forwarding
        // would have the controller respond with the physical form in its error
        // string, leaking the tenant prefix.
        preRejected.add(new CreateTopicsResponseData.CreatableTopicResult()
          .setName(logical)
          .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code)
          .setErrorMessage("Topic name '" + logical + "' is too long for the tenant namespace"))
      } else if (ctx.isInvalidLogicalForm(logical)) {
        // Logical names like `""`, `.`, `..`, or names with illegal chars
        // would otherwise prefix to `acme.`, `acme..`, etc. The controller
        // rejects them too, but its error string carries the physical form.
        preRejected.add(new CreateTopicsResponseData.CreatableTopicResult()
          .setName(logical)
          .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code)
          .setErrorMessage("Topic name '" + logical + "' is not a valid Kafka topic name"))
      } else {
        val physical = ctx.toPhysical(logical)
        physicalToLogical(physical) = logical
        rewrittenTopics.add(t.duplicate().setName(physical))
      }
    }
    if (rewrittenTopics.isEmpty) {
      // Every entry was rejected upstream; don't forward an empty CreateTopics.
      val responses = new CreateTopicsResponseData.CreatableTopicResultCollection(preRejected.size)
      preRejected.forEach(r => responses.add(r))
      requestChannel.sendResponse(request,
        new CreateTopicsResponse(new CreateTopicsResponseData().setTopics(responses)), None)
      return
    }
    createReq.data.setTopics(rewrittenTopics)
    forwardingManager.forwardRequest(request, createReq, {
      case Some(resp: CreateTopicsResponse) =>
        val rewritten = new CreateTopicsResponseData.CreatableTopicResultCollection(
          resp.data.topics.size + preRejected.size)
        preRejected.forEach(r => rewritten.add(r))
        resp.data.topics.forEach { r =>
          val logical = Option(r.name).map(p => physicalToLogical.getOrElse(p, ctx.toLogical(p))).orNull
          rewritten.add(r.duplicate()
            .setName(logical)
            .setErrorMessage(scrubMessage(r.errorMessage, ctx)))
        }
        resp.data.setTopics(rewritten)
        requestHelper.sendForwardedResponse(request, resp)
      case Some(other) =>
        requestHelper.sendForwardedResponse(request, other)
      case None => handleInvalidVersionsDuringForwarding(request)
    })
  }

  // The controller, group/txn coordinators and storage layer may embed a
  // physical name in an `errorMessage` string. There are TWO physical forms:
  //   - Topic-prefix form `<tenantId>.<name>`  e.g. "Topic 'acme.orders'…"
  //   - Principal-prefix form `__tenant_<tenantId>.<name>` used for groups,
  //     transactional ids and the principal itself
  //     e.g. "Group '__tenant_acme.app1' not found"
  //
  // Strip both forms so tenants never see the prefix in human-readable text.
  // The principal-prefix form MUST be stripped first because the topic-prefix
  // is a strict suffix substring of it ("__tenant_acme.foo" contains "acme.");
  // running the shorter pattern first would corrupt the longer one into
  // "__tenant_foo" before its own pass.
  private def scrubMessage(msg: String, ctx: TenantContext): String = {
    if (msg == null) return null
    if (!ctx.effectiveTenant.isPresent) return msg
    val tenantId = ctx.effectiveTenant.get
    val principalPrefix = TenantNamespace.PRINCIPAL_PREFIX + tenantId + "."
    val topicPrefix = tenantId + "."
    var out = msg
    if (out.contains(principalPrefix)) out = out.replace(principalPrefix, "")
    if (out.contains(topicPrefix)) out = out.replace(topicPrefix, "")
    out
  }

  // DELETE_TOPICS — Forwarded to the controller. v0-5 carries a list of topic
  // names; v6+ carries DeleteTopicState entries that may name OR id the topic.
  // For each entry with a name we translate logical → physical; for delete-by-id
  // we PRE-RESOLVE the id through metadataCache before forwarding — any id that
  // resolves to a topic outside the tenant's namespace (or that the broker has
  // never heard of) is rejected with UNKNOWN_TOPIC_ID at the broker, and never
  // reaches the controller. Internal topics ids are treated as foreign.
  // Any unsafe request (see TenantContext.isUnsafe) is refused outright.
  def handleDeleteTopicsRequest(request: RequestChannel.Request): Unit = {
    val ctx = tenantContextFor(request)
    val delReq = request.body[DeleteTopicsRequest]
    val version = delReq.version
    if (ctx.isUnsafe) {
      val results = new DeleteTopicsResponseData.DeletableTopicResultCollection()
      delReq.topics.forEach { t =>
        val result = new DeleteTopicsResponseData.DeletableTopicResult()
          .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
        if (t.name != null) result.setName(t.name)
        if (t.topicId != null && !t.topicId.equals(Uuid.ZERO_UUID)) result.setTopicId(t.topicId)
        results.add(result)
      }
      requestChannel.sendResponse(request,
        new DeleteTopicsResponse(new DeleteTopicsResponseData().setResponses(results)), None)
      return
    }
    if (!ctx.effectiveTenant.isPresent) {
      // Outside-in pollution guard. A non-tenant principal naming
      // `<knownTenantId>.X` would delete the tenant's physical topic from the
      // cluster-wide listener; refuse to forward such entries. Mirrors the
      // CreateTopics outside-in guard so creation and deletion stay symmetric.
      if (tenantConfig.allTenants.isEmpty) {
        forwardToController(request)
        return
      }
      handleNonTenantDeleteTopicsRequest(request, delReq, version)
      return
    }
    val physicalToLogical = mutable.Map[String, String]()
    val preRejected = new util.ArrayList[DeleteTopicsResponseData.DeletableTopicResult]()
    if (version >= 6) {
      val topicIdToName = metadataCache.topicIdsToNames()
      val forwardable = new util.ArrayList[DeleteTopicsRequestData.DeleteTopicState](delReq.data.topics.size)
      delReq.data.topics.forEach { t =>
        if (t.name != null) {
          val logical = t.name
          if (ctx.isReservedPhysicalForm(logical)) {
            // Tenant supplied `acme.orders` for tenant acme — rewriting would
            // hit `acme.acme.orders`, which (a) doesn't exist and would
            // surface as UNKNOWN_TOPIC_OR_PARTITION at the controller, and
            // (b) leaks the namespace contract. Surface INVALID_TOPIC with the
            // logical name preserved.
            preRejected.add(new DeleteTopicsResponseData.DeletableTopicResult()
              .setName(logical)
              .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code)
              .setErrorMessage("Topic name '" + logical + "' is reserved (tenant namespace prefix)"))
          } else if (ctx.isOverlongLogicalForm(logical)) {
            preRejected.add(new DeleteTopicsResponseData.DeletableTopicResult()
              .setName(logical)
              .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code)
              .setErrorMessage("Topic name '" + logical + "' is too long for the tenant namespace"))
          } else if (ctx.isInvalidLogicalForm(logical)) {
            preRejected.add(new DeleteTopicsResponseData.DeletableTopicResult()
              .setName(logical)
              .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code)
              .setErrorMessage("Topic name '" + logical + "' is not a valid Kafka topic name"))
          } else {
            val physical = ctx.toPhysical(logical)
            physicalToLogical(physical) = logical
            forwardable.add(t.duplicate().setName(physical))
          }
        } else {
          // delete-by-id: pre-resolve and authorise BEFORE forwarding so a
          // foreign UUID cannot cause a foreign topic to be deleted. Unknown
          // UUIDs and foreign UUIDs are indistinguishable in the response —
          // both surface as UNKNOWN_TOPIC_ID with name=null.
          val resolved = Option(topicIdToName.get(t.topicId))
          if (resolved.exists(name => ctx.belongsToTenant(name))) {
            forwardable.add(t.duplicate())
          } else {
            preRejected.add(new DeleteTopicsResponseData.DeletableTopicResult()
              .setName(null)
              .setTopicId(t.topicId)
              .setErrorCode(Errors.UNKNOWN_TOPIC_ID.code))
          }
        }
      }
      delReq.data.setTopics(forwardable)
      // If every entry was rejected at the broker, do not forward at all.
      if (forwardable.isEmpty) {
        val responses = new DeleteTopicsResponseData.DeletableTopicResultCollection(preRejected.size)
        preRejected.forEach(r => responses.add(r))
        requestChannel.sendResponse(request,
          new DeleteTopicsResponse(new DeleteTopicsResponseData().setResponses(responses)), None)
        return
      }
    } else {
      // v0-5 only carries names (no UUID path).
      val rewritten = new util.ArrayList[String](delReq.data.topicNames.size)
      delReq.data.topicNames.forEach { name =>
        if (ctx.isReservedPhysicalForm(name)) {
          preRejected.add(new DeleteTopicsResponseData.DeletableTopicResult()
            .setName(name)
            .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code)
            .setErrorMessage("Topic name '" + name + "' is reserved (tenant namespace prefix)"))
        } else if (ctx.isOverlongLogicalForm(name)) {
          preRejected.add(new DeleteTopicsResponseData.DeletableTopicResult()
            .setName(name)
            .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code)
            .setErrorMessage("Topic name '" + name + "' is too long for the tenant namespace"))
        } else if (ctx.isInvalidLogicalForm(name)) {
          preRejected.add(new DeleteTopicsResponseData.DeletableTopicResult()
            .setName(name)
            .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code)
            .setErrorMessage("Topic name '" + name + "' is not a valid Kafka topic name"))
        } else {
          val physical = ctx.toPhysical(name)
          physicalToLogical(physical) = name
          rewritten.add(physical)
        }
      }
      delReq.data.setTopicNames(rewritten)
      if (rewritten.isEmpty) {
        val responses = new DeleteTopicsResponseData.DeletableTopicResultCollection(preRejected.size)
        preRejected.forEach(r => responses.add(r))
        requestChannel.sendResponse(request,
          new DeleteTopicsResponse(new DeleteTopicsResponseData().setResponses(responses)), None)
        return
      }
    }
    forwardingManager.forwardRequest(request, delReq, {
      case Some(resp: DeleteTopicsResponse) =>
        val rewritten = new DeleteTopicsResponseData.DeletableTopicResultCollection(
          resp.data.responses.size + preRejected.size)
        preRejected.forEach(r => rewritten.add(r))
        resp.data.responses.forEach { r =>
          val physical = r.name
          val rebuilt = if (physical == null) {
            // Controller could not resolve the id (UNKNOWN_TOPIC_ID etc). The
            // name field is null, but errorMessage may still embed a physical
            // topic name (e.g. "topic foo.bar was deleted") — scrub it before
            // surfacing to the tenant.
            r.duplicate().setErrorMessage(scrubMessage(r.errorMessage, ctx))
          } else if (ctx.belongsToTenant(physical)) {
            val logical = physicalToLogical.getOrElse(physical, ctx.toLogical(physical))
            r.duplicate().setName(logical).setErrorMessage(scrubMessage(r.errorMessage, ctx))
          } else {
            // Defence-in-depth: if the controller surfaced a physical name we
            // did not pre-authorise (shouldn't happen now), redact rather than
            // leak the foreign name.
            new DeleteTopicsResponseData.DeletableTopicResult()
              .setName(null)
              .setTopicId(r.topicId)
              .setErrorCode(Errors.UNKNOWN_TOPIC_ID.code)
          }
          rewritten.add(rebuilt)
        }
        resp.data.setResponses(rewritten)
        requestHelper.sendForwardedResponse(request, resp)
      case Some(other) =>
        requestHelper.sendForwardedResponse(request, other)
      case None => handleInvalidVersionsDuringForwarding(request)
    })
  }

  // Outside-in pollution guard for DeleteTopics from a non-tenant principal on
  // a non-tenant listener. Refuse any entry whose name (or topic-id resolving
  // to a name) begins with `<knownTenantId>.`. This is the destructive twin of
  // the CreateTopics outside-in guard: without it, a privileged caller could
  // delete a tenant's physical topic from the cluster-wide listener and the
  // tenant would observe their logical topic silently vanishing.
  private def handleNonTenantDeleteTopicsRequest(request: RequestChannel.Request,
                                                  delReq: DeleteTopicsRequest,
                                                  version: Short): Unit = {
    val pollutionRejected = new util.ArrayList[DeleteTopicsResponseData.DeletableTopicResult]()
    if (version >= 6) {
      val topicIdToName = metadataCache.topicIdsToNames()
      val forwardable = new util.ArrayList[DeleteTopicsRequestData.DeleteTopicState](delReq.data.topics.size)
      delReq.data.topics.forEach { t =>
        // Resolve by-id entries so an admin cannot bypass the guard by sending
        // the UUID for `acme.X`. If the id is unresolvable, fall through to
        // forwardable — the controller will surface UNKNOWN_TOPIC_ID and the
        // existing scrub logic handles it.
        val effectiveName =
          if (t.name != null) t.name
          else topicIdToName.get(t.topicId)
        if (isReservedTenantNamespace(effectiveName)) {
          val r = new DeleteTopicsResponseData.DeletableTopicResult()
            .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code)
            .setErrorMessage("Topic name '" + effectiveName + "' is reserved (tenant namespace prefix)")
          if (t.name != null) r.setName(t.name) else r.setName(null)
          if (t.topicId != null && !t.topicId.equals(Uuid.ZERO_UUID)) r.setTopicId(t.topicId)
          pollutionRejected.add(r)
        } else {
          forwardable.add(t.duplicate())
        }
      }
      if (forwardable.isEmpty) {
        val responses = new DeleteTopicsResponseData.DeletableTopicResultCollection(pollutionRejected.size)
        pollutionRejected.forEach(r => responses.add(r))
        requestChannel.sendResponse(request,
          new DeleteTopicsResponse(new DeleteTopicsResponseData().setResponses(responses)), None)
        return
      }
      delReq.data.setTopics(forwardable)
    } else {
      val forwardable = new util.ArrayList[String](delReq.data.topicNames.size)
      delReq.data.topicNames.forEach { name =>
        if (isReservedTenantNamespace(name)) {
          pollutionRejected.add(new DeleteTopicsResponseData.DeletableTopicResult()
            .setName(name)
            .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code)
            .setErrorMessage("Topic name '" + name + "' is reserved (tenant namespace prefix)"))
        } else {
          forwardable.add(name)
        }
      }
      if (forwardable.isEmpty) {
        val responses = new DeleteTopicsResponseData.DeletableTopicResultCollection(pollutionRejected.size)
        pollutionRejected.forEach(r => responses.add(r))
        requestChannel.sendResponse(request,
          new DeleteTopicsResponse(new DeleteTopicsResponseData().setResponses(responses)), None)
        return
      }
      delReq.data.setTopicNames(forwardable)
    }
    forwardingManager.forwardRequest(request, delReq, {
      case Some(resp: DeleteTopicsResponse) =>
        val merged = new DeleteTopicsResponseData.DeletableTopicResultCollection(
          resp.data.responses.size + pollutionRejected.size)
        pollutionRejected.forEach(r => merged.add(r))
        resp.data.responses.forEach(r => merged.add(r.duplicate()))
        resp.data.setResponses(merged)
        requestHelper.sendForwardedResponse(request, resp)
      case Some(other) =>
        requestHelper.sendForwardedResponse(request, other)
      case None => handleInvalidVersionsDuringForwarding(request)
    })
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

      // Refuse any non-v1 API arriving in a tenant-scoped context. The handler
      // table below assumes per-API tenant awareness; for everything outside
      // v1 there is no rewrite path, and a silent pass-through would either
      // leak physical topic / group / txn names or let a privileged caller on
      // a tenant-bound listener mutate cluster-wide state. PROMPT.md fixes v1
      // to {Produce, Fetch, Metadata, CreateTopics, DeleteTopics, ListTopics}
      // and ListTopics is just the all-topics variant of Metadata. See
      // KafkaApis.TENANT_ALLOWED_APIS — the allow-list also covers connection
      // setup (SASL/API_VERSIONS) so a tenant-bound listener stays reachable
      // for auth.
      //
      // Wire shape: CLUSTER_AUTHORIZATION_FAILED. The refusal is a connection-
      // level capability decision, not a topic-resource decision; using
      // TOPIC_AUTHORIZATION_FAILED here is a probe vector — a non-topic API
      // (e.g. ListTransactions, DescribeCluster) returning a topic-flavoured
      // error tells the attacker the broker is treating their principal as
      // tenant-scoped (a per-API ACL denial would mirror the API's natural
      // error category, not always force TOPIC_AUTHORIZATION_FAILED).
      // CLUSTER_AUTHORIZATION_FAILED is uniform across API shapes and reveals
      // only that the caller lacks ClusterAction — a generic denial that
      // could equally describe any cluster-wide ACL.
      if (!KafkaApis.TENANT_ALLOWED_APIS.contains(request.header.apiKey)) {
        val tenantCtx = tenantContextFor(request)
        if (tenantCtx.effectiveTenant.isPresent) {
          info(s"Refusing ${request.header.apiKey} from tenant-scoped context " +
            s"(principal=${request.context.principal}, listener=${request.context.listenerName}, " +
            s"correlation id ${request.header.correlationId}, client id ${request.header.clientId})")
          requestHelper.sendErrorResponseMaybeThrottle(request, Errors.CLUSTER_AUTHORIZATION_FAILED.exception)
          return
        }
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
        case ApiKeys.CREATE_TOPICS => handleCreateTopicsRequest(request)
        case ApiKeys.DELETE_TOPICS => handleDeleteTopicsRequest(request)
        case ApiKeys.DELETE_RECORDS => handleDeleteRecordsRequest(request)
        case ApiKeys.INIT_PRODUCER_ID => handleInitProducerIdRequest(request, requestLocal)
        case ApiKeys.OFFSET_FOR_LEADER_EPOCH => handleOffsetForLeaderEpochRequest(request)
        case ApiKeys.ADD_PARTITIONS_TO_TXN => handleAddPartitionsToTxnRequest(request, requestLocal)
        case ApiKeys.ADD_OFFSETS_TO_TXN => handleAddOffsetsToTxnRequest(request, requestLocal)
        case ApiKeys.END_TXN => handleEndTxnRequest(request, requestLocal)
        case ApiKeys.WRITE_TXN_MARKERS => handleWriteTxnMarkersRequest(request, requestLocal)
        case ApiKeys.TXN_OFFSET_COMMIT => handleTxnOffsetCommitRequest(request, requestLocal).exceptionally(handleError)
        case ApiKeys.DESCRIBE_ACLS => handleDescribeAcls(request)
        case ApiKeys.CREATE_ACLS => handleCreateAclsRequest(request)
        case ApiKeys.DELETE_ACLS => handleDeleteAclsRequest(request)
        case ApiKeys.ALTER_CONFIGS => handleAlterConfigsRequest(request)
        case ApiKeys.DESCRIBE_CONFIGS => handleDescribeConfigsRequest(request)
        case ApiKeys.ALTER_REPLICA_LOG_DIRS => handleAlterReplicaLogDirsRequest(request)
        case ApiKeys.DESCRIBE_LOG_DIRS => handleDescribeLogDirsRequest(request)
        case ApiKeys.SASL_AUTHENTICATE => handleSaslAuthenticateRequest(request)
        case ApiKeys.CREATE_PARTITIONS => handleCreatePartitionsRequest(request)
        // Create, renew and expire DelegationTokens must first validate that the connection
        // itself is not authenticated with a delegation token before maybeForwardToController.
        case ApiKeys.CREATE_DELEGATION_TOKEN => handleCreateTokenRequest(request)
        case ApiKeys.RENEW_DELEGATION_TOKEN => handleRenewTokenRequest(request)
        case ApiKeys.EXPIRE_DELEGATION_TOKEN => handleExpireTokenRequest(request)
        case ApiKeys.DESCRIBE_DELEGATION_TOKEN => handleDescribeTokensRequest(request)
        case ApiKeys.DELETE_GROUPS => handleDeleteGroupsRequest(request, requestLocal).exceptionally(handleError)
        case ApiKeys.ELECT_LEADERS => handleElectLeadersRequest(request)
        case ApiKeys.INCREMENTAL_ALTER_CONFIGS => handleIncrementalAlterConfigsRequest(request)
        case ApiKeys.ALTER_PARTITION_REASSIGNMENTS => handleAlterPartitionReassignmentsRequest(request)
        case ApiKeys.LIST_PARTITION_REASSIGNMENTS => handleListPartitionReassignmentsRequest(request)
        case ApiKeys.OFFSET_DELETE => handleOffsetDeleteRequest(request, requestLocal).exceptionally(handleError)
        case ApiKeys.DESCRIBE_CLIENT_QUOTAS => handleDescribeClientQuotasRequest(request)
        case ApiKeys.ALTER_CLIENT_QUOTAS => forwardToController(request)
        case ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS => handleDescribeUserScramCredentialsRequest(request)
        case ApiKeys.ALTER_USER_SCRAM_CREDENTIALS => handleAlterUserScramCredentialsRequest(request)
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
    val tenantCtx = tenantContextFor(request)

    rewriteTenantGroupId(tenantCtx, offsetCommitRequest.data.groupId) match {
      case Left(err) =>
        // groupId is still the logical wire form on the request data — the
        // default error response echoes only the partition-shape we already
        // have, so no further rewriting is required.
        requestHelper.sendMaybeThrottle(request, offsetCommitRequest.getErrorResponse(err.exception))
        CompletableFuture.completedFuture[Unit](())
      case Right(physicalGroupId) =>
        // Reject the request if not authorized to the group (auth on the
        // physical name — that's the resource the broker actually stores).
        if (!authHelper.authorize(request.context, READ, GROUP, physicalGroupId)) {
          requestHelper.sendMaybeThrottle(request, offsetCommitRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
          CompletableFuture.completedFuture[Unit](())
        } else {
          offsetCommitRequest.data.setGroupId(physicalGroupId)
          // A tenant submitting a reserved-physical-form topic name (e.g. the
          // literal "acme.foo" for tenant acme) cannot be satisfied: the name
          // would round-trip to a physical-physical form, and merging the
          // per-topic rejection back into a response that may also carry a
          // legitimate "foo"→"acme.foo" commit would collide on the response
          // builder's by-name map. Refuse the WHOLE request rather than
          // partially satisfy and partially reject — the request itself is
          // malformed. The error response is built before in-rewrite so the
          // tenant sees the LITERAL names they submitted, not their physical
          // forms.
          if (tenantCtx.effectiveTenant.isPresent &&
              offsetCommitRequest.data.topics.asScala.exists(t => tenantCtx.isReservedPhysicalForm(t.name))) {
            requestHelper.sendMaybeThrottle(request,
              offsetCommitRequest.getErrorResponse(new InvalidTopicException(
                "OffsetCommit refused: one or more topic names use the reserved tenant-prefix form")))
            return CompletableFuture.completedFuture[Unit](())
          }
          if (tenantCtx.effectiveTenant.isPresent) {
            offsetCommitRequest.data.topics.forEach { topic =>
              topic.setName(tenantCtx.toPhysical(topic.name))
            }
          }

          val authorizedTopics = authHelper.filterByAuthorized(
            request.context,
            READ,
            TOPIC,
            offsetCommitRequest.data.topics.asScala
          )(_.name)

          val responseBuilder = new OffsetCommitResponse.Builder()
          val authorizedTopicsRequest = new mutable.ArrayBuffer[OffsetCommitRequestData.OffsetCommitRequestTopic]()
          offsetCommitRequest.data.topics.forEach { topic =>
            if (!tenantCtx.effectiveTenant.isPresent && isReservedTenantNamespace(topic.name)) {
              // Outside-in pollution scrub for cluster-wide callers. A non-tenant
              // principal naming a reserved tenant-prefix-shaped topic (e.g.
              // `acme.orders`) is refused with TOPIC_AUTHORIZATION_FAILED — the
              // same wire shape a regular ACL deny produces — so that:
              //   (a) the topic-existence oracle at `metadataCache.contains` two
              //       lines down cannot fire (NONE-vs-UNKNOWN_TOPIC_OR_PARTITION
              //       would otherwise reveal whether a tenant has materialised
              //       `acme.orders` even to callers with wildcard `Topic:*`),
              //   (b) `__consumer_offsets` cannot be polluted with cluster-admin-
              //       authored commits keyed on tenant namespaces — even though
              //       the COMMIT itself targets the admin's own group, landing
              //       records keyed `<group, acme.X, partition>` is a storage
              //       sink + a delete-records-on-coordinator-cleanup amplifier.
              // The same-tenant case is handled above (effectiveTenant.isPresent
              // branch already in-rewrites and refuses reserved-physical-form).
              responseBuilder.addPartitions[OffsetCommitRequestData.OffsetCommitRequestPartition](
                topic.name, topic.partitions, _.partitionIndex, Errors.TOPIC_AUTHORIZATION_FAILED)
            } else if (!authorizedTopics.contains(topic.name)) {
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

              topic.partitions.forEach { partition =>
                if (metadataCache.getLeaderAndIsr(topic.name, partition.partitionIndex).nonEmpty) {
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
            val response = responseBuilder.build()
            rewriteOffsetCommitResponseToLogical(response, tenantCtx)
            requestHelper.sendMaybeThrottle(request, response)
            CompletableFuture.completedFuture(())
          } else {
            // For version > 0, store offsets in Coordinator.
            commitOffsetsToCoordinator(
              request,
              offsetCommitRequest,
              authorizedTopicsRequest,
              responseBuilder,
              requestLocal,
              tenantCtx
            )
          }
        }
    }
  }

  // Rewrite physical topic names back to the LOGICAL form the tenant sent,
  // and drop the tenant prefix from anything still wearing it (including
  // partial coordinator failures that may quote the physical name).
  // Reserved-form requests are refused upfront (see handleOffsetCommitRequest)
  // so every name reaching here is either a real physical topic in this
  // tenant's namespace or an internal topic that toLogical leaves untouched.
  private def rewriteOffsetCommitResponseToLogical(
    response: OffsetCommitResponse,
    tenantCtx: TenantContext
  ): Unit = {
    if (!tenantCtx.effectiveTenant.isPresent) return
    response.data().topics().forEach(t => t.setName(tenantCtx.toLogical(t.name)))
  }

  private def commitOffsetsToCoordinator(
    request: RequestChannel.Request,
    offsetCommitRequest: OffsetCommitRequest,
    authorizedTopicsRequest: mutable.ArrayBuffer[OffsetCommitRequestData.OffsetCommitRequestTopic],
    responseBuilder: OffsetCommitResponse.Builder,
    requestLocal: RequestLocal,
    tenantCtx: TenantContext
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
        // Exception-path response is built from the (mutated) request, but
        // only carries shape — no topic names from coordinator state. Still
        // run the out-rewrite for symmetry with the success path.
        val errResponse = offsetCommitRequest.getErrorResponse(exception)
        rewriteOffsetCommitResponseToLogical(errResponse, tenantCtx)
        requestHelper.sendMaybeThrottle(request, errResponse)
      } else {
        val response = responseBuilder.merge(results).build()
        rewriteOffsetCommitResponseToLogical(response, tenantCtx)
        requestHelper.sendMaybeThrottle(request, response)
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

    val tenantCtx = tenantContextFor(request)
    val tenantScoped = tenantCtx.effectiveTenant.isPresent
    // Refuse any unsafe request — see TenantContext.isUnsafe. Each requested
    // topic-partition gets TOPIC_AUTHORIZATION_FAILED carrying the wire name
    // (logical from the caller's POV); the request reaches neither
    // authorization nor replicaManager. For acks=0 there is no response on
    // the wire — mirror the standard ack=0 error path and close the
    // connection so the client refreshes its metadata. Runs BEFORE the
    // transactional-id rewrite/auth below so its wire shape (per-partition
    // TOPIC_AUTHORIZATION_FAILED, acks=0 close) is preserved and cannot be
    // probed via the txn auth path.
    if (tenantCtx.isUnsafe) {
      val refused = mutable.Map[TopicPartition, PartitionResponse]()
      produceRequest.data.topicData.forEach(t => t.partitionData.forEach(p =>
        refused += new TopicPartition(t.name, p.index) -> new PartitionResponse(Errors.TOPIC_AUTHORIZATION_FAILED)))
      val refusedResponse = new ProduceResponse(refused.asJava)
      if (produceRequest.acks == 0) {
        info(s"Closing connection due to unsafe tenant context on acks=0 produce " +
          s"(correlation id ${request.header.correlationId}, client id ${request.header.clientId})")
        requestChannel.closeConnection(request, refusedResponse.errorCounts)
      } else {
        requestChannel.sendResponse(request, refusedResponse, None)
      }
      return
    }

    // Transactional-id rewrite. AddPartitionsToTxnManager.partitionFor hashes
    // the wire transactionalId to pick a `__transaction_state` partition, and
    // ReplicaManager.handleProduceAppend forwards it to the coordinator for
    // verification. Routing on the LOGICAL id would miss the state
    // InitProducerId stored under `__tenant_<id>.<name>` AND let two tenants
    // reusing the same external txn id collide on the verification cache key.
    // Mirror every other tenant-aware transactional handler: rewrite first,
    // then authorise on the physical name (so an ACL granted against the
    // resolved id applies consistently with InitProducerId / EndTxn /
    // AddOffsetsToTxn / TxnOffsetCommit).
    val logicalTransactionalId = produceRequest.transactionalId
    if (!tenantScoped && isReservedTenantPrincipalNamespace(logicalTransactionalId)) {
      // Outside-in: non-tenant caller naming `__tenant_<known>.foo` would
      // fence the tenant's producer slot. Refuse with the auth-failed wire
      // shape so the response cannot be used as a probe for tenant existence.
      requestHelper.sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
      return
    }
    val physicalTransactionalId: String =
      try tenantCtx.toPhysicalTxnId(logicalTransactionalId)
      catch {
        // A tenant addressing `__tenant_other.foo` — same wire shape an auth
        // failure would produce.
        case _: IllegalArgumentException =>
          requestHelper.sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
          return
      }

    if (RequestUtils.hasTransactionalRecords(produceRequest)) {
      val isAuthorizedTransactional = physicalTransactionalId != null &&
        authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, physicalTransactionalId)
      if (!isAuthorizedTransactional) {
        requestHelper.sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
        return
      }
    }

    // Reserved-physical-form guard. A tenant submitting `acme.orders` is
    // either confused or trying to address the storage namespace directly;
    // either way, rewriting would double-prefix into `acme.acme.orders` and
    // silently create the topic (auto-create on Produce). Refuse such topics
    // up front, keyed by the LOGICAL name the client sent.
    val invalidLogicalTopicResponses = mutable.Map[TopicPartition, PartitionResponse]()
    if (tenantScoped) {
      val rejected = new util.ArrayList[ProduceRequestData.TopicProduceData]()
      produceRequest.data.topicData.forEach { t =>
        // Four upstream-rejectable conditions:
        //   (a) reserved-physical-form (`acme.X` from tenant acme) → double-prefix
        //   (b) over-long logical form whose `<tenant>.<name>` exceeds 249 chars
        //   (c) invalid logical form (`""`, `.`, `..`, illegal chars) — would
        //       either let auto-create briefly materialise a malformed name or
        //       surface the physical form in the broker's own validation error.
        //   (d) #91 defence-in-depth: tenant writes to an internal topic
        //       (__consumer_offsets, __transaction_state, __share_group_state).
        //       toPhysical passes internal names through unchanged, so without
        //       this guard a misconfigured wildcard `WRITE Topic:*` ACL would
        //       let a tenant corrupt the cluster's offsets / txn / share-group
        //       log. Tenants never legitimately Produce to internal topics —
        //       coordinator writes go through dedicated APIs. TOPIC_AUTH_FAILED
        //       keeps the wire shape indistinguishable from a regular authz
        //       refusal so the tenant cannot probe ACL configuration via the
        //       error category.
        // (a)-(c) share INVALID_TOPIC_EXCEPTION; (d) returns TOPIC_AUTH_FAILED.
        // All bypass replicaManager; per-entry pre-rejection keeps the rest of
        // the batch alive.
        val refusalError =
          if (tenantCtx.isReservedPhysicalForm(t.name)
              || tenantCtx.isOverlongLogicalForm(t.name)
              || tenantCtx.isInvalidLogicalForm(t.name)) Some(Errors.INVALID_TOPIC_EXCEPTION)
          else if (Topic.isInternal(t.name)) Some(Errors.TOPIC_AUTHORIZATION_FAILED)
          else None
        refusalError.foreach { err =>
          rejected.add(t)
          t.partitionData.forEach { p =>
            invalidLogicalTopicResponses +=
              new TopicPartition(t.name, p.index) -> new PartitionResponse(err)
          }
        }
      }
      rejected.forEach(t => produceRequest.data.topicData.remove(t))
    } else if (!tenantConfig.allTenants.isEmpty) {
      // Outside-in pollution guard. A non-tenant principal on a non-tenant
      // listener naming `<knownTenantId>.X` would auto-create / append into
      // the tenant's physical log; the tenant's logical Fetch would then
      // return foreign records as if they had produced them. Refuse such
      // entries up front — destructive twin of the CreateTopics / DeleteTopics
      // outside-in guards. Internal topics are exempt.
      val rejected = new util.ArrayList[ProduceRequestData.TopicProduceData]()
      produceRequest.data.topicData.forEach { t =>
        if (isReservedTenantNamespace(t.name)) {
          rejected.add(t)
          t.partitionData.forEach { p =>
            invalidLogicalTopicResponses +=
              new TopicPartition(t.name, p.index) -> new PartitionResponse(Errors.INVALID_TOPIC_EXCEPTION)
          }
        }
      }
      rejected.forEach(t => produceRequest.data.topicData.remove(t))
    }

    // IN rewrite — topic names in the request are logical; authorization,
    // metadataCache.contains() and replicaManager.handleProduceAppend() below
    // all key on physical names. Mutate the request's TopicProduceData names
    // in place: the request object is a deserialised value owned by this
    // handler and will be GC'd after sendResponseCallback fires.
    if (tenantScoped) {
      produceRequest.data.topicData.forEach(t => t.setName(tenantCtx.toPhysical(t.name)))
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
      val physicalResponseStatus = responseStatus ++ unauthorizedTopicResponses ++ nonExistingTopicResponses ++ invalidRequestResponses
      var errorInResponse = false

      val nodeEndpoints = new mutable.HashMap[Int, Node]
      // Iterate with PHYSICAL TopicPartitions — getCurrentLeader uses
      // replicaManager / metadataCache, both of which key on physical names.
      physicalResponseStatus.foreachEntry { (topicPartition, status) =>
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

      // Reserved-form rejections are already INVALID_TOPIC_EXCEPTION entries
      // that bypassed replicaManager — they're not in physicalResponseStatus,
      // so the acks=0 close-on-error decision below must observe them
      // separately, otherwise a tenant producing only reserved-form names
      // with acks=0 would get a silent no-op instead of a connection close.
      if (invalidLogicalTopicResponses.nonEmpty) errorInResponse = true

      // OUT rewrite — the response map keys (TopicPartitions) carry the
      // physical topic name and the PartitionResponse values may embed the
      // physical name inside `errorMessage` (e.g. validators that quote the
      // offending topic) AND inside `recordErrors[].message` (KIP-467, v8+
      // per-record validation failures emitted by LogValidator, which quote
      // the physical TopicPartition — "in topic partition <tenant>.<topic>-N").
      // Rewrite the key via toLogical and every embedded string via
      // scrubMessage. PartitionResponse fields are public and at this point we
      // own the reference, but RecordError.message is final, so each
      // RecordError must be rebuilt rather than mutated. currentLeader and
      // other state set above are preserved.
      // invalidLogicalTopicResponses is already keyed by the LOGICAL name the
      // client sent; merge after the toLogical pass so it doesn't strip the
      // tenant-prefix portion the caller intentionally included.
      val mergedResponseStatus: Map[TopicPartition, PartitionResponse] = {
        val rewritten: Map[TopicPartition, PartitionResponse] =
          if (tenantScoped) physicalResponseStatus.map { case (tp, pr) =>
            if (pr.errorMessage != null) pr.errorMessage = scrubMessage(pr.errorMessage, tenantCtx)
            if (pr.recordErrors != null && !pr.recordErrors.isEmpty) {
              pr.recordErrors = pr.recordErrors.asScala.map { re =>
                if (re.message != null)
                  new ProduceResponse.RecordError(re.batchIndex, scrubMessage(re.message, tenantCtx))
                else re
              }.asJava
            }
            new TopicPartition(tenantCtx.toLogical(tp.topic), tp.partition) -> pr
          } else physicalResponseStatus
        rewritten ++ invalidLogicalTopicResponses
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
        transactionalId = physicalTransactionalId,
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

    val tenantCtx = tenantContextFor(request)
    val tenantScoped = tenantCtx.effectiveTenant.isPresent
    // Unsafe-request guard (see TenantContext.isUnsafe). Refuse every requested
    // partition with TOPIC_AUTHORIZATION_FAILED so the caller cannot piggyback
    // off the listener binding, an attacker-controlled `__tenant_` prefix on an
    // unbound listener, or a principal/listener mismatch. The check uses the
    // pre-fetch-context view of fetchData() so it does not depend on a session.
    if (tenantCtx.isUnsafe) {
      val refused = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]()
      fetchRequest.fetchData(topicNames).forEach { (tip, _) =>
        refused.put(tip, FetchResponse.partitionResponse(tip, Errors.TOPIC_AUTHORIZATION_FAILED))
      }
      val refusedResponse =
        FetchResponse.of(Errors.NONE, 0, FetchMetadata.INVALID_SESSION_ID, refused, Collections.emptyList())
      // The TopicIdPartition keys may carry physical names (v13+ resolved via
      // metadataCache); strip the tenant prefix before we ship the refusal so
      // a privileged caller never observes the physical namespace.
      rewriteFetchResponseToLogical(refusedResponse, tenantCtx)
      requestChannel.sendResponse(request, refusedResponse, None)
      return
    }

    val rawFetchData = fetchRequest.fetchData(topicNames)
    val rawForgottenTopics = fetchRequest.forgottenTopics(topicNames)

    // Tenant IN rewrite (logical → physical) happens BEFORE the fetch context
    // is built so session tracking, replicaManager, updateAndGenerateResponseData
    // and recordBytesOutMetric all run in physical space end-to-end. For v13+
    // the TIPs are already physical (resolved from metadataCache.topicIdsToNames);
    // for v0-12 they carry the logical name from the wire.
    //
    // Foreign TIPs (outside the tenant namespace, e.g. tenant beta probing a
    // topic id whose name resolves to "acme.orders") MUST NOT enter
    // fetchManager.newContext. If they did, an incremental fetch on the same
    // session would later iterate them via foreachPartition with no record of
    // their foreign-ness — and any permissive authorizer would then read foreign
    // data. We collect foreign TIPs in a side buffer and merge them into the
    // erroneous bucket after context creation, so the session is built only on
    // tenant-owned partitions.
    val foreignFetchTips = new util.LinkedHashMap[TopicIdPartition, FetchRequest.PartitionData]()
    // Outside-in pollution guard for non-tenant callers on cluster-wide listeners
    // (mirror of the Produce/DeleteTopics guards). Without it, a `User:* READ
    // Topic:*` cluster admin could drain tenant logs by naming `acme.orders`
    // directly: tenantScoped is false (non-tenant principal), the normalise
    // step is skipped, and rawFetchData flows straight into the authz/metadata
    // check — which a permissive ACL passes. Surface reserved-prefix entries
    // as UNKNOWN_TOPIC_OR_PARTITION (via foreignFetchTips → foreignErroneous
    // below) so the response is indistinguishable from a real miss; this
    // closes both the data leak and the existence oracle.
    //
    // The follower-fetch skip must be pinned to the inter-broker listener
    // (see `isInterBrokerFollowerFetch`). `FetchRequest.isFromFollower` is
    // purely wire-derived (replicaId >= 0); a CLUSTER_ACTION holder on a
    // cluster-wide listener could otherwise spoof it and read tenant
    // physical topics by name. A legitimate replica always arrives on the
    // inter-broker listener; any other replicaId>=0 fetch is treated as a
    // regular consumer fetch and subject to the outside-in guard.
    val outsideInGuardActive = !tenantScoped && !isInterBrokerFollowerFetch(request, fetchRequest) && !tenantConfig.allTenants.isEmpty
    val fetchData = if (tenantScoped) {
      val rewritten = new util.LinkedHashMap[TopicIdPartition, FetchRequest.PartitionData](rawFetchData.size)
      rawFetchData.forEach { (tip, data) =>
        normaliseTenantTopicForFetch(tip, tenantCtx, versionId) match {
          case Some(physicalTip) => rewritten.put(physicalTip, data)
          case None => foreignFetchTips.put(tip, data)
        }
      }
      rewritten
    } else if (outsideInGuardActive) {
      val kept = new util.LinkedHashMap[TopicIdPartition, FetchRequest.PartitionData](rawFetchData.size)
      rawFetchData.forEach { (tip, data) =>
        if (tip.topic != null && isReservedTenantNamespace(tip.topic)) foreignFetchTips.put(tip, data)
        else kept.put(tip, data)
      }
      kept
    } else rawFetchData
    val forgottenTopics = if (tenantScoped) {
      val rewritten = new util.ArrayList[TopicIdPartition](rawForgottenTopics.size)
      rawForgottenTopics.forEach { tip =>
        normaliseTenantTopicForFetch(tip, tenantCtx, versionId) match {
          case Some(physicalTip) => rewritten.add(physicalTip)
          case None =>
            // Forgotten ids that no longer resolve (topic deleted between
            // fetches) or that resolved to a foreign name still need to be
            // removed from the session. Passing the tip through as-is lets
            // FetchSession.update drop the stale entry; otherwise it would
            // linger forever until session reset.
            rewritten.add(tip)
        }
      }
      rewritten
    } else rawForgottenTopics

    val fetchContext = fetchManager.newContext(
      fetchRequest.version,
      fetchRequest.metadata,
      fetchRequest.isFromFollower,
      fetchData,
      forgottenTopics,
      topicNames,
      // Pass the requester's principal name so the session cache can refuse
      // cross-principal lookups (foreign sessionId can't disrupt or read
      // another tenant's session). See FetchSession.principalName.
      Some(request.context.principal.toString))

    val erroneous = mutable.ArrayBuffer[(TopicIdPartition, FetchResponseData.PartitionData)]()
    val interesting = mutable.ArrayBuffer[(TopicIdPartition, FetchRequest.PartitionData)]()
    // Foreign TIPs are surfaced as UNKNOWN_TOPIC_OR_PARTITION but must NOT enter
    // the `partitions` map handed to FullFetchContext.updateAndGenerateResponseData:
    // that map seeds the session cache via `new CachedPartition(part, fetchData.get(part), ...)`
    // and would (a) NPE for parts absent from fetchData, and (b) silently
    // resurrect the foreign TIP in the next incremental fetch on the same
    // session. Instead, we keep them in a side list and merge them into the
    // FetchResponse AFTER session creation, so the session never knows about
    // them. See `appendForeignErroneousRows` below.
    val foreignErroneous = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]()
    foreignFetchTips.forEach { (tip, _) =>
      foreignErroneous.put(tip, FetchResponse.partitionResponse(tip, Errors.UNKNOWN_TOPIC_OR_PARTITION))
    }
    // Defence-in-depth: a session entry that resolves to a physical name
    // outside the effective tenant must still be surfaced as
    // UNKNOWN_TOPIC_OR_PARTITION rather than fetched. For an Incremental
    // context this entry is in session.partitionMap, so it would be safe to
    // route through `erroneous` → `updates` (PartitionIterator finds it); for a
    // Full context we filtered foreign per-request entries out of fetchData
    // already, so anything reaching foreachPartition is owned by the tenant.
    // We still keep the check as a hard backstop.
    def isForeignForTenant(tip: TopicIdPartition): Boolean =
      tenantScoped && tip.topic != null && !isInternal(tip.topic) &&
        !tenantCtx.belongsToTenant(tip.topic)
    if (fetchRequest.isFromFollower) {
      // The follower must have ClusterAction on ClusterResource in order to fetch partition data.
      if (authHelper.authorize(request.context, CLUSTER_ACTION, CLUSTER, CLUSTER_NAME)) {
        fetchContext.foreachPartition { (topicIdPartition, data) =>
          if (topicIdPartition.topic == null)
            erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_ID)
          else if (isForeignForTenant(topicIdPartition))
            erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
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
        else if (isForeignForTenant(topicIdPartition))
          erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
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

    // the callback for process a fetch response, invoked before throttling
    def processResponseCallback(responsePartitionData: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      val partitions = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
      val reassigningPartitions = mutable.Set[TopicIdPartition]()
      val nodeEndpoints = new mutable.HashMap[Int, Node]
      responsePartitionData.foreach { case (tp, data) =>
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

      // Merge UNKNOWN_TOPIC_OR_PARTITION rows for foreign TIPs into the response.
      // These rows bypassed the session cache entirely (foreign TIPs were never
      // added to fetchData/partitions), so the session never gains a foothold
      // into another tenant's namespace. For v0-12 the topic name surfaced is
      // exactly the logical name the client sent on the wire; for v13+ the
      // topic field is not serialised so the choice does not matter.
      def appendForeignErroneousRows(fetchResponse: FetchResponse): Unit = {
        if (foreignErroneous.isEmpty) return
        val responses = fetchResponse.data.responses
        // Group consecutive foreign TIPs by (topicId, topic) so we emit one
        // FetchableTopicResponse per topic.
        var currentTopicResp: FetchResponseData.FetchableTopicResponse = null
        foreignErroneous.forEach { (tip, partData) =>
          if (currentTopicResp == null
              || (!currentTopicResp.topicId.equals(Uuid.ZERO_UUID) && !currentTopicResp.topicId.equals(tip.topicId))
              || (currentTopicResp.topicId.equals(Uuid.ZERO_UUID) && !currentTopicResp.topic.equals(tip.topicPartition.topic))) {
            currentTopicResp = new FetchResponseData.FetchableTopicResponse()
              .setTopic(tip.topicPartition.topic)
              .setTopicId(tip.topicId)
              .setPartitions(new util.ArrayList[FetchResponseData.PartitionData]())
            responses.add(currentTopicResp)
          }
          partData.setPartitionIndex(tip.partition)
          currentTopicResp.partitions.add(partData)
        }
      }

      if (fetchRequest.isFromFollower) {
        // We've already evaluated against the quota and are good to go. Just need to record it now.
        val fetchResponse = fetchContext.updateAndGenerateResponseData(partitions, Seq.empty.asJava)
        appendForeignErroneousRows(fetchResponse)
        val responseSize = KafkaApis.sizeOfThrottledPartitions(versionId, fetchResponse, quotas.leader)
        quotas.leader.record(responseSize)
        val responsePartitionsSize = fetchResponse.data().responses().stream().mapToInt(_.partitions().size()).sum()
        trace(s"Sending Fetch response with partitions.size=$responsePartitionsSize, " +
          s"metadata=${fetchResponse.sessionId}")
        recordBytesOutMetric(fetchResponse)
        rewriteFetchResponseToLogical(fetchResponse, tenantCtx)
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

        appendForeignErroneousRows(fetchResponse)
        recordBytesOutMetric(fetchResponse)
        rewriteFetchResponseToLogical(fetchResponse, tenantCtx)
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

    val tenantCtx = tenantContextFor(request)
    val tenantScoped = tenantCtx.effectiveTenant.isPresent

    // Refuse any unsafe request — every topic gets TOPIC_AUTHORIZATION_FAILED
    // carrying the wire (logical) name; replicaManager is never consulted.
    if (tenantCtx.isUnsafe) {
      val refused = offsetRequest.topics.asScala.map { topic =>
        new ListOffsetsTopicResponse()
          .setName(topic.name)
          .setPartitions(topic.partitions.asScala.map(p =>
            buildErrorResponse(Errors.TOPIC_AUTHORIZATION_FAILED, p)).asJava)
      }.toSeq
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new ListOffsetsResponse(new ListOffsetsResponseData()
          .setThrottleTimeMs(requestThrottleMs)
          .setTopics(refused.asJava)))
      return
    }

    // Reserved-form / overlong / invalid-logical-form guards (tenant path) and
    // outside-in pollution guard (non-tenant path). Same shape as Produce; the
    // rejected topics surface as INVALID_TOPIC_EXCEPTION at the partition level,
    // their LOGICAL names preserved on the wire, and they never reach
    // authorization or replicaManager. The accepted topics continue into the
    // normal flow below.
    val invalidLogicalResponses = new ArrayBuffer[ListOffsetsTopicResponse]()
    if (tenantScoped) {
      val rejected = new util.ArrayList[ListOffsetsTopic]()
      offsetRequest.topics.forEach { t =>
        if (tenantCtx.isReservedPhysicalForm(t.name)
            || tenantCtx.isOverlongLogicalForm(t.name)
            || tenantCtx.isInvalidLogicalForm(t.name)) {
          rejected.add(t)
          invalidLogicalResponses += new ListOffsetsTopicResponse()
            .setName(t.name)
            .setPartitions(t.partitions.asScala.map(p =>
              buildErrorResponse(Errors.INVALID_TOPIC_EXCEPTION, p)).asJava)
        }
      }
      rejected.forEach(t => offsetRequest.topics.remove(t))
    } else if (!tenantConfig.allTenants.isEmpty) {
      val rejected = new util.ArrayList[ListOffsetsTopic]()
      offsetRequest.topics.forEach { t =>
        if (isReservedTenantNamespace(t.name)) {
          rejected.add(t)
          invalidLogicalResponses += new ListOffsetsTopicResponse()
            .setName(t.name)
            .setPartitions(t.partitions.asScala.map(p =>
              buildErrorResponse(Errors.INVALID_TOPIC_EXCEPTION, p)).asJava)
        }
      }
      rejected.forEach(t => offsetRequest.topics.remove(t))
    }

    // IN rewrite — logical → physical for the surviving topics. Auth and
    // replicaManager both key on physical names from here on.
    if (tenantScoped) {
      offsetRequest.topics.forEach(t => t.setName(tenantCtx.toPhysical(t.name)))
    }

    // duplicatePartitions was computed once in the ListOffsetsRequest
    // constructor over the *logical* names the client sent. After the IN
    // rewrite the topic names in the request body no longer match those keys,
    // so ReplicaManager.fetchOffset (which compares against the post-rewrite
    // names) would never flag a tenant's duplicate partition and the request
    // would slip past INVALID_REQUEST. Recompute the set from the current
    // (physical) shape when a rewrite happened; otherwise keep the cached one.
    val duplicatePartitions: Set[TopicPartition] = if (tenantScoped) {
      val seen = scala.collection.mutable.Set[TopicPartition]()
      val dups = scala.collection.mutable.Set[TopicPartition]()
      offsetRequest.topics.forEach { t =>
        t.partitions.forEach { p =>
          val tp = new TopicPartition(t.name, p.partitionIndex)
          if (!seen.add(tp)) dups.add(tp)
        }
      }
      dups.toSet
    } else {
      offsetRequest.duplicatePartitions().asScala.toSet
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
      // OUT rewrite — each topic name on the wire is physical; rewrite back to
      // logical before the response goes out. The invalid-logical-form entries
      // already carry the logical name the client sent, so they are merged
      // after the toLogical pass to avoid stripping the prefix the caller
      // intentionally typed.
      if (tenantScoped) {
        mergedResponses.foreach(r => r.setName(tenantCtx.toLogical(r.name)))
      }
      val finalResponses = mergedResponses ++ invalidLogicalResponses
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new ListOffsetsResponse(new ListOffsetsResponseData()
          .setThrottleTimeMs(requestThrottleMs)
          .setTopics(finalResponses.asJava)))
    }

    if (authorizedRequestInfo.isEmpty) {
      sendResponseCallback(Seq.empty)
    } else {
      replicaManager.fetchOffset(authorizedRequestInfo, duplicatePartitions,
        offsetRequest.isolationLevel(), offsetRequest.replicaId(), clientId, correlationId, version,
        buildErrorResponse, sendResponseCallback, offsetRequest.timeoutMs())
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

    val tenantCtx = tenantContextFor(request)
    // Unsafe-request guard (see TenantContext.isUnsafe). Three rejection cases:
    //   1. Privileged caller on a tenant-bound listener (no principal tenant) —
    //      would otherwise have names rewritten into the tenant's namespace and
    //      pollute it. PROMPT.md flags this as data corruption.
    //   2. Principal/listener tenant mismatch — operator-controlled values
    //      disagree; broker won't pick a winner.
    //   3. Untrusted `__tenant_` prefix arriving on a listener without
    //      TenantPrincipalBuilder — closes the SASL_PLAIN username spoof.
    // For named topics: TOPIC_AUTHORIZATION_FAILED per requested topic; for
    // isAllTopics: empty response, the caller cannot see the namespace.
    if (tenantCtx.isUnsafe) {
      sendMetadataAuthorizationFailure(request, metadataRequest, requestVersion)
      return
    }

    // Check if topicId is presented firstly.
    val topicIds = metadataRequest.topicIds.asScala.toSet.filterNot(_ == Uuid.ZERO_UUID)
    val useTopicId = topicIds.nonEmpty

    // Only get topicIds and topicNames when supporting topicId
    val rawUnknownTopicIds = topicIds.filter(metadataCache.getTopicName(_).isEmpty)
    val rawKnownTopicNames = topicIds.flatMap(metadataCache.getTopicName)

    // IN rewrite — the request carries logical names; downstream metadataCache /
    // authorization / auto-topic-creation all operate on physical names.
    //   - isAllTopics: scope the visible cluster down to topics owned by this
    //     tenant (their physical prefix matches), plus pristine internal topics
    //     that pass through untouched.
    //   - useTopicId: lookup by id is already physical; just scope it to topics
    //     belonging to this tenant. IDs whose resolved name is foreign get the
    //     same UNKNOWN_TOPIC_ID shape as IDs the broker has never seen — the
    //     response cannot be used to probe foreign-topic existence.
    //   - explicit logical names: map each through toPhysical(...).
    // For non-tenant requests, tenantCtx is none() and these all reduce to
    // identity, so existing single-tenant behaviour is unchanged.
    val tenantScoped = tenantCtx.effectiveTenant.isPresent

    val (unknownTopicIds, knownTopicNames) = if (tenantScoped && useTopicId) {
      // For tenant-scoped id lookups, demote every id whose resolved name lies
      // outside the tenant's namespace (and is not an internal topic) to the
      // unknown set — same wire shape as IDs the broker actually does not know.
      val foreignKnownIds = topicIds.filter(id =>
        metadataCache.getTopicName(id) match {
          case opt if opt.isEmpty => false
          case opt => opt.exists(name => !tenantCtx.belongsToTenant(name) && !isInternal(name))
        })
      (rawUnknownTopicIds ++ foreignKnownIds,
        rawKnownTopicNames.filter(t => tenantCtx.belongsToTenant(t) || isInternal(t)))
    } else (rawUnknownTopicIds, rawKnownTopicNames)

    val unknownTopicIdsTopicMetadata = unknownTopicIds.map(topicId =>
        metadataResponseTopic(Errors.UNKNOWN_TOPIC_ID, null, topicId, isInternal = false, util.Collections.emptyList())).toSeq

    // Pre-rejection guard for explicit-name lookups:
    //   - Reserved physical form: a tenant asking for metadata about
    //     `acme.orders` would otherwise have it rewritten to `acme.acme.orders`;
    //     that physical topic doesn't exist (assuming the CreateTopics guard is
    //     intact) and the response would falsely report it unknown.
    //   - Over-long logical name: prefixing `<tenantId>.` would push the
    //     physical form past Kafka's 249-char cap; we'd then either request
    //     a name the cluster can't carry or surface a controller-side error
    //     containing the physical form. Refuse here, carrying the logical
    //     name back unchanged.
    //   - Invalid logical form (`""`, `.`, `..`, illegal chars): `Topic.isValid`
    //     would refuse the rewritten name; even passing the lookup unchanged
    //     would surface a downstream error with the physical form embedded.
    // All three cases surface as INVALID_TOPIC_EXCEPTION with the name preserved.
    def isInvalidTenantLookup(name: String): Boolean =
      tenantCtx.isReservedPhysicalForm(name) ||
        tenantCtx.isOverlongLogicalForm(name) ||
        tenantCtx.isInvalidLogicalForm(name)

    val reservedPhysicalForm: Seq[MetadataResponseTopic] =
      if (tenantScoped && !metadataRequest.isAllTopics && !useTopicId) {
        metadataRequest.topics.asScala.toSeq
          .filter(isInvalidTenantLookup)
          .map(name => metadataResponseTopic(
            Errors.INVALID_TOPIC_EXCEPTION,
            name,
            Uuid.ZERO_UUID,
            isInternal(name),
            util.Collections.emptyList()))
      } else Seq.empty

    val topics = if (metadataRequest.isAllTopics) {
      val all = metadataCache.getAllTopics()
      if (tenantScoped) all.filter(t => tenantCtx.belongsToTenant(t) || isInternal(t)) else all
    } else if (useTopicId) {
      knownTopicNames
    } else if (tenantScoped) {
      metadataRequest.topics.asScala.toSet
        .filterNot(isInvalidTenantLookup)
        .map(tenantCtx.toPhysical)
    } else {
      metadataRequest.topics.asScala.toSet.map(tenantCtx.toPhysical)
    }

    val authorizedForDescribeTopics = authHelper.filterByAuthorized(request.context, DESCRIBE, TOPIC,
      topics, logIfDenied = !metadataRequest.isAllTopics)(identity)
    var (authorizedTopics, unauthorizedForDescribeTopics) = topics.partition(authorizedForDescribeTopics.contains)
    var unauthorizedForCreateTopics = Set[String]()
    // Outside-in pollution via Metadata auto-create: a super-user on a
    // non-tenant listener with allowAutoTopicCreation=true requesting
    // "acme.foo" would otherwise trigger getTopicMetadata's auto-create path
    // and the controller would materialise a literal `acme.foo`. Tenant acme
    // would then see logical `foo` in ListTopics. Symmetric to the dispatch
    // guard in handleCreateTopicsRequest. Internal topics are exempt; describe
    // of existing tenant-prefixed topics is unaffected (metadataCache.contains
    // gates the auto-create branch).
    var pollutionRejectedTopics = Set[String]()

    if (authorizedTopics.nonEmpty) {
      val nonExistingTopics = authorizedTopics.filterNot(metadataCache.contains)
      if (metadataRequest.allowAutoTopicCreation && config.autoCreateTopicsEnable && nonExistingTopics.nonEmpty) {
        if (!tenantScoped && tenantConfig.allTenants.asScala.nonEmpty) {
          // STRUCTURAL CHECK (F7) — mirrors `isReservedTenantNamespace` above
          // rather than the locally-bound `allTenants` snapshot. A heterogeneous
          // broker without `gamma` in its config must still refuse to
          // auto-create `gamma.foo` if `gamma` is a syntactically valid tenant
          // id, because `gamma` may be bound on another broker. The outer gate
          // (`tenantConfig.allTenants.asScala.nonEmpty`) preserves the
          // stock-Kafka case where no tenants are configured anywhere on this
          // broker — see the helper docstring for the full rationale.
          pollutionRejectedTopics = nonExistingTopics.filter(isReservedTenantNamespace)
          if (pollutionRejectedTopics.nonEmpty) {
            authorizedTopics = authorizedTopics.diff(pollutionRejectedTopics)
          }
        }
        val stillNonExisting = nonExistingTopics.diff(pollutionRejectedTopics)
        if (stillNonExisting.nonEmpty &&
            !authHelper.authorize(request.context, CREATE, CLUSTER, CLUSTER_NAME, logIfDenied = false)) {
          val authorizedForCreateTopics = authHelper.filterByAuthorized(request.context, CREATE, TOPIC,
            stillNonExisting)(identity)
          unauthorizedForCreateTopics = stillNonExisting.diff(authorizedForCreateTopics)
          authorizedTopics = authorizedTopics.diff(unauthorizedForCreateTopics)
        }
      }
    }

    val pollutionRejectedTopicMetadata = pollutionRejectedTopics.map(topic =>
      metadataResponseTopic(Errors.INVALID_TOPIC_EXCEPTION, topic, Uuid.ZERO_UUID,
        isInternal(topic), util.Collections.emptyList()))

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
      topicMetadata ++ unauthorizedForCreateTopicMetadata ++ unauthorizedForDescribeTopicMetadata ++
      pollutionRejectedTopicMetadata

    // OUT rewrite — at this point every MetadataResponseTopic carries the
    // physical name (from metadataCache / auth lookups / autocreate errors).
    // Strip the prefix so the client sees the logical name it asked for,
    // including in error paths (TOPIC_AUTHORIZATION_FAILED, UNKNOWN_TOPIC,
    // INVALID_TOPIC_EXCEPTION). For non-tenant requests this is a no-op.
    if (tenantScoped) {
      completeTopicMetadata.foreach { t =>
        if (t.name != null) t.setName(tenantCtx.toLogical(t.name))
      }
    }
    // reservedPhysicalForm entries (reserved + over-long) already carry the
    // logical name the client sent (e.g. "acme.orders"); appending after the
    // OUT rewrite avoids toLogical stripping the prefix they intentionally
    // included, and avoids any attempt to rewrite an over-long name.
    val finalTopicMetadata = completeTopicMetadata ++ reservedPhysicalForm

    val brokers = metadataCache.getAliveBrokerNodes(request.context.listenerName)

    trace("Sending topic metadata %s and brokers %s for correlation id %d to client %s".format(finalTopicMetadata.mkString(","),
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
         finalTopicMetadata.asJava,
         clusterAuthorizedOperations
      ))
  }

  // Refuse to honour a Metadata request whose listener binds it into a tenant
  // namespace but whose principal has no tenant prefix — without this guard the
  // broker would silently rewrite the request into the tenant's namespace and
  // a super-user could pollute it. For explicit-topic requests we mark each
  // topic TOPIC_AUTHORIZATION_FAILED; for isAllTopics we return an empty
  // topic list (the caller does not get to enumerate the tenant's namespace).
  private def sendMetadataAuthorizationFailure(request: RequestChannel.Request,
                                                metadataRequest: MetadataRequest,
                                                requestVersion: Short): Unit = {
    val refused: Seq[MetadataResponseTopic] =
      if (metadataRequest.isAllTopics) Seq.empty
      else metadataRequest.topics.asScala.toSeq.map(name =>
        metadataResponseTopic(Errors.TOPIC_AUTHORIZATION_FAILED, name, Uuid.ZERO_UUID, isInternal(name), util.Collections.emptyList()))
    val brokers = metadataCache.getAliveBrokerNodes(request.context.listenerName)
    val controllerId = metadataCache.getControllerId.flatMap {
      case ZkCachedControllerId(id) => Some(id)
      case KRaftCachedControllerId(_) => metadataCache.getRandomAliveBrokerId
    }
    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      MetadataResponse.prepareResponse(
        requestVersion,
        requestThrottleMs,
        brokers.toList.asJava,
        clusterId,
        controllerId.getOrElse(MetadataResponse.NO_CONTROLLER_ID),
        refused.asJava,
        Int.MinValue))
  }

  def handleDescribeTopicPartitionsRequest(request: RequestChannel.Request): Unit = {
    describeTopicPartitionsRequestHandler match {
      case Some(handler) => {
        val response = handler.handleDescribeTopicPartitionsRequest(request)

        // Outside-in: a non-tenant caller on the cluster-wide listener must not
        // observe tenant-prefixed physical topics. fetchAllTopics paths in the
        // delegate handler iterate metadataCache.getAllTopics() directly (and
        // an `ALLOW User:* DESCRIBE Topic:*` ACL — typical for cluster admin —
        // passes every authz check), so the response leaks `acme.orders` etc.
        // Tenant principals never reach this handler — DESCRIBE_TOPIC_PARTITIONS
        // is outside TENANT_ALLOWED_APIS — so the guard only fires for
        // non-tenant callers. For fetchAllTopics, silently drop reserved
        // entries (matching the existing per-topic ACL-deny shape, which also
        // drops silently on the all-topics path). For explicit lists, replace
        // the entry with the same TOPIC_AUTHORIZATION_FAILED shape an authz
        // refusal already produces, so the wire response is indistinguishable.
        val tenantCtxFilter = tenantContextFor(request)
        if (!tenantCtxFilter.effectiveTenant.isPresent) {
          val req = request.body[DescribeTopicPartitionsRequest]
          val fetchAllTopics = req.data.topics.isEmpty
          val it = response.topics.iterator
          while (it.hasNext) {
            val topic = it.next()
            if (isReservedTenantNamespace(topic.name)) {
              if (fetchAllTopics) {
                it.remove()
              } else {
                topic.setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
                topic.setTopicId(Uuid.ZERO_UUID)
                topic.setIsInternal(false)
                topic.setPartitions(java.util.Collections.emptyList())
              }
            }
          }
        }

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
    val tenantCtx = tenantContextFor(request)
    val tenantScoped = tenantCtx.effectiveTenant.isPresent

    val futures = new mutable.ArrayBuffer[CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]](groups.size)
    groups.forEach { groupOffsetFetch =>
      val logicalGroupId = groupOffsetFetch.groupId
      rewriteTenantGroupId(tenantCtx, logicalGroupId) match {
        case Left(err) =>
          futures += CompletableFuture.completedFuture(OffsetFetchResponse.groupError(
            groupOffsetFetch, err, request.header.apiVersion()))
        case Right(physicalGroupId) =>
          if (!authHelper.authorize(request.context, DESCRIBE, GROUP, physicalGroupId)) {
            futures += CompletableFuture.completedFuture(OffsetFetchResponse.groupError(
              groupOffsetFetch, Errors.GROUP_AUTHORIZATION_FAILED, request.header.apiVersion()))
          } else {
            // Mutate for the coordinator call. The success-path response will
            // have its groupId reset to the logical form on its way out.
            groupOffsetFetch.setGroupId(physicalGroupId)
            // Rewrite topic names (specific-topics path). Reserved-physical
            // forms are silently dropped — the coordinator can't have stored
            // offsets under such names through this path anyway.
            if (tenantScoped && groupOffsetFetch.topics != null) {
              val accepted = new util.ArrayList[OffsetFetchRequestData.OffsetFetchRequestTopics]()
              groupOffsetFetch.topics.forEach { t =>
                try {
                  t.setName(tenantCtx.toPhysical(t.name))
                  accepted.add(t)
                } catch {
                  case _: org.apache.kafka.common.errors.InvalidTopicException => // drop
                }
              }
              groupOffsetFetch.setTopics(accepted)
            }
            val coordFuture =
              if (groupOffsetFetch.topics == null)
                fetchAllOffsetsForGroup(request.context, groupOffsetFetch, requireStable)
              else
                fetchOffsetsForGroup(request.context, groupOffsetFetch, requireStable)
            // Restore logical groupId + rewrite physical topic names back
            // to the logical form before the response reaches the wire.
            futures += coordFuture.thenApply { resp =>
              resp.setGroupId(logicalGroupId)
              rewriteOffsetFetchResponseTopicsToLogical(resp, tenantCtx)
              resp
            }
          }
      }
    }

    CompletableFuture.allOf(futures.toArray: _*).handle[Unit] { (_, _) =>
      val groupResponses = new ArrayBuffer[OffsetFetchResponseData.OffsetFetchResponseGroup](futures.size)
      futures.foreach(future => groupResponses += future.get())
      requestHelper.sendMaybeThrottle(request, new OffsetFetchResponse(groupResponses.asJava, request.context.apiVersion))
    }
  }

  // Rewrite topic names on an OffsetFetch response group back to the LOGICAL
  // form. Foreign-namespace and unprefixed names are dropped defensively:
  // every commit went through toPhysical, so any stored topic missing the
  // tenant prefix is either pre-tenancy residue or an injection attempt —
  // either way the tenant must not see it. Internal topic names are
  // impossible here (offsets aren't stored against `__consumer_offsets`).
  private def rewriteOffsetFetchResponseTopicsToLogical(
    resp: OffsetFetchResponseData.OffsetFetchResponseGroup,
    tenantCtx: TenantContext
  ): Unit = {
    if (!tenantCtx.effectiveTenant.isPresent || resp.topics == null) return
    val kept = new util.ArrayList[OffsetFetchResponseData.OffsetFetchResponseTopics]()
    resp.topics.forEach { t =>
      if (tenantCtx.belongsToTenant(t.name)) {
        t.setName(tenantCtx.toLogical(t.name))
        kept.add(t)
      }
    }
    resp.setTopics(kept)
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

  // Tenant gate for FindCoordinator. Returns a non-empty Errors when the tenant
  // context forbids the lookup for the given key type (unsafe context, or a
  // SHARE key from a tenant — share groups remain out of scope). None means
  // the call may proceed to getCoordinator with the rewritten key. GROUP and
  // TRANSACTION are both admitted for tenants in Phase 3b.
  private def rejectTenantFindCoordinator(
    tenantCtx: TenantContext,
    keyType: Byte
  ): Option[Errors] = {
    if (!tenantCtx.effectiveTenant.isPresent && !tenantCtx.isUnsafe) return None
    if (tenantCtx.isUnsafe) {
      // Privileged-on-tenant / mismatch / spoof: surface the auth error that
      // matches the caller-visible resource type so the client sees the same
      // shape it would for a regular unauthorised request.
      return Some(keyType match {
        case t if t == CoordinatorType.GROUP.id => Errors.GROUP_AUTHORIZATION_FAILED
        case t if t == CoordinatorType.TRANSACTION.id => Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED
        case _ => Errors.INVALID_REQUEST
      })
    }
    // Tenant principal: GROUP and TRANSACTION are admitted (rewritten to
    // their respective physical forms); SHARE and any unknown key type
    // remain refused at this gate.
    keyType match {
      case t if t == CoordinatorType.GROUP.id => None
      case t if t == CoordinatorType.TRANSACTION.id => None
      case _ => Some(Errors.INVALID_REQUEST)
    }
  }

  // Per-key rewrite for FindCoordinator. Identity for non-tenant callers and
  // for key types we don't rewrite (SHARE — share-group coordination is out
  // of scope). For tenant GROUP / TRANSACTION keys, returns Left(error) if
  // the logical key carries a cross-tenant `__tenant_<other>.` prefix or
  // any reserved-prefix-without-separator shape. Without this guard a single
  // bad key in a multi-key v4+ request would surface as a request-level
  // exception instead of per-key {GROUP,TRANSACTIONAL_ID}_AUTHORIZATION_FAILED.
  private def rewriteFindCoordinatorKey(
    tenantCtx: TenantContext,
    keyType: Byte,
    logicalKey: String
  ): Either[Errors, String] = {
    // Outside-in: non-tenant caller naming `__tenant_<known>.foo` would resolve
    // the tenant's GROUP or TRANSACTION coordinator and learn the broker that
    // hosts the partition — and, with cluster-admin ACLs, fence the slot. Refuse
    // with the auth-failed wire shape so the response is indistinguishable from
    // an ACL refusal on the same key.
    if (!tenantCtx.effectiveTenant.isPresent
        && isReservedTenantPrincipalNamespace(logicalKey)) {
      val err = keyType match {
        case t if t == CoordinatorType.GROUP.id => Errors.GROUP_AUTHORIZATION_FAILED
        case t if t == CoordinatorType.TRANSACTION.id => Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED
        case _ => Errors.INVALID_REQUEST
      }
      return Left(err)
    }
    keyType match {
      case t if t == CoordinatorType.GROUP.id =>
        try Right(tenantCtx.toPhysicalGroup(logicalKey))
        catch {
          case _: IllegalArgumentException => Left(Errors.GROUP_AUTHORIZATION_FAILED)
        }
      case t if t == CoordinatorType.TRANSACTION.id =>
        try Right(tenantCtx.toPhysicalTxnId(logicalKey))
        catch {
          case _: IllegalArgumentException => Left(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
        }
      case _ => Right(logicalKey)
    }
  }

  private def handleFindCoordinatorRequestV4AndAbove(request: RequestChannel.Request): Unit = {
    val findCoordinatorRequest = request.body[FindCoordinatorRequest]
    val tenantCtx = tenantContextFor(request)
    val keyType = findCoordinatorRequest.data.keyType
    val tenantReject = rejectTenantFindCoordinator(tenantCtx, keyType)

    val coordinators = findCoordinatorRequest.data.coordinatorKeys.asScala.map { logicalKey =>
      tenantReject match {
        case Some(err) =>
          new FindCoordinatorResponseData.Coordinator()
            .setKey(logicalKey)
            .setErrorCode(err.code)
            .setHost(Node.noNode.host)
            .setNodeId(Node.noNode.id)
            .setPort(Node.noNode.port)
        case None =>
          rewriteFindCoordinatorKey(tenantCtx, keyType, logicalKey) match {
            case Left(keyErr) =>
              // Cross-tenant prefix attempt — refuse with the auth-shape error
              // for this key, never letting toPhysicalGroup's exception bubble
              // out and fail the whole multi-key request.
              new FindCoordinatorResponseData.Coordinator()
                .setKey(logicalKey)
                .setErrorCode(keyErr.code)
                .setHost(Node.noNode.host)
                .setNodeId(Node.noNode.id)
                .setPort(Node.noNode.port)
            case Right(physicalKey) =>
              val (error, node) = getCoordinator(request, keyType, physicalKey)
              new FindCoordinatorResponseData.Coordinator()
                // Echo the LOGICAL key back — the tenant never sees the physical form.
                .setKey(logicalKey)
                .setErrorCode(error.code)
                .setHost(node.host)
                .setNodeId(node.id)
                .setPort(node.port)
          }
      }
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
    val tenantCtx = tenantContextFor(request)
    val keyType = findCoordinatorRequest.data.keyType
    val tenantReject = rejectTenantFindCoordinator(tenantCtx, keyType)

    val (error, node) = tenantReject match {
      case Some(err) => (err, Node.noNode)
      case None =>
        rewriteFindCoordinatorKey(tenantCtx, keyType, findCoordinatorRequest.data.key) match {
          case Left(keyErr) => (keyErr, Node.noNode)
          case Right(physicalKey) => getCoordinator(request, keyType, physicalKey)
        }
    }
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
      // Outside-in guard: refuse a cluster-wide caller naming `__tenant_<known>.*`
      // before it reaches the coordinator. See handleDeleteGroupsRequest.
      if (isReservedTenantPrincipalNamespace(groupId)) {
        response.groups.add(DescribeGroupsResponse.groupError(
          groupId,
          Errors.GROUP_AUTHORIZATION_FAILED
        ))
      } else if (!authHelper.authorize(request.context, DESCRIBE, GROUP, groupId)) {
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
    val tenantCtx = tenantContextFor(request)
    val hasClusterDescribe = authHelper.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME, logIfDenied = false)

    groupCoordinator.listGroups(
      request.context,
      listGroupsRequest.data
    ).handle[Unit] { (response, exception) =>
      if (exception != null) {
        requestHelper.sendMaybeThrottle(request, listGroupsRequest.getErrorResponse(exception))
      } else {
        // Hide tenant-internal coordinator records (`__tenant_<known>.*`) from
        // a cluster-wide listing so the wire response cannot be used to
        // enumerate which tenants exist. Tenant principals never reach this
        // handler — LIST_GROUPS is not in TENANT_ALLOWED_APIS, so the
        // dispatch gate refuses them upstream — but a non-tenant cluster
        // admin holding wildcard DESCRIBE would otherwise see every tenant's
        // physical group ids verbatim.
        val visibleGroups = response.groups.asScala.filter { group =>
          val authorised = hasClusterDescribe ||
            authHelper.authorize(request.context, DESCRIBE, GROUP, group.groupId, logIfDenied = false)
          val tenantInternal = !tenantCtx.effectiveTenant.isPresent &&
            isReservedTenantPrincipalNamespace(group.groupId)
          authorised && !tenantInternal
        }
        val listGroupsResponse = new ListGroupsResponse(response.setGroups(visibleGroups.asJava))
        requestHelper.sendMaybeThrottle(request, listGroupsResponse)
      }
    }
  }

  // Rewrite a logical group id to its physical form, refusing unsafe contexts
  // (privileged-on-tenant-listener / mismatch / spoof) and cross-tenant
  // prefix attempts. Returns Left(error) if the caller must be rejected
  // outright; Right(physical) if the call may proceed against the rewritten
  // id. Identity for non-tenant contexts.
  private def rewriteTenantGroupId(
    tenantCtx: TenantContext,
    logicalGroupId: String
  ): Either[Errors, String] = {
    if (tenantCtx.isUnsafe) {
      Left(Errors.GROUP_AUTHORIZATION_FAILED)
    } else if (!tenantCtx.effectiveTenant.isPresent
               && isReservedTenantPrincipalNamespace(logicalGroupId)) {
      // Outside-in: non-tenant caller naming `__tenant_<known>.foo` directly
      // addresses a tenant's coordinator slot. Refuse with the auth-failed
      // wire shape the coordinator would emit for an unauthorised access so
      // the response cannot be used to confirm tenant existence.
      Left(Errors.GROUP_AUTHORIZATION_FAILED)
    } else {
      try Right(tenantCtx.toPhysicalGroup(logicalGroupId))
      catch {
        // A tenant addressing `__tenant_other.foo` — refuse with the same
        // shape an authz failure would produce, so the wire response never
        // hints at the foreign tenant's existence.
        case _: IllegalArgumentException => Left(Errors.GROUP_AUTHORIZATION_FAILED)
      }
    }
  }

  private def rewriteTenantTxnId(
    tenantCtx: TenantContext,
    logicalTxnId: String
  ): Either[Errors, String] = {
    // Mirror rewriteTenantGroupId: unsafe context refuses with the wire-shape
    // error a foreign-tenant or unauthorised principal would already produce,
    // so callers cannot distinguish "you can't see this tenant" from
    // "rewrite failed" from "auth failed".
    if (tenantCtx.isUnsafe) {
      Left(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
    } else if (!tenantCtx.effectiveTenant.isPresent
               && isReservedTenantPrincipalNamespace(logicalTxnId)) {
      // Outside-in: non-tenant caller naming `__tenant_<known>.foo` fences the
      // tenant's producer slot in `__transaction_state`. The CreateTopics
      // pollution guard refuses naming the storage topic; this refuses naming
      // its keys.
      Left(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
    } else {
      try Right(tenantCtx.toPhysicalTxnId(logicalTxnId))
      catch {
        case _: IllegalArgumentException => Left(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
      }
    }
  }

  def handleJoinGroupRequest(
    request: RequestChannel.Request,
    requestLocal: RequestLocal
  ): CompletableFuture[Unit] = {
    val joinGroupRequest = request.body[JoinGroupRequest]
    val tenantCtx = tenantContextFor(request)

    rewriteTenantGroupId(tenantCtx, joinGroupRequest.data.groupId) match {
      case Left(err) =>
        requestHelper.sendMaybeThrottle(request, joinGroupRequest.getErrorResponse(err.exception))
        CompletableFuture.completedFuture[Unit](())
      case Right(physicalGroupId) =>
        joinGroupRequest.data.setGroupId(physicalGroupId)
        if (!authHelper.authorize(request.context, READ, GROUP, physicalGroupId)) {
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
  }

  def handleSyncGroupRequest(
    request: RequestChannel.Request,
    requestLocal: RequestLocal
  ): CompletableFuture[Unit] = {
    val syncGroupRequest = request.body[SyncGroupRequest]
    val tenantCtx = tenantContextFor(request)

    if (!syncGroupRequest.areMandatoryProtocolTypeAndNamePresent()) {
      // Starting from version 5, ProtocolType and ProtocolName fields are mandatory.
      requestHelper.sendMaybeThrottle(request, syncGroupRequest.getErrorResponse(Errors.INCONSISTENT_GROUP_PROTOCOL.exception))
      CompletableFuture.completedFuture[Unit](())
    } else rewriteTenantGroupId(tenantCtx, syncGroupRequest.data.groupId) match {
      case Left(err) =>
        requestHelper.sendMaybeThrottle(request, syncGroupRequest.getErrorResponse(err.exception))
        CompletableFuture.completedFuture[Unit](())
      case Right(physicalGroupId) =>
        syncGroupRequest.data.setGroupId(physicalGroupId)
        if (!authHelper.authorize(request.context, READ, GROUP, physicalGroupId)) {
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
  }

  def handleDeleteGroupsRequest(
    request: RequestChannel.Request,
    requestLocal: RequestLocal
  ): CompletableFuture[Unit] = {
    val deleteGroupsRequest = request.body[DeleteGroupsRequest]
    val groups = deleteGroupsRequest.data.groupsNames.asScala.distinct

    // Outside-in guard: a privileged caller on a non-tenant listener naming
    // `__tenant_<known>.foo` would directly delete that tenant's coordinator
    // record. The dispatch gate above only refuses tenant-scoped principals;
    // a cluster-wide admin who types the physical prefix would otherwise pass
    // straight through. Refuse reserved-form names with the same error code
    // the regular authz path uses so the wire shape stays uniform.
    val (reservedGroups, eligibleGroups) = groups.partition(isReservedTenantPrincipalNamespace)

    val (authorizedGroups, unauthorizedGroups) =
      authHelper.partitionSeqByAuthorized(request.context, DELETE, GROUP, eligibleGroups)(identity)

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

      reservedGroups.foreach { groupId =>
        response.results.add(new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId(groupId)
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code))
      }

      requestHelper.sendMaybeThrottle(request, new DeleteGroupsResponse(response))
    }
  }

  def handleHeartbeatRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val heartbeatRequest = request.body[HeartbeatRequest]
    val tenantCtx = tenantContextFor(request)

    rewriteTenantGroupId(tenantCtx, heartbeatRequest.data.groupId) match {
      case Left(err) =>
        requestHelper.sendMaybeThrottle(request, heartbeatRequest.getErrorResponse(err.exception))
        CompletableFuture.completedFuture[Unit](())
      case Right(physicalGroupId) =>
        heartbeatRequest.data.setGroupId(physicalGroupId)
        if (!authHelper.authorize(request.context, READ, GROUP, physicalGroupId)) {
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
  }

  def handleLeaveGroupRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val leaveGroupRequest = request.body[LeaveGroupRequest]
    val tenantCtx = tenantContextFor(request)

    rewriteTenantGroupId(tenantCtx, leaveGroupRequest.data.groupId) match {
      case Left(err) =>
        requestHelper.sendMaybeThrottle(request, leaveGroupRequest.getErrorResponse(err.exception))
        CompletableFuture.completedFuture[Unit](())
      case Right(physicalGroupId) =>
        leaveGroupRequest.data.setGroupId(physicalGroupId)
        if (!authHelper.authorize(request.context, READ, GROUP, physicalGroupId)) {
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
    //
    // Multi-tenancy filter: when the request arrives on a tenant-bound
    // listener OR carries a `__tenant_` principal (`effectiveTenant.isPresent`),
    // restrict the advertised surface to KafkaApis.TENANT_ALLOWED_APIS.
    // Reasons:
    //   (a) Honest tenant clients (admin / streams / connect) discover the
    //       surface through ApiVersions; advertising APIs the dispatch gate
    //       (line ~685) will then refuse causes confusing wire-level errors
    //       and noisy log spam.
    //   (b) Advertising the FULL broker surface leaks capability fingerprint
    //       — controller APIs, share-group APIs, internal txn-coordinator
    //       APIs — useful only as reconnaissance for a tenant attacker. The
    //       dispatch gate already refuses these requests but the response of
    //       this handler itself is a separate channel.
    // Note: the filter is keyed on listener binding too, so an unauthenticated
    // client probing a tenant listener gets the same filtered view as a
    // post-auth tenant principal. This matches the rest of the branch where
    // the listener owns the tenant binding (see TenantContext.effectiveTenant).
    def createResponseCallback(requestThrottleMs: Int): ApiVersionsResponse = {
      val apiVersionRequest = request.body[ApiVersionsRequest]
      if (apiVersionRequest.hasUnsupportedRequestVersion) {
        apiVersionRequest.getErrorResponse(requestThrottleMs, Errors.UNSUPPORTED_VERSION.exception)
      } else if (!apiVersionRequest.isValid) {
        apiVersionRequest.getErrorResponse(requestThrottleMs, Errors.INVALID_REQUEST.exception)
      } else {
        val response = apiVersionManager.apiVersionResponse(requestThrottleMs, request.header.apiVersion() < 4)
        val tenantCtx = tenantContextFor(request)
        if (tenantCtx.effectiveTenant.isPresent) {
          val allowed = KafkaApis.TENANT_ALLOWED_APIS
          val iter = response.data.apiKeys.iterator
          while (iter.hasNext) {
            val entry = iter.next
            if (!allowed.contains(ApiKeys.forId(entry.apiKey))) {
              iter.remove()
            }
          }
        }
        response
      }
    }
    requestHelper.sendResponseMaybeThrottle(request, createResponseCallback)
  }

  def handleDeleteRecordsRequest(request: RequestChannel.Request): Unit = {
    val deleteRecordsRequest = request.body[DeleteRecordsRequest]

    val tenantCtx = tenantContextFor(request)
    val tenantScoped = tenantCtx.effectiveTenant.isPresent

    // Refuse any unsafe request — every partition gets TOPIC_AUTHORIZATION_FAILED
    // keyed by the LOGICAL name the client sent; replicaManager is never consulted.
    if (tenantCtx.isUnsafe) {
      val refused = deleteRecordsRequest.data.topics.asScala.flatMap { topic =>
        topic.partitions.asScala.map(p =>
          new TopicPartition(topic.name, p.partitionIndex) ->
            new DeleteRecordsPartitionResult()
              .setPartitionIndex(p.partitionIndex)
              .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
              .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code))
      }.toMap
      sendDeleteRecordsResponse(request, refused)
      return
    }

    // Reserved-form / overlong / invalid-logical-form guards (tenant path) and
    // outside-in pollution guard (non-tenant path). Rejected partitions surface
    // as INVALID_TOPIC_EXCEPTION keyed by the LOGICAL name the client sent —
    // they never reach authorization or replicaManager.
    val invalidLogicalResponses = mutable.Map[TopicPartition, DeleteRecordsPartitionResult]()
    if (tenantScoped) {
      val rejected = new util.ArrayList[DeleteRecordsTopic]()
      deleteRecordsRequest.data.topics.forEach { t =>
        if (tenantCtx.isReservedPhysicalForm(t.name)
            || tenantCtx.isOverlongLogicalForm(t.name)
            || tenantCtx.isInvalidLogicalForm(t.name)) {
          rejected.add(t)
          t.partitions.forEach { p =>
            invalidLogicalResponses += new TopicPartition(t.name, p.partitionIndex) ->
              new DeleteRecordsPartitionResult()
                .setPartitionIndex(p.partitionIndex)
                .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
                .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code)
          }
        }
      }
      rejected.forEach(t => deleteRecordsRequest.data.topics.remove(t))
    } else if (!tenantConfig.allTenants.isEmpty) {
      val rejected = new util.ArrayList[DeleteRecordsTopic]()
      deleteRecordsRequest.data.topics.forEach { t =>
        if (isReservedTenantNamespace(t.name)) {
          rejected.add(t)
          t.partitions.forEach { p =>
            invalidLogicalResponses += new TopicPartition(t.name, p.partitionIndex) ->
              new DeleteRecordsPartitionResult()
                .setPartitionIndex(p.partitionIndex)
                .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
                .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code)
          }
        }
      }
      rejected.forEach(t => deleteRecordsRequest.data.topics.remove(t))
    }

    // IN rewrite — logical → physical for surviving topics. Auth and
    // replicaManager both key on physical names from here on.
    if (tenantScoped) {
      deleteRecordsRequest.data.topics.forEach(t => t.setName(tenantCtx.toPhysical(t.name)))
    }

    val unauthorizedTopicResponses = mutable.Map[TopicPartition, DeleteRecordsPartitionResult]()
    val nonExistingTopicResponses = mutable.Map[TopicPartition, DeleteRecordsPartitionResult]()
    val authorizedForDeleteTopicOffsets = mutable.Map[TopicPartition, Long]()

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
      else
        authorizedForDeleteTopicOffsets += (topicPartition -> offset)
    }

    // the callback for sending a DeleteRecordsResponse
    def sendResponseCallback(authorizedTopicResponses: Map[TopicPartition, DeleteRecordsPartitionResult]): Unit = {
      val physicalResponseStatus = authorizedTopicResponses ++ unauthorizedTopicResponses ++ nonExistingTopicResponses
      physicalResponseStatus.foreachEntry { (topicPartition, status) =>
        if (status.errorCode != Errors.NONE.code) {
          debug("DeleteRecordsRequest with correlation id %d from client %s on partition %s failed due to %s".format(
            request.header.correlationId,
            request.header.clientId,
            topicPartition,
            Errors.forCode(status.errorCode).exceptionName))
        }
      }

      // OUT rewrite — physical → logical on the TopicPartition keys. The
      // invalidLogicalResponses entries are already keyed by the LOGICAL name
      // the client sent; merge after the toLogical pass so the prefix the
      // caller intentionally typed is not stripped from them.
      val rewrittenLogical: Map[TopicPartition, DeleteRecordsPartitionResult] =
        if (tenantScoped) physicalResponseStatus.map { case (tp, pr) =>
          new TopicPartition(tenantCtx.toLogical(tp.topic), tp.partition) -> pr
        }.toMap
        else physicalResponseStatus.toMap
      val mergedResponseStatus = rewrittenLogical ++ invalidLogicalResponses
      sendDeleteRecordsResponse(request, mergedResponseStatus)
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

  private def sendDeleteRecordsResponse(request: RequestChannel.Request,
                                        mergedResponseStatus: Map[TopicPartition, DeleteRecordsPartitionResult]): Unit = {
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

  def handleInitProducerIdRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val initProducerIdRequest = request.body[InitProducerIdRequest]
    val logicalTransactionalId = initProducerIdRequest.data.transactionalId

    // Tenant-scope guard. The unsafe-context refusal (privileged-on-tenant /
    // mismatch / spoof) must short-circuit before we touch the coordinator
    // because the listener binding alone cannot disambiguate which tenant
    // a producer id should be allocated under.
    val tenantCtx = tenantContextFor(request)
    if (tenantCtx.isUnsafe) {
      val err =
        if (logicalTransactionalId != null) Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED
        else Errors.CLUSTER_AUTHORIZATION_FAILED
      requestHelper.sendErrorResponseMaybeThrottle(request, err.exception)
      return
    }
    // Outside-in: a non-tenant caller naming `__tenant_<known>.foo` would
    // allocate (and fence) a known tenant's producer slot. Refuse with the
    // auth-failed wire shape so the response cannot be used as a probe for
    // whether the tenant exists or has an outstanding producer.
    if (!tenantCtx.effectiveTenant.isPresent
        && isReservedTenantPrincipalNamespace(logicalTransactionalId)) {
      requestHelper.sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
      return
    }
    // Phase 3b: rewrite the tenant's logical transactional id to its physical
    // form (__tenant_<id>.<name>) before authorisation and before reaching
    // the transaction coordinator. The shared __transaction_state log is
    // keyed by hash(transactionalId), so two tenants colliding on the same
    // external id MUST resolve to distinct coordinator records — that
    // separation is what the prefix buys us. Null passes through (idempotent
    // producers). Cross-tenant `__tenant_<other>.foo` is refused defensively;
    // without this the tenant would be able to write into another tenant's
    // coordinator state simply by spelling their prefix.
    val transactionalId =
      try tenantCtx.toPhysicalTxnId(logicalTransactionalId)
      catch {
        case _: IllegalArgumentException =>
          requestHelper.sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
          return
      }

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
        // Trace LOGICAL — operators on the tenant listener should see the
        // id the client sent, not the physical bookkeeping name.
        trace(s"Completed $logicalTransactionalId's InitProducerIdRequest with result $result from client ${request.header.clientId}.")
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
    val tenantCtx = tenantContextFor(request)
    val logicalTransactionalId = endTxnRequest.data.transactionalId

    // Rewrite the transactional id to its physical form before auth + coordinator
    // dispatch — `__transaction_state` shards by hash(transactionalId), so two
    // tenants reusing the same external id only stay isolated when the physical
    // form (`__tenant_<id>.<external>`) is what reaches the coordinator. An
    // unsafe context, or a tenant trying to End a foreign-tenant-prefixed id,
    // refuses with TRANSACTIONAL_ID_AUTHORIZATION_FAILED — the same wire shape
    // unauthenticated callers already see.
    rewriteTenantTxnId(tenantCtx, logicalTransactionalId) match {
      case Left(err) =>
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          new EndTxnResponse(new EndTxnResponseData()
              .setErrorCode(err.code)
              .setThrottleTimeMs(requestThrottleMs)))
      case Right(physicalTransactionalId) =>
        if (authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, physicalTransactionalId)) {
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
              trace(s"Completed ${logicalTransactionalId}'s EndTxnRequest " +
                s"with committed: ${endTxnRequest.data.committed}, " +
                s"errors: $error from client ${request.header.clientId}.")
              responseBody
            }
            requestHelper.sendResponseMaybeThrottle(request, createResponse)
          }

          txnCoordinator.handleEndTransaction(physicalTransactionalId,
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

    // #152: WriteTxnMarkers is a coordinator→leader RPC that bypasses every
    // request-side rewrite: the caller chooses producerId, coordinatorEpoch
    // AND the target TopicPartition list, and the handler `appendRecords` with
    // `internalTopicsAllowed=true`. On the legitimate path the transaction
    // coordinator issues the request over the inter-broker listener with the
    // inter-broker principal; that path MUST be preserved because the
    // coordinator legitimately writes commit/abort markers into tenant
    // partitions on behalf of the tenant's own EndTxn. Outside the
    // inter-broker listener, any caller — a cluster admin holding ALTER:CLUSTER
    // or CLUSTER_ACTION, a service account on the regular client listener —
    // could otherwise plant ABORT markers into `acme.orders-0` and erase
    // tenant-committed records (consumers in read-committed isolation would
    // skip them), or plant COMMIT markers and expose uncommitted state. Refuse
    // reserved-namespace partitions per-element with TOPIC_AUTHORIZATION_FAILED,
    // same shape as a regular ACL deny. The structural namespace check covers
    // every tenant prefix, including unbound ones (defends against pre-binding
    // pollution).
    val writeTxnMarkersInterBroker = {
      val ibl = config.interBrokerListenerName
      ibl != null && ibl == request.context.listenerName
    }
    var skippedMarkers = 0
    for (marker <- markers.asScala) {
      val producerId = marker.producerId
      val partitionsWithCompatibleMessageFormat = new mutable.ArrayBuffer[TopicPartition]

      val currentErrors = new ConcurrentHashMap[TopicPartition, Errors]()
      marker.partitions.forEach { partition =>
        if (!writeTxnMarkersInterBroker && isReservedTenantNamespace(partition.topic)) {
          currentErrors.put(partition, Errors.TOPIC_AUTHORIZATION_FAILED)
        } else {
          replicaManager.onlinePartition(partition) match {
            case Some(_)  =>
              partitionsWithCompatibleMessageFormat += partition
            case None =>
              currentErrors.put(partition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
          }
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

    // Phase 3b: rewrite tenant-scoped transactional ids and topic names to
    // their physical form before authorisation and coordinator dispatch. The
    // wire-form response is built from request data (the logical id and the
    // logical topic names), so the client sees the names it sent back. An
    // unsafe context (privileged-on-tenant / mismatch / spoof) short-circuits
    // before any other check because the listener binding alone cannot
    // disambiguate which tenant the call belongs to — the guard must run
    // before authorizeClusterOperation so we never leak cluster-auth status
    // through this code path either.
    val tenantCtx = tenantContextFor(request)
    if (tenantCtx.isUnsafe) {
      requestHelper.sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
      return
    }

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
      val logicalTransactionalId = transaction.transactionalId

      if (logicalTransactionalId == null)
        throw new InvalidRequestException("Transactional ID can not be null in request.")

      // toPhysicalTxnId is identity for non-tenant contexts (inter-broker v4+)
      // so the same call covers both code paths. A cross-tenant prefix from a
      // tenant client (`__tenant_other.x`) raises IllegalArgumentException →
      // refuse this transaction with TRANSACTIONAL_ID_AUTHORIZATION_FAILED.
      // Outside-in: a non-tenant client naming `__tenant_<known>.x` directly
      // would fence the tenant's coordinator slot; refuse per-transaction
      // with the same wire shape so the response cannot be used to probe
      // for tenant existence. v >= 4 callers are inter-broker (gated by
      // authorizeClusterOperation above) so the guard is a no-op for them
      // but harmless — the prefix on inter-broker AddPartitions only ever
      // arrives from the broker's own outgoing rewrite, which we minted.
      val maybePhysicalTransactionalId: Either[Errors, String] =
        if (!tenantCtx.effectiveTenant.isPresent && version < 4
            && isReservedTenantPrincipalNamespace(logicalTransactionalId))
          Left(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
        else
          try Right(tenantCtx.toPhysicalTxnId(logicalTransactionalId))
          catch { case _: IllegalArgumentException => Left(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED) }

      val logicalPartitionsToAdd = partitionsByTransaction.get(logicalTransactionalId).asScala

      // Versions < 4 come from clients and must be authorized to write for the given transaction and for the given topics.
      // Authorisation runs against the physical transactional id so the ACL
      // surface stays consistent with topic ACLs (which also key on physical).
      val refuseTxn = maybePhysicalTransactionalId match {
        case Left(error) => Some(error)
        case Right(physical) if version < 4 && !authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, physical) =>
          Some(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
        case _ => None
      }
      if (refuseTxn.isDefined) {
        addResultAndMaybeSendResponse(addPartitionsToTxnRequest.errorResponseForTransaction(
          logicalTransactionalId, refuseTxn.get))
      } else {
        val physicalTransactionalId = maybePhysicalTransactionalId.toOption.get
        val unauthorizedTopicErrors = mutable.Map[TopicPartition, Errors]()
        val nonExistingTopicErrors = mutable.Map[TopicPartition, Errors]()
        val authorizedLogicalPartitions = mutable.Set[TopicPartition]()

        // A tenant addressing the physical namespace directly (`acme.foo` while
        // already scoped to acme) has no right to do so — refuse the partition
        // up-front as TOPIC_AUTHORIZATION_FAILED rather than rewriting twice.
        val (reservedLogicalTps, rewriteableLogicalTps) = logicalPartitionsToAdd
          .partition(tp => tenantCtx.isReservedPhysicalForm(tp.topic))
        reservedLogicalTps.foreach(tp => unauthorizedTopicErrors += tp -> Errors.TOPIC_AUTHORIZATION_FAILED)

        // Build a logical→physical mapping so we can authorise + check
        // metadataCache + dispatch on physical, while preserving the logical
        // partitions for the on-wire response.
        val physicalByLogical: Map[TopicPartition, TopicPartition] =
          rewriteableLogicalTps.iterator.map { tp =>
            tp -> new TopicPartition(tenantCtx.toPhysical(tp.topic), tp.partition)
          }.toMap

        // Only request versions less than 4 need write authorization since they come from clients.
        val authorizedPhysicalTopics =
          if (version < 4)
            authHelper.filterByAuthorized(request.context, WRITE, TOPIC,
              physicalByLogical.values.toSeq.filterNot(tp => Topic.isInternal(tp.topic)))(_.topic)
          else
            physicalByLogical.values.iterator.map(_.topic).toSet
        for ((logicalTp, physicalTp) <- physicalByLogical) {
          if (!authorizedPhysicalTopics.contains(physicalTp.topic))
            unauthorizedTopicErrors += logicalTp -> Errors.TOPIC_AUTHORIZATION_FAILED
          else if (!metadataCache.contains(physicalTp))
            nonExistingTopicErrors += logicalTp -> Errors.UNKNOWN_TOPIC_OR_PARTITION
          else
            authorizedLogicalPartitions.add(logicalTp)
        }

        if (unauthorizedTopicErrors.nonEmpty || nonExistingTopicErrors.nonEmpty) {
          // Any failed partition check causes the entire transaction to fail. We send the appropriate error codes for the
          // partitions which failed, and an 'OPERATION_NOT_ATTEMPTED' error code for the partitions which succeeded
          // the authorization check to indicate that they were not added to the transaction.
          val partitionErrors = unauthorizedTopicErrors ++ nonExistingTopicErrors ++
            authorizedLogicalPartitions.map(_ -> Errors.OPERATION_NOT_ATTEMPTED)
          addResultAndMaybeSendResponse(AddPartitionsToTxnResponse.resultForTransaction(
            logicalTransactionalId, partitionErrors.asJava))
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
            addResultAndMaybeSendResponse(addPartitionsToTxnRequest.errorResponseForTransaction(
              logicalTransactionalId, finalError))
          }

          val authorizedPhysicalPartitions = authorizedLogicalPartitions.map(physicalByLogical)

          if (!transaction.verifyOnly) {
            txnCoordinator.handleAddPartitionsToTransaction(physicalTransactionalId,
              transaction.producerId,
              transaction.producerEpoch,
              authorizedPhysicalPartitions,
              sendResponseCallback,
              TransactionVersion.transactionVersionForAddPartitionsToTxn(addPartitionsToTxnRequest),
              requestLocal)
          } else {
            txnCoordinator.handleVerifyPartitionsInTransaction(physicalTransactionalId,
              transaction.producerId,
              transaction.producerEpoch,
              authorizedPhysicalPartitions,
              addResultAndMaybeSendResponse)
          }
        }
      }
    }
  }

  def handleAddOffsetsToTxnRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val addOffsetsToTxnRequest = request.body[AddOffsetsToTxnRequest]
    val logicalTransactionalId = addOffsetsToTxnRequest.data.transactionalId
    val logicalGroupId = addOffsetsToTxnRequest.data.groupId

    // Phase 3b: rewrite tenant-scoped transactional id + group id to their
    // physical form. Both keys share the __tenant_<id>.<name> encoding (see
    // TenantNamespace) but are independent values — same external name in
    // different tenants must resolve to distinct coordinator state. The
    // unsafe-context guard must run first (listener binding alone cannot
    // disambiguate which tenant owns the call); cross-tenant prefixes from a
    // tenant client are surfaced as TRANSACTIONAL_ID_AUTHORIZATION_FAILED /
    // GROUP_AUTHORIZATION_FAILED rather than silently passed through.
    val tenantCtx = tenantContextFor(request)
    if (tenantCtx.isUnsafe) {
      requestHelper.sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
      return
    }

    // Outside-in: non-tenant caller naming `__tenant_<known>.foo` for either
    // the transactional id or the group id is fencing tenant coordinator
    // state. Refuse with the same auth-failed wire shape an unauthorised call
    // would already produce.
    if (!tenantCtx.effectiveTenant.isPresent
        && isReservedTenantPrincipalNamespace(logicalTransactionalId)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AddOffsetsToTxnResponse(new AddOffsetsToTxnResponseData()
          .setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code)
          .setThrottleTimeMs(requestThrottleMs)))
      return
    }
    if (!tenantCtx.effectiveTenant.isPresent
        && isReservedTenantPrincipalNamespace(logicalGroupId)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AddOffsetsToTxnResponse(new AddOffsetsToTxnResponseData()
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)
          .setThrottleTimeMs(requestThrottleMs)))
      return
    }

    val transactionalId =
      try tenantCtx.toPhysicalTxnId(logicalTransactionalId)
      catch {
        case _: IllegalArgumentException =>
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new AddOffsetsToTxnResponse(new AddOffsetsToTxnResponseData()
              .setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code)
              .setThrottleTimeMs(requestThrottleMs)))
          return
      }

    val groupId =
      try tenantCtx.toPhysicalGroup(logicalGroupId)
      catch {
        case _: IllegalArgumentException =>
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new AddOffsetsToTxnResponse(new AddOffsetsToTxnResponseData()
              .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)
              .setThrottleTimeMs(requestThrottleMs)))
          return
      }

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
          // Trace logical so operators on the tenant listener see what the
          // client actually sent; physical id is internal bookkeeping.
          trace(s"Completed $logicalTransactionalId's AddOffsetsToTxnRequest for group $logicalGroupId on partition " +
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
    val tenantCtx = tenantContextFor(request)

    def rewriteResponseToLogical(response: TxnOffsetCommitResponse): Unit = {
      if (tenantCtx.effectiveTenant.isPresent) {
        response.data().topics().forEach(t => t.setName(tenantCtx.toLogical(t.name)))
      }
    }

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

      rewriteResponseToLogical(response)
      requestHelper.sendMaybeThrottle(request, response)
    }

    // Three independent identifiers in this request must be rewritten to physical:
    //   - transactionalId : keys `__transaction_state` by hash; two tenants
    //     sharing an external txn id stay isolated only on the physical form.
    //   - groupId         : keys `__consumer_offsets` partition via
    //     groupCoordinator.commitTransactionalOffsets → partitionFor(groupId);
    //     same reasoning — the wire form alone collides across tenants.
    //   - topic names     : the offsets themselves point at physical topics;
    //     the response must strip the prefix back before reaching the client.
    rewriteTenantTxnId(tenantCtx, txnOffsetCommitRequest.data.transactionalId) match {
      case Left(err) =>
        sendResponse(txnOffsetCommitRequest.getErrorResponse(err.exception))
        return CompletableFuture.completedFuture[Unit](())
      case Right(physicalTxnId) =>
        txnOffsetCommitRequest.data.setTransactionalId(physicalTxnId)
    }

    rewriteTenantGroupId(tenantCtx, txnOffsetCommitRequest.data.groupId) match {
      case Left(err) =>
        sendResponse(txnOffsetCommitRequest.getErrorResponse(err.exception))
        return CompletableFuture.completedFuture[Unit](())
      case Right(physicalGroupId) =>
        txnOffsetCommitRequest.data.setGroupId(physicalGroupId)
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
      // Reserved-physical-form refusal: see handleOffsetCommitRequest. A tenant
      // submitting `acme.foo` would round-trip to `acme.acme.foo` and collide
      // on the response builder's by-name map; refuse the whole request rather
      // than partially satisfy. Send the error response directly through
      // requestHelper rather than `sendResponse` — the topic names on the wire
      // must stay as the literal logical names the tenant submitted, and the
      // sendResponse helper would otherwise toLogical-strip `acme.foo` into
      // `foo`, losing the offender's identity on the wire.
      if (tenantCtx.effectiveTenant.isPresent &&
          txnOffsetCommitRequest.data.topics.asScala.exists(t => tenantCtx.isReservedPhysicalForm(t.name))) {
        requestHelper.sendMaybeThrottle(request,
          txnOffsetCommitRequest.getErrorResponse(new InvalidTopicException(
            "TxnOffsetCommit refused: one or more topic names use the reserved tenant-prefix form")))
        return CompletableFuture.completedFuture[Unit](())
      }
      if (tenantCtx.effectiveTenant.isPresent) {
        txnOffsetCommitRequest.data.topics.forEach(t => t.setName(tenantCtx.toPhysical(t.name)))
      }

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

          topic.partitions.forEach { partition =>
            if (metadataCache.getLeaderAndIsr(topic.name, partition.partitionIndex).nonEmpty) {
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

  // DESCRIBE_ACLS — tenant existence-oracle + namespace-enumeration guard.
  //
  // The stock handler accepts any AclBindingFilter and returns every matching
  // binding. With multi-tenancy that is a triple leak for a cluster-wide caller:
  //
  //  1. EXISTENCE ORACLE: a LITERAL filter on `Topic:acme.orders` returns the
  //     ACL bindings if the topic exists — telling the caller which tenants
  //     own which logical topic names. The same probe on a USER/GROUP/TXN
  //     resource of form `__tenant_acme.alice` enumerates tenant principals,
  //     consumer groups, and transactional ids.
  //  2. PREFIX ENUMERATION: a PREFIXED filter on `acme.` (TOPIC) or
  //     `__tenant_acme.` (GROUP/TXN/USER) dumps every binding under the
  //     tenant's namespace.
  //  3. WILDCARD DUMP: a ResourceType.ANY / null-name filter — the caller's
  //     legitimate cluster admin reach — returns every binding in the cluster
  //     unscrubbed, including every tenant binding.
  //
  // Two-layer defense, matching the pattern used by handleCreateAclsRequest /
  // handleDeleteAclsRequest:
  //
  //  L1 (filter validation): refuse with CLUSTER_AUTHORIZATION_FAILED if the
  //     filter EXPLICITLY NAMES a tenant namespace via the patternFilter
  //     (TOPIC + reserved-topic name with a KNOWN tenant id; GROUP/TXN/USER +
  //     any `__tenant_*` shape) or via the entryFilter principal
  //     (`User:__tenant_<id>.*` for any id, bound or not). This closes the
  //     existence-oracle, prefix-enumeration, and pre-binding pollution
  //     discovery vectors.
  //  L2 (response scrub): for wildcard / ResourceType.ANY queries (which we
  //     intentionally let through — they are the inherent reach of cluster
  //     admin), pass a scrub predicate down to AclApis that drops every
  //     binding whose pattern name or ACE principal lives in a tenant
  //     namespace before the response is serialized.
  //
  // Tenant callers (effectiveTenant.isPresent) bypass this guard: their
  // authorizeClusterOperation(DESCRIBE) inside AclApis already gates the API,
  // and any LITERAL name a tenant queries is part of their own namespace —
  // no cross-tenant leak is possible from a tenant principal.
  //
  // When no tenants are configured (tenantConfig.allTenants is empty) the
  // topic-namespace guard is a no-op (isReservedTenantNamespace short-circuits
  // on empty knownTenants). The principal-prefix guards stay structural and
  // continue to refuse `__tenant_*` names — this is desired: even on a node
  // with no listener bindings (e.g. a split-mode KRaft controller, or a broker
  // before any tenant is bound) the `__tenant_` prefix is reserved.
  def handleDescribeAcls(request: RequestChannel.Request): Unit = {
    val tenantCtx = tenantContextFor(request)
    if (tenantCtx.effectiveTenant.isPresent) {
      aclApis.handleDescribeAcls(request)
      return
    }

    val describeReq = request.body[DescribeAclsRequest]
    val filter = describeReq.filter
    val patternFilter = filter.patternFilter
    val entryFilter = filter.entryFilter
    val filterName = patternFilter.name
    val filterRT = patternFilter.resourceType
    val filterPT = patternFilter.patternType
    val filterPrincipal = entryFilter.principal

    // L1: a non-null name on a tenant-scoped resource type that lands in a
    // reserved tenant namespace is refused outright. ResourceType.ANY with a
    // tenant-looking name is also refused (the caller is asking the server
    // to test the name against every resource type — which is itself an
    // oracle). #157: the pattern-aware helpers also refuse PREFIXED and MATCH
    // patterns whose name is a prefix of a tenant namespace anchor — those
    // would otherwise enumerate planted bypass ACLs that LITERAL guards miss.
    val topicFilterNamesForeignTenant =
      filterName != null &&
        (filterRT == ResourceType.TOPIC || filterRT == ResourceType.ANY) &&
        isReservedTenantTopicAclName(filterName, filterPT)
    val principalScopedFilterNamesForeignTenant =
      filterName != null &&
        (filterRT == ResourceType.GROUP ||
          filterRT == ResourceType.TRANSACTIONAL_ID ||
          filterRT == ResourceType.USER ||
          filterRT == ResourceType.ANY) &&
        isReservedTenantPrincipalAclName(filterName, filterPT)
    val entryFilterTargetsForeignTenantPrincipal =
      isReservedUserPrincipalLiteral(filterPrincipal)

    if (topicFilterNamesForeignTenant ||
        principalScopedFilterNamesForeignTenant ||
        entryFilterTargetsForeignTenantPrincipal) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new DescribeAclsResponse(new DescribeAclsResponseData()
          .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code)
          .setErrorMessage("Filter names a reserved tenant namespace")
          .setThrottleTimeMs(requestThrottleMs),
          describeReq.version))
      return
    }

    // L2: defense-in-depth scrub of any tenant-owned binding from the
    // serialized response. Reuses the pattern-aware helpers so a PREFIXED
    // binding planted with a dotless name (`Topic:PREFIXED:acme`) is dropped
    // from the wildcard enumeration too — keeping the L1 filter check and L2
    // binding check in lock-step (#157).
    def isForeignTenantBinding(b: AclBinding): Boolean = {
      val pattern = b.pattern
      val name = pattern.name
      val rt = pattern.resourceType
      val pt = pattern.patternType
      val byPatternName =
        (rt == ResourceType.TOPIC && isReservedTenantTopicAclName(name, pt)) ||
          ((rt == ResourceType.GROUP ||
            rt == ResourceType.TRANSACTIONAL_ID ||
            rt == ResourceType.USER) && isReservedTenantPrincipalAclName(name, pt))
      val byPrincipal = isReservedUserPrincipalLiteral(b.entry.principal)
      byPatternName || byPrincipal
    }
    aclApis.handleDescribeAcls(request, b => !isForeignTenantBinding(b))
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

    // Outside-in existence/end-offset oracle: a cluster-wide caller naming a
    // tenant's physical topic (`acme.orders`) would otherwise pass the
    // CLUSTER_ACTION check above and receive real `leaderEpoch` + `endOffset`
    // from `replicaManager.lastOffsetForLeaderEpoch`. That leaks both topic
    // existence and the tenant's current end offsets, and — combined with a
    // spoofed follower epoch — feeds a truncation oracle. Refuse per-entry
    // with the exact wire shape `unauthorizedTopics` already produces above
    // (each partition gets TOPIC_AUTHORIZATION_FAILED, no leaderEpoch, no
    // endOffset), so the refusal is indistinguishable from a legitimate
    // DESCRIBE deny. OFFSET_FOR_LEADER_EPOCH is outside TENANT_ALLOWED_APIS,
    // so tenant principals never reach here — this only fires for non-tenant
    // callers. Per-entry (not whole-batch) so a cluster admin can still ask
    // about legitimate non-tenant topics in the same request.
    //
    // Inter-broker follower exemption (#146): a real replica fetcher names
    // tenant physical topics by design (replication is per physical partition).
    // `isInterBrokerFollowerOffsetForLeaderEpoch` pins that trust to the
    // inter-broker listener — without it, this guard breaks tenant-topic
    // replication. The exemption is symmetric with `isInterBrokerFollowerFetch`
    // on the Fetch path (#112).
    val (cleanAuthorizedTopics, pollutionRejectedTopics) =
      if (tenantContextFor(request).effectiveTenant.isPresent ||
          isInterBrokerFollowerOffsetForLeaderEpoch(request, offsetForLeaderEpoch)) {
        (authorizedTopics, Seq.empty[OffsetForLeaderTopic])
      } else {
        authorizedTopics.partition(t => !isReservedTenantNamespace(t.topic))
      }

    val endOffsetsForAuthorizedPartitions = replicaManager.lastOffsetForLeaderEpoch(cleanAuthorizedTopics)
    val refusedTopics = unauthorizedTopics ++ pollutionRejectedTopics
    val endOffsetsForUnauthorizedPartitions = refusedTopics.map { offsetForLeaderTopic =>
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
      (endOffsetsForAuthorizedPartitions ++ endOffsetsForUnauthorizedPartitions).asJava.iterator
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
    // Outside-in pollution guard. A privileged caller on a non-tenant listener
    // could otherwise alter `acme.orders` directly (retention, segment.bytes, ...)
    // — silently mutating a tenant's storage. Tenant principals never reach
    // here (ALTER_CONFIGS is outside TENANT_ALLOWED_APIS); the guard is purely
    // for cluster-wide callers. Mirrors the CreateTopics outside-in pattern.
    //
    // GROUP resources (`__tenant_<id>.<rest>`) are refused with
    // GROUP_AUTHORIZATION_FAILED and a NULL errorMessage — the physical group
    // id would otherwise serve as a presence oracle for a tenant's consumer
    // groups. ControllerApis #126 refuses the same shape on the controller; the
    // broker mirror prevents an unnecessary forward + reassemble round-trip and
    // keeps both chokepoints in sync if either is ever regressed in isolation.
    val pollutionRejected = new util.ArrayList[AlterConfigsResponseData.AlterConfigsResourceResponse]()
    if (!tenantContextFor(request).effectiveTenant.isPresent) {
      val keep = new AlterConfigsRequestData.AlterConfigsResourceCollection(remaining.resources.size)
      remaining.resources.forEach { r =>
        val rType = ConfigResource.Type.forId(r.resourceType)
        if (rType == ConfigResource.Type.TOPIC && isReservedTenantNamespace(r.resourceName)) {
          pollutionRejected.add(new AlterConfigsResponseData.AlterConfigsResourceResponse()
            .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code)
            .setErrorMessage("Topic name '" + r.resourceName + "' is reserved (tenant namespace prefix)")
            .setResourceType(r.resourceType)
            .setResourceName(r.resourceName))
        } else if (rType == ConfigResource.Type.GROUP && isReservedTenantPrincipalNamespace(r.resourceName)) {
          pollutionRejected.add(new AlterConfigsResponseData.AlterConfigsResourceResponse()
            .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)
            .setErrorMessage(null)
            .setResourceType(r.resourceType)
            .setResourceName(r.resourceName))
        } else {
          keep.add(r.duplicate())
        }
      }
      if (!pollutionRejected.isEmpty) {
        remaining.setResources(keep)
      }
    }
    def sendResponse(secondPart: Option[ApiMessage]): Unit = {
      secondPart match {
        case Some(result: AlterConfigsResponseData) =>
          if (!pollutionRejected.isEmpty) {
            // Defensive copy: don't assume result.responses is mutable.
            val merged = new util.ArrayList[AlterConfigsResponseData.AlterConfigsResourceResponse](
              result.responses.size + pollutionRejected.size)
            merged.addAll(result.responses)
            merged.addAll(pollutionRejected)
            result.setResponses(merged)
          }
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
    // Outside-in pollution guard; see handleAlterConfigsRequest. Mutating an
    // individual config key (cleanup.policy=delete on a compacted log) is the
    // same level of damage as a full alter — same defence. GROUP arm mirrors
    // the broker-side scrub added with #139.
    val pollutionRejected = new util.ArrayList[IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse]()
    if (!tenantContextFor(request).effectiveTenant.isPresent) {
      val keep = new IncrementalAlterConfigsRequestData.AlterConfigsResourceCollection(remaining.resources.size)
      remaining.resources.forEach { r =>
        val rType = ConfigResource.Type.forId(r.resourceType)
        if (rType == ConfigResource.Type.TOPIC && isReservedTenantNamespace(r.resourceName)) {
          pollutionRejected.add(new IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse()
            .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code)
            .setErrorMessage("Topic name '" + r.resourceName + "' is reserved (tenant namespace prefix)")
            .setResourceType(r.resourceType)
            .setResourceName(r.resourceName))
        } else if (rType == ConfigResource.Type.GROUP && isReservedTenantPrincipalNamespace(r.resourceName)) {
          pollutionRejected.add(new IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse()
            .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)
            .setErrorMessage(null)
            .setResourceType(r.resourceType)
            .setResourceName(r.resourceName))
        } else {
          keep.add(r.duplicate())
        }
      }
      if (!pollutionRejected.isEmpty) {
        remaining.setResources(keep)
      }
    }

    def sendResponse(secondPart: Option[ApiMessage]): Unit = {
      secondPart match {
        case Some(result: IncrementalAlterConfigsResponseData) =>
          if (!pollutionRejected.isEmpty) {
            // Defensive copy: don't assume result.responses is mutable.
            val merged = new util.ArrayList[IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse](
              result.responses.size + pollutionRejected.size)
            merged.addAll(result.responses)
            merged.addAll(pollutionRejected)
            result.setResponses(merged)
          }
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

  // CREATE_PARTITIONS — non-tenant outside-in guard. A cluster-wide admin
  // asking for `acme.foo` to grow from 3 to 30 partitions would silently
  // reshape a tenant's storage, breaking key-to-partition mappings (and the
  // consumer-group / transactional-id sharding that depends on them). Tenant
  // principals never reach here (CREATE_PARTITIONS is outside TENANT_ALLOWED_APIS);
  // the guard is for cluster-wide callers naming a known tenant namespace.
  // Mirrors handleAlterConfigsRequest split-and-merge.
  def handleCreatePartitionsRequest(request: RequestChannel.Request): Unit = {
    val original = request.body[CreatePartitionsRequest]
    val pollutionRejected = new util.ArrayList[CreatePartitionsResponseData.CreatePartitionsTopicResult]()
    val data = original.data()
    if (!tenantContextFor(request).effectiveTenant.isPresent) {
      val keep = new CreatePartitionsRequestData.CreatePartitionsTopicCollection(data.topics().size)
      data.topics().forEach { t =>
        if (isReservedTenantNamespace(t.name())) {
          pollutionRejected.add(new CreatePartitionsResponseData.CreatePartitionsTopicResult()
            .setName(t.name())
            .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code)
            .setErrorMessage("Topic name '" + t.name() + "' is reserved (tenant namespace prefix)"))
        } else {
          keep.add(t.duplicate())
        }
      }
      if (!pollutionRejected.isEmpty) {
        data.setTopics(keep)
      }
    }

    def sendResponse(secondPart: Option[ApiMessage]): Unit = {
      secondPart match {
        case Some(result: CreatePartitionsResponseData) =>
          if (!pollutionRejected.isEmpty) {
            val merged = new util.ArrayList[CreatePartitionsResponseData.CreatePartitionsTopicResult](
              result.results.size + pollutionRejected.size)
            merged.addAll(result.results)
            merged.addAll(pollutionRejected)
            result.setResults(merged)
          }
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new CreatePartitionsResponse(result.setThrottleTimeMs(requestThrottleMs)))
        case _ => handleInvalidVersionsDuringForwarding(request)
      }
    }
    if (data.topics().isEmpty) {
      sendResponse(Some(new CreatePartitionsResponseData()))
    } else {
      forwardingManager.forwardRequest(request,
        new CreatePartitionsRequest.Builder(data).build(request.header.apiVersion()),
        response => sendResponse(response.map(_.asInstanceOf[CreatePartitionsResponse].data())))
    }
  }

  // ELECT_LEADERS — non-tenant outside-in guard. A cluster-wide admin naming
  // `acme.orders-0` could force an unclean leader election on a tenant
  // partition. A null topicPartitions list means "elect for all eligible" — a
  // cluster-wide sweep that legitimately affects every topic and is NOT a
  // naming attack; we let that through (the cluster admin already has ALTER
  // on CLUSTER). Only explicit naming of a reserved namespace is refused.
  def handleElectLeadersRequest(request: RequestChannel.Request): Unit = {
    val original = request.body[ElectLeadersRequest]
    val data = original.data()
    val pollutionRejected = new util.ArrayList[ElectLeadersResponseData.ReplicaElectionResult]()
    if (!tenantContextFor(request).effectiveTenant.isPresent && data.topicPartitions() != null) {
      val keep = new ElectLeadersRequestData.TopicPartitionsCollection(data.topicPartitions().size)
      data.topicPartitions().forEach { tp =>
        if (isReservedTenantNamespace(tp.topic())) {
          val partitionResults = new util.ArrayList[ElectLeadersResponseData.PartitionResult](tp.partitions().size)
          tp.partitions().forEach { p =>
            partitionResults.add(new ElectLeadersResponseData.PartitionResult()
              .setPartitionId(p)
              .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code)
              .setErrorMessage("Topic name '" + tp.topic() + "' is reserved (tenant namespace prefix)"))
          }
          pollutionRejected.add(new ElectLeadersResponseData.ReplicaElectionResult()
            .setTopic(tp.topic())
            .setPartitionResult(partitionResults))
        } else {
          keep.add(tp.duplicate())
        }
      }
      if (!pollutionRejected.isEmpty) {
        data.setTopicPartitions(keep)
      }
    }

    def sendResponse(secondPart: Option[ApiMessage]): Unit = {
      secondPart match {
        case Some(result: ElectLeadersResponseData) =>
          if (!pollutionRejected.isEmpty) {
            val merged = new util.ArrayList[ElectLeadersResponseData.ReplicaElectionResult](
              result.replicaElectionResults.size + pollutionRejected.size)
            merged.addAll(result.replicaElectionResults)
            merged.addAll(pollutionRejected)
            result.setReplicaElectionResults(merged)
          }
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new ElectLeadersResponse(result.setThrottleTimeMs(requestThrottleMs)))
        case _ => handleInvalidVersionsDuringForwarding(request)
      }
    }
    // After filtering, an originally non-null list that became empty means
    // *every* requested topic was reserved. Don't degrade into a null-sweep
    // (which would elect leaders for the entire cluster including the tenant
    // topics we just refused) — return only the rejections.
    val onlyReservedNamed = !pollutionRejected.isEmpty && data.topicPartitions() != null && data.topicPartitions().isEmpty
    if (onlyReservedNamed) {
      sendResponse(Some(new ElectLeadersResponseData()))
    } else {
      // ElectLeadersRequest.Builder takes a flat Collection<TopicPartition>; flatten
      // the kept entries (or pass null straight through for a sweep).
      val flat: util.Collection[TopicPartition] = if (data.topicPartitions() == null) null else {
        val out = new util.ArrayList[TopicPartition]()
        data.topicPartitions().forEach { tp =>
          tp.partitions().forEach { p => out.add(new TopicPartition(tp.topic(), p)) }
        }
        out
      }
      forwardingManager.forwardRequest(request,
        new ElectLeadersRequest.Builder(
          ElectionType.valueOf(data.electionType()), flat, data.timeoutMs())
          .build(request.header.apiVersion()),
        response => sendResponse(response.map(_.asInstanceOf[ElectLeadersResponse].data())))
    }
  }

  // ALTER_PARTITION_REASSIGNMENTS — non-tenant outside-in guard. A cluster-wide
  // admin naming `acme.orders` could rewire replica placement on tenant
  // partitions or, by passing null replicas, silently CANCEL the tenant's
  // existing reassignments. Both are refused (the null-replicas "cancel" path
  // travels in the same partitions list — we don't need a special branch).
  def handleAlterPartitionReassignmentsRequest(request: RequestChannel.Request): Unit = {
    val original = request.body[AlterPartitionReassignmentsRequest]
    val data = original.data()
    val pollutionRejected = new util.ArrayList[AlterPartitionReassignmentsResponseData.ReassignableTopicResponse]()
    if (!tenantContextFor(request).effectiveTenant.isPresent) {
      val keep = new util.ArrayList[AlterPartitionReassignmentsRequestData.ReassignableTopic](data.topics().size)
      data.topics().forEach { t =>
        if (isReservedTenantNamespace(t.name())) {
          val partResults = new util.ArrayList[AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse](t.partitions().size)
          t.partitions().forEach { p =>
            partResults.add(new AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse()
              .setPartitionIndex(p.partitionIndex())
              .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code)
              .setErrorMessage("Topic name '" + t.name() + "' is reserved (tenant namespace prefix)"))
          }
          pollutionRejected.add(new AlterPartitionReassignmentsResponseData.ReassignableTopicResponse()
            .setName(t.name())
            .setPartitions(partResults))
        } else {
          keep.add(t)
        }
      }
      if (!pollutionRejected.isEmpty) {
        data.setTopics(keep)
      }
    }

    def sendResponse(secondPart: Option[ApiMessage]): Unit = {
      secondPart match {
        case Some(result: AlterPartitionReassignmentsResponseData) =>
          if (!pollutionRejected.isEmpty) {
            val merged = new util.ArrayList[AlterPartitionReassignmentsResponseData.ReassignableTopicResponse](
              result.responses.size + pollutionRejected.size)
            merged.addAll(result.responses)
            merged.addAll(pollutionRejected)
            result.setResponses(merged)
          }
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new AlterPartitionReassignmentsResponse(result.setThrottleTimeMs(requestThrottleMs)))
        case _ => handleInvalidVersionsDuringForwarding(request)
      }
    }
    if (data.topics().isEmpty) {
      sendResponse(Some(new AlterPartitionReassignmentsResponseData()))
    } else {
      forwardingManager.forwardRequest(request,
        new AlterPartitionReassignmentsRequest.Builder(data).build(request.header.apiVersion()),
        response => sendResponse(response.map(_.asInstanceOf[AlterPartitionReassignmentsResponse].data())))
    }
  }

  // LIST_PARTITION_REASSIGNMENTS — outside-in name leak. The controller returns
  // every in-flight reassignment by its physical topic name; a cluster-wide
  // caller would otherwise see `acme.orders`, `beta.events`, ... A cluster
  // admin has no business knowing the tenant physical names. Tenants don't
  // reach this RPC (not in TENANT_ALLOWED_APIS), so unconditionally stripping
  // any reserved-prefix topic from the response is correct — the only callers
  // we see here are non-tenant. Stripping is silent (no per-entry "rejected")
  // so the caller cannot probe which prefixes are tenant ids by watching for
  // refusals; an unknown `foo.bar` and a tenant `acme.orders` are equally
  // absent from the response.
  def handleListPartitionReassignmentsRequest(request: RequestChannel.Request): Unit = {
    def responseCallback(responseOpt: Option[AbstractResponse]): Unit = {
      responseOpt match {
        case Some(response) =>
          val r = response.asInstanceOf[ListPartitionReassignmentsResponse]
          if (r.data.topics != null) {
            val filtered = new util.ArrayList[ListPartitionReassignmentsResponseData.OngoingTopicReassignment](r.data.topics.size)
            r.data.topics.forEach { t =>
              if (!isReservedTenantNamespace(t.name)) filtered.add(t)
            }
            r.data.setTopics(filtered)
          }
          requestHelper.sendForwardedResponse(request, response)
        case None => handleInvalidVersionsDuringForwarding(request)
      }
    }
    forwardingManager.forwardRequest(request, responseCallback)
  }

  // CREATE_ACLS — outside-in privilege-escalation guard. A cluster-wide admin
  // writing an ACL against a tenant resource (`Topic:acme.orders`) or naming a
  // tenant principal (`User:__tenant_acme.alice`) would directly mutate tenant
  // authorization state — granting cross-tenant access, escalating a tenant
  // principal's reach, or shadowing tenant-owned ACLs. The Authorizer applies
  // these bindings literally; it has no notion of tenant boundaries. Refuse
  // each entry that names a KNOWN tenant namespace; forward the rest.
  //
  // The CreateAcls response carries no resource info per entry — Results is
  // positional w.r.t. the original creations list — so we must preserve the
  // index of each rejected entry and interleave the controller's responses
  // back in at the kept positions.
  def handleCreateAclsRequest(request: RequestChannel.Request): Unit = {
    val original = request.body[CreateAclsRequest]
    val creations = original.data.creations
    val rejections = new util.HashMap[Integer, CreateAclsResponseData.AclCreationResult]()
    if (!tenantContextFor(request).effectiveTenant.isPresent) {
      var i = 0
      while (i < creations.size) {
        val c = creations.get(i)
        val pt = PatternType.fromCode(c.resourcePatternType)
        // #157 — pattern-aware refusal. The structural check now extends to
        // PREFIXED ACLs whose name is a prefix of a tenant namespace anchor
        // (`Topic:PREFIXED:acme` for bound tenant `acme`, `Group:PREFIXED:__tenant_acme`
        // for any tenant). Also extended from TOPIC-only to the full set of
        // tenant-scoped resource types (GROUP / TRANSACTIONAL_ID / USER),
        // mirroring the controller-side refusal so broker and controller stay
        // in lock-step under bootstrap.controllers re-routing.
        val topicRefuse = c.resourceType == ResourceType.TOPIC.code &&
          isReservedTenantTopicAclName(c.resourceName, pt)
        val principalNsRefuse =
          (c.resourceType == ResourceType.GROUP.code ||
            c.resourceType == ResourceType.TRANSACTIONAL_ID.code ||
            c.resourceType == ResourceType.USER.code) &&
            isReservedTenantPrincipalAclName(c.resourceName, pt)
        val principalRefuse = isReservedUserPrincipalLiteral(c.principal)
        if (topicRefuse || principalNsRefuse || principalRefuse) {
          val what =
            if (topicRefuse || principalNsRefuse) "Resource name '" + c.resourceName + "'"
            else "Principal '" + c.principal + "'"
          rejections.put(i, new CreateAclsResponseData.AclCreationResult()
            .setErrorCode(Errors.INVALID_REQUEST.code)
            .setErrorMessage(what + " is reserved (tenant namespace prefix)"))
        }
        i += 1
      }
    }
    if (rejections.isEmpty) {
      forwardToController(request)
      return
    }
    val kept = new util.ArrayList[CreateAclsRequestData.AclCreation](creations.size - rejections.size)
    var k = 0
    while (k < creations.size) {
      if (!rejections.containsKey(k)) kept.add(creations.get(k))
      k += 1
    }
    def sendResponse(controllerResponseOpt: Option[AbstractResponse]): Unit = {
      val controllerResults: util.List[CreateAclsResponseData.AclCreationResult] = controllerResponseOpt match {
        case Some(r: CreateAclsResponse) => r.data.results
        case _ => Collections.emptyList()
      }
      val merged = new util.ArrayList[CreateAclsResponseData.AclCreationResult](creations.size)
      var idx = 0
      var fwdIdx = 0
      while (idx < creations.size) {
        if (rejections.containsKey(idx)) {
          merged.add(rejections.get(idx))
        } else {
          if (fwdIdx < controllerResults.size) merged.add(controllerResults.get(fwdIdx))
          else merged.add(new CreateAclsResponseData.AclCreationResult()
            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code))
          fwdIdx += 1
        }
        idx += 1
      }
      requestHelper.sendResponseMaybeThrottle(request, throttleMs =>
        new CreateAclsResponse(new CreateAclsResponseData()
          .setThrottleTimeMs(throttleMs)
          .setResults(merged)))
    }
    if (kept.isEmpty) {
      sendResponse(None)
    } else {
      val newData = new CreateAclsRequestData().setCreations(kept)
      forwardingManager.forwardRequest(request,
        new CreateAclsRequest.Builder(newData).build(request.header.apiVersion()),
        sendResponse)
    }
  }

  // DELETE_ACLS — outside-in revocation/leak guard. Two threats:
  //
  //  1. EXPLICIT-NAME revocation: a cluster-wide admin deletes
  //     `Topic:acme.orders` or `User:__tenant_acme.alice ALLOW ...` filters.
  //     This silently rips tenant ACLs out from under the tenant; refuse.
  //  2. INFORMATION LEAK: DeleteAclsResponse.MatchingAcls echoes resource and
  //     principal names of every ACL that matched the filter. Even when the
  //     caller submitted a wildcard filter (which we DO leave alone — that is
  //     the inherent reach of cluster admin, not a naming attack), entries
  //     belonging to a reserved tenant namespace must not be echoed back.
  //
  // Wildcard filters (null resourceNameFilter or null principalFilter) are
  // intentionally permitted to PROCEED. Filtering them out would either lie
  // about deletions that did happen (option C) or refuse legitimate cluster
  // admin operations (option A). We accept that wildcard-driven deletes are
  // cluster admin power; the leak guard scrubs the matching ACLs from the
  // response so the cluster admin doesn't get a free enumeration of tenant
  // ACLs via wildcard probes.
  def handleDeleteAclsRequest(request: RequestChannel.Request): Unit = {
    val original = request.body[DeleteAclsRequest]
    val filters = original.data.filters
    val rejections = new util.HashMap[Integer, DeleteAclsResponseData.DeleteAclsFilterResult]()
    val onClusterListener = !tenantContextFor(request).effectiveTenant.isPresent
    if (onClusterListener) {
      var i = 0
      while (i < filters.size) {
        val f = filters.get(i)
        val ft = PatternType.fromCode(f.patternTypeFilter)
        // #157 — pattern-aware refusal mirrors the CreateAcls path. Also widens
        // from TOPIC-only to the principal-prefix resource types so a wildcard
        // `Group:PREFIXED:__tenant_acme` explicit filter is refused upfront.
        val topicRefuse = f.resourceTypeFilter == ResourceType.TOPIC.code &&
          f.resourceNameFilter != null &&
          isReservedTenantTopicAclName(f.resourceNameFilter, ft)
        val principalNsRefuse =
          (f.resourceTypeFilter == ResourceType.GROUP.code ||
            f.resourceTypeFilter == ResourceType.TRANSACTIONAL_ID.code ||
            f.resourceTypeFilter == ResourceType.USER.code) &&
            f.resourceNameFilter != null &&
            isReservedTenantPrincipalAclName(f.resourceNameFilter, ft)
        val principalRefuse = f.principalFilter != null &&
          isReservedUserPrincipalLiteral(f.principalFilter)
        if (topicRefuse || principalNsRefuse || principalRefuse) {
          val what =
            if (topicRefuse || principalNsRefuse) "Resource filter '" + f.resourceNameFilter + "'"
            else "Principal filter '" + f.principalFilter + "'"
          rejections.put(i, new DeleteAclsResponseData.DeleteAclsFilterResult()
            .setErrorCode(Errors.INVALID_REQUEST.code)
            .setErrorMessage(what + " names a reserved tenant namespace"))
        }
        i += 1
      }
    }
    val kept = new util.ArrayList[DeleteAclsRequestData.DeleteAclsFilter](filters.size - rejections.size)
    var k = 0
    while (k < filters.size) {
      if (!rejections.containsKey(k)) kept.add(filters.get(k))
      k += 1
    }
    def scrubMatchingAcls(fr: DeleteAclsResponseData.DeleteAclsFilterResult): Unit = {
      if (!onClusterListener) return
      if (fr.matchingAcls == null || fr.matchingAcls.isEmpty) return
      val out = new util.ArrayList[DeleteAclsResponseData.DeleteAclsMatchingAcl](fr.matchingAcls.size)
      fr.matchingAcls.forEach { m =>
        val mpt = PatternType.fromCode(m.patternType)
        val topicTenant =
          m.resourceType == ResourceType.TOPIC.code &&
            isReservedTenantTopicAclName(m.resourceName, mpt)
        val principalNsTenant =
          (m.resourceType == ResourceType.GROUP.code ||
            m.resourceType == ResourceType.TRANSACTIONAL_ID.code ||
            m.resourceType == ResourceType.USER.code) &&
            isReservedTenantPrincipalAclName(m.resourceName, mpt)
        val principalTenant = isReservedUserPrincipalLiteral(m.principal)
        if (!(topicTenant || principalNsTenant || principalTenant)) out.add(m)
      }
      fr.setMatchingAcls(out)
    }
    def sendResponse(controllerResponseOpt: Option[AbstractResponse]): Unit = {
      val controllerResults: util.List[DeleteAclsResponseData.DeleteAclsFilterResult] = controllerResponseOpt match {
        case Some(r: DeleteAclsResponse) => r.data.filterResults
        case _ => Collections.emptyList()
      }
      controllerResults.forEach(scrubMatchingAcls)
      val merged = new util.ArrayList[DeleteAclsResponseData.DeleteAclsFilterResult](filters.size)
      var idx = 0
      var fwdIdx = 0
      while (idx < filters.size) {
        if (rejections.containsKey(idx)) {
          merged.add(rejections.get(idx))
        } else {
          if (fwdIdx < controllerResults.size) merged.add(controllerResults.get(fwdIdx))
          else merged.add(new DeleteAclsResponseData.DeleteAclsFilterResult()
            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code))
          fwdIdx += 1
        }
        idx += 1
      }
      requestHelper.sendResponseMaybeThrottle(request, throttleMs =>
        new DeleteAclsResponse(
          new DeleteAclsResponseData()
            .setThrottleTimeMs(throttleMs)
            .setFilterResults(merged),
          request.header.apiVersion))
    }
    if (kept.isEmpty && !rejections.isEmpty) {
      sendResponse(None)
    } else if (rejections.isEmpty) {
      // No explicit-name rejections, but we still need to scrub matchingAcls
      // from the response on the cluster-wide listener (leak guard for
      // wildcard filters).
      def responseCallback(responseOpt: Option[AbstractResponse]): Unit = {
        responseOpt match {
          case Some(r: DeleteAclsResponse) =>
            r.data.filterResults.forEach(scrubMatchingAcls)
            requestHelper.sendForwardedResponse(request, r)
          case Some(other) => requestHelper.sendForwardedResponse(request, other)
          case None => handleInvalidVersionsDuringForwarding(request)
        }
      }
      forwardingManager.forwardRequest(request, responseCallback)
    } else {
      val newData = new DeleteAclsRequestData().setFilters(kept)
      forwardingManager.forwardRequest(request,
        new DeleteAclsRequest.Builder(newData).build(request.header.apiVersion()),
        sendResponse)
    }
  }

  // True iff `principalStr` is in the legacy `User:<name>` form AND the
  // <name> portion is any tenant-prefixed principal (`__tenant_<id>.<x>` with
  // non-empty `<id>`). ACL bindings serialize the principal as `User:foo`; the
  // tenant-encoded form is `User:__tenant_<id>.<user>`. The `__tenant_` prefix
  // is RESERVED — even an unknown `<id>` is treated as foreign because pre-
  // binding pollution would otherwise let an admin plant ACLs that the tenant
  // inherits on binding. The narrow ownership check is callerTenantFromPrincipal.
  private def isReservedUserPrincipalLiteral(principalStr: String): Boolean = {
    if (principalStr == null) return false
    val userPrefix = "User:"
    if (!principalStr.startsWith(userPrefix)) return false
    isReservedTenantPrincipalNamespace(principalStr.substring(userPrefix.length))
  }

  def handleDescribeConfigsRequest(request: RequestChannel.Request): Unit = {
    val describeConfigsRequest = request.body[DescribeConfigsRequest]
    val tenantCtx = tenantContextFor(request)

    if (!tenantCtx.effectiveTenant.isPresent) {
      // Outside-in pollution guard (#70). A cluster-wide caller on a non-
      // tenant-bound listener could otherwise ask DescribeConfigs for
      // `acme.orders` (TOPIC) or `__tenant_acme.G` (GROUP) — the cache lookup
      // would either confirm/deny existence (oracle) or return the tenant's
      // full topic-config (retention.ms, segment.bytes, cleanup.policy, ...).
      // Both leak. The tenant-aware branch below already refuses both shapes
      // for tenant principals; this branch is the mirror for cluster-wide
      // callers naming a foreign tenant namespace structurally. Tenant
      // principals never reach this branch (they have an effectiveTenant).
      val pollutionRefused = new ArrayBuffer[DescribeConfigsResponseData.DescribeConfigsResult]()
      val keep = new java.util.ArrayList[DescribeConfigsRequestData.DescribeConfigsResource]()
      describeConfigsRequest.data.resources.forEach { r =>
        val rt = ConfigResource.Type.forId(r.resourceType)
        if (rt == ConfigResource.Type.TOPIC && isReservedTenantNamespace(r.resourceName)) {
          // TOPIC_AUTHORIZATION_FAILED with null errorMessage: indistinguishable
          // from an ACL refusal, so existence isn't disclosed.
          pollutionRefused += refusedDescribeConfigsResult(r, Errors.TOPIC_AUTHORIZATION_FAILED, null)
        } else if (rt == ConfigResource.Type.GROUP && isReservedTenantPrincipalNamespace(r.resourceName)) {
          pollutionRefused += refusedDescribeConfigsResult(r, Errors.GROUP_AUTHORIZATION_FAILED, null)
        } else {
          keep.add(r)
        }
      }
      if (pollutionRefused.nonEmpty) {
        describeConfigsRequest.data.setResources(keep)
      }
      val responseData = configHelper.handleDescribeConfigsRequest(request, authHelper)
      if (pollutionRefused.nonEmpty) {
        val merged = new java.util.ArrayList[DescribeConfigsResponseData.DescribeConfigsResult]()
        merged.addAll(responseData.results)
        pollutionRefused.foreach(merged.add)
        responseData.setResults(merged)
      }
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new DescribeConfigsResponse(responseData.setThrottleTimeMs(requestThrottleMs)))
      return
    }

    // Phase 3c.1: tenant-aware DescribeConfigs.
    // The handler refuses cluster-wide resources (BROKER, BROKER_LOGGER,
    // CLIENT_METRICS) and internal topics, rewrites TOPIC and GROUP resource
    // names IN (logical → physical) before consulting configHelper, and
    // rewrites them back OUT (physical → logical) on the response. Resources
    // that cannot be served are merged in alongside the rewritten ones so
    // the tenant sees a complete answer keyed on the literal names it sent.

    if (tenantCtx.isUnsafe) {
      // Privileged-on-tenant-listener, principal/listener mismatch, or untrusted
      // tenant principal: refuse the WHOLE request. We never consult the cache
      // so we cannot leak names, and the per-resource error code matches what
      // an authz-failure would carry for that resource type.
      val results = describeConfigsRequest.data.resources.asScala.map { r =>
        val err = ConfigResource.Type.forId(r.resourceType) match {
          case ConfigResource.Type.TOPIC => Errors.TOPIC_AUTHORIZATION_FAILED
          case ConfigResource.Type.GROUP => Errors.GROUP_AUTHORIZATION_FAILED
          case _ => Errors.CLUSTER_AUTHORIZATION_FAILED
        }
        refusedDescribeConfigsResult(r, err, null)
      }.toList
      requestHelper.sendResponseMaybeThrottle(request, throttleMs =>
        new DescribeConfigsResponse(new DescribeConfigsResponseData()
          .setThrottleTimeMs(throttleMs)
          .setResults(results.asJava)))
      return
    }

    val rewriteable = new java.util.ArrayList[DescribeConfigsRequestData.DescribeConfigsResource]()
    val refused = new ArrayBuffer[DescribeConfigsResponseData.DescribeConfigsResult]()
    describeConfigsRequest.data.resources.forEach { resource =>
      val literalName = resource.resourceName
      ConfigResource.Type.forId(resource.resourceType) match {
        case ConfigResource.Type.BROKER |
             ConfigResource.Type.BROKER_LOGGER |
             ConfigResource.Type.CLIENT_METRICS =>
          // Cluster-wide configs are not tenant-scoped. A tenant must not be
          // able to discover broker, broker-logger, or client-metrics state.
          refused += refusedDescribeConfigsResult(resource, Errors.CLUSTER_AUTHORIZATION_FAILED, null)
        case ConfigResource.Type.TOPIC =>
          if (literalName == null) {
            // Topic.isInternal would NPE on a null name (INTERNAL_TOPICS is a
            // Set.of(...) which rejects null queries); refuse upfront with the
            // shape an absent-required-field would carry.
            refused += refusedDescribeConfigsResult(resource, Errors.INVALID_REQUEST,
              "DescribeConfigs refused: topic resource must carry a name")
          } else if (Topic.isInternal(literalName)) {
            // Internal topic configs (`__consumer_offsets`, `__transaction_state`,
            // `__share_group_state`) are broker-wide and tenant-opaque. Refuse
            // before reaching the cache so a misconfigured ACL cannot leak.
            refused += refusedDescribeConfigsResult(resource, Errors.TOPIC_AUTHORIZATION_FAILED, null)
          } else if (tenantCtx.isReservedPhysicalForm(literalName)) {
            // Reserved physical form (e.g. `acme.foo` from tenant acme): refuse
            // with the LITERAL name echoed back rather than rewriting into
            // `acme.acme.foo` and silently returning UNKNOWN_TOPIC_OR_PARTITION.
            refused += refusedDescribeConfigsResult(resource, Errors.INVALID_TOPIC_EXCEPTION,
              "DescribeConfigs refused: topic name uses reserved tenant-prefix form")
          } else if (tenantCtx.isInvalidLogicalForm(literalName)) {
            // Pre-validate against Kafka's topic charset rules using the LOGICAL
            // name. Letting the request reach ConfigHelper would run
            // `Topic.validate` on the PHYSICAL name and the resulting
            // InvalidTopicException would echo `<tenantId>.<name>` in
            // errorMessage — leaking the prefix back to the tenant.
            refused += refusedDescribeConfigsResult(resource, Errors.INVALID_TOPIC_EXCEPTION,
              "Topic name '" + literalName + "' is invalid")
          } else if (tenantCtx.isOverlongLogicalForm(literalName)) {
            refused += refusedDescribeConfigsResult(resource, Errors.INVALID_TOPIC_EXCEPTION,
              "Topic name '" + literalName + "' would exceed the maximum length when combined with the tenant prefix")
          } else {
            try {
              resource.setResourceName(tenantCtx.toPhysical(literalName))
              rewriteable.add(resource)
            } catch {
              case e: InvalidTopicException =>
                refused += refusedDescribeConfigsResult(resource, Errors.INVALID_TOPIC_EXCEPTION, e.getMessage)
            }
          }
        case ConfigResource.Type.GROUP =>
          if (literalName == null) {
            refused += refusedDescribeConfigsResult(resource, Errors.INVALID_REQUEST,
              "DescribeConfigs refused: group resource must carry a name")
          } else {
            try {
              resource.setResourceName(tenantCtx.toPhysicalGroup(literalName))
              rewriteable.add(resource)
            } catch {
              // Cross-tenant group prefix (`__tenant_other.group1`): refuse with
              // the wire-shape an authz failure would produce.
              case _: IllegalArgumentException =>
                refused += refusedDescribeConfigsResult(resource, Errors.GROUP_AUTHORIZATION_FAILED, null)
            }
          }
        case _ =>
          refused += refusedDescribeConfigsResult(resource, Errors.INVALID_REQUEST, null)
      }
    }

    describeConfigsRequest.data.setResources(rewriteable)
    val responseData = configHelper.handleDescribeConfigsRequest(request, authHelper)

    // OUT-rewrite: each result echoes back the logical name the tenant submitted.
    // We also scrub any echo of the physical name from `errorMessage`. The
    // pre-validation above blocks the main path that would leak the prefix
    // (Topic.validate on the rewritten name), but a downstream failure from
    // `configRepository` or `LogConfig.fromProps` could still set
    // `errorMessage = ApiError.fromThrowable(e).message` (ConfigHelper:170-183)
    // with the physical name embedded.
    responseData.results.forEach { result =>
      ConfigResource.Type.forId(result.resourceType) match {
        case ConfigResource.Type.TOPIC =>
          val physical = result.resourceName
          val logical = tenantCtx.toLogical(physical)
          result.setResourceName(logical)
          val msg = result.errorMessage
          if (msg != null && physical != null && physical != logical && msg.contains(physical)) {
            result.setErrorMessage(msg.replace(physical, logical))
          }
        case ConfigResource.Type.GROUP =>
          val physical = result.resourceName
          val logical = tenantCtx.toLogicalGroup(physical)
          result.setResourceName(logical)
          val msg = result.errorMessage
          if (msg != null && physical != null && physical != logical && msg.contains(physical)) {
            result.setErrorMessage(msg.replace(physical, logical))
          }
        case _ =>
      }
      // Catch-all: scrub any other physical-form occurrence (a different
      // tenant's resource name embedded in a server-side string, or a
      // principal-prefix form leaked by a downstream coordinator). The
      // resource-specific replace above only handles the exact resourceName
      // for this entry; this generic pass is defence-in-depth.
      result.setErrorMessage(scrubMessage(result.errorMessage, tenantCtx))
    }

    if (refused.nonEmpty) {
      val merged = new java.util.ArrayList[DescribeConfigsResponseData.DescribeConfigsResult]()
      merged.addAll(responseData.results)
      refused.foreach(merged.add)
      responseData.setResults(merged)
    }

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new DescribeConfigsResponse(responseData.setThrottleTimeMs(requestThrottleMs)))
  }

  private def refusedDescribeConfigsResult(
    resource: DescribeConfigsRequestData.DescribeConfigsResource,
    error: Errors,
    message: String
  ): DescribeConfigsResponseData.DescribeConfigsResult = {
    new DescribeConfigsResponseData.DescribeConfigsResult()
      .setErrorCode(error.code)
      .setErrorMessage(if (message != null) message else error.message)
      .setResourceName(resource.resourceName)
      .setResourceType(resource.resourceType)
      .setConfigs(Collections.emptyList[DescribeConfigsResponseData.DescribeConfigsResourceResult])
  }

  // ALTER_REPLICA_LOG_DIRS — outside-in storage-rebalance guard. The handler
  // gates only on CLUSTER:ALTER and then asks ReplicaManager to move each
  // (topic-partition → dir) mapping. A cluster-wide caller naming
  // `acme.orders` would otherwise force tenant log dirs onto a slow disk, a
  // pending-removal dir, or a dir under a different filesystem — all without
  // any tenant signal in the audit log. ALTER_REPLICA_LOG_DIRS is not in
  // TENANT_ALLOWED_APIS, so every caller reaching this branch is non-tenant
  // by construction; partition the input into (reservedPolluting, eligible)
  // by topic-name namespace, execute on the eligible set only, and synthesize
  // per-partition INVALID_TOPIC_EXCEPTION results for the reserved set so the
  // caller sees the refusal per-partition rather than the whole request being
  // silently truncated. Matches #82's pattern on AlterPartitionReassignments.
  def handleAlterReplicaLogDirsRequest(request: RequestChannel.Request): Unit = {
    val alterReplicaDirsRequest = request.body[AlterReplicaLogDirsRequest]
    if (authHelper.authorize(request.context, ALTER, CLUSTER, CLUSTER_NAME)) {
      val partitionDirs = alterReplicaDirsRequest.partitionDirs.asScala
      val (rejected, eligible) =
        if (tenantContextFor(request).effectiveTenant.isPresent) {
          (Map.empty[TopicPartition, String], partitionDirs)
        } else {
          partitionDirs.partition { case (tp, _) => isReservedTenantNamespace(tp.topic) }
        }
      val realResult = if (eligible.isEmpty) Map.empty[TopicPartition, Errors]
                       else replicaManager.alterReplicaLogDirs(eligible)
      val rejectedSynth: Map[TopicPartition, Errors] =
        rejected.iterator.map { case (tp, _) => (tp, Errors.INVALID_TOPIC_EXCEPTION) }.toMap
      val combined = realResult ++ rejectedSynth
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AlterReplicaLogDirsResponse(new AlterReplicaLogDirsResponseData()
          .setResults(combined.groupBy(_._1.topic).map {
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

  // DESCRIBE_LOG_DIRS — tenant existence-oracle + disk-usage enumeration guard.
  //
  // The stock handler authorises only on CLUSTER:DESCRIBE and then enumerates
  // either every local partition (isAllTopicPartitions) or the explicit topic
  // list. With multi-tenancy two leaks fall out for a cluster-wide caller:
  //
  //  1. WILDCARD ENUMERATION: isAllTopicPartitions emits every tenant's
  //     physical topic names with per-partition byte size + offset lag —
  //     strictly more revealing than #93's DescribeTopicPartitions leak
  //     (which exposed names only).
  //  2. EXISTENCE + DISK-USAGE ORACLE: an explicit `acme.orders` request
  //     either returns a populated result or a KafkaStorageException-shaped
  //     gap depending on local-partition presence, letting the caller probe
  //     tenant topic presence and traffic shape.
  //
  // Defense: silent-strip on both vectors (matches #87's
  // ListPartitionReassignments and #92's DescribeProducers patterns).
  //  - explicit-topic path: drop any TopicPartition whose topic name lives in
  //    a reserved tenant namespace BEFORE calling replicaManager.describeLogDirs,
  //    so the response never lists the topic and there is no error-code
  //    signal to compare against an unknown-topic miss.
  //  - all-partitions path: strip reserved-namespace topics from each Topics
  //    entry of every DescribeLogDirsResult after the local enumeration.
  //
  // Internal topics (`__consumer_offsets`, `__transaction_state`,
  // `__share_group_state`) are exempt — they are never tenant-prefixed and
  // legitimate cluster admin tooling needs to see them.
  //
  // DESCRIBE_LOG_DIRS is not in TENANT_ALLOWED_APIS, so tenant principals are
  // already refused at the dispatch gate; tenant context is empty here by
  // construction. Guard short-circuits cleanly when no tenants are configured.
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

        // Outside-in pre-filter on the explicit-topic path: a TopicPartition
        // naming a reserved-tenant topic is dropped before reaching the
        // log-dir scan. The all-partitions path doesn't need pre-filtering
        // (every partition came from the local log manager, all owned by
        // this broker); we scrub on the response side instead.
        val scrubbedPartitions =
          if (describeLogDirsDirRequest.isAllTopicPartitions) partitions
          else partitions.filterNot(tp => isReservedTenantNamespace(tp.topic))

        val rawResults = replicaManager.describeLogDirs(scrubbedPartitions)
        // Response-side scrub. The result list groups partitions by log dir,
        // then by topic; walk both levels and remove tenant-owned Topics
        // entries in place. Empty-after-scrub result entries are kept so the
        // response shape (one entry per log dir) is preserved.
        val scrubbedResults: List[DescribeLogDirsResponseData.DescribeLogDirsResult] =
          if (tenantConfig.allTenants.isEmpty) rawResults
          else rawResults.map { dirResult =>
            val keptTopics = dirResult.topics.asScala
              .filterNot(t => isReservedTenantNamespace(t.name))
              .asJava
            dirResult.setTopics(keptTopics)
          }
        (scrubbedResults, Errors.NONE)
      } else {
        (List.empty[DescribeLogDirsResponseData.DescribeLogDirsResult], Errors.CLUSTER_AUTHORIZATION_FAILED)
      }
    }
    requestHelper.sendResponseMaybeThrottle(request, throttleTimeMs => new DescribeLogDirsResponse(new DescribeLogDirsResponseData()
      .setThrottleTimeMs(throttleTimeMs)
      .setResults(logDirInfos.asJava)
      .setErrorCode(error.code)))
  }

  // Identity-laundering guard for SCRAM credential alteration. A cluster
  // super-user (or any CLUSTER_ACTION holder) can otherwise mint a SCRAM
  // credential for `__tenant_<id>.<user>`, then connect via SASL/SCRAM as
  // that username and have TenantPrincipalBuilder preserve the prefix on
  // the tenant's bound listener — giving them the tenant's identity. The
  // mirror attack via deletion can revoke a tenant user out from under the
  // tenant. Rule: a caller whose own principal is not within tenant T may
  // not name `__tenant_T.*` in any Upsertion.Name or Deletion.Name.
  // Atomic batch refusal: if any single entry is foreign-tenant, the whole
  // request is refused (one Result per affected user with
  // CLUSTER_AUTHORIZATION_FAILED). This avoids a partial-application
  // surface on a security-critical credential store and mirrors the
  // mint-time guard's all-or-nothing posture for delegation tokens.
  def handleAlterUserScramCredentialsRequest(request: RequestChannel.Request): Unit = {
    val alterRequest = request.body[AlterUserScramCredentialsRequest]
    val callerTenant = tenantContextFor(request).effectiveTenant
    def belongsToCallerTenant(name: String): Boolean =
      name != null && callerTenant.isPresent &&
        name.startsWith(TenantNamespace.PRINCIPAL_PREFIX + callerTenant.get + ".")
    def isForeignTenantPrincipal(name: String): Boolean =
      isReservedTenantPrincipalNamespace(name) && !belongsToCallerTenant(name)

    val foreignTenantPrincipal =
      alterRequest.data.upsertions.asScala.exists(u => isForeignTenantPrincipal(u.name)) ||
      alterRequest.data.deletions.asScala.exists(d => isForeignTenantPrincipal(d.name))

    if (foreignTenantPrincipal) {
      // One Result per affected user, error CLUSTER_AUTHORIZATION_FAILED.
      // We emit Results for every user named in the request (foreign or not)
      // so the client sees the whole batch refused, not a partial answer.
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
    } else {
      forwardToController(request)
    }
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

    // Identity-laundering guard. Minting a delegation token whose owner (or
    // any renewer) is in the tenant principal namespace transfers a tenant
    // identity: once minted, the token-bearer can present the token on the
    // tenant's own listener and TenantPrincipalBuilder will preserve the
    // prefix (same-tenant re-auth path, see preservesSameTenantToken-
    // ReauthUnchanged), giving them full tenant access. The cross-listener
    // replay is already refused inside the principal builder; this closes
    // the mint-time side. Rule: a caller whose own principal is not within
    // tenant T cannot mint or name a renewer in T's principal namespace.
    val tokenCallerTenant = tenantContextFor(request).effectiveTenant
    def belongsToCallerTenant(name: String): Boolean =
      name != null && tokenCallerTenant.isPresent &&
        name.startsWith(org.apache.kafka.server.tenant.TenantNamespace.PRINCIPAL_PREFIX + tokenCallerTenant.get + ".")
    def isForeignTenantPrincipal(name: String): Boolean =
      isReservedTenantPrincipalNamespace(name) && !belongsToCallerTenant(name)
    val foreignTenantPrincipal =
      isForeignTenantPrincipal(ownerPrincipalName) ||
      renewerList.exists(p => isForeignTenantPrincipal(p.getName))

    if (!allowTokenRequests(request)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        CreateDelegationTokenResponse.prepareResponse(request.context.requestVersion, requestThrottleMs,
          Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, owner, requester))
    } else if (foreignTenantPrincipal) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        CreateDelegationTokenResponse.prepareResponse(request.context.requestVersion, requestThrottleMs,
          Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED, owner, requester))
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

  // Tenant existence-oracle + HMAC-leak guard for DescribeDelegationToken.
  // The describe path returns the full DelegationToken — TokenInformation
  // (owner, tokenRequester, renewers, tokenId, timestamps) plus the raw HMAC
  // bytes — for every token the caller is "authorized" to see. Without a
  // tenant scrub a cluster-wide (non-tenant) caller can:
  //   (a) name `__tenant_<t>.<u>` in the owner-filter and learn whether that
  //       tenant user holds a token by presence vs. emptiness of the result;
  //   (b) call with the implicit owner-filter (data.owners == null, "all
  //       tokens I'm authorized for") and have tokens minted in the tenant
  //       namespace returned in the response — including the HMAC bytes.
  // The mint-time guard (handleCreateTokenRequest) refuses to create such
  // tokens, but legacy, out-of-band, or pre-guard creations remain a risk.
  // Defense in depth:
  //   1. Atomic refusal — if any owner in the explicit owner-filter is a
  //      foreign tenant principal, refuse the whole request with
  //      DELEGATION_TOKEN_AUTHORIZATION_FAILED. We refuse on ANY foreign
  //      entry (not just all-foreign) so a benign+foreign batch cannot be
  //      used to probe existence by comparing the response size.
  //   2. Outside-in scrub — drop every token from the response whose owner
  //      OR tokenRequester is a foreign tenant principal. tokenRequester
  //      matters: a tenant user can be the *requester* of a token whose
  //      owner is regular (delegated mint with CREATE_TOKENS on the owner),
  //      and surfacing that requester would still leak tenant identity.
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
      val describeCallerTenant = tenantContextFor(request).effectiveTenant
      def belongsToCallerTenant(name: String): Boolean =
        name != null && describeCallerTenant.isPresent &&
          name.startsWith(TenantNamespace.PRINCIPAL_PREFIX + describeCallerTenant.get + ".")
      def isForeignTenantPrincipal(name: String): Boolean =
        isReservedTenantPrincipalNamespace(name) && !belongsToCallerTenant(name)

      if (describeTokenRequest.ownersListEmpty()) {
        sendResponseCallback(Errors.NONE, List())
      }
      else {
        val ownerFilterNamesForeignTenant =
          describeTokenRequest.data.owners != null &&
            describeTokenRequest.data.owners.asScala.exists(o => isForeignTenantPrincipal(o.principalName))

        if (ownerFilterNamesForeignTenant) {
          sendResponseCallback(Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED, List.empty)
        } else {
          val owners = if (describeTokenRequest.data.owners == null)
            None
          else
            Some(describeTokenRequest.data.owners.asScala.map(p => new KafkaPrincipal(p.principalType(), p.principalName)).toList)
          def authorizeToken(tokenId: String) = authHelper.authorize(request.context, DESCRIBE, DELEGATION_TOKEN, tokenId)
          def authorizeRequester(owner: KafkaPrincipal) = authHelper.authorize(request.context, DESCRIBE_TOKENS, USER, owner.toString)
          def eligible(token: TokenInformation) = DelegationTokenManager
            .filterToken(requestPrincipal, owners, token, authorizeToken, authorizeRequester)
          val tokens = tokenManager.getTokens(eligible)
          val scrubbed = tokens.filterNot { dt =>
            val info = dt.tokenInfo
            isForeignTenantPrincipal(info.owner.getName) ||
              isForeignTenantPrincipal(info.tokenRequester.getName)
          }
          sendResponseCallback(Errors.NONE, scrubbed)
        }
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

    // Outside-in guard: refuse a cluster-wide caller naming `__tenant_<known>.*`
    // before it reaches the coordinator. See handleDeleteGroupsRequest.
    if (isReservedTenantPrincipalNamespace(offsetDeleteRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, offsetDeleteRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else if (!authHelper.authorize(request.context, DELETE, GROUP, offsetDeleteRequest.data.groupId)) {
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
      // Tenant-keyed quota records (USER or CLIENT_ID name with the
      // `__tenant_<id>.<x>` shape) live in the same image as cluster-scoped
      // quota records. Without a filter, a cluster-wide caller hitting this
      // endpoint with a wide filter (e.g. USER component MATCH_TYPE_SPECIFIED
      // or null match) enumerates every tenant's quota roster — the
      // principal name, the quota knob, and its value all leak. The tenant
      // path has a symmetric problem: a tenant must see ONLY their own
      // namespace's quotas, with the prefix stripped on the wire so the
      // logical user/client-id name is what comes back. Mirrors #115
      // (DescribeUserScramCredentials) and #117 (AlterClientQuotas refusal).
      val ctx = tenantContextFor(request)
      val callerTenant = ctx.effectiveTenant
      val kept = new util.ArrayList[DescribeClientQuotasResponseData.EntryData]()
      result.entries().forEach { entry =>
        val rewritten = scrubTenantQuotaEntry(entry, callerTenant)
        if (rewritten != null) kept.add(rewritten)
      }
      result.setEntries(kept)
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
        result.setThrottleTimeMs(requestThrottleMs)
        new DescribeClientQuotasResponse(result)
      })
    }
  }

  // Decide whether a single DescribeClientQuotas EntryData survives the
  // tenant-scrub, and if so, return a (possibly rewritten) copy with the
  // tenant prefix stripped from USER / CLIENT_ID for tenant-facing responses.
  //
  // Cluster-wide caller (effectiveTenant.isEmpty): drop any entry that names
  // a reserved-tenant USER or CLIENT_ID; everything else passes through
  // untouched.
  //
  // Tenant caller (effectiveTenant.isPresent): every USER and CLIENT_ID
  // component that exists in the entry must reside in the caller's own
  // tenant namespace. A pure cluster-scoped entry (regular user, regular
  // client-id) is dropped — tenants must not observe the global namespace.
  // IP-only entries are also dropped (no path to scope them per-tenant).
  // When the entry survives, USER and CLIENT_ID names are rewritten to
  // their logical form (`__tenant_acme.bob` → `bob`) before returning.
  private def scrubTenantQuotaEntry(entry: DescribeClientQuotasResponseData.EntryData,
                                    callerTenant: Optional[String])
  : DescribeClientQuotasResponseData.EntryData = {
    import org.apache.kafka.common.quota.ClientQuotaEntity
    val components = entry.entity()
    if (components == null) return entry
    val callerPrefix: String =
      if (callerTenant.isPresent) TenantNamespace.PRINCIPAL_PREFIX + callerTenant.get + "."
      else null

    // First pass: classify. A name with the reserved-tenant shape that does
    // not match callerPrefix is FOREIGN and disqualifies the entire entry.
    // A name without the reserved shape is CLUSTER-scoped.
    var sawTenantOwnComponent = false
    var i = 0
    while (i < components.size()) {
      val c = components.get(i)
      val t = c.entityType()
      val n = c.entityName()
      if (t == ClientQuotaEntity.USER || t == ClientQuotaEntity.CLIENT_ID) {
        if (n != null && isReservedTenantPrincipalNamespace(n)) {
          if (callerPrefix == null || !n.startsWith(callerPrefix)) {
            return null
          }
          sawTenantOwnComponent = true
        } else if (n != null && callerPrefix != null) {
          // Tenant caller, but this USER/CLIENT_ID is a plain cluster name.
          // Tenants must not observe cluster-scoped quota state.
          return null
        }
      }
      i += 1
    }
    if (callerPrefix != null && !sawTenantOwnComponent) {
      // Tenant caller and the entry has no USER/CLIENT_ID in their namespace
      // (e.g. IP-only). Drop — tenants only see their own slice.
      return null
    }
    if (callerPrefix == null) {
      // Cluster-wide caller and no foreign-tenant component matched; keep
      // the entry as-is.
      return entry
    }
    // Tenant caller: produce a rewritten copy with prefix-stripped USER /
    // CLIENT_ID so the logical name lands on the wire.
    val rewritten = new util.ArrayList[DescribeClientQuotasResponseData.EntityData](components.size())
    components.forEach { c =>
      val t = c.entityType()
      val n = c.entityName()
      if ((t == ClientQuotaEntity.USER || t == ClientQuotaEntity.CLIENT_ID)
          && n != null && n.startsWith(callerPrefix)) {
        rewritten.add(new DescribeClientQuotasResponseData.EntityData()
          .setEntityType(t)
          .setEntityName(n.substring(callerPrefix.length)))
      } else {
        rewritten.add(new DescribeClientQuotasResponseData.EntityData()
          .setEntityType(t)
          .setEntityName(n))
      }
    }
    new DescribeClientQuotasResponseData.EntryData()
      .setEntity(rewritten)
      .setValues(entry.values())
  }

  def handleDescribeUserScramCredentialsRequest(request: RequestChannel.Request): Unit = {
    val describeUserScramCredentialsRequest = request.body[DescribeUserScramCredentialsRequest]

    if (!authHelper.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        describeUserScramCredentialsRequest.getErrorResponse(requestThrottleMs, Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    } else {
      // Tenant SCRAM credentials live in the shared SCRAM image but their user
      // name carries the `__tenant_<id>.<user>` form. A cluster-wide caller
      // describing all users would otherwise enumerate every tenant's SCRAM
      // roster (count + per-user mechanism + iteration count). Explicit
      // lookup of a guessed `__tenant_*` name would either return the same
      // metadata or, for an unconfigured user, RESOURCE_NOT_FOUND with the
      // physical name in the message — an existence oracle either way.
      //
      // Refuse tenant-prefixed user names per-entry on input (return
      // RESOURCE_NOT_FOUND with no message), and strip tenant-prefixed
      // results from the "list all users" response so a non-tenant caller
      // sees a cluster-only view.
      val data = describeUserScramCredentialsRequest.data()
      val requested = data.users()
      val deferredResults = new util.ArrayList[DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult]()
      if (requested != null && !requested.isEmpty) {
        val filtered = new util.ArrayList[DescribeUserScramCredentialsRequestData.UserName](requested.size())
        requested.forEach { u =>
          if (u != null && isReservedTenantPrincipalNamespace(u.name())) {
            // RESOURCE_NOT_FOUND with no message — same shape as the
            // SCRAM image's response for a never-existed user.
            deferredResults.add(new DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult()
              .setUser(u.name())
              .setErrorCode(Errors.RESOURCE_NOT_FOUND.code()))
          } else {
            filtered.add(u)
          }
        }
        data.setUsers(filtered)
      }
      val result = metadataCache.asInstanceOf[KRaftMetadataCache].describeScramCredentials(data)
      // Strip tenant-prefixed entries from the response. Covers both the
      // "list all" path (where the input filter doesn't apply) and any
      // defence-in-depth case where a tenant entry slipped through.
      val kept = new util.ArrayList[DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult]()
      result.results().forEach { r =>
        if (!isReservedTenantPrincipalNamespace(r.user())) kept.add(r)
      }
      deferredResults.forEach(r => kept.add(r))
      result.setResults(kept)
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
    val tenantCtx = tenantContextFor(request)

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

      // Outside-in existence oracle: a non-tenant caller naming `acme.orders`
      // would otherwise get UNKNOWN_TOPIC_OR_PARTITION when the topic doesn't
      // exist and full producer state (producerId, epoch, currentTxnStartOffset)
      // when it does. The mismatch lets them probe tenant topic existence —
      // and on a hit, learn enough about in-flight transactions to coordinate
      // a WriteTxnMarkers fence. Refuse with TOPIC_AUTHORIZATION_FAILED before
      // touching `metadataCache.contains`, matching the wire shape an authz
      // refusal already produces so nothing distinguishes the two responses.
      // Tenant principals never reach here — DESCRIBE_PRODUCERS is outside
      // TENANT_ALLOWED_APIS — so the guard only fires for non-tenant callers.
      val outsideInRefused = !tenantCtx.effectiveTenant.isPresent &&
        isReservedTenantNamespace(topicRequest.name)

      val topicError = invalidTopicError.orElse {
        if (outsideInRefused) {
          Some(new ApiError(Errors.TOPIC_AUTHORIZATION_FAILED))
        } else if (!authHelper.authorize(request.context, READ, TOPIC, topicRequest.name)) {
          Some(new ApiError(Errors.TOPIC_AUTHORIZATION_FAILED))
        } else if (!metadataCache.contains(topicRequest.name))
          Some(new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION))
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
    val tenantCtx = tenantContextFor(request)
    val response = new DescribeTransactionsResponseData()

    describeTransactionsRequest.data.transactionalIds.forEach { transactionalId =>
      val outsideInRefused = !tenantCtx.effectiveTenant.isPresent &&
        isReservedTenantPrincipalNamespace(transactionalId)
      val transactionState = if (outsideInRefused) {
        // Outside-in: a non-tenant caller naming `__tenant_<known>.X` would
        // otherwise read the tenant's producer epoch, txn timeout and partition
        // list directly from `__transaction_state`. Refuse with the same wire
        // shape an authz failure produces so the response cannot be used to
        // probe tenant existence. Tenant principals are refused upstream by the
        // dispatch gate (DESCRIBE_TRANSACTIONS is not in TENANT_ALLOWED_APIS).
        new DescribeTransactionsResponseData.TransactionState()
          .setTransactionalId(transactionalId)
          .setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code)
      } else if (!authHelper.authorize(request.context, DESCRIBE, TRANSACTIONAL_ID, transactionalId)) {
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
    val tenantCtx = tenantContextFor(request)
    val filteredProducerIds = listTransactionsRequest.data.producerIdFilters.asScala.map(Long.unbox).toSet
    val filteredStates = listTransactionsRequest.data.stateFilters.asScala.toSet
    val durationFilter = listTransactionsRequest.data.durationFilter()
    val response = txnCoordinator.handleListTransactions(filteredProducerIds, filteredStates, durationFilter)

    // The response should contain only transactionalIds that the principal
    // has `Describe` permission to access. Also hide tenant-internal
    // (`__tenant_<known>.*`) entries from non-tenant callers so the listing
    // cannot be used to enumerate tenants. Tenant principals never reach
    // this handler (LIST_TRANSACTIONS is not in TENANT_ALLOWED_APIS).
    val transactionStateIter = response.transactionStates.iterator()
    while (transactionStateIter.hasNext) {
      val transactionState = transactionStateIter.next()
      val txnId = transactionState.transactionalId
      val tenantInternal = !tenantCtx.effectiveTenant.isPresent &&
        isReservedTenantPrincipalNamespace(txnId)
      if (tenantInternal ||
          !authHelper.authorize(request.context, DESCRIBE, TRANSACTIONAL_ID, txnId)) {
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
    } else if (isReservedTenantPrincipalNamespace(consumerGroupHeartbeatRequest.data.groupId)) {
      // Outside-in guard: refuse a cluster-wide caller naming `__tenant_<known>.*`
      // before it touches the group coordinator. See handleDeleteGroupsRequest.
      requestHelper.sendMaybeThrottle(request, consumerGroupHeartbeatRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else if (!authHelper.authorize(request.context, READ, GROUP, consumerGroupHeartbeatRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, consumerGroupHeartbeatRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      if (consumerGroupHeartbeatRequest.data.subscribedTopicNames != null &&
        !consumerGroupHeartbeatRequest.data.subscribedTopicNames.isEmpty) {
        // Outside-in subscription pollution: the groupId guard above only
        // inspects `groupId`, not `subscribedTopicNames`. A cluster-wide
        // caller naming `groupId="g"` (not reserved-form) and
        // `subscribedTopicNames=["acme.orders"]` would otherwise have the new
        // group coordinator record the subscription against the tenant's
        // PHYSICAL topic. Subsequent heartbeat responses surface that physical
        // name in `member.assignment.topicPartitions` — leaking topic
        // existence and end offsets, and letting a non-tenant principal
        // disrupt the tenant's rebalance protocol. Refuse the whole heartbeat
        // with TOPIC_AUTHORIZATION_FAILED before forwarding to the coordinator
        // — same wire shape the existing topic-authz refusal below produces.
        // CONSUMER_GROUP_HEARTBEAT is outside TENANT_ALLOWED_APIS so tenant
        // principals never reach here; this guard only fires for non-tenant
        // callers. The list-iteration cost is bounded by the request size.
        if (!tenantContextFor(request).effectiveTenant.isPresent &&
          consumerGroupHeartbeatRequest.data.subscribedTopicNames.asScala.exists(isReservedTenantNamespace)) {
          val responseData = new ConsumerGroupHeartbeatResponseData()
            .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
          requestHelper.sendMaybeThrottle(request, new ConsumerGroupHeartbeatResponse(responseData))
          return CompletableFuture.completedFuture[Unit](())
        }
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

      // #151 — outside-in regex subscription pollution, sibling of #132. The
      // KIP-848 v1+ `subscribedTopicRegex` field is resolved server-side at
      // `GroupMetadataManager.refreshRegularExpressions` against the FULL
      // broker topic universe (`MetadataImage.topics().topicsByName().keySet()`),
      // which on a multi-tenant broker contains every tenant's PHYSICAL names
      // (`acme.orders`, `beta.invoices`, …). A non-tenant cluster-wide caller
      // submitting `.*` (or any pattern shaped to match `<id>.<rest>`) would
      // therefore have the coordinator subscribe its group to every tenant's
      // topics — leaking existence + partition counts in
      // `member.assignment.topicPartitions`, and disrupting tenants' rebalance
      // protocol because they share the GroupMetadataManager.
      //
      // Worse, the regex string is persisted to `__consumer_offsets` verbatim
      // (`ConsumerGroupMemberMetadataValue.SubscribedTopicRegex`). On
      // coordinator failover or after a new tenant is later bound to the
      // broker, the same regex is replayed against the THEN-current topic
      // universe and silently starts matching new tenant topics — a latent
      // cross-tenant leak surviving restarts and tenant lifecycle.
      //
      // CONSUMER_GROUP_HEARTBEAT is outside `TENANT_ALLOWED_APIS` so tenant
      // principals are dispatch-refused upstream; this guard only ever fires
      // for non-tenant callers. Server-side regex containment is undecidable
      // in general, so per-pattern collision tests against bound tenant ids
      // produce false negatives for pre-binding tenants (#102/#114) and for
      // patterns using anchors / unicode classes. We take the conservative
      // line: on a broker with ANY tenant binding, refuse `subscribedTopicRegex`
      // from non-tenant callers outright. Clients that need regex subscription
      // on a multi-tenant broker can enumerate topics client-side and submit
      // an explicit `subscribedTopicNames` list, which IS scrubbed above.
      if (consumerGroupHeartbeatRequest.data.subscribedTopicRegex != null &&
        !consumerGroupHeartbeatRequest.data.subscribedTopicRegex.isEmpty &&
        !tenantContextFor(request).effectiveTenant.isPresent &&
        !tenantConfig.allTenants.isEmpty) {
        val responseData = new ConsumerGroupHeartbeatResponseData()
          .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
        requestHelper.sendMaybeThrottle(request, new ConsumerGroupHeartbeatResponse(responseData))
        return CompletableFuture.completedFuture[Unit](())
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
        // Outside-in guard: refuse a cluster-wide caller naming `__tenant_<known>.*`
        // before it reaches the coordinator. See handleDeleteGroupsRequest.
        if (isReservedTenantPrincipalNamespace(groupId)) {
          response.groups.add(new ConsumerGroupDescribeResponseData.DescribedGroup()
            .setGroupId(groupId)
            .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)
          )
        } else if (!authHelper.authorize(request.context, DESCRIBE, GROUP, groupId)) {
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
    } else if (isReservedTenantPrincipalNamespace(shareGroupHeartbeatRequest.data.groupId)) {
      // Outside-in guard: refuse a cluster-wide caller naming `__tenant_<known>.*`
      // before it touches the share-group coordinator. See handleDeleteGroupsRequest.
      requestHelper.sendMaybeThrottle(request, shareGroupHeartbeatRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else if (!authHelper.authorize(request.context, READ, GROUP, shareGroupHeartbeatRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, shareGroupHeartbeatRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      if (shareGroupHeartbeatRequest.data.subscribedTopicNames != null &&
        !shareGroupHeartbeatRequest.data.subscribedTopicNames.isEmpty) {
        // #133: mirror the consumer-side guard at handleConsumerGroupHeartbeat.
        // The `groupId` guard above only inspects the group name. A cluster-wide
        // caller (no tenant context) naming `groupId="g"` and
        // `subscribedTopicNames=["acme.orders"]` would otherwise have the share
        // coordinator record the subscription against tenant `acme`'s PHYSICAL
        // topic — leaking topic existence and end offsets via subsequent
        // heartbeat responses, and letting a non-tenant principal disrupt the
        // tenant's share-rebalance protocol. Refuse before forwarding.
        // SHARE_GROUP_HEARTBEAT is outside TENANT_ALLOWED_APIS so tenant
        // principals never reach here; this guard only fires for non-tenant
        // callers.
        if (!tenantContextFor(request).effectiveTenant.isPresent &&
          shareGroupHeartbeatRequest.data.subscribedTopicNames.asScala.exists(isReservedTenantNamespace)) {
          val responseData = new ShareGroupHeartbeatResponseData()
            .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
          requestHelper.sendMaybeThrottle(request, new ShareGroupHeartbeatResponse(responseData))
          return CompletableFuture.completedFuture[Unit](())
        }
        // Clients are not allowed to see topics that are not authorized for Describe.
        val subscribedTopicSet = shareGroupHeartbeatRequest.data.subscribedTopicNames.asScala.toSet
        val authorizedTopics = authHelper.filterByAuthorized(request.context, DESCRIBE, TOPIC,
          subscribedTopicSet)(identity)
        if (authorizedTopics.size < subscribedTopicSet.size) {
          val responseData = new ShareGroupHeartbeatResponseData()
            .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
          requestHelper.sendMaybeThrottle(request, new ShareGroupHeartbeatResponse(responseData))
          return CompletableFuture.completedFuture[Unit](())
        }
      }

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
        // Outside-in guard: refuse a cluster-wide caller naming `__tenant_<known>.*`
        // before it reaches the share-group coordinator. See handleDeleteGroupsRequest.
        if (isReservedTenantPrincipalNamespace(groupId)) {
          response.groups.add(new ShareGroupDescribeResponseData.DescribedGroup()
            .setGroupId(groupId)
            .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)
          )
        } else if (!authHelper.authorize(request.context, DESCRIBE, GROUP, groupId)) {
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

    // Outside-in guard: refuse a cluster-wide caller naming `__tenant_<known>.*`
    // before it reaches the share-group coordinator. See handleDeleteGroupsRequest.
    if (isReservedTenantPrincipalNamespace(groupId)) {
      requestHelper.sendMaybeThrottle(request, shareFetchRequest.getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, Errors.GROUP_AUTHORIZATION_FAILED.exception))
      return
    }

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
        else if (acknowledgeBatches.size() == 0)
          emptyAcknowledgements += topicIdPartition ->
            ShareAcknowledgeResponse.partitionResponse(topicIdPartition, Errors.NONE)
        else {
          interested += topicIdPartition -> acknowledgeBatches
        }
    }

    if (interested.isEmpty) {
      CompletableFuture.completedFuture(erroneous)
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

    // Outside-in guard: refuse a cluster-wide caller naming `__tenant_<known>.*`
    // before it reaches the share-group coordinator. See handleDeleteGroupsRequest.
    if (isReservedTenantPrincipalNamespace(groupId)) {
      requestHelper.sendMaybeThrottle(request,
        shareAcknowledgeRequest.getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, Errors.GROUP_AUTHORIZATION_FAILED.exception))
      return
    }

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

    // Outside-in scrub for the share-coordinator state plane. A non-tenant
    // principal holding CLUSTER_ACTION (a service account or super-user on a
    // non-tenant listener) must not be able to read share-group state keyed
    // on `__tenant_<id>.<group>` — that's the broker's stored form for tenant
    // share groups, populated only via the tenant-facing ShareGroupHeartbeat /
    // ShareFetch path (which already rewrites groupId by the time it reaches
    // the coordinator). Tenant principals themselves cannot reach this
    // handler (they never carry CLUSTER_ACTION), so the structural check
    // suffices — no same-tenant carve-out needed. Mirrors the principal-
    // namespace scrub on ShareFetch (L5975) and on the share-coordinator's
    // client-facing siblings closed by #64.
    if (isReservedTenantPrincipalNamespace(readShareGroupStateRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request,
        readShareGroupStateRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      return CompletableFuture.completedFuture[Unit](())
    }

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

    // Same scrub as Read: a cluster-action caller naming `__tenant_<id>.<g>`
    // here would PLANT share-partition state into the tenant's namespace
    // before the tenant's first heartbeat — corrupt offsets, replay state
    // batches, deliver junk records under a tenant group. The principal-
    // prefix check on the wire `groupId` is the right boundary because the
    // share coordinator stores by that key directly.
    if (isReservedTenantPrincipalNamespace(writeShareRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request,
        writeShareRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      return CompletableFuture.completedFuture[Unit](())
    }

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
  // The set of APIs a tenant-scoped context is allowed to invoke. v1 tenancy
  // covers Produce / Fetch / Metadata / CreateTopics / DeleteTopics — every
  // other request handler can read or mutate cluster state without going
  // through the logical→physical rewrite, so the safest thing is to refuse
  // them at the dispatch boundary rather than let a partially tenant-aware
  // handler leak physical names or pollute another tenant's namespace.
  // SASL_HANDSHAKE / SASL_AUTHENTICATE / API_VERSIONS happen during connection
  // setup before a tenant identity is meaningful and must remain reachable.
  // INIT_PRODUCER_ID is admitted for both the idempotent path (null
  // transactionalId — modern Java producers default to enable.idempotence=true
  // and call InitProducerId at start-up) and the transactional path. The
  // handler rewrites a non-null transactionalId to its physical form via
  // toPhysicalTxnId before authorisation and before reaching the transaction
  // coordinator; the __transaction_state log is keyed by hash(transactionalId)
  // so two tenants sharing the same external id resolve to distinct
  // coordinator records.
  // Note: ListTopics is the all-topics variant of Metadata and is covered by
  // ApiKeys.METADATA.
  //
  // Phase 2 (consumer-group ID rewrites) admits the runtime consumer APIs.
  // FIND_COORDINATOR routes a group lookup to its coordinator node (Phase 3b
  // extended this to TRANSACTION coordinator lookups with the same physical
  // rewrite);  JOIN_GROUP/SYNC_GROUP/HEARTBEAT/LEAVE_GROUP drive the rebalance
  // protocol; OFFSET_COMMIT/OFFSET_FETCH persist and read committed offsets.
  // Each handler rewrites the group id (and topic names where present) on the
  // way in and back on the way out.
  //
  // Phase 3a admits ListOffsets and DeleteRecords — both are pure topic-name
  // rewrites that key on physical names for authorization and replicaManager
  // and restore logical names on the response. The functional scenario for
  // "logical offsets are tenant-local" and "DeleteRecords advances only the
  // tenant's logical low-water mark" requires these handlers to be tenant-
  // aware rather than refused.
  //
  // Phase 3b.3 admits ADD_PARTITIONS_TO_TXN and ADD_OFFSETS_TO_TXN. Both
  // rewrite the transactional id (and topic names / group id where present)
  // to physical for authorisation + coordinator dispatch and echo logical
  // names on the response.
  //
  // Phase 3b.4 closes the loop on transactional traffic: END_TXN and
  // TXN_OFFSET_COMMIT now rewrite the transactional id (and group id +
  // topic names on TxnOffsetCommit) to physical for coordinator dispatch
  // and restore logical names on the response. The share / consumer-group
  // v2 APIs and the configs/groups admin APIs (Phase 3c / 3d) remain
  // refused at this dispatch boundary until those phases lift them.
  private[server] val TENANT_ALLOWED_APIS: Set[ApiKeys] = Set(
    ApiKeys.PRODUCE,
    ApiKeys.FETCH,
    ApiKeys.METADATA,
    ApiKeys.CREATE_TOPICS,
    ApiKeys.DELETE_TOPICS,
    ApiKeys.INIT_PRODUCER_ID,
    ApiKeys.FIND_COORDINATOR,
    ApiKeys.JOIN_GROUP,
    ApiKeys.SYNC_GROUP,
    ApiKeys.HEARTBEAT,
    ApiKeys.LEAVE_GROUP,
    ApiKeys.OFFSET_COMMIT,
    ApiKeys.OFFSET_FETCH,
    ApiKeys.LIST_OFFSETS,
    ApiKeys.DELETE_RECORDS,
    ApiKeys.ADD_PARTITIONS_TO_TXN,
    ApiKeys.ADD_OFFSETS_TO_TXN,
    ApiKeys.END_TXN,
    ApiKeys.TXN_OFFSET_COMMIT,
    ApiKeys.DESCRIBE_CONFIGS,
    ApiKeys.SASL_HANDSHAKE,
    ApiKeys.SASL_AUTHENTICATE,
    ApiKeys.API_VERSIONS
  )

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
