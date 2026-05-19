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

package kafka.server

import kafka.cluster.Partition
import kafka.coordinator.transaction.{InitProducerIdResult, TransactionCoordinator}
import kafka.log.{LogManager, UnifiedLog}
import kafka.network.RequestChannel
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.metadata.{ConfigRepository, KRaftMetadataCache, MockConfigRepository}
import kafka.server.share.SharePartitionManager
import kafka.utils.{CoreUtils, Log4jController, Logging, TestUtils}
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.common._
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type.{BROKER, BROKER_LOGGER}
import org.apache.kafka.common.errors.{ClusterAuthorizationException, UnsupportedVersionException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.{AddPartitionsToTxnTopic, AddPartitionsToTxnTopicCollection, AddPartitionsToTxnTransaction, AddPartitionsToTxnTransactionCollection}
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnResult
import org.apache.kafka.common.message.AlterConfigsRequestData.{AlterConfigsResource => LAlterConfigsResource, AlterConfigsResourceCollection => LAlterConfigsResourceCollection, AlterableConfig => LAlterableConfig, AlterableConfigCollection => LAlterableConfigCollection}
import org.apache.kafka.common.message.AlterConfigsResponseData.{AlterConfigsResourceResponse => LAlterConfigsResourceResponse}
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData.{DescribedGroup, TopicPartitions}
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult
import org.apache.kafka.common.message.DeleteRecordsRequestData
import org.apache.kafka.common.message.DeleteRecordsRequestData.{DeleteRecordsPartition, DeleteRecordsTopic}
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.{AlterConfigsResource => IAlterConfigsResource, AlterConfigsResourceCollection => IAlterConfigsResourceCollection, AlterableConfig => IAlterableConfig, AlterableConfigCollection => IAlterableConfigCollection}
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.{AlterConfigsResourceResponse => IAlterConfigsResourceResponse}
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.message.ListClientMetricsResourcesResponseData.ClientMetricsResource
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.message.ListOffsetsResponseData.{ListOffsetsPartitionResponse, ListOffsetsTopicResponse}
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic
import org.apache.kafka.common.message.OffsetDeleteRequestData.{OffsetDeleteRequestPartition, OffsetDeleteRequestTopic, OffsetDeleteRequestTopicCollection}
import org.apache.kafka.common.message.OffsetDeleteResponseData.{OffsetDeleteResponsePartition, OffsetDeleteResponsePartitionCollection, OffsetDeleteResponseTopic, OffsetDeleteResponseTopicCollection}
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.{OffsetForLeaderPartition, OffsetForLeaderTopic, OffsetForLeaderTopicCollection}
import org.apache.kafka.common.message.ShareFetchRequestData.{AcknowledgementBatch, ForgottenTopic}
import org.apache.kafka.common.message.ShareFetchResponseData.{AcquiredRecords, PartitionData, ShareFetchableTopicResponse}
import org.apache.kafka.common.metadata.{TopicRecord, PartitionRecord, RegisterBrokerRecord}
import org.apache.kafka.common.metadata.FeatureLevelRecord
import org.apache.kafka.common.metadata.RegisterBrokerRecord.{BrokerEndpoint, BrokerEndpointCollection}
import org.apache.kafka.common.protocol.ApiMessage
import org.apache.kafka.common.message._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.{ApiKeys, Errors, MessageUtil}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.WriteTxnMarkersRequest.TxnMarkerEntry
import org.apache.kafka.common.requests.{FetchMetadata => JFetchMetadata, _}
import org.apache.kafka.common.resource.{PatternType, Resource, ResourcePattern, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, KafkaPrincipalSerde, SecurityProtocol}
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource
import org.apache.kafka.common.utils.{ImplicitLinkedHashCollection, ProducerIdAndEpoch, SecurityUtils, Utils}
import org.apache.kafka.coordinator.group.GroupConfig.{CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, CONSUMER_SESSION_TIMEOUT_MS_CONFIG, SHARE_AUTO_OFFSET_RESET_CONFIG, SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, SHARE_RECORD_LOCK_DURATION_MS_CONFIG, SHARE_SESSION_TIMEOUT_MS_CONFIG}
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig
import org.apache.kafka.coordinator.group.{GroupConfig, GroupCoordinator, GroupCoordinatorConfig}
import org.apache.kafka.coordinator.share.{ShareCoordinator, ShareCoordinatorTestConfig}
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.network.metrics.{RequestChannelMetrics, RequestMetrics}
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.security.authorizer.AclEntry
import org.apache.kafka.server.{BrokerFeatures, ClientMetricsManager}
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.{FeatureVersion, FinalizedFeatures, GroupVersion, KRaftVersion, MetadataVersion, RequestLocal, TransactionVersion}
import org.apache.kafka.server.config.{KRaftConfigs, ReplicationConfigs, ServerConfigs, ServerLogConfigs}
import org.apache.kafka.server.metrics.ClientMetricsTestUtils
import org.apache.kafka.server.share.{CachedSharePartition, ErroneousAndValidPartitionData}
import org.apache.kafka.server.quota.ThrottleCallback
import org.apache.kafka.server.share.acknowledge.ShareAcknowledgementBatch
import org.apache.kafka.server.share.context.{FinalContext, ShareSessionContext}
import org.apache.kafka.server.share.session.{ShareSession, ShareSessionKey}
import org.apache.kafka.server.storage.log.{FetchParams, FetchPartitionData}
import org.apache.kafka.server.util.{FutureUtils, MockTime}
import org.apache.kafka.storage.internals.concentration.{ConcentrationHeaders, ConcentrationKernel, IdempotentBatchKey, LogicalProduceStamper, LogicalTopicDescriptor, Reservation}
import org.apache.kafka.storage.internals.log.{AppendOrigin, LogConfig}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, EnumSource, ValueSource}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}

import java.lang.{Byte => JByte}
import java.net.InetAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util
import java.util.Arrays.asList
import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.{Collections, Comparator, Optional, OptionalInt, OptionalLong, Properties}
import scala.collection.{Map, Seq, mutable}
import scala.jdk.CollectionConverters._

class KafkaApisTest extends Logging {
  private val requestChannel: RequestChannel = mock(classOf[RequestChannel])
  private val requestChannelMetrics: RequestChannelMetrics = mock(classOf[RequestChannelMetrics])
  private val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
  private val groupCoordinator: GroupCoordinator = mock(classOf[GroupCoordinator])
  private val shareCoordinator: ShareCoordinator = mock(classOf[ShareCoordinator])
  private val txnCoordinator: TransactionCoordinator = mock(classOf[TransactionCoordinator])
  private val forwardingManager: ForwardingManager = mock(classOf[ForwardingManager])
  private val autoTopicCreationManager: AutoTopicCreationManager = mock(classOf[AutoTopicCreationManager])

  private val kafkaPrincipalSerde = new KafkaPrincipalSerde {
    override def serialize(principal: KafkaPrincipal): Array[Byte] = Utils.utf8(principal.toString)
    override def deserialize(bytes: Array[Byte]): KafkaPrincipal = SecurityUtils.parseKafkaPrincipal(Utils.utf8(bytes))
  }
  private val metrics = new Metrics()
  private val brokerId = 1
  private var metadataCache: MetadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
  private val clientQuotaManager: ClientQuotaManager = mock(classOf[ClientQuotaManager])
  private val clientRequestQuotaManager: ClientRequestQuotaManager = mock(classOf[ClientRequestQuotaManager])
  private val clientControllerQuotaManager: ControllerMutationQuotaManager = mock(classOf[ControllerMutationQuotaManager])
  private val replicaQuotaManager: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
  private val quotas = new QuotaManagers(clientQuotaManager, clientQuotaManager, clientRequestQuotaManager,
    clientControllerQuotaManager, replicaQuotaManager, replicaQuotaManager, replicaQuotaManager, util.Optional.empty())
  private val fetchManager: FetchManager = mock(classOf[FetchManager])
  private val sharePartitionManager: SharePartitionManager = mock(classOf[SharePartitionManager])
  private val clientMetricsManager: ClientMetricsManager = mock(classOf[ClientMetricsManager])
  private val concentrationKernel: ConcentrationKernel = mock(classOf[ConcentrationKernel])
  private val brokerTopicStats = new BrokerTopicStats
  private val clusterId = "clusterId"
  private val time = new MockTime
  private val clientId = ""
  private var kafkaApis: KafkaApis = _
  private val partitionMaxBytes = 40000

  @AfterEach
  def tearDown(): Unit = {
    CoreUtils.swallow(quotas.shutdown(), this)
    if (kafkaApis != null)
      CoreUtils.swallow(kafkaApis.close(), this)
    TestUtils.clearYammerMetrics()
    metrics.close()
  }

  def createKafkaApis(
    authorizer: Option[Authorizer] = None,
    configRepository: ConfigRepository = new MockConfigRepository(),
    overrideProperties: Map[String, String] = Map.empty,
    featureVersions: Seq[FeatureVersion] = Seq.empty
  ): KafkaApis = {

    val properties = TestUtils.createBrokerConfig(brokerId)
    properties.put(KRaftConfigs.NODE_ID_CONFIG, brokerId.toString)
    properties.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    val voterId = brokerId + 1
    properties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, s"$voterId@localhost:9093")

    overrideProperties.foreach( p => properties.put(p._1, p._2))
    val config = new KafkaConfig(properties)

    val listenerType = ListenerType.BROKER
    val enabledApis = ApiKeys.apisForListener(listenerType).asScala

    val apiVersionManager = new SimpleApiVersionManager(
      listenerType,
      enabledApis,
      BrokerFeatures.defaultSupportedFeatures(true),
      true,
      () => new FinalizedFeatures(MetadataVersion.latestTesting(), Collections.emptyMap[String, java.lang.Short], 0, true))

    when(groupCoordinator.isNewGroupCoordinator).thenReturn(config.isNewGroupCoordinatorEnabled)
    setupFeatures(featureVersions)

    new KafkaApis(
      requestChannel = requestChannel,
      forwardingManager = forwardingManager,
      replicaManager = replicaManager,
      groupCoordinator = groupCoordinator,
      txnCoordinator = txnCoordinator,
      shareCoordinator = Some(shareCoordinator),
      autoTopicCreationManager = autoTopicCreationManager,
      brokerId = brokerId,
      config = config,
      configRepository = configRepository,
      metadataCache = metadataCache,
      metrics = metrics,
      authorizer = authorizer,
      quotas = quotas,
      fetchManager = fetchManager,
      sharePartitionManager = sharePartitionManager,
      brokerTopicStats = brokerTopicStats,
      clusterId = clusterId,
      time = time,
      tokenManager = null,
      apiVersionManager = apiVersionManager,
      clientMetricsManager = clientMetricsManager,
      concentrationKernel = concentrationKernel)
  }

  // Logical-topic non-produce paths (Fetch / ListOffsets / DeleteRecords) require this broker
  // to be the leader of the backing partition (Codex Q5 — see KafkaApis.handleFetchRequest).
  // Tests that exercise the happy path on those paths must stub onlinePartition to return a
  // Partition with isLeader=true; otherwise the new check rejects with NOT_LEADER_OR_FOLLOWER
  // before the path under test ever runs.
  private def stubBackingPartitionAsLeader(): Unit = {
    val backingPartition = mock(classOf[Partition])
    when(backingPartition.isLeader).thenReturn(true)
    when(replicaManager.onlinePartition(any[TopicPartition])).thenReturn(Some(backingPartition))
  }

  private def setupFeatures(featureVersions: Seq[FeatureVersion]): Unit = {
    if (featureVersions.isEmpty) return

    metadataCache match {
      case cache: KRaftMetadataCache =>
        when(cache.features()).thenReturn {
          new FinalizedFeatures(
            MetadataVersion.latestTesting,
            featureVersions.map { featureVersion =>
              featureVersion.featureName -> featureVersion.featureLevel.asInstanceOf[java.lang.Short]
            }.toMap.asJava,
            0,
            true
          )
        }

      case _ => throw new IllegalStateException("Test must set an instance of KRaftMetadataCache")
    }
  }

  @Test
  def testDescribeConfigsWithAuthorizer(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

    val operation = AclOperation.DESCRIBE_CONFIGS
    val resourceType = ResourceType.TOPIC
    val resourceName = "topic-1"
    val requestHeader = new RequestHeader(ApiKeys.DESCRIBE_CONFIGS, ApiKeys.DESCRIBE_CONFIGS.latestVersion,
      clientId, 0)

    val expectedActions = Seq(
      new Action(operation, new ResourcePattern(resourceType, resourceName, PatternType.LITERAL),
        1, true, true)
    )

    // Verify that authorize is only called once
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(expectedActions.asJava)))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    val configRepository: ConfigRepository = mock(classOf[ConfigRepository])
    val topicConfigs = new Properties()
    val propName = "min.insync.replicas"
    val propValue = "3"
    topicConfigs.put(propName, propValue)
    when(configRepository.topicConfig(resourceName)).thenReturn(topicConfigs)

    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.contains(resourceName)).thenReturn(true)

    val describeConfigsRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setIncludeSynonyms(true)
      .setResources(List(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName(resourceName)
        .setResourceType(ConfigResource.Type.TOPIC.id)).asJava))
      .build(requestHeader.apiVersion)
    val request = buildRequest(describeConfigsRequest, requestHeader = Option(requestHeader))

    kafkaApis = createKafkaApis(authorizer = Some(authorizer), configRepository = configRepository)
    kafkaApis.handleDescribeConfigsRequest(request)

    verify(authorizer).authorize(any(), ArgumentMatchers.eq(expectedActions.asJava))
    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    val results = response.data.results
    assertEquals(1, results.size)
    val describeConfigsResult = results.get(0)
    assertEquals(ConfigResource.Type.TOPIC.id, describeConfigsResult.resourceType)
    assertEquals(resourceName, describeConfigsResult.resourceName)
    val configs = describeConfigsResult.configs.asScala.filter(_.name == propName)
    assertEquals(1, configs.length)
    val describeConfigsResponseData = configs.head
    assertEquals(propName, describeConfigsResponseData.name)
    assertEquals(propValue, describeConfigsResponseData.value)
  }

  @Test
  def testDescribeConfigsRejectsBackingTopic(): Unit = {
    // Gemini r21 HIGH #159 — a DESCRIBE_CONFIGS-authorized principal targeting a backing topic
    // name must receive INVALID_TOPIC_EXCEPTION, not the substrate's actual configuration.
    // Without this guard the principal could read cleanup.policy / retention.ms / segment.bytes /
    // min.insync.replicas / replication.factor on the substrate that hosts every co-tenant
    // logical topic — confirming the backing name and exposing storage internals.
    val authorizer: Authorizer = mock(classOf[Authorizer])

    val operation = AclOperation.DESCRIBE_CONFIGS
    val resourceType = ResourceType.TOPIC
    val backingTopicName = "backing-topic-1"
    val requestHeader = new RequestHeader(ApiKeys.DESCRIBE_CONFIGS, ApiKeys.DESCRIBE_CONFIGS.latestVersion,
      clientId, 0)

    val expectedActions = Seq(
      new Action(operation, new ResourcePattern(resourceType, backingTopicName, PatternType.LITERAL),
        1, true, true)
    )
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(expectedActions.asJava)))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    val configRepository: ConfigRepository = mock(classOf[ConfigRepository])
    metadataCache = mock(classOf[KRaftMetadataCache])
    // metadataCache.contains intentionally NOT stubbed — the backing rejection must short-circuit
    // BEFORE the metadata-cache lookup so the substrate's configs are never loaded into memory.
    when(concentrationKernel.isBackingTopic(backingTopicName)).thenReturn(true)

    val describeConfigsRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setIncludeSynonyms(true)
      .setResources(List(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName(backingTopicName)
        .setResourceType(ConfigResource.Type.TOPIC.id)).asJava))
      .build(requestHeader.apiVersion)
    val request = buildRequest(describeConfigsRequest, requestHeader = Option(requestHeader))

    kafkaApis = createKafkaApis(authorizer = Some(authorizer), configRepository = configRepository)
    kafkaApis.handleDescribeConfigsRequest(request)

    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    val results = response.data.results
    assertEquals(1, results.size)
    val result = results.get(0)
    assertEquals(ConfigResource.Type.TOPIC.id, result.resourceType)
    assertEquals(backingTopicName, result.resourceName)
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, result.errorCode)
    assertEquals(0, result.configs.size,
      "Backing-topic rejection must return zero configs — no substrate settings may leak")
    // Substrate must not be loaded into memory — if topicConfig were called, the rejection
    // ran too late and a parallel codepath could have leaked the response.
    verify(configRepository, times(0)).topicConfig(backingTopicName)
  }

  @Test
  def testDescribeConfigsBackingShadowDefersToAuthorizationFailure(): Unit = {
    // Gemini r21 HIGH #159 — auth-first / shadow-second precedence. A principal lacking
    // DESCRIBE_CONFIGS on a backing topic name must receive TOPIC_AUTHORIZATION_FAILED, NOT
    // INVALID_TOPIC_EXCEPTION. The latter would let an unauthorized principal enumerate the
    // declared-backing set by probing names and observing the response code difference. Same
    // precedence pattern as #128/#137/#139/#145/#146.
    val authorizer: Authorizer = mock(classOf[Authorizer])

    val operation = AclOperation.DESCRIBE_CONFIGS
    val resourceType = ResourceType.TOPIC
    val backingTopicName = "backing-topic-1"
    val requestHeader = new RequestHeader(ApiKeys.DESCRIBE_CONFIGS, ApiKeys.DESCRIBE_CONFIGS.latestVersion,
      clientId, 0)

    val expectedActions = Seq(
      new Action(operation, new ResourcePattern(resourceType, backingTopicName, PatternType.LITERAL),
        1, true, true)
    )
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(expectedActions.asJava)))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)

    val configRepository: ConfigRepository = mock(classOf[ConfigRepository])
    metadataCache = mock(classOf[KRaftMetadataCache])

    val describeConfigsRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setIncludeSynonyms(true)
      .setResources(List(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName(backingTopicName)
        .setResourceType(ConfigResource.Type.TOPIC.id)).asJava))
      .build(requestHeader.apiVersion)
    val request = buildRequest(describeConfigsRequest, requestHeader = Option(requestHeader))

    kafkaApis = createKafkaApis(authorizer = Some(authorizer), configRepository = configRepository)
    kafkaApis.handleDescribeConfigsRequest(request)

    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    val results = response.data.results
    assertEquals(1, results.size)
    val result = results.get(0)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, result.errorCode,
      "Auth-first precedence — unauthorized backing probe must see TOPIC_AUTHORIZATION_FAILED, " +
        "not INVALID_TOPIC_EXCEPTION (which would leak the declared-backing set)")
    // The backing predicate must NOT have been consulted — the unauthorized partition catches
    // the resource before the describe loop runs. If isBackingTopic were called, an attacker
    // who can also issue allowed probes could time-side-channel the predicate cost.
    verify(concentrationKernel, times(0)).isBackingTopic(backingTopicName)
  }

  @Test
  def testDescribeConfigsBrokerRedactsConcentrationTopology(): Unit = {
    // r22 BLOCKER #202 — DescribeConfigs(BROKER) MUST NOT expose concentration.logical.topics
    // value. The config string encodes the full logical->backing topology
    // (logical-name:partitions:backing-name:partitions tuples). A principal with
    // DESCRIBE_CONFIGS on CLUSTER would otherwise read every backing-topic name and the
    // entire co-tenancy map. The key must still appear in the response (shape consistency
    // across brokers prevents key-presence probing of feature activation), but the value
    // must be null and isSensitive must be true.
    val authorizer: Authorizer = mock(classOf[Authorizer])

    val operation = AclOperation.DESCRIBE_CONFIGS
    val resourceType = ResourceType.CLUSTER
    val requestHeader = new RequestHeader(ApiKeys.DESCRIBE_CONFIGS, ApiKeys.DESCRIBE_CONFIGS.latestVersion,
      clientId, 0)

    val expectedActions = Seq(
      new Action(operation, new ResourcePattern(resourceType, Resource.CLUSTER_NAME, PatternType.LITERAL),
        1, true, true)
    )
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(expectedActions.asJava)))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    val topologyValue = "tenantA-events:4:__concentration-bk-1:32,tenantB-events:4:__concentration-bk-2:32"
    val topicConfigOverride = mutable.Map.empty[String, String]
    topicConfigOverride.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, topologyValue)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val describeConfigsRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setIncludeSynonyms(true)
      .setResources(List(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName(brokerId.toString)
        .setResourceType(ConfigResource.Type.BROKER.id)).asJava))
      .build(requestHeader.apiVersion)
    val request = buildRequest(describeConfigsRequest, requestHeader = Option(requestHeader))

    kafkaApis = createKafkaApis(authorizer = Some(authorizer), overrideProperties = topicConfigOverride)
    kafkaApis.handleDescribeConfigsRequest(request)

    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    val results = response.data.results
    assertEquals(1, results.size)
    val result = results.get(0)
    assertEquals(ConfigResource.Type.BROKER.id, result.resourceType)
    val concentrationEntries = result.configs.asScala
      .filter(_.name == ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG)
    assertEquals(1, concentrationEntries.size,
      "concentration.logical.topics key must remain in the BROKER response (shape consistency)")
    val entry = concentrationEntries.head
    assertNull(entry.value,
      s"concentration.logical.topics value MUST be redacted to null in DescribeConfigs(BROKER); got '${entry.value}'")
    assertTrue(entry.isSensitive,
      "concentration.logical.topics MUST be marked isSensitive=true so admin clients/log scrubbers redact it")
    // Synonyms (e.g. STATIC_BROKER_CONFIG) must also be redacted — configSynonyms branches on
    // the same isSensitive flag, so a leak in a synonym entry would defeat the primary redaction.
    entry.synonyms.asScala.foreach { syn =>
      assertNull(syn.value,
        s"Synonym ${syn.name} for concentration.logical.topics MUST also be redacted; got '${syn.value}'")
    }
    // Defence-in-depth: the raw topology string MUST NOT appear anywhere in the serialized
    // response — guards against future re-introduction via a parallel code path.
    val serialized = response.data.toString
    assertFalse(serialized.contains("__concentration-bk-"),
      s"Backing topic name leaked through DescribeConfigs(BROKER): $serialized")
    assertFalse(serialized.contains("tenantA-events:4"),
      s"Topology tuple leaked through DescribeConfigs(BROKER): $serialized")
  }

  @Test
  def testElectLeadersForwarding(): Unit = {
    val requestBuilder = new ElectLeadersRequest.Builder(ElectionType.PREFERRED, null, 30000)
    testKraftForwarding(ApiKeys.ELECT_LEADERS, requestBuilder)
  }

  @Test
  def testIncrementalConsumerGroupAlterConfigs(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

    val consumerGroupId = "consumer_group_1"
    val resource = new ConfigResource(ConfigResource.Type.GROUP, consumerGroupId)

    authorizeResource(authorizer, AclOperation.ALTER_CONFIGS, ResourceType.GROUP,
      consumerGroupId, AuthorizationResult.ALLOWED)

    val requestHeader = new RequestHeader(ApiKeys.INCREMENTAL_ALTER_CONFIGS,
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion, clientId, 0)

    val incrementalAlterConfigsRequest = getIncrementalAlterConfigRequestBuilder(
      Seq(resource), "consumer.session.timeout.ms", "45000").build(requestHeader.apiVersion)
    val request = buildRequest(incrementalAlterConfigsRequest, requestHeader = Option(requestHeader))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    createKafkaApis(authorizer = Some(authorizer)).handleIncrementalAlterConfigsRequest(request)
    verify(forwardingManager, times(1)).forwardRequest(
      any(),
      any(),
      any()
    )
  }

  @Test
  def testDescribeConfigsConsumerGroup(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val operation = AclOperation.DESCRIBE_CONFIGS
    val resourceType = ResourceType.GROUP
    val consumerGroupId = "consumer_group_1"
    val requestHeader =
      new RequestHeader(ApiKeys.DESCRIBE_CONFIGS, ApiKeys.DESCRIBE_CONFIGS.latestVersion, clientId, 0)
    val expectedActions = Seq(
      new Action(operation, new ResourcePattern(resourceType, consumerGroupId, PatternType.LITERAL),
        1, true, true)
    )

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(expectedActions.asJava)))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    val configRepository: ConfigRepository = mock(classOf[ConfigRepository])
    val cgConfigs = new Properties()
    cgConfigs.put(CONSUMER_SESSION_TIMEOUT_MS_CONFIG, GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_DEFAULT.toString)
    cgConfigs.put(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT.toString)
    cgConfigs.put(SHARE_SESSION_TIMEOUT_MS_CONFIG, GroupCoordinatorConfig.SHARE_GROUP_SESSION_TIMEOUT_MS_DEFAULT.toString)
    cgConfigs.put(SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, GroupCoordinatorConfig.SHARE_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT.toString)
    cgConfigs.put(SHARE_RECORD_LOCK_DURATION_MS_CONFIG, ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_DEFAULT.toString)
    cgConfigs.put(SHARE_AUTO_OFFSET_RESET_CONFIG, GroupConfig.SHARE_AUTO_OFFSET_RESET_DEFAULT)
    when(configRepository.groupConfig(consumerGroupId)).thenReturn(cgConfigs)

    val describeConfigsRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setIncludeSynonyms(true)
      .setResources(List(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName(consumerGroupId)
        .setResourceType(ConfigResource.Type.GROUP.id)).asJava))
      .build(requestHeader.apiVersion)
    val request = buildRequest(describeConfigsRequest,
      requestHeader = Option(requestHeader))
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    createKafkaApis(authorizer = Some(authorizer), configRepository = configRepository)
      .handleDescribeConfigsRequest(request)

    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    // Verify that authorize is only called once
    verify(authorizer, times(1)).authorize(any(), any())
    val results = response.data.results
    assertEquals(1, results.size)
    val describeConfigsResult = results.get(0)

    assertEquals(ConfigResource.Type.GROUP.id, describeConfigsResult.resourceType)
    assertEquals(consumerGroupId, describeConfigsResult.resourceName)
    val configs = describeConfigsResult.configs
    assertEquals(cgConfigs.size, configs.size)
  }

  @Test
  def testAlterConfigsClientMetrics(): Unit = {
    val subscriptionName = "client_metric_subscription_1"
    val authorizedResource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, subscriptionName)

    val props = ClientMetricsTestUtils.defaultProperties
    val configEntries = new util.ArrayList[AlterConfigsRequest.ConfigEntry]()
    props.forEach((x, y) =>
      configEntries.add(new AlterConfigsRequest.ConfigEntry(x.asInstanceOf[String], y.asInstanceOf[String])))

    val configs = Map(authorizedResource -> new AlterConfigsRequest.Config(configEntries))

    val requestHeader = new RequestHeader(ApiKeys.ALTER_CONFIGS, ApiKeys.ALTER_CONFIGS.latestVersion, clientId, 0)
    val apiRequest = new AlterConfigsRequest.Builder(configs.asJava, false).build(requestHeader.apiVersion)
    val request = buildRequest(apiRequest)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis()
    kafkaApis.handleAlterConfigsRequest(request)
    verify(forwardingManager, times(1)).forwardRequest(
      any(),
      any(),
      any()
    )
  }

  @Test
  def testIncrementalClientMetricAlterConfigs(): Unit = {
    val subscriptionName = "client_metric_subscription_1"
    val resource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, subscriptionName)

    val requestHeader = new RequestHeader(ApiKeys.INCREMENTAL_ALTER_CONFIGS,
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion, clientId, 0)

    val incrementalAlterConfigsRequest = getIncrementalAlterConfigRequestBuilder(
      Seq(resource), "metrics", "foo.bar").build(requestHeader.apiVersion)
    val request = buildRequest(incrementalAlterConfigsRequest, requestHeader = Option(requestHeader))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis()
    kafkaApis.handleIncrementalAlterConfigsRequest(request)
    verify(forwardingManager, times(1)).forwardRequest(
      any(),
      any(),
      any()
    )
  }

  private def getIncrementalAlterConfigRequestBuilder(configResources: Seq[ConfigResource],
                                                      configName: String,
                                                      configValue: String): IncrementalAlterConfigsRequest.Builder = {
    val resourceMap = configResources.map(configResource => {
      val entryToBeModified = new ConfigEntry(configName, configValue)
      configResource -> Set(new AlterConfigOp(entryToBeModified, OpType.SET)).asJavaCollection
    }).toMap.asJava
    new IncrementalAlterConfigsRequest.Builder(resourceMap, false)
  }

  @Test
  def testDescribeConfigsClientMetrics(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val operation = AclOperation.DESCRIBE_CONFIGS
    val resourceType = ResourceType.CLUSTER
    val subscriptionName = "client_metric_subscription_1"
    val requestHeader =
      new RequestHeader(ApiKeys.DESCRIBE_CONFIGS, ApiKeys.DESCRIBE_CONFIGS.latestVersion, clientId, 0)
    val expectedActions = Seq(
      new Action(operation, new ResourcePattern(resourceType, Resource.CLUSTER_NAME, PatternType.LITERAL),
        1, true, true)
    )

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(expectedActions.asJava)))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    val resource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, subscriptionName)
    val configRepository: ConfigRepository = mock(classOf[ConfigRepository])
    val cmConfigs = ClientMetricsTestUtils.defaultProperties
    when(configRepository.config(resource)).thenReturn(cmConfigs)

    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.contains(subscriptionName)).thenReturn(true)

    val describeConfigsRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setIncludeSynonyms(true)
      .setResources(List(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName(subscriptionName)
        .setResourceType(ConfigResource.Type.CLIENT_METRICS.id)).asJava))
      .build(requestHeader.apiVersion)
    val request = buildRequest(describeConfigsRequest,
      requestHeader = Option(requestHeader))

    kafkaApis = createKafkaApis(authorizer = Some(authorizer), configRepository = configRepository)
    kafkaApis.handleDescribeConfigsRequest(request)

    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    // Verify that authorize is only called once
    verify(authorizer, times(1)).authorize(any(), any())
    val results = response.data.results
    assertEquals(1, results.size)
    val describeConfigsResult = results.get(0)

    assertEquals(ConfigResource.Type.CLIENT_METRICS.id, describeConfigsResult.resourceType)
    assertEquals(subscriptionName, describeConfigsResult.resourceName)
    val configs = describeConfigsResult.configs
    assertEquals(cmConfigs.size, configs.size)
  }

  @Test
  def testDescribeQuorumForwardedForKRaftClusters(): Unit = {
    val requestData = DescribeQuorumRequest.singletonRequest(KafkaRaftServer.MetadataPartition)
    val requestBuilder = new DescribeQuorumRequest.Builder(requestData)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis()
    testForwardableApi(kafkaApis = kafkaApis,
      ApiKeys.DESCRIBE_QUORUM,
      requestBuilder
    )
  }

  private def testKraftForwarding(
    apiKey: ApiKeys,
    requestBuilder: AbstractRequest.Builder[_ <: AbstractRequest]
  ): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis()
    testForwardableApi(kafkaApis = kafkaApis,
      apiKey,
      requestBuilder
    )
  }

  private def testForwardableApi(
    kafkaApis: KafkaApis,
    apiKey: ApiKeys,
    requestBuilder: AbstractRequest.Builder[_ <: AbstractRequest]
  ): Unit = {
    val topicHeader = new RequestHeader(apiKey, apiKey.latestVersion,
      clientId, 0)

    val apiRequest = requestBuilder.build(topicHeader.apiVersion)
    val request = buildRequest(apiRequest)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    val forwardCallback: ArgumentCaptor[Option[AbstractResponse] => Unit] = ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)
    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      forwardCallback.capture()
    )
    assertNotNull(request.buffer, "The buffer was unexpectedly deallocated after " +
      s"`handle` returned (is $apiKey marked as forwardable in `ApiKeys`?)")

    val expectedResponse = apiRequest.getErrorResponse(Errors.NOT_CONTROLLER.exception)
    forwardCallback.getValue.apply(Some(expectedResponse))

    val capturedResponse = verifyNoThrottling[AbstractResponse](request)
    assertEquals(expectedResponse.data, capturedResponse.data)
  }

  private def authorizeResource(authorizer: Authorizer,
                                operation: AclOperation,
                                resourceType: ResourceType,
                                resourceName: String,
                                result: AuthorizationResult,
                                logIfAllowed: Boolean = true,
                                logIfDenied: Boolean = true): Unit = {
    val expectedAuthorizedAction = if (operation == AclOperation.CLUSTER_ACTION)
      new Action(operation,
        new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL),
        1, logIfAllowed, logIfDenied)
    else
      new Action(operation,
        new ResourcePattern(resourceType, resourceName, PatternType.LITERAL),
        1, logIfAllowed, logIfDenied)

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(Seq(expectedAuthorizedAction).asJava)))
      .thenReturn(Seq(result).asJava)
  }

  @Test
  def testIncrementalAlterConfigsWithAuthorizer(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

    val localResource = new ConfigResource(ConfigResource.Type.BROKER_LOGGER, "localResource")
    val forwardedResource = new ConfigResource(ConfigResource.Type.GROUP, "forwardedResource")

    val requestHeader = new RequestHeader(ApiKeys.INCREMENTAL_ALTER_CONFIGS, ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion, clientId, 0)

    val incrementalAlterConfigsRequest = getIncrementalAlterConfigRequestBuilder(Seq(localResource, forwardedResource))
      .build(requestHeader.apiVersion)
    val request = buildRequest(incrementalAlterConfigsRequest, requestHeader = Option(requestHeader))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleIncrementalAlterConfigsRequest(request)

    verify(authorizer, times(1)).authorize(any(), any())
    verify(forwardingManager, times(1)).forwardRequest(
      any(),
      any(),
      any()
    )
  }

  private def getIncrementalAlterConfigRequestBuilder(configResources: Seq[ConfigResource]): IncrementalAlterConfigsRequest.Builder = {
    val resourceMap = configResources.map(configResource => {
      configResource -> Set(
        new AlterConfigOp(new ConfigEntry("foo", "bar"),
        OpType.SET)).asJavaCollection
    }).toMap.asJava

    new IncrementalAlterConfigsRequest.Builder(resourceMap, false)
  }

  @ParameterizedTest
  @CsvSource(value = Array("0,1500", "1500,0", "3000,1000"))
  def testKRaftControllerThrottleTimeEnforced(
    controllerThrottleTimeMs: Int,
    requestThrottleTimeMs: Int
  ): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val topicToCreate = new CreatableTopic()
      .setName("topic")
      .setNumPartitions(1)
      .setReplicationFactor(1.toShort)

    val requestData = new CreateTopicsRequestData()
    requestData.topics().add(topicToCreate)

    val requestBuilder = new CreateTopicsRequest.Builder(requestData).build()
    val request = buildRequest(requestBuilder)

    kafkaApis = createKafkaApis()
    val forwardCallback: ArgumentCaptor[Option[AbstractResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(request, time.milliseconds()))
      .thenReturn(requestThrottleTimeMs)

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      forwardCallback.capture()
    )

    val responseData = new CreateTopicsResponseData()
      .setThrottleTimeMs(controllerThrottleTimeMs)
    responseData.topics().add(new CreatableTopicResult()
      .setErrorCode(Errors.THROTTLING_QUOTA_EXCEEDED.code))

    forwardCallback.getValue.apply(Some(new CreateTopicsResponse(responseData)))

    val expectedThrottleTimeMs = math.max(controllerThrottleTimeMs, requestThrottleTimeMs)

    verify(clientRequestQuotaManager).throttle(
      ArgumentMatchers.eq(request),
      any[ThrottleCallback](),
      ArgumentMatchers.eq(expectedThrottleTimeMs)
    )

    assertEquals(expectedThrottleTimeMs, responseData.throttleTimeMs)
  }

  @Test
  def testCreateTopicsForwardsUnchangedWhenNoLogicalTopicsDeclared(): Unit = {
    // No declared logical topics — the interceptor must hand off to the normal forwarding path
    // (the 2-arg forwardRequest overload, identical to a vanilla CreateTopics).
    when(concentrationKernel.allDeclaredLogicalTopicNames()).thenReturn(Collections.emptySet[String])
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val requestData = new CreateTopicsRequestData()
    requestData.topics().add(new CreatableTopic().setName("orders").setNumPartitions(1).setReplicationFactor(1.toShort))
    val request = buildRequest(new CreateTopicsRequest.Builder(requestData).build())

    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      any[Option[AbstractResponse] => Unit]()
    )
    verify(forwardingManager, never).forwardRequest(
      any[RequestChannel.Request](),
      any[AbstractRequest](),
      any[Option[AbstractResponse] => Unit]()
    )
    verify(requestChannel, never).sendResponse(any(), any(), any())
  }

  @Test
  def testCreateTopicsRejectsAllShadowingNamesWithoutForwarding(): Unit = {
    // Every requested topic name collides with a declared logical topic. The interceptor must
    // synthesize a CreateTopicsResponse with TOPIC_ALREADY_EXISTS per name and short-circuit —
    // no forward to the controller.
    when(concentrationKernel.allDeclaredLogicalTopicNames())
      .thenReturn(Set("orders", "events").asJava)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val requestData = new CreateTopicsRequestData()
    requestData.topics().add(new CreatableTopic().setName("orders").setNumPartitions(1).setReplicationFactor(1.toShort))
    requestData.topics().add(new CreatableTopic().setName("events").setNumPartitions(1).setReplicationFactor(1.toShort))
    val request = buildRequest(new CreateTopicsRequest.Builder(requestData).build())

    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    verify(forwardingManager, never).forwardRequest(
      any[RequestChannel.Request](),
      any[Option[AbstractResponse] => Unit]()
    )
    verify(forwardingManager, never).forwardRequest(
      any[RequestChannel.Request](),
      any[AbstractRequest](),
      any[Option[AbstractResponse] => Unit]()
    )

    val response = verifyNoThrottling[CreateTopicsResponse](request)
    val byName = response.data.topics().iterator().asScala.map(t => t.name() -> t.errorCode()).toMap
    assertEquals(2, byName.size)
    assertEquals(Errors.TOPIC_ALREADY_EXISTS.code, byName("orders"))
    assertEquals(Errors.TOPIC_ALREADY_EXISTS.code, byName("events"))
  }

  @Test
  def testCreateTopicsForwardsRemainderAndInjectsShadowRejections(): Unit = {
    // Mixed request: one logical-shadowing topic and one normal topic. The interceptor must
    // mutate the request body (drop the shadow), forward the remainder via the 3-arg overload,
    // and merge a TOPIC_ALREADY_EXISTS entry into the controller's reply.
    when(concentrationKernel.allDeclaredLogicalTopicNames())
      .thenReturn(Collections.singleton[String]("orders"))
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val requestData = new CreateTopicsRequestData()
    requestData.topics().add(new CreatableTopic().setName("orders").setNumPartitions(1).setReplicationFactor(1.toShort))
    requestData.topics().add(new CreatableTopic().setName("items").setNumPartitions(1).setReplicationFactor(1.toShort))
    val request = buildRequest(new CreateTopicsRequest.Builder(requestData).build())

    kafkaApis = createKafkaApis()
    val forwardedBody: ArgumentCaptor[AbstractRequest] = ArgumentCaptor.forClass(classOf[AbstractRequest])
    val forwardCallback: ArgumentCaptor[Option[AbstractResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      forwardedBody.capture(),
      forwardCallback.capture()
    )

    val forwarded = forwardedBody.getValue.asInstanceOf[CreateTopicsRequest]
    val forwardedNames = forwarded.data.topics().iterator().asScala.map(_.name()).toSet
    assertEquals(Set("items"), forwardedNames)

    // Controller responds with NONE for "items"; the interceptor must inject the
    // TOPIC_ALREADY_EXISTS entry for the shadow we removed before delivering to the client.
    val controllerResponseData = new CreateTopicsResponseData()
    controllerResponseData.topics().add(new CreatableTopicResult().setName("items").setErrorCode(Errors.NONE.code))
    forwardCallback.getValue.apply(Some(new CreateTopicsResponse(controllerResponseData)))

    val response = verifyNoThrottling[CreateTopicsResponse](request)
    val byName = response.data.topics().iterator().asScala.map(t => t.name() -> t.errorCode()).toMap
    assertEquals(2, byName.size)
    assertEquals(Errors.NONE.code, byName("items"))
    assertEquals(Errors.TOPIC_ALREADY_EXISTS.code, byName("orders"))
  }

  @Test
  def testCreateTopicsShadowReturnsAuthFailedForUnauthorizedNames(): Unit = {
    // r15 BLOCKER N1: the shadow-rejection short-circuit must not bypass the controller's
    // CreateTopics authorization. A caller without CREATE on the cluster nor CREATE on the
    // colliding topic must receive TOPIC_AUTHORIZATION_FAILED — not TOPIC_ALREADY_EXISTS —
    // otherwise the shadow path leaks the declared-logical-topic set to unauthenticated
    // clients (info disclosure) and silently flips the controller's auth verdict (ACL bypass).
    when(concentrationKernel.allDeclaredLogicalTopicNames())
      .thenReturn(Set("orders", "events").asJava)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    // DENY everything: no CLUSTER#CREATE, no TOPIC#CREATE for "orders" or "events".
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenAnswer { invocation =>
        invocation.getArgument(1).asInstanceOf[util.List[Action]].asScala
          .map(_ => AuthorizationResult.DENIED).asJava
      }

    val requestData = new CreateTopicsRequestData()
    requestData.topics().add(new CreatableTopic().setName("orders").setNumPartitions(1).setReplicationFactor(1.toShort))
    requestData.topics().add(new CreatableTopic().setName("events").setNumPartitions(1).setReplicationFactor(1.toShort))
    val request = buildRequest(new CreateTopicsRequest.Builder(requestData).build())

    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    // Every requested name collides AND is unauthorized — no forward.
    verify(forwardingManager, never).forwardRequest(
      any[RequestChannel.Request](),
      any[Option[AbstractResponse] => Unit]()
    )
    verify(forwardingManager, never).forwardRequest(
      any[RequestChannel.Request](),
      any[AbstractRequest](),
      any[Option[AbstractResponse] => Unit]()
    )

    val response = verifyNoThrottling[CreateTopicsResponse](request)
    val byName = response.data.topics().iterator().asScala.map(t => t.name() -> t.errorCode()).toMap
    assertEquals(2, byName.size)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, byName("orders"),
      "unauthorized colliding name must surface as AUTH_FAILED (not ALREADY_EXISTS) to avoid leaking the declared set")
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, byName("events"))
  }

  @Test
  def testCreateTopicsShadowMixesAuthFailedAndAlreadyExistsAndForwardsRemainder(): Unit = {
    // r15 BLOCKER N1: mixed scenario — three requested topics:
    //   - "orders" (logical, caller has TOPIC#CREATE)        => TOPIC_ALREADY_EXISTS
    //   - "events" (logical, caller LACKS TOPIC#CREATE)      => TOPIC_AUTHORIZATION_FAILED
    //   - "items"  (non-shadowed, forwarded to controller)   => controller verdict (NONE here)
    when(concentrationKernel.allDeclaredLogicalTopicNames())
      .thenReturn(Set("orders", "events").asJava)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenAnswer { invocation =>
        invocation.getArgument(1).asInstanceOf[util.List[Action]].asScala.map { action =>
          // CLUSTER#CREATE denied; TOPIC#CREATE allowed for "orders" only.
          if (action.resourcePattern().resourceType() == ResourceType.CLUSTER) AuthorizationResult.DENIED
          else if (action.resourcePattern().name() == "orders") AuthorizationResult.ALLOWED
          else AuthorizationResult.DENIED
        }.asJava
      }

    val requestData = new CreateTopicsRequestData()
    requestData.topics().add(new CreatableTopic().setName("orders").setNumPartitions(1).setReplicationFactor(1.toShort))
    requestData.topics().add(new CreatableTopic().setName("events").setNumPartitions(1).setReplicationFactor(1.toShort))
    requestData.topics().add(new CreatableTopic().setName("items").setNumPartitions(1).setReplicationFactor(1.toShort))
    val request = buildRequest(new CreateTopicsRequest.Builder(requestData).build())

    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    val forwardedBody: ArgumentCaptor[AbstractRequest] = ArgumentCaptor.forClass(classOf[AbstractRequest])
    val forwardCallback: ArgumentCaptor[Option[AbstractResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      forwardedBody.capture(),
      forwardCallback.capture()
    )

    // Only the non-shadowed name is forwarded; the controller does its own auth.
    val forwarded = forwardedBody.getValue.asInstanceOf[CreateTopicsRequest]
    val forwardedNames = forwarded.data.topics().iterator().asScala.map(_.name()).toSet
    assertEquals(Set("items"), forwardedNames)

    val controllerResponseData = new CreateTopicsResponseData()
    controllerResponseData.topics().add(new CreatableTopicResult().setName("items").setErrorCode(Errors.NONE.code))
    forwardCallback.getValue.apply(Some(new CreateTopicsResponse(controllerResponseData)))

    val response = verifyNoThrottling[CreateTopicsResponse](request)
    val byName = response.data.topics().iterator().asScala.map(t => t.name() -> t.errorCode()).toMap
    assertEquals(3, byName.size)
    assertEquals(Errors.NONE.code, byName("items"))
    assertEquals(Errors.TOPIC_ALREADY_EXISTS.code, byName("orders"),
      "authorized colliding name must surface as ALREADY_EXISTS")
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, byName("events"),
      "unauthorized colliding name must surface as AUTH_FAILED")
  }

  @Test
  def testCreateTopicsShadowAllowsClusterAuthToShortCircuit(): Unit = {
    // r15 BLOCKER N1: a caller with CLUSTER#CREATE is authorized for any topic name. The
    // interceptor must NOT call the TOPIC-level filter (would duplicate auth work) and must
    // surface ALREADY_EXISTS for shadowed names — same as the pre-N1 behavior.
    when(concentrationKernel.allDeclaredLogicalTopicNames())
      .thenReturn(Collections.singleton[String]("orders"))
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenAnswer { invocation =>
        invocation.getArgument(1).asInstanceOf[util.List[Action]].asScala.map { action =>
          // CLUSTER#CREATE allowed; bail out so the per-topic filter is never invoked.
          if (action.resourcePattern().resourceType() == ResourceType.CLUSTER) AuthorizationResult.ALLOWED
          else AuthorizationResult.DENIED
        }.asJava
      }

    val requestData = new CreateTopicsRequestData()
    requestData.topics().add(new CreatableTopic().setName("orders").setNumPartitions(1).setReplicationFactor(1.toShort))
    val request = buildRequest(new CreateTopicsRequest.Builder(requestData).build())

    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[CreateTopicsResponse](request)
    val byName = response.data.topics().iterator().asScala.map(t => t.name() -> t.errorCode()).toMap
    assertEquals(1, byName.size)
    assertEquals(Errors.TOPIC_ALREADY_EXISTS.code, byName("orders"))
  }

  @Test
  def testFindCoordinatorAutoTopicCreationForOffsetTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.GROUP)
  }

  @Test
  def testFindCoordinatorAutoTopicCreationForTxnTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.TRANSACTION)
  }

  @Test
  def testFindCoordinatorNotEnoughBrokersForOffsetTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.GROUP, hasEnoughLiveBrokers = false)
  }

  @Test
  def testFindCoordinatorNotEnoughBrokersForTxnTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.TRANSACTION, hasEnoughLiveBrokers = false)
  }

  @Test
  def testOldFindCoordinatorAutoTopicCreationForOffsetTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.GROUP, version = 3)
  }

  @Test
  def testOldFindCoordinatorAutoTopicCreationForTxnTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.TRANSACTION, version = 3)
  }

  @Test
  def testOldFindCoordinatorNotEnoughBrokersForOffsetTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.GROUP, hasEnoughLiveBrokers = false, version = 3)
  }

  @Test
  def testOldFindCoordinatorNotEnoughBrokersForTxnTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.TRANSACTION, hasEnoughLiveBrokers = false, version = 3)
  }

  @Test
  def testFindCoordinatorTooOldForShareStateTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.SHARE, checkAutoCreateTopic = false, version = 5)
  }

  @Test
  def testFindCoordinatorNoShareCoordinatorForShareStateTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.SHARE, checkAutoCreateTopic = false)
  }

  private def testFindCoordinatorWithTopicCreation(coordinatorType: CoordinatorType,
                                                   hasEnoughLiveBrokers: Boolean = true,
                                                   checkAutoCreateTopic: Boolean = true,
                                                   version: Short = ApiKeys.FIND_COORDINATOR.latestVersion): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

    val requestHeader = new RequestHeader(ApiKeys.FIND_COORDINATOR, version, clientId, 0)

    val numBrokersNeeded = 3

    setupBrokerMetadata(hasEnoughLiveBrokers, numBrokersNeeded)

    val requestTimeout = 10
    val topicConfigOverride = mutable.Map.empty[String, String]
    topicConfigOverride.put(ServerConfigs.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout.toString)

    val groupId = "group"
    val topicId = Uuid.randomUuid
    val partition = 0
    var key:String = groupId

    val topicName =
      coordinatorType match {
        case CoordinatorType.GROUP =>
          topicConfigOverride.put(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, numBrokersNeeded.toString)
          topicConfigOverride.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, numBrokersNeeded.toString)
          when(groupCoordinator.groupMetadataTopicConfigs).thenReturn(new Properties)
          authorizeResource(authorizer, AclOperation.DESCRIBE, ResourceType.GROUP,
            groupId, AuthorizationResult.ALLOWED)
          Topic.GROUP_METADATA_TOPIC_NAME
        case CoordinatorType.TRANSACTION =>
          topicConfigOverride.put(TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, numBrokersNeeded.toString)
          topicConfigOverride.put(TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, numBrokersNeeded.toString)
          when(txnCoordinator.transactionTopicConfigs).thenReturn(new Properties)
          authorizeResource(authorizer, AclOperation.DESCRIBE, ResourceType.TRANSACTIONAL_ID,
            groupId, AuthorizationResult.ALLOWED)
          Topic.TRANSACTION_STATE_TOPIC_NAME
        case CoordinatorType.SHARE =>
          authorizeResource(authorizer, AclOperation.CLUSTER_ACTION, ResourceType.CLUSTER,
            Resource.CLUSTER_NAME, AuthorizationResult.ALLOWED)
          key = "%s:%s:%d" format(groupId, topicId, partition)
          Topic.SHARE_GROUP_STATE_TOPIC_NAME
        case _ =>
          throw new IllegalStateException(s"Unknown coordinator type $coordinatorType")
      }

    val findCoordinatorRequestBuilder = if (version >= 4) {
      new FindCoordinatorRequest.Builder(
        new FindCoordinatorRequestData()
          .setKeyType(coordinatorType.id())
          .setCoordinatorKeys(asList(key)))
    } else {
      new FindCoordinatorRequest.Builder(
        new FindCoordinatorRequestData()
          .setKeyType(coordinatorType.id())
          .setKey(key))
    }
    val request = buildRequest(findCoordinatorRequestBuilder.build(requestHeader.apiVersion))
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    val capturedRequest = verifyTopicCreation(topicName, true, true, request)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer),
      overrideProperties = topicConfigOverride)
    kafkaApis.handleFindCoordinatorRequest(request)

    val response = verifyNoThrottling[FindCoordinatorResponse](request)
    if (coordinatorType == CoordinatorType.SHARE && version < 6) {
      assertEquals(Errors.INVALID_REQUEST.code, response.data.coordinators.get(0).errorCode)
    } else if (version >= 4) {
      assertEquals(Errors.COORDINATOR_NOT_AVAILABLE.code, response.data.coordinators.get(0).errorCode)
      assertEquals(key, response.data.coordinators.get(0).key)
    } else {
      assertEquals(Errors.COORDINATOR_NOT_AVAILABLE.code, response.data.errorCode)
      assertTrue(capturedRequest.getValue.isEmpty)
    }
    if (checkAutoCreateTopic) {
      assertTrue(capturedRequest.getValue.isEmpty)
    }
  }

  @Test
  def testMetadataAutoTopicCreationForOffsetTopic(): Unit = {
    testMetadataAutoTopicCreation(Topic.GROUP_METADATA_TOPIC_NAME, enableAutoTopicCreation = true,
      expectedError = Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testMetadataAutoTopicCreationForTxnTopic(): Unit = {
    testMetadataAutoTopicCreation(Topic.TRANSACTION_STATE_TOPIC_NAME, enableAutoTopicCreation = true,
      expectedError = Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testMetadataAutoTopicCreationForNonInternalTopic(): Unit = {
    testMetadataAutoTopicCreation("topic", enableAutoTopicCreation = true,
      expectedError = Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testMetadataAutoTopicCreationDisabledForOffsetTopic(): Unit = {
    testMetadataAutoTopicCreation(Topic.GROUP_METADATA_TOPIC_NAME, enableAutoTopicCreation = false,
      expectedError = Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testMetadataAutoTopicCreationDisabledForTxnTopic(): Unit = {
    testMetadataAutoTopicCreation(Topic.TRANSACTION_STATE_TOPIC_NAME, enableAutoTopicCreation = false,
      expectedError = Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testMetadataAutoTopicCreationDisabledForNonInternalTopic(): Unit = {
    testMetadataAutoTopicCreation("topic", enableAutoTopicCreation = false,
      expectedError = Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testMetadataAutoCreationDisabledForNonInternal(): Unit = {
    testMetadataAutoTopicCreation("topic", enableAutoTopicCreation = true,
      expectedError = Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  private def testMetadataAutoTopicCreation(topicName: String,
                                            enableAutoTopicCreation: Boolean,
                                            expectedError: Errors): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

    val requestHeader = new RequestHeader(ApiKeys.METADATA, ApiKeys.METADATA.latestVersion,
      clientId, 0)

    val numBrokersNeeded = 3
    addTopicToMetadataCache("some-topic", 1, 3)

    authorizeResource(authorizer, AclOperation.DESCRIBE, ResourceType.TOPIC,
      topicName, AuthorizationResult.ALLOWED)

    if (enableAutoTopicCreation)
      authorizeResource(authorizer, AclOperation.CREATE, ResourceType.CLUSTER,
        Resource.CLUSTER_NAME, AuthorizationResult.ALLOWED, logIfDenied = false)

    val topicConfigOverride = mutable.Map.empty[String, String]
    val isInternal =
      topicName match {
        case Topic.GROUP_METADATA_TOPIC_NAME =>
          topicConfigOverride.put(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, numBrokersNeeded.toString)
          topicConfigOverride.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, numBrokersNeeded.toString)
          when(groupCoordinator.groupMetadataTopicConfigs).thenReturn(new Properties)
          true

        case Topic.TRANSACTION_STATE_TOPIC_NAME =>
          topicConfigOverride.put(TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, numBrokersNeeded.toString)
          topicConfigOverride.put(TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, numBrokersNeeded.toString)
          when(txnCoordinator.transactionTopicConfigs).thenReturn(new Properties)
          true
        case _ =>
          topicConfigOverride.put(ServerLogConfigs.NUM_PARTITIONS_CONFIG, numBrokersNeeded.toString)
          topicConfigOverride.put(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, numBrokersNeeded.toString)
          false
      }

    val metadataRequest = new MetadataRequest.Builder(
      List(topicName).asJava, enableAutoTopicCreation
    ).build(requestHeader.apiVersion)
    val request = buildRequest(metadataRequest)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    val capturedRequest = verifyTopicCreation(topicName, enableAutoTopicCreation, isInternal, request)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer), overrideProperties = topicConfigOverride)
    kafkaApis.handleTopicMetadataRequest(request)

    val response = verifyNoThrottling[MetadataResponse](request)
    val expectedMetadataResponse = util.Collections.singletonList(new TopicMetadata(
      expectedError,
      topicName,
      isInternal,
      util.Collections.emptyList()
    ))

    assertEquals(expectedMetadataResponse, response.topicMetadata())

    if (enableAutoTopicCreation) {
      assertTrue(capturedRequest.getValue.isDefined)
      assertEquals(request.context, capturedRequest.getValue.get)
    }
  }

  private def verifyTopicCreation(topicName: String,
                                  enableAutoTopicCreation: Boolean,
                                  isInternal: Boolean,
                                  request: RequestChannel.Request): ArgumentCaptor[Option[RequestContext]] = {
    val capturedRequest: ArgumentCaptor[Option[RequestContext]] = ArgumentCaptor.forClass(classOf[Option[RequestContext]])
    if (enableAutoTopicCreation) {
      when(clientControllerQuotaManager.newPermissiveQuotaFor(ArgumentMatchers.eq(request)))
        .thenReturn(UnboundedControllerMutationQuota)

      when(autoTopicCreationManager.createTopics(
        ArgumentMatchers.eq(Set(topicName)),
        ArgumentMatchers.eq(UnboundedControllerMutationQuota),
        capturedRequest.capture())).thenReturn(
        Seq(new MetadataResponseTopic()
        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
        .setIsInternal(isInternal)
        .setName(topicName))
      )
    }
    capturedRequest
  }

  private def setupBrokerMetadata(hasEnoughLiveBrokers: Boolean, numBrokersNeeded: Int): Unit = {
    addTopicToMetadataCache("some-topic", 1,
      if (hasEnoughLiveBrokers)
        numBrokersNeeded
      else
        numBrokersNeeded - 1)
  }

  @Test
  def testInvalidMetadataRequestReturnsError(): Unit = {
    // Construct invalid MetadataRequestTopics. We will try each one separately and ensure the error is thrown.
    val topics = List(new MetadataRequestData.MetadataRequestTopic().setName(null).setTopicId(Uuid.randomUuid()),
      new MetadataRequestData.MetadataRequestTopic().setName(null),
      new MetadataRequestData.MetadataRequestTopic().setTopicId(Uuid.randomUuid()),
      new MetadataRequestData.MetadataRequestTopic().setName("topic1").setTopicId(Uuid.randomUuid()))

    // if version is 10 or 11, the invalid topic metadata should return an error
    val invalidVersions = Set(10, 11)
    invalidVersions.foreach( version =>
      topics.foreach(topic => {
        val metadataRequestData = new MetadataRequestData().setTopics(Collections.singletonList(topic))
        val request = buildRequest(new MetadataRequest(metadataRequestData, version.toShort))
        val kafkaApis = createKafkaApis()
        try {
          val capturedResponse: ArgumentCaptor[AbstractResponse] = ArgumentCaptor.forClass(classOf[AbstractResponse])
          kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)
          verify(requestChannel).sendResponse(
            ArgumentMatchers.eq(request),
            capturedResponse.capture(),
            any()
          )
          val response = capturedResponse.getValue.asInstanceOf[MetadataResponse]
          assertEquals(1, response.topicMetadata.size)
          assertEquals(1, response.errorCounts.get(Errors.INVALID_REQUEST))
          response.data.topics.forEach(topic => assertNotEquals(null, topic.name))
          reset(requestChannel)
        } finally {
          kafkaApis.close()
        }
      })
    )
  }

  @Test
  def testHandleOffsetCommitRequest(): Unit = {
    addTopicToMetadataCache("foo", numPartitions = 1)

    val offsetCommitRequest = new OffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(List(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10)).asJava)).asJava)

    val requestChannelRequest = buildRequest(new OffsetCommitRequest.Builder(offsetCommitRequest).build())

    val future = new CompletableFuture[OffsetCommitResponseData]()
    when(groupCoordinator.commitOffsets(
      requestChannelRequest.context,
      offsetCommitRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    // This is the response returned by the group coordinator.
    val offsetCommitResponse = new OffsetCommitResponseData()
      .setTopics(List(
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code)).asJava)).asJava)

    future.complete(offsetCommitResponse)
    val response = verifyNoThrottling[OffsetCommitResponse](requestChannelRequest)
    assertEquals(offsetCommitResponse, response.data)
  }

  @Test
  def testHandleOffsetCommitRequestFutureFailed(): Unit = {
    addTopicToMetadataCache("foo", numPartitions = 1)

    val offsetCommitRequest = new OffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(List(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10)).asJava)).asJava)

    val requestChannelRequest = buildRequest(new OffsetCommitRequest.Builder(offsetCommitRequest).build())

    val future = new CompletableFuture[OffsetCommitResponseData]()
    when(groupCoordinator.commitOffsets(
      requestChannelRequest.context,
      offsetCommitRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)

    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    val expectedOffsetCommitResponse = new OffsetCommitResponseData()
      .setTopics(List(
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NOT_COORDINATOR.code)).asJava)).asJava)

    future.completeExceptionally(Errors.NOT_COORDINATOR.exception)
    val response = verifyNoThrottling[OffsetCommitResponse](requestChannelRequest)
    assertEquals(expectedOffsetCommitResponse, response.data)
  }

  @Test
  def testHandleOffsetCommitRequestTopicsAndPartitionsValidation(): Unit = {
    addTopicToMetadataCache("foo", numPartitions = 2)
    addTopicToMetadataCache("bar", numPartitions = 2)

    val offsetCommitRequest = new OffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(List(
        // foo exists but only has 2 partitions.
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(20),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(2)
              .setCommittedOffset(30)).asJava),
        // bar exists.
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("bar")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(40),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(50)).asJava),
        // zar does not exist.
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("zar")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(60),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(70)).asJava)).asJava)

    val requestChannelRequest = buildRequest(new OffsetCommitRequest.Builder(offsetCommitRequest).build())

    // This is the request expected by the group coordinator.
    val expectedOffsetCommitRequest = new OffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(List(
        // foo exists but only has 2 partitions.
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(20)).asJava),
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("bar")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(40),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(50)).asJava)).asJava)

    val future = new CompletableFuture[OffsetCommitResponseData]()
    when(groupCoordinator.commitOffsets(
      requestChannelRequest.context,
      expectedOffsetCommitRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    // This is the response returned by the group coordinator.
    val offsetCommitResponse = new OffsetCommitResponseData()
      .setTopics(List(
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)).asJava),
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("bar")
          .setPartitions(List(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)).asJava)).asJava)

    val expectedOffsetCommitResponse = new OffsetCommitResponseData()
      .setTopics(List(
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            // foo-2 is first because partitions failing the validation
            // are put in the response first.
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(2)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)).asJava),
        // zar is before bar because topics failing the validation are
        // put in the response first.
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("zar")
          .setPartitions(List(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)).asJava),
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("bar")
          .setPartitions(List(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)).asJava)).asJava)

    future.complete(offsetCommitResponse)
    val response = verifyNoThrottling[OffsetCommitResponse](requestChannelRequest)
    assertEquals(expectedOffsetCommitResponse, response.data)
  }

  @Test
  def testOffsetCommitWithInvalidPartition(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 1)

    def checkInvalidPartition(invalidPartitionId: Int): Unit = {
      reset(replicaManager, clientRequestQuotaManager, requestChannel)

      val offsetCommitRequest = new OffsetCommitRequest.Builder(
        new OffsetCommitRequestData()
          .setGroupId("groupId")
          .setTopics(Collections.singletonList(
            new OffsetCommitRequestData.OffsetCommitRequestTopic()
              .setName(topic)
              .setPartitions(Collections.singletonList(
                new OffsetCommitRequestData.OffsetCommitRequestPartition()
                  .setPartitionIndex(invalidPartitionId)
                  .setCommittedOffset(15)
                  .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                  .setCommittedMetadata(""))
              )
          ))).build()

      val request = buildRequest(offsetCommitRequest)
      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)

        val response = verifyNoThrottling[OffsetCommitResponse](request)
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION,
          Errors.forCode(response.data.topics().get(0).partitions().get(0).errorCode))
      } finally {
        kafkaApis.close()
      }
    }

    checkInvalidPartition(-1)
    checkInvalidPartition(1) // topic has only one partition
  }

  @Test
  def testTxnOffsetCommitWithInvalidPartition(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 1)

    def checkInvalidPartition(invalidPartitionId: Int): Unit = {
      reset(replicaManager, clientRequestQuotaManager, requestChannel)

      val invalidTopicPartition = new TopicPartition(topic, invalidPartitionId)
      val partitionOffsetCommitData = new TxnOffsetCommitRequest.CommittedOffset(15L, "", Optional.empty())
      val offsetCommitRequest = new TxnOffsetCommitRequest.Builder(
        "txnId",
        "groupId",
        15L,
        0.toShort,
        Map(invalidTopicPartition -> partitionOffsetCommitData).asJava,
        true
      ).build()
      val request = buildRequest(offsetCommitRequest)
      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleTxnOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)

        val response = verifyNoThrottling[TxnOffsetCommitResponse](request)
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response.errors().get(invalidTopicPartition))
      } finally {
        kafkaApis.close()
      }
    }

    checkInvalidPartition(-1)
    checkInvalidPartition(1) // topic has only one partition
  }

  @Test
  def testHandleTxnOffsetCommitRequest(): Unit = {
    addTopicToMetadataCache("foo", numPartitions = 1)

    val txnOffsetCommitRequest = new TxnOffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setGenerationId(10)
      .setProducerId(20)
      .setProducerEpoch(30)
      .setGroupInstanceId("instance-id")
      .setTransactionalId("transactional-id")
      .setTopics(List(
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10)).asJava)).asJava)

    val requestChannelRequest = buildRequest(new TxnOffsetCommitRequest.Builder(txnOffsetCommitRequest).build())

    val future = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(txnCoordinator.partitionFor(txnOffsetCommitRequest.transactionalId)).thenReturn(0)
    when(groupCoordinator.commitTransactionalOffsets(
      requestChannelRequest.context,
      txnOffsetCommitRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    // This is the response returned by the group coordinator.
    val txnOffsetCommitResponse = new TxnOffsetCommitResponseData()
      .setTopics(List(
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code)).asJava)).asJava)

    future.complete(txnOffsetCommitResponse)
    val response = verifyNoThrottling[TxnOffsetCommitResponse](requestChannelRequest)
    assertEquals(txnOffsetCommitResponse, response.data)
  }

  @Test
  def testHandleTxnOffsetCommitRequestFutureFailed(): Unit = {
    addTopicToMetadataCache("foo", numPartitions = 1)

    val txnOffsetCommitRequest = new TxnOffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(List(
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10)).asJava)).asJava)

    val requestChannelRequest = buildRequest(new TxnOffsetCommitRequest.Builder(txnOffsetCommitRequest).build())

    val future = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(txnCoordinator.partitionFor(txnOffsetCommitRequest.transactionalId)).thenReturn(0)
    when(groupCoordinator.commitTransactionalOffsets(
      requestChannelRequest.context,
      txnOffsetCommitRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    val expectedTxnOffsetCommitResponse = new TxnOffsetCommitResponseData()
      .setTopics(List(
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NOT_COORDINATOR.code)).asJava)).asJava)

    future.completeExceptionally(Errors.NOT_COORDINATOR.exception)
    val response = verifyNoThrottling[TxnOffsetCommitResponse](requestChannelRequest)
    assertEquals(expectedTxnOffsetCommitResponse, response.data)
  }

  @Test
  def testHandleTxnOffsetCommitRequestTopicsAndPartitionsValidation(): Unit = {
    addTopicToMetadataCache("foo", numPartitions = 2)
    addTopicToMetadataCache("bar", numPartitions = 2)

    val txnOffsetCommitRequest = new TxnOffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(List(
        // foo exists but only has 2 partitions.
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10),
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(20),
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(2)
              .setCommittedOffset(30)).asJava),
        // bar exists.
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("bar")
          .setPartitions(List(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(40),
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(50)).asJava),
        // zar does not exist.
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("zar")
          .setPartitions(List(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(60),
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(70)).asJava)).asJava)

    val requestChannelRequest = buildRequest(new TxnOffsetCommitRequest.Builder(txnOffsetCommitRequest).build())

    // This is the request expected by the group coordinator.
    val expectedTxnOffsetCommitRequest = new TxnOffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(List(
        // foo exists but only has 2 partitions.
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10),
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(20)).asJava),
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("bar")
          .setPartitions(List(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(40),
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(50)).asJava)).asJava)

    val future = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(txnCoordinator.partitionFor(expectedTxnOffsetCommitRequest.transactionalId)).thenReturn(0)
    when(groupCoordinator.commitTransactionalOffsets(
      requestChannelRequest.context,
      expectedTxnOffsetCommitRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    // This is the response returned by the group coordinator.
    val txnOffsetCommitResponse = new TxnOffsetCommitResponseData()
      .setTopics(List(
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)).asJava),
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("bar")
          .setPartitions(List(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)).asJava)).asJava)

    val expectedTxnOffsetCommitResponse = new TxnOffsetCommitResponseData()
      .setTopics(List(
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            // foo-2 is first because partitions failing the validation
            // are put in the response first.
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(2)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code),
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)).asJava),
        // zar is before bar because topics failing the validation are
        // put in the response first.
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("zar")
          .setPartitions(List(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code),
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)).asJava),
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("bar")
          .setPartitions(List(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)).asJava)).asJava)

    future.complete(txnOffsetCommitResponse)
    val response = verifyNoThrottling[TxnOffsetCommitResponse](requestChannelRequest)
    assertEquals(expectedTxnOffsetCommitResponse, response.data)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.TXN_OFFSET_COMMIT)
  def shouldReplaceCoordinatorNotAvailableWithLoadInProcessInTxnOffsetCommitWithOlderClient(version: Short): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2)

    val topicPartition = new TopicPartition(topic, 1)
    val capturedResponse: ArgumentCaptor[TxnOffsetCommitResponse] = ArgumentCaptor.forClass(classOf[TxnOffsetCommitResponse])

    val partitionOffsetCommitData = new TxnOffsetCommitRequest.CommittedOffset(15L, "", Optional.empty())
    val groupId = "groupId"

    val producerId = 15L
    val epoch = 0.toShort

    val offsetCommitRequest = new TxnOffsetCommitRequest.Builder(
      "txnId",
      groupId,
      producerId,
      epoch,
      Map(topicPartition -> partitionOffsetCommitData).asJava,
      version >= TxnOffsetCommitRequest.LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2
    ).build(version)
    val request = buildRequest(offsetCommitRequest)

    val requestLocal = RequestLocal.withThreadConfinedCaching
    val future = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(txnCoordinator.partitionFor(offsetCommitRequest.data.transactionalId)).thenReturn(0)
    when(groupCoordinator.commitTransactionalOffsets(
      request.context,
      offsetCommitRequest.data,
      requestLocal.bufferSupplier
    )).thenReturn(future)

    future.complete(new TxnOffsetCommitResponseData()
      .setTopics(List(
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName(topicPartition.topic)
          .setPartitions(List(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(topicPartition.partition)
              .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code)
          ).asJava)
      ).asJava))
    kafkaApis = createKafkaApis()
    kafkaApis.handleTxnOffsetCommitRequest(request, requestLocal)

    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )
    val response = capturedResponse.getValue

    if (version < 2) {
      assertEquals(Errors.COORDINATOR_NOT_AVAILABLE, response.errors().get(topicPartition))
    } else {
      assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, response.errors().get(topicPartition))
    }
  }

  @Test
  def shouldReplaceProducerFencedWithInvalidProducerEpochInInitProducerIdWithOlderClient(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2)

    for (version <- ApiKeys.INIT_PRODUCER_ID.oldestVersion to ApiKeys.INIT_PRODUCER_ID.latestVersion) {

      reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val capturedResponse: ArgumentCaptor[InitProducerIdResponse] = ArgumentCaptor.forClass(classOf[InitProducerIdResponse])
      val responseCallback: ArgumentCaptor[InitProducerIdResult => Unit] = ArgumentCaptor.forClass(classOf[InitProducerIdResult => Unit])

      val transactionalId = "txnId"
      val producerId = if (version < 3)
        RecordBatch.NO_PRODUCER_ID
      else
        15

      val epoch = if (version < 3)
        RecordBatch.NO_PRODUCER_EPOCH
      else
        0.toShort

      val txnTimeoutMs = TimeUnit.MINUTES.toMillis(15).toInt

      val initProducerIdRequest = new InitProducerIdRequest.Builder(
        new InitProducerIdRequestData()
          .setTransactionalId(transactionalId)
          .setTransactionTimeoutMs(txnTimeoutMs)
          .setProducerId(producerId)
          .setProducerEpoch(epoch)
      ).build(version.toShort)

      val request = buildRequest(initProducerIdRequest)

      val expectedProducerIdAndEpoch = if (version < 3)
        Option.empty
      else
        Option(new ProducerIdAndEpoch(producerId, epoch))

      val requestLocal = RequestLocal.withThreadConfinedCaching
      when(txnCoordinator.handleInitProducerId(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(txnTimeoutMs),
        ArgumentMatchers.eq(expectedProducerIdAndEpoch),
        responseCallback.capture(),
        ArgumentMatchers.eq(requestLocal)
      )).thenAnswer(_ => responseCallback.getValue.apply(InitProducerIdResult(producerId, epoch, Errors.PRODUCER_FENCED)))
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleInitProducerIdRequest(request, requestLocal)

        verify(requestChannel).sendResponse(
          ArgumentMatchers.eq(request),
          capturedResponse.capture(),
          ArgumentMatchers.eq(None)
        )
        val response = capturedResponse.getValue

        if (version < 4) {
          assertEquals(Errors.INVALID_PRODUCER_EPOCH.code, response.data.errorCode)
        } else {
          assertEquals(Errors.PRODUCER_FENCED.code, response.data.errorCode)
        }
      } finally {
        kafkaApis.close()
      }
    }
  }

  @Test
  def shouldReplaceProducerFencedWithInvalidProducerEpochInAddOffsetToTxnWithOlderClient(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2)

    for (version <- ApiKeys.ADD_OFFSETS_TO_TXN.oldestVersion to ApiKeys.ADD_OFFSETS_TO_TXN.latestVersion) {

      reset(replicaManager, clientRequestQuotaManager, requestChannel, groupCoordinator, txnCoordinator)

      val capturedResponse: ArgumentCaptor[AddOffsetsToTxnResponse] = ArgumentCaptor.forClass(classOf[AddOffsetsToTxnResponse])
      val responseCallback: ArgumentCaptor[Errors => Unit] = ArgumentCaptor.forClass(classOf[Errors => Unit])

      val groupId = "groupId"
      val transactionalId = "txnId"
      val producerId = 15L
      val epoch = 0.toShort

      val addOffsetsToTxnRequest = new AddOffsetsToTxnRequest.Builder(
        new AddOffsetsToTxnRequestData()
          .setGroupId(groupId)
          .setTransactionalId(transactionalId)
          .setProducerId(producerId)
          .setProducerEpoch(epoch)
      ).build(version.toShort)
      val request = buildRequest(addOffsetsToTxnRequest)

      val partition = 1
      when(groupCoordinator.partitionFor(
        ArgumentMatchers.eq(groupId)
      )).thenReturn(partition)

      val requestLocal = RequestLocal.withThreadConfinedCaching
      when(txnCoordinator.handleAddPartitionsToTransaction(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(epoch),
        ArgumentMatchers.eq(Set(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, partition))),
        responseCallback.capture(),
        ArgumentMatchers.eq(TransactionVersion.TV_0),
        ArgumentMatchers.eq(requestLocal)
      )).thenAnswer(_ => responseCallback.getValue.apply(Errors.PRODUCER_FENCED))
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleAddOffsetsToTxnRequest(request, requestLocal)

        verify(requestChannel).sendResponse(
          ArgumentMatchers.eq(request),
          capturedResponse.capture(),
          ArgumentMatchers.eq(None)
        )
        val response = capturedResponse.getValue

        if (version < 2) {
          assertEquals(Errors.INVALID_PRODUCER_EPOCH.code, response.data.errorCode)
        } else {
          assertEquals(Errors.PRODUCER_FENCED.code, response.data.errorCode)
        }
      } finally {
        kafkaApis.close()
      }
    }
  }

  @Test
  def shouldReplaceProducerFencedWithInvalidProducerEpochInAddPartitionToTxnWithOlderClient(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2)

    for (version <- ApiKeys.ADD_PARTITIONS_TO_TXN.oldestVersion to 3) {

      reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val capturedResponse: ArgumentCaptor[AddPartitionsToTxnResponse] = ArgumentCaptor.forClass(classOf[AddPartitionsToTxnResponse])
      val responseCallback: ArgumentCaptor[Errors => Unit] = ArgumentCaptor.forClass(classOf[Errors => Unit])

      val transactionalId = "txnId"
      val producerId = 15L
      val epoch = 0.toShort

      val partition = 1
      val topicPartition = new TopicPartition(topic, partition)

      val addPartitionsToTxnRequest = AddPartitionsToTxnRequest.Builder.forClient(
        transactionalId,
        producerId,
        epoch,
        Collections.singletonList(topicPartition)
      ).build(version.toShort)
      val request = buildRequest(addPartitionsToTxnRequest)

      val requestLocal = RequestLocal.withThreadConfinedCaching
      when(txnCoordinator.handleAddPartitionsToTransaction(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(epoch),
        ArgumentMatchers.eq(Set(topicPartition)),
        responseCallback.capture(),
        ArgumentMatchers.eq(TransactionVersion.TV_0),
        ArgumentMatchers.eq(requestLocal)
      )).thenAnswer(_ => responseCallback.getValue.apply(Errors.PRODUCER_FENCED))
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleAddPartitionsToTxnRequest(request, requestLocal)

        verify(requestChannel).sendResponse(
          ArgumentMatchers.eq(request),
          capturedResponse.capture(),
          ArgumentMatchers.eq(None)
        )
        val response = capturedResponse.getValue

        if (version < 2) {
          assertEquals(Collections.singletonMap(topicPartition, Errors.INVALID_PRODUCER_EPOCH), response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID))
        } else {
          assertEquals(Collections.singletonMap(topicPartition, Errors.PRODUCER_FENCED), response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID))
        }
      } finally {
        kafkaApis.close()
      }
    }
  }

  @Test
  def testBatchedAddPartitionsToTxnRequest(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2)

    val responseCallback: ArgumentCaptor[Errors => Unit] = ArgumentCaptor.forClass(classOf[Errors => Unit])
    val verifyPartitionsCallback: ArgumentCaptor[AddPartitionsToTxnResult => Unit] = ArgumentCaptor.forClass(classOf[AddPartitionsToTxnResult => Unit])

    val transactionalId1 = "txnId1"
    val transactionalId2 = "txnId2"
    val producerId = 15L
    val epoch = 0.toShort

    val tp0 = new TopicPartition(topic, 0)
    val tp1 = new TopicPartition(topic, 1)

    val addPartitionsToTxnRequest = AddPartitionsToTxnRequest.Builder.forBroker(
      new AddPartitionsToTxnTransactionCollection(
        List(new AddPartitionsToTxnTransaction()
          .setTransactionalId(transactionalId1)
          .setProducerId(producerId)
          .setProducerEpoch(epoch)
          .setVerifyOnly(false)
          .setTopics(new AddPartitionsToTxnTopicCollection(
            Collections.singletonList(new AddPartitionsToTxnTopic()
              .setName(tp0.topic)
              .setPartitions(Collections.singletonList(tp0.partition))
            ).iterator())
          ), new AddPartitionsToTxnTransaction()
          .setTransactionalId(transactionalId2)
          .setProducerId(producerId)
          .setProducerEpoch(epoch)
          .setVerifyOnly(true)
          .setTopics(new AddPartitionsToTxnTopicCollection(
            Collections.singletonList(new AddPartitionsToTxnTopic()
              .setName(tp1.topic)
              .setPartitions(Collections.singletonList(tp1.partition))
            ).iterator())
          )
        ).asJava.iterator()
      )
    ).build(4.toShort)
    val request = buildRequest(addPartitionsToTxnRequest)

    val requestLocal = RequestLocal.withThreadConfinedCaching
    when(txnCoordinator.handleAddPartitionsToTransaction(
      ArgumentMatchers.eq(transactionalId1),
      ArgumentMatchers.eq(producerId),
      ArgumentMatchers.eq(epoch),
      ArgumentMatchers.eq(Set(tp0)),
      responseCallback.capture(),
      any[TransactionVersion],
      ArgumentMatchers.eq(requestLocal)
    )).thenAnswer(_ => responseCallback.getValue.apply(Errors.NONE))

    when(txnCoordinator.handleVerifyPartitionsInTransaction(
      ArgumentMatchers.eq(transactionalId2),
      ArgumentMatchers.eq(producerId),
      ArgumentMatchers.eq(epoch),
      ArgumentMatchers.eq(Set(tp1)),
      verifyPartitionsCallback.capture(),
    )).thenAnswer(_ => verifyPartitionsCallback.getValue.apply(AddPartitionsToTxnResponse.resultForTransaction(transactionalId2, Map(tp1 -> Errors.PRODUCER_FENCED).asJava)))
    kafkaApis = createKafkaApis()
    kafkaApis.handleAddPartitionsToTxnRequest(request, requestLocal)

    val response = verifyNoThrottling[AddPartitionsToTxnResponse](request)

    val expectedErrors = Map(
      transactionalId1 -> Collections.singletonMap(tp0, Errors.NONE),
      transactionalId2 -> Collections.singletonMap(tp1, Errors.PRODUCER_FENCED)
    ).asJava

    assertEquals(expectedErrors, response.errors())
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.ADD_PARTITIONS_TO_TXN)
  def testHandleAddPartitionsToTxnAuthorizationFailedAndMetrics(version: Short): Unit = {
    val requestMetrics = new RequestChannelMetrics(Collections.singleton(ApiKeys.ADD_PARTITIONS_TO_TXN))
    try {
      val topic = "topic"

      val transactionalId = "txnId1"
      val producerId = 15L
      val epoch = 0.toShort

      val tp = new TopicPartition(topic, 0)

      val addPartitionsToTxnRequest =
        if (version < 4)
          AddPartitionsToTxnRequest.Builder.forClient(
            transactionalId,
            producerId,
            epoch,
            Collections.singletonList(tp)).build(version)
        else
          AddPartitionsToTxnRequest.Builder.forBroker(
            new AddPartitionsToTxnTransactionCollection(
              List(new AddPartitionsToTxnTransaction()
                .setTransactionalId(transactionalId)
                .setProducerId(producerId)
                .setProducerEpoch(epoch)
                .setVerifyOnly(true)
                .setTopics(new AddPartitionsToTxnTopicCollection(
                  Collections.singletonList(new AddPartitionsToTxnTopic()
                    .setName(tp.topic)
                    .setPartitions(Collections.singletonList(tp.partition))
                  ).iterator()))
              ).asJava.iterator())).build(version)

      val requestChannelRequest = buildRequest(addPartitionsToTxnRequest, requestMetrics = requestMetrics)

      val authorizer: Authorizer = mock(classOf[Authorizer])
      when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
        .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
      kafkaApis = createKafkaApis(authorizer = Some(authorizer))
      kafkaApis.handle(
        requestChannelRequest,
        RequestLocal.noCaching
      )

      val response = verifyNoThrottlingAndUpdateMetrics[AddPartitionsToTxnResponse](requestChannelRequest)
      val error = if (version < 4)
        response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID).get(tp)
      else
        Errors.forCode(response.data().errorCode)

      val expectedError = if (version < 4) Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED else Errors.CLUSTER_AUTHORIZATION_FAILED
      assertEquals(expectedError, error)

      val metricName = if (version < 4) ApiKeys.ADD_PARTITIONS_TO_TXN.name else RequestMetrics.VERIFY_PARTITIONS_IN_TXN_METRIC_NAME
      assertEquals(8, TestUtils.metersCount(metricName))
    } finally {
      requestMetrics.close()
    }
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.ADD_PARTITIONS_TO_TXN)
  def testAddPartitionsToTxnOperationNotAttempted(version: Short): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 1)

    val transactionalId = "txnId1"
    val producerId = 15L
    val epoch = 0.toShort

    val tp0 = new TopicPartition(topic, 0)
    val tp1 = new TopicPartition(topic, 1)

    val addPartitionsToTxnRequest = if (version < 4)
      AddPartitionsToTxnRequest.Builder.forClient(
        transactionalId,
        producerId,
        epoch,
        List(tp0, tp1).asJava).build(version)
    else
      AddPartitionsToTxnRequest.Builder.forBroker(
        new AddPartitionsToTxnTransactionCollection(
          List(new AddPartitionsToTxnTransaction()
            .setTransactionalId(transactionalId)
            .setProducerId(producerId)
            .setProducerEpoch(epoch)
            .setVerifyOnly(true)
            .setTopics(new AddPartitionsToTxnTopicCollection(
              Collections.singletonList(new AddPartitionsToTxnTopic()
                .setName(tp0.topic)
                .setPartitions(List[Integer](tp0.partition, tp1.partition()).asJava)
              ).iterator()))
          ).asJava.iterator())).build(version)

    val requestChannelRequest = buildRequest(addPartitionsToTxnRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleAddPartitionsToTxnRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    val response = verifyNoThrottling[AddPartitionsToTxnResponse](requestChannelRequest)

    def checkErrorForTp(tp: TopicPartition, expectedError: Errors): Unit = {
      val error = if (version < 4)
        response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID).get(tp)
      else
        response.errors().get(transactionalId).get(tp)

      assertEquals(expectedError, error)
    }

    checkErrorForTp(tp0, Errors.OPERATION_NOT_ATTEMPTED)
    checkErrorForTp(tp1, Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testAddPartitionsToTxnRejectsBackingTopicWithInvalidTopicException(): Unit = {
    // r19 ADV-A BLOCKER #140: backing topics carry interleaved records for multiple logical
    // tenants. Enrolling a backing partition in a transaction means the eventual
    // WriteTxnMarkers writes a COMMIT/ABORT control record onto the backing partition — per
    // PROMPT.md a marker on the backing commits ACROSS every logical topic sharing that
    // partition, corrupting every other tenant's transactional view. The handler must reject
    // at the topic level with INVALID_TOPIC_EXCEPTION (non-retriable), mirroring the
    // produce-side backing rejection at KafkaApis.scala:553.
    val backingTopic = "backing-topic"
    addTopicToMetadataCache(backingTopic, numPartitions = 1)
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)
    // isLogicalTopic intentionally unstubbed (default false) — if the order regresses and
    // the backing guard moves below the logical check, the never() verify on txnCoordinator
    // would still catch it because the fall-through wouldn't add backing-tp to authorized.

    val transactionalId = "txnId1"
    val producerId = 15L
    val epoch = 0.toShort
    val tp = new TopicPartition(backingTopic, 0)

    val addPartitionsToTxnRequest = AddPartitionsToTxnRequest.Builder.forClient(
      transactionalId,
      producerId,
      epoch,
      Collections.singletonList(tp)).build(3.toShort)
    val request = buildRequest(addPartitionsToTxnRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleAddPartitionsToTxnRequest(request, RequestLocal.noCaching)

    val response = verifyNoThrottling[AddPartitionsToTxnResponse](request)
    val error = response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID).get(tp)
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION, error,
      "AddPartitionsToTxn on a backing topic must be rejected with INVALID_TOPIC_EXCEPTION")
    // Transaction coordinator must NEVER see the backing partition.
    verify(txnCoordinator, never()).handleAddPartitionsToTransaction(any(), any(), any(), any(), any(), any(), any())
    verify(txnCoordinator, never()).handleVerifyPartitionsInTransaction(any(), any(), any(), any(), any())
  }

  @Test
  def testAddPartitionsToTxnRejectsLogicalTopicAsNonTransactional(): Unit = {
    // r19 ADV-A BLOCKER #140 (logical side): logical topics are non-transactional in v1 — a
    // produce path already rejects transactional batches at KafkaApis.scala:621 with
    // INVALID_TXN_STATE. Reject at the txn-coord entry point too: otherwise the txn
    // coordinator records the logical-named partition as a participant, the producer
    // believes its txn is enrolled, and the eventual commit is silently inconsistent (no
    // logical-aware LSO tracking in v1).
    val logicalTopic = "logical-topic"
    addTopicToMetadataCache(logicalTopic, numPartitions = 1)
    when(concentrationKernel.isBackingTopic(logicalTopic)).thenReturn(false)
    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)

    val transactionalId = "txnId1"
    val producerId = 15L
    val epoch = 0.toShort
    val tp = new TopicPartition(logicalTopic, 0)

    val addPartitionsToTxnRequest = AddPartitionsToTxnRequest.Builder.forClient(
      transactionalId,
      producerId,
      epoch,
      Collections.singletonList(tp)).build(3.toShort)
    val request = buildRequest(addPartitionsToTxnRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleAddPartitionsToTxnRequest(request, RequestLocal.noCaching)

    val response = verifyNoThrottling[AddPartitionsToTxnResponse](request)
    val error = response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID).get(tp)
    assertEquals(Errors.INVALID_TXN_STATE, error,
      "AddPartitionsToTxn on a logical topic must be rejected with INVALID_TXN_STATE " +
        "(v1 declares logical topics non-transactional)")
    verify(txnCoordinator, never()).handleAddPartitionsToTransaction(any(), any(), any(), any(), any(), any(), any())
    verify(txnCoordinator, never()).handleVerifyPartitionsInTransaction(any(), any(), any(), any(), any())
  }

  @Test
  def testTxnOffsetCommitRejectsBackingTopicWithInvalidTopicException(): Unit = {
    // r19 ADV-A BLOCKER #142: backing topics carry interleaved records for multiple logical
    // tenants. Committing TRANSACTIONAL offsets against the backing partition smuggles the
    // backing partition into __consumer_offsets via the txn-staged offsets path — and there
    // is no logical-aware end-txn / abort logic in v1 to clear it. The handler must reject
    // at the topic level with INVALID_TOPIC_EXCEPTION (non-retriable), mirroring the
    // produce-side backing rejection at KafkaApis.scala:553 and the AddPartitionsToTxn
    // rejection landed in #140. The check must run AFTER authz (so unauthorized callers
    // see TOPIC_AUTHORIZATION_FAILED first, preserving the no-enumeration-oracle posture)
    // and BEFORE the offsets are forwarded to groupCoordinator.commitTransactionalOffsets.
    val backingTopic = "backing-topic"
    addTopicToMetadataCache(backingTopic, numPartitions = 1)
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)
    // isLogicalTopic intentionally unstubbed (default false) — if the order regresses and
    // the backing guard moves below the logical check, the verify(never) on
    // commitTransactionalOffsets would still catch a forward because the fall-through
    // would not place backing-tp in authorizedTopicCommittedOffsets.

    val backingTp = new TopicPartition(backingTopic, 0)
    val partitionOffsetCommitData = new TxnOffsetCommitRequest.CommittedOffset(15L, "", Optional.empty())
    val txnOffsetCommitRequest = new TxnOffsetCommitRequest.Builder(
      "txnId",
      "groupId",
      15L,
      0.toShort,
      Map(backingTp -> partitionOffsetCommitData).asJava,
      true
    ).build()
    val request = buildRequest(txnOffsetCommitRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis()
    kafkaApis.handleTxnOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[TxnOffsetCommitResponse](request)
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION, response.errors().get(backingTp),
      "TxnOffsetCommit on a backing topic must be rejected with INVALID_TOPIC_EXCEPTION")
    // Group coordinator must NEVER see the backing partition.
    verify(groupCoordinator, never()).commitTransactionalOffsets(any(), any(), any())
  }

  @Test
  def testTxnOffsetCommitRejectsLogicalTopicAsNonTransactional(): Unit = {
    // r19 ADV-A BLOCKER #142 (logical side): logical topics are non-transactional in v1 —
    // the produce path rejects transactional batches at KafkaApis.scala:621 with
    // INVALID_TXN_STATE, and AddPartitionsToTxn mirrors this rejection at KafkaApis.scala
    // (companion #140). Reject TxnOffsetCommit on logical topics too: otherwise the group
    // coordinator writes a tx-staged offset for the logical-named partition into
    // __consumer_offsets, and no logical-aware end-txn / abort logic exists to clear it
    // correctly — the offset would either stay staged forever (stuck consumer) or be
    // cleared by a marker that applies to the wrong logical scope (duplicated delivery).
    // Mirror INVALID_TXN_STATE end-to-end so a transactional client sees a coherent
    // non-retriable failure at every step of the protocol.
    val logicalTopic = "logical-topic"
    // Logical topics intentionally live OUTSIDE the metadata cache (the kernel owns their
    // partition count). We do NOT call addTopicToMetadataCache here — the production path
    // must rely on isLogicalTopic, not metadata presence.
    when(concentrationKernel.isBackingTopic(logicalTopic)).thenReturn(false)
    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)

    val logicalTp = new TopicPartition(logicalTopic, 0)
    val partitionOffsetCommitData = new TxnOffsetCommitRequest.CommittedOffset(15L, "", Optional.empty())
    val txnOffsetCommitRequest = new TxnOffsetCommitRequest.Builder(
      "txnId",
      "groupId",
      15L,
      0.toShort,
      Map(logicalTp -> partitionOffsetCommitData).asJava,
      true
    ).build()
    val request = buildRequest(txnOffsetCommitRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis()
    kafkaApis.handleTxnOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[TxnOffsetCommitResponse](request)
    assertEquals(Errors.INVALID_TXN_STATE, response.errors().get(logicalTp),
      "TxnOffsetCommit on a logical topic must be rejected with INVALID_TXN_STATE " +
        "(v1 declares logical topics non-transactional)")
    verify(groupCoordinator, never()).commitTransactionalOffsets(any(), any(), any())
  }

  @Test
  def shouldReplaceProducerFencedWithInvalidProducerEpochInEndTxnWithOlderClient(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2)

    for (version <- ApiKeys.END_TXN.oldestVersion to ApiKeys.END_TXN.latestVersion) {
      reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val capturedResponse: ArgumentCaptor[EndTxnResponse] = ArgumentCaptor.forClass(classOf[EndTxnResponse])
      val responseCallback: ArgumentCaptor[(Errors, Long, Short) => Unit] = ArgumentCaptor.forClass(classOf[(Errors, Long, Short) => Unit])

      val transactionalId = "txnId"
      val producerId = 15L
      val epoch = 0.toShort

      val clientTransactionVersion = if (version > 4) TransactionVersion.TV_2 else TransactionVersion.TV_0
      val isTransactionV2Enabled = clientTransactionVersion.equals(TransactionVersion.TV_2)

      val endTxnRequest = new EndTxnRequest.Builder(
        new EndTxnRequestData()
          .setTransactionalId(transactionalId)
          .setProducerId(producerId)
          .setProducerEpoch(epoch)
          .setCommitted(true),
        isTransactionV2Enabled
      ).build(version.toShort)
      val request = buildRequest(endTxnRequest)

      val requestLocal = RequestLocal.withThreadConfinedCaching
      when(txnCoordinator.handleEndTransaction(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(epoch),
        ArgumentMatchers.eq(TransactionResult.COMMIT),
        ArgumentMatchers.eq(clientTransactionVersion),
        responseCallback.capture(),
        ArgumentMatchers.eq(requestLocal)
      )).thenAnswer(_ => responseCallback.getValue.apply(Errors.PRODUCER_FENCED, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH))
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleEndTxnRequest(request, requestLocal)

        verify(requestChannel).sendResponse(
          ArgumentMatchers.eq(request),
          capturedResponse.capture(),
          ArgumentMatchers.eq(None)
        )
        val response = capturedResponse.getValue

        if (version < 2) {
          assertEquals(Errors.INVALID_PRODUCER_EPOCH.code, response.data.errorCode)
        } else {
          assertEquals(Errors.PRODUCER_FENCED.code, response.data.errorCode)
        }
      } finally {
        kafkaApis.close()
      }
    }
  }

  @Test
  def shouldReplaceProducerFencedWithInvalidProducerEpochInProduceResponse(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2)

    for (version <- ApiKeys.PRODUCE.oldestVersion to ApiKeys.PRODUCE.latestVersion) {

      reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

      val tp = new TopicPartition("topic", 0)

      val produceRequest = ProduceRequest.builder(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
          Collections.singletonList(new ProduceRequestData.TopicProduceData()
            .setName(tp.topic).setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(tp.partition)
              .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("test".getBytes))))))
            .iterator))
        .setAcks(1.toShort)
        .setTimeoutMs(5000))
        .build(version.toShort)
      val request = buildRequest(produceRequest)

      when(replicaManager.handleProduceAppend(anyLong,
        anyShort,
        ArgumentMatchers.eq(false),
        any(),
        any(),
        responseCallback.capture(),
        any(),
        any(),
        any(),
        any()
      )).thenAnswer(_ => responseCallback.getValue.apply(Map(tp -> new PartitionResponse(Errors.INVALID_PRODUCER_EPOCH))))

      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
        any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

        val response = verifyNoThrottling[ProduceResponse](request)

        assertEquals(1, response.data.responses.size)
        val topicProduceResponse = response.data.responses.asScala.head
        assertEquals(1, topicProduceResponse.partitionResponses.size)
        val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
        assertEquals(Errors.INVALID_PRODUCER_EPOCH, Errors.forCode(partitionProduceResponse.errorCode))
      } finally {
        kafkaApis.close()
      }
    }
  }

  @Test
  def testProduceToBackingTopicIsRejectedWithInvalidTopicException(): Unit = {
    // Concentration v1 invariant: a stock producer that names the physical backing topic
    // bypasses the logical-offset reservation, would interleave its records with payloads from
    // every declared logical topic on the same backing, and would corrupt per-logical-topic
    // offset sequencing. KafkaApis#handleProduceRequest must short-circuit the partition with
    // INVALID_TOPIC_EXCEPTION before it ever reaches ReplicaManager.
    val backingTopic = "backing-topic"
    addTopicToMetadataCache(backingTopic, numPartitions = 1)
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)

    val tp = new TopicPartition(backingTopic, 0)
    val produceRequest = ProduceRequest.builder(new ProduceRequestData()
      .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
        Collections.singletonList(new ProduceRequestData.TopicProduceData()
          .setName(tp.topic).setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(tp.partition)
              .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("payload".getBytes))))))
          .iterator))
      .setAcks(1.toShort)
      .setTimeoutMs(5000))
      .build(ApiKeys.PRODUCE.latestVersion)
    val request = buildRequest(produceRequest)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    kafkaApis = createKafkaApis()
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[ProduceResponse](request)
    assertEquals(1, response.data.responses.size)
    val topicProduceResponse = response.data.responses.asScala.head
    assertEquals(1, topicProduceResponse.partitionResponses.size)
    val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION, Errors.forCode(partitionProduceResponse.errorCode))

    // The ReplicaManager append path must not have been called for the rejected partition,
    // otherwise the rejection would be racing the real append rather than short-circuiting it.
    verify(replicaManager, never()).handleProduceAppend(anyLong, anyShort, ArgumentMatchers.eq(false),
      any(), any(), any(), any(), any(), any(), any())
  }

  @Test
  def testProduceToLogicalTopicWithCompactedBackingReturnsInvalidTopicException(): Unit = {
    // Concentration v1 cannot serve a compacted backing topic: compaction is keyed on the record
    // key alone, so logical topic A's tombstone on key K would silently delete logical topic B's
    // record with the same key (PROMPT.md "non-compacted backings only"). Codex CRIT #4: the
    // produce hook must reject this loudly with INVALID_TOPIC_EXCEPTION — non-retriable, signals
    // a topic-misconfig that requires operator action, not a transient broker condition.
    val logicalTopic = "orders"
    val backingTopic = "concentrated-compacted"
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    addTopicToMetadataCache(backingTopic, numPartitions = 4)

    val compactedConfigRepo = new kafka.server.metadata.MockConfigRepository()
    compactedConfigRepo.setTopicConfig(backingTopic,
      org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG, "compact")

    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))
    when(concentrationKernel.isBackingReady(any[TopicPartition])).thenReturn(true)
    when(concentrationKernel.assertBackingTopicNotCompacted(
      ArgumentMatchers.eq(backingTopic), ArgumentMatchers.eq("compact"))
    ).thenThrow(new IllegalStateException(
      s"Backing topic '$backingTopic' has cleanup.policy='compact'; concentration v1 requires a non-compacted backing"))

    val tp = new TopicPartition(logicalTopic, 0)
    val produceRequest = ProduceRequest.builder(new ProduceRequestData()
      .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
        Collections.singletonList(new ProduceRequestData.TopicProduceData()
          .setName(tp.topic).setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(tp.partition)
              .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("k".getBytes, "v".getBytes))))))
          .iterator))
      .setAcks(1.toShort)
      .setTimeoutMs(5000))
      .build(ApiKeys.PRODUCE.latestVersion)
    val request = buildRequest(produceRequest)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    kafkaApis = createKafkaApis(configRepository = compactedConfigRepo)
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[ProduceResponse](request)
    assertEquals(1, response.data.responses.size)
    val topicProduceResponse = response.data.responses.asScala.head
    assertEquals(logicalTopic, topicProduceResponse.name)
    val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
    assertEquals(0, partitionProduceResponse.index)
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION,
      Errors.forCode(partitionProduceResponse.errorCode),
      "compacted backing must surface as a non-retriable INVALID_TOPIC_EXCEPTION so the operator " +
      "fixes cleanup.policy rather than the client retrying forever")

    // The append path must never have been reached — the rejection must short-circuit BEFORE we
    // reserve logical offsets, otherwise a doomed produce would still consume a logical offset
    // and create a non-contiguous gap.
    verify(replicaManager, never()).handleProduceAppend(anyLong, anyShort, ArgumentMatchers.eq(false),
      any(), any(), any(), any(), any(), any(), any())
    verify(concentrationKernel, never()).reserveProduceBatch(any(), anyInt, anyInt)
  }

  @Test
  def testProduceToLogicalTopicStampsHeadersAndCommitsLogicalOffsets(): Unit = {
    // Concentration hook #2 happy path. A stock producer sends a 3-record batch to logical
    // topic "orders" partition 0. The hook must:
    //   1. Resolve backing partition via the kernel.
    //   2. Reserve 3 logical offsets atomically.
    //   3. Stamp every record with __concentration_logical_topic and __concentration_logical_offset.
    //   4. Route the rewritten batch to ReplicaManager keyed by the BACKING topic-partition.
    //   5. On success, commit the reservation and remap the response key back to the LOGICAL TP
    //      with baseOffset/lastOffset rewritten from backing offsets into logical offsets.
    // Anchoring all five in one test pins the full produce-path contract — splitting them would
    // either need a wider seam in production code or duplicate mock plumbing five times over.
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    addTopicToMetadataCache(backingTopic, numPartitions = 4)

    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))
    when(concentrationKernel.backingPartitionFor(logicalTopic, 0)).thenReturn(2)
    when(concentrationKernel.isBackingReady(any[TopicPartition])).thenReturn(true)
    // The kernel's logical start offset (advanced by past DeleteRecords) is decoupled from the
    // backing partition's start offset: a sibling logical topic deleting records on the same
    // backing must NOT make this producer's logStartOffset jump. Pin it to a recognisable value
    // so the response-remap assertion below proves we forwarded *this* number, not the backing's.
    when(concentrationKernel.startLogicalOffset(logicalTopic, 0)).thenReturn(42L)

    val res0 = mock(classOf[Reservation]); when(res0.logicalOffset).thenReturn(500L)
    val res1 = mock(classOf[Reservation]); when(res1.logicalOffset).thenReturn(501L)
    val res2 = mock(classOf[Reservation]); when(res2.logicalOffset).thenReturn(502L)
    val reservations = Array(res0, res1, res2)
    when(concentrationKernel.reserveProduceBatch(logicalTopic, 0, 3)).thenReturn(reservations)
    // HIGH #7: handleProduceRequest derives currentLeaderEpoch from onlinePartition; non-idempotent
    // producers don't hit the cache, but the lookup still runs unconditionally.
    when(replicaManager.onlinePartition(any[TopicPartition])).thenReturn(None)

    val tp = new TopicPartition(logicalTopic, 0)
    val originalRecords = MemoryRecords.withRecords(Compression.NONE,
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k2".getBytes, "v2".getBytes),
      new SimpleRecord("k3".getBytes, "v3".getBytes))
    val produceRequest = ProduceRequest.builder(new ProduceRequestData()
      .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
        Collections.singletonList(new ProduceRequestData.TopicProduceData()
          .setName(tp.topic).setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(tp.partition)
              .setRecords(originalRecords))))
          .iterator))
      .setAcks(1.toShort)
      .setTimeoutMs(5000))
      .build(ApiKeys.PRODUCE.latestVersion)
    val request = buildRequest(produceRequest)

    val entriesCaptor: ArgumentCaptor[Map[TopicPartition, MemoryRecords]] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, MemoryRecords]])
    val callbackCaptor: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

    val backingTp = new TopicPartition(backingTopic, 2)
    when(replicaManager.handleProduceAppend(anyLong, anyShort, ArgumentMatchers.eq(false),
      any(), entriesCaptor.capture(), callbackCaptor.capture(),
      any(), any(), any(), any())
    ).thenAnswer { _ =>
      // Simulate ReplicaManager succeeding with backing baseOffset=900. The hook must rewrite
      // this into the logical baseOffset (500) before the producer sees the response.
      // logStartOffset=99L is the BACKING topic's start; the hook must NOT leak this to the
      // producer's response (sibling-topic DeleteRecords would look like truncation otherwise).
      // The hook is expected to overwrite it with the kernel's startLogicalOffset (42L above).
      callbackCaptor.getValue.apply(Map(backingTp -> new PartitionResponse(
        Errors.NONE, 900L, RecordBatch.NO_TIMESTAMP, 99L)))
    }

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    kafkaApis = createKafkaApis()
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    // ReplicaManager saw the BACKING topic-partition, not the logical one, with stamped records.
    val capturedEntries = entriesCaptor.getValue
    assertTrue(capturedEntries.contains(backingTp),
      s"expected backing TP $backingTp in entries, got ${capturedEntries.keys}")
    assertFalse(capturedEntries.contains(tp),
      "logical TP must not leak into ReplicaManager.handleProduceAppend")
    val stamped = capturedEntries(backingTp)
    val stampedRecords = stamped.records().iterator()
    var i = 0
    while (stampedRecords.hasNext) {
      val r = stampedRecords.next()
      val hs = r.headers()
      assertTrue(hs.length >= 3, s"record $i missing concentration headers")
      assertEquals(ConcentrationHeaders.LOGICAL_TOPIC_HEADER, hs(hs.length - 3).key())
      assertEquals(logicalTopic, new String(hs(hs.length - 3).value(),
        java.nio.charset.StandardCharsets.UTF_8))
      assertEquals(ConcentrationHeaders.LOGICAL_PARTITION_HEADER, hs(hs.length - 2).key())
      assertEquals(0, java.nio.ByteBuffer.wrap(hs(hs.length - 2).value()).getInt,
        "logical partition stamped on the record must equal the producer's target logical partition")
      assertEquals(ConcentrationHeaders.LOGICAL_OFFSET_HEADER, hs(hs.length - 1).key())
      assertEquals(500L + i,
        java.nio.ByteBuffer.wrap(hs(hs.length - 1).value()).getLong)
      i += 1
    }
    assertEquals(3, i, "expected 3 stamped records")

    // Commit was invoked with the backing baseOffset returned by ReplicaManager.
    verify(concentrationKernel).commitProduceBatch(reservations, 900L)
    verify(concentrationKernel, never()).rollbackProduceBatch(any())

    // The producer-visible response is keyed by the LOGICAL TP and carries logical offsets.
    val response = verifyNoThrottling[ProduceResponse](request)
    assertEquals(1, response.data.responses.size)
    val topicProduceResponse = response.data.responses.asScala.head
    assertEquals(logicalTopic, topicProduceResponse.name)
    val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
    assertEquals(0, partitionProduceResponse.index)
    assertEquals(Errors.NONE, Errors.forCode(partitionProduceResponse.errorCode))
    assertEquals(500L, partitionProduceResponse.baseOffset,
      "baseOffset must be the FIRST logical offset, not the backing offset (900)")
    // lastOffset is internal to ProduceResponse.PartitionResponse and not on the wire schema,
    // so it can't be asserted on the wire-level PartitionProduceResponse here. baseOffset
    // remap is the producer-visible contract; the internal lastOffset rewrite is a
    // belt-and-suspenders for callbacks that consume the internal struct.
    assertEquals(42L, partitionProduceResponse.logStartOffset,
      "logStartOffset must come from kernel.startLogicalOffset for THIS logical TP, not the " +
      "backing's logStartOffset (99) — leaking the backing value would make sibling-topic " +
      "DeleteRecords look like truncation to idempotent producers")
  }

  @Test
  def testProduceToLogicalTopicRollsBackReservationOnAppendError(): Unit = {
    // When the backing append fails (NotLeader, log full, anything non-NONE), the kernel
    // reservation must be rolled back so the next produce doesn't see a phantom hole at the
    // reserved logical offsets. The producer-visible response is still keyed by the logical
    // TP — leaking the backing TP into the error path would be a metadata leak.
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    addTopicToMetadataCache(backingTopic, numPartitions = 4)
    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))
    when(concentrationKernel.backingPartitionFor(logicalTopic, 0)).thenReturn(2)
    when(concentrationKernel.isBackingReady(any[TopicPartition])).thenReturn(true)

    val res0 = mock(classOf[Reservation]); when(res0.logicalOffset).thenReturn(7L)
    val reservations = Array(res0)
    when(concentrationKernel.reserveProduceBatch(logicalTopic, 0, 1)).thenReturn(reservations)
    // HIGH #7: produce path always derives currentLeaderEpoch from onlinePartition.
    when(replicaManager.onlinePartition(any[TopicPartition])).thenReturn(None)

    val tp = new TopicPartition(logicalTopic, 0)
    val produceRequest = ProduceRequest.builder(new ProduceRequestData()
      .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
        Collections.singletonList(new ProduceRequestData.TopicProduceData()
          .setName(tp.topic).setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(tp.partition)
              .setRecords(MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord("payload".getBytes))))))
          .iterator))
      .setAcks(1.toShort)
      .setTimeoutMs(5000))
      .build(ApiKeys.PRODUCE.latestVersion)
    val request = buildRequest(produceRequest)

    val callbackCaptor: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])
    val backingTp = new TopicPartition(backingTopic, 2)
    when(replicaManager.handleProduceAppend(anyLong, anyShort, ArgumentMatchers.eq(false),
      any(), any(), callbackCaptor.capture(),
      any(), any(), any(), any())
    ).thenAnswer { _ =>
      // KAFKA_STORAGE_ERROR is a representative "append failed" condition that doesn't
      // require the response path to look up a current leader (which would need a mocked
      // Partition). It exercises the rollback branch without that ceremony.
      callbackCaptor.getValue.apply(Map(backingTp -> new PartitionResponse(Errors.KAFKA_STORAGE_ERROR)))
    }
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    kafkaApis = createKafkaApis()
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    verify(concentrationKernel).rollbackProduceBatch(reservations)
    verify(concentrationKernel, never()).commitProduceBatch(any(), anyLong)

    val response = verifyNoThrottling[ProduceResponse](request)
    val topicProduceResponse = response.data.responses.asScala.head
    assertEquals(logicalTopic, topicProduceResponse.name,
      "error response must be remapped to the logical TP — leaking the backing TP is a metadata leak")
    val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
    assertEquals(Errors.KAFKA_STORAGE_ERROR, Errors.forCode(partitionProduceResponse.errorCode))
  }

  @Test
  def testProduceToLogicalTopicRejectsOutOfRangePartitionWithUnknownTopicOrPartition(): Unit = {
    // A logical topic declared with N=1024 partitions must reject produces to partition 1024+.
    // This is a client routing bug, not transient — UNKNOWN_TOPIC_OR_PARTITION is the same
    // error stock topics return for out-of-range partition.
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic))
      .thenReturn(Optional.of(new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)))

    val tp = new TopicPartition(logicalTopic, 9999)
    val produceRequest = ProduceRequest.builder(new ProduceRequestData()
      .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
        Collections.singletonList(new ProduceRequestData.TopicProduceData()
          .setName(tp.topic).setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(tp.partition)
              .setRecords(MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord("payload".getBytes))))))
          .iterator))
      .setAcks(1.toShort).setTimeoutMs(5000))
      .build(ApiKeys.PRODUCE.latestVersion)
    val request = buildRequest(produceRequest)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    kafkaApis = createKafkaApis()
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[ProduceResponse](request)
    val partitionProduceResponse = response.data.responses.asScala.head.partitionResponses.asScala.head
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.forCode(partitionProduceResponse.errorCode))
    // Out-of-range partition must NEVER reach the reservation or append path.
    verify(concentrationKernel, never()).reserveProduceBatch(any[String], anyInt, anyInt)
    verify(replicaManager, never()).handleProduceAppend(anyLong, anyShort, ArgumentMatchers.eq(false),
      any(), any(), any(), any(), any(), any(), any())
  }

  @Test
  def testProduceToLogicalTopicWhenBackingReadinessGateClosedReturnsNotLeaderOrFollower(): Unit = {
    // Concentration GAP 2 (Commit B.2): when the backing-partition's per-broker readiness gate is
    // CLOSED — typically because this broker just lost leadership for the backing and the
    // KafkaConcentrationLeaderRecoverer has not yet rehydrated the tracker under the new
    // leader-epoch — every produce to a logical topic mapped onto that backing must be rejected
    // BEFORE we reserve logical offsets. Reserving against a stale tracker would risk handing out
    // offsets that collide with what the previous leader already acknowledged on the wire.
    //
    // NOT_LEADER_OR_FOLLOWER is the convergent Codex+Gemini recommendation: it's retriable on the
    // stock producer side (metadata refresh + retry) and requires no client-side wire-protocol
    // change. This pins that the gate short-circuits BEFORE reservation, stamping, and append.
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    addTopicToMetadataCache(backingTopic, numPartitions = 4)
    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))
    when(concentrationKernel.backingPartitionFor(logicalTopic, 0)).thenReturn(2)
    // Gate explicitly closed for the backing partition this produce routes to.
    when(concentrationKernel.isBackingReady(new TopicPartition(backingTopic, 2))).thenReturn(false)

    val tp = new TopicPartition(logicalTopic, 0)
    val produceRequest = ProduceRequest.builder(new ProduceRequestData()
      .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
        Collections.singletonList(new ProduceRequestData.TopicProduceData()
          .setName(tp.topic).setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(tp.partition)
              .setRecords(MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord("payload".getBytes))))))
          .iterator))
      .setAcks(1.toShort).setTimeoutMs(5000))
      .build(ApiKeys.PRODUCE.latestVersion)
    val request = buildRequest(produceRequest)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    // The NLOF response path queries replicaManager.getPartitionOrError. We don't care which
    // branch it takes for the assertion below — return a benign Left so the response carries
    // leaderId=-1 / leaderEpoch=-1 (matches the "broker has no current leader info" fallback).
    when(replicaManager.getPartitionOrError(any[TopicPartition]))
      .thenReturn(Left(Errors.NOT_LEADER_OR_FOLLOWER))
    kafkaApis = createKafkaApis()
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[ProduceResponse](request)
    val partitionProduceResponse = response.data.responses.asScala.head.partitionResponses.asScala.head
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, Errors.forCode(partitionProduceResponse.errorCode),
      "gate-closed backing must surface as NOT_LEADER_OR_FOLLOWER so stock idempotent producers " +
        "treat it as a retriable transient condition and refresh metadata")

    // Critical: the gate must short-circuit BEFORE reservation. Burning a logical offset against
    // a stale tracker is exactly the silent-corruption case Commit B.2 exists to prevent.
    verify(concentrationKernel, never()).reserveProduceBatch(any[String], anyInt, anyInt)
    verify(replicaManager, never()).handleProduceAppend(anyLong, anyShort, ArgumentMatchers.eq(false),
      any(), any(), any(), any(), any(), any(), any())
  }

  @Test
  def testFetchOnLogicalTopicWhenBackingReadinessGateClosedReturnsNotLeaderOrFollower(): Unit = {
    // Convergent Codex+Gemini Q1 finding on the GAP 2 chain: closing the gate only on the produce
    // path leaves a leak on Fetch. After leader-loss / before the recoverer republishes, the
    // sidecar's physical→logical translation is stale. Resolving a fetch against it would either
    // mis-position the consumer (silent wrong-record reads) or stream records from a sibling
    // logical topic. The gate must short-circuit BEFORE resolveBackingOffset is consulted and
    // BEFORE any backing fetch is issued.
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    val backingTopicId = Uuid.randomUuid()
    val logicalTopicId = Uuid.randomUuid()
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    addTopicToMetadataCache(backingTopic, numPartitions = 4, topicId = backingTopicId)
    addTopicToMetadataCache(logicalTopic, numPartitions = 1024, topicId = logicalTopicId)

    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))
    when(concentrationKernel.backingPartitionFor(logicalTopic, 0)).thenReturn(2)
    // Local broker IS the leader of the backing partition — pins this test on the readiness
    // gate path (not the leader-check path added for Codex Q5). Without this, default Mockito
    // returns None for onlinePartition, the leader check fires first, and the test passes for
    // the wrong reason.
    stubBackingPartitionAsLeader()
    // Gate explicitly closed for the backing partition this fetch would route to.
    when(concentrationKernel.isBackingReady(new TopicPartition(backingTopic, 2))).thenReturn(false)

    val logicalTip = new TopicIdPartition(logicalTopicId, new TopicPartition(logicalTopic, 0))
    val fetchData = Map(logicalTip ->
      new FetchRequest.PartitionData(logicalTip.topicId, 100L, 0L, 1_000_000, Optional.empty())).asJava
    val fetchDataBuilder = Map(logicalTip.topicPartition ->
      new FetchRequest.PartitionData(logicalTip.topicId, 100L, 0L, 1_000_000, Optional.empty())).asJava
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      new JFetchMetadata(0, 0), fetchData, false, false)
    when(fetchManager.newContext(
      any[Short], any[JFetchMetadata], any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      any[util.List[TopicIdPartition]], any[util.Map[Uuid, String]])).thenReturn(fetchContext)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val fetchRequest = new FetchRequest.Builder(ApiKeys.FETCH.latestVersion,
      ApiKeys.FETCH.latestVersion, -1, -1, 100, 0, fetchDataBuilder).build()
    val request = buildRequest(fetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleFetchRequest(request)

    // No backing fetch must be issued.
    verify(replicaManager, never()).fetchMessages(any[FetchParams],
      any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]],
      any[ReplicaQuota],
      any[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]())
    // Gate must short-circuit BEFORE the sidecar lookup.
    verify(concentrationKernel, never()).resolveBackingOffset(any[String], anyInt, anyLong)
    // Tracker reads beyond the gate must also be skipped — reading them against a half-rebuilt
    // tracker is exactly the silent-corruption window this gate exists to prevent.
    verify(concentrationKernel, never()).nextLogicalOffset(any[String], anyInt)
    verify(concentrationKernel, never()).startLogicalOffset(any[String], anyInt)

    val response = verifyNoThrottling[FetchResponse](request)
    val byTopic = response.data.responses.asScala.find(_.topicId == logicalTopicId).get
    val partitionResponse = byTopic.partitions.asScala.head
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, partitionResponse.errorCode,
      "gate-closed backing on Fetch must surface as NOT_LEADER_OR_FOLLOWER so stock consumers " +
        "refresh metadata and retry rather than seeing stale or wrong-topic records")
  }

  @Test
  def testListOffsetsOnLogicalTopicWhenBackingReadinessGateClosedReturnsNotLeaderOrFollower(): Unit = {
    // Same Q1 finding applied to ListOffsets: stock consumers calling seekToBeginning / seekToEnd
    // against a logical topic must NOT receive an offset derived from a half-rebuilt tracker. A
    // stale start/next would mis-seek the consumer into a region the new leader has not yet
    // observed. NOT_LEADER_OR_FOLLOWER pushes the client back through metadata refresh.
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    addTopicToMetadataCache(backingTopic, numPartitions = 4)

    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))
    when(concentrationKernel.backingPartitionFor(logicalTopic, 0)).thenReturn(2)
    // Local broker IS leader — pin this test on the gate, not the new leader-check (Codex Q5).
    stubBackingPartitionAsLeader()
    when(concentrationKernel.isBackingReady(new TopicPartition(backingTopic, 2))).thenReturn(false)

    val targetTimes = List(new ListOffsetsTopic()
      .setName(logicalTopic)
      .setPartitions(List(
        new ListOffsetsPartition().setPartitionIndex(0).setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
      ).asJava)).asJava
    val listOffsetRequest = ListOffsetsRequest.Builder.forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
      .setTargetTimes(targetTimes).build()
    val request = buildRequest(listOffsetRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis()
    kafkaApis.handleListOffsetRequest(request)

    val response = verifyNoThrottling[ListOffsetsResponse](request)
    val topicResp = response.topics.asScala.find(_.name == logicalTopic).get
    val partitionResp = topicResp.partitions.asScala.head
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, partitionResp.errorCode,
      "gate-closed backing on ListOffsets must surface as NOT_LEADER_OR_FOLLOWER")
    // Gate must short-circuit BEFORE any logical-offset read.
    verify(concentrationKernel, never()).nextLogicalOffset(any[String], anyInt)
    verify(concentrationKernel, never()).startLogicalOffset(any[String], anyInt)
  }

  @Test
  def testDeleteRecordsOnLogicalTopicWhenBackingReadinessGateClosedReturnsNotLeaderOrFollower(): Unit = {
    // Same Q1 finding applied to DeleteRecords: advancing the logical start offset against a
    // half-rebuilt tracker would either over-truncate (lose records the new leader hasn't yet
    // observed) or under-truncate (advance to a stale tail). NOT_LEADER_OR_FOLLOWER tells admin
    // clients to retry against the same broker after metadata refresh; the recoverer reopens
    // the gate once the rebuild lands.
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))
    when(concentrationKernel.backingPartitionFor(logicalTopic, 0)).thenReturn(2)
    // Local broker IS leader — pin this test on the gate, not the new leader-check (Codex Q5).
    stubBackingPartitionAsLeader()
    when(concentrationKernel.isBackingReady(new TopicPartition(backingTopic, 2))).thenReturn(false)

    val deleteRecordsRequest = new DeleteRecordsRequest.Builder(new DeleteRecordsRequestData()
      .setTopics(Collections.singletonList(new DeleteRecordsTopic()
        .setName(logicalTopic)
        .setPartitions(Collections.singletonList(new DeleteRecordsPartition()
          .setOffset(50L)
          .setPartitionIndex(0)))))
      .setTimeoutMs(5000)).build()
    val request = buildRequest(deleteRecordsRequest)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis()
    kafkaApis.handleDeleteRecordsRequest(request)

    val response = verifyNoThrottling[DeleteRecordsResponse](request)
    val partitionResult = response.data.topics.asScala.head.partitions.asScala.head
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, partitionResult.errorCode,
      "gate-closed backing on DeleteRecords must surface as NOT_LEADER_OR_FOLLOWER")
    assertEquals(DeleteRecordsResponse.INVALID_LOW_WATERMARK, partitionResult.lowWatermark)
    // Gate must short-circuit BEFORE any tracker mutation.
    verify(concentrationKernel, never()).advanceStartOffset(any[String], anyInt, anyLong)
  }

  @Test
  def testFetchOnLogicalTopicWhenLocalBrokerIsNotLeaderReturnsNotLeaderOrFollower(): Unit = {
    // Codex Q5 on GAP 2: stock consumers can be directed to fetch from a follower replica via
    // FetchResponse.preferred_read_replica + RackAwareReplicaSelector. Only the LEADER of the
    // backing partition has authoritative kernel state — the tracker on a follower reflects
    // no produces (the listener never opens its gate on a follower) or, worse, stale state
    // left over from a prior leadership term. Returning NOT_LEADER_OR_FOLLOWER routes the
    // consumer through metadata refresh, which will redirect it back to the leader.
    //
    // Pinned: the leader check fires BEFORE the readiness gate. Without this ordering, a
    // follower with a half-rebuilt tracker could pass an open gate left over from before
    // the leadership flip and serve garbage.
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    val backingTopicId = Uuid.randomUuid()
    val logicalTopicId = Uuid.randomUuid()
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    addTopicToMetadataCache(backingTopic, numPartitions = 4, topicId = backingTopicId)
    addTopicToMetadataCache(logicalTopic, numPartitions = 1024, topicId = logicalTopicId)

    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))
    when(concentrationKernel.backingPartitionFor(logicalTopic, 0)).thenReturn(2)
    // Local broker is a FOLLOWER of the backing partition: onlinePartition returns a partition
    // whose isLeader is false. (Returning None — the default Mockito behaviour — would also
    // trigger the rejection, but stubbing isLeader=false here exercises the explicit follower
    // case that motivated this check.)
    val backingPartition = mock(classOf[Partition])
    when(backingPartition.isLeader).thenReturn(false)
    when(replicaManager.onlinePartition(any[TopicPartition])).thenReturn(Some(backingPartition))

    val logicalTip = new TopicIdPartition(logicalTopicId, new TopicPartition(logicalTopic, 0))
    val fetchData = Map(logicalTip ->
      new FetchRequest.PartitionData(logicalTip.topicId, 100L, 0L, 1_000_000, Optional.empty())).asJava
    val fetchDataBuilder = Map(logicalTip.topicPartition ->
      new FetchRequest.PartitionData(logicalTip.topicId, 100L, 0L, 1_000_000, Optional.empty())).asJava
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      new JFetchMetadata(0, 0), fetchData, false, false)
    when(fetchManager.newContext(
      any[Short], any[JFetchMetadata], any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      any[util.List[TopicIdPartition]], any[util.Map[Uuid, String]])).thenReturn(fetchContext)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val fetchRequest = new FetchRequest.Builder(ApiKeys.FETCH.latestVersion,
      ApiKeys.FETCH.latestVersion, -1, -1, 100, 0, fetchDataBuilder).build()
    val request = buildRequest(fetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleFetchRequest(request)

    // No backing fetch — the leader check fired first.
    verify(replicaManager, never()).fetchMessages(any[FetchParams],
      any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]],
      any[ReplicaQuota],
      any[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]())
    // Leader-check ordering: BEFORE the readiness gate (isBackingReady) and BEFORE any kernel
    // offset translation. The order matters — a follower with an open-by-default gate must
    // still be rejected before we trust its tracker.
    verify(concentrationKernel, never()).isBackingReady(any[TopicPartition])
    verify(concentrationKernel, never()).resolveBackingOffset(any[String], anyInt, anyLong)
    verify(concentrationKernel, never()).nextLogicalOffset(any[String], anyInt)
    verify(concentrationKernel, never()).startLogicalOffset(any[String], anyInt)

    val response = verifyNoThrottling[FetchResponse](request)
    val byTopic = response.data.responses.asScala.find(_.topicId == logicalTopicId).get
    val partitionResponse = byTopic.partitions.asScala.head
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, partitionResponse.errorCode,
      "Fetch on a backing follower must surface as NOT_LEADER_OR_FOLLOWER so the consumer " +
        "refreshes metadata and is redirected to the leader (Codex Q5)")
  }

  @Test
  def testListOffsetsOnLogicalTopicWhenLocalBrokerIsNotLeaderReturnsNotLeaderOrFollower(): Unit = {
    // Codex Q5: same contract on ListOffsets. seekToBeginning / seekToEnd against a follower
    // would return offsets derived from a tracker that never observed the produces. Reject
    // before any kernel offset read.
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    addTopicToMetadataCache(backingTopic, numPartitions = 4)

    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))
    when(concentrationKernel.backingPartitionFor(logicalTopic, 0)).thenReturn(2)
    val backingPartition = mock(classOf[Partition])
    when(backingPartition.isLeader).thenReturn(false)
    when(replicaManager.onlinePartition(any[TopicPartition])).thenReturn(Some(backingPartition))

    val targetTimes = List(new ListOffsetsTopic()
      .setName(logicalTopic)
      .setPartitions(List(
        new ListOffsetsPartition().setPartitionIndex(0).setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
      ).asJava)).asJava
    val listOffsetRequest = ListOffsetsRequest.Builder.forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
      .setTargetTimes(targetTimes).build()
    val request = buildRequest(listOffsetRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis()
    kafkaApis.handleListOffsetRequest(request)

    val response = verifyNoThrottling[ListOffsetsResponse](request)
    val partitionResp = response.topics.asScala.find(_.name == logicalTopic).get.partitions.asScala.head
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, partitionResp.errorCode,
      "ListOffsets on a backing follower must surface as NOT_LEADER_OR_FOLLOWER (Codex Q5)")
    // Ordering: BEFORE the readiness gate and BEFORE any kernel offset read.
    verify(concentrationKernel, never()).isBackingReady(any[TopicPartition])
    verify(concentrationKernel, never()).nextLogicalOffset(any[String], anyInt)
    verify(concentrationKernel, never()).startLogicalOffset(any[String], anyInt)
  }

  @Test
  def testDeleteRecordsOnLogicalTopicWhenLocalBrokerIsNotLeaderReturnsNotLeaderOrFollower(): Unit = {
    // Codex Q5: admin clients normally route DeleteRecords to the leader via metadata, but the
    // protocol allows the request to land anywhere. Mutating kernel start offsets on a follower
    // would silently drift the follower's view of logical start from the leader's, causing
    // post-truncation reads to disagree across replicas after the next leadership flip.
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))
    when(concentrationKernel.backingPartitionFor(logicalTopic, 0)).thenReturn(2)
    val backingPartition = mock(classOf[Partition])
    when(backingPartition.isLeader).thenReturn(false)
    when(replicaManager.onlinePartition(any[TopicPartition])).thenReturn(Some(backingPartition))

    val deleteRecordsRequest = new DeleteRecordsRequest.Builder(new DeleteRecordsRequestData()
      .setTopics(Collections.singletonList(new DeleteRecordsTopic()
        .setName(logicalTopic)
        .setPartitions(Collections.singletonList(new DeleteRecordsPartition()
          .setOffset(50L)
          .setPartitionIndex(0)))))
      .setTimeoutMs(5000)).build()
    val request = buildRequest(deleteRecordsRequest)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis()
    kafkaApis.handleDeleteRecordsRequest(request)

    val response = verifyNoThrottling[DeleteRecordsResponse](request)
    val partitionResult = response.data.topics.asScala.head.partitions.asScala.head
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, partitionResult.errorCode,
      "DeleteRecords on a backing follower must surface as NOT_LEADER_OR_FOLLOWER (Codex Q5)")
    assertEquals(DeleteRecordsResponse.INVALID_LOW_WATERMARK, partitionResult.lowWatermark)
    // Ordering: BEFORE the readiness gate and BEFORE any tracker mutation.
    verify(concentrationKernel, never()).isBackingReady(any[TopicPartition])
    verify(concentrationKernel, never()).advanceStartOffset(any[String], anyInt, anyLong)
    verify(replicaManager, never()).deleteRecords(anyLong, any(), any(), anyBoolean)
  }

  @Test
  def testProduceToLogicalTopicCollidingOnBackingPartitionFailsSecondEntry(): Unit = {
    // v1 limitation: two logical topics in the SAME ProduceRequest routing to the same backing
    // partition would collide in authorizedRequestInfo's TP key, silently dropping or
    // coalescing records. We fail the SECOND entry loudly with KAFKA_STORAGE_ERROR rather than
    // silently corrupt offset sequencing. The first entry must still succeed normally.
    val logicalA = "orders"
    val logicalB = "events"
    val backingTopic = "concentrated"
    val descriptorA = new LogicalTopicDescriptor(logicalA, 1024, backingTopic, 4)
    val descriptorB = new LogicalTopicDescriptor(logicalB, 1024, backingTopic, 4)
    addTopicToMetadataCache(backingTopic, numPartitions = 4)
    when(concentrationKernel.isLogicalTopic(logicalA)).thenReturn(true)
    when(concentrationKernel.isLogicalTopic(logicalB)).thenReturn(true)
    when(concentrationKernel.describe(logicalA)).thenReturn(Optional.of(descriptorA))
    when(concentrationKernel.describe(logicalB)).thenReturn(Optional.of(descriptorB))
    when(concentrationKernel.isBackingReady(any[TopicPartition])).thenReturn(true)
    // Both logical topics route to backing partition 2 — engineered collision.
    when(concentrationKernel.backingPartitionFor(logicalA, 0)).thenReturn(2)
    when(concentrationKernel.backingPartitionFor(logicalB, 0)).thenReturn(2)

    val resA0 = mock(classOf[Reservation]); when(resA0.logicalOffset).thenReturn(100L)
    when(concentrationKernel.reserveProduceBatch(logicalA, 0, 1)).thenReturn(Array(resA0))
    // HIGH #7: produce path always derives currentLeaderEpoch from onlinePartition.
    when(replicaManager.onlinePartition(any[TopicPartition])).thenReturn(None)

    val tpA = new TopicPartition(logicalA, 0)
    val tpB = new TopicPartition(logicalB, 0)
    val produceRequest = ProduceRequest.builder(new ProduceRequestData()
      .setTopicData(new ProduceRequestData.TopicProduceDataCollection(java.util.Arrays.asList(
        new ProduceRequestData.TopicProduceData()
          .setName(tpA.topic).setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(tpA.partition)
              .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("a".getBytes))))),
        new ProduceRequestData.TopicProduceData()
          .setName(tpB.topic).setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(tpB.partition)
              .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("b".getBytes))))))
          .iterator))
      .setAcks(1.toShort).setTimeoutMs(5000))
      .build(ApiKeys.PRODUCE.latestVersion)
    val request = buildRequest(produceRequest)

    val callbackCaptor: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])
    val backingTp = new TopicPartition(backingTopic, 2)
    when(replicaManager.handleProduceAppend(anyLong, anyShort, ArgumentMatchers.eq(false),
      any(), any(), callbackCaptor.capture(),
      any(), any(), any(), any())
    ).thenAnswer { _ =>
      callbackCaptor.getValue.apply(Map(backingTp -> new PartitionResponse(
        Errors.NONE, 50L, RecordBatch.NO_TIMESTAMP, 0L)))
    }

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    kafkaApis = createKafkaApis()
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[ProduceResponse](request)
    val byTopic = response.data.responses.asScala.map(r => r.name -> r).toMap
    val responseA = byTopic(logicalA).partitionResponses.asScala.head
    val responseB = byTopic(logicalB).partitionResponses.asScala.head

    assertEquals(Errors.NONE, Errors.forCode(responseA.errorCode), "first colliding entry must succeed")
    assertEquals(100L, responseA.baseOffset)
    assertEquals(Errors.KAFKA_STORAGE_ERROR, Errors.forCode(responseB.errorCode),
      "second colliding entry must be rejected loudly, not silently coalesced")

    // Only the first entry consumed a reservation.
    verify(concentrationKernel).reserveProduceBatch(logicalA, 0, 1)
    verify(concentrationKernel, never()).reserveProduceBatch(ArgumentMatchers.eq(logicalB), anyInt, anyInt)
  }

  @Test
  def testProduceToLogicalTopicRejectsTransactionalBatchWithInvalidTxnState(): Unit = {
    // PROMPT.md "Careful" note: a COMMIT marker on the shared backing partition commits records
    // across every logical topic that maps to it, breaking per-logical-topic LSO semantics. v1
    // explicitly excludes transactions. The broker must therefore reject transactional produce
    // to logical topics BEFORE reserving offsets or appending — letting a transactional batch
    // through would silently corrupt cross-topic isolation. INVALID_TXN_STATE is non-retriable
    // on the producer side, matching the v1 contract of "transactional clients are unsupported".
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    addTopicToMetadataCache(backingTopic, numPartitions = 4)
    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))
    when(concentrationKernel.backingPartitionFor(logicalTopic, 0)).thenReturn(2)
    when(concentrationKernel.isBackingReady(any[TopicPartition])).thenReturn(true)

    val tp = new TopicPartition(logicalTopic, 0)
    val txnRecords = MemoryRecords.withTransactionalRecords(Compression.NONE,
      4242L, 7.toShort, 0, new SimpleRecord("payload".getBytes))
    // KafkaApis at line ~388 rejects transactional produce REQUESTS that lack an authorized
    // transactionalId, with TRANSACTIONAL_ID_AUTHORIZATION_FAILED, before per-partition routing
    // runs. To exercise the v1-specific INVALID_TXN_STATE branch we have to clear that gate:
    // set a non-null transactionalId, and rely on the default no-authorizer path which auto-
    // authorizes. Without this, the test would only verify the pre-existing global auth check.
    val produceRequest = ProduceRequest.builder(new ProduceRequestData()
      .setTransactionalId("test-txn-id")
      .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
        Collections.singletonList(new ProduceRequestData.TopicProduceData()
          .setName(tp.topic).setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(tp.partition)
              .setRecords(txnRecords))))
          .iterator))
      .setAcks(1.toShort).setTimeoutMs(5000))
      .build(ApiKeys.PRODUCE.latestVersion)
    val request = buildRequest(produceRequest)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    kafkaApis = createKafkaApis()
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[ProduceResponse](request)
    val partitionProduceResponse = response.data.responses.asScala.head.partitionResponses.asScala.head
    assertEquals(Errors.INVALID_TXN_STATE, Errors.forCode(partitionProduceResponse.errorCode))

    // Reservation, stamping, and append path must never run on a rejected transactional batch:
    // a half-stamped record on the backing log would survive a broker restart and re-emerge as
    // a recoverable logical record, which is exactly the silent corruption we're guarding against.
    verify(concentrationKernel, never()).reserveProduceBatch(any[String], anyInt, anyInt)
    verify(replicaManager, never()).handleProduceAppend(anyLong, anyShort, ArgumentMatchers.eq(false),
      any(), any(), any(), any(), any(), any(), any())
  }

  @Test
  def testProduceIdempotentToLogicalTopicIsRefusedWithInvalidProducerEpoch(): Unit = {
    // r14 BLOCKER B2 (#95). Two logical topics that share a backing partition can drive the
    // SAME producerId+baseSequence into the backing partition's ProducerStateManager from
    // different produce calls — the stamper preserves producer state on the rewritten batch,
    // so the backing PSM sees (producerId, seq=0) twice and rejects the second produce as a
    // duplicate, despite it succeeding to a different logical topic. v1 doesn't add
    // per-logical-topic PSM; the safe stance is to refuse idempotent produce on logical
    // topics with INVALID_PRODUCER_EPOCH (mirroring the existing transactional refusal),
    // forcing the producer onto the non-retriable fatal path so it surfaces loudly rather
    // than corrupting producer state.
    //
    // This test pins the v1 contract at the KafkaApis seam:
    //   - The response carries INVALID_PRODUCER_EPOCH; the kernel's reservation / cache /
    //     idempotent-batch surface is NEVER touched; ReplicaManager.handleProduceAppend is
    //     NEVER called. Any of these firing would mean the gate above the cache lookup did
    //     not fire — that is the exact regression this test guards against.
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    addTopicToMetadataCache(backingTopic, numPartitions = 4)

    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))
    when(concentrationKernel.backingPartitionFor(logicalTopic, 0)).thenReturn(2)
    when(concentrationKernel.isBackingReady(any[TopicPartition])).thenReturn(true)

    val producerId = 4242L
    val producerEpoch: Short = 0
    val baseSeq = 0
    val tp = new TopicPartition(logicalTopic, 0)
    val idempotentRecords = MemoryRecords.withIdempotentRecords(Compression.NONE,
      producerId, producerEpoch, baseSeq,
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k2".getBytes, "v2".getBytes),
      new SimpleRecord("k3".getBytes, "v3".getBytes))
    val produceRequest = ProduceRequest.builder(new ProduceRequestData()
      .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
        Collections.singletonList(new ProduceRequestData.TopicProduceData()
          .setName(tp.topic).setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(tp.partition)
              .setRecords(idempotentRecords))))
          .iterator))
      .setAcks(1.toShort)
      .setTimeoutMs(5000))
      .build(ApiKeys.PRODUCE.latestVersion)
    val request = buildRequest(produceRequest)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    kafkaApis = createKafkaApis()
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[ProduceResponse](request)
    val partitionProduceResponse = response.data.responses.asScala.head.partitionResponses.asScala.head
    assertEquals(Errors.INVALID_PRODUCER_EPOCH, Errors.forCode(partitionProduceResponse.errorCode),
      "idempotent produce on logical topics must be refused with INVALID_PRODUCER_EPOCH " +
      "(non-retriable fatal for the client) in v1 — a retriable code would let the producer " +
      "loop until it eventually corrupts backing PSM state across logical topics")

    // The gate is meant to fire BEFORE any kernel state is touched. If the gate regressed, the
    // cache lookup or reservation would run — either of which would let r14 B2 bite again.
    verify(concentrationKernel, never()).lookupIdempotentBatch(
      any[String], anyInt, any[IdempotentBatchKey], anyInt)
    verify(concentrationKernel, never()).reserveProduceBatch(any[String], anyInt, anyInt)
    verify(concentrationKernel, never()).commitProduceBatch(any(), anyLong)
    verify(concentrationKernel, never()).rollbackProduceBatch(any())
    verify(concentrationKernel, never()).recordIdempotentBatch(
      any[String], anyInt, any(), any())
    verify(replicaManager, never()).handleProduceAppend(anyLong, anyShort, ArgumentMatchers.eq(false),
      any(), any(), any(), any(), any(), any(), any())
  }

  @Test
  def testDeleteRecordsOnBackingTopicIsRejectedWithInvalidTopic(): Unit = {
    // Codex BLOCKER 4 / Gemini #58. A backing topic's physical log is shared by N logical
    // topics on the same partition. A stock DeleteRecords against the backing name would
    // truncate physical state under all of them while sidecar logical start offsets stay
    // unchanged — silent correctness break. The handler must reject at the same level as
    // the Produce-path guard (KafkaApis.scala:423), surfacing INVALID_TOPIC_EXCEPTION.
    val backingTopic = "backing-topic"
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)
    // isLogicalTopic is checked AFTER isBackingTopic; we don't stub it because the guard
    // must short-circuit before reaching it. If the order regressed, the unstubbed default
    // (false) would still let the request fall through to replicaManager — which the
    // never() verify below detects.

    val deleteRecordsRequest = new DeleteRecordsRequest.Builder(new DeleteRecordsRequestData()
      .setTopics(Collections.singletonList(new DeleteRecordsTopic()
        .setName(backingTopic)
        .setPartitions(Collections.singletonList(new DeleteRecordsPartition()
          .setOffset(42L)
          .setPartitionIndex(0)))))
      .setTimeoutMs(5000)).build()
    val request = buildRequest(deleteRecordsRequest)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis()
    kafkaApis.handleDeleteRecordsRequest(request)

    val response = verifyNoThrottling[DeleteRecordsResponse](request)
    val partitionResult = response.data.topics.asScala.head.partitions.asScala.head
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION, Errors.forCode(partitionResult.errorCode))
    assertEquals(DeleteRecordsResponse.INVALID_LOW_WATERMARK, partitionResult.lowWatermark)
    // Neither the kernel nor the replica manager must see this request — it is rejected
    // entirely at the handler level.
    verify(concentrationKernel, never()).advanceStartOffset(any[String], anyInt, anyLong)
    verify(replicaManager, never()).deleteRecords(anyLong, any(), any(), anyBoolean)
  }

  @Test
  def testDeleteRecordsOnLogicalTopicAdvancesKernelStartOffsetAndBypassesReplicaManager(): Unit = {
    // Concentration v1 PROMPT.md acceptance criterion 3: DeleteRecords on a logical topic
    // advances ONLY that logical partition's start offset; the backing log is not truncated.
    // KafkaApis.handleDeleteRecordsRequest must therefore route the partition to the kernel
    // and NEVER touch ReplicaManager.deleteRecords for logical-topic partitions.
    val logicalTopic = "logical-topic"
    val targetOffset = 50L
    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic))
      .thenReturn(Optional.of(new LogicalTopicDescriptor(logicalTopic, 4, "backing-topic", 1)))
    when(concentrationKernel.isBackingReady(any[TopicPartition])).thenReturn(true)
    stubBackingPartitionAsLeader()
    when(concentrationKernel.startLogicalOffset(logicalTopic, 0)).thenReturn(targetOffset)

    val deleteRecordsRequest = new DeleteRecordsRequest.Builder(new DeleteRecordsRequestData()
      .setTopics(Collections.singletonList(new DeleteRecordsTopic()
        .setName(logicalTopic)
        .setPartitions(Collections.singletonList(new DeleteRecordsPartition()
          .setOffset(targetOffset)
          .setPartitionIndex(0)))))
      .setTimeoutMs(5000)).build()
    val request = buildRequest(deleteRecordsRequest)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis()
    kafkaApis.handleDeleteRecordsRequest(request)

    val response = verifyNoThrottling[DeleteRecordsResponse](request)
    assertEquals(1, response.data.topics.size)
    val topicResult = response.data.topics.asScala.head
    assertEquals(logicalTopic, topicResult.name)
    assertEquals(1, topicResult.partitions.size)
    val partitionResult = topicResult.partitions.asScala.head
    assertEquals(Errors.NONE, Errors.forCode(partitionResult.errorCode))
    assertEquals(targetOffset, partitionResult.lowWatermark)

    // Kernel got the advance; ReplicaManager.deleteRecords never did.
    verify(concentrationKernel).advanceStartOffset(logicalTopic, 0, targetOffset)
    verify(replicaManager, never()).deleteRecords(anyLong, any(), any(), anyBoolean)
  }

  @Test
  def testDeleteRecordsOnLogicalTopicSentinelResolvesToNextLogicalOffset(): Unit = {
    // DeleteRecordsRequest.HIGH_WATERMARK (-1) means "delete up to the high water mark". For a
    // logical topic that is the kernel's nextLogicalOffset, not the backing log's LEO. The
    // hook must translate the sentinel before calling advanceStartOffset.
    val logicalTopic = "logical-topic"
    val highWaterMark = 100L
    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic))
      .thenReturn(Optional.of(new LogicalTopicDescriptor(logicalTopic, 4, "backing-topic", 1)))
    when(concentrationKernel.isBackingReady(any[TopicPartition])).thenReturn(true)
    stubBackingPartitionAsLeader()
    when(concentrationKernel.nextLogicalOffset(logicalTopic, 0)).thenReturn(highWaterMark)
    when(concentrationKernel.startLogicalOffset(logicalTopic, 0)).thenReturn(highWaterMark)

    val deleteRecordsRequest = new DeleteRecordsRequest.Builder(new DeleteRecordsRequestData()
      .setTopics(Collections.singletonList(new DeleteRecordsTopic()
        .setName(logicalTopic)
        .setPartitions(Collections.singletonList(new DeleteRecordsPartition()
          .setOffset(DeleteRecordsRequest.HIGH_WATERMARK)
          .setPartitionIndex(0)))))
      .setTimeoutMs(5000)).build()
    val request = buildRequest(deleteRecordsRequest)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis()
    kafkaApis.handleDeleteRecordsRequest(request)

    val response = verifyNoThrottling[DeleteRecordsResponse](request)
    val partitionResult = response.data.topics.asScala.head.partitions.asScala.head
    assertEquals(Errors.NONE, Errors.forCode(partitionResult.errorCode))
    assertEquals(highWaterMark, partitionResult.lowWatermark)
    // The kernel was asked to advance to the resolved high water mark, not the sentinel.
    verify(concentrationKernel).advanceStartOffset(logicalTopic, 0, highWaterMark)
  }

  @Test
  def testDeleteRecordsOnLogicalTopicMapsIllegalArgumentToOffsetOutOfRange(): Unit = {
    // The tracker throws IllegalArgumentException when newStart > nextLogicalOffset (past HW)
    // or newStart < currentStartOffset (moving backward). Both are client-visible as
    // OFFSET_OUT_OF_RANGE — the same error stock DeleteRecords returns for the same condition.
    val logicalTopic = "logical-topic"
    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic))
      .thenReturn(Optional.of(new LogicalTopicDescriptor(logicalTopic, 4, "backing-topic", 1)))
    when(concentrationKernel.isBackingReady(any[TopicPartition])).thenReturn(true)
    stubBackingPartitionAsLeader()
    doThrow(new IllegalArgumentException("offset past HW"))
      .when(concentrationKernel).advanceStartOffset(logicalTopic, 0, 9999L)

    val deleteRecordsRequest = new DeleteRecordsRequest.Builder(new DeleteRecordsRequestData()
      .setTopics(Collections.singletonList(new DeleteRecordsTopic()
        .setName(logicalTopic)
        .setPartitions(Collections.singletonList(new DeleteRecordsPartition()
          .setOffset(9999L)
          .setPartitionIndex(0)))))
      .setTimeoutMs(5000)).build()
    val request = buildRequest(deleteRecordsRequest)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis()
    kafkaApis.handleDeleteRecordsRequest(request)

    val response = verifyNoThrottling[DeleteRecordsResponse](request)
    val partitionResult = response.data.topics.asScala.head.partitions.asScala.head
    assertEquals(Errors.OFFSET_OUT_OF_RANGE, Errors.forCode(partitionResult.errorCode))
    assertEquals(DeleteRecordsResponse.INVALID_LOW_WATERMARK, partitionResult.lowWatermark)
    verify(replicaManager, never()).deleteRecords(anyLong, any(), any(), anyBoolean)
  }

  @Test
  def testDeleteRecordsOnLogicalTopicWithOutOfRangePartitionReturnsUnknownTopicOrPartition(): Unit = {
    // Audit-fix follow-up: the kernel facade's advanceStartOffset lazily creates tracker state
    // for any (topic, partition) key, so without an explicit bounds check the broker would
    // silently return NONE with lowWatermark=0 for partition 9999 of a 4-partition logical
    // topic. The hook resolves this by validating partition < descriptor.numLogicalPartitions
    // first and surfacing the failure as UNKNOWN_TOPIC_OR_PARTITION — the same error stock
    // Kafka returns for a partition index that doesn't exist.
    val logicalTopic = "logical-topic"
    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic))
      .thenReturn(Optional.of(new LogicalTopicDescriptor(logicalTopic, 4, "backing-topic", 1)))

    val deleteRecordsRequest = new DeleteRecordsRequest.Builder(new DeleteRecordsRequestData()
      .setTopics(Collections.singletonList(new DeleteRecordsTopic()
        .setName(logicalTopic)
        .setPartitions(Collections.singletonList(new DeleteRecordsPartition()
          .setOffset(0L)
          .setPartitionIndex(9999)))))
      .setTimeoutMs(5000)).build()
    val request = buildRequest(deleteRecordsRequest)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis()
    kafkaApis.handleDeleteRecordsRequest(request)

    val response = verifyNoThrottling[DeleteRecordsResponse](request)
    val partitionResult = response.data.topics.asScala.head.partitions.asScala.head
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.forCode(partitionResult.errorCode))
    assertEquals(DeleteRecordsResponse.INVALID_LOW_WATERMARK, partitionResult.lowWatermark)
    // The kernel must NOT have been asked to advance — bounds rejection happens before that call.
    verify(concentrationKernel, never()).advanceStartOffset(any[String], anyInt, anyLong)
    verify(replicaManager, never()).deleteRecords(anyLong, any(), any(), anyBoolean)
  }

  @Test
  def testDeleteRecordsOnLogicalTopicMapsIllegalStateToKafkaStorageError(): Unit = {
    // Audit-fix follow-up: the kernel facade's ensureOpen() throws IllegalStateException if a
    // DeleteRecords request arrives after kernel.close() has run (broker shutdown raced the
    // request). Without an explicit catch the exception would propagate into the request
    // handler and crash the data-plane thread. The hook maps it to KAFKA_STORAGE_ERROR — a
    // retriable storage-layer failure — so the client retries against the next live broker
    // rather than seeing a generic error.
    val logicalTopic = "logical-topic"
    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic))
      .thenReturn(Optional.of(new LogicalTopicDescriptor(logicalTopic, 4, "backing-topic", 1)))
    when(concentrationKernel.isBackingReady(any[TopicPartition])).thenReturn(true)
    stubBackingPartitionAsLeader()
    doThrow(new IllegalStateException("ConcentrationKernel is closed"))
      .when(concentrationKernel).advanceStartOffset(logicalTopic, 0, 42L)

    val deleteRecordsRequest = new DeleteRecordsRequest.Builder(new DeleteRecordsRequestData()
      .setTopics(Collections.singletonList(new DeleteRecordsTopic()
        .setName(logicalTopic)
        .setPartitions(Collections.singletonList(new DeleteRecordsPartition()
          .setOffset(42L)
          .setPartitionIndex(0)))))
      .setTimeoutMs(5000)).build()
    val request = buildRequest(deleteRecordsRequest)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis()
    kafkaApis.handleDeleteRecordsRequest(request)

    val response = verifyNoThrottling[DeleteRecordsResponse](request)
    val partitionResult = response.data.topics.asScala.head.partitions.asScala.head
    assertEquals(Errors.KAFKA_STORAGE_ERROR, Errors.forCode(partitionResult.errorCode))
    assertEquals(DeleteRecordsResponse.INVALID_LOW_WATERMARK, partitionResult.lowWatermark)
    verify(replicaManager, never()).deleteRecords(anyLong, any(), any(), anyBoolean)
  }

  @Test
  def testFetchOnBackingTopicIsRejectedWithInvalidTopic(): Unit = {
    // Codex HIGH 1. Direct client fetch against a backing topic name would expose raw
    // interleaved records belonging to every logical-topic tenant multiplexed onto the
    // backing partition — both their payloads and their concentration headers. The fetch
    // path must reject with INVALID_TOPIC_EXCEPTION at the same level as the Produce and
    // DeleteRecords guards. Internal backing fetches (issued by routeLogicalFetch) bypass
    // this classification loop, so this guard does not affect legitimate logical fetches.
    val backingTopic = "backing-topic"
    val backingTopicId = Uuid.randomUuid()
    addTopicToMetadataCache(backingTopic, numPartitions = 1, topicId = backingTopicId)
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)

    val backingTip = new TopicIdPartition(backingTopicId, new TopicPartition(backingTopic, 0))
    val fetchDataBuilder = Map(backingTip.topicPartition ->
      new FetchRequest.PartitionData(backingTip.topicId, 0L, 0L, 1_000_000, Optional.empty())).asJava
    val fetchData = Map(backingTip ->
      new FetchRequest.PartitionData(backingTip.topicId, 0L, 0L, 1_000_000, Optional.empty())).asJava
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      fetchMetadata, fetchData, false, false)
    when(fetchManager.newContext(
      any[Short], any[JFetchMetadata], any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      any[util.List[TopicIdPartition]], any[util.Map[Uuid, String]])).thenReturn(fetchContext)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val fetchRequest = new FetchRequest.Builder(ApiKeys.FETCH.latestVersion,
      ApiKeys.FETCH.latestVersion, -1, -1, 100, 0, fetchDataBuilder).build()
    val request = buildRequest(fetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleFetchRequest(request)

    val response = verifyNoThrottling[FetchResponse](request)
    val responseData = response.responseData(metadataCache.topicIdsToNames(), ApiKeys.FETCH.latestVersion)
    val partitionData = responseData.get(backingTip.topicPartition)
    assertNotNull(partitionData, "response must contain an entry for the rejected backing TIP")
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, partitionData.errorCode,
      "direct fetch on a backing topic must be rejected with INVALID_TOPIC_EXCEPTION")
    // ReplicaManager must never see the backing-topic fetch — the guard short-circuits it.
    verify(replicaManager, never()).fetchMessages(
      any[FetchParams], any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]],
      any[ReplicaQuota], any[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]())
  }

  @Test
  def testFetchOnLogicalTopicRoutesToBackingAndTranslatesResponse(): Unit = {
    // Concentration hook #3.B happy path. A stock consumer fetches logical topic "orders"
    // partition 0 at offset 100. The hook must:
    //   1. Resolve logical→backing partition via the kernel (backing partition 2 of "concentrated").
    //   2. Resolve logical→backing offset via the kernel (logical 100 → backing 7777).
    //   3. Issue ReplicaManager.fetchMessages against the backing TIP (not the logical TIP).
    //   4. On the response, strip non-matching records (events on the same backing partition),
    //      rewrite the surviving records' offsets back to logical (100, 101, 102), drop the
    //      two concentration headers, and re-key the response to the logical TIP.
    //   5. Set highWatermark / logStartOffset / lastStableOffset to logical-topic values.
    // Pinning all five in one test mirrors the produce-side coverage and the consumer's view of
    // the contract.
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    val backingTopicId = Uuid.randomUuid()
    val logicalTopicId = Uuid.randomUuid()
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    addTopicToMetadataCache(backingTopic, numPartitions = 4, topicId = backingTopicId)
    // The logical topic must appear in metadataCache so FetchResponse.responseData() can
    // resolve its UUID back to a name when decoding the response. (FetchResponse at
    // version >= 13 silently drops entries whose topicId isn't in the cache.) The hook fires
    // on isLogicalTopic BEFORE the metadataCache.contains check, so this registration
    // doesn't change which branch we take.
    addTopicToMetadataCache(logicalTopic, numPartitions = 1024, topicId = logicalTopicId)

    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))
    when(concentrationKernel.backingPartitionFor(logicalTopic, 0)).thenReturn(2)
    when(concentrationKernel.isBackingReady(any[TopicPartition])).thenReturn(true)
    stubBackingPartitionAsLeader()
    when(concentrationKernel.nextLogicalOffset(logicalTopic, 0)).thenReturn(103L)
    when(concentrationKernel.startLogicalOffset(logicalTopic, 0)).thenReturn(0L)
    when(concentrationKernel.resolveBackingOffset(logicalTopic, 0, 100L)).thenReturn(7777L)

    // Build the backing-side records that ReplicaManager would return: three "orders" records
    // interleaved with two "events" records (events must be filtered out by the translator).
    val backingRecords = backingRecordsFromInterleaved(
      ("orders", 0, 100L, "k0", "ord-0"),
      ("events", 0, 50L,  "kx", "evt-x"),
      ("orders", 0, 101L, "k1", "ord-1"),
      ("events", 0, 51L,  "ky", "evt-y"),
      ("orders", 0, 102L, "k2", "ord-2"))

    val backingTip = new TopicIdPartition(backingTopicId, new TopicPartition(backingTopic, 2))
    val fetchInfoCaptor: ArgumentCaptor[Seq[(TopicIdPartition, FetchRequest.PartitionData)]] =
      ArgumentCaptor.forClass(classOf[Seq[(TopicIdPartition, FetchRequest.PartitionData)]])
    when(replicaManager.fetchMessages(
      any[FetchParams],
      fetchInfoCaptor.capture(),
      any[ReplicaQuota],
      any[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]()
    )).thenAnswer { invocation =>
      val callback = invocation.getArgument(3).asInstanceOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]
      callback(Seq(backingTip -> new FetchPartitionData(Errors.NONE, 7780L, 0L, backingRecords,
        Optional.empty(), OptionalLong.of(7780L), Optional.empty(), OptionalInt.empty(), false)))
    }

    val logicalTip = new TopicIdPartition(logicalTopicId, new TopicPartition(logicalTopic, 0))
    val fetchData = Map(logicalTip ->
      new FetchRequest.PartitionData(logicalTip.topicId, 100L, 0L, 1_000_000, Optional.empty())).asJava
    val fetchDataBuilder = Map(logicalTip.topicPartition ->
      new FetchRequest.PartitionData(logicalTip.topicId, 100L, 0L, 1_000_000, Optional.empty())).asJava
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      fetchMetadata, fetchData, false, false)
    when(fetchManager.newContext(
      any[Short], any[JFetchMetadata], any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      any[util.List[TopicIdPartition]], any[util.Map[Uuid, String]])).thenReturn(fetchContext)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val fetchRequest = new FetchRequest.Builder(ApiKeys.FETCH.latestVersion,
      ApiKeys.FETCH.latestVersion, -1, -1, 100, 0, fetchDataBuilder).build()
    val request = buildRequest(fetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleFetchRequest(request)

    // ReplicaManager saw the BACKING TIP with the translated backing fetch offset (7777),
    // not the logical TIP with logical fetch offset (100).
    val capturedFetchInfo = fetchInfoCaptor.getValue.toMap
    assertTrue(capturedFetchInfo.contains(backingTip),
      s"expected backing TIP $backingTip in fetchInfos, got ${capturedFetchInfo.keys}")
    assertFalse(capturedFetchInfo.contains(logicalTip),
      "logical TIP must not leak into ReplicaManager.fetchMessages")
    assertEquals(7777L, capturedFetchInfo(backingTip).fetchOffset,
      "ReplicaManager must be asked for the BACKING offset, not the logical one")

    // The consumer-visible response is keyed by the LOGICAL TP.
    val response = verifyNoThrottling[FetchResponse](request)
    val responseData = response.responseData(metadataCache.topicIdsToNames(), ApiKeys.FETCH.latestVersion)
    assertTrue(responseData.containsKey(logicalTip.topicPartition),
      s"response must be keyed by logical TIP $logicalTip, got ${responseData.keySet()}")
    assertFalse(responseData.containsKey(backingTip.topicPartition),
      "backing TIP must not leak to the consumer")

    val partitionData = responseData.get(logicalTip.topicPartition)
    assertEquals(Errors.NONE.code, partitionData.errorCode)
    assertEquals(103L, partitionData.highWatermark, "highWatermark must be the LOGICAL next offset")
    assertEquals(0L, partitionData.logStartOffset, "logStartOffset must be the LOGICAL start")
    assertEquals(103L, partitionData.lastStableOffset, "v1 non-transactional: LSO == HW")

    // Records: only "orders" survives (events filtered), offsets rewritten to logical, headers stripped.
    val outRecords = FetchResponse.recordsOrFail(partitionData)
    val iter = outRecords.records().iterator()
    val seen = scala.collection.mutable.ArrayBuffer[(Long, String)]()
    while (iter.hasNext) {
      val r = iter.next()
      val bytes = new Array[Byte](r.value().remaining())
      r.value().duplicate().get(bytes)
      seen += ((r.offset(), new String(bytes, StandardCharsets.UTF_8)))
      // No concentration headers must leak to the consumer.
      r.headers().foreach { h =>
        assertNotEquals(ConcentrationHeaders.LOGICAL_TOPIC_HEADER, h.key())
        assertNotEquals(ConcentrationHeaders.LOGICAL_OFFSET_HEADER, h.key())
      }
    }
    assertEquals(Seq((100L, "ord-0"), (101L, "ord-1"), (102L, "ord-2")), seen.toSeq,
      "consumer must see only orders' records with logical offsets, in order")
  }

  @Test
  def testFetchCoalescesTwoLogicalPartitionsSharingOneBackingPartition(): Unit = {
    // Codex audit CRIT #2. PROMPT premise is N >> M, so logical partitions of the same topic
    // routinely share a backing partition. A stock consumer ROUTINELY puts many partitions in
    // one fetch request — that's the whole "consumer fetches all assigned partitions in one
    // round-trip" model. Pre-fix behaviour: two logical TIPs mapping to the same backing TIP
    // both inserted entries into logicalByBacking keyed by the backing TIP, the second
    // insertion overwrote the first, and the consumer's response silently dropped one logical
    // partition.
    //
    // After the fix: a single backing fetch is issued (coalesced), and the one backing response
    // is fanned out to BOTH logical TIPs with per-TIP demux. Pinned here: exactly one backing
    // fetch issued, both logical TIPs present in the consumer's response, each with only its
    // own demuxed records.
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    val backingTopicId = Uuid.randomUuid()
    val logicalTopicId = Uuid.randomUuid()
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    addTopicToMetadataCache(backingTopic, numPartitions = 4, topicId = backingTopicId)
    addTopicToMetadataCache(logicalTopic, numPartitions = 1024, topicId = logicalTopicId)

    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))
    when(concentrationKernel.isBackingReady(any[TopicPartition])).thenReturn(true)
    stubBackingPartitionAsLeader()
    // Both logical partitions map to the SAME backing partition — the bug condition.
    when(concentrationKernel.backingPartitionFor(logicalTopic, 0)).thenReturn(2)
    when(concentrationKernel.backingPartitionFor(logicalTopic, 4)).thenReturn(2)
    when(concentrationKernel.nextLogicalOffset(logicalTopic, 0)).thenReturn(10L)
    when(concentrationKernel.nextLogicalOffset(logicalTopic, 4)).thenReturn(20L)
    when(concentrationKernel.startLogicalOffset(logicalTopic, 0)).thenReturn(0L)
    when(concentrationKernel.startLogicalOffset(logicalTopic, 4)).thenReturn(0L)
    when(concentrationKernel.resolveBackingOffset(logicalTopic, 0, 0L)).thenReturn(5000L)
    when(concentrationKernel.resolveBackingOffset(logicalTopic, 4, 0L)).thenReturn(5050L)

    // Backing records interleave both logical partitions on the SHARED backing partition.
    val backingRecords = backingRecordsFromInterleaved(
      ("orders", 0, 0L, "k-p0-0", "p0-r0"),
      ("orders", 4, 0L, "k-p4-0", "p4-r0"),
      ("orders", 0, 1L, "k-p0-1", "p0-r1"),
      ("orders", 4, 1L, "k-p4-1", "p4-r1"),
      ("orders", 0, 2L, "k-p0-2", "p0-r2"))

    val backingTip = new TopicIdPartition(backingTopicId, new TopicPartition(backingTopic, 2))
    val fetchInfoCaptor: ArgumentCaptor[Seq[(TopicIdPartition, FetchRequest.PartitionData)]] =
      ArgumentCaptor.forClass(classOf[Seq[(TopicIdPartition, FetchRequest.PartitionData)]])
    when(replicaManager.fetchMessages(
      any[FetchParams],
      fetchInfoCaptor.capture(),
      any[ReplicaQuota],
      any[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]()
    )).thenAnswer { invocation =>
      val callback = invocation.getArgument(3).asInstanceOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]
      callback(Seq(backingTip -> new FetchPartitionData(Errors.NONE, 6000L, 0L, backingRecords,
        Optional.empty(), OptionalLong.of(6000L), Optional.empty(), OptionalInt.empty(), false)))
    }

    val logicalTipP0 = new TopicIdPartition(logicalTopicId, new TopicPartition(logicalTopic, 0))
    val logicalTipP4 = new TopicIdPartition(logicalTopicId, new TopicPartition(logicalTopic, 4))
    val fetchData = Map(
      logicalTipP0 -> new FetchRequest.PartitionData(logicalTopicId, 0L, 0L, 1_000_000, Optional.empty()),
      logicalTipP4 -> new FetchRequest.PartitionData(logicalTopicId, 0L, 0L, 500_000, Optional.empty())
    ).asJava
    val fetchDataBuilder = Map(
      logicalTipP0.topicPartition -> new FetchRequest.PartitionData(logicalTopicId, 0L, 0L, 1_000_000, Optional.empty()),
      logicalTipP4.topicPartition -> new FetchRequest.PartitionData(logicalTopicId, 0L, 0L, 500_000, Optional.empty())
    ).asJava
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      new JFetchMetadata(0, 0), fetchData, false, false)
    when(fetchManager.newContext(
      any[Short], any[JFetchMetadata], any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      any[util.List[TopicIdPartition]], any[util.Map[Uuid, String]])).thenReturn(fetchContext)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val fetchRequest = new FetchRequest.Builder(ApiKeys.FETCH.latestVersion,
      ApiKeys.FETCH.latestVersion, -1, -1, 100, 0, fetchDataBuilder).build()
    val request = buildRequest(fetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleFetchRequest(request)

    // EXACTLY ONE backing fetch despite TWO logical participants — this is the coalesce pin.
    val capturedFetchInfo = fetchInfoCaptor.getValue
    assertEquals(1, capturedFetchInfo.size,
      s"coalesced fetch must issue ONE backing read for shared backing partition; got ${capturedFetchInfo}")
    val (capturedTp, capturedPd) = capturedFetchInfo.head
    assertEquals(backingTip, capturedTp, "backing fetch must target the shared backing TIP")
    // Merged read window: earliest fetchOffset (min(5000, 5050) = 5000) and largest maxBytes.
    assertEquals(5000L, capturedPd.fetchOffset,
      "coalesced fetch must start at the earliest backing offset so every participant's data is reachable")
    assertEquals(1_000_000, capturedPd.maxBytes,
      "coalesced fetch must use the largest maxBytes so the response can fit the union of all participants")

    val response = verifyNoThrottling[FetchResponse](request)
    val responseData = response.responseData(metadataCache.topicIdsToNames(), ApiKeys.FETCH.latestVersion)

    // BOTH logical TIPs present in the consumer's response — pre-fix, only the last one survived.
    assertTrue(responseData.containsKey(logicalTipP0.topicPartition),
      s"orders-0 must be in response; got ${responseData.keySet()}")
    assertTrue(responseData.containsKey(logicalTipP4.topicPartition),
      s"orders-4 must be in response; got ${responseData.keySet()}")

    val p0Data = responseData.get(logicalTipP0.topicPartition)
    val p4Data = responseData.get(logicalTipP4.topicPartition)
    assertEquals(Errors.NONE.code, p0Data.errorCode)
    assertEquals(Errors.NONE.code, p4Data.errorCode)
    assertEquals(10L, p0Data.highWatermark, "orders-0 high watermark must match its own next logical offset")
    assertEquals(20L, p4Data.highWatermark, "orders-4 high watermark must match its own next logical offset")

    def collectValues(pd: FetchResponseData.PartitionData): Seq[(Long, String)] = {
      val out = scala.collection.mutable.ArrayBuffer[(Long, String)]()
      val iter = FetchResponse.recordsOrFail(pd).records().iterator()
      while (iter.hasNext) {
        val r = iter.next()
        val bytes = new Array[Byte](r.value().remaining())
        r.value().duplicate().get(bytes)
        out += ((r.offset(), new String(bytes, StandardCharsets.UTF_8)))
      }
      out.toSeq
    }
    // orders-0 sees only its three records, monotonic logical offsets.
    assertEquals(Seq((0L, "p0-r0"), (1L, "p0-r1"), (2L, "p0-r2")), collectValues(p0Data),
      "orders-0 consumer must see only partition-0 records — no leakage from orders-4 sharing the backing")
    // orders-4 sees only its two records, monotonic logical offsets.
    assertEquals(Seq((0L, "p4-r0"), (1L, "p4-r1")), collectValues(p4Data),
      "orders-4 consumer must see only partition-4 records — no leakage from orders-0 sharing the backing")
  }

  @Test
  def testFetchOnLogicalTopicAtTailReturnsEmptyWithoutHittingBacking(): Unit = {
    // A consumer fetching at the LOGICAL tail (fetchOffset == nextLogicalOffset) must get an
    // immediate empty NONE response. The broker must NOT issue a backing fetch — at the tail
    // every backing record belongs to OTHER logical topics, and the translator would filter it
    // all out anyway. Pin both behaviours: empty response AND no replicaManager.fetchMessages
    // call. v1 trades long-poll for this shortcut — documented in the production code comment.
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    val backingTopicId = Uuid.randomUuid()
    val logicalTopicId = Uuid.randomUuid()
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    addTopicToMetadataCache(backingTopic, numPartitions = 4, topicId = backingTopicId)
    addTopicToMetadataCache(logicalTopic, numPartitions = 1024, topicId = logicalTopicId)

    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))
    when(concentrationKernel.backingPartitionFor(logicalTopic, 0)).thenReturn(2)
    when(concentrationKernel.isBackingReady(any[TopicPartition])).thenReturn(true)
    stubBackingPartitionAsLeader()
    when(concentrationKernel.nextLogicalOffset(logicalTopic, 0)).thenReturn(42L)
    when(concentrationKernel.startLogicalOffset(logicalTopic, 0)).thenReturn(0L)

    val logicalTip = new TopicIdPartition(logicalTopicId, new TopicPartition(logicalTopic, 0))
    // Consumer fetches at exactly nextLogicalOffset — the log tail.
    val fetchData = Map(logicalTip ->
      new FetchRequest.PartitionData(logicalTip.topicId, 42L, 0L, 1_000_000, Optional.empty())).asJava
    val fetchDataBuilder = Map(logicalTip.topicPartition ->
      new FetchRequest.PartitionData(logicalTip.topicId, 42L, 0L, 1_000_000, Optional.empty())).asJava
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      new JFetchMetadata(0, 0), fetchData, false, false)
    when(fetchManager.newContext(
      any[Short], any[JFetchMetadata], any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      any[util.List[TopicIdPartition]], any[util.Map[Uuid, String]])).thenReturn(fetchContext)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val fetchRequest = new FetchRequest.Builder(ApiKeys.FETCH.latestVersion,
      ApiKeys.FETCH.latestVersion, -1, -1, 100, 0, fetchDataBuilder).build()
    val request = buildRequest(fetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleFetchRequest(request)

    verify(replicaManager, never()).fetchMessages(any[FetchParams],
      any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]],
      any[ReplicaQuota],
      any[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]())

    val response = verifyNoThrottling[FetchResponse](request)
    val responseData = response.responseData(metadataCache.topicIdsToNames(), ApiKeys.FETCH.latestVersion)
    val partitionData = responseData.get(logicalTip.topicPartition)
    assertEquals(Errors.NONE.code, partitionData.errorCode)
    assertEquals(42L, partitionData.highWatermark)
    assertEquals(42L, partitionData.lastStableOffset)
    assertEquals(0L, partitionData.logStartOffset)
    val out = FetchResponse.recordsOrFail(partitionData)
    assertEquals(0, out.sizeInBytes, "tail fetch must yield zero-byte records")
  }

  @Test
  def testFetchOnLogicalTopicBeyondTailReturnsOffsetOutOfRange(): Unit = {
    // A consumer fetching at offset > nextLogicalOffset is OFFSET_OUT_OF_RANGE — same contract
    // as a stock topic. This catches a buggy consumer that didn't reset its offset after the
    // log was truncated by DeleteRecords or after a consumer-group rebalance lost the position.
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    val backingTopicId = Uuid.randomUuid()
    val logicalTopicId = Uuid.randomUuid()
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    addTopicToMetadataCache(backingTopic, numPartitions = 4, topicId = backingTopicId)
    addTopicToMetadataCache(logicalTopic, numPartitions = 1024, topicId = logicalTopicId)

    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))
    when(concentrationKernel.backingPartitionFor(logicalTopic, 0)).thenReturn(2)
    when(concentrationKernel.isBackingReady(any[TopicPartition])).thenReturn(true)
    stubBackingPartitionAsLeader()
    when(concentrationKernel.nextLogicalOffset(logicalTopic, 0)).thenReturn(50L)
    when(concentrationKernel.startLogicalOffset(logicalTopic, 0)).thenReturn(10L)

    val logicalTip = new TopicIdPartition(logicalTopicId, new TopicPartition(logicalTopic, 0))
    // Consumer fetches past the tail — must be rejected, kernel.resolveBackingOffset never called.
    val fetchData = Map(logicalTip ->
      new FetchRequest.PartitionData(logicalTip.topicId, 9999L, 0L, 1_000_000, Optional.empty())).asJava
    val fetchDataBuilder = Map(logicalTip.topicPartition ->
      new FetchRequest.PartitionData(logicalTip.topicId, 9999L, 0L, 1_000_000, Optional.empty())).asJava
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      new JFetchMetadata(0, 0), fetchData, false, false)
    when(fetchManager.newContext(
      any[Short], any[JFetchMetadata], any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      any[util.List[TopicIdPartition]], any[util.Map[Uuid, String]])).thenReturn(fetchContext)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val fetchRequest = new FetchRequest.Builder(ApiKeys.FETCH.latestVersion,
      ApiKeys.FETCH.latestVersion, -1, -1, 100, 0, fetchDataBuilder).build()
    val request = buildRequest(fetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleFetchRequest(request)

    verify(concentrationKernel, never()).resolveBackingOffset(any[String], anyInt, anyLong)
    verify(replicaManager, never()).fetchMessages(any[FetchParams],
      any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]],
      any[ReplicaQuota],
      any[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]())

    val response = verifyNoThrottling[FetchResponse](request)
    val responseData = response.responseData(metadataCache.topicIdsToNames(), ApiKeys.FETCH.latestVersion)
    val partitionData = responseData.get(logicalTip.topicPartition)
    assertEquals(Errors.OFFSET_OUT_OF_RANGE.code, partitionData.errorCode)
  }

  @Test
  def testFetchOnLogicalTopicWithOutOfRangePartitionReturnsUnknownTopicOrPartition(): Unit = {
    // Partition index >= numLogicalPartitions is a client routing bug, not transient — same
    // contract as the produce path's out-of-range rejection.
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    val backingTopicId = Uuid.randomUuid()
    val logicalTopicId = Uuid.randomUuid()
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    addTopicToMetadataCache(backingTopic, numPartitions = 4, topicId = backingTopicId)
    addTopicToMetadataCache(logicalTopic, numPartitions = 1024, topicId = logicalTopicId)

    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))

    val logicalTip = new TopicIdPartition(logicalTopicId, new TopicPartition(logicalTopic, 9999))
    val fetchData = Map(logicalTip ->
      new FetchRequest.PartitionData(logicalTip.topicId, 0L, 0L, 1_000_000, Optional.empty())).asJava
    val fetchDataBuilder = Map(logicalTip.topicPartition ->
      new FetchRequest.PartitionData(logicalTip.topicId, 0L, 0L, 1_000_000, Optional.empty())).asJava
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      new JFetchMetadata(0, 0), fetchData, false, false)
    when(fetchManager.newContext(
      any[Short], any[JFetchMetadata], any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      any[util.List[TopicIdPartition]], any[util.Map[Uuid, String]])).thenReturn(fetchContext)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val fetchRequest = new FetchRequest.Builder(ApiKeys.FETCH.latestVersion,
      ApiKeys.FETCH.latestVersion, -1, -1, 100, 0, fetchDataBuilder).build()
    val request = buildRequest(fetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleFetchRequest(request)

    verify(concentrationKernel, never()).backingPartitionFor(any[String], anyInt)
    verify(replicaManager, never()).fetchMessages(any[FetchParams],
      any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]],
      any[ReplicaQuota],
      any[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]())

    val response = verifyNoThrottling[FetchResponse](request)
    val responseData = response.responseData(metadataCache.topicIdsToNames(), ApiKeys.FETCH.latestVersion)
    val partitionData = responseData.get(logicalTip.topicPartition)
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code, partitionData.errorCode)
  }

  @Test
  def testFetchOnLogicalTopicErrorResponseRemapsOffsetsToLogicalSpace(): Unit = {
    // r19 #131 BLOCKER. When ReplicaManager.fetchMessages returns a FetchPartitionData with a
    // non-NONE error (NOT_LEADER_OR_FOLLOWER, KAFKA_STORAGE_ERROR, OFFSET_OUT_OF_RANGE on the
    // backing partition, etc.), the offset fields on that data are in the BACKING offset
    // space. The pre-fix code passed the whole FetchPartitionData through verbatim to the
    // logical participant — leaking backing HW / logStartOffset to the consumer. Concretely:
    //   * backing retention can leave the backing log's logStartOffset thousands of records
    //     above zero while the logical topic's true start is 0 (no DeleteRecords issued);
    //   * the backing HW is the sum across all logical partitions coalesced onto it, easily
    //     orders of magnitude beyond any single logical partition's nextLogicalOffset.
    // A stock consumer that receives a transient NOT_LEADER_OR_FOLLOWER for "orders" partition
    // 0 would read those numbers as logical offsets, observe its committed offset (say 25) is
    // BELOW the leaked logStartOffset (say 88888), and on the next successful fetch trigger
    // OFFSET_OUT_OF_RANGE reset behaviour — skipping past the actual data to a meaningless
    // backing position. The fix remaps HW / logStartOffset / LSO to the LOGICAL offset space
    // even on error so the consumer's view of the partition's offset bounds remains coherent.
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    val backingTopicId = Uuid.randomUuid()
    val logicalTopicId = Uuid.randomUuid()
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    addTopicToMetadataCache(backingTopic, numPartitions = 4, topicId = backingTopicId)
    addTopicToMetadataCache(logicalTopic, numPartitions = 1024, topicId = logicalTopicId)

    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))
    when(concentrationKernel.backingPartitionFor(logicalTopic, 0)).thenReturn(2)
    when(concentrationKernel.isBackingReady(any[TopicPartition])).thenReturn(true)
    stubBackingPartitionAsLeader()
    // The logical offset space the consumer expects to see.
    when(concentrationKernel.nextLogicalOffset(logicalTopic, 0)).thenReturn(50L)
    when(concentrationKernel.startLogicalOffset(logicalTopic, 0)).thenReturn(0L)
    when(concentrationKernel.resolveBackingOffset(logicalTopic, 0, 25L)).thenReturn(7777L)

    // BACKING-space offsets the (pre-fix) code would have leaked. The 88888L logStartOffset
    // is the BLOCKER-defining condition: backing retention well above zero while the logical
    // topic's true start is zero. The 99999L HW is the sum of HWs across all logical
    // partitions concentrated onto this backing.
    val leakedBackingHw = 99999L
    val leakedBackingLogStart = 88888L
    val backingTip = new TopicIdPartition(backingTopicId, new TopicPartition(backingTopic, 2))
    when(replicaManager.fetchMessages(
      any[FetchParams],
      any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]],
      any[ReplicaQuota],
      any[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]()
    )).thenAnswer { invocation =>
      val callback = invocation.getArgument(3).asInstanceOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]
      // KAFKA_STORAGE_ERROR rather than NOT_LEADER_OR_FOLLOWER: at FETCH v16+ the
      // response path resolves a new leader for the NOT_LEADER / FENCED_EPOCH cases,
      // pulling in extra wiring (replicaManager.getPartitionOrError, alive-broker
      // lookup) that's irrelevant to the offset-leak invariant under test. Any
      // non-NONE error reproduces the leak, so pick the cleanest one.
      callback(Seq(backingTip -> new FetchPartitionData(
        Errors.KAFKA_STORAGE_ERROR,
        leakedBackingHw,
        leakedBackingLogStart,
        MemoryRecords.EMPTY,
        Optional.empty(),
        OptionalLong.of(leakedBackingHw),
        Optional.empty(),
        OptionalInt.empty(),
        false)))
    }

    val logicalTip = new TopicIdPartition(logicalTopicId, new TopicPartition(logicalTopic, 0))
    val fetchData = Map(logicalTip ->
      new FetchRequest.PartitionData(logicalTip.topicId, 25L, 0L, 1_000_000, Optional.empty())).asJava
    val fetchDataBuilder = Map(logicalTip.topicPartition ->
      new FetchRequest.PartitionData(logicalTip.topicId, 25L, 0L, 1_000_000, Optional.empty())).asJava
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      new JFetchMetadata(0, 0), fetchData, false, false)
    when(fetchManager.newContext(
      any[Short], any[JFetchMetadata], any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      any[util.List[TopicIdPartition]], any[util.Map[Uuid, String]])).thenReturn(fetchContext)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val fetchRequest = new FetchRequest.Builder(ApiKeys.FETCH.latestVersion,
      ApiKeys.FETCH.latestVersion, -1, -1, 100, 0, fetchDataBuilder).build()
    val request = buildRequest(fetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleFetchRequest(request)

    val response = verifyNoThrottling[FetchResponse](request)
    val responseData = response.responseData(metadataCache.topicIdsToNames(), ApiKeys.FETCH.latestVersion)
    val partitionData = responseData.get(logicalTip.topicPartition)
    assertNotNull(partitionData, "expected the logical TP to appear in the fetch response")

    // The error propagates to the logical participant — same fault as the backing fetch.
    assertEquals(Errors.KAFKA_STORAGE_ERROR.code, partitionData.errorCode,
      "the replication-level error against the backing must propagate to the logical TP")

    // CRUCIAL invariant: backing-space offsets MUST NOT leak through to the consumer.
    assertNotEquals(leakedBackingHw, partitionData.highWatermark,
      s"backing HW $leakedBackingHw must not leak to the logical consumer")
    assertNotEquals(leakedBackingLogStart, partitionData.logStartOffset,
      s"backing logStartOffset $leakedBackingLogStart must not leak to the logical consumer")
    assertNotEquals(leakedBackingHw, partitionData.lastStableOffset,
      s"backing LSO $leakedBackingHw must not leak to the logical consumer")

    // Positive invariant: offsets must be in the LOGICAL space.
    assertEquals(50L, partitionData.highWatermark,
      "highWatermark must be kernel.nextLogicalOffset for the logical partition")
    assertEquals(0L, partitionData.logStartOffset,
      "logStartOffset must be kernel.startLogicalOffset for the logical partition")
    assertEquals(50L, partitionData.lastStableOffset,
      "v1 non-transactional: LSO == logical HW")
  }

  // Helper for fetch-hook tests: build a MemoryRecords as if it had been produced through the
  // logical-topic stamper, with each record carrying ConcentrationHeaders for `logicalTopic`,
  // `logicalPartition`, and `logicalOffset`. Mirrors the wire shape the fetch hook sees coming
  // off the backing log.
  private def backingRecordsFromInterleaved(records: (String, Int, Long, String, String)*): MemoryRecords = {
    val stamped = new scala.collection.mutable.ArrayBuffer[MemoryRecords]()
    var total = 0
    records.foreach { case (logicalTopic, logicalPartition, logicalOffset, key, value) =>
      val single = MemoryRecords.withRecords(Compression.NONE,
        new SimpleRecord(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8)))
      val s = LogicalProduceStamper.stamp(single, logicalTopic, logicalPartition, Array(logicalOffset))
      stamped += s
      total += s.sizeInBytes()
    }
    val joined = java.nio.ByteBuffer.allocate(total)
    stamped.foreach(mr => joined.put(mr.buffer().duplicate()))
    joined.flip()
    MemoryRecords.readableRecords(joined)
  }

  @Test
  def testProduceResponseContainsNewLeaderOnNotLeaderOrFollower(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2, numBrokers = 3)

    for (version <- 10 to ApiKeys.PRODUCE.latestVersion) {

      reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

      val tp = new TopicPartition(topic, 0)
      val partition = mock(classOf[Partition])
      val newLeaderId = 2
      val newLeaderEpoch = 5

      val produceRequest = ProduceRequest.builder(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
          Collections.singletonList(new ProduceRequestData.TopicProduceData()
            .setName(tp.topic).setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(tp.partition)
              .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("test".getBytes))))))
            .iterator))
        .setAcks(1.toShort)
        .setTimeoutMs(5000))
        .build(version.toShort)
      val request = buildRequest(produceRequest)

      when(replicaManager.handleProduceAppend(anyLong,
        anyShort,
        ArgumentMatchers.eq(false),
        any(),
        any(),
        responseCallback.capture(),
        any(),
        any(),
        any(),
        any())
      ).thenAnswer(_ => responseCallback.getValue.apply(Map(tp -> new PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER))))

      when(replicaManager.getPartitionOrError(tp)).thenAnswer(_ => Right(partition))
      when(partition.leaderReplicaIdOpt).thenAnswer(_ => Some(newLeaderId))
      when(partition.getLeaderEpoch).thenAnswer(_ => newLeaderEpoch)

      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
        any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
      kafkaApis = createKafkaApis()
      kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

      val response = verifyNoThrottling[ProduceResponse](request)

      assertEquals(1, response.data.responses.size)
      val topicProduceResponse = response.data.responses.asScala.head
      assertEquals(1, topicProduceResponse.partitionResponses.size)
      val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, Errors.forCode(partitionProduceResponse.errorCode))
      assertEquals(newLeaderId, partitionProduceResponse.currentLeader.leaderId())
      assertEquals(newLeaderEpoch, partitionProduceResponse.currentLeader.leaderEpoch())
      assertEquals(1, response.data.nodeEndpoints.size)
      val node = response.data.nodeEndpoints.asScala.head
      assertEquals(2, node.nodeId)
      assertEquals("broker2", node.host)
    }
  }

  @Test
  def testProduceResponseReplicaManagerLookupErrorOnNotLeaderOrFollower(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2, numBrokers = 3)

    for (version <- 10 to ApiKeys.PRODUCE.latestVersion) {

      reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

      val tp = new TopicPartition(topic, 0)

      val produceRequest = ProduceRequest.builder(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
          Collections.singletonList(new ProduceRequestData.TopicProduceData()
            .setName(tp.topic).setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(tp.partition)
              .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("test".getBytes))))))
            .iterator))
        .setAcks(1.toShort)
        .setTimeoutMs(5000))
        .build(version.toShort)
      val request = buildRequest(produceRequest)

      when(replicaManager.handleProduceAppend(anyLong,
        anyShort,
        ArgumentMatchers.eq(false),
        any(),
        any(),
        responseCallback.capture(),
        any(),
        any(),
        any(),
        any())
      ).thenAnswer(_ => responseCallback.getValue.apply(Map(tp -> new PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER))))

      when(replicaManager.getPartitionOrError(tp)).thenAnswer(_ => Left(Errors.UNKNOWN_TOPIC_OR_PARTITION))

      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
        any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
      kafkaApis = createKafkaApis()
      kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

      val response = verifyNoThrottling[ProduceResponse](request)

      assertEquals(1, response.data.responses.size)
      val topicProduceResponse = response.data.responses.asScala.head
      assertEquals(1, topicProduceResponse.partitionResponses.size)
      val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, Errors.forCode(partitionProduceResponse.errorCode))
      // LeaderId and epoch should be the same values inserted into the metadata cache
      assertEquals(0, partitionProduceResponse.currentLeader.leaderId())
      assertEquals(1, partitionProduceResponse.currentLeader.leaderEpoch())
      assertEquals(1, response.data.nodeEndpoints.size)
      val node = response.data.nodeEndpoints.asScala.head
      assertEquals(0, node.nodeId)
      assertEquals("broker0", node.host)
    }
  }

  @Test
  def testProduceResponseMetadataLookupErrorOnNotLeaderOrFollower(): Unit = {
    val topic = "topic"
    metadataCache = mock(classOf[KRaftMetadataCache])

    for (version <- 10 to ApiKeys.PRODUCE.latestVersion) {

      reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

      val tp = new TopicPartition(topic, 0)

      val produceRequest = ProduceRequest.builder(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
          Collections.singletonList(new ProduceRequestData.TopicProduceData()
            .setName(tp.topic).setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(tp.partition)
              .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("test".getBytes))))))
            .iterator))
        .setAcks(1.toShort)
        .setTimeoutMs(5000))
        .build(version.toShort)
      val request = buildRequest(produceRequest)

      when(replicaManager.handleProduceAppend(anyLong,
        anyShort,
        ArgumentMatchers.eq(false),
        any(),
        any(),
        responseCallback.capture(),
        any(),
        any(),
        any(),
        any())
      ).thenAnswer(_ => responseCallback.getValue.apply(Map(tp -> new PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER))))

      when(replicaManager.getPartitionOrError(tp)).thenAnswer(_ => Left(Errors.UNKNOWN_TOPIC_OR_PARTITION))

      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
        any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
      when(metadataCache.contains(tp)).thenAnswer(_ => true)
      when(metadataCache.getLeaderAndIsr(tp.topic(), tp.partition())).thenAnswer(_ => Option.empty)
      when(metadataCache.getAliveBrokerNode(any(), any())).thenReturn(Option.empty)
      kafkaApis = createKafkaApis()
      kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

      val response = verifyNoThrottling[ProduceResponse](request)

      assertEquals(1, response.data.responses.size)
      val topicProduceResponse = response.data.responses.asScala.head
      assertEquals(1, topicProduceResponse.partitionResponses.size)
      val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, Errors.forCode(partitionProduceResponse.errorCode))
      assertEquals(-1, partitionProduceResponse.currentLeader.leaderId())
      assertEquals(-1, partitionProduceResponse.currentLeader.leaderEpoch())
      assertEquals(0, response.data.nodeEndpoints.size)
    }
  }

  @Test
  def testTransactionalParametersSetCorrectly(): Unit = {
    val topic = "topic"
    val transactionalId = "txn1"

    addTopicToMetadataCache(topic, numPartitions = 2)

    for (version <- ApiKeys.PRODUCE.oldestVersion to ApiKeys.PRODUCE.latestVersion) {

      reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val tp = new TopicPartition("topic", 0)

      val produceRequest = ProduceRequest.builder(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
          Collections.singletonList(new ProduceRequestData.TopicProduceData()
            .setName(tp.topic).setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(tp.partition)
              .setRecords(MemoryRecords.withTransactionalRecords(Compression.NONE, 0, 0, 0, new SimpleRecord("test".getBytes))))))
            .iterator))
        .setAcks(1.toShort)
        .setTransactionalId(transactionalId)
        .setTimeoutMs(5000))
        .build(version.toShort)
      val request = buildRequest(produceRequest)

      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

        verify(replicaManager).handleProduceAppend(anyLong,
          anyShort,
          ArgumentMatchers.eq(false),
          ArgumentMatchers.eq(transactionalId),
          any(),
          any(),
          any(),
          any(),
          any(),
          any())
      } finally {
        kafkaApis.close()
      }
    }
  }

  @Test
  def testAddPartitionsToTxnWithInvalidPartition(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 1)

    def checkInvalidPartition(invalidPartitionId: Int): Unit = {
      reset(replicaManager, clientRequestQuotaManager, requestChannel)

      val invalidTopicPartition = new TopicPartition(topic, invalidPartitionId)
      val addPartitionsToTxnRequest = AddPartitionsToTxnRequest.Builder.forClient(
        "txnlId", 15L, 0.toShort, List(invalidTopicPartition).asJava
      ).build()
      val request = buildRequest(addPartitionsToTxnRequest)

      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleAddPartitionsToTxnRequest(request, RequestLocal.withThreadConfinedCaching)

        val response = verifyNoThrottling[AddPartitionsToTxnResponse](request)
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID).get(invalidTopicPartition))
      } finally {
        kafkaApis.close()
      }
    }

    checkInvalidPartition(-1)
    checkInvalidPartition(1) // topic has only one partition
  }
  
  @Test
  def requiredAclsNotPresentWriteTxnMarkersThrowsAuthorizationException(): Unit = {
    val topicPartition = new TopicPartition("t", 0)
    val (_, request) = createWriteTxnMarkersRequest(asList(topicPartition))

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val clusterResource = new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL)
    val alterActions = Collections.singletonList(new Action(AclOperation.ALTER, clusterResource, 1, true, false))
    val clusterActions = Collections.singletonList(new Action(AclOperation.CLUSTER_ACTION, clusterResource, 1, true, true))
    val deniedList = Collections.singletonList(AuthorizationResult.DENIED)
    when(authorizer.authorize(
      request.context,
      alterActions
    )).thenReturn(deniedList)
    when(authorizer.authorize(
      request.context,
      clusterActions
    )).thenReturn(deniedList)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))

    assertThrows(classOf[ClusterAuthorizationException],
      () => kafkaApis.handleWriteTxnMarkersRequest(request, RequestLocal.withThreadConfinedCaching))
  }

  @Test
  def shouldRespondWithUnknownTopicWhenPartitionIsNotHosted(): Unit = {
    val topicPartition = new TopicPartition("t", 0)
    val (_, request) = createWriteTxnMarkersRequest(asList(topicPartition))
    val expectedErrors = Map(topicPartition -> Errors.UNKNOWN_TOPIC_OR_PARTITION).asJava
    val capturedResponse: ArgumentCaptor[WriteTxnMarkersResponse] = ArgumentCaptor.forClass(classOf[WriteTxnMarkersResponse])

    when(replicaManager.onlinePartition(topicPartition))
      .thenReturn(None)
    kafkaApis = createKafkaApis()
    kafkaApis.handleWriteTxnMarkersRequest(request, RequestLocal.withThreadConfinedCaching)

    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )
    val markersResponse = capturedResponse.getValue
    assertEquals(expectedErrors, markersResponse.errorsByProducerId.get(1L))
  }

  @Test
  def testWriteTxnMarkersShouldAllBeIncludedInTheResponse(): Unit = {
    // This test verifies the response will not be sent prematurely because of calling replicaManager append
    // with no records.
    val topicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)
    val writeTxnMarkersRequest = new WriteTxnMarkersRequest.Builder(
      asList(
        new TxnMarkerEntry(1, 1.toShort, 0, TransactionResult.COMMIT, asList(topicPartition)),
        new TxnMarkerEntry(2, 1.toShort, 0, TransactionResult.COMMIT, asList(topicPartition)),
      )).build()
    val request = buildRequest(writeTxnMarkersRequest)
    val capturedResponse: ArgumentCaptor[WriteTxnMarkersResponse] = ArgumentCaptor.forClass(classOf[WriteTxnMarkersResponse])

    when(replicaManager.onlinePartition(any()))
      .thenReturn(Some(mock(classOf[Partition])))
    when(groupCoordinator.isNewGroupCoordinator)
      .thenReturn(true)
    when(groupCoordinator.completeTransaction(
      ArgumentMatchers.eq(topicPartition),
      any(),
      ArgumentMatchers.eq(1.toShort),
      ArgumentMatchers.eq(0),
      ArgumentMatchers.eq(TransactionResult.COMMIT),
      any()
    )).thenReturn(CompletableFuture.completedFuture[Void](null))

    kafkaApis = createKafkaApis()
    kafkaApis.handleWriteTxnMarkersRequest(request, RequestLocal.withThreadConfinedCaching)

    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )
    val markersResponse = capturedResponse.getValue
    assertEquals(2, markersResponse.errorsByProducerId.size())
  }
  
  @Test
  def shouldRespondWithUnknownTopicOrPartitionForBadPartitionAndNoErrorsForGoodPartition(): Unit = {
    val tp1 = new TopicPartition("t", 0)
    val tp2 = new TopicPartition("t1", 0)
    val (_, request) = createWriteTxnMarkersRequest(asList(tp1, tp2))
    val expectedErrors = Map(tp1 -> Errors.UNKNOWN_TOPIC_OR_PARTITION, tp2 -> Errors.NONE).asJava

    val capturedResponse: ArgumentCaptor[WriteTxnMarkersResponse] = ArgumentCaptor.forClass(classOf[WriteTxnMarkersResponse])
    val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

    when(replicaManager.onlinePartition(tp1))
      .thenReturn(None)
    when(replicaManager.onlinePartition(tp2))
      .thenReturn(Some(mock(classOf[Partition])))

    val requestLocal = RequestLocal.withThreadConfinedCaching
    when(replicaManager.appendRecords(anyLong,
      anyShort,
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      any(),
      responseCallback.capture(),
      any(),
      any(),
      ArgumentMatchers.eq(requestLocal),
      any(),
      any()
    )).thenAnswer(_ => responseCallback.getValue.apply(Map(tp2 -> new PartitionResponse(Errors.NONE))))
    kafkaApis = createKafkaApis()
    kafkaApis.handleWriteTxnMarkersRequest(request, requestLocal)
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )

    val markersResponse = capturedResponse.getValue
    assertEquals(expectedErrors, markersResponse.errorsByProducerId.get(1L))
  }

  @Test
  def testWriteTxnMarkersRejectsBackingTopicWithUnknownTopicOrPartition(): Unit = {
    // r19 ADV-A BLOCKER #141: defense-in-depth backstop for #140. WriteTxnMarkers is the
    // last point at which a marker could land on a backing partition; if a buggy/compromised
    // peer broker, a stale txn-coordinator state, or any path that bypasses
    // AddPartitionsToTxn slips a backing partition in here, the marker would write a
    // COMMIT/ABORT control record onto the backing log — per PROMPT.md that atomically
    // commits/aborts EVERY co-tenant logical topic interleaved on that partition, silently
    // corrupting cross-tenant transactional state. The handler must reject backing
    // partitions before appendRecords is reached.
    val backingTopic = "backing-topic"
    val backingTp = new TopicPartition(backingTopic, 0)
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)
    // isLogicalTopic intentionally unstubbed (default false) — backing check fires first.

    val (_, request) = createWriteTxnMarkersRequest(asList(backingTp))
    val capturedResponse: ArgumentCaptor[WriteTxnMarkersResponse] =
      ArgumentCaptor.forClass(classOf[WriteTxnMarkersResponse])

    kafkaApis = createKafkaApis()
    kafkaApis.handleWriteTxnMarkersRequest(request, RequestLocal.withThreadConfinedCaching)

    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )
    val markersResponse = capturedResponse.getValue
    val expectedErrors = Map(backingTp -> Errors.UNKNOWN_TOPIC_OR_PARTITION).asJava
    assertEquals(expectedErrors, markersResponse.errorsByProducerId.get(1L),
      "WriteTxnMarkers on a backing topic must be rejected with UNKNOWN_TOPIC_OR_PARTITION " +
        "to prevent cross-tenant COMMIT/ABORT corruption")
    // replicaManager.appendRecords must NEVER be called with the backing partition.
    verify(replicaManager, never()).appendRecords(anyLong, anyShort, any(), any(), any(),
      any(), any(), any(), any(), any(), any())
    // Likewise onlinePartition must not even be consulted — the guard runs first so we
    // never expose backing topics to the marker-append path at any layer.
    verify(replicaManager, never()).onlinePartition(backingTp)
  }

  @Test
  def testWriteTxnMarkersRejectsLogicalTopicWithUnknownTopicOrPartition(): Unit = {
    // r19 ADV-A BLOCKER #141 (logical side): logical topics are non-transactional in v1.
    // produce-side rejects transactional batches at KafkaApis.scala:621 (INVALID_TXN_STATE),
    // AddPartitionsToTxn rejects enrollment at :2872 (#140). If a marker still arrives for a
    // logical partition the upstream guards already failed — this is the final fence. There
    // is no logical-aware LSO tracking, so even attempting to materialise a control record
    // for a logical-named partition is unsafe. Mirror the backing branch with
    // UNKNOWN_TOPIC_OR_PARTITION.
    val logicalTopic = "logical-topic"
    val logicalTp = new TopicPartition(logicalTopic, 0)
    when(concentrationKernel.isBackingTopic(logicalTopic)).thenReturn(false)
    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)

    val (_, request) = createWriteTxnMarkersRequest(asList(logicalTp))
    val capturedResponse: ArgumentCaptor[WriteTxnMarkersResponse] =
      ArgumentCaptor.forClass(classOf[WriteTxnMarkersResponse])

    kafkaApis = createKafkaApis()
    kafkaApis.handleWriteTxnMarkersRequest(request, RequestLocal.withThreadConfinedCaching)

    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )
    val markersResponse = capturedResponse.getValue
    val expectedErrors = Map(logicalTp -> Errors.UNKNOWN_TOPIC_OR_PARTITION).asJava
    assertEquals(expectedErrors, markersResponse.errorsByProducerId.get(1L),
      "WriteTxnMarkers on a logical topic must be rejected with UNKNOWN_TOPIC_OR_PARTITION " +
        "(v1 declares logical topics non-transactional, no logical-aware LSO tracking)")
    verify(replicaManager, never()).appendRecords(anyLong, anyShort, any(), any(), any(),
      any(), any(), any(), any(), any(), any())
    verify(replicaManager, never()).onlinePartition(logicalTp)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("ALTER", "CLUSTER_ACTION"))
  def shouldAppendToLogOnWriteTxnMarkersWhenCorrectMagicVersion(allowedAclOperation: String): Unit = {
    val topicPartition = new TopicPartition("t", 0)
    val request = createWriteTxnMarkersRequest(asList(topicPartition))._2
    when(replicaManager.onlinePartition(topicPartition))
      .thenReturn(Some(mock(classOf[Partition])))

    val requestLocal = RequestLocal.withThreadConfinedCaching

    // Allowing WriteTxnMarkers API with the help of allowedAclOperation parameter.
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val clusterResource = new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL)
    val allowedAction = Collections.singletonList(new Action(
      AclOperation.fromString(allowedAclOperation),
      clusterResource,
      1,
      true,
      allowedAclOperation.equals("CLUSTER_ACTION")
    ))
    val deniedList = Collections.singletonList(AuthorizationResult.DENIED)
    val allowedList = Collections.singletonList(AuthorizationResult.ALLOWED)
    when(authorizer.authorize(
      ArgumentMatchers.eq(request.context),
      any()
    )).thenReturn(deniedList)
    when(authorizer.authorize(
      request.context,
      allowedAction
    )).thenReturn(allowedList)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))

    kafkaApis.handleWriteTxnMarkersRequest(request, requestLocal)
    verify(replicaManager).appendRecords(anyLong,
      anyShort,
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      any(),
      any(),
      any(),
      any(),
      ArgumentMatchers.eq(requestLocal),
      any(),
      any())
  }

  @Test
  def testHandleWriteTxnMarkersRequestWithOldGroupCoordinator(): Unit = {
    val offset0 = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)
    val offset1 = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 1)
    val foo0 = new TopicPartition("foo", 0)
    val foo1 = new TopicPartition("foo", 1)

    val allPartitions = List(
      offset0,
      offset1,
      foo0,
      foo1
    )

    val writeTxnMarkersRequest = new WriteTxnMarkersRequest.Builder(
      List(
        new TxnMarkerEntry(
          1L,
          1.toShort,
          0,
          TransactionResult.COMMIT,
          List(offset0, foo0).asJava
        ),
        new TxnMarkerEntry(
          2L,
          1.toShort,
          0,
          TransactionResult.ABORT,
          List(offset1, foo1).asJava
        )
      ).asJava
    ).build()

    val requestChannelRequest = buildRequest(writeTxnMarkersRequest)

    allPartitions.foreach { tp =>
      when(replicaManager.onlinePartition(tp))
        .thenReturn(Some(mock(classOf[Partition])))
    }

    when(groupCoordinator.onTransactionCompleted(
      ArgumentMatchers.eq(1L),
      ArgumentMatchers.any(),
      ArgumentMatchers.eq(TransactionResult.COMMIT)
    )).thenReturn(CompletableFuture.completedFuture[Void](null))

    when(groupCoordinator.onTransactionCompleted(
      ArgumentMatchers.eq(2L),
      ArgumentMatchers.any(),
      ArgumentMatchers.eq(TransactionResult.ABORT)
    )).thenReturn(FutureUtils.failedFuture[Void](Errors.NOT_CONTROLLER.exception))

    val entriesPerPartition: ArgumentCaptor[Map[TopicPartition, MemoryRecords]] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, MemoryRecords]])
    val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

    when(replicaManager.appendRecords(
      ArgumentMatchers.eq(ServerConfigs.REQUEST_TIMEOUT_MS_DEFAULT.toLong),
      ArgumentMatchers.eq(-1),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      entriesPerPartition.capture(),
      responseCallback.capture(),
      any(),
      any(),
      ArgumentMatchers.eq(RequestLocal.noCaching()),
      any(),
      any()
    )).thenAnswer { _ =>
      responseCallback.getValue.apply(
        entriesPerPartition.getValue.keySet.map { tp =>
          tp -> new PartitionResponse(Errors.NONE)
        }.toMap
      )
    }
    kafkaApis = createKafkaApis(overrideProperties = Map(
      GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG -> "false"
    ))
    kafkaApis.handleWriteTxnMarkersRequest(requestChannelRequest, RequestLocal.noCaching())

    val expectedResponse = new WriteTxnMarkersResponseData()
      .setMarkers(List(
        new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
          .setProducerId(1L)
          .setTopics(List(
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName(Topic.GROUP_METADATA_TOPIC_NAME)
              .setPartitions(List(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(0)
                  .setErrorCode(Errors.NONE.code)
              ).asJava),
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName("foo")
              .setPartitions(List(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(0)
                  .setErrorCode(Errors.NONE.code)
              ).asJava)
          ).asJava),
        new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
          .setProducerId(2L)
          .setTopics(List(
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName(Topic.GROUP_METADATA_TOPIC_NAME)
              .setPartitions(List(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(1)
                  .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
              ).asJava),
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName("foo")
              .setPartitions(List(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(1)
                  .setErrorCode(Errors.NONE.code)
              ).asJava)
          ).asJava)
      ).asJava)

    val response = verifyNoThrottling[WriteTxnMarkersResponse](requestChannelRequest)
    assertEquals(normalize(expectedResponse), normalize(response.data))
  }

  @Test
  def testHandleWriteTxnMarkersRequestWithNewGroupCoordinator(): Unit = {
    val offset0 = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)
    val offset1 = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 1)
    val foo0 = new TopicPartition("foo", 0)
    val foo1 = new TopicPartition("foo", 1)

    val allPartitions = List(
      offset0,
      offset1,
      foo0,
      foo1
    )

    val writeTxnMarkersRequest = new WriteTxnMarkersRequest.Builder(
      List(
        new TxnMarkerEntry(
          1L,
          1.toShort,
          0,
          TransactionResult.COMMIT,
          List(offset0, foo0).asJava
        ),
        new TxnMarkerEntry(
          2L,
          1.toShort,
          0,
          TransactionResult.ABORT,
          List(offset1, foo1).asJava
        )
      ).asJava
    ).build()

    val requestChannelRequest = buildRequest(writeTxnMarkersRequest)

    allPartitions.foreach { tp =>
      when(replicaManager.onlinePartition(tp))
        .thenReturn(Some(mock(classOf[Partition])))
    }

    when(groupCoordinator.completeTransaction(
      ArgumentMatchers.eq(offset0),
      ArgumentMatchers.eq(1L),
      ArgumentMatchers.eq(1.toShort),
      ArgumentMatchers.eq(0),
      ArgumentMatchers.eq(TransactionResult.COMMIT),
      ArgumentMatchers.eq(Duration.ofMillis(ServerConfigs.REQUEST_TIMEOUT_MS_DEFAULT))
    )).thenReturn(CompletableFuture.completedFuture[Void](null))

    when(groupCoordinator.completeTransaction(
      ArgumentMatchers.eq(offset1),
      ArgumentMatchers.eq(2L),
      ArgumentMatchers.eq(1.toShort),
      ArgumentMatchers.eq(0),
      ArgumentMatchers.eq(TransactionResult.ABORT),
      ArgumentMatchers.eq(Duration.ofMillis(ServerConfigs.REQUEST_TIMEOUT_MS_DEFAULT))
    )).thenReturn(CompletableFuture.completedFuture[Void](null))

    val entriesPerPartition: ArgumentCaptor[Map[TopicPartition, MemoryRecords]] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, MemoryRecords]])
    val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

    when(replicaManager.appendRecords(
      ArgumentMatchers.eq(ServerConfigs.REQUEST_TIMEOUT_MS_DEFAULT.toLong),
      ArgumentMatchers.eq(-1),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      entriesPerPartition.capture(),
      responseCallback.capture(),
      any(),
      any(),
      ArgumentMatchers.eq(RequestLocal.noCaching),
      any(),
      any()
    )).thenAnswer { _ =>
      responseCallback.getValue.apply(
        entriesPerPartition.getValue.keySet.map { tp =>
          tp -> new PartitionResponse(Errors.NONE)
        }.toMap
      )
    }
    kafkaApis = createKafkaApis()
    kafkaApis.handleWriteTxnMarkersRequest(requestChannelRequest, RequestLocal.noCaching)

    val expectedResponse = new WriteTxnMarkersResponseData()
      .setMarkers(List(
        new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
          .setProducerId(1L)
          .setTopics(List(
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName(Topic.GROUP_METADATA_TOPIC_NAME)
              .setPartitions(List(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(0)
                  .setErrorCode(Errors.NONE.code)
              ).asJava),
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName("foo")
              .setPartitions(List(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(0)
                  .setErrorCode(Errors.NONE.code)
              ).asJava)
          ).asJava),
        new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
          .setProducerId(2L)
          .setTopics(List(
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName(Topic.GROUP_METADATA_TOPIC_NAME)
              .setPartitions(List(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(1)
                  .setErrorCode(Errors.NONE.code)
              ).asJava),
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName("foo")
              .setPartitions(List(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(1)
                  .setErrorCode(Errors.NONE.code)
              ).asJava)
          ).asJava)
      ).asJava)

    val response = verifyNoThrottling[WriteTxnMarkersResponse](requestChannelRequest)
    assertEquals(normalize(expectedResponse), normalize(response.data))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[Errors], names = Array(
    "COORDINATOR_NOT_AVAILABLE",
    "COORDINATOR_LOAD_IN_PROGRESS",
    "NOT_COORDINATOR",
    "REQUEST_TIMED_OUT"
  ))
  def testHandleWriteTxnMarkersRequestWithNewGroupCoordinatorErrorTranslation(error: Errors): Unit = {
    val offset0 = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)

    val writeTxnMarkersRequest = new WriteTxnMarkersRequest.Builder(
      List(
        new TxnMarkerEntry(
          1L,
          1.toShort,
          0,
          TransactionResult.COMMIT,
          List(offset0).asJava
        )
      ).asJava
    ).build()

    val requestChannelRequest = buildRequest(writeTxnMarkersRequest)

    when(replicaManager.onlinePartition(offset0))
      .thenReturn(Some(mock(classOf[Partition])))

    when(groupCoordinator.completeTransaction(
      ArgumentMatchers.eq(offset0),
      ArgumentMatchers.eq(1L),
      ArgumentMatchers.eq(1.toShort),
      ArgumentMatchers.eq(0),
      ArgumentMatchers.eq(TransactionResult.COMMIT),
      ArgumentMatchers.eq(Duration.ofMillis(ServerConfigs.REQUEST_TIMEOUT_MS_DEFAULT))
    )).thenReturn(FutureUtils.failedFuture[Void](error.exception()))
    kafkaApis = createKafkaApis()
    kafkaApis.handleWriteTxnMarkersRequest(requestChannelRequest, RequestLocal.noCaching)

    val expectedError = error match {
      case Errors.COORDINATOR_NOT_AVAILABLE | Errors.COORDINATOR_LOAD_IN_PROGRESS | Errors.NOT_COORDINATOR =>
        Errors.NOT_LEADER_OR_FOLLOWER
      case error =>
        error
    }

    val expectedResponse = new WriteTxnMarkersResponseData()
      .setMarkers(List(
        new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
          .setProducerId(1L)
          .setTopics(List(
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName(Topic.GROUP_METADATA_TOPIC_NAME)
              .setPartitions(List(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(0)
                  .setErrorCode(expectedError.code)
              ).asJava)
          ).asJava)
      ).asJava)

    val response = verifyNoThrottling[WriteTxnMarkersResponse](requestChannelRequest)
    assertEquals(normalize(expectedResponse), normalize(response.data))
  }

  private def normalize(
    response: WriteTxnMarkersResponseData
  ): WriteTxnMarkersResponseData = {
    val copy = response.duplicate()
    copy.markers.sort(
      Comparator.comparingLong[WriteTxnMarkersResponseData.WritableTxnMarkerResult](_.producerId)
    )
    copy.markers.forEach { marker =>
      marker.topics.sort((t1, t2) => t1.name.compareTo(t2.name))
      marker.topics.forEach { topic =>
        topic.partitions.sort(
          Comparator.comparingInt[WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult](_.partitionIndex)
        )
      }
    }
    copy
  }

  @Test
  def testListOffsetsOnLogicalTopicReturnsKernelLogicalOffsets(): Unit = {
    // Concentration ListOffsets contract (Codex HIGH): a stock consumer calling seekToBeginning
    // / seekToEnd against a logical topic must receive offsets in LOGICAL space. Returning the
    // backing offset — which is what handleListOffsetRequest would do without kernel awareness
    // — would either mis-seek the consumer (silent wrong-record reads) or trigger an immediate
    // OFFSET_OUT_OF_RANGE on the next fetch (the backing offset is invalid in logical-offset
    // space). The hook short-circuits ReplicaManager and answers from kernel state directly.
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    addTopicToMetadataCache(backingTopic, numPartitions = 4)

    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))
    when(concentrationKernel.isBackingReady(any[TopicPartition])).thenReturn(true)
    stubBackingPartitionAsLeader()
    // Pin three distinct values so the assertion proves we forwarded the EARLIEST→start and
    // LATEST→next routing rather than transposing them (a transposition would still pass any
    // "non-zero" check).
    when(concentrationKernel.startLogicalOffset(logicalTopic, 0)).thenReturn(42L)
    when(concentrationKernel.nextLogicalOffset(logicalTopic, 0)).thenReturn(1_000L)
    when(concentrationKernel.startLogicalOffset(logicalTopic, 7)).thenReturn(7L)

    val targetTimes = List(new ListOffsetsTopic()
      .setName(logicalTopic)
      .setPartitions(List(
        new ListOffsetsPartition().setPartitionIndex(0).setTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP),
        new ListOffsetsPartition().setPartitionIndex(0).setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP),
        new ListOffsetsPartition().setPartitionIndex(7).setTimestamp(ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP)
      ).asJava)).asJava
    val listOffsetRequest = ListOffsetsRequest.Builder.forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
      .setTargetTimes(targetTimes).build()
    val request = buildRequest(listOffsetRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis()
    kafkaApis.handleListOffsetRequest(request)

    val response = verifyNoThrottling[ListOffsetsResponse](request)
    val topicResp = response.topics.asScala.find(_.name == logicalTopic).get
    val byTimestampAndPart = topicResp.partitions.asScala
      .map(p => (p.partitionIndex, p.offset))
      .toList
    // Order of (index, offset) entries reflects the order of partitions in the request.
    assertEquals(List((0, 42L), (0, 1_000L), (7, 7L)), byTimestampAndPart,
      "EARLIEST/EARLIEST_LOCAL must resolve to kernel.startLogicalOffset and LATEST to " +
      "kernel.nextLogicalOffset — leaking backing offsets would mis-seek stock consumers")
    topicResp.partitions.asScala.foreach { p =>
      assertEquals(Errors.NONE.code, p.errorCode)
    }

    // ReplicaManager must never see logical-topic requests — they're answered from kernel state.
    verify(replicaManager, never()).fetchOffset(any(), any(), any(), anyInt, any(), anyInt, anyShort,
      any(), any(), anyInt)
  }

  @Test
  def testListOffsetsOnLogicalTopicRejectsTimestampLookupAsUnsupported(): Unit = {
    // Timestamp -> logical-offset reverse lookup needs a sidecar scan that v1 does not maintain.
    // The hook must refuse with a non-retriable error rather than return the backing offset
    // (which would silently mis-seek). UNSUPPORTED_FOR_MESSAGE_FORMAT is non-retriable and signals
    // "valid request, just not supported for this topic in this version" — the right shape for
    // a feature-gated rejection.
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    addTopicToMetadataCache(backingTopic, numPartitions = 4)

    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))
    when(concentrationKernel.isBackingReady(any[TopicPartition])).thenReturn(true)
    stubBackingPartitionAsLeader()

    val targetTimes = List(new ListOffsetsTopic()
      .setName(logicalTopic)
      .setPartitions(List(
        new ListOffsetsPartition().setPartitionIndex(0).setTimestamp(ListOffsetsRequest.MAX_TIMESTAMP),
        new ListOffsetsPartition().setPartitionIndex(0).setTimestamp(ListOffsetsRequest.LATEST_TIERED_TIMESTAMP),
        new ListOffsetsPartition().setPartitionIndex(0).setTimestamp(12345L)
      ).asJava)).asJava
    val listOffsetRequest = ListOffsetsRequest.Builder.forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
      .setTargetTimes(targetTimes).build()
    val request = buildRequest(listOffsetRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis()
    kafkaApis.handleListOffsetRequest(request)

    val response = verifyNoThrottling[ListOffsetsResponse](request)
    val topicResp = response.topics.asScala.find(_.name == logicalTopic).get
    topicResp.partitions.asScala.foreach { p =>
      assertEquals(Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT.code, p.errorCode,
        "timestamp lookups and tiered-timestamp lookups must be refused — v1 maintains no " +
        "timestamp -> logical-offset reverse index, returning the backing offset would mis-seek")
      assertEquals(ListOffsetsResponse.UNKNOWN_OFFSET, p.offset)
    }
  }

  @Test
  def testListOffsetsOnLogicalTopicRejectsOutOfRangePartitionAsUnknownTopicOrPartition(): Unit = {
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    addTopicToMetadataCache(backingTopic, numPartitions = 4)

    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))

    val targetTimes = List(new ListOffsetsTopic()
      .setName(logicalTopic)
      .setPartitions(List(
        new ListOffsetsPartition().setPartitionIndex(2048).setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
      ).asJava)).asJava
    val listOffsetRequest = ListOffsetsRequest.Builder.forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
      .setTargetTimes(targetTimes).build()
    val request = buildRequest(listOffsetRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis()
    kafkaApis.handleListOffsetRequest(request)

    val response = verifyNoThrottling[ListOffsetsResponse](request)
    val partitionData = response.topics.asScala.find(_.name == logicalTopic).get
      .partitions.asScala.head
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code, partitionData.errorCode,
      "out-of-range logical partition must surface as a client routing bug — not as a kernel " +
      "exception leaked through ReplicaManager")
  }

  @Test
  def testListOffsetsOnBackingTopicReturnsInvalidTopicException(): Unit = {
    // Symmetric with the produce path: a stock client must never address the backing topic by
    // name. ListOffsets on a backing topic would return offsets that interleave records from
    // every logical topic mapped to that backing — meaningless to any single consumer.
    val backingTopic = "concentrated"
    addTopicToMetadataCache(backingTopic, numPartitions = 4)
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)

    val targetTimes = List(new ListOffsetsTopic()
      .setName(backingTopic)
      .setPartitions(List(
        new ListOffsetsPartition().setPartitionIndex(0).setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
      ).asJava)).asJava
    val listOffsetRequest = ListOffsetsRequest.Builder.forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
      .setTargetTimes(targetTimes).build()
    val request = buildRequest(listOffsetRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis()
    kafkaApis.handleListOffsetRequest(request)

    val response = verifyNoThrottling[ListOffsetsResponse](request)
    val partitionData = response.topics.asScala.find(_.name == backingTopic).get
      .partitions.asScala.head
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, partitionData.errorCode)

    verify(replicaManager, never()).fetchOffset(any(), any(), any(), anyInt, any(), anyInt, anyShort,
      any(), any(), anyInt)
  }

  @Test
  def testOffsetForLeaderEpochOnLogicalTopicSynthesizesLogicalLeoAndBackingEpoch(): Unit = {
    // r16 BLOCKER #93-A. ReplicaManager.lastOffsetForLeaderEpoch returns UNKNOWN_TOPIC_OR_PARTITION
    // for logical topics (they're not in metadataCache). The consumer's KIP-320 truncation-detection
    // utility (OffsetsForLeaderEpochUtils) does NOT remove the partition from partitionsToRetry on
    // UNKNOWN — the consumer spins forever and can't complete fetch validation. The handler must
    // synthesize a response from kernel state. In v1 logical offsets are append-only at the logical
    // layer, so the honest answer is "no truncation up to current logical LEO under current backing
    // leader epoch" — endOffset >= clientOffset always holds and the consumer's check passes.
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    addTopicToMetadataCache(backingTopic, numPartitions = 4)

    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))
    when(concentrationKernel.backingPartitionFor(logicalTopic, 0)).thenReturn(0)
    when(concentrationKernel.isBackingReady(any[TopicPartition])).thenReturn(true)
    when(concentrationKernel.nextLogicalOffset(logicalTopic, 0)).thenReturn(7_500L)

    val backingPartition = mock(classOf[Partition])
    when(backingPartition.isLeader).thenReturn(true)
    when(backingPartition.getLeaderEpoch).thenReturn(11)
    when(replicaManager.onlinePartition(any[TopicPartition])).thenReturn(Some(backingPartition))

    val topic = new OffsetForLeaderTopic()
      .setTopic(logicalTopic)
      .setPartitions(List(
        new OffsetForLeaderPartition()
          .setPartition(0)
          .setLeaderEpoch(5)
          .setCurrentLeaderEpoch(11)
      ).asJava)
    val request = buildRequest(OffsetsForLeaderEpochRequest.Builder.forConsumer(
      new OffsetForLeaderTopicCollection(List(topic).iterator.asJava)).build())
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetForLeaderEpochRequest(request)

    val response = verifyNoThrottling[OffsetsForLeaderEpochResponse](request)
    val topicResp = response.data.topics.asScala.find(_.topic == logicalTopic).get
    val partitionResp = topicResp.partitions.asScala.head
    assertEquals(Errors.NONE.code, partitionResp.errorCode)
    assertEquals(0, partitionResp.partition)
    assertEquals(11, partitionResp.leaderEpoch,
      "logical synthesis must surface backing partition's current leader epoch — UNKNOWN_LEADER_EPOCH " +
      "default (-1) would mis-route the consumer through retry instead of completing validation")
    assertEquals(7_500L, partitionResp.endOffset,
      "endOffset must be the logical LEO (kernel.nextLogicalOffset), NOT the backing log's end — " +
      "a backing offset would make the consumer think it has missed records (endOffset < its position)")

    // ReplicaManager must NEVER receive a logical-topic OFLE; the kernel is the source of truth.
    verify(replicaManager, never()).lastOffsetForLeaderEpoch(any())
  }

  @Test
  def testOffsetForLeaderEpochOnLogicalTopicWhenBackingNotReadyReturnsNotLeaderOrFollower(): Unit = {
    // Same readiness gate as Produce/Fetch/ListOffsets. Returning the kernel's tracker state while
    // it's mid-rebuild (leader-loss or first acquisition) would expose stale offsets — the consumer
    // would compare its known offset against a stale LEO and either think no truncation occurred
    // when it did (silent wrong-data on next fetch) or reset to a stale offset.
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    addTopicToMetadataCache(backingTopic, numPartitions = 4)

    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))
    when(concentrationKernel.backingPartitionFor(logicalTopic, 0)).thenReturn(0)
    when(concentrationKernel.isBackingReady(any[TopicPartition])).thenReturn(false)

    val backingPartition = mock(classOf[Partition])
    when(backingPartition.isLeader).thenReturn(true)
    when(replicaManager.onlinePartition(any[TopicPartition])).thenReturn(Some(backingPartition))

    val topic = new OffsetForLeaderTopic()
      .setTopic(logicalTopic)
      .setPartitions(List(new OffsetForLeaderPartition().setPartition(0).setLeaderEpoch(5)).asJava)
    val request = buildRequest(OffsetsForLeaderEpochRequest.Builder.forConsumer(
      new OffsetForLeaderTopicCollection(List(topic).iterator.asJava)).build())
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetForLeaderEpochRequest(request)

    val response = verifyNoThrottling[OffsetsForLeaderEpochResponse](request)
    val partitionResp = response.data.topics.asScala.find(_.topic == logicalTopic).get
      .partitions.asScala.head
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, partitionResp.errorCode)
    // Default LEO/epoch on error path — consumer ignores these on NOT_LEADER, but be explicit.
    verify(concentrationKernel, never()).nextLogicalOffset(any[String], anyInt)
  }

  @Test
  def testOffsetForLeaderEpochOnLogicalTopicWhenNotLeaderReturnsNotLeaderOrFollower(): Unit = {
    // Same leader-only contract as Fetch (Codex Q5) and ListOffsets. A non-leader broker's kernel
    // tracker has never observed the produces (only the leader updates it), so it would return
    // logical LEO = 0 — making any consumer past offset 0 think every record after the first was
    // truncated. NOT_LEADER_OR_FOLLOWER routes the consumer through metadata refresh.
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    addTopicToMetadataCache(backingTopic, numPartitions = 4)

    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))
    when(concentrationKernel.backingPartitionFor(logicalTopic, 0)).thenReturn(0)
    val backingPartition = mock(classOf[Partition])
    when(backingPartition.isLeader).thenReturn(false)
    when(replicaManager.onlinePartition(any[TopicPartition])).thenReturn(Some(backingPartition))

    val topic = new OffsetForLeaderTopic()
      .setTopic(logicalTopic)
      .setPartitions(List(new OffsetForLeaderPartition().setPartition(0).setLeaderEpoch(5)).asJava)
    val request = buildRequest(OffsetsForLeaderEpochRequest.Builder.forConsumer(
      new OffsetForLeaderTopicCollection(List(topic).iterator.asJava)).build())
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetForLeaderEpochRequest(request)

    val response = verifyNoThrottling[OffsetsForLeaderEpochResponse](request)
    val partitionResp = response.data.topics.asScala.find(_.topic == logicalTopic).get
      .partitions.asScala.head
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, partitionResp.errorCode)
    verify(concentrationKernel, never()).nextLogicalOffset(any[String], anyInt)
  }

  @Test
  def testOffsetForLeaderEpochOnLogicalTopicOutOfRangePartitionReturnsUnknownTopicOrPartition(): Unit = {
    val logicalTopic = "orders"
    val backingTopic = "concentrated"
    val descriptor = new LogicalTopicDescriptor(logicalTopic, 1024, backingTopic, 4)
    addTopicToMetadataCache(backingTopic, numPartitions = 4)

    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.describe(logicalTopic)).thenReturn(Optional.of(descriptor))

    val topic = new OffsetForLeaderTopic()
      .setTopic(logicalTopic)
      .setPartitions(List(new OffsetForLeaderPartition().setPartition(2048).setLeaderEpoch(0)).asJava)
    val request = buildRequest(OffsetsForLeaderEpochRequest.Builder.forConsumer(
      new OffsetForLeaderTopicCollection(List(topic).iterator.asJava)).build())
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetForLeaderEpochRequest(request)

    val response = verifyNoThrottling[OffsetsForLeaderEpochResponse](request)
    val partitionResp = response.data.topics.asScala.find(_.topic == logicalTopic).get
      .partitions.asScala.head
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code, partitionResp.errorCode,
      "out-of-range logical partition must surface as UNKNOWN, NOT leak a kernel ArrayIndex through")
  }

  @Test
  def testOffsetForLeaderEpochRejectsBackingTopicForExternalConsumer(): Unit = {
    // r22 BLOCKER #189 — KIP-320 OffsetsForLeaderEpoch is used by *consumers* for log-truncation
    // detection (TOPIC.DESCRIBE auth path), and the handler responds with the BACKING partition's
    // leader epoch + end offset. Without this guard, a consumer principal authorized on the
    // raw backing-topic NAME would learn the substrate's leader epoch and physical LEO, which
    // are the union of every co-tenant's logical write activity — a direct cross-tenant
    // truncation/throughput oracle. The handler must surface INVALID_TOPIC_EXCEPTION for backing
    // topics on the external (non-CLUSTER_ACTION) auth path and NEVER invoke ReplicaManager for
    // them. Inter-broker callers retain access (covered by testOffsetForLeaderEpochInterBroker...).
    val backingTopic = "backing-topic-r22-189"
    val plainTopic = "tenant-topic-r22-189"
    addTopicToMetadataCache(backingTopic, numPartitions = 8)
    addTopicToMetadataCache(plainTopic, numPartitions = 2)
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)
    when(concentrationKernel.isBackingTopic(plainTopic)).thenReturn(false)
    when(concentrationKernel.isLogicalTopic(any[String])).thenReturn(false)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    // CLUSTER_ACTION on CLUSTER → DENIED (external consumer path)
    val clusterAction = new Action(AclOperation.CLUSTER_ACTION,
      new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL),
      1, true, false)
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(List(clusterAction).asJava)))
      .thenReturn(List(AuthorizationResult.DENIED).asJava)
    // DESCRIBE on TOPIC for both → ALLOWED (the consumer happens to have DESCRIBE on the backing
    // name — that's the threat: descriptive auth alone must not unlock the substrate oracle).
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.argThat[util.List[Action]] {
      (a: util.List[Action]) => a != null && a.size == 2 &&
        a.asScala.forall(act => act.operation == AclOperation.DESCRIBE &&
          act.resourcePattern.resourceType == ResourceType.TOPIC)
    })).thenReturn(List(AuthorizationResult.ALLOWED, AuthorizationResult.ALLOWED).asJava)

    val topics = new OffsetForLeaderTopicCollection(List(
      new OffsetForLeaderTopic()
        .setTopic(backingTopic)
        .setPartitions(List(
          new OffsetForLeaderPartition().setPartition(0).setLeaderEpoch(3),
          new OffsetForLeaderPartition().setPartition(1).setLeaderEpoch(3)).asJava),
      new OffsetForLeaderTopic()
        .setTopic(plainTopic)
        .setPartitions(List(
          new OffsetForLeaderPartition().setPartition(0).setLeaderEpoch(7)).asJava)
    ).iterator.asJava)
    val request = buildRequest(OffsetsForLeaderEpochRequest.Builder.forConsumer(topics).build())
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    // ReplicaManager will be called only for plainTopic; stub a NONE response so the
    // non-backing branch produces a normal result.
    val plainResult = new OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult()
      .setTopic(plainTopic)
      .setPartitions(List(
        new OffsetForLeaderEpochResponseData.EpochEndOffset()
          .setPartition(0).setErrorCode(Errors.NONE.code).setLeaderEpoch(7).setEndOffset(42L)
      ).asJava)
    when(replicaManager.lastOffsetForLeaderEpoch(any[Seq[OffsetForLeaderTopic]]))
      .thenReturn(Seq(plainResult))

    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleOffsetForLeaderEpochRequest(request)

    val response = verifyNoThrottling[OffsetsForLeaderEpochResponse](request)
    val backingResp = response.data.topics.asScala.find(_.topic == backingTopic).getOrElse(
      fail("Backing topic must appear in response with rejection error").asInstanceOf[Nothing])
    assertEquals(2, backingResp.partitions.size,
      "All backing partitions in the request must be reflected back, each with the rejection error")
    backingResp.partitions.asScala.foreach { p =>
      assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, p.errorCode,
        s"Backing partition ${p.partition} must be rejected with INVALID_TOPIC_EXCEPTION — " +
          "exposing backing leader-epoch/LEO would leak cross-tenant write activity")
      assertEquals(-1, p.leaderEpoch,
        "leaderEpoch must NOT be populated on rejection (default -1) — any populated value " +
          "would be a substrate-side leak")
      assertEquals(-1L, p.endOffset,
        "endOffset must NOT be populated on rejection (default -1) — populated end offset would " +
          "be the very physical LEO oracle this fix exists to prevent")
    }
    val plainResp = response.data.topics.asScala.find(_.topic == plainTopic).getOrElse(
      fail("Non-backing topic must be forwarded to ReplicaManager and appear in response")
        .asInstanceOf[Nothing])
    assertEquals(Errors.NONE.code, plainResp.partitions.asScala.head.errorCode)
    assertEquals(42L, plainResp.partitions.asScala.head.endOffset,
      "Non-backing topic must surface ReplicaManager's real end offset unchanged")

    // ReplicaManager MUST NOT have been asked for the backing topic. The lastOffsetForLeaderEpoch
    // call (if any) should have been invoked with a Seq containing only the plain topic.
    val captor = ArgumentCaptor.forClass(classOf[Seq[OffsetForLeaderTopic]])
    verify(replicaManager).lastOffsetForLeaderEpoch(captor.capture())
    val forwarded = captor.getValue
    assertTrue(forwarded.forall(_.topic != backingTopic),
      "Backing topic MUST NOT be forwarded to ReplicaManager.lastOffsetForLeaderEpoch — that " +
        "call would return the substrate's real leader-epoch end offset and leak co-tenant " +
        "write activity")
    assertTrue(forwarded.exists(_.topic == plainTopic),
      "Non-backing topic MUST be forwarded to ReplicaManager.lastOffsetForLeaderEpoch")
  }

  @Test
  def testOffsetForLeaderEpochUnauthorizedBackingProbeReturnsAuthorizationFailedNotInvalidTopic(): Unit = {
    // r22 BLOCKER #189 — auth-first / shadow-second precedence. A principal lacking DESCRIBE on
    // a backing topic name MUST see TOPIC_AUTHORIZATION_FAILED, NOT INVALID_TOPIC_EXCEPTION.
    // The latter would let an unauthorized principal enumerate the declared-backing set by
    // probing names and observing the response-code difference (existing-but-rejected vs
    // unauthorized). Same precedence pattern as #128/#137/#146/#159/#205.
    val backingTopic = "backing-topic-r22-189-authfail"
    addTopicToMetadataCache(backingTopic, numPartitions = 8)
    // isBackingTopic must NEVER be consulted on this path — auth catches the probe first.
    // We do NOT stub it; mockito's default-false return is sufficient AND it lets us assert
    // verify(never()) below.

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val clusterAction = new Action(AclOperation.CLUSTER_ACTION,
      new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL),
      1, true, false)
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(List(clusterAction).asJava)))
      .thenReturn(List(AuthorizationResult.DENIED).asJava)
    // DESCRIBE on backing topic → DENIED
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.argThat[util.List[Action]] {
      (a: util.List[Action]) => a != null && a.size == 1 &&
        a.get(0).operation == AclOperation.DESCRIBE &&
        a.get(0).resourcePattern.resourceType == ResourceType.TOPIC &&
        a.get(0).resourcePattern.name == backingTopic
    })).thenReturn(List(AuthorizationResult.DENIED).asJava)

    val topics = new OffsetForLeaderTopicCollection(List(
      new OffsetForLeaderTopic()
        .setTopic(backingTopic)
        .setPartitions(List(
          new OffsetForLeaderPartition().setPartition(0).setLeaderEpoch(3)).asJava)
    ).iterator.asJava)
    val request = buildRequest(OffsetsForLeaderEpochRequest.Builder.forConsumer(topics).build())
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleOffsetForLeaderEpochRequest(request)

    val response = verifyNoThrottling[OffsetsForLeaderEpochResponse](request)
    val partitionResp = response.data.topics.asScala.find(_.topic == backingTopic).get
      .partitions.asScala.head
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, partitionResp.errorCode,
      "Auth-first precedence — unauthorized backing probe must see TOPIC_AUTHORIZATION_FAILED, " +
        "not INVALID_TOPIC_EXCEPTION (which would leak the declared-backing set)")
    // The backing predicate must NOT have been consulted — the unauthorized path catches the
    // probe before backing filtering runs. If isBackingTopic were called here, an attacker who
    // can also issue allowed probes could time-side-channel the predicate cost.
    verify(concentrationKernel, never()).isBackingTopic(backingTopic)
    // ReplicaManager must never see the unauthorized topic either.
    verify(replicaManager, never()).lastOffsetForLeaderEpoch(any[Seq[OffsetForLeaderTopic]])
  }

  @Test
  def testOffsetForLeaderEpochInterBrokerPathStillSeesBackingTopics(): Unit = {
    // r22 BLOCKER #189 — the filter MUST be gated on the external auth path so inter-broker
    // replication (CLUSTER_ACTION) continues to receive truthful leader-epoch end offsets for
    // backing partitions. Without this, follower replicas could not run KIP-279 truncation
    // detection against the backing log and the cluster would lose replication safety.
    val backingTopic = "backing-topic-r22-189-interbroker"
    addTopicToMetadataCache(backingTopic, numPartitions = 8)
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)
    when(concentrationKernel.isLogicalTopic(any[String])).thenReturn(false)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val clusterAction = new Action(AclOperation.CLUSTER_ACTION,
      new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL),
      1, true, false)
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(List(clusterAction).asJava)))
      .thenReturn(List(AuthorizationResult.ALLOWED).asJava)

    val topics = new OffsetForLeaderTopicCollection(List(
      new OffsetForLeaderTopic()
        .setTopic(backingTopic)
        .setPartitions(List(
          new OffsetForLeaderPartition().setPartition(0).setLeaderEpoch(3)).asJava)
    ).iterator.asJava)
    val request = buildRequest(OffsetsForLeaderEpochRequest.Builder.forFollower(
      topics, brokerId).build())
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    val backingResult = new OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult()
      .setTopic(backingTopic)
      .setPartitions(List(
        new OffsetForLeaderEpochResponseData.EpochEndOffset()
          .setPartition(0).setErrorCode(Errors.NONE.code).setLeaderEpoch(3).setEndOffset(999L)
      ).asJava)
    when(replicaManager.lastOffsetForLeaderEpoch(any[Seq[OffsetForLeaderTopic]]))
      .thenReturn(Seq(backingResult))

    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleOffsetForLeaderEpochRequest(request)

    val response = verifyNoThrottling[OffsetsForLeaderEpochResponse](request)
    val partitionResp = response.data.topics.asScala.find(_.topic == backingTopic).get
      .partitions.asScala.head
    assertEquals(Errors.NONE.code, partitionResp.errorCode,
      "Inter-broker (CLUSTER_ACTION) caller must receive truthful leader-epoch end offset for " +
        "backing partition — replication safety depends on it")
    assertEquals(999L, partitionResp.endOffset,
      "Inter-broker caller's end offset must come straight from ReplicaManager, unaltered")
    // The backing-rejection filter MUST be bypassed for CLUSTER_ACTION — isBackingTopic should
    // never be invoked on this path (we partitioned on clusterAuthorized first).
    verify(concentrationKernel, never()).isBackingTopic(backingTopic)
    // ReplicaManager MUST be asked for the backing topic on the inter-broker path.
    val captor = ArgumentCaptor.forClass(classOf[Seq[OffsetForLeaderTopic]])
    verify(replicaManager).lastOffsetForLeaderEpoch(captor.capture())
    assertTrue(captor.getValue.exists(_.topic == backingTopic),
      "Inter-broker path MUST forward backing topic to ReplicaManager.lastOffsetForLeaderEpoch")
  }

  @Test
  def testLeaderReplicaIfLocalRaisesFencedLeaderEpoch(): Unit = {
    testListOffsetFailedGetLeaderReplica(Errors.FENCED_LEADER_EPOCH)
  }

  @Test
  def testLeaderReplicaIfLocalRaisesUnknownLeaderEpoch(): Unit = {
    testListOffsetFailedGetLeaderReplica(Errors.UNKNOWN_LEADER_EPOCH)
  }

  @Test
  def testLeaderReplicaIfLocalRaisesNotLeaderOrFollower(): Unit = {
    testListOffsetFailedGetLeaderReplica(Errors.NOT_LEADER_OR_FOLLOWER)
  }

  @Test
  def testLeaderReplicaIfLocalRaisesUnknownTopicOrPartition(): Unit = {
    testListOffsetFailedGetLeaderReplica(Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testHandleDeleteGroups(): Unit = {
    val deleteGroupsRequest = new DeleteGroupsRequestData().setGroupsNames(List(
      "group-1",
      "group-2",
      "group-3"
    ).asJava)

    val requestChannelRequest = buildRequest(new DeleteGroupsRequest.Builder(deleteGroupsRequest).build())

    val future = new CompletableFuture[DeleteGroupsResponseData.DeletableGroupResultCollection]()
    when(groupCoordinator.deleteGroups(
      requestChannelRequest.context,
      List("group-1", "group-2", "group-3").asJava,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleDeleteGroupsRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    val results = new DeleteGroupsResponseData.DeletableGroupResultCollection(List(
      new DeleteGroupsResponseData.DeletableGroupResult()
        .setGroupId("group-1")
        .setErrorCode(Errors.NONE.code),
      new DeleteGroupsResponseData.DeletableGroupResult()
        .setGroupId("group-2")
        .setErrorCode(Errors.NOT_CONTROLLER.code),
      new DeleteGroupsResponseData.DeletableGroupResult()
        .setGroupId("group-3")
        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code),
    ).iterator.asJava)

    future.complete(results)

    val expectedDeleteGroupsResponse = new DeleteGroupsResponseData()
      .setResults(results)

    val response = verifyNoThrottling[DeleteGroupsResponse](requestChannelRequest)
    assertEquals(expectedDeleteGroupsResponse, response.data)
  }

  @Test
  def testHandleDeleteGroupsFutureFailed(): Unit = {
    val deleteGroupsRequest = new DeleteGroupsRequestData().setGroupsNames(List(
      "group-1",
      "group-2",
      "group-3"
    ).asJava)

    val requestChannelRequest = buildRequest(new DeleteGroupsRequest.Builder(deleteGroupsRequest).build())

    val future = new CompletableFuture[DeleteGroupsResponseData.DeletableGroupResultCollection]()
    when(groupCoordinator.deleteGroups(
      requestChannelRequest.context,
      List("group-1", "group-2", "group-3").asJava,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleDeleteGroupsRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    future.completeExceptionally(Errors.NOT_CONTROLLER.exception)

    val expectedDeleteGroupsResponse = new DeleteGroupsResponseData()
      .setResults(new DeleteGroupsResponseData.DeletableGroupResultCollection(List(
        new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId("group-1")
          .setErrorCode(Errors.NOT_CONTROLLER.code),
        new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId("group-2")
          .setErrorCode(Errors.NOT_CONTROLLER.code),
        new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId("group-3")
          .setErrorCode(Errors.NOT_CONTROLLER.code),
      ).iterator.asJava))

    val response = verifyNoThrottling[DeleteGroupsResponse](requestChannelRequest)
    assertEquals(expectedDeleteGroupsResponse, response.data)
  }

  @Test
  def testHandleDeleteGroupsAuthenticationFailed(): Unit = {
    val deleteGroupsRequest = new DeleteGroupsRequestData().setGroupsNames(List(
      "group-1",
      "group-2",
      "group-3"
    ).asJava)

    val requestChannelRequest = buildRequest(new DeleteGroupsRequest.Builder(deleteGroupsRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])

    val acls = Map(
      "group-1" -> AuthorizationResult.DENIED,
      "group-2" -> AuthorizationResult.ALLOWED,
      "group-3" -> AuthorizationResult.ALLOWED
    )

    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.asScala.map { action =>
        acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED)
      }.asJava
    }

    val future = new CompletableFuture[DeleteGroupsResponseData.DeletableGroupResultCollection]()
    when(groupCoordinator.deleteGroups(
      requestChannelRequest.context,
      List("group-2", "group-3").asJava,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleDeleteGroupsRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    future.complete(new DeleteGroupsResponseData.DeletableGroupResultCollection(List(
      new DeleteGroupsResponseData.DeletableGroupResult()
        .setGroupId("group-2")
        .setErrorCode(Errors.NONE.code),
      new DeleteGroupsResponseData.DeletableGroupResult()
        .setGroupId("group-3")
        .setErrorCode(Errors.NONE.code)
    ).iterator.asJava))

    val expectedDeleteGroupsResponse = new DeleteGroupsResponseData()
      .setResults(new DeleteGroupsResponseData.DeletableGroupResultCollection(List(
        new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId("group-2")
          .setErrorCode(Errors.NONE.code),
        new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId("group-3")
          .setErrorCode(Errors.NONE.code),
        new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId("group-1")
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)).iterator.asJava))

    val response = verifyNoThrottling[DeleteGroupsResponse](requestChannelRequest)
    assertEquals(expectedDeleteGroupsResponse, response.data)
  }

  @Test
  def testHandleDescribeGroups(): Unit = {
    val describeGroupsRequest = new DescribeGroupsRequestData().setGroups(List(
      "group-1",
      "group-2",
      "group-3",
      "group-4"
    ).asJava)

    val requestChannelRequest = buildRequest(new DescribeGroupsRequest.Builder(describeGroupsRequest).build())

    val future = new CompletableFuture[util.List[DescribeGroupsResponseData.DescribedGroup]]()
    when(groupCoordinator.describeGroups(
      requestChannelRequest.context,
      describeGroupsRequest.groups
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleDescribeGroupsRequest(requestChannelRequest)

    val groupResults = List(
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-1")
        .setProtocolType("consumer")
        .setProtocolData("range")
        .setGroupState("Stable")
        .setMembers(List(
          new DescribeGroupsResponseData.DescribedGroupMember()
            .setMemberId("member-1")).asJava),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-2")
        .setErrorCode(Errors.NOT_COORDINATOR.code),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-3")
        .setErrorCode(Errors.REQUEST_TIMED_OUT.code),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-4")
        .setGroupState("Dead")
        .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code)
        .setErrorMessage("Group group-4 is not a classic group.")
    ).asJava

    future.complete(groupResults)

    val expectedDescribeGroupsResponse = new DescribeGroupsResponseData().setGroups(groupResults)
    val response = verifyNoThrottling[DescribeGroupsResponse](requestChannelRequest)
    assertEquals(expectedDescribeGroupsResponse, response.data)
  }

  @Test
  def testHandleDescribeGroupsFutureFailed(): Unit = {
    val describeGroupsRequest = new DescribeGroupsRequestData().setGroups(List(
      "group-1",
      "group-2",
      "group-3"
    ).asJava)

    val requestChannelRequest = buildRequest(new DescribeGroupsRequest.Builder(describeGroupsRequest).build())

    val future = new CompletableFuture[util.List[DescribeGroupsResponseData.DescribedGroup]]()
    when(groupCoordinator.describeGroups(
      requestChannelRequest.context,
      describeGroupsRequest.groups
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleDescribeGroupsRequest(requestChannelRequest)

    val expectedDescribeGroupsResponse = new DescribeGroupsResponseData().setGroups(List(
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-1")
        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-2")
        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-3")
        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
    ).asJava)

    future.completeExceptionally(Errors.UNKNOWN_SERVER_ERROR.exception)

    val response = verifyNoThrottling[DescribeGroupsResponse](requestChannelRequest)
    assertEquals(expectedDescribeGroupsResponse, response.data)
  }

  @Test
  def testHandleDescribeGroupsAuthenticationFailed(): Unit = {
    val describeGroupsRequest = new DescribeGroupsRequestData().setGroups(List(
      "group-1",
      "group-2",
      "group-3"
    ).asJava)

    val requestChannelRequest = buildRequest(new DescribeGroupsRequest.Builder(describeGroupsRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])

    val acls = Map(
      "group-1" -> AuthorizationResult.DENIED,
      "group-2" -> AuthorizationResult.ALLOWED,
      "group-3" -> AuthorizationResult.DENIED
    )

    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.asScala.map { action =>
        acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED)
      }.asJava
    }

    val future = new CompletableFuture[util.List[DescribeGroupsResponseData.DescribedGroup]]()
    when(groupCoordinator.describeGroups(
      requestChannelRequest.context,
      List("group-2").asJava
    )).thenReturn(future)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleDescribeGroupsRequest(requestChannelRequest)

    future.complete(List(
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-2")
        .setErrorCode(Errors.NOT_COORDINATOR.code)
    ).asJava)

    val expectedDescribeGroupsResponse = new DescribeGroupsResponseData().setGroups(List(
      // group-1 and group-3 are first because unauthorized are put first into the response.
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-1")
        .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-3")
        .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-2")
        .setErrorCode(Errors.NOT_COORDINATOR.code)
    ).asJava)

    val response = verifyNoThrottling[DescribeGroupsResponse](requestChannelRequest)
    assertEquals(expectedDescribeGroupsResponse, response.data)
  }

  @Test
  def testOffsetDelete(): Unit = {
    val group = "groupId"
    addTopicToMetadataCache("topic-1", numPartitions = 2)
    addTopicToMetadataCache("topic-2", numPartitions = 2)

    val topics = new OffsetDeleteRequestTopicCollection()
    topics.add(new OffsetDeleteRequestTopic()
      .setName("topic-1")
      .setPartitions(Seq(
        new OffsetDeleteRequestPartition().setPartitionIndex(0),
        new OffsetDeleteRequestPartition().setPartitionIndex(1)).asJava))
    topics.add(new OffsetDeleteRequestTopic()
      .setName("topic-2")
      .setPartitions(Seq(
        new OffsetDeleteRequestPartition().setPartitionIndex(0),
        new OffsetDeleteRequestPartition().setPartitionIndex(1)).asJava))

    val offsetDeleteRequest = new OffsetDeleteRequest.Builder(
      new OffsetDeleteRequestData()
        .setGroupId(group)
        .setTopics(topics)
    ).build()
    val request = buildRequest(offsetDeleteRequest)

    val requestLocal = RequestLocal.withThreadConfinedCaching
    val future = new CompletableFuture[OffsetDeleteResponseData]()
    when(groupCoordinator.deleteOffsets(
      request.context,
      offsetDeleteRequest.data,
      requestLocal.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetDeleteRequest(request, requestLocal)

    val offsetDeleteResponseData = new OffsetDeleteResponseData()
      .setTopics(new OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection(List(
        new OffsetDeleteResponseData.OffsetDeleteResponseTopic()
          .setName("topic-1")
          .setPartitions(new OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection(List(
            new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)
          ).asJava.iterator)),
        new OffsetDeleteResponseData.OffsetDeleteResponseTopic()
          .setName("topic-2")
          .setPartitions(new OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection(List(
            new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)
          ).asJava.iterator))
      ).asJava.iterator()))

    future.complete(offsetDeleteResponseData)

    val response = verifyNoThrottling[OffsetDeleteResponse](request)
    assertEquals(offsetDeleteResponseData, response.data)
  }

  @Test
  def testOffsetDeleteRejectsBackingTopic(): Unit = {
    // r22 BLOCKER #205 — backing topics for concentrated logical topics are real Kafka
    // topics, so metadataCache.contains returns true and handleOffsetDeleteRequest would
    // accept them without this guard. A principal authorized on the backing-topic NAME could
    // delete consumer-group offsets that legitimate co-tenant clients committed via their
    // LOGICAL topic, causing wholesale re-consumption / data re-processing across every
    // co-tenant group sharing that backing partition. The rejection MUST run AFTER auth so
    // an UNauthorized probe still receives TOPIC_AUTHORIZATION_FAILED and cannot enumerate
    // the declared-backing set (auth-first / shadow-second precedence, same as #159/#146).
    val group = "groupId"
    val backingTopic = "backing-topic-r22-205"
    val plainTopic = "tenant-topic-r22-205"
    addTopicToMetadataCache(backingTopic, numPartitions = 8)
    addTopicToMetadataCache(plainTopic, numPartitions = 2)
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)
    when(concentrationKernel.isBackingTopic(plainTopic)).thenReturn(false)

    val topics = new OffsetDeleteRequestTopicCollection()
    topics.add(new OffsetDeleteRequestTopic()
      .setName(backingTopic)
      .setPartitions(Seq(
        new OffsetDeleteRequestPartition().setPartitionIndex(0),
        new OffsetDeleteRequestPartition().setPartitionIndex(1)).asJava))
    topics.add(new OffsetDeleteRequestTopic()
      .setName(plainTopic)
      .setPartitions(Seq(
        new OffsetDeleteRequestPartition().setPartitionIndex(0)).asJava))

    val offsetDeleteRequest = new OffsetDeleteRequest.Builder(
      new OffsetDeleteRequestData().setGroupId(group).setTopics(topics)
    ).build()
    val request = buildRequest(offsetDeleteRequest)

    val requestLocal = RequestLocal.withThreadConfinedCaching
    val future = new CompletableFuture[OffsetDeleteResponseData]()
    // The coordinator MUST only see the non-backing topic in its request payload.
    when(groupCoordinator.deleteOffsets(
      ArgumentMatchers.eq(request.context),
      any[OffsetDeleteRequestData],
      ArgumentMatchers.eq(requestLocal.bufferSupplier)
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetDeleteRequest(request, requestLocal)

    // Drive the coordinator future to a successful response for the plain topic.
    future.complete(new OffsetDeleteResponseData()
      .setTopics(new OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection(List(
        new OffsetDeleteResponseData.OffsetDeleteResponseTopic()
          .setName(plainTopic)
          .setPartitions(new OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection(List(
            new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code)
          ).asJava.iterator))
      ).asJava.iterator())))

    val response = verifyNoThrottling[OffsetDeleteResponse](request)
    val backingResp = response.data.topics.find(backingTopic)
    assertNotNull(backingResp, "backing topic must still appear in response with rejection error")
    backingResp.partitions.forEach { p =>
      assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, p.errorCode,
        s"Backing topic partition ${p.partitionIndex} must be rejected with INVALID_TOPIC_EXCEPTION")
    }
    val plainResp = response.data.topics.find(plainTopic)
    assertNotNull(plainResp, "non-backing topic must be forwarded to coordinator and present in response")
    assertEquals(Errors.NONE.code, plainResp.partitions.find(0).errorCode,
      "non-backing topic offset deletion must succeed independently of backing rejection")
    // The coordinator MUST NOT have been asked to delete offsets for the backing topic.
    val captor = ArgumentCaptor.forClass(classOf[OffsetDeleteRequestData])
    verify(groupCoordinator).deleteOffsets(any(), captor.capture(), any())
    val forwarded = captor.getValue
    assertNull(forwarded.topics.find(backingTopic),
      "Backing topic MUST NOT be forwarded to group coordinator — offset state must never key on backing")
    assertNotNull(forwarded.topics.find(plainTopic),
      "Non-backing topic MUST be forwarded to group coordinator")
  }

  @Test
  def testOffsetCommitRejectsBackingTopic(): Unit = {
    // r22 BLOCKER #205 — backing topics for concentrated logical topics are real Kafka
    // topics, so metadataCache.contains returns true and handleOffsetCommitRequest would
    // accept them without this guard. A principal authorized on the backing-topic NAME could
    // commit arbitrary offsets keyed on the backing in __consumer_offsets, corrupting every
    // co-tenant consumer group sharing that backing partition. Rejection runs AFTER auth so
    // an UNauthorized probe still receives TOPIC_AUTHORIZATION_FAILED and cannot enumerate
    // the declared-backing set (auth-first / shadow-second precedence).
    val backingTopic = "backing-topic-r22-205-commit"
    val plainTopic = "tenant-topic-r22-205-commit"
    addTopicToMetadataCache(backingTopic, numPartitions = 8)
    addTopicToMetadataCache(plainTopic, numPartitions = 2)
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)
    when(concentrationKernel.isBackingTopic(plainTopic)).thenReturn(false)

    val offsetCommitRequest = new OffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(List(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName(backingTopic)
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(666),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(777)).asJava),
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName(plainTopic)
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10)).asJava)).asJava)

    val requestChannelRequest = buildRequest(new OffsetCommitRequest.Builder(offsetCommitRequest).build())

    val future = new CompletableFuture[OffsetCommitResponseData]()
    when(groupCoordinator.commitOffsets(
      ArgumentMatchers.eq(requestChannelRequest.context),
      any[OffsetCommitRequestData],
      any()
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    // Coordinator returns success only for the plain topic.
    future.complete(new OffsetCommitResponseData()
      .setTopics(List(
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName(plainTopic)
          .setPartitions(List(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code)).asJava)).asJava))

    val response = verifyNoThrottling[OffsetCommitResponse](requestChannelRequest)
    val responseTopics = response.data.topics.asScala
    val backingResp = responseTopics.find(_.name == backingTopic).getOrElse(
      fail("backing topic must appear in response with rejection error").asInstanceOf[Nothing])
    backingResp.partitions.asScala.foreach { p =>
      assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, p.errorCode,
        s"Backing topic partition ${p.partitionIndex} must be rejected with INVALID_TOPIC_EXCEPTION")
    }
    val plainResp = responseTopics.find(_.name == plainTopic).getOrElse(
      fail("non-backing topic must be forwarded and appear in response").asInstanceOf[Nothing])
    assertEquals(Errors.NONE.code, plainResp.partitions.asScala.head.errorCode,
      "non-backing topic offset commit must succeed independently of backing rejection")

    // The coordinator MUST NOT have been asked to commit offsets for the backing topic.
    val captor = ArgumentCaptor.forClass(classOf[OffsetCommitRequestData])
    verify(groupCoordinator).commitOffsets(any(), captor.capture(), any())
    val forwarded = captor.getValue
    assertFalse(forwarded.topics.asScala.exists(_.name == backingTopic),
      "Backing topic MUST NOT be forwarded to group coordinator — offset state must never key on backing")
    assertTrue(forwarded.topics.asScala.exists(_.name == plainTopic),
      "Non-backing topic MUST be forwarded to group coordinator")
  }

  @Test
  def testOffsetDeleteTopicsAndPartitionsValidation(): Unit = {
    val group = "groupId"
    addTopicToMetadataCache("foo", numPartitions = 2)
    addTopicToMetadataCache("bar", numPartitions = 2)

    val offsetDeleteRequest = new OffsetDeleteRequestData()
      .setGroupId(group)
      .setTopics(new OffsetDeleteRequestTopicCollection(List(
        // foo exists but has only 2 partitions.
        new OffsetDeleteRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetDeleteRequestPartition().setPartitionIndex(0),
            new OffsetDeleteRequestPartition().setPartitionIndex(1),
            new OffsetDeleteRequestPartition().setPartitionIndex(2)
          ).asJava),
        // bar exists.
        new OffsetDeleteRequestTopic()
          .setName("bar")
          .setPartitions(List(
            new OffsetDeleteRequestPartition().setPartitionIndex(0),
            new OffsetDeleteRequestPartition().setPartitionIndex(1)
          ).asJava),
        // zar does not exist.
        new OffsetDeleteRequestTopic()
          .setName("zar")
          .setPartitions(List(
            new OffsetDeleteRequestPartition().setPartitionIndex(0),
            new OffsetDeleteRequestPartition().setPartitionIndex(1)
          ).asJava),
      ).asJava.iterator))

    val requestChannelRequest = buildRequest(new OffsetDeleteRequest.Builder(offsetDeleteRequest).build())

    // This is the request expected by the group coordinator. It contains
    // only existing topic-partitions.
    val expectedOffsetDeleteRequest = new OffsetDeleteRequestData()
      .setGroupId(group)
      .setTopics(new OffsetDeleteRequestTopicCollection(List(
        new OffsetDeleteRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetDeleteRequestPartition().setPartitionIndex(0),
            new OffsetDeleteRequestPartition().setPartitionIndex(1)
          ).asJava),
        new OffsetDeleteRequestTopic()
          .setName("bar")
          .setPartitions(List(
            new OffsetDeleteRequestPartition().setPartitionIndex(0),
            new OffsetDeleteRequestPartition().setPartitionIndex(1)
          ).asJava)
      ).asJava.iterator))

    val future = new CompletableFuture[OffsetDeleteResponseData]()
    when(groupCoordinator.deleteOffsets(
      requestChannelRequest.context,
      expectedOffsetDeleteRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    // This is the response returned by the group coordinator.
    val offsetDeleteResponse = new OffsetDeleteResponseData()
      .setTopics(new OffsetDeleteResponseTopicCollection(List(
        new OffsetDeleteResponseTopic()
          .setName("foo")
          .setPartitions(new OffsetDeleteResponsePartitionCollection(List(
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)
          ).asJava.iterator)),
        new OffsetDeleteResponseTopic()
          .setName("bar")
          .setPartitions(new OffsetDeleteResponsePartitionCollection(List(
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)
          ).asJava.iterator)),
      ).asJava.iterator))

    val expectedOffsetDeleteResponse = new OffsetDeleteResponseData()
      .setTopics(new OffsetDeleteResponseTopicCollection(List(
        new OffsetDeleteResponseTopic()
          .setName("foo")
          .setPartitions(new OffsetDeleteResponsePartitionCollection(List(
            // foo-2 is first because partitions failing the validation
            // are put in the response first.
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(2)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code),
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)
          ).asJava.iterator)),
        // zar is before bar because topics failing the validation are
        // put in the response first.
        new OffsetDeleteResponseTopic()
          .setName("zar")
          .setPartitions(new OffsetDeleteResponsePartitionCollection(List(
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code),
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
          ).asJava.iterator)),
        new OffsetDeleteResponseTopic()
          .setName("bar")
          .setPartitions(new OffsetDeleteResponsePartitionCollection(List(
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)
          ).asJava.iterator)),
      ).asJava.iterator))

    future.complete(offsetDeleteResponse)
    val response = verifyNoThrottling[OffsetDeleteResponse](requestChannelRequest)
    assertEquals(expectedOffsetDeleteResponse, response.data)
  }

  @Test
  def testOffsetDeleteWithInvalidPartition(): Unit = {
    val group = "groupId"
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 1)

    def checkInvalidPartition(invalidPartitionId: Int): Unit = {
      reset(groupCoordinator, replicaManager, clientRequestQuotaManager, requestChannel)

      val topics = new OffsetDeleteRequestTopicCollection()
      topics.add(new OffsetDeleteRequestTopic()
        .setName(topic)
        .setPartitions(Collections.singletonList(
          new OffsetDeleteRequestPartition().setPartitionIndex(invalidPartitionId))))
      val offsetDeleteRequest = new OffsetDeleteRequest.Builder(
        new OffsetDeleteRequestData()
          .setGroupId(group)
          .setTopics(topics)
      ).build()
      val request = buildRequest(offsetDeleteRequest)

      // The group coordinator is called even if there are no
      // topic-partitions left after the validation.
      when(groupCoordinator.deleteOffsets(
        request.context,
        new OffsetDeleteRequestData().setGroupId(group),
        RequestLocal.noCaching.bufferSupplier
      )).thenReturn(CompletableFuture.completedFuture(
        new OffsetDeleteResponseData()
      ))
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleOffsetDeleteRequest(request, RequestLocal.noCaching)

        val response = verifyNoThrottling[OffsetDeleteResponse](request)

        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION,
          Errors.forCode(response.data.topics.find(topic).partitions.find(invalidPartitionId).errorCode))
      } finally {
        kafkaApis.close()
      }
    }

    checkInvalidPartition(-1)
    checkInvalidPartition(1) // topic has only one partition
  }

  @Test
  def testOffsetDeleteWithInvalidGroup(): Unit = {
    val group = "groupId"
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 1)

    val offsetDeleteRequest = new OffsetDeleteRequest.Builder(
      new OffsetDeleteRequestData().setGroupId(group)
    ).build()
    val request = buildRequest(offsetDeleteRequest)

    val future = new CompletableFuture[OffsetDeleteResponseData]()
    when(groupCoordinator.deleteOffsets(
      request.context,
      offsetDeleteRequest.data,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetDeleteRequest(request, RequestLocal.noCaching)

    future.completeExceptionally(Errors.GROUP_ID_NOT_FOUND.exception)

    val response = verifyNoThrottling[OffsetDeleteResponse](request)

    assertEquals(Errors.GROUP_ID_NOT_FOUND, Errors.forCode(response.data.errorCode))
  }

  @Test
  def testOffsetDeleteWithInvalidGroupWithTopLevelError(): Unit = {
    val group = "groupId"
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 1)

    val offsetDeleteRequest = new OffsetDeleteRequest.Builder(
      new OffsetDeleteRequestData()
        .setGroupId(group)
        .setTopics(new OffsetDeleteRequestTopicCollection(Collections.singletonList(new OffsetDeleteRequestTopic()
          .setName("topic-unknown")
          .setPartitions(Collections.singletonList(new OffsetDeleteRequestPartition()
            .setPartitionIndex(0)
          ))
        ).iterator()))
    ).build()
    val request = buildRequest(offsetDeleteRequest)

    val future = new CompletableFuture[OffsetDeleteResponseData]()
    when(groupCoordinator.deleteOffsets(
      request.context,
      new OffsetDeleteRequestData().setGroupId(group), // Nonexistent topics won't be passed to groupCoordinator.
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetDeleteRequest(request, RequestLocal.noCaching)

    future.complete(new OffsetDeleteResponseData()
      .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
    )

    val response = verifyNoThrottling[OffsetDeleteResponse](request)

    assertEquals(Errors.GROUP_ID_NOT_FOUND, Errors.forCode(response.data.errorCode))
  }

  private def testListOffsetFailedGetLeaderReplica(error: Errors): Unit = {
    val tp = new TopicPartition("foo", 0)
    val isolationLevel = IsolationLevel.READ_UNCOMMITTED
    val currentLeaderEpoch = Optional.of[Integer](15)

    when(replicaManager.fetchOffset(
      ArgumentMatchers.any[Seq[ListOffsetsTopic]](),
      ArgumentMatchers.eq(Set.empty[TopicPartition]),
      ArgumentMatchers.eq(isolationLevel),
      ArgumentMatchers.eq(ListOffsetsRequest.CONSUMER_REPLICA_ID),
      ArgumentMatchers.eq[String](clientId),
      ArgumentMatchers.anyInt(), // correlationId
      ArgumentMatchers.anyShort(), // version
      ArgumentMatchers.any[(Errors, ListOffsetsPartition) => ListOffsetsPartitionResponse](),
      ArgumentMatchers.any[List[ListOffsetsTopicResponse] => Unit](),
      ArgumentMatchers.anyInt() // timeoutMs
    )).thenAnswer(ans => {
      val callback = ans.getArgument[List[ListOffsetsTopicResponse] => Unit](8)
      val partitionResponse = new ListOffsetsPartitionResponse()
        .setErrorCode(error.code())
        .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)
        .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
        .setPartitionIndex(tp.partition())
      callback(List(new ListOffsetsTopicResponse().setName(tp.topic()).setPartitions(List(partitionResponse).asJava)))
    })

    val targetTimes = List(new ListOffsetsTopic()
      .setName(tp.topic)
      .setPartitions(List(new ListOffsetsPartition()
        .setPartitionIndex(tp.partition)
        .setTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP)
        .setCurrentLeaderEpoch(currentLeaderEpoch.get)).asJava)).asJava
    val listOffsetRequest = ListOffsetsRequest.Builder.forConsumer(true, isolationLevel)
      .setTargetTimes(targetTimes).build()
    val request = buildRequest(listOffsetRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis()
    kafkaApis.handleListOffsetRequest(request)

    val response = verifyNoThrottling[ListOffsetsResponse](request)
    val partitionDataOptional = response.topics.asScala.find(_.name == tp.topic).get
      .partitions.asScala.find(_.partitionIndex == tp.partition)
    assertTrue(partitionDataOptional.isDefined)

    val partitionData = partitionDataOptional.get
    assertEquals(error.code, partitionData.errorCode)
    assertEquals(ListOffsetsResponse.UNKNOWN_OFFSET, partitionData.offset)
    assertEquals(ListOffsetsResponse.UNKNOWN_TIMESTAMP, partitionData.timestamp)
  }

  @Test
  def testReadUncommittedConsumerListOffsetLatest(): Unit = {
    testConsumerListOffsetLatest(IsolationLevel.READ_UNCOMMITTED)
  }

  @Test
  def testReadCommittedConsumerListOffsetLatest(): Unit = {
    testConsumerListOffsetLatest(IsolationLevel.READ_COMMITTED)
  }

  @Test
  def testListOffsetMaxTimestampWithUnsupportedVersion(): Unit = {
    testConsumerListOffsetWithUnsupportedVersion(ListOffsetsRequest.MAX_TIMESTAMP, 6)
  }

  @Test
  def testListOffsetEarliestLocalTimestampWithUnsupportedVersion(): Unit = {
    testConsumerListOffsetWithUnsupportedVersion(ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, 7)
  }

  @Test
  def testListOffsetLatestTieredTimestampWithUnsupportedVersion(): Unit = {
    testConsumerListOffsetWithUnsupportedVersion(ListOffsetsRequest.LATEST_TIERED_TIMESTAMP, 8)
  }

  @Test
  def testListOffsetNegativeTimestampWithOneOrAboveVersion(): Unit = {
    testConsumerListOffsetWithUnsupportedVersion(-6, 1)
  }

  /**
   * Verifies that the metadata response is correct if the broker listeners are inconsistent (i.e. one broker has
   * more listeners than another) and the request is sent on the listener that exists in both brokers.
   */
  @Test
  def testMetadataRequestOnSharedListenerWithInconsistentListenersAcrossBrokers(): Unit = {
    val (plaintextListener, _) = updateMetadataCacheWithInconsistentListeners()
    val response = sendMetadataRequestWithInconsistentListeners(plaintextListener)
    assertEquals(Set(0, 1), response.brokers.asScala.map(_.id).toSet)
  }

  /**
   * Verifies that the metadata response is correct if the broker listeners are inconsistent (i.e. one broker has
   * more listeners than another) and the request is sent on the listener that exists in one broker.
   */
  @Test
  def testMetadataRequestOnDistinctListenerWithInconsistentListenersAcrossBrokers(): Unit = {
    val (_, anotherListener) = updateMetadataCacheWithInconsistentListeners()
    val response = sendMetadataRequestWithInconsistentListeners(anotherListener)
    assertEquals(Set(0), response.brokers.asScala.map(_.id).toSet)
  }

  @Test
  def testUnauthorizedTopicMetadataRequest(): Unit = {
    // 1. Set up broker information
    val plaintextListener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val endpoints = new BrokerEndpointCollection()
    endpoints.add(
      new BrokerEndpoint()
        .setHost("broker0")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(plaintextListener.value)
    )
    MetadataCacheTest.updateCache(metadataCache,
      Seq(new RegisterBrokerRecord().setBrokerId(0).setRack("rack").setFenced(false).setEndPoints(endpoints))
    )

    // 2. Set up authorizer
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val unauthorizedTopic = "unauthorized-topic"
    val authorizedTopic = "authorized-topic"

    val expectedActions = Seq(
      new Action(AclOperation.DESCRIBE, new ResourcePattern(ResourceType.TOPIC, unauthorizedTopic, PatternType.LITERAL), 1, true, true),
      new Action(AclOperation.DESCRIBE, new ResourcePattern(ResourceType.TOPIC, authorizedTopic, PatternType.LITERAL), 1, true, true)
    )

    when(authorizer.authorize(any[RequestContext], argThat((t: java.util.List[Action]) => t.containsAll(expectedActions.asJava))))
      .thenAnswer { invocation =>
        val actions = invocation.getArgument(1).asInstanceOf[util.List[Action]].asScala
        actions.map { action =>
          if (action.resourcePattern().name().equals(authorizedTopic))
            AuthorizationResult.ALLOWED
          else
            AuthorizationResult.DENIED
        }.asJava
      }

    // 3. Set up MetadataCache
    val authorizedTopicId = Uuid.randomUuid()
    val unauthorizedTopicId = Uuid.randomUuid()
    addTopicToMetadataCache(authorizedTopic, 1, topicId = authorizedTopicId)
    addTopicToMetadataCache(unauthorizedTopic, 1, topicId = unauthorizedTopicId)

    def createDummyPartitionRecord(topicId: Uuid) = {
      new PartitionRecord()
        .setTopicId(topicId)
        .setPartitionId(0)
        .setLeader(0)
        .setLeaderEpoch(0)
        .setReplicas(Collections.singletonList(0))
        .setIsr(Collections.singletonList(0))
    }

    val partitionRecords = Seq(authorizedTopicId, unauthorizedTopicId).map(createDummyPartitionRecord)
    MetadataCacheTest.updateCache(metadataCache, partitionRecords)

    // 4. Send TopicMetadataReq using topicId
    val metadataReqByTopicId = new MetadataRequest.Builder(util.Arrays.asList(authorizedTopicId, unauthorizedTopicId)).build()
    val repByTopicId = buildRequest(metadataReqByTopicId, plaintextListener)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleTopicMetadataRequest(repByTopicId)
    val metadataByTopicIdResp = verifyNoThrottling[MetadataResponse](repByTopicId)

    val metadataByTopicId = metadataByTopicIdResp.data().topics().asScala.groupBy(_.topicId()).map(kv => (kv._1, kv._2.head))

    metadataByTopicId.foreach { case (topicId, metadataResponseTopic) =>
      if (topicId == unauthorizedTopicId) {
        // Return an TOPIC_AUTHORIZATION_FAILED on unauthorized error regardless of leaking the existence of topic id
        assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), metadataResponseTopic.errorCode())
        // Do not return topic information on unauthorized error
        assertNull(metadataResponseTopic.name())
      } else {
        assertEquals(Errors.NONE.code(), metadataResponseTopic.errorCode())
        assertEquals(authorizedTopic, metadataResponseTopic.name())
      }
    }
    kafkaApis.close()

    // 4. Send TopicMetadataReq using topic name
    reset(clientRequestQuotaManager, requestChannel)
    val metadataReqByTopicName = new MetadataRequest.Builder(util.Arrays.asList(authorizedTopic, unauthorizedTopic), false).build()
    val repByTopicName = buildRequest(metadataReqByTopicName, plaintextListener)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleTopicMetadataRequest(repByTopicName)
    val metadataByTopicNameResp = verifyNoThrottling[MetadataResponse](repByTopicName)

    val metadataByTopicName = metadataByTopicNameResp.data().topics().asScala.groupBy(_.name()).map(kv => (kv._1, kv._2.head))

    metadataByTopicName.foreach { case (topicName, metadataResponseTopic) =>
      if (topicName == unauthorizedTopic) {
        assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), metadataResponseTopic.errorCode())
        // Do not return topic Id on unauthorized error
        assertEquals(Uuid.ZERO_UUID, metadataResponseTopic.topicId())
      } else {
        assertEquals(Errors.NONE.code(), metadataResponseTopic.errorCode())
        assertEquals(authorizedTopicId, metadataResponseTopic.topicId())
      }
    }
  }

  @Test
  def testUnauthorizedLogicalTopicMetadataByTopicIdReturnsLogicalUuid(): Unit = {
    // Regression: v12+ METADATA by topic-id for an unauthorized logical topic must surface the
    // kernel's deterministic logical UUID (not Uuid.ZERO_UUID from metadataCache.getTopicId).
    // Otherwise stock clients caching by topic-id cannot correlate the auth-failure response back
    // to the request and either loop forever or surface a misleading error.
    val plaintextListener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val endpoints = new BrokerEndpointCollection()
    endpoints.add(
      new BrokerEndpoint()
        .setHost("broker0")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(plaintextListener.value)
    )
    MetadataCacheTest.updateCache(metadataCache,
      Seq(new RegisterBrokerRecord().setBrokerId(0).setRack("rack").setFenced(false).setEndPoints(endpoints))
    )

    val logicalTopic = "logical-unauth"
    val logicalTopicId = Uuid.randomUuid()
    when(concentrationKernel.logicalTopicByTopicId(logicalTopicId)).thenReturn(Optional.of(logicalTopic))
    when(concentrationKernel.isLogicalTopic(logicalTopic)).thenReturn(true)
    when(concentrationKernel.logicalTopicId(logicalTopic)).thenReturn(logicalTopicId)
    when(concentrationKernel.allLogicalTopicNames()).thenReturn(java.util.Collections.emptySet())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenAnswer { invocation =>
        invocation.getArgument(1).asInstanceOf[util.List[Action]].asScala.map { action =>
          if (action.resourcePattern().name() == logicalTopic) AuthorizationResult.DENIED
          else AuthorizationResult.ALLOWED
        }.asJava
      }

    val metadataReq = new MetadataRequest.Builder(util.Arrays.asList(logicalTopicId)).build()
    val req = buildRequest(metadataReq, plaintextListener)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleTopicMetadataRequest(req)
    val resp = verifyNoThrottling[MetadataResponse](req)

    val responseTopics = resp.data().topics().asScala
    assertEquals(1, responseTopics.size, s"Expected exactly one topic in response, got: $responseTopics")
    val rt = responseTopics.head
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), rt.errorCode())
    assertNull(rt.name(), "topic name must not leak on auth failure")
    assertEquals(logicalTopicId, rt.topicId(),
      "unauthorized logical topic must echo back the kernel's logical UUID so the client can correlate")
  }

  @Test
  def testMetadataByTopicIdRejectsBackingTopicUuid(): Unit = {
    // r21 BLOCKER #167 — METADATA(useTopicId) MUST NOT forward leadership metadata for a backing
    // topic's UUID. Backing topics are concentration-internal; the all-topics branch already
    // strips them by name, but the topic-id refresh path (the normal client cache-refresh
    // codepath) used to resolve UUID->name via metadataCache.getTopicName and forward the
    // backing topic's metadata verbatim. Any client that has obtained a backing UUID via any side
    // channel (logs, metrics, error messages, controller events, monitoring tools) could then
    // discover the backing topic's leader and partition layout — exposing the internal storage
    // layout and re-opening every ID-based admin RPC (DeleteTopics-by-id, OffsetForLeaderEpoch,
    // etc.) that other guards already close on by-name. Treat backing UUIDs as UNKNOWN_TOPIC_ID.
    val plaintextListener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val endpoints = new BrokerEndpointCollection()
    endpoints.add(
      new BrokerEndpoint()
        .setHost("broker0")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(plaintextListener.value)
    )
    MetadataCacheTest.updateCache(metadataCache,
      Seq(new RegisterBrokerRecord().setBrokerId(0).setRack("rack").setFenced(false).setEndPoints(endpoints))
    )

    val backingTopic = "concentration_default_backing_0"
    val backingTopicId = Uuid.randomUuid()
    val regularTopic = "regular-topic"
    val regularTopicId = Uuid.randomUuid()

    addTopicToMetadataCache(backingTopic, 1, topicId = backingTopicId)
    addTopicToMetadataCache(regularTopic, 1, topicId = regularTopicId)

    def partitionRecord(topicId: Uuid) = new PartitionRecord()
      .setTopicId(topicId).setPartitionId(0).setLeader(0).setLeaderEpoch(0)
      .setReplicas(Collections.singletonList(0)).setIsr(Collections.singletonList(0))
    MetadataCacheTest.updateCache(metadataCache,
      Seq(partitionRecord(backingTopicId), partitionRecord(regularTopicId)))

    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)
    when(concentrationKernel.isBackingTopic(regularTopic)).thenReturn(false)
    when(concentrationKernel.isLogicalTopic(anyString)).thenReturn(false)
    when(concentrationKernel.allLogicalTopicNames()).thenReturn(java.util.Collections.emptySet())

    val metadataReq = new MetadataRequest.Builder(
      util.Arrays.asList(backingTopicId, regularTopicId)).build()
    val req = buildRequest(metadataReq, plaintextListener)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis()
    kafkaApis.handleTopicMetadataRequest(req)
    val resp = verifyNoThrottling[MetadataResponse](req)

    val byTopicId = resp.data().topics().asScala.groupBy(_.topicId()).map(kv => (kv._1, kv._2.head))

    // Backing UUID: response MUST be UNKNOWN_TOPIC_ID with no name, no partitions, no leader.
    val backingResp = byTopicId(backingTopicId)
    assertEquals(Errors.UNKNOWN_TOPIC_ID.code(), backingResp.errorCode(),
      "backing topic UUID must surface as UNKNOWN_TOPIC_ID — never forwarded as a real topic")
    assertNull(backingResp.name(),
      "backing topic name must not appear in the response — even a stripped name is a leak")
    assertEquals(0, backingResp.partitions().size(),
      "backing topic partitions must not be exposed — leader/ISR data is internal storage layout")

    // Regular topic still works.
    val regularResp = byTopicId(regularTopicId)
    assertEquals(Errors.NONE.code(), regularResp.errorCode())
    assertEquals(regularTopic, regularResp.name())
    assertEquals(1, regularResp.partitions().size())

    // Defense-in-depth: the backing topic name MUST NOT appear anywhere in the response payload
    // — no error message, no nested partition data, no leader hint.
    assertFalse(resp.data().topics().asScala.exists(_.name() == backingTopic),
      "backing topic name must never appear in any METADATA response slot")
  }

  @Test
  def testMetadataByNameRejectsBackingTopic(): Unit = {
    // r22 BLOCKER #164 — METADATA(topics=["<backing>"]) MUST NOT forward leadership metadata for
    // a backing topic. The useTopicId branch is closed by #167 and the isAllTopics branch already
    // strips backings; the by-name branch (the most common admin / client-refresh path) used to
    // fall through to metadataCache.getTopicMetadata, returning real partition leadership / ISR /
    // replicas for any backing topic to any caller authorized (or wildcard-authorized) on that
    // name. Treat by-name requests for backing topics as UNKNOWN_TOPIC_OR_PARTITION so that
    // backing-vs-missing is indistinguishable to clients.
    val plaintextListener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val endpoints = new BrokerEndpointCollection()
    endpoints.add(
      new BrokerEndpoint()
        .setHost("broker0")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(plaintextListener.value)
    )
    MetadataCacheTest.updateCache(metadataCache,
      Seq(new RegisterBrokerRecord().setBrokerId(0).setRack("rack").setFenced(false).setEndPoints(endpoints))
    )

    val backingTopic = "concentration_default_backing_0"
    val backingTopicId = Uuid.randomUuid()
    val regularTopic = "regular-topic-r22-164"
    val regularTopicId = Uuid.randomUuid()

    addTopicToMetadataCache(backingTopic, 1, topicId = backingTopicId)
    addTopicToMetadataCache(regularTopic, 1, topicId = regularTopicId)

    def partitionRecord(topicId: Uuid) = new PartitionRecord()
      .setTopicId(topicId).setPartitionId(0).setLeader(0).setLeaderEpoch(0)
      .setReplicas(Collections.singletonList(0)).setIsr(Collections.singletonList(0))
    MetadataCacheTest.updateCache(metadataCache,
      Seq(partitionRecord(backingTopicId), partitionRecord(regularTopicId)))

    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)
    when(concentrationKernel.isBackingTopic(regularTopic)).thenReturn(false)
    when(concentrationKernel.isLogicalTopic(anyString)).thenReturn(false)
    when(concentrationKernel.allLogicalTopicNames()).thenReturn(java.util.Collections.emptySet())

    // By-name request mixing a backing topic with a regular topic.
    val metadataReq = new MetadataRequest.Builder(
      util.Arrays.asList(backingTopic, regularTopic), false).build()
    val req = buildRequest(metadataReq, plaintextListener)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis()
    kafkaApis.handleTopicMetadataRequest(req)
    val resp = verifyNoThrottling[MetadataResponse](req)

    val byName = resp.data().topics().asScala.groupBy(_.name()).map(kv => (kv._1, kv._2.head))

    val backingResp = byName(backingTopic)
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), backingResp.errorCode(),
      "backing topic by-name must surface as UNKNOWN_TOPIC_OR_PARTITION — never forwarded as a real topic")
    assertEquals(Uuid.ZERO_UUID, backingResp.topicId(),
      "backing topic UUID must not leak through the by-name path")
    assertEquals(0, backingResp.partitions().size(),
      "backing topic partitions must not be exposed via the by-name path — leader/ISR data is internal storage layout")

    // Regular topic still resolves normally.
    val regularResp = byName(regularTopic)
    assertEquals(Errors.NONE.code(), regularResp.errorCode())
    assertEquals(regularTopicId, regularResp.topicId())
    assertEquals(1, regularResp.partitions().size())
  }

    /**
   * Verifies that sending a fetch request with version 9 works correctly when
   * ReplicaManager.getLogConfig returns None.
   */
  @Test
  def testFetchRequestV9WithNoLogConfig(): Unit = {
    val tidp = new TopicIdPartition(Uuid.ZERO_UUID, new TopicPartition("foo", 0))
    val tp = tidp.topicPartition
    addTopicToMetadataCache(tp.topic, numPartitions = 1)
    val hw = 3
    val timestamp = 1000

    when(replicaManager.getLogConfig(ArgumentMatchers.eq(tp))).thenReturn(None)

    when(replicaManager.fetchMessages(
      any[FetchParams],
      any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]],
      any[ReplicaQuota],
      any[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]()
    )).thenAnswer(invocation => {
      val callback = invocation.getArgument(3).asInstanceOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]
      val records = MemoryRecords.withRecords(Compression.NONE,
        new SimpleRecord(timestamp, "foo".getBytes(StandardCharsets.UTF_8)))
      callback(Seq(tidp -> new FetchPartitionData(Errors.NONE, hw, 0, records,
        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)))
    })

    val fetchData = Map(tidp -> new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0, 0, 1000,
      Optional.empty())).asJava
    val fetchDataBuilder = Map(tp -> new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0, 0, 1000,
      Optional.empty())).asJava
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      fetchMetadata, fetchData, false, false)
    when(fetchManager.newContext(
      any[Short],
      any[JFetchMetadata],
      any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      any[util.List[TopicIdPartition]],
      any[util.Map[Uuid, String]])).thenReturn(fetchContext)

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val fetchRequest = new FetchRequest.Builder(9, 9, -1, -1, 100, 0, fetchDataBuilder)
      .build()
    val request = buildRequest(fetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleFetchRequest(request)

    val response = verifyNoThrottling[FetchResponse](request)
    val responseData = response.responseData(metadataCache.topicIdsToNames(), 9)
    assertTrue(responseData.containsKey(tp))

    val partitionData = responseData.get(tp)
    assertEquals(Errors.NONE.code, partitionData.errorCode)
    assertEquals(hw, partitionData.highWatermark)
    assertEquals(-1, partitionData.lastStableOffset)
    assertEquals(0, partitionData.logStartOffset)
    assertEquals(timestamp, FetchResponse.recordsOrFail(partitionData).batches.iterator.next.maxTimestamp)
    assertNull(partitionData.abortedTransactions)
  }

  /**
   * Verifies that partitions with unknown topic ID errors are added to the erroneous set and there is not an attempt to fetch them.
   */
  @ParameterizedTest
  @ValueSource(ints = Array(-1, 0))
  def testFetchRequestErroneousPartitions(replicaId: Int): Unit = {
    val foo = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0))
    val unresolvedFoo = new TopicIdPartition(foo.topicId, new TopicPartition(null, foo.partition))

    addTopicToMetadataCache(foo.topic, 1, topicId = foo.topicId)

    // We will never return a logConfig when the topic name is null. This is ok since we won't have any records to convert.
    when(replicaManager.getLogConfig(ArgumentMatchers.eq(unresolvedFoo.topicPartition))).thenReturn(None)

    // Simulate unknown topic ID in the context
    val fetchData = Map(new TopicIdPartition(foo.topicId, new TopicPartition(null, foo.partition)) ->
      new FetchRequest.PartitionData(foo.topicId, 0, 0, 1000, Optional.empty())).asJava
    val fetchDataBuilder = Map(foo.topicPartition -> new FetchRequest.PartitionData(foo.topicId, 0, 0, 1000,
      Optional.empty())).asJava
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      fetchMetadata, fetchData, true, replicaId >= 0)
    // We expect to have the resolved partition, but we will simulate an unknown one with the fetchContext we return.
    when(fetchManager.newContext(
      ApiKeys.FETCH.latestVersion,
      fetchMetadata,
      replicaId >= 0,
      Collections.singletonMap(foo, new FetchRequest.PartitionData(foo.topicId, 0, 0, 1000, Optional.empty())),
      Collections.emptyList[TopicIdPartition],
      metadataCache.topicIdsToNames())
    ).thenReturn(fetchContext)

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    // If replicaId is -1 we will build a consumer request. Any non-negative replicaId will build a follower request.
    val replicaEpoch = if (replicaId < 0) -1 else 1
    val fetchRequest = new FetchRequest.Builder(ApiKeys.FETCH.latestVersion, ApiKeys.FETCH.latestVersion,
      replicaId, replicaEpoch, 100, 0, fetchDataBuilder).metadata(fetchMetadata).build()
    val request = buildRequest(fetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleFetchRequest(request)

    val response = verifyNoThrottling[FetchResponse](request)
    val responseData = response.responseData(metadataCache.topicIdsToNames(), ApiKeys.FETCH.latestVersion)
    assertTrue(responseData.containsKey(foo.topicPartition))

    val partitionData = responseData.get(foo.topicPartition)
    assertEquals(Errors.UNKNOWN_TOPIC_ID.code, partitionData.errorCode)
    assertEquals(-1, partitionData.highWatermark)
    assertEquals(-1, partitionData.lastStableOffset)
    assertEquals(-1, partitionData.logStartOffset)
    assertEquals(MemoryRecords.EMPTY, FetchResponse.recordsOrFail(partitionData))
  }

  @Test
  def testFetchResponseContainsNewLeaderOnNotLeaderOrFollower(): Unit = {
    val topicId = Uuid.randomUuid()
    val tidp = new TopicIdPartition(topicId, new TopicPartition("foo", 0))
    val tp = tidp.topicPartition
    addTopicToMetadataCache(tp.topic, numPartitions = 1, numBrokers = 3, topicId)

    when(replicaManager.getLogConfig(ArgumentMatchers.eq(tp))).thenReturn(Some(LogConfig.fromProps(
      Collections.emptyMap(),
      new Properties()
    )))

    val partition = mock(classOf[Partition])
    val newLeaderId = 2
    val newLeaderEpoch = 5

    when(replicaManager.getPartitionOrError(tp)).thenAnswer(_ => Right(partition))
    when(partition.leaderReplicaIdOpt).thenAnswer(_ => Some(newLeaderId))
    when(partition.getLeaderEpoch).thenAnswer(_ => newLeaderEpoch)

    when(replicaManager.fetchMessages(
      any[FetchParams],
      any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]],
      any[ReplicaQuota],
      any[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]()
    )).thenAnswer(invocation => {
      val callback = invocation.getArgument(3).asInstanceOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]
      callback(Seq(tidp -> new FetchPartitionData(Errors.NOT_LEADER_OR_FOLLOWER, UnifiedLog.UnknownOffset, UnifiedLog.UnknownOffset, MemoryRecords.EMPTY,
        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)))
    })

    val fetchData = Map(tidp -> new FetchRequest.PartitionData(topicId, 0, 0, 1000,
      Optional.empty())).asJava
    val fetchDataBuilder = Map(tp -> new FetchRequest.PartitionData(topicId, 0, 0, 1000,
      Optional.empty())).asJava
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      fetchMetadata, fetchData, true, false)
    when(fetchManager.newContext(
      any[Short],
      any[JFetchMetadata],
      any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      any[util.List[TopicIdPartition]],
      any[util.Map[Uuid, String]])).thenReturn(fetchContext)

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val fetchRequest = new FetchRequest.Builder(16, 16, -1, -1, 100, 0, fetchDataBuilder)
      .build()
    val request = buildRequest(fetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleFetchRequest(request)

    val response = verifyNoThrottling[FetchResponse](request)
    val responseData = response.responseData(metadataCache.topicIdsToNames(), 16)

    val partitionData = responseData.get(tp)
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, partitionData.errorCode)
    assertEquals(newLeaderId, partitionData.currentLeader.leaderId())
    assertEquals(newLeaderEpoch, partitionData.currentLeader.leaderEpoch())
    val node = response.data.nodeEndpoints.asScala
    assertEquals(Seq(2), node.map(_.nodeId))
    assertEquals(Seq("broker2"), node.map(_.host))
  }

  @Test
  def testHandleShareFetchRequestSuccessWithoutAcknowledgements(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    val shareSessionEpoch = 0

    val records = memoryRecords(10, 0)

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setAcknowledgeErrorCode(Errors.NONE.code)
            .setRecords(records)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    )

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(new ShareRequestMetadata(memberId, shareSessionEpoch), Map(
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchRequest.SharePartitionData(topicId, partitionMaxBytes)
      ).asJava)
    )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId("group").
      setMemberId(memberId.toString).
      setShareSessionEpoch(shareSessionEpoch).
      setTopics(List(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(partitionIndex)
            .setPartitionMaxBytes(partitionMaxBytes)).asJava)).asJava)

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).errorCode)
    assertEquals(records, topicResponses.get(0).partitions.get(0).records)
    assertArrayEquals(expectedAcquiredRecords(0, 9, 1).toArray(), topicResponses.get(0).partitions.get(0).acquiredRecords.toArray())
  }

  @Test
  def testHandleShareFetchRequestInvalidRequestOnInitialEpoch(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    val groupId = "group"
    val partitionIndex = 0

    val records = memoryRecords(10, 0)

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setAcknowledgeErrorCode(Errors.NONE.code)
            .setRecords(records)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    )

    val cachedSharePartitions = new ImplicitLinkedHashCollection[CachedSharePartition]
    cachedSharePartitions.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)), new ShareFetchRequest.SharePartitionData(topicId, partitionMaxBytes), false))

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any())).thenThrow(
      Errors.INVALID_REQUEST.exception()
    ).thenReturn(new ShareSessionContext(new ShareRequestMetadata(memberId, 1), new ShareSession(
      new ShareSessionKey(groupId, memberId), cachedSharePartitions, 0L, 0L, 2
    )))

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    var shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(0).
      setTopics(List(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(partitionIndex)
            .setPartitionMaxBytes(partitionMaxBytes)
            setAcknowledgementBatches(List(
            new AcknowledgementBatch()
              .setFirstOffset(0)
              .setLastOffset(9)
              .setAcknowledgeTypes(Collections.singletonList(1.toByte))
          ).asJava)
        ).asJava)
      ).asJava)

    var shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    var request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    var response = verifyNoThrottling[ShareFetchResponse](request)
    var responseData = response.data()

    assertEquals(Errors.INVALID_REQUEST.code, responseData.errorCode)

    // Testing whether the subsequent request with the incremented share session epoch works or not.
    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(List(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setPartitionMaxBytes(partitionMaxBytes)
        ).asJava)
      ).asJava)

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).errorCode)
    assertEquals(records, topicResponses.get(0).partitions.get(0).records)
    assertArrayEquals(expectedAcquiredRecords(0, 9, 1).toArray(), topicResponses.get(0).partitions.get(0).acquiredRecords.toArray())
  }

  @Test
  def testHandleShareFetchRequestInvalidRequestOnFinalEpoch(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    val groupId = "group"

    val records = memoryRecords(10, 0)

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setAcknowledgeErrorCode(Errors.NONE.code)
            .setRecords(records)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    )

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(new ShareRequestMetadata(memberId, 0), Map(
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchRequest.SharePartitionData(topicId, partitionMaxBytes)
      ).asJava)
    ).thenThrow(Errors.INVALID_REQUEST.exception)

    when(sharePartitionManager.releaseSession(any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(partitionIndex)
            .setErrorCode(Errors.NONE.code)
      ).asJava)
    )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    var shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(0).
      setTopics(List(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setPartitionMaxBytes(partitionMaxBytes)).asJava)).asJava)

    var shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    var request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    var response = verifyNoThrottling[ShareFetchResponse](request)
    var responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).errorCode)
    assertEquals(records, topicResponses.get(0).partitions.get(0).records)
    assertArrayEquals(expectedAcquiredRecords(0, 9, 1).toArray(), topicResponses.get(0).partitions.get(0).acquiredRecords.toArray())

    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(-1).
      setTopics(List(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setPartitionMaxBytes(partitionMaxBytes) // partitionMaxBytes are set even on the final fetch request, this is an invalid request
            .setAcknowledgementBatches(List(
              new AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()

    assertEquals(Errors.INVALID_REQUEST.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestFetchThrowsException(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any())).thenReturn(
      FutureUtils.failedFuture[util.Map[TopicIdPartition, ShareFetchResponseData.PartitionData]](Errors.UNKNOWN_SERVER_ERROR.exception())
    )

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(new ShareRequestMetadata(memberId, 0), Map(
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchRequest.SharePartitionData(topicId, partitionMaxBytes)
      ).asJava)
    )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId("group").
      setMemberId(memberId.toString).
      setShareSessionEpoch(0).
      setTopics(List(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setPartitionMaxBytes(partitionMaxBytes)).asJava)).asJava)

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()

    assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestAcknowledgeThrowsException(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    val groupId = "group"

    val records = memoryRecords(10, 0)

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    )

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      FutureUtils.failedFuture[util.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]](Errors.UNKNOWN_SERVER_ERROR.exception())
    )

    val cachedSharePartitions = new ImplicitLinkedHashCollection[CachedSharePartition]
    cachedSharePartitions.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)), new ShareFetchRequest.SharePartitionData(topicId, partitionMaxBytes), false))

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any()))
      .thenReturn(new ShareSessionContext(new ShareRequestMetadata(memberId, 1), new ShareSession(
        new ShareSessionKey(groupId, memberId), cachedSharePartitions, 0L, 0L, 2))
      )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(List(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setPartitionMaxBytes(partitionMaxBytes)
            .setAcknowledgementBatches(List(
              new AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()

    assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestFetchAndAcknowledgeThrowsException(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    val groupId = "group"

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any())).thenReturn(
      FutureUtils.failedFuture[util.Map[TopicIdPartition, ShareFetchResponseData.PartitionData]](Errors.UNKNOWN_SERVER_ERROR.exception())
    )

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      FutureUtils.failedFuture[util.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]](Errors.UNKNOWN_SERVER_ERROR.exception())
    )

    val cachedSharePartitions = new ImplicitLinkedHashCollection[CachedSharePartition]
    cachedSharePartitions.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)), new ShareFetchRequest.SharePartitionData(topicId, partitionMaxBytes), false))

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any()))
      .thenReturn(new ShareSessionContext(new ShareRequestMetadata(memberId, 1), new ShareSession(
        new ShareSessionKey(groupId, memberId), cachedSharePartitions, 0L, 0L, 2))
      )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(List(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setPartitionMaxBytes(partitionMaxBytes)
            .setAcknowledgementBatches(List(
              new AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()

    assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestErrorInReadingPartition(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    val records = MemoryRecords.EMPTY

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.REPLICA_NOT_AVAILABLE.code)
            .setRecords(records)
            .setAcquiredRecords(new util.ArrayList(List().asJava))
      ).asJava)
    )

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(new ShareRequestMetadata(memberId, 0), Map(
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchRequest.SharePartitionData(topicId, partitionMaxBytes)
      ).asJava)
    )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId("group").
      setMemberId(memberId.toString).
      setShareSessionEpoch(0).
      setTopics(List(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setPartitionMaxBytes(partitionMaxBytes)).asJava)).asJava)

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.REPLICA_NOT_AVAILABLE.code, topicResponses.get(0).partitions.get(0).errorCode)
    assertEquals(records, topicResponses.get(0).partitions.get(0).records)
    assertTrue(topicResponses.get(0).partitions.get(0).acquiredRecords.toArray().isEmpty)
  }

  @Test
  def testHandleShareFetchRequestShareSessionNotFoundError(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    val groupId = "group"
    val records = memoryRecords(10, 0)

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    )

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(new ShareRequestMetadata(memberId, 0), Map(
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchRequest.SharePartitionData(topicId, partitionMaxBytes)
      ).asJava)
    ).thenThrow(Errors.SHARE_SESSION_NOT_FOUND.exception)

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    var shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(0).
      setTopics(List(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setPartitionMaxBytes(partitionMaxBytes)).asJava)).asJava)

    var shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    var request = buildRequest(shareFetchRequest)
    // First share fetch request is to establish the share session with the broker.
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    var response = verifyNoThrottling[ShareFetchResponse](request)
    var responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).errorCode)
    assertEquals(records, topicResponses.get(0).partitions.get(0).records)
    assertArrayEquals(expectedAcquiredRecords(0, 9, 1).toArray(), topicResponses.get(0).partitions.get(0).acquiredRecords.toArray())

    val memberId2 = Uuid.randomUuid()

    // Using wrong member ID.
    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId2.toString).
      setShareSessionEpoch(1).
      setTopics(List(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setPartitionMaxBytes(partitionMaxBytes)).asJava)).asJava)

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()

    assertEquals(Errors.SHARE_SESSION_NOT_FOUND.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestInvalidShareSessionError(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    val groupId = "group"
    val records = memoryRecords(10, 0)

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    )

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(new ShareRequestMetadata(memberId, 0), Map(
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchRequest.SharePartitionData(topicId, partitionMaxBytes)
      ).asJava)
    ).thenThrow(Errors.INVALID_SHARE_SESSION_EPOCH.exception)

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    var shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(0).
      setTopics(List(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setPartitionMaxBytes(partitionMaxBytes)).asJava)).asJava)

    var shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    var request = buildRequest(shareFetchRequest)
    // First share fetch request is to establish the share session with the broker.
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    var response = verifyNoThrottling[ShareFetchResponse](request)
    var responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).errorCode)
    assertEquals(records, topicResponses.get(0).partitions.get(0).records)
    assertArrayEquals(expectedAcquiredRecords(0, 9, 1).toArray(), topicResponses.get(0).partitions.get(0).acquiredRecords.toArray())

    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(2). // Invalid share session epoch, should have 1 for the second request.
      setTopics(List(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setPartitionMaxBytes(partitionMaxBytes)).asJava)).asJava)

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()

    assertEquals(Errors.INVALID_SHARE_SESSION_EPOCH.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestShareSessionSuccessfullyEstablished(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.randomUuid()

    val groupId = "group"

    val records1 = memoryRecords(10, 0)
    val records2 = memoryRecords(10, 10)
    val records3 = memoryRecords(10, 20)

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records1)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    ).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setAcknowledgeErrorCode(Errors.NONE.code)
            .setRecords(records2)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(10)
                .setLastOffset(19)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    ).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setAcknowledgeErrorCode(Errors.NONE.code)
            .setRecords(records3)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(20)
                .setLastOffset(29)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    )

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(partitionIndex)
            .setErrorCode(Errors.NONE.code)
      ).asJava)
    ).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(partitionIndex)
            .setErrorCode(Errors.NONE.code)
      ).asJava)
    )

    val cachedSharePartitions = new ImplicitLinkedHashCollection[CachedSharePartition]
    cachedSharePartitions.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)), new ShareFetchRequest.SharePartitionData(topicId, partitionMaxBytes), false)
    )

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(new ShareRequestMetadata(memberId, 0), Map(
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchRequest.SharePartitionData(topicId, partitionMaxBytes)
      ).asJava)
    ).thenReturn(new ShareSessionContext(new ShareRequestMetadata(memberId, 1), new ShareSession(
      new ShareSessionKey(groupId, memberId), cachedSharePartitions, 0L, 0L, 2))
    ).thenReturn(new ShareSessionContext(new ShareRequestMetadata(memberId, 2), new ShareSession(
      new ShareSessionKey(groupId, memberId), cachedSharePartitions, 0L, 10L, 3))
    )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    var shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(0).
      setTopics(List(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(partitionIndex)
            .setPartitionMaxBytes(partitionMaxBytes)).asJava)).asJava)

    var shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    var request = buildRequest(shareFetchRequest)

    // First share fetch request is to establish the share session with the broker.
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    var response = verifyNoThrottling[ShareFetchResponse](request)
    var responseData = response.data()
    var topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())

    compareResponsePartitions(
      partitionIndex,
      Errors.NONE.code,
      Errors.NONE.code,
      records1,
      expectedAcquiredRecords(0, 9, 1),
      topicResponses.get(0).partitions.get(0)
    )

    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(List(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareFetchRequestData.FetchPartition().
            setPartitionIndex(partitionIndex).
            setPartitionMaxBytes(partitionMaxBytes).
            setAcknowledgementBatches(List(
              new ShareFetchRequestData.AcknowledgementBatch().
                setFirstOffset(0).
                setLastOffset(9).
                setAcknowledgeTypes(List[java.lang.Byte](1.toByte).asJava)).asJava)).asJava)).asJava)

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)

    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()
    topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())

    compareResponsePartitions(
      partitionIndex,
      Errors.NONE.code,
      Errors.NONE.code,
      records2,
      expectedAcquiredRecords(10, 19, 1),
      topicResponses.get(0).partitions.get(0)
    )

    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(2).
      setTopics(List(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareFetchRequestData.FetchPartition().
            setPartitionIndex(partitionIndex).
            setPartitionMaxBytes(partitionMaxBytes).
            setAcknowledgementBatches(List(
              new ShareFetchRequestData.AcknowledgementBatch().
                setFirstOffset(10).
                setLastOffset(19).
                setAcknowledgeTypes(List[java.lang.Byte](1.toByte).asJava)).asJava)).asJava)).asJava)

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)

    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()
    topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())

    compareResponsePartitions(
      partitionIndex,
      Errors.NONE.code,
      Errors.NONE.code,
      records3,
      expectedAcquiredRecords(20, 29, 1),
      topicResponses.get(0).partitions.get(0)
    )
  }

  @Test
  def testHandleShareFetchRequestSuccessfulShareSessionLifecycle(): Unit = {
    val topicName1 = "foo1"
    val topicId1 = Uuid.randomUuid()

    val topicName2 = "foo2"
    val topicId2 = Uuid.randomUuid()

    val topicName3 = "foo3"
    val topicId3 = Uuid.randomUuid()

    val topicName4 = "foo4"
    val topicId4 = Uuid.randomUuid()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName1, 2, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)
    addTopicToMetadataCache(topicName3, 1, topicId = topicId3)
    addTopicToMetadataCache(topicName4, 1, topicId = topicId4)
    val memberId: Uuid = Uuid.ZERO_UUID

    val records_t1_p1_1 = memoryRecords(10, 0)
    val records_t1_p2_1 = memoryRecords(10, 10)

    val records_t2_p1_1 = memoryRecords(10, 43)
    val records_t2_p2_1 = memoryRecords(10, 17)

    val records_t3_p1_1 = memoryRecords(20, 54)
    val records_t3_p1_2 = memoryRecords(20, 74)

    val records_t4_p1_1 = memoryRecords(15, 10)

    val groupId = "group"

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t1_p1_1)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ).asJava)),
        new TopicIdPartition(topicId1, new TopicPartition(topicName1, 1)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t1_p2_1)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(10)
                .setLastOffset(19)
                .setDeliveryCount(1)
            ).asJava)),
        new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t2_p1_1)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(43)
                .setLastOffset(52)
                .setDeliveryCount(1)
            ).asJava)),
        new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t2_p2_1)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(17)
                .setLastOffset(26)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    ).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId3, new TopicPartition(topicName3, 0)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t3_p1_1)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(54)
                .setLastOffset(73)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    ).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId3, new TopicPartition(topicName3, 0)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t3_p1_2)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(74)
                .setLastOffset(93)
                .setDeliveryCount(1)
            ).asJava)),
        new TopicIdPartition(topicId4, new TopicPartition(topicName4, 0)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t4_p1_1)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(10)
                .setLastOffset(24)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    )

    val cachedSharePartitions1 = new ImplicitLinkedHashCollection[CachedSharePartition]
    cachedSharePartitions1.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0)), new ShareFetchRequest.SharePartitionData(topicId1, partitionMaxBytes), false
    ))
    cachedSharePartitions1.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId1, new TopicPartition(topicName1, 1)), new ShareFetchRequest.SharePartitionData(topicId1, partitionMaxBytes), false
    ))
    cachedSharePartitions1.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0)), new ShareFetchRequest.SharePartitionData(topicId2, partitionMaxBytes), false
    ))
    cachedSharePartitions1.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1)), new ShareFetchRequest.SharePartitionData(topicId2, partitionMaxBytes), false
    ))
    cachedSharePartitions1.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId3, new TopicPartition(topicName3, 0)), new ShareFetchRequest.SharePartitionData(topicId3, partitionMaxBytes), false
    ))

    val cachedSharePartitions2 = new ImplicitLinkedHashCollection[CachedSharePartition]
    cachedSharePartitions2.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId3, new TopicPartition(topicName3, 0)), new ShareFetchRequest.SharePartitionData(topicId3, partitionMaxBytes), false
    ))
    cachedSharePartitions2.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId4, new TopicPartition(topicName4, 0)), new ShareFetchRequest.SharePartitionData(topicId4, partitionMaxBytes), false
    ))

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(new ShareRequestMetadata(memberId, 0), Map(
        new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0)) ->
          new ShareFetchRequest.SharePartitionData(topicId1, partitionMaxBytes),
        new TopicIdPartition(topicId1, new TopicPartition(topicName1, 1)) ->
          new ShareFetchRequest.SharePartitionData(topicId1, partitionMaxBytes),
        new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0)) ->
          new ShareFetchRequest.SharePartitionData(topicId2, partitionMaxBytes),
        new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1)) ->
          new ShareFetchRequest.SharePartitionData(topicId2, partitionMaxBytes)
      ).asJava)
    ).thenReturn(new ShareSessionContext(new ShareRequestMetadata(memberId, 1), new ShareSession(
      new ShareSessionKey(groupId, memberId), cachedSharePartitions1, 0L, 0L, 2))
    ).thenReturn(new ShareSessionContext(new ShareRequestMetadata(memberId, 2), new ShareSession(
      new ShareSessionKey(groupId, memberId), cachedSharePartitions2, 0L, 0L, 3))
    ).thenReturn(new FinalContext())

    when(sharePartitionManager.releaseSession(any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId3, new TopicPartition(topicName3, 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId4, new TopicPartition(topicName4, 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
      ).asJava)
    )

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId1, new TopicPartition(topicName1, 1)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId3, new TopicPartition(topicName3, 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId4, new TopicPartition(topicName4, 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
      ).asJava)
    )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    var shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(0).
      setTopics(List(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId1).
          setPartitions(List(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setPartitionMaxBytes(partitionMaxBytes),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
              .setPartitionMaxBytes(partitionMaxBytes)
          ).asJava),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId2).
          setPartitions(List(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setPartitionMaxBytes(partitionMaxBytes),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
              .setPartitionMaxBytes(partitionMaxBytes)
          ).asJava)
      ).asJava)

    var shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    var request = buildRequest(shareFetchRequest)
    // First share fetch request is to establish the share session with the broker.
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    var response = verifyNoThrottling[ShareFetchResponse](request)
    var responseData = response.data()
    var topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(2, topicResponses.size())
    var topicResponsesScala = topicResponses.asScala.toList
    var topicResponsesMap: Map[Uuid, ShareFetchableTopicResponse] = topicResponsesScala.map(topic => topic.topicId -> topic).toMap
    assertTrue(topicResponsesMap.contains(topicId1))
    val topicIdResponse1: ShareFetchableTopicResponse = topicResponsesMap.getOrElse(topicId1, null)
    assertEquals(2, topicIdResponse1.partitions.size())
    val partitionsScala1 = topicIdResponse1.partitions.asScala.toList
    val partitionsMap1: Map[Int, PartitionData] = partitionsScala1.map(partition => partition.partitionIndex -> partition).toMap
    assertTrue(partitionsMap1.contains(0))
    val partition11: PartitionData = partitionsMap1.getOrElse(0, null)

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t1_p1_1,
      expectedAcquiredRecords(0, 9, 1),
      partition11
    )

    assertTrue(partitionsMap1.contains(1))
    val partition12: PartitionData = partitionsMap1.getOrElse(1, null)

    compareResponsePartitions(
      1,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t1_p2_1,
      expectedAcquiredRecords(10, 19, 1),
      partition12
    )

    assertTrue(topicResponsesMap.contains(topicId2))
    val topicIdResponse2: ShareFetchableTopicResponse = topicResponsesMap.getOrElse(topicId2, null)
    assertEquals(2, topicIdResponse2.partitions.size())
    val partitionsScala2 = topicIdResponse2.partitions.asScala.toList
    val partitionsMap2: Map[Int, PartitionData] = partitionsScala2.map(partition => partition.partitionIndex -> partition).toMap
    assertTrue(partitionsMap2.contains(0))
    val partition21: PartitionData = partitionsMap2.getOrElse(0, null)

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t2_p1_1,
      expectedAcquiredRecords(43, 52, 1),
      partition21
    )

    assertTrue(partitionsMap2.contains(1))
    val partition22: PartitionData = partitionsMap2.getOrElse(1, null)

    compareResponsePartitions(
      1,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t2_p2_1,
      expectedAcquiredRecords(17, 26, 1),
      partition22
    )

    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(List(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId3).
          setPartitions(List(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setPartitionMaxBytes(partitionMaxBytes)
          ).asJava),
      ).asJava)

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()
    topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId3, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t3_p1_1,
      expectedAcquiredRecords(54, 73, 1),
      topicResponses.get(0).partitions.get(0)
    )

    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(2).
      setTopics(List(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId4).
          setPartitions(List(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setPartitionMaxBytes(partitionMaxBytes)
          ).asJava),
      ).asJava)
      .setForgottenTopicsData(List(
        new ForgottenTopic()
          .setTopicId(topicId1)
          .setPartitions(List(Integer.valueOf(0), Integer.valueOf(1)).asJava),
        new ForgottenTopic()
          .setTopicId(topicId2)
          .setPartitions(List(Integer.valueOf(0), Integer.valueOf(1)).asJava)
      ).asJava)

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()
    topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(2, topicResponses.size())
    topicResponsesScala = topicResponses.asScala.toList
    topicResponsesMap = topicResponsesScala.map(topic => topic.topicId -> topic).toMap
    assertTrue(topicResponsesMap.contains(topicId3))
    val topicIdResponse3 = topicResponsesMap.getOrElse(topicId3, null)
    assertEquals(1, topicIdResponse3.partitions.size())

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t3_p1_2,
      expectedAcquiredRecords(74, 93, 1),
      topicIdResponse3.partitions.get(0)
    )

    assertTrue(topicResponsesMap.contains(topicId4))
    val topicIdResponse4 = topicResponsesMap.getOrElse(topicId4, null)
    assertEquals(1, topicIdResponse4.partitions.size())

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t4_p1_1,
      expectedAcquiredRecords(10, 24, 1),
      topicIdResponse4.partitions.get(0)
    )

    // Final request with acknowledgements.
    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(-1).
      setTopics(List(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId1).
          setPartitions(List(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setPartitionMaxBytes(0)
              .setAcknowledgementBatches(List(
                new AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(Collections.singletonList(1.toByte)),
              ).asJava),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
              .setPartitionMaxBytes(0)
              .setAcknowledgementBatches(List(
                new AcknowledgementBatch()
                  .setFirstOffset(10)
                  .setLastOffset(19)
                  .setAcknowledgeTypes(Collections.singletonList(1.toByte)),
              ).asJava)
          ).asJava),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId2).
          setPartitions(List(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setPartitionMaxBytes(0)
              .setAcknowledgementBatches(List(
                new AcknowledgementBatch()
                  .setFirstOffset(43)
                  .setLastOffset(52)
                  .setAcknowledgeTypes(Collections.singletonList(1.toByte)),
              ).asJava),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
              .setPartitionMaxBytes(0)
              .setAcknowledgementBatches(List(
                new AcknowledgementBatch()
                  .setFirstOffset(17)
                  .setLastOffset(26)
                  .setAcknowledgeTypes(Collections.singletonList(1.toByte)),
              ).asJava)
          ).asJava),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId3).
          setPartitions(List(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setPartitionMaxBytes(0)
              .setAcknowledgementBatches(List(
                new AcknowledgementBatch()
                  .setFirstOffset(54)
                  .setLastOffset(93)
                  .setAcknowledgeTypes(Collections.singletonList(1.toByte)),
              ).asJava),
          ).asJava),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId4).
          setPartitions(List(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setPartitionMaxBytes(0)
              .setAcknowledgementBatches(List(
                new AcknowledgementBatch()
                  .setFirstOffset(10)
                  .setLastOffset(24)
                  .setAcknowledgeTypes(Collections.singletonList(1.toByte)),
              ).asJava),
          ).asJava),
      ).asJava)

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()
    topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
  }

  @Test
  def testHandleFetchFromShareFetchRequestSuccess(): Unit = {
    val shareSessionEpoch = 0
    val topicName1 = "foo1"
    val topicName2 = "foo2"
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)

    val memberId: Uuid = Uuid.ZERO_UUID
    val groupId: String = "group"

    val records_t1_p1 = memoryRecords(10, 0)
    val records_t2_p1 = memoryRecords(15, 0)
    val records_t2_p2 = memoryRecords(20, 0)

    val tp1 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0))
    val tp2 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0))
    val tp3 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1))

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        tp1 ->
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t1_p1)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ).asJava)),
        tp2 ->
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t2_p1)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(14)
                .setDeliveryCount(1)
            ).asJava)),
        tp3 ->
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t2_p2)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(19)
                .setDeliveryCount(1)
            ).asJava)),
      ).asJava)
    )

    val erroneousPartitions: util.Map[TopicIdPartition, ShareFetchResponseData.PartitionData] = new util.HashMap()

    val validPartitions: util.Map[TopicIdPartition, ShareFetchRequest.SharePartitionData] = new util.HashMap()
    validPartitions.put(
      tp1,
      new ShareFetchRequest.SharePartitionData(topicId1, partitionMaxBytes)
    )
    validPartitions.put(
      tp2,
      new ShareFetchRequest.SharePartitionData(topicId2, partitionMaxBytes)
    )
    validPartitions.put(
      tp3,
      new ShareFetchRequest.SharePartitionData(topicId2, partitionMaxBytes)
    )

    val erroneousAndValidPartitionData: ErroneousAndValidPartitionData =
      new ErroneousAndValidPartitionData(erroneousPartitions, validPartitions)

    var authorizedTopics: Set[String] = Set.empty[String]
    authorizedTopics = authorizedTopics + topicName1
    authorizedTopics = authorizedTopics + topicName2

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(shareSessionEpoch).
      setTopics(List(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId1).
          setPartitions(List(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setPartitionMaxBytes(partitionMaxBytes)
          ).asJava),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId2).
          setPartitions(List(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setPartitionMaxBytes(partitionMaxBytes),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
              .setPartitionMaxBytes(partitionMaxBytes)
          ).asJava),
      ).asJava)

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    // First share fetch request is to establish the share session with the broker.
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val fetchResult: Map[TopicIdPartition, ShareFetchResponseData.PartitionData] =
      kafkaApis.handleFetchFromShareFetchRequest(
        request,
        erroneousAndValidPartitionData,
        sharePartitionManager,
        authorizedTopics
      ).get()

    assertEquals(3, fetchResult.size)

    assertTrue(fetchResult.contains(tp1))
    val partitionData1: PartitionData = fetchResult.getOrElse(tp1, null)
    assertNotNull(partitionData1)

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t1_p1,
      expectedAcquiredRecords(0, 9, 1),
      partitionData1
    )

    assertTrue(fetchResult.contains(tp2))
    val partitionData2: PartitionData = fetchResult.getOrElse(tp2, null)
    assertNotNull(partitionData2)

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t2_p1,
      expectedAcquiredRecords(0, 14, 1),
      partitionData2
    )

    assertTrue(fetchResult.contains(tp3))
    val partitionData3: PartitionData = fetchResult.getOrElse(tp3, null)
    assertNotNull(partitionData3)

    compareResponsePartitions(
      1,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t2_p2,
      expectedAcquiredRecords(0, 19, 1),
      partitionData3
    )
  }

  @Test
  def testHandleShareFetchFromShareFetchRequestWithErroneousPartitions(): Unit = {
    val shareSessionEpoch = 0
    val topicName1 = "foo1"
    val topicName2 = "foo2"
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)

    val memberId: Uuid = Uuid.ZERO_UUID
    val groupId: String = "group"

    val records_t1_p1 = memoryRecords(10, 0)

    val tp1 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0))
    val tp2 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 1))
    val tp3 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0))

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        tp1 ->
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t1_p1)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    )

    val erroneousPartitions: util.Map[TopicIdPartition, ShareFetchResponseData.PartitionData] = new util.HashMap()
    erroneousPartitions.put(
      tp2,
      new ShareFetchResponseData.PartitionData()
        .setPartitionIndex(1)
        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
    )
    erroneousPartitions.put(
      tp3,
      new ShareFetchResponseData.PartitionData()
        .setPartitionIndex(0)
        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
    )

    val validPartitions: util.Map[TopicIdPartition, ShareFetchRequest.SharePartitionData] = new util.HashMap()
    validPartitions.put(
      tp1,
      new ShareFetchRequest.SharePartitionData(topicId1, partitionMaxBytes)
    )

    val erroneousAndValidPartitionData: ErroneousAndValidPartitionData =
      new ErroneousAndValidPartitionData(erroneousPartitions, validPartitions)

    var authorizedTopics: Set[String] = Set.empty[String]
    authorizedTopics = authorizedTopics + topicName1
    authorizedTopics = authorizedTopics + topicName2

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(shareSessionEpoch).
      setTopics(List(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId1).
          setPartitions(List(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setPartitionMaxBytes(partitionMaxBytes),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
              .setPartitionMaxBytes(partitionMaxBytes)
          ).asJava),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId2).
          setPartitions(List(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setPartitionMaxBytes(partitionMaxBytes),
          ).asJava),
      ).asJava)

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    // First share fetch request is to establish the share session with the broker.
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val fetchResult: Map[TopicIdPartition, ShareFetchResponseData.PartitionData] =
      kafkaApis.handleFetchFromShareFetchRequest(
        request,
        erroneousAndValidPartitionData,
        sharePartitionManager,
        authorizedTopics
      ).get()

    assertEquals(3, fetchResult.size)

    assertTrue(fetchResult.contains(tp1))
    val partitionData1: PartitionData = fetchResult.getOrElse(tp1, null)
    assertNotNull(partitionData1)

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t1_p1,
      expectedAcquiredRecords(0, 9, 1),
      partitionData1
    )

    assertTrue(fetchResult.contains(tp2))
    val partitionData2: PartitionData = fetchResult.getOrElse(tp2, null)
    assertNotNull(partitionData2)

    compareResponsePartitionsFetchError(
      1,
      Errors.UNKNOWN_TOPIC_OR_PARTITION.code,
      partitionData2
    )

    assertTrue(fetchResult.contains(tp3))
    val partitionData3: PartitionData = fetchResult.getOrElse(tp3, null)
    assertNotNull(partitionData3)

    compareResponsePartitionsFetchError(
      0,
      Errors.UNKNOWN_TOPIC_OR_PARTITION.code,
      partitionData3
    )
  }

  @Test
  def testHandleShareFetchFetchMessagesReturnErrorCode(): Unit = {
    val shareSessionEpoch = 0
    val topicName1 = "foo1"
    val topicName2 = "foo2"
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)

    val memberId: Uuid = Uuid.ZERO_UUID
    val groupId: String = "group"

    val emptyRecords = MemoryRecords.EMPTY

    val tp1 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0))
    val tp2 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0))
    val tp3 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1))

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        tp1 ->
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
            .setRecords(emptyRecords)
            .setAcquiredRecords(new util.ArrayList(List().asJava)),
        tp2 ->
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
            .setRecords(emptyRecords)
            .setAcquiredRecords(new util.ArrayList(List().asJava)),
        tp3 ->
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
            .setRecords(emptyRecords)
            .setAcquiredRecords(new util.ArrayList(List().asJava))
      ).asJava)
    )

    val erroneousPartitions: util.Map[TopicIdPartition, ShareFetchResponseData.PartitionData] = new util.HashMap()

    val validPartitions: util.Map[TopicIdPartition, ShareFetchRequest.SharePartitionData] = new util.HashMap()
    validPartitions.put(
      tp1,
      new ShareFetchRequest.SharePartitionData(topicId1, partitionMaxBytes)
    )
    validPartitions.put(
      tp2,
      new ShareFetchRequest.SharePartitionData(topicId2, partitionMaxBytes)
    )
    validPartitions.put(
      tp3,
      new ShareFetchRequest.SharePartitionData(topicId2, partitionMaxBytes)
    )

    val erroneousAndValidPartitionData: ErroneousAndValidPartitionData =
      new ErroneousAndValidPartitionData(erroneousPartitions, validPartitions)

    var authorizedTopics: Set[String] = Set.empty[String]
    authorizedTopics = authorizedTopics + topicName1
    authorizedTopics = authorizedTopics + topicName2

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(shareSessionEpoch).
      setTopics(List(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId1).
          setPartitions(List(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setPartitionMaxBytes(partitionMaxBytes)
          ).asJava),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId2).
          setPartitions(List(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setPartitionMaxBytes(partitionMaxBytes),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
              .setPartitionMaxBytes(partitionMaxBytes)
          ).asJava),
      ).asJava)

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    // First share fetch request is to establish the share session with the broker.
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val fetchResult: Map[TopicIdPartition, ShareFetchResponseData.PartitionData] =
      kafkaApis.handleFetchFromShareFetchRequest(
        request,
        erroneousAndValidPartitionData,
        sharePartitionManager,
        authorizedTopics
      ).get()

    assertEquals(3, fetchResult.size)

    assertTrue(fetchResult.contains(tp1))
    val partitionData1: PartitionData = fetchResult.getOrElse(tp1, null)
    assertNotNull(partitionData1)

    compareResponsePartitions(
      0,
      Errors.UNKNOWN_SERVER_ERROR.code,
      Errors.NONE.code,
      emptyRecords,
      Collections.emptyList[AcquiredRecords](),
      partitionData1
    )

    assertTrue(fetchResult.contains(tp2))
    val partitionData2: PartitionData = fetchResult.getOrElse(tp2, null)
    assertNotNull(partitionData2)

    compareResponsePartitions(
      0,
      Errors.UNKNOWN_SERVER_ERROR.code,
      Errors.NONE.code,
      emptyRecords,
      Collections.emptyList[AcquiredRecords](),
      partitionData2
    )

    assertTrue(fetchResult.contains(tp3))
    val partitionData3: PartitionData = fetchResult.getOrElse(tp3, null)
    assertNotNull(partitionData3)

    compareResponsePartitions(
      1,
      Errors.UNKNOWN_SERVER_ERROR.code,
      Errors.NONE.code,
      emptyRecords,
      Collections.emptyList[AcquiredRecords](),
      partitionData3
    )
  }

  @Test
  def testHandleShareFetchFromShareFetchRequestErrorTopicsInRequest(): Unit = {
    val shareSessionEpoch = 0
    val topicName1 = "foo1"
    val topicName2 = "foo2"
    val topicName3 = "foo3"
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()
    val topicId3 = Uuid.randomUuid()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)
    // topicName3 is not in the metadataCache.

    val memberId: Uuid = Uuid.ZERO_UUID
    val groupId: String = "group"

    val records1 = memoryRecords(10, 0)
    val records2 = memoryRecords(20, 0)

    val tp1 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0))
    val tp2 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0))
    val tp3 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1))
    val tp4 = new TopicIdPartition(topicId3, new TopicPartition(topicName3, 0))

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        tp2 ->
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
            .setRecords(records1)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ).asJava)),
        tp3 ->
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.NONE.code)
            .setRecords(records2)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(19)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    )

    val erroneousPartitions: util.Map[TopicIdPartition, ShareFetchResponseData.PartitionData] = new util.HashMap()

    val validPartitions: util.Map[TopicIdPartition, ShareFetchRequest.SharePartitionData] = new util.HashMap()
    validPartitions.put(
      tp1,
      new ShareFetchRequest.SharePartitionData(topicId1, partitionMaxBytes)
    )
    validPartitions.put(
      tp2,
      new ShareFetchRequest.SharePartitionData(topicId2, partitionMaxBytes)
    )
    validPartitions.put(
      tp3,
      new ShareFetchRequest.SharePartitionData(topicId2, partitionMaxBytes)
    )
    validPartitions.put(
      tp4,
      new ShareFetchRequest.SharePartitionData(topicId3, partitionMaxBytes)
    )

    val erroneousAndValidPartitionData: ErroneousAndValidPartitionData =
      new ErroneousAndValidPartitionData(erroneousPartitions, validPartitions)

    var authorizedTopics: Set[String] = Set.empty[String]
    // topicName1 is not in authorizedTopic.
    authorizedTopics = authorizedTopics + topicName2
    authorizedTopics = authorizedTopics + topicName3

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(shareSessionEpoch).
      setTopics(List(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId1).
          setPartitions(List(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setPartitionMaxBytes(partitionMaxBytes)
          ).asJava),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId2).
          setPartitions(List(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setPartitionMaxBytes(partitionMaxBytes),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
              .setPartitionMaxBytes(partitionMaxBytes)
          ).asJava),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId3).
          setPartitions(List(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setPartitionMaxBytes(partitionMaxBytes)
          ).asJava),
      ).asJava)

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val fetchResult: Map[TopicIdPartition, ShareFetchResponseData.PartitionData] =
      kafkaApis.handleFetchFromShareFetchRequest(
        request,
        erroneousAndValidPartitionData,
        sharePartitionManager,
        authorizedTopics
      ).get()

    assertEquals(4, fetchResult.size)

    assertTrue(fetchResult.contains(tp1))
    val partitionData1: PartitionData = fetchResult.getOrElse(tp1, null)
    assertNotNull(partitionData1)

    compareResponsePartitions(
      0,
      Errors.TOPIC_AUTHORIZATION_FAILED.code,
      Errors.NONE.code,
      null,
      Collections.emptyList[AcquiredRecords](),
      partitionData1
    )

    assertTrue(fetchResult.contains(tp2))
    val partitionData2: PartitionData = fetchResult.getOrElse(tp2, null)
    assertNotNull(partitionData2)

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records1,
      expectedAcquiredRecords(0, 9, 1),
      partitionData2
    )

    assertTrue(fetchResult.contains(tp3))
    val partitionData3: PartitionData = fetchResult.getOrElse(tp3, null)
    assertNotNull(partitionData3)

    compareResponsePartitions(
      1,
      Errors.NONE.code,
      Errors.NONE.code,
      records2,
      expectedAcquiredRecords(0, 19, 1),
      partitionData3
    )

    assertTrue(fetchResult.contains(tp4))
    val partitionData4: PartitionData = fetchResult.getOrElse(tp4, null)
    assertNotNull(partitionData4)

    compareResponsePartitions(
      0,
      Errors.UNKNOWN_TOPIC_OR_PARTITION.code,
      Errors.NONE.code,
      null,
      Collections.emptyList[AcquiredRecords](),
      partitionData4
    )
  }

  @Test
  def testHandleShareFetchOnBackingTopicIsRejectedWithInvalidTopic(): Unit = {
    // r19 ADV-A BLOCKER #136: a backing topic's physical log multiplexes records from N
    // logical topics; demux happens in LogicalFetchTranslator on the regular Fetch path.
    // SharePartitionManager does not apply LogicalFetchTranslator, so a share-fetch on the
    // backing name would deliver raw interleaved records (with ConcentrationHeaders) — a
    // cross-tenant payload leak. The handler must reject with INVALID_TOPIC_EXCEPTION at
    // the same level as Produce (KafkaApis.scala:553) and Fetch (KafkaApis.scala:1169).
    val backingTopic = "backing-topic"
    val backingTopicId = Uuid.randomUuid()
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(backingTopic, 1, topicId = backingTopicId)
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)
    // Intentionally do NOT stub isLogicalTopic — the backing guard must short-circuit before
    // it is reached. If the order regressed, the default (false) would let the request fall
    // through to sharePartitionManager.fetchMessages, which the never() verify below detects.

    val backingTip = new TopicIdPartition(backingTopicId, new TopicPartition(backingTopic, 0))
    val erroneousPartitions = new util.HashMap[TopicIdPartition, ShareFetchResponseData.PartitionData]()
    val validPartitions = new util.HashMap[TopicIdPartition, ShareFetchRequest.SharePartitionData]()
    validPartitions.put(backingTip, new ShareFetchRequest.SharePartitionData(backingTopicId, partitionMaxBytes))
    val erroneousAndValidPartitionData = new ErroneousAndValidPartitionData(erroneousPartitions, validPartitions)

    val authorizedTopics: Set[String] = Set(backingTopic)

    val shareFetchRequestData = new ShareFetchRequestData()
      .setGroupId("group")
      .setMemberId(Uuid.ZERO_UUID.toString)
      .setShareSessionEpoch(0)
      .setTopics(List(new ShareFetchRequestData.FetchTopic()
        .setTopicId(backingTopicId)
        .setPartitions(List(new ShareFetchRequestData.FetchPartition()
          .setPartitionIndex(0)
          .setPartitionMaxBytes(partitionMaxBytes)).asJava)).asJava)
    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )

    val fetchResult: Map[TopicIdPartition, ShareFetchResponseData.PartitionData] =
      kafkaApis.handleFetchFromShareFetchRequest(
        request,
        erroneousAndValidPartitionData,
        sharePartitionManager,
        authorizedTopics
      ).get()

    assertEquals(1, fetchResult.size)
    val partitionData = fetchResult.getOrElse(backingTip, null)
    assertNotNull(partitionData, "response must contain an entry for the rejected backing TIP")
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, partitionData.errorCode,
      "share-fetch on a backing topic must be rejected with INVALID_TOPIC_EXCEPTION")
    // SharePartitionManager must never see the backing-topic fetch — the guard short-circuits.
    verify(sharePartitionManager, never()).fetchMessages(any(), any(), any(), any())
  }

  @Test
  def testHandleShareAcknowledgeOnBackingTopicIsRejectedWithInvalidTopic(): Unit = {
    // r19 ADV-A BLOCKER #136 (ack side): symmetric with the share-fetch backing guard.
    // Acking a backing-topic offset would bind the share group's per-record state to backing
    // offsets that span multiple tenants, corrupting acquisition tracking for everyone sharing
    // that backing. Reject as INVALID_TOPIC_EXCEPTION (non-retriable) at the same level as the
    // share-fetch guard, before SharePartitionManager.acknowledge ever sees the request.
    val backingTopic = "backing-topic"
    val backingTopicId = Uuid.randomUuid()
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(backingTopic, 1, topicId = backingTopicId)
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)
    // isLogicalTopic is checked AFTER isBackingTopic and is intentionally unstubbed; default
    // (false) lets us catch any future ordering regression via the never() verify below.

    val backingTip = new TopicIdPartition(backingTopicId, new TopicPartition(backingTopic, 0))
    val acknowledgementData = mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]]()
    acknowledgementData += (backingTip -> util.Arrays.asList(
      new ShareAcknowledgementBatch(0, 9, Collections.singletonList(1.toByte))))

    val authorizedTopics: Set[String] = Set(backingTopic)
    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val ackResult = kafkaApis.handleAcknowledgements(
      acknowledgementData,
      erroneous,
      sharePartitionManager,
      authorizedTopics,
      "group",
      Uuid.randomUuid().toString
    ).get()

    assertEquals(1, ackResult.size)
    val partitionData = ackResult.getOrElse(backingTip, null)
    assertNotNull(partitionData, "ack response must contain an entry for the rejected backing TIP")
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, partitionData.errorCode,
      "share-acknowledge on a backing topic must be rejected with INVALID_TOPIC_EXCEPTION")
    verify(sharePartitionManager, never()).acknowledge(any(), any(), any())
  }

  private def compareResponsePartitions(expPartitionIndex: Int,
                                        expErrorCode: Short,
                                        expAckErrorCode: Short,
                                        expRecords: MemoryRecords,
                                        expAcquiredRecords: util.List[AcquiredRecords],
                                        partitionData: PartitionData): Unit = {
    assertEquals(expPartitionIndex, partitionData.partitionIndex)
    assertEquals(expErrorCode, partitionData.errorCode)
    assertEquals(expAckErrorCode, partitionData.acknowledgeErrorCode)
    assertEquals(expRecords, partitionData.records)
    assertArrayEquals(expAcquiredRecords.toArray(), partitionData.acquiredRecords.toArray())
  }

  private def compareResponsePartitionsFetchError(
                                                   expPartitionIndex: Int,
                                                   expErrorCode: Short,
                                                   partitionData: PartitionData
                                                 ): Unit = {
    assertEquals(expPartitionIndex, partitionData.partitionIndex)
    assertEquals(expErrorCode, partitionData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestSuccessWithAcknowledgements(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.randomUuid()

    val records1 = memoryRecords(10, 0)
    val records2 = memoryRecords(10, 10)

    val groupId = "group"

    when(sharePartitionManager.fetchMessages(any(), any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setAcknowledgeErrorCode(Errors.NONE.code)
            .setRecords(records1)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    ).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setAcknowledgeErrorCode(Errors.NONE.code)
            .setRecords(records2)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(10)
                .setLastOffset(19)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    )

    val cachedSharePartitions = new ImplicitLinkedHashCollection[CachedSharePartition]
    cachedSharePartitions.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId, new TopicPartition(topicName, 0)), new ShareFetchRequest.SharePartitionData(topicId, partitionMaxBytes), false
    ))

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(new ShareRequestMetadata(memberId, 0), Map(
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchRequest.SharePartitionData(topicId, partitionMaxBytes)
      ).asJava)
    ).thenReturn(new ShareSessionContext(new ShareRequestMetadata(memberId, 1), new ShareSession(
      new ShareSessionKey(groupId, memberId), cachedSharePartitions, 0L, 0L, 2))
    )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
      ).asJava)
    )

    var shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(0).
      setTopics(List(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setPartitionMaxBytes(40000)).asJava)).asJava)

    var shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    var request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    var response = verifyNoThrottling[ShareFetchResponse](request)
    var responseData = response.data()
    var topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).errorCode)
    assertEquals(records1, topicResponses.get(0).partitions.get(0).records)
    assertArrayEquals(expectedAcquiredRecords(0, 9, 1).toArray(), topicResponses.get(0).partitions.get(0).acquiredRecords.toArray())

    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId("group").
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(List(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setPartitionMaxBytes(40000)
            .setAcknowledgementBatches(List(
              new AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()
    topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).errorCode)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).acknowledgeErrorCode)
    assertEquals(records2, topicResponses.get(0).partitions.get(0).records)
    assertArrayEquals(expectedAcquiredRecords(10, 19, 1).toArray(), topicResponses.get(0).partitions.get(0).acquiredRecords.toArray())
  }

  @Test
  def testHandleShareFetchNewGroupCoordinatorDisabled(): Unit = {
    val topicId = Uuid.randomUuid()
    val memberId: Uuid = Uuid.randomUuid()
    val groupId = "group"

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(List(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setPartitionMaxBytes(40000)
            .setAcknowledgementBatches(List(
              new AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG -> "false",
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)

    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()

    assertEquals(Errors.UNSUPPORTED_VERSION.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchShareGroupDisabled(): Unit = {
    val topicId = Uuid.randomUuid()
    val memberId: Uuid = Uuid.randomUuid()
    val groupId = "group"

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(List(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setPartitionMaxBytes(40000)
            .setAcknowledgementBatches(List(
              new AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "false"),
      )
    kafkaApis.handleShareFetchRequest(request)

    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()

    assertEquals(Errors.UNSUPPORTED_VERSION.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestGroupAuthorizationError(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId("group").
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(List(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(partitionIndex)
            .setPartitionMaxBytes(40000)
            .setAcknowledgementBatches(List(
              new ShareFetchRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any(), any())).thenReturn(List[AuthorizationResult](
      AuthorizationResult.DENIED
    ).asJava)

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      authorizer = Option(authorizer),
      )
    kafkaApis.handleShareFetchRequest(request)

    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()

    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestReleaseAcquiredRecordsThrowError(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    val groupId = "group"

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
      ).asJava)
    )

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any())).thenReturn(
      new FinalContext()
    )

    when(sharePartitionManager.releaseSession(any(), any())).thenReturn(
      FutureUtils.failedFuture[util.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]](Errors.UNKNOWN_SERVER_ERROR.exception())
    )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(ShareRequestMetadata.FINAL_EPOCH).
      setTopics(List(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setAcknowledgementBatches(List(
              new AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)).asJava)

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).errorCode)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).acknowledgeErrorCode)
    assertNull(topicResponses.get(0).partitions.get(0).records)
    assertEquals(0, topicResponses.get(0).partitions.get(0).acquiredRecords.toArray().length)
  }

  @Test
  def testHandleShareAcknowledgeRequestSuccess(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.randomUuid()

    val groupId = "group"

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
      ).asJava)
    )

    doNothing().when(sharePartitionManager).acknowledgeSessionUpdate(any(), any())

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(List(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(List(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareAcknowledgeRequest(request)
    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).errorCode)
  }

  @Test
  def testHandleShareAcknowledgeNewGroupCoordinatorDisabled(): Unit = {
    val topicId = Uuid.randomUuid()
    val memberId: Uuid = Uuid.randomUuid()
    val groupId = "group"

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId.toString)
      .setShareSessionEpoch(1)
      .setTopics(List(new ShareAcknowledgeRequestData.AcknowledgeTopic()
        .setTopicId(topicId)
        .setPartitions(List(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(0)
            .setAcknowledgementBatches(List(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData).build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG -> "false",
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareAcknowledgeRequest(request)

    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()

    assertEquals(Errors.UNSUPPORTED_VERSION.code, responseData.errorCode)
  }

  @Test
  def testHandleShareAcknowledgeShareGroupDisabled(): Unit = {
    val topicId = Uuid.randomUuid()
    val memberId: Uuid = Uuid.randomUuid()
    val groupId = "group"

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId.toString)
      .setShareSessionEpoch(1)
      .setTopics(List(new ShareAcknowledgeRequestData.AcknowledgeTopic()
        .setTopicId(topicId)
        .setPartitions(List(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(0)
            .setAcknowledgementBatches(List(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData).build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "false"),
      )
    kafkaApis.handleShareAcknowledgeRequest(request)

    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()

    assertEquals(Errors.UNSUPPORTED_VERSION.code, responseData.errorCode)
  }

  @Test
  def testHandleShareAcknowledgeRequestGroupAuthorizationError(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId("group").
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(List(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(List(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any(), any())).thenReturn(List[AuthorizationResult](
      AuthorizationResult.DENIED
    ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      authorizer = Option(authorizer),
      )
    kafkaApis.handleShareAcknowledgeRequest(request)

    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()

    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, responseData.errorCode)
  }

  @Test
  def testHandleShareAcknowledgeRequestInvalidRequestOnInitialEpoch(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    when(sharePartitionManager.acknowledgeSessionUpdate(any(), any())).thenThrow(
      Errors.INVALID_SHARE_SESSION_EPOCH.exception
    )

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId("group").
      setMemberId(memberId.toString).
      setShareSessionEpoch(0).
      setTopics(List(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(List(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareAcknowledgeRequest(request)

    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()

    assertEquals(Errors.INVALID_SHARE_SESSION_EPOCH.code, responseData.errorCode)
  }

  @Test
  def testHandleShareAcknowledgeRequestSessionNotFound(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    when(sharePartitionManager.acknowledgeSessionUpdate(any(), any())).thenThrow(
      Errors.SHARE_SESSION_NOT_FOUND.exception
    )

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId("group").
      setMemberId(memberId.toString).
      setShareSessionEpoch(0).
      setTopics(List(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(List(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareAcknowledgeRequest(request)

    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()

    assertEquals(Errors.SHARE_SESSION_NOT_FOUND.code, responseData.errorCode)
  }

  @Test
  def testHandleShareAcknowledgeRequestBatchValidationError(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val groupId: String = "group"
    val memberId: Uuid = Uuid.ZERO_UUID

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    doNothing().when(sharePartitionManager).acknowledgeSessionUpdate(any(), any())

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(List(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(List(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(10)
                .setLastOffset(4) // end offset is less than base offset
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareAcknowledgeRequest(request)

    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.INVALID_REQUEST.code, topicResponses.get(0).partitions.get(0).errorCode)
  }

  @Test
  def testHandleShareAcknowledgeResponseContainsNewLeaderOnNotLeaderOrFollower(): Unit = {
    val topicId = Uuid.randomUuid()
    val topicName = "foo"
    val partitionIndex = 0
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex))
    val topicPartition = topicIdPartition.topicPartition
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicPartition.topic, numPartitions = 1, numBrokers = 3, topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    val partition = mock(classOf[Partition])
    val newLeaderId = 2
    val newLeaderEpoch = 5

    when(replicaManager.getPartitionOrError(topicPartition)).thenAnswer(_ => Right(partition))
    when(partition.leaderReplicaIdOpt).thenAnswer(_ => Some(newLeaderId))
    when(partition.getLeaderEpoch).thenAnswer(_ => newLeaderEpoch)

    doNothing().when(sharePartitionManager).acknowledgeSessionUpdate(any(), any())

    when(sharePartitionManager.acknowledge(
      any(),
      any(),
      any()
    )).thenReturn(CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
      new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
        new ShareAcknowledgeResponseData.PartitionData()
          .setPartitionIndex(partitionIndex)
          .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())
    ).asJava))

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId("group").
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(List(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(0)
            .setAcknowledgementBatches(List(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(10)
                .setLastOffset(20)
                .setAcknowledgeTypes(util.Arrays.asList(1.toByte,1.toByte,0.toByte,1.toByte,1.toByte,1.toByte,1.toByte,1.toByte,1.toByte,1.toByte,1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareAcknowledgeRequest(request)

    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, topicResponses.get(0).partitions.get(0).errorCode)
    assertEquals(newLeaderId, topicResponses.get(0).partitions.get(0).currentLeader.leaderId)
    assertEquals(newLeaderEpoch, topicResponses.get(0).partitions.get(0).currentLeader.leaderEpoch)
    assertEquals(2, responseData.nodeEndpoints.asScala.head.nodeId)
  }

  @Test
  def testHandleShareAcknowledgeRequestAcknowledgeThrowsError(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.randomUuid()

    val groupId = "group"

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      FutureUtils.failedFuture[util.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]](Errors.UNKNOWN_SERVER_ERROR.exception())
    )

    doNothing().when(sharePartitionManager).acknowledgeSessionUpdate(any(), any())

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(List(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(List(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareAcknowledgeRequest(request)
    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()

    assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, responseData.errorCode)
  }

  @Test
  def testHandleShareAcknowledgeRequestSuccessOnFinalEpoch(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.randomUuid()

    val groupId = "group"

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
      ).asJava)
    )

    doNothing().when(sharePartitionManager).acknowledgeSessionUpdate(any(), any())

    when(sharePartitionManager.releaseSession(any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
      ).asJava)
    )

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(ShareRequestMetadata.FINAL_EPOCH).
      setTopics(List(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(List(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareAcknowledgeRequest(request)
    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).errorCode)
  }

  @Test
  def testHandleShareAcknowledgeRequestReleaseAcquiredRecordsThrowError(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.randomUuid()

    val groupId = "group"

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
      ).asJava)
    )

    doNothing().when(sharePartitionManager).acknowledgeSessionUpdate(any(), any())

    when(sharePartitionManager.releaseSession(any(), any())).thenReturn(
      FutureUtils.failedFuture[util.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]](Errors.UNKNOWN_SERVER_ERROR.exception())
    )

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(ShareRequestMetadata.FINAL_EPOCH).
      setTopics(List(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(List(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareAcknowledgeRequest(request)
    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).errorCode)
  }

  private def expectedAcquiredRecords(firstOffset: Long, lastOffset: Long, deliveryCount: Int): util.List[AcquiredRecords] = {
    val acquiredRecordsList: util.List[AcquiredRecords] = new util.ArrayList()
    acquiredRecordsList.add(new AcquiredRecords()
      .setFirstOffset(firstOffset)
      .setLastOffset(lastOffset)
      .setDeliveryCount(deliveryCount.toShort))
    acquiredRecordsList
  }

  @Test
  def testGetAcknowledgeBatchesFromShareFetchRequest(): Unit = {
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId("group").
      setMemberId(Uuid.randomUuid().toString).
      setShareSessionEpoch(0).
      setTopics(List(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId1).
          setPartitions(List(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setPartitionMaxBytes(40000)
              .setAcknowledgementBatches(List(
                new ShareFetchRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(Collections.singletonList(1.toByte)),
                new ShareFetchRequestData.AcknowledgementBatch()
                  .setFirstOffset(10)
                  .setLastOffset(17)
                  .setAcknowledgeTypes(Collections.singletonList(1.toByte))
              ).asJava),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
              .setPartitionMaxBytes(40000)
              .setAcknowledgementBatches(List(
                new ShareFetchRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(Collections.singletonList(2.toByte))
              ).asJava)
          ).asJava),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId2).
          setPartitions(List(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setPartitionMaxBytes(40000)
              .setAcknowledgementBatches(List(
                new ShareFetchRequestData.AcknowledgementBatch()
                  .setFirstOffset(24)
                  .setLastOffset(65)
                  .setAcknowledgeTypes(Collections.singletonList(3.toByte))
              ).asJava),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
              .setPartitionMaxBytes(40000)
          ).asJava)
      ).asJava)
    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val topicNames = new util.HashMap[Uuid, String]
    topicNames.put(topicId1, "foo1")
    topicNames.put(topicId2, "foo2")
    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val acknowledgeBatches = kafkaApis.getAcknowledgeBatchesFromShareFetchRequest(shareFetchRequest, topicNames, erroneous)

    assertEquals(4, acknowledgeBatches.size)
    assertTrue(acknowledgeBatches.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))))
    assertTrue(acknowledgeBatches.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 1))))
    assertTrue(acknowledgeBatches.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 0))))

    assertTrue(compareAcknowledgementBatches(0, 9, 1, acknowledgeBatches.getOrElse(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)), null).get(0)))
    assertTrue(compareAcknowledgementBatches(10, 17, 1, acknowledgeBatches.getOrElse(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)), null).get(1)))
    assertTrue(compareAcknowledgementBatches(0, 9, 2, acknowledgeBatches.getOrElse(new TopicIdPartition(topicId1, new TopicPartition("foo1", 1)), null).get(0)))
    assertTrue(compareAcknowledgementBatches(24, 65, 3, acknowledgeBatches.getOrElse(new TopicIdPartition(topicId2, new TopicPartition("foo2", 0)), null).get(0)))
  }

  @Test
  def testGetAcknowledgeBatchesFromShareFetchRequestError(): Unit = {
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId("group").
      setMemberId(Uuid.randomUuid().toString).
      setShareSessionEpoch(0).
      setTopics(List(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId1).
          setPartitions(List(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setPartitionMaxBytes(40000)
              .setAcknowledgementBatches(List(
                new ShareFetchRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(Collections.singletonList(7.toByte)) // wrong acknowledgement type here (can only be 0, 1, 2 or 3)
              ).asJava),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
              .setPartitionMaxBytes(40000)
              .setAcknowledgementBatches(List(
                new ShareFetchRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(Collections.emptyList()) // wrong acknowledgement type here (can only be 0, 1, 2 or 3)
              ).asJava)
          ).asJava),
        new ShareFetchRequestData.FetchTopic()
          .setTopicId(topicId2)
          .setPartitions(List(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setPartitionMaxBytes(40000)
              .setAcknowledgementBatches(List(
                new ShareFetchRequestData.AcknowledgementBatch()
                  .setFirstOffset(24)
                  .setLastOffset(65)
                  .setAcknowledgeTypes(Collections.singletonList(3.toByte))
              ).asJava)
          ).asJava)
      ).asJava)
    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val topicIdNames = new util.HashMap[Uuid, String]
    topicIdNames.put(topicId1, "foo1") // topicId2 is not present in topicIdNames
    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val acknowledgeBatches = kafkaApis.getAcknowledgeBatchesFromShareFetchRequest(shareFetchRequest, topicIdNames, erroneous)
    val erroneousTopicIdPartitions = kafkaApis.validateAcknowledgementBatches(acknowledgeBatches, erroneous)

    assertEquals(3, erroneous.size)
    assertEquals(2, erroneousTopicIdPartitions.size)
    assertTrue(erroneous.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))))
    assertTrue(erroneous.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 1))))
    assertTrue(erroneous.contains(new TopicIdPartition(topicId2, new TopicPartition(null, 0))))
    assertEquals(Errors.INVALID_REQUEST.code, erroneous(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))).errorCode)
    assertEquals(Errors.INVALID_REQUEST.code, erroneous(new TopicIdPartition(topicId1, new TopicPartition("foo1", 1))).errorCode)
    assertEquals(Errors.UNKNOWN_TOPIC_ID.code, erroneous(new TopicIdPartition(topicId2, new TopicPartition(null, 0))).errorCode)
  }

  @Test
  def testGetAcknowledgeBatchesFromShareAcknowledgeRequest(): Unit = {
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId("group").
      setMemberId(Uuid.randomUuid().toString).
      setShareSessionEpoch(0).
      setTopics(List(
        new ShareAcknowledgeRequestData.AcknowledgeTopic().
          setTopicId(topicId1).
          setPartitions(List(
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(List(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(Collections.singletonList(1.toByte)),
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(10)
                  .setLastOffset(17)
                  .setAcknowledgeTypes(Collections.singletonList(1.toByte))
              ).asJava),
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(1)
              .setAcknowledgementBatches(List(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(Collections.singletonList(2.toByte))
              ).asJava)
          ).asJava),
        new ShareAcknowledgeRequestData.AcknowledgeTopic().
          setTopicId(topicId2).
          setPartitions(List(
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(List(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(24)
                  .setLastOffset(65)
                  .setAcknowledgeTypes(Collections.singletonList(3.toByte))
              ).asJava)
          ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val topicNames = new util.HashMap[Uuid, String]
    topicNames.put(topicId1, "foo1")
    topicNames.put(topicId2, "foo2")
    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val acknowledgeBatches = kafkaApis.getAcknowledgeBatchesFromShareAcknowledgeRequest(shareAcknowledgeRequest, topicNames, erroneous)

    assertEquals(3, acknowledgeBatches.size)
    assertTrue(acknowledgeBatches.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))))
    assertTrue(acknowledgeBatches.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 1))))
    assertTrue(acknowledgeBatches.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 0))))

    assertTrue(compareAcknowledgementBatches(0, 9, 1, acknowledgeBatches.getOrElse(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)), null).get(0)))
    assertTrue(compareAcknowledgementBatches(10, 17, 1, acknowledgeBatches.getOrElse(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)), null).get(1)))
    assertTrue(compareAcknowledgementBatches(0, 9, 2, acknowledgeBatches.getOrElse(new TopicIdPartition(topicId1, new TopicPartition("foo1", 1)), null).get(0)))
    assertTrue(compareAcknowledgementBatches(24, 65, 3, acknowledgeBatches.getOrElse(new TopicIdPartition(topicId2, new TopicPartition("foo2", 0)), null).get(0)))
  }

  @Test
  def testGetAcknowledgeBatchesFromShareAcknowledgeRequestError(): Unit = {
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId("group").
      setMemberId(Uuid.randomUuid().toString).
      setShareSessionEpoch(0).
      setTopics(List(
        new ShareAcknowledgeRequestData.AcknowledgeTopic().
          setTopicId(topicId1).
          setPartitions(List(
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(List(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(Collections.singletonList(7.toByte)) // wrong acknowledgement type here (can only be 0, 1, 2 or 3)
              ).asJava),
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(1)
              .setAcknowledgementBatches(List(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(Collections.emptyList()) // wrong acknowledgement type here (can only be 0, 1, 2 or 3)
              ).asJava)
          ).asJava),
        new ShareAcknowledgeRequestData.AcknowledgeTopic().
          setTopicId(topicId2).
          setPartitions(List(
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(List(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(24)
                  .setLastOffset(65)
                  .setAcknowledgeTypes(Collections.singletonList(3.toByte))
              ).asJava)
          ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val topicIdNames = new util.HashMap[Uuid, String]
    topicIdNames.put(topicId1, "foo1") // topicId2 not present in topicIdNames
    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val acknowledgeBatches = kafkaApis.getAcknowledgeBatchesFromShareAcknowledgeRequest(shareAcknowledgeRequest, topicIdNames, erroneous)
    val erroneousTopicIdPartitions = kafkaApis.validateAcknowledgementBatches(acknowledgeBatches, erroneous)

    assertEquals(3, erroneous.size)
    assertEquals(2, erroneousTopicIdPartitions.size)
    assertTrue(erroneous.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))))
    assertTrue(erroneous.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 1))))
    assertTrue(erroneous.contains(new TopicIdPartition(topicId2, new TopicPartition(null, 0))))

    assertTrue(erroneous.contains(new TopicIdPartition(topicId2, new TopicPartition(null, 0))))
    assertEquals(Errors.INVALID_REQUEST.code, erroneous(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))).errorCode)
    assertEquals(Errors.INVALID_REQUEST.code, erroneous(new TopicIdPartition(topicId1, new TopicPartition("foo1", 1))).errorCode)
    assertEquals(Errors.UNKNOWN_TOPIC_ID.code, erroneous(new TopicIdPartition(topicId2, new TopicPartition(null, 0))).errorCode)
  }

  @Test
  def testHandleAcknowledgementsSuccess(): Unit = {
    val groupId = "group"

    val topicName1 = "foo1"
    val topicName2 = "foo2"

    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()
    val memberId = Uuid.randomUuid()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)

    val tp1 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0))
    val tp2 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0))
    val tp3 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1))

    when(sharePartitionManager.acknowledge(any(), any(), any()))
      .thenReturn(CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        tp1 ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        tp2 ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        tp3 ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.NONE.code)
      ).asJava))

    val acknowledgementData = mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]]()

    acknowledgementData += (tp1 -> util.Arrays.asList(
      new ShareAcknowledgementBatch(0, 9, Collections.singletonList(1.toByte)),
      new ShareAcknowledgementBatch(10, 19, Collections.singletonList(2.toByte))
    ))
    acknowledgementData += (tp2 -> util.Arrays.asList(
      new ShareAcknowledgementBatch(5, 19, Collections.singletonList(2.toByte))
    ))
    acknowledgementData += (tp3 -> util.Arrays.asList(
      new ShareAcknowledgementBatch(34, 56, Collections.singletonList(1.toByte))
    ))

    val authorizedTopics: Set[String] = Set(topicName1, topicName2)

    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val ackResult = kafkaApis.handleAcknowledgements(
      acknowledgementData,
      erroneous,
      sharePartitionManager,
      authorizedTopics,
      groupId,
      memberId.toString
    ).get()

    assertEquals(3, ackResult.size)
    assertTrue(ackResult.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))))
    assertTrue(ackResult.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 0))))
    assertTrue(ackResult.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 1))))

    assertTrue(compareAcknowledgeResponsePartitionData(0, Errors.NONE.code, ackResult.getOrElse(
      new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)), null)))
    assertTrue(compareAcknowledgeResponsePartitionData(0, Errors.NONE.code, ackResult.getOrElse(
      new TopicIdPartition(topicId2, new TopicPartition("foo2", 0)), null)))
    assertTrue(compareAcknowledgeResponsePartitionData(1, Errors.NONE.code, ackResult.getOrElse(
      new TopicIdPartition(topicId2, new TopicPartition("foo2", 1)), null)))
  }

  @Test
  def testHandleAcknowledgementsInvalidAcknowledgementBatches(): Unit = {
    val groupId = "group"

    val topicName1 = "foo1"
    val topicName2 = "foo2"

    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()
    val memberId = Uuid.randomUuid()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)

    val tp1 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0))
    val tp2 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0))
    val tp3 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1))

    when(sharePartitionManager.acknowledge(any(), any(), any()))
      .thenReturn(CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId2, new TopicPartition("foo2", 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId2, new TopicPartition("foo2", 1)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.NONE.code)
      ).asJava))

    val acknowledgementData = mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]]()
    acknowledgementData += (tp1 -> util.Arrays.asList(
      new ShareAcknowledgementBatch(39, 24, Collections.singletonList(1.toByte)), // this is an invalid batch because last offset is less than base offset
      new ShareAcknowledgementBatch(43, 56, Collections.singletonList(2.toByte))
    ))
    acknowledgementData += (tp2 -> util.Arrays.asList(
      new ShareAcknowledgementBatch(5, 19, util.Arrays.asList(0.toByte, 2.toByte))
    ))
    acknowledgementData += (tp3 -> util.Arrays.asList(
      new ShareAcknowledgementBatch(34, 56, Collections.singletonList(1.toByte)),
      new ShareAcknowledgementBatch(10, 19, Collections.singletonList(1.toByte)) // this is an invalid batch because start is offset is less than previous end offset
    ))

    val authorizedTopics: Set[String] = Set(topicName1, topicName2)

    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val ackResult = kafkaApis.handleAcknowledgements(
      acknowledgementData,
      erroneous,
      sharePartitionManager,
      authorizedTopics,
      groupId,
      memberId.toString
    ).get()

    assertEquals(3, ackResult.size)
    assertTrue(ackResult.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))))
    assertTrue(ackResult.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 0))))
    assertTrue(ackResult.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 1))))

    assertTrue(compareAcknowledgeResponsePartitionData(0, Errors.INVALID_REQUEST.code, ackResult.getOrElse(
      new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)), null)))
    assertTrue(compareAcknowledgeResponsePartitionData(0, Errors.INVALID_REQUEST.code, ackResult.getOrElse(
      new TopicIdPartition(topicId2, new TopicPartition("foo2", 0)), null)))
    assertTrue(compareAcknowledgeResponsePartitionData(1, Errors.INVALID_REQUEST.code, ackResult.getOrElse(
      new TopicIdPartition(topicId2, new TopicPartition("foo2", 1)), null)))
  }

  @Test
  def testHandleAcknowledgementsUnauthorizedTopics(): Unit = {
    val groupId = "group"

    val topicName1 = "foo1"
    val topicName2 = "foo2"

    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()
    val memberId = Uuid.randomUuid()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    // Topic with id topicId1 is not present in Metadata Cache
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)

    val tp1 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0))
    val tp2 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0))
    val tp3 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1))

    when(sharePartitionManager.acknowledge(any(), any(), any()))
      .thenReturn(CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId2, new TopicPartition("foo2", 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId2, new TopicPartition("foo2", 1)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.NONE.code)
      ).asJava))

    val acknowledgementData = mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]]()

    acknowledgementData += (tp1 -> util.Arrays.asList(
      new ShareAcknowledgementBatch(24, 39, Collections.singletonList(1.toByte)),
      new ShareAcknowledgementBatch(43, 56, Collections.singletonList(2.toByte))
    ))
    acknowledgementData += (tp2 -> util.Arrays.asList(
      new ShareAcknowledgementBatch(5, 19, Collections.singletonList(2.toByte))
    ))
    acknowledgementData += (tp3 -> util.Arrays.asList(
      new ShareAcknowledgementBatch(34, 56, Collections.singletonList(1.toByte)),
      new ShareAcknowledgementBatch(67, 87, Collections.singletonList(1.toByte))
    ))

    val authorizedTopics: Set[String] = Set(topicName1) // Topic with topicId2 is not authorized

    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val ackResult = kafkaApis.handleAcknowledgements(
      acknowledgementData,
      erroneous,
      sharePartitionManager,
      authorizedTopics,
      groupId,
      memberId.toString
    ).get()

    assertEquals(3, ackResult.size)
    assertTrue(ackResult.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))))
    assertTrue(ackResult.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 0))))
    assertTrue(ackResult.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 1))))

    assertTrue(compareAcknowledgeResponsePartitionData(0, Errors.UNKNOWN_TOPIC_OR_PARTITION.code, ackResult.getOrElse(
      new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)), null)))
    assertTrue(compareAcknowledgeResponsePartitionData(0, Errors.TOPIC_AUTHORIZATION_FAILED.code, ackResult.getOrElse(
      new TopicIdPartition(topicId2, new TopicPartition("foo2", 0)), null)))
    assertTrue(compareAcknowledgeResponsePartitionData(1, Errors.TOPIC_AUTHORIZATION_FAILED.code, ackResult.getOrElse(
      new TopicIdPartition(topicId2, new TopicPartition("foo2", 1)), null)))
  }

  @Test
  def testHandleAcknowledgementsWithErroneous(): Unit = {
    val groupId = "group"

    val topicName1 = "foo1"
    val topicName2 = "foo2"

    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()
    val memberId = Uuid.randomUuid()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)

    val tp1 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0))
    val tp2 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0))
    val tp3 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1))

    when(sharePartitionManager.acknowledge(any(), any(), any()))
      .thenReturn(CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        tp1 ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        tp2 ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
      ).asJava))

    val acknowledgementData = mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]]()

    acknowledgementData += (tp1 -> util.Arrays.asList(
      new ShareAcknowledgementBatch(0, 9, Collections.singletonList(1.toByte)),
      new ShareAcknowledgementBatch(10, 19, Collections.singletonList(2.toByte))
    ))
    acknowledgementData += (tp2 -> util.Arrays.asList(
      new ShareAcknowledgementBatch(5, 19, Collections.singletonList(2.toByte))
    ))

    val authorizedTopics: Set[String] = Set(topicName1, topicName2)

    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    erroneous += (tp3 -> ShareAcknowledgeResponse.partitionResponse(tp3, Errors.UNKNOWN_TOPIC_ID))

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val ackResult = kafkaApis.handleAcknowledgements(
      acknowledgementData,
      erroneous,
      sharePartitionManager,
      authorizedTopics,
      groupId,
      memberId.toString
    ).get()

    assertEquals(3, ackResult.size)
    assertTrue(ackResult.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))))
    assertTrue(ackResult.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 0))))
    assertTrue(ackResult.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 1))))

    assertTrue(compareAcknowledgeResponsePartitionData(0, Errors.NONE.code, ackResult.getOrElse(
      new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)), null)))
    assertTrue(compareAcknowledgeResponsePartitionData(0, Errors.NONE.code, ackResult.getOrElse(
      new TopicIdPartition(topicId2, new TopicPartition("foo2", 0)), null)))
    assertTrue(compareAcknowledgeResponsePartitionData(1, Errors.UNKNOWN_TOPIC_ID.code, ackResult.getOrElse(
      new TopicIdPartition(topicId2, new TopicPartition("foo2", 1)), null)))
  }

  @Test
  def testProcessShareAcknowledgeResponse(): Unit = {
    val groupId = "group"

    val memberId = Uuid.randomUuid()

    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    val responseAcknowledgeData: mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData] = mutable.Map()
    responseAcknowledgeData += (new TopicIdPartition(topicId1, new TopicPartition("foo", 0)) ->
      new ShareAcknowledgeResponseData.PartitionData().setPartitionIndex(0).setErrorCode(Errors.NONE.code))
    responseAcknowledgeData += (new TopicIdPartition(topicId1, new TopicPartition("foo", 1)) ->
      new ShareAcknowledgeResponseData.PartitionData().setPartitionIndex(0).setErrorCode(Errors.INVALID_REQUEST.code))
    responseAcknowledgeData += (new TopicIdPartition(topicId2, new TopicPartition("bar", 0)) ->
      new ShareAcknowledgeResponseData.PartitionData().setPartitionIndex(0).setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code))
    responseAcknowledgeData += (new TopicIdPartition(topicId2, new TopicPartition("bar", 1)) ->
      new ShareAcknowledgeResponseData.PartitionData().setPartitionIndex(0).setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code))

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(List(
        new ShareAcknowledgeRequestData.AcknowledgeTopic().
          setTopicId(topicId1).
          setPartitions(List(
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(List(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(Collections.singletonList(1.toByte))
              ).asJava),
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(1)
              .setAcknowledgementBatches(List(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(Collections.singletonList(1.toByte))
              ).asJava)
          ).asJava),
        new ShareAcknowledgeRequestData.AcknowledgeTopic().
          setTopicId(topicId2).
          setPartitions(List(
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(List(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(Collections.singletonList(1.toByte))
              ).asJava),
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(1)
              .setAcknowledgementBatches(List(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(Collections.singletonList(1.toByte))
              ).asJava)
          ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val response = kafkaApis.processShareAcknowledgeResponse(responseAcknowledgeData, request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(2, topicResponses.size())

    val topicResponsesScala = topicResponses.asScala.toList
    val topicResponsesMap: Map[Uuid, ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponse] = topicResponsesScala.map(topic => topic.topicId -> topic).toMap
    assertTrue(topicResponsesMap.contains(topicId1))

    val topicIdResponse1: ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponse = topicResponsesMap.getOrElse(topicId1, null)
    assertEquals(2, topicIdResponse1.partitions().size())

    val partitionResponses1 = topicIdResponse1.partitions().asScala.toList
    val partitionResponsesMap1: Map[Int, ShareAcknowledgeResponseData.PartitionData] = partitionResponses1.map(partition => partition.partitionIndex -> partition).toMap
    assertTrue(partitionResponsesMap1.contains(0))
    assertTrue(partitionResponsesMap1.contains(1))
    assertTrue(partitionResponsesMap1.getOrElse(0, null).errorCode == Errors.NONE.code)
    assertTrue(partitionResponsesMap1.getOrElse(1, null).errorCode == Errors.INVALID_REQUEST.code)

    assertTrue(topicResponsesMap.contains(topicId2))

    val topicIdResponse2: ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponse = topicResponsesMap.getOrElse(topicId2, null)
    assertEquals(2, topicIdResponse2.partitions().size())

    val partitionResponses2 = topicIdResponse2.partitions().asScala.toList
    val partitionResponsesMap2: Map[Int, ShareAcknowledgeResponseData.PartitionData] = partitionResponses2.map(partition => partition.partitionIndex -> partition).toMap
    assertTrue(partitionResponsesMap2.contains(0))
    assertTrue(partitionResponsesMap2.contains(1))
    assertTrue(partitionResponsesMap2.getOrElse(0, null).errorCode == Errors.TOPIC_AUTHORIZATION_FAILED.code)
    assertTrue(partitionResponsesMap2.getOrElse(1, null).errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
  }

  private def compareAcknowledgementBatches(baseOffset: Long,
                                            endOffset: Long,
                                            acknowledgementType: Byte,
                                            acknowledgementBatch: ShareAcknowledgementBatch
                                           ): Boolean = {
    if (baseOffset == acknowledgementBatch.firstOffset()
      && endOffset == acknowledgementBatch.lastOffset()
      && acknowledgementType == acknowledgementBatch.acknowledgeTypes().get(0)) {
      return true
    }
    false
  }

  private def compareAcknowledgeResponsePartitionData(partitionIndex: Int,
                                              ackErrorCode: Short,
                                              partitionData: ShareAcknowledgeResponseData.PartitionData
                                             ): Boolean = {
    if (partitionIndex == partitionData.partitionIndex() && ackErrorCode == partitionData.errorCode()) {
      return true
    }
    false
  }

  private def memoryRecordsBuilder(numOfRecords: Int, startOffset: Long): MemoryRecordsBuilder = {

    val buffer: ByteBuffer = ByteBuffer.allocate(1024)
    val compression: Compression = Compression.of(CompressionType.NONE).build()
    val timestampType: TimestampType = TimestampType.CREATE_TIME

    val builder: MemoryRecordsBuilder = MemoryRecords.builder(buffer, compression, timestampType, startOffset)
    for (i <- 0 until numOfRecords) {
      builder.appendWithOffset(startOffset + i, 0L, TestUtils.randomBytes(10), TestUtils.randomBytes(10))
    }
    builder
  }

  private def memoryRecords(numOfRecords: Int, startOffset: Long): MemoryRecords = {
    memoryRecordsBuilder(numOfRecords, startOffset).build()
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.JOIN_GROUP)
  def testHandleJoinGroupRequest(version: Short): Unit = {
    val joinGroupRequest = new JoinGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setRebalanceTimeoutMs(1000)
      .setSessionTimeoutMs(2000)

    val requestChannelRequest = buildRequest(new JoinGroupRequest.Builder(joinGroupRequest).build(version))

    val expectedJoinGroupRequest = new JoinGroupRequestData()
      .setGroupId(joinGroupRequest.groupId)
      .setMemberId(joinGroupRequest.memberId)
      .setProtocolType(joinGroupRequest.protocolType)
      .setRebalanceTimeoutMs(if (version >= 1) joinGroupRequest.rebalanceTimeoutMs else joinGroupRequest.sessionTimeoutMs)
      .setSessionTimeoutMs(joinGroupRequest.sessionTimeoutMs)

    val future = new CompletableFuture[JoinGroupResponseData]()
    when(groupCoordinator.joinGroup(
      requestChannelRequest.context,
      expectedJoinGroupRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleJoinGroupRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    val expectedJoinGroupResponse = new JoinGroupResponseData()
      .setMemberId("member")
      .setGenerationId(0)
      .setLeader("leader")
      .setProtocolType(if (version >= 7) "consumer" else null)
      .setProtocolName("range")

    future.complete(expectedJoinGroupResponse)
    val response = verifyNoThrottling[JoinGroupResponse](requestChannelRequest)
    assertEquals(expectedJoinGroupResponse, response.data)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.JOIN_GROUP)
  def testJoinGroupProtocolNameBackwardCompatibility(version: Short): Unit = {
    val joinGroupRequest = new JoinGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setRebalanceTimeoutMs(1000)
      .setSessionTimeoutMs(2000)

    val requestChannelRequest = buildRequest(new JoinGroupRequest.Builder(joinGroupRequest).build(version))

    val expectedJoinGroupRequest = new JoinGroupRequestData()
      .setGroupId(joinGroupRequest.groupId)
      .setMemberId(joinGroupRequest.memberId)
      .setProtocolType(joinGroupRequest.protocolType)
      .setRebalanceTimeoutMs(if (version >= 1) joinGroupRequest.rebalanceTimeoutMs else joinGroupRequest.sessionTimeoutMs)
      .setSessionTimeoutMs(joinGroupRequest.sessionTimeoutMs)

    val future = new CompletableFuture[JoinGroupResponseData]()
    when(groupCoordinator.joinGroup(
      requestChannelRequest.context,
      expectedJoinGroupRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleJoinGroupRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    val joinGroupResponse = new JoinGroupResponseData()
      .setErrorCode(Errors.INCONSISTENT_GROUP_PROTOCOL.code)
      .setMemberId("member")
      .setProtocolName(null)

    val expectedJoinGroupResponse = new JoinGroupResponseData()
      .setErrorCode(Errors.INCONSISTENT_GROUP_PROTOCOL.code)
      .setMemberId("member")
      .setProtocolName(if (version >= 7) null else kafka.coordinator.group.GroupCoordinator.NoProtocol)

    future.complete(joinGroupResponse)
    val response = verifyNoThrottling[JoinGroupResponse](requestChannelRequest)
    assertEquals(expectedJoinGroupResponse, response.data)
  }

  @Test
  def testHandleJoinGroupRequestFutureFailed(): Unit = {
    val joinGroupRequest = new JoinGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setRebalanceTimeoutMs(1000)
      .setSessionTimeoutMs(2000)

    val requestChannelRequest = buildRequest(new JoinGroupRequest.Builder(joinGroupRequest).build())

    val future = new CompletableFuture[JoinGroupResponseData]()
    when(groupCoordinator.joinGroup(
      requestChannelRequest.context,
      joinGroupRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleJoinGroupRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    future.completeExceptionally(Errors.REQUEST_TIMED_OUT.exception)
    val response = verifyNoThrottling[JoinGroupResponse](requestChannelRequest)
    assertEquals(Errors.REQUEST_TIMED_OUT, response.error)
  }

  @Test
  def testHandleJoinGroupRequestAuthorizationFailed(): Unit = {
    val joinGroupRequest = new JoinGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setRebalanceTimeoutMs(1000)
      .setSessionTimeoutMs(2000)

    val requestChannelRequest = buildRequest(new JoinGroupRequest.Builder(joinGroupRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleJoinGroupRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    val response = verifyNoThrottling[JoinGroupResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED, response.error)
  }

  @Test
  def testHandleJoinGroupRequestUnexpectedException(): Unit = {
    val joinGroupRequest = new JoinGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setRebalanceTimeoutMs(1000)
      .setSessionTimeoutMs(2000)

    val requestChannelRequest = buildRequest(new JoinGroupRequest.Builder(joinGroupRequest).build())

    val future = new CompletableFuture[JoinGroupResponseData]()
    when(groupCoordinator.joinGroup(
      requestChannelRequest.context,
      joinGroupRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)

    var response: JoinGroupResponse = null
    when(requestChannel.sendResponse(any(), any(), any())).thenAnswer { _ =>
      throw new Exception("Something went wrong")
    }.thenAnswer { invocation =>
      response = invocation.getArgument(1, classOf[JoinGroupResponse])
    }
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    future.completeExceptionally(Errors.NOT_COORDINATOR.exception)

    // The exception expected here is the one thrown by `sendResponse`. As
    // `Exception` is not a Kafka errors, `UNKNOWN_SERVER_ERROR` is returned.
    assertEquals(Errors.UNKNOWN_SERVER_ERROR, response.error)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.SYNC_GROUP)
  def testHandleSyncGroupRequest(version: Short): Unit = {
    val syncGroupRequest = new SyncGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setProtocolName("range")

    val requestChannelRequest = buildRequest(new SyncGroupRequest.Builder(syncGroupRequest).build(version))

    val expectedSyncGroupRequest = new SyncGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType(if (version >= 5) "consumer" else null)
      .setProtocolName(if (version >= 5) "range" else null)

    val future = new CompletableFuture[SyncGroupResponseData]()
    when(groupCoordinator.syncGroup(
      requestChannelRequest.context,
      expectedSyncGroupRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleSyncGroupRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    val expectedSyncGroupResponse = new SyncGroupResponseData()
      .setProtocolType(if (version >= 5 ) "consumer" else null)
      .setProtocolName(if (version >= 5 ) "range" else null)

    future.complete(expectedSyncGroupResponse)
    val response = verifyNoThrottling[SyncGroupResponse](requestChannelRequest)
    assertEquals(expectedSyncGroupResponse, response.data)
  }

  @Test
  def testHandleSyncGroupRequestFutureFailed(): Unit = {
    val syncGroupRequest = new SyncGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setProtocolName("range")

    val requestChannelRequest = buildRequest(new SyncGroupRequest.Builder(syncGroupRequest).build())

    val expectedSyncGroupRequest = new SyncGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setProtocolName("range")

    val future = new CompletableFuture[SyncGroupResponseData]()
    when(groupCoordinator.syncGroup(
      requestChannelRequest.context,
      expectedSyncGroupRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleSyncGroupRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    future.completeExceptionally(Errors.UNKNOWN_SERVER_ERROR.exception)
    val response = verifyNoThrottling[SyncGroupResponse](requestChannelRequest)
    assertEquals(Errors.UNKNOWN_SERVER_ERROR, response.error)
  }

  @Test
  def testHandleSyncGroupRequestAuthenticationFailed(): Unit = {
    val syncGroupRequest = new SyncGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setProtocolName("range")

    val requestChannelRequest = buildRequest(new SyncGroupRequest.Builder(syncGroupRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleSyncGroupRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    val response = verifyNoThrottling[SyncGroupResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED, response.error)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.SYNC_GROUP)
  def testSyncGroupProtocolTypeAndNameAreMandatorySinceV5(version: Short): Unit = {
    val syncGroupRequest = new SyncGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")

    val requestChannelRequest = buildRequest(new SyncGroupRequest.Builder(syncGroupRequest).build(version))

    val expectedSyncGroupRequest = new SyncGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")

    val future = new CompletableFuture[SyncGroupResponseData]()
    when(groupCoordinator.syncGroup(
      requestChannelRequest.context,
      expectedSyncGroupRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleSyncGroupRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    if (version < 5) {
      future.complete(new SyncGroupResponseData()
        .setProtocolType("consumer")
        .setProtocolName("range"))
    }

    val response = verifyNoThrottling[SyncGroupResponse](requestChannelRequest)

    if (version < 5) {
      assertEquals(Errors.NONE, response.error)
    } else {
      assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL, response.error)
    }
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.HEARTBEAT)
  def testHandleHeartbeatRequest(version: Short): Unit = {
    val heartbeatRequest = new HeartbeatRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setGenerationId(0)

    val requestChannelRequest = buildRequest(new HeartbeatRequest.Builder(heartbeatRequest).build(version))

    val expectedHeartbeatRequest = new HeartbeatRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setGenerationId(0)

    val future = new CompletableFuture[HeartbeatResponseData]()
    when(groupCoordinator.heartbeat(
      requestChannelRequest.context,
      expectedHeartbeatRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleHeartbeatRequest(requestChannelRequest)

    val expectedHeartbeatResponse = new HeartbeatResponseData()
    future.complete(expectedHeartbeatResponse)
    val response = verifyNoThrottling[HeartbeatResponse](requestChannelRequest)
    assertEquals(expectedHeartbeatResponse, response.data)
  }

  @Test
  def testHandleHeartbeatRequestFutureFailed(): Unit = {
    val heartbeatRequest = new HeartbeatRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setGenerationId(0)

    val requestChannelRequest = buildRequest(new HeartbeatRequest.Builder(heartbeatRequest).build())

    val expectedHeartbeatRequest = new HeartbeatRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setGenerationId(0)

    val future = new CompletableFuture[HeartbeatResponseData]()
    when(groupCoordinator.heartbeat(
      requestChannelRequest.context,
      expectedHeartbeatRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleHeartbeatRequest(requestChannelRequest)

    future.completeExceptionally(Errors.UNKNOWN_SERVER_ERROR.exception)
    val response = verifyNoThrottling[HeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.UNKNOWN_SERVER_ERROR, response.error)
  }

  @Test
  def testHandleHeartbeatRequestAuthenticationFailed(): Unit = {
    val heartbeatRequest = new HeartbeatRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setGenerationId(0)

    val requestChannelRequest = buildRequest(new HeartbeatRequest.Builder(heartbeatRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleHeartbeatRequest(
      requestChannelRequest
    )

    val response = verifyNoThrottling[HeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED, response.error)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.LEAVE_GROUP)
  def testHandleLeaveGroupWithMultipleMembers(version: Short): Unit = {
    def makeRequest(version: Short): RequestChannel.Request = {
      buildRequest(new LeaveGroupRequest.Builder(
        "group",
        List(
          new MemberIdentity()
            .setMemberId("member-1")
            .setGroupInstanceId("instance-1"),
          new MemberIdentity()
            .setMemberId("member-2")
            .setGroupInstanceId("instance-2")
        ).asJava
      ).build(version))
    }

    if (version < 3) {
      // Request version earlier than version 3 do not support batching members.
      assertThrows(classOf[UnsupportedVersionException], () => makeRequest(version))
    } else {
      val requestChannelRequest = makeRequest(version)

      val expectedLeaveGroupRequest = new LeaveGroupRequestData()
        .setGroupId("group")
        .setMembers(List(
          new MemberIdentity()
            .setMemberId("member-1")
            .setGroupInstanceId("instance-1"),
          new MemberIdentity()
            .setMemberId("member-2")
            .setGroupInstanceId("instance-2")
        ).asJava)

      val future = new CompletableFuture[LeaveGroupResponseData]()
      when(groupCoordinator.leaveGroup(
        requestChannelRequest.context,
        expectedLeaveGroupRequest
      )).thenReturn(future)
      kafkaApis = createKafkaApis()
      kafkaApis.handleLeaveGroupRequest(requestChannelRequest)

      val expectedLeaveResponse = new LeaveGroupResponseData()
        .setErrorCode(Errors.NONE.code)
        .setMembers(List(
          new LeaveGroupResponseData.MemberResponse()
            .setMemberId("member-1")
            .setGroupInstanceId("instance-1"),
          new LeaveGroupResponseData.MemberResponse()
            .setMemberId("member-2")
            .setGroupInstanceId("instance-2"),
        ).asJava)

      future.complete(expectedLeaveResponse)
      val response = verifyNoThrottling[LeaveGroupResponse](requestChannelRequest)
      assertEquals(expectedLeaveResponse, response.data)
    }
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.LEAVE_GROUP)
  def testHandleLeaveGroupWithSingleMember(version: Short): Unit = {
    val requestChannelRequest = buildRequest(new LeaveGroupRequest.Builder(
      "group",
      List(
        new MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1")
      ).asJava
    ).build(version))

    val expectedLeaveGroupRequest = new LeaveGroupRequestData()
      .setGroupId("group")
      .setMembers(List(
        new MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId(if (version >= 3) "instance-1" else null)
      ).asJava)

    val future = new CompletableFuture[LeaveGroupResponseData]()
    when(groupCoordinator.leaveGroup(
      requestChannelRequest.context,
      expectedLeaveGroupRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleLeaveGroupRequest(requestChannelRequest)

    val leaveGroupResponse = new LeaveGroupResponseData()
      .setErrorCode(Errors.NONE.code)
      .setMembers(List(
        new LeaveGroupResponseData.MemberResponse()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1")
      ).asJava)

    val expectedLeaveResponse = if (version >= 3) {
      new LeaveGroupResponseData()
        .setErrorCode(Errors.NONE.code)
        .setMembers(List(
          new LeaveGroupResponseData.MemberResponse()
            .setMemberId("member-1")
            .setGroupInstanceId("instance-1")
        ).asJava)
    } else {
      new LeaveGroupResponseData()
        .setErrorCode(Errors.NONE.code)
    }

    future.complete(leaveGroupResponse)
    val response = verifyNoThrottling[LeaveGroupResponse](requestChannelRequest)
    assertEquals(expectedLeaveResponse, response.data)
  }

  @Test
  def testHandleLeaveGroupFutureFailed(): Unit = {
    val requestChannelRequest = buildRequest(new LeaveGroupRequest.Builder(
      "group",
      List(
        new MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1")
      ).asJava
    ).build(ApiKeys.LEAVE_GROUP.latestVersion))

    val expectedLeaveGroupRequest = new LeaveGroupRequestData()
      .setGroupId("group")
      .setMembers(List(
        new MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1")
      ).asJava)

    val future = new CompletableFuture[LeaveGroupResponseData]()
    when(groupCoordinator.leaveGroup(
      requestChannelRequest.context,
      expectedLeaveGroupRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleLeaveGroupRequest(requestChannelRequest)

    future.completeExceptionally(Errors.UNKNOWN_SERVER_ERROR.exception)
    val response = verifyNoThrottling[LeaveGroupResponse](requestChannelRequest)
    assertEquals(Errors.UNKNOWN_SERVER_ERROR, response.error)
  }

  @Test
  def testHandleLeaveGroupAuthenticationFailed(): Unit = {
    val requestChannelRequest = buildRequest(new LeaveGroupRequest.Builder(
      "group",
      List(
        new MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1")
      ).asJava
    ).build(ApiKeys.LEAVE_GROUP.latestVersion))

    val expectedLeaveGroupRequest = new LeaveGroupRequestData()
      .setGroupId("group")
      .setMembers(List(
        new MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1")
      ).asJava)

    val future = new CompletableFuture[LeaveGroupResponseData]()
    when(groupCoordinator.leaveGroup(
      requestChannelRequest.context,
      expectedLeaveGroupRequest
    )).thenReturn(future)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleLeaveGroupRequest(requestChannelRequest)

    val response = verifyNoThrottling[LeaveGroupResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED, response.error)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
  def testHandleOffsetFetchWithMultipleGroups(version: Short): Unit = {
    // Version 0 gets offsets from Zookeeper. We are not interested
    // in testing this here.
    if (version == 0) return

    def makeRequest(version: Short): RequestChannel.Request = {
      val groups = Map(
        "group-1" -> List(
          new TopicPartition("foo", 0),
          new TopicPartition("foo", 1)
        ).asJava,
        "group-2" -> null,
        "group-3" -> null,
        "group-4" -> null,
      ).asJava
      buildRequest(new OffsetFetchRequest.Builder(groups, false, false).build(version))
    }

    if (version < 8) {
      // Request version earlier than version 8 do not support batching groups.
      assertThrows(classOf[UnsupportedVersionException], () => makeRequest(version))
    } else {
      val requestChannelRequest = makeRequest(version)

      val group1Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
      when(groupCoordinator.fetchOffsets(
        requestChannelRequest.context,
        new OffsetFetchRequestData.OffsetFetchRequestGroup()
          .setGroupId("group-1")
          .setTopics(List(
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
              .setName("foo")
              .setPartitionIndexes(List[Integer](0, 1).asJava)).asJava),
        false
      )).thenReturn(group1Future)

      val group2Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
      when(groupCoordinator.fetchAllOffsets(
        requestChannelRequest.context,
        new OffsetFetchRequestData.OffsetFetchRequestGroup()
          .setGroupId("group-2")
          .setTopics(null),
        false
      )).thenReturn(group2Future)

      val group3Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
      when(groupCoordinator.fetchAllOffsets(
        requestChannelRequest.context,
        new OffsetFetchRequestData.OffsetFetchRequestGroup()
          .setGroupId("group-3")
          .setTopics(null),
        false
      )).thenReturn(group3Future)

      val group4Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
      when(groupCoordinator.fetchAllOffsets(
        requestChannelRequest.context,
        new OffsetFetchRequestData.OffsetFetchRequestGroup()
          .setGroupId("group-4")
          .setTopics(null),
        false
      )).thenReturn(group4Future)
      kafkaApis = createKafkaApis()
      kafkaApis.handleOffsetFetchRequest(requestChannelRequest)

      val group1Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
        .setGroupId("group-1")
        .setTopics(List(
          new OffsetFetchResponseData.OffsetFetchResponseTopics()
            .setName("foo")
            .setPartitions(List(
              new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(0)
                .setCommittedOffset(100)
                .setCommittedLeaderEpoch(1),
              new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(1)
                .setCommittedOffset(200)
                .setCommittedLeaderEpoch(2)
            ).asJava)
        ).asJava)

      val group2Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
        .setGroupId("group-2")
        .setTopics(List(
          new OffsetFetchResponseData.OffsetFetchResponseTopics()
            .setName("bar")
            .setPartitions(List(
              new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(0)
                .setCommittedOffset(100)
                .setCommittedLeaderEpoch(1),
              new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(1)
                .setCommittedOffset(200)
                .setCommittedLeaderEpoch(2),
              new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(2)
                .setCommittedOffset(300)
                .setCommittedLeaderEpoch(3)
            ).asJava)
        ).asJava)

      val group3Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
        .setGroupId("group-3")
        .setErrorCode(Errors.INVALID_GROUP_ID.code)

      val group4Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
        .setGroupId("group-4")
        .setErrorCode(Errors.INVALID_GROUP_ID.code)

      val expectedGroups = List(group1Response, group2Response, group3Response, group4Response)

      group1Future.complete(group1Response)
      group2Future.complete(group2Response)
      group3Future.completeExceptionally(Errors.INVALID_GROUP_ID.exception)
      group4Future.complete(group4Response)

      val response = verifyNoThrottling[OffsetFetchResponse](requestChannelRequest)
      assertEquals(expectedGroups.toSet, response.data.groups().asScala.toSet)
    }
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
  def testHandleOffsetFetchWithSingleGroup(version: Short): Unit = {
    // Version 0 gets offsets from Zookeeper. We are not interested
    // in testing this here.
    if (version == 0) return

    def makeRequest(version: Short): RequestChannel.Request = {
      buildRequest(new OffsetFetchRequest.Builder(
        "group-1",
        false,
        List(
          new TopicPartition("foo", 0),
          new TopicPartition("foo", 1)
        ).asJava,
        false
      ).build(version))
    }

    val requestChannelRequest = makeRequest(version)

    val future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-1")
        .setTopics(List(new OffsetFetchRequestData.OffsetFetchRequestTopics()
          .setName("foo")
          .setPartitionIndexes(List[Integer](0, 1).asJava)).asJava),
      false
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetFetchRequest(requestChannelRequest)

    val group1Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
      .setGroupId("group-1")
      .setTopics(List(
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName("foo")
          .setPartitions(List(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1),
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(1)
              .setCommittedOffset(200)
              .setCommittedLeaderEpoch(2)
          ).asJava)
      ).asJava)

    val expectedOffsetFetchResponse = if (version >= 8) {
      new OffsetFetchResponseData()
        .setGroups(List(group1Response).asJava)
    } else {
      new OffsetFetchResponseData()
        .setTopics(List(
          new OffsetFetchResponseData.OffsetFetchResponseTopic()
            .setName("foo")
            .setPartitions(List(
              new OffsetFetchResponseData.OffsetFetchResponsePartition()
                .setPartitionIndex(0)
                .setCommittedOffset(100)
                .setCommittedLeaderEpoch(if (version >= 5) 1 else -1),
              new OffsetFetchResponseData.OffsetFetchResponsePartition()
                .setPartitionIndex(1)
                .setCommittedOffset(200)
                .setCommittedLeaderEpoch(if (version >= 5) 2 else -1)
            ).asJava)
        ).asJava)
    }

    future.complete(group1Response)

    val response = verifyNoThrottling[OffsetFetchResponse](requestChannelRequest)
    assertEquals(expectedOffsetFetchResponse, response.data)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
  def testHandleOffsetFetchAllOffsetsWithSingleGroup(version: Short): Unit = {
    // Version 0 gets offsets from Zookeeper. Version 1 does not support fetching all
    // offsets request. We are not interested in testing these here.
    if (version < 2) return

    def makeRequest(version: Short): RequestChannel.Request = {
      buildRequest(new OffsetFetchRequest.Builder(
        "group-1",
        false,
        null, // all offsets.
        false
      ).build(version))
    }

    val requestChannelRequest = makeRequest(version)

    val future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchAllOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-1")
        .setTopics(null),
      false
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetFetchRequest(requestChannelRequest)

    val group1Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
      .setGroupId("group-1")
      .setTopics(List(
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName("foo")
          .setPartitions(List(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1),
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(1)
              .setCommittedOffset(200)
              .setCommittedLeaderEpoch(2)
          ).asJava)
      ).asJava)

    val expectedOffsetFetchResponse = if (version >= 8) {
      new OffsetFetchResponseData()
        .setGroups(List(group1Response).asJava)
    } else {
      new OffsetFetchResponseData()
        .setTopics(List(
          new OffsetFetchResponseData.OffsetFetchResponseTopic()
            .setName("foo")
            .setPartitions(List(
              new OffsetFetchResponseData.OffsetFetchResponsePartition()
                .setPartitionIndex(0)
                .setCommittedOffset(100)
                .setCommittedLeaderEpoch(if (version >= 5) 1 else -1),
              new OffsetFetchResponseData.OffsetFetchResponsePartition()
                .setPartitionIndex(1)
                .setCommittedOffset(200)
                .setCommittedLeaderEpoch(if (version >= 5) 2 else -1)
            ).asJava)
        ).asJava)
    }

    future.complete(group1Response)

    val response = verifyNoThrottling[OffsetFetchResponse](requestChannelRequest)
    assertEquals(expectedOffsetFetchResponse, response.data)
  }

  // r22 BLOCKER #151 (Agent 3 Finding #2, read-side complement to #205): OffsetFetch must drop
  // backing-topic entries from BOTH branches (all-topics and explicit-list). The write-side
  // OffsetCommit/OffsetDelete guard from #205 stops new commits from landing on a backing name,
  // but defence-in-depth on the read side closes two residual gaps:
  //   (a) pre-#205 offset rows that may exist in __consumer_offsets from a prior broker version,
  //   (b) an admin-misconfigured ACL granting DESCRIBE on a backing-topic name to a tenant — in
  //       which case the response would otherwise echo that backing's committed offsets back to
  //       the wrong principal.
  // Backing topics must surface as if non-existent (silent drop), matching METADATA(isAllTopics)
  // #157, DescribeTopicPartitions(all) #164 and DescribeLogDirs #185.
  @Test
  def testOffsetFetchAllOffsetsFiltersBackingTopics(): Unit = {
    val backingTopic = "backing-r22-151"
    val plainTopic = "plain-r22-151"
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)
    when(concentrationKernel.isBackingTopic(plainTopic)).thenReturn(false)

    val requestChannelRequest = buildRequest(new OffsetFetchRequest.Builder(
      "group-1",
      false,
      null, // all offsets.
      false
    ).build(ApiKeys.OFFSET_FETCH.latestVersion))

    val future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchAllOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-1")
        .setTopics(null),
      false
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetFetchRequest(requestChannelRequest)

    // Coordinator returns offsets for BOTH topics — the bug we're guarding against is the broker
    // forwarding them to the client. Without the isBackingTopic filter the response would
    // contain the backing-topic entry, leaking its existence + committed-offset progress.
    val coordinatorResponse = new OffsetFetchResponseData.OffsetFetchResponseGroup()
      .setGroupId("group-1")
      .setTopics(List(
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName(backingTopic)
          .setPartitions(List(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1)
          ).asJava),
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName(plainTopic)
          .setPartitions(List(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(200)
              .setCommittedLeaderEpoch(2)
          ).asJava)
      ).asJava)

    future.complete(coordinatorResponse)

    val response = verifyNoThrottling[OffsetFetchResponse](requestChannelRequest)
    val returnedTopicNames = response.data.groups.asScala.flatMap(_.topics.asScala.map(_.name)).toSet
    assertFalse(returnedTopicNames.contains(backingTopic),
      s"OffsetFetch(all) leaked backing topic '$backingTopic' to the client; returned=$returnedTopicNames")
    assertTrue(returnedTopicNames.contains(plainTopic),
      s"OffsetFetch(all) over-filtered: dropped legitimate '$plainTopic'; returned=$returnedTopicNames")
  }

  @Test
  def testOffsetFetchByListFiltersBackingTopicsBeforeCoordinator(): Unit = {
    val backingTopic = "backing-r22-151"
    val plainTopic = "plain-r22-151"
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)
    when(concentrationKernel.isBackingTopic(plainTopic)).thenReturn(false)

    val requestChannelRequest = buildRequest(new OffsetFetchRequest.Builder(
      "group-1",
      false,
      List(
        new TopicPartition(backingTopic, 0),
        new TopicPartition(plainTopic, 0)
      ).asJava,
      false
    ).build(ApiKeys.OFFSET_FETCH.latestVersion))

    // The coordinator must NEVER be asked about the backing topic — we filter pre-authz so the
    // backing-name path doesn't even reach __consumer_offsets storage. Capture the request the
    // broker hands to the coordinator and assert the backing topic is absent.
    val captor: ArgumentCaptor[OffsetFetchRequestData.OffsetFetchRequestGroup] =
      ArgumentCaptor.forClass(classOf[OffsetFetchRequestData.OffsetFetchRequestGroup])
    val future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchOffsets(
      ArgumentMatchers.eq(requestChannelRequest.context),
      captor.capture(),
      ArgumentMatchers.eq(false)
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetFetchRequest(requestChannelRequest)

    val coordinatorRequest = captor.getValue
    val requestedTopicNames = coordinatorRequest.topics.asScala.map(_.name).toSet
    assertFalse(requestedTopicNames.contains(backingTopic),
      s"OffsetFetch leaked backing topic '$backingTopic' down to the group coordinator; " +
        s"coordinator-requested=$requestedTopicNames")
    assertTrue(requestedTopicNames.contains(plainTopic),
      s"OffsetFetch over-filtered: dropped legitimate '$plainTopic' before the coordinator call; " +
        s"coordinator-requested=$requestedTopicNames")

    val coordinatorResponse = new OffsetFetchResponseData.OffsetFetchResponseGroup()
      .setGroupId("group-1")
      .setTopics(List(
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName(plainTopic)
          .setPartitions(List(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(200)
              .setCommittedLeaderEpoch(2)
          ).asJava)
      ).asJava)
    future.complete(coordinatorResponse)

    val response = verifyNoThrottling[OffsetFetchResponse](requestChannelRequest)
    val returnedTopicNames = response.data.groups.asScala.flatMap(_.topics.asScala.map(_.name)).toSet
    assertFalse(returnedTopicNames.contains(backingTopic),
      s"OffsetFetch(byList) leaked backing topic '$backingTopic' to the client; returned=$returnedTopicNames")
  }

  @Test
  def testHandleOffsetFetchAuthorization(): Unit = {
    def makeRequest(version: Short): RequestChannel.Request = {
      val groups = Map(
        "group-1" -> List(
          new TopicPartition("foo", 0),
          new TopicPartition("bar", 0)
        ).asJava,
        "group-2" -> List(
          new TopicPartition("foo", 0),
          new TopicPartition("bar", 0)
        ).asJava,
        "group-3" -> null,
        "group-4" -> null,
      ).asJava
      buildRequest(new OffsetFetchRequest.Builder(groups, false, false).build(version))
    }

    val requestChannelRequest = makeRequest(ApiKeys.OFFSET_FETCH.latestVersion)

    val authorizer: Authorizer = mock(classOf[Authorizer])

    val acls = Map(
      "group-1" -> AuthorizationResult.ALLOWED,
      "group-2" -> AuthorizationResult.DENIED,
      "group-3" -> AuthorizationResult.ALLOWED,
      "group-4" -> AuthorizationResult.DENIED,
      "foo" -> AuthorizationResult.DENIED,
      "bar" -> AuthorizationResult.ALLOWED
    )

    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.asScala.map { action =>
        acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED)
      }.asJava
    }

    // group-1 is allowed and bar is allowed.
    val group1Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-1")
        .setTopics(List(new OffsetFetchRequestData.OffsetFetchRequestTopics()
          .setName("bar")
          .setPartitionIndexes(List[Integer](0).asJava)).asJava),
      false
    )).thenReturn(group1Future)

    // group-3 is allowed and bar is allowed.
    val group3Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchAllOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-3")
        .setTopics(null),
      false
    )).thenReturn(group3Future)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val group1ResponseFromCoordinator = new OffsetFetchResponseData.OffsetFetchResponseGroup()
      .setGroupId("group-1")
      .setTopics(List(
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName("bar")
          .setPartitions(List(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1)
          ).asJava)
      ).asJava)

    val group3ResponseFromCoordinator = new OffsetFetchResponseData.OffsetFetchResponseGroup()
      .setGroupId("group-3")
      .setTopics(List(
        // foo should be filtered out.
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName("foo")
          .setPartitions(List(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1)
          ).asJava),
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName("bar")
          .setPartitions(List(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1)
          ).asJava)
      ).asJava)

    val expectedOffsetFetchResponse = new OffsetFetchResponseData()
      .setGroups(List(
        // group-1 is authorized but foo is not.
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("group-1")
          .setTopics(List(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setName("bar")
              .setPartitions(List(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(0)
                  .setCommittedOffset(100)
                  .setCommittedLeaderEpoch(1)
              ).asJava),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setName("foo")
              .setPartitions(List(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(0)
                  .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
                  .setCommittedOffset(-1)
              ).asJava)
          ).asJava),
        // group-2 is not authorized.
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("group-2")
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code),
        // group-3 is authorized but foo is not.
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("group-3")
          .setTopics(List(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setName("bar")
              .setPartitions(List(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(0)
                  .setCommittedOffset(100)
                  .setCommittedLeaderEpoch(1)
              ).asJava)
          ).asJava),
        // group-4 is not authorized.
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("group-4")
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code),
      ).asJava)

    group1Future.complete(group1ResponseFromCoordinator)
    group3Future.complete(group3ResponseFromCoordinator)

    val response = verifyNoThrottling[OffsetFetchResponse](requestChannelRequest)
    assertEquals(expectedOffsetFetchResponse, response.data)
  }

  @Test
  def testHandleOffsetFetchWithUnauthorizedTopicAndTopLevelError(): Unit = {
    def makeRequest(version: Short): RequestChannel.Request = {
      val groups = Map(
        "group-1" -> List(
          new TopicPartition("foo", 0),
          new TopicPartition("bar", 0)
        ).asJava,
        "group-2" -> List(
          new TopicPartition("foo", 0),
          new TopicPartition("bar", 0)
        ).asJava
      ).asJava
      buildRequest(new OffsetFetchRequest.Builder(groups, false, false).build(version))
    }

    val requestChannelRequest = makeRequest(ApiKeys.OFFSET_FETCH.latestVersion)

    val authorizer: Authorizer = mock(classOf[Authorizer])

    val acls = Map(
      "group-1" -> AuthorizationResult.ALLOWED,
      "group-2" -> AuthorizationResult.ALLOWED,
      "foo" -> AuthorizationResult.DENIED,
      "bar" -> AuthorizationResult.ALLOWED
    )

    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.asScala.map { action =>
        acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED)
      }.asJava
    }

    // group-1 and group-2 are allowed and bar is allowed.
    val group1Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-1")
        .setTopics(List(new OffsetFetchRequestData.OffsetFetchRequestTopics()
          .setName("bar")
          .setPartitionIndexes(List[Integer](0).asJava)).asJava),
      false
    )).thenReturn(group1Future)

    val group2Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-2")
        .setTopics(List(new OffsetFetchRequestData.OffsetFetchRequestTopics()
          .setName("bar")
          .setPartitionIndexes(List[Integer](0).asJava)).asJava),
      false
    )).thenReturn(group1Future)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    // group-2 mocks using the new group coordinator.
    // When the coordinator is not active, a response with top-level error code is returned
    // despite that the requested topic is not authorized and fails.
    val group2ResponseFromCoordinator = new OffsetFetchResponseData.OffsetFetchResponseGroup()
      .setGroupId("group-2")
      .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code)

    val expectedOffsetFetchResponse = new OffsetFetchResponseData()
      .setGroups(List(
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("group-1")
          .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code),
        group2ResponseFromCoordinator
      ).asJava)

    group1Future.completeExceptionally(Errors.COORDINATOR_NOT_AVAILABLE.exception)
    group2Future.complete(group2ResponseFromCoordinator)

    val response = verifyNoThrottling[OffsetFetchResponse](requestChannelRequest)
    assertEquals(expectedOffsetFetchResponse, response.data)
  }

  @Test
  def testReassignmentAndReplicationBytesOutRateWhenReassigning(): Unit = {
    assertReassignmentAndReplicationBytesOutPerSec(true)
  }

  @Test
  def testReassignmentAndReplicationBytesOutRateWhenNotReassigning(): Unit = {
    assertReassignmentAndReplicationBytesOutPerSec(false)
  }

  private def assertReassignmentAndReplicationBytesOutPerSec(isReassigning: Boolean): Unit = {
    val leaderEpoch = 0
    val tp0 = new TopicPartition("tp", 0)
    val topicId = Uuid.randomUuid()
    val tidp0 = new TopicIdPartition(topicId, tp0)

    setupBasicMetadataCache(tp0.topic, numPartitions = 1, 1, topicId)
    val hw = 3

    val fetchDataBuilder = Collections.singletonMap(tp0, new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0, 0, Int.MaxValue, Optional.of(leaderEpoch)))
    val fetchData = Collections.singletonMap(tidp0, new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0, 0, Int.MaxValue, Optional.of(leaderEpoch)))
    val fetchFromFollower = buildRequest(new FetchRequest.Builder(
      ApiKeys.FETCH.oldestVersion(), ApiKeys.FETCH.latestVersion(), 1, 1, 1000, 0, fetchDataBuilder).build())

    val records = MemoryRecords.withRecords(Compression.NONE,
      new SimpleRecord(1000, "foo".getBytes(StandardCharsets.UTF_8)))
    when(replicaManager.fetchMessages(
      any[FetchParams],
      any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]],
      any[ReplicaQuota],
      any[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]()
    )).thenAnswer(invocation => {
      val callback = invocation.getArgument(3).asInstanceOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]
      callback(Seq(tidp0 -> new FetchPartitionData(Errors.NONE, hw, 0, records,
        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), isReassigning)))
    })

    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      fetchMetadata, fetchData, true, true)
    when(fetchManager.newContext(
      any[Short],
      any[JFetchMetadata],
      any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      any[util.List[TopicIdPartition]],
      any[util.Map[Uuid, String]])).thenReturn(fetchContext)

    when(replicaManager.getLogConfig(ArgumentMatchers.eq(tp0))).thenReturn(None)
    when(replicaManager.isAddingReplica(any(), anyInt)).thenReturn(isReassigning)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(fetchFromFollower, RequestLocal.withThreadConfinedCaching)
    verify(replicaQuotaManager).record(anyLong)

    if (isReassigning)
      assertEquals(records.sizeInBytes(), brokerTopicStats.allTopicsStats.reassignmentBytesOutPerSec.get.count())
    else
      assertEquals(0, brokerTopicStats.allTopicsStats.reassignmentBytesOutPerSec.get.count())
    assertEquals(records.sizeInBytes(), brokerTopicStats.allTopicsStats.replicationBytesOutRate.get.count())
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.LIST_GROUPS)
  def testListGroupsRequest(version: Short): Unit = {
    val listGroupsRequest = new ListGroupsRequestData()
      .setStatesFilter(if (version >= 4) List("Stable", "Empty").asJava else List.empty.asJava)
      .setTypesFilter(if (version >= 5) List("classic", "consumer").asJava else List.empty.asJava)

    val requestChannelRequest = buildRequest(new ListGroupsRequest.Builder(listGroupsRequest).build(version))

    val expectedListGroupsRequest = new ListGroupsRequestData()
      .setStatesFilter(if (version >= 4) List("Stable", "Empty").asJava else List.empty.asJava)
      .setTypesFilter(if (version >= 5) List("classic", "consumer").asJava else List.empty.asJava)

    val future = new CompletableFuture[ListGroupsResponseData]()
    when(groupCoordinator.listGroups(
      requestChannelRequest.context,
      expectedListGroupsRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleListGroupsRequest(requestChannelRequest)

    val expectedListGroupsResponse = new ListGroupsResponseData()
      .setGroups(List(
        new ListGroupsResponseData.ListedGroup()
          .setGroupId("group1")
          .setProtocolType("protocol1")
          .setGroupState(if (version >= 4) "Stable" else "")
          .setGroupType(if (version >= 5) "consumer" else ""),
        new ListGroupsResponseData.ListedGroup()
          .setGroupId("group2")
          .setProtocolType("protocol2")
          .setGroupState(if (version >= 4) "Empty" else "")
          .setGroupType(if (version >= 5) "classic" else ""),
        new ListGroupsResponseData.ListedGroup()
          .setGroupId("group3")
          .setProtocolType("protocol3")
          .setGroupState(if (version >= 4) "Stable" else "")
          .setGroupType(if (version >= 5) "classic" else ""),
      ).asJava)

    future.complete(expectedListGroupsResponse)
    val response = verifyNoThrottling[ListGroupsResponse](requestChannelRequest)
    assertEquals(expectedListGroupsResponse, response.data)
  }

  @Test
  def testListGroupsRequestFutureFailed(): Unit = {
    val listGroupsRequest = new ListGroupsRequestData()
      .setStatesFilter(List("Stable", "Empty").asJava)
      .setTypesFilter(List("classic", "consumer").asJava)

    val requestChannelRequest = buildRequest(new ListGroupsRequest.Builder(listGroupsRequest).build())

    val expectedListGroupsRequest = new ListGroupsRequestData()
      .setStatesFilter(List("Stable", "Empty").asJava)
      .setTypesFilter(List("classic", "consumer").asJava)

    val future = new CompletableFuture[ListGroupsResponseData]()
    when(groupCoordinator.listGroups(
      requestChannelRequest.context,
      expectedListGroupsRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleListGroupsRequest(requestChannelRequest)

    future.completeExceptionally(Errors.UNKNOWN_SERVER_ERROR.exception)
    val response = verifyNoThrottling[ListGroupsResponse](requestChannelRequest)
    assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, response.data.errorCode)
  }

  @Test
  def testListGroupsRequestFiltersUnauthorizedGroupsWithDescribeCluster(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

    authorizeResource(
      authorizer,
      AclOperation.DESCRIBE,
      ResourceType.GROUP,
      "group1",
      AuthorizationResult.DENIED,
      logIfDenied = false
    )
    authorizeResource(
      authorizer,
      AclOperation.DESCRIBE,
      ResourceType.GROUP,
      "group2",
      AuthorizationResult.DENIED,
      logIfDenied = false
    )
    authorizeResource(
      authorizer,
      AclOperation.DESCRIBE,
      ResourceType.CLUSTER,
      Resource.CLUSTER_NAME,
      AuthorizationResult.ALLOWED,
      logIfDenied = false
    )

    testListGroupsRequestFiltersUnauthorizedGroups(
      authorizer,
      List("group1", "group2"),
      List("group1", "group2")
    )
  }

  @Test
  def testListGroupsRequestFiltersUnauthorizedGroupsWithDescribeGroups(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

    authorizeResource(
      authorizer,
      AclOperation.DESCRIBE,
      ResourceType.GROUP,
      "group1",
      AuthorizationResult.DENIED,
      logIfDenied = false
    )
    authorizeResource(
      authorizer,
      AclOperation.DESCRIBE,
      ResourceType.GROUP,
      "group2",
      AuthorizationResult.ALLOWED,
      logIfDenied = false
    )
    authorizeResource(
      authorizer,
      AclOperation.DESCRIBE,
      ResourceType.CLUSTER,
      Resource.CLUSTER_NAME,
      AuthorizationResult.DENIED,
      logIfDenied = false
    )

    testListGroupsRequestFiltersUnauthorizedGroups(
      authorizer,
      List("group1", "group2"),
      List("group2")
    )
  }

  def testListGroupsRequestFiltersUnauthorizedGroups(
    authorizer: Authorizer,
    groups: List[String],
    expectedGroups: List[String],
  ): Unit = {
    val listGroupsRequest = new ListGroupsRequestData()

    val requestChannelRequest = buildRequest(new ListGroupsRequest.Builder(listGroupsRequest).build())

    val expectedListGroupsRequest = new ListGroupsRequestData()

    val future = new CompletableFuture[ListGroupsResponseData]()
    when(groupCoordinator.listGroups(
      requestChannelRequest.context,
      expectedListGroupsRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleListGroupsRequest(requestChannelRequest)

    val listGroupsResponse = new ListGroupsResponseData()
    groups.foreach { groupId =>
      listGroupsResponse.groups.add(new ListGroupsResponseData.ListedGroup()
        .setGroupId(groupId)
      )
    }

    val expectedListGroupsResponse = new ListGroupsResponseData()
    expectedGroups.foreach { groupId =>
      expectedListGroupsResponse.groups.add(new ListGroupsResponseData.ListedGroup()
        .setGroupId(groupId)
      )
    }

    future.complete(listGroupsResponse)
    val response = verifyNoThrottling[ListGroupsResponse](requestChannelRequest)
    assertEquals(expectedListGroupsResponse, response.data)
  }

  @Test
  def testDescribeClusterRequest(): Unit = {
    val plaintextListener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val endpoints = new BrokerEndpointCollection()
    endpoints.add(
      new BrokerEndpoint()
        .setHost("broker0")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(plaintextListener.value)
    )
    endpoints.add(
      new BrokerEndpoint()
        .setHost("broker1")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(plaintextListener.value)
    )

    MetadataCacheTest.updateCache(metadataCache,
      Seq(new RegisterBrokerRecord()
        .setBrokerId(brokerId)
        .setRack("rack")
        .setFenced(false)
        .setEndPoints(endpoints)))

    val describeClusterRequest = new DescribeClusterRequest.Builder(new DescribeClusterRequestData()
      .setIncludeClusterAuthorizedOperations(true)).build()

    val request = buildRequest(describeClusterRequest, plaintextListener)
    kafkaApis = createKafkaApis()
    kafkaApis.handleDescribeCluster(request)

    val describeClusterResponse = verifyNoThrottling[DescribeClusterResponse](request)

    assertEquals(clusterId, describeClusterResponse.data.clusterId)
    assertEquals(8096, describeClusterResponse.data.clusterAuthorizedOperations)
    assertEquals(metadataCache.getAliveBrokerNodes(plaintextListener).toSet,
      describeClusterResponse.nodes.asScala.values.toSet)
  }

  /**
   * Return pair of listener names in the metadataCache: PLAINTEXT and LISTENER2 respectively.
   */
  private def updateMetadataCacheWithInconsistentListeners(): (ListenerName, ListenerName) = {
    val plaintextListener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val anotherListener = new ListenerName("LISTENER2")

    val endpoints0 = new BrokerEndpointCollection()
    endpoints0.add(
      new BrokerEndpoint()
        .setHost("broker0")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(plaintextListener.value)
    )
    endpoints0.add(
      new BrokerEndpoint()
        .setHost("broker0")
        .setPort(9093)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(anotherListener.value)
    )

    val endpoints1 = new BrokerEndpointCollection()
    endpoints1.add(
      new BrokerEndpoint()
        .setHost("broker1")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(plaintextListener.value)
    )

    MetadataCacheTest.updateCache(metadataCache,
      Seq(new RegisterBrokerRecord().setBrokerId(0).setRack("rack").setFenced(false).setEndPoints(endpoints0),
      new RegisterBrokerRecord().setBrokerId(1).setRack("rack").setFenced(false).setEndPoints(endpoints1))
    )

    (plaintextListener, anotherListener)
  }

  private def sendMetadataRequestWithInconsistentListeners(requestListener: ListenerName): MetadataResponse = {
    val metadataRequest = MetadataRequest.Builder.allTopics.build()
    val requestChannelRequest = buildRequest(metadataRequest, requestListener)
    kafkaApis = createKafkaApis()
    kafkaApis.handleTopicMetadataRequest(requestChannelRequest)

    verifyNoThrottling[MetadataResponse](requestChannelRequest)
  }

  private def testConsumerListOffsetWithUnsupportedVersion(timestamp: Long, version: Short): Unit = {
    val tp = new TopicPartition("foo", 0)
    val targetTimes = List(new ListOffsetsTopic()
      .setName(tp.topic)
      .setPartitions(List(new ListOffsetsPartition()
        .setPartitionIndex(tp.partition)
        .setTimestamp(timestamp)).asJava)).asJava

    when(replicaManager.fetchOffset(
      ArgumentMatchers.any[Seq[ListOffsetsTopic]](),
      ArgumentMatchers.eq(Set.empty[TopicPartition]),
      ArgumentMatchers.eq(IsolationLevel.READ_UNCOMMITTED),
      ArgumentMatchers.eq(ListOffsetsRequest.CONSUMER_REPLICA_ID),
      ArgumentMatchers.eq[String](clientId),
      ArgumentMatchers.anyInt(), // correlationId
      ArgumentMatchers.anyShort(), // version
      ArgumentMatchers.any[(Errors, ListOffsetsPartition) => ListOffsetsPartitionResponse](),
      ArgumentMatchers.any[List[ListOffsetsTopicResponse] => Unit](),
      ArgumentMatchers.anyInt() // timeoutMs
    )).thenAnswer(ans => {
      val version = ans.getArgument[Short](6)
      val callback = ans.getArgument[List[ListOffsetsTopicResponse] => Unit](8)
      val errorCode = if (ReplicaManager.isListOffsetsTimestampUnsupported(timestamp, version))
        Errors.UNSUPPORTED_VERSION.code()
      else
        Errors.INVALID_REQUEST.code()
      val partitionResponse = new ListOffsetsPartitionResponse()
        .setErrorCode(errorCode)
        .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)
        .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
        .setPartitionIndex(tp.partition())
      callback(List(new ListOffsetsTopicResponse().setName(tp.topic()).setPartitions(List(partitionResponse).asJava)))
    })

    val data = new ListOffsetsRequestData().setTopics(targetTimes).setReplicaId(ListOffsetsRequest.CONSUMER_REPLICA_ID)
    val listOffsetRequest = ListOffsetsRequest.parse(MessageUtil.toByteBuffer(data, version), version)
    val request = buildRequest(listOffsetRequest)

    kafkaApis = createKafkaApis()
    kafkaApis.handleListOffsetRequest(request)

    val response = verifyNoThrottling[ListOffsetsResponse](request)
    val partitionDataOptional = response.topics.asScala.find(_.name == tp.topic).get
      .partitions.asScala.find(_.partitionIndex == tp.partition)
    assertTrue(partitionDataOptional.isDefined)

    val partitionData = partitionDataOptional.get
    assertEquals(Errors.UNSUPPORTED_VERSION.code, partitionData.errorCode)
  }

  private def testConsumerListOffsetLatest(isolationLevel: IsolationLevel): Unit = {
    val tp = new TopicPartition("foo", 0)
    val latestOffset = 15L

    val targetTimes = List(new ListOffsetsTopic()
      .setName(tp.topic)
      .setPartitions(List(new ListOffsetsPartition()
        .setPartitionIndex(tp.partition)
        .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)).asJava)).asJava

    when(replicaManager.fetchOffset(
      ArgumentMatchers.eq(targetTimes.asScala.toSeq),
      ArgumentMatchers.eq(Set.empty[TopicPartition]),
      ArgumentMatchers.eq(isolationLevel),
      ArgumentMatchers.eq(ListOffsetsRequest.CONSUMER_REPLICA_ID),
      ArgumentMatchers.eq[String](clientId),
      ArgumentMatchers.anyInt(), // correlationId
      ArgumentMatchers.anyShort(), // version
      ArgumentMatchers.any[(Errors, ListOffsetsPartition) => ListOffsetsPartitionResponse](),
      ArgumentMatchers.any[List[ListOffsetsTopicResponse] => Unit](),
      ArgumentMatchers.anyInt() // timeoutMs
    )).thenAnswer(ans => {
      val callback = ans.getArgument[List[ListOffsetsTopicResponse] => Unit](8)
      val partitionResponse = new ListOffsetsPartitionResponse()
        .setErrorCode(Errors.NONE.code())
        .setOffset(latestOffset)
        .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
        .setPartitionIndex(tp.partition())
      callback(List(new ListOffsetsTopicResponse().setName(tp.topic()).setPartitions(List(partitionResponse).asJava)))
    })

    val listOffsetRequest = ListOffsetsRequest.Builder.forConsumer(true, isolationLevel)
      .setTargetTimes(targetTimes).build()
    val request = buildRequest(listOffsetRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleListOffsetRequest(request)

    val response = verifyNoThrottling[ListOffsetsResponse](request)
    val partitionDataOptional = response.topics.asScala.find(_.name == tp.topic).get
      .partitions.asScala.find(_.partitionIndex == tp.partition)
    assertTrue(partitionDataOptional.isDefined)

    val partitionData = partitionDataOptional.get
    assertEquals(Errors.NONE.code, partitionData.errorCode)
    assertEquals(latestOffset, partitionData.offset)
    assertEquals(ListOffsetsResponse.UNKNOWN_TIMESTAMP, partitionData.timestamp)
  }

  private def createWriteTxnMarkersRequest(partitions: util.List[TopicPartition]) = {
    val writeTxnMarkersRequest = new WriteTxnMarkersRequest.Builder(
      asList(new TxnMarkerEntry(1, 1.toShort, 0, TransactionResult.COMMIT, partitions))).build()
    (writeTxnMarkersRequest, buildRequest(writeTxnMarkersRequest))
  }

  private def buildRequest(request: AbstractRequest,
                           listenerName: ListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
                           fromPrivilegedListener: Boolean = false,
                           requestHeader: Option[RequestHeader] = None,
                           requestMetrics: RequestChannelMetrics = requestChannelMetrics): RequestChannel.Request = {
    val buffer = request.serializeWithHeader(
      requestHeader.getOrElse(new RequestHeader(request.apiKey, request.version, clientId, 0)))

    // read the header from the buffer first so that the body can be read next from the Request constructor
    val header = RequestHeader.parse(buffer)
    // DelegationTokens require the context authenticated to be non SecurityProtocol.PLAINTEXT
    // and have a non KafkaPrincipal.ANONYMOUS principal. This test is done before the check
    // for forwarding because after forwarding the context will have a different context.
    // We validate the context authenticated failure case in other integration tests.
    val context = new RequestContext(header, "1", InetAddress.getLocalHost, Optional.empty(),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice"), listenerName, SecurityProtocol.SSL,
      ClientInformation.EMPTY, fromPrivilegedListener, Optional.of(kafkaPrincipalSerde))
    new RequestChannel.Request(processor = 1, context = context, startTimeNanos = 0, MemoryPool.NONE, buffer,
      requestMetrics, envelope = None)
  }

  private def verifyNoThrottling[T <: AbstractResponse](
    request: RequestChannel.Request
  ): T = {
    val capturedResponse: ArgumentCaptor[AbstractResponse] = ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      any()
    )
    val response = capturedResponse.getValue
    val buffer = MessageUtil.toByteBuffer(
      response.data,
      request.context.header.apiVersion
    )
    AbstractResponse.parseResponse(
      request.context.header.apiKey,
      buffer,
      request.context.header.apiVersion,
    ).asInstanceOf[T]
  }

  private def verifyNoThrottlingAndUpdateMetrics[T <: AbstractResponse](
    request: RequestChannel.Request
  ): T = {
    val capturedResponse: ArgumentCaptor[AbstractResponse] = ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      any()
    )
    val response = capturedResponse.getValue
    val buffer = MessageUtil.toByteBuffer(
      response.data,
      request.context.header.apiVersion
    )

    // Create the RequestChannel.Response that is created when sendResponse is called in order to update the metrics.
    val sendResponse = new RequestChannel.SendResponse(
      request,
      request.buildResponseSend(response),
      request.responseNode(response),
      None
    )
    request.updateRequestMetrics(time.milliseconds(), sendResponse)

    AbstractResponse.parseResponse(
      request.context.header.apiKey,
      buffer,
      request.context.header.apiVersion,
    ).asInstanceOf[T]
  }

  private def createBasicMetadata(topic: String,
                                  numPartitions: Int,
                                  brokerEpoch: Long,
                                  numBrokers: Int,
                                  topicId: Uuid): Seq[ApiMessage] = {

    val results = new mutable.ArrayBuffer[ApiMessage]()
    val topicRecord = new TopicRecord().setName(topic).setTopicId(topicId)
    results += topicRecord

    val replicas = List(0.asInstanceOf[Integer]).asJava

    def createPartitionRecord(partition: Int) = new PartitionRecord()
      .setTopicId(topicId)
      .setPartitionId(partition)
      .setLeader(0)
      .setLeaderEpoch(1)
      .setReplicas(replicas)
      .setIsr(replicas)

    val plaintextListener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val partitionRecords = (0 until numPartitions).map(createPartitionRecord)
    val liveBrokers = (0 until numBrokers).map(
      brokerId => createMetadataBroker(brokerId, plaintextListener, brokerEpoch))
    partitionRecords.foreach(record => results += record)
    liveBrokers.foreach(record => results +=record)

    results.toSeq
  }

  private def setupBasicMetadataCache(topic: String, numPartitions: Int, numBrokers: Int, topicId: Uuid): Unit = {
    val updateMetadata = createBasicMetadata(topic, numPartitions, 0, numBrokers, topicId)
    MetadataCacheTest.updateCache(metadataCache, updateMetadata)
  }

  private def addTopicToMetadataCache(topic: String, numPartitions: Int, numBrokers: Int = 1, topicId: Uuid = Uuid.ZERO_UUID): Unit = {
    val updateMetadata = createBasicMetadata(topic, numPartitions, 0, numBrokers, topicId)
    MetadataCacheTest.updateCache(metadataCache, updateMetadata)
  }

  private def createMetadataBroker(brokerId: Int,
                                   listener: ListenerName,
                                   brokerEpoch: Long): RegisterBrokerRecord = {
    val endpoints = new BrokerEndpointCollection()
    endpoints.add(
      new BrokerEndpoint()
        .setHost("broker" + brokerId)
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(listener.value)
    )

    new RegisterBrokerRecord()
      .setBrokerId(brokerId)
      .setRack("rack")
      .setFenced(false)
      .setEndPoints(endpoints)
      .setBrokerEpoch(brokerEpoch)
  }

  @Test
  def testAlterReplicaLogDirs(): Unit = {
    val data = new AlterReplicaLogDirsRequestData()
    val dir = new AlterReplicaLogDirsRequestData.AlterReplicaLogDir()
      .setPath("/foo")
    dir.topics().add(new AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopic().setName("t0").setPartitions(asList(0, 1, 2)))
    data.dirs().add(dir)
    val alterReplicaLogDirsRequest = new AlterReplicaLogDirsRequest.Builder(
      data
    ).build()
    val request = buildRequest(alterReplicaLogDirsRequest)

    reset(replicaManager, clientRequestQuotaManager, requestChannel)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    val t0p0 = new TopicPartition("t0", 0)
    val t0p1 = new TopicPartition("t0", 1)
    val t0p2 = new TopicPartition("t0", 2)
    val partitionResults = Map(
      t0p0 -> Errors.NONE,
      t0p1 -> Errors.LOG_DIR_NOT_FOUND,
      t0p2 -> Errors.INVALID_TOPIC_EXCEPTION)
    when(replicaManager.alterReplicaLogDirs(ArgumentMatchers.eq(Map(
      t0p0 -> "/foo",
      t0p1 -> "/foo",
      t0p2 -> "/foo"))))
    .thenReturn(partitionResults)
    kafkaApis = createKafkaApis()
    kafkaApis.handleAlterReplicaLogDirsRequest(request)

    val response = verifyNoThrottling[AlterReplicaLogDirsResponse](request)
    assertEquals(partitionResults, response.data.results.asScala.flatMap { tr =>
      tr.partitions().asScala.map { pr =>
        new TopicPartition(tr.topicName, pr.partitionIndex) -> Errors.forCode(pr.errorCode)
      }
    }.toMap)
    assertEquals(Map(Errors.NONE -> 1,
      Errors.LOG_DIR_NOT_FOUND -> 1,
      Errors.INVALID_TOPIC_EXCEPTION -> 1).asJava, response.errorCounts)
  }

  @Test
  def testSizeOfThrottledPartitions(): Unit = {
    val topicNames = new util.HashMap[Uuid, String]
    val topicIds = new util.HashMap[String, Uuid]()
    def fetchResponse(data: Map[TopicIdPartition, String]): FetchResponse = {
      val responseData = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData](
        data.map { case (tp, raw) =>
          tp -> new FetchResponseData.PartitionData()
            .setPartitionIndex(tp.topicPartition.partition)
            .setHighWatermark(105)
            .setLastStableOffset(105)
            .setLogStartOffset(0)
            .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(100, raw.getBytes(StandardCharsets.UTF_8))))
      }.toMap.asJava)

      data.foreach{case (tp, _) =>
        topicIds.put(tp.topicPartition.topic, tp.topicId)
        topicNames.put(tp.topicId, tp.topicPartition.topic)
      }
      FetchResponse.of(Errors.NONE, 100, 100, responseData)
    }

    val throttledPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("throttledData", 0))
    val throttledData = Map(throttledPartition -> "throttledData")
    val expectedSize = FetchResponse.sizeOf(FetchResponseData.HIGHEST_SUPPORTED_VERSION,
      fetchResponse(throttledData).responseData(topicNames, FetchResponseData.HIGHEST_SUPPORTED_VERSION).entrySet.asScala.map( entry =>
      (new TopicIdPartition(Uuid.ZERO_UUID, entry.getKey), entry.getValue)).toMap.asJava.entrySet.iterator)

    val response = fetchResponse(throttledData ++ Map(new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("nonThrottledData", 0)) -> "nonThrottledData"))

    val quota = Mockito.mock(classOf[ReplicationQuotaManager])
    Mockito.when(quota.isThrottled(ArgumentMatchers.any(classOf[TopicPartition])))
      .thenAnswer(invocation => throttledPartition.topicPartition == invocation.getArgument(0).asInstanceOf[TopicPartition])

    assertEquals(expectedSize, KafkaApis.sizeOfThrottledPartitions(FetchResponseData.HIGHEST_SUPPORTED_VERSION, response, quota))
  }

  @Test
  def testDescribeProducers(): Unit = {
    val tp1 = new TopicPartition("foo", 0)
    val tp2 = new TopicPartition("bar", 3)
    val tp3 = new TopicPartition("baz", 1)
    val tp4 = new TopicPartition("invalid;topic", 1)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val data = new DescribeProducersRequestData().setTopics(List(
      new DescribeProducersRequestData.TopicRequest()
        .setName(tp1.topic)
        .setPartitionIndexes(List(Int.box(tp1.partition)).asJava),
      new DescribeProducersRequestData.TopicRequest()
        .setName(tp2.topic)
        .setPartitionIndexes(List(Int.box(tp2.partition)).asJava),
      new DescribeProducersRequestData.TopicRequest()
        .setName(tp3.topic)
        .setPartitionIndexes(List(Int.box(tp3.partition)).asJava),
      new DescribeProducersRequestData.TopicRequest()
        .setName(tp4.topic)
        .setPartitionIndexes(List(Int.box(tp4.partition)).asJava)
    ).asJava)

    def buildExpectedActions(topic: String): util.List[Action] = {
      val pattern = new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL)
      val action = new Action(AclOperation.READ, pattern, 1, true, true)
      Collections.singletonList(action)
    }

    // Topic `foo` is authorized and present in the metadata
    addTopicToMetadataCache(tp1.topic, 4) // We will only access the first topic
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions(tp1.topic))))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    // Topic `bar` is not authorized
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions(tp2.topic))))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)

    // Topic `baz` is authorized, but not present in the metadata
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions(tp3.topic))))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    when(replicaManager.activeProducerState(tp1))
      .thenReturn(new DescribeProducersResponseData.PartitionResponse()
        .setErrorCode(Errors.NONE.code)
        .setPartitionIndex(tp1.partition)
        .setActiveProducers(List(
          new DescribeProducersResponseData.ProducerState()
            .setProducerId(12345L)
            .setProducerEpoch(15)
            .setLastSequence(100)
            .setLastTimestamp(time.milliseconds())
            .setCurrentTxnStartOffset(-1)
            .setCoordinatorEpoch(200)
        ).asJava))

    val describeProducersRequest = new DescribeProducersRequest.Builder(data).build()
    val request = buildRequest(describeProducersRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleDescribeProducersRequest(request)

    val response = verifyNoThrottling[DescribeProducersResponse](request)
    assertEquals(Set("foo", "bar", "baz", "invalid;topic"), response.data.topics.asScala.map(_.name).toSet)

    def assertPartitionError(
      topicPartition: TopicPartition,
      error: Errors
    ): DescribeProducersResponseData.PartitionResponse = {
      val topicData = response.data.topics.asScala.find(_.name == topicPartition.topic).get
      val partitionData = topicData.partitions.asScala.find(_.partitionIndex == topicPartition.partition).get
      assertEquals(error, Errors.forCode(partitionData.errorCode))
      partitionData
    }

    val fooPartition = assertPartitionError(tp1, Errors.NONE)
    assertEquals(Errors.NONE, Errors.forCode(fooPartition.errorCode))
    assertEquals(1, fooPartition.activeProducers.size)
    val fooProducer = fooPartition.activeProducers.get(0)
    assertEquals(12345L, fooProducer.producerId)
    assertEquals(15, fooProducer.producerEpoch)
    assertEquals(100, fooProducer.lastSequence)
    assertEquals(time.milliseconds(), fooProducer.lastTimestamp)
    assertEquals(-1, fooProducer.currentTxnStartOffset)
    assertEquals(200, fooProducer.coordinatorEpoch)

    assertPartitionError(tp2, Errors.TOPIC_AUTHORIZATION_FAILED)
    assertPartitionError(tp3, Errors.UNKNOWN_TOPIC_OR_PARTITION)
    assertPartitionError(tp4, Errors.INVALID_TOPIC_EXCEPTION)
  }

  @Test
  def testDescribeProducersRejectsBackingTopic(): Unit = {
    // r22 BLOCKER #175 — backing topics for concentrated logical topics carry the
    // ProducerId/Epoch/LastSequence state of every co-tenant writing to them. Without
    // this guard, a principal authorized on the backing-topic NAME would receive the
    // union of every co-tenant's producer state (in-flight transactions, idempotency
    // keys, last sequence per producer), enough to fingerprint co-tenant traffic and
    // predict next sequence numbers. The handler MUST reject backing topics with
    // INVALID_TOPIC_EXCEPTION and NEVER consult ReplicaManager.activeProducerState.
    // Auth-first / shadow-second precedence: an UNauthorized probe still gets
    // TOPIC_AUTHORIZATION_FAILED and cannot enumerate the declared-backing set.
    val backingTopic = "backing-topic-r22-175"
    val plainTopic = "tenant-topic-r22-175"
    val plainTp = new TopicPartition(plainTopic, 0)

    addTopicToMetadataCache(backingTopic, numPartitions = 8)
    addTopicToMetadataCache(plainTopic, numPartitions = 2)
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)
    when(concentrationKernel.isBackingTopic(plainTopic)).thenReturn(false)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    def buildExpectedActions(topic: String): util.List[Action] = {
      val pattern = new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL)
      val action = new Action(AclOperation.READ, pattern, 1, true, true)
      Collections.singletonList(action)
    }
    // The principal is authorized to READ on both topics — the test exists to show that
    // authorization alone MUST NOT unlock the substrate's producer state.
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions(backingTopic))))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions(plainTopic))))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    // Plain-topic state should still be returned as-is.
    when(replicaManager.activeProducerState(plainTp))
      .thenReturn(new DescribeProducersResponseData.PartitionResponse()
        .setErrorCode(Errors.NONE.code)
        .setPartitionIndex(plainTp.partition)
        .setActiveProducers(List(
          new DescribeProducersResponseData.ProducerState()
            .setProducerId(777L)
            .setProducerEpoch(3)
            .setLastSequence(50)
            .setLastTimestamp(time.milliseconds())
            .setCurrentTxnStartOffset(-1)
            .setCoordinatorEpoch(1)
        ).asJava))

    val data = new DescribeProducersRequestData().setTopics(List(
      new DescribeProducersRequestData.TopicRequest()
        .setName(backingTopic)
        .setPartitionIndexes(List(Int.box(0), Int.box(1)).asJava),
      new DescribeProducersRequestData.TopicRequest()
        .setName(plainTopic)
        .setPartitionIndexes(List(Int.box(0)).asJava)
    ).asJava)
    val request = buildRequest(new DescribeProducersRequest.Builder(data).build())
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleDescribeProducersRequest(request)

    val response = verifyNoThrottling[DescribeProducersResponse](request)
    val backingResp = response.data.topics.asScala.find(_.name == backingTopic).getOrElse(
      fail("Backing topic must appear in response with rejection error").asInstanceOf[Nothing])
    assertEquals(2, backingResp.partitions.size,
      "Every requested backing partition must be reflected back with the rejection error")
    backingResp.partitions.asScala.foreach { p =>
      assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, p.errorCode,
        s"Backing partition ${p.partitionIndex} must be rejected with INVALID_TOPIC_EXCEPTION " +
          "— surfacing producer state would leak co-tenant identity")
      assertTrue(p.activeProducers == null || p.activeProducers.isEmpty,
        "activeProducers list must be empty on rejection — any populated entry would leak " +
          "the very co-tenant producer state this fix exists to hide")
    }

    val plainResp = response.data.topics.asScala.find(_.name == plainTopic).getOrElse(
      fail("Plain topic must appear in response with real producer state").asInstanceOf[Nothing])
    assertEquals(Errors.NONE.code, plainResp.partitions.asScala.head.errorCode)
    assertEquals(1, plainResp.partitions.asScala.head.activeProducers.size,
      "Plain topic producer state must be forwarded unchanged")
    assertEquals(777L, plainResp.partitions.asScala.head.activeProducers.asScala.head.producerId)

    // ReplicaManager MUST NEVER be asked for the backing topic's producer state — that's the
    // entire cross-tenant leak this fix prevents.
    verify(replicaManager, never()).activeProducerState(new TopicPartition(backingTopic, 0))
    verify(replicaManager, never()).activeProducerState(new TopicPartition(backingTopic, 1))
    verify(replicaManager, times(1)).activeProducerState(plainTp)
  }

  @Test
  def testDescribeProducersUnauthorizedBackingProbeReturnsAuthorizationFailedNotInvalidTopic(): Unit = {
    // r22 BLOCKER #175 — auth-first / shadow-second precedence. An UNauthorized probe of a
    // backing-topic name must receive TOPIC_AUTHORIZATION_FAILED, NOT INVALID_TOPIC_EXCEPTION.
    // Otherwise an unauthorized attacker could enumerate the declared-backing set by probing
    // names and reading response-code differences. The backing predicate MUST NOT be consulted
    // on this path — the auth check catches the probe first.
    val backingTopic = "backing-topic-r22-175-authfail"
    addTopicToMetadataCache(backingTopic, numPartitions = 8)
    // isBackingTopic must NEVER be called on this path — leave the mock unstubbed so we can
    // assert verify(never()).

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val readBacking = new Action(AclOperation.READ,
      new ResourcePattern(ResourceType.TOPIC, backingTopic, PatternType.LITERAL), 1, true, true)
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(Collections.singletonList(readBacking))))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)

    val data = new DescribeProducersRequestData().setTopics(List(
      new DescribeProducersRequestData.TopicRequest()
        .setName(backingTopic)
        .setPartitionIndexes(List(Int.box(0)).asJava)
    ).asJava)
    val request = buildRequest(new DescribeProducersRequest.Builder(data).build())
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleDescribeProducersRequest(request)

    val response = verifyNoThrottling[DescribeProducersResponse](request)
    val topicResp = response.data.topics.asScala.find(_.name == backingTopic).get
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code,
      topicResp.partitions.asScala.head.errorCode,
      "Auth-first precedence — unauthorized backing probe must see TOPIC_AUTHORIZATION_FAILED, " +
        "not INVALID_TOPIC_EXCEPTION (which would leak the declared-backing set)")
    verify(concentrationKernel, never()).isBackingTopic(backingTopic)
    verify(replicaManager, never()).activeProducerState(any[TopicPartition])
  }

  @Test
  def testDescribeLogDirsFiltersBackingTopicByName(): Unit = {
    // r22 BLOCKER (escalated from Agent 3 cross-tenant audit) — DescribeLogDirs is gated on
    // cluster-level DESCRIBE, which cannot distinguish per-tenant scope: a principal authorized
    // to DESCRIBE the cluster otherwise sees every backing topic's name, partition count, log-dir
    // path, and aggregate disk usage (the SUM of every co-tenant's bytes). That is an
    // enumeration oracle plus a per-tenant write-rate side channel. The by-name branch must drop
    // backing partitions BEFORE the partition set reaches ReplicaManager.describeLogDirs.
    val backingTopic = "backing-r22-185"
    val plainTopic = "tenant-r22-185"
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)
    when(concentrationKernel.isBackingTopic(plainTopic)).thenReturn(false)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val clusterDescribe = new Action(AclOperation.DESCRIBE,
      new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL),
      1, true, true)
    when(authorizer.authorize(any[RequestContext],
      ArgumentMatchers.eq(Collections.singletonList(clusterDescribe))))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    val captor: ArgumentCaptor[Set[TopicPartition]] =
      ArgumentCaptor.forClass(classOf[Set[TopicPartition]])
    when(replicaManager.describeLogDirs(captor.capture()))
      .thenReturn(List.empty[DescribeLogDirsResponseData.DescribeLogDirsResult])

    val data = new DescribeLogDirsRequestData()
      .setTopics(new DescribeLogDirsRequestData.DescribableLogDirTopicCollection(
        util.Arrays.asList(
          new DescribeLogDirsRequestData.DescribableLogDirTopic()
            .setTopic(backingTopic)
            .setPartitions(util.Arrays.asList(Int.box(0), Int.box(1))),
          new DescribeLogDirsRequestData.DescribableLogDirTopic()
            .setTopic(plainTopic)
            .setPartitions(util.Arrays.asList(Int.box(0)))
        ).iterator()))
    val request = buildRequest(new DescribeLogDirsRequest.Builder(data).build())
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleDescribeLogDirsRequest(request)

    val captured = captor.getValue
    assertFalse(captured.exists(_.topic == backingTopic),
      s"DescribeLogDirs must NOT forward backing topic '$backingTopic' to ReplicaManager.describeLogDirs — " +
        "doing so would leak per-broker partition count, log-dir paths, and aggregate cross-tenant " +
        s"disk usage (the SUM of co-tenant bytes). Captured set was: $captured")
    assertTrue(captured.contains(new TopicPartition(plainTopic, 0)),
      s"Plain (non-backing) topic '$plainTopic' must still reach ReplicaManager. Captured set: $captured")
  }

  @Test
  def testDescribeLogDirsFiltersBackingTopicAllTopicsBranch(): Unit = {
    // Mirror of the by-name guard for the isAllTopicPartitions branch — the same filter must
    // apply to both code paths, otherwise an attacker simply asks "give me ALL log dirs" and
    // enumerates every backing topic on this broker in a single RPC. This is the same pattern
    // closed at METADATA(isAllTopics) BLOCKER #157 and DescribeTopicPartitions(all) BLOCKER #164;
    // DescribeLogDirs(isAllTopicPartitions=true) is the third leg of that triad.
    val backingTopic = "backing-r22-185-all"
    val plainTopic = "tenant-r22-185-all"
    val backingTp = new TopicPartition(backingTopic, 0)
    val plainTp = new TopicPartition(plainTopic, 0)
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)
    when(concentrationKernel.isBackingTopic(plainTopic)).thenReturn(false)

    val backingLog = mock(classOf[UnifiedLog])
    val plainLog = mock(classOf[UnifiedLog])
    when(backingLog.topicPartition).thenReturn(backingTp)
    when(plainLog.topicPartition).thenReturn(plainTp)
    val logManagerMock = mock(classOf[LogManager])
    when(logManagerMock.allLogs).thenReturn(Iterable[UnifiedLog](backingLog, plainLog))
    when(replicaManager.logManager).thenReturn(logManagerMock)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val clusterDescribe = new Action(AclOperation.DESCRIBE,
      new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL),
      1, true, true)
    when(authorizer.authorize(any[RequestContext],
      ArgumentMatchers.eq(Collections.singletonList(clusterDescribe))))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    val captor: ArgumentCaptor[Set[TopicPartition]] =
      ArgumentCaptor.forClass(classOf[Set[TopicPartition]])
    when(replicaManager.describeLogDirs(captor.capture()))
      .thenReturn(List.empty[DescribeLogDirsResponseData.DescribeLogDirsResult])

    // setTopics(null) makes isAllTopicPartitions == true.
    val data = new DescribeLogDirsRequestData().setTopics(null)
    val request = buildRequest(new DescribeLogDirsRequest.Builder(data).build())
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleDescribeLogDirsRequest(request)

    val captured = captor.getValue
    assertFalse(captured.contains(backingTp),
      s"DescribeLogDirs(isAllTopicPartitions=true) MUST NOT enumerate backing topics — this is " +
        s"the all-topics discovery vector. Captured set: $captured")
    assertTrue(captured.contains(plainTp),
      s"Plain topic '$plainTopic' must still appear in all-topics enumeration. Captured set: $captured")
  }

  @Test
  def testDescribeTransactions(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val data = new DescribeTransactionsRequestData()
      .setTransactionalIds(List("foo", "bar").asJava)
    val describeTransactionsRequest = new DescribeTransactionsRequest.Builder(data).build()
    val request = buildRequest(describeTransactionsRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    def buildExpectedActions(transactionalId: String): util.List[Action] = {
      val pattern = new ResourcePattern(ResourceType.TRANSACTIONAL_ID, transactionalId, PatternType.LITERAL)
      val action = new Action(AclOperation.DESCRIBE, pattern, 1, true, true)
      Collections.singletonList(action)
    }

    when(txnCoordinator.handleDescribeTransactions("foo"))
      .thenReturn(new DescribeTransactionsResponseData.TransactionState()
        .setErrorCode(Errors.NONE.code)
        .setTransactionalId("foo")
        .setProducerId(12345L)
        .setProducerEpoch(15)
        .setTransactionStartTimeMs(time.milliseconds())
        .setTransactionState("CompleteCommit")
        .setTransactionTimeoutMs(10000))

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions("foo"))))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions("bar"))))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleDescribeTransactionsRequest(request)

    val response = verifyNoThrottling[DescribeTransactionsResponse](request)
    assertEquals(2, response.data.transactionStates.size)

    val fooState = response.data.transactionStates.asScala.find(_.transactionalId == "foo").get
    assertEquals(Errors.NONE.code, fooState.errorCode)
    assertEquals(12345L, fooState.producerId)
    assertEquals(15, fooState.producerEpoch)
    assertEquals(time.milliseconds(), fooState.transactionStartTimeMs)
    assertEquals("CompleteCommit", fooState.transactionState)
    assertEquals(10000, fooState.transactionTimeoutMs)
    assertEquals(List.empty, fooState.topics.asScala.toList)

    val barState = response.data.transactionStates.asScala.find(_.transactionalId == "bar").get
    assertEquals(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code, barState.errorCode)
  }

  @Test
  def testDescribeTransactionsFiltersUnauthorizedTopics(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val transactionalId = "foo"
    val data = new DescribeTransactionsRequestData()
      .setTransactionalIds(List(transactionalId).asJava)
    val describeTransactionsRequest = new DescribeTransactionsRequest.Builder(data).build()
    val request = buildRequest(describeTransactionsRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    def expectDescribe(
      resourceType: ResourceType,
      transactionalId: String,
      result: AuthorizationResult
    ): Unit = {
      val pattern = new ResourcePattern(resourceType, transactionalId, PatternType.LITERAL)
      val action = new Action(AclOperation.DESCRIBE, pattern, 1, true, true)
      val actions = Collections.singletonList(action)

      when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(actions)))
        .thenReturn(Seq(result).asJava)
    }

    // Principal is authorized to one of the two topics. The second topic should be
    // filtered from the result.
    expectDescribe(ResourceType.TRANSACTIONAL_ID, transactionalId, AuthorizationResult.ALLOWED)
    expectDescribe(ResourceType.TOPIC, "foo", AuthorizationResult.ALLOWED)
    expectDescribe(ResourceType.TOPIC, "bar", AuthorizationResult.DENIED)

    def mkTopicData(
      topic: String,
      partitions: Seq[Int]
    ): DescribeTransactionsResponseData.TopicData = {
      new DescribeTransactionsResponseData.TopicData()
        .setTopic(topic)
        .setPartitions(partitions.map(Int.box).asJava)
    }

    val describeTransactionsResponse = new DescribeTransactionsResponseData.TransactionState()
      .setErrorCode(Errors.NONE.code)
      .setTransactionalId(transactionalId)
      .setProducerId(12345L)
      .setProducerEpoch(15)
      .setTransactionStartTimeMs(time.milliseconds())
      .setTransactionState("Ongoing")
      .setTransactionTimeoutMs(10000)

    describeTransactionsResponse.topics.add(mkTopicData(topic = "foo", Seq(1, 2)))
    describeTransactionsResponse.topics.add(mkTopicData(topic = "bar", Seq(3, 4)))

    when(txnCoordinator.handleDescribeTransactions("foo"))
      .thenReturn(describeTransactionsResponse)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleDescribeTransactionsRequest(request)

    val response = verifyNoThrottling[DescribeTransactionsResponse](request)
    assertEquals(1, response.data.transactionStates.size)

    val fooState = response.data.transactionStates.asScala.find(_.transactionalId == "foo").get
    assertEquals(Errors.NONE.code, fooState.errorCode)
    assertEquals(12345L, fooState.producerId)
    assertEquals(15, fooState.producerEpoch)
    assertEquals(time.milliseconds(), fooState.transactionStartTimeMs)
    assertEquals("Ongoing", fooState.transactionState)
    assertEquals(10000, fooState.transactionTimeoutMs)
    assertEquals(List(mkTopicData(topic = "foo", Seq(1, 2))), fooState.topics.asScala.toList)
  }

  @Test
  def testListTransactionsErrorResponse(): Unit = {
    val data = new ListTransactionsRequestData()
    val listTransactionsRequest = new ListTransactionsRequest.Builder(data).build()
    val request = buildRequest(listTransactionsRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    when(txnCoordinator.handleListTransactions(Set.empty[Long], Set.empty[String], -1L))
      .thenReturn(new ListTransactionsResponseData()
        .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code))
    kafkaApis = createKafkaApis()
    kafkaApis.handleListTransactionsRequest(request)

    val response = verifyNoThrottling[ListTransactionsResponse](request)
    assertEquals(0, response.data.transactionStates.size)
    assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, Errors.forCode(response.data.errorCode))
  }

  @Test
  def testListTransactionsAuthorization(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val data = new ListTransactionsRequestData()
    val listTransactionsRequest = new ListTransactionsRequest.Builder(data).build()
    val request = buildRequest(listTransactionsRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    val transactionStates = new util.ArrayList[ListTransactionsResponseData.TransactionState]()
    transactionStates.add(new ListTransactionsResponseData.TransactionState()
      .setTransactionalId("foo")
      .setProducerId(12345L)
      .setTransactionState("Ongoing"))
    transactionStates.add(new ListTransactionsResponseData.TransactionState()
      .setTransactionalId("bar")
      .setProducerId(98765)
      .setTransactionState("PrepareAbort"))

    when(txnCoordinator.handleListTransactions(Set.empty[Long], Set.empty[String], -1L))
      .thenReturn(new ListTransactionsResponseData()
        .setErrorCode(Errors.NONE.code)
        .setTransactionStates(transactionStates))

    def buildExpectedActions(transactionalId: String): util.List[Action] = {
      val pattern = new ResourcePattern(ResourceType.TRANSACTIONAL_ID, transactionalId, PatternType.LITERAL)
      val action = new Action(AclOperation.DESCRIBE, pattern, 1, true, true)
      Collections.singletonList(action)
    }

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions("foo"))))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions("bar"))))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleListTransactionsRequest(request)

    val response = verifyNoThrottling[ListTransactionsResponse](request)
    assertEquals(1, response.data.transactionStates.size())
    val transactionState = response.data.transactionStates.get(0)
    assertEquals("foo", transactionState.transactionalId)
    assertEquals(12345L, transactionState.producerId)
    assertEquals("Ongoing", transactionState.transactionState)
  }

  @Test
  def testEmptyLegacyAlterConfigsRequestWithKRaft(): Unit = {
    val request = buildRequest(new AlterConfigsRequest(new AlterConfigsRequestData(), 1.toShort))
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis()
    kafkaApis.handleAlterConfigsRequest(request)
    val response = verifyNoThrottling[AlterConfigsResponse](request)
    assertEquals(new AlterConfigsResponseData(), response.data())
  }

  @Test
  def testInvalidLegacyAlterConfigsRequestWithKRaft(): Unit = {
    val request = buildRequest(new AlterConfigsRequest(new AlterConfigsRequestData().
      setValidateOnly(true).
      setResources(new LAlterConfigsResourceCollection(asList(
        new LAlterConfigsResource().
          setResourceName(brokerId.toString).
          setResourceType(BROKER.id()).
          setConfigs(new LAlterableConfigCollection(asList(new LAlterableConfig().
            setName("foo").
            setValue(null)).iterator()))).iterator())), 1.toShort))
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis()
    kafkaApis.handleAlterConfigsRequest(request)
    val response = verifyNoThrottling[AlterConfigsResponse](request)
    assertEquals(new AlterConfigsResponseData().setResponses(asList(
      new LAlterConfigsResourceResponse().
        setErrorCode(Errors.INVALID_REQUEST.code()).
        setErrorMessage("Null value not supported for : foo").
        setResourceName(brokerId.toString).
        setResourceType(BROKER.id()))),
      response.data())
  }

  @Test
  def testEmptyIncrementalAlterConfigsRequestWithKRaft(): Unit = {
    val alterConfigsRequest = new IncrementalAlterConfigsRequest(new IncrementalAlterConfigsRequestData(), 1.toShort)
    assertEquals(
      "IncrementalAlterConfigsRequestData(resources=[], validateOnly=false)",
      alterConfigsRequest.toString
    )
    val request = buildRequest(alterConfigsRequest)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis()
    kafkaApis.handleIncrementalAlterConfigsRequest(request)
    val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
    assertEquals(new IncrementalAlterConfigsResponseData(), response.data())
  }

  @Test
  def testLog4jIncrementalAlterConfigsRequestWithKRaft(): Unit = {
    val alterConfigsRequest = new IncrementalAlterConfigsRequest(new IncrementalAlterConfigsRequestData().
      setValidateOnly(true).
      setResources(new IAlterConfigsResourceCollection(asList(new IAlterConfigsResource().
        setResourceName(brokerId.toString).
        setResourceType(BROKER_LOGGER.id()).
        setConfigs(new IAlterableConfigCollection(asList(new IAlterableConfig().
          setName(Log4jController.ROOT_LOGGER).
          setValue("TRACE")).iterator()))).iterator())),
        1.toShort)
    assertEquals(
      "IncrementalAlterConfigsRequestData(resources=[" +
        "AlterConfigsResource(resourceType=" + BROKER_LOGGER.id() + ", " +
        "resourceName='"+ brokerId + "', " +
        "configs=[AlterableConfig(name='" + Log4jController.ROOT_LOGGER + "', configOperation=0, value='REDACTED')])], " +
        "validateOnly=true)",
      alterConfigsRequest.toString
    )
    val request = buildRequest(alterConfigsRequest)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis()
    kafkaApis.handleIncrementalAlterConfigsRequest(request)
    val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
    assertEquals(new IncrementalAlterConfigsResponseData().setResponses(asList(
      new IAlterConfigsResourceResponse().
        setErrorCode(0.toShort).
        setErrorMessage(null).
        setResourceName(brokerId.toString).
        setResourceType(BROKER_LOGGER.id()))),
      response.data())
  }

  @Test
  def testConsumerGroupHeartbeatReturnsUnsupportedVersion(): Unit = {
    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ConsumerGroupHeartbeatRequest.Builder(consumerGroupHeartbeatRequest).build())
    metadataCache = {
      val cache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_1)
      val delta = new MetadataDelta(MetadataImage.EMPTY);
      delta.replay(new FeatureLevelRecord()
        .setName(MetadataVersion.FEATURE_NAME)
        .setFeatureLevel(MetadataVersion.MINIMUM_VERSION.featureLevel())
      )
      cache.setImage(delta.apply(MetadataProvenance.EMPTY))
      cache
    }
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val expectedHeartbeatResponse = new ConsumerGroupHeartbeatResponseData()
      .setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(expectedHeartbeatResponse, response.data)
  }

  @Test
  def testConsumerGroupHeartbeatRequest(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ConsumerGroupHeartbeatRequest.Builder(consumerGroupHeartbeatRequest).build())

    val future = new CompletableFuture[ConsumerGroupHeartbeatResponseData]()
    when(groupCoordinator.consumerGroupHeartbeat(
      requestChannelRequest.context,
      consumerGroupHeartbeatRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val consumerGroupHeartbeatResponse = new ConsumerGroupHeartbeatResponseData()
      .setMemberId("member")

    future.complete(consumerGroupHeartbeatResponse)
    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(consumerGroupHeartbeatResponse, response.data)
  }

  @Test
  def testConsumerGroupHeartbeatRequestFutureFailed(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ConsumerGroupHeartbeatRequest.Builder(consumerGroupHeartbeatRequest).build())

    val future = new CompletableFuture[ConsumerGroupHeartbeatResponseData]()
    when(groupCoordinator.consumerGroupHeartbeat(
      requestChannelRequest.context,
      consumerGroupHeartbeatRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    future.completeExceptionally(Errors.FENCED_MEMBER_EPOCH.exception)
    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.FENCED_MEMBER_EPOCH.code, response.data.errorCode)
  }

  @Test
  def testConsumerGroupHeartbeatRequestGroupAuthorizationFailed(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ConsumerGroupHeartbeatRequest.Builder(consumerGroupHeartbeatRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.errorCode)
  }

  @Test
  def testConsumerGroupHeartbeatRequestTopicAuthorizationFailed(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])
    val groupId = "group"
    val fooTopicName = "foo"
    val barTopicName = "bar"
    val zarTopicName = "zar"

    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequestData()
      .setGroupId(groupId)
      .setSubscribedTopicNames(List(fooTopicName, barTopicName, zarTopicName).asJava)

    val requestChannelRequest = buildRequest(new ConsumerGroupHeartbeatRequest.Builder(consumerGroupHeartbeatRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val acls = Map(
      groupId -> AuthorizationResult.ALLOWED,
      fooTopicName -> AuthorizationResult.ALLOWED,
      barTopicName -> AuthorizationResult.DENIED,
    )
    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.asScala.map { action =>
        acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED)
      }.asJava
    }

    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, response.data.errorCode)
  }

  // r22 BLOCKER #210: a stock KIP-848 consumer that explicitly subscribes to a backing topic
  // name must be rejected with INVALID_REQUEST and a name-redacted message — without this
  // guard the subscription persists into __consumer_offsets and the assignment plumbing
  // forwards the backing UUID to the consumer, exposing co-tenant data via stock fetch.
  @Test
  def testConsumerGroupHeartbeatRequestRejectsBackingTopicSubscription(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])
    val groupId = "group"
    val logicalTopic = "tenant-logical"
    val backingTopic = "__concentration_backing_0"

    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequestData()
      .setGroupId(groupId)
      .setSubscribedTopicNames(List(logicalTopic, backingTopic).asJava)

    val requestChannelRequest = buildRequest(new ConsumerGroupHeartbeatRequest.Builder(consumerGroupHeartbeatRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]])).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.asScala.map(_ => AuthorizationResult.ALLOWED).asJava
    }
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)
    when(concentrationKernel.isBackingTopic(logicalTopic)).thenReturn(false)

    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.INVALID_REQUEST.code, response.data.errorCode)
    // The error message must not echo the backing-topic name (existence oracle for wildcard-authorized callers).
    assertFalse(response.data.errorMessage == null || response.data.errorMessage.contains(backingTopic),
      s"error message must not leak backing-topic name, was: ${response.data.errorMessage}")
    // The coordinator must NOT be invoked once we reject — backing must never reach group state.
    verify(groupCoordinator, never()).consumerGroupHeartbeat(any[RequestContext], any[ConsumerGroupHeartbeatRequestData])
  }

  // r22 #163 companion: with no authorizer configured, the backing-topic guard must still
  // run — backings are a deployment invariant, not an ACL concern.
  @Test
  def testConsumerGroupHeartbeatRequestRejectsBackingTopicWithoutAuthorizer(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])
    val groupId = "group"
    val backingTopic = "__concentration_backing_0"

    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequestData()
      .setGroupId(groupId)
      .setSubscribedTopicNames(List(backingTopic).asJava)

    val requestChannelRequest = buildRequest(new ConsumerGroupHeartbeatRequest.Builder(consumerGroupHeartbeatRequest).build())

    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)

    kafkaApis = createKafkaApis(
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.INVALID_REQUEST.code, response.data.errorCode)
    assertFalse(response.data.errorMessage == null || response.data.errorMessage.contains(backingTopic),
      s"error message must not leak backing-topic name, was: ${response.data.errorMessage}")
    verify(groupCoordinator, never()).consumerGroupHeartbeat(any[RequestContext], any[ConsumerGroupHeartbeatRequestData])
  }

  // r22 BLOCKER #227 (Agent 3 Finding #3): defense-in-depth on the ASSIGNMENT response of
  // ConsumerGroupHeartbeat. Even though #210 rejects explicit backing-name subscriptions at the
  // request boundary, the coordinator can still derive a backing topicId via (a) regex resolution
  // (pending #209/#169/#143) or (b) coordinator state predating the #210 deployment. Without the
  // assignment-response filter, the consumer receives the backing UUID, resolves it on its next
  // Fetch, and reads cross-tenant bytes.
  @Test
  def testConsumerGroupHeartbeatFiltersBackingTopicFromAssignment(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])
    val groupId = "group"
    val backingTopic = "backing-r22-227"
    val plainTopic = "plain-r22-227"
    val backingUuid = Uuid.randomUuid()
    val plainUuid = Uuid.randomUuid()

    when(metadataCache.getTopicName(backingUuid)).thenReturn(Some(backingTopic))
    when(metadataCache.getTopicName(plainUuid)).thenReturn(Some(plainTopic))
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)
    when(concentrationKernel.isBackingTopic(plainTopic)).thenReturn(false)

    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequestData().setGroupId(groupId)
    val requestChannelRequest = buildRequest(new ConsumerGroupHeartbeatRequest.Builder(consumerGroupHeartbeatRequest).build())

    val future = new CompletableFuture[ConsumerGroupHeartbeatResponseData]()
    when(groupCoordinator.consumerGroupHeartbeat(
      requestChannelRequest.context,
      consumerGroupHeartbeatRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis(featureVersions = Seq(GroupVersion.GV_1))
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    // Coordinator returns an assignment containing BOTH a backing topicId and a plain one.
    // The bug we're guarding is the broker forwarding the backing topicId unchanged.
    val coordinatorResponse = new ConsumerGroupHeartbeatResponseData()
      .setMemberId("member-227")
      .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
        .setTopicPartitions(List(
          new ConsumerGroupHeartbeatResponseData.TopicPartitions()
            .setTopicId(backingUuid)
            .setPartitions(List(Integer.valueOf(0), Integer.valueOf(1)).asJava),
          new ConsumerGroupHeartbeatResponseData.TopicPartitions()
            .setTopicId(plainUuid)
            .setPartitions(List(Integer.valueOf(0)).asJava)
        ).asJava))
    future.complete(coordinatorResponse)

    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](requestChannelRequest)
    val returnedTopicIds = response.data.assignment.topicPartitions.asScala.map(_.topicId).toSet
    assertFalse(returnedTopicIds.contains(backingUuid),
      s"ConsumerGroupHeartbeat leaked backing topicId '$backingUuid' (resolves to backing topic '$backingTopic') to the consumer; returned=$returnedTopicIds")
    assertTrue(returnedTopicIds.contains(plainUuid),
      s"ConsumerGroupHeartbeat over-filtered: dropped legitimate topicId '$plainUuid'; returned=$returnedTopicIds")
  }

  // r22 BLOCKER #227 — fail-closed on a UUID that doesn't resolve in this broker's metadata
  // cache. A stale-cache window cannot be used to forward a backing UUID under a None
  // resolution, matching the share-state #171 precedent. A legit assignment dropped this way is
  // re-issued on the next heartbeat cycle, which is the standard KIP-848 reconciliation contract.
  @Test
  def testConsumerGroupHeartbeatDropsUnresolvableTopicIdFromAssignment(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])
    val groupId = "group"
    val unresolvableUuid = Uuid.randomUuid()
    val resolvableUuid = Uuid.randomUuid()
    val resolvableTopic = "resolvable-r22-227"

    when(metadataCache.getTopicName(unresolvableUuid)).thenReturn(None)
    when(metadataCache.getTopicName(resolvableUuid)).thenReturn(Some(resolvableTopic))
    when(concentrationKernel.isBackingTopic(resolvableTopic)).thenReturn(false)

    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequestData().setGroupId(groupId)
    val requestChannelRequest = buildRequest(new ConsumerGroupHeartbeatRequest.Builder(consumerGroupHeartbeatRequest).build())

    val future = new CompletableFuture[ConsumerGroupHeartbeatResponseData]()
    when(groupCoordinator.consumerGroupHeartbeat(
      requestChannelRequest.context,
      consumerGroupHeartbeatRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis(featureVersions = Seq(GroupVersion.GV_1))
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val coordinatorResponse = new ConsumerGroupHeartbeatResponseData()
      .setMemberId("member-227b")
      .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
        .setTopicPartitions(List(
          new ConsumerGroupHeartbeatResponseData.TopicPartitions()
            .setTopicId(unresolvableUuid)
            .setPartitions(List(Integer.valueOf(0)).asJava),
          new ConsumerGroupHeartbeatResponseData.TopicPartitions()
            .setTopicId(resolvableUuid)
            .setPartitions(List(Integer.valueOf(0)).asJava)
        ).asJava))
    future.complete(coordinatorResponse)

    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](requestChannelRequest)
    val returnedTopicIds = response.data.assignment.topicPartitions.asScala.map(_.topicId).toSet
    assertFalse(returnedTopicIds.contains(unresolvableUuid),
      s"ConsumerGroupHeartbeat failed open on unresolvable topicId '$unresolvableUuid' — could be used to forward a backing UUID through a stale-cache window; returned=$returnedTopicIds")
    assertTrue(returnedTopicIds.contains(resolvableUuid),
      s"ConsumerGroupHeartbeat over-filtered: dropped resolvable plain topicId '$resolvableUuid'; returned=$returnedTopicIds")
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testConsumerGroupDescribe(includeAuthorizedOperations: Boolean): Unit = {
    val fooTopicName = "foo"
    val barTopicName = "bar"
    metadataCache = mock(classOf[KRaftMetadataCache])

    val groupIds = List("group-id-0", "group-id-1", "group-id-2").asJava
    val consumerGroupDescribeRequestData = new ConsumerGroupDescribeRequestData()
      .setIncludeAuthorizedOperations(includeAuthorizedOperations)
    consumerGroupDescribeRequestData.groupIds.addAll(groupIds)
    val requestChannelRequest = buildRequest(new ConsumerGroupDescribeRequest.Builder(consumerGroupDescribeRequestData, true).build())

    val future = new CompletableFuture[util.List[ConsumerGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.consumerGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val member0 = new ConsumerGroupDescribeResponseData.Member()
      .setMemberId("member0")
      .setAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new TopicPartitions().setTopicName(fooTopicName)).asJava))
      .setTargetAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new TopicPartitions().setTopicName(fooTopicName)).asJava))

    val member1 = new ConsumerGroupDescribeResponseData.Member()
      .setMemberId("member1")
      .setAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new TopicPartitions().setTopicName(fooTopicName)).asJava))
      .setTargetAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new TopicPartitions().setTopicName(fooTopicName),
          new TopicPartitions().setTopicName(barTopicName)).asJava))

    val member2 = new ConsumerGroupDescribeResponseData.Member()
      .setMemberId("member2")
      .setAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new TopicPartitions().setTopicName(barTopicName)).asJava))
      .setTargetAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new TopicPartitions().setTopicName(fooTopicName)).asJava))

    future.complete(List(
      new DescribedGroup()
        .setGroupId(groupIds.get(0))
        .setMembers(List(member0).asJava),
      new DescribedGroup()
        .setGroupId(groupIds.get(1))
        .setMembers(List(member0, member1).asJava),
      new DescribedGroup()
        .setGroupId(groupIds.get(2))
        .setMembers(List(member2).asJava)
    ).asJava)

    var authorizedOperationsInt = Int.MinValue
    if (includeAuthorizedOperations) {
      authorizedOperationsInt = Utils.to32BitField(
        AclEntry.supportedOperations(ResourceType.GROUP).asScala
          .map(_.code.asInstanceOf[JByte]).asJava)
    }

    // Can't reuse the above list here because we would not test the implementation in KafkaApis then
    val describedGroups = List(
      new DescribedGroup()
        .setGroupId(groupIds.get(0))
        .setMembers(List(member0).asJava),
      new DescribedGroup()
        .setGroupId(groupIds.get(1))
        .setMembers(List(member0, member1).asJava),
      new DescribedGroup()
        .setGroupId(groupIds.get(2))
        .setMembers(List(member2).asJava)
    ).map(group => group.setAuthorizedOperations(authorizedOperationsInt))
    val expectedConsumerGroupDescribeResponseData = new ConsumerGroupDescribeResponseData()
      .setGroups(describedGroups.asJava)

    val response = verifyNoThrottling[ConsumerGroupDescribeResponse](requestChannelRequest)

    assertEquals(expectedConsumerGroupDescribeResponseData, response.data)
  }

  @Test
  def testConsumerGroupDescribeReturnsUnsupportedVersion(): Unit = {
    val groupId = "group0"
    val consumerGroupDescribeRequestData = new ConsumerGroupDescribeRequestData()
    consumerGroupDescribeRequestData.groupIds.add(groupId)
    val requestChannelRequest = buildRequest(new ConsumerGroupDescribeRequest.Builder(consumerGroupDescribeRequestData, true).build())

    val errorCode = Errors.UNSUPPORTED_VERSION.code
    val expectedDescribedGroup = new DescribedGroup().setGroupId(groupId).setErrorCode(errorCode)
    val expectedResponse = new ConsumerGroupDescribeResponseData()
    expectedResponse.groups.add(expectedDescribedGroup)
    metadataCache = {
      val cache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_1)
      val delta = new MetadataDelta(MetadataImage.EMPTY);
      delta.replay(new FeatureLevelRecord()
        .setName(MetadataVersion.FEATURE_NAME)
        .setFeatureLevel(MetadataVersion.MINIMUM_VERSION.featureLevel())
      )
      cache.setImage(delta.apply(MetadataProvenance.EMPTY))
      cache
    }
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)
    val response = verifyNoThrottling[ConsumerGroupDescribeResponse](requestChannelRequest)

    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testConsumerGroupDescribeAuthorizationFailed(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val consumerGroupDescribeRequestData = new ConsumerGroupDescribeRequestData()
    consumerGroupDescribeRequestData.groupIds.add("group-id")
    val requestChannelRequest = buildRequest(new ConsumerGroupDescribeRequest.Builder(consumerGroupDescribeRequestData, true).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)

    val future = new CompletableFuture[util.List[ConsumerGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.consumerGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    future.complete(List().asJava)
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[ConsumerGroupDescribeResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.groups.get(0).errorCode)
  }

  @Test
  def testConsumerGroupDescribeFutureFailed(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val consumerGroupDescribeRequestData = new ConsumerGroupDescribeRequestData()
    consumerGroupDescribeRequestData.groupIds.add("group-id")
    val requestChannelRequest = buildRequest(new ConsumerGroupDescribeRequest.Builder(consumerGroupDescribeRequestData, true).build())

    val future = new CompletableFuture[util.List[ConsumerGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.consumerGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    future.completeExceptionally(Errors.FENCED_MEMBER_EPOCH.exception)
    val response = verifyNoThrottling[ConsumerGroupDescribeResponse](requestChannelRequest)
    assertEquals(Errors.FENCED_MEMBER_EPOCH.code, response.data.groups.get(0).errorCode)
  }

  @Test
  def testConsumerGroupDescribeFilterUnauthorizedTopics(): Unit = {
    val fooTopicName = "foo"
    val barTopicName = "bar"
    val errorMessage = "The group has described topic(s) that the client is not authorized to describe."

    metadataCache = mock(classOf[KRaftMetadataCache])

    val groupIds = List("group-id-0", "group-id-1", "group-id-2").asJava
    val consumerGroupDescribeRequestData = new ConsumerGroupDescribeRequestData()
      .setGroupIds(groupIds)
    val requestChannelRequest = buildRequest(new ConsumerGroupDescribeRequest.Builder(consumerGroupDescribeRequestData, true).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val acls = Map(
      groupIds.get(0) -> AuthorizationResult.ALLOWED,
      groupIds.get(1) -> AuthorizationResult.ALLOWED,
      groupIds.get(2) -> AuthorizationResult.ALLOWED,
      fooTopicName    -> AuthorizationResult.ALLOWED,
      barTopicName    -> AuthorizationResult.DENIED,
    )
    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.asScala.map { action =>
        acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED)
      }.asJava
    }

    val future = new CompletableFuture[util.List[ConsumerGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.consumerGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val member0 = new ConsumerGroupDescribeResponseData.Member()
      .setMemberId("member0")
      .setAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new TopicPartitions().setTopicName(fooTopicName)).asJava))
      .setTargetAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new TopicPartitions().setTopicName(fooTopicName)).asJava))

    val member1 = new ConsumerGroupDescribeResponseData.Member()
      .setMemberId("member1")
      .setAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new TopicPartitions().setTopicName(fooTopicName)).asJava))
      .setTargetAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new TopicPartitions().setTopicName(fooTopicName),
          new TopicPartitions().setTopicName(barTopicName)).asJava))

    val member2 = new ConsumerGroupDescribeResponseData.Member()
      .setMemberId("member2")
      .setAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new TopicPartitions().setTopicName(barTopicName)).asJava))
      .setTargetAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new TopicPartitions().setTopicName(fooTopicName)).asJava))

    future.complete(List(
      new DescribedGroup()
        .setGroupId(groupIds.get(0))
        .setMembers(List(member0).asJava),
      new DescribedGroup()
        .setGroupId(groupIds.get(1))
        .setMembers(List(member0, member1).asJava),
      new DescribedGroup()
        .setGroupId(groupIds.get(2))
        .setMembers(List(member2).asJava)
    ).asJava)

    val expectedConsumerGroupDescribeResponseData = new ConsumerGroupDescribeResponseData()
      .setGroups(List(
        new DescribedGroup()
          .setGroupId(groupIds.get(0))
          .setMembers(List(member0).asJava),
        new DescribedGroup()
          .setGroupId(groupIds.get(1))
          .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
          .setErrorMessage(errorMessage),
        new DescribedGroup()
          .setGroupId(groupIds.get(2))
          .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
          .setErrorMessage(errorMessage)
      ).asJava)

    val response = verifyNoThrottling[ConsumerGroupDescribeResponse](requestChannelRequest)

    assertEquals(expectedConsumerGroupDescribeResponseData, response.data)
  }

  // r22 BLOCKER #211: a group whose member assignment contains a backing topic must be
  // collapsed to TOPIC_AUTHORIZATION_FAILED with empty members and a generic message — the
  // response must be indistinguishable from a vanilla unauthorized-topic case so an attacker
  // cannot use the error code differential as an existence oracle for backing topics.
  // Mirrors the r20 #173/#174 pattern.
  @Test
  def testConsumerGroupDescribeStripsBackingTopicFromAssignment(): Unit = {
    val logicalTopic = "tenant-logical"
    val backingTopic = "__concentration_backing_0"
    val errorMessage = "The group has described topic(s) that the client is not authorized to describe."

    metadataCache = mock(classOf[KRaftMetadataCache])

    val groupIds = List("group-id-clean", "group-id-tainted").asJava
    val consumerGroupDescribeRequestData = new ConsumerGroupDescribeRequestData()
      .setGroupIds(groupIds)
    val requestChannelRequest = buildRequest(new ConsumerGroupDescribeRequest.Builder(consumerGroupDescribeRequestData, true).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val acls = Map(
      groupIds.get(0) -> AuthorizationResult.ALLOWED,
      groupIds.get(1) -> AuthorizationResult.ALLOWED,
      logicalTopic    -> AuthorizationResult.ALLOWED,
      // Backing name is also authorized for DESCRIBE (wildcard authz) — the kernel guard
      // must still strip it. This pins auth-first + shadow-second precedence.
      backingTopic    -> AuthorizationResult.ALLOWED,
    )
    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.asScala.map { action =>
        acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED)
      }.asJava
    }
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)
    when(concentrationKernel.isBackingTopic(logicalTopic)).thenReturn(false)

    val future = new CompletableFuture[util.List[ConsumerGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.consumerGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val cleanMember = new ConsumerGroupDescribeResponseData.Member()
      .setMemberId("clean-member")
      .setAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(new TopicPartitions().setTopicName(logicalTopic)).asJava))
      .setTargetAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(new TopicPartitions().setTopicName(logicalTopic)).asJava))

    val taintedMember = new ConsumerGroupDescribeResponseData.Member()
      .setMemberId("tainted-member")
      .setAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(new TopicPartitions().setTopicName(logicalTopic)).asJava))
      .setTargetAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        // Backing in target assignment — could only have got there via a regex match or
        // a classic JoinGroup path; the response filter is the last line of defence.
        .setTopicPartitions(List(new TopicPartitions().setTopicName(backingTopic)).asJava))

    future.complete(List(
      new DescribedGroup()
        .setGroupId(groupIds.get(0))
        .setMembers(List(cleanMember).asJava),
      new DescribedGroup()
        .setGroupId(groupIds.get(1))
        .setMembers(List(taintedMember).asJava)
    ).asJava)

    val response = verifyNoThrottling[ConsumerGroupDescribeResponse](requestChannelRequest)
    val groups = response.data.groups.asScala.toList
    assertEquals(2, groups.size)

    // Clean group passes through unchanged.
    val cleanGroup = groups.find(_.groupId == groupIds.get(0)).get
    assertEquals(Errors.NONE.code, cleanGroup.errorCode)
    assertEquals(1, cleanGroup.members.size)

    // Tainted group: collapsed to TOPIC_AUTHORIZATION_FAILED, empty members, generic message
    // that does NOT mention the backing-topic name.
    val taintedGroup = groups.find(_.groupId == groupIds.get(1)).get
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, taintedGroup.errorCode)
    assertEquals(errorMessage, taintedGroup.errorMessage)
    assertEquals(0, taintedGroup.members.size)
    assertFalse(taintedGroup.errorMessage.contains(backingTopic),
      s"error message must not leak backing-topic name, was: ${taintedGroup.errorMessage}")
  }

  @Test
  def testGetTelemetrySubscriptions(): Unit = {
    val request = buildRequest(new GetTelemetrySubscriptionsRequest.Builder(
      new GetTelemetrySubscriptionsRequestData(), true).build())

    when(clientMetricsManager.isTelemetryReceiverConfigured).thenReturn(true)
    when(clientMetricsManager.processGetTelemetrySubscriptionRequest(any[GetTelemetrySubscriptionsRequest](),
      any[RequestContext]())).thenReturn(new GetTelemetrySubscriptionsResponse(
      new GetTelemetrySubscriptionsResponseData()))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)

    val response = verifyNoThrottling[GetTelemetrySubscriptionsResponse](request)

    val expectedResponse = new GetTelemetrySubscriptionsResponseData()
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testGetTelemetrySubscriptionsWithException(): Unit = {
    val request = buildRequest(new GetTelemetrySubscriptionsRequest.Builder(
      new GetTelemetrySubscriptionsRequestData(), true).build())

    when(clientMetricsManager.isTelemetryReceiverConfigured).thenReturn(true)
    when(clientMetricsManager.processGetTelemetrySubscriptionRequest(any[GetTelemetrySubscriptionsRequest](),
      any[RequestContext]())).thenThrow(new RuntimeException("test"))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)

    val response = verifyNoThrottling[GetTelemetrySubscriptionsResponse](request)

    val expectedResponse = new GetTelemetrySubscriptionsResponseData().setErrorCode(Errors.INVALID_REQUEST.code)
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testPushTelemetry(): Unit = {
    val request = buildRequest(new PushTelemetryRequest.Builder(new PushTelemetryRequestData(), true).build())

    when(clientMetricsManager.isTelemetryReceiverConfigured).thenReturn(true)
    when(clientMetricsManager.processPushTelemetryRequest(any[PushTelemetryRequest](), any[RequestContext]()))
      .thenReturn(new PushTelemetryResponse(new PushTelemetryResponseData()))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)
    val response = verifyNoThrottling[PushTelemetryResponse](request)

    val expectedResponse = new PushTelemetryResponseData().setErrorCode(Errors.NONE.code)
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testPushTelemetryWithException(): Unit = {
    val request = buildRequest(new PushTelemetryRequest.Builder(new PushTelemetryRequestData(), true).build())

    when(clientMetricsManager.isTelemetryReceiverConfigured).thenReturn(true)
    when(clientMetricsManager.processPushTelemetryRequest(any[PushTelemetryRequest](), any[RequestContext]()))
      .thenThrow(new RuntimeException("test"))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)
    val response = verifyNoThrottling[PushTelemetryResponse](request)

    val expectedResponse = new PushTelemetryResponseData().setErrorCode(Errors.INVALID_REQUEST.code)
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testListClientMetricsResources(): Unit = {
    val request = buildRequest(new ListClientMetricsResourcesRequest.Builder(new ListClientMetricsResourcesRequestData()).build())
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val resources = new mutable.HashSet[String]
    resources.add("test1")
    resources.add("test2")
    when(clientMetricsManager.listClientMetricsResources).thenReturn(resources.asJava)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)
    val response = verifyNoThrottling[ListClientMetricsResourcesResponse](request)
    val expectedResponse = new ListClientMetricsResourcesResponseData().setClientMetricsResources(
      resources.map(resource => new ClientMetricsResource().setName(resource)).toBuffer.asJava)
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testListClientMetricsResourcesEmptyResponse(): Unit = {
    val request = buildRequest(new ListClientMetricsResourcesRequest.Builder(new ListClientMetricsResourcesRequestData()).build())
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val resources = new mutable.HashSet[String]
    when(clientMetricsManager.listClientMetricsResources).thenReturn(resources.asJava)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)
    val response = verifyNoThrottling[ListClientMetricsResourcesResponse](request)
    val expectedResponse = new ListClientMetricsResourcesResponseData()
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testListClientMetricsResourcesWithException(): Unit = {
    val request = buildRequest(new ListClientMetricsResourcesRequest.Builder(new ListClientMetricsResourcesRequestData()).build())
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    when(clientMetricsManager.listClientMetricsResources).thenThrow(new RuntimeException("test"))
    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)
    val response = verifyNoThrottling[ListClientMetricsResourcesResponse](request)

    val expectedResponse = new ListClientMetricsResourcesResponseData().setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testShareGroupHeartbeatReturnsUnsupportedVersion(): Unit = {
    val shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ShareGroupHeartbeatRequest.Builder(shareGroupHeartbeatRequest, true).build())
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val expectedHeartbeatResponse = new ShareGroupHeartbeatResponseData()
      .setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    val response = verifyNoThrottling[ShareGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(expectedHeartbeatResponse, response.data)
  }

  @Test
  def testShareGroupHeartbeatRequest(): Unit = {
    val shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ShareGroupHeartbeatRequest.Builder(shareGroupHeartbeatRequest, true).build())

    val future = new CompletableFuture[ShareGroupHeartbeatResponseData]()
    when(groupCoordinator.shareGroupHeartbeat(
      requestChannelRequest.context,
      shareGroupHeartbeatRequest
    )).thenReturn(future)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val shareGroupHeartbeatResponse = new ShareGroupHeartbeatResponseData()
      .setMemberId("member")

    future.complete(shareGroupHeartbeatResponse)
    val response = verifyNoThrottling[ShareGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(shareGroupHeartbeatResponse, response.data)
  }

  @Test
  def testShareGroupHeartbeatRequestAuthorizationFailed(): Unit = {
    val shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ShareGroupHeartbeatRequest.Builder(shareGroupHeartbeatRequest, true).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      authorizer = Some(authorizer),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[ShareGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.errorCode)
  }

  @Test
  def testShareGroupHeartbeatRequestFutureFailed(): Unit = {
    val shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ShareGroupHeartbeatRequest.Builder(shareGroupHeartbeatRequest, true).build())

    val future = new CompletableFuture[ShareGroupHeartbeatResponseData]()
    when(groupCoordinator.shareGroupHeartbeat(
      requestChannelRequest.context,
      shareGroupHeartbeatRequest
    )).thenReturn(future)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    future.completeExceptionally(Errors.FENCED_MEMBER_EPOCH.exception)
    val response = verifyNoThrottling[ShareGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.FENCED_MEMBER_EPOCH.code, response.data.errorCode)
  }

  // r22 HIGH #162: regression for the #158 share-group backing-name reject. The original
  // error message echoed the rejected backing names, which let a wildcard-authorized caller
  // distinguish real backing topics from arbitrary names that happen to authz. The redacted
  // message must NOT mention any backing-topic name.
  @Test
  def testShareGroupHeartbeatRejectsBackingTopicWithoutLeakingName(): Unit = {
    val groupId = "share-group"
    val backingTopic = "__concentration_backing_0"
    val shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequestData()
      .setGroupId(groupId)
      .setSubscribedTopicNames(List(backingTopic).asJava)

    val requestChannelRequest = buildRequest(new ShareGroupHeartbeatRequest.Builder(shareGroupHeartbeatRequest, true).build())
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]])).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.asScala.map(_ => AuthorizationResult.ALLOWED).asJava
    }
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)

    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[ShareGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.INVALID_REQUEST.code, response.data.errorCode)
    assertNotNull(response.data.errorMessage)
    assertFalse(response.data.errorMessage.contains(backingTopic),
      s"error message must not leak backing-topic name, was: ${response.data.errorMessage}")
    verify(groupCoordinator, never()).shareGroupHeartbeat(any[RequestContext], any[ShareGroupHeartbeatRequestData])
  }

  // r22 BLOCKER #228 (sibling of #227): defense-in-depth on the ASSIGNMENT response of
  // ShareGroupHeartbeat. The r20 #158 fix rejects explicit backing-name subscriptions at the
  // request boundary, but the coordinator can still derive a backing topicId via (a) regex
  // resolution (pending #168/#169/#143) or (b) share-group state predating the #158 deployment.
  // Without the assignment-response filter, the consumer receives the backing UUID, resolves
  // it on its next ShareFetch, and reads cross-tenant bytes.
  @Test
  def testShareGroupHeartbeatFiltersBackingTopicFromAssignment(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])
    val groupId = "share-group"
    val backingTopic = "backing-r22-228"
    val plainTopic = "plain-r22-228"
    val backingUuid = Uuid.randomUuid()
    val plainUuid = Uuid.randomUuid()

    when(metadataCache.getTopicName(backingUuid)).thenReturn(Some(backingTopic))
    when(metadataCache.getTopicName(plainUuid)).thenReturn(Some(plainTopic))
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)
    when(concentrationKernel.isBackingTopic(plainTopic)).thenReturn(false)

    val shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequestData().setGroupId(groupId)
    val requestChannelRequest = buildRequest(new ShareGroupHeartbeatRequest.Builder(shareGroupHeartbeatRequest, true).build())

    val future = new CompletableFuture[ShareGroupHeartbeatResponseData]()
    when(groupCoordinator.shareGroupHeartbeat(
      requestChannelRequest.context,
      shareGroupHeartbeatRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    // Coordinator returns an assignment containing BOTH a backing topicId and a plain one.
    // The bug we're guarding is the broker forwarding the backing topicId unchanged.
    val coordinatorResponse = new ShareGroupHeartbeatResponseData()
      .setMemberId("member-228")
      .setAssignment(new ShareGroupHeartbeatResponseData.Assignment()
        .setTopicPartitions(List(
          new ShareGroupHeartbeatResponseData.TopicPartitions()
            .setTopicId(backingUuid)
            .setPartitions(List(Integer.valueOf(0), Integer.valueOf(1)).asJava),
          new ShareGroupHeartbeatResponseData.TopicPartitions()
            .setTopicId(plainUuid)
            .setPartitions(List(Integer.valueOf(0)).asJava)
        ).asJava))
    future.complete(coordinatorResponse)

    val response = verifyNoThrottling[ShareGroupHeartbeatResponse](requestChannelRequest)
    val returnedTopicIds = response.data.assignment.topicPartitions.asScala.map(_.topicId).toSet
    assertFalse(returnedTopicIds.contains(backingUuid),
      s"ShareGroupHeartbeat leaked backing topicId '$backingUuid' (resolves to backing topic '$backingTopic') to the consumer; returned=$returnedTopicIds")
    assertTrue(returnedTopicIds.contains(plainUuid),
      s"ShareGroupHeartbeat over-filtered: dropped legitimate topicId '$plainUuid'; returned=$returnedTopicIds")
  }

  // r22 BLOCKER #228 — fail-closed on a UUID that doesn't resolve in this broker's metadata
  // cache. A stale-cache window cannot be used to forward a backing UUID under a None
  // resolution, matching the share-state #171 precedent (and matching the symmetric #227
  // policy for ConsumerGroupHeartbeat). A legit assignment dropped this way is re-issued on
  // the next heartbeat cycle.
  @Test
  def testShareGroupHeartbeatDropsUnresolvableTopicIdFromAssignment(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])
    val groupId = "share-group"
    val unresolvableUuid = Uuid.randomUuid()
    val resolvableUuid = Uuid.randomUuid()
    val resolvableTopic = "resolvable-r22-228"

    when(metadataCache.getTopicName(unresolvableUuid)).thenReturn(None)
    when(metadataCache.getTopicName(resolvableUuid)).thenReturn(Some(resolvableTopic))
    when(concentrationKernel.isBackingTopic(resolvableTopic)).thenReturn(false)

    val shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequestData().setGroupId(groupId)
    val requestChannelRequest = buildRequest(new ShareGroupHeartbeatRequest.Builder(shareGroupHeartbeatRequest, true).build())

    val future = new CompletableFuture[ShareGroupHeartbeatResponseData]()
    when(groupCoordinator.shareGroupHeartbeat(
      requestChannelRequest.context,
      shareGroupHeartbeatRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val coordinatorResponse = new ShareGroupHeartbeatResponseData()
      .setMemberId("member-228b")
      .setAssignment(new ShareGroupHeartbeatResponseData.Assignment()
        .setTopicPartitions(List(
          new ShareGroupHeartbeatResponseData.TopicPartitions()
            .setTopicId(unresolvableUuid)
            .setPartitions(List(Integer.valueOf(0)).asJava),
          new ShareGroupHeartbeatResponseData.TopicPartitions()
            .setTopicId(resolvableUuid)
            .setPartitions(List(Integer.valueOf(0)).asJava)
        ).asJava))
    future.complete(coordinatorResponse)

    val response = verifyNoThrottling[ShareGroupHeartbeatResponse](requestChannelRequest)
    val returnedTopicIds = response.data.assignment.topicPartitions.asScala.map(_.topicId).toSet
    assertFalse(returnedTopicIds.contains(unresolvableUuid),
      s"ShareGroupHeartbeat failed open on unresolvable topicId '$unresolvableUuid' — could be used to forward a backing UUID through a stale-cache window; returned=$returnedTopicIds")
    assertTrue(returnedTopicIds.contains(resolvableUuid),
      s"ShareGroupHeartbeat over-filtered: dropped resolvable plain topicId '$resolvableUuid'; returned=$returnedTopicIds")
  }

  @Test
  def testShareGroupDescribeSuccess(): Unit = {
    val groupIds = List("share-group-id-0", "share-group-id-1").asJava
    val describedGroups: util.List[ShareGroupDescribeResponseData.DescribedGroup] = List(
      new ShareGroupDescribeResponseData.DescribedGroup().setGroupId(groupIds.get(0)),
      new ShareGroupDescribeResponseData.DescribedGroup().setGroupId(groupIds.get(1))
    ).asJava
    getShareGroupDescribeResponse(groupIds, Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true")
      , true, null, describedGroups)
  }

  @Test
  def testShareGroupDescribeReturnsUnsupportedVersion(): Unit = {
    val groupIds = List("share-group-id-0", "share-group-id-1").asJava
    val describedGroups: util.List[ShareGroupDescribeResponseData.DescribedGroup] = List(
      new ShareGroupDescribeResponseData.DescribedGroup().setGroupId(groupIds.get(0)),
      new ShareGroupDescribeResponseData.DescribedGroup().setGroupId(groupIds.get(1))
    ).asJava
    val response = getShareGroupDescribeResponse(groupIds, Map.empty, false, null, describedGroups)
    assertNotNull(response.data)
    assertEquals(2, response.data.groups.size)
    response.data.groups.forEach(group => assertEquals(Errors.UNSUPPORTED_VERSION.code(), group.errorCode()))
  }

  @Test
  def testShareGroupDescribeRequestAuthorizationFailed(): Unit = {
    val groupIds = List("share-group-id-0", "share-group-id-1").asJava
    val describedGroups: util.List[ShareGroupDescribeResponseData.DescribedGroup] = List().asJava
    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
    val response = getShareGroupDescribeResponse(groupIds, Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true")
      , false, authorizer, describedGroups)
    assertNotNull(response.data)
    assertEquals(2, response.data.groups.size)
    response.data.groups.forEach(group => assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code(), group.errorCode()))
  }

  @Test
  def testShareGroupDescribeRequestAuthorizationFailedForOneGroup(): Unit = {
    val groupIds = List("share-group-id-fail-0", "share-group-id-1").asJava
    val describedGroups: util.List[ShareGroupDescribeResponseData.DescribedGroup] = List(
      new ShareGroupDescribeResponseData.DescribedGroup().setGroupId(groupIds.get(1))
    ).asJava

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava, Seq(AuthorizationResult.ALLOWED).asJava)

    val response = getShareGroupDescribeResponse(groupIds, Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true")
      , false, authorizer, describedGroups)

    assertNotNull(response.data)
    assertEquals(2, response.data.groups.size)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code(), response.data.groups.get(0).errorCode())
    assertEquals(Errors.NONE.code(), response.data.groups.get(1).errorCode())
  }

  @Test
  def testReadShareGroupStateSuccess(): Unit = {
    val topicId = Uuid.randomUuid();
    val readRequestData = new ReadShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(List(
        new ReadShareGroupStateRequestData.ReadStateData()
          .setTopicId(topicId)
          .setPartitions(List(
            new ReadShareGroupStateRequestData.PartitionData()
              .setPartition(1)
              .setLeaderEpoch(1)
          ).asJava)
      ).asJava)

    val readStateResultData: util.List[ReadShareGroupStateResponseData.ReadStateResult] = List(
      new ReadShareGroupStateResponseData.ReadStateResult()
        .setTopicId(topicId)
        .setPartitions(List(
          new ReadShareGroupStateResponseData.PartitionResult()
            .setPartition(1)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(null)
            .setStateEpoch(1)
            .setStartOffset(10)
            .setStateBatches(List(
              new ReadShareGroupStateResponseData.StateBatch()
                .setFirstOffset(11)
                .setLastOffset(15)
                .setDeliveryState(0)
                .setDeliveryCount(1)
            ).asJava)
        ).asJava)
    ).asJava

    val config = Map(
      ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true",
    )

    val response = getReadShareGroupResponse(
      readRequestData,
      config ++ ShareCoordinatorTestConfig.testConfigMap().asScala,
      verifyNoErr = true,
      null,
      readStateResultData
    )

    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
  }

  @Test
  def testReadShareGroupStateAuthorizationFailed(): Unit = {
    val topicId = Uuid.randomUuid();
    val readRequestData = new ReadShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(List(
        new ReadShareGroupStateRequestData.ReadStateData()
          .setTopicId(topicId)
          .setPartitions(List(
            new ReadShareGroupStateRequestData.PartitionData()
              .setPartition(1)
              .setLeaderEpoch(1)
          ).asJava)
      ).asJava)

    val readStateResultData: util.List[ReadShareGroupStateResponseData.ReadStateResult] = List(
      new ReadShareGroupStateResponseData.ReadStateResult()
        .setTopicId(topicId)
        .setPartitions(List(
          new ReadShareGroupStateResponseData.PartitionResult()
            .setPartition(1)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(null)
            .setStateEpoch(1)
            .setStartOffset(10)
            .setStateBatches(List(
              new ReadShareGroupStateResponseData.StateBatch()
                .setFirstOffset(11)
                .setLastOffset(15)
                .setDeliveryState(0)
                .setDeliveryCount(1)
            ).asJava)
        ).asJava)
    ).asJava

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava, Seq(AuthorizationResult.ALLOWED).asJava)

    val config = Map(
      ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true",
    )

    val response = getReadShareGroupResponse(
      readRequestData,
      config ++ ShareCoordinatorTestConfig.testConfigMap().asScala,
      verifyNoErr = false,
      authorizer,
      readStateResultData
    )

    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
    response.data.results.forEach(readResult => {
      assertEquals(1, readResult.partitions.size)
      assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code(), readResult.partitions.get(0).errorCode())
    })
  }

  @Test
  def testReadShareGroupStateRejectsBackingTopic(): Unit = {
    // r19 ADV-A BLOCKER #144 — share-state RPCs key on TopicId; a misbehaving share-broker (or a
    // future bug in the share-fetch path that bypasses #136 via topic-ID) could send a backing
    // topic's UUID here, and the share-coordinator would happily store/retrieve state under
    // (groupId, backingTopicId, partition) — fan-out across every logical tenant on that backing.
    // Defense-in-depth at the share-coord entry: resolve UUID → name and reject backing topics.
    val backingTopic = "backing-topic"
    val backingTopicId = Uuid.randomUuid()
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(backingTopic, numPartitions = 4, topicId = backingTopicId)
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)

    val readRequestData = new ReadShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(List(
        new ReadShareGroupStateRequestData.ReadStateData()
          .setTopicId(backingTopicId)
          .setPartitions(List(
            new ReadShareGroupStateRequestData.PartitionData()
              .setPartition(1)
              .setLeaderEpoch(1),
            new ReadShareGroupStateRequestData.PartitionData()
              .setPartition(2)
              .setLeaderEpoch(1)
          ).asJava)
      ).asJava)

    val requestChannelRequest = buildRequest(new ReadShareGroupStateRequest.Builder(readRequestData, true).build())
    kafkaApis = createKafkaApis(
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true") ++
        ShareCoordinatorTestConfig.testConfigMap().asScala
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching())

    val response = verifyNoThrottling[ReadShareGroupStateResponse](requestChannelRequest)
    assertEquals(1, response.data.results.size)
    val topicResult = response.data.results.get(0)
    assertEquals(backingTopicId, topicResult.topicId)
    assertEquals(2, topicResult.partitions.size)
    topicResult.partitions.forEach { partResult =>
      assertEquals(Errors.INVALID_REQUEST.code(), partResult.errorCode(),
        s"partition ${partResult.partition} should be rejected")
    }
    // The share coordinator must never see this backing-topic key.
    verify(shareCoordinator, never()).readState(any[RequestContext], any[ReadShareGroupStateRequestData])
  }

  @Test
  def testReadShareGroupStatePartitionsBackingFromLegitimate(): Unit = {
    // Mixed request: one backing topic (rejected) + one legitimate topic (forwarded).
    // Verifies the rejected topic does not poison the legitimate one and that the share
    // coordinator is called with only the legitimate topic.
    val backingTopic = "backing-topic"
    val backingTopicId = Uuid.randomUuid()
    val legitTopic = "legit-topic"
    val legitTopicId = Uuid.randomUuid()
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(backingTopic, numPartitions = 4, topicId = backingTopicId)
    addTopicToMetadataCache(legitTopic, numPartitions = 2, topicId = legitTopicId)
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)
    when(concentrationKernel.isBackingTopic(legitTopic)).thenReturn(false)

    val readRequestData = new ReadShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(List(
        new ReadShareGroupStateRequestData.ReadStateData()
          .setTopicId(backingTopicId)
          .setPartitions(List(
            new ReadShareGroupStateRequestData.PartitionData().setPartition(0).setLeaderEpoch(1)
          ).asJava),
        new ReadShareGroupStateRequestData.ReadStateData()
          .setTopicId(legitTopicId)
          .setPartitions(List(
            new ReadShareGroupStateRequestData.PartitionData().setPartition(0).setLeaderEpoch(1)
          ).asJava)
      ).asJava)

    val future = new CompletableFuture[ReadShareGroupStateResponseData]()
    when(shareCoordinator.readState(any[RequestContext], any[ReadShareGroupStateRequestData])).thenReturn(future)
    val requestChannelRequest = buildRequest(new ReadShareGroupStateRequest.Builder(readRequestData, true).build())
    kafkaApis = createKafkaApis(
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true") ++
        ShareCoordinatorTestConfig.testConfigMap().asScala
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching())

    // Share coordinator handles ONLY the legitimate topic; complete its future to release the response.
    future.complete(new ReadShareGroupStateResponseData()
      .setResults(List(
        new ReadShareGroupStateResponseData.ReadStateResult()
          .setTopicId(legitTopicId)
          .setPartitions(List(new ReadShareGroupStateResponseData.PartitionResult()
            .setPartition(0)
            .setErrorCode(Errors.NONE.code())
            .setStateEpoch(1)
            .setStartOffset(0)).asJava)
      ).asJava))

    val response = verifyNoThrottling[ReadShareGroupStateResponse](requestChannelRequest)
    val byTopic = response.data.results.asScala.map(r => r.topicId -> r).toMap
    assertEquals(2, byTopic.size)
    assertEquals(Errors.INVALID_REQUEST.code(), byTopic(backingTopicId).partitions.get(0).errorCode())
    assertEquals(Errors.NONE.code(), byTopic(legitTopicId).partitions.get(0).errorCode())

    // Verify the request forwarded to the coordinator excluded the backing topic.
    val forwarded = ArgumentCaptor.forClass(classOf[ReadShareGroupStateRequestData])
    verify(shareCoordinator).readState(any[RequestContext], forwarded.capture())
    val forwardedTopics = forwarded.getValue.topics.asScala.map(_.topicId).toSet
    assertFalse(forwardedTopics.contains(backingTopicId), "backing topic must not reach share-coordinator")
    assertEquals(Set(legitTopicId), forwardedTopics)
  }

  @Test
  def testReadShareGroupStateFailsClosedOnUnresolvedTopicId(): Unit = {
    // r21 BLOCKER #171 — when the broker's metadata image cannot resolve a TopicId (cache stale
    // mid-failover, image rebuild in progress, deleted-name not yet evicted across the refresh
    // window), the original guard at partitionShareStateTopicsForBackingTopic used
    //   metadataCache.getTopicName(id).exists(concentrationKernel.isBackingTopic)
    // which collapsed a None resolution into the forward bucket, letting a backing-topic UUID
    // through during that transient window — corrupting __share_group_state.
    // Fail-closed: an unresolved UUID is rejected with UNKNOWN_TOPIC_ID and the share-coord
    // never sees the request. This denies a brief UX window for legitimate UNKNOWN_TOPIC_ID
    // flows in exchange for closing the cross-tenant corruption surface.
    val mysteryTopicId = Uuid.randomUuid()
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    // INTENTIONALLY do NOT add this topic to the metadata cache — simulate the stale-image race.

    val readRequestData = new ReadShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(List(
        new ReadShareGroupStateRequestData.ReadStateData()
          .setTopicId(mysteryTopicId)
          .setPartitions(List(
            new ReadShareGroupStateRequestData.PartitionData().setPartition(0).setLeaderEpoch(1),
            new ReadShareGroupStateRequestData.PartitionData().setPartition(1).setLeaderEpoch(1)
          ).asJava)
      ).asJava)

    val requestChannelRequest = buildRequest(new ReadShareGroupStateRequest.Builder(readRequestData, true).build())
    kafkaApis = createKafkaApis(
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true") ++
        ShareCoordinatorTestConfig.testConfigMap().asScala
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching())

    val response = verifyNoThrottling[ReadShareGroupStateResponse](requestChannelRequest)
    assertEquals(1, response.data.results.size)
    val topicResult = response.data.results.get(0)
    assertEquals(mysteryTopicId, topicResult.topicId)
    assertEquals(2, topicResult.partitions.size)
    topicResult.partitions.forEach { partResult =>
      assertEquals(Errors.UNKNOWN_TOPIC_ID.code(), partResult.errorCode(),
        s"partition ${partResult.partition} must surface UNKNOWN_TOPIC_ID — never silently forwarded")
    }
    // The share coordinator must never see an unresolved topic-id — that's the whole point.
    verify(shareCoordinator, never()).readState(any[RequestContext], any[ReadShareGroupStateRequestData])
  }

  @Test
  def testWriteShareGroupStateFailsClosedOnUnresolvedTopicId(): Unit = {
    // r21 BLOCKER #171 — symmetric with the read path. WriteShareGroupState persists to
    // __share_group_state, so any backing UUID slipping through is unrecoverable without manual
    // coordinator-log surgery; failing closed on unresolved IDs is the only safe posture.
    val mysteryTopicId = Uuid.randomUuid()
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    // INTENTIONALLY do NOT add this topic to the metadata cache.

    val writeRequestData = new WriteShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(List(
        new WriteShareGroupStateRequestData.WriteStateData()
          .setTopicId(mysteryTopicId)
          .setPartitions(List(
            new WriteShareGroupStateRequestData.PartitionData()
              .setPartition(0).setLeaderEpoch(1).setStateEpoch(1).setStartOffset(0)
          ).asJava)
      ).asJava)

    val requestChannelRequest = buildRequest(new WriteShareGroupStateRequest.Builder(writeRequestData, true).build())
    kafkaApis = createKafkaApis(
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true") ++
        ShareCoordinatorTestConfig.testConfigMap().asScala
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching())

    val response = verifyNoThrottling[WriteShareGroupStateResponse](requestChannelRequest)
    assertEquals(1, response.data.results.size)
    val topicResult = response.data.results.get(0)
    assertEquals(mysteryTopicId, topicResult.topicId)
    assertEquals(1, topicResult.partitions.size)
    assertEquals(Errors.UNKNOWN_TOPIC_ID.code(), topicResult.partitions.get(0).errorCode(),
      "unresolved UUID must NOT be persisted — fail-closed prevents backing-UUID corruption of __share_group_state")
    verify(shareCoordinator, never()).writeState(any[RequestContext], any[WriteShareGroupStateRequestData])
  }

  @Test
  def testWriteShareGroupStateSuccess(): Unit = {
    val topicId = Uuid.randomUuid();
    val writeRequestData = new WriteShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(List(
        new WriteShareGroupStateRequestData.WriteStateData()
          .setTopicId(topicId)
          .setPartitions(List(
            new WriteShareGroupStateRequestData.PartitionData()
              .setPartition(1)
              .setLeaderEpoch(1)
              .setStateEpoch(2)
              .setStartOffset(10)
              .setStateBatches(List(
                new WriteShareGroupStateRequestData.StateBatch()
                  .setFirstOffset(11)
                  .setLastOffset(15)
                  .setDeliveryCount(1)
                  .setDeliveryState(0)
              ).asJava)
          ).asJava)
      ).asJava)

    val writeStateResultData: util.List[WriteShareGroupStateResponseData.WriteStateResult] = List(
      new WriteShareGroupStateResponseData.WriteStateResult()
        .setTopicId(topicId)
        .setPartitions(List(
          new WriteShareGroupStateResponseData.PartitionResult()
            .setPartition(1)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(null)
        ).asJava)
    ).asJava

    val config = Map(
      ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true",
    )

    val response = getWriteShareGroupResponse(
      writeRequestData,
      config ++ ShareCoordinatorTestConfig.testConfigMap().asScala,
      verifyNoErr = true,
      null,
      writeStateResultData
    )

    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
  }

  @Test
  def testWriteShareGroupStateAuthorizationFailed(): Unit = {
    val topicId = Uuid.randomUuid();
    val writeRequestData = new WriteShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(List(
        new WriteShareGroupStateRequestData.WriteStateData()
          .setTopicId(topicId)
          .setPartitions(List(
            new WriteShareGroupStateRequestData.PartitionData()
              .setPartition(1)
              .setLeaderEpoch(1)
              .setStateEpoch(2)
              .setStartOffset(10)
              .setStateBatches(List(
                new WriteShareGroupStateRequestData.StateBatch()
                  .setFirstOffset(11)
                  .setLastOffset(15)
                  .setDeliveryCount(1)
                  .setDeliveryState(0)
              ).asJava)
          ).asJava)
      ).asJava)

    val writeStateResultData: util.List[WriteShareGroupStateResponseData.WriteStateResult] = List(
      new WriteShareGroupStateResponseData.WriteStateResult()
        .setTopicId(topicId)
        .setPartitions(List(
          new WriteShareGroupStateResponseData.PartitionResult()
            .setPartition(1)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(null)
        ).asJava)
    ).asJava

    val config = Map(
      ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true",
    )

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava, Seq(AuthorizationResult.ALLOWED).asJava)

    val response = getWriteShareGroupResponse(
      writeRequestData,
      config ++ ShareCoordinatorTestConfig.testConfigMap().asScala,
      verifyNoErr = false,
      authorizer,
      writeStateResultData
    )

    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
    response.data.results.forEach(writeResult => {
      assertEquals(1, writeResult.partitions.size)
      assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code(), writeResult.partitions.get(0).errorCode())
    })
  }

  @Test
  def testWriteShareGroupStateRejectsBackingTopic(): Unit = {
    // r19 ADV-A BLOCKER #144 — symmetric with the read path: a backing-topic UUID slipping into
    // WriteShareGroupState would scribble share-group state under (groupId, backingTopicId,
    // partition), corrupting acquisition tracking for every logical tenant on that backing.
    val backingTopic = "backing-topic"
    val backingTopicId = Uuid.randomUuid()
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(backingTopic, numPartitions = 4, topicId = backingTopicId)
    when(concentrationKernel.isBackingTopic(backingTopic)).thenReturn(true)

    val writeRequestData = new WriteShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(List(
        new WriteShareGroupStateRequestData.WriteStateData()
          .setTopicId(backingTopicId)
          .setPartitions(List(
            new WriteShareGroupStateRequestData.PartitionData()
              .setPartition(1)
              .setLeaderEpoch(1)
              .setStateEpoch(2)
              .setStartOffset(10)
              .setStateBatches(List(
                new WriteShareGroupStateRequestData.StateBatch()
                  .setFirstOffset(11)
                  .setLastOffset(15)
                  .setDeliveryCount(1)
                  .setDeliveryState(0)
              ).asJava)
          ).asJava)
      ).asJava)

    val requestChannelRequest = buildRequest(new WriteShareGroupStateRequest.Builder(writeRequestData, true).build())
    kafkaApis = createKafkaApis(
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true") ++
        ShareCoordinatorTestConfig.testConfigMap().asScala
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching())

    val response = verifyNoThrottling[WriteShareGroupStateResponse](requestChannelRequest)
    assertEquals(1, response.data.results.size)
    val topicResult = response.data.results.get(0)
    assertEquals(backingTopicId, topicResult.topicId)
    assertEquals(1, topicResult.partitions.size)
    assertEquals(Errors.INVALID_REQUEST.code(), topicResult.partitions.get(0).errorCode())
    // The share coordinator must never see this backing-topic key — even one corrupt
    // write would persist cross-tenant state in __share_group_state.
    verify(shareCoordinator, never()).writeState(any[RequestContext], any[WriteShareGroupStateRequestData])
  }

  def getShareGroupDescribeResponse(groupIds: util.List[String], configOverrides: Map[String, String] = Map.empty,
                                    verifyNoErr: Boolean = true, authorizer: Authorizer = null,
                                    describedGroups: util.List[ShareGroupDescribeResponseData.DescribedGroup]): ShareGroupDescribeResponse = {
    val shareGroupDescribeRequestData = new ShareGroupDescribeRequestData()
    shareGroupDescribeRequestData.groupIds.addAll(groupIds)
    val requestChannelRequest = buildRequest(new ShareGroupDescribeRequest.Builder(shareGroupDescribeRequestData, true).build())

    val future = new CompletableFuture[util.List[ShareGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.shareGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis(
      overrideProperties = configOverrides,
      authorizer = Option(authorizer),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    future.complete(describedGroups)

    val response = verifyNoThrottling[ShareGroupDescribeResponse](requestChannelRequest)
    if (verifyNoErr) {
      val expectedShareGroupDescribeResponseData = new ShareGroupDescribeResponseData()
        .setGroups(describedGroups)
      assertEquals(expectedShareGroupDescribeResponseData, response.data)
    }
    response
  }

  def getReadShareGroupResponse(requestData: ReadShareGroupStateRequestData, configOverrides: Map[String, String] = Map.empty,
                                verifyNoErr: Boolean = true, authorizer: Authorizer = null,
                                readStateResult: util.List[ReadShareGroupStateResponseData.ReadStateResult]): ReadShareGroupStateResponse = {
    val requestChannelRequest = buildRequest(new ReadShareGroupStateRequest.Builder(requestData, true).build())

    val future = new CompletableFuture[ReadShareGroupStateResponseData]()
    when(shareCoordinator.readState(
      any[RequestContext],
      any[ReadShareGroupStateRequestData]
    )).thenReturn(future)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    // r21 BLOCKER #171 — the share-state guard now fails-closed on UUIDs that don't resolve
    // through metadataCache. The helper resets the cache, so we re-register each request's
    // topic-id under a synthetic non-backing name to keep success-path tests exercisable. The
    // fail-closed coverage is in testRead/WriteShareGroupStateFailsClosedOnUnresolvedTopicId,
    // which construct kafkaApis without going through this helper.
    requestData.topics.asScala.foreach { td =>
      addTopicToMetadataCache(s"share-state-helper-${td.topicId}", numPartitions = 4, topicId = td.topicId)
    }
    kafkaApis = createKafkaApis(
      overrideProperties = configOverrides,
      authorizer = Option(authorizer),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching())

    future.complete(new ReadShareGroupStateResponseData()
      .setResults(readStateResult))

    val response = verifyNoThrottling[ReadShareGroupStateResponse](requestChannelRequest)
    if (verifyNoErr) {
      val expectedReadShareGroupStateResponseData = new ReadShareGroupStateResponseData()
        .setResults(readStateResult)
      assertEquals(expectedReadShareGroupStateResponseData, response.data)
    }
    response
  }

  def getWriteShareGroupResponse(requestData: WriteShareGroupStateRequestData, configOverrides: Map[String, String] = Map.empty,
                                verifyNoErr: Boolean = true, authorizer: Authorizer = null,
                                 writeStateResult: util.List[WriteShareGroupStateResponseData.WriteStateResult]): WriteShareGroupStateResponse = {
    val requestChannelRequest = buildRequest(new WriteShareGroupStateRequest.Builder(requestData, true).build())

    val future = new CompletableFuture[WriteShareGroupStateResponseData]()
    when(shareCoordinator.writeState(
      any[RequestContext],
      any[WriteShareGroupStateRequestData]
    )).thenReturn(future)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    // r21 BLOCKER #171 — see getReadShareGroupResponse for the same rationale: helper resets
    // the cache, so re-register each request's topic-id so the success-path tests pass through
    // the fail-closed unresolved-UUID guard installed in partitionShareStateTopicsForBackingTopic.
    requestData.topics.asScala.foreach { td =>
      addTopicToMetadataCache(s"share-state-helper-${td.topicId}", numPartitions = 4, topicId = td.topicId)
    }
    kafkaApis = createKafkaApis(
      overrideProperties = configOverrides,
      authorizer = Option(authorizer),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching())

    future.complete(new WriteShareGroupStateResponseData()
      .setResults(writeStateResult))

    val response = verifyNoThrottling[WriteShareGroupStateResponse](requestChannelRequest)
    if (verifyNoErr) {
      val expectedWriteShareGroupStateResponseData = new WriteShareGroupStateResponseData()
        .setResults(writeStateResult)
      assertEquals(expectedWriteShareGroupStateResponseData, response.data)
    }
    response
  }
}
