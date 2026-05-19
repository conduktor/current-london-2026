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
import kafka.log.UnifiedLog
import kafka.network.RequestChannel
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.metadata.{ConfigRepository, KRaftMetadataCache, MockConfigRepository}
import kafka.server.share.SharePartitionManager
import kafka.utils.{CoreUtils, Log4jController, Logging, TestUtils}
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.common._
import org.apache.kafka.common.acl.{AccessControlEntry, AccessControlEntryFilter, AclBinding, AclBindingFilter, AclOperation, AclPermissionType}
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
import org.apache.kafka.common.message.DeleteRecordsRequestData.{DeleteRecordsPartition => DRPartition, DeleteRecordsTopic => DRTopic}
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.{AlterConfigsResource => IAlterConfigsResource, AlterConfigsResourceCollection => IAlterConfigsResourceCollection, AlterableConfig => IAlterableConfig, AlterableConfigCollection => IAlterableConfigCollection}
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.{AlterConfigsResourceResponse => IAlterConfigsResourceResponse}
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.message.ListClientMetricsResourcesResponseData.ClientMetricsResource
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.message.ListOffsetsResponseData.{ListOffsetsPartitionResponse, ListOffsetsTopicResponse}
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic
import org.apache.kafka.common.message.OffsetDeleteRequestData.{OffsetDeleteRequestPartition, OffsetDeleteRequestTopic, OffsetDeleteRequestTopicCollection}
import org.apache.kafka.common.message.OffsetDeleteResponseData.{OffsetDeleteResponsePartition, OffsetDeleteResponsePartitionCollection, OffsetDeleteResponseTopic, OffsetDeleteResponseTopicCollection}
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
import org.apache.kafka.common.resource.{PatternType, Resource, ResourcePattern, ResourcePatternFilter, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, KafkaPrincipalSerde, SecurityProtocol}
import org.apache.kafka.common.security.token.delegation.{DelegationToken, TokenInformation}
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
import org.apache.kafka.server.tenant.TenantConfig
import org.apache.kafka.server.util.{FutureUtils, MockTime}
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
    featureVersions: Seq[FeatureVersion] = Seq.empty,
    tenantConfig: TenantConfig = TenantConfig.empty(),
    tokenManager: DelegationTokenManager = null
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
      tokenManager = tokenManager,
      apiVersionManager = apiVersionManager,
      clientMetricsManager = clientMetricsManager,
      tenantConfig = tenantConfig)
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
  def testElectLeadersSweepForwarded(): Unit = {
    // ElectLeaders with null topicPartitions is the cluster-wide "elect for all
    // eligible" sweep. The outside-in pollution guard does NOT block sweeps
    // (the threat is explicit naming of a reserved namespace, not the inherent
    // reality that cluster admins can affect everything). Verify that the
    // request is still forwarded to the controller.
    val requestBuilder = new ElectLeadersRequest.Builder(ElectionType.PREFERRED, null, 30000)
    val header = new RequestHeader(ApiKeys.ELECT_LEADERS, ApiKeys.ELECT_LEADERS.latestVersion, clientId, 0)
    val request = buildRequest(requestBuilder.build(header.apiVersion), requestHeader = Option(header))
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis()
    kafkaApis.handleElectLeadersRequest(request)
    verify(forwardingManager, times(1)).forwardRequest(
      any[RequestChannel.Request](),
      any[AbstractRequest](),
      any[Option[AbstractResponse] => Unit]())
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
  def testWriteTxnMarkersClusterWideListenerRefusesReservedNamespacePartitions(): Unit = {
    // #152 — WriteTxnMarkers is a coordinator→leader RPC that bypasses every
    // request-side rewrite: the caller chooses the producerId, the
    // coordinatorEpoch, AND the target TopicPartition list. The handler then
    // calls `replicaManager.appendRecords(internalTopicsAllowed=true)` with
    // those partitions as-is. Without the per-partition reserved-namespace
    // refusal, a cluster admin holding ALTER:CLUSTER or CLUSTER_ACTION on the
    // regular client listener could plant ABORT markers in `acme.orders-0`
    // and erase tenant-committed records (read-committed consumers skip
    // aborted records), or plant COMMIT markers to expose pending state.
    //
    // The fix refuses tenant-namespace partitions per-element with
    // TOPIC_AUTHORIZATION_FAILED on every non-inter-broker listener; this
    // test pins that behaviour and asserts the good partition still proceeds
    // (the bad one must NOT short-circuit the rest of the batch).
    val tenantPartition = new TopicPartition("acme.orders", 0)
    val normalPartition = new TopicPartition("t", 0)
    val (_, request) = createWriteTxnMarkersRequest(asList(tenantPartition, normalPartition))
    val expectedErrors = Map(
      tenantPartition -> Errors.TOPIC_AUTHORIZATION_FAILED,
      normalPartition -> Errors.NONE).asJava

    val capturedResponse: ArgumentCaptor[WriteTxnMarkersResponse] =
      ArgumentCaptor.forClass(classOf[WriteTxnMarkersResponse])
    val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])
    val entriesCaptor: ArgumentCaptor[Map[TopicPartition, MemoryRecords]] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, MemoryRecords]])

    // The tenant partition must NEVER reach onlinePartition; only the normal
    // one does. If the namespace guard accidentally swallowed both, the
    // verify(onlinePartition).times(1) below would catch the regression.
    when(replicaManager.onlinePartition(normalPartition))
      .thenReturn(Some(mock(classOf[Partition])))

    val requestLocal = RequestLocal.withThreadConfinedCaching
    when(replicaManager.appendRecords(anyLong,
      anyShort,
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      entriesCaptor.capture(),
      responseCallback.capture(),
      any(),
      any(),
      ArgumentMatchers.eq(requestLocal),
      any(),
      any()
    )).thenAnswer(_ => responseCallback.getValue.apply(Map(normalPartition -> new PartitionResponse(Errors.NONE))))

    // Tenant must be bound for the structural namespace check to fire
    // (`isReservedTenantNamespace` short-circuits to false when
    // `tenantConfig.allTenants.isEmpty`). Default test listener is PLAINTEXT
    // == config.interBrokerListenerName, so we route the request through
    // EXTERNAL_SASL to model "cluster admin on a non-inter-broker listener".
    val attackerListener = new ListenerName("EXTERNAL_SASL")
    val attackerRequest = buildRequest(
      request.body[WriteTxnMarkersRequest],
      listenerName = attackerListener,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "cluster-admin"))

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleWriteTxnMarkersRequest(attackerRequest, requestLocal)

    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(attackerRequest),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )
    val markersResponse = capturedResponse.getValue
    assertEquals(expectedErrors, markersResponse.errorsByProducerId.get(1L))

    // Defence-in-depth: `appendRecords` must be invoked, and its entries map
    // must contain ONLY the normal partition. If the tenant partition leaked
    // through, this would catch it.
    val capturedEntries = entriesCaptor.getValue
    assertEquals(Set(normalPartition), capturedEntries.keySet,
      s"appendRecords must not see the tenant-namespace partition; saw ${capturedEntries.keySet}")

    // And: onlinePartition must never be called for the tenant partition.
    verify(replicaManager, never()).onlinePartition(tenantPartition)
  }

  @Test
  def testWriteTxnMarkersInterBrokerListenerProceedsOnTenantPartitions(): Unit = {
    // #152 complement — the legitimate transaction coordinator path. The
    // coordinator issues WriteTxnMarkers over `config.interBrokerListenerName`
    // (PLAINTEXT in the test broker) to materialise commit/abort markers in
    // the tenant's own partitions on behalf of the tenant's EndTxn. That
    // path MUST be preserved or transactional writes by tenants would never
    // complete. Same request shape as the attacker test; only the listener
    // differs.
    val tenantPartition = new TopicPartition("acme.orders", 0)
    val (_, request) = createWriteTxnMarkersRequest(asList(tenantPartition))
    val expectedErrors = Map(tenantPartition -> Errors.NONE).asJava

    val capturedResponse: ArgumentCaptor[WriteTxnMarkersResponse] =
      ArgumentCaptor.forClass(classOf[WriteTxnMarkersResponse])
    val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

    when(replicaManager.onlinePartition(tenantPartition))
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
    )).thenAnswer(_ => responseCallback.getValue.apply(Map(tenantPartition -> new PartitionResponse(Errors.NONE))))

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleWriteTxnMarkersRequest(request, requestLocal)

    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )
    val markersResponse = capturedResponse.getValue
    assertEquals(expectedErrors, markersResponse.errorsByProducerId.get(1L))
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
      any[util.Map[Uuid, String]],
      any[Option[String]])).thenReturn(fetchContext)

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
      ArgumentMatchers.eq[Short](ApiKeys.FETCH.latestVersion),
      ArgumentMatchers.eq(fetchMetadata),
      ArgumentMatchers.eq(replicaId >= 0),
      ArgumentMatchers.eq(Collections.singletonMap(foo, new FetchRequest.PartitionData(foo.topicId, 0, 0, 1000, Optional.empty()))),
      ArgumentMatchers.eq(Collections.emptyList[TopicIdPartition]),
      ArgumentMatchers.eq(metadataCache.topicIdsToNames()),
      any[Option[String]]
    )).thenReturn(fetchContext)

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
      any[util.Map[Uuid, String]],
      any[Option[String]])).thenReturn(fetchContext)

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
      any[util.Map[Uuid, String]],
      any[Option[String]])).thenReturn(fetchContext)

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
                           requestMetrics: RequestChannelMetrics = requestChannelMetrics,
                           principal: KafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice")): RequestChannel.Request = {
    val buffer = request.serializeWithHeader(
      requestHeader.getOrElse(new RequestHeader(request.apiKey, request.version, clientId, 0)))

    // read the header from the buffer first so that the body can be read next from the Request constructor
    val header = RequestHeader.parse(buffer)
    // DelegationTokens require the context authenticated to be non SecurityProtocol.PLAINTEXT
    // and have a non KafkaPrincipal.ANONYMOUS principal. This test is done before the check
    // for forwarding because after forwarding the context will have a different context.
    // We validate the context authenticated failure case in other integration tests.
    val context = new RequestContext(header, "1", InetAddress.getLocalHost, Optional.empty(),
      principal, listenerName, SecurityProtocol.SSL,
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
  def testAlterReplicaLogDirsClusterWideListenerRefusesTenantPartition(): Unit = {
    // A cluster-wide ALTER caller naming `acme.orders-0` would otherwise force
    // tenant log dirs onto a different (slow / pending-removal) disk without
    // any tenant signal in the audit log. The broker must partition the input
    // into eligible vs reserved-tenant before calling replicaManager — only
    // the eligible map reaches ReplicaManager; the reserved-tenant entries
    // surface as per-partition INVALID_TOPIC_EXCEPTION in the response.
    val data = new AlterReplicaLogDirsRequestData()
    val dir = new AlterReplicaLogDirsRequestData.AlterReplicaLogDir().setPath("/foo")
    dir.topics().add(new AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopic()
      .setName("acme.orders").setPartitions(asList(0)))
    dir.topics().add(new AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopic()
      .setName("plain").setPartitions(asList(0)))
    data.dirs().add(dir)
    val req = new AlterReplicaLogDirsRequest.Builder(data).build()
    val request = buildRequest(req)

    reset(replicaManager, clientRequestQuotaManager, requestChannel)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    val plainTp = new TopicPartition("plain", 0)
    when(replicaManager.alterReplicaLogDirs(ArgumentMatchers.eq(Map(plainTp -> "/foo"))))
      .thenReturn(Map(plainTp -> Errors.NONE))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAlterReplicaLogDirsRequest(request)

    val response = verifyNoThrottling[AlterReplicaLogDirsResponse](request)
    val byTopic = response.data.results.asScala.map(r => r.topicName -> r).toMap
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code,
      byTopic("acme.orders").partitions.asScala.head.errorCode,
      "tenant-prefixed partition must be refused on cluster-wide listener")
    assertEquals(Errors.NONE.code,
      byTopic("plain").partitions.asScala.head.errorCode,
      "non-polluting partition must still reach ReplicaManager")
    // ReplicaManager must NOT have seen the polluting partition.
    verify(replicaManager).alterReplicaLogDirs(ArgumentMatchers.eq(Map(plainTp -> "/foo")))
  }

  @Test
  def testAlterReplicaLogDirsClusterWideListenerAllPollutingShortCircuits(): Unit = {
    // When every requested partition lives in a tenant namespace, the eligible
    // map is empty — ReplicaManager.alterReplicaLogDirs must NOT be called at
    // all (asking it to act on an empty map is wasteful and noisy). The caller
    // still receives one INVALID_TOPIC_EXCEPTION per requested partition.
    val data = new AlterReplicaLogDirsRequestData()
    val dir = new AlterReplicaLogDirsRequestData.AlterReplicaLogDir().setPath("/foo")
    dir.topics().add(new AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopic()
      .setName("acme.orders").setPartitions(asList(0, 1)))
    data.dirs().add(dir)
    val req = new AlterReplicaLogDirsRequest.Builder(data).build()
    val request = buildRequest(req)

    reset(replicaManager, clientRequestQuotaManager, requestChannel)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAlterReplicaLogDirsRequest(request)

    val response = verifyNoThrottling[AlterReplicaLogDirsResponse](request)
    val parts = response.data.results.asScala.head.partitions.asScala
      .map(p => p.partitionIndex.toInt -> p.errorCode).toMap
    assertEquals(Set(Errors.INVALID_TOPIC_EXCEPTION.code), parts.values.toSet)
    assertEquals(Set(0, 1), parts.keySet)
    verify(replicaManager, never()).alterReplicaLogDirs(any[Map[TopicPartition, String]]())
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
  def testReadShareGroupStateRefusesTenantPrefixedGroupIdFromClusterWideCaller(): Unit = {
    // #148 regression. A non-tenant principal holding CLUSTER_ACTION on the
    // share-coordinator listener (a service account, a super-user, a sidecar)
    // would otherwise be able to address `__tenant_<id>.<group>` directly and
    // read the tenant's share-partition state — bypassing every tenant-facing
    // rewrite that hides the physical key from the wire-facing surface. The
    // scrub at handleReadShareGroupStateRequest refuses the request structurally
    // (no allTenants lookup, no principal-tenant parsing) so the guarantee
    // holds even on a controller that has never seen the binding.
    val topicId = Uuid.randomUuid()
    val readRequestData = new ReadShareGroupStateRequestData()
      .setGroupId("__tenant_acme.G")
      .setTopics(List(
        new ReadShareGroupStateRequestData.ReadStateData()
          .setTopicId(topicId)
          .setPartitions(List(
            new ReadShareGroupStateRequestData.PartitionData()
              .setPartition(0)
              .setLeaderEpoch(1)
          ).asJava)
      ).asJava)

    val requestChannelRequest = buildRequest(
      new ReadShareGroupStateRequest.Builder(readRequestData, true).build())

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true")
        ++ ShareCoordinatorTestConfig.testConfigMap().asScala
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching())

    val response = verifyNoThrottling[ReadShareGroupStateResponse](requestChannelRequest)
    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
    response.data.results.forEach { result =>
      assertEquals(topicId, result.topicId)
      assertEquals(1, result.partitions.size)
      assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, result.partitions.get(0).errorCode,
        "Cluster-wide caller addressing `__tenant_*` groupId must be refused with GROUP_AUTHORIZATION_FAILED")
    }
    verify(shareCoordinator, never()).readState(any[RequestContext], any[ReadShareGroupStateRequestData])
  }

  @Test
  def testWriteShareGroupStateRefusesTenantPrefixedGroupIdFromClusterWideCaller(): Unit = {
    // #148 regression — write side. Letting a cluster-wide caller WRITE to
    // `__tenant_<id>.<group>` is strictly worse than the read case: it plants
    // share-partition state into the tenant's namespace BEFORE the tenant's
    // first heartbeat — corrupt offsets, replay state batches, delivery counts
    // chosen by an outsider. The structural scrub closes that vector by
    // refusing the request before the share coordinator's writeState is
    // invoked, so no record is ever produced into the share-state topic under
    // a tenant key.
    val topicId = Uuid.randomUuid()
    val writeRequestData = new WriteShareGroupStateRequestData()
      .setGroupId("__tenant_acme.G")
      .setTopics(List(
        new WriteShareGroupStateRequestData.WriteStateData()
          .setTopicId(topicId)
          .setPartitions(List(
            new WriteShareGroupStateRequestData.PartitionData()
              .setPartition(0)
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

    val requestChannelRequest = buildRequest(
      new WriteShareGroupStateRequest.Builder(writeRequestData, true).build())

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true")
        ++ ShareCoordinatorTestConfig.testConfigMap().asScala
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching())

    val response = verifyNoThrottling[WriteShareGroupStateResponse](requestChannelRequest)
    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
    response.data.results.forEach { result =>
      assertEquals(topicId, result.topicId)
      assertEquals(1, result.partitions.size)
      assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, result.partitions.get(0).errorCode,
        "Cluster-wide caller WRITING to `__tenant_*` groupId must be refused before writeState is invoked")
    }
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

  // ---------------------------------------------------------------------------
  // Multi-tenancy — Metadata / ListTopics
  //
  // These tests pin down the contract described in PROMPT.md for the Metadata
  // path: a tenant client sees a pristine cluster scoped to its own namespace;
  // physical names never leak out (including in errors); a privileged caller on
  // a tenant-bound listener without a tenant principal is refused rather than
  // silently rewritten into the tenant's namespace.
  // ---------------------------------------------------------------------------

  private val TENANT_LISTENER = new ListenerName("TENANT_ACME")

  private def tenantConfigBinding(tenantId: String, listener: ListenerName): TenantConfig = {
    val props = new util.HashMap[String, Object]()
    props.put(s"listener.name.${listener.value.toLowerCase}.tenant.id", tenantId)
    TenantConfig.from(props)
  }

  private def tenantPrincipal(tenantId: String, user: String): KafkaPrincipal =
    new KafkaPrincipal(KafkaPrincipal.USER_TYPE, s"__tenant_$tenantId.$user")

  @Test
  def testMetadataTenantRequestRewritesLogicalNameToPhysicalForCacheLookup(): Unit = {
    // Tenant requests "orders"; broker must find the topic stored as "acme.orders".
    addTopicToMetadataCache("acme.orders", numPartitions = 1)

    val metadataRequest = new MetadataRequest.Builder(List("orders").asJava, false).build()
    val request = buildRequest(
      metadataRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    addTopicToMetadataCache("acme.orders", numPartitions = 1)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleTopicMetadataRequest(request)

    val response = verifyNoThrottling[MetadataResponse](request)
    val topics = response.topicMetadata().asScala.map(_.topic).toSet
    assertEquals(Set("orders"), topics, "client must see the logical name, not the physical one")
    assertTrue(response.errors.asScala.values.forall(_ == Errors.NONE),
      s"unexpected errors in response: ${response.errors}")
  }

  @Test
  def testMetadataTenantResponseStripsPhysicalPrefixFromErrorResponses(): Unit = {
    // Topic does not exist; broker returns UNKNOWN_TOPIC_OR_PARTITION. The
    // error response must reference "orders", not "acme.orders" — otherwise
    // the physical prefix leaks via the error path.
    val metadataRequest = new MetadataRequest.Builder(List("orders").asJava, false).build()
    val request = buildRequest(
      metadataRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleTopicMetadataRequest(request)

    val response = verifyNoThrottling[MetadataResponse](request)
    val errored = response.topicMetadata().asScala.toSeq
    assertEquals(1, errored.size)
    assertEquals("orders", errored.head.topic, "error response must carry the logical name")
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, errored.head.error)
  }

  @Test
  def testMetadataTenantIsAllTopicsScopesToTenantNamespace(): Unit = {
    // Tenant lists all topics; broker must hide other tenants' physical topics
    // and strip the prefix from its own. Internal topics (consumer offsets) are
    // not tenant-scoped and pass through unchanged.
    val metadataRequest = MetadataRequest.Builder.allTopics().build()
    val request = buildRequest(
      metadataRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    addTopicToMetadataCache("acme.orders", numPartitions = 1)
    addTopicToMetadataCache("acme.payments", numPartitions = 1)
    addTopicToMetadataCache("beta.orders", numPartitions = 1)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleTopicMetadataRequest(request)

    val response = verifyNoThrottling[MetadataResponse](request)
    val visible = response.topicMetadata().asScala.map(_.topic).toSet
    assertEquals(Set("orders", "payments"), visible,
      "tenant must see only its own topics with the prefix stripped")
  }

  @Test
  def testMetadataPrivilegedCallerOnTenantBoundListenerIsRejected(): Unit = {
    // Super-user without a `__tenant_` prefix hitting a tenant-bound listener:
    // the broker MUST NOT silently rewrite "orders" into "acme.orders" — that
    // would let the privileged caller pollute the tenant namespace.
    addTopicToMetadataCache("acme.orders", numPartitions = 1)

    val metadataRequest = new MetadataRequest.Builder(List("orders").asJava, false).build()
    val request = buildRequest(
      metadataRequest,
      listenerName = TENANT_LISTENER,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice")) // no tenant prefix

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    addTopicToMetadataCache("acme.orders", numPartitions = 1)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleTopicMetadataRequest(request)

    val response = verifyNoThrottling[MetadataResponse](request)
    val errored = response.topicMetadata().asScala.toSeq
    assertEquals(1, errored.size)
    assertEquals("orders", errored.head.topic,
      "rejection error must reference the logical name the caller used")
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, errored.head.error,
      "privileged caller on tenant listener without tenant prefix must be refused")
  }

  @Test
  def testMetadataNonTenantRequestUnchangedWhenNoBinding(): Unit = {
    // Existing single-tenant behaviour is unchanged when no tenant binding
    // exists for the listener and the principal carries no `__tenant_` prefix.
    addTopicToMetadataCache("plain-topic", numPartitions = 1)

    val metadataRequest = new MetadataRequest.Builder(List("plain-topic").asJava, false).build()
    val request = buildRequest(metadataRequest)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    addTopicToMetadataCache("plain-topic", numPartitions = 1)
    kafkaApis = createKafkaApis()
    kafkaApis.handleTopicMetadataRequest(request)

    val response = verifyNoThrottling[MetadataResponse](request)
    val topics = response.topicMetadata().asScala.map(_.topic).toSet
    assertEquals(Set("plain-topic"), topics)
  }

  @Test
  def testMetadataByIdTenantForeignAndUnknownIdsShareTheSameShape(): Unit = {
    // Metadata-by-id with two ids: one is unknown to the broker; the other
    // resolves to a topic owned by another tenant. If foreign ids were dropped
    // (the old code path) and unknown ids surfaced as UNKNOWN_TOPIC_ID, the
    // shape of the response would be a probe oracle for foreign-topic
    // existence. Both must surface as UNKNOWN_TOPIC_ID with null name and the
    // queried id echoed back.
    val foreignId = Uuid.randomUuid()
    val unknownId = Uuid.randomUuid()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    addTopicToMetadataCache("beta.orders", numPartitions = 1, topicId = foreignId)

    val metadataRequest = new MetadataRequest.Builder(
      new MetadataRequestData()
        .setTopics(util.Arrays.asList(
          new MetadataRequestData.MetadataRequestTopic().setTopicId(foreignId),
          new MetadataRequestData.MetadataRequestTopic().setTopicId(unknownId)))
        .setAllowAutoTopicCreation(false))
      .build(ApiKeys.METADATA.latestVersion)
    val request = buildRequest(
      metadataRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleTopicMetadataRequest(request)

    val response = verifyNoThrottling[MetadataResponse](request)
    val resultsById = response.topicMetadata().asScala.map(t => t.topicId -> t).toMap
    assertEquals(Set(foreignId, unknownId), resultsById.keySet,
      "both ids must appear in the response so foreign and unknown are indistinguishable")
    Set(foreignId, unknownId).foreach { id =>
      val entry = resultsById(id)
      assertEquals(Errors.UNKNOWN_TOPIC_ID, entry.error,
        s"id $id must surface as UNKNOWN_TOPIC_ID regardless of whether the broker knows it")
      assertNull(entry.topic, s"id $id must not carry any topic name in the response")
    }
  }

  // ---------------------------------------------------------------------------
  // Produce — multi-tenancy
  //
  // The producer client sees only the logical topic name. The broker authorizes,
  // looks up metadata, and appends against the physical (prefixed) name. Errors
  // come back keyed on the logical name. A privileged caller without a tenant
  // prefix on a tenant-bound listener is refused outright; we never silently
  // rewrite its writes into the tenant namespace.
  // ---------------------------------------------------------------------------

  private def buildSingleTopicProduceRequest(topic: String, partition: Int = 0, acks: Short = 1): ProduceRequest = {
    ProduceRequest.builder(new ProduceRequestData()
      .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
        Collections.singletonList(new ProduceRequestData.TopicProduceData()
          .setName(topic).setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(partition)
              .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("test".getBytes))))))
          .iterator))
      .setAcks(acks)
      .setTimeoutMs(5000))
      .build(ApiKeys.PRODUCE.latestVersion)
  }

  @Test
  def testProduceTenantRequestRewritesLogicalNameToPhysicalForReplicaManager(): Unit = {
    // The producer sends "orders"; replicaManager.handleProduceAppend must be
    // invoked with the physical TopicPartition "acme.orders-0", and the
    // response visible to the client must carry the logical name.
    val physicalTopic = "acme.orders"
    addTopicToMetadataCache(physicalTopic, numPartitions = 1)

    val produceRequest = buildSingleTopicProduceRequest("orders")
    val request = buildRequest(
      produceRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])
    val entriesPerPartition: ArgumentCaptor[Map[TopicPartition, MemoryRecords]] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, MemoryRecords]])

    when(replicaManager.handleProduceAppend(
      anyLong, anyShort, ArgumentMatchers.eq(false), any(),
      entriesPerPartition.capture(),
      responseCallback.capture(),
      any(), any(), any(), any())
    ).thenAnswer(_ => responseCallback.getValue.apply(
      Map(new TopicPartition(physicalTopic, 0) -> new PartitionResponse(Errors.NONE))))

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    // replicaManager saw the physical name
    val captured = entriesPerPartition.getValue
    assertEquals(Set(new TopicPartition(physicalTopic, 0)), captured.keySet,
      "replicaManager must receive the physical TopicPartition")

    // client sees the logical name in the response
    val response = verifyNoThrottling[ProduceResponse](request)
    assertEquals(1, response.data.responses.size)
    val topicProduceResponse = response.data.responses.asScala.head
    assertEquals("orders", topicProduceResponse.name,
      "client must see the logical topic name in the produce response")
    val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
    assertEquals(Errors.NONE, Errors.forCode(partitionProduceResponse.errorCode))
  }

  @Test
  def testProduceTenantResponseStripsPhysicalPrefixFromErrorResponses(): Unit = {
    // Tenant produces to an unknown topic. The error must mention "orders",
    // not "acme.orders" — otherwise the physical prefix leaks via the error
    // path. The topic is not in metadataCache so KafkaApis short-circuits
    // with UNKNOWN_TOPIC_OR_PARTITION before reaching replicaManager.
    val produceRequest = buildSingleTopicProduceRequest("orders")
    val request = buildRequest(
      produceRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[ProduceResponse](request)
    assertEquals(1, response.data.responses.size)
    val topicProduceResponse = response.data.responses.asScala.head
    assertEquals("orders", topicProduceResponse.name,
      "error response must carry the logical name")
    val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION,
      Errors.forCode(partitionProduceResponse.errorCode))
  }

  @Test
  def testProduceTenantScrubsPhysicalPrefixFromReplicaManagerErrorMessage(): Unit = {
    // replicaManager / log validation can embed the physical topic name in
    // PartitionResponse.errorMessage (e.g. record validators quoting the
    // offending topic). The OUT rewrite must scrub that string in addition to
    // rewriting the TopicPartition key, otherwise the physical prefix leaks
    // to the tenant client through the error-message side channel.
    val physicalTopic = "acme.orders"
    addTopicToMetadataCache(physicalTopic, numPartitions = 1)

    val produceRequest = buildSingleTopicProduceRequest("orders")
    val request = buildRequest(
      produceRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

    val leakedMessage = s"Invalid record for topic '$physicalTopic'"
    when(replicaManager.handleProduceAppend(
      anyLong, anyShort, ArgumentMatchers.eq(false), any(),
      any(), responseCallback.capture(),
      any(), any(), any(), any())
    ).thenAnswer(_ => responseCallback.getValue.apply(
      Map(new TopicPartition(physicalTopic, 0) ->
        new PartitionResponse(Errors.INVALID_RECORD, leakedMessage))))

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[ProduceResponse](request)
    val topicProduceResponse = response.data.responses.asScala.head
    assertEquals("orders", topicProduceResponse.name,
      "topic name in response must be logical")
    val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
    assertEquals(Errors.INVALID_RECORD,
      Errors.forCode(partitionProduceResponse.errorCode))
    val msg = partitionProduceResponse.errorMessage
    assertNotNull(msg, "errorMessage must be propagated to the client")
    assertFalse(msg.contains("acme.orders"),
      s"physical prefix must be scrubbed from errorMessage but found in: $msg")
    assertTrue(msg.contains("orders"),
      s"logical topic name must remain in errorMessage: $msg")
  }

  @Test
  def testProduceTenantScrubsPrincipalPrefixFromReplicaManagerErrorMessage(): Unit = {
    // Coordinator paths (txn coordinator fencer, group coordinator) embed the
    // PRINCIPAL-prefix form `__tenant_<id>.<name>` in error messages they
    // bubble up to replicaManager.handleProduceAppend (e.g. "Transactional id
    // '__tenant_acme.my-txn' is fenced"). scrubMessage must strip this form in
    // addition to the topic-prefix form, otherwise the tenant learns the
    // internal principal-id naming convention and can confirm that a sibling
    // tenant exists by submitting that tenant's id and watching the error.
    val physicalTopic = "acme.orders"
    addTopicToMetadataCache(physicalTopic, numPartitions = 1)

    val produceRequest = buildSingleTopicProduceRequest("orders")
    val request = buildRequest(
      produceRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

    // Embed BOTH forms in the leaked message — exercises the precedence
    // (principal-prefix must run before topic-prefix or "__tenant_acme."
    // would be corrupted into "__tenant_" mid-scrub).
    val leakedMessage = "Transactional id '__tenant_acme.my-txn' is fenced on topic 'acme.orders'"
    when(replicaManager.handleProduceAppend(
      anyLong, anyShort, ArgumentMatchers.eq(false), any(),
      any(), responseCallback.capture(),
      any(), any(), any(), any())
    ).thenAnswer(_ => responseCallback.getValue.apply(
      Map(new TopicPartition(physicalTopic, 0) ->
        new PartitionResponse(Errors.INVALID_RECORD, leakedMessage))))

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[ProduceResponse](request)
    val partitionProduceResponse =
      response.data.responses.asScala.head.partitionResponses.asScala.head
    val msg = partitionProduceResponse.errorMessage
    assertNotNull(msg, "errorMessage must be propagated to the client")
    assertFalse(msg.contains("__tenant_"),
      s"principal-prefix form must be scrubbed but found in: $msg")
    assertFalse(msg.contains("acme."),
      s"any tenant-prefix occurrence must be scrubbed but found in: $msg")
    // The remaining tokens must still carry useful diagnostics — the txn id
    // and topic name without the prefix.
    assertTrue(msg.contains("my-txn"),
      s"logical txn id must remain in scrubbed message: $msg")
    assertTrue(msg.contains("orders"),
      s"logical topic name must remain in scrubbed message: $msg")
  }

  @Test
  def testProduceTenantScrubsPhysicalPrefixFromRecordErrorsMessage(): Unit = {
    // #134: KIP-467 per-record validation errors (ProduceResponse v8+) carry a
    // `recordErrors[].message` string that LogValidator builds by interpolating
    // the PHYSICAL TopicPartition (e.g. "Compacted topic cannot accept message
    // without key in topic partition acme.orders-0"). The PartitionResponse
    // OUT-rewrite previously only scrubbed `errorMessage`, not the
    // `recordErrors` list — so the physical tenant prefix leaked back to the
    // tenant client via this side channel. Own-tenant only, but violates the
    // contract that tenants never see their physical prefix on the wire.
    // The fix rebuilds each RecordError with scrubMessage applied
    // (RecordError.message is final, so in-place mutation isn't possible).
    val physicalTopic = "acme.orders"
    addTopicToMetadataCache(physicalTopic, numPartitions = 1)

    val produceRequest = buildSingleTopicProduceRequest("orders")
    val request = buildRequest(
      produceRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

    // Mirror what LogValidator.validateKey emits on a compacted topic with
    // missing key — the actual production source of the physical-name leak.
    val leakedRecordMsg =
      s"Compacted topic cannot accept message without key in topic partition $physicalTopic-0"
    val recordErrors = java.util.List.of(
      new org.apache.kafka.common.requests.ProduceResponse.RecordError(0, leakedRecordMsg))
    val pr = new PartitionResponse(Errors.INVALID_RECORD, 0L, -1L, 0L, recordErrors, null)
    when(replicaManager.handleProduceAppend(
      anyLong, anyShort, ArgumentMatchers.eq(false), any(),
      any(), responseCallback.capture(),
      any(), any(), any(), any())
    ).thenAnswer(_ => responseCallback.getValue.apply(
      Map(new TopicPartition(physicalTopic, 0) -> pr)))

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[ProduceResponse](request)
    val topicProduceResponse = response.data.responses.asScala.head
    assertEquals("orders", topicProduceResponse.name,
      "topic name in response must be logical")
    val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
    assertEquals(Errors.INVALID_RECORD,
      Errors.forCode(partitionProduceResponse.errorCode))
    val rEs = partitionProduceResponse.recordErrors
    assertNotNull(rEs, "recordErrors list must be propagated to the client")
    assertEquals(1, rEs.size, "the single record error must be preserved")
    val recordErrMsg = rEs.get(0).batchIndexErrorMessage
    assertNotNull(recordErrMsg, "recordErrors[0] message must be propagated")
    assertFalse(recordErrMsg.contains("acme.orders"),
      s"physical prefix must be scrubbed from recordErrors[].message but found in: $recordErrMsg")
    assertTrue(recordErrMsg.contains("orders"),
      s"logical topic name must remain in scrubbed recordErrors[].message: $recordErrMsg")
    assertEquals(0, rEs.get(0).batchIndex,
      "batchIndex must be preserved across the scrub-rebuild")
  }

  @Test
  def testProducePrivilegedCallerOnTenantBoundListenerIsRejected(): Unit = {
    // Super-user without a `__tenant_` prefix produces on a tenant-bound
    // listener. The broker MUST refuse every partition rather than silently
    // rewrite "orders" into "acme.orders" — that would let a privileged caller
    // pollute the tenant namespace, which is data corruption.
    addTopicToMetadataCache("acme.orders", numPartitions = 1)

    val produceRequest = buildSingleTopicProduceRequest("orders")
    val request = buildRequest(
      produceRequest,
      listenerName = TENANT_LISTENER,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice"))

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[ProduceResponse](request)
    assertEquals(1, response.data.responses.size)
    val topicProduceResponse = response.data.responses.asScala.head
    assertEquals("orders", topicProduceResponse.name,
      "rejection response must carry the logical name the caller used")
    val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED,
      Errors.forCode(partitionProduceResponse.errorCode),
      "privileged caller on tenant listener without tenant prefix must be refused")

    // replicaManager MUST NOT be invoked: the request never reaches the append path.
    verify(replicaManager, never()).handleProduceAppend(
      anyLong, anyShort, anyBoolean, any(), any(), any(), any(), any(), any(), any())
  }

  @Test
  def testProduceUnsafeWithAcksZeroClosesConnection(): Unit = {
    // acks=0: the producer does not expect a response. Sending a regular
    // ProduceResponse would leave the client waiting on a frame the wire
    // contract says the server will not send. The standard ack=0 error path
    // closes the connection so the client refreshes its metadata; the unsafe
    // refusal path MUST mirror that — anything else degrades into a wire
    // protocol mismatch on every refused fire-and-forget produce.
    addTopicToMetadataCache("acme.orders", numPartitions = 1)

    val produceRequest = buildSingleTopicProduceRequest("orders", acks = 0.toShort)
    val request = buildRequest(
      produceRequest,
      listenerName = TENANT_LISTENER,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice")) // no tenant prefix

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    verify(requestChannel).closeConnection(
      ArgumentMatchers.eq(request),
      any[java.util.Map[Errors, Integer]]())
    verify(requestChannel, never()).sendResponse(any(), any(), any())
    verify(replicaManager, never()).handleProduceAppend(
      anyLong, anyShort, anyBoolean(), any(), any(), any(), any(), any(), any(), any())
  }

  @Test
  def testProduceMismatchedPrincipalListenerIsRejected(): Unit = {
    // The TENANT_ACME listener is bound to "acme", but the principal claims
    // "__tenant_beta.alice". Operator-controlled values disagree. The broker
    // MUST refuse rather than pick a winner — otherwise a misconfiguration
    // could quietly route writes into a foreign tenant's namespace.
    addTopicToMetadataCache("acme.orders", numPartitions = 1)
    addTopicToMetadataCache("beta.orders", numPartitions = 1)

    val produceRequest = buildSingleTopicProduceRequest("orders")
    val request = buildRequest(
      produceRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("beta", "alice"))

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[ProduceResponse](request)
    assertEquals(1, response.data.responses.size)
    val topicProduceResponse = response.data.responses.asScala.head
    assertEquals("orders", topicProduceResponse.name,
      "rejection echoes the wire name; physical prefixes never appear in the response")
    val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED,
      Errors.forCode(partitionProduceResponse.errorCode),
      "principal/listener tenant disagreement must be refused")

    verify(replicaManager, never()).handleProduceAppend(
      anyLong, anyShort, anyBoolean(), any(), any(), any(), any(), any(), any(), any())
  }

  @Test
  def testProduceUntrustedTenantPrincipalOnUnboundListenerIsRejected(): Unit = {
    // SASL_PLAIN spoof vector: a client picks the username "__tenant_acme.alice"
    // on a listener that has NO TenantPrincipalBuilder binding. Without a
    // binding the prefix cannot have been minted by us, so honoring it would
    // let the client choose its own tenant identity. Refuse outright.
    addTopicToMetadataCache("acme.orders", numPartitions = 1)

    val produceRequest = buildSingleTopicProduceRequest("orders")
    // PLAINTEXT listener: no tenant binding for this listener name.
    val request = buildRequest(
      produceRequest,
      principal = tenantPrincipal("acme", "alice"))

    // tenantConfig is empty — no listener has a binding.
    kafkaApis = createKafkaApis(authorizer = None)
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[ProduceResponse](request)
    val topicProduceResponse = response.data.responses.asScala.head
    val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED,
      Errors.forCode(partitionProduceResponse.errorCode),
      "an untrusted __tenant_ principal on an unbound listener must be refused")

    verify(replicaManager, never()).handleProduceAppend(
      anyLong, anyShort, anyBoolean(), any(), any(), any(), any(), any(), any(), any())
  }

  @Test
  def testMetadataMismatchedPrincipalListenerIsRejected(): Unit = {
    addTopicToMetadataCache("acme.orders", numPartitions = 1)
    addTopicToMetadataCache("beta.orders", numPartitions = 1)

    val metadataRequest = new MetadataRequest.Builder(List("orders").asJava, false).build()
    val request = buildRequest(
      metadataRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("beta", "alice"))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    addTopicToMetadataCache("acme.orders", numPartitions = 1)
    addTopicToMetadataCache("beta.orders", numPartitions = 1)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleTopicMetadataRequest(request)

    val response = verifyNoThrottling[MetadataResponse](request)
    val errored = response.topicMetadata().asScala.toSeq
    assertEquals(1, errored.size)
    assertEquals("orders", errored.head.topic,
      "rejection echoes the logical name the caller used; never the physical one")
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, errored.head.error,
      "principal/listener tenant disagreement must be refused")
  }

  @Test
  def testMetadataUntrustedTenantPrincipalOnUnboundListenerIsRejected(): Unit = {
    // Same spoof vector as Produce: refuse outright when a __tenant_ prefix
    // arrives on a listener that did not mint it.
    addTopicToMetadataCache("acme.orders", numPartitions = 1)

    val metadataRequest = new MetadataRequest.Builder(List("orders").asJava, false).build()
    val request = buildRequest(
      metadataRequest,
      principal = tenantPrincipal("acme", "alice"))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    addTopicToMetadataCache("acme.orders", numPartitions = 1)
    kafkaApis = createKafkaApis()
    kafkaApis.handleTopicMetadataRequest(request)

    val response = verifyNoThrottling[MetadataResponse](request)
    val errored = response.topicMetadata().asScala.toSeq
    assertEquals(1, errored.size)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, errored.head.error,
      "an untrusted __tenant_ principal on an unbound listener must be refused")
  }

  @Test
  def testProduceNonTenantRequestUnchangedWhenNoBinding(): Unit = {
    // Single-tenant behaviour unchanged: no listener binding, no principal
    // prefix, no rewrite, and no guard fires.
    val topic = "plain-topic"
    addTopicToMetadataCache(topic, numPartitions = 1)

    val produceRequest = buildSingleTopicProduceRequest(topic)
    val request = buildRequest(produceRequest)

    val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])
    val entriesPerPartition: ArgumentCaptor[Map[TopicPartition, MemoryRecords]] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, MemoryRecords]])

    when(replicaManager.handleProduceAppend(
      anyLong, anyShort, ArgumentMatchers.eq(false), any(),
      entriesPerPartition.capture(),
      responseCallback.capture(),
      any(), any(), any(), any())
    ).thenAnswer(_ => responseCallback.getValue.apply(
      Map(new TopicPartition(topic, 0) -> new PartitionResponse(Errors.NONE))))

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    kafkaApis = createKafkaApis()
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    assertEquals(Set(new TopicPartition(topic, 0)), entriesPerPartition.getValue.keySet)
    val response = verifyNoThrottling[ProduceResponse](request)
    assertEquals(topic, response.data.responses.asScala.head.name)
  }

  // ---------------------------------------------------------------------------
  // Fetch — multi-tenancy
  //
  // Modern (v13+) Fetch is keyed on topic IDs; the tenant client never puts a
  // name on the wire. The broker resolves the topic ID to a physical name
  // via metadataCache.topicIdsToNames(), serves data from the physical log,
  // and the v13+ response carries only the topic ID — so the physical name
  // never leaks. Three things still matter and are tested here:
  //
  //   1. tenant boundary — a tenant must not be able to fetch a topic ID
  //      whose physical name lives outside its namespace, even if it somehow
  //      learned that ID. We return UNKNOWN_TOPIC_OR_PARTITION rather than
  //      revealing whether the topic exists.
  //   2. privileged-on-tenant-listener guard — same rationale as Produce /
  //      Metadata: don't let a super-user piggyback off the listener binding.
  //   3. unchanged single-tenant behaviour when no binding exists.
  // ---------------------------------------------------------------------------

  private def buildSingleTopicFetchRequest(topicId: Uuid, tp: TopicPartition): FetchRequest = {
    val fetchDataBuilder = Map(tp -> new FetchRequest.PartitionData(topicId, 0, 0, 1000,
      Optional.empty())).asJava
    new FetchRequest.Builder(16, 16, -1, -1, 100, 0, fetchDataBuilder).build()
  }

  @Test
  def testFetchTenantRequestRoutesToPhysicalTopicAndDoesNotLeakName(): Unit = {
    // Tenant fetches by topic ID. metadataCache resolves the ID to "acme.orders"
    // (physical). replicaManager.fetchMessages must receive a TopicIdPartition
    // whose topic name is the physical one; the v13+ response carries no name
    // so the client only sees the topicId.
    val topicId = Uuid.randomUuid()
    val physicalTp = new TopicPartition("acme.orders", 0)
    val tidp = new TopicIdPartition(topicId, physicalTp)
    addTopicToMetadataCache(physicalTp.topic, numPartitions = 1, numBrokers = 1, topicId)

    when(replicaManager.fetchMessages(
      any[FetchParams],
      any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]],
      any[ReplicaQuota],
      any[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]()
    )).thenAnswer(invocation => {
      val interesting = invocation.getArgument(1).asInstanceOf[Seq[(TopicIdPartition, FetchRequest.PartitionData)]]
      // Pin down the contract: replicaManager must be called with the physical name.
      assertEquals(Set(tidp), interesting.map(_._1).toSet)
      val callback = invocation.getArgument(3).asInstanceOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]
      callback(Seq(tidp -> new FetchPartitionData(Errors.NONE, 100, 0, MemoryRecords.EMPTY,
        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)))
    })

    val fetchData = Map(tidp -> new FetchRequest.PartitionData(topicId, 0, 0, 1000,
      Optional.empty())).asJava
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      fetchMetadata, fetchData, true, false)
    when(fetchManager.newContext(
      any[Short], any[JFetchMetadata], any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      any[util.List[TopicIdPartition]],
      any[util.Map[Uuid, String]],
      any[Option[String]])).thenReturn(fetchContext)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val fetchRequest = buildSingleTopicFetchRequest(topicId, physicalTp)
    val request = buildRequest(
      fetchRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleFetchRequest(request)

    val response = verifyNoThrottling[FetchResponse](request)
    // v13+ response: only topicId is on the wire. The client never sees the
    // physical name "acme.orders" because the Topic field is absent from v13+.
    val topicResponses = response.data.responses.asScala.toSeq
    assertEquals(1, topicResponses.size)
    assertEquals(topicId, topicResponses.head.topicId)
    val partitionData = topicResponses.head.partitions.asScala.head
    assertEquals(Errors.NONE.code, partitionData.errorCode)
  }

  @Test
  def testFetchTenantBoundaryViolationReturnsUnknownTopic(): Unit = {
    // Tenant beta somehow obtains topic ID for "acme.orders" and tries to fetch
    // it. The broker must surface UNKNOWN_TOPIC_OR_PARTITION rather than serve
    // data from another tenant's namespace. replicaManager is never invoked.
    val topicId = Uuid.randomUuid()
    val physicalTp = new TopicPartition("acme.orders", 0)
    val tidp = new TopicIdPartition(topicId, physicalTp)
    addTopicToMetadataCache(physicalTp.topic, numPartitions = 1, numBrokers = 1, topicId)

    val fetchData = Map(tidp -> new FetchRequest.PartitionData(topicId, 0, 0, 1000,
      Optional.empty())).asJava
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      fetchMetadata, fetchData, true, false)
    when(fetchManager.newContext(
      any[Short], any[JFetchMetadata], any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      any[util.List[TopicIdPartition]],
      any[util.Map[Uuid, String]],
      any[Option[String]])).thenReturn(fetchContext)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val fetchRequest = buildSingleTopicFetchRequest(topicId, physicalTp)
    val request = buildRequest(
      fetchRequest,
      listenerName = new ListenerName("TENANT_BETA"),
      principal = tenantPrincipal("beta", "bob"))

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("beta", new ListenerName("TENANT_BETA")))
    kafkaApis.handleFetchRequest(request)

    val response = verifyNoThrottling[FetchResponse](request)
    val partitionData = response.data.responses.asScala.head.partitions.asScala.head
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code, partitionData.errorCode,
      "tenant must not be able to fetch outside its namespace")

    verify(replicaManager, never()).fetchMessages(any(), any(), any(), any())
  }

  @Test
  def testFetchTenantForeignPartitionMustNotEnterSession(): Unit = {
    // The CRITICAL leak: if a foreign TopicIdPartition (a topic id whose
    // physical name lives outside the tenant's namespace) is passed to
    // fetchManager.newContext, the FetchSession will cache it. A later
    // incremental fetch on that session would iterate the cached entry via
    // foreachPartition with no per-request "foreign" marker, and any
    // permissive authorizer (or a misconfigured cluster) would then read
    // foreign data. The defence is: don't let foreign TIPs into the session
    // at all. We still surface UNKNOWN_TOPIC_OR_PARTITION for them so the
    // response is identical to "topic doesn't exist".
    val topicId = Uuid.randomUuid()
    val physicalTp = new TopicPartition("acme.orders", 0)
    addTopicToMetadataCache(physicalTp.topic, numPartitions = 1, numBrokers = 1, topicId)

    val emptyFetchData = new util.LinkedHashMap[TopicIdPartition, FetchRequest.PartitionData]()
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      fetchMetadata, emptyFetchData, true, false)
    val newContextFetchDataCaptor = ArgumentCaptor.forClass(classOf[util.Map[TopicIdPartition, FetchRequest.PartitionData]])
    when(fetchManager.newContext(
      any[Short], any[JFetchMetadata], any[Boolean],
      newContextFetchDataCaptor.capture(),
      any[util.List[TopicIdPartition]],
      any[util.Map[Uuid, String]],
      any[Option[String]])).thenReturn(fetchContext)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val fetchRequest = buildSingleTopicFetchRequest(topicId, physicalTp)
    val request = buildRequest(
      fetchRequest,
      listenerName = new ListenerName("TENANT_BETA"),
      principal = tenantPrincipal("beta", "bob"))

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("beta", new ListenerName("TENANT_BETA")))
    kafkaApis.handleFetchRequest(request)

    assertTrue(newContextFetchDataCaptor.getValue.isEmpty,
      s"foreign TIPs must not enter fetchManager.newContext; saw ${newContextFetchDataCaptor.getValue.keySet}")
    val response = verifyNoThrottling[FetchResponse](request)
    val partitionData = response.data.responses.asScala.head.partitions.asScala.head
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code, partitionData.errorCode,
      "tenant probing a foreign id must still see UNKNOWN_TOPIC_OR_PARTITION on the first request")
  }

  @Test
  def testFetchTenantRefusesInternalTopicEvenIfAclWouldAllow(): Unit = {
    // Defence-in-depth: a tenant principal must never be able to Fetch
    // __consumer_offsets / __transaction_state / __share_group_state directly.
    // The legitimate path is via the coordinator APIs (OffsetFetch,
    // FindCoordinator, AddPartitionsToTxn, ...) which key by tenant-scoped id.
    // If an operator mistakenly granted READ on __consumer_offsets, a direct
    // Fetch would expose every tenant's commits. The handler refuses regardless
    // of ACL — replicaManager is never invoked — and surfaces
    // UNKNOWN_TOPIC_OR_PARTITION so existence cannot be probed.
    //
    // The internal TIP is filtered into `foreignFetchTips` BEFORE
    // `fetchManager.newContext`, so the resulting session map is empty — we
    // model that by handing newContext an empty FullFetchContext and asserting
    // (via the captor) that no TIP entered the session. The wire-level
    // UNKNOWN_TOPIC_OR_PARTITION is merged in via `appendForeignErroneousRows`.
    val topicId = Uuid.randomUuid()
    val internalTp = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)
    addTopicToMetadataCache(internalTp.topic, numPartitions = 1, numBrokers = 1, topicId)

    val emptyFetchData = new util.LinkedHashMap[TopicIdPartition, FetchRequest.PartitionData]()
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      fetchMetadata, emptyFetchData, true, false)
    val newContextFetchDataCaptor = ArgumentCaptor.forClass(classOf[util.Map[TopicIdPartition, FetchRequest.PartitionData]])
    when(fetchManager.newContext(
      any[Short], any[JFetchMetadata], any[Boolean],
      newContextFetchDataCaptor.capture(),
      any[util.List[TopicIdPartition]],
      any[util.Map[Uuid, String]],
      any[Option[String]])).thenReturn(fetchContext)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val fetchRequest = buildSingleTopicFetchRequest(topicId, internalTp)
    val request = buildRequest(
      fetchRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleFetchRequest(request)

    assertTrue(newContextFetchDataCaptor.getValue.isEmpty,
      s"internal-topic TIPs must not enter fetchManager.newContext; saw ${newContextFetchDataCaptor.getValue.keySet}")
    val response = verifyNoThrottling[FetchResponse](request)
    val partitionData = response.data.responses.asScala.head.partitions.asScala.head
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code, partitionData.errorCode,
      "tenant must not be able to Fetch __consumer_offsets directly")

    verify(replicaManager, never()).fetchMessages(any(), any(), any(), any())
  }

  @Test
  def testFetchTenantForgottenUnresolvableIdIsPassedThroughForSessionRemoval(): Unit = {
    // When a tenant deletes a topic and then issues an incremental fetch
    // forgetting it, the topic id no longer resolves via
    // metadataCache.topicIdsToNames — so the TIP coming off the wire has
    // topic=null. The broker must still pass the forgotten entry through to
    // FetchSession.update so the stale session record is dropped. Previously
    // the forgotten id was filtered out entirely, leaving a zombie session
    // entry that kept producing errors until the client reset the session.
    val deletedId = Uuid.randomUuid()
    // Note: deletedId is intentionally NOT registered with metadataCache so
    // topicIdsToNames() will not have a mapping for it.
    val emptyFetchData = new util.LinkedHashMap[TopicIdPartition, FetchRequest.PartitionData]()
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      fetchMetadata, emptyFetchData, true, false)
    val forgottenCaptor = ArgumentCaptor.forClass(classOf[util.List[TopicIdPartition]])
    when(fetchManager.newContext(
      any[Short], any[JFetchMetadata], any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      forgottenCaptor.capture(),
      any[util.Map[Uuid, String]],
      any[Option[String]])).thenReturn(fetchContext)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val toForgetTidp = new TopicIdPartition(deletedId, 0, null)
    val fetchDataBuilder = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]()
    val fetchRequest = new FetchRequest.Builder(13, 13, -1, -1, 100, 0, fetchDataBuilder)
      .removed(util.Arrays.asList(toForgetTidp))
      .build(13.toShort)
    val request = buildRequest(
      fetchRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleFetchRequest(request)

    val forwardedForgotten = forgottenCaptor.getValue.asScala
    assertEquals(1, forwardedForgotten.size,
      s"forgotten ids with null name must reach FetchSession.update so the stale session entry can be removed; saw $forwardedForgotten")
    assertEquals(deletedId, forwardedForgotten.head.topicId,
      "the forgotten topic id must be the one the client asked to remove")
  }

  @Test
  def testFetchPrivilegedCallerOnTenantBoundListenerIsRejected(): Unit = {
    // Super-user without `__tenant_` prefix fetching on a tenant-bound
    // listener: every requested partition gets TOPIC_AUTHORIZATION_FAILED;
    // replicaManager is never invoked.
    val topicId = Uuid.randomUuid()
    val physicalTp = new TopicPartition("acme.orders", 0)
    addTopicToMetadataCache(physicalTp.topic, numPartitions = 1, numBrokers = 1, topicId)

    val fetchRequest = buildSingleTopicFetchRequest(topicId, physicalTp)
    val request = buildRequest(
      fetchRequest,
      listenerName = TENANT_LISTENER,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice"))

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleFetchRequest(request)

    val response = verifyNoThrottling[FetchResponse](request)
    val partitionData = response.data.responses.asScala.head.partitions.asScala.head
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, partitionData.errorCode,
      "privileged caller on tenant listener without tenant prefix must be refused")

    verify(replicaManager, never()).fetchMessages(any(), any(), any(), any())
  }

  @Test
  def testFetchClusterWideCallerRefusesTenantReservedPhysicalForm(): Unit = {
    // Outside-in pollution guard for Fetch (mirror of the Produce / DeleteTopics
    // guards). A non-tenant principal on the cluster-wide listener naming
    // `acme.orders` directly would otherwise reach the authz/metadata stage —
    // and a typical `User:* READ Topic:*` cluster-admin ACL passes that. With
    // the guard, the reserved TIP is bucketed into foreignFetchTips before
    // fetchManager.newContext, surfaces as UNKNOWN_TOPIC_OR_PARTITION (the same
    // shape as a real miss → no existence oracle), and is never fetched. The
    // TIP must also be kept out of the session map: otherwise an incremental
    // fetch on the same session would later iterate it via foreachPartition
    // with no per-request foreign marker, recreating the leak.
    val topicId = Uuid.randomUuid()
    val physicalTp = new TopicPartition("acme.orders", 0)
    addTopicToMetadataCache(physicalTp.topic, numPartitions = 1, numBrokers = 1, topicId)

    val emptyFetchData = new util.LinkedHashMap[TopicIdPartition, FetchRequest.PartitionData]()
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      fetchMetadata, emptyFetchData, true, false)
    val newContextFetchDataCaptor = ArgumentCaptor.forClass(classOf[util.Map[TopicIdPartition, FetchRequest.PartitionData]])
    when(fetchManager.newContext(
      any[Short], any[JFetchMetadata], any[Boolean],
      newContextFetchDataCaptor.capture(),
      any[util.List[TopicIdPartition]],
      any[util.Map[Uuid, String]],
      any[Option[String]])).thenReturn(fetchContext)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val fetchRequest = buildSingleTopicFetchRequest(topicId, physicalTp)
    val request = buildRequest(fetchRequest)

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleFetchRequest(request)

    assertTrue(newContextFetchDataCaptor.getValue.isEmpty,
      s"reserved-physical-form TIPs must not enter fetchManager.newContext on the cluster-wide listener; saw ${newContextFetchDataCaptor.getValue.keySet}")

    val response = verifyNoThrottling[FetchResponse](request)
    val partitionData = response.data.responses.asScala.head.partitions.asScala.head
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code, partitionData.errorCode,
      "non-tenant caller naming a tenant physical topic must see UNKNOWN_TOPIC_OR_PARTITION, identical to a real miss")

    verify(replicaManager, never()).fetchMessages(any(), any(), any(), any())
  }

  @Test
  def testFetchClusterWideCallerKeepsDottedNameWhenNoTenantsConfigured(): Unit = {
    // Without configured tenants, `acme.orders` is just a topic name with a
    // dot. The outside-in guard must short-circuit so legitimate non-tenant
    // clusters keep their ability to fetch from any topic. Verified by capturing
    // the fetchData passed into fetchManager.newContext and asserting the TIP
    // survived — replicaManager mocking is unnecessary because we stop at the
    // session boundary.
    val topicId = Uuid.randomUuid()
    val tp = new TopicPartition("acme.orders", 0)
    val tidp = new TopicIdPartition(topicId, tp)
    addTopicToMetadataCache(tp.topic, numPartitions = 1, numBrokers = 1, topicId)

    val emptyFetchData = new util.LinkedHashMap[TopicIdPartition, FetchRequest.PartitionData]()
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      fetchMetadata, emptyFetchData, true, false)
    val newContextFetchDataCaptor = ArgumentCaptor.forClass(classOf[util.Map[TopicIdPartition, FetchRequest.PartitionData]])
    when(fetchManager.newContext(
      any[Short], any[JFetchMetadata], any[Boolean],
      newContextFetchDataCaptor.capture(),
      any[util.List[TopicIdPartition]],
      any[util.Map[Uuid, String]],
      any[Option[String]])).thenReturn(fetchContext)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val fetchRequest = buildSingleTopicFetchRequest(topicId, tp)
    val request = buildRequest(fetchRequest)

    kafkaApis = createKafkaApis() // no tenantConfig → allTenants is empty
    kafkaApis.handleFetchRequest(request)

    val capturedFetchData = newContextFetchDataCaptor.getValue
    assertTrue(capturedFetchData.containsKey(tidp),
      s"with no tenants configured the dotted TIP must reach fetchManager.newContext; saw ${capturedFetchData.keySet}")
  }

  @Test
  def testFetchFollowerSpoofOnClusterWideListenerRefusesTenantPhysicalForm(): Unit = {
    // Adversarial: `FetchRequest.isFromFollower` is wire-derived (replicaId
    // >= 0); a caller with CLUSTER_ACTION on a cluster-wide listener can flip
    // it from -1 to 99 and turn into a "follower". The previous outside-in
    // predicate (`!fetchRequest.isFromFollower`) used that single boolean to
    // disable the foreign-fetch guard, and the follower branch's
    // CLUSTER_ACTION check let the spoofed request through. The fix pins
    // follower trust to `config.interBrokerListenerName`: replicaId>=0 on any
    // other listener is treated as a regular consumer fetch and the
    // outside-in guard buckets `acme.orders` into foreignFetchTips →
    // UNKNOWN_TOPIC_OR_PARTITION, indistinguishable from a real miss.
    val attackerListener = new ListenerName("EXTERNAL_SASL")
    val topicId = Uuid.randomUuid()
    val physicalTp = new TopicPartition("acme.orders", 0)
    addTopicToMetadataCache(physicalTp.topic, numPartitions = 1, numBrokers = 1, topicId)

    val emptyFetchData = new util.LinkedHashMap[TopicIdPartition, FetchRequest.PartitionData]()
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      fetchMetadata, emptyFetchData, true, true)
    val newContextFetchDataCaptor = ArgumentCaptor.forClass(classOf[util.Map[TopicIdPartition, FetchRequest.PartitionData]])
    when(fetchManager.newContext(
      any[Short], any[JFetchMetadata], any[Boolean],
      newContextFetchDataCaptor.capture(),
      any[util.List[TopicIdPartition]],
      any[util.Map[Uuid, String]],
      any[Option[String]])).thenReturn(fetchContext)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    // Spoofed follower fetch: replicaId=99 (not -1). Default inter-broker
    // listener is PLAINTEXT; the attacker arrives on EXTERNAL_SASL with
    // CLUSTER_ACTION-class privileges (no tenant prefix on the principal).
    val fetchDataBuilder = Map(physicalTp -> new FetchRequest.PartitionData(topicId, 0, 0, 1000,
      Optional.empty())).asJava
    val fetchRequest = new FetchRequest.Builder(16, 16, 99, 1, 100, 0, fetchDataBuilder).build()
    val request = buildRequest(
      fetchRequest,
      listenerName = attackerListener,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "cluster-admin"))

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleFetchRequest(request)

    assertTrue(newContextFetchDataCaptor.getValue.isEmpty,
      s"spoofed-follower reserved-form TIPs must not enter fetchManager.newContext; saw ${newContextFetchDataCaptor.getValue.keySet}")

    val response = verifyNoThrottling[FetchResponse](request)
    val partitionData = response.data.responses.asScala.head.partitions.asScala.head
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code, partitionData.errorCode,
      "spoofed-follower fetch on a non-inter-broker listener must see UNKNOWN_TOPIC_OR_PARTITION, identical to a real miss")

    verify(replicaManager, never()).fetchMessages(any(), any(), any(), any())
  }

  @Test
  def testFetchLegitimateFollowerOnInterBrokerListenerSkipsOutsideInGuard(): Unit = {
    // Complement of the spoof test: a real replica fetch on the inter-broker
    // listener (PLAINTEXT in test setup, == config.interBrokerListenerName)
    // must continue to address tenant physical topics by name. Otherwise
    // replication of tenant partitions would be broken by the fix above.
    // The TIP must reach fetchManager.newContext untouched.
    val topicId = Uuid.randomUuid()
    val physicalTp = new TopicPartition("acme.orders", 0)
    val tidp = new TopicIdPartition(topicId, physicalTp)
    addTopicToMetadataCache(physicalTp.topic, numPartitions = 1, numBrokers = 1, topicId)

    val emptyFetchData = new util.LinkedHashMap[TopicIdPartition, FetchRequest.PartitionData]()
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      fetchMetadata, emptyFetchData, true, true)
    val newContextFetchDataCaptor = ArgumentCaptor.forClass(classOf[util.Map[TopicIdPartition, FetchRequest.PartitionData]])
    when(fetchManager.newContext(
      any[Short], any[JFetchMetadata], any[Boolean],
      newContextFetchDataCaptor.capture(),
      any[util.List[TopicIdPartition]],
      any[util.Map[Uuid, String]],
      any[Option[String]])).thenReturn(fetchContext)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val fetchDataBuilder = Map(physicalTp -> new FetchRequest.PartitionData(topicId, 0, 0, 1000,
      Optional.empty())).asJava
    // replicaId=2 = legitimate follower; listenerName defaults to PLAINTEXT
    // which is exactly the inter-broker listener in the test broker config.
    val fetchRequest = new FetchRequest.Builder(16, 16, 2, 1, 100, 0, fetchDataBuilder).build()
    val request = buildRequest(
      fetchRequest,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "broker-2"))

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleFetchRequest(request)

    val capturedFetchData = newContextFetchDataCaptor.getValue
    assertTrue(capturedFetchData.containsKey(tidp),
      s"legitimate follower fetch on inter-broker listener must reach fetchManager.newContext; saw ${capturedFetchData.keySet}")
  }

  @Test
  def testFetchV12TenantResponseTopicFieldIsLogical(): Unit = {
    // Pre-id-fetch (v0-12) carries the Topic string on the wire. The broker
    // must rewrite logical -> physical end-to-end (so sessions, replicaManager
    // and response generation all run on physical names) AND rewrite the
    // response's Topic field back to logical, or it would leak `acme.orders`
    // to a tenant that asked for `orders`.
    val topicId = Uuid.randomUuid()
    val logicalTp = new TopicPartition("orders", 0)
    val physicalTp = new TopicPartition("acme.orders", 0)
    val physicalTidp = new TopicIdPartition(Uuid.ZERO_UUID, physicalTp)
    addTopicToMetadataCache(physicalTp.topic, numPartitions = 1, numBrokers = 1, topicId)

    when(replicaManager.fetchMessages(
      any[FetchParams],
      any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]],
      any[ReplicaQuota],
      any[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]()
    )).thenAnswer(invocation => {
      val interesting = invocation.getArgument(1).asInstanceOf[Seq[(TopicIdPartition, FetchRequest.PartitionData)]]
      assertEquals(Set("acme.orders"), interesting.map(_._1.topic).toSet,
        "replicaManager must be fetched against the physical name")
      val callback = invocation.getArgument(3).asInstanceOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]
      callback(Seq(physicalTidp -> new FetchPartitionData(Errors.NONE, 100, 0, MemoryRecords.EMPTY,
        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)))
    })

    // Build a v12 request that carries the LOGICAL topic name on the wire.
    // After the handler's IN rewrite the fetchContext is built around the
    // PHYSICAL TopicIdPartition, so the test's fake fetchContext is keyed by
    // the physical TIP — anything else would diverge from production state.
    val fetchData = Map(physicalTidp -> new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0, 0, 1000,
      Optional.empty())).asJava
    val fetchDataBuilder = Map(logicalTp -> new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0, 0, 1000,
      Optional.empty())).asJava
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      fetchMetadata, fetchData, true, false)
    val newContextFetchDataCaptor = ArgumentCaptor.forClass(classOf[util.Map[TopicIdPartition, FetchRequest.PartitionData]])
    when(fetchManager.newContext(
      any[Short], any[JFetchMetadata], any[Boolean],
      newContextFetchDataCaptor.capture(),
      any[util.List[TopicIdPartition]],
      any[util.Map[Uuid, String]],
      any[Option[String]])).thenReturn(fetchContext)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val fetchRequest = new FetchRequest.Builder(12, 12, -1, -1, 100, 0, fetchDataBuilder).build()
    val request = buildRequest(
      fetchRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleFetchRequest(request)

    // IN rewrite happened before the fetch context was built.
    assertEquals(Set("acme.orders"), newContextFetchDataCaptor.getValue.keySet.asScala.map(_.topic).toSet,
      "fetchManager.newContext must be called with physical names so session tracking is symmetric end-to-end")

    val response = verifyNoThrottling[FetchResponse](request)
    val topicResponses = response.data.responses.asScala.toSeq
    assertEquals(1, topicResponses.size)
    assertEquals("orders", topicResponses.head.topic,
      "v0-12 response topic field must be the logical name, never the physical prefix")
  }

  // ---------------------------------------------------------------------------
  // CreateTopics — multi-tenancy
  //
  // CreateTopics is forwarded to the controller. The broker rewrites the
  // request body from logical → physical names BEFORE forwarding so the
  // controller stores the topic under the tenant-prefixed name. The response
  // is rewritten the other way so the client sees only the logical name —
  // including in error responses (e.g. TOPIC_ALREADY_EXISTS). A privileged
  // caller on a tenant-bound listener is refused before forwarding.
  // ---------------------------------------------------------------------------

  private def captureForwardedCreateTopics(request: RequestChannel.Request)
      : (CreateTopicsRequest, Option[AbstractResponse] => Unit) = {
    val bodyCaptor: ArgumentCaptor[AbstractRequest] = ArgumentCaptor.forClass(classOf[AbstractRequest])
    val callbackCaptor: ArgumentCaptor[Option[AbstractResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])
    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      bodyCaptor.capture(),
      callbackCaptor.capture())
    (bodyCaptor.getValue.asInstanceOf[CreateTopicsRequest], callbackCaptor.getValue)
  }

  @Test
  def testCreateTopicsTenantRewritesLogicalNameToPhysicalBeforeForwarding(): Unit = {
    // The tenant submits CreateTopics for "orders"; the broker must forward
    // the request to the controller with the physical name "acme.orders".
    // The controller's response (also physical) must come back to the client
    // stripped to "orders".
    val createRequest = new CreateTopicsRequest.Builder(new CreateTopicsRequestData()
      .setTopics(new CreateTopicsRequestData.CreatableTopicCollection(
        Collections.singleton(new CreateTopicsRequestData.CreatableTopic()
          .setName("orders").setNumPartitions(1).setReplicationFactor(1.toShort)).iterator)))
      .build()
    val request = buildRequest(
      createRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleCreateTopicsRequest(request)

    val (forwarded, callback) = captureForwardedCreateTopics(request)
    assertEquals(Set("acme.orders"), forwarded.data.topics.asScala.map(_.name).toSet,
      "controller must see the physical topic name")

    // Simulate the controller's response (physical name + topic id).
    val topicId = Uuid.randomUuid()
    val controllerResponse = new CreateTopicsResponse(new CreateTopicsResponseData()
      .setTopics(new CreateTopicsResponseData.CreatableTopicResultCollection(
        Collections.singleton(new CreateTopicsResponseData.CreatableTopicResult()
          .setName("acme.orders").setTopicId(topicId).setErrorCode(Errors.NONE.code)).iterator)))
    callback(Some(controllerResponse))

    val response = verifyNoThrottling[CreateTopicsResponse](request)
    assertEquals(1, response.data.topics.size)
    val result = response.data.topics.asScala.head
    assertEquals("orders", result.name, "client must see the logical topic name")
    assertEquals(topicId, result.topicId, "topic id must be preserved through the rewrite")
    assertEquals(Errors.NONE.code, result.errorCode)
  }

  @Test
  def testCreateTopicsTenantResponseStripsPhysicalPrefixFromErrorResponses(): Unit = {
    // Topic already exists at the physical layer; the controller returns
    // TOPIC_ALREADY_EXISTS keyed on "acme.orders". The client must see the
    // error against "orders" — the physical prefix must never leak.
    val createRequest = new CreateTopicsRequest.Builder(new CreateTopicsRequestData()
      .setTopics(new CreateTopicsRequestData.CreatableTopicCollection(
        Collections.singleton(new CreateTopicsRequestData.CreatableTopic()
          .setName("orders").setNumPartitions(1).setReplicationFactor(1.toShort)).iterator)))
      .build()
    val request = buildRequest(
      createRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleCreateTopicsRequest(request)

    val (_, callback) = captureForwardedCreateTopics(request)
    val controllerResponse = new CreateTopicsResponse(new CreateTopicsResponseData()
      .setTopics(new CreateTopicsResponseData.CreatableTopicResultCollection(
        Collections.singleton(new CreateTopicsResponseData.CreatableTopicResult()
          .setName("acme.orders")
          .setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code)
          .setErrorMessage("Topic 'acme.orders' already exists.")).iterator)))
    callback(Some(controllerResponse))

    val response = verifyNoThrottling[CreateTopicsResponse](request)
    val result = response.data.topics.asScala.head
    assertEquals("orders", result.name, "error response must carry the logical name")
    assertEquals(Errors.TOPIC_ALREADY_EXISTS.code, result.errorCode)
  }

  @Test
  def testCreateTopicsPrivilegedCallerOnTenantBoundListenerIsRejected(): Unit = {
    // Super-user without `__tenant_` prefix on a tenant-bound listener:
    // refuse rather than rewrite the create into the tenant namespace
    // (silent rewrite would let a privileged caller pollute the tenant).
    val createRequest = new CreateTopicsRequest.Builder(new CreateTopicsRequestData()
      .setTopics(new CreateTopicsRequestData.CreatableTopicCollection(
        Collections.singleton(new CreateTopicsRequestData.CreatableTopic()
          .setName("orders").setNumPartitions(1).setReplicationFactor(1.toShort)).iterator)))
      .build()
    val request = buildRequest(
      createRequest,
      listenerName = TENANT_LISTENER,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleCreateTopicsRequest(request)

    val response = verifyNoThrottling[CreateTopicsResponse](request)
    val result = response.data.topics.asScala.head
    assertEquals("orders", result.name, "rejection response must carry the logical name")
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, result.errorCode,
      "privileged caller on tenant listener without tenant prefix must be refused")

    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testCreateTopicsNonTenantRequestUnchangedWhenNoBinding(): Unit = {
    // Single-tenant behaviour unchanged: no listener binding, no principal
    // prefix, request forwarded verbatim via the original 2-arg path.
    val createRequest = new CreateTopicsRequest.Builder(new CreateTopicsRequestData()
      .setTopics(new CreateTopicsRequestData.CreatableTopicCollection(
        Collections.singleton(new CreateTopicsRequestData.CreatableTopic()
          .setName("plain-topic").setNumPartitions(1).setReplicationFactor(1.toShort)).iterator)))
      .build()
    val request = buildRequest(createRequest)

    kafkaApis = createKafkaApis()
    kafkaApis.handleCreateTopicsRequest(request)

    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      any[Option[AbstractResponse] => Unit]())
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
  }

  // ---------------------------------------------------------------------------
  // DeleteTopics — multi-tenancy
  //
  // The same shape as CreateTopics: rewrite request IN, forward, rewrite
  // response OUT, refuse privileged-on-tenant-listener. v6+ supports delete
  // by topic id; if the controller resolves the id to a foreign physical
  // name we redact the response to UNKNOWN_TOPIC_ID rather than leaking it.
  // ---------------------------------------------------------------------------

  private def captureForwardedDeleteTopics(request: RequestChannel.Request)
      : (DeleteTopicsRequest, Option[AbstractResponse] => Unit) = {
    val bodyCaptor: ArgumentCaptor[AbstractRequest] = ArgumentCaptor.forClass(classOf[AbstractRequest])
    val callbackCaptor: ArgumentCaptor[Option[AbstractResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])
    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      bodyCaptor.capture(),
      callbackCaptor.capture())
    (bodyCaptor.getValue.asInstanceOf[DeleteTopicsRequest], callbackCaptor.getValue)
  }

  @Test
  def testDeleteTopicsTenantRewritesLogicalNameToPhysicalBeforeForwarding(): Unit = {
    // Tenant submits DeleteTopics for "orders"; controller must see "acme.orders"
    // on the wire and the client must see "orders" on the way back.
    val deleteRequest = new DeleteTopicsRequest.Builder(new DeleteTopicsRequestData()
      .setTopics(util.Arrays.asList(new DeleteTopicsRequestData.DeleteTopicState().setName("orders")))
      .setTimeoutMs(5000)).build()
    val request = buildRequest(
      deleteRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDeleteTopicsRequest(request)

    val (forwarded, callback) = captureForwardedDeleteTopics(request)
    assertEquals(Set("acme.orders"),
      forwarded.data.topics.asScala.map(_.name).toSet,
      "controller must see the physical topic name")

    val topicId = Uuid.randomUuid()
    val controllerResponse = new DeleteTopicsResponse(new DeleteTopicsResponseData()
      .setResponses(new DeleteTopicsResponseData.DeletableTopicResultCollection(
        Collections.singleton(new DeleteTopicsResponseData.DeletableTopicResult()
          .setName("acme.orders").setTopicId(topicId).setErrorCode(Errors.NONE.code)).iterator)))
    callback(Some(controllerResponse))

    val response = verifyNoThrottling[DeleteTopicsResponse](request)
    val result = response.data.responses.asScala.head
    assertEquals("orders", result.name, "client must see the logical topic name")
    assertEquals(topicId, result.topicId)
    assertEquals(Errors.NONE.code, result.errorCode)
  }

  @Test
  def testDeleteTopicsTenantResponseStripsPhysicalPrefixFromErrorResponses(): Unit = {
    // Topic doesn't exist; controller returns UNKNOWN_TOPIC_OR_PARTITION
    // keyed on "acme.orders". Client must see the error against "orders".
    val deleteRequest = new DeleteTopicsRequest.Builder(new DeleteTopicsRequestData()
      .setTopics(util.Arrays.asList(new DeleteTopicsRequestData.DeleteTopicState().setName("orders")))
      .setTimeoutMs(5000)).build()
    val request = buildRequest(
      deleteRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDeleteTopicsRequest(request)

    val (_, callback) = captureForwardedDeleteTopics(request)
    val controllerResponse = new DeleteTopicsResponse(new DeleteTopicsResponseData()
      .setResponses(new DeleteTopicsResponseData.DeletableTopicResultCollection(
        Collections.singleton(new DeleteTopicsResponseData.DeletableTopicResult()
          .setName("acme.orders").setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
          .setErrorMessage("This server does not host this topic-partition.")).iterator)))
    callback(Some(controllerResponse))

    val response = verifyNoThrottling[DeleteTopicsResponse](request)
    val result = response.data.responses.asScala.head
    assertEquals("orders", result.name, "error response must carry the logical name")
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code, result.errorCode)
  }

  @Test
  def testDeleteTopicsTenantScrubsErrorMessageWhenControllerOmitsTopicName(): Unit = {
    // Defence-in-depth: when the controller can't resolve the topic id and
    // returns `name=null`, errorMessage may still embed a physical name like
    // "topic acme.orders not found". The broker must scrub that string before
    // surfacing it to the tenant — otherwise the prefix leaks through the
    // name=null escape hatch.
    val deleteRequest = new DeleteTopicsRequest.Builder(new DeleteTopicsRequestData()
      .setTopics(util.Arrays.asList(new DeleteTopicsRequestData.DeleteTopicState().setName("orders")))
      .setTimeoutMs(5000)).build()
    val request = buildRequest(
      deleteRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDeleteTopicsRequest(request)

    val (_, callback) = captureForwardedDeleteTopics(request)
    val controllerResponse = new DeleteTopicsResponse(new DeleteTopicsResponseData()
      .setResponses(new DeleteTopicsResponseData.DeletableTopicResultCollection(
        Collections.singleton(new DeleteTopicsResponseData.DeletableTopicResult()
          .setName(null)
          .setErrorCode(Errors.UNKNOWN_TOPIC_ID.code)
          .setErrorMessage("Topic acme.orders could not be resolved.")).iterator)))
    callback(Some(controllerResponse))

    val response = verifyNoThrottling[DeleteTopicsResponse](request)
    val result = response.data.responses.asScala.head
    assertNull(result.name, "passthrough rows keep null name")
    assertEquals(Errors.UNKNOWN_TOPIC_ID.code, result.errorCode)
    assertNotNull(result.errorMessage)
    assertFalse(result.errorMessage.contains("acme.orders"),
      s"physical topic name must be scrubbed from passthrough errorMessage: ${result.errorMessage}")
    assertTrue(result.errorMessage.contains("orders"),
      s"logical name should remain visible to the tenant: ${result.errorMessage}")
  }

  @Test
  def testDeleteTopicsTenantBoundaryViolationByIdReturnsUnknownTopicId(): Unit = {
    // Tenant submits delete-by-id for a UUID that the broker can resolve to a
    // foreign tenant's physical topic ("beta.orders"). The broker MUST pre-
    // reject without forwarding — leaving the decision to the controller would
    // let a tenant delete arbitrary topics by id. Unknown UUIDs and foreign
    // UUIDs share the same response shape (null name, UNKNOWN_TOPIC_ID) so
    // the response cannot be used to probe the existence of foreign topics.
    val foreignId = Uuid.randomUuid()
    addTopicToMetadataCache("beta.orders", numPartitions = 1, topicId = foreignId)

    val deleteRequest = new DeleteTopicsRequest.Builder(new DeleteTopicsRequestData()
      .setTopics(util.Arrays.asList(new DeleteTopicsRequestData.DeleteTopicState().setTopicId(foreignId)))
      .setTimeoutMs(5000)).build()
    val request = buildRequest(
      deleteRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDeleteTopicsRequest(request)

    // The request never reaches the controller — every UUID is foreign.
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())

    val response = verifyNoThrottling[DeleteTopicsResponse](request)
    val result = response.data.responses.asScala.head
    assertNull(result.name, "foreign tenant topic name must be redacted from the response")
    assertEquals(foreignId, result.topicId)
    assertEquals(Errors.UNKNOWN_TOPIC_ID.code, result.errorCode,
      "cross-tenant delete-by-id must surface UNKNOWN_TOPIC_ID")
  }

  @Test
  def testDeleteTopicsPrivilegedCallerOnTenantBoundListenerIsRejected(): Unit = {
    val deleteRequest = new DeleteTopicsRequest.Builder(new DeleteTopicsRequestData()
      .setTopics(util.Arrays.asList(new DeleteTopicsRequestData.DeleteTopicState().setName("orders")))
      .setTimeoutMs(5000)).build()
    val request = buildRequest(
      deleteRequest,
      listenerName = TENANT_LISTENER,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDeleteTopicsRequest(request)

    val response = verifyNoThrottling[DeleteTopicsResponse](request)
    val result = response.data.responses.asScala.head
    assertEquals("orders", result.name, "rejection response must carry the logical name")
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, result.errorCode,
      "privileged caller on tenant listener without tenant prefix must be refused")

    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testDeleteTopicsNonTenantRequestUnchangedWhenNoBinding(): Unit = {
    val deleteRequest = new DeleteTopicsRequest.Builder(new DeleteTopicsRequestData()
      .setTopics(util.Arrays.asList(new DeleteTopicsRequestData.DeleteTopicState().setName("plain-topic")))
      .setTimeoutMs(5000)).build()
    val request = buildRequest(deleteRequest)

    kafkaApis = createKafkaApis()
    kafkaApis.handleDeleteTopicsRequest(request)

    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      any[Option[AbstractResponse] => Unit]())
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testNonV1ApiFromTenantPrincipalIsRefusedAtDispatch(): Unit = {
    // PROMPT.md fixes v1 to {Produce, Fetch, Metadata, CreateTopics, DeleteTopics,
    // ListTopics} plus the APIs admitted in later phases (consumer-coordination,
    // ListOffsets, DeleteRecords). A tenant principal calling any API still
    // outside that allow-list (here DescribeGroups) has no tenant-aware handler
    // to rewrite their request — passing through would leak physical names or
    // pollute another tenant's namespace. The dispatch-level gate refuses the
    // request without ever entering the handler.
    val describeGroupsRequest = new DescribeGroupsRequest.Builder(
      new DescribeGroupsRequestData().setGroups(util.Arrays.asList("any-group"))).build()
    val request = buildRequest(describeGroupsRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[DescribeGroupsResponse](request)
    val errorCodes = response.data.groups.asScala.map(_.errorCode)
    assertTrue(errorCodes.nonEmpty, "expected at least one group in the synthesized error response")
    assertTrue(errorCodes.forall(_ == Errors.CLUSTER_AUTHORIZATION_FAILED.code),
      "tenant principal calling a non-v1 API must be refused at dispatch with CLUSTER_AUTHORIZATION_FAILED " +
      "(not TOPIC_AUTHORIZATION_FAILED — the refusal is a capability decision, not a topic-resource decision; " +
      "see KafkaApis dispatch comment)")
    verify(groupCoordinator, never()).describeGroups(any[RequestContext](), any[util.List[String]]())
  }

  @Test
  def testApiVersionsTenantFiltersResponseToAllowedSurface(): Unit = {
    // A tenant-bound client (or anyone connecting to a tenant-bound listener
    // pre-auth) must only see APIs in TENANT_ALLOWED_APIS. Advertising the
    // full broker surface leaks capability fingerprint AND points honest
    // tenants at APIs the dispatch gate will refuse — wasting a round-trip
    // and producing noisy errors.
    val apiVersionsRequest = new ApiVersionsRequest.Builder().build()
    val request = buildRequest(
      apiVersionsRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleApiVersionsRequest(request)

    val response = verifyNoThrottling[ApiVersionsResponse](request)
    val advertised = response.data.apiKeys.asScala.map(v => ApiKeys.forId(v.apiKey)).toSet
    assertEquals(KafkaApis.TENANT_ALLOWED_APIS, advertised,
      "tenant ApiVersions response must advertise exactly the allow-list, not the full broker surface")
  }

  @Test
  def testApiVersionsTenantListenerPreAuthAlsoFiltersResponse(): Unit = {
    // Pre-SASL handshake, principal is ANONYMOUS but the listener is already
    // tenant-bound. The filter must fire on listener binding alone — a tenant
    // client connecting to TENANT_LISTENER gets a filtered surface BEFORE its
    // SASL handshake completes, so the negotiation itself only references the
    // APIs it is allowed to invoke. Mirrors TenantContext.effectiveTenant
    // semantics where the listener owns the binding when the principal is
    // plain.
    val apiVersionsRequest = new ApiVersionsRequest.Builder().build()
    val request = buildRequest(
      apiVersionsRequest,
      listenerName = TENANT_LISTENER,
      principal = KafkaPrincipal.ANONYMOUS)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleApiVersionsRequest(request)

    val response = verifyNoThrottling[ApiVersionsResponse](request)
    val advertised = response.data.apiKeys.asScala.map(v => ApiKeys.forId(v.apiKey)).toSet
    assertEquals(KafkaApis.TENANT_ALLOWED_APIS, advertised,
      "pre-auth ApiVersions on a tenant listener must be filtered to the allow-list")
    assertTrue(advertised.contains(ApiKeys.SASL_HANDSHAKE),
      "SASL_HANDSHAKE must remain advertised so the client can complete the handshake")
    assertTrue(advertised.contains(ApiKeys.SASL_AUTHENTICATE),
      "SASL_AUTHENTICATE must remain advertised so the client can complete the handshake")
  }

  @Test
  def testApiVersionsNonTenantListenerReturnsFullSurface(): Unit = {
    // Cluster-wide (admin) listener with no tenant binding: the response must
    // advertise the full broker surface unchanged. This is the existing
    // behaviour the multi-tenancy change must not regress.
    val apiVersionsRequest = new ApiVersionsRequest.Builder().build()
    val request = buildRequest(apiVersionsRequest) // default: cluster-wide listener, "Alice"

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleApiVersionsRequest(request)

    val response = verifyNoThrottling[ApiVersionsResponse](request)
    val advertised = response.data.apiKeys.asScala.map(v => ApiKeys.forId(v.apiKey)).toSet
    // Sanity: the unfiltered surface is a strict superset of the allow-list
    // AND includes APIs we know the tenant filter would strip.
    assertTrue(KafkaApis.TENANT_ALLOWED_APIS.subsetOf(advertised),
      "unfiltered response must include every tenant-allowed API")
    assertTrue(advertised.contains(ApiKeys.DESCRIBE_GROUPS),
      "unfiltered response must include DESCRIBE_GROUPS (not in TENANT_ALLOWED_APIS)")
  }

  // ---------------------------------------------------------------------------
  // Reserved-physical-form guard — applies to every v1 surface
  //
  // A tenant submitting a logical name that already begins with its own
  // physical prefix (e.g. tenant acme asking for "acme.orders") is either
  // confused or trying to break out of its namespace. Rewriting would
  // double-prefix into "acme.acme.orders" — auto-created on Produce/Metadata
  // and silently materialised by CreateTopics. Each handler refuses such
  // entries up front with the wire name preserved.
  // ---------------------------------------------------------------------------

  @Test
  def testProduceTenantRejectsReservedPhysicalFormLogicalName(): Unit = {
    // Tenant acme produces to "acme.orders" — a name that would double-prefix
    // to physical "acme.acme.orders". The broker must refuse with
    // INVALID_TOPIC_EXCEPTION carrying the wire name; replicaManager must not
    // be invoked for this topic.
    val produceRequest = buildSingleTopicProduceRequest("acme.orders")
    val request = buildRequest(
      produceRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[ProduceResponse](request)
    assertEquals(1, response.data.responses.size)
    val topicResp = response.data.responses.asScala.head
    assertEquals("acme.orders", topicResp.name,
      "rejection must keep the wire name; toLogical would have silently stripped the prefix")
    val partitionResp = topicResp.partitionResponses.asScala.head
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION,
      Errors.forCode(partitionResp.errorCode),
      "double-prefix produce must be refused with INVALID_TOPIC_EXCEPTION")
    verify(replicaManager, never()).handleProduceAppend(
      anyLong, anyShort, anyBoolean, any(), any(), any(), any(), any(), any(), any())
  }

  @Test
  def testProduceTenantRejectsReservedPhysicalFormLogicalNameWithAcksZeroClosesConnection(): Unit = {
    // acks=0 path: reserved-form rejections are emitted into invalidLogicalTopicResponses,
    // a side channel that bypasses replicaManager and therefore is absent from
    // physicalResponseStatus. Without an explicit signal the standard ack=0
    // "errors → close" branch falls through to sendNoOpResponseExemptThrottle
    // and the client never learns its produce was refused — silent data loss.
    // The acks=0 close-on-error decision must observe these rejections too.
    val produceRequest = buildSingleTopicProduceRequest("acme.orders", acks = 0.toShort)
    val request = buildRequest(
      produceRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    verify(requestChannel).closeConnection(
      ArgumentMatchers.eq(request),
      any[java.util.Map[Errors, Integer]]())
    verify(requestChannel, never()).sendResponse(any(), any(), any())
    verify(replicaManager, never()).handleProduceAppend(
      anyLong, anyShort, anyBoolean(), any(), any(), any(), any(), any(), any(), any())
  }

  @Test
  def testProduceTenantRefusedOnConsumerOffsetsInternalTopic(): Unit = {
    // #91 defence-in-depth: a tenant must NEVER Produce to an internal topic
    // (__consumer_offsets, __transaction_state, __share_group_state).
    // TenantNamespace.toPhysical passes internal names through unchanged
    // because Topic.isInternal short-circuits the prefix rewrite — without
    // an explicit refusal here, a misconfigured wildcard `WRITE Topic:*`
    // grant on the tenant principal would let the tenant corrupt the
    // cluster's offsets log directly. TOPIC_AUTHORIZATION_FAILED keeps the
    // wire shape indistinguishable from a regular authz refusal so the
    // tenant cannot probe ACL configuration through the error category.
    val produceRequest = buildSingleTopicProduceRequest(Topic.GROUP_METADATA_TOPIC_NAME)
    val request = buildRequest(
      produceRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[ProduceResponse](request)
    val topicResp = response.data.responses.asScala.head
    assertEquals(Topic.GROUP_METADATA_TOPIC_NAME, topicResp.name,
      "rejection echoes the wire name unchanged (internal topics are never prefixed)")
    val partitionResp = topicResp.partitionResponses.asScala.head
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED,
      Errors.forCode(partitionResp.errorCode),
      "tenant Produce to internal topic must be refused with TOPIC_AUTHORIZATION_FAILED")
    verify(replicaManager, never()).handleProduceAppend(
      anyLong, anyShort, anyBoolean, any(), any(), any(), any(), any(), any(), any())
  }

  @Test
  def testProduceTenantRefusedOnTransactionStateInternalTopic(): Unit = {
    // #91 sibling: same defence-in-depth for __transaction_state.
    val produceRequest = buildSingleTopicProduceRequest(Topic.TRANSACTION_STATE_TOPIC_NAME)
    val request = buildRequest(
      produceRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[ProduceResponse](request)
    val partitionResp = response.data.responses.asScala.head.partitionResponses.asScala.head
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED,
      Errors.forCode(partitionResp.errorCode))
    verify(replicaManager, never()).handleProduceAppend(
      anyLong, anyShort, anyBoolean, any(), any(), any(), any(), any(), any(), any())
  }

  @Test
  def testProduceTenantRefusedOnShareGroupStateInternalTopic(): Unit = {
    // #91 sibling: same defence-in-depth for __share_group_state (KIP-932).
    val produceRequest = buildSingleTopicProduceRequest(Topic.SHARE_GROUP_STATE_TOPIC_NAME)
    val request = buildRequest(
      produceRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[ProduceResponse](request)
    val partitionResp = response.data.responses.asScala.head.partitionResponses.asScala.head
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED,
      Errors.forCode(partitionResp.errorCode))
    verify(replicaManager, never()).handleProduceAppend(
      anyLong, anyShort, anyBoolean, any(), any(), any(), any(), any(), any(), any())
  }

  @Test
  def testCreateTopicsTenantRejectsReservedPhysicalFormLogicalName(): Unit = {
    // CreateTopics is the loudest auto-pollution vector — the controller would
    // happily materialise "acme.acme.orders" for tenant acme. The broker must
    // refuse the entry, never forward, and leave the rest of the batch intact.
    val createRequest = new CreateTopicsRequest.Builder(new CreateTopicsRequestData()
      .setTopics(new CreateTopicsRequestData.CreatableTopicCollection(
        Collections.singleton(new CreateTopicsRequestData.CreatableTopic()
          .setName("acme.orders").setNumPartitions(1).setReplicationFactor(1.toShort)).iterator)))
      .build()
    val request = buildRequest(
      createRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleCreateTopicsRequest(request)

    val response = verifyNoThrottling[CreateTopicsResponse](request)
    val result = response.data.topics.asScala.head
    assertEquals("acme.orders", result.name,
      "rejection must keep the wire name the client sent")
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, result.errorCode,
      "double-prefix create must be refused with INVALID_TOPIC_EXCEPTION")
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testCreateTopicsTenantRejectsOverlongLogicalNameWithoutLeakingPhysicalForm(): Unit = {
    // Kafka caps topic names at 249 chars. A 245-char logical name for tenant
    // acme would inflate to "acme." + 245 = 250 chars on the wire to the
    // controller, which would reject it — but the controller's rejection
    // string would embed the physical name, leaking the tenant prefix. The
    // broker must refuse the entry up front with INVALID_TOPIC_EXCEPTION and
    // the LOGICAL name in the error message, and not forward.
    val tooLongLogical = "a" * 245
    val createRequest = new CreateTopicsRequest.Builder(new CreateTopicsRequestData()
      .setTopics(new CreateTopicsRequestData.CreatableTopicCollection(
        Collections.singleton(new CreateTopicsRequestData.CreatableTopic()
          .setName(tooLongLogical).setNumPartitions(1).setReplicationFactor(1.toShort)).iterator)))
      .build()
    val request = buildRequest(
      createRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleCreateTopicsRequest(request)

    val response = verifyNoThrottling[CreateTopicsResponse](request)
    val result = response.data.topics.asScala.head
    assertEquals(tooLongLogical, result.name,
      "rejection must echo the LOGICAL name the client sent, never the physical form")
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, result.errorCode)
    assertNotNull(result.errorMessage)
    assertTrue(result.errorMessage.contains(tooLongLogical),
      "error message must quote the logical name back to the tenant")
    assertFalse(result.errorMessage.contains("acme." + tooLongLogical),
      "error message must not embed the physical form: " + result.errorMessage)
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testCreateTopicsTenantRejectsLogicalNamesFailingKafkaTopicValidation(): Unit = {
    // Logical names that Kafka's own Topic.validate refuses ("", ".", "..",
    // illegal chars) would, if rewritten, produce physical names like `acme.`,
    // `acme..`, `acme...`, `acme.a/b` — names the controller itself rejects.
    // The broker must refuse the entry up front with INVALID_TOPIC_EXCEPTION
    // and the LOGICAL name preserved, and not forward.
    val invalidNames = util.Arrays.asList(
      new CreateTopicsRequestData.CreatableTopic()
        .setName(".").setNumPartitions(1).setReplicationFactor(1.toShort),
      new CreateTopicsRequestData.CreatableTopic()
        .setName("..").setNumPartitions(1).setReplicationFactor(1.toShort),
      new CreateTopicsRequestData.CreatableTopic()
        .setName("a/b").setNumPartitions(1).setReplicationFactor(1.toShort))
    val createRequest = new CreateTopicsRequest.Builder(new CreateTopicsRequestData()
      .setTopics(new CreateTopicsRequestData.CreatableTopicCollection(invalidNames.iterator)))
      .build()
    val request = buildRequest(
      createRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleCreateTopicsRequest(request)

    val response = verifyNoThrottling[CreateTopicsResponse](request)
    val byName = response.data.topics.asScala.map(t => t.name -> t).toMap
    Set(".", "..", "a/b").foreach { name =>
      val r = byName(name)
      assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, r.errorCode,
        s"invalid logical name '$name' must be refused with INVALID_TOPIC_EXCEPTION")
      assertNotNull(r.errorMessage)
      assertTrue(r.errorMessage.contains(name),
        s"error message must quote the logical name '$name' back: ${r.errorMessage}")
      assertFalse(r.errorMessage.contains("acme." + name),
        s"error message must not embed the physical form 'acme.$name': ${r.errorMessage}")
    }
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testCreateTopicsClusterWideListenerRejectsTenantPrefixedNames(): Unit = {
    // Outside-in pollution: a super-user on the cluster-wide (non-tenant)
    // listener could otherwise CreateTopics("acme.foo") literally; tenant acme
    // on its own listener would then see `foo` in ListTopics because the broker
    // cannot tell intent apart from prefix. The broker refuses the entry at
    // the dispatch layer so the controller never materialises the topic.
    val createRequest = new CreateTopicsRequest.Builder(new CreateTopicsRequestData()
      .setTopics(new CreateTopicsRequestData.CreatableTopicCollection(
        Collections.singleton(new CreateTopicsRequestData.CreatableTopic()
          .setName("acme.foo").setNumPartitions(1).setReplicationFactor(1.toShort)).iterator)))
      .build()
    val request = buildRequest(createRequest)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleCreateTopicsRequest(request)

    val response = verifyNoThrottling[CreateTopicsResponse](request)
    val result = response.data.topics.asScala.head
    assertEquals("acme.foo", result.name,
      "rejection must keep the wire name the cluster-wide caller sent")
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, result.errorCode,
      "tenant-prefixed name from a non-tenant listener must be refused")
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testCreateTopicsClusterWideListenerMixesAllowedAndRejectedEntries(): Unit = {
    // Mixed batch: one tenant-prefixed name (rejected), one neutral name
    // (forwarded). The pattern mirrors the per-tenant reserved-form guard:
    // refuse the polluting entry, forward the rest, merge responses on return.
    val createRequest = new CreateTopicsRequest.Builder(new CreateTopicsRequestData()
      .setTopics(new CreateTopicsRequestData.CreatableTopicCollection(util.Arrays.asList(
        new CreateTopicsRequestData.CreatableTopic()
          .setName("acme.foo").setNumPartitions(1).setReplicationFactor(1.toShort),
        new CreateTopicsRequestData.CreatableTopic()
          .setName("plain-topic").setNumPartitions(1).setReplicationFactor(1.toShort)).iterator)))
      .build()
    val request = buildRequest(createRequest)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleCreateTopicsRequest(request)

    val bodyCaptor: ArgumentCaptor[AbstractRequest] = ArgumentCaptor.forClass(classOf[AbstractRequest])
    val callbackCaptor: ArgumentCaptor[Option[AbstractResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])
    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      bodyCaptor.capture(),
      callbackCaptor.capture())
    val forwarded = bodyCaptor.getValue.asInstanceOf[CreateTopicsRequest]
    assertEquals(Set("plain-topic"),
      forwarded.data.topics.asScala.map(_.name).toSet,
      "only the non-polluting entry must reach the controller")

    val controllerResponse = new CreateTopicsResponse(new CreateTopicsResponseData()
      .setTopics(new CreateTopicsResponseData.CreatableTopicResultCollection(
        Collections.singleton(new CreateTopicsResponseData.CreatableTopicResult()
          .setName("plain-topic").setErrorCode(Errors.NONE.code)).iterator)))
    val callback = callbackCaptor.getValue
    callback(Some(controllerResponse))

    val response = verifyNoThrottling[CreateTopicsResponse](request)
    val byName = response.data.topics.asScala.map(t => t.name -> t.errorCode).toMap
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, byName("acme.foo"),
      "tenant-prefixed entry must be rejected at the broker")
    assertEquals(Errors.NONE.code, byName("plain-topic"),
      "non-polluting entry must surface the controller's outcome unchanged")
  }

  @Test
  def testCreateTopicsClusterWideListenerForwardsTenantLookingNamesWhenNoTenantsConfigured(): Unit = {
    // Pollution guard is gated on TenantConfig.allTenants. With no tenants
    // configured, the broker behaves as a stock single-tenant cluster and the
    // request is forwarded verbatim — including topics whose names happen to
    // contain a dot.
    val createRequest = new CreateTopicsRequest.Builder(new CreateTopicsRequestData()
      .setTopics(new CreateTopicsRequestData.CreatableTopicCollection(
        Collections.singleton(new CreateTopicsRequestData.CreatableTopic()
          .setName("acme.foo").setNumPartitions(1).setReplicationFactor(1.toShort)).iterator)))
      .build()
    val request = buildRequest(createRequest)

    kafkaApis = createKafkaApis()
    kafkaApis.handleCreateTopicsRequest(request)

    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      any[Option[AbstractResponse] => Unit]())
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testCreateTopicsClusterWideListenerRejectsHeterogeneousBrokerForeignTenantName(): Unit = {
    // F7: structural shape check. Broker is bound to tenant `acme` LOCALLY but
    // a different broker (in a heterogeneous multi-broker deployment) may be
    // bound to `gamma`. A cluster-wide caller naming `gamma.foo` on THIS broker
    // must still be rejected — the previous iteration over
    // `tenantConfig.allTenants` missed this because `gamma` is not in the
    // local snapshot. With the structural check, `gamma` satisfies
    // TenantNamespace.validateTenantId → reserved → INVALID_TOPIC_EXCEPTION.
    val createRequest = new CreateTopicsRequest.Builder(new CreateTopicsRequestData()
      .setTopics(new CreateTopicsRequestData.CreatableTopicCollection(
        Collections.singleton(new CreateTopicsRequestData.CreatableTopic()
          .setName("gamma.foo").setNumPartitions(1).setReplicationFactor(1.toShort)).iterator)))
      .build()
    val request = buildRequest(createRequest)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleCreateTopicsRequest(request)

    val response = verifyNoThrottling[CreateTopicsResponse](request)
    val byName = response.data.topics.asScala.map(t => t.name -> t.errorCode).toMap
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, byName("gamma.foo"),
      "structurally tenant-shaped name (foreign to local broker config) must be rejected")
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testAlterConfigsClusterWideListenerRejectsHeterogeneousBrokerForeignTenantName(): Unit = {
    // F7 sibling: same heterogeneous-broker bug for AlterConfigs. The local
    // broker has `acme` bound; a cluster-wide caller alters configs on
    // `gamma.foo` (gamma bound on a different broker). Structural check
    // rejects without forwarding.
    val resource = new ConfigResource(ConfigResource.Type.TOPIC, "gamma.foo")
    val configEntries = new util.ArrayList[AlterConfigsRequest.ConfigEntry]()
    configEntries.add(new AlterConfigsRequest.ConfigEntry("retention.ms", "60000"))
    val configs = Map(resource -> new AlterConfigsRequest.Config(configEntries)).asJava
    val alterRequest = new AlterConfigsRequest.Builder(configs, false).build()
    val request = buildRequest(alterRequest)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAlterConfigsRequest(request)

    val response = verifyNoThrottling[AlterConfigsResponse](request)
    val byName = response.data.responses.asScala.map(r => r.resourceName -> r).toMap
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, byName("gamma.foo").errorCode,
      "heterogeneous-broker foreign-tenant name must be refused at structural shape level")
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testCreateTopicsClusterWideListenerForwardsSingleUnderscoreNameWhenTenantsConfigured(): Unit = {
    // F7 negative case: `_confluent-metrics` and similar single-`_` prefix
    // names are an operator convention (Confluent platform topics, Connect
    // connector configs) and must NOT be classified as a tenant namespace
    // even when tenants are bound. The structural check exits on
    // `name.startsWith("_")` before reaching the dot-shape rule.
    val createRequest = new CreateTopicsRequest.Builder(new CreateTopicsRequestData()
      .setTopics(new CreateTopicsRequestData.CreatableTopicCollection(
        Collections.singleton(new CreateTopicsRequestData.CreatableTopic()
          .setName("_confluent-metrics").setNumPartitions(1).setReplicationFactor(1.toShort)).iterator)))
      .build()
    val request = buildRequest(createRequest)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleCreateTopicsRequest(request)

    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testCreateTopicsClusterWideListenerPassesInternalTopicsThrough(): Unit = {
    // Internal topics are never tenant-namespaced. Even with tenants
    // configured, `__consumer_offsets` (etc.) must not be misclassified as
    // pollution and must reach the controller via the standard path.
    val createRequest = new CreateTopicsRequest.Builder(new CreateTopicsRequestData()
      .setTopics(new CreateTopicsRequestData.CreatableTopicCollection(
        Collections.singleton(new CreateTopicsRequestData.CreatableTopic()
          .setName("__consumer_offsets").setNumPartitions(1).setReplicationFactor(1.toShort)).iterator)))
      .build()
    val request = buildRequest(createRequest)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleCreateTopicsRequest(request)

    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      any[Option[AbstractResponse] => Unit]())
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testAlterConfigsClusterWideListenerRejectsTenantPrefixedTopic(): Unit = {
    // Outside-in pollution: a super-user on the cluster-wide listener could
    // otherwise AlterConfigs(TOPIC, "acme.foo", retention.ms=...) and silently
    // mutate tenant acme's storage. The broker refuses the entry before
    // forwarding so the controller never sees `acme.foo`. Mirrors the
    // CreateTopics outside-in guard.
    val resource = new ConfigResource(ConfigResource.Type.TOPIC, "acme.foo")
    val configEntries = new util.ArrayList[AlterConfigsRequest.ConfigEntry]()
    configEntries.add(new AlterConfigsRequest.ConfigEntry("retention.ms", "60000"))
    val configs = Map(resource -> new AlterConfigsRequest.Config(configEntries)).asJava
    val alterRequest = new AlterConfigsRequest.Builder(configs, false).build()
    val request = buildRequest(alterRequest)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAlterConfigsRequest(request)

    val response = verifyNoThrottling[AlterConfigsResponse](request)
    val byName = response.data.responses.asScala.map(r => r.resourceName -> r).toMap
    assertEquals(1, byName.size, "single resource in / single response out")
    val rejected = byName("acme.foo")
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, rejected.errorCode,
      "tenant-prefixed topic must be refused on cluster-wide listener")
    assertEquals(ConfigResource.Type.TOPIC.id, rejected.resourceType)
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testAlterConfigsClusterWideListenerMixesAllowedAndRejectedEntries(): Unit = {
    // Mixed batch: one tenant-prefixed TOPIC (rejected), one neutral TOPIC
    // (forwarded). The pollution guard splits the batch, the neutral entry
    // reaches the controller, both surface in the merged response.
    val polluting = new ConfigResource(ConfigResource.Type.TOPIC, "acme.foo")
    val neutral = new ConfigResource(ConfigResource.Type.TOPIC, "plain-topic")
    val configEntries = new util.ArrayList[AlterConfigsRequest.ConfigEntry]()
    configEntries.add(new AlterConfigsRequest.ConfigEntry("retention.ms", "60000"))
    val configs = Map(
      polluting -> new AlterConfigsRequest.Config(configEntries),
      neutral -> new AlterConfigsRequest.Config(configEntries)).asJava
    val alterRequest = new AlterConfigsRequest.Builder(configs, false).build()
    val request = buildRequest(alterRequest)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAlterConfigsRequest(request)

    val bodyCaptor: ArgumentCaptor[AbstractRequest] = ArgumentCaptor.forClass(classOf[AbstractRequest])
    val callbackCaptor: ArgumentCaptor[Option[AbstractResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])
    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      bodyCaptor.capture(),
      callbackCaptor.capture())
    val forwarded = bodyCaptor.getValue.asInstanceOf[AlterConfigsRequest]
    val forwardedNames = forwarded.data.resources.asScala.map(_.resourceName).toSet
    assertEquals(Set("plain-topic"), forwardedNames,
      "only the non-polluting entry must reach the controller")

    val controllerResponse = new AlterConfigsResponseData().setResponses(asList(
      new LAlterConfigsResourceResponse()
        .setErrorCode(Errors.NONE.code)
        .setResourceName("plain-topic")
        .setResourceType(ConfigResource.Type.TOPIC.id)))
    val alterCallback = callbackCaptor.getValue
    alterCallback(Some(new AlterConfigsResponse(controllerResponse)))

    val response = verifyNoThrottling[AlterConfigsResponse](request)
    val byName = response.data.responses.asScala.map(r => r.resourceName -> r.errorCode).toMap
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, byName("acme.foo"),
      "polluting entry rejected at the broker")
    assertEquals(Errors.NONE.code, byName("plain-topic"),
      "non-polluting entry surfaces controller's outcome unchanged")
  }

  @Test
  def testAlterConfigsClusterWideListenerLeavesNonTopicResourcesAlone(): Unit = {
    // The guard only targets TOPIC-typed resources. A CLIENT_METRICS
    // subscription whose name happens to share a tenant prefix is NOT topic
    // pollution and must be forwarded verbatim — the tenant namespace lives in
    // topics + coordinator records, not in metric subscriptions.
    val resource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, "acme.metrics")
    val configEntries = new util.ArrayList[AlterConfigsRequest.ConfigEntry]()
    configEntries.add(new AlterConfigsRequest.ConfigEntry("metrics", "x.y"))
    val configs = Map(resource -> new AlterConfigsRequest.Config(configEntries)).asJava
    val alterRequest = new AlterConfigsRequest.Builder(configs, false).build()
    val request = buildRequest(alterRequest)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAlterConfigsRequest(request)

    verify(forwardingManager, times(1)).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testAlterConfigsClusterWideListenerForwardsTenantLookingNamesWhenNoTenantsConfigured(): Unit = {
    // Guard is gated on TenantConfig.allTenants. With no tenants configured the
    // broker behaves as a stock cluster — `acme.foo` is just a topic name, not
    // a pollution signal.
    val resource = new ConfigResource(ConfigResource.Type.TOPIC, "acme.foo")
    val configEntries = new util.ArrayList[AlterConfigsRequest.ConfigEntry]()
    configEntries.add(new AlterConfigsRequest.ConfigEntry("retention.ms", "60000"))
    val configs = Map(resource -> new AlterConfigsRequest.Config(configEntries)).asJava
    val alterRequest = new AlterConfigsRequest.Builder(configs, false).build()
    val request = buildRequest(alterRequest)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis()
    kafkaApis.handleAlterConfigsRequest(request)

    verify(forwardingManager, times(1)).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testIncrementalAlterConfigsClusterWideListenerRejectsTenantPrefixedTopic(): Unit = {
    // Same outside-in pollution as the legacy alter — incremental form lets a
    // super-user flip `cleanup.policy=delete` on a compacted tenant log, with
    // the same level of damage. Reject at the broker before forwarding.
    val resource = new ConfigResource(ConfigResource.Type.TOPIC, "acme.foo")
    val incrementalRequest = getIncrementalAlterConfigRequestBuilder(
      Seq(resource), "retention.ms", "60000").build()
    val request = buildRequest(incrementalRequest)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleIncrementalAlterConfigsRequest(request)

    val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
    val byName = response.data.responses.asScala.map(r => r.resourceName -> r).toMap
    assertEquals(1, byName.size)
    val rejected = byName("acme.foo")
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, rejected.errorCode,
      "tenant-prefixed topic must be refused on cluster-wide listener")
    assertEquals(ConfigResource.Type.TOPIC.id, rejected.resourceType)
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testIncrementalAlterConfigsClusterWideListenerMixesAllowedAndRejectedEntries(): Unit = {
    val polluting = new ConfigResource(ConfigResource.Type.TOPIC, "acme.foo")
    val neutral = new ConfigResource(ConfigResource.Type.TOPIC, "plain-topic")
    val incrementalRequest = getIncrementalAlterConfigRequestBuilder(
      Seq(polluting, neutral), "retention.ms", "60000").build()
    val request = buildRequest(incrementalRequest)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleIncrementalAlterConfigsRequest(request)

    val bodyCaptor: ArgumentCaptor[AbstractRequest] = ArgumentCaptor.forClass(classOf[AbstractRequest])
    val callbackCaptor: ArgumentCaptor[Option[AbstractResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])
    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      bodyCaptor.capture(),
      callbackCaptor.capture())
    val forwarded = bodyCaptor.getValue.asInstanceOf[IncrementalAlterConfigsRequest]
    val forwardedNames = forwarded.data.resources.asScala.map(_.resourceName).toSet
    assertEquals(Set("plain-topic"), forwardedNames,
      "only the non-polluting entry must reach the controller")

    val controllerResponse = new IncrementalAlterConfigsResponseData().setResponses(asList(
      new IAlterConfigsResourceResponse()
        .setErrorCode(Errors.NONE.code)
        .setResourceName("plain-topic")
        .setResourceType(ConfigResource.Type.TOPIC.id)))
    val incrementalCallback = callbackCaptor.getValue
    incrementalCallback(Some(new IncrementalAlterConfigsResponse(controllerResponse)))

    val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
    val byName = response.data.responses.asScala.map(r => r.resourceName -> r.errorCode).toMap
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, byName("acme.foo"))
    assertEquals(Errors.NONE.code, byName("plain-topic"))
  }

  @Test
  def testAlterConfigsClusterWideListenerRejectsTenantPrefixedGroup(): Unit = {
    // #139: GROUP defence-in-depth. A privileged caller on the cluster-wide
    // listener could otherwise legacy-AlterConfigs(GROUP, "__tenant_acme.foo",
    // session.timeout.ms=2147483647) and silently degrade tenant acme's
    // rebalance behaviour. ControllerApis #126 already refuses this on the
    // controller side; the broker mirror short-circuits before forwarding so
    // the merged response carries the same wire shape as an ACL refusal —
    // GROUP_AUTHORIZATION_FAILED, NULL errorMessage (no presence oracle for
    // tenant-acme's groups).
    val resource = new ConfigResource(ConfigResource.Type.GROUP, "__tenant_acme.foo")
    val configEntries = new util.ArrayList[AlterConfigsRequest.ConfigEntry]()
    configEntries.add(new AlterConfigsRequest.ConfigEntry("session.timeout.ms", "60000"))
    val configs = Map(resource -> new AlterConfigsRequest.Config(configEntries)).asJava
    val alterRequest = new AlterConfigsRequest.Builder(configs, false).build()
    val request = buildRequest(alterRequest)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAlterConfigsRequest(request)

    val response = verifyNoThrottling[AlterConfigsResponse](request)
    val byName = response.data.responses.asScala.map(r => r.resourceName -> r).toMap
    assertEquals(1, byName.size, "single resource in / single response out")
    val rejected = byName("__tenant_acme.foo")
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, rejected.errorCode,
      "tenant-namespace group must be refused on cluster-wide listener")
    assertEquals(ConfigResource.Type.GROUP.id, rejected.resourceType)
    assertNull(rejected.errorMessage,
      "errorMessage must be null so refusal is indistinguishable from a plain ACL deny — no presence oracle")
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testIncrementalAlterConfigsClusterWideListenerRejectsTenantPrefixedGroup(): Unit = {
    // #139: same defence as the legacy alter, exercised on the incremental
    // path. Flipping group session.timeout via incremental ops is the same
    // tenant-config pollution; refuse before forwarding.
    val resource = new ConfigResource(ConfigResource.Type.GROUP, "__tenant_acme.foo")
    val incrementalRequest = getIncrementalAlterConfigRequestBuilder(
      Seq(resource), "session.timeout.ms", "60000").build()
    val request = buildRequest(incrementalRequest)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleIncrementalAlterConfigsRequest(request)

    val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
    val byName = response.data.responses.asScala.map(r => r.resourceName -> r).toMap
    assertEquals(1, byName.size)
    val rejected = byName("__tenant_acme.foo")
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, rejected.errorCode)
    assertEquals(ConfigResource.Type.GROUP.id, rejected.resourceType)
    assertNull(rejected.errorMessage,
      "errorMessage must be null so refusal is indistinguishable from a plain ACL deny")
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testIncrementalAlterConfigsClusterWideListenerMixedTopicAndGroupPollution(): Unit = {
    // #139: end-to-end mixed-batch with one polluting TOPIC, one polluting
    // GROUP, and one legitimate TOPIC. Confirms the two pollution arms compose
    // (each refuses with its own error code) and the neutral resource still
    // forwards. Pins the wire shape that an admin tool would observe on a
    // single round-trip.
    val pollutingTopic = new ConfigResource(ConfigResource.Type.TOPIC, "acme.foo")
    val pollutingGroup = new ConfigResource(ConfigResource.Type.GROUP, "__tenant_acme.bar")
    val neutralTopic = new ConfigResource(ConfigResource.Type.TOPIC, "plain-topic")
    val incrementalRequest = getIncrementalAlterConfigRequestBuilder(
      Seq(pollutingTopic, pollutingGroup, neutralTopic), "retention.ms", "60000").build()
    val request = buildRequest(incrementalRequest)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleIncrementalAlterConfigsRequest(request)

    val bodyCaptor: ArgumentCaptor[AbstractRequest] = ArgumentCaptor.forClass(classOf[AbstractRequest])
    val callbackCaptor: ArgumentCaptor[Option[AbstractResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])
    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      bodyCaptor.capture(),
      callbackCaptor.capture())
    val forwarded = bodyCaptor.getValue.asInstanceOf[IncrementalAlterConfigsRequest]
    val forwardedNames = forwarded.data.resources.asScala.map(r => (r.resourceName, r.resourceType)).toSet
    assertEquals(Set(("plain-topic", ConfigResource.Type.TOPIC.id)), forwardedNames,
      "only the neutral resource may reach the controller")

    val controllerResponse = new IncrementalAlterConfigsResponseData().setResponses(asList(
      new IAlterConfigsResourceResponse()
        .setErrorCode(Errors.NONE.code)
        .setResourceName("plain-topic")
        .setResourceType(ConfigResource.Type.TOPIC.id)))
    val mixedCallback = callbackCaptor.getValue
    mixedCallback(Some(new IncrementalAlterConfigsResponse(controllerResponse)))

    val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
    val byKey = response.data.responses.asScala.map(r => (r.resourceName, r.resourceType) -> r).toMap
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code,
      byKey(("acme.foo", ConfigResource.Type.TOPIC.id)).errorCode,
      "TOPIC pollution → INVALID_TOPIC_EXCEPTION")
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code,
      byKey(("__tenant_acme.bar", ConfigResource.Type.GROUP.id)).errorCode,
      "GROUP pollution → GROUP_AUTHORIZATION_FAILED")
    assertNull(byKey(("__tenant_acme.bar", ConfigResource.Type.GROUP.id)).errorMessage,
      "GROUP refusal must carry null errorMessage")
    assertEquals(Errors.NONE.code,
      byKey(("plain-topic", ConfigResource.Type.TOPIC.id)).errorCode,
      "neutral resource surfaces controller outcome")
  }

  @Test
  def testCreatePartitionsClusterWideListenerRejectsTenantPrefixedTopic(): Unit = {
    // Outside-in: a cluster-wide super-user could otherwise grow `acme.orders`
    // from 3 to 30 partitions, breaking the tenant's key-to-partition mapping
    // (and the consumer-group/transactional-id sharding that depends on it).
    // Refuse with INVALID_TOPIC_EXCEPTION before forwarding.
    val topic = new CreatePartitionsRequestData.CreatePartitionsTopic()
      .setName("acme.orders").setCount(30)
    val data = new CreatePartitionsRequestData()
      .setTimeoutMs(5000)
    data.topics().add(topic)
    val createReq = new CreatePartitionsRequest.Builder(data).build()
    val request = buildRequest(createReq)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleCreatePartitionsRequest(request)

    val response = verifyNoThrottling[CreatePartitionsResponse](request)
    val byName = response.data.results.asScala.map(r => r.name -> r).toMap
    assertEquals(1, byName.size, "single topic in / single result out")
    val rejected = byName("acme.orders")
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, rejected.errorCode,
      "tenant-prefixed topic must be refused on cluster-wide listener")
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testCreatePartitionsClusterWideListenerMixesAllowedAndRejectedEntries(): Unit = {
    val polluting = new CreatePartitionsRequestData.CreatePartitionsTopic()
      .setName("acme.orders").setCount(30)
    val neutral = new CreatePartitionsRequestData.CreatePartitionsTopic()
      .setName("plain-topic").setCount(6)
    val data = new CreatePartitionsRequestData().setTimeoutMs(5000)
    data.topics().add(polluting)
    data.topics().add(neutral)
    val createReq = new CreatePartitionsRequest.Builder(data).build()
    val request = buildRequest(createReq)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleCreatePartitionsRequest(request)

    val bodyCaptor: ArgumentCaptor[AbstractRequest] = ArgumentCaptor.forClass(classOf[AbstractRequest])
    val callbackCaptor: ArgumentCaptor[Option[AbstractResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])
    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request), bodyCaptor.capture(), callbackCaptor.capture())
    val forwarded = bodyCaptor.getValue.asInstanceOf[CreatePartitionsRequest]
    val forwardedNames = forwarded.data.topics.asScala.map(_.name).toSet
    assertEquals(Set("plain-topic"), forwardedNames,
      "only the non-polluting entry must reach the controller")

    val controllerResponse = new CreatePartitionsResponseData()
      .setResults(asList(new CreatePartitionsResponseData.CreatePartitionsTopicResult()
        .setName("plain-topic").setErrorCode(Errors.NONE.code)))
    callbackCaptor.getValue.apply(Some(new CreatePartitionsResponse(controllerResponse)))

    val response = verifyNoThrottling[CreatePartitionsResponse](request)
    val byName = response.data.results.asScala.map(r => r.name -> r.errorCode).toMap
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, byName("acme.orders"))
    assertEquals(Errors.NONE.code, byName("plain-topic"))
  }

  @Test
  def testElectLeadersClusterWideListenerRejectsTenantPrefixedTopic(): Unit = {
    // A cluster-wide admin naming `acme.orders` would force an unclean leader
    // election on a tenant partition. Refuse with INVALID_TOPIC_EXCEPTION
    // per partition, do not forward to controller.
    val builder = new ElectLeadersRequest.Builder(
      ElectionType.PREFERRED,
      util.Arrays.asList(new TopicPartition("acme.orders", 0), new TopicPartition("acme.orders", 1)),
      30000)
    val request = buildRequest(builder.build())

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleElectLeadersRequest(request)

    val response = verifyNoThrottling[ElectLeadersResponse](request)
    assertEquals(1, response.data.replicaElectionResults.size, "single topic in / single result group")
    val res = response.data.replicaElectionResults.asScala.head
    assertEquals("acme.orders", res.topic)
    val byPart = res.partitionResult.asScala.map(p => p.partitionId -> p.errorCode).toMap
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, byPart(0))
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, byPart(1))
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testElectLeadersClusterWideListenerMixesAllowedAndRejectedEntries(): Unit = {
    val builder = new ElectLeadersRequest.Builder(
      ElectionType.PREFERRED,
      util.Arrays.asList(new TopicPartition("acme.orders", 0), new TopicPartition("plain-topic", 0)),
      30000)
    val request = buildRequest(builder.build())

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleElectLeadersRequest(request)

    val bodyCaptor: ArgumentCaptor[AbstractRequest] = ArgumentCaptor.forClass(classOf[AbstractRequest])
    val callbackCaptor: ArgumentCaptor[Option[AbstractResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])
    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request), bodyCaptor.capture(), callbackCaptor.capture())
    val forwarded = bodyCaptor.getValue.asInstanceOf[ElectLeadersRequest]
    val forwardedTopics = forwarded.data.topicPartitions.asScala.map(_.topic).toSet
    assertEquals(Set("plain-topic"), forwardedTopics,
      "only the non-polluting topic must reach the controller")

    val controllerResponse = new ElectLeadersResponseData()
      .setReplicaElectionResults(asList(new ElectLeadersResponseData.ReplicaElectionResult()
        .setTopic("plain-topic")
        .setPartitionResult(asList(new ElectLeadersResponseData.PartitionResult()
          .setPartitionId(0).setErrorCode(Errors.NONE.code)))))
    callbackCaptor.getValue.apply(Some(new ElectLeadersResponse(controllerResponse)))

    val response = verifyNoThrottling[ElectLeadersResponse](request)
    val byTopic = response.data.replicaElectionResults.asScala.map(r => r.topic -> r).toMap
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code,
      byTopic("acme.orders").partitionResult.asScala.head.errorCode)
    assertEquals(Errors.NONE.code,
      byTopic("plain-topic").partitionResult.asScala.head.errorCode)
  }

  @Test
  def testAlterPartitionReassignmentsClusterWideListenerRejectsTenantPrefixedTopic(): Unit = {
    // A cluster-wide admin naming `acme.orders` could rewire replica placement
    // OR — by passing null replicas — silently cancel the tenant's existing
    // reassignments. Both vectors are refused at the broker.
    val topic = new AlterPartitionReassignmentsRequestData.ReassignableTopic().setName("acme.orders")
    topic.partitions().add(new AlterPartitionReassignmentsRequestData.ReassignablePartition()
      .setPartitionIndex(0).setReplicas(null)) // null replicas = cancel reassignment
    val data = new AlterPartitionReassignmentsRequestData().setTimeoutMs(5000)
    data.topics().add(topic)
    val req = new AlterPartitionReassignmentsRequest.Builder(data).build()
    val request = buildRequest(req)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAlterPartitionReassignmentsRequest(request)

    val response = verifyNoThrottling[AlterPartitionReassignmentsResponse](request)
    val byName = response.data.responses.asScala.map(r => r.name -> r).toMap
    val rejected = byName("acme.orders")
    val parts = rejected.partitions.asScala.map(p => p.partitionIndex -> p.errorCode).toMap
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, parts(0),
      "tenant-prefixed topic must be refused on cluster-wide listener (cancel-reassignment vector)")
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testAlterPartitionReassignmentsClusterWideListenerMixesAllowedAndRejectedEntries(): Unit = {
    val polluting = new AlterPartitionReassignmentsRequestData.ReassignableTopic().setName("acme.orders")
    polluting.partitions().add(new AlterPartitionReassignmentsRequestData.ReassignablePartition()
      .setPartitionIndex(0).setReplicas(util.Arrays.asList(1, 2)))
    val neutral = new AlterPartitionReassignmentsRequestData.ReassignableTopic().setName("plain-topic")
    neutral.partitions().add(new AlterPartitionReassignmentsRequestData.ReassignablePartition()
      .setPartitionIndex(0).setReplicas(util.Arrays.asList(1, 2)))
    val data = new AlterPartitionReassignmentsRequestData().setTimeoutMs(5000)
    data.topics().add(polluting)
    data.topics().add(neutral)
    val req = new AlterPartitionReassignmentsRequest.Builder(data).build()
    val request = buildRequest(req)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAlterPartitionReassignmentsRequest(request)

    val bodyCaptor: ArgumentCaptor[AbstractRequest] = ArgumentCaptor.forClass(classOf[AbstractRequest])
    val callbackCaptor: ArgumentCaptor[Option[AbstractResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])
    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request), bodyCaptor.capture(), callbackCaptor.capture())
    val forwarded = bodyCaptor.getValue.asInstanceOf[AlterPartitionReassignmentsRequest]
    val forwardedNames = forwarded.data.topics.asScala.map(_.name).toSet
    assertEquals(Set("plain-topic"), forwardedNames,
      "only the non-polluting topic must reach the controller")

    val controllerResponse = new AlterPartitionReassignmentsResponseData()
      .setResponses(asList(new AlterPartitionReassignmentsResponseData.ReassignableTopicResponse()
        .setName("plain-topic")
        .setPartitions(asList(new AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse()
          .setPartitionIndex(0).setErrorCode(Errors.NONE.code)))))
    callbackCaptor.getValue.apply(Some(new AlterPartitionReassignmentsResponse(controllerResponse)))

    val response = verifyNoThrottling[AlterPartitionReassignmentsResponse](request)
    val byName = response.data.responses.asScala.map(r => r.name -> r).toMap
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code,
      byName("acme.orders").partitions.asScala.head.errorCode)
    assertEquals(Errors.NONE.code,
      byName("plain-topic").partitions.asScala.head.errorCode)
  }

  @Test
  def testListPartitionReassignmentsStripsTenantTopicsFromControllerResponse(): Unit = {
    // The controller has no notion of tenants — it returns every in-flight
    // reassignment by its physical topic name. A cluster-wide caller seeing
    // `acme.orders` in the response learns the existence + partition shape of
    // tenant data. The broker must scrub reserved-prefix topics from the
    // response before handing it back. Scrubbing is silent: no per-entry
    // "rejected" status, so the caller cannot distinguish "topic doesn't
    // exist" from "topic exists but belongs to a tenant".
    val req = new ListPartitionReassignmentsRequest.Builder(
      new ListPartitionReassignmentsRequestData().setTimeoutMs(5000)).build()
    val request = buildRequest(req)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleListPartitionReassignmentsRequest(request)

    val callbackCaptor: ArgumentCaptor[Option[AbstractResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])
    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      callbackCaptor.capture())

    val controllerData = new ListPartitionReassignmentsResponseData()
      .setTopics(util.Arrays.asList(
        new ListPartitionReassignmentsResponseData.OngoingTopicReassignment()
          .setName("acme.orders")
          .setPartitions(util.Arrays.asList(
            new ListPartitionReassignmentsResponseData.OngoingPartitionReassignment()
              .setPartitionIndex(0).setReplicas(util.Arrays.asList(1, 2)))),
        new ListPartitionReassignmentsResponseData.OngoingTopicReassignment()
          .setName("public-orders")
          .setPartitions(util.Arrays.asList(
            new ListPartitionReassignmentsResponseData.OngoingPartitionReassignment()
              .setPartitionIndex(0).setReplicas(util.Arrays.asList(3, 4))))))
    callbackCaptor.getValue.apply(Some(new ListPartitionReassignmentsResponse(controllerData)))

    val response = verifyNoThrottling[ListPartitionReassignmentsResponse](request)
    val names = response.data.topics.asScala.map(_.name).toSet
    assertEquals(Set("public-orders"), names,
      "tenant-prefixed reassignment must be stripped from the response")
  }

  @Test
  def testListPartitionReassignmentsLeavesNeutralResponseAlone(): Unit = {
    // Control: with no tenant-prefixed topics in the controller response, the
    // filter passes the topic list through unchanged.
    val req = new ListPartitionReassignmentsRequest.Builder(
      new ListPartitionReassignmentsRequestData().setTimeoutMs(5000)).build()
    val request = buildRequest(req)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleListPartitionReassignmentsRequest(request)

    val callbackCaptor: ArgumentCaptor[Option[AbstractResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])
    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      callbackCaptor.capture())

    val controllerData = new ListPartitionReassignmentsResponseData()
      .setTopics(util.Arrays.asList(
        new ListPartitionReassignmentsResponseData.OngoingTopicReassignment()
          .setName("public-orders")
          .setPartitions(util.Arrays.asList(
            new ListPartitionReassignmentsResponseData.OngoingPartitionReassignment()
              .setPartitionIndex(0).setReplicas(util.Arrays.asList(3, 4))))))
    callbackCaptor.getValue.apply(Some(new ListPartitionReassignmentsResponse(controllerData)))

    val response = verifyNoThrottling[ListPartitionReassignmentsResponse](request)
    val names = response.data.topics.asScala.map(_.name).toSet
    assertEquals(Set("public-orders"), names)
  }

  private def aclCreation(resourceType: ResourceType,
                          resourceName: String,
                          principal: String): CreateAclsRequestData.AclCreation =
    new CreateAclsRequestData.AclCreation()
      .setResourceType(resourceType.code)
      .setResourceName(resourceName)
      .setResourcePatternType(PatternType.LITERAL.code)
      .setPrincipal(principal)
      .setHost("*")
      .setOperation(AclOperation.READ.code)
      .setPermissionType(AclPermissionType.ALLOW.code)

  @Test
  def testCreateAclsClusterWideListenerRejectsTenantPrefixedTopicResource(): Unit = {
    // ACL written against `Topic:acme.orders` would grant cross-tenant access
    // to the named principal. The Authorizer applies the binding literally;
    // it has no notion of tenant ownership. Refuse before forwarding.
    val creation = aclCreation(ResourceType.TOPIC, "acme.orders", "User:bob")
    val req = new CreateAclsRequest.Builder(new CreateAclsRequestData()
      .setCreations(util.Arrays.asList(creation))).build()
    val request = buildRequest(req)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleCreateAclsRequest(request)

    val response = verifyNoThrottling[CreateAclsResponse](request)
    assertEquals(1, response.data.results.size)
    val r = response.data.results.get(0)
    assertEquals(Errors.INVALID_REQUEST.code, r.errorCode)
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testCreateAclsClusterWideListenerRejectsTenantPrefixedPrincipal(): Unit = {
    // ACL targeting `User:__tenant_acme.alice` would escalate or shadow the
    // tenant principal's authorization without the tenant's consent.
    val creation = aclCreation(ResourceType.TOPIC, "plain-topic", "User:__tenant_acme.alice")
    val req = new CreateAclsRequest.Builder(new CreateAclsRequestData()
      .setCreations(util.Arrays.asList(creation))).build()
    val request = buildRequest(req)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleCreateAclsRequest(request)

    val response = verifyNoThrottling[CreateAclsResponse](request)
    assertEquals(1, response.data.results.size)
    assertEquals(Errors.INVALID_REQUEST.code, response.data.results.get(0).errorCode)
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testCreateAclsClusterWideListenerMixesAllowedAndRejectedEntries(): Unit = {
    // Mixed batch [tenant-topic, neutral, tenant-principal, neutral]: only
    // entries 1 and 3 (0-indexed) reach the controller; entries 0 and 2 are
    // rejected at the broker. The response must surface results at the
    // original positions — CreateAclsResponse.Results is positional w.r.t.
    // the request's Creations.
    val creations = util.Arrays.asList(
      aclCreation(ResourceType.TOPIC, "acme.orders", "User:bob"),       // 0: refuse
      aclCreation(ResourceType.TOPIC, "plain-a", "User:bob"),           // 1: allow
      aclCreation(ResourceType.TOPIC, "plain-b", "User:__tenant_acme.alice"), // 2: refuse
      aclCreation(ResourceType.TOPIC, "plain-c", "User:carol"))         // 3: allow
    val req = new CreateAclsRequest.Builder(new CreateAclsRequestData()
      .setCreations(creations)).build()
    val request = buildRequest(req)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleCreateAclsRequest(request)

    val bodyCaptor: ArgumentCaptor[AbstractRequest] = ArgumentCaptor.forClass(classOf[AbstractRequest])
    val callbackCaptor: ArgumentCaptor[Option[AbstractResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])
    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request), bodyCaptor.capture(), callbackCaptor.capture())
    val forwarded = bodyCaptor.getValue.asInstanceOf[CreateAclsRequest]
    val forwardedNames = forwarded.data.creations.asScala.map(c =>
      (c.resourceName, c.principal)).toList
    assertEquals(List(("plain-a", "User:bob"), ("plain-c", "User:carol")), forwardedNames,
      "only the two non-polluting entries must reach the controller")

    // Controller responds for the two kept entries (positions 0 and 1 in the
    // forwarded request); the merger must place them back at positions 1 and 3.
    val controllerResults = util.Arrays.asList(
      new CreateAclsResponseData.AclCreationResult().setErrorCode(Errors.NONE.code),
      new CreateAclsResponseData.AclCreationResult().setErrorCode(Errors.NONE.code))
    callbackCaptor.getValue.apply(Some(new CreateAclsResponse(
      new CreateAclsResponseData().setResults(controllerResults))))

    val response = verifyNoThrottling[CreateAclsResponse](request)
    val codes = response.data.results.asScala.map(_.errorCode).toList
    assertEquals(List(
      Errors.INVALID_REQUEST.code,  // 0: refused
      Errors.NONE.code,             // 1: controller said OK
      Errors.INVALID_REQUEST.code,  // 2: refused
      Errors.NONE.code), codes)     // 3: controller said OK
  }

  @Test
  def testCreateAclsClusterWideListenerForwardsTenantLookingNamesWhenNoTenantsConfigured(): Unit = {
    // Guard is gated on TenantConfig.allTenants. With no tenants, `acme.foo`
    // is just a topic name and the broker forwards verbatim.
    val creation = aclCreation(ResourceType.TOPIC, "acme.orders", "User:bob")
    val req = new CreateAclsRequest.Builder(new CreateAclsRequestData()
      .setCreations(util.Arrays.asList(creation))).build()
    val request = buildRequest(req)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis()
    kafkaApis.handleCreateAclsRequest(request)

    verify(forwardingManager, times(1)).forwardRequest(
      ArgumentMatchers.eq(request),
      any[Option[AbstractResponse] => Unit]())
  }

  private def aclFilter(resourceType: ResourceType,
                        resourceNameFilter: String,
                        principalFilter: String): DeleteAclsRequestData.DeleteAclsFilter =
    new DeleteAclsRequestData.DeleteAclsFilter()
      .setResourceTypeFilter(resourceType.code)
      .setResourceNameFilter(resourceNameFilter)
      .setPatternTypeFilter(PatternType.LITERAL.code)
      .setPrincipalFilter(principalFilter)
      .setHostFilter(null)
      .setOperation(AclOperation.READ.code)
      .setPermissionType(AclPermissionType.ALLOW.code)

  @Test
  def testDeleteAclsClusterWideListenerRejectsExplicitTenantTopicFilter(): Unit = {
    // Explicit-name filter against `Topic:acme.orders` would yank tenant ACLs.
    // Refuse at the broker; the controller never sees the filter.
    val filter = aclFilter(ResourceType.TOPIC, "acme.orders", null)
    val req = new DeleteAclsRequest.Builder(new DeleteAclsRequestData()
      .setFilters(util.Arrays.asList(filter))).build()
    val request = buildRequest(req)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDeleteAclsRequest(request)

    val response = verifyNoThrottling[DeleteAclsResponse](request)
    assertEquals(1, response.data.filterResults.size)
    assertEquals(Errors.INVALID_REQUEST.code, response.data.filterResults.get(0).errorCode)
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testDeleteAclsClusterWideListenerRejectsExplicitTenantPrincipalFilter(): Unit = {
    // Explicit principal filter `User:__tenant_acme.alice` revokes the
    // tenant principal's grants. Refuse.
    val filter = aclFilter(ResourceType.TOPIC, null, "User:__tenant_acme.alice")
    val req = new DeleteAclsRequest.Builder(new DeleteAclsRequestData()
      .setFilters(util.Arrays.asList(filter))).build()
    val request = buildRequest(req)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDeleteAclsRequest(request)

    val response = verifyNoThrottling[DeleteAclsResponse](request)
    assertEquals(1, response.data.filterResults.size)
    assertEquals(Errors.INVALID_REQUEST.code, response.data.filterResults.get(0).errorCode)
  }

  @Test
  def testDeleteAclsClusterWideListenerScrubsMatchingAclsForWildcardFilter(): Unit = {
    // Wildcard filter (null resourceNameFilter AND null principalFilter) DOES
    // proceed to the controller — that is legitimate cluster-admin reach. But
    // the response's MatchingAcls echoes resource + principal names of every
    // ACL that matched. Tenant-owned entries in that echo are a free
    // enumeration of tenant ACLs; the leak guard scrubs them.
    val filter = aclFilter(ResourceType.TOPIC, null, null)
    val req = new DeleteAclsRequest.Builder(new DeleteAclsRequestData()
      .setFilters(util.Arrays.asList(filter))).build()
    val request = buildRequest(req)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDeleteAclsRequest(request)

    val callbackCaptor: ArgumentCaptor[Option[AbstractResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])
    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request), callbackCaptor.capture())

    val controllerMatchingAcls = util.Arrays.asList(
      new DeleteAclsResponseData.DeleteAclsMatchingAcl()
        .setResourceType(ResourceType.TOPIC.code)
        .setResourceName("acme.orders")
        .setPatternType(PatternType.LITERAL.code)
        .setPrincipal("User:bob")
        .setHost("*")
        .setOperation(AclOperation.READ.code)
        .setPermissionType(AclPermissionType.ALLOW.code),
      new DeleteAclsResponseData.DeleteAclsMatchingAcl()
        .setResourceType(ResourceType.TOPIC.code)
        .setResourceName("plain-topic")
        .setPatternType(PatternType.LITERAL.code)
        .setPrincipal("User:__tenant_acme.alice")
        .setHost("*")
        .setOperation(AclOperation.READ.code)
        .setPermissionType(AclPermissionType.ALLOW.code),
      new DeleteAclsResponseData.DeleteAclsMatchingAcl()
        .setResourceType(ResourceType.TOPIC.code)
        .setResourceName("plain-topic")
        .setPatternType(PatternType.LITERAL.code)
        .setPrincipal("User:bob")
        .setHost("*")
        .setOperation(AclOperation.READ.code)
        .setPermissionType(AclPermissionType.ALLOW.code))
    val filterResult = new DeleteAclsResponseData.DeleteAclsFilterResult()
      .setErrorCode(Errors.NONE.code)
      .setMatchingAcls(controllerMatchingAcls)
    callbackCaptor.getValue.apply(Some(new DeleteAclsResponse(
      new DeleteAclsResponseData().setFilterResults(util.Arrays.asList(filterResult)),
      req.version)))

    val response = verifyNoThrottling[DeleteAclsResponse](request)
    val matching = response.data.filterResults.get(0).matchingAcls.asScala.toList
    assertEquals(1, matching.size,
      "tenant-named resource and tenant-principal entries must be scrubbed from MatchingAcls")
    assertEquals("plain-topic", matching.head.resourceName)
    assertEquals("User:bob", matching.head.principal)
  }

  @Test
  def testDeleteAclsClusterWideListenerLeavesNeutralExplicitFilterAlone(): Unit = {
    // A specific filter that does NOT name a tenant namespace must reach the
    // controller verbatim; the response's MatchingAcls (also non-tenant) must
    // pass through untouched.
    val filter = aclFilter(ResourceType.TOPIC, "plain-topic", "User:bob")
    val req = new DeleteAclsRequest.Builder(new DeleteAclsRequestData()
      .setFilters(util.Arrays.asList(filter))).build()
    val request = buildRequest(req)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDeleteAclsRequest(request)

    val callbackCaptor: ArgumentCaptor[Option[AbstractResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])
    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request), callbackCaptor.capture())

    val matchingAcl = new DeleteAclsResponseData.DeleteAclsMatchingAcl()
      .setResourceType(ResourceType.TOPIC.code)
      .setResourceName("plain-topic")
      .setPatternType(PatternType.LITERAL.code)
      .setPrincipal("User:bob")
      .setHost("*")
      .setOperation(AclOperation.READ.code)
      .setPermissionType(AclPermissionType.ALLOW.code)
    val filterResult = new DeleteAclsResponseData.DeleteAclsFilterResult()
      .setErrorCode(Errors.NONE.code)
      .setMatchingAcls(util.Arrays.asList(matchingAcl))
    callbackCaptor.getValue.apply(Some(new DeleteAclsResponse(
      new DeleteAclsResponseData().setFilterResults(util.Arrays.asList(filterResult)),
      req.version)))

    val response = verifyNoThrottling[DeleteAclsResponse](request)
    assertEquals(1, response.data.filterResults.get(0).matchingAcls.size,
      "non-tenant matching ACL must not be scrubbed")
  }

  // ------------------------------------------------------------------
  // DescribeAcls — tenant existence-oracle + namespace-enumeration leak
  // (KafkaApis.handleDescribeAcls L1 filter validation + L2 response scrub).
  // ------------------------------------------------------------------

  private def describeAclsRequest(rt: ResourceType,
                                  name: String,
                                  patternType: PatternType,
                                  principal: String): DescribeAclsRequest = {
    val patternFilter = new ResourcePatternFilter(rt, name, patternType)
    val entryFilter = new AccessControlEntryFilter(principal, null,
      AclOperation.ANY, AclPermissionType.ANY)
    new DescribeAclsRequest.Builder(new AclBindingFilter(patternFilter, entryFilter)).build()
  }

  private def aclBinding(rt: ResourceType,
                         resourceName: String,
                         patternType: PatternType,
                         principal: String): AclBinding = {
    new AclBinding(
      new ResourcePattern(rt, resourceName, patternType),
      new AccessControlEntry(principal, "*", AclOperation.READ, AclPermissionType.ALLOW))
  }

  private def authorizerAllowingClusterDescribe(): Authorizer = {
    val auth: Authorizer = mock(classOf[Authorizer])
    // authorizeClusterOperation(DESCRIBE) under the hood issues a single
    // Action(CLUSTER, kafka-cluster, DESCRIBE) authorize() — return ALLOWED.
    when(auth.authorize(any[RequestContext], any[util.List[Action]]()))
      .thenAnswer(inv => {
        val actions = inv.getArgument[util.List[Action]](1)
        val out = new util.ArrayList[AuthorizationResult](actions.size)
        actions.forEach(_ => out.add(AuthorizationResult.ALLOWED))
        out
      })
    auth
  }

  @Test
  def testDescribeAclsClusterWideListenerRefusesLiteralTenantTopicFilter(): Unit = {
    // L1: a LITERAL filter naming `Topic:acme.orders` is an existence oracle —
    // the response (NONE vs SECURITY_DISABLED vs binding count) reveals whether
    // acme owns that topic and which principals hold ACLs on it. The guard
    // refuses with CLUSTER_AUTHORIZATION_FAILED before the Authorizer is asked.
    val req = describeAclsRequest(ResourceType.TOPIC, "acme.orders", PatternType.LITERAL, null)
    val request = buildRequest(req)
    val auth = authorizerAllowingClusterDescribe()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(
      authorizer = Some(auth),
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeAcls(request)

    val response = verifyNoThrottling[DescribeAclsResponse](request)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, response.data.errorCode)
    assertEquals(0, response.data.resources.size, "L1 refusal must not leak any ACL bindings")
    // L1 short-circuits before AclApis is invoked — Authorizer is never asked
    // about cluster DESCRIBE and never asked for acls().
    verify(auth, never()).acls(any[AclBindingFilter]())
  }

  @Test
  def testDescribeAclsClusterWideListenerRefusesPrefixedTenantTopicFilter(): Unit = {
    // L1: PREFIXED filter naming `Topic:acme.` dumps every binding under
    // acme's topic namespace. Refuse outright.
    val req = describeAclsRequest(ResourceType.TOPIC, "acme.", PatternType.PREFIXED, null)
    val request = buildRequest(req)
    val auth = authorizerAllowingClusterDescribe()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(
      authorizer = Some(auth),
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeAcls(request)

    val response = verifyNoThrottling[DescribeAclsResponse](request)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, response.data.errorCode)
    verify(auth, never()).acls(any[AclBindingFilter]())
  }

  @Test
  def testDescribeAclsClusterWideListenerRefusesTenantPrincipalFilter(): Unit = {
    // L1: entryFilter principal `User:__tenant_acme.alice` is an existence
    // oracle for the tenant principal — the response telegraphs whether alice
    // exists in acme's namespace via the binding count.
    val req = describeAclsRequest(ResourceType.ANY, null, PatternType.ANY,
      "User:__tenant_acme.alice")
    val request = buildRequest(req)
    val auth = authorizerAllowingClusterDescribe()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(
      authorizer = Some(auth),
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeAcls(request)

    val response = verifyNoThrottling[DescribeAclsResponse](request)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, response.data.errorCode)
    verify(auth, never()).acls(any[AclBindingFilter]())
  }

  @Test
  def testDescribeAclsClusterWideListenerRefusesTenantPrincipalGroupFilter(): Unit = {
    // L1: GROUP resourceType + name `__tenant_acme.cg-1` reveals whether the
    // tenant has a consumer group with that id.
    val req = describeAclsRequest(ResourceType.GROUP, "__tenant_acme.cg-1",
      PatternType.LITERAL, null)
    val request = buildRequest(req)
    val auth = authorizerAllowingClusterDescribe()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(
      authorizer = Some(auth),
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeAcls(request)

    val response = verifyNoThrottling[DescribeAclsResponse](request)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, response.data.errorCode)
    verify(auth, never()).acls(any[AclBindingFilter]())
  }

  @Test
  def testDescribeAclsClusterWideListenerScrubsTenantBindingsFromWildcard(): Unit = {
    // L2: a legitimate wildcard / ResourceType.ANY filter is allowed through
    // (this is the inherent reach of cluster admin), but the response is
    // scrubbed of any AclBinding whose pattern name or principal lives in a
    // tenant namespace. Mix tenant + neutral bindings; only the neutral entry
    // appears in the response.
    val req = describeAclsRequest(ResourceType.ANY, null, PatternType.ANY, null)
    val request = buildRequest(req)
    val auth = authorizerAllowingClusterDescribe()

    val bindings = util.Arrays.asList(
      aclBinding(ResourceType.TOPIC, "acme.orders", PatternType.LITERAL, "User:bob"),
      aclBinding(ResourceType.TOPIC, "plain-topic", PatternType.LITERAL, "User:bob"),
      aclBinding(ResourceType.GROUP, "__tenant_acme.cg-1", PatternType.LITERAL, "User:bob"),
      aclBinding(ResourceType.TOPIC, "neutral-2", PatternType.LITERAL, "User:__tenant_acme.alice"))
    when(auth.acls(any[AclBindingFilter]())).thenReturn(bindings)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(
      authorizer = Some(auth),
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeAcls(request)

    val response = verifyNoThrottling[DescribeAclsResponse](request)
    assertEquals(Errors.NONE.code, response.data.errorCode)
    // 4 bindings in → 1 binding out (plain-topic with User:bob). The tenant
    // topic, tenant group, and tenant-principal-targeted entries are scrubbed.
    val flattened = response.data.resources.asScala.flatMap { r =>
      r.acls.asScala.map(a => (r.resourceType, r.resourceName, a.principal))
    }.toSet
    assertEquals(Set((ResourceType.TOPIC.code, "plain-topic", "User:bob")), flattened,
      "wildcard response must scrub every tenant-namespaced binding")
  }

  @Test
  def testDescribeAclsClusterWideListenerNeutralFilterPassesThrough(): Unit = {
    // A neutral LITERAL filter (no tenant namespace in either pattern name or
    // principal) passes L1 and returns its bindings unscrubbed.
    val req = describeAclsRequest(ResourceType.TOPIC, "plain-topic",
      PatternType.LITERAL, "User:bob")
    val request = buildRequest(req)
    val auth = authorizerAllowingClusterDescribe()
    val bindings = util.Arrays.asList(
      aclBinding(ResourceType.TOPIC, "plain-topic", PatternType.LITERAL, "User:bob"))
    when(auth.acls(any[AclBindingFilter]())).thenReturn(bindings)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(
      authorizer = Some(auth),
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeAcls(request)

    val response = verifyNoThrottling[DescribeAclsResponse](request)
    assertEquals(Errors.NONE.code, response.data.errorCode)
    assertEquals(1, response.data.resources.size)
    assertEquals("plain-topic", response.data.resources.get(0).resourceName)
  }

  @Test
  def testDescribeAclsClusterWideListenerUnknownTenantPrincipalRefused(): Unit = {
    // The `__tenant_` prefix is RESERVED — any `__tenant_<id>.<x>` is reserved
    // namespace shape regardless of whether `<id>` is currently bound on this
    // node. A cluster-wide admin asking for ACLs whose principal filter is
    // `User:__tenant_unknown.bob` is probing a not-yet-bound tenant's slot;
    // refusing the lookup at L1 closes the pre-binding pollution leak (admin
    // plants ACLs under `__tenant_<future>.X`, future tenant inherits them on
    // bind) and the enumeration oracle that would otherwise let a privileged
    // caller learn which tenant ids any operator has ever attached ACLs to.
    val req = describeAclsRequest(ResourceType.ANY, null, PatternType.ANY,
      "User:__tenant_unknown.bob")
    val request = buildRequest(req)
    val auth = authorizerAllowingClusterDescribe()
    val bindings = util.Arrays.asList(
      aclBinding(ResourceType.TOPIC, "plain-topic", PatternType.LITERAL,
        "User:__tenant_unknown.bob"))
    when(auth.acls(any[AclBindingFilter]())).thenReturn(bindings)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(
      authorizer = Some(auth),
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeAcls(request)

    val response = verifyNoThrottling[DescribeAclsResponse](request)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, response.data.errorCode,
      "any `__tenant_*` prefix is reserved — refuse at L1 to close pre-binding pollution oracle")
    assertEquals(0, response.data.resources.size,
      "refusal must not leak any bindings")
    verify(auth, never()).acls(any[AclBindingFilter]())
  }

  @Test
  def testDescribeAclsTenantListenerBypassesGuardAndReturnsBindings(): Unit = {
    // A tenant principal on the tenant listener: handleDescribeAcls bypasses
    // the L1/L2 guards (their own namespace is not foreign to them) and
    // delegates straight to AclApis. authorizeClusterOperation(DESCRIBE) still
    // applies inside AclApis, so the Authorizer is consulted; we ALLOW it for
    // the test to ensure delegation works end-to-end without the guard
    // erroneously firing on a tenant caller.
    val req = describeAclsRequest(ResourceType.TOPIC, "acme.orders",
      PatternType.LITERAL, null)
    val request = buildRequest(
      req,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))
    val auth = authorizerAllowingClusterDescribe()
    val bindings = util.Arrays.asList(
      aclBinding(ResourceType.TOPIC, "acme.orders", PatternType.LITERAL, "User:bob"))
    when(auth.acls(any[AclBindingFilter]())).thenReturn(bindings)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(
      authorizer = Some(auth),
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeAcls(request)

    val response = verifyNoThrottling[DescribeAclsResponse](request)
    assertEquals(Errors.NONE.code, response.data.errorCode,
      "tenant caller must not be refused on its own namespace")
    assertEquals(1, response.data.resources.size,
      "tenant caller must see bindings for its own namespace")
  }

  @Test
  def testCreatePartitionsTenantListenerForwardsUnchanged(): Unit = {
    // Tenant principals never reach handleCreatePartitionsRequest in production
    // (CREATE_PARTITIONS is outside TENANT_ALLOWED_APIS — refused at dispatch).
    // The handler's `!effectiveTenant.isPresent` branch is the guard: a tenant
    // context skips the filter entirely, leaving the request to forward as-is.
    val topic = new CreatePartitionsRequestData.CreatePartitionsTopic()
      .setName("acme.orders").setCount(30)
    val data = new CreatePartitionsRequestData().setTimeoutMs(5000)
    data.topics().add(topic)
    val createReq = new CreatePartitionsRequest.Builder(data).build()
    val request = buildRequest(
      createReq,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleCreatePartitionsRequest(request)

    verify(forwardingManager, times(1)).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testDeleteTopicsTenantRejectsReservedPhysicalFormLogicalName(): Unit = {
    // DeleteTopics by-name with `acme.orders` would rewrite to `acme.acme.orders`,
    // which (if it exists at all) is a phantom artefact rather than the topic the
    // tenant means. Refuse with INVALID_TOPIC_EXCEPTION and don't forward.
    val deleteRequest = new DeleteTopicsRequest.Builder(new DeleteTopicsRequestData()
      .setTopics(util.Arrays.asList(new DeleteTopicsRequestData.DeleteTopicState().setName("acme.orders")))
      .setTimeoutMs(5000)).build()
    val request = buildRequest(
      deleteRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDeleteTopicsRequest(request)

    val response = verifyNoThrottling[DeleteTopicsResponse](request)
    val result = response.data.responses.asScala.head
    assertEquals("acme.orders", result.name,
      "rejection must keep the wire name the client sent")
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, result.errorCode,
      "double-prefix delete-by-name must be refused with INVALID_TOPIC_EXCEPTION")
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testMetadataTenantRejectsReservedPhysicalFormLogicalName(): Unit = {
    // A Metadata lookup for `acme.orders` from tenant acme would rewrite to
    // `acme.acme.orders` — a phantom topic the tenant cannot reason about. The
    // broker surfaces INVALID_TOPIC_EXCEPTION with the wire name preserved
    // rather than silently returning UNKNOWN_TOPIC_OR_PARTITION for the phantom.
    val metadataRequest = new MetadataRequest.Builder(List("acme.orders").asJava, false).build()
    val request = buildRequest(
      metadataRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleTopicMetadataRequest(request)

    val response = verifyNoThrottling[MetadataResponse](request)
    val errored = response.topicMetadata().asScala.toSeq
    assertEquals(1, errored.size)
    assertEquals("acme.orders", errored.head.topic,
      "rejection must keep the wire name; toLogical would have silently stripped the prefix")
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION, errored.head.error,
      "double-prefix metadata must be refused with INVALID_TOPIC_EXCEPTION")
  }

  @Test
  def testMetadataClusterWideListenerRejectsTenantPrefixedNamesFromAutoCreate(): Unit = {
    // Outside-in pollution via the Metadata auto-create path: a super-user on
    // the cluster-wide (non-tenant) listener requesting Metadata for "acme.foo"
    // with allowAutoTopicCreation=true would otherwise reach the auto-create
    // branch, which materialises literal `acme.foo` via the controller. Tenant
    // acme on its own listener would then see `foo` in ListTopics. Symmetric
    // to the CreateTopics dispatch guard: the broker refuses the entry before
    // the auto-create call so the controller never materialises the topic.
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    val metadataRequest = new MetadataRequest.Builder(List("acme.foo").asJava, true).build()
    val request = buildRequest(metadataRequest)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleTopicMetadataRequest(request)

    val response = verifyNoThrottling[MetadataResponse](request)
    val entries = response.topicMetadata().asScala.toSeq
    assertEquals(1, entries.size)
    assertEquals("acme.foo", entries.head.topic,
      "rejection must keep the wire name the cluster-wide caller sent")
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION, entries.head.error,
      "tenant-prefixed auto-create from a non-tenant listener must be refused")
    verify(autoTopicCreationManager, never()).createTopics(
      any[Set[String]](), any[ControllerMutationQuota](), any[Option[RequestContext]]())
  }

  @Test
  def testMetadataClusterWideListenerAutoCreatesTenantLookingNameWhenNoTenantsConfigured(): Unit = {
    // Pollution guard is gated on TenantConfig.allTenants. With no tenants
    // configured, the broker behaves as a stock single-tenant cluster and the
    // auto-create path proceeds for names that happen to contain a dot.
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    val metadataRequest = new MetadataRequest.Builder(List("acme.foo").asJava, true).build()
    val request = buildRequest(metadataRequest)

    val capturedRequest = verifyTopicCreation("acme.foo",
      enableAutoTopicCreation = true, isInternal = false, request)
    kafkaApis = createKafkaApis()
    kafkaApis.handleTopicMetadataRequest(request)

    val response = verifyNoThrottling[MetadataResponse](request)
    assertEquals(1, response.topicMetadata().size,
      "without a tenant binding the request must surface the auto-create attempt verbatim")
    assertTrue(capturedRequest.getValue.isDefined,
      "auto-create must be invoked for the dot-bearing name when no tenants are configured")
  }

  @Test
  def testMetadataClusterWideListenerPassesInternalAutoCreateThrough(): Unit = {
    // Internal topics are never tenant-namespaced. Even with tenants
    // configured, `__consumer_offsets` must not be misclassified as pollution
    // — the auto-create path for the offset topic must reach the controller.
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    val metadataRequest = new MetadataRequest.Builder(
      List(Topic.GROUP_METADATA_TOPIC_NAME).asJava, true).build()
    val request = buildRequest(metadataRequest)

    val groupConfig = mutable.Map.empty[String, String]
    groupConfig.put(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, "3")
    groupConfig.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "3")
    when(groupCoordinator.groupMetadataTopicConfigs).thenReturn(new Properties)

    val capturedRequest = verifyTopicCreation(Topic.GROUP_METADATA_TOPIC_NAME,
      enableAutoTopicCreation = true, isInternal = true, request)
    kafkaApis = createKafkaApis(
      overrideProperties = groupConfig.toMap,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleTopicMetadataRequest(request)

    verifyNoThrottling[MetadataResponse](request)
    assertTrue(capturedRequest.getValue.isDefined,
      "internal topic auto-create must reach the controller even with tenants configured")
  }

  @Test
  def testMetadataClusterWideListenerMixesAllowedAndPollutingNames(): Unit = {
    // Mixed batch: one tenant-prefixed name (rejected, never reaches
    // auto-create) and one neutral name (forwarded to auto-create). Mirrors
    // the CreateTopics mixed-batch shape: refuse the polluter, let the rest
    // flow through.
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    val metadataRequest = new MetadataRequest.Builder(
      List("acme.foo", "plain-topic").asJava, true).build()
    val request = buildRequest(metadataRequest)

    val capturedRequest = verifyTopicCreation("plain-topic",
      enableAutoTopicCreation = true, isInternal = false, request)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleTopicMetadataRequest(request)

    val response = verifyNoThrottling[MetadataResponse](request)
    val byName = response.topicMetadata().asScala.map(t => t.topic -> t.error).toMap
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION, byName("acme.foo"),
      "tenant-prefixed entry must be rejected at the broker")
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, byName("plain-topic"),
      "neutral entry must surface the auto-create stub from the controller")
    assertTrue(capturedRequest.getValue.isDefined,
      "auto-create must be invoked for the neutral entry but not for the polluter")
    verify(autoTopicCreationManager, never()).createTopics(
      ArgumentMatchers.eq(Set("acme.foo")),
      any[ControllerMutationQuota](),
      any[Option[RequestContext]]())
    verify(autoTopicCreationManager, never()).createTopics(
      ArgumentMatchers.eq(Set("acme.foo", "plain-topic")),
      any[ControllerMutationQuota](),
      any[Option[RequestContext]]())
  }

  @Test
  def testProduceClusterWideListenerRejectsTenantPrefixedNames(): Unit = {
    // Outside-in pollution via Produce: a super-user on the cluster-wide
    // (non-tenant) listener producing to "acme.orders" would auto-create /
    // append into tenant acme's physical log; tenant acme's logical Fetch
    // would then return foreign records as if they had produced them. The
    // broker refuses the entry with INVALID_TOPIC_EXCEPTION before reaching
    // replicaManager — destructive twin of the CreateTopics outside-in guard.
    val produceRequest = buildSingleTopicProduceRequest("acme.orders")
    val request = buildRequest(produceRequest)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[ProduceResponse](request)
    assertEquals(1, response.data.responses.size)
    val topicResp = response.data.responses.asScala.head
    assertEquals("acme.orders", topicResp.name,
      "rejection must keep the wire name the cluster-wide caller sent")
    val partitionResp = topicResp.partitionResponses.asScala.head
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION,
      Errors.forCode(partitionResp.errorCode),
      "tenant-prefixed produce from a non-tenant listener must be refused")
    verify(replicaManager, never()).handleProduceAppend(
      anyLong, anyShort, anyBoolean, any(), any(), any(), any(), any(), any(), any())
  }

  @Test
  def testProduceClusterWideListenerWithAcksZeroClosesConnectionOnReject(): Unit = {
    // acks=0 + outside-in pollution: the rejection lives in
    // invalidLogicalTopicResponses which bypasses replicaManager. Without
    // the explicit `errorInResponse = true` signal, the acks=0 path would
    // fall through to sendNoOpResponseExemptThrottle and the polluter would
    // never learn the produce was refused — symmetric with the per-tenant
    // reserved-form acks=0 trap.
    val produceRequest = buildSingleTopicProduceRequest("acme.orders", acks = 0.toShort)
    val request = buildRequest(produceRequest)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    verify(requestChannel).closeConnection(
      ArgumentMatchers.eq(request),
      any[java.util.Map[Errors, Integer]]())
    verify(requestChannel, never()).sendResponse(any(), any(), any())
    verify(replicaManager, never()).handleProduceAppend(
      anyLong, anyShort, anyBoolean(), any(), any(), any(), any(), any(), any(), any())
  }

  @Test
  def testProduceClusterWideListenerForwardsTenantLookingNamesWhenNoTenantsConfigured(): Unit = {
    // Pollution guard is gated on TenantConfig.allTenants. With no tenants
    // configured, the broker behaves as a stock single-tenant cluster and the
    // produce flows through normally — including topics whose names happen to
    // contain a dot. Topic exists in metadataCache so the request reaches
    // replicaManager (where the test stubs it to NONE).
    val topic = "acme.orders"
    addTopicToMetadataCache(topic, numPartitions = 1)

    val produceRequest = buildSingleTopicProduceRequest(topic)
    val request = buildRequest(produceRequest)

    val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])
    when(replicaManager.handleProduceAppend(
      anyLong, anyShort, ArgumentMatchers.eq(false), any(),
      any(), responseCallback.capture(),
      any(), any(), any(), any())
    ).thenAnswer(_ => responseCallback.getValue.apply(
      Map(new TopicPartition(topic, 0) -> new PartitionResponse(Errors.NONE))))

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    kafkaApis = createKafkaApis(authorizer = None)
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[ProduceResponse](request)
    val topicResp = response.data.responses.asScala.head
    assertEquals(topic, topicResp.name,
      "without a tenant binding the request must surface the wire name verbatim")
    assertEquals(Errors.NONE,
      Errors.forCode(topicResp.partitionResponses.asScala.head.errorCode),
      "without a tenant binding the produce must succeed verbatim")
  }

  @Test
  def testDeleteTopicsClusterWideListenerRejectsTenantPrefixedNames(): Unit = {
    // Outside-in pollution via DeleteTopics by-name: a super-user on the
    // cluster-wide (non-tenant) listener deleting "acme.foo" would otherwise
    // forward to the controller, which would happily drop the tenant's
    // physical log; tenant acme would observe their logical `foo` silently
    // vanishing. The broker refuses the entry before forwarding.
    val deleteRequest = new DeleteTopicsRequest.Builder(new DeleteTopicsRequestData()
      .setTopics(util.Arrays.asList(new DeleteTopicsRequestData.DeleteTopicState().setName("acme.foo")))
      .setTimeoutMs(5000)).build()
    val request = buildRequest(deleteRequest)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDeleteTopicsRequest(request)

    val response = verifyNoThrottling[DeleteTopicsResponse](request)
    val result = response.data.responses.asScala.head
    assertEquals("acme.foo", result.name,
      "rejection must keep the wire name the cluster-wide caller sent")
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, result.errorCode,
      "tenant-prefixed delete from a non-tenant listener must be refused")
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testDeleteTopicsClusterWideListenerMixesAllowedAndRejectedEntries(): Unit = {
    // Mixed batch: one tenant-prefixed name (rejected at broker), one neutral
    // (forwarded). Mirrors the CreateTopics outside-in mixed-batch shape:
    // refuse the polluter, forward the rest, merge responses on return.
    val deleteRequest = new DeleteTopicsRequest.Builder(new DeleteTopicsRequestData()
      .setTopics(util.Arrays.asList(
        new DeleteTopicsRequestData.DeleteTopicState().setName("acme.foo"),
        new DeleteTopicsRequestData.DeleteTopicState().setName("plain-topic")))
      .setTimeoutMs(5000)).build()
    val request = buildRequest(deleteRequest)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDeleteTopicsRequest(request)

    val bodyCaptor: ArgumentCaptor[AbstractRequest] = ArgumentCaptor.forClass(classOf[AbstractRequest])
    val callbackCaptor: ArgumentCaptor[Option[AbstractResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])
    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      bodyCaptor.capture(),
      callbackCaptor.capture())
    val forwarded = bodyCaptor.getValue.asInstanceOf[DeleteTopicsRequest]
    assertEquals(Set("plain-topic"),
      forwarded.data.topics.asScala.map(_.name).toSet,
      "only the non-polluting entry must reach the controller")

    val controllerResponse = new DeleteTopicsResponse(new DeleteTopicsResponseData()
      .setResponses(new DeleteTopicsResponseData.DeletableTopicResultCollection(
        Collections.singleton(new DeleteTopicsResponseData.DeletableTopicResult()
          .setName("plain-topic").setErrorCode(Errors.NONE.code)).iterator)))
    callbackCaptor.getValue.apply(Some(controllerResponse))

    val response = verifyNoThrottling[DeleteTopicsResponse](request)
    val byName = response.data.responses.asScala.map(r => r.name -> r.errorCode).toMap
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, byName("acme.foo"),
      "tenant-prefixed entry must be rejected at the broker")
    assertEquals(Errors.NONE.code, byName("plain-topic"),
      "non-polluting entry must surface the controller's outcome unchanged")
  }

  @Test
  def testDeleteTopicsClusterWideListenerForwardsTenantLookingNamesWhenNoTenantsConfigured(): Unit = {
    // Pollution guard is gated on TenantConfig.allTenants. With no tenants
    // configured, the broker behaves as a stock single-tenant cluster and the
    // delete is forwarded verbatim — including topics whose names happen to
    // contain a dot.
    val deleteRequest = new DeleteTopicsRequest.Builder(new DeleteTopicsRequestData()
      .setTopics(util.Arrays.asList(new DeleteTopicsRequestData.DeleteTopicState().setName("acme.foo")))
      .setTimeoutMs(5000)).build()
    val request = buildRequest(deleteRequest)

    kafkaApis = createKafkaApis()
    kafkaApis.handleDeleteTopicsRequest(request)

    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      any[Option[AbstractResponse] => Unit]())
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[AbstractRequest](), any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testDeleteTopicsClusterWideListenerPassesInternalTopicsThrough(): Unit = {
    // Internal topics are never tenant-namespaced. Even with tenants
    // configured, `__consumer_offsets` (etc.) must not be misclassified as
    // pollution — delete-by-name must reach the controller via the standard
    // forwarded path.
    val deleteRequest = new DeleteTopicsRequest.Builder(new DeleteTopicsRequestData()
      .setTopics(util.Arrays.asList(
        new DeleteTopicsRequestData.DeleteTopicState().setName(Topic.GROUP_METADATA_TOPIC_NAME)))
      .setTimeoutMs(5000)).build()
    val request = buildRequest(deleteRequest)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDeleteTopicsRequest(request)

    val bodyCaptor: ArgumentCaptor[AbstractRequest] = ArgumentCaptor.forClass(classOf[AbstractRequest])
    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      bodyCaptor.capture(),
      any[Option[AbstractResponse] => Unit]())
    val forwarded = bodyCaptor.getValue.asInstanceOf[DeleteTopicsRequest]
    assertEquals(Set(Topic.GROUP_METADATA_TOPIC_NAME),
      forwarded.data.topics.asScala.map(_.name).toSet,
      "internal topic delete must reach the controller even with tenants configured")
  }

  @Test
  def testNonV1ApiFromPrivilegedCallerOnTenantBoundListenerIsRefusedAtDispatch(): Unit = {
    // The silent-pollution trap extends to every API still outside the
    // tenant allow-list: a super-user on a tenant-bound listener without a
    // `__tenant_` prefix in their principal would otherwise reach the
    // handler with a tenant-bound listener context. The dispatch-level
    // gate refuses before DescribeGroups / AlterConfigs / DescribeAcls
    // can read or mutate cluster state under the implicit tenant binding.
    // (APIs admitted to the allow-list — Produce, Fetch, Metadata,
    // ListOffsets, DeleteRecords, consumer-coordination — each carry an
    // equivalent `tenantCtx.isUnsafe` guard at the head of the handler;
    // tested separately for each handler.)
    val describeGroupsRequest = new DescribeGroupsRequest.Builder(
      new DescribeGroupsRequestData().setGroups(util.Arrays.asList("any-group"))).build()
    val request = buildRequest(describeGroupsRequest,
      listenerName = TENANT_LISTENER,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice")) // no tenant prefix

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[DescribeGroupsResponse](request)
    val errorCodes = response.data.groups.asScala.map(_.errorCode)
    assertTrue(errorCodes.nonEmpty, "expected at least one group in the synthesized error response")
    assertTrue(errorCodes.forall(_ == Errors.CLUSTER_AUTHORIZATION_FAILED.code),
      "privileged caller on tenant listener must be refused with CLUSTER_AUTHORIZATION_FAILED on disallowed APIs " +
      "— uniform error code across all API shapes, removes the topic-flavoured probe vector")
    verify(groupCoordinator, never()).describeGroups(any[RequestContext](), any[util.List[String]]())
  }

  @Test
  def testDispatchRefusalErrorCodeIsUniformAcrossApiCategories(): Unit = {
    // Probe-vector regression guard: the dispatch refusal must produce the
    // SAME error code regardless of the API's natural category (topic-,
    // group-, txn-, cluster-flavoured). Before this change, the refusal
    // returned TOPIC_AUTHORIZATION_FAILED for every API — which on a
    // non-topic API like LIST_TRANSACTIONS revealed that the broker was
    // forcing a topic-flavoured error, telling the attacker the principal
    // is being treated as tenant-scoped. After the fix, both refusals
    // must surface CLUSTER_AUTHORIZATION_FAILED.
    def refuse(req: AbstractRequest): Short = {
      val request = buildRequest(req,
        listenerName = TENANT_LISTENER,
        principal = tenantPrincipal("acme", "alice"))
      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
        any[RequestChannel.Request](), any[Long])).thenReturn(0)
      kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
      kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)
      val resp = verifyNoThrottling[AbstractResponse](request)
      // Extract the top-level (or first-element) error code from each shape.
      resp match {
        case dgr: DescribeGroupsResponse =>
          dgr.data.groups.asScala.head.errorCode
        case ltr: ListTransactionsResponse =>
          ltr.data.errorCode
        case _ => fail(s"unexpected response type: ${resp.getClass}").asInstanceOf[Short]
      }
    }

    val groupCode = refuse(new DescribeGroupsRequest.Builder(
      new DescribeGroupsRequestData().setGroups(util.Arrays.asList("any"))).build())
    val txnCode = refuse(new ListTransactionsRequest.Builder(
      new ListTransactionsRequestData()).build())
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, groupCode,
      "group-shaped API refusal must surface CLUSTER_AUTHORIZATION_FAILED, not its natural GROUP_AUTHORIZATION_FAILED")
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, txnCode,
      "txn-shaped API refusal must surface CLUSTER_AUTHORIZATION_FAILED, not its natural TRANSACTIONAL_ID_AUTHORIZATION_FAILED")
    assertEquals(groupCode, txnCode,
      "refusal error code must NOT co-vary with API category — that co-variance is the probe vector")
  }

  @Test
  def testInitProducerIdTenantAllowsIdempotentProducer(): Unit = {
    // Modern Java producers default to enable.idempotence=true and call
    // InitProducerId at start-up with transactionalId=null to obtain a
    // producer id + epoch. Refusing this on tenant-bound listeners would
    // break stock clients — the v1 scope explicitly carries idempotent
    // producers, only transactions are excluded.
    val initRequest = new InitProducerIdRequest.Builder(new InitProducerIdRequestData()
      .setTransactionalId(null)
      .setTransactionTimeoutMs(TimeUnit.MINUTES.toMillis(1).toInt)
      .setProducerId(RecordBatch.NO_PRODUCER_ID)
      .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH))
      .build()
    val request = buildRequest(initRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    val responseCallback: ArgumentCaptor[InitProducerIdResult => Unit] =
      ArgumentCaptor.forClass(classOf[InitProducerIdResult => Unit])
    val requestLocal = RequestLocal.withThreadConfinedCaching
    when(txnCoordinator.handleInitProducerId(
      ArgumentMatchers.eq(null.asInstanceOf[String]),
      anyInt(),
      ArgumentMatchers.eq(Option.empty),
      responseCallback.capture(),
      ArgumentMatchers.eq(requestLocal)
    )).thenAnswer(_ => responseCallback.getValue.apply(InitProducerIdResult(42L, 0.toShort, Errors.NONE)))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleInitProducerIdRequest(request, requestLocal)

    val response = verifyNoThrottling[InitProducerIdResponse](request)
    assertEquals(Errors.NONE.code, response.data.errorCode,
      "idempotent InitProducerId from a tenant principal must succeed")
    assertEquals(42L, response.data.producerId)
    verify(txnCoordinator).handleInitProducerId(
      ArgumentMatchers.eq(null.asInstanceOf[String]),
      anyInt(),
      ArgumentMatchers.eq(Option.empty),
      any[InitProducerIdResult => Unit](),
      ArgumentMatchers.eq(requestLocal))
  }

  @Test
  def testInitProducerIdTenantRewritesTransactionalIdToPhysicalForm(): Unit = {
    // Phase 3b: tenants may now use transactional producers. The broker
    // rewrites the logical transactional id ("my-txn") to its physical form
    // (__tenant_acme.my-txn) before authorisation and before reaching the
    // txn coordinator. __transaction_state is keyed by hash(transactional_id)
    // — two tenants both naming a transaction "my-txn" must resolve to
    // distinct coordinator records, otherwise they would share producer-id
    // / epoch state and silently fence each other.
    val initRequest = new InitProducerIdRequest.Builder(new InitProducerIdRequestData()
      .setTransactionalId("my-txn")
      .setTransactionTimeoutMs(TimeUnit.MINUTES.toMillis(1).toInt)
      .setProducerId(RecordBatch.NO_PRODUCER_ID)
      .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH))
      .build()
    val request = buildRequest(initRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    val responseCallback: ArgumentCaptor[InitProducerIdResult => Unit] =
      ArgumentCaptor.forClass(classOf[InitProducerIdResult => Unit])
    val requestLocal = RequestLocal.withThreadConfinedCaching
    when(txnCoordinator.handleInitProducerId(
      ArgumentMatchers.eq("__tenant_acme.my-txn"),
      anyInt(),
      ArgumentMatchers.eq(Option.empty),
      responseCallback.capture(),
      ArgumentMatchers.eq(requestLocal)
    )).thenAnswer(_ => responseCallback.getValue.apply(InitProducerIdResult(99L, 0.toShort, Errors.NONE)))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleInitProducerIdRequest(request, requestLocal)

    val response = verifyNoThrottling[InitProducerIdResponse](request)
    assertEquals(Errors.NONE.code, response.data.errorCode,
      "transactional InitProducerId from a tenant must succeed after physical rewrite")
    assertEquals(99L, response.data.producerId)
    // The physical id is what the coordinator actually saw — proving the
    // tenant namespace isolation, not just a happy-path response shape.
    verify(txnCoordinator).handleInitProducerId(
      ArgumentMatchers.eq("__tenant_acme.my-txn"),
      anyInt(),
      ArgumentMatchers.eq(Option.empty),
      any[InitProducerIdResult => Unit](),
      ArgumentMatchers.eq(requestLocal))
  }

  @Test
  def testInitProducerIdTenantRefusesCrossTenantPrefixedTransactionalId(): Unit = {
    // A tenant addressing `__tenant_other.foo` is either hostile or
    // confused. toPhysicalTxnId refuses to rewrap it into
    // `__tenant_acme.__tenant_other.foo` and throws; the handler must
    // catch that and respond with TRANSACTIONAL_ID_AUTHORIZATION_FAILED
    // — never letting the request reach the coordinator with a foreign
    // prefix in scope.
    val initRequest = new InitProducerIdRequest.Builder(new InitProducerIdRequestData()
      .setTransactionalId("__tenant_other.foo")
      .setTransactionTimeoutMs(TimeUnit.MINUTES.toMillis(1).toInt)
      .setProducerId(RecordBatch.NO_PRODUCER_ID)
      .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH))
      .build()
    val request = buildRequest(initRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleInitProducerIdRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[InitProducerIdResponse](request)
    assertEquals(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code, response.data.errorCode,
      "cross-tenant prefixed transactional id must be refused before reaching the coordinator")
    verify(txnCoordinator, never()).handleInitProducerId(
      any[String](), anyInt(), any[Option[ProducerIdAndEpoch]](),
      any[InitProducerIdResult => Unit](), any[RequestLocal]())
  }

  @Test
  def testInitProducerIdTenantAdminToolPassthroughForAlreadyPhysicalTransactionalId(): Unit = {
    // PROMPT.md scenario 49 generalised to txn ids: an admin tool that
    // explicitly addresses the physical form (`__tenant_acme.foo`) must
    // succeed without double-prefixing into `__tenant_acme.__tenant_acme.foo`.
    val initRequest = new InitProducerIdRequest.Builder(new InitProducerIdRequestData()
      .setTransactionalId("__tenant_acme.foo")
      .setTransactionTimeoutMs(TimeUnit.MINUTES.toMillis(1).toInt)
      .setProducerId(RecordBatch.NO_PRODUCER_ID)
      .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH))
      .build()
    val request = buildRequest(initRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    val responseCallback: ArgumentCaptor[InitProducerIdResult => Unit] =
      ArgumentCaptor.forClass(classOf[InitProducerIdResult => Unit])
    val requestLocal = RequestLocal.withThreadConfinedCaching
    when(txnCoordinator.handleInitProducerId(
      ArgumentMatchers.eq("__tenant_acme.foo"),
      anyInt(),
      ArgumentMatchers.eq(Option.empty),
      responseCallback.capture(),
      ArgumentMatchers.eq(requestLocal)
    )).thenAnswer(_ => responseCallback.getValue.apply(InitProducerIdResult(7L, 0.toShort, Errors.NONE)))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleInitProducerIdRequest(request, requestLocal)

    val response = verifyNoThrottling[InitProducerIdResponse](request)
    assertEquals(Errors.NONE.code, response.data.errorCode)
    verify(txnCoordinator).handleInitProducerId(
      ArgumentMatchers.eq("__tenant_acme.foo"),
      anyInt(),
      ArgumentMatchers.eq(Option.empty),
      any[InitProducerIdResult => Unit](),
      ArgumentMatchers.eq(requestLocal))
  }

  @Test
  def testInitProducerIdPrivilegedCallerOnTenantBoundListenerWithTransactionalIdIsRefused(): Unit = {
    // Companion of the idempotent-path refusal: a super-user without a
    // `__tenant_` prefix on the tenant-bound listener, this time submitting
    // a transactional id. The unsafe-context guard MUST run before the
    // rewrite — otherwise the listener binding alone would silently wrap
    // "my-txn" into "__tenant_acme.my-txn" and the privileged caller would
    // end up driving the tenant's coordinator state.
    val initRequest = new InitProducerIdRequest.Builder(new InitProducerIdRequestData()
      .setTransactionalId("my-txn")
      .setTransactionTimeoutMs(TimeUnit.MINUTES.toMillis(1).toInt)
      .setProducerId(RecordBatch.NO_PRODUCER_ID)
      .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH))
      .build()
    val request = buildRequest(initRequest,
      listenerName = TENANT_LISTENER,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice")) // no tenant prefix

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleInitProducerIdRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[InitProducerIdResponse](request)
    assertEquals(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code, response.data.errorCode,
      "privileged caller on a tenant listener with a transactional id must be refused " +
        "with TRANSACTIONAL_ID_AUTHORIZATION_FAILED, not silently wrapped under the listener binding")
    verify(txnCoordinator, never()).handleInitProducerId(
      any[String](), anyInt(), any[Option[ProducerIdAndEpoch]](),
      any[InitProducerIdResult => Unit](), any[RequestLocal]())
  }

  @Test
  def testInitProducerIdPrivilegedCallerOnTenantBoundListenerIsRefused(): Unit = {
    // The standing trap: a super-user without a `__tenant_` prefix lands on
    // the tenant-bound listener. Without the unsafe-context guard the broker
    // would happily allocate a producer id under the listener's tenant
    // binding, letting that caller produce into acme's topics afterwards.
    val initRequest = new InitProducerIdRequest.Builder(new InitProducerIdRequestData()
      .setTransactionalId(null)
      .setTransactionTimeoutMs(TimeUnit.MINUTES.toMillis(1).toInt)
      .setProducerId(RecordBatch.NO_PRODUCER_ID)
      .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH))
      .build()
    val request = buildRequest(initRequest,
      listenerName = TENANT_LISTENER,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice")) // no tenant prefix

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleInitProducerIdRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[InitProducerIdResponse](request)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, response.data.errorCode,
      "privileged caller on a tenant listener must be refused at the InitProducerId guard")
    verify(txnCoordinator, never()).handleInitProducerId(
      any[String](), anyInt(), any[Option[ProducerIdAndEpoch]](),
      any[InitProducerIdResult => Unit](), any[RequestLocal]())
  }

  // ---------------------------------------------------------------------------
  // Phase 2 (PROMPT.md scenario 49): consumer-group id rewrites.
  //
  // A tenant submits a LOGICAL group id (e.g. "orders-consumer"); the broker
  // stores and addresses the group under the PHYSICAL form
  // "__tenant_<id>.<name>" — the principal prefix is reused so the wire form
  // can never collide with a tenant topic name. The tenant must only ever see
  // its logical id back in responses; the physical prefix is the broker's
  // private bookkeeping. These tests verify the rewrite-in / rewrite-out
  // contract for every consumer-runtime handler we admit in this phase, plus
  // the unsafe-context refusals (privileged-on-tenant-listener, transactional
  // FindCoordinator, cross-tenant prefix attempts).
  // ---------------------------------------------------------------------------

  @Test
  def testFindCoordinatorV4TenantRewritesGroupKeyInAndEchoesLogicalKeyOut(): Unit = {
    val findCoord = new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
      .setKeyType(CoordinatorType.GROUP.id)
      .setCoordinatorKeys(asList("orders-consumer"))).build(ApiKeys.FIND_COORDINATOR.latestVersion)
    val request = buildRequest(findCoord,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleFindCoordinatorRequest(request)

    verify(groupCoordinator).partitionFor("__tenant_acme.orders-consumer")
    val response = verifyNoThrottling[FindCoordinatorResponse](request)
    val coord = response.data.coordinators.get(0)
    assertEquals("orders-consumer", coord.key,
      "tenant must see the LOGICAL coordinator key, never the physical form")
  }

  @Test
  def testFindCoordinatorV4TenantRewritesTransactionalKeyInAndEchoesLogicalOut(): Unit = {
    // Phase 3b: TRANSACTION keys are now admitted for tenants. The logical
    // key the client sent ("txn-1") is rewritten to the physical form
    // (__tenant_acme.txn-1) before partitionFor and authorisation, then the
    // LOGICAL key is echoed back so the tenant never sees the physical
    // bookkeeping form in the response. partitionFor(physical) is what
    // proves the tenant namespace is actually in play — equal-named
    // transactions across tenants land on distinct coordinator partitions.
    val findCoord = new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
      .setKeyType(CoordinatorType.TRANSACTION.id)
      .setCoordinatorKeys(asList("txn-1"))).build(ApiKeys.FIND_COORDINATOR.latestVersion)
    val request = buildRequest(findCoord,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleFindCoordinatorRequest(request)

    verify(txnCoordinator).partitionFor("__tenant_acme.txn-1")
    val response = verifyNoThrottling[FindCoordinatorResponse](request)
    val coord = response.data.coordinators.get(0)
    assertEquals("txn-1", coord.key,
      "tenant must see the LOGICAL transactional id, never the physical form")
  }

  @Test
  def testFindCoordinatorV4TenantRefusesCrossTenantPrefixedTransactionalKey(): Unit = {
    // Defensive: a tenant submitting `__tenant_other.foo` as a TRANSACTION
    // key must be refused with TRANSACTIONAL_ID_AUTHORIZATION_FAILED before
    // partitionFor is ever called — the foreign prefix would otherwise be
    // double-wrapped or leaked to the coordinator. Echoes the LOGICAL key
    // back unchanged.
    val findCoord = new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
      .setKeyType(CoordinatorType.TRANSACTION.id)
      .setCoordinatorKeys(asList("__tenant_other.foo"))).build(ApiKeys.FIND_COORDINATOR.latestVersion)
    val request = buildRequest(findCoord,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleFindCoordinatorRequest(request)

    val response = verifyNoThrottling[FindCoordinatorResponse](request)
    val coord = response.data.coordinators.get(0)
    assertEquals(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code, coord.errorCode)
    assertEquals("__tenant_other.foo", coord.key,
      "logical key (including the bad foreign prefix) must be echoed back unchanged")
    verify(txnCoordinator, never()).partitionFor(anyString())
  }

  @Test
  def testFindCoordinatorV4PrivilegedCallerOnTenantListenerRefusedForTransactionalKey(): Unit = {
    // The same unsafe-context refusal pattern as the GROUP case, but the
    // error surfaces as TRANSACTIONAL_ID_AUTHORIZATION_FAILED so the client
    // sees the shape it would for a regular unauthorised txn request.
    val findCoord = new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
      .setKeyType(CoordinatorType.TRANSACTION.id)
      .setCoordinatorKeys(asList("txn-1"))).build(ApiKeys.FIND_COORDINATOR.latestVersion)
    val request = buildRequest(findCoord,
      listenerName = TENANT_LISTENER,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleFindCoordinatorRequest(request)

    val response = verifyNoThrottling[FindCoordinatorResponse](request)
    val coord = response.data.coordinators.get(0)
    assertEquals(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code, coord.errorCode)
    assertEquals("txn-1", coord.key)
    verify(txnCoordinator, never()).partitionFor(anyString())
  }

  @Test
  def testFindCoordinatorV4PrivilegedCallerOnTenantListenerIsRefused(): Unit = {
    // Standing PROMPT.md trap: a super-user with no `__tenant_` prefix hits the
    // tenant-bound listener. The broker must NOT silently resolve "orders" into
    // "__tenant_acme.orders" using the listener binding — that would let the
    // privileged caller drive a tenant's coordinator without owning the tenant
    // principal. The unsafe-context guard returns GROUP_AUTHORIZATION_FAILED
    // and never invokes the coordinator.
    val findCoord = new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
      .setKeyType(CoordinatorType.GROUP.id)
      .setCoordinatorKeys(asList("orders-consumer"))).build(ApiKeys.FIND_COORDINATOR.latestVersion)
    val request = buildRequest(findCoord,
      listenerName = TENANT_LISTENER,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleFindCoordinatorRequest(request)

    val response = verifyNoThrottling[FindCoordinatorResponse](request)
    val coord = response.data.coordinators.get(0)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, coord.errorCode)
    assertEquals("orders-consumer", coord.key)
    verify(groupCoordinator, never()).partitionFor(anyString())
  }

  @Test
  def testFindCoordinatorV3TenantRewritesGroupKeyToPhysicalForm(): Unit = {
    // v0-3 carries a single non-batched key on the `key` field. The rewrite
    // must apply there too, not just on the v4+ `coordinatorKeys` list.
    val findCoord = new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
      .setKeyType(CoordinatorType.GROUP.id)
      .setKey("orders-consumer")).build(3.toShort)
    val request = buildRequest(findCoord,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleFindCoordinatorRequest(request)

    verify(groupCoordinator).partitionFor("__tenant_acme.orders-consumer")
  }

  @Test
  def testFindCoordinatorV3TenantRewritesTransactionalKeyToPhysicalForm(): Unit = {
    // v0-3 carries a single non-batched key. handleFindCoordinatorRequestLessThanV4
    // is a separate codepath from V4+; both must rewrite TRANSACTION keys the
    // same way. Without this test, a regression in the V<4 branch (e.g. someone
    // adds a per-key rewrite to V4+ only) would slip through silently.
    val findCoord = new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
      .setKeyType(CoordinatorType.TRANSACTION.id)
      .setKey("txn-1")).build(3.toShort)
    val request = buildRequest(findCoord,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleFindCoordinatorRequest(request)

    verify(txnCoordinator).partitionFor("__tenant_acme.txn-1")
  }

  @Test
  def testAddPartitionsToTxnTenantRewritesTransactionalIdAndTopicsToPhysical(): Unit = {
    // Phase 3b: AddPartitionsToTxn is the producer's first contact with the
    // txn coordinator after InitProducerId. Both the transactional id AND
    // every topic-partition being enrolled in the transaction must reach
    // the coordinator in physical form — otherwise the txn coordinator's
    // per-transaction set of "fenced" partitions would record logical names
    // and stop matching the physical names ReplicaManager writes to.
    val physicalTopic = "acme.orders"
    addTopicToMetadataCache(physicalTopic, numPartitions = 1)

    val tp = new TopicPartition("orders", 0)
    val addPartitionsToTxnRequest = AddPartitionsToTxnRequest.Builder.forClient(
      "my-txn", 42L, 0.toShort, Collections.singletonList(tp)
    ).build(3.toShort)
    val request = buildRequest(addPartitionsToTxnRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    val responseCallback: ArgumentCaptor[Errors => Unit] =
      ArgumentCaptor.forClass(classOf[Errors => Unit])
    val requestLocal = RequestLocal.withThreadConfinedCaching
    when(txnCoordinator.handleAddPartitionsToTransaction(
      ArgumentMatchers.eq("__tenant_acme.my-txn"),
      ArgumentMatchers.eq(42L),
      ArgumentMatchers.eq(0.toShort),
      ArgumentMatchers.eq(Set(new TopicPartition(physicalTopic, 0))),
      responseCallback.capture(),
      ArgumentMatchers.any[TransactionVersion](),
      ArgumentMatchers.eq(requestLocal)
    )).thenAnswer(_ => responseCallback.getValue.apply(Errors.NONE))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAddPartitionsToTxnRequest(request, requestLocal)

    val response = verifyNoThrottling[AddPartitionsToTxnResponse](request)
    // The errors() map echoes the request transactional id (logical) and the
    // topics named in `data.transactions().find(txnId).topics()` (also the
    // logical names from the request data — preserved untouched).
    val txnErrors = response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID)
    assertEquals(Collections.singletonMap(tp, Errors.NONE), txnErrors,
      "client must see logical TopicPartition + NONE after the rewrite reaches the coordinator")
    verify(txnCoordinator).handleAddPartitionsToTransaction(
      ArgumentMatchers.eq("__tenant_acme.my-txn"),
      ArgumentMatchers.eq(42L),
      ArgumentMatchers.eq(0.toShort),
      ArgumentMatchers.eq(Set(new TopicPartition(physicalTopic, 0))),
      any[Errors => Unit](),
      ArgumentMatchers.any[TransactionVersion](),
      ArgumentMatchers.eq(requestLocal))
  }

  @Test
  def testAddPartitionsToTxnTenantRefusesCrossTenantPrefixedTransactionalId(): Unit = {
    // A tenant addressing `__tenant_other.foo` is hostile or confused. The
    // handler catches IllegalArgumentException from toPhysicalTxnId and
    // surfaces TRANSACTIONAL_ID_AUTHORIZATION_FAILED per-transaction without
    // touching the coordinator. The logical (foreign-prefixed) txn id is
    // echoed back so the client sees the value it submitted.
    addTopicToMetadataCache("acme.orders", numPartitions = 1)
    val tp = new TopicPartition("orders", 0)
    val addPartitionsToTxnRequest = AddPartitionsToTxnRequest.Builder.forClient(
      "__tenant_other.foo", 42L, 0.toShort, Collections.singletonList(tp)
    ).build(3.toShort)
    val request = buildRequest(addPartitionsToTxnRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAddPartitionsToTxnRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[AddPartitionsToTxnResponse](request)
    val txnErrors = response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID)
    assertEquals(Collections.singletonMap(tp, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED), txnErrors)
    verify(txnCoordinator, never()).handleAddPartitionsToTransaction(
      anyString(), anyLong(), anyShort(), any[Set[TopicPartition]](),
      any[Errors => Unit](), any[TransactionVersion](), any[RequestLocal]())
  }

  @Test
  def testAddPartitionsToTxnTenantRejectsReservedPhysicalFormTopic(): Unit = {
    // A tenant submitting `acme.foo` (the physical form already prefixed with
    // its own tenant id) has no right to address the physical namespace
    // directly. Without this check, toPhysical would either throw and crash
    // the handler or — if the prefix check were skipped — silently
    // double-prefix. The expected client-visible outcome is the per-partition
    // failure shape, with the logical name "acme.foo" preserved on the wire.
    addTopicToMetadataCache("acme.orders", numPartitions = 1)
    val reserved = new TopicPartition("acme.foo", 0)
    val addPartitionsToTxnRequest = AddPartitionsToTxnRequest.Builder.forClient(
      "my-txn", 42L, 0.toShort, Collections.singletonList(reserved)
    ).build(3.toShort)
    val request = buildRequest(addPartitionsToTxnRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAddPartitionsToTxnRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[AddPartitionsToTxnResponse](request)
    val txnErrors = response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID)
    assertEquals(Collections.singletonMap(reserved, Errors.TOPIC_AUTHORIZATION_FAILED), txnErrors,
      "reserved-form logical topic name must be refused TOPIC_AUTHORIZATION_FAILED before reaching the coordinator")
    verify(txnCoordinator, never()).handleAddPartitionsToTransaction(
      anyString(), anyLong(), anyShort(), any[Set[TopicPartition]](),
      any[Errors => Unit](), any[TransactionVersion](), any[RequestLocal]())
  }

  @Test
  def testAddPartitionsToTxnPrivilegedCallerOnTenantBoundListenerIsRefused(): Unit = {
    // Standing PROMPT.md trap: a super-user with no `__tenant_` prefix hits
    // the tenant-bound listener. The unsafe-context guard must short-circuit
    // before the rewrite — otherwise the listener binding alone would wrap
    // "my-txn" into "__tenant_acme.my-txn" and the privileged caller would
    // drive the tenant's coordinator state.
    addTopicToMetadataCache("acme.orders", numPartitions = 1)
    val tp = new TopicPartition("orders", 0)
    val addPartitionsToTxnRequest = AddPartitionsToTxnRequest.Builder.forClient(
      "my-txn", 42L, 0.toShort, Collections.singletonList(tp)
    ).build(3.toShort)
    val request = buildRequest(addPartitionsToTxnRequest,
      listenerName = TENANT_LISTENER,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAddPartitionsToTxnRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[AddPartitionsToTxnResponse](request)
    // V<4 routes per-request errors through resultsByTopicV3AndBelow keyed by
    // V3_AND_BELOW_TXN_ID; the error must reach the wire even though errorCode
    // is unused on that version.
    val txnErrors = response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID)
    assertEquals(Collections.singletonMap(tp, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED), txnErrors,
      "unsafe context must short-circuit with a transaction-level refusal")
    verify(txnCoordinator, never()).handleAddPartitionsToTransaction(
      anyString(), anyLong(), anyShort(), any[Set[TopicPartition]](),
      any[Errors => Unit](), any[TransactionVersion](), any[RequestLocal]())
  }

  @Test
  def testAddOffsetsToTxnTenantRewritesTransactionalIdAndGroupIdToPhysical(): Unit = {
    // AddOffsetsToTxn registers a group's __consumer_offsets partition with a
    // transaction. Both keys must reach the coordinator in physical form: the
    // transactional id (so two tenants sharing the same logical txn id resolve
    // to different __transaction_state records) AND the group id (so the
    // partition number partitionFor() returns is the tenant-isolated one).
    val partition = 7
    when(groupCoordinator.partitionFor(ArgumentMatchers.eq("__tenant_acme.orders-consumer")))
      .thenReturn(partition)

    val addOffsetsToTxnRequest = new AddOffsetsToTxnRequest.Builder(
      new AddOffsetsToTxnRequestData()
        .setGroupId("orders-consumer")
        .setTransactionalId("my-txn")
        .setProducerId(42L)
        .setProducerEpoch(0.toShort)
    ).build(ApiKeys.ADD_OFFSETS_TO_TXN.latestVersion)
    val request = buildRequest(addOffsetsToTxnRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    val responseCallback: ArgumentCaptor[Errors => Unit] =
      ArgumentCaptor.forClass(classOf[Errors => Unit])
    val requestLocal = RequestLocal.withThreadConfinedCaching
    when(txnCoordinator.handleAddPartitionsToTransaction(
      ArgumentMatchers.eq("__tenant_acme.my-txn"),
      ArgumentMatchers.eq(42L),
      ArgumentMatchers.eq(0.toShort),
      ArgumentMatchers.eq(Set(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, partition))),
      responseCallback.capture(),
      ArgumentMatchers.eq(TransactionVersion.TV_0),
      ArgumentMatchers.eq(requestLocal)
    )).thenAnswer(_ => responseCallback.getValue.apply(Errors.NONE))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAddOffsetsToTxnRequest(request, requestLocal)

    val response = verifyNoThrottling[AddOffsetsToTxnResponse](request)
    assertEquals(Errors.NONE.code, response.data.errorCode)
    // partitionFor saw the physical group id — proves the offsets partition
    // selection is tenant-isolated.
    verify(groupCoordinator).partitionFor("__tenant_acme.orders-consumer")
    verify(txnCoordinator).handleAddPartitionsToTransaction(
      ArgumentMatchers.eq("__tenant_acme.my-txn"),
      ArgumentMatchers.eq(42L),
      ArgumentMatchers.eq(0.toShort),
      ArgumentMatchers.eq(Set(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, partition))),
      any[Errors => Unit](),
      ArgumentMatchers.eq(TransactionVersion.TV_0),
      ArgumentMatchers.eq(requestLocal))
  }

  @Test
  def testAddOffsetsToTxnTenantRefusesCrossTenantPrefixedTransactionalId(): Unit = {
    val addOffsetsToTxnRequest = new AddOffsetsToTxnRequest.Builder(
      new AddOffsetsToTxnRequestData()
        .setGroupId("orders-consumer")
        .setTransactionalId("__tenant_other.foo")
        .setProducerId(42L)
        .setProducerEpoch(0.toShort)
    ).build(ApiKeys.ADD_OFFSETS_TO_TXN.latestVersion)
    val request = buildRequest(addOffsetsToTxnRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAddOffsetsToTxnRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[AddOffsetsToTxnResponse](request)
    assertEquals(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code, response.data.errorCode)
    verify(groupCoordinator, never()).partitionFor(anyString())
    verify(txnCoordinator, never()).handleAddPartitionsToTransaction(
      anyString(), anyLong(), anyShort(), any[Set[TopicPartition]](),
      any[Errors => Unit](), any[TransactionVersion](), any[RequestLocal]())
  }

  @Test
  def testAddOffsetsToTxnTenantRefusesCrossTenantPrefixedGroupId(): Unit = {
    // The txn id is fine, but the group id carries a foreign tenant prefix.
    // Surface as GROUP_AUTHORIZATION_FAILED (not TRANSACTIONAL_ID_...) so the
    // client can distinguish which key failed.
    val addOffsetsToTxnRequest = new AddOffsetsToTxnRequest.Builder(
      new AddOffsetsToTxnRequestData()
        .setGroupId("__tenant_other.foo")
        .setTransactionalId("my-txn")
        .setProducerId(42L)
        .setProducerEpoch(0.toShort)
    ).build(ApiKeys.ADD_OFFSETS_TO_TXN.latestVersion)
    val request = buildRequest(addOffsetsToTxnRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAddOffsetsToTxnRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[AddOffsetsToTxnResponse](request)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.errorCode)
    verify(groupCoordinator, never()).partitionFor(anyString())
    verify(txnCoordinator, never()).handleAddPartitionsToTransaction(
      anyString(), anyLong(), anyShort(), any[Set[TopicPartition]](),
      any[Errors => Unit](), any[TransactionVersion](), any[RequestLocal]())
  }

  @Test
  def testAddOffsetsToTxnPrivilegedCallerOnTenantBoundListenerIsRefused(): Unit = {
    val addOffsetsToTxnRequest = new AddOffsetsToTxnRequest.Builder(
      new AddOffsetsToTxnRequestData()
        .setGroupId("orders-consumer")
        .setTransactionalId("my-txn")
        .setProducerId(42L)
        .setProducerEpoch(0.toShort)
    ).build(ApiKeys.ADD_OFFSETS_TO_TXN.latestVersion)
    val request = buildRequest(addOffsetsToTxnRequest,
      listenerName = TENANT_LISTENER,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAddOffsetsToTxnRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[AddOffsetsToTxnResponse](request)
    assertEquals(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code, response.data.errorCode)
    verify(groupCoordinator, never()).partitionFor(anyString())
    verify(txnCoordinator, never()).handleAddPartitionsToTransaction(
      anyString(), anyLong(), anyShort(), any[Set[TopicPartition]](),
      any[Errors => Unit](), any[TransactionVersion](), any[RequestLocal]())
  }

  @Test
  def testEndTxnTenantRewritesTransactionalIdToPhysical(): Unit = {
    // `__transaction_state` shards by hash(transactionalId); two tenants
    // sharing the external id "my-txn" only stay isolated when the physical
    // form (`__tenant_acme.my-txn`) is what reaches the coordinator.
    val responseCallback: ArgumentCaptor[(Errors, Long, Short) => Unit] =
      ArgumentCaptor.forClass(classOf[(Errors, Long, Short) => Unit])
    val endTxnRequest = new EndTxnRequest.Builder(
      new EndTxnRequestData()
        .setTransactionalId("my-txn")
        .setProducerId(42L)
        .setProducerEpoch(0.toShort)
        .setCommitted(true),
      true
    ).build(ApiKeys.END_TXN.latestVersion)
    val request = buildRequest(endTxnRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    val requestLocal = RequestLocal.withThreadConfinedCaching
    when(txnCoordinator.handleEndTransaction(
      ArgumentMatchers.eq("__tenant_acme.my-txn"),
      ArgumentMatchers.eq(42L),
      ArgumentMatchers.eq(0.toShort),
      ArgumentMatchers.eq(TransactionResult.COMMIT),
      any[TransactionVersion](),
      responseCallback.capture(),
      ArgumentMatchers.eq(requestLocal)
    )).thenAnswer(_ => responseCallback.getValue.apply(Errors.NONE, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleEndTxnRequest(request, requestLocal)

    val response = verifyNoThrottling[EndTxnResponse](request)
    assertEquals(Errors.NONE.code, response.data.errorCode)
    // The coordinator-side verify above pins the physical form; no extra
    // assertion needed.
  }

  @Test
  def testEndTxnTenantRefusesCrossTenantPrefixedTransactionalId(): Unit = {
    // Tenant `acme` tries to End a transaction keyed `__tenant_other.foo`.
    // The rewrite refuses with TRANSACTIONAL_ID_AUTHORIZATION_FAILED — same
    // wire shape an unauthorised principal would produce — so the response
    // cannot leak the foreign tenant's existence.
    val endTxnRequest = new EndTxnRequest.Builder(
      new EndTxnRequestData()
        .setTransactionalId("__tenant_other.foo")
        .setProducerId(42L)
        .setProducerEpoch(0.toShort)
        .setCommitted(true),
      true
    ).build(ApiKeys.END_TXN.latestVersion)
    val request = buildRequest(endTxnRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleEndTxnRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[EndTxnResponse](request)
    assertEquals(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code, response.data.errorCode)
    verify(txnCoordinator, never()).handleEndTransaction(
      anyString(), anyLong(), anyShort(), any[TransactionResult](),
      any[TransactionVersion](), any(), any[RequestLocal]())
  }

  @Test
  def testEndTxnPrivilegedCallerOnTenantBoundListenerIsRefused(): Unit = {
    // Super-user with no `__tenant_` prefix hits the tenant-bound listener.
    // The unsafe-context guard must short-circuit before the rewrite — the
    // listener binding alone would otherwise wrap "my-txn" into the tenant
    // namespace and let the privileged caller drive tenant coordinator state.
    val endTxnRequest = new EndTxnRequest.Builder(
      new EndTxnRequestData()
        .setTransactionalId("my-txn")
        .setProducerId(42L)
        .setProducerEpoch(0.toShort)
        .setCommitted(true),
      true
    ).build(ApiKeys.END_TXN.latestVersion)
    val request = buildRequest(endTxnRequest,
      listenerName = TENANT_LISTENER,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleEndTxnRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[EndTxnResponse](request)
    assertEquals(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code, response.data.errorCode)
    verify(txnCoordinator, never()).handleEndTransaction(
      anyString(), anyLong(), anyShort(), any[TransactionResult](),
      any[TransactionVersion](), any(), any[RequestLocal]())
  }

  @Test
  def testTxnOffsetCommitTenantRewritesAllThreeKeysToPhysical(): Unit = {
    // TxnOffsetCommit routes THREE identifiers into the broker state:
    //   - transactionalId  → __transaction_state record key
    //   - groupId          → __consumer_offsets partition selector
    //   - per-topic offset → topic-partition key in __consumer_offsets
    // All three must reach the coordinator on physical names and the response
    // must echo logical names back to the tenant.
    val topic = "acme.orders"
    addTopicToMetadataCache(topic, numPartitions = 1)
    val partitionOffsetCommitData = new TxnOffsetCommitRequest.CommittedOffset(15L, "", Optional.empty())
    val tp = new TopicPartition("orders", 0)

    val offsetCommitRequest = new TxnOffsetCommitRequest.Builder(
      "my-txn",
      "orders-consumer",
      42L,
      0.toShort,
      Map(tp -> partitionOffsetCommitData).asJava,
      false
    ).build(ApiKeys.TXN_OFFSET_COMMIT.latestVersion)
    val request = buildRequest(offsetCommitRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    val requestLocal = RequestLocal.withThreadConfinedCaching
    val future = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(groupCoordinator.commitTransactionalOffsets(
      ArgumentMatchers.eq(request.context),
      any[TxnOffsetCommitRequestData](),
      ArgumentMatchers.eq(requestLocal.bufferSupplier)
    )).thenReturn(future)

    future.complete(new TxnOffsetCommitResponseData()
      .setTopics(List(
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("acme.orders")
          .setPartitions(List(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code)
          ).asJava)
      ).asJava))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleTxnOffsetCommitRequest(request, requestLocal)

    // The coordinator must see physical names on all three keys.
    val captor = ArgumentCaptor.forClass(classOf[TxnOffsetCommitRequestData])
    verify(groupCoordinator).commitTransactionalOffsets(
      ArgumentMatchers.eq(request.context),
      captor.capture(),
      ArgumentMatchers.eq(requestLocal.bufferSupplier))
    val captured = captor.getValue
    assertEquals("__tenant_acme.my-txn", captured.transactionalId)
    assertEquals("__tenant_acme.orders-consumer", captured.groupId)
    assertEquals(1, captured.topics.size)
    assertEquals("acme.orders", captured.topics.get(0).name)

    // The response must surface logical names to the client.
    val response = verifyNoThrottling[TxnOffsetCommitResponse](request)
    assertEquals(1, response.data.topics.size)
    assertEquals("orders", response.data.topics.get(0).name)
    assertEquals(Errors.NONE.code, response.data.topics.get(0).partitions.get(0).errorCode)
  }

  @Test
  def testTxnOffsetCommitTenantRefusesCrossTenantPrefixedTransactionalId(): Unit = {
    val offsetCommitRequest = new TxnOffsetCommitRequest.Builder(
      "__tenant_other.foo",
      "orders-consumer",
      42L,
      0.toShort,
      Map(new TopicPartition("orders", 0) ->
        new TxnOffsetCommitRequest.CommittedOffset(15L, "", Optional.empty())).asJava,
      false
    ).build(ApiKeys.TXN_OFFSET_COMMIT.latestVersion)
    val request = buildRequest(offsetCommitRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleTxnOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[TxnOffsetCommitResponse](request)
    // getErrorResponse echoes every partition with the error; check the one
    // partition we sent.
    assertEquals(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
      response.errors().get(new TopicPartition("orders", 0)))
    verify(groupCoordinator, never()).commitTransactionalOffsets(
      any[RequestContext](), any[TxnOffsetCommitRequestData](), any())
  }

  @Test
  def testTxnOffsetCommitTenantRefusesCrossTenantPrefixedGroupId(): Unit = {
    val offsetCommitRequest = new TxnOffsetCommitRequest.Builder(
      "my-txn",
      "__tenant_other.foo",
      42L,
      0.toShort,
      Map(new TopicPartition("orders", 0) ->
        new TxnOffsetCommitRequest.CommittedOffset(15L, "", Optional.empty())).asJava,
      false
    ).build(ApiKeys.TXN_OFFSET_COMMIT.latestVersion)
    val request = buildRequest(offsetCommitRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleTxnOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[TxnOffsetCommitResponse](request)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED,
      response.errors().get(new TopicPartition("orders", 0)))
    verify(groupCoordinator, never()).commitTransactionalOffsets(
      any[RequestContext](), any[TxnOffsetCommitRequestData](), any())
  }

  @Test
  def testTxnOffsetCommitTenantRejectsReservedPhysicalFormTopic(): Unit = {
    // A tenant submitting the reserved physical form `acme.foo` would round-
    // trip to `acme.acme.foo` and collide on the response builder's by-name
    // map. Refuse the whole request with INVALID_TOPIC_EXCEPTION.
    val offsetCommitRequest = new TxnOffsetCommitRequest.Builder(
      "my-txn",
      "orders-consumer",
      42L,
      0.toShort,
      Map(new TopicPartition("acme.foo", 0) ->
        new TxnOffsetCommitRequest.CommittedOffset(15L, "", Optional.empty())).asJava,
      false
    ).build(ApiKeys.TXN_OFFSET_COMMIT.latestVersion)
    val request = buildRequest(offsetCommitRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleTxnOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[TxnOffsetCommitResponse](request)
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION,
      response.errors().get(new TopicPartition("acme.foo", 0)))
    verify(groupCoordinator, never()).commitTransactionalOffsets(
      any[RequestContext](), any[TxnOffsetCommitRequestData](), any())
  }

  @Test
  def testTxnOffsetCommitPrivilegedCallerOnTenantBoundListenerIsRefused(): Unit = {
    val offsetCommitRequest = new TxnOffsetCommitRequest.Builder(
      "my-txn",
      "orders-consumer",
      42L,
      0.toShort,
      Map(new TopicPartition("orders", 0) ->
        new TxnOffsetCommitRequest.CommittedOffset(15L, "", Optional.empty())).asJava,
      false
    ).build(ApiKeys.TXN_OFFSET_COMMIT.latestVersion)
    val request = buildRequest(offsetCommitRequest,
      listenerName = TENANT_LISTENER,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleTxnOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[TxnOffsetCommitResponse](request)
    assertEquals(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
      response.errors().get(new TopicPartition("orders", 0)))
    verify(groupCoordinator, never()).commitTransactionalOffsets(
      any[RequestContext](), any[TxnOffsetCommitRequestData](), any())
  }

  @Test
  def testJoinGroupTenantRewritesGroupIdToPhysical(): Unit = {
    val data = new JoinGroupRequestData()
      .setGroupId("orders-consumer")
      .setMemberId("member-1")
      .setProtocolType("consumer")
      .setRebalanceTimeoutMs(1000)
      .setSessionTimeoutMs(2000)
    val joinReq = new JoinGroupRequest.Builder(data).build(ApiKeys.JOIN_GROUP.latestVersion)
    val request = buildRequest(joinReq,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    // The handler mutates the in-place data: by the time Mockito's equality
    // matcher runs, the request's groupId field has been overwritten with the
    // physical form. The expected object mirrors that mutation.
    val expected = new JoinGroupRequestData()
      .setGroupId("__tenant_acme.orders-consumer")
      .setMemberId("member-1")
      .setProtocolType("consumer")
      .setRebalanceTimeoutMs(1000)
      .setSessionTimeoutMs(2000)

    val future = new CompletableFuture[JoinGroupResponseData]()
    when(groupCoordinator.joinGroup(
      request.context,
      expected,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleJoinGroupRequest(request, RequestLocal.noCaching)

    future.complete(new JoinGroupResponseData().setMemberId("member-1"))
    verifyNoThrottling[JoinGroupResponse](request)
    verify(groupCoordinator).joinGroup(
      ArgumentMatchers.eq(request.context),
      ArgumentMatchers.eq(expected),
      ArgumentMatchers.eq(RequestLocal.noCaching.bufferSupplier))
  }

  @Test
  def testJoinGroupTenantRefusesCrossTenantPrefixedGroupId(): Unit = {
    // A tenant addressing `__tenant_other.foo` is either confused or hostile;
    // the broker refuses with GROUP_AUTHORIZATION_FAILED so the wire response
    // never hints at the existence of any other tenant's namespace.
    val data = new JoinGroupRequestData()
      .setGroupId("__tenant_other.foo")
      .setMemberId("member-1")
      .setProtocolType("consumer")
      .setRebalanceTimeoutMs(1000)
      .setSessionTimeoutMs(2000)
    val request = buildRequest(new JoinGroupRequest.Builder(data).build(),
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleJoinGroupRequest(request, RequestLocal.noCaching)

    val response = verifyNoThrottling[JoinGroupResponse](request)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED, response.error)
    verify(groupCoordinator, never()).joinGroup(
      any[RequestContext](),
      any[JoinGroupRequestData](),
      any[org.apache.kafka.common.utils.BufferSupplier]())
  }

  @Test
  def testSyncGroupTenantRewritesGroupIdToPhysical(): Unit = {
    val data = new SyncGroupRequestData()
      .setGroupId("orders-consumer")
      .setMemberId("member-1")
      .setGenerationId(0)
      .setProtocolType("consumer")
      .setProtocolName("range")
    val request = buildRequest(new SyncGroupRequest.Builder(data).build(),
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    val expected = new SyncGroupRequestData()
      .setGroupId("__tenant_acme.orders-consumer")
      .setMemberId("member-1")
      .setGenerationId(0)
      .setProtocolType("consumer")
      .setProtocolName("range")

    val future = new CompletableFuture[SyncGroupResponseData]()
    when(groupCoordinator.syncGroup(
      request.context,
      expected,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleSyncGroupRequest(request, RequestLocal.noCaching)

    future.complete(new SyncGroupResponseData())
    verifyNoThrottling[SyncGroupResponse](request)
    verify(groupCoordinator).syncGroup(
      ArgumentMatchers.eq(request.context),
      ArgumentMatchers.eq(expected),
      ArgumentMatchers.eq(RequestLocal.noCaching.bufferSupplier))
  }

  @Test
  def testHeartbeatTenantRewritesGroupIdToPhysical(): Unit = {
    val data = new HeartbeatRequestData()
      .setGroupId("orders-consumer")
      .setMemberId("member-1")
      .setGenerationId(0)
    val request = buildRequest(new HeartbeatRequest.Builder(data).build(),
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    val expected = new HeartbeatRequestData()
      .setGroupId("__tenant_acme.orders-consumer")
      .setMemberId("member-1")
      .setGenerationId(0)

    val future = new CompletableFuture[HeartbeatResponseData]()
    when(groupCoordinator.heartbeat(request.context, expected)).thenReturn(future)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleHeartbeatRequest(request)

    future.complete(new HeartbeatResponseData())
    verifyNoThrottling[HeartbeatResponse](request)
    verify(groupCoordinator).heartbeat(
      ArgumentMatchers.eq(request.context),
      ArgumentMatchers.eq(expected))
  }

  @Test
  def testLeaveGroupTenantRewritesGroupIdToPhysical(): Unit = {
    val request = buildRequest(new LeaveGroupRequest.Builder(
      "orders-consumer",
      List(new MemberIdentity().setMemberId("member-1")).asJava
    ).build(),
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    val expected = new LeaveGroupRequestData()
      .setGroupId("__tenant_acme.orders-consumer")
      .setMembers(List(new MemberIdentity().setMemberId("member-1")).asJava)

    val future = new CompletableFuture[LeaveGroupResponseData]()
    when(groupCoordinator.leaveGroup(request.context, expected)).thenReturn(future)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleLeaveGroupRequest(request)

    future.complete(new LeaveGroupResponseData())
    verifyNoThrottling[LeaveGroupResponse](request)
    verify(groupCoordinator).leaveGroup(
      ArgumentMatchers.eq(request.context),
      ArgumentMatchers.eq(expected))
  }

  @Test
  def testOffsetCommitTenantRewritesGroupAndTopicAndStripsTopicOnResponse(): Unit = {
    // The full happy-path round-trip: tenant submits {group=orders-consumer,
    // topic=orders}; the broker addresses the coordinator with the physical
    // form {group=__tenant_acme.orders-consumer, topic=acme.orders} and
    // returns the LOGICAL form back to the tenant on the response side.
    addTopicToMetadataCache("acme.orders", numPartitions = 1)

    val data = new OffsetCommitRequestData()
      .setGroupId("orders-consumer")
      .setMemberId("member-1")
      .setTopics(List(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("orders")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(42)).asJava)).asJava)
    val request = buildRequest(new OffsetCommitRequest.Builder(data).build(),
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    val expectedCoordinatorRequest = new OffsetCommitRequestData()
      .setGroupId("__tenant_acme.orders-consumer")
      .setMemberId("member-1")
      .setTopics(List(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("acme.orders")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(42)).asJava)).asJava)

    val future = new CompletableFuture[OffsetCommitResponseData]()
    when(groupCoordinator.commitOffsets(
      request.context,
      expectedCoordinatorRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleOffsetCommitRequest(request, RequestLocal.noCaching)

    future.complete(new OffsetCommitResponseData()
      .setTopics(List(
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("acme.orders")
          .setPartitions(List(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code)).asJava)).asJava))

    val response = verifyNoThrottling[OffsetCommitResponse](request)
    val topics = response.data.topics.asScala
    assertEquals(1, topics.size)
    assertEquals("orders", topics.head.name,
      "tenant must see the LOGICAL topic name on the response, not the physical prefix")
    verify(groupCoordinator).commitOffsets(
      ArgumentMatchers.eq(request.context),
      ArgumentMatchers.eq(expectedCoordinatorRequest),
      ArgumentMatchers.eq(RequestLocal.noCaching.bufferSupplier))
  }

  @Test
  def testOffsetCommitTenantRefusesReservedPhysicalFormTopic(): Unit = {
    // A tenant submitting a literal "acme.foo" topic name is malformed: the
    // name would round-trip to a physical-physical form, and (per Codex
    // round-7 finding) merging the rejection back into a response that may
    // also carry a legitimate "foo"→"acme.foo" commit collides on the
    // OffsetCommitResponse.Builder's by-name HashMap. Rather than partially
    // satisfy and partially reject, the WHOLE request is refused with
    // INVALID_TOPIC_EXCEPTION, with all literal names echoed back unchanged
    // so the tenant can correlate the error to what they submitted.
    val data = new OffsetCommitRequestData()
      .setGroupId("orders-consumer")
      .setMemberId("member-1")
      .setTopics(List(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(1)).asJava),
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("acme.foo")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(42)).asJava)).asJava)
    val request = buildRequest(new OffsetCommitRequest.Builder(data).build(),
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleOffsetCommitRequest(request, RequestLocal.noCaching)

    val response = verifyNoThrottling[OffsetCommitResponse](request)
    val topics = response.data.topics.asScala
    // Whole-request rejection: every submitted topic carries INVALID_TOPIC_EXCEPTION
    // with its LITERAL name unchanged. Even the legitimate "foo" entry is
    // refused — the request itself is malformed, and partial satisfaction
    // would collide on the response builder.
    assertEquals(2, topics.size)
    val byName = topics.map(t => t.name -> t).toMap
    assertTrue(byName.contains("foo"),
      "legit-form topic must echo back unchanged in the rejection response")
    assertTrue(byName.contains("acme.foo"),
      "reserved-form topic must echo back the LITERAL logical name the tenant sent")
    topics.foreach { t =>
      assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code,
        t.partitions.asScala.head.errorCode,
        s"every topic in the rejection response must carry INVALID_TOPIC_EXCEPTION, got ${t.name}")
    }
    verify(groupCoordinator, never()).commitOffsets(
      any[RequestContext](),
      any[OffsetCommitRequestData](),
      any[org.apache.kafka.common.utils.BufferSupplier]())
  }

  @Test
  def testOffsetCommitClusterWideListenerRefusesReservedNamespaceTopicsWithoutOracle(): Unit = {
    // Round-6 Agent G finding (#147): handleOffsetCommitRequest only refuses
    // reserved-physical-form topics when the caller is a tenant principal.
    // For a cluster-wide caller (no `__tenant_` in identity) the wire topic
    // names previously fell straight through to `metadataCache.contains`,
    // producing NONE for existing tenant topics and UNKNOWN_TOPIC_OR_PARTITION
    // for non-existent ones — a binary topic-existence oracle revealing
    // whether the tenant has materialised a given topic. The fix adds an
    // outside-in scrub that synthesises TOPIC_AUTHORIZATION_FAILED for any
    // reserved-form topic name from a non-tenant caller, identical wire
    // shape regardless of whether the underlying tenant topic exists.
    //
    // This test exercises BOTH the existing and the non-existing case in the
    // same request and asserts the error codes are byte-equal — the oracle
    // is closed at the handler boundary, never reaching the metadataCache
    // probe, never reaching the coordinator.
    addTopicToMetadataCache("acme.orders", numPartitions = 1)
    // intentionally do NOT add "acme.ghost" to metadataCache

    val data = new OffsetCommitRequestData()
      .setGroupId("admin-probe-group")
      .setMemberId("member-x")
      .setTopics(List(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("acme.orders")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(1)).asJava),
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("acme.ghost")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(1)).asJava)).asJava)
    val request = buildRequest(new OffsetCommitRequest.Builder(data).build(),
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "cluster-admin")) // default listener — NOT TENANT_LISTENER

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleOffsetCommitRequest(request, RequestLocal.noCaching)

    val response = verifyNoThrottling[OffsetCommitResponse](request)
    val byName = response.data.topics.asScala.map(t => t.name -> t).toMap
    assertEquals(2, byName.size, "both reserved-namespace topics must echo back in the rejection response")
    val ordersError = byName("acme.orders").partitions.asScala.head.errorCode
    val ghostError = byName("acme.ghost").partitions.asScala.head.errorCode
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, ordersError,
      "existing tenant topic must surface as TOPIC_AUTHORIZATION_FAILED to a cluster-wide caller, not NONE")
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, ghostError,
      "non-existent reserved-form name must surface as TOPIC_AUTHORIZATION_FAILED, not UNKNOWN_TOPIC_OR_PARTITION")
    assertEquals(ordersError, ghostError,
      "existence oracle: cluster-wide caller must NOT be able to distinguish a real tenant topic from a hypothetical one via OffsetCommit response codes")
    verify(groupCoordinator, never()).commitOffsets(
      any[RequestContext](),
      any[OffsetCommitRequestData](),
      any[org.apache.kafka.common.utils.BufferSupplier]())
  }

  @Test
  def testFindCoordinatorV4TenantRefusesCrossTenantPrefixedKey(): Unit = {
    // PROMPT.md scenario 49 — a tenant on its listener addressing
    // "__tenant_other.foo" must be refused at the per-key level without
    // letting TenantNamespace.groupToPhysical's IllegalArgumentException
    // bubble out and fail the whole multi-key request. The wire form
    // never hints at the foreign tenant's existence.
    val keys = List("orders-consumer", "__tenant_other.foo", "another-group").asJava
    val data = new FindCoordinatorRequestData()
      .setKeyType(CoordinatorType.GROUP.id)
      .setCoordinatorKeys(keys)
    val request = buildRequest(new FindCoordinatorRequest.Builder(data).build(ApiKeys.FIND_COORDINATOR.latestVersion),
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    addTopicToMetadataCache(Topic.GROUP_METADATA_TOPIC_NAME, numPartitions = 1)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleFindCoordinatorRequest(request)

    val response = verifyNoThrottling[FindCoordinatorResponse](request)
    val byKey = response.data.coordinators.asScala.map(c => c.key -> c).toMap
    assertEquals(3, byKey.size)
    // Legit keys proceed (their per-key error is whatever the coordinator
    // path produces — what matters here is they are NOT short-circuited
    // with GROUP_AUTHORIZATION_FAILED).
    assertNotEquals(Errors.GROUP_AUTHORIZATION_FAILED.code,
      byKey("orders-consumer").errorCode,
      "valid logical key must reach the coordinator lookup, not be auth-refused")
    assertNotEquals(Errors.GROUP_AUTHORIZATION_FAILED.code,
      byKey("another-group").errorCode,
      "valid logical key must reach the coordinator lookup, not be auth-refused")
    // Cross-tenant key is refused per-key with the auth-shape error.
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code,
      byKey("__tenant_other.foo").errorCode,
      "cross-tenant prefixed key must be refused at the per-key level")
  }

  @Test
  def testOffsetFetchV8TenantRewritesGroupAndTopicAndDropsForeignTopicsOnResponse(): Unit = {
    // Two-fold contract: (a) on the way in, both the group id and the topic
    // names of a specific-topic fetch are rewritten to their physical forms
    // before reaching the coordinator; (b) on the way out, the response's
    // group id is restored to the logical form, the tenant's own topic gets
    // its prefix stripped, and any FOREIGN-namespace topic the coordinator
    // hands back (cross-tenant residue, mis-routed entry) is defensively
    // dropped before the tenant can see it.
    val version = ApiKeys.OFFSET_FETCH.latestVersion
    val groups = Map(
      "orders-consumer" -> List(new TopicPartition("orders", 0)).asJava
    ).asJava
    val request = buildRequest(new OffsetFetchRequest.Builder(groups, false, false).build(version),
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    val expectedCoordinatorRequest = new OffsetFetchRequestData.OffsetFetchRequestGroup()
      .setGroupId("__tenant_acme.orders-consumer")
      .setTopics(List(
        new OffsetFetchRequestData.OffsetFetchRequestTopics()
          .setName("acme.orders")
          .setPartitionIndexes(List[Integer](0).asJava)).asJava)

    val coordFuture = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchOffsets(
      request.context,
      expectedCoordinatorRequest,
      false
    )).thenReturn(coordFuture)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleOffsetFetchRequest(request)

    // Coordinator returns the tenant's own topic AND a foreign-namespace
    // residue ("beta.intruder"). The defensive filter must keep only the
    // former and strip its prefix, never letting "beta.intruder" reach the
    // tenant.
    coordFuture.complete(new OffsetFetchResponseData.OffsetFetchResponseGroup()
      .setGroupId("__tenant_acme.orders-consumer")
      .setTopics(List(
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName("acme.orders")
          .setPartitions(List(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)).asJava),
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName("beta.intruder")
          .setPartitions(List(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(999)).asJava)
      ).asJava))

    val response = verifyNoThrottling[OffsetFetchResponse](request)
    val groupsOut = response.data.groups.asScala
    assertEquals(1, groupsOut.size)
    assertEquals("orders-consumer", groupsOut.head.groupId,
      "tenant must see the LOGICAL group id on the response, not the physical prefix")
    val topicsOut = groupsOut.head.topics.asScala.map(_.name).toSet
    assertEquals(Set("orders"), topicsOut,
      "foreign-namespace topic must be dropped; own-topic must be stripped of prefix")
    verify(groupCoordinator).fetchOffsets(
      ArgumentMatchers.eq(request.context),
      ArgumentMatchers.eq(expectedCoordinatorRequest),
      ArgumentMatchers.eq(false))
  }

  @Test
  def testListOffsetsTenantRewritesTopicNameInAndOut(): Unit = {
    // PROMPT.md functional scenario: tenant A's logical offsets are tenant-local
    // and independent of another tenant's traffic on the same backing.
    // The IN-rewrite sends the physical name to replicaManager (so authorization
    // and the local log lookup both key on physical); the OUT-rewrite restores
    // the logical name before the response goes out so the tenant never sees
    // its prefix.
    val targetTimes = util.Arrays.asList(new ListOffsetsTopic()
      .setName("orders")
      .setPartitions(util.Arrays.asList(new ListOffsetsPartition()
        .setPartitionIndex(0)
        .setTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP))))
    val listOffsetsRequest = ListOffsetsRequest.Builder.forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
      .setTargetTimes(targetTimes).build()
    val request = buildRequest(listOffsetsRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    val topicsCaptor: ArgumentCaptor[Seq[ListOffsetsTopic]] =
      ArgumentCaptor.forClass(classOf[Seq[ListOffsetsTopic]])
    when(replicaManager.fetchOffset(
      topicsCaptor.capture(),
      ArgumentMatchers.eq(Set.empty[TopicPartition]),
      ArgumentMatchers.eq(IsolationLevel.READ_UNCOMMITTED),
      anyInt(),
      any[String](),
      anyInt(),
      anyShort(),
      any[(Errors, ListOffsetsPartition) => ListOffsetsPartitionResponse](),
      any[List[ListOffsetsTopicResponse] => Unit](),
      anyInt()
    )).thenAnswer(ans => {
      val captured = ans.getArgument[Seq[ListOffsetsTopic]](0)
      val callback = ans.getArgument[List[ListOffsetsTopicResponse] => Unit](8)
      callback(captured.map(t => new ListOffsetsTopicResponse()
        .setName(t.name)
        .setPartitions(t.partitions.asScala.map(p =>
          new ListOffsetsPartitionResponse()
            .setPartitionIndex(p.partitionIndex)
            .setErrorCode(Errors.NONE.code)
            .setTimestamp(0L)
            .setOffset(42L)).asJava)).toList)
    })

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleListOffsetRequest(request)

    // IN-side: replicaManager sees the physical topic name.
    assertEquals(Set("acme.orders"), topicsCaptor.getValue.map(_.name).toSet,
      "IN rewrite must hand the physical topic name to replicaManager")

    // OUT-side: client sees only the logical name.
    val response = verifyNoThrottling[ListOffsetsResponse](request)
    val byName = response.topics.asScala.map(t => t.name -> t).toMap
    assertTrue(byName.contains("orders"),
      "OUT rewrite must restore the logical name on the response")
    assertFalse(byName.contains("acme.orders"),
      "physical topic name must never leak to the tenant on the wire")
    assertEquals(42L, byName("orders").partitions.asScala.head.offset)
  }

  @Test
  def testListOffsetsTenantDuplicatePartitionsFlaggedAfterRewrite(): Unit = {
    // The ListOffsetsRequest constructor caches duplicatePartitions over the
    // ORIGINAL (logical) names the client sent. The handler then rewrites topic
    // names in place to physical before passing them to ReplicaManager, which
    // checks each post-rewrite TopicPartition against that cached set. Without
    // recomputation the set never matches and tenant duplicates silently slip
    // past the INVALID_REQUEST contract that stock clients see. This test
    // pins the recomputed-on-physical behaviour.
    val targetTimes = util.Arrays.asList(new ListOffsetsTopic()
      .setName("orders")
      .setPartitions(util.Arrays.asList(
        new ListOffsetsPartition()
          .setPartitionIndex(0)
          .setTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP),
        new ListOffsetsPartition()
          .setPartitionIndex(0)
          .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP))))
    val listOffsetsRequest = ListOffsetsRequest.Builder.forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
      .setTargetTimes(targetTimes).build()
    val request = buildRequest(listOffsetsRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    val duplicatesCaptor: ArgumentCaptor[Set[TopicPartition]] =
      ArgumentCaptor.forClass(classOf[Set[TopicPartition]])
    when(replicaManager.fetchOffset(
      any[Seq[ListOffsetsTopic]](),
      duplicatesCaptor.capture(),
      any[IsolationLevel](),
      anyInt(),
      any[String](),
      anyInt(),
      anyShort(),
      any[(Errors, ListOffsetsPartition) => ListOffsetsPartitionResponse](),
      any[List[ListOffsetsTopicResponse] => Unit](),
      anyInt()
    )).thenAnswer(_ => ())

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleListOffsetRequest(request)

    // The duplicate set passed to ReplicaManager must carry the PHYSICAL key
    // ReplicaManager will see when it scans the topics it just received,
    // otherwise the set acts as if empty for tenant requests.
    assertEquals(Set(new TopicPartition("acme.orders", 0)), duplicatesCaptor.getValue,
      "duplicate detection must operate on the same (physical) key space ReplicaManager sees")
  }

  @Test
  def testListOffsetsTenantRefusesReservedPhysicalFormTopic(): Unit = {
    // A tenant submitting `acme.orders` (the reserved physical form) is either
    // confused or probing the storage namespace. Refuse INVALID_TOPIC_EXCEPTION
    // at the partition level with the LITERAL name the tenant sent; never
    // reach replicaManager.
    val targetTimes = util.Arrays.asList(new ListOffsetsTopic()
      .setName("acme.orders")
      .setPartitions(util.Arrays.asList(new ListOffsetsPartition()
        .setPartitionIndex(0)
        .setTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP))))
    val listOffsetsRequest = ListOffsetsRequest.Builder.forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
      .setTargetTimes(targetTimes).build()
    val request = buildRequest(listOffsetsRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleListOffsetRequest(request)

    val response = verifyNoThrottling[ListOffsetsResponse](request)
    val topics = response.topics.asScala
    assertEquals(1, topics.size)
    assertEquals("acme.orders", topics.head.name,
      "reserved-form rejection must echo back the LITERAL name the tenant sent")
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code,
      topics.head.partitions.asScala.head.errorCode)
    verify(replicaManager, never()).fetchOffset(
      any[Seq[ListOffsetsTopic]](), any[Set[TopicPartition]](), any[IsolationLevel](),
      anyInt(), any[String](), anyInt(), anyShort(),
      any[(Errors, ListOffsetsPartition) => ListOffsetsPartitionResponse](),
      any[List[ListOffsetsTopicResponse] => Unit](),
      anyInt())
  }

  @Test
  def testListOffsetsPrivilegedCallerOnTenantBoundListenerIsRefused(): Unit = {
    // A super-user on a tenant-bound listener without a `__tenant_` prefix
    // would otherwise reach replicaManager with the listener-derived tenant
    // binding and read offsets in the tenant's namespace under their own
    // principal. The handler-level isUnsafe guard refuses every partition.
    val targetTimes = util.Arrays.asList(new ListOffsetsTopic()
      .setName("orders")
      .setPartitions(util.Arrays.asList(new ListOffsetsPartition()
        .setPartitionIndex(0)
        .setTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP))))
    val listOffsetsRequest = ListOffsetsRequest.Builder.forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
      .setTargetTimes(targetTimes).build()
    val request = buildRequest(listOffsetsRequest,
      listenerName = TENANT_LISTENER,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice")) // no tenant prefix

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleListOffsetRequest(request)

    val response = verifyNoThrottling[ListOffsetsResponse](request)
    val topics = response.topics.asScala
    assertEquals(Set("orders"), topics.map(_.name).toSet,
      "wire name must be the LOGICAL name the caller submitted, not the physical form")
    assertTrue(topics.head.partitions.asScala.forall(_.errorCode == Errors.TOPIC_AUTHORIZATION_FAILED.code),
      "every partition must carry TOPIC_AUTHORIZATION_FAILED on unsafe context")
    verify(replicaManager, never()).fetchOffset(
      any[Seq[ListOffsetsTopic]](), any[Set[TopicPartition]](), any[IsolationLevel](),
      anyInt(), any[String](), anyInt(), anyShort(),
      any[(Errors, ListOffsetsPartition) => ListOffsetsPartitionResponse](),
      any[List[ListOffsetsTopicResponse] => Unit](),
      anyInt())
  }

  @Test
  def testListOffsetsClusterWideListenerRejectsTenantPrefixedNames(): Unit = {
    // Outside-in pollution guard. A non-tenant principal on a non-tenant
    // listener naming `acme.orders` would otherwise probe the tenant's
    // physical log directly. Refuse it with INVALID_TOPIC_EXCEPTION before
    // it can reach authorization or replicaManager.
    val targetTimes = util.Arrays.asList(new ListOffsetsTopic()
      .setName("acme.orders")
      .setPartitions(util.Arrays.asList(new ListOffsetsPartition()
        .setPartitionIndex(0)
        .setTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP))))
    val listOffsetsRequest = ListOffsetsRequest.Builder.forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
      .setTargetTimes(targetTimes).build()
    val request = buildRequest(listOffsetsRequest)  // default plaintext listener, plain principal

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleListOffsetRequest(request)

    val response = verifyNoThrottling[ListOffsetsResponse](request)
    val topics = response.topics.asScala
    assertEquals(1, topics.size)
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code,
      topics.head.partitions.asScala.head.errorCode,
      "outside-in pollution guard must refuse the tenant-prefixed name")
    verify(replicaManager, never()).fetchOffset(
      any[Seq[ListOffsetsTopic]](), any[Set[TopicPartition]](), any[IsolationLevel](),
      anyInt(), any[String](), anyInt(), anyShort(),
      any[(Errors, ListOffsetsPartition) => ListOffsetsPartitionResponse](),
      any[List[ListOffsetsTopicResponse] => Unit](),
      anyInt())
  }

  @Test
  def testDeleteRecordsTenantRewritesTopicNameInAndOut(): Unit = {
    // PROMPT.md functional scenario: DeleteRecords from tenant A advances only
    // its logical low-water mark. IN rewrites logical → physical so the
    // replicaManager keys on the physical log; OUT rewrites physical → logical
    // so the tenant sees only its own name.
    addTopicToMetadataCache("acme.orders", numPartitions = 1)
    val deleteRequest = new DeleteRecordsRequest.Builder(new DeleteRecordsRequestData()
      .setTimeoutMs(1000)
      .setTopics(util.Arrays.asList(new DRTopic()
        .setName("orders")
        .setPartitions(util.Arrays.asList(new DRPartition()
          .setPartitionIndex(0)
          .setOffset(50L)))))).build()
    val request = buildRequest(deleteRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    val offsetsCaptor: ArgumentCaptor[Map[TopicPartition, Long]] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, Long]])
    when(replicaManager.deleteRecords(
      anyLong(),
      offsetsCaptor.capture(),
      any[Map[TopicPartition, DeleteRecordsPartitionResult] => Unit](),
      anyBoolean()
    )).thenAnswer(ans => {
      val captured = ans.getArgument[Map[TopicPartition, Long]](1)
      val callback = ans.getArgument[Map[TopicPartition, DeleteRecordsPartitionResult] => Unit](2)
      callback(captured.map { case (tp, _) =>
        tp -> new DeleteRecordsPartitionResult()
          .setPartitionIndex(tp.partition)
          .setLowWatermark(50L)
          .setErrorCode(Errors.NONE.code)
      }.toMap)
    })

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDeleteRecordsRequest(request)

    // IN-side: replicaManager sees the physical topic name.
    assertEquals(Set("acme.orders"), offsetsCaptor.getValue.keys.map(_.topic).toSet,
      "IN rewrite must hand the physical topic name to replicaManager")

    // OUT-side: client sees only the logical name.
    val response = verifyNoThrottling[DeleteRecordsResponse](request)
    val byName = response.data.topics.asScala.map(t => t.name -> t).toMap
    assertTrue(byName.contains("orders"),
      "OUT rewrite must restore the logical name on the response")
    assertFalse(byName.contains("acme.orders"),
      "physical topic name must never leak to the tenant on the wire")
    assertEquals(50L, byName("orders").partitions.asScala.head.lowWatermark)
    assertEquals(Errors.NONE.code, byName("orders").partitions.asScala.head.errorCode)
  }

  @Test
  def testDeleteRecordsTenantRefusesReservedPhysicalFormTopic(): Unit = {
    val deleteRequest = new DeleteRecordsRequest.Builder(new DeleteRecordsRequestData()
      .setTimeoutMs(1000)
      .setTopics(util.Arrays.asList(new DRTopic()
        .setName("acme.orders")
        .setPartitions(util.Arrays.asList(new DRPartition()
          .setPartitionIndex(0)
          .setOffset(10L)))))).build()
    val request = buildRequest(deleteRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDeleteRecordsRequest(request)

    val response = verifyNoThrottling[DeleteRecordsResponse](request)
    val topics = response.data.topics.asScala
    assertEquals(1, topics.size)
    assertEquals("acme.orders", topics.head.name,
      "reserved-form rejection must echo back the LITERAL name the tenant sent")
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code,
      topics.head.partitions.asScala.head.errorCode)
    verify(replicaManager, never()).deleteRecords(
      anyLong(), any[Map[TopicPartition, Long]](),
      any[Map[TopicPartition, DeleteRecordsPartitionResult] => Unit](),
      anyBoolean())
  }

  @Test
  def testDeleteRecordsPrivilegedCallerOnTenantBoundListenerIsRefused(): Unit = {
    val deleteRequest = new DeleteRecordsRequest.Builder(new DeleteRecordsRequestData()
      .setTimeoutMs(1000)
      .setTopics(util.Arrays.asList(new DRTopic()
        .setName("orders")
        .setPartitions(util.Arrays.asList(new DRPartition()
          .setPartitionIndex(0)
          .setOffset(10L)))))).build()
    val request = buildRequest(deleteRequest,
      listenerName = TENANT_LISTENER,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice")) // no tenant prefix

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDeleteRecordsRequest(request)

    val response = verifyNoThrottling[DeleteRecordsResponse](request)
    val topics = response.data.topics.asScala
    assertEquals(Set("orders"), topics.map(_.name).toSet,
      "wire name must be the LOGICAL name the caller submitted, not the physical form")
    assertTrue(topics.head.partitions.asScala.forall(_.errorCode == Errors.TOPIC_AUTHORIZATION_FAILED.code),
      "every partition must carry TOPIC_AUTHORIZATION_FAILED on unsafe context")
    verify(replicaManager, never()).deleteRecords(
      anyLong(), any[Map[TopicPartition, Long]](),
      any[Map[TopicPartition, DeleteRecordsPartitionResult] => Unit](),
      anyBoolean())
  }

  @Test
  def testDeleteRecordsClusterWideListenerRejectsTenantPrefixedNames(): Unit = {
    val deleteRequest = new DeleteRecordsRequest.Builder(new DeleteRecordsRequestData()
      .setTimeoutMs(1000)
      .setTopics(util.Arrays.asList(new DRTopic()
        .setName("acme.orders")
        .setPartitions(util.Arrays.asList(new DRPartition()
          .setPartitionIndex(0)
          .setOffset(10L)))))).build()
    val request = buildRequest(deleteRequest)  // default plaintext listener, plain principal

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDeleteRecordsRequest(request)

    val response = verifyNoThrottling[DeleteRecordsResponse](request)
    val topics = response.data.topics.asScala
    assertEquals(1, topics.size)
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code,
      topics.head.partitions.asScala.head.errorCode,
      "outside-in pollution guard must refuse the tenant-prefixed name")
    verify(replicaManager, never()).deleteRecords(
      anyLong(), any[Map[TopicPartition, Long]](),
      any[Map[TopicPartition, DeleteRecordsPartitionResult] => Unit](),
      anyBoolean())
  }

  // ---- Phase 3c.1: DescribeConfigs tenant rewrite ----

  @Test
  def testDescribeConfigsTenantRewritesTopicNameToPhysical(): Unit = {
    // Tenant `acme` describes topic `orders`. configHelper must look up
    // `acme.orders` in the metadata cache and config repository; the response
    // must carry the logical name `orders` back to the tenant.
    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.contains("acme.orders")).thenReturn(true)

    val topicConfigs = new Properties()
    topicConfigs.put("min.insync.replicas", "3")
    val configRepository = mock(classOf[ConfigRepository])
    when(configRepository.topicConfig("acme.orders")).thenReturn(topicConfigs)

    val describeRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setResources(List(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName("orders")
        .setResourceType(ConfigResource.Type.TOPIC.id)).asJava))
      .build(ApiKeys.DESCRIBE_CONFIGS.latestVersion)
    val request = buildRequest(describeRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(
      configRepository = configRepository,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeConfigsRequest(request)

    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    val results = response.data.results.asScala
    assertEquals(1, results.size)
    val result = results.head
    assertEquals(Errors.NONE.code, result.errorCode,
      "topic config lookup must succeed against the physical name")
    assertEquals("orders", result.resourceName,
      "response must echo the LOGICAL name back, not the physical form")
    assertEquals(ConfigResource.Type.TOPIC.id, result.resourceType)
    assertTrue(result.configs.asScala.exists(_.name == "min.insync.replicas"),
      "configHelper must have read the topic's physical config row")
    // Defence in depth: configHelper should NEVER have been asked for the logical name.
    verify(configRepository, never()).topicConfig("orders")
  }

  @Test
  def testDescribeConfigsTenantRefusesReservedPhysicalFormTopic(): Unit = {
    // Tenant `acme` submits the literal `acme.orders` (the reserved
    // physical form for its own namespace). Refuse with the LITERAL name
    // echoed back so the wire shape never round-trips into `acme.acme.orders`
    // and the tenant cannot infer broker storage conventions from the error.
    val describeRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setResources(List(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName("acme.orders")
        .setResourceType(ConfigResource.Type.TOPIC.id)).asJava))
      .build(ApiKeys.DESCRIBE_CONFIGS.latestVersion)
    val request = buildRequest(describeRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeConfigsRequest(request)

    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    val results = response.data.results.asScala
    assertEquals(1, results.size)
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, results.head.errorCode)
    assertEquals("acme.orders", results.head.resourceName,
      "literal name must be echoed back unchanged")
  }

  @Test
  def testDescribeConfigsTenantRefusesInternalTopic(): Unit = {
    // `__consumer_offsets` configs are cluster-wide. A tenant must never see
    // them through its own listener — the refusal happens before any cache
    // lookup so a permissive ACL cannot leak.
    val describeRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setResources(List(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName("__consumer_offsets")
        .setResourceType(ConfigResource.Type.TOPIC.id)).asJava))
      .build(ApiKeys.DESCRIBE_CONFIGS.latestVersion)
    val request = buildRequest(describeRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeConfigsRequest(request)

    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    val results = response.data.results.asScala
    assertEquals(1, results.size)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, results.head.errorCode)
    assertEquals("__consumer_offsets", results.head.resourceName)
  }

  @Test
  def testDescribeConfigsTenantRefusesBrokerResource(): Unit = {
    // BROKER / BROKER_LOGGER / CLIENT_METRICS describe cluster-wide state.
    // A tenant must not be able to introspect the broker's dynamic config.
    val describeRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setResources(List(
        new DescribeConfigsRequestData.DescribeConfigsResource()
          .setResourceName("0")
          .setResourceType(ConfigResource.Type.BROKER.id),
        new DescribeConfigsRequestData.DescribeConfigsResource()
          .setResourceName("0")
          .setResourceType(ConfigResource.Type.BROKER_LOGGER.id),
        new DescribeConfigsRequestData.DescribeConfigsResource()
          .setResourceName("client-metrics-subscription-1")
          .setResourceType(ConfigResource.Type.CLIENT_METRICS.id)).asJava))
      .build(ApiKeys.DESCRIBE_CONFIGS.latestVersion)
    val request = buildRequest(describeRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeConfigsRequest(request)

    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    val results = response.data.results.asScala
    assertEquals(3, results.size)
    assertTrue(results.forall(_.errorCode == Errors.CLUSTER_AUTHORIZATION_FAILED.code),
      "every cluster-wide resource must be refused with CLUSTER_AUTHORIZATION_FAILED")
  }

  @Test
  def testDescribeConfigsTenantRewritesGroupNameToPhysical(): Unit = {
    // Group configs are stored under the physical group name
    // `__tenant_acme.<logical>`; the lookup must use that form, while the
    // response must echo the tenant-visible logical id.
    val physicalGroup = "__tenant_acme.my-consumer"
    val groupConfigs = new Properties()
    groupConfigs.put(CONSUMER_SESSION_TIMEOUT_MS_CONFIG,
      GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_DEFAULT.toString)
    val configRepository = mock(classOf[ConfigRepository])
    when(configRepository.groupConfig(physicalGroup)).thenReturn(groupConfigs)

    val describeRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setResources(List(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName("my-consumer")
        .setResourceType(ConfigResource.Type.GROUP.id)).asJava))
      .build(ApiKeys.DESCRIBE_CONFIGS.latestVersion)
    val request = buildRequest(describeRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(
      configRepository = configRepository,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeConfigsRequest(request)

    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    val results = response.data.results.asScala
    assertEquals(1, results.size)
    val result = results.head
    assertEquals(Errors.NONE.code, result.errorCode)
    assertEquals("my-consumer", result.resourceName,
      "response must echo the LOGICAL group id, not the __tenant_-prefixed form")
    // The mock would have returned an empty Properties for any other key.
    verify(configRepository).groupConfig(physicalGroup)
    verify(configRepository, never()).groupConfig("my-consumer")
  }

  @Test
  def testDescribeConfigsTenantRefusesCrossTenantGroup(): Unit = {
    // Tenant `acme` queries the configs for group `__tenant_other.foo`.
    // The rewrite refuses with GROUP_AUTHORIZATION_FAILED — same wire shape
    // the unauthorised-group response carries — so the foreign tenant's
    // existence is not implied by the error.
    val describeRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setResources(List(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName("__tenant_other.foo")
        .setResourceType(ConfigResource.Type.GROUP.id)).asJava))
      .build(ApiKeys.DESCRIBE_CONFIGS.latestVersion)
    val request = buildRequest(describeRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeConfigsRequest(request)

    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    val results = response.data.results.asScala
    assertEquals(1, results.size)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, results.head.errorCode)
    assertEquals("__tenant_other.foo", results.head.resourceName,
      "the literal cross-tenant name is preserved on the wire")
  }

  @Test
  def testDescribeConfigsPrivilegedCallerOnTenantBoundListenerIsRefused(): Unit = {
    // Super-user (no `__tenant_` prefix) on a tenant-bound listener. The
    // unsafe-context short-circuit refuses the WHOLE request before the
    // rewrite — otherwise the listener binding alone would silently wrap
    // `orders` into `acme.orders` and let the privileged caller drive
    // tenant state.
    val describeRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setResources(List(
        new DescribeConfigsRequestData.DescribeConfigsResource()
          .setResourceName("orders")
          .setResourceType(ConfigResource.Type.TOPIC.id),
        new DescribeConfigsRequestData.DescribeConfigsResource()
          .setResourceName("my-consumer")
          .setResourceType(ConfigResource.Type.GROUP.id)).asJava))
      .build(ApiKeys.DESCRIBE_CONFIGS.latestVersion)
    val request = buildRequest(describeRequest,
      listenerName = TENANT_LISTENER,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeConfigsRequest(request)

    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    val results = response.data.results.asScala
    assertEquals(2, results.size)
    val topicResult = results.find(_.resourceType == ConfigResource.Type.TOPIC.id).get
    val groupResult = results.find(_.resourceType == ConfigResource.Type.GROUP.id).get
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, topicResult.errorCode)
    assertEquals("orders", topicResult.resourceName)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, groupResult.errorCode)
    assertEquals("my-consumer", groupResult.resourceName)
  }

  @Test
  def testDescribeConfigsTenantMergesRefusedAndSuccessfulResources(): Unit = {
    // A request mixing a valid topic with a refused cluster-wide resource
    // must return both — the success keyed on the logical name, the refusal
    // carrying its literal name. Order doesn't matter; both must be present.
    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.contains("acme.orders")).thenReturn(true)

    val topicConfigs = new Properties()
    topicConfigs.put("min.insync.replicas", "3")
    val configRepository = mock(classOf[ConfigRepository])
    when(configRepository.topicConfig("acme.orders")).thenReturn(topicConfigs)

    val describeRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setResources(List(
        new DescribeConfigsRequestData.DescribeConfigsResource()
          .setResourceName("orders")
          .setResourceType(ConfigResource.Type.TOPIC.id),
        new DescribeConfigsRequestData.DescribeConfigsResource()
          .setResourceName("0")
          .setResourceType(ConfigResource.Type.BROKER.id)).asJava))
      .build(ApiKeys.DESCRIBE_CONFIGS.latestVersion)
    val request = buildRequest(describeRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(
      configRepository = configRepository,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeConfigsRequest(request)

    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    val results = response.data.results.asScala
    assertEquals(2, results.size)
    val topicResult = results.find(_.resourceType == ConfigResource.Type.TOPIC.id).get
    val brokerResult = results.find(_.resourceType == ConfigResource.Type.BROKER.id).get
    assertEquals(Errors.NONE.code, topicResult.errorCode)
    assertEquals("orders", topicResult.resourceName)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, brokerResult.errorCode)
    assertEquals("0", brokerResult.resourceName)
  }

  @Test
  def testDescribeConfigsTenantRefusesInvalidLogicalTopicName(): Unit = {
    // Tenant `acme` submits `..` — invalid by Kafka's topic charset rules.
    // Reaching ConfigHelper would run `Topic.validate("acme...")` which throws
    // with the PHYSICAL name embedded in the message, leaking the prefix back
    // through `errorMessage`. The handler must refuse with the LOGICAL name
    // before ever calling `toPhysical`.
    val describeRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setResources(List(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName("..")
        .setResourceType(ConfigResource.Type.TOPIC.id)).asJava))
      .build(ApiKeys.DESCRIBE_CONFIGS.latestVersion)
    val request = buildRequest(describeRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeConfigsRequest(request)

    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    val results = response.data.results.asScala
    assertEquals(1, results.size)
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, results.head.errorCode)
    assertEquals("..", results.head.resourceName, "literal logical name must round-trip")
    assertNotNull(results.head.errorMessage)
    assertFalse(results.head.errorMessage.contains("acme."),
      "errorMessage must not echo the physical prefix")
  }

  @Test
  def testDescribeConfigsTenantRefusesOverlongLogicalTopicName(): Unit = {
    // The logical name fits in MAX_NAME_LENGTH (249) on its own but the
    // physical form `acme.X` overshoots. ConfigHelper's `Topic.validate` would
    // surface the physical name; the handler refuses pre-rewrite.
    val overlong = "x" * 248
    val describeRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setResources(List(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName(overlong)
        .setResourceType(ConfigResource.Type.TOPIC.id)).asJava))
      .build(ApiKeys.DESCRIBE_CONFIGS.latestVersion)
    val request = buildRequest(describeRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeConfigsRequest(request)

    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    val results = response.data.results.asScala
    assertEquals(1, results.size)
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, results.head.errorCode)
    assertEquals(overlong, results.head.resourceName)
    assertFalse(results.head.errorMessage.contains("acme."),
      "errorMessage must not echo the physical prefix")
  }

  @Test
  def testDescribeConfigsClusterWideCallerRefusesReservedTopicNamespace(): Unit = {
    // Outside-in pollution (#70): a cluster-wide admin on a non-tenant-bound
    // listener submits DescribeConfigs for TOPIC `acme.orders`. Without the
    // guard, ConfigHelper would either confirm/deny existence (existence
    // oracle) or hand back the tenant's full topic config (retention.ms,
    // segment.bytes, cleanup.policy, ...). Refuse with TOPIC_AUTHORIZATION_FAILED
    // so the wire shape is indistinguishable from an ACL refusal.
    val configRepository = mock(classOf[ConfigRepository])
    val describeRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setResources(List(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName("acme.orders")
        .setResourceType(ConfigResource.Type.TOPIC.id)).asJava))
      .build(ApiKeys.DESCRIBE_CONFIGS.latestVersion)
    val request = buildRequest(describeRequest,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis(
      configRepository = configRepository,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeConfigsRequest(request)

    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    val results = response.data.results.asScala
    assertEquals(1, results.size)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, results.head.errorCode)
    assertEquals("acme.orders", results.head.resourceName,
      "literal name echoed back unchanged")
    // Never reach ConfigHelper for the refused resource — no oracle, no leak.
    verify(configRepository, never()).topicConfig("acme.orders")
  }

  @Test
  def testDescribeConfigsClusterWideCallerRefusesReservedGroupNamespace(): Unit = {
    // Same outside-in pollution as TOPIC, but for GROUP. The physical group
    // form is `__tenant_<id>.<group>`; a cluster-wide admin must not be able
    // to read group config under a tenant's namespace.
    val configRepository = mock(classOf[ConfigRepository])
    val describeRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setResources(List(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName("__tenant_acme.my-consumer")
        .setResourceType(ConfigResource.Type.GROUP.id)).asJava))
      .build(ApiKeys.DESCRIBE_CONFIGS.latestVersion)
    val request = buildRequest(describeRequest,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis(
      configRepository = configRepository,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeConfigsRequest(request)

    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    val results = response.data.results.asScala
    assertEquals(1, results.size)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, results.head.errorCode)
    assertEquals("__tenant_acme.my-consumer", results.head.resourceName)
    verify(configRepository, never()).groupConfig("__tenant_acme.my-consumer")
  }

  @Test
  def testDescribeConfigsClusterWideMergesRefusedAndPlainResources(): Unit = {
    // Mixed batch: cluster-wide admin asks for a foreign-tenant TOPIC PLUS a
    // legitimate non-tenant TOPIC. Refused entry must coexist with the
    // ConfigHelper-served entry on the response.
    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.contains("public-topic")).thenReturn(true)
    val plainConfigs = new Properties()
    plainConfigs.put("min.insync.replicas", "2")
    val configRepository = mock(classOf[ConfigRepository])
    when(configRepository.topicConfig("public-topic")).thenReturn(plainConfigs)

    val describeRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setResources(List(
        new DescribeConfigsRequestData.DescribeConfigsResource()
          .setResourceName("acme.orders")
          .setResourceType(ConfigResource.Type.TOPIC.id),
        new DescribeConfigsRequestData.DescribeConfigsResource()
          .setResourceName("public-topic")
          .setResourceType(ConfigResource.Type.TOPIC.id)).asJava))
      .build(ApiKeys.DESCRIBE_CONFIGS.latestVersion)
    val request = buildRequest(describeRequest,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis(
      configRepository = configRepository,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeConfigsRequest(request)

    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    val results = response.data.results.asScala
    assertEquals(2, results.size)
    val refused = results.find(_.resourceName == "acme.orders").get
    val allowed = results.find(_.resourceName == "public-topic").get
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, refused.errorCode)
    assertEquals(Errors.NONE.code, allowed.errorCode)
    verify(configRepository, never()).topicConfig("acme.orders")
    verify(configRepository).topicConfig("public-topic")
  }

  @Test
  def testDescribeConfigsClusterWideWithNoTenantsBoundAllowsDottedTopics(): Unit = {
    // No tenant configured on the broker → `isReservedTenantNamespace` is
    // false for ANY name. A pre-existing operational topic like
    // `archive.events` must round-trip to ConfigHelper unchanged.
    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.contains("archive.events")).thenReturn(true)
    val topicConfigs = new Properties()
    topicConfigs.put("min.insync.replicas", "2")
    val configRepository = mock(classOf[ConfigRepository])
    when(configRepository.topicConfig("archive.events")).thenReturn(topicConfigs)

    val describeRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setResources(List(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName("archive.events")
        .setResourceType(ConfigResource.Type.TOPIC.id)).asJava))
      .build(ApiKeys.DESCRIBE_CONFIGS.latestVersion)
    val request = buildRequest(describeRequest,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    // No tenants configured: tenantConfig defaults to empty.
    kafkaApis = createKafkaApis(configRepository = configRepository)
    kafkaApis.handleDescribeConfigsRequest(request)

    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    val results = response.data.results.asScala
    assertEquals(1, results.size)
    assertEquals(Errors.NONE.code, results.head.errorCode,
      "no tenants bound → dotted topic must pass through")
    assertEquals("archive.events", results.head.resourceName)
    verify(configRepository).topicConfig("archive.events")
  }

  @Test
  def testInitProducerIdOutsideInRefusesTenantPrincipalNamespace(): Unit = {
    // Outside-in coordinator-namespace pollution: a privileged caller on a
    // cluster-wide (non-tenant) listener submits `__tenant_acme.tx` as the
    // transactional id. Without the guard, toPhysicalTxnId is identity for
    // non-tenant contexts, the auth check would pass for the super-user, and
    // the InitProducerId would fence acme's producer slot in
    // __transaction_state. Refuse with TRANSACTIONAL_ID_AUTHORIZATION_FAILED
    // before reaching the coordinator.
    val initRequest = new InitProducerIdRequest.Builder(new InitProducerIdRequestData()
      .setTransactionalId("__tenant_acme.tx")
      .setTransactionTimeoutMs(TimeUnit.MINUTES.toMillis(1).toInt)
      .setProducerId(RecordBatch.NO_PRODUCER_ID)
      .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH))
      .build()
    val request = buildRequest(initRequest) // default: cluster-wide listener, "Alice" principal

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleInitProducerIdRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[InitProducerIdResponse](request)
    assertEquals(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code, response.data.errorCode,
      "non-tenant caller naming `__tenant_<known>.X` must be refused — coordinator slot is tenant-owned")
    verify(txnCoordinator, never()).handleInitProducerId(
      any[String](), anyInt(), any[Option[ProducerIdAndEpoch]](),
      any[InitProducerIdResult => Unit](), any[RequestLocal]())
  }

  @Test
  def testInitProducerIdOutsideInRefusesTenantPrincipalPrefixOnNodeWithEmptyTenantConfig(): Unit = {
    // Models the split-mode KRaft / pre-binding-on-this-broker case: the node
    // has NO listener-tenant bindings (TenantConfig.empty), but a cluster-wide
    // admin tries to plant a producer epoch under `__tenant_acme.tx`. The
    // structural guard must still fire here — otherwise the planted epoch
    // becomes a back-door fence once acme is bound on this or any other node.
    // The old narrow check would return false because `allTenants.isEmpty`,
    // letting the planted record through.
    val initRequest = new InitProducerIdRequest.Builder(new InitProducerIdRequestData()
      .setTransactionalId("__tenant_acme.tx")
      .setTransactionTimeoutMs(TimeUnit.MINUTES.toMillis(1).toInt)
      .setProducerId(RecordBatch.NO_PRODUCER_ID)
      .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH))
      .build()
    val request = buildRequest(initRequest) // cluster-wide listener, no tenant binding

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = TenantConfig.empty())
    kafkaApis.handleInitProducerIdRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[InitProducerIdResponse](request)
    assertEquals(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code, response.data.errorCode,
      "the `__tenant_` prefix is reserved even on nodes with no listener bindings — closes split-mode KRaft + pre-binding gap")
    verify(txnCoordinator, never()).handleInitProducerId(
      any[String](), anyInt(), any[Option[ProducerIdAndEpoch]](),
      any[InitProducerIdResult => Unit](), any[RequestLocal]())
  }

  @Test
  def testInitProducerIdOutsideInRefusesUnknownTenantPrincipalPrefix(): Unit = {
    // The `__tenant_` prefix is RESERVED — any `__tenant_<id>.<x>` names the
    // coordinator slot that tenant `<id>` will inherit once `<id>` is bound on
    // any broker. A cluster-wide caller naming `__tenant_unknown.tx` here would
    // otherwise mint a producer-id-and-epoch on that slot before the tenant
    // arrives; the tenant's first InitProducerId on bind would then receive a
    // fenced epoch from another caller. Refuse structurally regardless of
    // whether `unknown` is currently bound (closes pre-binding pollution AND
    // the split-mode KRaft case where the broker has tenants but the
    // controller's TenantConfig is empty).
    val initRequest = new InitProducerIdRequest.Builder(new InitProducerIdRequestData()
      .setTransactionalId("__tenant_unknown.tx")
      .setTransactionTimeoutMs(TimeUnit.MINUTES.toMillis(1).toInt)
      .setProducerId(RecordBatch.NO_PRODUCER_ID)
      .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH))
      .build()
    val request = buildRequest(initRequest)

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleInitProducerIdRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[InitProducerIdResponse](request)
    assertEquals(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code, response.data.errorCode,
      "the `__tenant_` prefix is reserved on every coordinator-keyed RPC — refuse pre-binding pollution attempt")
    verify(txnCoordinator, never()).handleInitProducerId(
      any[String](), anyInt(), any[Option[ProducerIdAndEpoch]](),
      any[InitProducerIdResult => Unit](), any[RequestLocal]())
  }

  @Test
  def testEndTxnOutsideInRefusesTenantPrincipalNamespace(): Unit = {
    // EndTxn routes through rewriteTenantTxnId — the centralized fix covers it.
    // A non-tenant super-user naming `__tenant_acme.tx` would otherwise drive
    // acme's coordinator to abort/commit a transaction it doesn't own.
    val endTxnRequest = new EndTxnRequest.Builder(new EndTxnRequestData()
      .setTransactionalId("__tenant_acme.tx")
      .setProducerId(42L)
      .setProducerEpoch(0.toShort)
      .setCommitted(true), true).build()
    val request = buildRequest(endTxnRequest)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleEndTxnRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[EndTxnResponse](request)
    assertEquals(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code, response.data.errorCode)
    verify(txnCoordinator, never()).handleEndTransaction(
      anyString(), anyLong(), anyShort(), any(), any(),
      any[(Errors, Long, Short) => Unit](), any[RequestLocal]())
  }

  @Test
  def testJoinGroupOutsideInRefusesTenantPrincipalNamespace(): Unit = {
    // JoinGroup routes through rewriteTenantGroupId — centralized fix covers it.
    val joinGroupRequest = new JoinGroupRequest.Builder(new JoinGroupRequestData()
      .setGroupId("__tenant_acme.consumer")
      .setSessionTimeoutMs(10000)
      .setRebalanceTimeoutMs(60000)
      .setProtocolType("consumer")
      .setMemberId("")
      .setProtocols(new JoinGroupRequestData.JoinGroupRequestProtocolCollection())).build()
    val request = buildRequest(joinGroupRequest)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleJoinGroupRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[JoinGroupResponse](request)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.errorCode)
    verify(groupCoordinator, never()).joinGroup(any(), any(), any())
  }

  @Test
  def testAddOffsetsToTxnOutsideInRefusesTenantPrincipalNamespaceTxnId(): Unit = {
    val req = new AddOffsetsToTxnRequest.Builder(new AddOffsetsToTxnRequestData()
      .setGroupId("orders-consumer")
      .setTransactionalId("__tenant_acme.tx")
      .setProducerId(42L)
      .setProducerEpoch(0.toShort)).build(ApiKeys.ADD_OFFSETS_TO_TXN.latestVersion)
    val request = buildRequest(req)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAddOffsetsToTxnRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[AddOffsetsToTxnResponse](request)
    assertEquals(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code, response.data.errorCode)
    verify(groupCoordinator, never()).partitionFor(anyString())
    verify(txnCoordinator, never()).handleAddPartitionsToTransaction(
      anyString(), anyLong(), anyShort(), any[Set[TopicPartition]](),
      any[Errors => Unit](), any[TransactionVersion](), any[RequestLocal]())
  }

  @Test
  def testAddOffsetsToTxnOutsideInRefusesTenantPrincipalNamespaceGroupId(): Unit = {
    val req = new AddOffsetsToTxnRequest.Builder(new AddOffsetsToTxnRequestData()
      .setGroupId("__tenant_acme.consumer")
      .setTransactionalId("my-tx")
      .setProducerId(42L)
      .setProducerEpoch(0.toShort)).build(ApiKeys.ADD_OFFSETS_TO_TXN.latestVersion)
    val request = buildRequest(req)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAddOffsetsToTxnRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[AddOffsetsToTxnResponse](request)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.errorCode)
    verify(groupCoordinator, never()).partitionFor(anyString())
  }

  @Test
  def testAddPartitionsToTxnOutsideInRefusesTenantPrincipalNamespaceV3(): Unit = {
    // v3 = client path; v >= 4 is inter-broker and gated by CLUSTER_ACTION
    // separately. The outside-in guard fires per-transaction on v < 4.
    val tp = new TopicPartition("topic", 0)
    val req = AddPartitionsToTxnRequest.Builder.forClient(
      "__tenant_acme.tx", 42L, 0.toShort, Collections.singletonList(tp)
    ).build(3.toShort)
    val request = buildRequest(req)

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAddPartitionsToTxnRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[AddPartitionsToTxnResponse](request)
    val txnErrors = response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID)
    assertEquals(Collections.singletonMap(tp, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED), txnErrors)
    verify(txnCoordinator, never()).handleAddPartitionsToTransaction(
      anyString(), anyLong(), anyShort(), any[Set[TopicPartition]](),
      any[Errors => Unit](), any[TransactionVersion](), any[RequestLocal]())
  }

  @Test
  def testFindCoordinatorOutsideInRefusesTenantPrincipalNamespaceGroupV4(): Unit = {
    val findCoord = new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
      .setKeyType(CoordinatorType.GROUP.id)
      .setCoordinatorKeys(util.Arrays.asList("__tenant_acme.consumer"))).build()
    val request = buildRequest(findCoord)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleFindCoordinatorRequest(request)

    val response = verifyNoThrottling[FindCoordinatorResponse](request)
    val coords = response.data.coordinators.asScala
    assertEquals(1, coords.size)
    assertEquals("__tenant_acme.consumer", coords.head.key,
      "echo key verbatim so the response cannot probe whether the rewrite happened")
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, coords.head.errorCode)
    verify(groupCoordinator, never()).partitionFor(anyString())
  }

  @Test
  def testFindCoordinatorOutsideInRefusesTenantPrincipalNamespaceTxnV4(): Unit = {
    val findCoord = new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
      .setKeyType(CoordinatorType.TRANSACTION.id)
      .setCoordinatorKeys(util.Arrays.asList("__tenant_acme.tx"))).build()
    val request = buildRequest(findCoord)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleFindCoordinatorRequest(request)

    val response = verifyNoThrottling[FindCoordinatorResponse](request)
    val coords = response.data.coordinators.asScala
    assertEquals(1, coords.size)
    assertEquals("__tenant_acme.tx", coords.head.key)
    assertEquals(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code, coords.head.errorCode)
    verify(txnCoordinator, never()).partitionFor(anyString())
  }

  @Test
  def testOffsetFetchOutsideInRefusesTenantPrincipalNamespace(): Unit = {
    // OffsetFetch routes through rewriteTenantGroupId — the centralized fix
    // covers it. A non-tenant super-user fetching `__tenant_acme.consumer`'s
    // committed offsets would learn the tenant's progress without an ACL on
    // the tenant. Refuse with GROUP_AUTHORIZATION_FAILED.
    val groups = Map[String, java.util.List[TopicPartition]](
      "__tenant_acme.consumer" -> List(new TopicPartition("topic", 0)).asJava
    ).asJava
    val req = new OffsetFetchRequest.Builder(groups, false, false)
      .build(ApiKeys.OFFSET_FETCH.latestVersion)
    val request = buildRequest(req)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleOffsetFetchRequest(request)

    val response = verifyNoThrottling[OffsetFetchResponse](request)
    val groupResult = response.data.groups.asScala.head
    assertEquals("__tenant_acme.consumer", groupResult.groupId,
      "echo groupId verbatim — wire form is what the caller sent")
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, groupResult.errorCode)
  }

  @Test
  def testProduceTenantRewritesTransactionalIdToPhysicalForReplicaManager(): Unit = {
    // Tenant produces transactional records with txnId="my-txn". The wire id
    // hashes to a __transaction_state partition different from the one
    // InitProducerId stored state under (`__tenant_acme.my-txn`), so without
    // rewrite the verification call would miss state AND two tenants reusing
    // the same external id would collide on the coordinator's verification
    // cache key. Assert: replicaManager.handleProduceAppend receives the
    // physical txnId.
    val physicalTopic = "acme.orders"
    addTopicToMetadataCache(physicalTopic, numPartitions = 1)

    val produceRequest = ProduceRequest.builder(new ProduceRequestData()
      .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
        Collections.singletonList(new ProduceRequestData.TopicProduceData()
          .setName("orders").setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(0)
              .setRecords(MemoryRecords.withTransactionalRecords(
                Compression.NONE, 0, 0, 0, new SimpleRecord("test".getBytes))))))
          .iterator))
      .setAcks(1.toShort)
      .setTransactionalId("my-txn")
      .setTimeoutMs(5000))
      .build(ApiKeys.PRODUCE.latestVersion)
    val request = buildRequest(
      produceRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    verify(replicaManager).handleProduceAppend(
      anyLong,
      anyShort,
      ArgumentMatchers.eq(false),
      ArgumentMatchers.eq("__tenant_acme.my-txn"),
      any(),
      any(),
      any(),
      any(),
      any(),
      any())
  }

  @Test
  def testProduceTenantRefusesCrossTenantTransactionalId(): Unit = {
    // Tenant `acme` sets transactionalId="__tenant_other.foo". toPhysicalTxnId
    // would attempt to double-prefix and throw IllegalArgumentException; the
    // handler must surface TRANSACTIONAL_ID_AUTHORIZATION_FAILED so the wire
    // shape doesn't hint that "other" exists or has an outstanding producer.
    val physicalTopic = "acme.orders"
    addTopicToMetadataCache(physicalTopic, numPartitions = 1)

    val produceRequest = ProduceRequest.builder(new ProduceRequestData()
      .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
        Collections.singletonList(new ProduceRequestData.TopicProduceData()
          .setName("orders").setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(0)
              .setRecords(MemoryRecords.withTransactionalRecords(
                Compression.NONE, 0, 0, 0, new SimpleRecord("test".getBytes))))))
          .iterator))
      .setAcks(1.toShort)
      .setTransactionalId("__tenant_other.foo")
      .setTimeoutMs(5000))
      .build(ApiKeys.PRODUCE.latestVersion)
    val request = buildRequest(
      produceRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[ProduceResponse](request)
    assertEquals(1, response.data.responses.size)
    val partitionResponse = response.data.responses.asScala.head.partitionResponses.asScala.head
    assertEquals(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code, partitionResponse.errorCode)
    verify(replicaManager, never()).handleProduceAppend(
      anyLong, anyShort, any[Boolean](), any[String](),
      any(), any(), any(), any(), any(), any())
  }

  @Test
  def testProduceOutsideInRefusesTenantPrincipalNamespaceTxnId(): Unit = {
    // Non-tenant super-user on the cluster-wide listener submits
    // transactionalId="__tenant_acme.tx". Without rewrite, toPhysicalTxnId is
    // identity, the auth check passes for a super-user, and the produce
    // verification call would fence acme's producer slot. Refuse outside-in
    // with TRANSACTIONAL_ID_AUTHORIZATION_FAILED before the request reaches
    // replicaManager.
    val produceRequest = ProduceRequest.builder(new ProduceRequestData()
      .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
        Collections.singletonList(new ProduceRequestData.TopicProduceData()
          .setName("topic").setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(0)
              .setRecords(MemoryRecords.withTransactionalRecords(
                Compression.NONE, 0, 0, 0, new SimpleRecord("test".getBytes))))))
          .iterator))
      .setAcks(1.toShort)
      .setTransactionalId("__tenant_acme.tx")
      .setTimeoutMs(5000))
      .build(ApiKeys.PRODUCE.latestVersion)
    val request = buildRequest(produceRequest) // default: cluster-wide listener, "Alice"

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

    val response = verifyNoThrottling[ProduceResponse](request)
    assertEquals(1, response.data.responses.size)
    val partitionResponse = response.data.responses.asScala.head.partitionResponses.asScala.head
    assertEquals(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code, partitionResponse.errorCode,
      "non-tenant caller naming `__tenant_<known>.X` must be refused before reaching the coordinator")
    verify(replicaManager, never()).handleProduceAppend(
      anyLong, anyShort, any[Boolean](), any[String](),
      any(), any(), any(), any(), any(), any())
  }

  @Test
  def testDeleteGroupsOutsideInRefusesTenantPrincipalNamespace(): Unit = {
    // Non-tenant cluster-wide caller asks to delete `__tenant_acme.consumer`.
    // The dispatch gate doesn't fire (DELETE_GROUPS isn't in TENANT_ALLOWED_APIS,
    // but the caller is non-tenant); without an outside-in guard the call would
    // reach groupCoordinator.deleteGroups and drop acme's coordinator record.
    val req = new DeleteGroupsRequest.Builder(new DeleteGroupsRequestData()
      .setGroupsNames(List("__tenant_acme.consumer", "regular-group").asJava)).build()
    val request = buildRequest(req)

    val future = new CompletableFuture[DeleteGroupsResponseData.DeletableGroupResultCollection]()
    when(groupCoordinator.deleteGroups(
      request.context,
      List("regular-group").asJava,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDeleteGroupsRequest(request, RequestLocal.noCaching)

    future.complete(new DeleteGroupsResponseData.DeletableGroupResultCollection(List(
      new DeleteGroupsResponseData.DeletableGroupResult()
        .setGroupId("regular-group").setErrorCode(Errors.NONE.code)
    ).iterator.asJava))

    val response = verifyNoThrottling[DeleteGroupsResponse](request)
    val results = response.data.results.asScala.map(r => r.groupId -> r.errorCode).toMap
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, results("__tenant_acme.consumer"),
      "reserved-form group must be refused before the coordinator call")
    assertEquals(Errors.NONE.code, results("regular-group"),
      "non-reserved groups in the same batch must still be processed")
  }

  @Test
  def testOffsetDeleteOutsideInRefusesTenantPrincipalNamespace(): Unit = {
    val req = new OffsetDeleteRequest.Builder(new OffsetDeleteRequestData()
      .setGroupId("__tenant_acme.consumer")
      .setTopics(new OffsetDeleteRequestTopicCollection(List(
        new OffsetDeleteRequestTopic().setName("topic").setPartitions(List(
          new OffsetDeleteRequestPartition().setPartitionIndex(0)).asJava)
      ).iterator.asJava))).build()
    val request = buildRequest(req)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleOffsetDeleteRequest(request, RequestLocal.noCaching)

    val response = verifyNoThrottling[OffsetDeleteResponse](request)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.errorCode,
      "reserved-form groupId must be refused before reaching the coordinator")
    verify(groupCoordinator, never()).deleteOffsets(any(), any(), any())
  }

  @Test
  def testConsumerGroupHeartbeatOutsideInRefusesTenantPrincipalNamespace(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])
    val req = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData().setGroupId("__tenant_acme.consumer")).build()
    val request = buildRequest(req)

    kafkaApis = createKafkaApis(
      featureVersions = Seq(GroupVersion.GV_1),
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleConsumerGroupHeartbeat(request)

    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](request)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.errorCode,
      "reserved-form groupId must be refused before the new group coordinator")
    verify(groupCoordinator, never()).consumerGroupHeartbeat(any(), any())
  }

  @Test
  def testConsumerGroupDescribeOutsideInRefusesTenantPrincipalNamespace(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])
    val req = new ConsumerGroupDescribeRequest.Builder(
      new ConsumerGroupDescribeRequestData().setGroupIds(
        List("__tenant_acme.consumer", "regular-group").asJava)).build()
    val request = buildRequest(req)

    when(groupCoordinator.consumerGroupDescribe(any(), any()))
      .thenReturn(CompletableFuture.completedFuture(List(
        new ConsumerGroupDescribeResponseData.DescribedGroup()
          .setGroupId("regular-group").setErrorCode(Errors.NONE.code)
      ).asJava))

    kafkaApis = createKafkaApis(
      featureVersions = Seq(GroupVersion.GV_1),
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleConsumerGroupDescribe(request)

    val response = verifyNoThrottling[ConsumerGroupDescribeResponse](request)
    val results = response.data.groups.asScala.map(g => g.groupId -> g.errorCode).toMap
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, results("__tenant_acme.consumer"),
      "reserved-form group must be refused per-entry")
    assertEquals(Errors.NONE.code, results("regular-group"),
      "non-reserved groups in the same batch must still be processed")
  }

  @Test
  def testDescribeGroupsOutsideInRefusesTenantPrincipalNamespace(): Unit = {
    val req = new DescribeGroupsRequest.Builder(new DescribeGroupsRequestData()
      .setGroups(List("__tenant_acme.consumer", "regular-group").asJava)).build()
    val request = buildRequest(req)

    when(groupCoordinator.describeGroups(any(), any()))
      .thenReturn(CompletableFuture.completedFuture(List(
        new DescribeGroupsResponseData.DescribedGroup()
          .setGroupId("regular-group").setErrorCode(Errors.NONE.code)
      ).asJava))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeGroupsRequest(request)

    val response = verifyNoThrottling[DescribeGroupsResponse](request)
    val results = response.data.groups.asScala.map(g => g.groupId -> g.errorCode).toMap
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, results("__tenant_acme.consumer"),
      "reserved-form group must be refused per-entry")
    assertEquals(Errors.NONE.code, results("regular-group"))
  }

  @Test
  def testShareGroupHeartbeatOutsideInRefusesTenantPrincipalNamespace(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    val req = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData().setGroupId("__tenant_acme.consumer"), true).build()
    val request = buildRequest(req)

    kafkaApis = createKafkaApis(
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleShareGroupHeartbeat(request)

    val response = verifyNoThrottling[ShareGroupHeartbeatResponse](request)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.errorCode,
      "reserved-form groupId must be refused before reaching the share-group coordinator")
    verify(groupCoordinator, never()).shareGroupHeartbeat(any(), any())
  }

  // ---------------------------------------------------------------------------
  // ShareGroupHeartbeat subscribedTopicNames outside-in scrub (#133)
  //
  // Mirrors the consumer-side #132 fix. The existing groupId guard above only
  // inspects `groupId`. A cluster-wide caller can still pass an innocuous
  // groupId and `subscribedTopicNames=["acme.orders"]`; without this guard the
  // new share-group coordinator would record the subscription against the
  // tenant's physical topic — leaking topic existence and end offsets through
  // subsequent heartbeat assignments, and letting a non-tenant principal
  // disrupt the tenant's share-rebalance protocol. Refuse the whole heartbeat
  // with TOPIC_AUTHORIZATION_FAILED before the coordinator is touched.
  // ---------------------------------------------------------------------------
  @Test
  def testShareGroupHeartbeatOutsideInRefusesReservedSubscribedTopicName(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    val req = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("regular-group")
        .setSubscribedTopicNames(List("regular-topic", "acme.orders").asJava),
      true).build()
    val request = buildRequest(req)

    kafkaApis = createKafkaApis(
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleShareGroupHeartbeat(request)

    val response = verifyNoThrottling[ShareGroupHeartbeatResponse](request)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, response.data.errorCode,
      "reserved-namespace topic in share subscribedTopicNames must be refused before the coordinator")
    verify(groupCoordinator, never()).shareGroupHeartbeat(any(), any())
  }

  @Test
  def testShareGroupHeartbeatClusterWideListenerForwardsDottedNamesWhenNoTenantsConfigured(): Unit = {
    // No tenants configured: `acme.orders` is just a dotted topic name. The
    // outside-in guard must not fire; the request goes through to the share
    // coordinator as on stock Kafka. Pins the "no-tenants → stock semantics"
    // contract for the share-heartbeat path.
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    val data = new ShareGroupHeartbeatRequestData()
      .setGroupId("regular-group")
      .setSubscribedTopicNames(List("acme.orders").asJava)
    val req = new ShareGroupHeartbeatRequest.Builder(data, true).build()
    val request = buildRequest(req)

    val future = new CompletableFuture[ShareGroupHeartbeatResponseData]()
    when(groupCoordinator.shareGroupHeartbeat(request.context, data)).thenReturn(future)

    kafkaApis = createKafkaApis(
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"))
    kafkaApis.handleShareGroupHeartbeat(request)

    val coordinatorResponse = new ShareGroupHeartbeatResponseData().setMemberId("m")
    future.complete(coordinatorResponse)
    val response = verifyNoThrottling[ShareGroupHeartbeatResponse](request)
    assertEquals(coordinatorResponse, response.data,
      "with no tenants configured the dotted topic name is not reserved on share heartbeat")
  }

  @Test
  def testShareGroupDescribeOutsideInRefusesTenantPrincipalNamespace(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    val req = new ShareGroupDescribeRequest.Builder(
      new ShareGroupDescribeRequestData().setGroupIds(
        List("__tenant_acme.consumer", "regular-group").asJava), true).build()
    val request = buildRequest(req)

    when(groupCoordinator.shareGroupDescribe(any(), any()))
      .thenReturn(CompletableFuture.completedFuture(List(
        new ShareGroupDescribeResponseData.DescribedGroup()
          .setGroupId("regular-group").setErrorCode(Errors.NONE.code)
      ).asJava))

    kafkaApis = createKafkaApis(
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleShareGroupDescribe(request)

    val response = verifyNoThrottling[ShareGroupDescribeResponse](request)
    val results = response.data.groups.asScala.map(g => g.groupId -> g.errorCode).toMap
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, results("__tenant_acme.consumer"),
      "reserved-form group must be refused per-entry on share-group describe")
    assertEquals(Errors.NONE.code, results("regular-group"))
  }

  @Test
  def testShareFetchOutsideInRefusesTenantPrincipalNamespace(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val req = new ShareFetchRequest.Builder(new ShareFetchRequestData()
      .setGroupId("__tenant_acme.consumer")
      .setMemberId(Uuid.ZERO_UUID.toString)
      .setShareSessionEpoch(1)).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(req)

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleShareFetchRequest(request)

    val response = verifyNoThrottling[ShareFetchResponse](request)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.errorCode,
      "reserved-form groupId must be refused before share-fetch context is built")
    verify(sharePartitionManager, never()).newContext(anyString(), any(), any(), any(), anyBoolean())
  }

  @Test
  def testShareAcknowledgeOutsideInRefusesTenantPrincipalNamespace(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val req = new ShareAcknowledgeRequest.Builder(new ShareAcknowledgeRequestData()
      .setGroupId("__tenant_acme.consumer")
      .setMemberId(Uuid.ZERO_UUID.toString)
      .setShareSessionEpoch(1)).build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(req)

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleShareAcknowledgeRequest(request)

    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.errorCode,
      "reserved-form groupId must be refused before share-acknowledge proceeds")
    verify(sharePartitionManager, never()).acknowledgeSessionUpdate(anyString(), any())
  }

  // The dispatch gate refuses tenant principals from LIST_GROUPS (not in
  // TENANT_ALLOWED_APIS). The remaining outside-in vector is a non-tenant
  // cluster admin: their wildcard DESCRIBE GROUP would otherwise return every
  // tenant's physical `__tenant_*.*` group id verbatim, enumerating which
  // tenants exist on the broker. The handler now filters those entries out.
  @Test
  def testListGroupsOutsideInFiltersTenantPrincipalNamespace(): Unit = {
    val listGroupsRequest = new ListGroupsRequestData()
    val request = buildRequest(new ListGroupsRequest.Builder(listGroupsRequest).build())

    val future = new CompletableFuture[ListGroupsResponseData]()
    when(groupCoordinator.listGroups(request.context, listGroupsRequest)).thenReturn(future)
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleListGroupsRequest(request)

    future.complete(new ListGroupsResponseData().setGroups(List(
      new ListGroupsResponseData.ListedGroup().setGroupId("__tenant_acme.consumer"),
      new ListGroupsResponseData.ListedGroup().setGroupId("regular-group")
    ).asJava))

    val response = verifyNoThrottling[ListGroupsResponse](request)
    val visible = response.data.groups.asScala.map(_.groupId).toSet
    assertEquals(Set("regular-group"), visible,
      "non-tenant caller must not see tenant-internal group ids in the listing")
  }

  @Test
  def testDescribeTransactionsOutsideInRefusesTenantPrincipalNamespace(): Unit = {
    val data = new DescribeTransactionsRequestData()
      .setTransactionalIds(List("__tenant_acme.checkout-tx", "regular-txn").asJava)
    val request = buildRequest(new DescribeTransactionsRequest.Builder(data).build())
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    when(txnCoordinator.handleDescribeTransactions("regular-txn"))
      .thenReturn(new DescribeTransactionsResponseData.TransactionState()
        .setErrorCode(Errors.NONE.code)
        .setTransactionalId("regular-txn")
        .setProducerId(7L)
        .setProducerEpoch(1)
        .setTransactionState("Ongoing")
        .setTransactionTimeoutMs(60_000))
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeTransactionsRequest(request)

    val response = verifyNoThrottling[DescribeTransactionsResponse](request)
    val states = response.data.transactionStates.asScala.map(s => s.transactionalId -> s.errorCode).toMap
    assertEquals(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code, states("__tenant_acme.checkout-tx"),
      "reserved-form txnId must be refused without consulting the coordinator")
    assertEquals(Errors.NONE.code, states("regular-txn"),
      "sibling non-reserved txnId must still succeed in the same batch")
    verify(txnCoordinator, never()).handleDescribeTransactions(ArgumentMatchers.eq("__tenant_acme.checkout-tx"))
  }

  @Test
  def testDescribeProducersOutsideInRefusesTenantPhysicalTopic(): Unit = {
    // A non-tenant caller on the cluster-wide listener asking for the producer
    // state of `acme.orders` (the PHYSICAL form of acme's logical `orders`)
    // could otherwise distinguish "topic doesn't exist" (UNKNOWN_TOPIC_OR_PARTITION)
    // from "topic exists and these producers are writing to it" (NONE +
    // ProducerState[]). The mismatch is an existence oracle on tenant topics,
    // and on a hit it leaks producerId / producerEpoch / currentTxnStartOffset —
    // enough to issue a forged WriteTxnMarkers and fence the tenant's producer.
    // Refuse before consulting metadataCache or replicaManager, returning the
    // same TOPIC_AUTHORIZATION_FAILED shape that an authz refusal produces.
    val acmeOrders = new TopicPartition("acme.orders", 0)
    val regular = new TopicPartition("regular-topic", 0)

    val data = new DescribeProducersRequestData().setTopics(List(
      new DescribeProducersRequestData.TopicRequest()
        .setName(acmeOrders.topic)
        .setPartitionIndexes(List(Int.box(acmeOrders.partition)).asJava),
      new DescribeProducersRequestData.TopicRequest()
        .setName(regular.topic)
        .setPartitionIndexes(List(Int.box(regular.partition)).asJava)
    ).asJava)
    val describeProducersRequest = new DescribeProducersRequest.Builder(data).build()
    val request = buildRequest(describeProducersRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    // Both physical topics exist in metadata; without the guard, the response
    // would diverge across the existence axis (the second-stage check is
    // metadataCache.contains).
    addTopicToMetadataCache(acmeOrders.topic, numPartitions = 1)
    addTopicToMetadataCache(regular.topic, numPartitions = 1)
    when(replicaManager.activeProducerState(regular))
      .thenReturn(new DescribeProducersResponseData.PartitionResponse()
        .setErrorCode(Errors.NONE.code)
        .setPartitionIndex(regular.partition)
        .setActiveProducers(List(
          new DescribeProducersResponseData.ProducerState()
            .setProducerId(42L)
            .setProducerEpoch(3)
        ).asJava))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeProducersRequest(request)

    val response = verifyNoThrottling[DescribeProducersResponse](request)
    val byTopic = response.data.topics.asScala.map(t => t.name -> t).toMap

    val acmePart = byTopic(acmeOrders.topic).partitions.asScala.find(_.partitionIndex == acmeOrders.partition).get
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, acmePart.errorCode,
      "reserved-physical-form topic must be refused with TOPIC_AUTHORIZATION_FAILED")
    assertTrue(acmePart.activeProducers.isEmpty,
      "refused topic must not leak any producer state")
    val regularPart = byTopic(regular.topic).partitions.asScala.find(_.partitionIndex == regular.partition).get
    assertEquals(Errors.NONE.code, regularPart.errorCode,
      "sibling non-reserved topic must still succeed in the same batch")
    assertEquals(1, regularPart.activeProducers.size)
    assertEquals(42L, regularPart.activeProducers.get(0).producerId)

    // The guard must short-circuit before reaching replicaManager — otherwise
    // a probe could time the partition lookup and still derive existence.
    verify(replicaManager, never()).activeProducerState(acmeOrders)
  }

  @Test
  def testDescribeProducersClusterWideListenerForwardsTenantLookingNamesWhenNoTenantsConfigured(): Unit = {
    // Without any configured tenants, the `<id>.<topic>` form is not a tenant
    // reservation — it is just a topic name with a dot. The guard must not
    // fire here, otherwise legitimate non-tenant clusters lose the ability to
    // describe producers for any topic with a dot in its name.
    val dotted = new TopicPartition("acme.orders", 0)
    val data = new DescribeProducersRequestData().setTopics(List(
      new DescribeProducersRequestData.TopicRequest()
        .setName(dotted.topic)
        .setPartitionIndexes(List(Int.box(dotted.partition)).asJava)
    ).asJava)
    val request = buildRequest(new DescribeProducersRequest.Builder(data).build())
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    addTopicToMetadataCache(dotted.topic, numPartitions = 1)
    when(replicaManager.activeProducerState(dotted))
      .thenReturn(new DescribeProducersResponseData.PartitionResponse()
        .setErrorCode(Errors.NONE.code)
        .setPartitionIndex(dotted.partition)
        .setActiveProducers(List(
          new DescribeProducersResponseData.ProducerState()
            .setProducerId(99L).setProducerEpoch(1)
        ).asJava))

    kafkaApis = createKafkaApis()
    kafkaApis.handleDescribeProducersRequest(request)

    val response = verifyNoThrottling[DescribeProducersResponse](request)
    val part = response.data.topics.asScala.head.partitions.asScala.head
    assertEquals(Errors.NONE.code, part.errorCode,
      "with no tenants configured the dotted topic name is not reserved")
    assertEquals(99L, part.activeProducers.get(0).producerId)
  }

  // ---------------------------------------------------------------------------
  // OffsetForLeaderEpoch outside-in scrub (#131)
  //
  // The handler grants CLUSTER_ACTION to a cluster-wide caller and then hands
  // each topic to `replicaManager.lastOffsetForLeaderEpoch`. Without the
  // outside-in guard, a non-tenant caller naming `acme.orders` (the PHYSICAL
  // form of acme's logical `orders`) would receive real `leaderEpoch` and
  // `endOffset` for the tenant's storage — an existence oracle plus an
  // end-offset leak plus a truncation oracle when paired with a spoofed
  // current epoch. The guard refuses per-entry with TOPIC_AUTHORIZATION_FAILED,
  // matching the exact wire shape of the existing DESCRIBE-deny branch.
  // ---------------------------------------------------------------------------
  @Test
  def testOffsetForLeaderEpochOutsideInRefusesTenantPhysicalTopic(): Unit = {
    val reservedTopic = "acme.orders"
    val regularTopic = "regular-topic"
    val partition = 0

    val topics = new OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection()
    topics.add(new OffsetForLeaderEpochRequestData.OffsetForLeaderTopic()
      .setTopic(reservedTopic)
      .setPartitions(List(new OffsetForLeaderEpochRequestData.OffsetForLeaderPartition()
        .setPartition(partition).setLeaderEpoch(0).setCurrentLeaderEpoch(-1)).asJava))
    topics.add(new OffsetForLeaderEpochRequestData.OffsetForLeaderTopic()
      .setTopic(regularTopic)
      .setPartitions(List(new OffsetForLeaderEpochRequestData.OffsetForLeaderPartition()
        .setPartition(partition).setLeaderEpoch(0).setCurrentLeaderEpoch(-1)).asJava))

    val request = buildRequest(OffsetsForLeaderEpochRequest.Builder.forConsumer(topics).build())
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    // Mock the regular topic returning a populated `EpochEndOffset`. Without
    // the outside-in guard, replicaManager would also receive the reserved
    // topic and a similar shape would leak the tenant's end offset.
    when(replicaManager.lastOffsetForLeaderEpoch(any[Seq[OffsetForLeaderEpochRequestData.OffsetForLeaderTopic]]))
      .thenAnswer { invocation =>
        val passed = invocation.getArgument[Seq[OffsetForLeaderEpochRequestData.OffsetForLeaderTopic]](0)
        passed.map { t =>
          new OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult()
            .setTopic(t.topic)
            .setPartitions(t.partitions.asScala.map { p =>
              new OffsetForLeaderEpochResponseData.EpochEndOffset()
                .setPartition(p.partition)
                .setErrorCode(Errors.NONE.code)
                .setLeaderEpoch(7)
                .setEndOffset(123L)
            }.toList.asJava)
        }
      }

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleOffsetForLeaderEpochRequest(request)

    val response = verifyNoThrottling[OffsetsForLeaderEpochResponse](request)
    val byTopic = response.data.topics.asScala.map(t => t.topic -> t).toMap

    val acmePart = byTopic(reservedTopic).partitions.asScala.find(_.partition == partition).get
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, acmePart.errorCode,
      "reserved-physical-form topic must be refused with TOPIC_AUTHORIZATION_FAILED")
    assertEquals(-1, acmePart.leaderEpoch,
      "refused topic must not leak leaderEpoch — wire shape must match a DESCRIBE-deny")
    assertEquals(-1L, acmePart.endOffset,
      "refused topic must not leak endOffset — wire shape must match a DESCRIBE-deny")

    val regularPart = byTopic(regularTopic).partitions.asScala.find(_.partition == partition).get
    assertEquals(Errors.NONE.code, regularPart.errorCode,
      "non-reserved topic in the same batch must still be processed")
    assertEquals(7, regularPart.leaderEpoch)
    assertEquals(123L, regularPart.endOffset)

    // Per-entry refusal: the reserved topic must never reach replicaManager,
    // otherwise a probe could time the call and still derive existence.
    val captor: ArgumentCaptor[Seq[OffsetForLeaderEpochRequestData.OffsetForLeaderTopic]] =
      ArgumentCaptor.forClass(classOf[Seq[OffsetForLeaderEpochRequestData.OffsetForLeaderTopic]])
    verify(replicaManager).lastOffsetForLeaderEpoch(captor.capture())
    val invokedTopics = captor.getValue.map(_.topic).toSet
    assertEquals(Set(regularTopic), invokedTopics,
      "replicaManager.lastOffsetForLeaderEpoch must only see the non-reserved entry")
  }

  @Test
  def testOffsetForLeaderEpochClusterWideListenerForwardsTenantLookingNamesWhenNoTenantsConfigured(): Unit = {
    // Without any configured tenants, `acme.orders` is just a dotted topic
    // name; the outside-in guard must not fire — stock Kafka behaviour must
    // be preserved on non-tenant clusters.
    val topicName = "acme.orders"
    val partition = 0

    val topics = new OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection()
    topics.add(new OffsetForLeaderEpochRequestData.OffsetForLeaderTopic()
      .setTopic(topicName)
      .setPartitions(List(new OffsetForLeaderEpochRequestData.OffsetForLeaderPartition()
        .setPartition(partition).setLeaderEpoch(0).setCurrentLeaderEpoch(-1)).asJava))

    val request = buildRequest(OffsetsForLeaderEpochRequest.Builder.forConsumer(topics).build())
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    when(replicaManager.lastOffsetForLeaderEpoch(any[Seq[OffsetForLeaderEpochRequestData.OffsetForLeaderTopic]]))
      .thenAnswer { invocation =>
        val passed = invocation.getArgument[Seq[OffsetForLeaderEpochRequestData.OffsetForLeaderTopic]](0)
        passed.map { t =>
          new OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult()
            .setTopic(t.topic)
            .setPartitions(t.partitions.asScala.map { p =>
              new OffsetForLeaderEpochResponseData.EpochEndOffset()
                .setPartition(p.partition)
                .setErrorCode(Errors.NONE.code)
                .setLeaderEpoch(2)
                .setEndOffset(42L)
            }.toList.asJava)
        }
      }

    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetForLeaderEpochRequest(request)

    val response = verifyNoThrottling[OffsetsForLeaderEpochResponse](request)
    val part = response.data.topics.asScala.head.partitions.asScala.head
    assertEquals(Errors.NONE.code, part.errorCode,
      "with no tenants configured the dotted topic name is not reserved")
    assertEquals(2, part.leaderEpoch)
    assertEquals(42L, part.endOffset)
  }

  @Test
  def testOffsetForLeaderEpochLegitimateFollowerOnInterBrokerListenerSkipsOutsideInGuard(): Unit = {
    // #146 — sibling of #112's `testFetchLegitimateFollowerOnInterBrokerListenerSkipsOutsideInGuard`.
    //
    // A real replica fetcher (`RemoteLeaderEndPoint.fetchEpochEndOffsets`) sends
    // OFLE with `replicaId == brokerConfig.brokerId` over the inter-broker
    // listener every time a follower needs to re-anchor its log against a
    // leader-epoch change. Without `isInterBrokerFollowerOffsetForLeaderEpoch`
    // the outside-in guard added by #131 would refuse every tenant-prefixed
    // partition with TOPIC_AUTHORIZATION_FAILED, breaking log-truncation
    // cycles and silently shrinking the ISR after any epoch bump.
    val reservedTopic = "acme.orders"
    val partition = 0

    val topics = new OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection()
    topics.add(new OffsetForLeaderEpochRequestData.OffsetForLeaderTopic()
      .setTopic(reservedTopic)
      .setPartitions(List(new OffsetForLeaderEpochRequestData.OffsetForLeaderPartition()
        .setPartition(partition).setLeaderEpoch(0).setCurrentLeaderEpoch(-1)).asJava))

    // forFollower(topics, replicaId=2): replicaId>=0; default listener in test
    // setup is PLAINTEXT which is also the inter-broker listener — the trust
    // pin matches.
    val request = buildRequest(
      OffsetsForLeaderEpochRequest.Builder.forFollower(topics, 2).build())
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    when(replicaManager.lastOffsetForLeaderEpoch(any[Seq[OffsetForLeaderEpochRequestData.OffsetForLeaderTopic]]))
      .thenAnswer { invocation =>
        val passed = invocation.getArgument[Seq[OffsetForLeaderEpochRequestData.OffsetForLeaderTopic]](0)
        passed.map { t =>
          new OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult()
            .setTopic(t.topic)
            .setPartitions(t.partitions.asScala.map { p =>
              new OffsetForLeaderEpochResponseData.EpochEndOffset()
                .setPartition(p.partition)
                .setErrorCode(Errors.NONE.code)
                .setLeaderEpoch(11)
                .setEndOffset(456L)
            }.toList.asJava)
        }
      }

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleOffsetForLeaderEpochRequest(request)

    val response = verifyNoThrottling[OffsetsForLeaderEpochResponse](request)
    val part = response.data.topics.asScala.head.partitions.asScala.head
    assertEquals(Errors.NONE.code, part.errorCode,
      "legitimate inter-broker follower OFLE on tenant-prefixed topic must NOT be refused — replication would otherwise silently break")
    assertEquals(11, part.leaderEpoch)
    assertEquals(456L, part.endOffset)

    // Defence-in-depth witness: replicaManager must actually see the
    // tenant-prefixed topic (i.e. the guard's partition step did NOT bucket
    // it into pollutionRejectedTopics).
    val captor: ArgumentCaptor[Seq[OffsetForLeaderEpochRequestData.OffsetForLeaderTopic]] =
      ArgumentCaptor.forClass(classOf[Seq[OffsetForLeaderEpochRequestData.OffsetForLeaderTopic]])
    verify(replicaManager).lastOffsetForLeaderEpoch(captor.capture())
    assertEquals(Set(reservedTopic), captor.getValue.map(_.topic).toSet,
      "follower-side OFLE must reach replicaManager.lastOffsetForLeaderEpoch unchanged")
  }

  @Test
  def testOffsetForLeaderEpochSpoofedFollowerOnClusterWideListenerStillRefused(): Unit = {
    // #146 complement: the inter-broker exemption is listener-pinned. A
    // cluster-wide caller with CLUSTER_ACTION on a non-inter-broker listener
    // cannot bypass the #131 outside-in guard by flipping replicaId from -1 to
    // 99. This protects against the OFLE flavour of the Fetch-follower spoof
    // closed by #112 / `testFetchFollowerSpoofOnClusterWideListenerRefusesTenantPhysicalForm`.
    val attackerListener = new ListenerName("EXTERNAL_SASL")
    val reservedTopic = "acme.orders"
    val partition = 0

    val topics = new OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection()
    topics.add(new OffsetForLeaderEpochRequestData.OffsetForLeaderTopic()
      .setTopic(reservedTopic)
      .setPartitions(List(new OffsetForLeaderEpochRequestData.OffsetForLeaderPartition()
        .setPartition(partition).setLeaderEpoch(0).setCurrentLeaderEpoch(-1)).asJava))

    val request = buildRequest(
      OffsetsForLeaderEpochRequest.Builder.forFollower(topics, 99).build(),
      listenerName = attackerListener,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "cluster-admin"))
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    // cleanAuthorizedTopics is empty after the guard partitions the reserved
    // topic into pollutionRejectedTopics; stub a benign empty answer so the
    // handler can compose the response.
    when(replicaManager.lastOffsetForLeaderEpoch(any[Seq[OffsetForLeaderEpochRequestData.OffsetForLeaderTopic]]))
      .thenReturn(Seq.empty[OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult])

    kafkaApis = createKafkaApis(
      authorizer = None,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleOffsetForLeaderEpochRequest(request)

    val response = verifyNoThrottling[OffsetsForLeaderEpochResponse](request)
    val part = response.data.topics.asScala.head.partitions.asScala.head
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, part.errorCode,
      "spoofed-follower OFLE on a non-inter-broker listener must still be refused — replicaId is client-controlled, the listener is the trust witness")
    assertEquals(-1, part.leaderEpoch,
      "refused topic must not leak leaderEpoch")
    assertEquals(-1L, part.endOffset,
      "refused topic must not leak endOffset")

    // The reserved topic must never reach replicaManager — otherwise a probe
    // could time the partition lookup and still derive existence.
    val captor: ArgumentCaptor[Seq[OffsetForLeaderEpochRequestData.OffsetForLeaderTopic]] =
      ArgumentCaptor.forClass(classOf[Seq[OffsetForLeaderEpochRequestData.OffsetForLeaderTopic]])
    verify(replicaManager).lastOffsetForLeaderEpoch(captor.capture())
    assertFalse(captor.getValue.exists(_.topic == reservedTopic),
      "reserved-physical topic must never reach replicaManager — probe-by-timing must be impossible")
  }

  // ---------------------------------------------------------------------------
  // ConsumerGroupHeartbeat (KIP-848) subscribedTopicNames outside-in scrub (#132)
  //
  // The existing groupId guard (#63) only inspects `groupId`. A cluster-wide
  // caller can still pass `groupId="regular-group"` and
  // `subscribedTopicNames=["acme.orders"]`; the new group coordinator would
  // then record the subscription against the tenant's physical topic and the
  // assignment surfaces the physical name back to the caller. Refuse the whole
  // heartbeat with TOPIC_AUTHORIZATION_FAILED — the same wire shape the
  // sibling topic-authz refusal already produces.
  // ---------------------------------------------------------------------------
  @Test
  def testConsumerGroupHeartbeatOutsideInRefusesReservedSubscribedTopicName(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])
    val req = new ConsumerGroupHeartbeatRequest.Builder(
      new ConsumerGroupHeartbeatRequestData()
        .setGroupId("regular-group")
        .setSubscribedTopicNames(List("regular-topic", "acme.orders").asJava)
    ).build()
    val request = buildRequest(req)

    kafkaApis = createKafkaApis(
      featureVersions = Seq(GroupVersion.GV_1),
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleConsumerGroupHeartbeat(request)

    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](request)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, response.data.errorCode,
      "reserved-namespace topic in subscribedTopicNames must be refused before the coordinator")
    verify(groupCoordinator, never()).consumerGroupHeartbeat(any(), any())
  }

  @Test
  def testConsumerGroupHeartbeatClusterWideListenerForwardsTenantLookingNamesWhenNoTenantsConfigured(): Unit = {
    // No tenants configured: `acme.orders` is just a dotted topic name. The
    // outside-in guard must not fire; the request goes through to the
    // coordinator as on stock Kafka.
    metadataCache = mock(classOf[KRaftMetadataCache])
    val data = new ConsumerGroupHeartbeatRequestData()
      .setGroupId("regular-group")
      .setSubscribedTopicNames(List("acme.orders").asJava)
    val req = new ConsumerGroupHeartbeatRequest.Builder(data).build()
    val request = buildRequest(req)

    val future = new CompletableFuture[ConsumerGroupHeartbeatResponseData]()
    when(groupCoordinator.consumerGroupHeartbeat(request.context, data)).thenReturn(future)

    kafkaApis = createKafkaApis(featureVersions = Seq(GroupVersion.GV_1))
    kafkaApis.handleConsumerGroupHeartbeat(request)

    val coordinatorResponse = new ConsumerGroupHeartbeatResponseData().setMemberId("m")
    future.complete(coordinatorResponse)
    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](request)
    assertEquals(coordinatorResponse, response.data,
      "with no tenants configured the dotted topic name is not reserved")
  }

  @Test
  def testDescribeTopicPartitionsAllTopicsSilentlyDropsTenantPhysicalTopics(): Unit = {
    // fetchAllTopics path: the handler iterates metadataCache.getAllTopics()
    // directly and forwards every topic the caller is authorized to DESCRIBE.
    // A non-tenant caller on the cluster-wide listener with a permissive
    // `User:* DESCRIBE Topic:*` ACL would otherwise enumerate every tenant's
    // physical topic names (`acme.orders`, `acme.payments`, ...) — exactly the
    // existence-oracle / tenant-enumeration leak we close on the Metadata path.
    // Reserved entries must be silently dropped (no error rows, no nextCursor
    // perturbation), matching the existing per-topic ACL-deny shape on the
    // all-topics path.
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    addTopicToMetadataCache("acme.orders", numPartitions = 1)
    addTopicToMetadataCache("acme.payments", numPartitions = 1)
    addTopicToMetadataCache("regular-topic", numPartitions = 1)

    val req = new DescribeTopicPartitionsRequest(new DescribeTopicPartitionsRequestData())
    val request = buildRequest(req)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeTopicPartitionsRequest(request)

    val response = verifyNoThrottling[DescribeTopicPartitionsResponse](request)
    val names = response.data.topics.asScala.map(_.name).toSet
    assertEquals(Set("regular-topic"), names,
      "reserved-physical-form topics must be silently dropped from the all-topics response")
  }

  @Test
  def testDescribeTopicPartitionsExplicitListRefusesTenantPhysicalTopic(): Unit = {
    // Explicit-list path: a non-tenant caller naming `acme.orders` directly
    // would otherwise receive the full partition metadata (leader id, replicas,
    // ISR, ELR) for acme's physical topic — enough to drive a targeted DoS or
    // a replica-targeted produce attack. The guard must surface the wire shape
    // an authz refusal already produces: TOPIC_AUTHORIZATION_FAILED, ZERO_UUID,
    // empty partitions, isInternal=false. Sibling non-reserved topics in the
    // same request batch must still resolve normally.
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    addTopicToMetadataCache("acme.orders", numPartitions = 1)
    addTopicToMetadataCache("regular-topic", numPartitions = 1)

    val data = new DescribeTopicPartitionsRequestData().setTopics(List(
      new DescribeTopicPartitionsRequestData.TopicRequest().setName("acme.orders"),
      new DescribeTopicPartitionsRequestData.TopicRequest().setName("regular-topic")
    ).asJava)
    val request = buildRequest(new DescribeTopicPartitionsRequest(data))
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeTopicPartitionsRequest(request)

    val response = verifyNoThrottling[DescribeTopicPartitionsResponse](request)
    val byName = response.data.topics.asScala.map(t => t.name -> t).toMap

    val acme = byName("acme.orders")
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, acme.errorCode,
      "reserved-physical-form topic must be refused with TOPIC_AUTHORIZATION_FAILED")
    assertEquals(Uuid.ZERO_UUID, acme.topicId,
      "refused topic must not leak its uuid")
    assertTrue(acme.partitions.isEmpty,
      "refused topic must not leak partition layout")
    assertFalse(acme.isInternal,
      "refused topic must not be marked internal")

    val regular = byName("regular-topic")
    assertEquals(Errors.NONE.code, regular.errorCode,
      "sibling non-reserved topic must still succeed in the same batch")
    assertFalse(regular.partitions.isEmpty,
      "non-reserved topic must carry its normal partition data")
  }

  @Test
  def testDescribeTopicPartitionsClusterWideListenerKeepsDottedNamesWhenNoTenantsConfigured(): Unit = {
    // Without any configured tenants, `<id>.<topic>` is just a topic name with
    // a dot — the guard must not fire, otherwise legitimate non-tenant clusters
    // would suddenly lose the ability to describe topics with dots in their
    // names.
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    addTopicToMetadataCache("acme.orders", numPartitions = 1)

    val data = new DescribeTopicPartitionsRequestData().setTopics(List(
      new DescribeTopicPartitionsRequestData.TopicRequest().setName("acme.orders")
    ).asJava)
    val request = buildRequest(new DescribeTopicPartitionsRequest(data))
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis = createKafkaApis()
    kafkaApis.handleDescribeTopicPartitionsRequest(request)

    val response = verifyNoThrottling[DescribeTopicPartitionsResponse](request)
    val topics = response.data.topics.asScala
    assertEquals(1, topics.size)
    assertEquals("acme.orders", topics.head.name)
    assertEquals(Errors.NONE.code, topics.head.errorCode,
      "with no tenants configured the dotted topic name is not reserved")
    assertFalse(topics.head.partitions.isEmpty,
      "dotted topic must surface its normal partition metadata when no tenants are configured")
  }

  @Test
  def testListTransactionsOutsideInFiltersTenantPrincipalNamespace(): Unit = {
    val data = new ListTransactionsRequestData()
    val request = buildRequest(new ListTransactionsRequest.Builder(data).build())
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    val transactionStates = new util.ArrayList[ListTransactionsResponseData.TransactionState]()
    transactionStates.add(new ListTransactionsResponseData.TransactionState()
      .setTransactionalId("__tenant_acme.checkout-tx").setProducerId(7L).setTransactionState("Ongoing"))
    transactionStates.add(new ListTransactionsResponseData.TransactionState()
      .setTransactionalId("regular-txn").setProducerId(8L).setTransactionState("Ongoing"))
    when(txnCoordinator.handleListTransactions(Set.empty[Long], Set.empty[String], -1L))
      .thenReturn(new ListTransactionsResponseData()
        .setErrorCode(Errors.NONE.code)
        .setTransactionStates(transactionStates))
    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleListTransactionsRequest(request)

    val response = verifyNoThrottling[ListTransactionsResponse](request)
    val visible = response.data.transactionStates.asScala.map(_.transactionalId).toSet
    assertEquals(Set("regular-txn"), visible,
      "non-tenant caller must not see tenant-internal transactional ids in the listing")
  }

  // ---------------------------------------------------------------------------
  // DescribeLogDirs — tenant existence-oracle + disk-usage enumeration guard.
  //
  // The stock handler authorises only on CLUSTER:DESCRIBE then enumerates
  // every local partition (isAllTopicPartitions) or the explicit topic list.
  // For a cluster-wide caller against a multi-tenant broker that yields two
  // leaks:
  //  - WILDCARD: response includes every tenant's physical topic name with
  //    per-partition size + offset-lag (strictly more revealing than #93).
  //  - EXPLICIT: a request naming `acme.orders` reveals existence + disk
  //    usage of that tenant's topic.
  //
  // Defense: silent-strip on both vectors. Tests pin the contract for the
  // explicit-topic pre-filter, the response-side scrub (which doubles as
  // defense-in-depth against an out-of-band describeLogDirs implementation),
  // and the neutral-passthrough when no tenants are configured. Internal
  // topics (`__consumer_offsets`, `__transaction_state`, ...) are exempt.
  // ---------------------------------------------------------------------------

  private def describeLogDirsResult(logDir: String,
                                    topics: Seq[String]): DescribeLogDirsResponseData.DescribeLogDirsResult = {
    val dirTopics = topics.map(name =>
      new DescribeLogDirsResponseData.DescribeLogDirsTopic()
        .setName(name)
        .setPartitions(util.Collections.singletonList(
          new DescribeLogDirsResponseData.DescribeLogDirsPartition()
            .setPartitionIndex(0).setPartitionSize(1024L).setOffsetLag(0L).setIsFutureKey(false))))
    new DescribeLogDirsResponseData.DescribeLogDirsResult()
      .setLogDir(logDir)
      .setErrorCode(Errors.NONE.code)
      .setTotalBytes(1L << 30)
      .setUsableBytes(1L << 28)
      .setTopics(dirTopics.asJava)
  }

  @Test
  def testDescribeLogDirsExplicitTenantTopicPreFilteredBeforeReplicaManager(): Unit = {
    // Outside-in pre-filter: a non-tenant caller on the cluster-wide listener
    // asking about `acme.orders` (physical form of acme's logical `orders`)
    // is dropped at the partitions-set stage, BEFORE replicaManager.describeLogDirs
    // is invoked. That makes the response indistinguishable from
    // "topic does not exist on this broker" (no result row) and prevents
    // timing/cost differentials that would otherwise distinguish a refused
    // tenant-topic probe from a true unknown-topic miss.
    val data = new DescribeLogDirsRequestData().setTopics(
      new DescribeLogDirsRequestData.DescribableLogDirTopicCollection(util.Arrays.asList(
        new DescribeLogDirsRequestData.DescribableLogDirTopic()
          .setTopic("acme.orders")
          .setPartitions(util.Collections.singletonList(Int.box(0))),
        new DescribeLogDirsRequestData.DescribableLogDirTopic()
          .setTopic("regular-topic")
          .setPartitions(util.Collections.singletonList(Int.box(0)))
      ).iterator()))
    val request = buildRequest(new DescribeLogDirsRequest.Builder(data).build())
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    val partitionsCaptor: ArgumentCaptor[Set[TopicPartition]] =
      ArgumentCaptor.forClass(classOf[Set[TopicPartition]])
    when(replicaManager.describeLogDirs(partitionsCaptor.capture()))
      .thenReturn(List(describeLogDirsResult("/var/lib/kafka", Seq("regular-topic"))))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeLogDirsRequest(request)

    val captured = partitionsCaptor.getValue
    assertEquals(Set(new TopicPartition("regular-topic", 0)), captured,
      "reserved-form partition must be stripped before reaching replicaManager.describeLogDirs")
    assertFalse(captured.exists(_.topic == "acme.orders"),
      "tenant-physical partition must not be passed to the log-dir scan")

    val response = verifyNoThrottling[DescribeLogDirsResponse](request)
    val visibleTopics = response.data.results.asScala
      .flatMap(_.topics.asScala.map(_.name)).toSet
    assertEquals(Set("regular-topic"), visibleTopics,
      "tenant-physical topic must not appear in any result row")
    assertEquals(Errors.NONE.code, response.data.errorCode,
      "top-level error code stays NONE: the stripped partition is silent, not refused")
  }

  @Test
  def testDescribeLogDirsResponseSideScrubsTenantTopicsDefenseInDepth(): Unit = {
    // Defense-in-depth: even if describeLogDirs returns a result containing
    // a tenant-namespaced topic (e.g. because of a future refactor or a
    // backdoor path that bypasses the pre-filter), the response-side scrub
    // removes it. Two configured tenants (acme + beta) pin that the scrub
    // covers every known prefix, not just the listener-bound one.
    val data = new DescribeLogDirsRequestData().setTopics(
      new DescribeLogDirsRequestData.DescribableLogDirTopicCollection(util.Arrays.asList(
        new DescribeLogDirsRequestData.DescribableLogDirTopic()
          .setTopic("regular-topic")
          .setPartitions(util.Collections.singletonList(Int.box(0)))
      ).iterator()))
    val request = buildRequest(new DescribeLogDirsRequest.Builder(data).build())
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    // Mock returns BOTH the requested non-tenant topic AND a tenant-prefixed
    // topic the request never named. The response-side scrub must strip it.
    when(replicaManager.describeLogDirs(any[Set[TopicPartition]]()))
      .thenReturn(List(describeLogDirsResult("/var/lib/kafka",
        Seq("acme.orders", "regular-topic", "beta.events"))))

    val props = new util.HashMap[String, Object]()
    props.put(s"listener.name.${TENANT_LISTENER.value.toLowerCase}.tenant.id", "acme")
    props.put("listener.name.tenant_beta.tenant.id", "beta")
    kafkaApis = createKafkaApis(tenantConfig = TenantConfig.from(props))
    kafkaApis.handleDescribeLogDirsRequest(request)

    val response = verifyNoThrottling[DescribeLogDirsResponse](request)
    val visibleTopics = response.data.results.asScala
      .flatMap(_.topics.asScala.map(_.name)).toSet
    assertEquals(Set("regular-topic"), visibleTopics,
      "response-side scrub must remove every reserved-tenant topic across all configured prefixes")
    assertEquals(1, response.data.results.size,
      "log-dir result entries are preserved even when their Topics list is scrubbed to empty")
  }

  @Test
  def testDescribeLogDirsAllTopicsScrubsTenantTopicsFromResponse(): Unit = {
    // Wildcard path (setTopics(null) → isAllTopicPartitions=true). The handler
    // enumerates every local partition, then asks replicaManager to describe
    // them. Without the scrub, the response would list every tenant's
    // physical topic name plus per-partition size + offset lag — the
    // primary vector this guard is designed to close.
    val data = new DescribeLogDirsRequestData().setTopics(
      new DescribeLogDirsRequestData.DescribableLogDirTopicCollection())
    val request = buildRequest(new DescribeLogDirsRequest.Builder(data).build())
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    // logManager.allLogs is iterated to build the partitions set; return an
    // empty iterable — the test only exercises the response-side scrub.
    val mockLogManager = mock(classOf[kafka.log.LogManager])
    when(mockLogManager.allLogs).thenReturn(Iterable.empty[UnifiedLog])
    when(replicaManager.logManager).thenReturn(mockLogManager)
    when(replicaManager.describeLogDirs(any[Set[TopicPartition]]()))
      .thenReturn(List(describeLogDirsResult("/var/lib/kafka",
        Seq("acme.orders", "acme.payments", "public-topic", "__consumer_offsets"))))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeLogDirsRequest(request)

    val response = verifyNoThrottling[DescribeLogDirsResponse](request)
    val visibleTopics = response.data.results.asScala
      .flatMap(_.topics.asScala.map(_.name)).toSet
    assertEquals(Set("public-topic", "__consumer_offsets"), visibleTopics,
      "wildcard path must strip every acme.* topic but retain internal + non-reserved topics")
  }

  @Test
  def testDescribeLogDirsInternalTopicsRetained(): Unit = {
    // Internal topics (`__consumer_offsets`, `__transaction_state`,
    // `__share_group_state`) are never tenant-prefixed; legitimate cluster
    // admin tooling must continue to see them through both the pre-filter
    // and the response-side scrub.
    val data = new DescribeLogDirsRequestData().setTopics(
      new DescribeLogDirsRequestData.DescribableLogDirTopicCollection(util.Arrays.asList(
        new DescribeLogDirsRequestData.DescribableLogDirTopic()
          .setTopic("__consumer_offsets")
          .setPartitions(util.Collections.singletonList(Int.box(0))),
        new DescribeLogDirsRequestData.DescribableLogDirTopic()
          .setTopic("__transaction_state")
          .setPartitions(util.Collections.singletonList(Int.box(0)))
      ).iterator()))
    val request = buildRequest(new DescribeLogDirsRequest.Builder(data).build())
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    val partitionsCaptor: ArgumentCaptor[Set[TopicPartition]] =
      ArgumentCaptor.forClass(classOf[Set[TopicPartition]])
    when(replicaManager.describeLogDirs(partitionsCaptor.capture()))
      .thenReturn(List(describeLogDirsResult("/var/lib/kafka",
        Seq("__consumer_offsets", "__transaction_state"))))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeLogDirsRequest(request)

    val captured = partitionsCaptor.getValue
    assertEquals(
      Set(new TopicPartition("__consumer_offsets", 0), new TopicPartition("__transaction_state", 0)),
      captured,
      "internal topics must reach the log-dir scan unfiltered")

    val response = verifyNoThrottling[DescribeLogDirsResponse](request)
    val visibleTopics = response.data.results.asScala
      .flatMap(_.topics.asScala.map(_.name)).toSet
    assertEquals(Set("__consumer_offsets", "__transaction_state"), visibleTopics,
      "internal topics must survive the response-side scrub")
  }

  @Test
  def testDescribeLogDirsClusterWideListenerKeepsDottedNamesWhenNoTenantsConfigured(): Unit = {
    // Without any configured tenants, `<id>.<topic>` is just a name with a
    // dot — the guard must not fire, otherwise legitimate non-tenant clusters
    // lose visibility into topics with dots in their names.
    val data = new DescribeLogDirsRequestData().setTopics(
      new DescribeLogDirsRequestData.DescribableLogDirTopicCollection(util.Arrays.asList(
        new DescribeLogDirsRequestData.DescribableLogDirTopic()
          .setTopic("acme.orders")
          .setPartitions(util.Collections.singletonList(Int.box(0)))
      ).iterator()))
    val request = buildRequest(new DescribeLogDirsRequest.Builder(data).build())
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    val partitionsCaptor: ArgumentCaptor[Set[TopicPartition]] =
      ArgumentCaptor.forClass(classOf[Set[TopicPartition]])
    when(replicaManager.describeLogDirs(partitionsCaptor.capture()))
      .thenReturn(List(describeLogDirsResult("/var/lib/kafka", Seq("acme.orders"))))

    kafkaApis = createKafkaApis() // no tenantConfig
    kafkaApis.handleDescribeLogDirsRequest(request)

    val captured = partitionsCaptor.getValue
    assertEquals(Set(new TopicPartition("acme.orders", 0)), captured,
      "with no tenants configured the dotted topic must reach the log-dir scan unfiltered")

    val response = verifyNoThrottling[DescribeLogDirsResponse](request)
    val visibleTopics = response.data.results.asScala
      .flatMap(_.topics.asScala.map(_.name)).toSet
    assertEquals(Set("acme.orders"), visibleTopics,
      "with no tenants configured the dotted topic must appear in the response")
  }

  // ---------------------------------------------------------------------------
  // CreateDelegationToken — multi-tenancy identity-laundering guard
  //
  // Minting a token whose owner (or any renewer) sits inside the tenant
  // principal namespace (`__tenant_<id>.<user>`) transfers tenant identity:
  // once minted, the holder can present the token on the tenant's own listener
  // and TenantPrincipalBuilder will preserve the prefix on re-auth (see
  // TenantPrincipalBuilderTest#preservesSameTenantTokenReauthUnchanged), giving
  // them full access in that tenant. The cross-listener replay is already
  // refused inside the principal builder; these tests pin the mint-time side.
  // Rule: a caller whose own principal is not within tenant T cannot mint a
  // token whose owner OR a renewer is in T's principal namespace.
  // ---------------------------------------------------------------------------

  @Test
  def testCreateDelegationTokenClusterWideCallerRefusesTenantPrefixedOwner(): Unit = {
    val createRequest = new CreateDelegationTokenRequest.Builder(
      new CreateDelegationTokenRequestData()
        .setOwnerPrincipalType(KafkaPrincipal.USER_TYPE)
        .setOwnerPrincipalName("__tenant_acme.alice")).build()
    val request = buildRequest(
      createRequest,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleCreateTokenRequest(request)

    val response = verifyNoThrottling[CreateDelegationTokenResponse](request)
    assertEquals(Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED.code, response.data.errorCode,
      "minting a token whose owner is in a tenant principal namespace must be refused for a non-tenant caller")
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testCreateDelegationTokenClusterWideCallerKeepsRegularOwnerForwarded(): Unit = {
    // Control: non-tenant caller minting for an ordinary principal must still
    // be forwarded — the guard targets the tenant namespace only.
    val createRequest = new CreateDelegationTokenRequest.Builder(
      new CreateDelegationTokenRequestData()
        .setOwnerPrincipalType(KafkaPrincipal.USER_TYPE)
        .setOwnerPrincipalName("regular-user")).build()
    val request = buildRequest(
      createRequest,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "regular-user"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleCreateTokenRequest(request)

    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testCreateDelegationTokenSameTenantCallerMintsForOwnPrincipalForwarded(): Unit = {
    // Legitimate path: tenant principal mints a token for itself on its own
    // listener. The owner matches the caller's effective tenant, so the guard
    // must let it through.
    val createRequest = new CreateDelegationTokenRequest.Builder(
      new CreateDelegationTokenRequestData()
        .setOwnerPrincipalType(KafkaPrincipal.USER_TYPE)
        .setOwnerPrincipalName("__tenant_acme.alice")).build()
    val request = buildRequest(
      createRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleCreateTokenRequest(request)

    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testCreateDelegationTokenClusterWideCallerRefusesTenantPrefixedRenewer(): Unit = {
    // The renewer side of the guard: a non-tenant caller cannot register a
    // renewer inside a tenant's principal namespace even if the owner looks
    // ordinary — otherwise the renewer would be able to extend a token whose
    // owner-side guard the broker assumes is also enforced.
    val renewers = new util.ArrayList[CreateDelegationTokenRequestData.CreatableRenewers]()
    renewers.add(new CreateDelegationTokenRequestData.CreatableRenewers()
      .setPrincipalType(KafkaPrincipal.USER_TYPE)
      .setPrincipalName("__tenant_acme.bob"))
    val createRequest = new CreateDelegationTokenRequest.Builder(
      new CreateDelegationTokenRequestData()
        .setOwnerPrincipalType(KafkaPrincipal.USER_TYPE)
        .setOwnerPrincipalName("regular-user")
        .setRenewers(renewers)).build()
    val request = buildRequest(
      createRequest,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleCreateTokenRequest(request)

    val response = verifyNoThrottling[CreateDelegationTokenResponse](request)
    assertEquals(Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED.code, response.data.errorCode,
      "a non-tenant caller cannot register a renewer inside a tenant principal namespace")
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[Option[AbstractResponse] => Unit]())
  }

  // ---------------------------------------------------------------------------
  // DescribeDelegationToken — tenant existence-oracle + HMAC leak guard.
  //
  // The handler returns DelegationToken (TokenInformation + raw HMAC bytes)
  // for every token the caller is "authorized" to describe. Without a tenant
  // scrub a cluster-wide (non-tenant) caller can:
  //   (a) name `__tenant_<t>.<u>` as an owner-filter and learn whether that
  //       tenant user holds a token by presence/emptiness in the result;
  //   (b) call with implicit owner-filter and have any tenant-owned token
  //       (including its raw HMAC) returned.
  // Defense in depth:
  //   1. Atomic refusal — owner-filter naming any foreign tenant principal
  //      yields DELEGATION_TOKEN_AUTHORIZATION_FAILED.
  //   2. Outside-in scrub — drop every token whose owner OR tokenRequester
  //      is a foreign tenant principal.
  // ---------------------------------------------------------------------------

  private def newTokenInformation(
    owner: KafkaPrincipal,
    tokenRequester: KafkaPrincipal,
    tokenId: String
  ): TokenInformation = {
    new TokenInformation(
      tokenId,
      owner,
      tokenRequester,
      java.util.Collections.emptyList[KafkaPrincipal](),
      0L, 0L, 0L)
  }

  private def newDelegationToken(info: TokenInformation): DelegationToken =
    new DelegationToken(info, "hmac-secret-bytes".getBytes(java.nio.charset.StandardCharsets.UTF_8))

  // Token APIs early-exit with DELEGATION_TOKEN_AUTH_DISABLED unless the
  // broker config defines a delegation-token secret key. The actual key value
  // is irrelevant to the tenant guard — we only need tokenAuthEnabled == true.
  private val tokenAuthEnabledProps: Map[String, String] = Map(
    "delegation.token.secret.key" -> "test-delegation-token-secret-key")

  @Test
  def testDescribeDelegationTokenClusterWideCallerRefusesTenantOwnerFilter(): Unit = {
    // Layer 1: a cluster-wide caller naming `__tenant_acme.alice` as an
    // owner-filter is refused atomically. The handler never even consults the
    // token store — refusing on the request side closes the existence-oracle
    // before tokenManager.getTokens runs.
    val owners = util.List.of(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice"))
    val describeRequest = new DescribeDelegationTokenRequest.Builder(owners).build()
    val tokenManagerMock = mock(classOf[DelegationTokenManager])
    val request = buildRequest(
      describeRequest,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis(
      overrideProperties = tokenAuthEnabledProps,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER),
      tokenManager = tokenManagerMock)
    kafkaApis.handleDescribeTokensRequest(request)

    val response = verifyNoThrottling[DescribeDelegationTokenResponse](request)
    assertEquals(Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED.code, response.error.code,
      "a cluster-wide caller cannot probe tenant token existence via owner-filter")
    assertTrue(response.tokens.isEmpty,
      "refused request must not surface any tokens at all")
    verify(tokenManagerMock, never()).getTokens(any())
  }

  @Test
  def testDescribeDelegationTokenAtomicRefusalWhenBenignOwnerMixedWithTenant(): Unit = {
    // Atomic refusal: a benign + foreign-tenant batch is refused as a whole.
    // Otherwise a caller could probe existence by comparing the response size
    // to a control request that only contained the benign owner.
    val owners = util.List.of(
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "regular-user"),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice"))
    val describeRequest = new DescribeDelegationTokenRequest.Builder(owners).build()
    val tokenManagerMock = mock(classOf[DelegationTokenManager])
    val request = buildRequest(
      describeRequest,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis(
      overrideProperties = tokenAuthEnabledProps,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER),
      tokenManager = tokenManagerMock)
    kafkaApis.handleDescribeTokensRequest(request)

    val response = verifyNoThrottling[DescribeDelegationTokenResponse](request)
    assertEquals(Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED.code, response.error.code,
      "a mixed batch with one foreign-tenant owner must refuse atomically — no partial response")
    verify(tokenManagerMock, never()).getTokens(any())
  }

  @Test
  def testDescribeDelegationTokenCrossTenantOwnerFilterRefused(): Unit = {
    // The acme tenant's principal cannot name `__tenant_beta.*` in the filter:
    // the same-tenant exemption is strict, it does not span tenants.
    val owners = util.List.of(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_beta.bob"))
    val describeRequest = new DescribeDelegationTokenRequest.Builder(owners).build()
    val tokenManagerMock = mock(classOf[DelegationTokenManager])
    val request = buildRequest(
      describeRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    val props = new util.HashMap[String, Object]()
    props.put(s"listener.name.${TENANT_LISTENER.value.toLowerCase}.tenant.id", "acme")
    props.put("listener.name.tenant_beta.tenant.id", "beta")
    kafkaApis = createKafkaApis(
      overrideProperties = tokenAuthEnabledProps,
      tenantConfig = TenantConfig.from(props),
      tokenManager = tokenManagerMock)
    kafkaApis.handleDescribeTokensRequest(request)

    val response = verifyNoThrottling[DescribeDelegationTokenResponse](request)
    assertEquals(Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED.code, response.error.code,
      "an acme tenant caller cannot probe beta tenant token existence")
    verify(tokenManagerMock, never()).getTokens(any())
  }

  @Test
  def testDescribeDelegationTokenScrubDropsTenantOwnedTokens(): Unit = {
    // Layer 2: the implicit-filter path (data.owners == null) still flows
    // through tokenManager.getTokens. The scrub drops every token whose owner
    // sits in a foreign tenant namespace — closing the HMAC leak even if a
    // legacy token was minted before the mint-time guard landed.
    val tenantToken = newDelegationToken(newTokenInformation(
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice"),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice"),
      "tok-tenant"))
    val regularToken = newDelegationToken(newTokenInformation(
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "regular-user"),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "regular-user"),
      "tok-regular"))
    val tokenManagerMock = mock(classOf[DelegationTokenManager])
    when(tokenManagerMock.getTokens(any())).thenReturn(List(tenantToken, regularToken))

    val describeRequest = new DescribeDelegationTokenRequest.Builder(null).build()
    val request = buildRequest(
      describeRequest,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis(
      overrideProperties = tokenAuthEnabledProps,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER),
      tokenManager = tokenManagerMock)
    kafkaApis.handleDescribeTokensRequest(request)

    val response = verifyNoThrottling[DescribeDelegationTokenResponse](request)
    assertEquals(Errors.NONE.code, response.error.code)
    val tokens = response.tokens.asScala.toList
    assertEquals(1, tokens.size,
      "every token owned by a tenant principal must be scrubbed from the response")
    assertEquals("tok-regular", tokens.head.tokenInfo.tokenId,
      "only non-tenant tokens may surface to a cluster-wide caller")
  }

  @Test
  def testDescribeDelegationTokenScrubDropsTokensWithForeignTokenRequester(): Unit = {
    // The HMAC is also revealed to the tokenRequester (the delegated minter).
    // A token whose owner is a regular user but whose tokenRequester is a
    // tenant principal must still be scrubbed — otherwise we leak the fact
    // that a tenant user has been delegated mint rights for that regular
    // owner, and we leak the HMAC.
    val mixedToken = newDelegationToken(newTokenInformation(
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "regular-user"),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice"),
      "tok-mixed"))
    val tokenManagerMock = mock(classOf[DelegationTokenManager])
    when(tokenManagerMock.getTokens(any())).thenReturn(List(mixedToken))

    val describeRequest = new DescribeDelegationTokenRequest.Builder(null).build()
    val request = buildRequest(
      describeRequest,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis(
      overrideProperties = tokenAuthEnabledProps,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER),
      tokenManager = tokenManagerMock)
    kafkaApis.handleDescribeTokensRequest(request)

    val response = verifyNoThrottling[DescribeDelegationTokenResponse](request)
    assertEquals(Errors.NONE.code, response.error.code)
    assertTrue(response.tokens.isEmpty,
      "tokens with a foreign-tenant tokenRequester must be scrubbed (HMAC + identity leak)")
  }

  @Test
  def testDescribeDelegationTokenSameTenantCallerSeesOwnTokens(): Unit = {
    // Legitimate path: tenant principal asks for its own tokens on its own
    // listener. The owner-filter resolves to the caller's effective tenant,
    // so the gate must let it through and the scrub must not strip own-tenant
    // tokens.
    val ownTenantToken = newDelegationToken(newTokenInformation(
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice"),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice"),
      "tok-own"))
    val tokenManagerMock = mock(classOf[DelegationTokenManager])
    when(tokenManagerMock.getTokens(any())).thenReturn(List(ownTenantToken))

    val owners = util.List.of(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice"))
    val describeRequest = new DescribeDelegationTokenRequest.Builder(owners).build()
    val request = buildRequest(
      describeRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(
      overrideProperties = tokenAuthEnabledProps,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER),
      tokenManager = tokenManagerMock)
    kafkaApis.handleDescribeTokensRequest(request)

    val response = verifyNoThrottling[DescribeDelegationTokenResponse](request)
    assertEquals(Errors.NONE.code, response.error.code,
      "a same-tenant caller is not refused on its own owner-filter")
    assertEquals(1, response.tokens.size,
      "the scrub must not strip own-tenant tokens from the result")
  }

  @Test
  def testDescribeDelegationTokenClusterWideCallerSeesOnlyRegularTokens(): Unit = {
    // Regression test: a cluster-wide caller asking for a regular user gets
    // the full unfiltered behavior (Errors.NONE) and the scrub passes only
    // non-tenant tokens through. Pins both the no-refusal path and the
    // scrub's pass-through.
    val regularToken = newDelegationToken(newTokenInformation(
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "regular-user"),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "regular-user"),
      "tok-regular"))
    val tokenManagerMock = mock(classOf[DelegationTokenManager])
    when(tokenManagerMock.getTokens(any())).thenReturn(List(regularToken))

    val owners = util.List.of(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "regular-user"))
    val describeRequest = new DescribeDelegationTokenRequest.Builder(owners).build()
    val request = buildRequest(
      describeRequest,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis(
      overrideProperties = tokenAuthEnabledProps,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER),
      tokenManager = tokenManagerMock)
    kafkaApis.handleDescribeTokensRequest(request)

    val response = verifyNoThrottling[DescribeDelegationTokenResponse](request)
    assertEquals(Errors.NONE.code, response.error.code)
    assertEquals(1, response.tokens.size)
    assertEquals("tok-regular", response.tokens.get(0).tokenInfo.tokenId)
  }

  @Test
  def testDescribeDelegationTokenUnknownTenantPrefixOwnerRefused(): Unit = {
    // The `__tenant_` prefix is RESERVED — a cluster-wide admin asking to
    // describe tokens whose owner is `__tenant_xyz.dave` is probing a not-yet-
    // bound tenant's token slot. Without this refusal, an admin could enumerate
    // any tenant id any operator has ever minted a token for (pre-binding
    // pollution discovery), or — combined with CreateDelegationToken minting on
    // the same slot — gain a credential the tenant inherits on bind. Refuse
    // structurally regardless of whether `xyz` is currently bound.
    val tokenManagerMock = mock(classOf[DelegationTokenManager])

    val owners = util.List.of(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_xyz.dave"))
    val describeRequest = new DescribeDelegationTokenRequest.Builder(owners).build()
    val request = buildRequest(
      describeRequest,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis(
      overrideProperties = tokenAuthEnabledProps,
      tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER),
      tokenManager = tokenManagerMock)
    kafkaApis.handleDescribeTokensRequest(request)

    val response = verifyNoThrottling[DescribeDelegationTokenResponse](request)
    assertEquals(Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED.code, response.error.code,
      "the `__tenant_` prefix is reserved — refuse enumeration even for unbound tenant ids")
    assertTrue(response.tokens.isEmpty, "refusal must not leak any tokens")
    verify(tokenManagerMock, never()).getTokens(any())
  }

  // ---------------------------------------------------------------------------
  // AlterUserScramCredentials — multi-tenancy identity-laundering guard
  //
  // A cluster super-user can otherwise mint a SCRAM credential whose user is
  // `__tenant_<id>.<user>`, then SASL/SCRAM-authenticate as that username and
  // have TenantPrincipalBuilder preserve the prefix on the tenant's bound
  // listener — full tenant access. The mirror attack via deletion can revoke
  // a tenant user. Rule: a caller whose own principal is not within tenant T
  // cannot Upsert or Delete a SCRAM credential whose user is in
  // `__tenant_T.*`. Atomic batch refusal (every Result carries
  // CLUSTER_AUTHORIZATION_FAILED) mirrors the delegation-token mint guard.
  // ---------------------------------------------------------------------------

  @Test
  def testAlterUserScramCredentialsClusterWideCallerRefusesTenantPrefixedUpsertion(): Unit = {
    val upsertions = new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialUpsertion]()
    upsertions.add(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
      .setName("__tenant_acme.alice")
      .setMechanism(1.toByte)
      .setIterations(8192)
      .setSalt(Array.emptyByteArray)
      .setSaltedPassword(Array.emptyByteArray))
    val alterRequest = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData().setUpsertions(upsertions)).build()
    val request = buildRequest(
      alterRequest,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAlterUserScramCredentialsRequest(request)

    val response = verifyNoThrottling[AlterUserScramCredentialsResponse](request)
    assertEquals(1, response.data.results.size)
    val result = response.data.results.get(0)
    assertEquals("__tenant_acme.alice", result.user)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, result.errorCode,
      "minting a SCRAM credential whose user is in a tenant principal namespace must be refused for a non-tenant caller")
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testAlterUserScramCredentialsClusterWideCallerRefusesTenantPrefixedDeletion(): Unit = {
    val deletions = new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialDeletion]()
    deletions.add(new AlterUserScramCredentialsRequestData.ScramCredentialDeletion()
      .setName("__tenant_acme.alice")
      .setMechanism(1.toByte))
    val alterRequest = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData().setDeletions(deletions)).build()
    val request = buildRequest(
      alterRequest,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAlterUserScramCredentialsRequest(request)

    val response = verifyNoThrottling[AlterUserScramCredentialsResponse](request)
    assertEquals(1, response.data.results.size)
    val result = response.data.results.get(0)
    assertEquals("__tenant_acme.alice", result.user)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, result.errorCode,
      "deleting a SCRAM credential for a tenant principal from a non-tenant caller must be refused (no silent eviction of tenant users)")
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testAlterUserScramCredentialsAtomicMixedBatchRefused(): Unit = {
    // The whole batch is refused (every named user gets CLUSTER_AUTHORIZATION_FAILED)
    // if any single entry targets a foreign tenant — no partial application on a
    // security-critical credential store.
    val upsertions = new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialUpsertion]()
    upsertions.add(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
      .setName("regular-user")
      .setMechanism(1.toByte)
      .setIterations(8192)
      .setSalt(Array.emptyByteArray)
      .setSaltedPassword(Array.emptyByteArray))
    upsertions.add(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
      .setName("__tenant_acme.alice")
      .setMechanism(1.toByte)
      .setIterations(8192)
      .setSalt(Array.emptyByteArray)
      .setSaltedPassword(Array.emptyByteArray))
    val alterRequest = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData().setUpsertions(upsertions)).build()
    val request = buildRequest(
      alterRequest,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAlterUserScramCredentialsRequest(request)

    val response = verifyNoThrottling[AlterUserScramCredentialsResponse](request)
    assertEquals(2, response.data.results.size,
      "every user in the batch must be reflected in the refusal response so the client cannot infer which entry was foreign")
    response.data.results.forEach { r =>
      assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, r.errorCode,
        s"all entries (including the benign `${r.user}`) must be refused atomically when the batch contains a foreign-tenant entry")
    }
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testAlterUserScramCredentialsClusterWideCallerKeepsRegularUserForwarded(): Unit = {
    // Control: a non-tenant caller altering an ordinary user must still be
    // forwarded — the guard targets the tenant namespace only.
    val upsertions = new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialUpsertion]()
    upsertions.add(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
      .setName("regular-user")
      .setMechanism(1.toByte)
      .setIterations(8192)
      .setSalt(Array.emptyByteArray)
      .setSaltedPassword(Array.emptyByteArray))
    val alterRequest = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData().setUpsertions(upsertions)).build()
    val request = buildRequest(
      alterRequest,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAlterUserScramCredentialsRequest(request)

    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testAlterUserScramCredentialsSameTenantCallerForOwnUserForwarded(): Unit = {
    // Legitimate path: tenant principal alters a SCRAM credential for its own
    // tenant on its own listener. The user matches the caller's effective
    // tenant, so the guard must let it through.
    val upsertions = new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialUpsertion]()
    upsertions.add(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
      .setName("__tenant_acme.alice")
      .setMechanism(1.toByte)
      .setIterations(8192)
      .setSalt(Array.emptyByteArray)
      .setSaltedPassword(Array.emptyByteArray))
    val alterRequest = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData().setUpsertions(upsertions)).build()
    val request = buildRequest(
      alterRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAlterUserScramCredentialsRequest(request)

    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testAlterUserScramCredentialsUnknownTenantPrefixedUserRefused(): Unit = {
    // The `__tenant_` prefix is RESERVED — minting (or deleting) a SCRAM
    // credential for `__tenant_xyz.dave` is the planting form of pre-binding
    // pollution: once tenant `xyz` is bound on some broker, that broker's
    // TenantPrincipalBuilder will mint the `__tenant_xyz.dave` principal on
    // SASL/SCRAM auth and the planted credential becomes a back-door tenant
    // identity. Refuse structurally regardless of binding state — closes the
    // pollution path on the broker side and the split-mode KRaft case where
    // the controller has no listener bindings.
    val upsertions = new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialUpsertion]()
    upsertions.add(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
      .setName("__tenant_xyz.dave")
      .setMechanism(1.toByte)
      .setIterations(8192)
      .setSalt(Array.emptyByteArray)
      .setSaltedPassword(Array.emptyByteArray))
    val alterRequest = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData().setUpsertions(upsertions)).build()
    val request = buildRequest(
      alterRequest,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleAlterUserScramCredentialsRequest(request)

    val response = verifyNoThrottling[AlterUserScramCredentialsResponse](request)
    assertEquals(1, response.data.results.size)
    val result = response.data.results.get(0)
    assertEquals("__tenant_xyz.dave", result.user)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, result.errorCode,
      "the `__tenant_` prefix is reserved — refuse credential planting for unbound tenant ids")
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testAlterUserScramCredentialsCrossTenantCallerRefused(): Unit = {
    // Two-tenant fixture: acme caller cannot touch a beta-tenant user, even
    // though the caller is itself a tenant principal. The "same tenant"
    // exemption is strict — it does not extend to OTHER tenants.
    val upsertions = new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialUpsertion]()
    upsertions.add(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
      .setName("__tenant_beta.bob")
      .setMechanism(1.toByte)
      .setIterations(8192)
      .setSalt(Array.emptyByteArray)
      .setSaltedPassword(Array.emptyByteArray))
    val alterRequest = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData().setUpsertions(upsertions)).build()
    val request = buildRequest(
      alterRequest,
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    val props = new util.HashMap[String, Object]()
    props.put(s"listener.name.${TENANT_LISTENER.value.toLowerCase}.tenant.id", "acme")
    props.put("listener.name.tenant_beta.tenant.id", "beta")
    kafkaApis = createKafkaApis(tenantConfig = TenantConfig.from(props))
    kafkaApis.handleAlterUserScramCredentialsRequest(request)

    val response = verifyNoThrottling[AlterUserScramCredentialsResponse](request)
    assertEquals(1, response.data.results.size)
    val result = response.data.results.get(0)
    assertEquals("__tenant_beta.bob", result.user)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, result.errorCode,
      "an acme caller cannot alter a beta-tenant SCRAM credential")
    verify(forwardingManager, never()).forwardRequest(any[RequestChannel.Request](),
      any[Option[AbstractResponse] => Unit]())
  }

  @Test
  def testDescribeUserScramCredentialsClusterWideCallerFiltersTenantPrefixedFromListAll(): Unit = {
    // The "list all users" form (null/empty Users) makes the SCRAM image
    // dump every credentialled user — including any `__tenant_*` entries —
    // back to the caller. From a cluster-wide listener this leaks the tenant
    // roster (and per-user mechanism + iteration count). The handler must
    // strip those entries before returning.
    val cacheMock = mock(classOf[KRaftMetadataCache])
    metadataCache = cacheMock
    val allResults = new DescribeUserScramCredentialsResponseData()
      .setResults(util.Arrays.asList(
        new DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult()
          .setUser("admin")
          .setCredentialInfos(util.Arrays.asList(
            new DescribeUserScramCredentialsResponseData.CredentialInfo()
              .setMechanism(1.toByte).setIterations(8192))),
        new DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult()
          .setUser("__tenant_acme.alice")
          .setCredentialInfos(util.Arrays.asList(
            new DescribeUserScramCredentialsResponseData.CredentialInfo()
              .setMechanism(1.toByte).setIterations(8192))),
        new DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult()
          .setUser("__tenant_beta.bob")
          .setCredentialInfos(util.Arrays.asList(
            new DescribeUserScramCredentialsResponseData.CredentialInfo()
              .setMechanism(2.toByte).setIterations(4096)))))
    when(cacheMock.describeScramCredentials(any[DescribeUserScramCredentialsRequestData]))
      .thenReturn(allResults)

    val describeRequest = new DescribeUserScramCredentialsRequest.Builder(
      new DescribeUserScramCredentialsRequestData().setUsers(null)).build()
    val request = buildRequest(
      describeRequest,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis()
    kafkaApis.handleDescribeUserScramCredentialsRequest(request)

    val response = verifyNoThrottling[DescribeUserScramCredentialsResponse](request)
    val users = response.data().results().asScala.map(_.user).toSet
    assertEquals(Set("admin"), users,
      "tenant SCRAM users must be filtered out of the cluster-wide 'list all' response")
  }

  @Test
  def testDescribeUserScramCredentialsClusterWideCallerRefusesExplicitTenantUser(): Unit = {
    // Explicit-list form: the adversary names `__tenant_acme.alice` to probe
    // existence. The image would return the SCRAM metadata if the user
    // exists, or RESOURCE_NOT_FOUND with the physical name in the message if
    // it doesn't — either branch confirms or denies existence. Refuse the
    // tenant-prefixed name pre-image with RESOURCE_NOT_FOUND and an empty
    // error message so the response is indistinguishable from "user never
    // existed".
    val cacheMock = mock(classOf[KRaftMetadataCache])
    metadataCache = cacheMock
    // If the handler had let the tenant-prefixed name reach the image, this
    // mock would have returned metadata. The test asserts the image is
    // called with an empty Users list (so it returns the empty placeholder).
    when(cacheMock.describeScramCredentials(any[DescribeUserScramCredentialsRequestData]))
      .thenReturn(new DescribeUserScramCredentialsResponseData())

    val users = new util.ArrayList[DescribeUserScramCredentialsRequestData.UserName]()
    users.add(new DescribeUserScramCredentialsRequestData.UserName().setName("__tenant_acme.alice"))
    val describeRequest = new DescribeUserScramCredentialsRequest.Builder(
      new DescribeUserScramCredentialsRequestData().setUsers(users)).build()
    val request = buildRequest(
      describeRequest,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis()
    kafkaApis.handleDescribeUserScramCredentialsRequest(request)

    val response = verifyNoThrottling[DescribeUserScramCredentialsResponse](request)
    val results = response.data().results().asScala
    assertEquals(1, results.size, "exactly one result for one named user")
    val r = results.head
    assertEquals("__tenant_acme.alice", r.user)
    assertEquals(Errors.RESOURCE_NOT_FOUND.code, r.errorCode,
      "tenant-prefixed SCRAM user lookup must be indistinguishable from never-existed")
    assertTrue(r.errorMessage == null || r.errorMessage.isEmpty,
      "error message must be empty so it does not leak existence vs absence")
    assertTrue(r.credentialInfos() == null || r.credentialInfos().isEmpty,
      "no SCRAM metadata may be returned for a tenant-prefixed user from a cluster-wide caller")
  }

  @Test
  def testDescribeUserScramCredentialsMixedBatchSplitsTenantAndClusterEntries(): Unit = {
    // Mixed batch where the caller names a legitimate cluster user AND a
    // tenant-prefixed user. The cluster user must round-trip; the tenant
    // user must come back with RESOURCE_NOT_FOUND and no oracle leak. The
    // ordering of results is not specified by the protocol — assert by
    // user name lookup.
    val cacheMock = mock(classOf[KRaftMetadataCache])
    metadataCache = cacheMock
    when(cacheMock.describeScramCredentials(any[DescribeUserScramCredentialsRequestData]))
      .thenAnswer(invocation => {
        val req = invocation.getArgument[DescribeUserScramCredentialsRequestData](0)
        // The handler must have stripped the tenant-prefixed user from the
        // request BEFORE calling the image. Mirror the image's behaviour:
        // produce one entry per requested user.
        val data = new DescribeUserScramCredentialsResponseData()
        req.users().forEach { u =>
          data.results().add(new DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult()
            .setUser(u.name())
            .setCredentialInfos(util.Arrays.asList(
              new DescribeUserScramCredentialsResponseData.CredentialInfo()
                .setMechanism(1.toByte).setIterations(8192))))
        }
        data
      })

    val users = new util.ArrayList[DescribeUserScramCredentialsRequestData.UserName]()
    users.add(new DescribeUserScramCredentialsRequestData.UserName().setName("ops-admin"))
    users.add(new DescribeUserScramCredentialsRequestData.UserName().setName("__tenant_acme.alice"))
    val describeRequest = new DescribeUserScramCredentialsRequest.Builder(
      new DescribeUserScramCredentialsRequestData().setUsers(users)).build()
    val request = buildRequest(
      describeRequest,
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis()
    kafkaApis.handleDescribeUserScramCredentialsRequest(request)

    val response = verifyNoThrottling[DescribeUserScramCredentialsResponse](request)
    val byUser = response.data().results().asScala.map(r => r.user -> r).toMap
    assertEquals(Set("ops-admin", "__tenant_acme.alice"), byUser.keySet,
      "both named users must appear in the response, but with different shapes")
    val cluster = byUser("ops-admin")
    assertEquals(Errors.NONE.code, cluster.errorCode,
      "cluster user must round-trip with full SCRAM metadata")
    assertEquals(1, cluster.credentialInfos().size)
    val tenant = byUser("__tenant_acme.alice")
    assertEquals(Errors.RESOURCE_NOT_FOUND.code, tenant.errorCode,
      "tenant user must be returned as RESOURCE_NOT_FOUND, regardless of actual existence")
    assertTrue(tenant.errorMessage == null || tenant.errorMessage.isEmpty,
      "error message must be empty so it does not leak existence vs absence")
    assertTrue(tenant.credentialInfos() == null || tenant.credentialInfos().isEmpty)
  }

  // ---------------------------------------------------------------------------
  // #116 — DescribeClientQuotas tenant scrub on the broker.
  //
  // The cluster-quota image holds quota records keyed on principal/client-id
  // strings. Tenant quota records use the reserved principal form
  // `__tenant_<id>.<user>` (USER) or, in pathological deployments, an
  // analogous CLIENT_ID convention. Without a filter, a cluster-wide caller
  // hitting the broker with a wide DescribeClientQuotas filter enumerates
  // every tenant's quota roster — principal name, knob, value.
  //
  // Tenant view: a tenant must see ONLY their own namespace's quotas with
  // the prefix stripped on the wire so the logical name lands at the client.
  // ---------------------------------------------------------------------------

  private def quotaEntry(entityType: String, entityName: String,
                        knob: String = "producer_byte_rate", value: Double = 1024.0)
  : DescribeClientQuotasResponseData.EntryData = {
    val entity = new util.ArrayList[DescribeClientQuotasResponseData.EntityData]()
    entity.add(new DescribeClientQuotasResponseData.EntityData()
      .setEntityType(entityType).setEntityName(entityName))
    val values = new util.ArrayList[DescribeClientQuotasResponseData.ValueData]()
    values.add(new DescribeClientQuotasResponseData.ValueData().setKey(knob).setValue(value))
    new DescribeClientQuotasResponseData.EntryData().setEntity(entity).setValues(values)
  }

  private def buildDescribeClientQuotasRequest(): DescribeClientQuotasRequest = {
    // Wide filter: a single USER component with MATCH_TYPE_SPECIFIED (the
    // form an adversary would use to enumerate every USER-keyed entry).
    val componentData = new util.ArrayList[DescribeClientQuotasRequestData.ComponentData]()
    componentData.add(new DescribeClientQuotasRequestData.ComponentData()
      .setEntityType(org.apache.kafka.common.quota.ClientQuotaEntity.USER)
      .setMatchType(DescribeClientQuotasRequest.MATCH_TYPE_SPECIFIED)
      .setMatch(null))
    val data = new DescribeClientQuotasRequestData().setComponents(componentData).setStrict(false)
    new DescribeClientQuotasRequest(data, ApiKeys.DESCRIBE_CLIENT_QUOTAS.latestVersion)
  }

  private def stubDescribeClientQuotasImage(entries: DescribeClientQuotasResponseData.EntryData*): Unit = {
    val cacheMock = mock(classOf[KRaftMetadataCache])
    metadataCache = cacheMock
    val data = new DescribeClientQuotasResponseData()
      .setEntries(util.Arrays.asList(entries: _*))
    when(cacheMock.describeClientQuotas(any[DescribeClientQuotasRequestData])).thenReturn(data)
  }

  // Convenience: pull entries from a response keyed by their (single) USER
  // entityName, so assertions can ignore ordering and zero-in on the leaked
  // names. Entries with no USER component are bucketed under None.
  private def usersIn(resp: DescribeClientQuotasResponse): Set[String] = {
    resp.data().entries().asScala.flatMap { entry =>
      entry.entity().asScala.find(_.entityType() == org.apache.kafka.common.quota.ClientQuotaEntity.USER)
        .map(_.entityName())
    }.toSet
  }

  @Test
  def testDescribeClientQuotasClusterWideCallerFiltersTenantPrefixedUser(): Unit = {
    // Cluster admin runs an enumeration query. The image dump contains both
    // a plain cluster user AND a tenant-prefixed user. The handler must
    // strip the tenant entry before sending the response — leaking the
    // principal name would materialise the tenant's quota roster.
    stubDescribeClientQuotasImage(
      quotaEntry(org.apache.kafka.common.quota.ClientQuotaEntity.USER, "regular-user"),
      quotaEntry(org.apache.kafka.common.quota.ClientQuotaEntity.USER, "__tenant_acme.bob"))

    val request = buildRequest(
      buildDescribeClientQuotasRequest(),
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis()
    kafkaApis.handleDescribeClientQuotasRequest(request)

    val response = verifyNoThrottling[DescribeClientQuotasResponse](request)
    assertEquals(Set("regular-user"), usersIn(response),
      "cluster-wide caller must not see the tenant-prefixed USER entry")
  }

  @Test
  def testDescribeClientQuotasClusterWideCallerPreservesPlainUsers(): Unit = {
    // Regression guard for #116: filtering must not over-filter. Plain users
    // (no `__tenant_` prefix) round-trip untouched even when sitting next to
    // tenant entries in the image dump.
    stubDescribeClientQuotasImage(
      quotaEntry(org.apache.kafka.common.quota.ClientQuotaEntity.USER, "bob"),
      quotaEntry(org.apache.kafka.common.quota.ClientQuotaEntity.USER, "alice"),
      quotaEntry(org.apache.kafka.common.quota.ClientQuotaEntity.USER, "__tenant_acme.eve"))

    val request = buildRequest(
      buildDescribeClientQuotasRequest(),
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis()
    kafkaApis.handleDescribeClientQuotasRequest(request)

    val response = verifyNoThrottling[DescribeClientQuotasResponse](request)
    assertEquals(Set("bob", "alice"), usersIn(response),
      "plain cluster users must round-trip untouched")
  }

  @Test
  def testDescribeClientQuotasTenantCallerSeesOwnEntriesWithPrefixStripped(): Unit = {
    // Tenant caller on a tenant-bound listener sees ONLY their own
    // namespace, and the wire response carries logical user names.
    // `__tenant_acme.bob` arrives as `bob`.
    stubDescribeClientQuotasImage(
      quotaEntry(org.apache.kafka.common.quota.ClientQuotaEntity.USER, "__tenant_acme.bob",
        knob = "producer_byte_rate", value = 2048.0),
      quotaEntry(org.apache.kafka.common.quota.ClientQuotaEntity.USER, "__tenant_acme.alice",
        knob = "consumer_byte_rate", value = 4096.0))

    val request = buildRequest(
      buildDescribeClientQuotasRequest(),
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeClientQuotasRequest(request)

    val response = verifyNoThrottling[DescribeClientQuotasResponse](request)
    assertEquals(Set("bob", "alice"), usersIn(response),
      "tenant caller must see their own users with prefix stripped")
    // Quota values must be preserved verbatim — only the entity name field
    // is rewritten.
    val entries = response.data().entries().asScala
    val knobs = entries.flatMap(_.values().asScala.map(v => v.key() -> v.value())).toMap
    assertEquals(2048.0, knobs("producer_byte_rate"), 0.0)
    assertEquals(4096.0, knobs("consumer_byte_rate"), 0.0)
  }

  @Test
  def testDescribeClientQuotasTenantCallerHidesOtherTenantsEntries(): Unit = {
    // Cross-tenant isolation: tenant `acme` must never observe tenant
    // `beta`'s quotas, even when the image dump contains them. The two
    // tenants share the same backing image; ONLY the prefix distinguishes
    // them, so the filter is what gives tenant `acme` an isolated view.
    stubDescribeClientQuotasImage(
      quotaEntry(org.apache.kafka.common.quota.ClientQuotaEntity.USER, "__tenant_acme.bob"),
      quotaEntry(org.apache.kafka.common.quota.ClientQuotaEntity.USER, "__tenant_beta.charlie"))

    val request = buildRequest(
      buildDescribeClientQuotasRequest(),
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeClientQuotasRequest(request)

    val response = verifyNoThrottling[DescribeClientQuotasResponse](request)
    assertEquals(Set("bob"), usersIn(response),
      "tenant `acme` must not see tenant `beta`'s quota entries")
  }

  @Test
  def testDescribeClientQuotasTenantCallerHidesClusterScopedEntries(): Unit = {
    // A tenant must not observe cluster-scoped quotas (which would either
    // signal that a cluster admin exists with that name, or leak that
    // cluster-default quotas were tuned). Drop non-tenant-prefixed USER
    // entries from a tenant's response.
    stubDescribeClientQuotasImage(
      quotaEntry(org.apache.kafka.common.quota.ClientQuotaEntity.USER, "__tenant_acme.bob"),
      quotaEntry(org.apache.kafka.common.quota.ClientQuotaEntity.USER, "regular-cluster-admin"))

    val request = buildRequest(
      buildDescribeClientQuotasRequest(),
      listenerName = TENANT_LISTENER,
      principal = tenantPrincipal("acme", "alice"))

    kafkaApis = createKafkaApis(tenantConfig = tenantConfigBinding("acme", TENANT_LISTENER))
    kafkaApis.handleDescribeClientQuotasRequest(request)

    val response = verifyNoThrottling[DescribeClientQuotasResponse](request)
    assertEquals(Set("bob"), usersIn(response),
      "tenant caller must not see cluster-scoped USER quota entries")
  }

  @Test
  def testDescribeClientQuotasStockKafkaPassesThroughUnchanged(): Unit = {
    // Stock Kafka deployment (no tenants configured anywhere). Every entry
    // the image returns must round-trip verbatim — the filter must not
    // accidentally trim a deployment with no tenancy. The presence of an
    // unrelated `_` or `.` in a user name must not be misinterpreted as a
    // tenant prefix.
    stubDescribeClientQuotasImage(
      quotaEntry(org.apache.kafka.common.quota.ClientQuotaEntity.USER, "alice"),
      quotaEntry(org.apache.kafka.common.quota.ClientQuotaEntity.USER, "bob.smith"),
      quotaEntry(org.apache.kafka.common.quota.ClientQuotaEntity.USER, "_internal-svc"))

    val request = buildRequest(
      buildDescribeClientQuotasRequest(),
      principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    kafkaApis = createKafkaApis()  // no tenantConfig — stock Kafka
    kafkaApis.handleDescribeClientQuotasRequest(request)

    val response = verifyNoThrottling[DescribeClientQuotasResponse](request)
    assertEquals(Set("alice", "bob.smith", "_internal-svc"), usersIn(response),
      "stock Kafka deployment must round-trip every USER entry verbatim")
  }

}
