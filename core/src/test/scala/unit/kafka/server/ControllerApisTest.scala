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

import kafka.network.RequestChannel
import kafka.raft.RaftManager
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.metadata.KRaftMetadataCache
import org.apache.kafka.clients.admin.AlterConfigOp
import org.apache.kafka.common.Uuid.ZERO_UUID
import org.apache.kafka.common.acl.{AccessControlEntry, AccessControlEntryFilter, AclBinding, AclBindingFilter, AclOperation, AclPermissionType}
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.AlterConfigsRequestData.{AlterConfigsResource => OldAlterConfigsResource, AlterConfigsResourceCollection => OldAlterConfigsResourceCollection, AlterableConfig => OldAlterableConfig, AlterableConfigCollection => OldAlterableConfigCollection}
import org.apache.kafka.common.message.AlterConfigsResponseData.{AlterConfigsResourceResponse => OldAlterConfigsResourceResponse}
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult
import org.apache.kafka.common.message.CreateTopicsRequestData.{CreatableTopic, CreatableTopicCollection}
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult
import org.apache.kafka.common.message.DeleteTopicsRequestData.DeleteTopicState
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.{AlterConfigsResource, AlterConfigsResourceCollection, AlterableConfig, AlterableConfigCollection}
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse
import org.apache.kafka.common.message._
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.Errors._
import org.apache.kafka.common.protocol.{ApiKeys, ApiMessage, Errors, MessageUtil}
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.{PatternType, Resource, ResourcePattern, ResourcePatternFilter, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.test.MockController
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.{ElectionType, Uuid}
import org.apache.kafka.controller.ControllerRequestContextUtil.ANONYMOUS_CONTEXT
import org.apache.kafka.controller.{Controller, ControllerRequestContext, ResultOrError}
import org.apache.kafka.image.publisher.ControllerRegistrationsPublisher
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.network.metrics.RequestChannelMetrics
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.server.authorizer.{AclCreateResult, AclDeleteResult, Action, AuthorizableRequestContext, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.{ApiMessageAndVersion, FinalizedFeatures, KRaftVersion, MetadataVersion, ProducerIdsBlock, RequestLocal}
import org.apache.kafka.server.config.{KRaftConfigs, ServerConfigs}
import org.apache.kafka.server.util.FutureUtils
import org.apache.kafka.storage.internals.log.CleanerConfig
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.slf4j.LoggerFactory

import java.net.InetAddress
import java.util
import java.util.Collections.{singleton, singletonList, singletonMap}
import java.util.concurrent.{CompletableFuture, ExecutionException, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import java.util.{Collections, Optional, Properties}
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

class ControllerApisTest {
  val logger = LoggerFactory.getLogger(classOf[ControllerApisTest])

  object MockControllerMutationQuota {
    val errorMessage = "quota exceeded in test"
    var throttleTimeMs = 1000
  }

  case class MockControllerMutationQuota(quota: Int) extends ControllerMutationQuota {
    var permitsRecorded = 0.0

    override def isExceeded: Boolean = permitsRecorded > quota

    override def record(permits: Double): Unit = {
      if (permits >= 0) {
        permitsRecorded += permits
        if (isExceeded)
          throw new ThrottlingQuotaExceededException(throttleTime, MockControllerMutationQuota.errorMessage)
      }
    }

    override def throttleTime: Int = if (isExceeded) MockControllerMutationQuota.throttleTimeMs else 0
  }

  private val nodeId = 1
  private val brokerRack = "Rack1"
  private val clientID = "Client1"
  private val requestChannelMetrics: RequestChannelMetrics = mock(classOf[RequestChannelMetrics])
  private val requestChannel: RequestChannel = mock(classOf[RequestChannel])
  private val time = new MockTime
  private val clientQuotaManager: ClientQuotaManager = mock(classOf[ClientQuotaManager])
  private val clientRequestQuotaManager: ClientRequestQuotaManager = mock(classOf[ClientRequestQuotaManager])
  private val neverThrottlingClientControllerQuotaManager: ControllerMutationQuotaManager = mock(classOf[ControllerMutationQuotaManager])
  when(neverThrottlingClientControllerQuotaManager.newQuotaFor(
    any(classOf[RequestChannel.Request]),
    any(classOf[Short])
  )).thenReturn(
    MockControllerMutationQuota(Integer.MAX_VALUE) // never throttles
  )
  private val alwaysThrottlingClientControllerQuotaManager: ControllerMutationQuotaManager = mock(classOf[ControllerMutationQuotaManager])
  when(alwaysThrottlingClientControllerQuotaManager.newQuotaFor(
    any(classOf[RequestChannel.Request]),
    any(classOf[Short])
  )).thenReturn(
    MockControllerMutationQuota(0) // always throttles
  )
  private val replicaQuotaManager: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
  private val raftManager: RaftManager[ApiMessageAndVersion] = mock(classOf[RaftManager[ApiMessageAndVersion]])
  private val metadataCache: KRaftMetadataCache = MetadataCache.kRaftMetadataCache(0, () => KRaftVersion.KRAFT_VERSION_0)

  private val quotasNeverThrottleControllerMutations = new QuotaManagers(
    clientQuotaManager,
    clientQuotaManager,
    clientRequestQuotaManager,
    neverThrottlingClientControllerQuotaManager,
    replicaQuotaManager,
    replicaQuotaManager,
    replicaQuotaManager,
    Optional.empty())

  private val quotasAlwaysThrottleControllerMutations = new QuotaManagers(
    clientQuotaManager,
    clientQuotaManager,
    clientRequestQuotaManager,
    alwaysThrottlingClientControllerQuotaManager,
    replicaQuotaManager,
    replicaQuotaManager,
    replicaQuotaManager,
    Optional.empty())

  private var controllerApis: ControllerApis = _

  private def createControllerApis(authorizer: Option[Authorizer],
                                   controller: Controller,
                                   props: Properties = new Properties(),
                                   throttle: Boolean = false,
                                   tenantConfig: org.apache.kafka.server.tenant.TenantConfig =
                                     org.apache.kafka.server.tenant.TenantConfig.empty()): ControllerApis = {
    props.put(KRaftConfigs.NODE_ID_CONFIG, nodeId: java.lang.Integer)
    props.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller")
    props.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    props.put(SocketServerConfigs.LISTENERS_CONFIG, "CONTROLLER://:9092")
    props.put(QuorumConfig.QUORUM_VOTERS_CONFIG, s"$nodeId@localhost:9092")
    new ControllerApis(
      requestChannel,
      authorizer,
      if (throttle) quotasAlwaysThrottleControllerMutations else quotasNeverThrottleControllerMutations,
      time,
      controller,
      raftManager,
      new KafkaConfig(props),
      "JgxuGe9URy-E-ceaL04lEw",
      new ControllerRegistrationsPublisher(),
      new SimpleApiVersionManager(
        ListenerType.CONTROLLER,
        true,
        () => FinalizedFeatures.fromKRaftVersion(MetadataVersion.latestTesting())),
      metadataCache,
      tenantConfig
    )
  }

  /**
   * Build a RequestChannel.Request from the AbstractRequest
   *
   * @param request - AbstractRequest
   * @param listenerName - Default listener for the RequestChannel
   * @tparam T - Type of AbstractRequest
   * @return
   */
  private def buildRequest[T <: AbstractRequest](
    request: AbstractRequest,
    listenerName: ListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
  ): RequestChannel.Request = {
    val buffer = request.serializeWithHeader(new RequestHeader(request.apiKey, request.version, clientID, 0))

    // read the header from the buffer first so that the body can be read next from the Request constructor
    val header = RequestHeader.parse(buffer)
    val context = new RequestContext(header, "1", InetAddress.getLocalHost, KafkaPrincipal.ANONYMOUS,
      listenerName, SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false)
    new RequestChannel.Request(processor = 1, context = context, startTimeNanos = 0, MemoryPool.NONE, buffer,
      requestChannelMetrics)
  }

  def createDenyAllAuthorizer(): Authorizer = {
    val authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(
      any(classOf[AuthorizableRequestContext]),
      any(classOf[java.util.List[Action]])
    )).thenReturn(
      singletonList(AuthorizationResult.DENIED)
    )
    authorizer
  }

  @Test
  def testUnauthorizedFetch(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => {
      controllerApis = createControllerApis(Some(createDenyAllAuthorizer()), new MockController.Builder().build())
      controllerApis.handleFetch(buildRequest(new FetchRequest(new FetchRequestData(), 12)))
    })
  }

  @Test
  def testFetchSentToKRaft(): Unit = {
    when(
      raftManager.handleRequest(
        any(classOf[RequestContext]),
        any(classOf[RequestHeader]),
        any(classOf[ApiMessage]),
        any(classOf[Long])
      )
    ).thenReturn(
      new CompletableFuture[ApiMessage]()
    )

    controllerApis = createControllerApis(None, new MockController.Builder().build())
    controllerApis.handleFetch(buildRequest(new FetchRequest(new FetchRequestData(), 12)))

    verify(raftManager).handleRequest(
      ArgumentMatchers.any(),
      ArgumentMatchers.any(),
      ArgumentMatchers.any(),
      ArgumentMatchers.any()
    )
  }

  @Test
  def testFetchLocalTimeComputedCorrectly(): Unit = {
    val localTimeDurationMs = 5
    val initialTimeNanos = time.nanoseconds()
    val initialTimeMs = time.milliseconds()

    when(
      raftManager.handleRequest(
        any(classOf[RequestContext]),
        any(classOf[RequestHeader]),
        any(classOf[ApiMessage]),
        any(classOf[Long])
      )
    ).thenAnswer { _ =>
      time.sleep(localTimeDurationMs)
      new CompletableFuture[ApiMessage]()
    }

    // Local time should be updated when `ControllerApis.handle` returns
    val fetchRequestData = new FetchRequestData()
    val request = buildRequest(new FetchRequest(fetchRequestData, ApiKeys.FETCH.latestVersion))
    controllerApis = createControllerApis(None, new MockController.Builder().build())
    controllerApis.handle(request, RequestLocal.noCaching)


    verify(raftManager).handleRequest(
      ArgumentMatchers.eq(request.context),
      ArgumentMatchers.eq(request.header),
      ArgumentMatchers.eq(fetchRequestData),
      ArgumentMatchers.eq(initialTimeMs)
    )

    assertEquals(localTimeDurationMs, TimeUnit.MILLISECONDS.convert(
      request.apiLocalCompleteTimeNanos - initialTimeNanos,
      TimeUnit.NANOSECONDS
    ))
  }

  @Test
  def testUnauthorizedFetchSnapshot(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => {
      controllerApis = createControllerApis(Some(createDenyAllAuthorizer()), new MockController.Builder().build())
      controllerApis.handleFetchSnapshot(buildRequest(new FetchSnapshotRequest(new FetchSnapshotRequestData(), 0)))
    })
  }

  @Test
  def testFetchSnapshotSentToKRaft(): Unit = {
    when(
      raftManager.handleRequest(
        any(classOf[RequestContext]),
        any(classOf[RequestHeader]),
        any(classOf[ApiMessage]),
        any(classOf[Long])
      )
    ).thenReturn(
      new CompletableFuture[ApiMessage]()
    )

    controllerApis = createControllerApis(None, new MockController.Builder().build())
    controllerApis.handleFetchSnapshot(buildRequest(new FetchSnapshotRequest(new FetchSnapshotRequestData(), 0)))

    verify(raftManager).handleRequest(
      ArgumentMatchers.any(),
      ArgumentMatchers.any(),
      ArgumentMatchers.any(),
      ArgumentMatchers.any()
    )
  }

  @Test
  def testUnauthorizedVote(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => {
      controllerApis = createControllerApis(Some(createDenyAllAuthorizer()), new MockController.Builder().build())
      controllerApis.handleVote(buildRequest(new VoteRequest.Builder(new VoteRequestData()).build(0)))
    })
  }

  @Test
  def testHandleLegacyAlterConfigsErrors(): Unit = {
    val requestData = new AlterConfigsRequestData().setResources(
      new OldAlterConfigsResourceCollection(util.Arrays.asList(
        new OldAlterConfigsResource().
          setResourceName("1").
          setResourceType(ConfigResource.Type.BROKER.id()).
          setConfigs(new OldAlterableConfigCollection(util.Arrays.asList(new OldAlterableConfig().
            setName(CleanerConfig.LOG_CLEANER_BACKOFF_MS_PROP).
            setValue("100000")).iterator())),
        new OldAlterConfigsResource().
          setResourceName("2").
          setResourceType(ConfigResource.Type.BROKER.id()).
          setConfigs(new OldAlterableConfigCollection(util.Arrays.asList(new OldAlterableConfig().
            setName(CleanerConfig.LOG_CLEANER_BACKOFF_MS_PROP).
            setValue("100000")).iterator())),
        new OldAlterConfigsResource().
          setResourceName("2").
          setResourceType(ConfigResource.Type.BROKER.id()).
          setConfigs(new OldAlterableConfigCollection(util.Arrays.asList(new OldAlterableConfig().
            setName(CleanerConfig.LOG_CLEANER_BACKOFF_MS_PROP).
            setValue("100000")).iterator())),
        new OldAlterConfigsResource().
          setResourceName("baz").
          setResourceType(123.toByte).
          setConfigs(new OldAlterableConfigCollection(util.Arrays.asList(new OldAlterableConfig().
            setName("foo").
            setValue("bar")).iterator())),
        ).iterator()))
    val request = buildRequest(new AlterConfigsRequest(requestData, 0))
    controllerApis = createControllerApis(Some(createDenyAllAuthorizer()), new MockController.Builder().build())
    controllerApis.handleLegacyAlterConfigs(request)
    val capturedResponse: ArgumentCaptor[AbstractResponse] =
      ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None))
    assertNotNull(capturedResponse.getValue)
    val response = capturedResponse.getValue.asInstanceOf[AlterConfigsResponse]
    assertEquals(Set(
      new OldAlterConfigsResourceResponse().
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Duplicate resource.").
        setResourceName("2").
        setResourceType(ConfigResource.Type.BROKER.id()),
      new OldAlterConfigsResourceResponse().
        setErrorCode(UNSUPPORTED_VERSION.code()).
        setErrorMessage("Unknown resource type 123.").
        setResourceName("baz").
        setResourceType(123.toByte),
      new OldAlterConfigsResourceResponse().
        setErrorCode(CLUSTER_AUTHORIZATION_FAILED.code()).
        setErrorMessage("Cluster authorization failed.").
        setResourceName("1").
        setResourceType(ConfigResource.Type.BROKER.id())),
      response.data().responses().asScala.toSet)
  }

  @Test
  def testUnauthorizedBeginQuorumEpoch(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => {
      controllerApis = createControllerApis(Some(createDenyAllAuthorizer()), new MockController.Builder().build())
      controllerApis.handleBeginQuorumEpoch(buildRequest(new BeginQuorumEpochRequest.Builder(
        new BeginQuorumEpochRequestData()).build(0)))
    })
  }

  @Test
  def testUnauthorizedEndQuorumEpoch(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => {
      controllerApis = createControllerApis(Some(createDenyAllAuthorizer()), new MockController.Builder().build())
      controllerApis.handleEndQuorumEpoch(buildRequest(new EndQuorumEpochRequest.Builder(
        new EndQuorumEpochRequestData()).build(0)))
    })
  }

  @Test
  def testUnauthorizedDescribeQuorum(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => {
      controllerApis = createControllerApis(Some(createDenyAllAuthorizer()), new MockController.Builder().build())
      controllerApis.handleDescribeQuorum(buildRequest(new DescribeQuorumRequest.Builder(
        new DescribeQuorumRequestData()).build(0)))
    })
  }

  @Test
  def testUnauthorizedHandleAlterPartitionRequest(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => {
      controllerApis = createControllerApis(Some(createDenyAllAuthorizer()), new MockController.Builder().build())
      controllerApis.handleAlterPartitionRequest(buildRequest(new AlterPartitionRequest.Builder(
        new AlterPartitionRequestData()).build(ApiKeys.ALTER_PARTITION.latestVersion)))
    })
  }

  @Test
  def testUnauthorizedHandleBrokerHeartBeatRequest(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => {
      controllerApis = createControllerApis(Some(createDenyAllAuthorizer()), new MockController.Builder().build())
      controllerApis.handleBrokerHeartBeatRequest(buildRequest(new BrokerHeartbeatRequest.Builder(
        new BrokerHeartbeatRequestData()).build(0)))
    })
  }

  @Test
  def testUnauthorizedHandleUnregisterBroker(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => {
      controllerApis = createControllerApis(Some(createDenyAllAuthorizer()), new MockController.Builder().build())
      controllerApis.handleUnregisterBroker(buildRequest(new UnregisterBrokerRequest.Builder(
        new UnregisterBrokerRequestData()).build(0)))
    })
  }

  @Test
  def testClose(): Unit = {
    controllerApis = createControllerApis(Some(createDenyAllAuthorizer()), mock(classOf[Controller]))
    controllerApis.close()
    assertTrue(controllerApis.isClosed)
  }

  @Test
  def testUnauthorizedBrokerRegistration(): Unit = {
    val brokerRegistrationRequest = new BrokerRegistrationRequest.Builder(
      new BrokerRegistrationRequestData()
        .setBrokerId(nodeId)
        .setRack(brokerRack)
    ).build()

    val request = buildRequest(brokerRegistrationRequest)
    val capturedResponse: ArgumentCaptor[AbstractResponse] = ArgumentCaptor.forClass(classOf[AbstractResponse])

    controllerApis = createControllerApis(Some(createDenyAllAuthorizer()), mock(classOf[Controller]))
    controllerApis.handle(request, RequestLocal.withThreadConfinedCaching)
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None))

    assertNotNull(capturedResponse.getValue)

    val brokerRegistrationResponse = capturedResponse.getValue.asInstanceOf[BrokerRegistrationResponse]
    assertEquals(Map(CLUSTER_AUTHORIZATION_FAILED -> 1),
      brokerRegistrationResponse.errorCounts().asScala)
  }

  @Test
  def testUnauthorizedHandleAlterClientQuotas(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => {
      controllerApis = createControllerApis(Some(createDenyAllAuthorizer()), new MockController.Builder().build())
      controllerApis.handleAlterClientQuotas(buildRequest(new AlterClientQuotasRequest(
        new AlterClientQuotasRequestData(), 0)))
    })
  }

  @Test
  def testUnauthorizedHandleIncrementalAlterConfigs(): Unit = {
    val requestData = new IncrementalAlterConfigsRequestData().setResources(
      new AlterConfigsResourceCollection(
        util.Arrays.asList(new AlterConfigsResource().
          setResourceName("1").
          setResourceType(ConfigResource.Type.BROKER.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName(CleanerConfig.LOG_CLEANER_BACKOFF_MS_PROP).
            setValue("100000").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator())),
        new AlterConfigsResource().
          setResourceName("foo").
          setResourceType(ConfigResource.Type.TOPIC.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName(TopicConfig.FLUSH_MS_CONFIG).
            setValue("1000").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator())),
        new AlterConfigsResource().
          setResourceName("sub").
          setResourceType(ConfigResource.Type.CLIENT_METRICS.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName("interval.ms").
            setValue("100000").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator())),
        new AlterConfigsResource().
          setResourceName("group-foo").
          setResourceType(ConfigResource.Type.GROUP.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName("consumer.session.timeout.ms").
            setValue("50000").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator()))
        ).iterator()))
    val request = buildRequest(new IncrementalAlterConfigsRequest.Builder(requestData).build(0))
    controllerApis = createControllerApis(Some(createDenyAllAuthorizer()),
      new MockController.Builder().build())
    controllerApis.handleIncrementalAlterConfigs(request)
    val capturedResponse: ArgumentCaptor[AbstractResponse] =
      ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None))
    assertNotNull(capturedResponse.getValue)
    val response = capturedResponse.getValue.asInstanceOf[IncrementalAlterConfigsResponse]
    assertEquals(Set(new AlterConfigsResourceResponse().
      setErrorCode(CLUSTER_AUTHORIZATION_FAILED.code()).
      setErrorMessage(CLUSTER_AUTHORIZATION_FAILED.message()).
      setResourceName("1").
      setResourceType(ConfigResource.Type.BROKER.id()),
      new AlterConfigsResourceResponse().
        setErrorCode(TOPIC_AUTHORIZATION_FAILED.code()).
        setErrorMessage(TOPIC_AUTHORIZATION_FAILED.message()).
        setResourceName("foo").
        setResourceType(ConfigResource.Type.TOPIC.id()),
      new AlterConfigsResourceResponse().
        setErrorCode(CLUSTER_AUTHORIZATION_FAILED.code()).
        setErrorMessage(CLUSTER_AUTHORIZATION_FAILED.message()).
        setResourceName("sub").
        setResourceType(ConfigResource.Type.CLIENT_METRICS.id()),
      new AlterConfigsResourceResponse().
        setErrorCode(GROUP_AUTHORIZATION_FAILED.code()).
        setErrorMessage(GROUP_AUTHORIZATION_FAILED.message()).
        setResourceName("group-foo").
        setResourceType(ConfigResource.Type.GROUP.id())),
      response.data().responses().asScala.toSet)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testInvalidIncrementalAlterConfigsResources(denyAllAuthorizer: Boolean): Unit = {
    val requestData = new IncrementalAlterConfigsRequestData().setResources(
      new AlterConfigsResourceCollection(util.Arrays.asList(
        new AlterConfigsResource().
          setResourceName("1").
          setResourceType(ConfigResource.Type.BROKER_LOGGER.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName("kafka.server.ControllerApisTest").
            setValue("DEBUG").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator())),
        new AlterConfigsResource().
          setResourceName("3").
          setResourceType(ConfigResource.Type.BROKER.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName(CleanerConfig.LOG_CLEANER_BACKOFF_MS_PROP).
            setValue("100000").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator())),
        new AlterConfigsResource().
          setResourceName("3").
          setResourceType(ConfigResource.Type.BROKER.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName(CleanerConfig.LOG_CLEANER_BACKOFF_MS_PROP).
            setValue("100000").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator())),
        new AlterConfigsResource().
          setResourceName("foo").
          setResourceType(124.toByte).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName("foo").
            setValue("bar").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator())),
        new AlterConfigsResource().
          setResourceName("sub").
          setResourceType(ConfigResource.Type.CLIENT_METRICS.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName("interval.ms").
            setValue("1").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator())),
        new AlterConfigsResource().
          setResourceName("sub1").
          setResourceType(ConfigResource.Type.CLIENT_METRICS.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName("interval.ms").
            setValue("1").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator())),
        new AlterConfigsResource().
          setResourceName("sub1").
          setResourceType(ConfigResource.Type.CLIENT_METRICS.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName("interval.ms").
            setValue("1").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator())),
        new AlterConfigsResource().
          setResourceName("group-foo").
          setResourceType(ConfigResource.Type.GROUP.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName("consumer.session.timeout.ms").
            setValue("50000").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator())),
        new AlterConfigsResource().
          setResourceName("group-foo1").
          setResourceType(ConfigResource.Type.GROUP.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName("consumer.session.timeout.ms").
            setValue("50000").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator())),
        new AlterConfigsResource().
          setResourceName("group-foo1").
          setResourceType(ConfigResource.Type.GROUP.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName("consumer.session.timeout.ms").
            setValue("50000").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator()))
        ).iterator()))
    val request = buildRequest(new IncrementalAlterConfigsRequest.Builder(requestData).build(0))
    val authorizer = if (denyAllAuthorizer) {
      Some(createDenyAllAuthorizer())
    } else {
      None
    }
    controllerApis = createControllerApis(authorizer, new MockController.Builder().build())
    controllerApis.handleIncrementalAlterConfigs(request)
    val capturedResponse: ArgumentCaptor[AbstractResponse] =
      ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None))
    assertNotNull(capturedResponse.getValue)
    val response = capturedResponse.getValue.asInstanceOf[IncrementalAlterConfigsResponse]
    assertEquals(Set(
      new AlterConfigsResourceResponse().
        setErrorCode(if (denyAllAuthorizer) CLUSTER_AUTHORIZATION_FAILED.code() else NONE.code()).
        setErrorMessage(if (denyAllAuthorizer) CLUSTER_AUTHORIZATION_FAILED.message() else null).
        setResourceName("1").
        setResourceType(ConfigResource.Type.BROKER_LOGGER.id()),
      new AlterConfigsResourceResponse().
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Duplicate resource.").
        setResourceName("3").
        setResourceType(ConfigResource.Type.BROKER.id()),
      new AlterConfigsResourceResponse().
        setErrorCode(UNSUPPORTED_VERSION.code()).
        setErrorMessage("Unknown resource type 124.").
        setResourceName("foo").
        setResourceType(124.toByte),
      new AlterConfigsResourceResponse().
        setErrorCode(if (denyAllAuthorizer) CLUSTER_AUTHORIZATION_FAILED.code() else NONE.code()).
        setErrorMessage(if (denyAllAuthorizer) CLUSTER_AUTHORIZATION_FAILED.message() else null).
        setResourceName("sub").
        setResourceType(ConfigResource.Type.CLIENT_METRICS.id()),
      new AlterConfigsResourceResponse().
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Duplicate resource.").
        setResourceName("sub1").
        setResourceType(ConfigResource.Type.CLIENT_METRICS.id()),
      new AlterConfigsResourceResponse().
        setErrorCode(if (denyAllAuthorizer) GROUP_AUTHORIZATION_FAILED.code() else NONE.code()).
        setErrorMessage(if (denyAllAuthorizer) GROUP_AUTHORIZATION_FAILED.message() else null).
        setResourceName("group-foo").
        setResourceType(ConfigResource.Type.GROUP.id()),
      new AlterConfigsResourceResponse().
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Duplicate resource.").
        setResourceName("group-foo1").
        setResourceType(ConfigResource.Type.GROUP.id())),
      response.data().responses().asScala.toSet)
  }

  @Test
  def testUnauthorizedHandleAlterPartitionReassignments(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => {
      controllerApis = createControllerApis(Some(createDenyAllAuthorizer()), new MockController.Builder().build())
      controllerApis.handleAlterPartitionReassignments(buildRequest(new AlterPartitionReassignmentsRequest.Builder(
        new AlterPartitionReassignmentsRequestData()).build()))
    })
  }

  @Test
  def testUnauthorizedHandleAllocateProducerIds(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => {
      controllerApis = createControllerApis(Some(createDenyAllAuthorizer()), new MockController.Builder().build())
      controllerApis.handleAllocateProducerIdsRequest(buildRequest(new AllocateProducerIdsRequest.Builder(
        new AllocateProducerIdsRequestData()).build()))
    })
  }

  @Test
  def testUnauthorizedHandleListPartitionReassignments(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => {
      controllerApis = createControllerApis(Some(createDenyAllAuthorizer()), new MockController.Builder().build())
      controllerApis.handleListPartitionReassignments(buildRequest(new ListPartitionReassignmentsRequest.Builder(
        new ListPartitionReassignmentsRequestData()).build()))
    })
  }

  @Test
  def testCreateTopics(): Unit = {
    val controller = new MockController.Builder().build()
    controllerApis = createControllerApis(None, controller)
    val request = new CreateTopicsRequestData().setTopics(new CreatableTopicCollection(
      util.Arrays.asList(new CreatableTopic().setName("foo").setNumPartitions(1).setReplicationFactor(3),
        new CreatableTopic().setName("foo").setNumPartitions(2).setReplicationFactor(3),
        new CreatableTopic().setName("bar").setNumPartitions(2).setReplicationFactor(3),
        new CreatableTopic().setName("bar").setNumPartitions(2).setReplicationFactor(3),
        new CreatableTopic().setName("bar").setNumPartitions(2).setReplicationFactor(3),
        new CreatableTopic().setName("baz").setNumPartitions(2).setReplicationFactor(3),
        new CreatableTopic().setName("indescribable").setNumPartitions(2).setReplicationFactor(3),
        new CreatableTopic().setName("quux").setNumPartitions(2).setReplicationFactor(3),
        new CreatableTopic().setName(Topic.CLUSTER_METADATA_TOPIC_NAME).setNumPartitions(2).setReplicationFactor(3),
      ).iterator()))
    val expectedResponse = Set(new CreatableTopicResult().setName("foo").
      setErrorCode(INVALID_REQUEST.code()).
      setErrorMessage("Duplicate topic name."),
      new CreatableTopicResult().setName("bar").
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Duplicate topic name."),
      new CreatableTopicResult().setName("baz").
        setErrorCode(NONE.code()).
        setTopicId(new Uuid(0L, 1L)).
        setNumPartitions(2).
        setReplicationFactor(3).
        setTopicConfigErrorCode(NONE.code()),
      new CreatableTopicResult().setName("indescribable").
        setErrorCode(NONE.code()).
        setTopicId(new Uuid(0L, 2L)).
        setTopicConfigErrorCode(TOPIC_AUTHORIZATION_FAILED.code()),
      new CreatableTopicResult().setName("quux").
        setErrorCode(TOPIC_AUTHORIZATION_FAILED.code()).
        setErrorMessage("Authorization failed."),
      new CreatableTopicResult().setName(Topic.CLUSTER_METADATA_TOPIC_NAME).
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage(s"Creation of internal topic ${Topic.CLUSTER_METADATA_TOPIC_NAME} is prohibited."))
    assertEquals(expectedResponse, controllerApis.createTopics(ANONYMOUS_CONTEXT, request,
      hasClusterAuth = false,
      _ => Set("baz", "indescribable"),
      _ => Set("baz")).get().topics().asScala.toSet)
  }

  @ParameterizedTest(name = "testCreateTopicsMutationQuota with throttle: {0}")
  @ValueSource(booleans = Array(true, false))
  def testCreateTopicsMutationQuota(throttle: Boolean): Unit = {
    val controller = new MockController.Builder().build()
    controllerApis = createControllerApis(None, controller, new Properties(), throttle)
    val topicName = "foo"
    val requestData = new CreateTopicsRequestData().setTopics(new CreatableTopicCollection(
      util.Collections.singletonList(new CreatableTopic().setName(topicName).setNumPartitions(1).setReplicationFactor(1)).iterator()))
    val request = new CreateTopicsRequest.Builder(requestData).build()
    val expectedResponseDataUnthrottled = Set(new CreatableTopicResult().setName(topicName).
      setErrorCode(NONE.code()).
      setTopicId(new Uuid(0L, 1L)).
      setNumPartitions(1).
      setReplicationFactor(1).
      setTopicConfigErrorCode(NONE.code()))
    val expectedResponseDataThrottled = Set(new CreatableTopicResult().setName(topicName).
      setErrorCode(THROTTLING_QUOTA_EXCEEDED.code()).
      setErrorMessage(THROTTLING_QUOTA_EXCEEDED.message()))
    val response = handleRequest[CreateTopicsResponse](request, controllerApis)
    if (throttle) {
      assertEquals(expectedResponseDataThrottled, response.data.topics().asScala.toSet)
      assertEquals(MockControllerMutationQuota.throttleTimeMs, response.throttleTimeMs())
    } else {
      assertEquals(expectedResponseDataUnthrottled, response.data.topics().asScala.toSet)
      assertEquals(0, response.throttleTimeMs())
    }
  }

  @Test
  def testDeleteTopicsByName(): Unit = {
    val fooId = Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")
    val controller = new MockController.Builder().newInitialTopic("foo", fooId).build()
    controllerApis = createControllerApis(None, controller)
    val request = new DeleteTopicsRequestData().setTopicNames(
      util.Arrays.asList("foo", "bar", "quux", "quux"))
    val expectedResponse = Set(new DeletableTopicResult().setName("quux").
      setErrorCode(INVALID_REQUEST.code()).
      setErrorMessage("Duplicate topic name."),
      new DeletableTopicResult().setName("bar").
        setErrorCode(UNKNOWN_TOPIC_OR_PARTITION.code()).
        setErrorMessage("This server does not host this topic-partition."),
      new DeletableTopicResult().setName("foo").setTopicId(fooId))
    assertEquals(expectedResponse, controllerApis.deleteTopics(ANONYMOUS_CONTEXT, request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      hasClusterAuth = true,
      _ => Set.empty,
      _ => Set.empty).get().asScala.toSet)
  }

  @Test
  def testDeleteTopicsById(): Unit = {
    val fooId = Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")
    val barId = Uuid.fromString("VlFu5c51ToiNx64wtwkhQw")
    val quuxId = Uuid.fromString("ObXkLhL_S5W62FAE67U3MQ")
    val controller = new MockController.Builder().newInitialTopic("foo", fooId).build()
    controllerApis = createControllerApis(None, controller)
    val request = new DeleteTopicsRequestData()
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(fooId))
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(barId))
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(quuxId))
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(quuxId))
    val response = Set(new DeletableTopicResult().setName(null).setTopicId(quuxId).
      setErrorCode(INVALID_REQUEST.code()).
      setErrorMessage("Duplicate topic id."),
      new DeletableTopicResult().setName(null).setTopicId(barId).
        setErrorCode(UNKNOWN_TOPIC_ID.code()).
        setErrorMessage("This server does not host this topic ID."),
      new DeletableTopicResult().setName("foo").setTopicId(fooId))
    assertEquals(response, controllerApis.deleteTopics(ANONYMOUS_CONTEXT, request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      hasClusterAuth = true,
      _ => Set.empty,
      _ => Set.empty).get().asScala.toSet)
  }

  @Test
  def testInvalidDeleteTopicsRequest(): Unit = {
    val fooId = Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")
    val barId = Uuid.fromString("VlFu5c51ToiNx64wtwkhQw")
    val bazId = Uuid.fromString("YOS4oQ3UT9eSAZahN1ysSA")
    val controller = new MockController.Builder().
      newInitialTopic("foo", fooId).
      newInitialTopic("bar", barId).build()
    controllerApis = createControllerApis(None, controller)
    val request = new DeleteTopicsRequestData()
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(ZERO_UUID))
    request.topics().add(new DeleteTopicState().setName("foo").setTopicId(fooId))
    request.topics().add(new DeleteTopicState().setName("bar").setTopicId(ZERO_UUID))
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(barId))
    request.topics().add(new DeleteTopicState().setName("quux").setTopicId(ZERO_UUID))
    request.topics().add(new DeleteTopicState().setName("quux").setTopicId(ZERO_UUID))
    request.topics().add(new DeleteTopicState().setName("quux").setTopicId(ZERO_UUID))
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(bazId))
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(bazId))
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(bazId))
    val response = Set(new DeletableTopicResult().setName(null).setTopicId(ZERO_UUID).
      setErrorCode(INVALID_REQUEST.code()).
      setErrorMessage("Neither topic name nor id were specified."),
      new DeletableTopicResult().setName("foo").setTopicId(fooId).
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("You may not specify both topic name and topic id."),
      new DeletableTopicResult().setName("bar").setTopicId(barId).
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("The provided topic name maps to an ID that was already supplied."),
      new DeletableTopicResult().setName("quux").setTopicId(ZERO_UUID).
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Duplicate topic name."),
      new DeletableTopicResult().setName(null).setTopicId(bazId).
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Duplicate topic id."))
    assertEquals(response, controllerApis.deleteTopics(ANONYMOUS_CONTEXT, request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      hasClusterAuth = false,
      names => names.toSet,
      names => names.toSet).get().asScala.toSet)
  }

  @Test
  def testNotAuthorizedToDeleteWithTopicExisting(): Unit = {
    val fooId = Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")
    val barId = Uuid.fromString("VlFu5c51ToiNx64wtwkhQw")
    val bazId = Uuid.fromString("hr4TVh3YQiu3p16Awkka6w")
    val quuxId = Uuid.fromString("5URoQzW_RJiERVZXJgUVLg")
    val controller = new MockController.Builder().
      newInitialTopic("foo", fooId).
      newInitialTopic("bar", barId).
      newInitialTopic("baz", bazId).
      newInitialTopic("quux", quuxId).build()
    controllerApis = createControllerApis(None, controller)
    val request = new DeleteTopicsRequestData()
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(fooId))
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(barId))
    request.topics().add(new DeleteTopicState().setName("baz").setTopicId(ZERO_UUID))
    request.topics().add(new DeleteTopicState().setName("quux").setTopicId(ZERO_UUID))
    val response = Set(new DeletableTopicResult().setName(null).setTopicId(barId).
      setErrorCode(TOPIC_AUTHORIZATION_FAILED.code).
      setErrorMessage(TOPIC_AUTHORIZATION_FAILED.message),
      new DeletableTopicResult().setName("quux").setTopicId(ZERO_UUID).
        setErrorCode(TOPIC_AUTHORIZATION_FAILED.code).
        setErrorMessage(TOPIC_AUTHORIZATION_FAILED.message),
      new DeletableTopicResult().setName("baz").setTopicId(ZERO_UUID).
        setErrorCode(TOPIC_AUTHORIZATION_FAILED.code).
        setErrorMessage(TOPIC_AUTHORIZATION_FAILED.message),
      new DeletableTopicResult().setName("foo").setTopicId(fooId).
        setErrorCode(TOPIC_AUTHORIZATION_FAILED.code).
        setErrorMessage(TOPIC_AUTHORIZATION_FAILED.message))
    assertEquals(response, controllerApis.deleteTopics(ANONYMOUS_CONTEXT, request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      hasClusterAuth = false,
      _ => Set("foo", "baz"),
      _ => Set.empty).get().asScala.toSet)
  }

  @Test
  def testNotAuthorizedToDeleteWithTopicNotExisting(): Unit = {
    val barId = Uuid.fromString("VlFu5c51ToiNx64wtwkhQw")
    val controller = new MockController.Builder().build()
    controllerApis = createControllerApis(None, controller)
    val request = new DeleteTopicsRequestData()
    request.topics().add(new DeleteTopicState().setName("foo").setTopicId(ZERO_UUID))
    request.topics().add(new DeleteTopicState().setName("bar").setTopicId(ZERO_UUID))
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(barId))
    val expectedResponse = Set(new DeletableTopicResult().setName("foo").
      setErrorCode(UNKNOWN_TOPIC_OR_PARTITION.code).
      setErrorMessage(UNKNOWN_TOPIC_OR_PARTITION.message),
      new DeletableTopicResult().setName("bar").
        setErrorCode(TOPIC_AUTHORIZATION_FAILED.code).
        setErrorMessage(TOPIC_AUTHORIZATION_FAILED.message),
      new DeletableTopicResult().setName(null).setTopicId(barId).
        setErrorCode(UNKNOWN_TOPIC_ID.code).
        setErrorMessage(UNKNOWN_TOPIC_ID.message))
    assertEquals(expectedResponse, controllerApis.deleteTopics(ANONYMOUS_CONTEXT, request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      hasClusterAuth = false,
      _ => Set("foo"),
      _ => Set.empty).get().asScala.toSet)
  }

  @Test
  def testNotControllerErrorPreventsDeletingTopics(): Unit = {
    val fooId = Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")
    val barId = Uuid.fromString("VlFu5c51ToiNx64wtwkhQw")
    val controller = new MockController.Builder().
      newInitialTopic("foo", fooId).build()
    controller.setActive(false)
    controllerApis = createControllerApis(None, controller)
    val request = new DeleteTopicsRequestData()
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(fooId))
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(barId))
    assertEquals(classOf[NotControllerException], assertThrows(
      classOf[ExecutionException], () => controllerApis.deleteTopics(ANONYMOUS_CONTEXT, request,
        ApiKeys.DELETE_TOPICS.latestVersion().toInt,
        hasClusterAuth = false,
        _ => Set("foo", "bar"),
        _ => Set("foo", "bar")).get()).getCause.getClass)
  }

  @Test
  def testDeleteTopicsDisabled(): Unit = {
    val fooId = Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")
    val controller = new MockController.Builder().
      newInitialTopic("foo", fooId).build()
    val props = new Properties()
    props.put(ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG, "false")
    controllerApis = createControllerApis(None, controller, props)
    val request = new DeleteTopicsRequestData()
    request.topics().add(new DeleteTopicState().setName("foo").setTopicId(ZERO_UUID))
    assertThrows(classOf[TopicDeletionDisabledException],
      () => controllerApis.deleteTopics(ANONYMOUS_CONTEXT, request,
        ApiKeys.DELETE_TOPICS.latestVersion().toInt,
        hasClusterAuth = false,
        _ => Set("foo", "bar"),
        _ => Set("foo", "bar")))
    assertThrows(classOf[InvalidRequestException],
      () => controllerApis.deleteTopics(ANONYMOUS_CONTEXT, request,
        1,
        hasClusterAuth = false,
        _ => Set("foo", "bar"),
        _ => Set("foo", "bar")))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testCreatePartitionsRequest(validateOnly: Boolean): Unit = {
    val controller = mock(classOf[Controller])
    controllerApis = createControllerApis(None, controller)
    val request = new CreatePartitionsRequestData()
    request.topics().add(new CreatePartitionsTopic().setName("foo").setAssignments(null).setCount(5))
    request.topics().add(new CreatePartitionsTopic().setName("bar").setAssignments(null).setCount(5))
    request.topics().add(new CreatePartitionsTopic().setName("bar").setAssignments(null).setCount(5))
    request.topics().add(new CreatePartitionsTopic().setName("bar").setAssignments(null).setCount(5))
    request.topics().add(new CreatePartitionsTopic().setName("baz").setAssignments(null).setCount(5))
    request.setValidateOnly(validateOnly)

    // Check if the controller is called correctly with the 'validateOnly' field set appropriately.
    when(controller.createPartitions(
      any(),
      ArgumentMatchers.eq(
        Collections.singletonList(
          new CreatePartitionsTopic().setName("foo").setAssignments(null).setCount(5))),
      ArgumentMatchers.eq(validateOnly))).thenReturn(CompletableFuture
      .completedFuture(Collections.singletonList(
        new CreatePartitionsTopicResult().setName("foo").
          setErrorCode(NONE.code()).
          setErrorMessage(null)
      )))
    assertEquals(Set(new CreatePartitionsTopicResult().setName("foo").
      setErrorCode(NONE.code()).
      setErrorMessage(null),
      new CreatePartitionsTopicResult().setName("bar").
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Duplicate topic name."),
      new CreatePartitionsTopicResult().setName("baz").
        setErrorCode(TOPIC_AUTHORIZATION_FAILED.code()).
        setErrorMessage(null)),
      controllerApis.createPartitions(ANONYMOUS_CONTEXT, request,
        _ => Set("foo", "bar")).get().asScala.toSet)
  }

  @Test
  def testCreatePartitionsAuthorization(): Unit = {
    val controller = new MockController.Builder()
      .newInitialTopic("foo", Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q"))
      .build()
    val authorizer = mock(classOf[Authorizer])
    controllerApis = createControllerApis(Some(authorizer), controller)
    val requestData = new CreatePartitionsRequestData()
    requestData.topics().add(new CreatePartitionsTopic().setName("foo").setAssignments(null).setCount(2))
    requestData.topics().add(new CreatePartitionsTopic().setName("bar").setAssignments(null).setCount(10))
    val request = new CreatePartitionsRequest.Builder(requestData).build()

    val fooResource = new ResourcePattern(ResourceType.TOPIC, "foo", PatternType.LITERAL)
    val fooAction = new Action(AclOperation.ALTER, fooResource, 1, true, true)

    val barResource = new ResourcePattern(ResourceType.TOPIC, "bar", PatternType.LITERAL)
    val barAction = new Action(AclOperation.ALTER, barResource, 1, true, true)

    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument[util.List[Action]](1).asScala
      val results = actions.map { action =>
        if (action == fooAction) AuthorizationResult.ALLOWED
        else if (action == barAction) AuthorizationResult.DENIED
        else throw new AssertionError(s"Unexpected action $action")
      }
      new util.ArrayList[AuthorizationResult](results.asJava)
    }

    val response = handleRequest[CreatePartitionsResponse](request, controllerApis)
    val results = response.data.results.asScala
    assertEquals(Some(Errors.NONE), results.find(_.name == "foo").map(result => Errors.forCode(result.errorCode)))
    assertEquals(Some(Errors.TOPIC_AUTHORIZATION_FAILED), results.find(_.name == "bar").map(result => Errors.forCode(result.errorCode)))
  }

  @ParameterizedTest(name = "testCreatePartitionsMutationQuota with throttle: {0}")
  @ValueSource(booleans = Array(true, false))
  def testCreatePartitionsMutationQuota(throttle: Boolean): Unit = {
    val topicName = "foo"
    val controller = new MockController.Builder()
      .newInitialTopic(topicName, Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q"), 1)
      .build()
    controllerApis = createControllerApis(None, controller, new Properties(), throttle)
    val requestData = new CreatePartitionsRequestData()
    requestData.topics().add(new CreatePartitionsTopic().setName(topicName).setAssignments(null).setCount(2))
    val request = new CreatePartitionsRequest.Builder(requestData).build()
    val expectedResponseDataUnthrottled = Set(new CreatePartitionsTopicResult().setName(topicName).
      setErrorCode(NONE.code()))
    val expectedResponseDataThrottled = Set(new CreatePartitionsTopicResult().setName(topicName).
      setErrorCode(THROTTLING_QUOTA_EXCEEDED.code()).
      setErrorMessage(THROTTLING_QUOTA_EXCEEDED.message()))
    val response = handleRequest[CreatePartitionsResponse](request, controllerApis)
    if (throttle) {
      assertEquals(expectedResponseDataThrottled, response.data.results().asScala.toSet)
      assertEquals(MockControllerMutationQuota.throttleTimeMs, response.throttleTimeMs())
    } else {
      assertEquals(expectedResponseDataUnthrottled, response.data.results().asScala.toSet)
      assertEquals(0, response.throttleTimeMs())
    }
  }

  @Test
  def testElectLeadersAuthorization(): Unit = {
    val authorizer = mock(classOf[Authorizer])
    val controller = mock(classOf[Controller])
    controllerApis = createControllerApis(Some(authorizer), controller)
    val request = new ElectLeadersRequest.Builder(
      ElectionType.PREFERRED,
      null,
      30000
    ).build()

    val resource = new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL)
    val actions = singletonList(new Action(AclOperation.ALTER, resource, 1, true, true))

    when(authorizer.authorize(
      any[RequestContext],
      ArgumentMatchers.eq(actions)
    )).thenReturn(singletonList(AuthorizationResult.DENIED))

    val response = handleRequest[ElectLeadersResponse](request, controllerApis)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED, Errors.forCode(response.data.errorCode))
  }

  @Test
  def testElectLeadersHandledByController(): Unit = {
    val controller = mock(classOf[Controller])
    controllerApis = createControllerApis(None, controller)
    val request = new ElectLeadersRequest.Builder(
      ElectionType.PREFERRED,
      null,
      30000
    ).build()

    val responseData = new ElectLeadersResponseData()
      .setErrorCode(Errors.NOT_CONTROLLER.code)

    when(controller.electLeaders(any[ControllerRequestContext],
      ArgumentMatchers.eq(request.data)
    )).thenReturn(CompletableFuture.completedFuture(responseData))

    val response = handleRequest[ElectLeadersResponse](request, controllerApis)
    assertEquals(Errors.NOT_CONTROLLER, Errors.forCode(response.data.errorCode))
  }

  @Test
  def testDeleteTopicsReturnsNotController(): Unit = {
    val topicId = Uuid.randomUuid()
    val topicName = "foo"
    val controller = mock(classOf[Controller])
    controllerApis = createControllerApis(None, controller)
    val findNamesFuture = CompletableFuture.completedFuture(
      singletonMap(topicId, new ResultOrError(topicName))
    )
    when(controller.findTopicNames(
      any[ControllerRequestContext],
      ArgumentMatchers.eq(singleton(topicId))
    )).thenReturn(findNamesFuture)

    val findIdsFuture = CompletableFuture.completedFuture(
      Collections.emptyMap[String, ResultOrError[Uuid]]()
    )
    when(controller.findTopicIds(
      any[ControllerRequestContext],
      ArgumentMatchers.eq(Collections.emptySet())
    )).thenReturn(findIdsFuture)

    val deleteFuture = new CompletableFuture[util.Map[Uuid, ApiError]]()
    deleteFuture.completeExceptionally(new NotControllerException("Controller has moved"))
    when(controller.deleteTopics(
      any[ControllerRequestContext],
      ArgumentMatchers.eq(singleton(topicId))
    )).thenReturn(deleteFuture)

    val request = new DeleteTopicsRequest.Builder(
      new DeleteTopicsRequestData().setTopics(singletonList(
        new DeleteTopicState().setTopicId(topicId)
      ))
    ).build()

    val response = handleRequest[DeleteTopicsResponse](request, controllerApis)
    val topicIdResponse = response.data.responses.asScala.find(_.topicId == topicId).get
    assertEquals(Errors.NOT_CONTROLLER, Errors.forCode(topicIdResponse.errorCode))
  }

  @ParameterizedTest(name = "testDeleteTopicsMutationQuota with throttle: {0}")
  @ValueSource(booleans = Array(true, false))
  def testDeleteTopicsMutationQuota(throttle: Boolean): Unit = {
    val topicId = Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")
    val topicName = "foo"
    val controller = new MockController.Builder()
      .newInitialTopic(topicName, topicId, 1)
      .build()
    controllerApis = createControllerApis(None, controller, new Properties(), throttle)
    val requestData = new DeleteTopicsRequestData().setTopics(singletonList(
      new DeleteTopicState().setTopicId(topicId)))
    val request = new DeleteTopicsRequest.Builder(requestData).build()
    val expectedResponseDataUnthrottled = Set(new DeletableTopicResult().setName(topicName).
      setTopicId(topicId).
      setErrorCode(NONE.code()))
    val expectedResponseDataThrottled = Set(new DeletableTopicResult().setName(topicName).
      setTopicId(topicId).
      setErrorCode(THROTTLING_QUOTA_EXCEEDED.code()).
      setErrorMessage(THROTTLING_QUOTA_EXCEEDED.message()))
    val response = handleRequest[DeleteTopicsResponse](request, controllerApis)
    if (throttle) {
      assertEquals(expectedResponseDataThrottled, response.data.responses().asScala.toSet)
      assertEquals(MockControllerMutationQuota.throttleTimeMs, response.throttleTimeMs())
    } else {
      assertEquals(expectedResponseDataUnthrottled, response.data.responses().asScala.toSet)
      assertEquals(0, response.throttleTimeMs())
    }
  }

  @Test
  def testAllocateProducerIdsReturnsNotController(): Unit = {
    val controller = mock(classOf[Controller])
    controllerApis = createControllerApis(None, controller)
    // We construct the future here to mimic the logic in `QuorumController.allocateProducerIds`.
    // When an exception is raised on the original future, the `thenApply` future is also completed
    // exceptionally, but the underlying cause is wrapped in a `CompletionException`.
    val future = new CompletableFuture[ProducerIdsBlock]
    val thenApplyFuture = future.thenApply[AllocateProducerIdsResponseData] { result =>
      new AllocateProducerIdsResponseData()
        .setProducerIdStart(result.firstProducerId())
        .setProducerIdLen(result.size())
    }
    future.completeExceptionally(new NotControllerException("Controller has moved"))

    val request = new AllocateProducerIdsRequest.Builder(
      new AllocateProducerIdsRequestData()
        .setBrokerId(4)
        .setBrokerEpoch(93234)
    ).build()

    when(controller.allocateProducerIds(
      any[ControllerRequestContext],
      ArgumentMatchers.eq(request.data)
    )).thenReturn(thenApplyFuture)

    val response = handleRequest[AllocateProducerIdsResponse](request, controllerApis)
    assertEquals(Errors.NOT_CONTROLLER, response.error)
  }

  @Test
  def testAssignReplicasToDirs(): Unit = {
    val controller = mock(classOf[Controller])
    val authorizer = mock(classOf[Authorizer])
    controllerApis = createControllerApis(Some(authorizer), controller)
    val request = new AssignReplicasToDirsRequest.Builder(new AssignReplicasToDirsRequestData()).build()

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(Collections.singletonList(new Action(
      AclOperation.CLUSTER_ACTION,
      new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL),
      1, true, true
    )))))
      .thenReturn(Collections.singletonList(AuthorizationResult.ALLOWED))
    when(controller.assignReplicasToDirs(any[ControllerRequestContext], ArgumentMatchers.eq(request.data)))
      .thenReturn(FutureUtils.failedFuture[AssignReplicasToDirsResponseData](Errors.UNKNOWN_TOPIC_OR_PARTITION.exception()))

    val response = handleRequest[AssignReplicasToDirsResponse](request, controllerApis)
    assertEquals(new AssignReplicasToDirsResponseData().setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()), response.data)
  }

  private def handleRequest[T <: AbstractResponse](
    request: AbstractRequest,
    controllerApis: ControllerApis
  )(
    implicit classTag: ClassTag[T]
  ): T = {
    val req = buildRequest(request)

    controllerApis.handle(req, RequestLocal.noCaching)

    val capturedResponse: ArgumentCaptor[AbstractResponse] =
      ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(req),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )

    capturedResponse.getValue match {
      case response: T => response
      case response =>
        throw new ClassCastException(s"Expected response with type ${classTag.runtimeClass}, " +
          s"but found ${response.getClass}")
    }

  }

  @Test
  def testCompletableFutureExceptions(): Unit = {
    // This test simulates an error in a completable future as we return from the controller. We need to ensure
    // that any exception throw in the completion phase is properly captured and translated to an error response.
    val request = buildRequest(new FetchRequest(new FetchRequestData(), 12))
    val response = new FetchResponseData()
    val responseFuture = new CompletableFuture[ApiMessage]()
    val errorResponseFuture = new AtomicReference[AbstractResponse]()
    when(raftManager.handleRequest(any(), any(), any(), any())).thenReturn(responseFuture)
    when(requestChannel.sendResponse(any(), any(), any())).thenAnswer { _ =>
      // Simulate an encoding failure in the initial fetch response
      throw new UnsupportedVersionException("Something went wrong")
    }.thenAnswer { invocation =>
      val resp = invocation.getArgument(1, classOf[AbstractResponse])
      errorResponseFuture.set(resp)
    }

    // Calling handle does not block since we do not call get() in ControllerApis
    controllerApis = createControllerApis(None, new MockController.Builder().build())
    controllerApis.handle(request, null)

    // When we complete this future, the completion stages will fire (including the error handler in ControllerApis#request)
    responseFuture.complete(response)

    // Now we should get an error response with UNSUPPORTED_VERSION
    val errorResponse = errorResponseFuture.get()
    assertEquals(1, errorResponse.errorCounts().getOrDefault(Errors.UNSUPPORTED_VERSION, 0))
  }

  @Test
  def testUnauthorizedControllerRegistrationRequest(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => {
      controllerApis = createControllerApis(Some(createDenyAllAuthorizer()), new MockController.Builder().build())
      controllerApis.handleControllerRegistration(buildRequest(
        new ControllerRegistrationRequest(new ControllerRegistrationRequestData(), 0.toShort)))
    })
  }

  @Test
  def testUnauthorizedDescribeClusterRequest(): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], () => {
      controllerApis = createControllerApis(Some(createDenyAllAuthorizer()), new MockController.Builder().build())
      controllerApis.handleDescribeCluster(buildRequest(
        new DescribeClusterRequest(new DescribeClusterRequestData(), 1.toShort)))
    })
  }

  // ---------------------------------------------------------------------------
  // CreateDelegationToken — controller-side defence-in-depth.
  //
  // The broker-side guard in KafkaApis.handleCreateTokenRequest refuses tenant
  // identity-laundering mints before forwarding. Two routes bypass the broker:
  //   1. CreateDelegationTokenRequest declares listeners=[broker, controller],
  //      so an admin client can connect directly to the controller listener;
  //   2. Any CLUSTER_ACTION holder can wrap the request in an Envelope and
  //      have the controller re-dispatch it on the original caller's behalf
  //      via handleEnvelopeRequest.
  //
  // The controller listener has no per-listener tenant binding (TenantConfig
  // is constructed from broker originals to know WHICH tenant ids are known on
  // this cluster, but the controller never speaks SASL on a tenant listener).
  // Tenant identity on the controller is therefore derived from the principal
  // name alone: `__tenant_<id>.<user>` where <id> is a known tenant id.
  //
  // Rule (mirrors KafkaApis): a caller whose principal is not within tenant T
  // cannot mint a token whose owner OR any renewer is within T's principal
  // namespace.
  // ---------------------------------------------------------------------------

  private def tenantConfigBinding(tenantId: String, listenerName: String): org.apache.kafka.server.tenant.TenantConfig = {
    val originals = new java.util.HashMap[String, AnyRef]()
    originals.put(s"listener.name.${listenerName.toLowerCase}.tenant.id", tenantId)
    org.apache.kafka.server.tenant.TenantConfig.from(originals)
  }

  private def buildTokenRequest(
    request: AbstractRequest,
    principal: KafkaPrincipal
  ): RequestChannel.Request = {
    val buffer = request.serializeWithHeader(new RequestHeader(request.apiKey, request.version, clientID, 0))
    val header = RequestHeader.parse(buffer)
    // SASL_PLAINTEXT + non-ANONYMOUS principal so allowTokenRequests() returns
    // true; the controller-side guard runs before any of the post-allow paths.
    val context = new RequestContext(
      header, "1", InetAddress.getLocalHost, principal,
      ListenerName.normalised("CONTROLLER"),
      SecurityProtocol.SASL_PLAINTEXT, ClientInformation.EMPTY, false)
    new RequestChannel.Request(
      processor = 1, context = context, startTimeNanos = 0,
      MemoryPool.NONE, buffer, requestChannelMetrics)
  }

  private def captureSentResponse(request: RequestChannel.Request): AbstractResponse = {
    val captor: ArgumentCaptor[AbstractResponse] = ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      captor.capture(),
      any())
    captor.getValue
  }

  @Test
  def testControllerCreateDelegationTokenRefusesTenantPrefixedOwnerOnSplitModeController(): Unit = {
    // Split-mode KRaft / pre-binding case: the controller node is configured
    // with `process.roles=controller` only, so it has no broker listeners and
    // its TenantConfig is empty. Without a structural guard, every controller-
    // side tenant check would collapse on `allTenants.isEmpty` and a cluster
    // admin could mint delegation tokens for ANY tenant principal — including
    // ones already bound on the broker nodes. Asserts the structural widening
    // of isReservedTenantPrincipalNamespace closes this regression on the
    // controller side.
    val createRequest = new CreateDelegationTokenRequest.Builder(
      new CreateDelegationTokenRequestData()
        .setOwnerPrincipalType(KafkaPrincipal.USER_TYPE)
        .setOwnerPrincipalName("__tenant_acme.alice")).build()
    val request = buildTokenRequest(createRequest, new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis = createControllerApis(
      authorizer = None,
      controller = new MockController.Builder().build(),
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())
    controllerApis.handleCreateDelegationTokenRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[CreateDelegationTokenResponse]
    assertEquals(Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED.code, response.data.errorCode,
      "the `__tenant_` prefix is reserved even on a controller with no listener bindings — closes split-mode KRaft gap")
  }

  @Test
  def testControllerAlterUserScramCredentialsRefusesTenantPrefixedUpsertionOnSplitModeController(): Unit = {
    // Companion to the delegation-token split-mode test: a SCRAM upsertion
    // for `__tenant_acme.alice` on a controller with empty TenantConfig must
    // still be refused. Without the structural guard, a cluster admin could
    // mint SCRAM credentials for the tenant on the controller (where the
    // record is persisted) and the broker would happily authenticate the
    // tenant identity on its tenant listener at the next SASL handshake.
    val upsertions = new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialUpsertion]()
    upsertions.add(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
      .setName("__tenant_acme.alice")
      .setMechanism(1.toByte)
      .setIterations(8192)
      .setSalt(Array.emptyByteArray)
      .setSaltedPassword(Array.emptyByteArray))
    val alterRequest = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData().setUpsertions(upsertions)).build()
    val request = buildTokenRequest(alterRequest, new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis = createControllerApis(
      authorizer = None,
      controller = new MockController.Builder().build(),
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())
    controllerApis.handle(request, RequestLocal.noCaching())

    val response = captureSentResponse(request).asInstanceOf[AlterUserScramCredentialsResponse]
    assertEquals(1, response.data.results.size)
    val result = response.data.results.get(0)
    assertEquals("__tenant_acme.alice", result.user)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, result.errorCode,
      "controller must refuse SCRAM planting under `__tenant_*` even when its TenantConfig is empty (split-mode KRaft)")
  }

  @Test
  def testControllerCreateDelegationTokenRefusesTenantPrefixedOwnerFromClusterCaller(): Unit = {
    // Non-tenant caller asking the CONTROLLER to mint a token whose owner is
    // `__tenant_acme.alice`. The broker-side guard does not apply on this
    // route — the controller's own guard must refuse and return
    // DELEGATION_TOKEN_AUTHORIZATION_FAILED. MockController throws if
    // controller.createDelegationToken is reached, so the test also proves
    // we did NOT fall through to the controller layer.
    val createRequest = new CreateDelegationTokenRequest.Builder(
      new CreateDelegationTokenRequestData()
        .setOwnerPrincipalType(KafkaPrincipal.USER_TYPE)
        .setOwnerPrincipalName("__tenant_acme.alice")).build()
    val request = buildTokenRequest(createRequest, new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis = createControllerApis(
      authorizer = None,
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleCreateDelegationTokenRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[CreateDelegationTokenResponse]
    assertEquals(Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED.code, response.data.errorCode,
      "non-tenant caller cannot mint a token whose owner sits in a tenant principal namespace")
  }

  @Test
  def testControllerCreateDelegationTokenRefusesTenantPrefixedRenewerFromClusterCaller(): Unit = {
    val renewers = new util.ArrayList[CreateDelegationTokenRequestData.CreatableRenewers]()
    renewers.add(new CreateDelegationTokenRequestData.CreatableRenewers()
      .setPrincipalType(KafkaPrincipal.USER_TYPE)
      .setPrincipalName("__tenant_acme.bob"))
    val createRequest = new CreateDelegationTokenRequest.Builder(
      new CreateDelegationTokenRequestData()
        .setOwnerPrincipalType(KafkaPrincipal.USER_TYPE)
        .setOwnerPrincipalName("regular-user")
        .setRenewers(renewers)).build()
    val request = buildTokenRequest(createRequest, new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis = createControllerApis(
      authorizer = None,
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleCreateDelegationTokenRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[CreateDelegationTokenResponse]
    assertEquals(Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED.code, response.data.errorCode,
      "non-tenant caller cannot register a renewer inside a tenant principal namespace")
  }

  @Test
  def testControllerCreateDelegationTokenAllowsRegularOwnerForClusterCaller(): Unit = {
    // Control: a non-tenant caller minting for an ordinary principal must
    // still pass the guard and reach controller.createDelegationToken.
    val createRequest = new CreateDelegationTokenRequest.Builder(
      new CreateDelegationTokenRequestData()
        .setOwnerPrincipalType(KafkaPrincipal.USER_TYPE)
        .setOwnerPrincipalName("regular-user")).build()
    val callerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "regular-user")
    val request = buildTokenRequest(createRequest, callerPrincipal)

    val controller = mock(classOf[Controller])
    when(controller.createDelegationToken(any(classOf[ControllerRequestContext]),
      any(classOf[CreateDelegationTokenRequestData])))
      .thenReturn(CompletableFuture.completedFuture(
        new CreateDelegationTokenResponseData()
          .setPrincipalType(KafkaPrincipal.USER_TYPE)
          .setPrincipalName("regular-user")
          .setTokenRequesterPrincipalType(KafkaPrincipal.USER_TYPE)
          .setTokenRequesterPrincipalName("regular-user")
          .setHmac(Array.emptyByteArray)
          .setTokenId("tid")))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleCreateDelegationTokenRequest(request).get()

    verify(controller).createDelegationToken(
      any(classOf[ControllerRequestContext]),
      any(classOf[CreateDelegationTokenRequestData]))
  }

  @Test
  def testControllerCreateDelegationTokenAllowsSameTenantCallerForOwnPrincipal(): Unit = {
    // Legitimate tenant flow: tenant principal __tenant_acme.alice asks (via
    // envelope re-dispatch or directly) the controller to mint a token for
    // their own principal. The guard must let it through; the broker-side
    // guard already covers this case from the other direction.
    val createRequest = new CreateDelegationTokenRequest.Builder(
      new CreateDelegationTokenRequestData()
        .setOwnerPrincipalType(KafkaPrincipal.USER_TYPE)
        .setOwnerPrincipalName("__tenant_acme.alice")).build()
    val callerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice")
    val request = buildTokenRequest(createRequest, callerPrincipal)

    val controller = mock(classOf[Controller])
    when(controller.createDelegationToken(any(classOf[ControllerRequestContext]),
      any(classOf[CreateDelegationTokenRequestData])))
      .thenReturn(CompletableFuture.completedFuture(
        new CreateDelegationTokenResponseData()
          .setPrincipalType(KafkaPrincipal.USER_TYPE)
          .setPrincipalName("__tenant_acme.alice")
          .setTokenRequesterPrincipalType(KafkaPrincipal.USER_TYPE)
          .setTokenRequesterPrincipalName("__tenant_acme.alice")
          .setHmac(Array.emptyByteArray)
          .setTokenId("tid")))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleCreateDelegationTokenRequest(request).get()

    verify(controller).createDelegationToken(
      any(classOf[ControllerRequestContext]),
      any(classOf[CreateDelegationTokenRequestData]))
  }

  // ---------------------------------------------------------------------------
  // AlterUserScramCredentials — controller-side identity-laundering guard
  //
  // Mirrors the broker-side test block in KafkaApisTest. The controller listener
  // is reachable directly (the SCRAM request schema declares listeners=
  // [broker,controller]) and via Envelope re-dispatch from CLUSTER_ACTION
  // holders. Without the controller-side guard, an attacker could bypass the
  // broker by either routing directly to the controller or by wrapping the
  // SCRAM request in an Envelope and asking a CLUSTER_ACTION holder to forward
  // it. MockController throws UnsupportedOperationException from
  // alterUserScramCredentials, so any reach-through is a hard test failure —
  // these tests double as proof that the guard refuses BEFORE the controller
  // layer is invoked.
  // ---------------------------------------------------------------------------

  @Test
  def testControllerAlterUserScramCredentialsRefusesTenantPrefixedUpsertionFromClusterCaller(): Unit = {
    val upsertions = new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialUpsertion]()
    upsertions.add(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
      .setName("__tenant_acme.alice")
      .setMechanism(1.toByte)
      .setIterations(8192)
      .setSalt(Array.emptyByteArray)
      .setSaltedPassword(Array.emptyByteArray))
    val alterRequest = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData().setUpsertions(upsertions)).build()
    val request = buildTokenRequest(alterRequest, new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis = createControllerApis(
      authorizer = None,
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handle(request, RequestLocal.noCaching())

    val response = captureSentResponse(request).asInstanceOf[AlterUserScramCredentialsResponse]
    assertEquals(1, response.data.results.size)
    val result = response.data.results.get(0)
    assertEquals("__tenant_acme.alice", result.user)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, result.errorCode,
      "controller must refuse a non-tenant caller minting a SCRAM credential for a tenant principal")
  }

  @Test
  def testControllerAlterUserScramCredentialsRefusesTenantPrefixedDeletionFromClusterCaller(): Unit = {
    val deletions = new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialDeletion]()
    deletions.add(new AlterUserScramCredentialsRequestData.ScramCredentialDeletion()
      .setName("__tenant_acme.alice")
      .setMechanism(1.toByte))
    val alterRequest = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData().setDeletions(deletions)).build()
    val request = buildTokenRequest(alterRequest, new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis = createControllerApis(
      authorizer = None,
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handle(request, RequestLocal.noCaching())

    val response = captureSentResponse(request).asInstanceOf[AlterUserScramCredentialsResponse]
    assertEquals(1, response.data.results.size)
    val result = response.data.results.get(0)
    assertEquals("__tenant_acme.alice", result.user)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, result.errorCode,
      "controller must refuse a non-tenant caller deleting a tenant principal's SCRAM credential")
  }

  @Test
  def testControllerAlterUserScramCredentialsAtomicMixedBatchRefused(): Unit = {
    // All-or-nothing: a mixed batch containing one foreign-tenant entry must
    // refuse every entry, mirroring the broker.
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
    val request = buildTokenRequest(alterRequest, new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis = createControllerApis(
      authorizer = None,
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handle(request, RequestLocal.noCaching())

    val response = captureSentResponse(request).asInstanceOf[AlterUserScramCredentialsResponse]
    assertEquals(2, response.data.results.size)
    response.data.results.forEach { r =>
      assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, r.errorCode,
        s"all entries (including `${r.user}`) must be refused atomically when the batch contains a foreign-tenant entry")
    }
  }

  @Test
  def testControllerAlterUserScramCredentialsAllowsRegularUserForClusterCaller(): Unit = {
    // Control: a non-tenant caller altering an ordinary user reaches the
    // controller. The guard must not over-refuse.
    val upsertions = new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialUpsertion]()
    upsertions.add(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
      .setName("regular-user")
      .setMechanism(1.toByte)
      .setIterations(8192)
      .setSalt(Array.emptyByteArray)
      .setSaltedPassword(Array.emptyByteArray))
    val alterRequest = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData().setUpsertions(upsertions)).build()
    val request = buildTokenRequest(alterRequest, new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    val controller = mock(classOf[Controller])
    when(controller.alterUserScramCredentials(any(classOf[ControllerRequestContext]),
      any(classOf[AlterUserScramCredentialsRequestData])))
      .thenReturn(CompletableFuture.completedFuture(new AlterUserScramCredentialsResponseData()))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handle(request, RequestLocal.noCaching())

    verify(controller).alterUserScramCredentials(
      any(classOf[ControllerRequestContext]),
      any(classOf[AlterUserScramCredentialsRequestData]))
  }

  @Test
  def testControllerAlterUserScramCredentialsAllowsSameTenantCallerForOwnUser(): Unit = {
    // Legitimate tenant flow: tenant principal __tenant_acme.alice asks the
    // controller (directly or via Envelope re-dispatch) to alter a SCRAM
    // credential for its own tenant's user. The guard must let it through.
    val upsertions = new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialUpsertion]()
    upsertions.add(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
      .setName("__tenant_acme.alice")
      .setMechanism(1.toByte)
      .setIterations(8192)
      .setSalt(Array.emptyByteArray)
      .setSaltedPassword(Array.emptyByteArray))
    val alterRequest = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData().setUpsertions(upsertions)).build()
    val callerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice")
    val request = buildTokenRequest(alterRequest, callerPrincipal)

    val controller = mock(classOf[Controller])
    when(controller.alterUserScramCredentials(any(classOf[ControllerRequestContext]),
      any(classOf[AlterUserScramCredentialsRequestData])))
      .thenReturn(CompletableFuture.completedFuture(new AlterUserScramCredentialsResponseData()))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handle(request, RequestLocal.noCaching())

    verify(controller).alterUserScramCredentials(
      any(classOf[ControllerRequestContext]),
      any(classOf[AlterUserScramCredentialsRequestData]))
  }

  // ---------------------------------------------------------------------------
  // Envelope re-dispatch tenant trust boundary (#110)
  //
  // EnvelopeUtils.handleEnvelopeRequest deserialises the inner forwardedPrincipal
  // via KafkaPrincipalSerde without any signature/MAC check. In a default KRaft
  // deployment every broker holds CLUSTER_ACTION via super.users — so without
  // a guard at the envelope layer, any broker could forge a `__tenant_<id>.<u>`
  // principal in an envelope, re-dispatch it into the controller, and reach
  // handlers that grant tenant capability (mint delegation token, alter SCRAM,
  // attach quotas …) — fully bypassing every controller-side tenant guard.
  //
  // The fix mirrors the broker's TENANT_ALLOWED_APIS dispatch gate onto the
  // envelope path: a forwarded tenant-namespaced principal is only allowed to
  // re-enter for APIs the tenant dispatch boundary would itself admit.
  // ---------------------------------------------------------------------------

  private val envelopePrincipalSerde = new org.apache.kafka.common.security.auth.KafkaPrincipalSerde {
    override def serialize(principal: KafkaPrincipal): Array[Byte] =
      org.apache.kafka.common.utils.Utils.utf8(principal.toString)
    override def deserialize(bytes: Array[Byte]): KafkaPrincipal =
      org.apache.kafka.common.utils.SecurityUtils.parseKafkaPrincipal(
        org.apache.kafka.common.utils.Utils.utf8(bytes))
  }

  private def captureEnvelopeResponse(request: RequestChannel.Request): EnvelopeResponse = {
    val captor: ArgumentCaptor[AbstractResponse] = ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      captor.capture(),
      any())
    captor.getValue.asInstanceOf[EnvelopeResponse]
  }

  @Test
  def testEnvelopeRefusesForgedTenantPrincipalOnCreateDelegationToken(): Unit = {
    val tokenRequest = new CreateDelegationTokenRequest.Builder(
      new CreateDelegationTokenRequestData()
        .setOwnerPrincipalType(KafkaPrincipal.USER_TYPE)
        .setOwnerPrincipalName("victim")).build()
    val forged = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice")
    val envelopeRequest = kafka.utils.TestUtils.buildEnvelopeRequest(
      tokenRequest, envelopePrincipalSerde, requestChannelMetrics, time.nanoseconds(),
      forwardedPrincipal = forged,
      outerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis = createControllerApis(
      authorizer = None,
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "CONTROLLER"))
    controllerApis.handle(envelopeRequest, RequestLocal.noCaching())

    val response = captureEnvelopeResponse(envelopeRequest)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED, response.error,
      "envelope gate must refuse forged tenant principal for CREATE_DELEGATION_TOKEN")
  }

  @Test
  def testEnvelopeRefusesForgedTenantPrincipalOnAlterUserScramCredentials(): Unit = {
    val upsertions = new util.ArrayList[AlterUserScramCredentialsRequestData.ScramCredentialUpsertion]()
    upsertions.add(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
      .setName("victim")
      .setMechanism(1.toByte)
      .setIterations(8192)
      .setSalt(Array.emptyByteArray)
      .setSaltedPassword(Array.emptyByteArray))
    val alterRequest = new AlterUserScramCredentialsRequest.Builder(
      new AlterUserScramCredentialsRequestData().setUpsertions(upsertions)).build()
    val forged = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice")
    val envelopeRequest = kafka.utils.TestUtils.buildEnvelopeRequest(
      alterRequest, envelopePrincipalSerde, requestChannelMetrics, time.nanoseconds(),
      forwardedPrincipal = forged,
      outerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis = createControllerApis(
      authorizer = None,
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "CONTROLLER"))
    controllerApis.handle(envelopeRequest, RequestLocal.noCaching())

    val response = captureEnvelopeResponse(envelopeRequest)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED, response.error,
      "envelope gate must refuse forged tenant principal for ALTER_USER_SCRAM_CREDENTIALS")
  }

  @Test
  def testEnvelopeRefusesForgedTenantPrincipalOnAlterClientQuotas(): Unit = {
    val quotaRequest = new AlterClientQuotasRequest(
      new AlterClientQuotasRequestData()
        .setEntries(new util.ArrayList[AlterClientQuotasRequestData.EntryData]())
        .setValidateOnly(false),
      0.toShort)
    val forged = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice")
    val envelopeRequest = kafka.utils.TestUtils.buildEnvelopeRequest(
      quotaRequest, envelopePrincipalSerde, requestChannelMetrics, time.nanoseconds(),
      forwardedPrincipal = forged,
      outerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis = createControllerApis(
      authorizer = None,
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "CONTROLLER"))
    controllerApis.handle(envelopeRequest, RequestLocal.noCaching())

    val response = captureEnvelopeResponse(envelopeRequest)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED, response.error,
      "envelope gate must refuse forged tenant principal for ALTER_CLIENT_QUOTAS")
  }

  @Test
  def testEnvelopeRefusesForgedTenantPrincipalOnRenewDelegationToken(): Unit = {
    // RENEW_DELEGATION_TOKEN is outside TENANT_ALLOWED_APIS — the request
    // data carries only an HMAC (no principal-name field), so the controller
    // handler has no caller-supplied identity to validate. Cross-tenant
    // defence relies on the envelope gate refusing a forged tenant
    // forwardedPrincipal here, before the handler is ever invoked. Without
    // this gate a CLUSTER_ACTION holder could envelope a RENEW with
    // `forwardedPrincipal=__tenant_<id>.<u>` and reach the renew handler;
    // there, `allowedToRenew` (strict KafkaPrincipal equality) would still
    // catch most cross-owner attempts but it would not catch the case where
    // the forged principal coincidentally appears in the token's `renewers`
    // list (e.g. a tenant added their own operator-bound principal at create
    // time). The envelope gate closes the gap by structurally refusing the
    // forwarded tenant identity for any non-tenant-allowed API.
    val renewRequest = new RenewDelegationTokenRequest.Builder(
      new RenewDelegationTokenRequestData()
        .setHmac(Array.emptyByteArray)
        .setRenewPeriodMs(86400000L)).build()
    val forged = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice")
    val envelopeRequest = kafka.utils.TestUtils.buildEnvelopeRequest(
      renewRequest, envelopePrincipalSerde, requestChannelMetrics, time.nanoseconds(),
      forwardedPrincipal = forged,
      outerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis = createControllerApis(
      authorizer = None,
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "CONTROLLER"))
    controllerApis.handle(envelopeRequest, RequestLocal.noCaching())

    val response = captureEnvelopeResponse(envelopeRequest)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED, response.error,
      "envelope gate must refuse forged tenant principal for RENEW_DELEGATION_TOKEN")
  }

  @Test
  def testEnvelopeRefusesForgedTenantPrincipalOnExpireDelegationToken(): Unit = {
    // Same shape as RENEW above; EXPIRE_DELEGATION_TOKEN is also outside
    // TENANT_ALLOWED_APIS and also carries only an HMAC. A forged tenant
    // forwardedPrincipal would otherwise reach `allowedToRenew` (which gates
    // both renew and expire) and succeed if the forged identity matches an
    // entry in the token's renewers list.
    val expireRequest = new ExpireDelegationTokenRequest.Builder(
      new ExpireDelegationTokenRequestData()
        .setHmac(Array.emptyByteArray)
        .setExpiryTimePeriodMs(-1L)).build()
    val forged = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice")
    val envelopeRequest = kafka.utils.TestUtils.buildEnvelopeRequest(
      expireRequest, envelopePrincipalSerde, requestChannelMetrics, time.nanoseconds(),
      forwardedPrincipal = forged,
      outerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis = createControllerApis(
      authorizer = None,
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "CONTROLLER"))
    controllerApis.handle(envelopeRequest, RequestLocal.noCaching())

    val response = captureEnvelopeResponse(envelopeRequest)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED, response.error,
      "envelope gate must refuse forged tenant principal for EXPIRE_DELEGATION_TOKEN")
  }

  @Test
  def testEnvelopeRefusesEmptyTenantIdForgedPrincipal(): Unit = {
    // `__tenant_.alice` matches the PRINCIPAL_PREFIX startsWith — the gate
    // must not require a well-formed tenant id; it must refuse on prefix
    // alone. parseTenantId may treat this as malformed elsewhere, but the
    // envelope gate cannot rely on that.
    val tokenRequest = new CreateDelegationTokenRequest.Builder(
      new CreateDelegationTokenRequestData()
        .setOwnerPrincipalType(KafkaPrincipal.USER_TYPE)
        .setOwnerPrincipalName("victim")).build()
    val forged = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_.alice")
    val envelopeRequest = kafka.utils.TestUtils.buildEnvelopeRequest(
      tokenRequest, envelopePrincipalSerde, requestChannelMetrics, time.nanoseconds(),
      forwardedPrincipal = forged,
      outerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis = createControllerApis(
      authorizer = None,
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "CONTROLLER"))
    controllerApis.handle(envelopeRequest, RequestLocal.noCaching())

    val response = captureEnvelopeResponse(envelopeRequest)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED, response.error,
      "envelope gate must refuse `__tenant_.<u>` (empty tenant id) for non-tenant-allowed API")
  }

  @Test
  def testEnvelopeAllowsTenantPrincipalOnCreateTopics(): Unit = {
    // Legitimate forwarded tenant flow: a broker receives CreateTopics from a
    // tenant client, envelopes it to the controller with the (authentic)
    // tenant principal as forwardedPrincipal. CREATE_TOPICS is in
    // TENANT_ALLOWED_APIS — the gate must let this through and the controller
    // must perform the create. Load-bearing must-not-regress case: prove the
    // gate is scoped to non-tenant-allowed APIs and does not refuse the only
    // controller-routed legitimate tenant flow.
    val topics = new CreatableTopicCollection()
    topics.add(new CreatableTopic().setName("__tenant_acme.foo").setNumPartitions(1).setReplicationFactor(1.toShort))
    val createTopicsRequest = new CreateTopicsRequest.Builder(
      new CreateTopicsRequestData().setTopics(topics)).build()
    val tenantPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice")
    val envelopeRequest = kafka.utils.TestUtils.buildEnvelopeRequest(
      createTopicsRequest, envelopePrincipalSerde, requestChannelMetrics, time.nanoseconds(),
      forwardedPrincipal = tenantPrincipal,
      outerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    val controller = mock(classOf[Controller])
    when(controller.createTopics(
      any(classOf[ControllerRequestContext]),
      any(classOf[CreateTopicsRequestData]),
      any(classOf[java.util.Set[String]])))
      .thenReturn(CompletableFuture.completedFuture(new CreateTopicsResponseData()))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = tenantConfigBinding("acme", "CONTROLLER"))
    controllerApis.handle(envelopeRequest, RequestLocal.noCaching())

    // The gate did NOT refuse — the forwarded request reached the controller.
    verify(controller).createTopics(
      any(classOf[ControllerRequestContext]),
      any(classOf[CreateTopicsRequestData]),
      any(classOf[java.util.Set[String]]))
  }

  // ---------------------------------------------------------------------------
  // bootstrap.controllers TOPIC scrub (#121)
  //
  // Every topic-namespacing scrub in KafkaApis is bypassed when AdminClient
  // talks directly to the controller listener via `bootstrap.controllers`
  // (KIP-590). ControllerApis must apply its own outside-in refusal so the
  // metadata log can never receive `<tenantId>.X` mutations from a privileged
  // caller. The tests below pin the rule per handler: refuse the tenant-named
  // entry, let the neutral entry through.
  // ---------------------------------------------------------------------------

  @Test
  def testControllerCreateTopicsRefusesTenantPrefixedNameOnBootstrapControllers(): Unit = {
    // Adversary on bootstrap.controllers sends `acme.orders`. Without the
    // controller-side scrub the broker preprocess is skipped, so the metadata
    // log would record the tenant-namespaced topic; tenant acme would later
    // see it in ListTopics.
    val controller = new MockController.Builder().build()
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    val request = new CreateTopicsRequest.Builder(
      new CreateTopicsRequestData().setTopics(new CreatableTopicCollection(util.Arrays.asList(
        new CreatableTopic().setName("acme.orders").setNumPartitions(1).setReplicationFactor(1)
      ).iterator()))).build()
    val response = handleRequest[CreateTopicsResponse](request, controllerApis)
    val results = response.data.topics().asScala.map(t => t.name -> t.errorCode).toMap
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, results("acme.orders"),
      "controller-direct CreateTopics must refuse tenant-prefixed topic name")
  }

  @Test
  def testControllerCreateTopicsMixesAllowedAndRejectedOnBootstrapControllers(): Unit = {
    // A mixed batch must reach the controller for the non-polluting entries
    // and surface the polluting entries as per-name INVALID_TOPIC_EXCEPTION.
    // Empty-after-scrub is exercised in a separate test.
    val controller = new MockController.Builder().build()
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    val request = new CreateTopicsRequest.Builder(
      new CreateTopicsRequestData().setTopics(new CreatableTopicCollection(util.Arrays.asList(
        new CreatableTopic().setName("acme.orders").setNumPartitions(1).setReplicationFactor(1),
        new CreatableTopic().setName("plain").setNumPartitions(1).setReplicationFactor(1)
      ).iterator()))).build()
    val response = handleRequest[CreateTopicsResponse](request, controllerApis)
    val results = response.data.topics().asScala.map(t => t.name -> t.errorCode).toMap
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, results("acme.orders"),
      "tenant-namespaced entry must surface INVALID_TOPIC_EXCEPTION")
    assertEquals(Errors.NONE.code, results("plain"),
      "neutral entry must still reach the controller and succeed")
  }

  @Test
  def testControllerCreateTopicsAllTenantNamedShortCircuitsControllerCall(): Unit = {
    // When every requested topic name is in a reserved tenant namespace, the
    // request must never reach controller.createTopics — synthesise the
    // response directly so no controller-state work is wasted on what is by
    // definition all garbage.
    val controller = mock(classOf[Controller])
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    val request = new CreateTopicsRequest.Builder(
      new CreateTopicsRequestData().setTopics(new CreatableTopicCollection(util.Arrays.asList(
        new CreatableTopic().setName("acme.orders").setNumPartitions(1).setReplicationFactor(1),
        new CreatableTopic().setName("acme.payments").setNumPartitions(1).setReplicationFactor(1)
      ).iterator()))).build()
    val response = handleRequest[CreateTopicsResponse](request, controllerApis)
    val results = response.data.topics().asScala.map(t => t.name -> t.errorCode).toMap
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, results("acme.orders"))
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, results("acme.payments"))
    verify(controller, never()).createTopics(any(), any(), any())
  }

  @Test
  def testControllerCreateTopicsAllowsLegitimateForwardedTenantPrincipal(): Unit = {
    // Must-not-regress: the legitimate forwarded tenant flow places a
    // *physical* tenant-prefixed name in the metadata-log mutation. A tenant
    // client sends `CreateTopics("foo")` to its broker listener; KafkaApis
    // rewrites the name to `acme.foo` and envelopes the request to the
    // controller with `forwardedPrincipal=__tenant_acme.alice`. The
    // outside-in scrub must NOT refuse `acme.foo` here — the broker has
    // already validated the listener/principal binding (#86, #110), so
    // tenant identity is canonical from the forwarded principal. Refusing
    // would silently break every tenant CreateTopics.
    val topics = new CreatableTopicCollection()
    topics.add(new CreatableTopic().setName("acme.foo").setNumPartitions(1).setReplicationFactor(1.toShort))
    val createTopicsRequest = new CreateTopicsRequest.Builder(
      new CreateTopicsRequestData().setTopics(topics)).build()
    val tenantPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice")
    val envelopeRequest = kafka.utils.TestUtils.buildEnvelopeRequest(
      createTopicsRequest, envelopePrincipalSerde, requestChannelMetrics, time.nanoseconds(),
      forwardedPrincipal = tenantPrincipal,
      outerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    val controller = mock(classOf[Controller])
    when(controller.createTopics(
      any(classOf[ControllerRequestContext]),
      any(classOf[CreateTopicsRequestData]),
      any(classOf[java.util.Set[String]])))
      .thenReturn(CompletableFuture.completedFuture(new CreateTopicsResponseData()))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = tenantConfigBinding("acme", "CONTROLLER"))
    controllerApis.handle(envelopeRequest, RequestLocal.noCaching())

    // The scrub did NOT refuse: the controller received the create call
    // with the physical name. Without principal awareness, this assertion
    // would fire `times(0)` and the legitimate flow would be silently
    // refused at the controller.
    val createCaptor: ArgumentCaptor[CreateTopicsRequestData] =
      ArgumentCaptor.forClass(classOf[CreateTopicsRequestData])
    verify(controller).createTopics(
      any(classOf[ControllerRequestContext]),
      createCaptor.capture(),
      any(classOf[java.util.Set[String]]))
    val forwarded = createCaptor.getValue.topics().asScala.map(_.name).toList
    assertEquals(List("acme.foo"), forwarded,
      "legitimate tenant CreateTopics must reach controller with its physical name intact")
  }

  @Test
  def testControllerCreateTopicsRefusesCrossTenantPrincipalForeignNamespace(): Unit = {
    // Cross-tenant pollution: a known tenant `evil` (or any `__tenant_*`
    // principal) tries to create `acme.foo`. Even though the caller is
    // tenant-namespaced, their tenant id does not match the topic's
    // namespace prefix — so the scrub must refuse and the controller must
    // never see the create. Pins the principal-AWARE shape: refusal keys
    // on (topic-prefix ∈ knownTenants) AND (callerTenant ≠ topic-prefix),
    // not on "any tenant-prefixed name".
    val topics = new CreatableTopicCollection()
    topics.add(new CreatableTopic().setName("acme.foo").setNumPartitions(1).setReplicationFactor(1.toShort))
    val createTopicsRequest = new CreateTopicsRequest.Builder(
      new CreateTopicsRequestData().setTopics(topics)).build()
    val crossTenantPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_evil.alice")
    val envelopeRequest = kafka.utils.TestUtils.buildEnvelopeRequest(
      createTopicsRequest, envelopePrincipalSerde, requestChannelMetrics, time.nanoseconds(),
      forwardedPrincipal = crossTenantPrincipal,
      outerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    val controller = mock(classOf[Controller])
    // Configure BOTH tenants so the cross-tenant principal would otherwise
    // be recognised as a (foreign-to-acme) legitimate tenant. The scrub
    // must still refuse because the topic prefix names a different tenant.
    val originals = new java.util.HashMap[String, AnyRef]()
    originals.put("listener.name.tenant_acme.tenant.id", "acme")
    originals.put("listener.name.tenant_evil.tenant.id", "evil")
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.from(originals))
    controllerApis.handle(envelopeRequest, RequestLocal.noCaching())

    verify(controller, never()).createTopics(any(), any(), any())
  }

  // ---------------------------------------------------------------------------
  // bootstrap.controllers TOPIC scrub — DeleteTopics (#121 step 2)
  //
  // DeleteTopics has three distinct input paths in the request body:
  //   1. legacy `topicNames` (List[String])
  //   2. `topics` with `.name` set and `.topicId == ZERO_UUID`
  //   3. `topics` with `.topicId` set and `.name == null` — the controller
  //      resolves the id to a name via `findTopicNames` AFTER the named-input
  //      scrub runs
  // Each must refuse a tenant-namespaced name when the caller's principal is
  // not within that tenant. The UUID path is especially nasty: a cluster-wide
  // caller that learns or guesses a tenant topic's id could otherwise delete
  // it through the controller with no tenant context whatsoever.
  // ---------------------------------------------------------------------------

  @Test
  def testControllerDeleteTopicsRefusesTenantPrefixedNameOnBootstrapControllers(): Unit = {
    // Path 1+2: legacy `topicNames` AND `topics[].name`. Cluster-acting
    // anonymous caller via bootstrap.controllers sends both shapes targeting
    // `acme.orders` — both must surface INVALID_TOPIC_EXCEPTION and the
    // controller must never see the deletion.
    val controller = mock(classOf[Controller])
    // The controller is still consulted to resolve the (empty) UUID set and
    // the (empty post-scrub) name set, and the deletion path is invoked
    // with an empty id set. Stub all three to empty results so the scrub
    // is the ONLY barrier between the request and any actual deletion.
    when(controller.findTopicNames(any(classOf[ControllerRequestContext]), any(classOf[java.util.Collection[Uuid]])))
      .thenReturn(CompletableFuture.completedFuture(java.util.Collections.emptyMap[Uuid, ResultOrError[String]]()))
    when(controller.findTopicIds(any(classOf[ControllerRequestContext]), any(classOf[java.util.Collection[String]])))
      .thenReturn(CompletableFuture.completedFuture(java.util.Collections.emptyMap[String, ResultOrError[Uuid]]()))
    when(controller.deleteTopics(any(classOf[ControllerRequestContext]), any(classOf[java.util.Set[Uuid]])))
      .thenReturn(CompletableFuture.completedFuture(java.util.Collections.emptyMap[Uuid, org.apache.kafka.common.requests.ApiError]()))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    val request = new DeleteTopicsRequestData()
    request.setTopicNames(util.Arrays.asList("acme.orders"))
    request.topics().add(new DeleteTopicState().setName("acme.payments").setTopicId(ZERO_UUID))
    val results = controllerApis.deleteTopics(ANONYMOUS_CONTEXT, request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      hasClusterAuth = true,
      _ => Set.empty,
      _ => Set.empty).get().asScala.toList
    val resultsByName = results.map(r => r.name -> r.errorCode).toMap
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, resultsByName("acme.orders"),
      "controller-direct DeleteTopics must refuse tenant-prefixed name from `topicNames`")
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, resultsByName("acme.payments"),
      "controller-direct DeleteTopics must refuse tenant-prefixed name from `topics[].name`")
    // The controller is still called for housekeeping (resolve IDs, then
    // pass the surviving id set to deleteTopics) but the surviving id set
    // MUST be empty — no scrubbed name made it through to actual deletion.
    val idCaptor: ArgumentCaptor[java.util.Set[Uuid]] =
      ArgumentCaptor.forClass(classOf[java.util.Set[Uuid]])
    verify(controller).deleteTopics(any(classOf[ControllerRequestContext]), idCaptor.capture())
    assertEquals(java.util.Collections.emptySet[Uuid](), idCaptor.getValue,
      "scrub must short-circuit before any id reaches controller.deleteTopics")
    // A name-only refusal carries ZERO_UUID; we never reveal whatever id the
    // controller happens to hold for that physical name.
    val resultsById = results.map(r => r.name -> r.topicId).toMap
    assertEquals(ZERO_UUID, resultsById("acme.orders"))
    assertEquals(ZERO_UUID, resultsById("acme.payments"))
  }

  @Test
  def testControllerDeleteTopicsRefusesTenantOwnedUuidOnBootstrapControllers(): Unit = {
    // Path 3 (the gnarly one): caller supplies a UUID with no name and the
    // controller resolves it via findTopicNames. The resolved name reveals
    // the topic lives in tenant `acme`'s namespace — the scrub MUST refuse
    // at that point and the deletion MUST not proceed. Without the
    // callback-side scrub, this id-only request would call
    // `controller.deleteTopics(idToName.keySet)` and quietly delete the
    // tenant's topic.
    val acmeOrdersId = Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")
    val controller = new MockController.Builder()
      .newInitialTopic("acme.orders", acmeOrdersId).build()
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    val request = new DeleteTopicsRequestData()
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(acmeOrdersId))
    val results = controllerApis.deleteTopics(ANONYMOUS_CONTEXT, request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      hasClusterAuth = true,
      // hasClusterAuth=true short-circuits the authorize-by-name filter
      // (`(topicsToAuthenticate.toSet, topicsToAuthenticate.toSet)`) so the
      // scrub is the ONLY barrier between the resolved name and deletion.
      _ => Set.empty,
      _ => Set.empty).get().asScala.toList
    assertEquals(1, results.size, "id-only delete must surface exactly one response")
    val result = results.head
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, result.errorCode,
      "UUID-resolved tenant topic must surface INVALID_TOPIC_EXCEPTION")
    assertEquals("acme.orders", result.name,
      "refusal must echo the resolved physical name so the cluster operator can diagnose")
    assertEquals(acmeOrdersId, result.topicId,
      "UUID path must surface the original id, not ZERO_UUID (named path is the one that uses ZERO_UUID)")
    // Verify the topic still exists in MockController — the scrub blocked
    // the deletion, not just the response synthesis.
    val survives = controller.findTopicIds(ANONYMOUS_CONTEXT,
      util.Collections.singleton("acme.orders")).get()
    assertEquals(acmeOrdersId, survives.get("acme.orders").result(),
      "MockController must still hold the topic — scrub must block the actual delete, not just the response")
  }

  @Test
  def testControllerDeleteTopicsAllowsLegitimateForwardedTenantPrincipal(): Unit = {
    // Must-not-regress: legitimate tenant DeleteTopics. Tenant client sends
    // `DeleteTopics("orders")` to its broker listener; KafkaApis rewrites
    // the name to `acme.orders` and envelopes the request to the controller
    // with `forwardedPrincipal=__tenant_acme.alice`. The principal-aware
    // scrub must NOT refuse here — the broker has already validated the
    // listener/principal binding (#86, #110), so the caller's tenant
    // identity is canonical from the forwarded principal.
    val deleteTopicsRequest = new DeleteTopicsRequest.Builder(
      new DeleteTopicsRequestData().setTopicNames(util.Arrays.asList("acme.orders"))).build()
    val tenantPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice")
    val envelopeRequest = kafka.utils.TestUtils.buildEnvelopeRequest(
      deleteTopicsRequest, envelopePrincipalSerde, requestChannelMetrics, time.nanoseconds(),
      forwardedPrincipal = tenantPrincipal,
      outerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    val controller = mock(classOf[Controller])
    val acmeOrdersId = Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")
    when(controller.findTopicNames(any(classOf[ControllerRequestContext]), any(classOf[java.util.Collection[Uuid]])))
      .thenReturn(CompletableFuture.completedFuture(java.util.Collections.emptyMap[Uuid, ResultOrError[String]]()))
    when(controller.findTopicIds(any(classOf[ControllerRequestContext]), any(classOf[java.util.Collection[String]])))
      .thenReturn(CompletableFuture.completedFuture(
        java.util.Collections.singletonMap("acme.orders", new ResultOrError[Uuid](acmeOrdersId))))
    when(controller.deleteTopics(any(classOf[ControllerRequestContext]), any(classOf[java.util.Set[Uuid]])))
      .thenReturn(CompletableFuture.completedFuture(
        java.util.Collections.singletonMap(acmeOrdersId, org.apache.kafka.common.requests.ApiError.NONE)))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = tenantConfigBinding("acme", "CONTROLLER"))
    controllerApis.handle(envelopeRequest, RequestLocal.noCaching())

    // The scrub did NOT refuse: the controller received the delete with
    // the physical name resolved to its id. Without principal-aware
    // gating, this assertion would fail and the legitimate tenant flow
    // would be silently broken at the controller.
    verify(controller).deleteTopics(any(classOf[ControllerRequestContext]),
      any(classOf[java.util.Set[Uuid]]))
  }

  @Test
  def testControllerDeleteTopicsRefusesCrossTenantPrincipalForeignNamespace(): Unit = {
    // Cross-tenant pollution by way of a known-tenant principal. The
    // adversary holds credentials for tenant `evil` (or merely names a
    // `__tenant_*` principal that the controller treats as a known tenant)
    // and asks the controller — via envelope — to delete `acme.orders`.
    // Even though the principal is tenant-namespaced, the topic's prefix
    // names a DIFFERENT tenant. The scrub must refuse: `callerTenant` is
    // `Some("evil")` and `isForeignTenantNamespace("acme.orders", Some("evil"))`
    // is true because tenant `acme` is known AND `Some("evil") != Some("acme")`.
    val deleteTopicsRequest = new DeleteTopicsRequest.Builder(
      new DeleteTopicsRequestData().setTopicNames(util.Arrays.asList("acme.orders"))).build()
    val crossTenantPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_evil.alice")
    val envelopeRequest = kafka.utils.TestUtils.buildEnvelopeRequest(
      deleteTopicsRequest, envelopePrincipalSerde, requestChannelMetrics, time.nanoseconds(),
      forwardedPrincipal = crossTenantPrincipal,
      outerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    val controller = mock(classOf[Controller])
    val originals = new java.util.HashMap[String, AnyRef]()
    originals.put("listener.name.tenant_acme.tenant.id", "acme")
    originals.put("listener.name.tenant_evil.tenant.id", "evil")
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.from(originals))
    controllerApis.handle(envelopeRequest, RequestLocal.noCaching())

    // The cross-tenant request must never reach controller.deleteTopics
    // and must not even probe findTopicIds (the resolution-by-name lookup
    // would tell evil's principal whether acme.orders exists).
    verify(controller, never()).deleteTopics(any(), any())
    verify(controller, never()).findTopicIds(any(), any())
  }

  // ---------------------------------------------------------------------------
  // CreatePartitions outside-in scrub on bootstrap.controllers (#121 step 3).
  // Adding partitions to `acme.orders` from a cluster-wide caller would silently
  // grow a tenant topic; tenant clients never see the request and the topic's
  // partition count would jump out from under them. The scrub mirrors
  // CreateTopics/DeleteTopics: principal-aware, so the legitimate forwarded
  // tenant flow (broker rewrote `orders` → `acme.orders`, envelope carries
  // `__tenant_acme.alice`) passes through, but any other caller (anonymous
  // cluster-wide, cross-tenant principal) is refused per-topic.
  // ---------------------------------------------------------------------------

  @Test
  def testControllerCreatePartitionsRefusesTenantPrefixedNameOnBootstrapControllers(): Unit = {
    // Cluster-acting anonymous caller via bootstrap.controllers asks to grow
    // `acme.orders`. The scrub must refuse and the controller must never see
    // the mutation. We stub controller.createPartitions to empty so the scrub
    // is the only barrier between the request and the actual mutation.
    val controller = mock(classOf[Controller])
    when(controller.createPartitions(
      any(classOf[ControllerRequestContext]),
      any(classOf[util.List[CreatePartitionsTopic]]),
      ArgumentMatchers.eq(false)))
      .thenReturn(CompletableFuture.completedFuture(java.util.Collections.emptyList[CreatePartitionsTopicResult]()))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    val request = new CreatePartitionsRequestData()
    request.topics().add(new CreatePartitionsTopic().setName("acme.orders").setAssignments(null).setCount(10))
    val results = controllerApis.createPartitions(ANONYMOUS_CONTEXT, request,
      _ => Set("acme.orders")).get().asScala.toList
    assertEquals(1, results.size, "single refusal expected")
    val r = results.head
    assertEquals("acme.orders", r.name)
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, r.errorCode,
      "controller-direct CreatePartitions must refuse tenant-prefixed name")
    // The surviving topic list reaching controller.createPartitions MUST be
    // empty — no name made it past the scrub.
    val topicsCaptor: ArgumentCaptor[util.List[CreatePartitionsTopic]] =
      ArgumentCaptor.forClass(classOf[util.List[CreatePartitionsTopic]])
    verify(controller).createPartitions(
      any(classOf[ControllerRequestContext]),
      topicsCaptor.capture(),
      ArgumentMatchers.eq(false))
    assertEquals(java.util.Collections.emptyList[CreatePartitionsTopic](), topicsCaptor.getValue,
      "scrub must short-circuit before any topic reaches controller.createPartitions")
  }

  @Test
  def testControllerCreatePartitionsMixesAllowedAndRejected(): Unit = {
    // A cluster-wide caller submits a batch with one legitimate cluster topic
    // (`cluster-metrics`) and one tenant-prefixed name (`acme.orders`). The
    // scrub must refuse `acme.orders` per-entry but pass `cluster-metrics`
    // through to controller.createPartitions. This proves the scrub is
    // per-entry, not request-aborting, matching CreateTopics/DeleteTopics
    // batch semantics.
    val controller = mock(classOf[Controller])
    val allowedResult = new CreatePartitionsTopicResult().setName("cluster-metrics").setErrorCode(NONE.code)
    when(controller.createPartitions(
      any(classOf[ControllerRequestContext]),
      any(classOf[util.List[CreatePartitionsTopic]]),
      ArgumentMatchers.eq(false)))
      .thenReturn(CompletableFuture.completedFuture(java.util.Collections.singletonList(allowedResult)))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    val request = new CreatePartitionsRequestData()
    request.topics().add(new CreatePartitionsTopic().setName("cluster-metrics").setAssignments(null).setCount(5))
    request.topics().add(new CreatePartitionsTopic().setName("acme.orders").setAssignments(null).setCount(10))
    val results = controllerApis.createPartitions(ANONYMOUS_CONTEXT, request,
      _ => Set("cluster-metrics", "acme.orders")).get().asScala.toList
    val byName = results.map(r => r.name -> r.errorCode).toMap
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, byName("acme.orders"),
      "tenant-prefixed name must be refused")
    assertEquals(NONE.code, byName("cluster-metrics"),
      "legitimate cluster-scope topic must pass through unchanged")
    // Controller saw only the legitimate one.
    val topicsCaptor: ArgumentCaptor[util.List[CreatePartitionsTopic]] =
      ArgumentCaptor.forClass(classOf[util.List[CreatePartitionsTopic]])
    verify(controller).createPartitions(
      any(classOf[ControllerRequestContext]),
      topicsCaptor.capture(),
      ArgumentMatchers.eq(false))
    assertEquals(1, topicsCaptor.getValue.size,
      "exactly one topic must reach controller.createPartitions")
    assertEquals("cluster-metrics", topicsCaptor.getValue.get(0).name)
  }

  @Test
  def testControllerCreatePartitionsExemptsInternalTopicForClusterCaller(): Unit = {
    // Must-not-regress: a cluster-wide admin growing partitions on a Kafka-
    // internal topic (`__consumer_offsets`, `__transaction_state`,
    // `__share_group_state`) — names that pass `Topic.isInternal` — must not
    // be refused by the tenant-namespace scrub. Internal topics are shared
    // by all tenants on the broker; they belong to no tenant. Same exemption
    // the broker-side guards already apply.
    //
    // (Note: CREATE_PARTITIONS is NOT in TENANT_ALLOWED_APIS, so a tenant
    // principal cannot reach this handler at all — the legitimate-tenant
    // regression case is moot. This test instead pins the internal-topic
    // exemption, which IS the only must-not-regress for the scrub on the
    // controller listener today.)
    val controller = mock(classOf[Controller])
    when(controller.createPartitions(
      any(classOf[ControllerRequestContext]),
      any(classOf[util.List[CreatePartitionsTopic]]),
      ArgumentMatchers.eq(false)))
      .thenReturn(CompletableFuture.completedFuture(java.util.Collections.singletonList(
        new CreatePartitionsTopicResult().setName(Topic.GROUP_METADATA_TOPIC_NAME).setErrorCode(NONE.code))))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    val request = new CreatePartitionsRequestData()
    request.topics().add(new CreatePartitionsTopic()
      .setName(Topic.GROUP_METADATA_TOPIC_NAME).setAssignments(null).setCount(60))
    val results = controllerApis.createPartitions(ANONYMOUS_CONTEXT, request,
      _ => Set(Topic.GROUP_METADATA_TOPIC_NAME)).get().asScala.toList
    assertEquals(1, results.size)
    assertEquals(NONE.code, results.head.errorCode,
      "internal topic must be exempt from tenant-namespace scrub")
    // Internal topic reached controller.createPartitions intact.
    val topicsCaptor: ArgumentCaptor[util.List[CreatePartitionsTopic]] =
      ArgumentCaptor.forClass(classOf[util.List[CreatePartitionsTopic]])
    verify(controller).createPartitions(
      any(classOf[ControllerRequestContext]),
      topicsCaptor.capture(),
      ArgumentMatchers.eq(false))
    assertEquals(1, topicsCaptor.getValue.size)
    assertEquals(Topic.GROUP_METADATA_TOPIC_NAME, topicsCaptor.getValue.get(0).name)
  }

  @Test
  def testControllerCreatePartitionsRefusesCrossTenantPrincipalForeignNamespace(): Unit = {
    // Cross-tenant pollution via a known-tenant principal. The adversary
    // holds credentials for tenant `evil` and envelopes a CreatePartitions
    // targeting `acme.orders`. The scrub must refuse: `callerTenant` is
    // `Some("evil")` and `isForeignTenantNamespace("acme.orders", Some("evil"))`
    // returns true because tenant `acme` is known AND
    // `Some("evil") != Some("acme")`.
    val crossRequestData = new CreatePartitionsRequestData()
    crossRequestData.topics().add(
      new CreatePartitionsTopic().setName("acme.orders").setAssignments(null).setCount(10))
    val createPartitionsRequest = new CreatePartitionsRequest.Builder(crossRequestData).build()
    val crossTenantPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_evil.alice")
    val envelopeRequest = kafka.utils.TestUtils.buildEnvelopeRequest(
      createPartitionsRequest, envelopePrincipalSerde, requestChannelMetrics, time.nanoseconds(),
      forwardedPrincipal = crossTenantPrincipal,
      outerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    val controller = mock(classOf[Controller])
    val originals = new java.util.HashMap[String, AnyRef]()
    originals.put("listener.name.tenant_acme.tenant.id", "acme")
    originals.put("listener.name.tenant_evil.tenant.id", "evil")
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.from(originals))
    controllerApis.handle(envelopeRequest, RequestLocal.noCaching())

    // Cross-tenant request must never reach controller.createPartitions.
    verify(controller, never()).createPartitions(any(), any(), ArgumentMatchers.anyBoolean())
  }

  // ---------------------------------------------------------------------------
  // Split-mode KRaft / task #114 — STRUCTURAL TOPIC scrub.
  //
  // A controller-only node (`process.roles=controller`) has no broker
  // listeners, so its TenantConfig is constructed from broker originals that
  // contain ZERO `listener.name.<x>.tenant.id` keys. `allTenants` is empty.
  // The pre-task-#114 scrub keyed on `allTenants.iterator.exists(t =>
  // name.startsWith(t + "."))`, which collapsed to `false` for every name on
  // every request. Result: every controller-side TOPIC scrub on a split-mode
  // controller was a no-op and a cluster admin could create / delete / add
  // partitions to any tenant-prefixed topic by reaching bootstrap.controllers
  // directly.
  //
  // The structural fix replaces the `allTenants` lookup with a tenant-id
  // SHAPE check (`TenantNamespace.validateTenantId(prefix)`) and derives
  // callerTenant from the principal name structurally (no `allTenants`
  // membership gate). Combined-mode and split-mode now produce identical
  // decisions for any given (principal, topic) pair.
  //
  // Behavioural break documented in the helper comments: a cluster admin
  // naming a topic `foo.bar` from `bootstrap.controllers` is refused when
  // `foo` is a syntactically valid tenant id, even if `foo` is not currently
  // bound. Internal topics, single-`_` prefixes, and dotless names remain
  // allowed.
  // ---------------------------------------------------------------------------

  @Test
  def testControllerSplitModeCreateTopicsRefusesTenantPrefixedName(): Unit = {
    // Adversary on bootstrap.controllers of a SPLIT-MODE controller node.
    // No `tenant.id` binding anywhere in this process's config, so before
    // task #114 `allTenants.isEmpty` short-circuited the scrub to `false`
    // and `acme.orders` flowed straight to the metadata log. With the
    // structural check, the topic must be refused.
    val controller = new MockController.Builder().build()
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())
    val request = new CreateTopicsRequest.Builder(
      new CreateTopicsRequestData().setTopics(new CreatableTopicCollection(util.Arrays.asList(
        new CreatableTopic().setName("acme.orders").setNumPartitions(1).setReplicationFactor(1)
      ).iterator()))).build()
    val response = handleRequest[CreateTopicsResponse](request, controllerApis)
    val results = response.data.topics().asScala.map(t => t.name -> t.errorCode).toMap
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, results("acme.orders"),
      "split-mode controller (empty TenantConfig) must STILL refuse tenant-prefixed names — #114")
  }

  @Test
  def testControllerSplitModeCreateTopicsAllowsForwardedTenantPrincipal(): Unit = {
    // Companion to the refusal above: the legitimate forwarded tenant flow
    // (broker rewrote `foo` → `acme.foo`, envelope carries
    // `__tenant_acme.alice`) must continue to work in split mode too. The
    // structural `callerTenantFromPrincipal` derives tenant `acme` from the
    // principal name alone — no `allTenants` membership required.
    val topics = new CreatableTopicCollection()
    topics.add(new CreatableTopic().setName("acme.foo").setNumPartitions(1).setReplicationFactor(1.toShort))
    val createTopicsRequest = new CreateTopicsRequest.Builder(
      new CreateTopicsRequestData().setTopics(topics)).build()
    val tenantPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice")
    val envelopeRequest = kafka.utils.TestUtils.buildEnvelopeRequest(
      createTopicsRequest, envelopePrincipalSerde, requestChannelMetrics, time.nanoseconds(),
      forwardedPrincipal = tenantPrincipal,
      outerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    val controller = mock(classOf[Controller])
    when(controller.createTopics(
      any(classOf[ControllerRequestContext]),
      any(classOf[CreateTopicsRequestData]),
      any(classOf[java.util.Set[String]])))
      .thenReturn(CompletableFuture.completedFuture(new CreateTopicsResponseData()))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())
    controllerApis.handle(envelopeRequest, RequestLocal.noCaching())

    val createCaptor: ArgumentCaptor[CreateTopicsRequestData] =
      ArgumentCaptor.forClass(classOf[CreateTopicsRequestData])
    verify(controller).createTopics(
      any(classOf[ControllerRequestContext]),
      createCaptor.capture(),
      any(classOf[java.util.Set[String]]))
    val forwarded = createCaptor.getValue.topics().asScala.map(_.name).toList
    assertEquals(List("acme.foo"), forwarded,
      "split-mode controller must STILL pass the legitimate forwarded tenant create through — #114")
  }

  @Test
  def testControllerSplitModeDeleteTopicsAllowsForwardedTenantPrincipal(): Unit = {
    // DELETE_TOPICS is in TENANT_ALLOWED_APIS, so a tenant client legitimately
    // forwards a DeleteTopics request through the broker → controller route.
    // On a split-mode controller with empty TenantConfig, the structural
    // `callerTenantFromPrincipal` must still recognise `__tenant_acme.alice`
    // as tenant `acme` and let `acme.orders` reach controller.deleteTopics.
    val topics = new util.ArrayList[DeleteTopicState]()
    topics.add(new DeleteTopicState().setName("acme.orders").setTopicId(ZERO_UUID))
    val deleteRequest = new DeleteTopicsRequest.Builder(
      new DeleteTopicsRequestData().setTopics(topics).setTimeoutMs(5000)).build()
    val tenantPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice")
    val envelopeRequest = kafka.utils.TestUtils.buildEnvelopeRequest(
      deleteRequest, envelopePrincipalSerde, requestChannelMetrics, time.nanoseconds(),
      forwardedPrincipal = tenantPrincipal,
      outerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    val controller = mock(classOf[Controller])
    when(controller.findTopicNames(any(classOf[ControllerRequestContext]), any(classOf[java.util.Collection[Uuid]])))
      .thenReturn(CompletableFuture.completedFuture(java.util.Collections.emptyMap[Uuid, ResultOrError[String]]()))
    val nameToId = new util.HashMap[String, ResultOrError[Uuid]]()
    val topicUuid = Uuid.randomUuid()
    nameToId.put("acme.orders", new ResultOrError(topicUuid))
    when(controller.findTopicIds(any(classOf[ControllerRequestContext]), any(classOf[java.util.Collection[String]])))
      .thenReturn(CompletableFuture.completedFuture(nameToId))
    val deleteResults = new util.HashMap[Uuid, org.apache.kafka.common.requests.ApiError]()
    deleteResults.put(topicUuid, org.apache.kafka.common.requests.ApiError.NONE)
    when(controller.deleteTopics(any(classOf[ControllerRequestContext]), any(classOf[java.util.Set[Uuid]])))
      .thenReturn(CompletableFuture.completedFuture(deleteResults))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())
    controllerApis.handle(envelopeRequest, RequestLocal.noCaching())

    // The scrub did not refuse: controller.deleteTopics was invoked with
    // the resolved tenant uuid, not short-circuited to an empty set.
    val idCaptor: ArgumentCaptor[java.util.Set[Uuid]] =
      ArgumentCaptor.forClass(classOf[java.util.Set[Uuid]])
    verify(controller).deleteTopics(any(classOf[ControllerRequestContext]), idCaptor.capture())
    assertEquals(java.util.Collections.singleton(topicUuid), idCaptor.getValue,
      "split-mode controller must pass the legitimate forwarded tenant delete through — #114")
  }

  @Test
  def testControllerSplitModeCreateTopicsRefusesFreeDottedNameWithValidTenantShape(): Unit = {
    // Behavioural-break test (documented in the isForeignTenantNamespace
    // comment): a cluster admin naming a topic `cluster-metrics.frob` is now
    // refused on bootstrap.controllers, because `cluster-metrics` is a
    // syntactically valid tenant id (passes Topic.isValid, no `__` prefix,
    // no dot) AND the caller has no tenant principal binding. The pre-#114
    // combined-mode behaviour accepted this name because `cluster-metrics`
    // was not in `allTenants`. The cost is real (free-dotted names from a
    // cluster admin are off the table when the first segment looks like a
    // tenant id) but the structural check is the only way to make split-mode
    // controllers safe.
    //
    // Workaround for operators: name cluster-scope topics with a single-`_`
    // prefix (`_cluster-metrics.frob`) or with no dot at all
    // (`cluster-metrics-frob`); both remain allowed by the scrub.
    val controller = new MockController.Builder().build()
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())
    val request = new CreateTopicsRequest.Builder(
      new CreateTopicsRequestData().setTopics(new CreatableTopicCollection(util.Arrays.asList(
        new CreatableTopic().setName("cluster-metrics.frob").setNumPartitions(1).setReplicationFactor(1)
      ).iterator()))).build()
    val response = handleRequest[CreateTopicsResponse](request, controllerApis)
    val results = response.data.topics().asScala.map(t => t.name -> t.errorCode).toMap
    assertEquals(Errors.INVALID_TOPIC_EXCEPTION.code, results("cluster-metrics.frob"),
      "structural scrub refuses any `<x>.<y>` from a cluster-wide caller when `<x>` is tenant-id-shaped — documented break")
  }

  @Test
  def testControllerSplitModeCreateTopicsAllowsSingleUnderscorePrefixName(): Unit = {
    // The single-`_` prefix is the documented escape hatch for cluster-scope
    // topics that need a dot in the name (Connect connector configs like
    // `_confluent-monitoring.frob`, operator-internal `_metrics.lag`). The
    // structural scrub MUST allow these through even in split mode.
    val controller = new MockController.Builder().build()
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())
    val request = new CreateTopicsRequest.Builder(
      new CreateTopicsRequestData().setTopics(new CreatableTopicCollection(util.Arrays.asList(
        new CreatableTopic().setName("_confluent-monitoring.frob").setNumPartitions(1).setReplicationFactor(1)
      ).iterator()))).build()
    val response = handleRequest[CreateTopicsResponse](request, controllerApis)
    val results = response.data.topics().asScala.map(t => t.name -> t.errorCode).toMap
    assertEquals(Errors.NONE.code, results("_confluent-monitoring.frob"),
      "single-`_` prefix exempts the name from the structural tenant-namespace scrub — escape hatch")
  }

  @Test
  def testControllerSplitModeCreateTopicsAllowsDotlessName(): Unit = {
    // A dot-free name has no `<x>.<rest>` shape, so it can never be in any
    // tenant's namespace. Must remain allowed.
    val controller = new MockController.Builder().build()
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())
    val request = new CreateTopicsRequest.Builder(
      new CreateTopicsRequestData().setTopics(new CreatableTopicCollection(util.Arrays.asList(
        new CreatableTopic().setName("plain-topic").setNumPartitions(1).setReplicationFactor(1)
      ).iterator()))).build()
    val response = handleRequest[CreateTopicsResponse](request, controllerApis)
    val results = response.data.topics().asScala.map(t => t.name -> t.errorCode).toMap
    assertEquals(Errors.NONE.code, results("plain-topic"),
      "dotless name has no tenant-namespace shape and must pass the structural scrub")
  }

  @Test
  def testControllerSplitModeCreatePartitionsRefusesCrossTenantOnEmptyConfig(): Unit = {
    // Cross-tenant pollution on split mode: a tenant principal `evil` (not
    // bound on THIS controller but a valid forwarded identity from a peer
    // broker) submits CreatePartitions for `acme.orders`. The structural
    // check sees `callerTenant = Some("evil")` and `acme.orders`'s prefix
    // `acme` is tenant-id-shaped — refused (callerTenant != prefix). Note
    // that on this controller node, `evil` is not in any binding either —
    // proving the check works purely on the principal structure.
    val crossRequestData = new CreatePartitionsRequestData()
    crossRequestData.topics().add(
      new CreatePartitionsTopic().setName("acme.orders").setAssignments(null).setCount(10))
    val createPartitionsRequest = new CreatePartitionsRequest.Builder(crossRequestData).build()
    val crossTenantPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_evil.alice")
    val envelopeRequest = kafka.utils.TestUtils.buildEnvelopeRequest(
      createPartitionsRequest, envelopePrincipalSerde, requestChannelMetrics, time.nanoseconds(),
      forwardedPrincipal = crossTenantPrincipal,
      outerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    val controller = mock(classOf[Controller])
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())
    controllerApis.handle(envelopeRequest, RequestLocal.noCaching())

    verify(controller, never()).createPartitions(any(), any(), ArgumentMatchers.anyBoolean())
  }

  @Test
  def testControllerSplitModeCreateTopicsAllowsInternalTopic(): Unit = {
    // Must-not-regress: internal topics (`__consumer_offsets`, etc.) remain
    // exempt from the structural scrub in split mode too.
    val controller = mock(classOf[Controller])
    when(controller.createTopics(
      any(classOf[ControllerRequestContext]),
      any(classOf[CreateTopicsRequestData]),
      any(classOf[java.util.Set[String]])))
      .thenReturn(CompletableFuture.completedFuture(new CreateTopicsResponseData()))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())
    val request = new CreateTopicsRequest.Builder(
      new CreateTopicsRequestData().setTopics(new CreatableTopicCollection(util.Arrays.asList(
        new CreatableTopic().setName(Topic.GROUP_METADATA_TOPIC_NAME)
          .setNumPartitions(1).setReplicationFactor(1)
      ).iterator()))).build()
    handleRequest[CreateTopicsResponse](request, controllerApis)
    // controller.createTopics was invoked — the scrub did not short-circuit.
    val createCaptor: ArgumentCaptor[CreateTopicsRequestData] =
      ArgumentCaptor.forClass(classOf[CreateTopicsRequestData])
    verify(controller).createTopics(
      any(classOf[ControllerRequestContext]),
      createCaptor.capture(),
      any(classOf[java.util.Set[String]]))
    val forwarded = createCaptor.getValue.topics().asScala.map(_.name).toList
    assertEquals(List(Topic.GROUP_METADATA_TOPIC_NAME), forwarded,
      "internal topic must reach controller.createTopics intact in split mode")
  }

  // ---------------------------------------------------------------------------
  // #117 — AlterClientQuotas USER-entity tenant scrub on the controller.
  //
  // A cluster-wide admin (super-user on a non-tenant listener, or any caller
  // with ALTER on CLUSTER) hitting the controller listener directly through
  // `bootstrap.controllers` — or reaching here via the broker forward at
  // KafkaApis.scala line 808 — must NOT be able to persist a quota record
  // keyed on `__tenant_<id>.<user>`. The broker enforces quotas by runtime
  // principal name, so such a record silently DOS's (`producer_byte_rate=0`)
  // or elevates a tenant principal's traffic with no tenant-side audit.
  //
  // Per-tenant carve-out: a forwarded tenant principal can legitimately tune
  // quotas for its own user namespace (`__tenant_acme.alice` may set quotas
  // on `__tenant_acme.bob`), but cross-tenant entries are refused even from
  // a tenant principal.
  // ---------------------------------------------------------------------------

  private def buildAlterClientQuotasRequest(entries: util.Collection[ClientQuotaAlteration],
                                            principal: KafkaPrincipal): RequestChannel.Request = {
    val request = new AlterClientQuotasRequest.Builder(entries, false).build()
    buildTokenRequest(request, principal)
  }

  private def quotaEntityForUser(user: String): ClientQuotaEntity = {
    val entries = new util.HashMap[String, String]()
    entries.put(ClientQuotaEntity.USER, user)
    new ClientQuotaEntity(entries)
  }

  @Test
  def testControllerAlterClientQuotasRefusesTenantPrefixedUserFromClusterWideCaller(): Unit = {
    val controller = mock(classOf[Controller])
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    val tenantEntity = quotaEntityForUser("__tenant_acme.alice")
    val ops = util.Collections.singletonList(
      new ClientQuotaAlteration.Op("producer_byte_rate", 0.0))
    val entries = util.Collections.singletonList(new ClientQuotaAlteration(tenantEntity, ops))
    val req = buildAlterClientQuotasRequest(entries,
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis.handleAlterClientQuotas(req)

    // Short-circuit: with every entry refused, the controller must never be
    // asked to persist the record. Otherwise a regression in the merge below
    // could re-introduce the laundered tenant quota.
    verify(controller, never()).alterClientQuotas(
      any(classOf[ControllerRequestContext]),
      any(),
      anyBoolean())

    val response = captureSentResponse(req).asInstanceOf[AlterClientQuotasResponse]
    val entry = response.data().entries().asScala.head
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code(), entry.errorCode(),
      "cluster-wide caller must be refused for a tenant-prefixed USER entity")
    assertTrue(entry.errorMessage() == null || entry.errorMessage().isEmpty,
      "error message must not leak the physical tenant principal name back to the caller")
  }

  @Test
  def testControllerAlterClientQuotasAllowsSameTenantUserFromTenantCaller(): Unit = {
    val controller = mock(classOf[Controller])
    val tenantEntity = quotaEntityForUser("__tenant_acme.bob")
    // Stub controller to acknowledge the (legitimately-tenant-owned) entry.
    when(controller.alterClientQuotas(
      any(classOf[ControllerRequestContext]),
      any(),
      anyBoolean()))
      .thenReturn(CompletableFuture.completedFuture(
        java.util.Collections.singletonMap(tenantEntity, ApiError.NONE)))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    val ops = util.Collections.singletonList(
      new ClientQuotaAlteration.Op("producer_byte_rate", 1024.0 * 1024))
    val entries = util.Collections.singletonList(new ClientQuotaAlteration(tenantEntity, ops))
    val req = buildAlterClientQuotasRequest(entries,
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice"))

    controllerApis.handleAlterClientQuotas(req)

    verify(controller).alterClientQuotas(
      any(classOf[ControllerRequestContext]),
      any(),
      anyBoolean())
    val response = captureSentResponse(req).asInstanceOf[AlterClientQuotasResponse]
    val entry = response.data().entries().asScala.head
    assertEquals(Errors.NONE.code(), entry.errorCode(),
      "tenant caller must be allowed to tune quotas inside its own namespace")
  }

  @Test
  def testControllerAlterClientQuotasRefusesCrossTenantUserFromTenantCaller(): Unit = {
    val controller = mock(classOf[Controller])
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    // Caller is in tenant `acme`; target USER entity is inside tenant `evil`.
    val crossTenantEntity = quotaEntityForUser("__tenant_evil.alice")
    val ops = util.Collections.singletonList(
      new ClientQuotaAlteration.Op("producer_byte_rate", 0.0))
    val entries = util.Collections.singletonList(new ClientQuotaAlteration(crossTenantEntity, ops))
    val req = buildAlterClientQuotasRequest(entries,
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.attacker"))

    controllerApis.handleAlterClientQuotas(req)

    verify(controller, never()).alterClientQuotas(
      any(classOf[ControllerRequestContext]),
      any(),
      anyBoolean())
    val response = captureSentResponse(req).asInstanceOf[AlterClientQuotasResponse]
    val entry = response.data().entries().asScala.head
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code(), entry.errorCode(),
      "tenant caller must NOT be able to set quotas on another tenant's USER namespace")
    assertTrue(entry.errorMessage() == null || entry.errorMessage().isEmpty,
      "error message must not leak the foreign tenant principal name back to the caller")
  }

  @Test
  def testControllerAlterClientQuotasMixedBatchSplitsRefusedAndAccepted(): Unit = {
    val controller = mock(classOf[Controller])
    val clusterEntity = quotaEntityForUser("regular-cluster-user")
    val tenantEntity = quotaEntityForUser("__tenant_acme.alice")
    // The controller MUST only see the cluster entry; the tenant one is
    // refused upstream. Stub returns NONE for the cluster entry only.
    when(controller.alterClientQuotas(
      any(classOf[ControllerRequestContext]),
      any(),
      anyBoolean()))
      .thenReturn(CompletableFuture.completedFuture(
        java.util.Collections.singletonMap(clusterEntity, ApiError.NONE)))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    val ops = util.Collections.singletonList(
      new ClientQuotaAlteration.Op("producer_byte_rate", 1024.0 * 1024))
    val entries = new util.ArrayList[ClientQuotaAlteration]()
    entries.add(new ClientQuotaAlteration(clusterEntity, ops))
    entries.add(new ClientQuotaAlteration(tenantEntity, ops))
    val req = buildAlterClientQuotasRequest(entries,
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis.handleAlterClientQuotas(req)

    // Capture the entries actually forwarded to the controller — the tenant
    // one must NOT be present.
    val forwarded: ArgumentCaptor[util.Collection[ClientQuotaAlteration]] =
      ArgumentCaptor.forClass(classOf[util.Collection[ClientQuotaAlteration]])
    verify(controller).alterClientQuotas(
      any(classOf[ControllerRequestContext]),
      forwarded.capture(),
      anyBoolean())
    val seenEntities = forwarded.getValue.asScala.map(_.entity()).toSet
    assertEquals(Set(clusterEntity), seenEntities,
      "only the cluster-namespaced entry must reach the controller")

    val response = captureSentResponse(req).asInstanceOf[AlterClientQuotasResponse]
    val byUser: Map[String, Short] = response.data().entries().asScala.flatMap { entry =>
      entry.entity().asScala.find(_.entityType() == ClientQuotaEntity.USER)
        .map(u => u.entityName() -> entry.errorCode())
    }.toMap
    assertEquals(Errors.NONE.code(), byUser("regular-cluster-user"),
      "cluster user must round-trip with NONE")
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code(), byUser("__tenant_acme.alice"),
      "tenant user must come back with CLUSTER_AUTHORIZATION_FAILED")
  }

  // ---------------------------------------------------------------------------
  // #136 — AlterClientQuotas guard must cover EVERY entity-entry type, not
  // just USER. A ClientQuotaEntity is a composite (USER, CLIENT_ID, IP and any
  // future entity-type the message schema may carry); the original #117 fix
  // only inspected the USER entry, leaving a non-USER entry-value free to
  // carry the reserved `__tenant_*` prefix into the metadata write. Even if
  // CLIENT_ID/IP are not used by the broker to key runtime sensors, persisting
  // any record in the reserved namespace from a non-tenant caller violates the
  // structural contract (the entry would also be invisible in
  // DescribeClientQuotas response for cluster-wide callers due to #116,
  // creating silent metadata pollution).
  // ---------------------------------------------------------------------------

  private def quotaEntityForClientId(clientId: String): ClientQuotaEntity = {
    val entries = new util.HashMap[String, String]()
    entries.put(ClientQuotaEntity.CLIENT_ID, clientId)
    new ClientQuotaEntity(entries)
  }

  private def quotaEntityForUserAndClientId(user: String, clientId: String): ClientQuotaEntity = {
    val entries = new util.HashMap[String, String]()
    entries.put(ClientQuotaEntity.USER, user)
    entries.put(ClientQuotaEntity.CLIENT_ID, clientId)
    new ClientQuotaEntity(entries)
  }

  @Test
  def testControllerAlterClientQuotasRefusesTenantPrefixedClientIdFromClusterWideCaller(): Unit = {
    val controller = mock(classOf[Controller])
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    // CLIENT_ID-only entity carrying the tenant-prefix shape. A cluster-wide
    // admin must NOT be able to plant this record — even though CLIENT_ID is
    // not the runtime sensor key, persisting any record in the reserved
    // namespace from a non-tenant caller is forbidden by the contract.
    val tenantEntity = quotaEntityForClientId("__tenant_acme.alice")
    val ops = util.Collections.singletonList(
      new ClientQuotaAlteration.Op("producer_byte_rate", 0.0))
    val entries = util.Collections.singletonList(new ClientQuotaAlteration(tenantEntity, ops))
    val req = buildAlterClientQuotasRequest(entries,
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis.handleAlterClientQuotas(req)

    verify(controller, never()).alterClientQuotas(
      any(classOf[ControllerRequestContext]), any(), anyBoolean())
    val response = captureSentResponse(req).asInstanceOf[AlterClientQuotasResponse]
    val entry = response.data().entries().asScala.head
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code(), entry.errorCode(),
      "cluster-wide caller must be refused for a tenant-prefixed CLIENT_ID entity (#136)")
    assertTrue(entry.errorMessage() == null || entry.errorMessage().isEmpty,
      "errorMessage must not echo the physical CLIENT_ID back to the caller")
  }

  @Test
  def testControllerAlterClientQuotasRefusesCompositeWhereOnlyClientIdIsForeignTenant(): Unit = {
    val controller = mock(classOf[Controller])
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    // USER value is a regular cluster user (would pass the USER-only check);
    // CLIENT_ID is tenant-shaped. The composite must still be refused — a
    // partial-accept would persist the tenant-shaped half via the composite
    // key (USER, CLIENT_ID), an invisible-to-everyone metadata record.
    val mixedEntity = quotaEntityForUserAndClientId(
      user = "regular-cluster-user",
      clientId = "__tenant_acme.alice")
    val ops = util.Collections.singletonList(
      new ClientQuotaAlteration.Op("producer_byte_rate", 0.0))
    val entries = util.Collections.singletonList(new ClientQuotaAlteration(mixedEntity, ops))
    val req = buildAlterClientQuotasRequest(entries,
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis.handleAlterClientQuotas(req)

    verify(controller, never()).alterClientQuotas(
      any(classOf[ControllerRequestContext]), any(), anyBoolean())
    val response = captureSentResponse(req).asInstanceOf[AlterClientQuotasResponse]
    val entry = response.data().entries().asScala.head
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code(), entry.errorCode(),
      "ANY tenant-shaped entry-value in a composite entity must refuse the whole alteration (#136)")
  }

  @Test
  def testControllerAlterClientQuotasAllowsSameTenantClientIdFromTenantCaller(): Unit = {
    val controller = mock(classOf[Controller])
    val tenantEntity = quotaEntityForClientId("__tenant_acme.app-1")
    when(controller.alterClientQuotas(
      any(classOf[ControllerRequestContext]), any(), anyBoolean()))
      .thenReturn(CompletableFuture.completedFuture(
        java.util.Collections.singletonMap(tenantEntity, ApiError.NONE)))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    // Same-tenant carve-out: a forwarded `__tenant_acme.X` may tune quotas
    // for CLIENT_ID entries inside its own namespace.
    val ops = util.Collections.singletonList(
      new ClientQuotaAlteration.Op("producer_byte_rate", 1024.0 * 1024))
    val entries = util.Collections.singletonList(new ClientQuotaAlteration(tenantEntity, ops))
    val req = buildAlterClientQuotasRequest(entries,
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice"))

    controllerApis.handleAlterClientQuotas(req)

    verify(controller).alterClientQuotas(
      any(classOf[ControllerRequestContext]), any(), anyBoolean())
    val response = captureSentResponse(req).asInstanceOf[AlterClientQuotasResponse]
    val entry = response.data().entries().asScala.head
    assertEquals(Errors.NONE.code(), entry.errorCode(),
      "tenant caller must be allowed to tune CLIENT_ID quotas inside its own namespace")
  }

  @Test
  def testControllerAlterClientQuotasRefusesCrossTenantClientIdFromTenantCaller(): Unit = {
    val controller = mock(classOf[Controller])
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    // Cross-tenant CLIENT_ID attempt: caller is `__tenant_acme.*`, value is
    // `__tenant_evil.*`. Must be refused even from a tenant principal.
    val crossEntity = quotaEntityForClientId("__tenant_evil.app-1")
    val ops = util.Collections.singletonList(
      new ClientQuotaAlteration.Op("producer_byte_rate", 0.0))
    val entries = util.Collections.singletonList(new ClientQuotaAlteration(crossEntity, ops))
    val req = buildAlterClientQuotasRequest(entries,
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.attacker"))

    controllerApis.handleAlterClientQuotas(req)

    verify(controller, never()).alterClientQuotas(
      any(classOf[ControllerRequestContext]), any(), anyBoolean())
    val response = captureSentResponse(req).asInstanceOf[AlterClientQuotasResponse]
    val entry = response.data().entries().asScala.head
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code(), entry.errorCode(),
      "tenant caller must NOT plant CLIENT_ID quotas in a foreign tenant's namespace (#136)")
  }

  // ---------------------------------------------------------------------------
  // #126 / F1 — handleIncrementalAlterConfigs + handleLegacyAlterConfigs on
  // ControllerApis pre-scrub TOPIC and GROUP resource names. A privileged
  // caller hitting the controller listener via `bootstrap.controllers`
  // (KIP-590) skips every broker-side scrub (KafkaApis.scala:4039 / 4094),
  // and the GROUP type is unscrubbed even on the broker, so the controller is
  // the sole chokepoint. The scrub:
  //   - TOPIC: refuse `<tenantId>.<rest>` shape unless caller is in tenant
  //     `<tenantId>`. Error code INVALID_TOPIC_EXCEPTION, echoed name (the
  //     name is public via Metadata anyway).
  //   - GROUP: refuse `__tenant_<tenantId>.<rest>` shape unless caller is in
  //     tenant `<tenantId>`. Error code GROUP_AUTHORIZATION_FAILED with NULL
  //     errorMessage — leaking the physical group id back would be a presence
  //     oracle for that tenant's consumer groups.
  // ---------------------------------------------------------------------------

  private def incrementalResource(resourceType: ConfigResource.Type, name: String): AlterConfigsResource = {
    new AlterConfigsResource()
      .setResourceName(name)
      .setResourceType(resourceType.id())
      .setConfigs(new AlterableConfigCollection(util.Arrays.asList(
        new AlterableConfig().setName("retention.ms").setValue("60000")
          .setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator()))
  }

  private def legacyResource(resourceType: ConfigResource.Type, name: String): OldAlterConfigsResource = {
    new OldAlterConfigsResource()
      .setResourceName(name)
      .setResourceType(resourceType.id())
      .setConfigs(new OldAlterableConfigCollection(util.Arrays.asList(
        new OldAlterableConfig().setName("retention.ms").setValue("60000")).iterator()))
  }

  @Test
  def testControllerIncrementalAlterConfigsRefusesForeignTenantTopicFromClusterWideCaller(): Unit = {
    val controller = mock(classOf[Controller])
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    val requestData = new IncrementalAlterConfigsRequestData().setResources(
      new AlterConfigsResourceCollection(util.Arrays.asList(
        incrementalResource(ConfigResource.Type.TOPIC, "acme.orders")).iterator()))
    val req = buildTokenRequest(
      new IncrementalAlterConfigsRequest.Builder(requestData).build(0),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis.handleIncrementalAlterConfigs(req)

    // Controller must NEVER see the foreign-tenant TOPIC. If it did, the
    // metadata write layer would persist the topic-config record verbatim.
    verify(controller, never()).incrementalAlterConfigs(
      any(classOf[ControllerRequestContext]), any(), anyBoolean())

    val response = captureSentResponse(req).asInstanceOf[IncrementalAlterConfigsResponse]
    val r = response.data().responses().asScala.head
    assertEquals(INVALID_TOPIC_EXCEPTION.code(), r.errorCode(),
      "cluster-wide caller must be refused for a tenant-prefixed TOPIC resource")
    assertEquals("acme.orders", r.resourceName())
  }

  @Test
  def testControllerIncrementalAlterConfigsRefusesForeignTenantGroupFromClusterWideCaller(): Unit = {
    val controller = mock(classOf[Controller])
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    val requestData = new IncrementalAlterConfigsRequestData().setResources(
      new AlterConfigsResourceCollection(util.Arrays.asList(
        incrementalResource(ConfigResource.Type.GROUP, "__tenant_acme.alice-group")).iterator()))
    val req = buildTokenRequest(
      new IncrementalAlterConfigsRequest.Builder(requestData).build(0),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis.handleIncrementalAlterConfigs(req)

    verify(controller, never()).incrementalAlterConfigs(
      any(classOf[ControllerRequestContext]), any(), anyBoolean())

    val response = captureSentResponse(req).asInstanceOf[IncrementalAlterConfigsResponse]
    val r = response.data().responses().asScala.head
    assertEquals(GROUP_AUTHORIZATION_FAILED.code(), r.errorCode(),
      "cluster-wide caller must be refused for a tenant-prefixed GROUP resource")
    assertTrue(r.errorMessage() == null || r.errorMessage().isEmpty,
      "error message must not echo the physical tenant group id back to the caller (presence oracle)")
  }

  @Test
  def testControllerIncrementalAlterConfigsAllowsSameTenantGroupFromTenantCaller(): Unit = {
    val controller = mock(classOf[Controller])
    // Stub controller to acknowledge the legitimately-tenant-owned entry.
    when(controller.incrementalAlterConfigs(
      any(classOf[ControllerRequestContext]), any(), anyBoolean()))
      .thenReturn(CompletableFuture.completedFuture(
        java.util.Collections.singletonMap(
          new ConfigResource(ConfigResource.Type.GROUP, "__tenant_acme.bob-group"),
          ApiError.NONE)))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    val requestData = new IncrementalAlterConfigsRequestData().setResources(
      new AlterConfigsResourceCollection(util.Arrays.asList(
        incrementalResource(ConfigResource.Type.GROUP, "__tenant_acme.bob-group")).iterator()))
    val req = buildTokenRequest(
      new IncrementalAlterConfigsRequest.Builder(requestData).build(0),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice"))

    controllerApis.handleIncrementalAlterConfigs(req)

    verify(controller).incrementalAlterConfigs(
      any(classOf[ControllerRequestContext]), any(), anyBoolean())
    val response = captureSentResponse(req).asInstanceOf[IncrementalAlterConfigsResponse]
    val r = response.data().responses().asScala.head
    assertEquals(NONE.code(), r.errorCode(),
      "a forwarded tenant principal may tune configs for groups in its own namespace")
  }

  @Test
  def testControllerIncrementalAlterConfigsMixedBatchSplitsRefusedAndAccepted(): Unit = {
    val controller = mock(classOf[Controller])
    val clusterResource = new ConfigResource(ConfigResource.Type.GROUP, "regular-cluster-group")
    // Controller will only see the cluster-namespaced entry; the tenant
    // entries (TOPIC + GROUP) are refused upstream and never forwarded.
    when(controller.incrementalAlterConfigs(
      any(classOf[ControllerRequestContext]), any(), anyBoolean()))
      .thenReturn(CompletableFuture.completedFuture(
        java.util.Collections.singletonMap(clusterResource, ApiError.NONE)))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    val requestData = new IncrementalAlterConfigsRequestData().setResources(
      new AlterConfigsResourceCollection(util.Arrays.asList(
        incrementalResource(ConfigResource.Type.GROUP, "regular-cluster-group"),
        incrementalResource(ConfigResource.Type.TOPIC, "acme.orders"),
        incrementalResource(ConfigResource.Type.GROUP, "__tenant_acme.alice-group")
      ).iterator()))
    val req = buildTokenRequest(
      new IncrementalAlterConfigsRequest.Builder(requestData).build(0),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis.handleIncrementalAlterConfigs(req)

    // Verify only the cluster-namespaced GROUP reached the controller.
    val forwarded: ArgumentCaptor[util.Map[ConfigResource, util.Map[String, java.util.Map.Entry[AlterConfigOp.OpType, String]]]] =
      ArgumentCaptor.forClass(classOf[util.Map[ConfigResource, util.Map[String, java.util.Map.Entry[AlterConfigOp.OpType, String]]]])
    verify(controller).incrementalAlterConfigs(
      any(classOf[ControllerRequestContext]), forwarded.capture(), anyBoolean())
    assertEquals(Set(clusterResource), forwarded.getValue.keySet().asScala.toSet,
      "only the cluster-namespaced GROUP must reach the controller")

    val response = captureSentResponse(req).asInstanceOf[IncrementalAlterConfigsResponse]
    val byName: Map[String, Short] = response.data().responses().asScala
      .map(r => r.resourceName() -> r.errorCode()).toMap
    assertEquals(NONE.code(), byName("regular-cluster-group"),
      "cluster group must round-trip with NONE")
    assertEquals(INVALID_TOPIC_EXCEPTION.code(), byName("acme.orders"),
      "tenant TOPIC must come back with INVALID_TOPIC_EXCEPTION")
    assertEquals(GROUP_AUTHORIZATION_FAILED.code(), byName("__tenant_acme.alice-group"),
      "tenant GROUP must come back with GROUP_AUTHORIZATION_FAILED")
  }

  @Test
  def testControllerLegacyAlterConfigsRefusesForeignTenantTopicFromClusterWideCaller(): Unit = {
    val controller = mock(classOf[Controller])
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    val requestData = new AlterConfigsRequestData().setResources(
      new OldAlterConfigsResourceCollection(util.Arrays.asList(
        legacyResource(ConfigResource.Type.TOPIC, "acme.orders")).iterator()))
    val req = buildTokenRequest(
      new AlterConfigsRequest(requestData, 0),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis.handleLegacyAlterConfigs(req)

    verify(controller, never()).legacyAlterConfigs(
      any(classOf[ControllerRequestContext]), any(), anyBoolean())

    val response = captureSentResponse(req).asInstanceOf[AlterConfigsResponse]
    val r = response.data().responses().asScala.head
    assertEquals(INVALID_TOPIC_EXCEPTION.code(), r.errorCode(),
      "legacy handler must also refuse a tenant-prefixed TOPIC from a cluster-wide caller")
    assertEquals("acme.orders", r.resourceName())
  }

  // ---------------------------------------------------------------------------
  // #70: DescribeConfigs on the controller listener is the SOLE chokepoint
  // against the `bootstrap.controllers` (KIP-590) bypass: a cluster-wide Admin
  // talking directly to the controller skips the broker-side KafkaApis scrub
  // and could ask for `acme.orders` (TOPIC) or `__tenant_acme.alice-group`
  // (GROUP). Without a controller-side scrub the response carries:
  //   - for a tenant topic that exists, the full topic config (retention.ms,
  //     segment.bytes, cleanup.policy, ...) — full disclosure;
  //   - for a tenant topic that does not exist, UNKNOWN_TOPIC_OR_PARTITION
  //     (whereas a refused entry would carry TOPIC_AUTHORIZATION_FAILED) —
  //     existence oracle.
  // Same-tenant carve-out: a forwarded `__tenant_acme.alice` principal must
  // still be able to read configs of resources in its own namespace.
  // ---------------------------------------------------------------------------

  private def describeConfigsResource(rt: ConfigResource.Type, name: String): DescribeConfigsRequestData.DescribeConfigsResource = {
    new DescribeConfigsRequestData.DescribeConfigsResource()
      .setResourceType(rt.id())
      .setResourceName(name)
  }

  @Test
  def testControllerDescribeConfigsRefusesForeignTenantTopicFromBootstrapControllersAdmin(): Unit = {
    val controller = mock(classOf[Controller])
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    val requestData = new DescribeConfigsRequestData().setResources(
      util.Arrays.asList(describeConfigsResource(ConfigResource.Type.TOPIC, "acme.orders")))
    val req = buildTokenRequest(
      new DescribeConfigsRequest.Builder(requestData).build(1),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis.handleDescribeConfigsRequest(req)

    val response = captureSentResponse(req).asInstanceOf[DescribeConfigsResponse]
    val r = response.data().results().asScala.head
    assertEquals(TOPIC_AUTHORIZATION_FAILED.code(), r.errorCode(),
      "bootstrap.controllers admin must be refused for a tenant-prefixed TOPIC; otherwise the response would leak the topic's full config (#70)")
    assertEquals("acme.orders", r.resourceName(),
      "echoed name must be the structurally-refused logical form")
    assertTrue(r.configs() == null || r.configs().isEmpty,
      "no config entries may be returned for a refused tenant TOPIC")
    assertTrue(r.errorMessage() == null || r.errorMessage().isEmpty,
      "errorMessage must not echo physical names back to the cluster-wide caller (existence oracle)")
  }

  @Test
  def testControllerDescribeConfigsRefusesForeignTenantGroupFromBootstrapControllersAdmin(): Unit = {
    val controller = mock(classOf[Controller])
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    val requestData = new DescribeConfigsRequestData().setResources(
      util.Arrays.asList(describeConfigsResource(ConfigResource.Type.GROUP, "__tenant_acme.alice-group")))
    val req = buildTokenRequest(
      new DescribeConfigsRequest.Builder(requestData).build(1),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis.handleDescribeConfigsRequest(req)

    val response = captureSentResponse(req).asInstanceOf[DescribeConfigsResponse]
    val r = response.data().results().asScala.head
    assertEquals(GROUP_AUTHORIZATION_FAILED.code(), r.errorCode(),
      "bootstrap.controllers admin must be refused for a tenant-prefixed GROUP")
    assertEquals("__tenant_acme.alice-group", r.resourceName())
    assertTrue(r.configs() == null || r.configs().isEmpty,
      "no config entries for a refused tenant GROUP")
    assertTrue(r.errorMessage() == null || r.errorMessage().isEmpty,
      "errorMessage must not echo physical group id back to the cluster-wide caller")
  }

  @Test
  def testControllerDescribeConfigsAllowsSameTenantGroupFromForwardedTenantPrincipal(): Unit = {
    val controller = mock(classOf[Controller])
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    val requestData = new DescribeConfigsRequestData().setResources(
      util.Arrays.asList(describeConfigsResource(ConfigResource.Type.GROUP, "__tenant_acme.bob-group")))
    val req = buildTokenRequest(
      new DescribeConfigsRequest.Builder(requestData).build(1),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice"))

    controllerApis.handleDescribeConfigsRequest(req)

    val response = captureSentResponse(req).asInstanceOf[DescribeConfigsResponse]
    val r = response.data().results().asScala.head
    // The carve-out lets the resource flow into ConfigHelper, which then
    // resolves the GROUP via the (empty) metadata cache: default group config
    // is returned with NONE. The point of the assertion is the *negative*:
    // the entry was NOT short-circuited with GROUP_AUTHORIZATION_FAILED, i.e.
    // the same-tenant carve-out (callerOwnsPrincipalNamespaceName) fired.
    assertEquals(NONE.code(), r.errorCode(),
      "a forwarded tenant principal may read configs for groups in its own namespace; the controller-side scrub must NOT refuse it")
    assertEquals("__tenant_acme.bob-group", r.resourceName())
  }

  @Test
  def testControllerDescribeConfigsMixedBatchSplitsRefusedAndForwarded(): Unit = {
    val controller = mock(classOf[Controller])
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    val requestData = new DescribeConfigsRequestData().setResources(
      util.Arrays.asList(
        describeConfigsResource(ConfigResource.Type.TOPIC, "public-topic"),
        describeConfigsResource(ConfigResource.Type.TOPIC, "acme.orders"),
        describeConfigsResource(ConfigResource.Type.GROUP, "__tenant_acme.alice-group")
      ))
    val req = buildTokenRequest(
      new DescribeConfigsRequest.Builder(requestData).build(1),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis.handleDescribeConfigsRequest(req)

    val response = captureSentResponse(req).asInstanceOf[DescribeConfigsResponse]
    val byName: Map[String, Short] = response.data().results().asScala
      .map(r => r.resourceName() -> r.errorCode()).toMap

    // The plain TOPIC name is not refused upstream; the metadata cache is
    // empty in this fixture so ConfigHelper returns UNKNOWN_TOPIC_OR_PARTITION
    // — what matters is that it is NOT TOPIC_AUTHORIZATION_FAILED, i.e. the
    // scrub did not collateral-damage a cluster-namespaced topic.
    assertEquals(UNKNOWN_TOPIC_OR_PARTITION.code(), byName("public-topic"),
      "non-tenant TOPIC must be forwarded to ConfigHelper untouched")
    assertEquals(TOPIC_AUTHORIZATION_FAILED.code(), byName("acme.orders"),
      "tenant TOPIC must be refused with TOPIC_AUTHORIZATION_FAILED")
    assertEquals(GROUP_AUTHORIZATION_FAILED.code(), byName("__tenant_acme.alice-group"),
      "tenant GROUP must be refused with GROUP_AUTHORIZATION_FAILED")
  }

  // ---------------------------------------------------------------------------
  // F4: AlterPartitionReassignments — outside-in TOPIC scrub on the controller
  // listener. A direct `bootstrap.controllers` Admin (KIP-590) bypasses the
  // broker-side scrub in KafkaApis.handleAlterPartitionReassignmentsRequest;
  // the controller is therefore the SOLE chokepoint on this path. Refuse
  // foreign-tenant topic entries per-topic; pass cluster topics through.
  // Same-tenant carve-out via `isForeignTenantNamespace`: a forwarded tenant
  // principal may reassign topics in its own namespace.
  // ---------------------------------------------------------------------------

  @Test
  def testControllerAlterPartitionReassignmentsRefusesForeignTenantTopic(): Unit = {
    val controller = mock(classOf[Controller])
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    val data = new AlterPartitionReassignmentsRequestData()
    data.topics().add(new AlterPartitionReassignmentsRequestData.ReassignableTopic()
      .setName("acme.orders")
      .setPartitions(util.Arrays.asList(
        new AlterPartitionReassignmentsRequestData.ReassignablePartition()
          .setPartitionIndex(0)
          .setReplicas(util.Arrays.asList(java.lang.Integer.valueOf(1), java.lang.Integer.valueOf(2))))))
    val req = buildTokenRequest(
      new AlterPartitionReassignmentsRequest.Builder(data).build(),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis.handleAlterPartitionReassignments(req)

    // Controller must NEVER see the foreign-tenant topic. If it did, the
    // reassignment record would be appended to the metadata log verbatim.
    verify(controller, never()).alterPartitionReassignments(
      any(classOf[ControllerRequestContext]),
      any(classOf[AlterPartitionReassignmentsRequestData]))

    val response = captureSentResponse(req).asInstanceOf[AlterPartitionReassignmentsResponse]
    val responses = response.data().responses().asScala.toList
    assertEquals(1, responses.size, "single refused topic expected")
    assertEquals("acme.orders", responses.head.name())
    val part = responses.head.partitions().asScala.head
    assertEquals(0, part.partitionIndex())
    assertEquals(INVALID_TOPIC_EXCEPTION.code(), part.errorCode(),
      "cluster-wide caller must be refused on a tenant-prefixed topic")
    assertNotNull(part.errorMessage())
    assertTrue(part.errorMessage().contains("acme.orders"),
      "topic name is public via Metadata; echo it back to the caller")
  }

  @Test
  def testControllerAlterPartitionReassignmentsAllowsSameTenantTopicFromTenantCaller(): Unit = {
    val controller = mock(classOf[Controller])
    val controllerResponse = new AlterPartitionReassignmentsResponseData()
    controllerResponse.responses().add(new AlterPartitionReassignmentsResponseData.ReassignableTopicResponse()
      .setName("acme.orders")
      .setPartitions(util.Arrays.asList(
        new AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse()
          .setPartitionIndex(0)
          .setErrorCode(NONE.code))))
    when(controller.alterPartitionReassignments(
      any(classOf[ControllerRequestContext]),
      any(classOf[AlterPartitionReassignmentsRequestData])))
      .thenReturn(CompletableFuture.completedFuture(controllerResponse))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    val data = new AlterPartitionReassignmentsRequestData()
    data.topics().add(new AlterPartitionReassignmentsRequestData.ReassignableTopic()
      .setName("acme.orders")
      .setPartitions(util.Arrays.asList(
        new AlterPartitionReassignmentsRequestData.ReassignablePartition()
          .setPartitionIndex(0)
          .setReplicas(util.Arrays.asList(java.lang.Integer.valueOf(1))))))
    val req = buildTokenRequest(
      new AlterPartitionReassignmentsRequest.Builder(data).build(),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice"))

    controllerApis.handleAlterPartitionReassignments(req)

    val forwarded: ArgumentCaptor[AlterPartitionReassignmentsRequestData] =
      ArgumentCaptor.forClass(classOf[AlterPartitionReassignmentsRequestData])
    verify(controller).alterPartitionReassignments(
      any(classOf[ControllerRequestContext]),
      forwarded.capture())
    assertEquals(1, forwarded.getValue.topics().size,
      "tenant principal's own-namespace topic must reach the controller")
    assertEquals("acme.orders", forwarded.getValue.topics().get(0).name())

    val response = captureSentResponse(req).asInstanceOf[AlterPartitionReassignmentsResponse]
    val p = response.data().responses().asScala.head.partitions().asScala.head
    assertEquals(NONE.code(), p.errorCode(),
      "forwarded tenant principal may reassign topics in its own namespace")
  }

  @Test
  def testControllerAlterPartitionReassignmentsMixedBatchSplitsRefusedAndAccepted(): Unit = {
    val controller = mock(classOf[Controller])
    val controllerResponse = new AlterPartitionReassignmentsResponseData()
    controllerResponse.responses().add(new AlterPartitionReassignmentsResponseData.ReassignableTopicResponse()
      .setName("cluster-metrics")
      .setPartitions(util.Arrays.asList(
        new AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse()
          .setPartitionIndex(0)
          .setErrorCode(NONE.code))))
    when(controller.alterPartitionReassignments(
      any(classOf[ControllerRequestContext]),
      any(classOf[AlterPartitionReassignmentsRequestData])))
      .thenReturn(CompletableFuture.completedFuture(controllerResponse))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    val data = new AlterPartitionReassignmentsRequestData()
    data.topics().add(new AlterPartitionReassignmentsRequestData.ReassignableTopic()
      .setName("acme.orders")
      .setPartitions(util.Arrays.asList(
        new AlterPartitionReassignmentsRequestData.ReassignablePartition()
          .setPartitionIndex(0)
          .setReplicas(util.Arrays.asList(java.lang.Integer.valueOf(1))))))
    data.topics().add(new AlterPartitionReassignmentsRequestData.ReassignableTopic()
      .setName("cluster-metrics")
      .setPartitions(util.Arrays.asList(
        new AlterPartitionReassignmentsRequestData.ReassignablePartition()
          .setPartitionIndex(0)
          .setReplicas(util.Arrays.asList(java.lang.Integer.valueOf(1))))))
    val req = buildTokenRequest(
      new AlterPartitionReassignmentsRequest.Builder(data).build(),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis.handleAlterPartitionReassignments(req)

    // Only the cluster-scope topic reaches controller.alterPartitionReassignments.
    val forwarded: ArgumentCaptor[AlterPartitionReassignmentsRequestData] =
      ArgumentCaptor.forClass(classOf[AlterPartitionReassignmentsRequestData])
    verify(controller).alterPartitionReassignments(
      any(classOf[ControllerRequestContext]),
      forwarded.capture())
    assertEquals(util.Arrays.asList("cluster-metrics"),
      forwarded.getValue.topics().asScala.map(_.name()).asJava,
      "only the cluster-scope topic must reach the controller")

    val response = captureSentResponse(req).asInstanceOf[AlterPartitionReassignmentsResponse]
    val byName: Map[String, Short] = response.data().responses().asScala
      .map(t => t.name() -> t.partitions().asScala.head.errorCode()).toMap
    assertEquals(INVALID_TOPIC_EXCEPTION.code(), byName("acme.orders"),
      "tenant TOPIC must be refused with INVALID_TOPIC_EXCEPTION")
    assertEquals(NONE.code(), byName("cluster-metrics"),
      "legitimate cluster topic must round-trip with NONE")
  }

  // ---------------------------------------------------------------------------
  // F6: ElectLeaders — outside-in TOPIC scrub + null-sweep guard on the
  // controller listener. Two attack shapes:
  //
  //   1. Named-topic mode (`topicPartitions != null`): refuse per-topic just
  //      like AlterPartitionReassignments above.
  //
  //   2. Null-sweep mode (`topicPartitions == null`): the controller would
  //      otherwise enumerate every topic in topicsByName and elect leaders on
  //      each. A forwarded tenant principal must NEVER null-sweep — refused
  //      with CLUSTER_AUTHORIZATION_FAILED. A cluster-wide caller's null-sweep
  //      is forwarded (the admin has ALTER on CLUSTER, same as the broker's
  //      stated rationale) but the response is filtered post-hoc to strip
  //      tenant-namespaced topic names — defence-in-depth against using the
  //      sweep response as a topic-name enumeration oracle.
  // ---------------------------------------------------------------------------

  @Test
  def testControllerElectLeadersRefusesForeignTenantTopic(): Unit = {
    val controller = mock(classOf[Controller])
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    val req = buildTokenRequest(
      new ElectLeadersRequest.Builder(ElectionType.PREFERRED,
        util.Arrays.asList(new org.apache.kafka.common.TopicPartition("acme.orders", 0)), 30000).build(),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis.handleElectLeaders(req)

    // Controller must NEVER see the foreign-tenant topic.
    verify(controller, never()).electLeaders(
      any(classOf[ControllerRequestContext]),
      any(classOf[ElectLeadersRequestData]))

    val response = captureSentResponse(req).asInstanceOf[ElectLeadersResponse]
    val results = response.data().replicaElectionResults().asScala.toList
    assertEquals(1, results.size)
    assertEquals("acme.orders", results.head.topic())
    val pr = results.head.partitionResult().asScala.head
    assertEquals(0, pr.partitionId())
    assertEquals(INVALID_TOPIC_EXCEPTION.code(), pr.errorCode(),
      "cluster-wide caller must be refused on a tenant-prefixed topic")
    assertNotNull(pr.errorMessage())
    assertTrue(pr.errorMessage().contains("acme.orders"),
      "topic name is public via Metadata; echo it back to the caller")
  }

  @Test
  def testControllerElectLeadersAllowsSameTenantTopicFromTenantCaller(): Unit = {
    val controller = mock(classOf[Controller])
    val controllerResponse = new ElectLeadersResponseData()
    controllerResponse.replicaElectionResults().add(new ElectLeadersResponseData.ReplicaElectionResult()
      .setTopic("acme.orders")
      .setPartitionResult(util.Arrays.asList(
        new ElectLeadersResponseData.PartitionResult().setPartitionId(0).setErrorCode(NONE.code))))
    when(controller.electLeaders(
      any(classOf[ControllerRequestContext]),
      any(classOf[ElectLeadersRequestData])))
      .thenReturn(CompletableFuture.completedFuture(controllerResponse))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    val req = buildTokenRequest(
      new ElectLeadersRequest.Builder(ElectionType.PREFERRED,
        util.Arrays.asList(new org.apache.kafka.common.TopicPartition("acme.orders", 0)), 30000).build(),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice"))

    controllerApis.handleElectLeaders(req)

    val forwarded: ArgumentCaptor[ElectLeadersRequestData] =
      ArgumentCaptor.forClass(classOf[ElectLeadersRequestData])
    verify(controller).electLeaders(
      any(classOf[ControllerRequestContext]),
      forwarded.capture())
    val forwardedTopics = forwarded.getValue.topicPartitions().asScala.map(_.topic()).toList
    assertEquals(List("acme.orders"), forwardedTopics,
      "tenant principal's own-namespace topic must reach the controller")

    val response = captureSentResponse(req).asInstanceOf[ElectLeadersResponse]
    val pr = response.data().replicaElectionResults().asScala.head.partitionResult().asScala.head
    assertEquals(NONE.code(), pr.errorCode(),
      "forwarded tenant principal may elect leaders for topics in its own namespace")
  }

  @Test
  def testControllerElectLeadersMixedBatchSplitsRefusedAndAccepted(): Unit = {
    val controller = mock(classOf[Controller])
    val controllerResponse = new ElectLeadersResponseData()
    controllerResponse.replicaElectionResults().add(new ElectLeadersResponseData.ReplicaElectionResult()
      .setTopic("cluster-metrics")
      .setPartitionResult(util.Arrays.asList(
        new ElectLeadersResponseData.PartitionResult().setPartitionId(0).setErrorCode(NONE.code))))
    when(controller.electLeaders(
      any(classOf[ControllerRequestContext]),
      any(classOf[ElectLeadersRequestData])))
      .thenReturn(CompletableFuture.completedFuture(controllerResponse))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    val req = buildTokenRequest(
      new ElectLeadersRequest.Builder(ElectionType.PREFERRED,
        util.Arrays.asList(
          new org.apache.kafka.common.TopicPartition("acme.orders", 0),
          new org.apache.kafka.common.TopicPartition("cluster-metrics", 0)
        ), 30000).build(),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis.handleElectLeaders(req)

    // Only the cluster-scope topic reaches the controller.
    val forwarded: ArgumentCaptor[ElectLeadersRequestData] =
      ArgumentCaptor.forClass(classOf[ElectLeadersRequestData])
    verify(controller).electLeaders(
      any(classOf[ControllerRequestContext]),
      forwarded.capture())
    assertEquals(List("cluster-metrics"),
      forwarded.getValue.topicPartitions().asScala.map(_.topic()).toList,
      "only the cluster-scope topic must reach the controller")

    val response = captureSentResponse(req).asInstanceOf[ElectLeadersResponse]
    val byName: Map[String, Short] = response.data().replicaElectionResults().asScala
      .map(t => t.topic() -> t.partitionResult().asScala.head.errorCode()).toMap
    assertEquals(INVALID_TOPIC_EXCEPTION.code(), byName("acme.orders"),
      "tenant TOPIC must be refused with INVALID_TOPIC_EXCEPTION")
    assertEquals(NONE.code(), byName("cluster-metrics"),
      "legitimate cluster topic must round-trip with NONE")
  }

  @Test
  def testControllerElectLeadersRefusesNullSweepFromTenantCaller(): Unit = {
    // A forwarded tenant principal trying to elect leaders for ALL topics is
    // a cluster-wide operation a tenant has no business performing. The
    // controller must short-circuit with CLUSTER_AUTHORIZATION_FAILED — never
    // even ENUMERATE the topic universe on behalf of a tenant.
    val controller = mock(classOf[Controller])
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    val req = buildTokenRequest(
      new ElectLeadersRequest.Builder(ElectionType.PREFERRED, null, 30000).build(),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice"))

    controllerApis.handleElectLeaders(req)

    verify(controller, never()).electLeaders(
      any(classOf[ControllerRequestContext]),
      any(classOf[ElectLeadersRequestData]))

    val response = captureSentResponse(req).asInstanceOf[ElectLeadersResponse]
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code(), response.data().errorCode(),
      "a tenant principal must never null-sweep the whole cluster")
  }

  @Test
  def testControllerElectLeadersNullSweepStripsTenantTopicsForClusterCaller(): Unit = {
    // Defence-in-depth: the cluster-wide caller's null-sweep is forwarded
    // (the admin has ALTER on CLUSTER and may legitimately want this) but the
    // controller would otherwise enumerate every tenant topic name back in
    // the response — useful as a probe for an admin with no Metadata reach.
    // The handler must strip tenant-namespaced entries from the response.
    val controller = mock(classOf[Controller])
    val controllerResponse = new ElectLeadersResponseData()
    controllerResponse.replicaElectionResults().add(new ElectLeadersResponseData.ReplicaElectionResult()
      .setTopic("cluster-metrics")
      .setPartitionResult(util.Arrays.asList(
        new ElectLeadersResponseData.PartitionResult().setPartitionId(0).setErrorCode(NONE.code))))
    controllerResponse.replicaElectionResults().add(new ElectLeadersResponseData.ReplicaElectionResult()
      .setTopic("acme.orders")
      .setPartitionResult(util.Arrays.asList(
        new ElectLeadersResponseData.PartitionResult().setPartitionId(0).setErrorCode(NONE.code))))
    when(controller.electLeaders(
      any(classOf[ControllerRequestContext]),
      any(classOf[ElectLeadersRequestData])))
      .thenReturn(CompletableFuture.completedFuture(controllerResponse))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    val req = buildTokenRequest(
      new ElectLeadersRequest.Builder(ElectionType.PREFERRED, null, 30000).build(),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis.handleElectLeaders(req)

    // The forwarded request to the controller stays null-sweep.
    val forwarded: ArgumentCaptor[ElectLeadersRequestData] =
      ArgumentCaptor.forClass(classOf[ElectLeadersRequestData])
    verify(controller).electLeaders(
      any(classOf[ControllerRequestContext]),
      forwarded.capture())
    assertNull(forwarded.getValue.topicPartitions(),
      "cluster-wide null-sweep is forwarded to the controller as null")

    val response = captureSentResponse(req).asInstanceOf[ElectLeadersResponse]
    val topics = response.data().replicaElectionResults().asScala.map(_.topic()).toSet
    assertEquals(Set("cluster-metrics"), topics,
      "tenant-namespaced topic names must be stripped from the null-sweep response")
  }

  // ---------------------------------------------------------------------------
  // F5: ListPartitionReassignments — outside-in TOPIC scrub on the controller
  // listener PLUS response-side enumeration defence. A direct `bootstrap.
  // controllers` AdminClient (KIP-590) reaches handleListPartitionReassignments
  // without traversing KafkaApis, so the broker-side filter in KafkaApis does
  // not run. Two attack shapes:
  //
  //   1. List-all mode (`topics == null`): the controller would otherwise
  //      enumerate every in-progress reassignment cluster-wide — a tenant-
  //      topic-presence oracle plus a partition-count leak. The handler must
  //      strip tenant-namespaced topics from the response (cluster-wide caller)
  //      or return only own-tenant topics rewritten to their LOGICAL form
  //      (tenant caller).
  //
  //   2. Per-topic mode (`topics != null`): a cluster-wide or cross-tenant
  //      caller could name a foreign tenant topic explicitly. The response
  //      schema has no per-entry error slot, so the handler refuses the entire
  //      request with top-level INVALID_TOPIC_EXCEPTION — matching
  //      F4/AlterPartitionReassignments in spirit (explicit, not silent).
  //
  // Same-tenant carve-out via `callerTenantFromPrincipal`: a forwarded
  // `__tenant_acme.alice` may name `orders` (LOGICAL) per-topic; the handler
  // translates to `acme.orders` before forwarding and back on the response.
  // ---------------------------------------------------------------------------

  @Test
  def testControllerListPartitionReassignmentsClusterWideListAllStripsTenantNamespaced(): Unit = {
    // Cluster-wide caller, list-all sweep. The controller would happily return
    // every in-progress reassignment — including tenant-prefixed topics —
    // turning the response into a tenant-topic-presence oracle. The handler
    // must filter out any topic whose leading segment is structurally a valid
    // tenant id, keeping only genuine cluster topics.
    val controller = mock(classOf[Controller])
    val controllerResponse = new ListPartitionReassignmentsResponseData()
    controllerResponse.topics().add(new ListPartitionReassignmentsResponseData.OngoingTopicReassignment()
      .setName("cluster-metrics")
      .setPartitions(util.Arrays.asList(
        new ListPartitionReassignmentsResponseData.OngoingPartitionReassignment()
          .setPartitionIndex(0)
          .setReplicas(util.Arrays.asList(java.lang.Integer.valueOf(1), java.lang.Integer.valueOf(2)))
          .setAddingReplicas(util.Arrays.asList(java.lang.Integer.valueOf(2)))
          .setRemovingReplicas(util.Collections.emptyList[java.lang.Integer]))))
    controllerResponse.topics().add(new ListPartitionReassignmentsResponseData.OngoingTopicReassignment()
      .setName("acme.orders")
      .setPartitions(util.Arrays.asList(
        new ListPartitionReassignmentsResponseData.OngoingPartitionReassignment()
          .setPartitionIndex(0)
          .setReplicas(util.Arrays.asList(java.lang.Integer.valueOf(3), java.lang.Integer.valueOf(4)))
          .setAddingReplicas(util.Arrays.asList(java.lang.Integer.valueOf(4)))
          .setRemovingReplicas(util.Collections.emptyList[java.lang.Integer]))))
    controllerResponse.topics().add(new ListPartitionReassignmentsResponseData.OngoingTopicReassignment()
      .setName("beta.events")
      .setPartitions(util.Arrays.asList(
        new ListPartitionReassignmentsResponseData.OngoingPartitionReassignment()
          .setPartitionIndex(0)
          .setReplicas(util.Arrays.asList(java.lang.Integer.valueOf(5)))
          .setAddingReplicas(util.Collections.emptyList[java.lang.Integer])
          .setRemovingReplicas(util.Collections.emptyList[java.lang.Integer]))))
    when(controller.listPartitionReassignments(
      any(classOf[ControllerRequestContext]),
      any(classOf[ListPartitionReassignmentsRequestData])))
      .thenReturn(CompletableFuture.completedFuture(controllerResponse))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    // topics() is null on a freshly-constructed RequestData — that's the list-all sweep.
    val data = new ListPartitionReassignmentsRequestData()
    assertNull(data.topics(), "list-all mode requires topics == null on the wire")
    val req = buildTokenRequest(
      new ListPartitionReassignmentsRequest.Builder(data).build(),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis.handleListPartitionReassignments(req)

    // The forwarded request stays list-all (null topics).
    val forwarded: ArgumentCaptor[ListPartitionReassignmentsRequestData] =
      ArgumentCaptor.forClass(classOf[ListPartitionReassignmentsRequestData])
    verify(controller).listPartitionReassignments(
      any(classOf[ControllerRequestContext]),
      forwarded.capture())
    assertNull(forwarded.getValue.topics(),
      "cluster-wide list-all sweep is forwarded to the controller as null")

    val response = captureSentResponse(req).asInstanceOf[ListPartitionReassignmentsResponse]
    assertEquals(NONE.code(), response.data().errorCode())
    val topicNames = response.data().topics().asScala.map(_.name()).toSet
    assertEquals(Set("cluster-metrics"), topicNames,
      "tenant-namespaced topic names must be stripped from the list-all response")
  }

  @Test
  def testControllerListPartitionReassignmentsTenantListAllReturnsOnlyOwnNamespaceAsLogical(): Unit = {
    // Forwarded tenant principal, list-all sweep. The controller returns the
    // physical names it stores (`acme.orders`, `beta.events`, `cluster-metrics`)
    // — the handler must keep only entries in this tenant's `<id>.` namespace
    // AND rewrite them back to their logical form so the tenant only ever sees
    // the names it would have submitted itself.
    val controller = mock(classOf[Controller])
    val controllerResponse = new ListPartitionReassignmentsResponseData()
    controllerResponse.topics().add(new ListPartitionReassignmentsResponseData.OngoingTopicReassignment()
      .setName("acme.orders")
      .setPartitions(util.Arrays.asList(
        new ListPartitionReassignmentsResponseData.OngoingPartitionReassignment()
          .setPartitionIndex(0)
          .setReplicas(util.Arrays.asList(java.lang.Integer.valueOf(1), java.lang.Integer.valueOf(2)))
          .setAddingReplicas(util.Arrays.asList(java.lang.Integer.valueOf(2)))
          .setRemovingReplicas(util.Collections.emptyList[java.lang.Integer]))))
    controllerResponse.topics().add(new ListPartitionReassignmentsResponseData.OngoingTopicReassignment()
      .setName("beta.events")
      .setPartitions(util.Arrays.asList(
        new ListPartitionReassignmentsResponseData.OngoingPartitionReassignment()
          .setPartitionIndex(0)
          .setReplicas(util.Arrays.asList(java.lang.Integer.valueOf(3)))
          .setAddingReplicas(util.Collections.emptyList[java.lang.Integer])
          .setRemovingReplicas(util.Collections.emptyList[java.lang.Integer]))))
    controllerResponse.topics().add(new ListPartitionReassignmentsResponseData.OngoingTopicReassignment()
      .setName("cluster-metrics")
      .setPartitions(util.Arrays.asList(
        new ListPartitionReassignmentsResponseData.OngoingPartitionReassignment()
          .setPartitionIndex(0)
          .setReplicas(util.Arrays.asList(java.lang.Integer.valueOf(4)))
          .setAddingReplicas(util.Collections.emptyList[java.lang.Integer])
          .setRemovingReplicas(util.Collections.emptyList[java.lang.Integer]))))
    when(controller.listPartitionReassignments(
      any(classOf[ControllerRequestContext]),
      any(classOf[ListPartitionReassignmentsRequestData])))
      .thenReturn(CompletableFuture.completedFuture(controllerResponse))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    val data = new ListPartitionReassignmentsRequestData()
    val req = buildTokenRequest(
      new ListPartitionReassignmentsRequest.Builder(data).build(),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice"))

    controllerApis.handleListPartitionReassignments(req)

    val response = captureSentResponse(req).asInstanceOf[ListPartitionReassignmentsResponse]
    assertEquals(NONE.code(), response.data().errorCode())
    val topicNames = response.data().topics().asScala.map(_.name()).toSet
    assertEquals(Set("orders"), topicNames,
      "tenant caller must see only own-namespace topics, rewritten to LOGICAL names")
  }

  @Test
  def testControllerListPartitionReassignmentsClusterWidePerTopicForeignRefused(): Unit = {
    // Cluster-wide caller naming a mixed batch (one cluster topic + one foreign
    // tenant topic) per-topic. The response schema has NO per-entry error slot
    // (only top-level ErrorCode/ErrorMessage on OngoingTopicReassignment is
    // absent), so partial refusal is not expressible. The handler refuses the
    // entire request with top-level INVALID_TOPIC_EXCEPTION and the controller
    // is never called.
    val controller = mock(classOf[Controller])
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    val data = new ListPartitionReassignmentsRequestData()
    // `topics` is `nullableVersions: "0+", default: null` on the wire, so the
    // generated POJO returns null unless we materialise the list explicitly.
    data.setTopics(new util.ArrayList[ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics]())
    data.topics().add(new ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics()
      .setName("cluster-metrics")
      .setPartitionIndexes(util.Arrays.asList(java.lang.Integer.valueOf(0))))
    data.topics().add(new ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics()
      .setName("acme.orders")
      .setPartitionIndexes(util.Arrays.asList(java.lang.Integer.valueOf(0))))
    val req = buildTokenRequest(
      new ListPartitionReassignmentsRequest.Builder(data).build(),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "admin"))

    controllerApis.handleListPartitionReassignments(req)

    // Controller must NEVER see the foreign-tenant topic. Whole request refused.
    verify(controller, never()).listPartitionReassignments(
      any(classOf[ControllerRequestContext]),
      any(classOf[ListPartitionReassignmentsRequestData]))

    val response = captureSentResponse(req).asInstanceOf[ListPartitionReassignmentsResponse]
    assertEquals(INVALID_TOPIC_EXCEPTION.code(), response.data().errorCode(),
      "any foreign-tenant entry must refuse the entire request (no per-entry error slot)")
    assertNotNull(response.data().errorMessage())
    assertTrue(response.data().errorMessage().contains("acme.orders"),
      "topic name is public via Metadata; echo it back to the caller")
    assertTrue(response.data().topics() == null || response.data().topics().isEmpty,
      "refused request returns no per-topic entries")
  }

  @Test
  def testControllerListPartitionReassignmentsTenantPerTopicTranslatesLogicalToPhysical(): Unit = {
    // Forwarded tenant principal names `orders` per-topic (the LOGICAL form
    // it would have submitted on the tenant listener). The handler must
    // translate to `acme.orders` BEFORE forwarding (the controller only
    // knows physical names) and translate the controller's `acme.orders`
    // response back to `orders` so the tenant never sees its own prefix.
    val controller = mock(classOf[Controller])
    val controllerResponse = new ListPartitionReassignmentsResponseData()
    controllerResponse.topics().add(new ListPartitionReassignmentsResponseData.OngoingTopicReassignment()
      .setName("acme.orders")
      .setPartitions(util.Arrays.asList(
        new ListPartitionReassignmentsResponseData.OngoingPartitionReassignment()
          .setPartitionIndex(0)
          .setReplicas(util.Arrays.asList(java.lang.Integer.valueOf(1), java.lang.Integer.valueOf(2)))
          .setAddingReplicas(util.Arrays.asList(java.lang.Integer.valueOf(2)))
          .setRemovingReplicas(util.Collections.emptyList[java.lang.Integer]))))
    when(controller.listPartitionReassignments(
      any(classOf[ControllerRequestContext]),
      any(classOf[ListPartitionReassignmentsRequestData])))
      .thenReturn(CompletableFuture.completedFuture(controllerResponse))
    controllerApis = createControllerApis(
      authorizer = None,
      controller = controller,
      tenantConfig = org.apache.kafka.server.tenant.TenantConfig.empty())

    val data = new ListPartitionReassignmentsRequestData()
    data.setTopics(new util.ArrayList[ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics]())
    data.topics().add(new ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics()
      .setName("orders")
      .setPartitionIndexes(util.Arrays.asList(java.lang.Integer.valueOf(0))))
    val req = buildTokenRequest(
      new ListPartitionReassignmentsRequest.Builder(data).build(),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "__tenant_acme.alice"))

    controllerApis.handleListPartitionReassignments(req)

    // Forwarded request must carry the PHYSICAL form so the controller's
    // physical-name store actually matches.
    val forwarded: ArgumentCaptor[ListPartitionReassignmentsRequestData] =
      ArgumentCaptor.forClass(classOf[ListPartitionReassignmentsRequestData])
    verify(controller).listPartitionReassignments(
      any(classOf[ControllerRequestContext]),
      forwarded.capture())
    assertEquals(util.Arrays.asList("acme.orders"),
      forwarded.getValue.topics().asScala.map(_.name()).asJava,
      "tenant per-topic logical name must be rewritten to physical before forwarding")

    val response = captureSentResponse(req).asInstanceOf[ListPartitionReassignmentsResponse]
    assertEquals(NONE.code(), response.data().errorCode())
    val topicNames = response.data().topics().asScala.map(_.name()).toSet
    assertEquals(Set("orders"), topicNames,
      "response must echo the tenant's LOGICAL name, never the physical form")
  }

  // ---------------------------------------------------------------------------
  // bootstrap.controllers ACL scrub (#122)
  //
  // CreateAcls / DeleteAcls / DescribeAcls all declare `listeners=[broker,
  // controller]` so an AdminClient configured with `bootstrap.controllers`
  // reaches them directly on the controller, bypassing the broker-side scrubs
  // (#88, #98, #115). Without the controller-side guard a cluster-wide caller
  // could:
  //   - CREATE_ACLS: mint a literal ACL keyed by a tenant resource name
  //     (Topic:acme.orders) or a tenant principal (User:__tenant_acme.bob),
  //   - DELETE_ACLS: yank every ACL of a tenant via an explicit filter, or
  //     enumerate tenant ACLs from MatchingAcls in a wildcard filter,
  //   - DESCRIBE_ACLS: probe tenant ownership / enumerate tenant principals.
  //
  // Same-tenant carve-out: a forwarded `__tenant_<id>.<u>` principal IS
  // allowed to operate on bindings inside its own namespace — the broker has
  // already validated the listener/principal binding (#86, #110), so the
  // forwarded principal is canonical.
  // ---------------------------------------------------------------------------

  private def aclCreation(resourceType: ResourceType,
                          resourceName: String,
                          principal: String): CreateAclsRequestData.AclCreation =
    aclCreation(resourceType, resourceName, PatternType.LITERAL, principal)

  private def aclCreation(resourceType: ResourceType,
                          resourceName: String,
                          patternType: PatternType,
                          principal: String): CreateAclsRequestData.AclCreation =
    new CreateAclsRequestData.AclCreation()
      .setResourceType(resourceType.code)
      .setResourceName(resourceName)
      .setResourcePatternType(patternType.code)
      .setPrincipal(principal)
      .setHost("*")
      .setOperation(AclOperation.READ.code)
      .setPermissionType(AclPermissionType.ALLOW.code)

  private def aclFilter(resourceType: ResourceType,
                        resourceNameFilter: String,
                        principalFilter: String): DeleteAclsRequestData.DeleteAclsFilter =
    aclFilter(resourceType, resourceNameFilter, PatternType.LITERAL, principalFilter)

  private def aclFilter(resourceType: ResourceType,
                        resourceNameFilter: String,
                        patternType: PatternType,
                        principalFilter: String): DeleteAclsRequestData.DeleteAclsFilter =
    new DeleteAclsRequestData.DeleteAclsFilter()
      .setResourceTypeFilter(resourceType.code)
      .setResourceNameFilter(resourceNameFilter)
      .setPatternTypeFilter(patternType.code)
      .setPrincipalFilter(principalFilter)
      .setHostFilter(null)
      .setOperation(AclOperation.READ.code)
      .setPermissionType(AclPermissionType.ALLOW.code)

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

  // The ACL handler short-circuits with SECURITY_DISABLED if no Authorizer is
  // configured — the scrub never runs. Tests need a real Authorizer instance
  // that ALLOWS cluster operations (so the scrub is the only gate left) and
  // exposes stubbable createAcls/deleteAcls/acls hooks.
  private def authorizerAllowingClusterOps(): Authorizer = {
    val auth: Authorizer = mock(classOf[Authorizer])
    when(auth.authorize(any[AuthorizableRequestContext], any[util.List[Action]]()))
      .thenAnswer(inv => {
        val actions = inv.getArgument[util.List[Action]](1)
        val out = new util.ArrayList[AuthorizationResult](actions.size)
        actions.forEach(_ => out.add(AuthorizationResult.ALLOWED))
        out
      })
    auth
  }

  // Build a CONTROLLER-listener request whose caller is anonymous. This
  // models the cluster-acting admin reaching the controller directly via
  // bootstrap.controllers — the precise threat model the scrub closes
  // (no broker has rewritten the request, so the wire form is verbatim).
  private def buildControllerRequest(request: AbstractRequest): RequestChannel.Request = {
    val buffer = request.serializeWithHeader(new RequestHeader(request.apiKey, request.version, clientID, 0))
    val header = RequestHeader.parse(buffer)
    val context = new RequestContext(header, "1", InetAddress.getLocalHost, KafkaPrincipal.ANONYMOUS,
      ListenerName.normalised("CONTROLLER"),
      SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false)
    new RequestChannel.Request(processor = 1, context = context, startTimeNanos = 0,
      MemoryPool.NONE, buffer, requestChannelMetrics)
  }

  @Test
  def testControllerCreateAclsRefusesTenantPrefixedTopicOnBootstrapControllers(): Unit = {
    // Cluster-acting admin via bootstrap.controllers writes a literal ACL
    // keyed by `Topic:acme.orders`. Without the controller-side scrub the
    // metadata log would record the binding and tenant acme would later see
    // bob hold READ on its `orders` topic.
    val creation = aclCreation(ResourceType.TOPIC, "acme.orders", "User:bob")
    val req = new CreateAclsRequest.Builder(new CreateAclsRequestData()
      .setCreations(util.Arrays.asList(creation))).build()
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleCreateAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[CreateAclsResponse]
    assertEquals(1, response.results.size)
    assertEquals(Errors.INVALID_REQUEST.code, response.results.get(0).errorCode,
      "controller-direct CreateAcls must refuse tenant-namespaced resource name")
    verify(auth, never()).createAcls(any(), any())
  }

  @Test
  def testControllerCreateAclsRefusesTenantPrefixedGroupOnBootstrapControllers(): Unit = {
    // Same shape, GROUP resource. The group id is always the principal-prefix
    // form `__tenant_<id>.<group>`, so the scrub must accept literal
    // `__tenant_acme.app1` as a reserved name and refuse if the caller does
    // not own it.
    val creation = aclCreation(ResourceType.GROUP, "__tenant_acme.app1", "User:bob")
    val req = new CreateAclsRequest.Builder(new CreateAclsRequestData()
      .setCreations(util.Arrays.asList(creation))).build()
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleCreateAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[CreateAclsResponse]
    assertEquals(Errors.INVALID_REQUEST.code, response.results.get(0).errorCode,
      "controller-direct CreateAcls must refuse `__tenant_acme.*` GROUP name from a cluster caller")
    verify(auth, never()).createAcls(any(), any())
  }

  @Test
  def testControllerCreateAclsRefusesTenantPrefixedPrincipalOnBootstrapControllers(): Unit = {
    // The principal field of the AclEntry — laundering authority into a
    // tenant principal's namespace. `User:__tenant_acme.bob` must be refused.
    val creation = aclCreation(ResourceType.TOPIC, "plain-topic", "User:__tenant_acme.bob")
    val req = new CreateAclsRequest.Builder(new CreateAclsRequestData()
      .setCreations(util.Arrays.asList(creation))).build()
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleCreateAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[CreateAclsResponse]
    assertEquals(Errors.INVALID_REQUEST.code, response.results.get(0).errorCode,
      "controller-direct CreateAcls must refuse `User:__tenant_acme.*` from a non-owning caller")
    verify(auth, never()).createAcls(any(), any())
  }

  @Test
  def testControllerCreateAclsRefusesGroupTypedTenantPrincipalOnBootstrapControllers(): Unit = {
    // #169: the principal-literal scrub must refuse ANY <type>:__tenant_*
    // form, not just `User:`. A forged ACL with principal field
    // `Group:__tenant_acme.bob` would otherwise sail through L1 — and if a
    // future custom principal builder or authorizer emits a Group-typed
    // tenant principal (which `parseTenantId` now refuses post-#168), the
    // dead ACL would still pollute the metadata log. Refuse at L1.
    val creation = aclCreation(ResourceType.TOPIC, "plain-topic", "Group:__tenant_acme.bob")
    val req = new CreateAclsRequest.Builder(new CreateAclsRequestData()
      .setCreations(util.Arrays.asList(creation))).build()
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleCreateAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[CreateAclsResponse]
    assertEquals(Errors.INVALID_REQUEST.code, response.results.get(0).errorCode,
      "controller-direct CreateAcls must refuse `Group:__tenant_acme.*` from a cluster caller")
    verify(auth, never()).createAcls(any(), any())
  }

  @Test
  def testControllerCreateAclsRefusesLowercaseUserTypedTenantPrincipalOnBootstrapControllers(): Unit = {
    // #169: a lowercase `user:` type prefix on the ACL principal field would
    // otherwise slip through the (case-sensitive) startsWith("User:") check.
    // The widened scrub keys on the first colon, not the literal "User:".
    val creation = aclCreation(ResourceType.TOPIC, "plain-topic", "user:__tenant_acme.bob")
    val req = new CreateAclsRequest.Builder(new CreateAclsRequestData()
      .setCreations(util.Arrays.asList(creation))).build()
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleCreateAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[CreateAclsResponse]
    assertEquals(Errors.INVALID_REQUEST.code, response.results.get(0).errorCode,
      "controller-direct CreateAcls must refuse `user:__tenant_acme.*` (lowercase type) from a cluster caller")
    verify(auth, never()).createAcls(any(), any())
  }

  @Test
  def testControllerCreateAclsMixesAllowedAndRejectedOnBootstrapControllers(): Unit = {
    // Mixed batch: a tenant-polluting binding sits next to a neutral one.
    // The scrub must refuse the tenant binding per-entry and forward the
    // neutral one to the Authorizer; the response must be positional.
    val creations = util.Arrays.asList(
      aclCreation(ResourceType.TOPIC, "acme.orders", "User:bob"),  // 0: refuse
      aclCreation(ResourceType.TOPIC, "plain-a", "User:bob"))      // 1: allow
    val req = new CreateAclsRequest.Builder(new CreateAclsRequestData()
      .setCreations(creations)).build()
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    // Authorizer accepts the one neutral binding it sees and returns NONE.
    val createdFuture = new CompletableFuture[AclCreateResult]()
    createdFuture.complete(AclCreateResult.SUCCESS)
    when(auth.createAcls(any[AuthorizableRequestContext](), any[util.List[AclBinding]]()))
      .thenAnswer(inv => {
        val bindings = inv.getArgument[util.List[AclBinding]](1)
        val out = new util.ArrayList[CompletableFuture[AclCreateResult]](bindings.size)
        bindings.forEach(_ => out.add(createdFuture))
        out
      })
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleCreateAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[CreateAclsResponse]
    val codes = response.results.asScala.map(_.errorCode).toList
    assertEquals(List(Errors.INVALID_REQUEST.code, Errors.NONE.code), codes,
      "tenant entry must be refused at position 0; neutral entry forwarded at position 1")

    // The Authorizer must have seen ONLY the neutral binding.
    val bindingCaptor: ArgumentCaptor[util.List[AclBinding]] =
      ArgumentCaptor.forClass(classOf[util.List[AclBinding]])
    verify(auth).createAcls(any[AuthorizableRequestContext](), bindingCaptor.capture())
    val passed = bindingCaptor.getValue.asScala.map(_.pattern.name).toList
    assertEquals(List("plain-a"), passed,
      "scrub must short-circuit before the polluting binding reaches the Authorizer")
  }

  @Test
  def testControllerDeleteAclsRefusesExplicitTenantTopicFilter(): Unit = {
    // Explicit-name filter `Topic:acme.orders` deletes every ACL of tenant
    // acme that names this resource. Refuse before the Authorizer is asked.
    val filter = aclFilter(ResourceType.TOPIC, "acme.orders", null)
    val req = new DeleteAclsRequest.Builder(new DeleteAclsRequestData()
      .setFilters(util.Arrays.asList(filter))).build()
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleDeleteAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[DeleteAclsResponse]
    assertEquals(1, response.filterResults.size)
    assertEquals(Errors.INVALID_REQUEST.code, response.filterResults.get(0).errorCode,
      "controller-direct DeleteAcls must refuse an explicit tenant topic filter")
    verify(auth, never()).deleteAcls(any(), any())
  }

  @Test
  def testControllerDeleteAclsRefusesExplicitTenantPrincipalFilter(): Unit = {
    val filter = aclFilter(ResourceType.TOPIC, null, "User:__tenant_acme.alice")
    val req = new DeleteAclsRequest.Builder(new DeleteAclsRequestData()
      .setFilters(util.Arrays.asList(filter))).build()
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleDeleteAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[DeleteAclsResponse]
    assertEquals(Errors.INVALID_REQUEST.code, response.filterResults.get(0).errorCode,
      "controller-direct DeleteAcls must refuse an explicit tenant principal filter")
    verify(auth, never()).deleteAcls(any(), any())
  }

  @Test
  def testControllerDeleteAclsScrubsMatchingAclsForWildcardFilter(): Unit = {
    // Wildcard filter is legitimate cluster-admin reach — let it through, but
    // scrub tenant-named entries out of the MatchingAcls echo so the response
    // does not enumerate tenant ACLs for a non-owning caller.
    val filter = aclFilter(ResourceType.TOPIC, null, null)
    val req = new DeleteAclsRequest.Builder(new DeleteAclsRequestData()
      .setFilters(util.Arrays.asList(filter))).build()
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    // Stub the Authorizer to return a mix of tenant and neutral bindings.
    val deletedFuture = new CompletableFuture[AclDeleteResult]()
    deletedFuture.complete(new AclDeleteResult(util.Arrays.asList(
      new AclDeleteResult.AclBindingDeleteResult(
        aclBinding(ResourceType.TOPIC, "acme.orders", PatternType.LITERAL, "User:bob")),
      new AclDeleteResult.AclBindingDeleteResult(
        aclBinding(ResourceType.TOPIC, "plain-topic", PatternType.LITERAL, "User:__tenant_acme.alice")),
      new AclDeleteResult.AclBindingDeleteResult(
        aclBinding(ResourceType.TOPIC, "plain-topic", PatternType.LITERAL, "User:bob")))))
    when(auth.deleteAcls(any[AuthorizableRequestContext](), any[util.List[AclBindingFilter]]()))
      .thenAnswer(_ => {
        val out = new util.ArrayList[CompletableFuture[AclDeleteResult]](1)
        out.add(deletedFuture)
        out
      })
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleDeleteAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[DeleteAclsResponse]
    val matching = response.filterResults.get(0).matchingAcls.asScala.toList
    assertEquals(1, matching.size,
      "tenant-named resource and tenant-principal entries must be scrubbed from MatchingAcls")
    assertEquals("plain-topic", matching.head.resourceName)
    assertEquals("User:bob", matching.head.principal)
  }

  @Test
  def testControllerDescribeAclsRefusesTenantTopicLiteralFilter(): Unit = {
    // L1 refusal: a LITERAL filter `Topic:acme.orders` from a cluster caller
    // probes whether tenant `acme` owns that topic. The controller path now
    // mirrors the broker (#162) by refusing the filter outright with
    // INVALID_REQUEST — `auth.acls(...)` must NEVER be called, because the
    // mere act of querying with a tenant-shaped filter is itself the probe
    // we are closing. (Before #162 the response was scrubbed AFTER the
    // Authorizer call, leaving a timing/error-shape side channel against
    // bootstrap.controllers that differed from the broker listener.)
    val req = describeAclsRequest(ResourceType.TOPIC, "acme.orders", PatternType.LITERAL, null)
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleDescribeAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[DescribeAclsResponse]
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, response.data.errorCode,
      "controller-direct DescribeAcls L1 must mirror the broker's CLUSTER_AUTHORIZATION_FAILED " +
        "code so bootstrap.brokers and bootstrap.controllers cannot be fingerprinted by error-code probes (#162)")
    assertEquals(0, response.acls.size,
      "L1 refusal short-circuits before any binding is enumerated")
    verify(auth, never()).acls(any[AclBindingFilter]())
  }

  @Test
  def testControllerDescribeAclsScrubsTenantBindingsFromWildcard(): Unit = {
    // Wildcard query: the response must carry only non-tenant bindings.
    val req = describeAclsRequest(ResourceType.ANY, null, PatternType.ANY, null)
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    when(auth.acls(any[AclBindingFilter]()))
      .thenReturn(util.Arrays.asList(
        aclBinding(ResourceType.TOPIC, "acme.orders", PatternType.LITERAL, "User:bob"),
        aclBinding(ResourceType.TOPIC, "plain-topic", PatternType.LITERAL, "User:bob"),
        aclBinding(ResourceType.GROUP, "__tenant_acme.cg-1", PatternType.LITERAL, "User:bob"),
        aclBinding(ResourceType.TOPIC, "neutral-2", PatternType.LITERAL, "User:__tenant_acme.alice")))
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleDescribeAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[DescribeAclsResponse]
    val surviving = response.acls.asScala.flatMap { res =>
      res.acls.asScala.map(a => (res.resourceType, res.resourceName, a.principal))
    }.toSet
    assertEquals(Set((ResourceType.TOPIC.code, "plain-topic", "User:bob")), surviving,
      "tenant-named TOPIC, tenant-shaped GROUP, and tenant principal echo must all be scrubbed")
  }

  // ---------------------------------------------------------------------------
  // #157 PREFIXED bypass — ControllerApis path mirror.
  //
  // StandardAuthorizer literal-startsWith semantics: a PREFIXED binding
  // `Topic:PREFIXED:acme` matches every literal topic name beginning with
  // "acme", which includes the entire tenant `acme.*` namespace. The legacy
  // LITERAL-only structural guard on the controller listener never inspected
  // PREFIXED, so any cluster-acting admin reaching bootstrap.controllers could
  // plant a binding that covers a tenant namespace under names like
  // `Topic:PREFIXED:acme`, `Topic:PREFIXED:ac`, `Topic:PREFIXED:""`, or
  // `Group:PREFIXED:__tenant_*`. The new dispatcher must refuse all of these
  // structurally — `tenantConfig.allTenants` is empty in split-mode KRaft
  // controller, so the refusal cannot rely on a known-tenant list.
  // ---------------------------------------------------------------------------

  @Test
  def testControllerCreateAclsRefusesPrefixedDotlessTenantIdTopicOnBootstrapControllers(): Unit = {
    // `Topic:PREFIXED:acme` — dotless first segment that, on extension to
    // `acme.<topic>`, would belong to tenant acme. Refuse structurally.
    val creation = aclCreation(ResourceType.TOPIC, "acme", PatternType.PREFIXED, "User:bob")
    val req = new CreateAclsRequest.Builder(new CreateAclsRequestData()
      .setCreations(util.Arrays.asList(creation))).build()
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleCreateAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[CreateAclsResponse]
    assertEquals(Errors.INVALID_REQUEST.code, response.results.get(0).errorCode,
      "controller-direct CreateAcls must refuse PREFIXED:acme — startsWith-matches the entire acme.* namespace")
    verify(auth, never()).createAcls(any(), any())
  }

  @Test
  def testControllerCreateAclsRefusesPrefixedShortPrefixOfTenantIdTopicOnBootstrapControllers(): Unit = {
    // `Topic:PREFIXED:ac` — strict prefix of `acme`. StandardAuthorizer
    // startsWith would still match `acme.*`, so the structural refusal must
    // treat any dotless first segment as polluting (it could extend into any
    // tenant id).
    val creation = aclCreation(ResourceType.TOPIC, "ac", PatternType.PREFIXED, "User:bob")
    val req = new CreateAclsRequest.Builder(new CreateAclsRequestData()
      .setCreations(util.Arrays.asList(creation))).build()
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleCreateAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[CreateAclsResponse]
    assertEquals(Errors.INVALID_REQUEST.code, response.results.get(0).errorCode,
      "controller-direct CreateAcls must refuse PREFIXED:ac — extends to acme.* and any other tenant id beginning with `ac`")
    verify(auth, never()).createAcls(any(), any())
  }

  @Test
  def testControllerCreateAclsRefusesPrefixedEmptyNameTopicOnBootstrapControllers(): Unit = {
    // `Topic:PREFIXED:""` — universal startsWith match. Refuse on the
    // controller listener too: the broker-side guard would refuse it, and the
    // controller-direct path must not leave a hole.
    val creation = aclCreation(ResourceType.TOPIC, "", PatternType.PREFIXED, "User:bob")
    val req = new CreateAclsRequest.Builder(new CreateAclsRequestData()
      .setCreations(util.Arrays.asList(creation))).build()
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleCreateAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[CreateAclsResponse]
    assertEquals(Errors.INVALID_REQUEST.code, response.results.get(0).errorCode,
      "controller-direct CreateAcls must refuse PREFIXED:\"\" — empty prefix matches every literal name")
    verify(auth, never()).createAcls(any(), any())
  }

  @Test
  def testControllerCreateAclsAllowsPrefixedUnderscoreTopicOnBootstrapControllers(): Unit = {
    // Positive control: `Topic:PREFIXED:_` is the Connect / `_confluent-*`
    // operator carveout. The structural guard must not refuse the entire `_`
    // namespace just because PREFIXED is now policed.
    val creation = aclCreation(ResourceType.TOPIC, "_", PatternType.PREFIXED, "User:bob")
    val req = new CreateAclsRequest.Builder(new CreateAclsRequestData()
      .setCreations(util.Arrays.asList(creation))).build()
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    val createdFuture = new CompletableFuture[AclCreateResult]()
    createdFuture.complete(AclCreateResult.SUCCESS)
    when(auth.createAcls(any[AuthorizableRequestContext](), any[util.List[AclBinding]]()))
      .thenAnswer(inv => {
        val bindings = inv.getArgument[util.List[AclBinding]](1)
        val out = new util.ArrayList[CompletableFuture[AclCreateResult]](bindings.size)
        bindings.forEach(_ => out.add(createdFuture))
        out
      })
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleCreateAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[CreateAclsResponse]
    assertEquals(Errors.NONE.code, response.results.get(0).errorCode,
      "PREFIXED:_ is the operator carveout and must remain forwarded to the Authorizer")
    verify(auth).createAcls(any(), any())
  }

  @Test
  def testControllerCreateAclsRefusesPrefixedPrincipalNamespaceSentinelOnBootstrapControllers(): Unit = {
    // `Group:PREFIXED:__tenant_` — the sentinel prefix itself, which under
    // startsWith matches every tenant-encoded group id. Refuse structurally.
    val creation = aclCreation(ResourceType.GROUP, "__tenant_", PatternType.PREFIXED, "User:bob")
    val req = new CreateAclsRequest.Builder(new CreateAclsRequestData()
      .setCreations(util.Arrays.asList(creation))).build()
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleCreateAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[CreateAclsResponse]
    assertEquals(Errors.INVALID_REQUEST.code, response.results.get(0).errorCode,
      "controller-direct CreateAcls must refuse PREFIXED:__tenant_ — startsWith-matches every tenant group id")
    verify(auth, never()).createAcls(any(), any())
  }

  @Test
  def testControllerCreateAclsRefusesPrefixedPrincipalAnchorWithoutTrailingDotOnBootstrapControllers(): Unit = {
    // `Group:PREFIXED:__tenant_acme` (no trailing dot) extends to
    // `__tenant_acmex.<user>` and friends — foreign tenant ids that begin with
    // the bound tenant's id. Refuse structurally so the dot-anchor invariant
    // is not bypassed by omitting the dot.
    val creation = aclCreation(ResourceType.GROUP, "__tenant_acme", PatternType.PREFIXED, "User:bob")
    val req = new CreateAclsRequest.Builder(new CreateAclsRequestData()
      .setCreations(util.Arrays.asList(creation))).build()
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleCreateAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[CreateAclsResponse]
    assertEquals(Errors.INVALID_REQUEST.code, response.results.get(0).errorCode,
      "controller-direct CreateAcls must refuse PREFIXED:__tenant_acme — anchor without trailing dot reaches foreign tenants")
    verify(auth, never()).createAcls(any(), any())
  }

  @Test
  def testControllerDeleteAclsRefusesPrefixedDotlessTenantIdFilterOnBootstrapControllers(): Unit = {
    // DeleteAcls L1 dispatch must refuse PREFIXED:acme too — otherwise a
    // cluster-acting caller could enumerate every tenant binding under acme.
    val filter = aclFilter(ResourceType.TOPIC, "acme", PatternType.PREFIXED, null)
    val req = new DeleteAclsRequest.Builder(new DeleteAclsRequestData()
      .setFilters(util.Arrays.asList(filter))).build()
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleDeleteAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[DeleteAclsResponse]
    assertEquals(Errors.INVALID_REQUEST.code, response.filterResults.get(0).errorCode,
      "controller-direct DeleteAcls must refuse PREFIXED:acme — startsWith-matches the entire acme.* namespace")
    verify(auth, never()).deleteAcls(any(), any())
  }

  @Test
  def testControllerDescribeAclsScrubsPrefixedTenantBindingsFromWildcard(): Unit = {
    // DescribeAcls on the controller listener post-filters bindings via the
    // pattern-aware `aclTenantBindingAllowed` callback. A PREFIXED binding
    // planted with a dotless `Topic:PREFIXED:acme` name (the #157 bypass shape)
    // would otherwise be echoed verbatim to the cluster-direct caller because
    // its literal name does not lexically equal `acme.`. The new
    // pattern-aware helpers must refuse that binding too.
    val req = describeAclsRequest(ResourceType.ANY, null, PatternType.ANY, null)
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    when(auth.acls(any[AclBindingFilter]()))
      .thenReturn(util.Arrays.asList(
        aclBinding(ResourceType.TOPIC, "acme", PatternType.PREFIXED, "User:bob"),
        aclBinding(ResourceType.GROUP, "__tenant_", PatternType.PREFIXED, "User:bob"),
        aclBinding(ResourceType.GROUP, "__tenant_acme", PatternType.PREFIXED, "User:bob"),
        aclBinding(ResourceType.TOPIC, "plain-topic", PatternType.LITERAL, "User:bob")))
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleDescribeAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[DescribeAclsResponse]
    val surviving = response.acls.asScala.flatMap { res =>
      res.acls.asScala.map(a => (res.resourceType, res.resourceName, a.principal))
    }.toSet
    assertEquals(Set((ResourceType.TOPIC.code, "plain-topic", "User:bob")), surviving,
      "PREFIXED bypass bindings (Topic:PREFIXED:acme, Group:PREFIXED:__tenant_, Group:PREFIXED:__tenant_acme) must be scrubbed")
  }

  // ---------------------------------------------------------------------------
  // #162 / #163 / #164 / #165 follow-ups to #157 — DescribeAcls L1 mirror,
  // fail-closed UNKNOWN PatternType, MATCH PatternType coverage, isolated
  // entryFilter principal coverage.
  // ---------------------------------------------------------------------------

  // #162: controller DescribeAcls L1 must refuse a PREFIXED tenant-shaped filter,
  // mirroring the broker — otherwise a caller probing bootstrap.controllers can
  // get a different response surface than bootstrap.brokers for the same
  // attempted enumeration.
  @Test
  def testControllerDescribeAclsRefusesPrefixedDotlessTenantIdTopicFilterOnBootstrapControllers(): Unit = {
    val req = describeAclsRequest(ResourceType.TOPIC, "acme", PatternType.PREFIXED, null)
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleDescribeAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[DescribeAclsResponse]
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, response.data.errorCode,
      "controller-direct DescribeAcls must refuse PREFIXED:acme — startsWith-matches the entire acme.* namespace")
    assertEquals(0, response.acls.size)
    verify(auth, never()).acls(any[AclBindingFilter]())
  }

  // #164: MATCH PatternType — `Topic:MATCH:acme.orders` is a filter pattern
  // type that StandardAuthorizer matches against literal AND prefix bindings.
  // The L1 refusal must catch it on every entry point (Create/Delete/Describe).
  @Test
  def testControllerDescribeAclsRefusesMatchTenantTopicFilter(): Unit = {
    val req = describeAclsRequest(ResourceType.TOPIC, "acme.orders", PatternType.MATCH, null)
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleDescribeAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[DescribeAclsResponse]
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, response.data.errorCode,
      "MATCH on a tenant topic name must be refused — it would enumerate any LITERAL or PREFIXED binding under that name")
    verify(auth, never()).acls(any[AclBindingFilter]())
  }

  // #163: an UNKNOWN PatternType (invalid wire byte or future enum value) must
  // be refused on every tenant-shaped name. Before the fail-closed change in
  // #163 the `case _ => false` branch let any non-{LITERAL,PREFIXED,MATCH,ANY}
  // wire byte through — and a downstream Authorizer might still interpret it.
  // We use a raw byte 99 to simulate "future protocol revision" rather than
  // sending a documented enum value.
  @Test
  def testControllerDescribeAclsRefusesUnknownPatternTypeFilterFailsClosed(): Unit = {
    // Bypass DescribeAclsRequest.Builder: an AclBindingFilter collapses
    // PatternType.fromCode(99) into the UNKNOWN enum, then `.code()` emits
    // byte 0, which the wire validator catches. A raw-socket adversary can
    // deliver byte 99 directly — fromCode(99) returns UNKNOWN at the helper
    // but the wire validation (compares against UNKNOWN.code = 0) lets it through.
    // We mirror that adversary by serialising the data with byte 99 and reparsing.
    val data = new org.apache.kafka.common.message.DescribeAclsRequestData()
      .setResourceTypeFilter(ResourceType.TOPIC.code)
      .setResourceNameFilter("acme.orders")
      .setPatternTypeFilter(99.toByte)
      .setPrincipalFilter(null)
      .setHostFilter(null)
      .setOperation(AclOperation.ANY.code)
      .setPermissionType(AclPermissionType.ANY.code)
    val version = ApiKeys.DESCRIBE_ACLS.latestVersion
    val req = DescribeAclsRequest.parse(MessageUtil.toByteBuffer(data, version), version)
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleDescribeAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[DescribeAclsResponse]
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, response.data.errorCode,
      "UNKNOWN PatternType on a tenant name must fail-closed at L1 — refuses if EITHER LITERAL or PREFIXED interpretation would name a foreign tenant")
    verify(auth, never()).acls(any[AclBindingFilter]())
  }

  // #163: same fail-closed semantics on CreateAcls.
  @Test
  def testControllerCreateAclsRefusesUnknownPatternTypeFailsClosed(): Unit = {
    val creation = new CreateAclsRequestData.AclCreation()
      .setResourceType(ResourceType.TOPIC.code)
      .setResourceName("acme.orders")
      .setResourcePatternType(99.toByte)  // raw wire byte — PatternType.fromCode → UNKNOWN
      .setPrincipal("User:bob")
      .setHost("*")
      .setOperation(AclOperation.READ.code)
      .setPermissionType(AclPermissionType.ALLOW.code)
    val req = new CreateAclsRequest.Builder(new CreateAclsRequestData()
      .setCreations(util.Arrays.asList(creation))).build()
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleCreateAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[CreateAclsResponse]
    assertEquals(Errors.INVALID_REQUEST.code, response.results.get(0).errorCode,
      "UNKNOWN PatternType on a tenant resource name must fail-closed on the controller create path")
    verify(auth, never()).createAcls(any(), any())
  }

  // #163: same fail-closed semantics on DeleteAcls.
  @Test
  def testControllerDeleteAclsRefusesUnknownPatternTypeFilterFailsClosed(): Unit = {
    val filter = new DeleteAclsRequestData.DeleteAclsFilter()
      .setResourceTypeFilter(ResourceType.TOPIC.code)
      .setResourceNameFilter("acme.orders")
      .setPatternTypeFilter(99.toByte)  // raw wire byte — PatternType.fromCode → UNKNOWN
      .setPrincipalFilter(null)
      .setHostFilter(null)
      .setOperation(AclOperation.READ.code)
      .setPermissionType(AclPermissionType.ALLOW.code)
    val req = new DeleteAclsRequest.Builder(new DeleteAclsRequestData()
      .setFilters(util.Arrays.asList(filter))).build()
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleDeleteAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[DeleteAclsResponse]
    assertEquals(Errors.INVALID_REQUEST.code, response.filterResults.get(0).errorCode,
      "UNKNOWN PatternType on a tenant-named delete filter must fail-closed at L1")
    verify(auth, never()).deleteAcls(any(), any())
  }

  // #164: MATCH on CreateAcls — even though MATCH bindings are nonsensical on
  // creation, the broker/controller surface accepts the wire shape so the
  // refusal must be structural.
  @Test
  def testControllerCreateAclsRefusesMatchTenantTopicOnBootstrapControllers(): Unit = {
    val creation = aclCreation(ResourceType.TOPIC, "acme.orders", PatternType.MATCH, "User:bob")
    val req = new CreateAclsRequest.Builder(new CreateAclsRequestData()
      .setCreations(util.Arrays.asList(creation))).build()
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleCreateAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[CreateAclsResponse]
    assertEquals(Errors.INVALID_REQUEST.code, response.results.get(0).errorCode,
      "MATCH on a tenant resource name must be refused on the create path too")
    verify(auth, never()).createAcls(any(), any())
  }

  // #164: MATCH on DeleteAcls.
  @Test
  def testControllerDeleteAclsRefusesMatchTenantTopicFilter(): Unit = {
    val filter = aclFilter(ResourceType.TOPIC, "acme.orders", PatternType.MATCH, null)
    val req = new DeleteAclsRequest.Builder(new DeleteAclsRequestData()
      .setFilters(util.Arrays.asList(filter))).build()
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleDeleteAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[DeleteAclsResponse]
    assertEquals(Errors.INVALID_REQUEST.code, response.filterResults.get(0).errorCode,
      "MATCH on a tenant-named delete filter must be refused")
    verify(auth, never()).deleteAcls(any(), any())
  }

  // #165: isolated coverage for the entryFilter principal channel — a filter
  // with a NULL resource name and a tenant-shaped `User:__tenant_acme.*`
  // principal must be refused on every L1 path. Existing #157 tests pair this
  // with a tenant-shaped resource name, so the principal-channel refusal is
  // not independently witnessed.
  @Test
  def testControllerDescribeAclsRefusesEntryFilterTenantPrincipalOnly(): Unit = {
    // Resource pattern is wildcard (ANY + null) — only the entryFilter
    // principal names a foreign tenant.
    val req = describeAclsRequest(ResourceType.ANY, null, PatternType.ANY,
      "User:__tenant_acme.alice")
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleDescribeAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[DescribeAclsResponse]
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, response.data.errorCode,
      "DescribeAcls L1 must refuse a wildcard-resource filter whose entryFilter principal names a tenant")
    verify(auth, never()).acls(any[AclBindingFilter]())
  }

  // #165: same on DeleteAcls — the principal channel must independently refuse.
  @Test
  def testControllerDeleteAclsRefusesEntryFilterTenantPrincipalOnly(): Unit = {
    val filter = new DeleteAclsRequestData.DeleteAclsFilter()
      .setResourceTypeFilter(ResourceType.ANY.code)
      .setResourceNameFilter(null)
      .setPatternTypeFilter(PatternType.ANY.code)
      .setPrincipalFilter("User:__tenant_acme.alice")
      .setHostFilter(null)
      .setOperation(AclOperation.ANY.code)
      .setPermissionType(AclPermissionType.ANY.code)
    val req = new DeleteAclsRequest.Builder(new DeleteAclsRequestData()
      .setFilters(util.Arrays.asList(filter))).build()
    val request = buildControllerRequest(req)

    val auth = authorizerAllowingClusterOps()
    controllerApis = createControllerApis(
      authorizer = Some(auth),
      controller = new MockController.Builder().build(),
      tenantConfig = tenantConfigBinding("acme", "TENANT_ACME"))
    controllerApis.handleDeleteAclsRequest(request).get()

    val response = captureSentResponse(request).asInstanceOf[DeleteAclsResponse]
    assertEquals(Errors.INVALID_REQUEST.code, response.filterResults.get(0).errorCode,
      "DeleteAcls L1 must refuse a wildcard-resource filter whose principal names a tenant")
    verify(auth, never()).deleteAcls(any(), any())
  }

  @Test
  def testDescribeUserScramCredentialsNotDispatchedOnControllerListener(): Unit = {
    // Anchors the fail-closed dispatch invariant for DESCRIBE_USER_SCRAM_CREDENTIALS.
    // The schema (clients/.../DescribeUserScramCredentialsRequest.json) declares
    // `"listeners": ["broker", "controller"]`, so the wire framework accepts the
    // request on the controller listener — but `ControllerApis.handle` has no
    // `case ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS`, so the request lands on the
    // fall-through `case _` and is refused with an error response.
    //
    // That fall-through is the load-bearing fact: if a future contributor wires
    // a controller-side handler (mirroring `handleAlterUserScramCredentials`),
    // they MUST also wire the tenant scrub that `KafkaApis` applies on the
    // broker side (closed by #115), otherwise the controller path silently
    // enumerates every tenant SCRAM user to any cluster-wide caller. This test
    // turns red the moment a case is added, forcing the contributor to read
    // #115 / #122 and add the scrub before the test is updated.
    val controller = mock(classOf[Controller])
    val request = buildRequest(new DescribeUserScramCredentialsRequest.Builder(
      new DescribeUserScramCredentialsRequestData()).build())
    controllerApis = createControllerApis(None, controller)

    controllerApis.handle(request, RequestLocal.noCaching)

    verifyNoInteractions(controller)
    val response = captureSentResponse(request).asInstanceOf[DescribeUserScramCredentialsResponse]
    // The unmapped-ApiKey path throws `new ApiException("Unsupported ApiKey ...")`; ApiError
    // deliberately scrubs the message for UNKNOWN_SERVER_ERROR (to avoid leaking internals),
    // so the message itself is null on the wire. The errorCode is the load-bearing assertion:
    // it pins the fall-through path. The day someone wires a real handler, the code will
    // change and turn this test red — the comment then leads them to #115 / #109 / #122.
    assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, response.data.errorCode,
      "ControllerApis.handle must hit the fail-closed `case _` path for DESCRIBE_USER_SCRAM_CREDENTIALS; "
        + "any other code means a dispatch case was added — before re-tightening this assertion, "
        + "wire the same tenant scrub KafkaApis applies on the broker side (closed by #115).")
  }

  @Test
  def testDescribeDelegationTokenNotDispatchedOnControllerListener(): Unit = {
    // Companion to testDescribeUserScramCredentialsNotDispatchedOnControllerListener.
    // DescribeDelegationTokenRequest.json also declares `"listeners": ["broker", "controller"]`
    // and ControllerApis.handle has no case — the request falls through to the
    // fail-closed `case _` and the request channel returns an error response.
    //
    // Closes the regression path that #109 cleaned up on the broker side: the
    // broker handler now scrubs raw HMACs for cross-tenant callers. A future
    // contributor adding a controller-side case must read #109 / #122 and wire
    // the same scrub before this test passes again.
    val controller = mock(classOf[Controller])
    val request = buildRequest(new DescribeDelegationTokenRequest(
      new DescribeDelegationTokenRequestData(), ApiKeys.DESCRIBE_DELEGATION_TOKEN.latestVersion))
    controllerApis = createControllerApis(None, controller)

    controllerApis.handle(request, RequestLocal.noCaching)

    verifyNoInteractions(controller)
    val response = captureSentResponse(request).asInstanceOf[DescribeDelegationTokenResponse]
    // See sibling test for the rationale on UNKNOWN_SERVER_ERROR — same fall-through.
    assertEquals(Errors.UNKNOWN_SERVER_ERROR, response.error,
      "ControllerApis.handle must hit the fail-closed `case _` path for DESCRIBE_DELEGATION_TOKEN; "
        + "any other code means a dispatch case was added — before re-tightening this assertion, "
        + "wire the same tenant scrub KafkaApis applies on the broker side (closed by #109).")
  }

  @AfterEach
  def tearDown(): Unit = {
    quotasNeverThrottleControllerMutations.shutdown()
    quotasAlwaysThrottleControllerMutations.shutdown()
    if (controllerApis != null && !controllerApis.isClosed)
      controllerApis.close()
  }
}
