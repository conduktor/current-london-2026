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
import org.apache.kafka.common.acl.AclOperation
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
import org.apache.kafka.common.protocol.{ApiKeys, ApiMessage, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.{PatternType, Resource, ResourcePattern, ResourceType}
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
import org.apache.kafka.server.authorizer.{Action, AuthorizableRequestContext, AuthorizationResult, Authorizer}
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

  @AfterEach
  def tearDown(): Unit = {
    quotasNeverThrottleControllerMutations.shutdown()
    quotasAlwaysThrottleControllerMutations.shutdown()
    if (controllerApis != null && !controllerApis.isClosed)
      controllerApis.close()
  }
}
