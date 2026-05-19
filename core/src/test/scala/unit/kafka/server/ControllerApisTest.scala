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
import org.apache.kafka.common.message.CreateTopicsRequestData.{CreatableTopic, CreatableTopicCollection, CreatableTopicConfig, CreatableTopicConfigCollection}
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
import org.apache.kafka.server.views.ViewTopicConfig
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
                                   metadataCacheOverride: Option[KRaftMetadataCache] = None): ControllerApis = {
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
      metadataCacheOverride.getOrElse(metadataCache)
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
      _ => Set("baz"),
      _ => Set.empty[String]).get().topics().asScala.toSet)
  }

  private def viewConfigs(backing: String, predicate: String = "true"): CreatableTopicConfigCollection = {
    val configs = new CreatableTopicConfigCollection()
    configs.add(new CreatableTopicConfig().
      setName(ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG).
      setValue(backing))
    configs.add(new CreatableTopicConfig().
      setName(ViewTopicConfig.VIEW_CEL_PREDICATE_CONFIG).
      setValue(predicate))
    configs
  }

  /**
   * Round 23: CreateTopics must refuse to bind a view to a backing topic the requester cannot
   * READ. The fetch path treats a view as a transparent window onto its backing and intentionally
   * skips the per-fetch backing ACL check (the predicate is the access control). Without a
   * READ-on-backing gate at create time, any principal with CREATE on a namespace can publish a
   * view over an arbitrary topic (e.g. __consumer_offsets) and read it back — privilege
   * escalation. This test pins the closed-on-deny posture: even though "spy_view" itself is
   * CREATE-authorized, the request must fail because READ on the backing is not authorized.
   */
  @Test
  def testCreateTopicsWithUnauthorizedViewBackingFailsClosedToProtectAgainstPrivEscalation(): Unit = {
    val controller = new MockController.Builder().build()
    controllerApis = createControllerApis(None, controller)
    val request = new CreateTopicsRequestData().setTopics(new CreatableTopicCollection(
      util.Arrays.asList(
        new CreatableTopic().setName("spy_view").setNumPartitions(1).setReplicationFactor(1).
          setConfigs(viewConfigs("__consumer_offsets"))
      ).iterator()))
    val response = controllerApis.createTopics(ANONYMOUS_CONTEXT, request,
      hasClusterAuth = true,
      _ => Set("spy_view"),
      _ => Set("spy_view"),
      _ => Set.empty[String]).get()
    val result = response.topics().asScala.find(_.name() == "spy_view").get
    assertEquals(TOPIC_AUTHORIZATION_FAILED.code(), result.errorCode())
    assertEquals("Authorization failed.", result.errorMessage())
  }

  /**
   * Round 23: positive case. When the requester DOES have READ on the backing, view creation
   * proceeds. Pins that the new gate is not a blanket-block — only an unauthorized backing is
   * rejected. Without this case, a regression that always-denies view creation would still pass
   * the negative test above.
   */
  @Test
  def testCreateTopicsWithAuthorizedViewBackingSucceeds(): Unit = {
    val controller = new MockController.Builder().build()
    controllerApis = createControllerApis(None, controller)
    val request = new CreateTopicsRequestData().setTopics(new CreatableTopicCollection(
      util.Arrays.asList(
        new CreatableTopic().setName("alice_view").setNumPartitions(1).setReplicationFactor(1).
          setConfigs(viewConfigs("alice_backing"))
      ).iterator()))
    val response = controllerApis.createTopics(ANONYMOUS_CONTEXT, request,
      hasClusterAuth = true,
      _ => Set("alice_view"),
      _ => Set("alice_view"),
      _ => Set("alice_backing")).get()
    val result = response.topics().asScala.find(_.name() == "alice_view").get
    assertEquals(NONE.code(), result.errorCode())
  }

  /**
   * Round 23: IncrementalAlterConfigs must enforce the same READ-on-backing gate when the
   * `view.backing.topic` config is being SET on an existing topic. Without this check, a principal
   * with only ALTER_CONFIGS on a topic could rebind its backing to any topic in the cluster and
   * read it via fetch.
   */
  @Test
  def testIncrementalAlterConfigsToSetViewBackingRequiresReadOnBacking(): Unit = {
    val viewTopicName = "alice_view"
    val unauthorizedBacking = "__consumer_offsets"
    val requestData = new IncrementalAlterConfigsRequestData().setResources(
      new AlterConfigsResourceCollection(util.Arrays.asList(
        new AlterConfigsResource().
          setResourceName(viewTopicName).
          setResourceType(ConfigResource.Type.TOPIC.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName(ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG).
            setValue(unauthorizedBacking).
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator()))
        ).iterator()))
    val request = buildRequest(new IncrementalAlterConfigsRequest.Builder(requestData).build(0))

    val authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(
      any[AuthorizableRequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument[util.List[Action]](1).asScala
      val results = actions.map { action =>
        val op = action.operation()
        val resourceName = action.resourcePattern().name()
        // ALTER_CONFIGS on the view name is fine; READ on the backing is what we are gating.
        if (op == AclOperation.ALTER_CONFIGS && resourceName == viewTopicName) AuthorizationResult.ALLOWED
        else AuthorizationResult.DENIED
      }
      new util.ArrayList[AuthorizationResult](results.asJava)
    }
    controllerApis = createControllerApis(Some(authorizer), new MockController.Builder().build())
    controllerApis.handleIncrementalAlterConfigs(request)
    val capturedResponse: ArgumentCaptor[AbstractResponse] =
      ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None))
    val response = capturedResponse.getValue.asInstanceOf[IncrementalAlterConfigsResponse]
    val viewResponse = response.data().responses().asScala.find(_.resourceName() == viewTopicName).get
    assertEquals(TOPIC_AUTHORIZATION_FAILED.code(), viewResponse.errorCode())
    assertEquals("Authorization failed.", viewResponse.errorMessage())
  }

  /**
   * Round 24: the deprecated `ApiKeys.ALTER_CONFIGS` (full-replace) shares the round-23 threat
   * model with `INCREMENTAL_ALTER_CONFIGS` — a principal with ALTER_CONFIGS on a topic could send
   * a legacy AlterConfigs request setting `view.backing.topic=<sensitive>` and bypass the
   * incremental-API gate. This test pins that the legacy handler also rejects the request when
   * the requester lacks READ on the proposed backing.
   */
  @Test
  def testLegacyAlterConfigsToSetViewBackingRequiresReadOnBacking(): Unit = {
    val viewTopicName = "alice_view"
    val unauthorizedBacking = "__consumer_offsets"
    val requestData = new AlterConfigsRequestData().setResources(
      new OldAlterConfigsResourceCollection(util.Arrays.asList(
        new OldAlterConfigsResource().
          setResourceName(viewTopicName).
          setResourceType(ConfigResource.Type.TOPIC.id()).
          setConfigs(new OldAlterableConfigCollection(util.Arrays.asList(new OldAlterableConfig().
            setName(ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG).
            setValue(unauthorizedBacking)).iterator()))
        ).iterator()))
    val request = buildRequest(new AlterConfigsRequest(requestData, 0))

    val authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(
      any[AuthorizableRequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument[util.List[Action]](1).asScala
      val results = actions.map { action =>
        val op = action.operation()
        val resourceName = action.resourcePattern().name()
        // ALTER_CONFIGS on the view name is fine; READ on the backing is what we are gating.
        if (op == AclOperation.ALTER_CONFIGS && resourceName == viewTopicName) AuthorizationResult.ALLOWED
        else AuthorizationResult.DENIED
      }
      new util.ArrayList[AuthorizationResult](results.asJava)
    }
    controllerApis = createControllerApis(Some(authorizer), new MockController.Builder().build())
    controllerApis.handleLegacyAlterConfigs(request)
    val capturedResponse: ArgumentCaptor[AbstractResponse] =
      ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None))
    val response = capturedResponse.getValue.asInstanceOf[AlterConfigsResponse]
    val viewResponse = response.data().responses().asScala.find(_.resourceName() == viewTopicName).get
    assertEquals(TOPIC_AUTHORIZATION_FAILED.code(), viewResponse.errorCode())
    assertEquals("Authorization failed.", viewResponse.errorMessage())
  }

  /**
   * R38 (Codex): the round-23 IncrementalAlterConfigs gate only fired when `view.backing.topic`
   * was being SET. A principal with ALTER_CONFIGS on an existing view (and no READ on the
   * current backing) could send `SET view.cel.predicate = "true"` to relax the predicate to
   * admit everything, then fetch the view and read the full backing stream — the predicate IS
   * the security boundary at fetch time per the rationale in KafkaApis.handleFetchRequest. This
   * test pins that any view.* mutation on a current view requires READ on the current backing.
   * Covered keys: predicate (the exploit vector Codex demonstrated), offset.mode (defense in
   * depth; even if there is no current observable predicate-relaxation via offset.mode, gating
   * here keeps the rule uniform and survives future offset-mode semantics changes).
   */
  @Test
  def testIncrementalAlterConfigsOfViewPredicateOnExistingViewRequiresReadOnCurrentBacking(): Unit = {
    for (viewKey <- Seq(ViewTopicConfig.VIEW_CEL_PREDICATE_CONFIG,
                        ViewTopicConfig.VIEW_OFFSET_MODE_CONFIG)) {
      val viewTopicName = "alice_view"
      val currentBacking = "orders_private"
      // Seed metadataCache with the current view binding.
      val metadataCacheMock = mock(classOf[KRaftMetadataCache])
      val currentProps = new Properties()
      currentProps.put(ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG, currentBacking)
      currentProps.put(ViewTopicConfig.VIEW_CEL_PREDICATE_CONFIG, "body.region == 'EU'")
      when(metadataCacheMock.topicConfig(viewTopicName)).thenReturn(currentProps)

      val newValue = if (viewKey == ViewTopicConfig.VIEW_CEL_PREDICATE_CONFIG) "true"
                     else ViewTopicConfig.VIEW_OFFSET_MODE_SOURCE_SPARSE
      val requestData = new IncrementalAlterConfigsRequestData().setResources(
        new AlterConfigsResourceCollection(util.Arrays.asList(
          new AlterConfigsResource().
            setResourceName(viewTopicName).
            setResourceType(ConfigResource.Type.TOPIC.id()).
            setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
              setName(viewKey).
              setValue(newValue).
              setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator()))
          ).iterator()))
      val request = buildRequest(new IncrementalAlterConfigsRequest.Builder(requestData).build(0))

      val authorizer = mock(classOf[Authorizer])
      when(authorizer.authorize(
        any[AuthorizableRequestContext],
        any[util.List[Action]]
      )).thenAnswer { invocation =>
        val actions = invocation.getArgument[util.List[Action]](1).asScala
        val results = actions.map { action =>
          val op = action.operation()
          val resourceName = action.resourcePattern().name()
          if (op == AclOperation.ALTER_CONFIGS && resourceName == viewTopicName) AuthorizationResult.ALLOWED
          else AuthorizationResult.DENIED
        }
        new util.ArrayList[AuthorizationResult](results.asJava)
      }
      controllerApis = createControllerApis(Some(authorizer), new MockController.Builder().build(),
        metadataCacheOverride = Some(metadataCacheMock))
      controllerApis.handleIncrementalAlterConfigs(request)
      val capturedResponse: ArgumentCaptor[AbstractResponse] =
        ArgumentCaptor.forClass(classOf[AbstractResponse])
      verify(requestChannel, atLeastOnce()).sendResponse(
        ArgumentMatchers.eq(request),
        capturedResponse.capture(),
        ArgumentMatchers.eq(None))
      val response = capturedResponse.getValue.asInstanceOf[IncrementalAlterConfigsResponse]
      val viewResponse = response.data().responses().asScala.find(_.resourceName() == viewTopicName).get
      assertEquals(TOPIC_AUTHORIZATION_FAILED.code(), viewResponse.errorCode(),
        s"alter on view.* config $viewKey on existing view without READ on current backing must be rejected")
      assertEquals("Authorization failed.", viewResponse.errorMessage())
      reset(requestChannel)
    }
  }

  /**
   * R38 positive: same scenario, but the principal HAS READ on the current backing → the alter
   * succeeds. Pins that the new gate is not a blanket block.
   */
  @Test
  def testIncrementalAlterConfigsOfViewPredicateWithReadOnCurrentBackingSucceeds(): Unit = {
    val viewTopicName = "alice_view"
    val currentBacking = "orders_private"
    val metadataCacheMock = mock(classOf[KRaftMetadataCache])
    val currentProps = new Properties()
    currentProps.put(ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG, currentBacking)
    when(metadataCacheMock.topicConfig(viewTopicName)).thenReturn(currentProps)

    val requestData = new IncrementalAlterConfigsRequestData().setResources(
      new AlterConfigsResourceCollection(util.Arrays.asList(
        new AlterConfigsResource().
          setResourceName(viewTopicName).
          setResourceType(ConfigResource.Type.TOPIC.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName(ViewTopicConfig.VIEW_CEL_PREDICATE_CONFIG).
            setValue("body.region == 'US'").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator()))
        ).iterator()))
    val request = buildRequest(new IncrementalAlterConfigsRequest.Builder(requestData).build(0))

    val authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(
      any[AuthorizableRequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument[util.List[Action]](1).asScala
      val results = actions.map { action =>
        val op = action.operation()
        val resourceName = action.resourcePattern().name()
        if (op == AclOperation.ALTER_CONFIGS && resourceName == viewTopicName) AuthorizationResult.ALLOWED
        else if (op == AclOperation.READ && resourceName == currentBacking) AuthorizationResult.ALLOWED
        else AuthorizationResult.DENIED
      }
      new util.ArrayList[AuthorizationResult](results.asJava)
    }
    // R40b: MockController must be seeded with the same backing the broker preflight sees, so the
    // precondition map passed by ControllerApis matches the controller-side state.
    val mockController = new MockController.Builder().
      newInitialConfig(new ConfigResource(ConfigResource.Type.TOPIC, viewTopicName),
        ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG, currentBacking).
      build()
    controllerApis = createControllerApis(Some(authorizer), mockController,
      metadataCacheOverride = Some(metadataCacheMock))
    controllerApis.handleIncrementalAlterConfigs(request)
    val capturedResponse: ArgumentCaptor[AbstractResponse] =
      ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None))
    val response = capturedResponse.getValue.asInstanceOf[IncrementalAlterConfigsResponse]
    val viewResponse = response.data().responses().asScala.find(_.resourceName() == viewTopicName).get
    assertEquals(NONE.code(), viewResponse.errorCode(),
      "alter on view.cel.predicate WITH READ on current backing must succeed")
  }

  /**
   * R38: an ALTER_CONFIGS on a NON-VIEW topic that happens to set a non-view config (e.g.
   * cleanup.policy) must NOT be gated on any backing READ — the topic isn't a view, there is
   * no backing to gate on. Without this case a regression that blanket-rejects could pass the
   * negative test above.
   */
  @Test
  def testIncrementalAlterConfigsOnNonViewTopicIsNotGatedByViewBackingCheck(): Unit = {
    val regularTopicName = "regular_topic"
    val metadataCacheMock = mock(classOf[KRaftMetadataCache])
    when(metadataCacheMock.topicConfig(regularTopicName)).thenReturn(new Properties())

    val requestData = new IncrementalAlterConfigsRequestData().setResources(
      new AlterConfigsResourceCollection(util.Arrays.asList(
        new AlterConfigsResource().
          setResourceName(regularTopicName).
          setResourceType(ConfigResource.Type.TOPIC.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName(TopicConfig.CLEANUP_POLICY_CONFIG).
            setValue("delete").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator()))
        ).iterator()))
    val request = buildRequest(new IncrementalAlterConfigsRequest.Builder(requestData).build(0))

    val authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(
      any[AuthorizableRequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument[util.List[Action]](1).asScala
      val results = actions.map { action =>
        val op = action.operation()
        val resourceName = action.resourcePattern().name()
        // Only ALTER_CONFIGS on the regular topic is allowed.
        if (op == AclOperation.ALTER_CONFIGS && resourceName == regularTopicName) AuthorizationResult.ALLOWED
        else AuthorizationResult.DENIED
      }
      new util.ArrayList[AuthorizationResult](results.asJava)
    }
    controllerApis = createControllerApis(Some(authorizer), new MockController.Builder().build(),
      metadataCacheOverride = Some(metadataCacheMock))
    controllerApis.handleIncrementalAlterConfigs(request)
    val capturedResponse: ArgumentCaptor[AbstractResponse] =
      ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None))
    val response = capturedResponse.getValue.asInstanceOf[IncrementalAlterConfigsResponse]
    val viewResponse = response.data().responses().asScala.find(_.resourceName() == regularTopicName).get
    // Either the alter succeeds (NONE) or fails for an unrelated reason (e.g. MockController
    // semantics), but NOT TOPIC_AUTHORIZATION_FAILED — that would mean the view gate fired
    // incorrectly on a non-view topic.
    assertNotEquals(TOPIC_AUTHORIZATION_FAILED.code(), viewResponse.errorCode(),
      "alter on non-view topic must not be gated by the view-backing READ check")
  }

  /**
   * R38: the same predicate-only bypass exists in legacy AlterConfigs (full-replace). A principal
   * with ALTER_CONFIGS could submit a legacy AlterConfigs that includes view.cel.predicate=`true`
   * without including view.backing.topic — the round-24 gate at line 587 only fires when
   * view.backing.topic appears in the new (replacement) map. Pin that an existing-view legacy
   * AlterConfigs touching any view.* key also requires READ on the current backing.
   */
  @Test
  def testLegacyAlterConfigsOfViewPredicateOnExistingViewRequiresReadOnCurrentBacking(): Unit = {
    val viewTopicName = "alice_view"
    val currentBacking = "orders_private"
    val metadataCacheMock = mock(classOf[KRaftMetadataCache])
    val currentProps = new Properties()
    currentProps.put(ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG, currentBacking)
    when(metadataCacheMock.topicConfig(viewTopicName)).thenReturn(currentProps)

    val requestData = new AlterConfigsRequestData().setResources(
      new OldAlterConfigsResourceCollection(util.Arrays.asList(
        new OldAlterConfigsResource().
          setResourceName(viewTopicName).
          setResourceType(ConfigResource.Type.TOPIC.id()).
          setConfigs(new OldAlterableConfigCollection(util.Arrays.asList(new OldAlterableConfig().
            setName(ViewTopicConfig.VIEW_CEL_PREDICATE_CONFIG).
            setValue("true")).iterator()))
        ).iterator()))
    val request = buildRequest(new AlterConfigsRequest(requestData, 0))

    val authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(
      any[AuthorizableRequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument[util.List[Action]](1).asScala
      val results = actions.map { action =>
        val op = action.operation()
        val resourceName = action.resourcePattern().name()
        if (op == AclOperation.ALTER_CONFIGS && resourceName == viewTopicName) AuthorizationResult.ALLOWED
        else AuthorizationResult.DENIED
      }
      new util.ArrayList[AuthorizationResult](results.asJava)
    }
    controllerApis = createControllerApis(Some(authorizer), new MockController.Builder().build(),
      metadataCacheOverride = Some(metadataCacheMock))
    controllerApis.handleLegacyAlterConfigs(request)
    val capturedResponse: ArgumentCaptor[AbstractResponse] =
      ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None))
    val response = capturedResponse.getValue.asInstanceOf[AlterConfigsResponse]
    val viewResponse = response.data().responses().asScala.find(_.resourceName() == viewTopicName).get
    assertEquals(TOPIC_AUTHORIZATION_FAILED.code(), viewResponse.errorCode(),
      "legacy AlterConfigs touching view.* on existing view without READ on current backing must be rejected")
    assertEquals("Authorization failed.", viewResponse.errorMessage())
  }

  /**
   * R40b: even with READ on the CURRENT backing at preflight time, a predicate-only alter must
   * be rejected if the controller's authoritative current-backing value differs at apply time —
   * i.e. a concurrent rebind committed inside the controller event loop in the broker→controller
   * gap. The R38 broker-side gate uses {@code metadataCache.topicConfig}, which is a broker
   * snapshot; the controller is the source of truth. ControllerApis now passes the preflight-seen
   * backing as a precondition; the controller (here MockController seeded with the post-rebind
   * value) rejects with INVALID_REQUEST when the precondition does not match.
   */
  @Test
  def testIncrementalAlterConfigsOfViewPredicateRejectedWhenBackingRacedAtController(): Unit = {
    val viewTopicName = "alice_view"
    val backingAtBroker = "orders_v1"
    val backingAtController = "orders_v2" // race-committed by a different principal before our alter applied
    val metadataCacheMock = mock(classOf[KRaftMetadataCache])
    val currentProps = new Properties()
    currentProps.put(ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG, backingAtBroker)
    when(metadataCacheMock.topicConfig(viewTopicName)).thenReturn(currentProps)

    val requestData = new IncrementalAlterConfigsRequestData().setResources(
      new AlterConfigsResourceCollection(util.Arrays.asList(
        new AlterConfigsResource().
          setResourceName(viewTopicName).
          setResourceType(ConfigResource.Type.TOPIC.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName(ViewTopicConfig.VIEW_CEL_PREDICATE_CONFIG).
            setValue("true").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator()))
        ).iterator()))
    val request = buildRequest(new IncrementalAlterConfigsRequest.Builder(requestData).build(0))

    val authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(
      any[AuthorizableRequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument[util.List[Action]](1).asScala
      val results = actions.map { action =>
        val op = action.operation()
        val resourceName = action.resourcePattern().name()
        if (op == AclOperation.ALTER_CONFIGS && resourceName == viewTopicName) AuthorizationResult.ALLOWED
        // Principal has READ on the OLD backing (what the broker preflight sees), but the
        // controller has already moved to backingAtController, which is the test point.
        else if (op == AclOperation.READ && resourceName == backingAtBroker) AuthorizationResult.ALLOWED
        else AuthorizationResult.DENIED
      }
      new util.ArrayList[AuthorizationResult](results.asJava)
    }

    val mockController = new MockController.Builder().
      newInitialConfig(new ConfigResource(ConfigResource.Type.TOPIC, viewTopicName),
        ViewTopicConfig.VIEW_BACKING_TOPIC_CONFIG, backingAtController).
      build()
    controllerApis = createControllerApis(Some(authorizer), mockController,
      metadataCacheOverride = Some(metadataCacheMock))
    controllerApis.handleIncrementalAlterConfigs(request)

    val capturedResponse: ArgumentCaptor[AbstractResponse] =
      ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None))
    val response = capturedResponse.getValue.asInstanceOf[IncrementalAlterConfigsResponse]
    val viewResponse = response.data().responses().asScala.find(_.resourceName() == viewTopicName).get
    assertEquals(INVALID_REQUEST.code(), viewResponse.errorCode(),
      "TOCTOU rebind between broker preflight and controller apply must be caught by the precondition")
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

  @AfterEach
  def tearDown(): Unit = {
    quotasNeverThrottleControllerMutations.shutdown()
    quotasAlwaysThrottleControllerMutations.shutdown()
    if (controllerApis != null && !controllerApis.isClosed)
      controllerApis.close()
  }
}
