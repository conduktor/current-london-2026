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
                                   throttle: Boolean = false): ControllerApis = {
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
      metadataCache
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

  /**
   * r15 BLOCKER N3 — Cross-broker TOCTOU on CreateTopics shadow.
   *
   * The controller is the single serialization point for topic creation. A broker that does
   * not hold the logical declaration for "orders" forwards a CreateTopics("orders") that
   * propagates back to a broker that DOES hold it; without controller-side enforcement, the
   * physical topic gets created and the broker-side shadow overlay narrows the window but
   * does not prevent the create. This test pins the controller-side rejection: authorized
   * names that collide with a declared logical topic short-circuit to TOPIC_ALREADY_EXISTS.
   */
  @Test
  def testCreateTopicsRejectsDeclaredLogicalShadow(): Unit = {
    val controller = new MockController.Builder().build()
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4")
    controllerApis = createControllerApis(None, controller, props)
    val request = new CreateTopicsRequestData().setTopics(new CreatableTopicCollection(
      util.Arrays.asList(
        new CreatableTopic().setName("orders").setNumPartitions(1).setReplicationFactor(3),
        new CreatableTopic().setName("safe").setNumPartitions(1).setReplicationFactor(3),
      ).iterator()))
    val expectedResponse = Set(
      new CreatableTopicResult().setName("orders").
        setErrorCode(TOPIC_ALREADY_EXISTS.code()).
        setErrorMessage("Topic name collides with a declared logical topic on this " +
          "controller; refusing to create a physical topic that would shadow it."),
      new CreatableTopicResult().setName("safe").
        setErrorCode(NONE.code()).
        setTopicId(new Uuid(0L, 1L)).
        setNumPartitions(1).
        setReplicationFactor(3).
        setTopicConfigErrorCode(NONE.code()))
    assertEquals(expectedResponse, controllerApis.createTopics(ANONYMOUS_CONTEXT, request,
      hasClusterAuth = true,
      _ => Set("orders", "safe"),
      _ => Set("orders", "safe")).get().topics().asScala.toSet)
  }

  /**
   * r15 BLOCKER N3 — auth precedence on the controller side.
   *
   * When a name is BOTH unauthorized AND a logical-shadow collider, the controller must
   * return TOPIC_AUTHORIZATION_FAILED, not TOPIC_ALREADY_EXISTS. The shadow verdict would
   * leak the existence of the declared logical topic to a principal with no DESCRIBE rights.
   * This mirrors the broker-side interceptor's precedence rule.
   */
  @Test
  def testCreateTopicsShadowDefersToAuthorizationFailure(): Unit = {
    val controller = new MockController.Builder().build()
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4,payments:50:shared:4")
    controllerApis = createControllerApis(None, controller, props)
    val request = new CreateTopicsRequestData().setTopics(new CreatableTopicCollection(
      util.Arrays.asList(
        new CreatableTopic().setName("orders").setNumPartitions(1).setReplicationFactor(3),
        new CreatableTopic().setName("payments").setNumPartitions(1).setReplicationFactor(3),
        new CreatableTopic().setName("neither").setNumPartitions(1).setReplicationFactor(3),
      ).iterator()))
    val expectedResponse = Set(
      new CreatableTopicResult().setName("orders").
        setErrorCode(TOPIC_ALREADY_EXISTS.code()).
        setErrorMessage("Topic name collides with a declared logical topic on this " +
          "controller; refusing to create a physical topic that would shadow it."),
      new CreatableTopicResult().setName("payments").
        setErrorCode(TOPIC_AUTHORIZATION_FAILED.code()).
        setErrorMessage("Authorization failed."),
      new CreatableTopicResult().setName("neither").
        setErrorCode(NONE.code()).
        setTopicId(new Uuid(0L, 1L)).
        setNumPartitions(1).
        setReplicationFactor(3).
        setTopicConfigErrorCode(NONE.code()))
    assertEquals(expectedResponse, controllerApis.createTopics(ANONYMOUS_CONTEXT, request,
      hasClusterAuth = false,
      _ => Set("orders", "neither"),
      _ => Set("orders", "neither")).get().topics().asScala.toSet)
  }

  /**
   * r16 MEDIUM N3-followup — shadow-rejected names must consume mutation quota.
   *
   * The controller's own createTopics path records the quota internally; we short-circuit
   * before reaching it for shadowed names, so without explicit accounting an authorized
   * client could probe the entire declared-logical-topic set at zero quota cost — a cheap
   * DoS / declaration-enumeration oracle. Verify that the quota recorder is invoked with
   * the sum of shadowed numPartitions.
   */
  @Test
  def testCreateTopicsShadowChargesMutationQuota(): Unit = {
    val controller = new MockController.Builder().build()
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4,payments:50:shared:4")
    controllerApis = createControllerApis(None, controller, props)
    val request = new CreateTopicsRequestData().setTopics(new CreatableTopicCollection(
      util.Arrays.asList(
        new CreatableTopic().setName("orders").setNumPartitions(7).setReplicationFactor(3),
        new CreatableTopic().setName("payments").setNumPartitions(3).setReplicationFactor(3),
        new CreatableTopic().setName("safe").setNumPartitions(1).setReplicationFactor(3),
      ).iterator()))
    val quotaCharges = new util.ArrayList[Integer]()
    val ctx = org.apache.kafka.controller.ControllerRequestContextUtil.anonymousContextFor(
      ApiKeys.CREATE_TOPICS,
      ApiKeys.CREATE_TOPICS.latestVersion(),
      new java.util.function.Consumer[Integer]() {
        override def accept(permits: Integer): Unit = quotaCharges.add(permits)
      })
    controllerApis.createTopics(ctx, request,
      hasClusterAuth = true,
      _ => Set("orders", "payments", "safe"),
      _ => Set("orders", "payments", "safe")).get()
    // Two shadow-rejected names: "orders" (7 partitions) + "payments" (3 partitions) = 10 permits.
    // We assert containment (not equality) because the MockController inside createTopics may
    // also invoke the recorder for the "safe" topic that it actually creates.
    assertTrue(quotaCharges.contains(10),
      s"expected shadow-rejection to charge 10 mutation-quota permits; got $quotaCharges")
  }

  /**
   * r17 BLOCKER — quota-throw must NOT propagate synchronously out of createTopics.
   *
   * Before the fix: context.applyPartitionChangeQuota for the shadow bulk charge threw
   * ThrottlingQuotaExceededException synchronously, escaping createTopics entirely. The
   * throw was caught by handle()'s outer catch, which routed to
   * CreateTopicsRequest.getErrorResponse(t) — which paints EVERY topic in the request
   * (including innocent authorized non-shadow names) with THROTTLING_QUOTA_EXCEEDED and
   * bypasses sendResponseMaybeThrottleWithControllerQuota entirely.
   *
   * The fix catches the exception inside createTopics, flips the per-shadow verdict to
   * THROTTLING_QUOTA_EXCEEDED, and lets the (non-shadow) tail proceed through
   * controller.createTopics. The discriminator below: an applier that throws ONLY on the
   * bulk shadow charge (>=2 permits) and succeeds on MockController's per-topic single-
   * permit charges — innocent topics therefore get NONE in the response, proving that
   * createTopics did NOT take the synchronous-throw path.
   */
  @Test
  def testCreateTopicsShadowQuotaThrowOnlyHitsShadowedNames(): Unit = {
    val controller = new MockController.Builder().build()
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4,payments:50:shared:4")
    controllerApis = createControllerApis(None, controller, props)
    val request = new CreateTopicsRequestData().setTopics(new CreatableTopicCollection(
      util.Arrays.asList(
        new CreatableTopic().setName("orders").setNumPartitions(1).setReplicationFactor(3),
        new CreatableTopic().setName("payments").setNumPartitions(1).setReplicationFactor(3),
        new CreatableTopic().setName("innocent1").setNumPartitions(1).setReplicationFactor(3),
        new CreatableTopic().setName("innocent2").setNumPartitions(1).setReplicationFactor(3),
      ).iterator()))
    val ctx = org.apache.kafka.controller.ControllerRequestContextUtil.anonymousContextFor(
      ApiKeys.CREATE_TOPICS,
      ApiKeys.CREATE_TOPICS.latestVersion(),
      new java.util.function.Consumer[Integer]() {
        override def accept(permits: Integer): Unit = {
          if (permits >= 2) throw new ThrottlingQuotaExceededException(0, "test bulk")
        }
      })
    val response = controllerApis.createTopics(ctx, request,
      hasClusterAuth = true,
      _ => Set("orders", "payments", "innocent1", "innocent2"),
      _ => Set("orders", "payments", "innocent1", "innocent2")).get()
    val byName = response.topics().asScala.map(r => r.name -> r.errorCode).toMap
    assertEquals(THROTTLING_QUOTA_EXCEEDED.code, byName("orders"),
      s"orders should be throttled; got ${byName.get("orders")}")
    assertEquals(THROTTLING_QUOTA_EXCEEDED.code, byName("payments"),
      s"payments should be throttled; got ${byName.get("payments")}")
    assertEquals(NONE.code, byName("innocent1"),
      s"innocent1 must NOT inherit the throttle verdict; got ${byName.get("innocent1")}")
    assertEquals(NONE.code, byName("innocent2"),
      s"innocent2 must NOT inherit the throttle verdict; got ${byName.get("innocent2")}")
  }

  /**
   * r17 HIGH — Int overflow on shadow-partition charge sum.
   *
   * Two shadowed topics with numPartitions = Int.MaxValue used to overflow .sum to a
   * negative value, which was then handed to applyPartitionChangeQuota and recorded
   * into the quota sensor as a negative permit (corrupting the per-(user,clientId) bucket).
   * The fix saturates with Long arithmetic and caps at Integer.MAX_VALUE.
   */
  @Test
  def testCreateTopicsShadowChargeSaturatesOnIntOverflow(): Unit = {
    val controller = new MockController.Builder().build()
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4,payments:50:shared:4")
    controllerApis = createControllerApis(None, controller, props)
    val request = new CreateTopicsRequestData().setTopics(new CreatableTopicCollection(
      util.Arrays.asList(
        new CreatableTopic().setName("orders").setNumPartitions(Int.MaxValue).setReplicationFactor(3),
        new CreatableTopic().setName("payments").setNumPartitions(Int.MaxValue).setReplicationFactor(3),
      ).iterator()))
    val quotaCharges = new util.ArrayList[Integer]()
    val ctx = org.apache.kafka.controller.ControllerRequestContextUtil.anonymousContextFor(
      ApiKeys.CREATE_TOPICS,
      ApiKeys.CREATE_TOPICS.latestVersion(),
      new java.util.function.Consumer[Integer]() {
        override def accept(permits: Integer): Unit = quotaCharges.add(permits)
      })
    controllerApis.createTopics(ctx, request,
      hasClusterAuth = true,
      _ => Set("orders", "payments"),
      _ => Set("orders", "payments")).get()
    assertTrue(quotaCharges.asScala.exists(p => p == Int.MaxValue),
      s"expected saturated charge of Int.MaxValue; got $quotaCharges")
    assertTrue(quotaCharges.asScala.forall(p => p >= 0),
      s"no charge should be negative; got $quotaCharges")
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

  /**
   * r17 BLOCKER #127 — DeleteTopics on a declared logical-only name returned
   * UNKNOWN_TOPIC_OR_PARTITION (via ReplicationControlManager.findTopicIds), breaking the
   * standard `listTopics → deleteTopics` round-trip — listTopics surfaces logical names
   * via the broker's DescribeTopicPartitions synthesis, then deleteTopics blows up.
   *
   * Symmetric to CreateTopics' shadow rejection (TOPIC_ALREADY_EXISTS with explanatory
   * message). The required behaviour is INVALID_REQUEST with the operator-facing
   * remediation. Innocent names in the same request must continue through to the
   * controller and surface their own (normal) errors.
   */
  @Test
  def testDeleteTopicsRejectsDeclaredLogicalName(): Unit = {
    val controller = new MockController.Builder().build()
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4,payments:50:shared:4")
    controllerApis = createControllerApis(None, controller, props)
    val request = new DeleteTopicsRequestData().setTopicNames(
      util.Arrays.asList("orders", "payments", "innocent"))
    val expectedResponse = Set(
      new DeletableTopicResult().setName("orders").
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Topic 'orders' is a declared logical topic in concentration.logical.topics on " +
          "this controller. Logical topics cannot be deleted via DeleteTopics; remove the " +
          "declaration from the controller's broker config and restart, then any physical " +
          "topic of the same name can be deleted via the normal path."),
      new DeletableTopicResult().setName("payments").
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Topic 'payments' is a declared logical topic in concentration.logical.topics on " +
          "this controller. Logical topics cannot be deleted via DeleteTopics; remove the " +
          "declaration from the controller's broker config and restart, then any physical " +
          "topic of the same name can be deleted via the normal path."),
      new DeletableTopicResult().setName("innocent").
        setErrorCode(UNKNOWN_TOPIC_OR_PARTITION.code()).
        setErrorMessage("This server does not host this topic-partition."))
    assertEquals(expectedResponse, controllerApis.deleteTopics(ANONYMOUS_CONTEXT, request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      hasClusterAuth = true,
      _ => Set.empty,
      _ => Set.empty).get().asScala.toSet)
  }

  /**
   * r17 ADV-A1 BLOCKER — DeleteTopics shadow guard must NOT run before authorization.
   *
   * Before the fix, the shadow rejection at the top of deleteTopics() leaked which names
   * were declared logical topics to ANY caller, regardless of authz: an unauthorized
   * client would see INVALID_REQUEST with the operator message for declared logical names,
   * and TOPIC_AUTHORIZATION_FAILED for everything else. That's an enumeration oracle on
   * the broker's logical-topic config — same auth-precedence flaw the #113 fix closed on
   * CreateTopics. The required behaviour: every name the principal cannot describe must
   * surface TOPIC_AUTHORIZATION_FAILED, identical to non-existent unauthorized names.
   */
  @Test
  def testDeleteTopicsLogicalRejectionDoesNotLeakToUnauthorized(): Unit = {
    val controller = new MockController.Builder().build()
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4,payments:50:shared:4")
    controllerApis = createControllerApis(None, controller, props)
    val request = new DeleteTopicsRequestData().setTopicNames(
      util.Arrays.asList("orders", "payments", "innocent"))

    // Unauthorized principal: hasClusterAuth=false AND empty describable/deletable
    // simulates a client with zero ACLs. Every name should look identical from the
    // outside — TOPIC_AUTHORIZATION_FAILED across the board, no logical-topic leak.
    val expectedResponse = Set(
      new DeletableTopicResult().setName("orders").setErrorCode(TOPIC_AUTHORIZATION_FAILED.code())
        .setErrorMessage("Topic authorization failed."),
      new DeletableTopicResult().setName("payments").setErrorCode(TOPIC_AUTHORIZATION_FAILED.code())
        .setErrorMessage("Topic authorization failed."),
      new DeletableTopicResult().setName("innocent").setErrorCode(TOPIC_AUTHORIZATION_FAILED.code())
        .setErrorMessage("Topic authorization failed."))
    assertEquals(expectedResponse, controllerApis.deleteTopics(ANONYMOUS_CONTEXT, request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      hasClusterAuth = false,
      _ => Set.empty, // not describable
      _ => Set.empty  // not deletable
    ).get().asScala.toSet)
  }

  /**
   * r17 ADV-A1 BLOCKER follow-up — A principal with DESCRIBE on a logical name but not
   * DELETE must still see TOPIC_AUTHORIZATION_FAILED, not INVALID_REQUEST. The shadow
   * rejection only fires once the principal has BOTH describe and delete authz.
   */
  @Test
  def testDeleteTopicsLogicalRejectionRequiresDeletePermission(): Unit = {
    val controller = new MockController.Builder().build()
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4")
    controllerApis = createControllerApis(None, controller, props)
    val request = new DeleteTopicsRequestData().setTopicNames(
      util.Arrays.asList("orders"))

    // Describable but NOT deletable: the principal can list the topic but cannot delete
    // it. The shadow disclosure must wait for DELETE authz too — otherwise a read-only
    // principal could enumerate the declared logical-topic set.
    val expectedResponse = Set(
      new DeletableTopicResult().setName("orders").setErrorCode(TOPIC_AUTHORIZATION_FAILED.code())
        .setErrorMessage("Topic authorization failed."))
    assertEquals(expectedResponse, controllerApis.deleteTopics(ANONYMOUS_CONTEXT, request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      hasClusterAuth = false,
      _ => Set("orders"), // describable
      _ => Set.empty       // not deletable
    ).get().asScala.toSet)
  }

  /**
   * r19 ADV-A1-followup BLOCKER #128-regression — DeleteTopics by UUID must observe the
   * same shadow guard as DeleteTopics by name. Before this fix, the ID iterator filtered
   * only on !deletable, so a principal with DELETE on "orders" could send
   * DeleteTopics(UUID=<physical "orders" id>) and the physical backing topic of a declared
   * logical name was silently deleted — exactly the data-loss bug commit 0a5844d477 was
   * supposed to close, but reopened via the UUID path. UUIDs are trivial to obtain (any
   * metadata response surfaces them), so this is reachable in the wild, not a theoretical
   * gap.
   */
  @Test
  def testDeleteTopicsByIdRejectsDeclaredLogicalName(): Unit = {
    val ordersId = Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")
    val innocentId = Uuid.fromString("VlFu5c51ToiNx64wtwkhQw")
    val controller = new MockController.Builder().
      newInitialTopic("orders", ordersId).
      newInitialTopic("innocent", innocentId).build()
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4")
    controllerApis = createControllerApis(None, controller, props)
    val request = new DeleteTopicsRequestData()
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(ordersId))
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(innocentId))

    // ordersId resolves to "orders" — declared logical, so refuse with the operator
    // message (auth has already cleared because hasClusterAuth=true). innocentId resolves
    // to "innocent" — non-declared, deletion proceeds normally.
    val expectedResponse = Set(
      new DeletableTopicResult().setName("orders").setTopicId(ordersId).
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Topic 'orders' is a declared logical topic in concentration.logical.topics on " +
          "this controller. Logical topics cannot be deleted via DeleteTopics; remove the " +
          "declaration from the controller's broker config and restart, then any physical " +
          "topic of the same name can be deleted via the normal path."),
      new DeletableTopicResult().setName("innocent").setTopicId(innocentId))
    assertEquals(expectedResponse, controllerApis.deleteTopics(ANONYMOUS_CONTEXT, request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      hasClusterAuth = true,
      _ => Set.empty,
      _ => Set.empty).get().asScala.toSet)
  }

  /**
   * r19 ADV-A1-followup BLOCKER #128-regression — auth-precedence parity for the UUID
   * path. A principal that can describe "orders" via the UUID but cannot delete it must
   * see TOPIC_AUTHORIZATION_FAILED (identical shape to a real-but-unauthorized topic),
   * NOT the operator's INVALID_REQUEST message. Otherwise the UUID path becomes an
   * enumeration oracle on the declared logical-topic set for describe-only principals,
   * symmetric to the name-path oracle the previous test pinned shut.
   */
  @Test
  def testDeleteTopicsByIdLogicalRejectionRequiresDeletePermission(): Unit = {
    val ordersId = Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")
    val controller = new MockController.Builder().
      newInitialTopic("orders", ordersId).build()
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4")
    controllerApis = createControllerApis(None, controller, props)
    val request = new DeleteTopicsRequestData()
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(ordersId))

    // Describable but NOT deletable: the ID iterator's existing !deletable branch
    // already returns TOPIC_AUTHORIZATION_FAILED, which is the correct masking shape —
    // identical to what a real-but-unauthorized topic would return. This test pins
    // that the regression fix did not accidentally widen disclosure.
    val expectedResponse = Set(
      new DeletableTopicResult().setName("orders").setTopicId(ordersId).
        setErrorCode(TOPIC_AUTHORIZATION_FAILED.code()).
        setErrorMessage(TOPIC_AUTHORIZATION_FAILED.message()))
    assertEquals(expectedResponse, controllerApis.deleteTopics(ANONYMOUS_CONTEXT, request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      hasClusterAuth = false,
      _ => Set("orders"), // describable
      _ => Set.empty       // not deletable
    ).get().asScala.toSet)
  }

  /**
   * r19 ADV-A BLOCKER #139 — DeleteTopics must refuse the backing-topic name of a declared
   * logical, by NAME path. Before this fix, a principal with DELETE on the backing name
   * "shared" could send DeleteTopics(["shared"]) and the controller would delete the
   * physical backing log — taking every logical tenant on that backing with it (committed
   * offsets, sidecar indices, in-flight produces). The same shape as #137 but on a
   * data-loss RPC instead of a partition-count RPC.
   */
  @Test
  def testDeleteTopicsRejectsDeclaredBackingName(): Unit = {
    val sharedId = Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")
    val controller = new MockController.Builder().
      newInitialTopic("shared", sharedId).build()
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4,payments:50:shared:4")
    controllerApis = createControllerApis(None, controller, props)
    val request = new DeleteTopicsRequestData().setTopicNames(
      util.Arrays.asList("shared", "innocent"))
    val expectedResponse = Set(
      new DeletableTopicResult().setName("shared").
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Topic 'shared' is the backing topic for one or more declared logical " +
          "topics in concentration.logical.topics on this controller. Backing topics cannot be " +
          "deleted via DeleteTopics while declarations are active; remove the declaration(s) " +
          "and restart to delete the backing."),
      new DeletableTopicResult().setName("innocent").
        setErrorCode(UNKNOWN_TOPIC_OR_PARTITION.code()).
        setErrorMessage("This server does not host this topic-partition."))
    assertEquals(expectedResponse, controllerApis.deleteTopics(ANONYMOUS_CONTEXT, request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      hasClusterAuth = true,
      _ => Set.empty,
      _ => Set.empty).get().asScala.toSet)
  }

  /**
   * r19 ADV-A BLOCKER #139 — DeleteTopics by UUID must also refuse the backing-topic name.
   * UUIDs are trivial to obtain (any METADATA response surfaces them), so the UUID path
   * must hold the same data-loss guard as the name path. After findTopicNames resolves
   * the backing UUID to its name "shared", the declaredBackingTopicNames check rejects.
   */
  @Test
  def testDeleteTopicsByIdRejectsDeclaredBackingName(): Unit = {
    val sharedId = Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")
    val innocentId = Uuid.fromString("VlFu5c51ToiNx64wtwkhQw")
    val controller = new MockController.Builder().
      newInitialTopic("shared", sharedId).
      newInitialTopic("innocent", innocentId).build()
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4")
    controllerApis = createControllerApis(None, controller, props)
    val request = new DeleteTopicsRequestData()
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(sharedId))
    request.topics().add(new DeleteTopicState().setName(null).setTopicId(innocentId))

    val expectedResponse = Set(
      new DeletableTopicResult().setName("shared").setTopicId(sharedId).
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage("Topic 'shared' is the backing topic for one or more declared logical " +
          "topics in concentration.logical.topics on this controller. Backing topics cannot be " +
          "deleted via DeleteTopics while declarations are active; remove the declaration(s) " +
          "and restart to delete the backing."),
      new DeletableTopicResult().setName("innocent").setTopicId(innocentId))
    assertEquals(expectedResponse, controllerApis.deleteTopics(ANONYMOUS_CONTEXT, request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      hasClusterAuth = true,
      _ => Set.empty,
      _ => Set.empty).get().asScala.toSet)
  }

  /**
   * r19 ADV-A BLOCKER #139 — auth-first / shadow-second precedence on the name path. A
   * describe-only principal must see TOPIC_AUTHORIZATION_FAILED (identical to a real-but-
   * unauthorized topic), NOT the backing-name operator message. Otherwise the declared-
   * backing set is enumerable by anyone with DESCRIBE.
   */
  @Test
  def testDeleteTopicsBackingRejectionRequiresDeletePermission(): Unit = {
    val sharedId = Uuid.fromString("vZKYST0pSA2HO5x_6hoO2Q")
    val controller = new MockController.Builder().
      newInitialTopic("shared", sharedId).build()
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4")
    controllerApis = createControllerApis(None, controller, props)
    val request = new DeleteTopicsRequestData().setTopicNames(
      util.Arrays.asList("shared"))

    val expectedResponse = Set(
      new DeletableTopicResult().setName("shared").setErrorCode(TOPIC_AUTHORIZATION_FAILED.code())
        .setErrorMessage("Topic authorization failed."))
    assertEquals(expectedResponse, controllerApis.deleteTopics(ANONYMOUS_CONTEXT, request,
      ApiKeys.DELETE_TOPICS.latestVersion().toInt,
      hasClusterAuth = false,
      _ => Set("shared"), // describable
      _ => Set.empty       // not deletable
    ).get().asScala.toSet)
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

  /**
   * r19 ADV-A BLOCKER #137 — CreatePartitions on a declared-logical name must be rejected.
   *
   * An operator holding ALTER on a topic that shares its name with a declared logical topic
   * could otherwise silently expand the partition count of the physical topic of the same
   * name. That physical is meant to be hidden behind the logical declaration by the
   * shadow-overlay on every broker; mutating it via CreatePartitions creates a divergence
   * between controller metadata and the per-broker overlay view, breaking concentration's
   * topology invariants. The controller is the single serialization point for partition
   * counts; the parser is the only source of truth here (no controller-side kernel).
   */
  @Test
  def testCreatePartitionsRejectsDeclaredLogicalName(): Unit = {
    val controller = mock(classOf[Controller])
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4")
    controllerApis = createControllerApis(None, controller, props)
    val request = new CreatePartitionsRequestData()
    request.topics().add(new CreatePartitionsTopic().setName("orders").setAssignments(null).setCount(8))
    request.topics().add(new CreatePartitionsTopic().setName("safe").setAssignments(null).setCount(2))

    when(controller.createPartitions(
      any(),
      ArgumentMatchers.eq(
        Collections.singletonList(
          new CreatePartitionsTopic().setName("safe").setAssignments(null).setCount(2))),
      ArgumentMatchers.eq(false))).thenReturn(CompletableFuture
      .completedFuture(Collections.singletonList(
        new CreatePartitionsTopicResult().setName("safe").
          setErrorCode(NONE.code()).
          setErrorMessage(null)
      )))

    val results = controllerApis.createPartitions(ANONYMOUS_CONTEXT, request,
      _ => Set("orders", "safe")).get().asScala.toSet

    assertEquals(2, results.size)
    val ordersResult = results.find(_.name == "orders").get
    assertEquals(INVALID_REQUEST.code, ordersResult.errorCode,
      "CreatePartitions on a declared logical topic name must be rejected with INVALID_REQUEST")
    assertTrue(ordersResult.errorMessage.contains("declared logical topic"),
      s"error message must explain the rejection: ${ordersResult.errorMessage}")
    val safeResult = results.find(_.name == "safe").get
    assertEquals(NONE.code, safeResult.errorCode,
      "non-shadowed topic must still pass through to the controller")
    // The shadow rejection must NOT reach the underlying controller for the rejected name.
    verify(controller, never()).createPartitions(
      any(),
      ArgumentMatchers.argThat[util.List[CreatePartitionsTopic]](topics =>
        topics.asScala.exists(_.name == "orders")),
      anyBoolean)
  }

  /**
   * r19 ADV-A BLOCKER #137 (backing side) — CreatePartitions on a backing topic name must
   * also be rejected. Expanding the backing's partition count while declarations are live
   * would create dead partitions (no logical-to-backing mapping for the new indices) and
   * diverge the controller's partition count from the per-descriptor count the kernel uses
   * to map logical→backing on every broker. Same auth-first / shadow-second precedence as
   * the logical-name guard above.
   */
  @Test
  def testCreatePartitionsRejectsDeclaredBackingName(): Unit = {
    val controller = mock(classOf[Controller])
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4")
    controllerApis = createControllerApis(None, controller, props)
    val request = new CreatePartitionsRequestData()
    request.topics().add(new CreatePartitionsTopic().setName("shared").setAssignments(null).setCount(8))

    // Empty topics list still reaches the underlying controller stub; return an empty result.
    when(controller.createPartitions(any(), any(), anyBoolean))
      .thenReturn(CompletableFuture.completedFuture(Collections.emptyList()))

    val results = controllerApis.createPartitions(ANONYMOUS_CONTEXT, request,
      _ => Set("shared")).get().asScala.toSet

    assertEquals(1, results.size)
    val sharedResult = results.head
    assertEquals("shared", sharedResult.name)
    assertEquals(INVALID_REQUEST.code, sharedResult.errorCode,
      "CreatePartitions on a backing topic of a declared logical must be rejected with INVALID_REQUEST")
    assertTrue(sharedResult.errorMessage.contains("backing topic"),
      s"error message must explain the rejection: ${sharedResult.errorMessage}")
    // The shadow rejection must NOT reach the underlying controller for the rejected name.
    verify(controller, never()).createPartitions(
      any(),
      ArgumentMatchers.argThat[util.List[CreatePartitionsTopic]](topics =>
        topics.asScala.exists(_.name == "shared")),
      anyBoolean)
  }

  /**
   * r19 ADV-A BLOCKER #137 — auth-first / shadow-second precedence.
   *
   * When a CreatePartitions name is both unauthorized AND a logical-shadow collider, the
   * controller must return TOPIC_AUTHORIZATION_FAILED, not the operator remediation message.
   * Otherwise the declared-logical set is enumerable to any principal who can probe arbitrary
   * names without ALTER. Mirrors the same precedence pinned for CreateTopics and DeleteTopics.
   */
  @Test
  def testCreatePartitionsShadowDefersToAuthorizationFailure(): Unit = {
    val controller = mock(classOf[Controller])
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4")
    controllerApis = createControllerApis(None, controller, props)
    val request = new CreatePartitionsRequestData()
    request.topics().add(new CreatePartitionsTopic().setName("orders").setAssignments(null).setCount(8))
    request.topics().add(new CreatePartitionsTopic().setName("shared").setAssignments(null).setCount(8))

    // Empty topics list still reaches the underlying controller stub; return an empty result.
    when(controller.createPartitions(any(), any(), anyBoolean))
      .thenReturn(CompletableFuture.completedFuture(Collections.emptyList()))

    // Authorizer denies both the logical name and the backing name.
    val results = controllerApis.createPartitions(ANONYMOUS_CONTEXT, request,
      _ => Set.empty[String]).get().asScala.toSet

    assertEquals(2, results.size)
    assertEquals(TOPIC_AUTHORIZATION_FAILED.code, results.find(_.name == "orders").get.errorCode,
      "unauthorized declared-logical name must return TOPIC_AUTHORIZATION_FAILED, " +
        "not the operator remediation message (which would leak the declared set)")
    assertEquals(TOPIC_AUTHORIZATION_FAILED.code, results.find(_.name == "shared").get.errorCode,
      "unauthorized backing name must also return TOPIC_AUTHORIZATION_FAILED")
    // Neither rejected name reaches the underlying controller.
    verify(controller, never()).createPartitions(
      any(),
      ArgumentMatchers.argThat[util.List[CreatePartitionsTopic]](topics =>
        topics.asScala.exists(t => t.name == "orders" || t.name == "shared")),
      anyBoolean)
  }

  // ---------------------------------------------------------------------------------------------
  // r19 ADV-A HIGH #146 — AlterConfigs / IncrementalAlterConfigs concentration guard.
  //
  // Without this guard, an operator holding ALTER_CONFIGS on a TOPIC resource named after a
  // declared backing topic could mutate retention.bytes / cleanup.policy / segment.bytes /
  // min.insync.replicas on the substrate, disrupting EVERY logical tenant sharing that backing.
  // For declared-logical names, AlterConfigs has no defined semantics in v1 (task #105 tracks
  // wiring descriptor changes through the kernel) — silently routing to the underlying
  // controller either lands the change on a phantom physical topic or returns
  // UNKNOWN_TOPIC_OR_PARTITION. Both are wrong: must be explicitly rejected with a clear
  // operator remediation message.
  //
  // The controller is the canonical mutation point (broker-side preprocess is "nothing to do"
  // for TOPIC and forwards directly to the controller), so one guard at handleLegacyAlterConfigs
  // / handleIncrementalAlterConfigs covers both legacy and incremental, both controller- and
  // broker-originated paths. Auth-first / shadow-second precedence preserved per #128/#137/#139.
  // ---------------------------------------------------------------------------------------------

  @Test
  def testLegacyAlterConfigsRejectsDeclaredLogicalTopic(): Unit = {
    val controller = mock(classOf[Controller])
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4")
    controllerApis = createControllerApis(None, controller, props)
    // controller stub returns NONE for any name that DOES reach it.
    when(controller.legacyAlterConfigs(any(), any(), anyBoolean()))
      .thenReturn(CompletableFuture.completedFuture(Collections.emptyMap[ConfigResource, ApiError]()))

    val requestData = new AlterConfigsRequestData().setResources(
      new OldAlterConfigsResourceCollection(util.Arrays.asList(
        new OldAlterConfigsResource().
          setResourceName("orders").
          setResourceType(ConfigResource.Type.TOPIC.id()).
          setConfigs(new OldAlterableConfigCollection(util.Arrays.asList(new OldAlterableConfig().
            setName(TopicConfig.RETENTION_BYTES_CONFIG).
            setValue("1000000")).iterator())),
        new OldAlterConfigsResource().
          setResourceName("safe").
          setResourceType(ConfigResource.Type.TOPIC.id()).
          setConfigs(new OldAlterableConfigCollection(util.Arrays.asList(new OldAlterableConfig().
            setName(TopicConfig.RETENTION_BYTES_CONFIG).
            setValue("1000000")).iterator()))
      ).iterator()))
    val request = buildRequest(new AlterConfigsRequest(requestData, 0))
    controllerApis.handleLegacyAlterConfigs(request)

    val capturedResponse: ArgumentCaptor[AbstractResponse] =
      ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request), capturedResponse.capture(), ArgumentMatchers.eq(None))
    val response = capturedResponse.getValue.asInstanceOf[AlterConfigsResponse]
    val byName = response.data().responses().asScala.map(r => r.resourceName() -> r).toMap
    assertEquals(INVALID_TOPIC_EXCEPTION.code(), byName("orders").errorCode(),
      "AlterConfigs on a declared logical topic must be rejected with INVALID_TOPIC_EXCEPTION")
    assertTrue(Option(byName("orders").errorMessage()).exists(_.contains("logical topic")),
      s"error message must explain the rejection: ${byName("orders").errorMessage()}")
    // The rejected resource must NOT reach the underlying controller; the controller may still
    // be invoked for the non-shadowed "safe" name.
    verify(controller, never()).legacyAlterConfigs(
      any(),
      ArgumentMatchers.argThat[util.Map[ConfigResource, util.Map[String, String]]](map =>
        map.keySet().asScala.exists(_.name() == "orders")),
      anyBoolean())
  }

  @Test
  def testLegacyAlterConfigsRejectsDeclaredBackingTopic(): Unit = {
    val controller = mock(classOf[Controller])
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4")
    controllerApis = createControllerApis(None, controller, props)
    when(controller.legacyAlterConfigs(any(), any(), anyBoolean()))
      .thenReturn(CompletableFuture.completedFuture(Collections.emptyMap[ConfigResource, ApiError]()))

    val requestData = new AlterConfigsRequestData().setResources(
      new OldAlterConfigsResourceCollection(util.Arrays.asList(
        new OldAlterConfigsResource().
          setResourceName("shared").
          setResourceType(ConfigResource.Type.TOPIC.id()).
          setConfigs(new OldAlterableConfigCollection(util.Arrays.asList(new OldAlterableConfig().
            setName(TopicConfig.RETENTION_BYTES_CONFIG).
            setValue("1000000")).iterator()))
      ).iterator()))
    val request = buildRequest(new AlterConfigsRequest(requestData, 0))
    controllerApis.handleLegacyAlterConfigs(request)

    val capturedResponse: ArgumentCaptor[AbstractResponse] =
      ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request), capturedResponse.capture(), ArgumentMatchers.eq(None))
    val response = capturedResponse.getValue.asInstanceOf[AlterConfigsResponse]
    val byName = response.data().responses().asScala.map(r => r.resourceName() -> r).toMap
    assertEquals(INVALID_TOPIC_EXCEPTION.code(), byName("shared").errorCode(),
      "AlterConfigs on a backing topic of a declared logical must be rejected with INVALID_TOPIC_EXCEPTION")
    assertTrue(Option(byName("shared").errorMessage()).exists(_.contains("backing topic")),
      s"error message must explain the rejection: ${byName("shared").errorMessage()}")
    verify(controller, never()).legacyAlterConfigs(
      any(),
      ArgumentMatchers.argThat[util.Map[ConfigResource, util.Map[String, String]]](map =>
        map.keySet().asScala.exists(_.name() == "shared")),
      anyBoolean())
  }

  @Test
  def testIncrementalAlterConfigsRejectsDeclaredLogicalTopic(): Unit = {
    val controller = mock(classOf[Controller])
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4")
    controllerApis = createControllerApis(None, controller, props)
    when(controller.incrementalAlterConfigs(any(), any(), anyBoolean()))
      .thenReturn(CompletableFuture.completedFuture(Collections.emptyMap[ConfigResource, ApiError]()))

    val requestData = new IncrementalAlterConfigsRequestData().setResources(
      new AlterConfigsResourceCollection(util.Arrays.asList(
        new AlterConfigsResource().
          setResourceName("orders").
          setResourceType(ConfigResource.Type.TOPIC.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName(TopicConfig.CLEANUP_POLICY_CONFIG).
            setValue("compact").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator())),
        new AlterConfigsResource().
          setResourceName("safe").
          setResourceType(ConfigResource.Type.TOPIC.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName(TopicConfig.RETENTION_BYTES_CONFIG).
            setValue("1000000").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator()))
      ).iterator()))
    val request = buildRequest(new IncrementalAlterConfigsRequest.Builder(requestData).build(0))
    controllerApis.handleIncrementalAlterConfigs(request)

    val capturedResponse: ArgumentCaptor[AbstractResponse] =
      ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request), capturedResponse.capture(), ArgumentMatchers.eq(None))
    val response = capturedResponse.getValue.asInstanceOf[IncrementalAlterConfigsResponse]
    val byName = response.data().responses().asScala.map(r => r.resourceName() -> r).toMap
    assertEquals(INVALID_TOPIC_EXCEPTION.code(), byName("orders").errorCode(),
      "IncrementalAlterConfigs on a declared logical topic must be rejected with INVALID_TOPIC_EXCEPTION")
    assertTrue(Option(byName("orders").errorMessage()).exists(_.contains("logical topic")),
      s"error message must explain the rejection: ${byName("orders").errorMessage()}")
    verify(controller, never()).incrementalAlterConfigs(
      any(),
      ArgumentMatchers.argThat[util.Map[ConfigResource, util.Map[String, util.Map.Entry[AlterConfigOp.OpType, String]]]](map =>
        map.keySet().asScala.exists(_.name() == "orders")),
      anyBoolean())
  }

  @Test
  def testIncrementalAlterConfigsRejectsDeclaredBackingTopic(): Unit = {
    val controller = mock(classOf[Controller])
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4")
    controllerApis = createControllerApis(None, controller, props)
    when(controller.incrementalAlterConfigs(any(), any(), anyBoolean()))
      .thenReturn(CompletableFuture.completedFuture(Collections.emptyMap[ConfigResource, ApiError]()))

    val requestData = new IncrementalAlterConfigsRequestData().setResources(
      new AlterConfigsResourceCollection(util.Arrays.asList(
        new AlterConfigsResource().
          setResourceName("shared").
          setResourceType(ConfigResource.Type.TOPIC.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName(TopicConfig.RETENTION_BYTES_CONFIG).
            setValue("1").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator()))
      ).iterator()))
    val request = buildRequest(new IncrementalAlterConfigsRequest.Builder(requestData).build(0))
    controllerApis.handleIncrementalAlterConfigs(request)

    val capturedResponse: ArgumentCaptor[AbstractResponse] =
      ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request), capturedResponse.capture(), ArgumentMatchers.eq(None))
    val response = capturedResponse.getValue.asInstanceOf[IncrementalAlterConfigsResponse]
    val byName = response.data().responses().asScala.map(r => r.resourceName() -> r).toMap
    assertEquals(INVALID_TOPIC_EXCEPTION.code(), byName("shared").errorCode(),
      "IncrementalAlterConfigs on a backing topic of a declared logical must be rejected with INVALID_TOPIC_EXCEPTION")
    assertTrue(Option(byName("shared").errorMessage()).exists(_.contains("backing topic")),
      s"error message must explain the rejection: ${byName("shared").errorMessage()}")
    verify(controller, never()).incrementalAlterConfigs(
      any(),
      ArgumentMatchers.argThat[util.Map[ConfigResource, util.Map[String, util.Map.Entry[AlterConfigOp.OpType, String]]]](map =>
        map.keySet().asScala.exists(_.name() == "shared")),
      anyBoolean())
  }

  /**
   * r19 ADV-A HIGH #146 — auth-first / shadow-second precedence for AlterConfigs.
   *
   * When a TOPIC name is BOTH unauthorized (no ALTER_CONFIGS) AND a declared-logical/backing
   * collider, the controller must return TOPIC_AUTHORIZATION_FAILED — NOT the operator
   * remediation message. Otherwise the declared-logical / declared-backing sets are enumerable
   * by any principal who can probe arbitrary names without ALTER_CONFIGS. Mirrors the precedence
   * pinned for CreateTopics / DeleteTopics / CreatePartitions (#128/#137/#139).
   */
  @Test
  def testAlterConfigsShadowDefersToAuthorizationFailure(): Unit = {
    val controller = mock(classOf[Controller])
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4")
    when(controller.incrementalAlterConfigs(any(), any(), anyBoolean()))
      .thenReturn(CompletableFuture.completedFuture(Collections.emptyMap[ConfigResource, ApiError]()))
    // Deny-all authorizer: every authorize() call returns DENIED.
    controllerApis = createControllerApis(Some(createDenyAllAuthorizer()), controller, props)

    val requestData = new IncrementalAlterConfigsRequestData().setResources(
      new AlterConfigsResourceCollection(util.Arrays.asList(
        new AlterConfigsResource().
          setResourceName("orders").
          setResourceType(ConfigResource.Type.TOPIC.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName(TopicConfig.RETENTION_BYTES_CONFIG).
            setValue("1").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator())),
        new AlterConfigsResource().
          setResourceName("shared").
          setResourceType(ConfigResource.Type.TOPIC.id()).
          setConfigs(new AlterableConfigCollection(util.Arrays.asList(new AlterableConfig().
            setName(TopicConfig.RETENTION_BYTES_CONFIG).
            setValue("1").
            setConfigOperation(AlterConfigOp.OpType.SET.id())).iterator()))
      ).iterator()))
    val request = buildRequest(new IncrementalAlterConfigsRequest.Builder(requestData).build(0))
    controllerApis.handleIncrementalAlterConfigs(request)

    val capturedResponse: ArgumentCaptor[AbstractResponse] =
      ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request), capturedResponse.capture(), ArgumentMatchers.eq(None))
    val response = capturedResponse.getValue.asInstanceOf[IncrementalAlterConfigsResponse]
    val byName = response.data().responses().asScala.map(r => r.resourceName() -> r.errorCode()).toMap
    assertEquals(TOPIC_AUTHORIZATION_FAILED.code(), byName("orders"),
      "unauthorized declared-logical name must return TOPIC_AUTHORIZATION_FAILED, " +
        "not the operator remediation message (which would leak the declared set)")
    assertEquals(TOPIC_AUTHORIZATION_FAILED.code(), byName("shared"),
      "unauthorized backing name must also return TOPIC_AUTHORIZATION_FAILED")
    verify(controller, never()).incrementalAlterConfigs(any(), any(), anyBoolean())
  }

  /**
   * r19 ADV-A HIGH #145 — AlterPartitionReassignments on a backing topic name must be
   * rejected. Without this guard, a CLUSTER ALTER operator could move the backing topic's
   * replicas, which transparently moves data for every co-tenant logical topic on that
   * backing — a stealth re-tenanting attack or accidental cross-tenant data-locality
   * breach. Per-partition INVALID_REQUEST mirrors the precedence used by DeleteTopics /
   * CreatePartitions on declared names; cluster auth has already cleared, so disclosure
   * of the operator-remediation message is bounded to privileged principals.
   */
  @Test
  def testAlterPartitionReassignmentsRejectsBackingTopic(): Unit = {
    val controller = mock(classOf[Controller])
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4")
    controllerApis = createControllerApis(None, controller, props)

    val backingPartitions = new util.ArrayList[AlterPartitionReassignmentsRequestData.ReassignablePartition]()
    backingPartitions.add(new AlterPartitionReassignmentsRequestData.ReassignablePartition()
      .setPartitionIndex(0).setReplicas(java.util.Arrays.asList(1, 2, 3)))
    backingPartitions.add(new AlterPartitionReassignmentsRequestData.ReassignablePartition()
      .setPartitionIndex(1).setReplicas(java.util.Arrays.asList(2, 3, 4)))
    val safePartitions = new util.ArrayList[AlterPartitionReassignmentsRequestData.ReassignablePartition]()
    safePartitions.add(new AlterPartitionReassignmentsRequestData.ReassignablePartition()
      .setPartitionIndex(0).setReplicas(java.util.Arrays.asList(1, 2, 3)))
    val data = new AlterPartitionReassignmentsRequestData().setTimeoutMs(30000)
    data.topics.add(new AlterPartitionReassignmentsRequestData.ReassignableTopic()
      .setName("shared").setPartitions(backingPartitions))
    data.topics.add(new AlterPartitionReassignmentsRequestData.ReassignableTopic()
      .setName("safe").setPartitions(safePartitions))

    val controllerResponse = new AlterPartitionReassignmentsResponseData().setErrorMessage(null)
    val safeTopicResp = new AlterPartitionReassignmentsResponseData.ReassignableTopicResponse().setName("safe")
    safeTopicResp.partitions.add(new AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse()
      .setPartitionIndex(0).setErrorCode(NONE.code))
    controllerResponse.responses.add(safeTopicResp)
    when(controller.alterPartitionReassignments(any[ControllerRequestContext],
      ArgumentMatchers.argThat[AlterPartitionReassignmentsRequestData](req =>
        req.topics.asScala.forall(_.name == "safe"))))
      .thenReturn(CompletableFuture.completedFuture(controllerResponse))

    val response = handleRequest[AlterPartitionReassignmentsResponse](
      new AlterPartitionReassignmentsRequest.Builder(data).build(), controllerApis)
    val topicResponses = response.data.responses.asScala.map(t => t.name -> t).toMap

    val sharedResp = topicResponses("shared")
    assertEquals(2, sharedResp.partitions.size)
    sharedResp.partitions.asScala.foreach { p =>
      assertEquals(INVALID_REQUEST.code, p.errorCode,
        s"AlterPartitionReassignments on backing partition ${p.partitionIndex} must be rejected")
      assertTrue(p.errorMessage != null && p.errorMessage.contains("backing topic"),
        s"error message must identify the backing-topic rejection: ${p.errorMessage}")
    }

    val safeResp = topicResponses("safe")
    assertEquals(1, safeResp.partitions.size)
    assertEquals(NONE.code, safeResp.partitions.get(0).errorCode,
      "non-shadowed topic must still pass through to the controller")

    // The shadow rejection must NOT reach the underlying controller for the backing name.
    verify(controller, never()).alterPartitionReassignments(
      any[ControllerRequestContext],
      ArgumentMatchers.argThat[AlterPartitionReassignmentsRequestData](req =>
        req.topics.asScala.exists(_.name == "shared")))
  }

  /**
   * r19 ADV-A HIGH #145 (logical side) — AlterPartitionReassignments on a declared logical
   * topic name must also be rejected. Logical topics have no physical partitions in KRaft
   * metadata; passing them through would surface UNKNOWN_TOPIC_OR_PARTITION as an
   * enumeration oracle observable to any CLUSTER ALTER principal. Reject explicitly with
   * INVALID_REQUEST and a remediation message.
   */
  @Test
  def testAlterPartitionReassignmentsRejectsLogicalTopic(): Unit = {
    val controller = mock(classOf[Controller])
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4")
    controllerApis = createControllerApis(None, controller, props)

    val logicalPartitions = new util.ArrayList[AlterPartitionReassignmentsRequestData.ReassignablePartition]()
    logicalPartitions.add(new AlterPartitionReassignmentsRequestData.ReassignablePartition()
      .setPartitionIndex(7).setReplicas(java.util.Arrays.asList(1, 2, 3)))
    val data = new AlterPartitionReassignmentsRequestData().setTimeoutMs(30000)
    data.topics.add(new AlterPartitionReassignmentsRequestData.ReassignableTopic()
      .setName("orders").setPartitions(logicalPartitions))

    val response = handleRequest[AlterPartitionReassignmentsResponse](
      new AlterPartitionReassignmentsRequest.Builder(data).build(), controllerApis)
    val ordersResp = response.data.responses.asScala.find(_.name == "orders").get
    assertEquals(1, ordersResp.partitions.size)
    val partResp = ordersResp.partitions.get(0)
    assertEquals(INVALID_REQUEST.code, partResp.errorCode,
      "AlterPartitionReassignments on a declared logical topic must be rejected with INVALID_REQUEST")
    assertTrue(partResp.errorMessage != null && partResp.errorMessage.contains("declared logical topic"),
      s"error message must identify the logical-topic rejection: ${partResp.errorMessage}")

    // The controller is short-circuited because every requested topic was shadowed.
    verify(controller, never()).alterPartitionReassignments(
      any[ControllerRequestContext], any[AlterPartitionReassignmentsRequestData])
  }

  /**
   * r19 ADV-A HIGH #145 — legitimate (non-concentration) reassignments must still pass
   * through unchanged. Proves no regression on the unconcentrated control path.
   */
  @Test
  def testAlterPartitionReassignmentsAllowsRegularTopic(): Unit = {
    val controller = mock(classOf[Controller])
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4")
    controllerApis = createControllerApis(None, controller, props)

    val partitions = new util.ArrayList[AlterPartitionReassignmentsRequestData.ReassignablePartition]()
    partitions.add(new AlterPartitionReassignmentsRequestData.ReassignablePartition()
      .setPartitionIndex(0).setReplicas(java.util.Arrays.asList(1, 2, 3)))
    val data = new AlterPartitionReassignmentsRequestData().setTimeoutMs(30000)
    data.topics.add(new AlterPartitionReassignmentsRequestData.ReassignableTopic()
      .setName("regular").setPartitions(partitions))

    val controllerResponse = new AlterPartitionReassignmentsResponseData().setErrorMessage(null)
    val regularResp = new AlterPartitionReassignmentsResponseData.ReassignableTopicResponse().setName("regular")
    regularResp.partitions.add(new AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse()
      .setPartitionIndex(0).setErrorCode(NONE.code))
    controllerResponse.responses.add(regularResp)
    when(controller.alterPartitionReassignments(any[ControllerRequestContext],
      any[AlterPartitionReassignmentsRequestData]))
      .thenReturn(CompletableFuture.completedFuture(controllerResponse))

    val response = handleRequest[AlterPartitionReassignmentsResponse](
      new AlterPartitionReassignmentsRequest.Builder(data).build(), controllerApis)
    val resp = response.data.responses.asScala.find(_.name == "regular").get
    assertEquals(NONE.code, resp.partitions.get(0).errorCode,
      "non-shadowed topic must reach the controller and return its result unchanged")
  }

  /**
   * r19 ADV-A HIGH #145 (list side, explicit) — ListPartitionReassignments must not surface
   * declared logical or backing topics when listed by name. The controller would silently
   * skip logical names (no metadata) but stripping them before forwarding closes a
   * response-shape enumeration oracle, and stripping backing names prevents observing the
   * stealth-re-tenanting signal a backing reassignment in flight would otherwise expose.
   */
  @Test
  def testListPartitionReassignmentsFiltersDeclaredTopicsByName(): Unit = {
    val controller = mock(classOf[Controller])
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4")
    controllerApis = createControllerApis(None, controller, props)

    val data = new ListPartitionReassignmentsRequestData().setTimeoutMs(30000)
    val topics = new util.ArrayList[ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics]()
    topics.add(new ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics()
      .setName("orders").setPartitionIndexes(java.util.Arrays.asList(Integer.valueOf(0))))
    topics.add(new ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics()
      .setName("shared").setPartitionIndexes(java.util.Arrays.asList(Integer.valueOf(0))))
    topics.add(new ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics()
      .setName("regular").setPartitionIndexes(java.util.Arrays.asList(Integer.valueOf(0))))
    data.setTopics(topics)

    val controllerResponse = new ListPartitionReassignmentsResponseData().setErrorMessage(null)
    when(controller.listPartitionReassignments(any[ControllerRequestContext],
      ArgumentMatchers.argThat[ListPartitionReassignmentsRequestData](req =>
        req.topics != null && req.topics.size == 1 && req.topics.get(0).name == "regular")))
      .thenReturn(CompletableFuture.completedFuture(controllerResponse))

    handleRequest[ListPartitionReassignmentsResponse](
      new ListPartitionReassignmentsRequest.Builder(data).build(), controllerApis)

    // The controller must NEVER see the declared topic names on the request side.
    verify(controller, never()).listPartitionReassignments(
      any[ControllerRequestContext],
      ArgumentMatchers.argThat[ListPartitionReassignmentsRequestData](req =>
        req.topics != null && req.topics.asScala.exists(t => t.name == "orders" || t.name == "shared")))
  }

  /**
   * r19 ADV-A HIGH #145 (list side, all) — when ListPartitionReassignments is called with
   * a null topic list (list-all), any declared logical or backing topic surfacing in the
   * controller's response must be stripped before the client sees it. Defense-in-depth in
   * case a backing reassignment was initiated out-of-band or pre-declaration.
   */
  @Test
  def testListPartitionReassignmentsFiltersDeclaredTopicsFromListAllResponse(): Unit = {
    val controller = mock(classOf[Controller])
    val props = new Properties()
    props.put(ServerConfigs.CONCENTRATION_LOGICAL_TOPICS_CONFIG, "orders:100:shared:4")
    controllerApis = createControllerApis(None, controller, props)

    val data = new ListPartitionReassignmentsRequestData().setTimeoutMs(30000)
    // topics == null means "list all"
    data.setTopics(null)

    val controllerResponse = new ListPartitionReassignmentsResponseData().setErrorMessage(null)
    controllerResponse.topics.add(new ListPartitionReassignmentsResponseData.OngoingTopicReassignment()
      .setName("regular"))
    controllerResponse.topics.add(new ListPartitionReassignmentsResponseData.OngoingTopicReassignment()
      .setName("shared"))
    controllerResponse.topics.add(new ListPartitionReassignmentsResponseData.OngoingTopicReassignment()
      .setName("orders"))
    when(controller.listPartitionReassignments(any[ControllerRequestContext],
      any[ListPartitionReassignmentsRequestData]))
      .thenReturn(CompletableFuture.completedFuture(controllerResponse))

    val response = handleRequest[ListPartitionReassignmentsResponse](
      new ListPartitionReassignmentsRequest.Builder(data).build(), controllerApis)
    val names = response.data.topics.asScala.map(_.name).toSet
    assertEquals(Set("regular"), names,
      "declared logical and backing topics must be stripped from the list-all response")
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
