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

package kafka.network

import java.io.IOException
import java.net._
import java.nio.ByteBuffer
import java.nio.channels.{Selector => NSelector, _}
import java.util
import java.util.Optional
import java.util.concurrent._
import java.util.concurrent.atomic._
import kafka.cluster.EndPoint
import kafka.network.Processor._
import kafka.network.RequestChannel.{CloseConnectionResponse, EndThrottlingResponse, NoOpResponse, SendResponse, StartThrottlingResponse}
import kafka.network.SocketServer._
import kafka.server.{ApiVersionManager, BrokerReconfigurable, KafkaConfig}
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import kafka.utils._
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.{InvalidRequestException, UnsupportedVersionException}
import org.apache.kafka.common.memory.{MemoryPool, SimpleMemoryPool}
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.metrics.stats.{Avg, CumulativeSum, Meter, Rate}
import org.apache.kafka.common.network.KafkaChannel.ChannelMuteEvent
import org.apache.kafka.common.network.{ChannelBuilder, ChannelBuilders, ClientInformation, KafkaChannel, ListenerName, ListenerReconfigurable, NetworkSend, Selectable, Send, ServerConnectionId, Selector => KSelector}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{ApiVersionsRequest, RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{KafkaThread, LogContext, Time, Utils}
import org.apache.kafka.common.{Endpoint, KafkaException, MetricName, Reconfigurable}
import org.apache.kafka.network.{ConnectionQuotaEntity, ConnectionThrottledException, SocketServerConfigs, TooManyConnectionsException}
import org.apache.kafka.network.iouring.{BrokerSelector, NioBrokerSelector}
import org.apache.kafka.security.CredentialProvider
import org.apache.kafka.server.ServerSocketFactory
import org.apache.kafka.server.config.QuotaConfig
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.network.ConnectionDisconnectListener
import org.apache.kafka.server.quota.QuotaUtils
import org.apache.kafka.server.util.FutureUtils
import org.slf4j.event.Level

import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.ControlThrowable

/**
 * Handles new connections, requests and responses to and from broker.
 * Kafka supports two types of request planes :
 *  - data-plane :
 *    - Handles requests from clients and other brokers in the cluster.
 *    - The threading model is
 *      1 Acceptor thread per listener, that handles new connections.
 *      It is possible to configure multiple data-planes by specifying multiple "," separated endpoints for "listeners" in KafkaConfig.
 *      Acceptor has N Processor threads that each have their own selector and read requests from sockets
 *      M Handler threads that handle requests and produce responses back to the processor threads for writing.
 */
class SocketServer(
  val config: KafkaConfig,
  val metrics: Metrics,
  val time: Time,
  val credentialProvider: CredentialProvider,
  val apiVersionManager: ApiVersionManager,
  val socketFactory: ServerSocketFactory = ServerSocketFactory.INSTANCE,
  val connectionDisconnectListeners: Seq[ConnectionDisconnectListener] = Seq.empty
) extends Logging with BrokerReconfigurable {

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  private val maxQueuedRequests = config.queuedMaxRequests

  protected val nodeId: Int = config.brokerId

  private val logContext = new LogContext(s"[SocketServer listenerType=${apiVersionManager.listenerType}, nodeId=$nodeId] ")

  this.logIdent = logContext.logPrefix

  private val memoryPoolSensor = metrics.sensor("MemoryPoolUtilization")
  private val memoryPoolDepletedPercentMetricName = metrics.metricName("MemoryPoolAvgDepletedPercent", MetricsGroup)
  private val memoryPoolDepletedTimeMetricName = metrics.metricName("MemoryPoolDepletedTimeTotal", MetricsGroup)
  memoryPoolSensor.add(new Meter(TimeUnit.MILLISECONDS, memoryPoolDepletedPercentMetricName, memoryPoolDepletedTimeMetricName))
  private val memoryPool = if (config.queuedMaxBytes > 0) new SimpleMemoryPool(config.queuedMaxBytes, config.socketRequestMaxBytes, false, memoryPoolSensor) else MemoryPool.NONE
  // data-plane
  private[network] val dataPlaneAcceptors = new ConcurrentHashMap[EndPoint, DataPlaneAcceptor]()
  val dataPlaneRequestChannel = new RequestChannel(maxQueuedRequests, DataPlaneAcceptor.MetricPrefix, time, apiVersionManager.newRequestMetrics)

  private[this] val nextProcessorId: AtomicInteger = new AtomicInteger(0)
  val connectionQuotas = new ConnectionQuotas(config, time, metrics)

  /**
   * A future which is completed once all the authorizer futures are complete.
   */
  private val allAuthorizerFuturesComplete = new CompletableFuture[Void]

  /**
   * True if the SocketServer is stopped. Must be accessed under the SocketServer lock.
   */
  private var stopped = false

  // Socket server metrics
  metricsGroup.newGauge(s"${DataPlaneAcceptor.MetricPrefix}NetworkProcessorAvgIdlePercent", () => SocketServer.this.synchronized {
    val dataPlaneProcessors = dataPlaneAcceptors.asScala.values.flatMap(a => a.processors)
    val ioWaitRatioMetricNames = dataPlaneProcessors.map { p =>
      metrics.metricName("io-wait-ratio", MetricsGroup, p.metricTags)
    }
    if (dataPlaneProcessors.isEmpty) {
      1.0
    } else {
      ioWaitRatioMetricNames.map { metricName =>
        Option(metrics.metric(metricName)).fold(0.0)(m => Math.min(m.metricValue.asInstanceOf[Double], 1.0))
      }.sum / dataPlaneProcessors.size
    }
  })

  metricsGroup.newGauge("MemoryPoolAvailable", () => memoryPool.availableMemory)
  metricsGroup.newGauge("MemoryPoolUsed", () => memoryPool.size() - memoryPool.availableMemory)
  metricsGroup.newGauge(s"${DataPlaneAcceptor.MetricPrefix}ExpiredConnectionsKilledCount", () => SocketServer.this.synchronized {
    val dataPlaneProcessors = dataPlaneAcceptors.asScala.values.flatMap(a => a.processors)
    val expiredConnectionsKilledCountMetricNames = dataPlaneProcessors.map { p =>
      metrics.metricName("expired-connections-killed-count", MetricsGroup, p.metricTags)
    }
    expiredConnectionsKilledCountMetricNames.map { metricName =>
      Option(metrics.metric(metricName)).fold(0.0)(m => m.metricValue.asInstanceOf[Double])
    }.sum
  })

  // Create acceptors and processors for the statically configured endpoints when the
  // SocketServer is constructed. Note that this just opens the ports and creates the data
  // structures. It does not start the acceptors and processors or their associated JVM
  // threads.
  if (apiVersionManager.listenerType.equals(ListenerType.CONTROLLER)) {
    config.controllerListeners.foreach(createDataPlaneAcceptorAndProcessors)
  } else {
    config.dataPlaneListeners.foreach(createDataPlaneAcceptorAndProcessors)
  }

  // Processors are now created by each Acceptor. However to preserve compatibility, we need to number the processors
  // globally, so we keep the nextProcessorId counter in SocketServer
  def nextProcessorId(): Int = {
    nextProcessorId.getAndIncrement()
  }

  /**
   * This method enables request processing for all endpoints managed by this SocketServer. Each
   * endpoint will be brought up asynchronously as soon as its associated future is completed.
   * Therefore, we do not know that any particular request processor will be running by the end of
   * this function -- just that it might be running.
   *
   * @param authorizerFutures     Future per [[EndPoint]] used to wait before starting the
   *                              processor corresponding to the [[EndPoint]]. Any endpoint
   *                              that does not appear in this map will be started once all
   *                              authorizerFutures are complete.
   *
   * @return                      A future which is completed when all of the acceptor threads have
   *                              successfully started. If any of them do not start, the future will
   *                              be completed with an exception.
   */
  def enableRequestProcessing(
    authorizerFutures: Map[Endpoint, CompletableFuture[Void]]
  ): CompletableFuture[Void] = this.synchronized {
    if (stopped) {
      throw new RuntimeException("Can't enable request processing: SocketServer is stopped.")
    }

    def chainAcceptorFuture(acceptor: Acceptor): Unit = {
      // Because of ephemeral ports, we need to match acceptors to futures by looking at
      // the listener name, rather than the endpoint object.
      val authorizerFuture = authorizerFutures.find {
        case (endpoint, _) => acceptor.endPoint.listenerName.value().equals(endpoint.listenerName().get())
      } match {
        case None => allAuthorizerFuturesComplete
        case Some((_, future)) => future
      }
      authorizerFuture.whenComplete((_, e) => {
        if (e != null) {
          // If the authorizer failed to start, fail the acceptor's startedFuture.
          acceptor.startedFuture.completeExceptionally(e)
        } else {
          // Once the authorizer has started, attempt to start the associated acceptor. The Acceptor.start()
          // function will complete the acceptor started future (either successfully or not)
          acceptor.start()
        }
      })
    }

    info("Enabling request processing.")
    dataPlaneAcceptors.values().forEach(chainAcceptorFuture)
    FutureUtils.chainFuture(CompletableFuture.allOf(authorizerFutures.values.toArray: _*),
        allAuthorizerFuturesComplete)

    // Construct a future that will be completed when all Acceptors have been successfully started.
    // Alternately, if any of them fail to start, this future will be completed exceptionally.
    val enableFuture = new CompletableFuture[Void]
    FutureUtils.chainFuture(CompletableFuture.allOf(dataPlaneAcceptors.values().asScala.toArray.map(_.startedFuture): _*), enableFuture)
    enableFuture
  }

  private def createDataPlaneAcceptorAndProcessors(endpoint: EndPoint): Unit = synchronized {
    if (stopped) {
      throw new RuntimeException("Can't create new data plane acceptor and processors: SocketServer is stopped.")
    }
    val parsedConfigs = config.valuesFromThisConfigWithPrefixOverride(endpoint.listenerName.configPrefix)
    connectionQuotas.addListener(config, endpoint.listenerName)
    val isPrivilegedListener = config.interBrokerListenerName == endpoint.listenerName
    val dataPlaneAcceptor = createDataPlaneAcceptor(endpoint, isPrivilegedListener, dataPlaneRequestChannel)
    config.addReconfigurable(dataPlaneAcceptor)
    dataPlaneAcceptor.configure(parsedConfigs)
    dataPlaneAcceptors.put(endpoint, dataPlaneAcceptor)
    info(s"Created data-plane acceptor and processors for endpoint : ${endpoint.listenerName}")
  }

  private def endpoints = config.listeners.map(l => l.listenerName -> l).toMap

  protected def createDataPlaneAcceptor(endPoint: EndPoint, isPrivilegedListener: Boolean, requestChannel: RequestChannel): DataPlaneAcceptor = {
    new DataPlaneAcceptor(this, endPoint, config, nodeId, connectionQuotas, time, isPrivilegedListener, requestChannel, metrics, credentialProvider, logContext, memoryPool, apiVersionManager)
  }

  /**
   * Stop processing requests and new connections.
   */
  def stopProcessingRequests(): Unit = synchronized {
    if (!stopped) {
      stopped = true
      info("Stopping socket server request processors")
      dataPlaneAcceptors.asScala.values.foreach(_.beginShutdown())
      dataPlaneAcceptors.asScala.values.foreach(_.close())
      dataPlaneRequestChannel.clear()
      info("Stopped socket server request processors")
    }
  }

  /**
   * Shutdown the socket server. If still processing requests, shutdown
   * acceptors and processors first.
   */
  def shutdown(): Unit = {
    info("Shutting down socket server")
    allAuthorizerFuturesComplete.completeExceptionally(new TimeoutException("The socket " +
      "server was shut down before the Authorizer could be completely initialized."))
    this.synchronized {
      stopProcessingRequests()
      dataPlaneRequestChannel.shutdown()
      connectionQuotas.close()
    }
    info("Shutdown completed")
  }

  def boundPort(listenerName: ListenerName): Int = {
    try {
      val acceptor = dataPlaneAcceptors.get(endpoints(listenerName))
      if (acceptor != null) {
        acceptor.localPort
      } else {
        throw new KafkaException("Could not find listenerName : " + listenerName + " in data-plane.")
      }
    } catch {
      case e: Exception =>
        throw new KafkaException("Tried to check for port of non-existing protocol", e)
    }
  }

  /**
   * This method is called to dynamically add listeners.
   */
  def addListeners(listenersAdded: Seq[EndPoint]): Unit = synchronized {
    if (stopped) {
      throw new RuntimeException("can't add new listeners: SocketServer is stopped.")
    }
    info(s"Adding data-plane listeners for endpoints $listenersAdded")
    listenersAdded.foreach { endpoint =>
      createDataPlaneAcceptorAndProcessors(endpoint)
      val acceptor = dataPlaneAcceptors.get(endpoint)
      // There is no authorizer future for this new listener endpoint. So start the
      // listener once all authorizer futures are complete.
      allAuthorizerFuturesComplete.whenComplete((_, e) => {
        if (e != null) {
          acceptor.startedFuture.completeExceptionally(e)
        } else {
          acceptor.start()
        }
      })
    }
  }

  def removeListeners(listenersRemoved: Seq[EndPoint]): Unit = synchronized {
    info(s"Removing data-plane listeners for endpoints $listenersRemoved")
    listenersRemoved.foreach { endpoint =>
      connectionQuotas.removeListener(config, endpoint.listenerName)
      dataPlaneAcceptors.asScala.remove(endpoint).foreach { acceptor =>
        acceptor.beginShutdown()
        acceptor.close()
        config.removeReconfigurable(acceptor)
      }
    }
  }

  override def reconfigurableConfigs: Set[String] = SocketServer.ReconfigurableConfigs

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {

  }

  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    val maxConnectionsPerIp = newConfig.maxConnectionsPerIp
    if (maxConnectionsPerIp != oldConfig.maxConnectionsPerIp) {
      info(s"Updating maxConnectionsPerIp: $maxConnectionsPerIp")
      connectionQuotas.updateMaxConnectionsPerIp(maxConnectionsPerIp)
    }
    val maxConnectionsPerIpOverrides = newConfig.maxConnectionsPerIpOverrides
    if (maxConnectionsPerIpOverrides != oldConfig.maxConnectionsPerIpOverrides) {
      info(s"Updating maxConnectionsPerIpOverrides: ${maxConnectionsPerIpOverrides.map { case (k, v) => s"$k=$v" }.mkString(",")}")
      connectionQuotas.updateMaxConnectionsPerIpOverride(maxConnectionsPerIpOverrides)
    }
    val maxConnections = newConfig.maxConnections
    if (maxConnections != oldConfig.maxConnections) {
      info(s"Updating broker-wide maxConnections: $maxConnections")
      connectionQuotas.updateBrokerMaxConnections(maxConnections)
    }
    val maxConnectionRate = newConfig.maxConnectionCreationRate
    if (maxConnectionRate != oldConfig.maxConnectionCreationRate) {
      info(s"Updating broker-wide maxConnectionCreationRate: $maxConnectionRate")
      connectionQuotas.updateBrokerMaxConnectionRate(maxConnectionRate)
    }
  }

  // For test usage
  private[network] def connectionCount(address: InetAddress): Int =
    Option(connectionQuotas).fold(0)(_.get(address))

  // For test usage
  def dataPlaneAcceptor(listenerName: String): Option[DataPlaneAcceptor] = {
    dataPlaneAcceptors.asScala.foreach { case (endPoint, acceptor) =>
      if (endPoint.listenerName.value() == listenerName)
        return Some(acceptor)
    }
    None
  }
}

object SocketServer {
  val MetricsGroup = "socket-server-metrics"

  val ReconfigurableConfigs: Set[String] = Set(
    SocketServerConfigs.MAX_CONNECTIONS_PER_IP_CONFIG,
    SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG,
    SocketServerConfigs.MAX_CONNECTIONS_CONFIG,
    SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG)

  val ListenerReconfigurableConfigs: Set[String] = Set(SocketServerConfigs.MAX_CONNECTIONS_CONFIG, SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG)

  def closeSocket(channel: SocketChannel): Unit = {
    Utils.closeQuietly(channel.socket, "channel socket")
    Utils.closeQuietly(channel, "channel")
  }

  /**
   * Builds the F-INT-LOG line emitted once per listener at Acceptor construction. Shape pinned
   * to the resolution-matrix examples in `server/.../iouring/README.md` lines 127-130:
   *
   *   Listener PLAINTEXT://0.0.0.0:9092 resolved to io_uring backend
   *   Listener SSL://0.0.0.0:9093 resolved to nio backend (PLAINTEXT-only contract for io_uring v1)
   *
   * The trailing PLAINTEXT-only annotation appears only when ALL of these hold:
   *   - effective backend is nio,
   *   - listener security protocol is non-PLAINTEXT,
   *   - and the operator did NOT explicitly pin nio (`socket.selector.implementation=nio`).
   *
   * Operator who pinned nio explicitly hasn't been downgraded — adding the io_uring-flavoured
   * annotation to their log line would be confusing. The annotation is reserved for the
   * `auto`/`io_uring` request that fell back to nio because of the v1 PLAINTEXT-only contract.
   *
   * Wildcard binds (host empty or null) display as `0.0.0.0` to match the README example;
   * actual IPv6 wildcard binds with explicit `[::]` would carry through verbatim.
   */
  private[network] def buildResolvedBackendLogLine(
      listenerName: String,
      host: String,
      port: Int,
      securityProtocol: SecurityProtocol,
      requestedBackend: org.apache.kafka.network.iouring.SelectorImplementation,
      effectiveUsesIoUring: Boolean): String = {
    val displayHost = if (Utils.isBlank(host)) "0.0.0.0" else host
    val backend = if (effectiveUsesIoUring) "io_uring" else "nio"
    val annotation =
      if (!effectiveUsesIoUring
          && securityProtocol != SecurityProtocol.PLAINTEXT
          && requestedBackend != org.apache.kafka.network.iouring.SelectorImplementation.NIO) {
        " (PLAINTEXT-only contract for io_uring v1)"
      } else ""
    s"Listener $listenerName://$displayHost:$port resolved to $backend backend$annotation"
  }
}

object DataPlaneAcceptor {
  val ThreadPrefix: String = "data-plane"
  val MetricPrefix: String = ""
  val ListenerReconfigurableConfigs: Set[String] = Set(SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG)
}

class DataPlaneAcceptor(socketServer: SocketServer,
                        endPoint: EndPoint,
                        config: KafkaConfig,
                        nodeId: Int,
                        connectionQuotas: ConnectionQuotas,
                        time: Time,
                        isPrivilegedListener: Boolean,
                        requestChannel: RequestChannel,
                        metrics: Metrics,
                        credentialProvider: CredentialProvider,
                        logContext: LogContext,
                        memoryPool: MemoryPool,
                        apiVersionManager: ApiVersionManager)
  extends Acceptor(socketServer,
                   endPoint,
                   config,
                   nodeId,
                   connectionQuotas,
                   time,
                   isPrivilegedListener,
                   requestChannel,
                   metrics,
                   credentialProvider,
                   logContext,
                   memoryPool,
                   apiVersionManager) with ListenerReconfigurable {

  override def metricPrefix(): String = DataPlaneAcceptor.MetricPrefix
  override def threadPrefix(): String = DataPlaneAcceptor.ThreadPrefix

  /**
   * Returns the listener name associated with this reconfigurable. Listener-specific
   * configs corresponding to this listener name are provided for reconfiguration.
   */
  override def listenerName(): ListenerName = endPoint.listenerName

  /**
   * Returns the names of configs that may be reconfigured.
   */
  override def reconfigurableConfigs(): util.Set[String] = DataPlaneAcceptor.ListenerReconfigurableConfigs.asJava


  /**
   * Validates the provided configuration. The provided map contains
   * all configs including any reconfigurable configs that may be different
   * from the initial configuration. Reconfiguration will be not performed
   * if this method throws any exception.
   *
   * @throws ConfigException if the provided configs are not valid. The exception
   *                         message from ConfigException will be returned to the client in
   *                         the AlterConfigs response.
   */
  override def validateReconfiguration(configs: util.Map[String, _]): Unit = {
    configs.forEach { (k, v) =>
      if (reconfigurableConfigs().contains(k)) {
        val newValue = v.asInstanceOf[Int]
        val oldValue = processors.length
        if (newValue != oldValue) {
          val errorMsg = s"Dynamic thread count update validation failed for $k=$v"
          if (newValue <= 0)
            throw new ConfigException(s"$errorMsg, value should be at least 1")
          if (newValue < oldValue / 2)
            throw new ConfigException(s"$errorMsg, value should be at least half the current value $oldValue")
          if (newValue > oldValue * 2)
            throw new ConfigException(s"$errorMsg, value should not be greater than double the current value $oldValue")
        }
      }
    }
  }

  /**
   * Reconfigures this instance with the given key-value pairs. The provided
   * map contains all configs including any reconfigurable configs that
   * may have changed since the object was initially configured using
   * [[org.apache.kafka.common.Configurable#configure( Map )]]. This method will only be invoked if
   * the configs have passed validation using [[validateReconfiguration( Map )]].
   */
  override def reconfigure(configs: util.Map[String, _]): Unit = {
    val newNumNetworkThreads = configs.get(SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG).asInstanceOf[Int]

    if (newNumNetworkThreads != processors.length) {
      info(s"Resizing network thread pool size for ${endPoint.listenerName} listener from ${processors.length} to $newNumNetworkThreads")
      if (newNumNetworkThreads > processors.length) {
        addProcessors(newNumNetworkThreads - processors.length)
      } else if (newNumNetworkThreads < processors.length) {
        removeProcessors(processors.length - newNumNetworkThreads)
      }
    }
  }

  /**
   * Configure this class with the given key-value pairs
   */
  override def configure(configs: util.Map[String, _]): Unit = {
    addProcessors(configs.get(SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG).asInstanceOf[Int])
  }
}

/**
 * Thread that accepts and configures new connections. There is one of these per endpoint.
 */
private[kafka] abstract class Acceptor(val socketServer: SocketServer,
                                       val endPoint: EndPoint,
                                       var config: KafkaConfig,
                                       nodeId: Int,
                                       val connectionQuotas: ConnectionQuotas,
                                       time: Time,
                                       isPrivilegedListener: Boolean,
                                       requestChannel: RequestChannel,
                                       metrics: Metrics,
                                       credentialProvider: CredentialProvider,
                                       logContext: LogContext,
                                       memoryPool: MemoryPool,
                                       apiVersionManager: ApiVersionManager)
  extends Runnable with Logging {

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  val shouldRun = new AtomicBoolean(true)

  def metricPrefix(): String
  def threadPrefix(): String

  private val sendBufferSize = config.socketSendBufferBytes
  private val recvBufferSize = config.socketReceiveBufferBytes
  private val listenBacklogSize = config.socketListenBacklogSize

  private val nioSelector = NSelector.open()

  // Resolve the effective I/O backend ONCE, eagerly, before any socket-opening side effect
  // — the 3-arg overload applies the port=0 downgrade contract:
  //   - auto + port=0 → silent NIO downgrade (returns false).
  //   - explicit io_uring + port=0 → hard-fail (throws IllegalStateException right here).
  // Eager evaluation matters because the wildcard NIO pre-open below would otherwise leak a
  // file descriptor on the hard-fail path: it runs from the var-initializer block, and a
  // throw from a later usesIoUring lookup would propagate out of the constructor *after*
  // serverChannel was bound, with no `closeAll` ever running (the Acceptor thread never
  // starts on a failed construct).
  private val effectiveUsesIoUring: Boolean =
    config.usesIoUring(endPoint.listenerName, endPoint.securityProtocol, endPoint.port)

  // If the port is configured as 0, we are using a wildcard port, so we need to open the socket
  // before we can find out what port we have. If it is set to a nonzero value, defer opening
  // the socket until we start the Acceptor. The reason for deferring the socket opening is so
  // that systems which assume that the socket being open indicates readiness are not confused.
  // Skipped on the io_uring path (effectiveUsesIoUring is true only for explicit non-zero
  // ports — see above), where the per-Processor IoUringServerListener does the bind via Netty.
  private[network] var serverChannel: ServerSocketChannel  = _
  private[network] val localPort: Int  = if (endPoint.port != 0) {
    endPoint.port
  } else if (!effectiveUsesIoUring) {
    serverChannel = openServerSocket(endPoint.host, endPoint.port, listenBacklogSize)
    val newPort = serverChannel.socket().getLocalPort
    info(s"Opened wildcard endpoint ${endPoint.host}:$newPort")
    newPort
  } else {
    // Unreachable: the 3-arg usesIoUring throws on explicit io_uring + port=0, and the
    // auto + port=0 case downgrades to NIO so effectiveUsesIoUring is false. This branch
    // is defensive — if a future change adds a third resolution outcome, the broker fails
    // fast with the same operator-actionable shape rather than silently leaking a port=0.
    throw new IllegalStateException(
      s"io_uring listener ${endPoint.listenerName} resolved with port=0 (wildcard); " +
      "SO_REUSEPORT sharding requires every Processor to bind on the same explicit port.")
  }

  // One-shot, operator-visible record of which I/O backend the resolver picked for this
  // listener. Without this line a wildcard "io_uring auto" config can silently fall through
  // to NIO (e.g. kernel without io_uring, or a non-PLAINTEXT listener that v1 forbids on the
  // io_uring path) and operators have no signal that a tuning intent was downgraded —
  // every existing log line refers to the listener by name only. Shape pinned to the README
  // resolution-matrix examples by SocketServer.buildResolvedBackendLogLine (covered by
  // SocketServerFIntLogTest). socketSelectorImplementationFor honors the per-listener
  // `listener.name.<name>.socket.selector.implementation` override and falls back to the
  // broker-wide value, so the annotation logic sees the request view actually applied to
  // this listener.
  info(SocketServer.buildResolvedBackendLogLine(
    endPoint.listenerName.value, endPoint.host, localPort, endPoint.securityProtocol,
    config.socketSelectorImplementationFor(endPoint.listenerName), effectiveUsesIoUring))

  private[network] val processors = new ArrayBuffer[Processor]()
  // Build the metric name explicitly in order to keep the existing name for compatibility
  private val backwardCompatibilityMetricGroup = new KafkaMetricsGroup("kafka.network", "Acceptor")
  private val blockedPercentMeterMetricName = backwardCompatibilityMetricGroup.metricName(
    s"${metricPrefix()}AcceptorBlockedPercent",
    Map(ListenerMetricTag -> endPoint.listenerName.value).asJava)
  private val blockedPercentMeter = metricsGroup.newMeter(blockedPercentMeterMetricName,"blocked time", TimeUnit.NANOSECONDS)
  // Exposed so Processor.applyConnectionQuotasForNewlyAcceptedChannels can call
  // ConnectionQuotas.inc on the io_uring accept path (where the Acceptor doesn't run
  // inc(), because Netty's event loop handed the channel straight to the Processor).
  private[network] def acceptorBlockedPercentMeter: com.yammer.metrics.core.Meter = blockedPercentMeter
  private var currentProcessorIndex = 0
  private[network] val throttledSockets = new mutable.PriorityQueue[DelayedCloseSocket]()
  private val started = new AtomicBoolean()
  private[network] val startedFuture = new CompletableFuture[Void]()

  val thread: KafkaThread = KafkaThread.nonDaemon(
    s"${threadPrefix()}-kafka-socket-acceptor-${endPoint.listenerName}-${endPoint.securityProtocol}-${endPoint.port}",
    this)

  def start(): Unit = synchronized {
    try {
      if (!shouldRun.get()) {
        throw new ClosedChannelException()
      }
      // io_uring listeners bind via the per-Processor IoUringServerListener (SO_REUSEPORT),
      // so the Acceptor must NOT open a NIO ServerSocketChannel on the same address — doing
      // so would either fail with EADDRINUSE (if the Processor bound first) or steal accepts
      // away from the io_uring path. The Acceptor thread still starts, but its run loop
      // skips the NIO accept work for io_uring listeners.
      if (serverChannel == null && !usesIoUring) {
        serverChannel = openServerSocket(endPoint.host, endPoint.port, listenBacklogSize)
        debug(s"Opened endpoint ${endPoint.host}:${endPoint.port}")
      }
      debug(s"Starting processors for listener ${endPoint.listenerName}")
      processors.foreach(_.start())
      debug(s"Starting acceptor thread for listener ${endPoint.listenerName}")
      thread.start()
      startedFuture.complete(null)
      started.set(true)
    } catch {
      case e: ClosedChannelException =>
        debug(s"Refusing to start acceptor for ${endPoint.listenerName} since the acceptor has already been shut down.")
        startedFuture.completeExceptionally(e)
      case t: Throwable =>
        error(s"Unable to start acceptor for ${endPoint.listenerName}", t)
        startedFuture.completeExceptionally(new RuntimeException(s"Unable to start acceptor for ${endPoint.listenerName}", t))
    }
  }

  private[network] case class DelayedCloseSocket(socket: SocketChannel, endThrottleTimeMs: Long) extends Ordered[DelayedCloseSocket] {
    override def compare(that: DelayedCloseSocket): Int = endThrottleTimeMs compare that.endThrottleTimeMs
  }

  private[network] def removeProcessors(removeCount: Int): Unit = synchronized {
    // Shutdown `removeCount` processors. Remove them from the processor list first so that no more
    // connections are assigned. Shutdown the removed processors, closing the selector and its connections.
    // The processors are then removed from `requestChannel` and any pending responses to these processors are dropped.
    val toRemove = processors.takeRight(removeCount)
    processors.remove(processors.size - removeCount, removeCount)
    toRemove.foreach(_.close())
    toRemove.foreach(processor => requestChannel.removeProcessor(processor.id))
  }

  def beginShutdown(): Unit = {
    if (shouldRun.getAndSet(false)) {
      wakeup()
      synchronized {
        processors.foreach(_.beginShutdown())
      }
    }
  }

  def close(): Unit = {
    beginShutdown()
    thread.join()
    if (!started.get) {
      closeAll()
    }
    synchronized {
      processors.foreach(_.close())
    }
  }

  /**
   * Accept loop that checks for new connection attempts. For io_uring listeners the per-
   * Processor IoUringServerListener owns the accept loop via Netty, so the Acceptor thread
   * has nothing to dispatch — it just blocks on the nioSelector with a timeout so wakeup()
   * during shutdown still returns promptly.
   */
  override def run(): Unit = {
    val ioUring = usesIoUring
    if (!ioUring) {
      serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)
    }
    try {
      while (shouldRun.get()) {
        try {
          if (ioUring) {
            nioSelector.select(500)
            nioSelector.selectedKeys().clear()
          } else {
            acceptNewConnections()
          }
          closeThrottledConnections()
        }
        catch {
          // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
          // to a select operation on a specific channel or a bad request. We don't want
          // the broker to stop responding to requests from other clients in these scenarios.
          case e: ControlThrowable => throw e
          case e: Throwable => error("Error occurred", e)
        }
      }
    } finally {
      closeAll()
    }
  }

  /**
   * Whether this Acceptor's listener is served by the io_uring backend. Returns the
   * eagerly-resolved value computed once at construction (see effectiveUsesIoUring).
   * Eager resolution ensures the port=0 hard-fail (explicit io_uring) and silent
   * downgrade (auto) both run before the wildcard pre-open, so the NIO socket is
   * never leaked on the hard-fail path.
   */
  private[network] def usesIoUring: Boolean = effectiveUsesIoUring

  private def closeAll(): Unit = {
    debug("Closing server socket, selector, and any throttled sockets.")
    // The serverChannel will be null if Acceptor's thread is not started
    Utils.closeQuietly(serverChannel, "Acceptor serverChannel")
    Utils.closeQuietly(nioSelector, "Acceptor nioSelector")
    throttledSockets.foreach(throttledSocket => closeSocket(throttledSocket.socket))
    throttledSockets.clear()
  }

  /**
   * Create a server socket to listen for connections on.
   */
  private def openServerSocket(host: String, port: Int, listenBacklogSize: Int): ServerSocketChannel = {
    val socketAddress = if (Utils.isBlank(host)) {
      new InetSocketAddress(port)
    } else {
      new InetSocketAddress(host, port)
    }
    val serverChannel = socketServer.socketFactory.openServerSocket(
      endPoint.listenerName.value(),
      socketAddress,
      listenBacklogSize,
      recvBufferSize)
    info(s"Awaiting socket connections on ${socketAddress.getHostString}:${serverChannel.socket.getLocalPort}.")
    serverChannel
  }

  /**
   * Listen for new connections and assign accepted connections to processors using round-robin.
   */
  private def acceptNewConnections(): Unit = {
    val ready = nioSelector.select(500)
    if (ready > 0) {
      val keys = nioSelector.selectedKeys()
      val iter = keys.iterator()
      while (iter.hasNext && shouldRun.get()) {
        try {
          val key = iter.next
          iter.remove()

          if (key.isAcceptable) {
            accept(key).foreach { socketChannel =>
              // Assign the channel to the next processor (using round-robin) to which the
              // channel can be added without blocking. If newConnections queue is full on
              // all processors, block until the last one is able to accept a connection.
              var retriesLeft = synchronized(processors.length)
              var processor: Processor = null
              do {
                retriesLeft -= 1
                processor = synchronized {
                  // adjust the index (if necessary) and retrieve the processor atomically for
                  // correct behaviour in case the number of processors is reduced dynamically
                  currentProcessorIndex = currentProcessorIndex % processors.length
                  processors(currentProcessorIndex)
                }
                currentProcessorIndex += 1
              } while (!assignNewConnection(socketChannel, processor, retriesLeft == 0))
            }
          } else
            throw new IllegalStateException("Unrecognized key state for acceptor thread.")
        } catch {
          case e: Throwable => error("Error while accepting connection", e)
        }
      }
    }
  }

  /**
   * Accept a new connection
   */
  private def accept(key: SelectionKey): Option[SocketChannel] = {
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val socketChannel = serverSocketChannel.accept()
    try {
      connectionQuotas.inc(endPoint.listenerName, socketChannel.socket.getInetAddress, blockedPercentMeter)
      configureAcceptedSocketChannel(socketChannel)
      Some(socketChannel)
    } catch {
      case e: TooManyConnectionsException =>
        info(s"Rejected connection from ${e.ip}, address already has the configured maximum of ${e.count} connections.")
        connectionQuotas.closeChannel(this, endPoint.listenerName, socketChannel)
        None
      case e: ConnectionThrottledException =>
        val ip = socketChannel.socket.getInetAddress
        debug(s"Delaying closing of connection from $ip for ${e.throttleTimeMs} ms")
        val endThrottleTimeMs = e.startThrottleTimeMs + e.throttleTimeMs
        throttledSockets += DelayedCloseSocket(socketChannel, endThrottleTimeMs)
        None
      case e: IOException =>
        error(s"Encountered an error while configuring the connection, closing it.", e)
        connectionQuotas.closeChannel(this, endPoint.listenerName, socketChannel)
        None
    }
  }

  protected def configureAcceptedSocketChannel(socketChannel: SocketChannel): Unit = {
    socketChannel.configureBlocking(false)
    socketChannel.socket().setTcpNoDelay(true)
    socketChannel.socket().setKeepAlive(true)
    if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
      socketChannel.socket().setSendBufferSize(sendBufferSize)
  }

  /**
   * Close sockets for any connections that have been throttled.
   */
  private def closeThrottledConnections(): Unit = {
    val timeMs = time.milliseconds
    while (throttledSockets.headOption.exists(_.endThrottleTimeMs < timeMs)) {
      val closingSocket = throttledSockets.dequeue()
      debug(s"Closing socket from ip ${closingSocket.socket.getRemoteAddress}")
      closeSocket(closingSocket.socket)
    }
  }

  private def assignNewConnection(socketChannel: SocketChannel, processor: Processor, mayBlock: Boolean): Boolean = {
    if (processor.accept(socketChannel, mayBlock, blockedPercentMeter)) {
      debug(s"Accepted connection from ${socketChannel.socket.getRemoteSocketAddress} on" +
        s" ${socketChannel.socket.getLocalSocketAddress} and assigned it to processor ${processor.id}," +
        s" sendBufferSize [actual|requested]: [${socketChannel.socket.getSendBufferSize}|$sendBufferSize]" +
        s" recvBufferSize [actual|requested]: [${socketChannel.socket.getReceiveBufferSize}|$recvBufferSize]")
      true
    } else
      false
  }

  /**
   * Wakeup the thread for selection.
   */
  def wakeup(): Unit = nioSelector.wakeup()

  def addProcessors(toCreate: Int): Unit = synchronized {
    val listenerName = endPoint.listenerName
    val securityProtocol = endPoint.securityProtocol
    val listenerProcessors = new ArrayBuffer[Processor]()

    for (_ <- 0 until toCreate) {
      val processor = newProcessor(socketServer.nextProcessorId(), listenerName, securityProtocol, socketServer.connectionDisconnectListeners)
      listenerProcessors += processor
      requestChannel.addProcessor(processor)

      if (started.get) {
        processor.start()
      }
    }
    processors ++= listenerProcessors
  }

  def newProcessor(id: Int,
                   listenerName: ListenerName,
                   securityProtocol: SecurityProtocol,
                   connectionDisconnectListeners: Seq[ConnectionDisconnectListener]): Processor = {
    val name = s"${threadPrefix()}-kafka-network-thread-$nodeId-${endPoint.listenerName}-${endPoint.securityProtocol}-$id"
    new Processor(id,
                  time,
                  config.socketRequestMaxBytes,
                  requestChannel,
                  connectionQuotas,
                  config.connectionsMaxIdleMs,
                  config.failedAuthenticationDelayMs,
                  listenerName,
                  securityProtocol,
                  endPoint,
                  config,
                  metrics,
                  credentialProvider,
                  memoryPool,
                  logContext,
                  Processor.ConnectionQueueSize,
                  isPrivilegedListener,
                  apiVersionManager,
                  name,
                  connectionDisconnectListeners,
                  acceptorBlockedPercentMeter)
  }
}

/**
 * The pair of I/O objects a Processor owns. For NIO listeners {@code listener} is None and
 * the Acceptor binds the server socket. For io_uring listeners {@code listener} is Some and
 * holds the per-Processor IoUringServerListener (bound on the same address via
 * SO_REUSEPORT). Closing the bundle tears the listener down before the selector so accepted
 * channels still flow through {@code onDisconnect} before the selector is gone.
 */
private[network] case class ProcessorIoBundle(
  selector: BrokerSelector,
  listener: Option[org.apache.kafka.network.iouring.IoUringServerListener]) {
  def close(): Unit = {
    listener.foreach(l => try l.close() catch { case _: Throwable => /* logged downstream */ })
    selector.close()
  }
}

private[kafka] object Processor {
  private val IdlePercentMetricName = "IdlePercent"
  val NetworkProcessorMetricTag = "networkProcessor"
  val ListenerMetricTag = "listener"
  val ConnectionQueueSize = 20

  private[network] def parseRequestHeader(apiVersionManager: ApiVersionManager, buffer: ByteBuffer): RequestHeader = {
    val header = RequestHeader.parse(buffer)
    if (apiVersionManager.isApiEnabled(header.apiKey, header.apiVersion)) {
      header
    } else if (header.isApiVersionSupported()) {
      throw new InvalidRequestException(s"Received request for disabled api with key ${header.apiKey.id} (${header.apiKey().name}) and version ${header.apiVersion}")
    } else {
      throw new UnsupportedVersionException(s"Received request for api with key ${header.apiKey.id} (${header.apiKey().name}) and unsupported version ${header.apiVersion}")
    }
  }
}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selector
 *
 * @param isPrivilegedListener The privileged listener flag is used as one factor to determine whether
 *                             a certain request is forwarded or not. When the control plane is defined,
 *                             the control plane processor would be fellow broker's choice for sending
 *                             forwarding requests; if the control plane is not defined, the processor
 *                             relying on the inter broker listener would be acting as the privileged listener.
 */
private[kafka] class Processor(
  val id: Int,
  time: Time,
  maxRequestSize: Int,
  requestChannel: RequestChannel,
  connectionQuotas: ConnectionQuotas,
  connectionsMaxIdleMs: Long,
  failedAuthenticationDelayMs: Int,
  listenerName: ListenerName,
  securityProtocol: SecurityProtocol,
  // Bind address for this listener — used by the io_uring path so each Processor can stand
  // up its own IoUringServerListener on the configured port via SO_REUSEPORT. Unused on the
  // NIO path (where the Acceptor binds once and hands SocketChannels to register()).
  endPoint: EndPoint,
  config: KafkaConfig,
  metrics: Metrics,
  credentialProvider: CredentialProvider,
  memoryPool: MemoryPool,
  logContext: LogContext,
  connectionQueueSize: Int,
  isPrivilegedListener: Boolean,
  apiVersionManager: ApiVersionManager,
  threadName: String,
  connectionDisconnectListeners: Seq[ConnectionDisconnectListener],
  // Acceptor-owned Yammer meter that gates connectionQuotas.inc. The Acceptor uses it on the
  // NIO accept path; the Processor needs it on the io_uring path because that's where inc()
  // runs (Netty's event loop is upstream of us, not Acceptor.accept).
  acceptorBlockedPercentMeter: com.yammer.metrics.core.Meter
) extends Runnable with Logging {
  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  val shouldRun: AtomicBoolean = new AtomicBoolean(true)
  private val started: AtomicBoolean = new AtomicBoolean()

  val thread: KafkaThread = KafkaThread.nonDaemon(threadName, this)

  private val newConnections = new ArrayBlockingQueue[SocketChannel](connectionQueueSize)
  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()
  private val responseQueue = new LinkedBlockingDeque[RequestChannel.Response]()

  private[kafka] val metricTags = mutable.LinkedHashMap(
    ListenerMetricTag -> listenerName.value,
    NetworkProcessorMetricTag -> id.toString
  ).asJava

  metricsGroup.newGauge(IdlePercentMetricName, () => {
    Option(metrics.metric(metrics.metricName("io-wait-ratio", MetricsGroup, metricTags))).fold(0.0)(m =>
      Math.min(m.metricValue.asInstanceOf[Double], 1.0))
  },
    // for compatibility, only add a networkProcessor tag to the Yammer Metrics alias (the equivalent Selector metric
    // also includes the listener name)
    Map(NetworkProcessorMetricTag -> id.toString).asJava
  )

  private val expiredConnectionsKilledCount = new CumulativeSum()
  private val expiredConnectionsKilledCountMetricName = metrics.metricName("expired-connections-killed-count", MetricsGroup, metricTags)
  metrics.addMetric(expiredConnectionsKilledCountMetricName, expiredConnectionsKilledCount)

  // The selector is a BrokerSelector so an alternative I/O backend (e.g. Netty io_uring)
  // can be swapped in without modifying clients/. The default path keeps using the Java-NIO
  // KSelector — we just wrap it in a thin NioBrokerSelector adapter so Processor talks to a
  // single broker-side type.
  //
  // For listeners resolved to io_uring (Linux + PLAINTEXT + config knob), we construct an
  // IoUringSelector + IoUringServerListener pair instead. The listener binds the same address
  // as the Acceptor's endPoint via SO_REUSEPORT (the kernel shards accepts across all
  // io_uring Processors on this listener), and the Acceptor's own NIO bind+accept is skipped
  // — see Acceptor.start()/run() guards.
  private[network] val ioBundle: ProcessorIoBundle = buildIoBundle()
  private[network] val selector: BrokerSelector = ioBundle.selector

  private def buildIoBundle(): ProcessorIoBundle = {
    // Port-aware usesIoUring: returns false for auto + port=0 (silent NIO downgrade),
    // and throws for explicit io_uring + port=0 — but in the latter case the Acceptor
    // constructor already threw long before this Processor instantiated. The remaining
    // branch here is the happy path (explicit non-zero port that resolved to io_uring).
    if (config.usesIoUring(listenerName, securityProtocol, endPoint.port)) {
      // Threaded to IoUringPlaintextAuthenticator so a user-configured PRINCIPAL_BUILDER_CLASS_CONFIG
      // is honored on this listener — mirroring the NIO PlaintextChannelBuilder path which calls
      // configure(channelBuilderConfigs(config, listenerName)).
      val ioUringChannelConfigs = ioUringChannelBuilderConfigs()
      val ioUringSelector = new org.apache.kafka.network.iouring.IoUringSelector(
        listenerName,
        maxRequestSize,
        memoryPool,
        java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(connectionsMaxIdleMs),
        time,
        id,
        ioUringChannelConfigs)
      // Mirror Acceptor.openServerSocket (SocketServer.scala:657-662): a wildcard listener
      // configured as PLAINTEXT://:9092 has endPoint.host == null/blank, and
      // InetSocketAddress(null, port) throws IllegalArgumentException. We must use the
      // wildcard form for blank hosts so the io_uring path binds 0.0.0.0:port, matching
      // NIO. Without this guard the default config (auto + Linux + PLAINTEXT://:9092)
      // crashes broker startup before any connection is accepted.
      val bindAddress =
        if (Utils.isBlank(endPoint.host)) new java.net.InetSocketAddress(endPoint.port)
        else new java.net.InetSocketAddress(endPoint.host, endPoint.port)
      // Apply socket.send.buffer.bytes/socket.receive.buffer.bytes so io_uring matches the
      // NIO Acceptor parity at SocketServer.scala:740-746 / openServerSocket(..., recvBuf).
      // Without this, an operator-tuned SO_SNDBUF/SO_RCVBUF silently has no effect on the
      // io_uring listener and per-connection throughput diverges from the NIO baseline on
      // the same broker.
      val listener = new org.apache.kafka.network.iouring.IoUringServerListener(
        bindAddress,
        ioUringSelector,
        config.socketListenBacklogSize,
        config.socketSendBufferBytes,
        config.socketReceiveBufferBytes)
      ProcessorIoBundle(ioUringSelector, Some(listener))
    } else {
      ProcessorIoBundle(new NioBrokerSelector(createSelector(
        ChannelBuilders.serverChannelBuilder(
          listenerName,
          listenerName == config.interBrokerListenerName,
          securityProtocol,
          config,
          credentialProvider.credentialCache,
          credentialProvider.tokenCache,
          time,
          logContext,
          version => apiVersionManager.apiVersionResponse(throttleTimeMs = 0, version < 4)
        )
      )), None)
    }
  }

  /**
   * Build the configs map handed to IoUringPlaintextAuthenticator's principal builder,
   * mirroring ChannelBuilders.channelBuilderConfigs (package-private under clients/).
   *
   * The NIO PlaintextChannelBuilder receives a map that merges
   * config.valuesWithPrefixOverride(listenerPrefix) (the parsed view, with per-listener
   * overrides applied) with config.originals() (the raw user-provided keys, including any
   * keys NOT in the schema such as principal.builder.* custom keys). A Configurable
   * principal builder reads this combined map in configure().
   *
   * If the io_uring path only forwarded valuesWithPrefixOverride, a custom builder whose
   * configure() reads non-schema keys would see them on a NIO listener but not on an
   * io_uring listener — a silent transport-level divergence. Replicate the merge here so
   * the two transports agree on what the user said.
   */
  private def ioUringChannelBuilderConfigs(): java.util.Map[String, _] = {
    val parsedConfigs = new java.util.HashMap[String, AnyRef]()
    parsedConfigs.putAll(config.valuesWithPrefixOverride(listenerName.configPrefix))
    val listenerPrefix = listenerName.configPrefix
    config.originals.entrySet.forEach { e =>
      val key = e.getKey
      // Skip: already in parsed map.
      if (parsedConfigs.containsKey(key)) {
        // no-op
      // Skip: listener-prefixed duplicate where the unprefixed key is already parsed.
      } else if (key.startsWith(listenerPrefix) &&
                 parsedConfigs.containsKey(key.substring(listenerPrefix.length))) {
        // no-op
      } else {
        // Mirrors ChannelBuilders.channelBuilderConfigs' third filter: drop keys whose
        // first-dot-stripped suffix is already in parsed (e.g. mechanism-prefixed keys
        // like "PLAIN.some.prop" when "some.prop" exists).
        val dotIdx = key.indexOf('.')
        if (dotIdx >= 0 && parsedConfigs.containsKey(key.substring(dotIdx + 1))) {
          // no-op
        } else {
          parsedConfigs.put(key, e.getValue)
        }
      }
    }
    parsedConfigs
  }

  // Visible to override for testing. Returns the underlying Java-NIO KSelector so existing
  // TestableSelector overrides keep working unchanged — the Processor wraps it above.
  protected[network] def createSelector(channelBuilder: ChannelBuilder): KSelector = {
    channelBuilder match {
      case reconfigurable: Reconfigurable => config.addReconfigurable(reconfigurable)
      case _ =>
    }
    new KSelector(
      maxRequestSize,
      connectionsMaxIdleMs,
      failedAuthenticationDelayMs,
      metrics,
      time,
      "socket-server",
      metricTags,
      false,
      true,
      channelBuilder,
      memoryPool,
      logContext)
  }

  // Connection ids have the format `localAddr:localPort-remoteAddr:remotePort-index`. The index is a
  // non-negative incrementing value that ensures that even if remotePort is reused after a connection is
  // closed, connection ids are not reused while requests from the closed connection are being processed.
  private var nextConnectionIndex = 0

  override def run(): Unit = {
    try {
      while (shouldRun.get()) {
        try {
          // setup any new connections that have been queued up
          configureNewConnections()
          // register any new responses for writing
          processNewResponses()
          poll()
          // io_uring listeners accept on the Netty event loop, so ConnectionQuotas.inc
          // happens here (after poll) rather than in Acceptor.accept. NIO listeners have
          // already had inc applied by the Acceptor, and broker-side selector.connected()
          // is empty for them — so this is effectively a no-op on the NIO path.
          applyConnectionQuotasForNewlyAcceptedChannels()
          processCompletedReceives()
          processCompletedSends()
          processDisconnected()
          closeExcessConnections()
        } catch {
          // We catch all the throwables here to prevent the processor thread from exiting. We do this because
          // letting a processor exit might cause a bigger impact on the broker. This behavior might need to be
          // reviewed if we see an exception that needs the entire broker to stop. Usually the exceptions thrown would
          // be either associated with a specific socket channel or a bad request. These exceptions are caught and
          // processed by the individual methods above which close the failing channel and continue processing other
          // channels. So this catch block should only ever see ControlThrowables.
          case e: Throwable => processException("Processor got uncaught exception.", e)
        }
      }
    } finally {
      debug(s"Closing selector - processor $id")
      CoreUtils.swallow(closeAll(), this, Level.ERROR)
    }
  }

  private[network] def processException(errorMessage: String, throwable: Throwable): Unit = {
    throwable match {
      case e: ControlThrowable => throw e
      case e => error(errorMessage, e)
    }
  }

  private def processChannelException(channelId: String, errorMessage: String, throwable: Throwable): Unit = {
    if (openOrClosingChannel(channelId).isDefined) {
      error(s"Closing socket for $channelId because of error", throwable)
      close(channelId)
    }
    processException(errorMessage, throwable)
  }

  private def processNewResponses(): Unit = {
    var currentResponse: RequestChannel.Response = null
    while ({currentResponse = dequeueResponse(); currentResponse != null}) {
      val channelId = currentResponse.request.context.connectionId
      try {
        currentResponse match {
          case response: NoOpResponse =>
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer
            updateRequestMetrics(response)
            trace(s"Socket server received empty response to send, registering for read: $response")
            // Try unmuting the channel. If there was no quota violation and the channel has not been throttled,
            // it will be unmuted immediately. If the channel has been throttled, it will be unmuted only if the
            // throttling delay has already passed by now.
            handleChannelMuteEvent(channelId, ChannelMuteEvent.RESPONSE_SENT)
            tryUnmuteChannel(channelId)

          case response: SendResponse =>
            sendResponse(response, response.responseSend)
          case response: CloseConnectionResponse =>
            updateRequestMetrics(response)
            trace("Closing socket connection actively according to the response code.")
            close(channelId)
          case _: StartThrottlingResponse =>
            handleChannelMuteEvent(channelId, ChannelMuteEvent.THROTTLE_STARTED)
          case _: EndThrottlingResponse =>
            // Try unmuting the channel. The channel will be unmuted only if the response has already been sent out to
            // the client.
            handleChannelMuteEvent(channelId, ChannelMuteEvent.THROTTLE_ENDED)
            tryUnmuteChannel(channelId)
          case _ =>
            throw new IllegalArgumentException(s"Unknown response type: ${currentResponse.getClass}")
        }
      } catch {
        case e: Throwable =>
          processChannelException(channelId, s"Exception while processing response for $channelId", e)
      }
    }
  }

  // `protected` for test usage
  protected[network] def sendResponse(response: RequestChannel.Response, responseSend: Send): Unit = {
    val connectionId = response.request.context.connectionId
    trace(s"Socket server received response to send to $connectionId, registering for write and sending data: $response")
    // `channel` can be None if the connection was closed remotely or if selector closed it for being idle for too long
    if (channel(connectionId).isEmpty) {
      warn(s"Attempting to send response via channel for which there is no open connection, connection id $connectionId")
      response.request.updateRequestMetrics(0L, response)
    }
    // Invoke send for closingChannel as well so that the send is failed and the channel closed properly and
    // removed from the Selector after discarding any pending staged receives.
    // `openOrClosingChannel` can be None if the selector closed the connection because it was idle for too long
    if (openOrClosingChannel(connectionId).isDefined) {
      selector.send(new NetworkSend(connectionId, responseSend))
      inflightResponses += (connectionId -> response)
    }
  }

  private def poll(): Unit = {
    val pollTimeout = if (newConnections.isEmpty) 300 else 0
    try selector.poll(pollTimeout)
    catch {
      case e @ (_: IllegalStateException | _: IOException) =>
        // The exception is not re-thrown and any completed sends/receives/connections/disconnections
        // from this poll will be processed.
        error(s"Processor $id poll failed", e)
    }
  }

  private def processCompletedReceives(): Unit = {
    selector.completedReceives.forEach { receive =>
      try {
        openOrClosingChannel(receive.source) match {
          case Some(channel) =>
            val header = parseRequestHeader(apiVersionManager, receive.payload)
            if (header.apiKey == ApiKeys.SASL_HANDSHAKE && channel.maybeBeginServerReauthentication(receive,
              () => time.nanoseconds()))
              trace(s"Begin re-authentication: $channel")
            else {
              val nowNanos = time.nanoseconds()
              if (channel.serverAuthenticationSessionExpired(nowNanos)) {
                // be sure to decrease connection count and drop any in-flight responses
                debug(s"Disconnecting expired channel: $channel : $header")
                close(channel.id)
                expiredConnectionsKilledCount.record(null, 1, 0)
              } else {
                val connectionId = receive.source
                val context = new RequestContext(header, connectionId, channel.socketAddress, Optional.of(channel.socketPort()),
                  channel.principal, listenerName, securityProtocol, channel.channelMetadataRegistry.clientInformation,
                  isPrivilegedListener, channel.principalSerde)

                val req = new RequestChannel.Request(processor = id, context = context,
                  startTimeNanos = nowNanos, memoryPool, receive.payload, requestChannel.metrics, None)

                // KIP-511: ApiVersionsRequest is intercepted here to catch the client software name
                // and version. It is done here to avoid wiring things up to the api layer.
                if (header.apiKey == ApiKeys.API_VERSIONS) {
                  val apiVersionsRequest = req.body[ApiVersionsRequest]
                  if (apiVersionsRequest.isValid) {
                    channel.channelMetadataRegistry.registerClientInformation(new ClientInformation(
                      apiVersionsRequest.data.clientSoftwareName,
                      apiVersionsRequest.data.clientSoftwareVersion))
                  }
                }
                requestChannel.sendRequest(req)
                selector.mute(connectionId)
                handleChannelMuteEvent(connectionId, ChannelMuteEvent.REQUEST_RECEIVED)
              }
            }
          case None =>
            // This should never happen since completed receives are processed immediately after `poll()`
            throw new IllegalStateException(s"Channel ${receive.source} removed from selector before processing completed receive")
        }
      } catch {
        // note that even though we got an exception, we can assume that receive.source is valid.
        // Issues with constructing a valid receive object were handled earlier
        case e: Throwable =>
          processChannelException(receive.source, s"Exception while processing request from ${receive.source}", e)
      }
    }
    selector.clearCompletedReceives()
  }

  private def processCompletedSends(): Unit = {
    selector.completedSends.forEach { send =>
      try {
        val response = inflightResponses.remove(send.destinationId).getOrElse {
          throw new IllegalStateException(s"Send for ${send.destinationId} completed, but not in `inflightResponses`")
        }

        // Invoke send completion callback, and then update request metrics since there might be some
        // request metrics got updated during callback
        response.onComplete.foreach(onComplete => onComplete(send))
        updateRequestMetrics(response)

        // Try unmuting the channel. If there was no quota violation and the channel has not been throttled,
        // it will be unmuted immediately. If the channel has been throttled, it will unmuted only if the throttling
        // delay has already passed by now.
        handleChannelMuteEvent(send.destinationId, ChannelMuteEvent.RESPONSE_SENT)
        tryUnmuteChannel(send.destinationId)
      } catch {
        case e: Throwable => processChannelException(send.destinationId,
          s"Exception while processing completed send to ${send.destinationId}", e)
      }
    }
    selector.clearCompletedSends()
  }

  private def updateRequestMetrics(response: RequestChannel.Response): Unit = {
    val request = response.request
    val networkThreadTimeNanos = openOrClosingChannel(request.context.connectionId).fold(0L)(_.getAndResetNetworkThreadTimeNanos())
    request.updateRequestMetrics(networkThreadTimeNanos, response)
  }

  /**
   * Apply ConnectionQuotas.inc for channels that just surfaced from selector.connected().
   * io_uring listeners accept inside Netty's event loop and never went through
   * Acceptor.accept (which is where NIO inc'd the quota), so the gate has to land here
   * instead.
   *
   * Refusal paths:
   *  - tryInc returns false (broker max-connections hit, or listener connection-rate
   *    throttle): no inc happened, so we just close the selector channel.
   *  - TooManyConnectionsException: tryInc increments the per-IP / listener / total
   *    counters BEFORE checking the per-IP max (see ConnectionQuotas.tryInc), so by
   *    the time the exception fires the inc has already landed. selector.close on the
   *    io_uring path is silent (it does NOT surface in selector.disconnected, so
   *    processDisconnected will not run the normal dec). Roll the inc back here.
   *  - ConnectionThrottledException: thrown by recordIpConnectionMaybeThrottle, which
   *    runs before the inc, so no rollback is needed.
   *
   * For NIO listeners selector.connected() is always empty (broker-side Processors don't
   * initiate outgoing connections), so this method is a hot-path no-op there.
   */
  private def applyConnectionQuotasForNewlyAcceptedChannels(): Unit = {
    val newlyConnected = selector.connected()
    if (newlyConnected.isEmpty) return
    newlyConnected.forEach { connectionId =>
      val channel = selector.channel(connectionId)
      if (channel != null) {
        val address = channel.socketAddress
        if (address != null) {
          try {
            // tryInc — never blocks. On NIO the dedicated Acceptor thread can wait for a
            // slot via inc(); here the caller is the Processor's poll loop, so a wait
            // would stall every other channel on this Processor.
            if (!connectionQuotas.tryInc(listenerName, address)) {
              info(s"Closing io_uring connection $connectionId from $address: broker- or " +
                s"listener-level connection slot unavailable or rate-limited.")
              selector.close(connectionId)
            }
          } catch {
            case e: TooManyConnectionsException =>
              info(s"Closing io_uring connection $connectionId from ${e.ip}: " +
                s"already has the configured maximum of ${e.count} connections.")
              // tryInc incremented counts before throwing; selector.close is silent on
              // the io_uring path, so processDisconnected will not run dec. Roll back
              // the inc explicitly, otherwise the per-IP / listener / total counters
              // leak permanently and the next refusal compares against a stale count.
              connectionQuotas.dec(listenerName, address)
              selector.close(connectionId)
            case e: ConnectionThrottledException =>
              debug(s"Closing throttled io_uring connection $connectionId from $address " +
                s"(throttle ${e.throttleTimeMs}ms)")
              selector.close(connectionId)
          }
        }
      }
    }
  }

  private def processDisconnected(): Unit = {
    selector.disconnected.keySet.forEach { connectionId =>
      try {
        val remoteHost = ServerConnectionId.fromString(connectionId).orElseThrow { () =>
          throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
        }.remoteHost
        inflightResponses.remove(connectionId).foreach(updateRequestMetrics)
        // the channel has been closed by the selector but the quotas still need to be updated
        connectionQuotas.dec(listenerName, InetAddress.getByName(remoteHost))
        // Call listeners to notify for closed connection.
        connectionDisconnectListeners.foreach(listener => CoreUtils.swallow(() -> listener.onDisconnect(connectionId), this, Level.ERROR))
      } catch {
        case e: Throwable => processException(s"Exception while processing disconnection of $connectionId", e)
      }
    }
  }

  private def closeExcessConnections(): Unit = {
    if (connectionQuotas.maxConnectionsExceeded(listenerName)) {
      val channel = selector.lowestPriorityChannel()
      if (channel != null)
        close(channel.id)
    }
  }

  /**
   * Close the connection identified by `connectionId` and decrement the connection count.
   * The channel will be immediately removed from the selector's `channels` or `closingChannels`
   * and no further disconnect notifications will be sent for this channel by the selector.
   * If responses are pending for the channel, they are dropped and metrics is updated.
   * If the channel has already been removed from selector, no action is taken.
   */
  private def close(connectionId: String): Unit = {
    openOrClosingChannel(connectionId).foreach { channel =>
      debug(s"Closing selector connection $connectionId")
      val address = channel.socketAddress
      if (address != null)
        connectionQuotas.dec(listenerName, address)
      selector.close(connectionId)
      // Call listeners to notify for closed connection.
      connectionDisconnectListeners.foreach(listener => CoreUtils.swallow(() -> listener.onDisconnect(connectionId), this, Level.ERROR))

      inflightResponses.remove(connectionId).foreach(response => updateRequestMetrics(response))
    }
  }

  /**
   * Queue up a new connection for reading
   */
  def accept(socketChannel: SocketChannel,
             mayBlock: Boolean,
             acceptorIdlePercentMeter: com.yammer.metrics.core.Meter): Boolean = {
    val accepted = {
      if (newConnections.offer(socketChannel))
        true
      else if (mayBlock) {
        val startNs = time.nanoseconds
        newConnections.put(socketChannel)
        acceptorIdlePercentMeter.mark(time.nanoseconds() - startNs)
        true
      } else
        false
    }
    if (accepted)
      wakeup()
    accepted
  }

  /**
   * Register any new connections that have been queued up. The number of connections processed
   * in each iteration is limited to ensure that traffic and connection close notifications of
   * existing channels are handled promptly.
   */
  private def configureNewConnections(): Unit = {
    var connectionsProcessed = 0
    while (connectionsProcessed < connectionQueueSize && !newConnections.isEmpty) {
      val channel = newConnections.poll()
      try {
        debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")
        selector.register(connectionId(channel.socket), channel)
        connectionsProcessed += 1
      } catch {
        // We explicitly catch all exceptions and close the socket to avoid a socket leak.
        case e: Throwable =>
          val remoteAddress = channel.socket.getRemoteSocketAddress
          // need to close the channel here to avoid a socket leak.
          connectionQuotas.closeChannel(this, listenerName, channel)
          processException(s"Processor $id closed connection from $remoteAddress", e)
      }
    }
  }

  /**
   * Close the selector and all open connections. For io_uring listeners we tear down the
   * IoUringServerListener first so no new accepts surface mid-shutdown, then drain pending
   * channels through the usual selector.close(id) path, then close the selector itself.
   */
  private def closeAll(): Unit = {
    ioBundle.listener.foreach { l =>
      try l.close()
      catch { case t: Throwable => error("Error closing IoUringServerListener", t) }
    }
    while (!newConnections.isEmpty) {
      newConnections.poll().close()
    }
    selector.channels.forEach { channel =>
      close(channel.id)
    }
    selector.close()
    metricsGroup.removeMetric(IdlePercentMetricName, Map(NetworkProcessorMetricTag -> id.toString).asJava)
  }

  // 'protected` to allow override for testing
  protected[network] def connectionId(socket: Socket): String = {
    val connId = ServerConnectionId.generateConnectionId(socket, id, nextConnectionIndex)
    nextConnectionIndex = if (nextConnectionIndex == Int.MaxValue) 0 else nextConnectionIndex + 1
    connId
  }

  private[network] def enqueueResponse(response: RequestChannel.Response): Unit = {
    responseQueue.put(response)
    wakeup()
  }

  private def dequeueResponse(): RequestChannel.Response = {
    val response = responseQueue.poll()
    if (response != null)
      response.request.responseDequeueTimeNanos = Time.SYSTEM.nanoseconds
    response
  }

  private[network] def responseQueueSize = responseQueue.size

  // Only for testing
  private[network] def inflightResponseCount: Int = inflightResponses.size

  // Visible for testing
  // Only methods that are safe to call on a disconnected channel should be invoked on 'openOrClosingChannel'.
  private[network] def openOrClosingChannel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId)).orElse(Option(selector.closingChannel(connectionId)))

  // Indicate the specified channel that the specified channel mute-related event has happened so that it can change its
  // mute state.
  private def handleChannelMuteEvent(connectionId: String, event: ChannelMuteEvent): Unit = {
    openOrClosingChannel(connectionId).foreach(c => c.handleChannelMuteEvent(event))
  }

  private def tryUnmuteChannel(connectionId: String): Unit = {
    openOrClosingChannel(connectionId).foreach(c => selector.unmute(c.id))
  }

  /* For test usage */
  private[network] def channel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId))

  def start(): Unit = {
    if (!started.getAndSet(true)) {
      // io_uring: bind the per-Processor listener here, not in the Processor constructor.
      // Acceptor.start() is the gate that runs only after SocketServer.enableRequestProcessing's
      // authorizerFuture resolves, so binding here means the LISTEN socket appears to the network
      // exactly when the broker is ready to handle requests — mirroring the NIO deferred-open
      // contract at line 552 (the NIO Acceptor opens its ServerSocketChannel here, not in its
      // constructor, "so that systems which assume that the socket being open indicates readiness
      // are not confused"). If start() is never called (broker shutdown between construction and
      // enableRequestProcessing), the listener's close() handles the unbind-less cleanup path.
      ioBundle.listener.foreach(_.start())
      try {
        thread.start()
      } catch {
        case t: Throwable =>
          // thread.start() can throw OutOfMemoryError under nproc/ulimit-thread
          // exhaustion or SecurityException under a SecurityManager. The Processor's
          // run() never executes, so its `finally { closeAll() }` (line ~1095) cannot
          // reclaim the LISTEN socket just bound by ioBundle.listener.start(), nor the
          // ioBundle.selector + event-loop group constructed in the Processor ctor.
          // The close() path's `if (!started.get) closeAll()` guard also skips cleanup
          // because `started` was flipped to true above. Without this explicit cleanup,
          // the LISTEN port stays bound for the JVM's lifetime on every nproc-exhausted
          // broker start.
          CoreUtils.swallow(closeAll(), this, Level.ERROR)
          throw t
      }
    }
  }

  /**
   * Wakeup the thread for selection.
   */
  def wakeup(): Unit = selector.wakeup()

  def beginShutdown(): Unit = {
    if (shouldRun.getAndSet(false)) {
      wakeup()
    }
  }

  def close(): Unit = {
    try {
      beginShutdown()
      thread.join()
      if (!started.get) {
        CoreUtils.swallow(closeAll(), this, Level.ERROR)
      }
    } finally {
      metricsGroup.removeMetric("IdlePercent", Map("networkProcessor" -> id.toString).asJava)
      metrics.removeMetric(expiredConnectionsKilledCountMetricName)
    }
  }
}

class ConnectionQuotas(config: KafkaConfig, time: Time, metrics: Metrics) extends Logging with AutoCloseable {

  @volatile private var defaultMaxConnectionsPerIp: Int = config.maxConnectionsPerIp
  @volatile private var maxConnectionsPerIpOverrides = config.maxConnectionsPerIpOverrides.map { case (host, count) => (InetAddress.getByName(host), count) }
  @volatile private var brokerMaxConnections = config.maxConnections
  private val interBrokerListenerName = config.interBrokerListenerName
  private val counts = mutable.Map[InetAddress, Int]()

  // Listener counts and configs are synchronized on `counts`
  private val listenerCounts = mutable.Map[ListenerName, Int]()
  private[network] val maxConnectionsPerListener = mutable.Map[ListenerName, ListenerConnectionQuota]()
  @volatile private var totalCount = 0
  // updates to defaultConnectionRatePerIp or connectionRatePerIp must be synchronized on `counts`
  @volatile private var defaultConnectionRatePerIp = QuotaConfig.IP_CONNECTION_RATE_DEFAULT.intValue()
  private val connectionRatePerIp = new ConcurrentHashMap[InetAddress, Int]()
  // sensor that tracks broker-wide connection creation rate and limit (quota)
  private val brokerConnectionRateSensor = getOrCreateConnectionRateQuotaSensor(config.maxConnectionCreationRate, ConnectionQuotaEntity.brokerQuotaEntity())
  private val maxThrottleTimeMs = TimeUnit.SECONDS.toMillis(config.quotaConfig.quotaWindowSizeSeconds.toLong)

  def inc(listenerName: ListenerName, address: InetAddress, acceptorBlockedPercentMeter: com.yammer.metrics.core.Meter): Unit = {
    counts.synchronized {
      waitForConnectionSlot(listenerName, acceptorBlockedPercentMeter)

      recordIpConnectionMaybeThrottle(listenerName, address)
      val count = counts.getOrElseUpdate(address, 0)
      counts.put(address, count + 1)
      totalCount += 1
      if (listenerCounts.contains(listenerName)) {
        listenerCounts.put(listenerName, listenerCounts(listenerName) + 1)
      }
      val max = maxConnectionsPerIpOverrides.getOrElse(address, defaultMaxConnectionsPerIp)
      if (count >= max)
        throw new TooManyConnectionsException(address, max)
    }
  }

  /**
   * Non-blocking variant of [[inc]] used by the io_uring Processor accept path.
   *
   * On the NIO path [[inc]] blocks the dedicated Acceptor thread when the broker is at
   * max.connections or when the listener connection-rate quota requires a throttle.
   * On the io_uring path the caller is the Processor's poll loop — blocking it would
   * stall every other channel on that Processor, including in-flight requests. This
   * method instead refuses (returns false) when the slot is unavailable or a throttle
   * applies, after un-recording the rate-sensor sample so accounting stays correct.
   *
   * Like [[inc]], may throw [[TooManyConnectionsException]] (per-IP max) or
   * [[ConnectionThrottledException]] (per-IP rate); callers MUST close the channel on
   * either return-false or exception.
   */
  def tryInc(listenerName: ListenerName, address: InetAddress): Boolean = {
    counts.synchronized {
      val timeMs = time.milliseconds
      val throttleTimeMs = math.max(recordConnectionAndGetThrottleTimeMs(listenerName, timeMs), 0)
      if (throttleTimeMs > 0 || !connectionSlotAvailable(listenerName)) {
        // Un-record the speculative +1 from recordConnectionAndGetThrottleTimeMs so the
        // rate sensor doesn't drift when we refuse the connection. Mirrors the un-record
        // path used by recordIpConnectionMaybeThrottle for IP throttle refusal.
        if (!protectedListener(listenerName)) {
          brokerConnectionRateSensor.record(-1.0, timeMs, false)
        }
        maxConnectionsPerListener
          .get(listenerName)
          .foreach(_.connectionRateSensor.record(-1.0, timeMs, false))
        return false
      }

      recordIpConnectionMaybeThrottle(listenerName, address)
      val count = counts.getOrElseUpdate(address, 0)
      counts.put(address, count + 1)
      totalCount += 1
      if (listenerCounts.contains(listenerName)) {
        listenerCounts.put(listenerName, listenerCounts(listenerName) + 1)
      }
      val max = maxConnectionsPerIpOverrides.getOrElse(address, defaultMaxConnectionsPerIp)
      if (count >= max)
        throw new TooManyConnectionsException(address, max)
      true
    }
  }

  private[network] def updateMaxConnectionsPerIp(maxConnectionsPerIp: Int): Unit = {
    defaultMaxConnectionsPerIp = maxConnectionsPerIp
  }

  private[network] def updateMaxConnectionsPerIpOverride(overrideQuotas: Map[String, Int]): Unit = {
    maxConnectionsPerIpOverrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }
  }

  private[network] def updateBrokerMaxConnections(maxConnections: Int): Unit = {
    counts.synchronized {
      brokerMaxConnections = maxConnections
      counts.notifyAll()
    }
  }

  private[network] def updateBrokerMaxConnectionRate(maxConnectionRate: Int): Unit = {
    // if there is a connection waiting on the rate throttle delay, we will let it wait the original delay even if
    // the rate limit increases, because it is just one connection per listener and the code is simpler that way
    updateConnectionRateQuota(maxConnectionRate, ConnectionQuotaEntity.brokerQuotaEntity())
  }

  /**
   * Update the connection rate quota for a given IP and updates quota configs for updated IPs.
   * If an IP is given, metric config will be updated only for the given IP, otherwise
   * all metric configs will be checked and updated if required.
   *
   * @param ip ip to update or default if None
   * @param maxConnectionRate new connection rate, or resets entity to default if None
   */
  def updateIpConnectionRateQuota(ip: Option[InetAddress], maxConnectionRate: Option[Int]): Unit = synchronized {
    def isIpConnectionRateMetric(metricName: MetricName) = {
      metricName.name == ConnectionQuotaEntity.CONNECTION_RATE_METRIC_NAME &&
      metricName.group == MetricsGroup &&
      metricName.tags.containsKey(ConnectionQuotaEntity.IP_METRIC_TAG)
    }

    def shouldUpdateQuota(metric: KafkaMetric, quotaLimit: Int) = {
      quotaLimit != metric.config.quota.bound
    }

    ip match {
      case Some(address) =>
        // synchronize on counts to ensure reading an IP connection rate quota and creating a quota config is atomic
        counts.synchronized {
          maxConnectionRate match {
            case Some(rate) =>
              info(s"Updating max connection rate override for $address to $rate")
              connectionRatePerIp.put(address, rate)
            case None =>
              info(s"Removing max connection rate override for $address")
              connectionRatePerIp.remove(address)
          }
        }
        updateConnectionRateQuota(connectionRateForIp(address), ConnectionQuotaEntity.ipQuotaEntity(address))
      case None =>
        // synchronize on counts to ensure reading an IP connection rate quota and creating a quota config is atomic
        counts.synchronized {
          defaultConnectionRatePerIp = maxConnectionRate.getOrElse(QuotaConfig.IP_CONNECTION_RATE_DEFAULT.intValue())
        }
        info(s"Updated default max IP connection rate to $defaultConnectionRatePerIp")
        metrics.metrics.forEach { (metricName, metric) =>
          if (isIpConnectionRateMetric(metricName)) {
            val quota = connectionRateForIp(InetAddress.getByName(metricName.tags.get(ConnectionQuotaEntity.IP_METRIC_TAG)))
            if (shouldUpdateQuota(metric, quota)) {
              debug(s"Updating existing connection rate quota config for ${metricName.tags} to $quota")
              metric.config(rateQuotaMetricConfig(quota))
            }
          }
        }
    }
  }

  // Visible for testing
  def connectionRateForIp(ip: InetAddress): Int = {
    connectionRatePerIp.getOrDefault(ip, defaultConnectionRatePerIp)
  }

  private[network] def addListener(config: KafkaConfig, listenerName: ListenerName): Unit = {
    counts.synchronized {
      if (!maxConnectionsPerListener.contains(listenerName)) {
        val newListenerQuota = new ListenerConnectionQuota(counts, listenerName)
        maxConnectionsPerListener.put(listenerName, newListenerQuota)
        listenerCounts.put(listenerName, 0)
        config.addReconfigurable(newListenerQuota)
        newListenerQuota.configure(config.valuesWithPrefixOverride(listenerName.configPrefix))
      }
      counts.notifyAll()
    }
  }

  private[network] def removeListener(config: KafkaConfig, listenerName: ListenerName): Unit = {
    counts.synchronized {
      maxConnectionsPerListener.remove(listenerName).foreach { listenerQuota =>
        listenerCounts.remove(listenerName)
        // once listener is removed from maxConnectionsPerListener, no metrics will be recorded into listener's sensor
        // so it is safe to remove sensor here
        listenerQuota.close()
        counts.notifyAll() // wake up any waiting acceptors to close cleanly
        config.removeReconfigurable(listenerQuota)
      }
    }
  }

  def dec(listenerName: ListenerName, address: InetAddress): Unit = {
    counts.synchronized {
      val count = counts.getOrElse(address,
        throw new IllegalArgumentException(s"Attempted to decrease connection count for address with no connections, address: $address"))
      if (count == 1)
        counts.remove(address)
      else
        counts.put(address, count - 1)

      if (totalCount <= 0)
        error(s"Attempted to decrease total connection count for broker with no connections")
      totalCount -= 1

      if (maxConnectionsPerListener.contains(listenerName)) {
        val listenerCount = listenerCounts(listenerName)
        if (listenerCount == 0)
          error(s"Attempted to decrease connection count for listener $listenerName with no connections")
        else
          listenerCounts.put(listenerName, listenerCount - 1)
      }
      counts.notifyAll() // wake up any acceptors waiting to process a new connection since listener connection limit was reached
    }
  }

  def get(address: InetAddress): Int = counts.synchronized {
    counts.getOrElse(address, 0)
  }

  private def waitForConnectionSlot(listenerName: ListenerName,
                                    acceptorBlockedPercentMeter: com.yammer.metrics.core.Meter): Unit = {
    counts.synchronized {
      val startThrottleTimeMs = time.milliseconds
      val throttleTimeMs = math.max(recordConnectionAndGetThrottleTimeMs(listenerName, startThrottleTimeMs), 0)

      if (throttleTimeMs > 0 || !connectionSlotAvailable(listenerName)) {
        val startNs = time.nanoseconds
        val endThrottleTimeMs = startThrottleTimeMs + throttleTimeMs
        var remainingThrottleTimeMs = throttleTimeMs
        do {
          counts.wait(remainingThrottleTimeMs)
          remainingThrottleTimeMs = math.max(endThrottleTimeMs - time.milliseconds, 0)
        } while (remainingThrottleTimeMs > 0 || !connectionSlotAvailable(listenerName))
        acceptorBlockedPercentMeter.mark(time.nanoseconds - startNs)
      }
    }
  }

  // This is invoked in every poll iteration and we close one LRU connection in an iteration
  // if necessary
  def maxConnectionsExceeded(listenerName: ListenerName): Boolean = {
    totalCount > brokerMaxConnections && !protectedListener(listenerName)
  }

  private def connectionSlotAvailable(listenerName: ListenerName): Boolean = {
    if (listenerCounts(listenerName) >= maxListenerConnections(listenerName))
      false
    else if (protectedListener(listenerName))
      true
    else
      totalCount < brokerMaxConnections
  }

  private def protectedListener(listenerName: ListenerName): Boolean =
    interBrokerListenerName == listenerName && listenerCounts.size > 1

  private def maxListenerConnections(listenerName: ListenerName): Int =
    maxConnectionsPerListener.get(listenerName).map(_.maxConnections).getOrElse(Int.MaxValue)

  /**
   * Calculates the delay needed to bring the observed connection creation rate to listener-level limit or to broker-wide
   * limit, whichever the longest. The delay is capped to the quota window size defined by QuotaWindowSizeSecondsProp
   *
   * @param listenerName listener for which calculate the delay
   * @param timeMs current time in milliseconds
   * @return delay in milliseconds
   */
  private def recordConnectionAndGetThrottleTimeMs(listenerName: ListenerName, timeMs: Long): Long = {
    def recordAndGetListenerThrottleTime(minThrottleTimeMs: Int): Int = {
      maxConnectionsPerListener
        .get(listenerName)
        .map { listenerQuota =>
          val listenerThrottleTimeMs = recordAndGetThrottleTimeMs(listenerQuota.connectionRateSensor, timeMs)
          val throttleTimeMs = math.max(minThrottleTimeMs, listenerThrottleTimeMs)
          // record throttle time due to hitting connection rate quota
          if (throttleTimeMs > 0) {
            listenerQuota.listenerConnectionRateThrottleSensor.record(throttleTimeMs.toDouble, timeMs)
          }
          throttleTimeMs
        }
        .getOrElse(0)
    }

    if (protectedListener(listenerName)) {
      recordAndGetListenerThrottleTime(0)
    } else {
      val brokerThrottleTimeMs = recordAndGetThrottleTimeMs(brokerConnectionRateSensor, timeMs)
      recordAndGetListenerThrottleTime(brokerThrottleTimeMs)
    }
  }

  /**
   * Record IP throttle time on the corresponding listener. To avoid over-recording listener/broker connection rate, we
   * also un-record the listener and broker connection if the IP gets throttled.
   *
   * @param listenerName listener to un-record connection
   * @param throttleMs IP throttle time to record for listener
   * @param timeMs current time in milliseconds
   */
  private def updateListenerMetrics(listenerName: ListenerName, throttleMs: Long, timeMs: Long): Unit = {
    if (!protectedListener(listenerName)) {
      brokerConnectionRateSensor.record(-1.0, timeMs, false)
    }
    maxConnectionsPerListener
      .get(listenerName)
      .foreach { listenerQuota =>
        listenerQuota.ipConnectionRateThrottleSensor.record(throttleMs.toDouble, timeMs)
        listenerQuota.connectionRateSensor.record(-1.0, timeMs, false)
      }
  }

  /**
   * Calculates the delay needed to bring the observed connection creation rate to the IP limit.
   * If the connection would cause an IP quota violation, un-record the connection for both IP,
   * listener, and broker connection rate and throw a ConnectionThrottledException. Calls to
   * this function must be performed with the counts lock to ensure that reading the IP
   * connection rate quota and creating the sensor's metric config is atomic.
   *
   * @param listenerName listener to unrecord connection if throttled
   * @param address ip address to record connection
   */
  private def recordIpConnectionMaybeThrottle(listenerName: ListenerName, address: InetAddress): Unit = {
    val connectionRateQuota = connectionRateForIp(address)
    val quotaEnabled = connectionRateQuota != QuotaConfig.IP_CONNECTION_RATE_DEFAULT
    if (quotaEnabled) {
      val sensor = getOrCreateConnectionRateQuotaSensor(connectionRateQuota, ConnectionQuotaEntity.ipQuotaEntity(address))
      val timeMs = time.milliseconds
      val throttleMs = recordAndGetThrottleTimeMs(sensor, timeMs)
      if (throttleMs > 0) {
        trace(s"Throttling $address for $throttleMs ms")
        // unrecord the connection since we won't accept the connection
        sensor.record(-1.0, timeMs, false)
        updateListenerMetrics(listenerName, throttleMs, timeMs)
        throw new ConnectionThrottledException(address, timeMs, throttleMs)
      }
    }
  }

  /**
   * Records a new connection into a given connection acceptance rate sensor 'sensor' and returns throttle time
   * in milliseconds if quota got violated
   * @param sensor sensor to record connection
   * @param timeMs current time in milliseconds
   * @return throttle time in milliseconds if quota got violated, otherwise 0
   */
  private def recordAndGetThrottleTimeMs(sensor: Sensor, timeMs: Long): Int = {
    try {
      sensor.record(1.0, timeMs)
      0
    } catch {
      case e: QuotaViolationException =>
        val throttleTimeMs = QuotaUtils.boundedThrottleTime(e, maxThrottleTimeMs, timeMs).toInt
        debug(s"Quota violated for sensor (${sensor.name}). Delay time: $throttleTimeMs ms")
        throttleTimeMs
    }
  }

  /**
   * Creates sensor for tracking the connection creation rate and corresponding connection rate quota for a given
   * listener or broker-wide, if listener is not provided.
   * @param quotaLimit connection creation rate quota
   * @param connectionQuotaEntity entity to create the sensor for
   */
  private def getOrCreateConnectionRateQuotaSensor(quotaLimit: Int, connectionQuotaEntity: ConnectionQuotaEntity): Sensor = {
    Option(metrics.getSensor(connectionQuotaEntity.sensorName)).getOrElse {
      val sensor = metrics.sensor(
        connectionQuotaEntity.sensorName,
        rateQuotaMetricConfig(quotaLimit),
        connectionQuotaEntity.sensorExpiration
      )
      sensor.add(connectionRateMetricName(connectionQuotaEntity), new Rate, null)
      sensor
    }
  }

  /**
   * Updates quota configuration for a given connection quota entity
   */
  private def updateConnectionRateQuota(quotaLimit: Int, connectionQuotaEntity: ConnectionQuotaEntity): Unit = {
    Option(metrics.metric(connectionRateMetricName(connectionQuotaEntity))).foreach { metric =>
      metric.config(rateQuotaMetricConfig(quotaLimit))
      info(s"Updated ${connectionQuotaEntity.metricName} max connection creation rate to $quotaLimit")
    }
  }

  private def connectionRateMetricName(connectionQuotaEntity: ConnectionQuotaEntity): MetricName = {
    metrics.metricName(
      connectionQuotaEntity.metricName,
      MetricsGroup,
      s"Tracking rate of accepting new connections (per second)",
      connectionQuotaEntity.metricTags)
  }

  private def rateQuotaMetricConfig(quotaLimit: Int): MetricConfig = {
    new MetricConfig()
      .timeWindow(config.quotaConfig.quotaWindowSizeSeconds.toLong, TimeUnit.SECONDS)
      .samples(config.quotaConfig.numQuotaSamples)
      .quota(new Quota(quotaLimit, true))
  }

  def close(): Unit = {
    metrics.removeSensor(brokerConnectionRateSensor.name)
    maxConnectionsPerListener.values.foreach(_.close())
  }

  class ListenerConnectionQuota(lock: Object, listener: ListenerName) extends ListenerReconfigurable with AutoCloseable {
    @volatile private var _maxConnections = Int.MaxValue
    private[network] val connectionRateSensor = getOrCreateConnectionRateQuotaSensor(Int.MaxValue, ConnectionQuotaEntity.listenerQuotaEntity(listener.value))
    private[network] val listenerConnectionRateThrottleSensor = createConnectionRateThrottleSensor(ConnectionQuotaEntity.LISTENER_THROTTLE_PREFIX)
    private[network] val ipConnectionRateThrottleSensor = createConnectionRateThrottleSensor(ConnectionQuotaEntity.IP_THROTTLE_PREFIX)

    def maxConnections: Int = _maxConnections

    override def listenerName(): ListenerName = listener

    override def configure(configs: util.Map[String, _]): Unit = {
      _maxConnections = maxConnections(configs)
      updateConnectionRateQuota(maxConnectionCreationRate(configs), ConnectionQuotaEntity.listenerQuotaEntity(listener.value))
    }

    override def reconfigurableConfigs(): util.Set[String] = {
      SocketServer.ListenerReconfigurableConfigs.asJava
    }

    override def validateReconfiguration(configs: util.Map[String, _]): Unit = {
      val value = maxConnections(configs)
      if (value <= 0)
        throw new ConfigException(s"Invalid ${SocketServerConfigs.MAX_CONNECTIONS_CONFIG} $value")

      val rate = maxConnectionCreationRate(configs)
      if (rate <= 0)
        throw new ConfigException(s"Invalid ${SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG} $rate")
    }

    override def reconfigure(configs: util.Map[String, _]): Unit = {
      lock.synchronized {
        _maxConnections = maxConnections(configs)
        updateConnectionRateQuota(maxConnectionCreationRate(configs), ConnectionQuotaEntity.listenerQuotaEntity(listener.value))
        lock.notifyAll()
      }
    }

    def close(): Unit = {
      metrics.removeSensor(connectionRateSensor.name)
      metrics.removeSensor(listenerConnectionRateThrottleSensor.name)
      metrics.removeSensor(ipConnectionRateThrottleSensor.name)
    }

    private def maxConnections(configs: util.Map[String, _]): Int = {
      Option(configs.get(SocketServerConfigs.MAX_CONNECTIONS_CONFIG)).map(_.toString.toInt).getOrElse(Int.MaxValue)
    }

    private def maxConnectionCreationRate(configs: util.Map[String, _]): Int = {
      Option(configs.get(SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG)).map(_.toString.toInt).getOrElse(Int.MaxValue)
    }

    /**
     * Creates sensor for tracking the average throttle time on this listener due to hitting broker/listener connection
     * rate or IP connection rate quota. The average is out of all throttle times > 0, which is consistent with the
     * bandwidth and request quota throttle time metrics.
     */
    private def createConnectionRateThrottleSensor(throttlePrefix: String): Sensor = {
      val sensor = metrics.sensor(s"${throttlePrefix}ConnectionRateThrottleTime-${listener.value}")
      val metricName = metrics.metricName(s"${throttlePrefix}connection-accept-throttle-time",
        MetricsGroup,
        "Tracking average throttle-time, out of non-zero throttle times, per listener",
        Map(ListenerMetricTag -> listener.value).asJava)
      sensor.add(metricName, new Avg)
      sensor
    }
  }

  /**
   * Close `channel` and decrement the connection count.
   */
  def closeChannel(log: Logging, listenerName: ListenerName, channel: SocketChannel): Unit = {
    if (channel != null) {
      log.debug(s"Closing connection from ${channel.socket.getRemoteSocketAddress}")
      dec(listenerName, channel.socket.getInetAddress)
      closeSocket(channel)
    }
  }

}
