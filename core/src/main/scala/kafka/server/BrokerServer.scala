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

import kafka.coordinator.group.{CoordinatorLoaderImpl, CoordinatorPartitionWriter, GroupCoordinatorAdapter}
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.log.LogManager
import kafka.log.remote.RemoteLogManager
import kafka.network.{DataPlaneAcceptor, SocketServer}
import kafka.raft.KafkaRaftManager
import kafka.server.metadata._
import kafka.server.share.SharePartitionManager
import kafka.utils.CoreUtils
import org.apache.kafka.common.config.{ConfigException, ConfigResource, TopicConfig}
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.{LogContext, Time, Utils}
import org.apache.kafka.common.{ClusterResource, TopicPartition, Uuid}
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord
import org.apache.kafka.coordinator.group.metrics.{GroupCoordinatorMetrics, GroupCoordinatorRuntimeMetrics}
import org.apache.kafka.coordinator.group.{GroupConfigManager, GroupCoordinator, GroupCoordinatorRecordSerde, GroupCoordinatorService}
import org.apache.kafka.coordinator.share.metrics.{ShareCoordinatorMetrics, ShareCoordinatorRuntimeMetrics}
import org.apache.kafka.coordinator.share.{ShareCoordinator, ShareCoordinatorRecordSerde, ShareCoordinatorService}
import org.apache.kafka.coordinator.transaction.ProducerIdManager
import org.apache.kafka.image.publisher.{BrokerRegistrationTracker, MetadataPublisher}
import org.apache.kafka.metadata.{BrokerState, ListenerInfo}
import org.apache.kafka.security.CredentialProvider
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.{ApiMessageAndVersion, DirectoryEventHandler, NodeToControllerChannelManager, TopicIdPartition}
import org.apache.kafka.server.config.{ConfigType, ServerConfigs}
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig
import org.apache.kafka.server.metrics.{ClientMetricsReceiverPlugin, KafkaYammerMetrics}
import org.apache.kafka.server.network.{EndpointReadyFutures, KafkaAuthorizerServerInfo}
import org.apache.kafka.server.rules.{GovernanceTopic, RuleEngine}
import org.apache.kafka.server.share.persister.{DefaultStatePersister, NoOpShareStatePersister, Persister, PersisterStateManager}
import org.apache.kafka.server.share.session.ShareSessionCache
import org.apache.kafka.server.util.timer.{SystemTimer, SystemTimerReaper}
import org.apache.kafka.server.util.{Deadline, FutureUtils, KafkaScheduler}
import org.apache.kafka.server.{AssignmentsManager, BrokerFeatures, ClientMetricsManager, DelayedActionQueue}
import org.apache.kafka.storage.internals.log.LogDirFailureChannel
import org.apache.kafka.storage.log.metrics.BrokerTopicStats

import java.time.Duration
import java.util
import java.util.Optional
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.{Condition, ReentrantLock}
import java.util.concurrent.{CompletableFuture, ExecutionException, TimeUnit, TimeoutException}
import scala.collection.Map
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOption


object BrokerServer {
  /**
   * Polling interval, in milliseconds, for the broker's periodic re-read of
   * the local `__governance` log. Sub-second cadence keeps rule propagation
   * latency low while staying cheap when the log is idle (a single
   * [[ReplicaManager.getLog]] + offset compare per tick when there are no
   * new records to replay).
   */
  val GovernanceDrainIntervalMs: Long = 200L

  /**
   * Bounded wait, in milliseconds, that the startup-path governance drain
   * gives the local {@code __governance} log to become available before
   * aborting broker startup. Codex deep-audit P0: if this broker is a replica
   * of {@code __governance-0} per cluster metadata but [[ReplicaManager.getLog]]
   * has not yet opened the log dir (race between metadata catch-up and
   * LogManager finishing log recovery), [[BrokerGovernanceBootstrap.drainOnce]]
   * returning {@code 0L} would let [[SocketServer.enableRequestProcessing]]
   * open client traffic before the engine has any rules installed — a silent
   * fail-empty window for every DENY rule on the topic. [[drainStartup]]
   * bounded-waits up to this duration, then throws and broker startup aborts.
   *
   * <p>30 seconds is generous relative to typical log-recovery times on healthy
   * disks (sub-second), and tight enough that an operator notices on the next
   * restart rather than discovering it from a missed enforcement window. Tune
   * upward if you run brokers with very large log-dir state or slow storage.
   */
  val GovernanceStartupDrainDeadlineMs: Long = 30_000L

  /**
   * Fail-closed enforcement of {@code cleanup.policy=compact} on the
   * {@code __governance} topic. Audit round-5 finding {@code a016e43dc},
   * round-12 tombstone/compaction sub-agent HIGH-1, and round-13
   * log-compaction race sub-agent HIGH-1 (which tightened the check from
   * "includes compact" to "equals compact exactly").
   *
   * <p>If {@code __governance} exists in the metadata image and its effective
   * cleanup policy is NOT exactly {@code compact}, throw
   * [[IllegalStateException]]. Callers in [[BrokerServer.startup]] do not catch
   * — broker startup aborts before [[kafka.network.SocketServer]] opens client
   * traffic, with the offending policy and a one-line remediation command in
   * the exception message.
   *
   * <p>Why exact-match, not substring: {@code compact,delete} is a valid Kafka
   * policy that enables BOTH compaction AND retention-based whole-segment
   * deletion. A PUT rule with no tombstone still gets deleted once
   * {@code retention.ms} (default 7 days) elapses on its segment — exactly the
   * fail-stale regression this gate exists to close. Allowing
   * {@code compact,delete} would re-open the hole the gate is trying to plug.
   * Operators with a legitimate need to also enforce retention (e.g. to GC
   * very old tombstones) should accept the fail-stale risk explicitly via a
   * future opt-in property, not slip through a substring loophole.
   *
   * <p>"Effective policy" mirrors how the log layer resolves it: topic-level
   * override if present, otherwise the broker-default
   * {@code log.cleanup.policy} list joined by comma. Both are passed in by the
   * caller; this helper only enforces the check and crafts the message, so a
   * unit test can drive every branch without a live cluster. The list is
   * compared after a trim of each element so that
   * {@code log.cleanup.policy=compact, delete} (a single broker-default value
   * with a stray space) is treated the same as {@code [compact, delete]}.
   *
   * <p>When {@code topicExists} is false, the helper returns silently — fresh
   * clusters where the operator has not yet created {@code __governance} are
   * valid; the broker comes up with an empty {@code RuleSet} and the regular
   * drain picks up rules once the topic appears (Audit
   * {@link kafka.server.BrokerServer#clusterStartsCleanWhenGovernanceTopicIsAbsent}
   * pins the absent-topic branch).
   *
   * @param topicExists true iff the metadata image has a TopicImage for the
   *                    governance topic (i.e. operator has created it)
   * @param topicLevelCleanupPolicy topic-level cleanup.policy override, or
   *                                null when no override is set
   * @param brokerDefaultCleanupPolicy broker-default cleanup.policy list
   *                                   (Kafka default: {@code ["delete"]})
   */
  def requireGovernanceTopicCompactPolicy(
      topicExists: Boolean,
      topicLevelCleanupPolicy: String,
      brokerDefaultCleanupPolicy: util.List[String]): Unit = {
    if (!topicExists) return
    val raw = if (topicLevelCleanupPolicy != null) topicLevelCleanupPolicy
      else brokerDefaultCleanupPolicy.asScala.mkString(",")
    // Normalise: split on comma, trim whitespace per element, sort + lowercase.
    // {compact} (single-element set) is the ONLY accepted shape.
    val components = raw.split(",").iterator
      .map(_.trim.toLowerCase(java.util.Locale.ROOT))
      .filter(_.nonEmpty)
      .toSet
    if (components != Set(TopicConfig.CLEANUP_POLICY_COMPACT)) {
      throw new IllegalStateException(
        s"Topic ${GovernanceTopic.NAME} has effective cleanup.policy='$raw' " +
          s"which is not exactly '${TopicConfig.CLEANUP_POLICY_COMPACT}'. " +
          s"The 'delete' co-policy (i.e. 'compact,delete') is NOT accepted on " +
          s"this topic because retention.ms (default 7 days) would still delete " +
          s"rule records whole-segment, causing CEL DENY rules to silently " +
          s"disappear and the broker to fail OPEN after restart. The broker " +
          s"refuses to start until this is fixed. Run: " +
          s"bin/kafka-configs.sh --bootstrap-server <broker> --alter --entity-type topics " +
          s"--entity-name ${GovernanceTopic.NAME} --add-config cleanup.policy=compact " +
          s"(audit finding a016e43dc, round-12 HIGH-1, round-13 HIGH-1).")
    }
  }

  /**
   * Fail-closed enforcement that the {@code __governance} topic is created
   * with exactly one partition. Round-14 BLOCKER N5 (compaction sub-agent).
   *
   * <p>[[BrokerGovernanceBootstrap]] hardcodes its drain cursor on
   * {@code TopicPartition(__governance, 0)}: it only ever consumes partition
   * 0 of this topic. If an operator creates the topic with
   * {@code --partitions 3}, rule keys hash across partitions 0/1/2 (Kafka's
   * default DefaultPartitioner uses {@code murmur2(key)} when a key is
   * present) and the broker silently drains roughly one-third of them,
   * skipping the rest. Every DENY rule that hashes to partition 1 or 2
   * silently disappears — a fail-OPEN of the entire rule engine that
   * survives restarts and produces no diagnostic.
   *
   * <p>The contract on this topic is "single-partition compacted log" — a
   * cluster-wide ordered stream of rule mutations. There is no use case for
   * sharding it: the request path consults the engine's atomic
   * {@link kafka.server.RuleEngine#active} snapshot per request, never the
   * topic, so partition fanout adds no scaling benefit and only opens this
   * fail-OPEN hole. Closing this is mandatory before opening client
   * traffic; the broker refuses to start until the topic is recreated with
   * the correct shape.
   *
   * <p>When {@code topicExists} is false (fresh cluster, topic not yet
   * created) the helper returns silently — same posture as
   * {@link #requireGovernanceTopicCompactPolicy}: a yet-to-be-created topic
   * cannot be misshapen.
   *
   * @param topicExists true iff the metadata image has a TopicImage for the
   *                    governance topic (i.e. operator has created it)
   * @param partitionCount the number of partitions on the topic, read from
   *                       {@code topicImage.partitions().size()}; ignored
   *                       when {@code topicExists} is false
   */
  def requireGovernanceTopicSinglePartition(
      topicExists: Boolean,
      partitionCount: Int): Unit = {
    if (!topicExists) return
    if (partitionCount != 1) {
      throw new IllegalStateException(
        s"Topic ${GovernanceTopic.NAME} has $partitionCount partition(s), but " +
          s"the broker's rule-engine bootstrap consumes only partition 0 of " +
          s"this topic. A non-1 partition count silently drops every rule " +
          s"whose key hashes to a non-0 partition, causing the broker to " +
          s"fail OPEN on those CEL DENY rules. The broker refuses to start " +
          s"until this is fixed. There is NO supported way to alter partition " +
          s"count on a compacted topic in place — delete and recreate: " +
          s"bin/kafka-topics.sh --bootstrap-server <broker> --delete --topic " +
          s"${GovernanceTopic.NAME} && bin/kafka-topics.sh --bootstrap-server " +
          s"<broker> --create --topic ${GovernanceTopic.NAME} --partitions 1 " +
          s"--replication-factor <RF> --config cleanup.policy=compact " +
          s"(audit round-14 BLOCKER N5).")
    }
  }

  /**
   * Parser for the {@code governance.bootstrap.require.local.replica} broker
   * property. Fail-CLOSED by design: the only values that disable the
   * require-local-replica safety gate are exact, lowercase, trimmed matches of
   * {@code "false"}, {@code "no"}, or {@code "0"}. Any other value — including
   * {@code null} (absent), typos (e.g. {@code "fals"}), unknown booleans
   * (e.g. {@code "off"}, {@code "yes"}, {@code "1"}), and garbage — yields
   * {@code true} and leaves the gate engaged.
   *
   * <p>This knob is read directly from {@link KafkaConfig#originals()} rather
   * than through a typed accessor. The intentional decision to keep it off the
   * typed config surface ({@link org.apache.kafka.server.config.ServerConfigs#CONFIG_DEF})
   * is load-bearing — not for parser semantics (ConfigDef.BOOLEAN's coercion
   * is roughly equivalent: case-insensitive trimmed match of "true"/"false")
   * but for the dynamic-config admit path:
   * <ul>
   *   <li>If this key were typed, {@link DynamicBrokerConfig#AllDynamicConfigs}
   *       would either include it (admitting runtime AlterConfigs writes) or
   *       have to explicitly exclude it. Either way, the admit-time gate
   *       would consult typed validation, and a future maintainer adding a
   *       {@code BrokerReconfigurable} listener for symmetry with other
   *       broker configs would split-brain runtime state (consumed by the
   *       listener) versus next-restart state (consumed here) — with no
   *       rolling-restart story for moving the gate.</li>
   *   <li>An admit-time ConfigException on a malformed value (which
   *       ConfigDef.BOOLEAN would raise for, say, {@code "off"}) would
   *       reject the ENTIRE incremental alteration batch, including
   *       unrelated keys. A silently-accepted-and-ignored value (because the
   *       key isn't in CONFIG_DEF) is the louder operator failure mode —
   *       paired with the broker-startup ERROR signal at
   *       {@code BrokerGovernanceBootstrap.scala:226-238} on the only
   *       branch where {@code =false} is operator-visible.</li>
   * </ul>
   * The forcing-function test
   * {@code governanceBootstrapRequireLocalReplicaIsUntypedAndNonDynamic}
   * (DynamicBrokerConfigTest, R39-E-3) asserts the key is neither in
   * {@code CONFIG_DEF} nor in {@code AllDynamicConfigs}, so any future drift
   * toward typed/dynamic registration breaks the test before it ships.
   *
   * <p>Round-39 audit (R39-E-2) flagged that this parser had only indirect
   * test coverage (via the error-message assertions in the bootstrap-level
   * tests). Extracted here so a direct unit test can pin every branch without
   * standing up a broker.
   *
   * @param rawValue the value read from {@code config.originals().get(key)}
   *                 (may be {@code null} when the key is absent; otherwise
   *                 typically a {@code String} but may be any {@code Object}
   *                 that has a meaningful {@code toString})
   * @return {@code false} only on exact lowercase-trimmed match of
   *         {@code "false"}, {@code "no"}, or {@code "0"}; {@code true} for
   *         every other input including {@code null}
   */
  def parseRequireLocalReplica(rawValue: AnyRef): Boolean = {
    Option(rawValue).map(_.toString.trim.toLowerCase(java.util.Locale.ROOT)) match {
      case Some("false") | Some("no") | Some("0") => false
      case _ => true
    }
  }
}

/**
 * A Kafka broker that runs in KRaft (Kafka Raft) mode.
 */
class BrokerServer(
  val sharedServer: SharedServer
) extends KafkaBroker {
  val config: KafkaConfig = sharedServer.brokerConfig
  val time: Time = sharedServer.time
  def metrics: Metrics = sharedServer.metrics

  // Get raftManager from SharedServer. It will be initialized during startup.
  def raftManager: KafkaRaftManager[ApiMessageAndVersion] = sharedServer.raftManager

  override def brokerState: BrokerState = Option(lifecycleManager).
    flatMap(m => Some(m.state)).getOrElse(BrokerState.NOT_RUNNING)

  import kafka.server.Server._

  private val logContext: LogContext = new LogContext(s"[BrokerServer id=${config.nodeId}] ")

  this.logIdent = logContext.logPrefix

  @volatile var lifecycleManager: BrokerLifecycleManager = _

  private var assignmentsManager: AssignmentsManager = _

  private val isShuttingDown = new AtomicBoolean(false)

  val lock: ReentrantLock = new ReentrantLock()
  val awaitShutdownCond: Condition = lock.newCondition()
  var status: ProcessStatus = SHUTDOWN

  @volatile var dataPlaneRequestProcessor: KafkaApis = _

  /**
   * Engine that evaluates CEL DENY rules against every request in
   * [[KafkaApis.handle]]. Created early — before [[dataPlaneRequestProcessor]]
   * — so that its [[org.apache.kafka.server.rules.RuleSet]] is already
   * installed by the time [[SocketServer.enableRequestProcessing]] opens the
   * listeners. The drain itself is performed by [[governanceBootstrap]].
   */
  @volatile var ruleEngine: RuleEngine = _

  @volatile var governanceBootstrap: BrokerGovernanceBootstrap = _

  var authorizer: Option[Authorizer] = None
  @volatile var socketServer: SocketServer = _
  var dataPlaneRequestHandlerPool: KafkaRequestHandlerPool = _

  var logDirFailureChannel: LogDirFailureChannel = _
  var logManager: LogManager = _
  var remoteLogManagerOpt: Option[RemoteLogManager] = None

  var tokenManager: DelegationTokenManager = _

  var dynamicConfigHandlers: Map[String, ConfigHandler] = _

  @volatile private[this] var _replicaManager: ReplicaManager = _

  var credentialProvider: CredentialProvider = _
  var tokenCache: DelegationTokenCache = _

  @volatile var groupCoordinator: GroupCoordinator = _

  var groupConfigManager: GroupConfigManager = _

  var transactionCoordinator: TransactionCoordinator = _

  var shareCoordinator: Option[ShareCoordinator] = None

  var clientToControllerChannelManager: NodeToControllerChannelManager = _

  var forwardingManager: ForwardingManager = _

  var alterPartitionManager: AlterPartitionManager = _

  var autoTopicCreationManager: AutoTopicCreationManager = _

  var kafkaScheduler: KafkaScheduler = _

  @volatile var metadataCache: KRaftMetadataCache = _

  var quotaManagers: QuotaFactory.QuotaManagers = _

  var clientQuotaMetadataManager: ClientQuotaMetadataManager = _

  @volatile var brokerTopicStats: BrokerTopicStats = _

  val clusterId: String = sharedServer.metaPropsEnsemble.clusterId().get()

  var brokerMetadataPublisher: BrokerMetadataPublisher = _

  var brokerRegistrationTracker: BrokerRegistrationTracker = _

  val brokerFeatures: BrokerFeatures = BrokerFeatures.createDefault(config.unstableFeatureVersionsEnabled)

  def kafkaYammerMetrics: KafkaYammerMetrics = KafkaYammerMetrics.INSTANCE

  val metadataPublishers: util.List[MetadataPublisher] = new util.ArrayList[MetadataPublisher]()

  var clientMetricsManager: ClientMetricsManager = _

  var sharePartitionManager: SharePartitionManager = _

  var persister: Persister = _

  private def maybeChangeStatus(from: ProcessStatus, to: ProcessStatus): Boolean = {
    lock.lock()
    try {
      if (status != from) return false
      info(s"Transition from $status to $to")

      status = to
      if (to == SHUTTING_DOWN) {
        isShuttingDown.set(true)
      } else if (to == SHUTDOWN) {
        isShuttingDown.set(false)
        awaitShutdownCond.signalAll()
      }
    } finally {
      lock.unlock()
    }
    true
  }

  def replicaManager: ReplicaManager = _replicaManager

  override def startup(): Unit = {
    if (!maybeChangeStatus(SHUTDOWN, STARTING)) return
    val startupDeadline = Deadline.fromDelay(time, config.serverMaxStartupTimeMs, TimeUnit.MILLISECONDS)
    try {
      sharedServer.startForBroker()

      info("Starting broker")

      val clientMetricsReceiverPlugin = new ClientMetricsReceiverPlugin()
      config.dynamicConfig.initialize(Some(clientMetricsReceiverPlugin))

      /* start scheduler */
      kafkaScheduler = new KafkaScheduler(config.backgroundThreads)
      kafkaScheduler.startup()

      /* register broker metrics */
      brokerTopicStats = new BrokerTopicStats(config.remoteLogManagerConfig.isRemoteStorageSystemEnabled())

      quotaManagers = QuotaFactory.instantiate(config, metrics, time, s"broker-${config.nodeId}-")

      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size)

      metadataCache = MetadataCache.kRaftMetadataCache(config.nodeId, () => raftManager.client.kraftVersion())

      // Create log manager, but don't start it because we need to delay any potential unclean shutdown log recovery
      // until we catch up on the metadata log and have up-to-date topic and broker configs.
      logManager = LogManager(config,
        sharedServer.metaPropsEnsemble.errorLogDirs().asScala.toSeq,
        metadataCache,
        kafkaScheduler,
        time,
        brokerTopicStats,
        logDirFailureChannel,
        keepPartitionMetadataFile = true)

      remoteLogManagerOpt = createRemoteLogManager()

      lifecycleManager = new BrokerLifecycleManager(config,
        time,
        s"broker-${config.nodeId}-",
        isZkBroker = false,
        logDirs = logManager.directoryIdsSet,
        () => new Thread(() => shutdown(), "kafka-shutdown-thread").start())

      // Enable delegation token cache for all SCRAM mechanisms to simplify dynamic update.
      // This keeps the cache up-to-date if new SCRAM mechanisms are enabled dynamically.
      tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
      credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache)

      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "controller quorum voters future",
        sharedServer.controllerQuorumVotersFuture,
        startupDeadline, time)
      val controllerNodeProvider = RaftControllerNodeProvider(raftManager, config)

      clientToControllerChannelManager = new NodeToControllerChannelManagerImpl(
        controllerNodeProvider,
        time,
        metrics,
        config,
        channelName = "forwarding",
        s"broker-${config.nodeId}-",
        retryTimeoutMs = 60000
      )
      clientToControllerChannelManager.start()
      forwardingManager = new ForwardingManagerImpl(clientToControllerChannelManager, metrics)
      clientMetricsManager = new ClientMetricsManager(clientMetricsReceiverPlugin, config.clientTelemetryMaxBytes, time, metrics)

      val apiVersionManager = ApiVersionManager(
        ListenerType.BROKER,
        config,
        forwardingManager,
        brokerFeatures,
        metadataCache,
        Some(clientMetricsManager)
      )

      val connectionDisconnectListeners = Seq(clientMetricsManager.connectionDisconnectListener())
      // Create and start the socket server acceptor threads so that the bound port is known.
      // Delay starting processors until the end of the initialization sequence to ensure
      // that credentials have been loaded before processing authentications.
      socketServer = new SocketServer(config,
        metrics,
        time,
        credentialProvider,
        apiVersionManager,
        sharedServer.socketFactory,
        connectionDisconnectListeners)

      clientQuotaMetadataManager = new ClientQuotaMetadataManager(quotaManagers, socketServer.connectionQuotas)

      val listenerInfo = ListenerInfo.create(Optional.of(config.interBrokerListenerName.value()),
          config.effectiveAdvertisedBrokerListeners.map(_.toJava).asJava).
            withWildcardHostnamesResolved().
            withEphemeralPortsCorrected(name => socketServer.boundPort(new ListenerName(name)))

      alterPartitionManager = AlterPartitionManager(
        config,
        scheduler = kafkaScheduler,
        controllerNodeProvider,
        time = time,
        metrics,
        s"broker-${config.nodeId}-",
        brokerEpochSupplier = () => lifecycleManager.brokerEpoch
      )
      alterPartitionManager.start()

      val addPartitionsLogContext = new LogContext(s"[AddPartitionsToTxnManager broker=${config.brokerId}]")
      val addPartitionsToTxnNetworkClient = NetworkUtils.buildNetworkClient("AddPartitionsManager", config, metrics, time, addPartitionsLogContext)
      val addPartitionsToTxnManager = new AddPartitionsToTxnManager(
        config,
        addPartitionsToTxnNetworkClient,
        metadataCache,
        // The transaction coordinator is not created at this point so we must
        // use a lambda here.
        transactionalId => transactionCoordinator.partitionFor(transactionalId),
        time
      )

      val assignmentsChannelManager = new NodeToControllerChannelManagerImpl(
        controllerNodeProvider,
        time,
        metrics,
        config,
        "directory-assignments",
        s"broker-${config.nodeId}-",
        retryTimeoutMs = 60000
      )
      assignmentsManager = new AssignmentsManager(
        time,
        assignmentsChannelManager,
        config.brokerId,
        () => metadataCache.getImage(),
        (directoryId: Uuid) => logManager.directoryPath(directoryId).
          getOrElse("[unknown directory path]")
      )
      val directoryEventHandler = new DirectoryEventHandler {
        override def handleAssignment(partition: TopicIdPartition, directoryId: Uuid, reason: String, callback: Runnable): Unit =
          assignmentsManager.onAssignment(partition, directoryId, reason, callback)

        override def handleFailure(directoryId: Uuid): Unit =
          lifecycleManager.propagateDirectoryFailure(directoryId, config.logDirFailureTimeoutMs)
      }

      /**
       * TODO: move this action queue to handle thread so we can simplify concurrency handling
       */
      val defaultActionQueue = new DelayedActionQueue

      this._replicaManager = new ReplicaManager(
        config = config,
        metrics = metrics,
        time = time,
        scheduler = kafkaScheduler,
        logManager = logManager,
        remoteLogManager = remoteLogManagerOpt,
        quotaManagers = quotaManagers,
        metadataCache = metadataCache,
        logDirFailureChannel = logDirFailureChannel,
        alterPartitionManager = alterPartitionManager,
        brokerTopicStats = brokerTopicStats,
        isShuttingDown = isShuttingDown,
        threadNamePrefix = None, // The ReplicaManager only runs on the broker, and already includes the ID in thread names.
        delayedRemoteFetchPurgatoryParam = None,
        brokerEpochSupplier = () => lifecycleManager.brokerEpoch,
        addPartitionsToTxnManager = Some(addPartitionsToTxnManager),
        directoryEventHandler = directoryEventHandler,
        defaultActionQueue = defaultActionQueue
      )

      /* start token manager */
      tokenManager = new DelegationTokenManager(config, tokenCache, time)
      tokenManager.startup()

      // Create and initialize an authorizer if one is configured.
      authorizer = config.createNewAuthorizer()
      authorizer.foreach(_.configure(config.originals))

      /* initializing the groupConfigManager */
      groupConfigManager = new GroupConfigManager(config.groupCoordinatorConfig.extractGroupConfigMap(config.shareGroupConfig))

      /* create share coordinator */
      shareCoordinator = createShareCoordinator()

      /* create persister */
      persister = createShareStatePersister()

      groupCoordinator = createGroupCoordinator()

      val producerIdManagerSupplier = () => ProducerIdManager.rpc(
        config.brokerId,
        time,
        () => lifecycleManager.brokerEpoch,
        clientToControllerChannelManager
      )

      // Create transaction coordinator, but don't start it until we've started replica manager.
      // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
      transactionCoordinator = TransactionCoordinator(config, replicaManager,
        new KafkaScheduler(1, true, "transaction-log-manager-"),
        producerIdManagerSupplier, metrics, metadataCache, Time.SYSTEM)

      autoTopicCreationManager = new DefaultAutoTopicCreationManager(
        config, clientToControllerChannelManager, groupCoordinator,
        transactionCoordinator, shareCoordinator)

      dynamicConfigHandlers = Map[String, ConfigHandler](
        ConfigType.TOPIC -> new TopicConfigHandler(replicaManager, config, quotaManagers),
        ConfigType.BROKER -> new BrokerConfigHandler(config, quotaManagers),
        ConfigType.CLIENT_METRICS -> new ClientMetricsConfigHandler(clientMetricsManager),
        ConfigType.GROUP -> new GroupConfigHandler(groupCoordinator))

      val featuresRemapped = BrokerFeatures.createDefaultFeatureMap(brokerFeatures)

      val brokerLifecycleChannelManager = new NodeToControllerChannelManagerImpl(
        controllerNodeProvider,
        time,
        metrics,
        config,
        "heartbeat",
        s"broker-${config.nodeId}-",
        config.brokerHeartbeatIntervalMs
      )
      lifecycleManager.start(
        () => sharedServer.loader.lastAppliedOffset(),
        brokerLifecycleChannelManager,
        clusterId,
        listenerInfo.toBrokerRegistrationRequest,
        featuresRemapped,
        logManager.readBrokerEpochFromCleanShutdownFiles()
      )

      // The FetchSessionCache is divided into config.numIoThreads shards, each responsible
      // for Math.max(1, shardNum * sessionIdRange) <= sessionId < (shardNum + 1) * sessionIdRange
      val sessionIdRange = Int.MaxValue / NumFetchSessionCacheShards
      val fetchSessionCacheShards = (0 until NumFetchSessionCacheShards)
        .map(shardNum => new FetchSessionCacheShard(
          config.maxIncrementalFetchSessionCacheSlots / NumFetchSessionCacheShards,
          KafkaBroker.MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS,
          sessionIdRange,
          shardNum
        ))
      val fetchManager = new FetchManager(Time.SYSTEM, new FetchSessionCache(fetchSessionCacheShards))

      val shareFetchSessionCache : ShareSessionCache = new ShareSessionCache(
        config.shareGroupConfig.shareGroupMaxGroups * config.groupCoordinatorConfig.shareGroupMaxSize,
        KafkaBroker.MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS)

      sharePartitionManager = new SharePartitionManager(
        replicaManager,
        time,
        shareFetchSessionCache,
        config.shareGroupConfig.shareGroupRecordLockDurationMs,
        config.shareGroupConfig.shareGroupDeliveryCountLimit,
        config.shareGroupConfig.shareGroupPartitionMaxRecordLocks,
        config.shareGroupConfig.shareFetchMaxFetchRecords,
        persister,
        groupConfigManager,
        metrics
      )

      // Build the RuleEngine before KafkaApis so the request gate has a real
      // (initially empty) RuleSet to consult from the very first request.
      // The actual drain from the __governance log happens below, before
      // SocketServer.enableRequestProcessing — see governanceBootstrap.
      //
      // Codex deep-audit P1#1 (round-2): the trusted-bypass set is sourced
      // from the dedicated `governance.bypass.principals` config, NOT from
      // `super.users`. The two were coupled in an earlier iteration; that
      // coupling was wrong on three independent axes:
      //   1. It over-granted the bypass to non-broker super-users that
      //      happened to reach the inter-broker listener.
      //   2. It under-granted when the broker principal was ACL-authorized
      //      but not enrolled in super.users.
      //   3. An empty super.users (a valid config) silently re-introduced
      //      the listener-only fallback the dedicated config was meant to
      //      close.
      // The new path uses a narrow identity concept (which principals may
      // ride the broker-internal traffic bypass) decoupled from the broad
      // authorization concept that super.users represents. Malformed entries
      // fail broker startup loudly inside parseBypassPrincipals — no silent
      // drop, no fail-open.
      //
      // Codex round-3 P0: an EMPTY parsed set must also fail broker startup,
      // not silently produce a broker mode where a DENY-all/FETCH rule can
      // block broker-internal traffic (replica fetchers, the __governance
      // log consumer, KRaft metadata fetches). The fix is unambiguous and
      // operator-facing: the broker refuses to start until at least one
      // principal is enrolled. Operators must explicitly list the broker's
      // own authenticated principal (eg. `User:ANONYMOUS` for PLAINTEXT
      // inter-broker, or the SSL/SASL-derived principal otherwise).
      //
      // Codex round-4 F1: use `config.getString(...)` (resolves ConfigDef
      // defaults) NOT `config.originals().get(...)` (raw input only —
      // returns null when the operator leaves the config unset, defeating
      // the ConfigDef default). The chosen production default is "" so the
      // unset case correctly fails startup; tests set this explicitly via
      // TestUtils.createBrokerConfig.
      // R34-B-3: pass `logCanonicalisation=true` so operators see the
      // X500 canonical-form rewrite line at startup. This is the only
      // call site that should log canonicalisation — the validator/
      // admin-API admission path uses the silent no-arg overload to
      // close the log-amplification primitive opened by R34-B-1.
      val bypassPrincipals: java.util.Set[String] =
        RuleEngine.parseBypassPrincipals(
          config.getString(ServerConfigs.GOVERNANCE_BYPASS_PRINCIPALS_CONFIG),
          true)
      if (bypassPrincipals.isEmpty) {
        throw new ConfigException(
          ServerConfigs.GOVERNANCE_BYPASS_PRINCIPALS_CONFIG,
          "",
          "must be a non-empty semicolon-separated list of Kafka principals. " +
            "At least one entry MUST be the broker's own authenticated principal " +
            "(eg. `User:ANONYMOUS` for PLAINTEXT inter-broker; the SSL/SASL-derived " +
            "principal otherwise) so that broker-internal traffic — replica fetchers, " +
            "the __governance log consumer, KRaft metadata fetches — cannot be blocked " +
            "by a DENY-all CEL rule. An empty list would leave inter-broker traffic " +
            "subject to rule evaluation and is rejected at startup."
        )
      }
      ruleEngine = new RuleEngine(bypassPrincipals)

      // Authoritative probe for "is this broker a replica of __governance-0?".
      // The legacy bootstrap conflated "topic absent" with "broker not a
      // replica" via a single `getLog == None` branch, which silently fail-
      // opened on every non-replica broker. We use the KRaft metadata image
      // — the same image the broker uses for every other replica-assignment
      // decision — and let BrokerGovernanceBootstrap fail closed (or warn
      // loudly with the config knob off) on the NonReplica state.
      //
      // Why this is safe against a "metadata not yet caught up" race
      // (Codex P1 audit follow-up): the startup-path drainOnce() below is
      // called AFTER `lifecycleManager.initialCatchUpFuture` AND
      // `brokerMetadataPublisher.firstPublishFuture` complete — i.e. AFTER
      // this broker has been acknowledged caught-up by the active controller
      // and AFTER metadata has been published up to the cluster-metadata
      // partition's high-water-mark. By that point `metadataCache.currentImage`
      // reflects a coherent cluster-wide snapshot, not a partial pre-catch-up
      // view, so `TopicAbsent` here genuinely means "the cluster does not
      // have __governance yet", not "this broker has not seen the topic
      // creation record yet". Do NOT move the bootstrap drain before those
      // catch-up waits; if a refactor ever does, this probe loses its
      // authority and the TopicAbsent branch becomes a fail-open window for
      // any broker still catching up at the moment startup runs.
      val localReplicaProbe: () => LocalReplicaStatus = () => {
        val image = metadataCache.currentImage()
        val topicImage = image.topics().getTopic(GovernanceTopic.NAME)
        if (topicImage == null) {
          LocalReplicaStatus.TopicAbsent
        } else {
          val part = topicImage.partitions().get(0)
          if (part == null) LocalReplicaStatus.TopicAbsent
          else if (part.replicas.contains(config.nodeId)) LocalReplicaStatus.LocalReplica
          else LocalReplicaStatus.NonReplica
        }
      }
      // governance.bootstrap.require.local.replica — fail-closed by default.
      // Read directly from originals() rather than wiring through KafkaConfig
      // so this P0 broker-safety knob doesn't drag in config doc / validator
      // surface area. Promote to a first-class config if it ever sees broader
      // operational use. Parser extracted to BrokerServer.parseRequireLocalReplica
      // for direct unit-test coverage of the fail-closed contract (R39-E-2).
      val requireLocalReplica: Boolean = BrokerServer.parseRequireLocalReplica(
        config.originals().get("governance.bootstrap.require.local.replica"))
      // Catchup probe (Codex deep-audit P0b + P0c): tells drainStartup when
      // this broker's local log is safe to drain — i.e. when local HW
      // reflects a recent cluster-committed point on the governance
      // partition. Two sufficient conditions, both observable on this
      // broker without an RPC to the leader:
      //   1. This broker IS the leader (its HW is, by definition, the
      //      cluster-wide commit point).
      //   2. This broker is a follower IN the controller-published ISR
      //      (the controller considers this broker caught up within
      //      replica.lag.time.max.ms).
      //
      // P0c subtlety (Codex follow-up review of P0b): the LOCAL Partition
      // object is the WRONG source for the ISR check on a follower.
      // Partition.makeFollower clears the local ISR to Set.empty
      // (kafka.cluster.Partition line 853, `isr = Set.empty`) — that field
      // is leader-side bookkeeping and is meaningless on followers. Using
      // `partition.inSyncReplicaIds.contains(nodeId)` on a follower
      // therefore ALWAYS returns false, so the probe would never flip true
      // and every follower broker would time out at the drainStartup
      // deadline. The authoritative source is the controller-published
      // metadata image, which is what `localReplicaProbe` already uses
      // for the replicas check above. Reuse that pattern here.
      //
      // For the leader detection we still read the local Partition object —
      // `Partition.isLeader` is set correctly on leaders. Equivalent
      // metadata-side check would be
      // `partitionImage.leader == config.nodeId`, but local self-leadership
      // is the cheaper and stricter answer (we cannot be the active leader
      // unless our local Partition state agrees).
      val tpGov = new org.apache.kafka.common.TopicPartition(GovernanceTopic.NAME, 0)
      val caughtUpProbe: () => Boolean = () => {
        // Self-leadership branch: local Partition state.
        val isLeader = replicaManager.onlinePartition(tpGov).exists(_.isLeader)
        if (isLeader) {
          true
        } else {
          // Follower-in-ISR branch: read the controller-published ISR
          // from the metadata image. This survives the makeFollower
          // clear-to-empty, and matches the controller's own definition
          // of "caught up".
          val image = metadataCache.currentImage()
          val topicImage = image.topics().getTopic(GovernanceTopic.NAME)
          if (topicImage == null) {
            false
          } else {
            val part = topicImage.partitions().get(0)
            part != null && part.isr.contains(config.nodeId)
          }
        }
      }
      // Round-15 BLOCKER-2 (recent-changes sub-agent): live partition-count
      // probe sourced from the controller-published metadata image. Works on
      // every broker — replica or not — so a broker that never holds a
      // __governance replica still detects an operator AlterPartitions that
      // grew the topic past one partition and surfaces the silent-fail-OPEN.
      // Defaults to 1 when the topic is absent (no drift to flag yet) and
      // when the metadata image has no partitions map (catastrophic — log
      // the absence loudly via the bootstrap's WARN throttle anyway by
      // reporting 0 so the > 1 guard stays silent but a later partitions()
      // anomaly is still caught by the > 1 check the next time around).
      val partitionCountProbe: () => Int = () => {
        val image = metadataCache.currentImage()
        val topicImage = image.topics().getTopic(GovernanceTopic.NAME)
        if (topicImage == null) 1
        else {
          val parts = topicImage.partitions()
          if (parts == null) 1 else parts.size()
        }
      }
      governanceBootstrap = new BrokerGovernanceBootstrap(
        replicaManager = replicaManager,
        ruleEngine = ruleEngine,
        localReplicaStatus = localReplicaProbe,
        requireLocalReplica = requireLocalReplica,
        caughtUpProbe = caughtUpProbe,
        partitionCountProbe = partitionCountProbe)

      dataPlaneRequestProcessor = new KafkaApis(
        requestChannel = socketServer.dataPlaneRequestChannel,
        forwardingManager = forwardingManager,
        replicaManager = replicaManager,
        groupCoordinator = groupCoordinator,
        txnCoordinator = transactionCoordinator,
        shareCoordinator = shareCoordinator,
        autoTopicCreationManager = autoTopicCreationManager,
        brokerId = config.nodeId,
        config = config,
        configRepository = metadataCache,
        metadataCache = metadataCache,
        metrics = metrics,
        authorizer = authorizer,
        quotas = quotaManagers,
        fetchManager = fetchManager,
        sharePartitionManager = sharePartitionManager,
        brokerTopicStats = brokerTopicStats,
        clusterId = clusterId,
        time = time,
        tokenManager = tokenManager,
        apiVersionManager = apiVersionManager,
        clientMetricsManager = clientMetricsManager,
        ruleEngine = ruleEngine)

      dataPlaneRequestHandlerPool = new KafkaRequestHandlerPool(config.nodeId,
        socketServer.dataPlaneRequestChannel, dataPlaneRequestProcessor, time,
        config.numIoThreads, s"${DataPlaneAcceptor.MetricPrefix}RequestHandlerAvgIdlePercent",
        DataPlaneAcceptor.ThreadPrefix)

      // Start RemoteLogManager before initializing broker metadata publishers.
      remoteLogManagerOpt.foreach { rlm =>
        val listenerName = config.remoteLogManagerConfig.remoteLogMetadataManagerListenerName()
        if (listenerName != null) {
          val endpoint = listenerInfo.listeners().values().stream
            .filter(e =>
              e.listenerName().isPresent &&
                ListenerName.normalised(e.listenerName().get()).equals(ListenerName.normalised(listenerName))
            )
            .findFirst()
            .orElseThrow(() => new ConfigException(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP,
              listenerName, "Should be set as a listener name within valid broker listener name list: " + listenerInfo.listeners().values()))
          rlm.onEndPointCreated(endpoint)
        }
        rlm.startup()
      }

      metadataPublishers.add(new MetadataVersionConfigValidator(config, sharedServer.metadataPublishingFaultHandler))
      brokerMetadataPublisher = new BrokerMetadataPublisher(config,
        metadataCache,
        logManager,
        replicaManager,
        groupCoordinator,
        transactionCoordinator,
        shareCoordinator,
        new DynamicConfigPublisher(
          config,
          sharedServer.metadataPublishingFaultHandler,
          dynamicConfigHandlers.toMap,
        "broker"),
        new DynamicClientQuotaPublisher(
          config,
          sharedServer.metadataPublishingFaultHandler,
          "broker",
          clientQuotaMetadataManager,
        ),
        new DynamicTopicClusterQuotaPublisher(
          clusterId,
          config,
          sharedServer.metadataPublishingFaultHandler,
          "broker",
          quotaManagers,
        ),
        new ScramPublisher(
          config,
          sharedServer.metadataPublishingFaultHandler,
          "broker",
          credentialProvider),
        new DelegationTokenPublisher(
          config,
          sharedServer.metadataPublishingFaultHandler,
          "broker",
          tokenManager),
        new AclPublisher(
          config.nodeId,
          sharedServer.metadataPublishingFaultHandler,
          "broker",
          authorizer
        ),
        sharedServer.initialBrokerMetadataLoadFaultHandler,
        sharedServer.metadataPublishingFaultHandler
      )
      // If the BrokerLifecycleManager's initial catch-up future fails, it means we timed out
      // or are shutting down before we could catch up. Therefore, also fail the firstPublishFuture.
      lifecycleManager.initialCatchUpFuture.whenComplete((_, e) => {
        if (e != null) brokerMetadataPublisher.firstPublishFuture.completeExceptionally(e)
      })
      metadataPublishers.add(brokerMetadataPublisher)
      brokerRegistrationTracker = new BrokerRegistrationTracker(config.brokerId,
        () => lifecycleManager.resendBrokerRegistrationUnlessZkMode())
      metadataPublishers.add(brokerRegistrationTracker)


      // Register parts of the broker that can be reconfigured via dynamic configs.  This needs to
      // be done before we publish the dynamic configs, so that we don't miss anything.
      config.dynamicConfig.addReconfigurables(this)

      // Install all the metadata publishers.
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "the broker metadata publishers to be installed",
        sharedServer.loader.installPublishers(metadataPublishers), startupDeadline, time)

      // Wait for this broker to contact the quorum, and for the active controller to acknowledge
      // us as caught up. It will do this by returning a heartbeat response with isCaughtUp set to
      // true. The BrokerLifecycleManager tracks this.
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "the controller to acknowledge that we are caught up",
        lifecycleManager.initialCatchUpFuture, startupDeadline, time)

      // Wait for the first metadata update to be published. Metadata updates are not published
      // until we read at least up to the high water mark of the cluster metadata partition.
      // Usually, we publish the initial metadata before lifecycleManager.initialCatchUpFuture
      // is completed, so this check is not necessary. But this is a simple check to make
      // completely sure.
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "the initial broker metadata update to be published",
        brokerMetadataPublisher.firstPublishFuture , startupDeadline, time)

      // Now that we have loaded some metadata, we can log a reasonably up-to-date broker
      // configuration.  Keep in mind that KafkaConfig.originals is a mutable field that gets set
      // by the dynamic configuration publisher. Ironically, KafkaConfig.originals does not
      // contain the original configuration values.
      new KafkaConfig(config.originals(), true)

      // We're now ready to unfence the broker. This also allows this broker to transition
      // from RECOVERY state to RUNNING state, once the controller unfences the broker.
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "the broker to be unfenced",
        lifecycleManager.setReadyToUnfence(), startupDeadline, time)

      // Enable inbound TCP connections. Each endpoint will be started only once its matching
      // authorizer future is completed.
      val endpointReadyFutures = {
        val builder = new EndpointReadyFutures.Builder()
        builder.build(authorizer.toJava,
          new KafkaAuthorizerServerInfo(
            new ClusterResource(clusterId),
            config.nodeId,
            listenerInfo.listeners().values(),
            listenerInfo.firstListener(),
            config.earlyStartListeners.map(_.value()).asJava))
      }
      val authorizerFutures = endpointReadyFutures.futures().asScala.toMap

      // Drain __governance from the local log into the RuleEngine BEFORE
      // opening the SocketServer to client connections. Direct-log read via
      // ReplicaManager.getLog bypasses the network entirely, so this works
      // without the chicken-and-egg of a KafkaConsumer needing the socket
      // open in order to fetch from this very broker.
      //
      // Fail-closed policy: if the startup drain throws, do NOT catch it —
      // let it propagate so broker startup aborts before SocketServer opens.
      // The alternative (catch + WARN + continue) is silent fail-open: a
      // broker that IS a replica of __governance but can't read its local log
      // (corrupt segment, disk fault) would open client traffic with an
      // empty RuleSet while real DENY rules exist on the topic, evading
      // enforcement.
      //
      // Unlike the scheduler's drainOnce, drainStartup bounded-waits for the
      // local log to become available when the cluster metadata says this
      // broker IS a replica. drainOnce's "log briefly disappeared → keep
      // last-known-good" posture is correct AFTER a successful prior install
      // (engine.active() carries forward), but at first startup that
      // last-known-good IS RuleSet.EMPTY — returning 0L without ever reading
      // the log would silently bypass every rule. drainStartup polls until
      // the log opens, then drains; if the deadline elapses, it throws an
      // IllegalStateException naming the partition and the operator recovery
      // path. Codex deep-audit P0.
      //
      // What we still log: drained count on success. Hard failures crash
      // startup with the original exception in the broker log, which is
      // the visibility we want — an operator must intervene rather than
      // a security-critical event sliding by at WARN level.
      //
      // __governance compaction enforcement (audit round-5 finding
      // a016e43dc + round-12 tombstone/compaction sub-agent HIGH-1).
      // If cleanup.policy on the topic does not include "compact",
      // retention-based deletion eventually erases older rule records.
      // The first restart that occurs after retention.ms elapses
      // (default 7 days) drains a truncated log, installs RuleSet.EMPTY,
      // and the broker fail-OPENs every previously-denied request.
      // This is a delayed, audit-invisible regression — operators only
      // notice when unauthorized traffic shows up in request logs.
      //
      // Round-5 originally WARN-and-proceeded here, on the rationale that
      // "hard-failing startup over a topic config the broker doesn't own
      // would be a heavy hammer for a config typo". Round-12 reversed that
      // call: a WARN that an operator can ignore is not a safety mechanism
      // for a security-critical gate, and the silent fail-open after the
      // retention window is exactly the kind of latent regression a
      // security audit must close. The remediation is one operator
      // command (`kafka-configs.sh --alter ... cleanup.policy=compact`),
      // executed once, and the broker comes up — far less costly than a
      // silent fail-open weeks after deploy. Effective policy = topic-
      // level override if present, otherwise the broker-default
      // log.cleanup.policy (Kafka default: "delete"), matching how
      // LogManager resolves the policy when opening this log.
      // R35-A3 [MED]: snapshot the metadata image ONCE and use it for every
      // governance topic read. The metadata cache can swap snapshots between
      // independent `currentImage()` calls — on cluster cold-start the
      // controller may publish a new image right as this broker reaches the
      // gate, and a between-reads swap producing asymmetric `null`/non-null
      // observations would short-circuit `topicExists=false` on the gate
      // input even though `getTopic` already returned a non-null entry.
      // Concrete bad shape: image-1 lacks the topic and the original
      // double-read computed `govImage=null` → `govTopicLevel=null` →
      // `topicExists=false`; image-2 has the topic created and the partition
      // gate's `partitions().size()` would have read the value from image-2,
      // but the call site fell into the `else 0` branch keyed on
      // `govImage != null` (the IMAGE-1 read). The two gates would see
      // disagreeing views of the same topic. Snapshotting once binds the
      // gates to a coherent metadata observation.
      val govImage0 = metadataCache.currentImage()
      val govImage = govImage0.topics().getTopic(GovernanceTopic.NAME)
      val govTopicLevel = if (govImage != null) govImage0.configs()
        .configMapForResource(new ConfigResource(ConfigResource.Type.TOPIC, GovernanceTopic.NAME))
        .get(TopicConfig.CLEANUP_POLICY_CONFIG) else null
      BrokerServer.requireGovernanceTopicCompactPolicy(
        govImage != null, govTopicLevel, config.logCleanupPolicy)
      // Round-14 BLOCKER N5 (compaction sub-agent): BrokerGovernanceBootstrap
      // hardcodes drain on partition 0 of __governance. A multi-partition
      // topic silently fail-OPENs every rule whose key hashes to a non-0
      // partition. Fail-closed before opening client traffic.
      BrokerServer.requireGovernanceTopicSinglePartition(
        govImage != null, if (govImage != null) govImage.partitions().size() else 0)
      val drained = governanceBootstrap.drainStartup(
        BrokerServer.GovernanceStartupDrainDeadlineMs)
      info(s"governance bootstrap drained $drained rule record(s) from " +
        s"${GovernanceTopic.NAME} before opening request processing")
      // Schedule ongoing re-drain so rule updates published after startup
      // are picked up without restart.
      governanceBootstrap.scheduleOngoing(kafkaScheduler,
        BrokerServer.GovernanceDrainIntervalMs)

      val enableRequestProcessingFuture = socketServer.enableRequestProcessing(authorizerFutures)

      // Block here until all the authorizer futures are complete.
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "all of the authorizer futures to be completed",
        CompletableFuture.allOf(authorizerFutures.values.toSeq: _*), startupDeadline, time)

      // Wait for all the SocketServer ports to be open, and the Acceptors to be started.
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "all of the SocketServer Acceptors to be started",
        enableRequestProcessingFuture, startupDeadline, time)

      maybeChangeStatus(STARTING, STARTED)
    } catch {
      case e: Throwable =>
        maybeChangeStatus(STARTING, STARTED)
        fatal("Fatal error during broker startup. Prepare to shutdown", e)
        shutdown()
        throw if (e.isInstanceOf[ExecutionException]) e.getCause else e
    }
  }

  private def createGroupCoordinator(): GroupCoordinator = {
    // Create group coordinator, but don't start it until we've started replica manager.
    // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good
    // to fix the underlying issue.
    if (config.isNewGroupCoordinatorEnabled) {
      val time = Time.SYSTEM
      val serde = new GroupCoordinatorRecordSerde
      val timer = new SystemTimerReaper(
        "group-coordinator-reaper",
        new SystemTimer("group-coordinator")
      )
      val loader = new CoordinatorLoaderImpl[CoordinatorRecord](
        time,
        replicaManager,
        serde,
        config.groupCoordinatorConfig.offsetsLoadBufferSize,
        CoordinatorLoaderImpl.DEFAULT_COMMIT_INTERVAL_OFFSETS
      )
      val writer = new CoordinatorPartitionWriter(
        replicaManager
      )
      new GroupCoordinatorService.Builder(config.brokerId, config.groupCoordinatorConfig)
        .withTime(time)
        .withTimer(timer)
        .withLoader(loader)
        .withWriter(writer)
        .withCoordinatorRuntimeMetrics(new GroupCoordinatorRuntimeMetrics(metrics))
        .withGroupCoordinatorMetrics(new GroupCoordinatorMetrics(KafkaYammerMetrics.defaultRegistry, metrics))
        .withGroupConfigManager(groupConfigManager)
        .withAuthorizer(authorizer.toJava)
        .build()
    } else {
      GroupCoordinatorAdapter(
        config,
        replicaManager,
        Time.SYSTEM,
        metrics
      )
    }
  }

  private def createShareCoordinator(): Option[ShareCoordinator] = {
    if (config.shareGroupConfig.isShareGroupEnabled &&
      config.shareGroupConfig.shareGroupPersisterClassName().nonEmpty) {
      val time = Time.SYSTEM
      val timer = new SystemTimerReaper(
        "share-coordinator-reaper",
        new SystemTimer("share-coordinator")
      )

      val serde = new ShareCoordinatorRecordSerde
      val loader = new CoordinatorLoaderImpl[CoordinatorRecord](
        time,
        replicaManager,
        serde,
        config.shareCoordinatorConfig.shareCoordinatorLoadBufferSize(),
        CoordinatorLoaderImpl.DEFAULT_COMMIT_INTERVAL_OFFSETS
      )
      val writer = new CoordinatorPartitionWriter(
        replicaManager
      )
      Some(new ShareCoordinatorService.Builder(config.brokerId, config.shareCoordinatorConfig)
        .withTimer(timer)
        .withTime(time)
        .withLoader(loader)
        .withWriter(writer)
        .withCoordinatorRuntimeMetrics(new ShareCoordinatorRuntimeMetrics(metrics))
        .withCoordinatorMetrics(new ShareCoordinatorMetrics(metrics))
        .build())
    } else {
      None
    }
  }

  private def createShareStatePersister(): Persister = {
    if (config.shareGroupConfig.isShareGroupEnabled &&
      config.shareGroupConfig.shareGroupPersisterClassName.nonEmpty) {
      val klass = Utils.loadClass(config.shareGroupConfig.shareGroupPersisterClassName, classOf[Object]).asInstanceOf[Class[Persister]]

      if (klass.getName.equals(classOf[DefaultStatePersister].getName)) {
        klass.getConstructor(classOf[PersisterStateManager])
          .newInstance(
            new PersisterStateManager(
              NetworkUtils.buildNetworkClient("Persister", config, metrics, Time.SYSTEM, new LogContext(s"[Persister broker=${config.brokerId}]")),
              new ShareCoordinatorMetadataCacheHelperImpl(metadataCache, key => shareCoordinator.get.partitionFor(key), config.interBrokerListenerName),
              Time.SYSTEM,
              new SystemTimerReaper(
                "persister-state-manager-reaper",
                new SystemTimer("persister")
              )
            )
          )
      } else if (klass.getName.equals(classOf[NoOpShareStatePersister].getName)) {
        info("Using no op persister")
        new NoOpShareStatePersister()
      } else {
        error("Unknown persister specified. Persister is only factory pluggable!")
        throw new IllegalArgumentException("Unknown persiser specified " + config.shareGroupConfig.shareGroupPersisterClassName)
      }
    } else {
      // in case share coordinator not enabled or
      // persister class name deliberately empty (key=)
      info("Using no op persister")
      new NoOpShareStatePersister()
    }
  }

  protected def createRemoteLogManager(): Option[RemoteLogManager] = {
    if (config.remoteLogManagerConfig.isRemoteStorageSystemEnabled()) {
      Some(new RemoteLogManager(config.remoteLogManagerConfig, config.brokerId, config.logDirs.head, clusterId, time,
        (tp: TopicPartition) => logManager.getLog(tp).toJava,
        (tp: TopicPartition, remoteLogStartOffset: java.lang.Long) => {
          logManager.getLog(tp).foreach { log =>
            log.updateLogStartOffsetFromRemoteTier(remoteLogStartOffset)
          }
        },
        brokerTopicStats, metrics))
    } else {
      None
    }
  }

  override def shutdown(timeout: Duration): Unit = {
    if (!maybeChangeStatus(STARTED, SHUTTING_DOWN)) return
    try {
      val deadline = time.milliseconds() + timeout.toMillis
      info("shutting down")

      if (config.controlledShutdownEnable) {
        if (replicaManager != null)
          replicaManager.beginControlledShutdown()

        if (lifecycleManager != null) {
          lifecycleManager.beginControlledShutdown()
          try {
            val controlledShutdownTimeoutMs = deadline - time.milliseconds()
            lifecycleManager.controlledShutdownFuture.get(controlledShutdownTimeoutMs, TimeUnit.MILLISECONDS)
          } catch {
            case _: TimeoutException =>
              error("Timed out waiting for the controller to approve controlled shutdown")
            case e: Throwable =>
              error("Got unexpected exception waiting for controlled shutdown future", e)
          }
        }
      }
      if (lifecycleManager != null)
        lifecycleManager.beginShutdown()

      // Stop socket server to stop accepting any more connections and requests.
      // Socket server will be shutdown towards the end of the sequence.
      if (socketServer != null) {
        CoreUtils.swallow(socketServer.stopProcessingRequests(), this)
      }
      metadataPublishers.forEach(p => sharedServer.loader.removeAndClosePublisher(p).get())
      metadataPublishers.clear()
      if (dataPlaneRequestHandlerPool != null)
        CoreUtils.swallow(dataPlaneRequestHandlerPool.shutdown(), this)
      if (dataPlaneRequestProcessor != null)
        CoreUtils.swallow(dataPlaneRequestProcessor.close(), this)
      authorizer.foreach(Utils.closeQuietly(_, "authorizer"))

      /**
       * We must shutdown the scheduler early because otherwise, the scheduler could touch other
       * resources that might have been shutdown and cause exceptions.
       * For example, if we didn't shutdown the scheduler first, when LogManager was closing
       * partitions one by one, the scheduler might concurrently delete old segments due to
       * retention. However, the old segments could have been closed by the LogManager, which would
       * cause an IOException and subsequently mark logdir as offline. As a result, the broker would
       * not flush the remaining partitions or write the clean shutdown marker. Ultimately, the
       * broker would have to take hours to recover the log during restart.
       */
      if (kafkaScheduler != null)
        CoreUtils.swallow(kafkaScheduler.shutdown(), this)

      if (transactionCoordinator != null)
        CoreUtils.swallow(transactionCoordinator.shutdown(), this)

      if (groupConfigManager != null)
        CoreUtils.swallow(groupConfigManager.close(), this)
      if (groupCoordinator != null)
        CoreUtils.swallow(groupCoordinator.shutdown(), this)
      if (shareCoordinator.isDefined)
        CoreUtils.swallow(shareCoordinator.get.shutdown(), this)

      if (tokenManager != null)
        CoreUtils.swallow(tokenManager.shutdown(), this)

      if (assignmentsManager != null)
        CoreUtils.swallow(assignmentsManager.close(), this)

      if (replicaManager != null)
        CoreUtils.swallow(replicaManager.shutdown(), this)

      if (alterPartitionManager != null)
        CoreUtils.swallow(alterPartitionManager.shutdown(), this)

      if (forwardingManager != null)
        CoreUtils.swallow(forwardingManager.close(), this)

      if (clientToControllerChannelManager != null)
        CoreUtils.swallow(clientToControllerChannelManager.shutdown(), this)

      if (logManager != null) {
        val brokerEpoch = if (lifecycleManager != null) lifecycleManager.brokerEpoch else -1
        CoreUtils.swallow(logManager.shutdown(brokerEpoch), this)
      }

      // Close remote log manager to give a chance to any of its underlying clients
      // (especially in RemoteStorageManager and RemoteLogMetadataManager) to close gracefully.
      remoteLogManagerOpt.foreach(Utils.closeQuietly(_, "remote log manager"))

      if (quotaManagers != null)
        CoreUtils.swallow(quotaManagers.shutdown(), this)

      if (socketServer != null)
        CoreUtils.swallow(socketServer.shutdown(), this)

      Utils.closeQuietly(brokerTopicStats, "broker topic stats")
      Utils.closeQuietly(sharePartitionManager, "share partition manager")

      if (persister != null)
        CoreUtils.swallow(persister.stop(), this)

      isShuttingDown.set(false)

      if (lifecycleManager != null)
        CoreUtils.swallow(lifecycleManager.close(), this)

      CoreUtils.swallow(config.dynamicConfig.clear(), this)
      Utils.closeQuietly(clientMetricsManager, "client metrics manager")
      sharedServer.stopForBroker()
      info("shut down completed")
    } catch {
      case e: Throwable =>
        fatal("Fatal error during broker shutdown.", e)
        throw e
    } finally {
      maybeChangeStatus(SHUTTING_DOWN, SHUTDOWN)
    }
  }

  override def isShutdown(): Boolean = {
    status == SHUTDOWN || status == SHUTTING_DOWN
  }

  override def awaitShutdown(): Unit = {
    lock.lock()
    try {
      while (true) {
        if (status == SHUTDOWN) return
        awaitShutdownCond.awaitUninterruptibly()
      }
    } finally {
      lock.unlock()
    }
  }

  override def boundPort(listenerName: ListenerName): Int = socketServer.boundPort(listenerName)

}
