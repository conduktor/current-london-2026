/*
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

import java.util
import java.util.Properties
import org.apache.kafka.common.config.{ConfigException, ConfigResource}
import org.apache.kafka.common.config.ConfigResource.Type.{BROKER, CLIENT_METRICS, GROUP, TOPIC}
import org.apache.kafka.controller.ConfigurationValidator
import org.apache.kafka.common.errors.{InvalidConfigurationException, InvalidRequestException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.coordinator.group.GroupConfigManager
import org.apache.kafka.server.config.ServerConfigs
import org.apache.kafka.server.metrics.ClientMetricsConfigs
import org.apache.kafka.server.rules.BypassPrincipalsValidator
import org.apache.kafka.storage.internals.log.LogConfig

import scala.collection.mutable

/**
 * The validator that the controller uses for dynamic configuration changes.
 * It performs the generic validation, which can't be bypassed. If an AlterConfigPolicy
 * is configured, the controller will check that after verifying that this passes.
 *
 * For changes to BROKER resources, the forwarding broker performs an extra validation step
 * in {@link kafka.server.ConfigAdminManager#preprocess()} before sending the change to
 * the controller. Therefore, the validation here is just a kind of sanity check, which
 * should never fail under normal conditions.
 *
 * This validator does not handle changes to BROKER_LOGGER resources. Despite being bundled
 * in the same RPC, BROKER_LOGGER is not really a dynamic configuration in the same sense
 * as the others. It is not persisted to the metadata log.
 */
class ControllerConfigurationValidator(kafkaConfig: KafkaConfig) extends ConfigurationValidator {
  // R34-B-1 [HIGH]: the controller-direct admin path (KIP-919,
  // `kafka-configs --bootstrap-controller`) historically validates ONLY
  // resource.name() for BROKER resources — newConfigs values are not
  // inspected. For most broker configs that mirror behavior of the
  // broker-routed path's ConfigAdminManager.preprocess, but for
  // governance.bypass.principals it opens a delayed time-bomb: a
  // malformed value lands in KRaft metadata via the controller-direct
  // route, and the broker refuses to start at the next restart
  // (BrokerGovernanceBootstrap calls
  // RuleEngine.parseBypassPrincipals). The admin who typed the bad
  // value sees a successful ack and never associates the eventual
  // restart failure with their change.
  //
  // Scope of this gate: this is a deliberately NARROW fix, not a
  // blanket "value-validate every BROKER config at the controller"
  // change. We only enforce the validators that protect security-
  // critical broker startup invariants (governance.bypass.principals).
  // Adding more configs here is a separate decision per-config.
  private val brokerConfigValidators: Map[String, BypassPrincipalsValidator] = Map(
    ServerConfigs.GOVERNANCE_BYPASS_PRINCIPALS_CONFIG -> new BypassPrincipalsValidator()
  )

  private def validateBrokerConfigValues(
    newConfigs: util.Map[String, String]
  ): Unit = {
    newConfigs.forEach((key, value) => {
      brokerConfigValidators.get(key).foreach { validator =>
        try {
          validator.ensureValid(key, value)
        } catch {
          case e: ConfigException =>
            // The BypassPrincipalsValidator already LogSafe-sanitises
            // the offending segment before embedding it in the
            // exception message. Rewrap into the controller's expected
            // failure type, preserving the diagnostic verbatim so the
            // admin still sees which config and which value were
            // rejected.
            throw new InvalidConfigurationException(e.getMessage, e)
        }
      }
    })
  }

  private def validateTopicName(
    name: String
  ): Unit = {
    if (name.isEmpty) {
      throw new InvalidRequestException("Default topic resources are not allowed.")
    }
    Topic.validate(name)
  }

  private def validateBrokerName(
    name: String
  ): Unit = {
    if (name.nonEmpty) {
      val brokerId = try {
        Integer.valueOf(name)
      } catch {
        case _: NumberFormatException =>
          throw new InvalidRequestException("Unable to parse broker name as a base 10 number.")
      }
      if (brokerId < 0) {
        throw new InvalidRequestException("Invalid negative broker ID.")
      }
    }
  }

  private def validateGroupName(
    name: String
  ): Unit = {
    if (name.isEmpty) {
      throw new InvalidRequestException("Default group resources are not allowed.")
    }
  }

  private def throwExceptionForUnknownResourceType(
    resource: ConfigResource
  ): Unit = {
    // Note: we should never handle BROKER_LOGGER resources here, since changes to
    // those resources are not persisted in the metadata.
    throw new InvalidRequestException(s"Unknown resource type ${resource.`type`}")
  }

  override def validate(
    resource: ConfigResource
  ): Unit = {
    resource.`type`() match {
      case TOPIC => validateTopicName(resource.name())
      case BROKER => validateBrokerName(resource.name())
      case _ => throwExceptionForUnknownResourceType(resource)
    }
  }

  override def validate(
    resource: ConfigResource,
    newConfigs: util.Map[String, String],
    oldConfigs: util.Map[String, String]
  ): Unit = {
    resource.`type`() match {
      case TOPIC =>
        validateTopicName(resource.name())
        val properties = new Properties()
        val nullTopicConfigs = new mutable.ArrayBuffer[String]()
        newConfigs.forEach((key, value) => {
          if (value == null) {
            nullTopicConfigs += key
          } else {
            properties.setProperty(key, value)
          }
        })
        if (nullTopicConfigs.nonEmpty) {
          throw new InvalidConfigurationException("Null value not supported for topic configs: " +
            nullTopicConfigs.mkString(","))
        }
        LogConfig.validate(oldConfigs, properties, kafkaConfig.extractLogConfigMap,
          kafkaConfig.remoteLogManagerConfig.isRemoteStorageSystemEnabled())
      case BROKER =>
        validateBrokerName(resource.name())
        // R34-B-1: gate the bypass-principals time-bomb. See the
        // brokerConfigValidators field comment for scope rationale.
        validateBrokerConfigValues(newConfigs)
      case CLIENT_METRICS =>
        val properties = new Properties()
        newConfigs.forEach((key, value) => properties.setProperty(key, value))
        ClientMetricsConfigs.validate(resource.name(), properties)
      case GROUP =>
        validateGroupName(resource.name())
        val properties = new Properties()
        val nullGroupConfigs = new mutable.ArrayBuffer[String]()
        newConfigs.forEach((key, value) => {
          if (value == null) {
            nullGroupConfigs += key
          } else {
            properties.setProperty(key, value)
          }
        })
        if (nullGroupConfigs.nonEmpty) {
          throw new InvalidConfigurationException("Null value not supported for group configs: " +
            nullGroupConfigs.mkString(","))
        }
        GroupConfigManager.validate(properties, kafkaConfig.groupCoordinatorConfig, kafkaConfig.shareGroupConfig)
      case _ => throwExceptionForUnknownResourceType(resource)
    }
  }
}
