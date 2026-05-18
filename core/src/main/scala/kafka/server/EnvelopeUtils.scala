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

import java.net.{InetAddress, UnknownHostException}
import java.nio.ByteBuffer
import kafka.network.RequestChannel
import org.apache.kafka.common.errors.{ClusterAuthorizationException, InvalidRequestException, PrincipalDeserializationException, UnsupportedVersionException}
import org.apache.kafka.common.network.ClientInformation
import org.apache.kafka.common.requests.{EnvelopeRequest, RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.network.metrics.RequestChannelMetrics
import org.apache.kafka.server.tenant.TenantNamespace

import scala.jdk.OptionConverters.RichOptional


object EnvelopeUtils {
  def handleEnvelopeRequest(
    request: RequestChannel.Request,
    requestChannelMetrics: RequestChannelMetrics,
    handler: RequestChannel.Request => Unit
  ): Unit = {
    val envelope = request.body[EnvelopeRequest]
    val forwardedPrincipal = parseForwardedPrincipal(request.context, envelope.requestPrincipal)
    val forwardedClientAddress = parseForwardedClientAddress(envelope.clientAddress)

    val forwardedRequestBuffer = envelope.requestData.duplicate()
    val forwardedRequestHeader = parseForwardedRequestHeader(forwardedRequestBuffer)

    val forwardedApi = forwardedRequestHeader.apiKey
    if (!forwardedApi.forwardable) {
      throw new InvalidRequestException(s"API $forwardedApi is not enabled or is not eligible for forwarding")
    }

    // Tenant trust boundary on the envelope re-dispatch path. The outer CLUSTER_ACTION
    // check on the controller envelope handler does not authenticate the inner
    // forwardedPrincipal — KafkaPrincipalSerde.deserialize has no signature/MAC.
    // In a default KRaft deployment every broker holds CLUSTER_ACTION via super.users,
    // so any broker could otherwise forge `__tenant_<id>.<user>` as the forwarded
    // identity and re-dispatch into any controller handler — bypassing both the
    // broker-side TENANT_ALLOWED_APIS dispatch gate and every controller-side
    // tenant-namespace guard (mint delegation token, alter SCRAM credentials, …).
    // Refuse forwarded tenant identities for APIs outside the tenant-allowed surface.
    if (forwardedPrincipal.getName.startsWith(TenantNamespace.PRINCIPAL_PREFIX)
        && !KafkaApis.TENANT_ALLOWED_APIS.contains(forwardedApi)) {
      throw new ClusterAuthorizationException(
        s"Envelope carries tenant-namespaced principal ${forwardedPrincipal.getName} " +
          s"for API $forwardedApi outside the tenant-allowed surface")
    }

    val forwardedContext = new RequestContext(
      forwardedRequestHeader,
      request.context.connectionId,
      forwardedClientAddress,
      forwardedPrincipal,
      request.context.listenerName,
      request.context.securityProtocol,
      ClientInformation.EMPTY,
      request.context.fromPrivilegedListener
    )

    val forwardedRequest = parseForwardedRequest(
      request,
      forwardedContext,
      forwardedRequestBuffer,
      requestChannelMetrics
    )
    handler(forwardedRequest)
  }

  private def parseForwardedClientAddress(
    address: Array[Byte]
  ): InetAddress = {
    try {
      InetAddress.getByAddress(address)
    } catch {
      case e: UnknownHostException =>
        throw new InvalidRequestException("Failed to parse client address from envelope", e)
    }
  }

  private def parseForwardedRequest(
    envelope: RequestChannel.Request,
    forwardedContext: RequestContext,
    buffer: ByteBuffer,
    requestChannelMetrics: RequestChannelMetrics
  ): RequestChannel.Request = {
    try {
      val forwardedRequest = new RequestChannel.Request(
        processor = envelope.processor,
        context = forwardedContext,
        startTimeNanos = envelope.startTimeNanos,
        envelope.memoryPool,
        buffer,
        requestChannelMetrics,
        Some(envelope)
      )
      // set the dequeue time of forwardedRequest as the value of envelope request
      forwardedRequest.requestDequeueTimeNanos = envelope.requestDequeueTimeNanos
      forwardedRequest
    } catch {
      case e: InvalidRequestException =>
        // We use UNSUPPORTED_VERSION if the embedded request cannot be parsed.
        // The purpose is to disambiguate structural errors in the envelope request
        // itself, such as an invalid client address.
        throw new UnsupportedVersionException(s"Failed to parse forwarded request " +
          s"with header ${forwardedContext.header}", e)
    }
  }

  private def parseForwardedRequestHeader(
    buffer: ByteBuffer
  ): RequestHeader = {
    try {
      RequestHeader.parse(buffer)
    } catch {
      case e: InvalidRequestException =>
        // We use UNSUPPORTED_VERSION if the embedded request cannot be parsed.
        // The purpose is to disambiguate structural errors in the envelope request
        // itself, such as an invalid client address.
        throw new UnsupportedVersionException("Failed to parse request header from envelope", e)
    }
  }

  private def parseForwardedPrincipal(
    envelopeContext: RequestContext,
    principalBytes: Array[Byte]
  ): KafkaPrincipal = {
    envelopeContext.principalSerde.toScala match {
      case Some(serde) =>
        try {
          serde.deserialize(principalBytes)
        } catch {
          case e: Exception =>
            throw new PrincipalDeserializationException("Failed to deserialize client principal from envelope", e)
        }

      case None =>
        throw new PrincipalDeserializationException("Could not deserialize principal since " +
          "no `KafkaPrincipalSerde` has been defined")
    }
  }
}
