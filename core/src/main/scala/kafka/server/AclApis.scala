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
import kafka.utils.Logging
import org.apache.kafka.common.acl.AclOperation._
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.acl.AclBindingFilter
import org.apache.kafka.common.errors._
import org.apache.kafka.common.message.CreateAclsResponseData.AclCreationResult
import org.apache.kafka.common.message.DeleteAclsResponseData.DeleteAclsFilterResult
import org.apache.kafka.common.message._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.security.authorizer.AuthorizerUtils
import org.apache.kafka.server.authorizer._

import java.util
import java.util.concurrent.CompletableFuture
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOptional

/**
 * Logic to handle ACL requests.
 */
class AclApis(authHelper: AuthHelper,
              authorizer: Option[Authorizer],
              requestHelper: RequestHandlerHelper,
              name: String,
              config: KafkaConfig) extends Logging {
  this.logIdent = "[AclApis-%s-%s] ".format(name, config.nodeId)
  private val alterAclsPurgatory =
      new DelayedFuturePurgatory(purgatoryName = "AlterAcls", brokerId = config.nodeId)

  def isClosed: Boolean = alterAclsPurgatory.isShutdown

  def close(): Unit = alterAclsPurgatory.shutdown()

  // postFilter is the tenant-leak scrub: KafkaApis passes a predicate that
  // returns false for AclBindings naming a reserved tenant namespace, so a
  // cluster-wide caller running a wildcard / ResourceType.ANY filter does not
  // receive a free dump of tenant ACLs. The default is `_ => true` (no scrub)
  // so non-tenant deployments and tests calling AclApis directly are unchanged.
  def handleDescribeAcls(request: RequestChannel.Request,
                         postFilter: AclBinding => Boolean = _ => true): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, DESCRIBE)
    val describeAclsRequest = request.body[DescribeAclsRequest]
    authorizer match {
      case None =>
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          new DescribeAclsResponse(new DescribeAclsResponseData()
            .setErrorCode(Errors.SECURITY_DISABLED.code)
            .setErrorMessage("No Authorizer is configured on the broker")
            .setThrottleTimeMs(requestThrottleMs),
          describeAclsRequest.version))
      case Some(auth) =>
        val filter = describeAclsRequest.filter
        val scrubbed = new util.ArrayList[AclBinding]()
        auth.acls(filter).forEach { b => if (postFilter(b)) scrubbed.add(b) }
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          new DescribeAclsResponse(new DescribeAclsResponseData()
            .setThrottleTimeMs(requestThrottleMs)
            .setResources(DescribeAclsResponse.aclsResources(scrubbed)),
          describeAclsRequest.version))
    }
    CompletableFuture.completedFuture[Unit](())
  }

  // preFilter is the tenant-namespace scrub for outside-in pollution. ControllerApis
  // passes a predicate that returns Some(ApiError) for AclBindings that name a
  // reserved tenant namespace not owned by the caller (so a cluster-wide caller
  // on `bootstrap.controllers` cannot mint `Topic:acme.orders` or
  // `User:__tenant_acme.bob` ACLs literally on the metadata log). The default is
  // `_ => None` (no scrub) so non-tenant deployments and tests calling AclApis
  // directly are unchanged.
  def handleCreateAcls(request: RequestChannel.Request,
                       preFilter: AclBinding => Option[ApiError] = _ => None): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, ALTER)
    val createAclsRequest = request.body[CreateAclsRequest]

    authorizer match {
      case None => requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        createAclsRequest.getErrorResponse(requestThrottleMs,
          new SecurityDisabledException("No Authorizer is configured.")))
        CompletableFuture.completedFuture[Unit](())
      case Some(auth) =>
        val allBindings = createAclsRequest.aclCreations.asScala.map(CreateAclsRequest.aclBinding)
        val errorResults = mutable.Map[AclBinding, AclCreateResult]()
        val validBindings = new ArrayBuffer[AclBinding]
        allBindings.foreach { acl =>
          val resource = acl.pattern
          val throwable = if (resource.resourceType == ResourceType.CLUSTER && !AuthorizerUtils.isClusterResource(resource.name))
              new InvalidRequestException("The only valid name for the CLUSTER resource is " + CLUSTER_NAME)
          else if (resource.name.isEmpty)
            new InvalidRequestException("Invalid empty resource name")
          else
            null
          val tenantRefusal = if (throwable == null) preFilter(acl) else None
          if (throwable != null) {
            debug(s"Failed to add acl $acl to $resource", throwable)
            errorResults(acl) = new AclCreateResult(throwable)
          } else if (tenantRefusal.isDefined) {
            errorResults(acl) = new AclCreateResult(tenantRefusal.get.exception())
          } else
            validBindings += acl
        }

        val future = new CompletableFuture[util.List[AclCreationResult]]()
        // Skip the Authorizer call entirely when every binding was rejected by
        // the preFilter — otherwise we'd waste an empty round-trip and surprise
        // tests / authorizers that assert no-op semantics on empty input.
        val createResults: scala.collection.mutable.Buffer[CompletableFuture[AclCreateResult]] =
          if (validBindings.isEmpty) scala.collection.mutable.Buffer.empty
          else auth.createAcls(request.context, validBindings.asJava).asScala.map(_.toCompletableFuture)

        def sendResponseCallback(): Unit = {
          val aclCreationResults = allBindings.map { acl =>
            val result = errorResults.getOrElse(acl, createResults(validBindings.indexOf(acl)).get)
            val creationResult = new AclCreationResult()
            result.exception.toScala.foreach { throwable =>
              val apiError = ApiError.fromThrowable(throwable)
              creationResult
                .setErrorCode(apiError.error.code)
                .setErrorMessage(apiError.message)
            }
            creationResult
          }
          future.complete(aclCreationResults.asJava)
        }
        if (createResults.isEmpty) {
          // Nothing forwarded — complete synchronously to avoid deadlocking the
          // purgatory on an empty watch set.
          sendResponseCallback()
        } else {
          alterAclsPurgatory.tryCompleteElseWatch(config.connectionsMaxIdleMs, createResults, sendResponseCallback)
        }

        future.thenApply[Unit] { aclCreationResults =>
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new CreateAclsResponse(new CreateAclsResponseData()
              .setThrottleTimeMs(requestThrottleMs)
              .setResults(aclCreationResults)))
        }
    }
  }

  // preFilter rejects whole filters that explicitly name a foreign tenant
  // namespace (e.g. `Topic:acme.orders` or `User:__tenant_acme.bob`) — refuses
  // them with the supplied ApiError so a cluster-wide caller cannot delete
  // tenant ACLs from `bootstrap.controllers`.
  //
  // postFilter scrubs the per-filter MatchingAcls echo: a wildcard filter that
  // we LET THROUGH still surfaces tenant-owned bindings in the response
  // (resource + principal name) — the postFilter drops them before serialisation
  // to plug that enumeration leak. Defaults are no-ops so non-tenant deployments
  // and tests calling AclApis directly are unchanged.
  def handleDeleteAcls(request: RequestChannel.Request,
                       preFilter: AclBindingFilter => Option[ApiError] = _ => None,
                       postFilter: AclBinding => Boolean = _ => true): CompletableFuture[Unit] = {
    authHelper.authorizeClusterOperation(request, ALTER)
    val deleteAclsRequest = request.body[DeleteAclsRequest]
    authorizer match {
      case None =>
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          deleteAclsRequest.getErrorResponse(requestThrottleMs,
            new SecurityDisabledException("No Authorizer is configured.")))
        CompletableFuture.completedFuture[Unit](())
      case Some(auth) =>
        val allFilters = deleteAclsRequest.filters.asScala.toList
        // Split into rejected (positional) and forwarded.
        val rejections = mutable.Map[Int, ApiError]()
        val keptIndexed = new ArrayBuffer[(Int, AclBindingFilter)]
        allFilters.zipWithIndex.foreach { case (f, idx) =>
          preFilter(f) match {
            case Some(err) => rejections(idx) = err
            case None => keptIndexed += ((idx, f))
          }
        }
        val keptFilters = keptIndexed.map(_._2).asJava
        // Skip the Authorizer call when every filter was rejected by the
        // preFilter — same rationale as the empty-bindings short-circuit on
        // createAcls above.
        val deleteResults: List[CompletableFuture[AclDeleteResult]] =
          if (keptFilters.isEmpty) Nil
          else auth.deleteAcls(request.context, keptFilters).asScala.map(_.toCompletableFuture).toList

        val future = new CompletableFuture[util.List[DeleteAclsFilterResult]]()
        def sendResponseCallback(): Unit = {
          val forwardedResults = deleteResults.map(_.get).map(DeleteAclsResponse.filterResult)
          // Apply postFilter to each forwarded result's MatchingAcls.
          forwardedResults.foreach { fr =>
            if (fr.matchingAcls != null && !fr.matchingAcls.isEmpty) {
              val scrubbed = new util.ArrayList[DeleteAclsResponseData.DeleteAclsMatchingAcl](fr.matchingAcls.size)
              fr.matchingAcls.forEach { m =>
                if (postFilter(DeleteAclsResponse.aclBinding(m))) scrubbed.add(m)
              }
              fr.setMatchingAcls(scrubbed)
            }
          }
          // Interleave at original positions.
          val merged = new util.ArrayList[DeleteAclsFilterResult](allFilters.size)
          var fwd = 0
          allFilters.indices.foreach { i =>
            rejections.get(i) match {
              case Some(err) =>
                merged.add(new DeleteAclsFilterResult()
                  .setErrorCode(err.error.code)
                  .setErrorMessage(err.message))
              case None =>
                merged.add(forwardedResults(fwd))
                fwd += 1
            }
          }
          future.complete(merged)
        }

        if (deleteResults.isEmpty) {
          // No forwarded filters — complete synchronously so we don't deadlock
          // the purgatory on an empty watch set.
          sendResponseCallback()
        } else {
          alterAclsPurgatory.tryCompleteElseWatch(config.connectionsMaxIdleMs, deleteResults, sendResponseCallback)
        }
        future.thenApply[Unit] { filterResults =>
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new DeleteAclsResponse(
              new DeleteAclsResponseData()
                .setThrottleTimeMs(requestThrottleMs)
                .setFilterResults(filterResults),
              deleteAclsRequest.version))
        }
    }
  }
 }
