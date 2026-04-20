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
package kafka.server.http

import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.util.{Optional, UUID}
import java.util.concurrent.{CompletableFuture, ThreadLocalRandom, TimeUnit}

import jakarta.ws.rs.container.ContainerRequestContext
import jakarta.ws.rs.core.{Context, MediaType, Response}
import jakarta.ws.rs.{Consumes, POST, Path, PathParam, Produces}

import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.internals.Plugin
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.record.internal.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.{RequestContext, RequestHeader}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.storage.internals.log.AppendOrigin

import kafka.server.{AuthHelper, ReplicaManager}

/**
 * REST endpoint for `POST /v1/topics/{name}`. Reuses the broker's server-side
 * produce path: authorizes via [[AuthHelper]] and appends via
 * [[ReplicaManager.appendRecords]] with a `CompletableFuture` callback, so no
 * `RequestChannel` plumbing is needed.
 */
@Path("/v1/topics")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
class ProduceResource(
    replicaManager: ReplicaManager,
    authorizerPlugin: Option[Plugin[Authorizer]],
    metadataCache: MetadataCache,
    time: Time) {

  private val authHelper = new AuthHelper(authorizerPlugin)
  private val listenerName = new ListenerName("HTTP")
  private val clientInfo = new ClientInformation("http-rest", "1.0")

  @POST
  @Path("/{name}")
  def produce(@PathParam("name") topic: String,
              body: ProduceBody,
              @Context httpCtx: ContainerRequestContext): Response = {

    val principal = Option(httpCtx.getProperty(BasicAuthFilter.PRINCIPAL_PROPERTY))
      .map(_.asInstanceOf[KafkaPrincipal])
      .getOrElse(KafkaPrincipal.ANONYMOUS)

    val reqCtx = new RequestContext(
      new RequestHeader(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion(), "http-rest", 0),
      "http-" + UUID.randomUUID(),
      InetAddress.getLoopbackAddress,
      Optional.empty(),
      principal,
      listenerName,
      SecurityProtocol.PLAINTEXT,
      clientInfo,
      false)

    if (!authHelper.authorize(reqCtx, AclOperation.WRITE, ResourceType.TOPIC, topic)) {
      return error(Response.Status.FORBIDDEN, "TOPIC_AUTHORIZATION_FAILED")
    }

    val topicId = metadataCache.getTopicId(topic)
    if (topicId == Uuid.ZERO_UUID) {
      return error(Response.Status.NOT_FOUND, "UNKNOWN_TOPIC")
    }

    val numPartitions = metadataCache.numPartitions(topic).orElse(Int.box(1)).intValue()
    val keyBytes = Option(body).flatMap(b => Option(b.key)).map(_.getBytes(StandardCharsets.UTF_8)).orNull
    val valueBytes = Option(body).flatMap(b => Option(b.value)).map(_.getBytes(StandardCharsets.UTF_8)).orNull

    val partition = if (keyBytes == null) {
      ThreadLocalRandom.current().nextInt(numPartitions)
    } else {
      Math.floorMod(java.util.Arrays.hashCode(keyBytes), numPartitions)
    }

    val tip = new TopicIdPartition(topicId, new TopicPartition(topic, partition))
    val record = new SimpleRecord(time.milliseconds(), keyBytes, valueBytes)
    val records = MemoryRecords.withRecords(Compression.NONE, record)

    val future = new CompletableFuture[java.util.Map[TopicIdPartition, PartitionResponse]]()

    replicaManager.appendRecords(
      timeout = 30000L,
      requiredAcks = 1.toShort,
      internalTopicsAllowed = false,
      origin = AppendOrigin.CLIENT,
      entriesPerPartition = Map(tip -> records),
      responseCallback = result => future.complete(result))

    val result = future.get(35, TimeUnit.SECONDS)
    val pr = result.get(tip)

    if (pr == null) {
      error(Response.Status.INTERNAL_SERVER_ERROR, "NO_RESPONSE")
    } else if (pr.error != org.apache.kafka.common.protocol.Errors.NONE) {
      error(Response.Status.INTERNAL_SERVER_ERROR, pr.error.name())
    } else {
      Response.ok(new ProduceResponseBody(partition, pr.baseOffset)).build()
    }
  }

  private def error(status: Response.Status, code: String): Response =
    Response.status(status).entity(java.util.Map.of("error", code)).build()
}
