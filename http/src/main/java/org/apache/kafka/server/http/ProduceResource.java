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
package org.apache.kafka.server.http;

import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.http.api.AuthorizationHelper;
import org.apache.kafka.server.http.api.HttpEndpoint;
import org.apache.kafka.server.http.api.RecordAppender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * REST endpoint for {@code POST /v1/topics/{name}}. Reuses the broker's
 * server-side produce path: authorizes via {@link AuthorizationHelper} and
 * appends via {@link RecordAppender}, resuming the suspended async response
 * from the append callback so no Jetty worker thread blocks on I/O.
 *
 * <h3>Design trade-offs worth knowing</h3>
 *
 * <p><b>Partitioning.</b> We compute the target partition with
 * {@link BuiltInPartitioner#partitionForKey}, which is murmur2 — the default
 * {@code KafkaProducer} partitioner. A record POSTed with key {@code k} lands
 * on the same partition as the same key produced through a default-configured
 * {@code KafkaProducer}. Clients that ship a custom {@code partitioner.class}
 * on the producer side will land keys differently; the REST proxy cannot
 * introspect client-side partitioner plugins and does not try to.</p>
 *
 * <p><b>SecurityProtocol in the RequestContext.</b> Authorizers see
 * {@link SecurityProtocol#SSL} for HTTPS (truthful — TLS) and
 * {@link SecurityProtocol#PLAINTEXT} for HTTP (truthful — no transport
 * encryption). The REST proxy is not a binary Kafka client, so
 * {@link ApiKeys#PRODUCE} in the {@link RequestHeader} is a shape concession,
 * not a wire protocol claim. An {@link Authorizer} that inspects
 * {@code ApiKeys} to decide ACLs will treat this request as a PRODUCE — which
 * is semantically what it is.</p>
 */
@Path("/v1/topics")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ProduceResource {

    private static final Logger LOG = LoggerFactory.getLogger(ProduceResource.class);

    /** Leeway between the broker's append deadline and Jersey's async timeout, in ms. */
    private static final long ASYNC_TIMEOUT_SLACK_MS = 5_000L;

    private static final short REQUIRED_ACKS_LEADER = 1;

    private final RecordAppender appender;
    private final AuthorizationHelper auth;
    private final MetadataCache metadataCache;
    private final Time time;
    private final long appendTimeoutMs;
    private final long asyncTimeoutMs;

    // Hoisted — immutable and shared across requests.
    private final ClientInformation clientInfo = new ClientInformation("http-rest", "1.0");

    public ProduceResource(RecordAppender appender,
                           AuthorizationHelper auth,
                           MetadataCache metadataCache,
                           Time time,
                           int requestTimeoutMs) {
        this.appender = appender;
        this.auth = auth;
        this.metadataCache = metadataCache;
        this.time = time;
        this.appendTimeoutMs = requestTimeoutMs;
        this.asyncTimeoutMs = requestTimeoutMs + ASYNC_TIMEOUT_SLACK_MS;
    }

    @POST
    @Path("/{name}")
    public void produce(@PathParam("name") String topic,
                        ProduceBody body,
                        @Context ContainerRequestContext httpCtx,
                        @Context HttpServletRequest servletRequest,
                        @Suspended AsyncResponse asyncResponse) {

        // Bound the suspension so a stuck append doesn't pin a connection forever.
        asyncResponse.setTimeout(asyncTimeoutMs, TimeUnit.MILLISECONDS);
        asyncResponse.setTimeoutHandler(ar -> ar.resume(error(504, "REQUEST_TIMED_OUT")));

        RequestContext reqCtx = buildRequestContext(httpCtx, servletRequest);

        if (!auth.authorize(reqCtx, AclOperation.WRITE, ResourceType.TOPIC, topic)) {
            asyncResponse.resume(error(Response.Status.FORBIDDEN, "TOPIC_AUTHORIZATION_FAILED"));
            return;
        }

        Uuid topicId = metadataCache.getTopicId(topic);
        if (topicId.equals(Uuid.ZERO_UUID)) {
            asyncResponse.resume(error(Response.Status.NOT_FOUND, "UNKNOWN_TOPIC"));
            return;
        }

        // One metadata lookup, reused — avoids the race where the topic is
        // deleted between an isEmpty() check and a follow-up get() that
        // would then throw NoSuchElementException and turn into a bare 500.
        Optional<Integer> numPartitionsOpt = metadataCache.numPartitions(topic);
        if (numPartitionsOpt.isEmpty()) {
            asyncResponse.resume(error(Response.Status.SERVICE_UNAVAILABLE, "TOPIC_METADATA_UNAVAILABLE"));
            return;
        }

        appendRecord(topic, topicId, numPartitionsOpt.get(), body, asyncResponse);
    }

    private RequestContext buildRequestContext(ContainerRequestContext httpCtx, HttpServletRequest servletRequest) {
        KafkaPrincipal principal = Optional.ofNullable(httpCtx.getProperty(BasicAuthFilter.PRINCIPAL_PROPERTY))
                .map(KafkaPrincipal.class::cast)
                .orElse(KafkaPrincipal.ANONYMOUS);

        boolean isSecure = servletRequest.isSecure();
        return new RequestContext(
                new RequestHeader(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion(), "http-rest", 0),
                "http-" + UUID.randomUUID(),
                resolveClientAddress(servletRequest),
                Optional.of(servletRequest.getRemotePort()),
                principal,
                isSecure ? HttpEndpoint.HTTPS : HttpEndpoint.HTTP,
                isSecure ? SecurityProtocol.SSL : SecurityProtocol.PLAINTEXT,
                clientInfo,
                false);
    }

    private static InetAddress resolveClientAddress(HttpServletRequest servletRequest) {
        try {
            return InetAddress.getByName(servletRequest.getRemoteAddr());
        } catch (Exception e) {
            return InetAddress.getLoopbackAddress();
        }
    }

    private void appendRecord(String topic, Uuid topicId, int numPartitions,
                              ProduceBody body, AsyncResponse asyncResponse) {
        byte[] keyBytes = body == null || body.key() == null ? null : body.key().getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = body == null || body.value() == null ? null : body.value().getBytes(StandardCharsets.UTF_8);

        int partition = keyBytes == null
                ? ThreadLocalRandom.current().nextInt(numPartitions)
                : BuiltInPartitioner.partitionForKey(keyBytes, numPartitions);

        TopicIdPartition tip = new TopicIdPartition(topicId, new TopicPartition(topic, partition));
        SimpleRecord record = new SimpleRecord(time.milliseconds(), keyBytes, valueBytes);
        MemoryRecords records = MemoryRecords.withRecords(Compression.NONE, record);

        try {
            appender.appendRecords(appendTimeoutMs, REQUIRED_ACKS_LEADER,
                    Map.of(tip, records),
                    result -> asyncResponse.resume(responseFor(partition, result.get(tip))));
        } catch (Exception e) {
            LOG.warn("Unexpected error during append for topic {}", topic, e);
            asyncResponse.resume(error(Response.Status.INTERNAL_SERVER_ERROR, "APPEND_FAILED"));
        }
    }

    private static Response responseFor(int partition, PartitionResponse pr) {
        if (pr == null) {
            return error(Response.Status.INTERNAL_SERVER_ERROR, "NO_RESPONSE");
        }
        if (pr.error != Errors.NONE) {
            return error(httpStatusFor(pr.error), pr.error.name());
        }
        return Response.ok(new ProduceResponseBody(partition, pr.baseOffset)).build();
    }

    private static Response error(Response.Status status, String code) {
        return Response.status(status).entity(Map.of("error", code)).build();
    }

    private static Response error(int status, String code) {
        return Response.status(status).entity(Map.of("error", code)).build();
    }

    /**
     * Map Kafka error codes to HTTP status so clients get something they can
     * act on. Default is 500 — we only deviate when the Kafka error clearly
     * corresponds to a different HTTP status.
     */
    private static int httpStatusFor(Errors e) {
        return switch (e) {
            case TOPIC_AUTHORIZATION_FAILED,
                 CLUSTER_AUTHORIZATION_FAILED,
                 DELEGATION_TOKEN_AUTHORIZATION_FAILED -> 403;
            case UNKNOWN_TOPIC_OR_PARTITION,
                 UNKNOWN_TOPIC_ID -> 404;
            case MESSAGE_TOO_LARGE -> 413;
            case INVALID_TOPIC_EXCEPTION,
                 INVALID_REQUIRED_ACKS,
                 CORRUPT_MESSAGE -> 400;
            case NOT_LEADER_OR_FOLLOWER,
                 NOT_ENOUGH_REPLICAS,
                 NOT_ENOUGH_REPLICAS_AFTER_APPEND,
                 KAFKA_STORAGE_ERROR -> 503;
            case REQUEST_TIMED_OUT -> 504;
            default -> 500;
        };
    }
}
