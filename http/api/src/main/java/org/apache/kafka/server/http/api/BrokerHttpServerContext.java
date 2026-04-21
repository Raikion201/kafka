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
package org.apache.kafka.server.http.api;

import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.MetadataCache;

import java.util.List;
import java.util.Map;

/**
 * Immutable bag passed from the broker to the {@code :http} implementation.
 * Deliberately narrow — everything {@code :http} needs arrives pre-extracted
 * so the implementation cannot reach back into the broker config and read
 * unrelated keys. If a future feature needs something new, add a typed
 * field here rather than handing the impl a generic config object.
 *
 * @param endpoints              parsed HTTP/HTTPS listeners from {@code listeners=}
 * @param executorThreads        Jetty {@code QueuedThreadPool} size
 * @param basicCredentials       {@code user -> password}; empty disables Basic Auth
 * @param sslConfigsByListener   per-listener SSL configs keyed by {@link HttpEndpoint#listenerName()},
 *                               already resolved through {@code valuesWithPrefixOverride(listener.name.<n>.)}
 *                               by the broker so {@code :http} doesn't have to know about config prefixes
 * @param requestTimeoutMs       broker's {@code request.timeout.ms} — used as the append deadline and
 *                               as the basis for the async-response suspension ceiling
 * @param swaggerUiEnabled       when {@code false}, {@code /openapi.yaml} and {@code /swagger} are not served
 * @param reconfigurableRegistry hook for registering the HTTPS {@code SslContextFactory} with
 *                               {@code DynamicBrokerConfig} so cert rotation works
 * @param appender               narrow adapter over {@code ReplicaManager.appendRecords}
 * @param auth                   narrow adapter over {@code AuthHelper.authorize}
 * @param metadataCache          broker metadata cache (topic id + partition count lookups)
 * @param time                   broker clock
 */
public record BrokerHttpServerContext(
        List<HttpEndpoint> endpoints,
        int executorThreads,
        Map<String, String> basicCredentials,
        Map<ListenerName, Map<String, Object>> sslConfigsByListener,
        int requestTimeoutMs,
        boolean swaggerUiEnabled,
        ReconfigurableRegistry reconfigurableRegistry,
        RecordAppender appender,
        AuthorizationHelper auth,
        MetadataCache metadataCache,
        Time time) { }
