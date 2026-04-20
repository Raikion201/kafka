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
package kafka.server.http;

import kafka.server.KafkaConfig;
import kafka.server.ReplicaManager;

import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.http.SslContextFactories;

import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import scala.Option;

/**
 * Embedded HTTP REST server for the Kafka broker. Activated when {@code listeners=}
 * contains one or more {@code HTTP://} or {@code HTTPS://} entries.
 */
public class HttpRestServer {

    private static final Logger log = LoggerFactory.getLogger(HttpRestServer.class);

    private final List<KafkaConfig.HttpEndpoint> endpoints;
    private final int executorThreads;
    private final Map<String, String> basicCredentials;
    private final KafkaConfig brokerConfig;
    private final ReplicaManager replicaManager;
    private final Option<Plugin<Authorizer>> authorizerPlugin;
    private final MetadataCache metadataCache;
    private final Time time;

    private Server jetty;

    public HttpRestServer(
            List<KafkaConfig.HttpEndpoint> endpoints,
            int executorThreads,
            Map<String, String> basicCredentials,
            KafkaConfig brokerConfig,
            ReplicaManager replicaManager,
            Option<Plugin<Authorizer>> authorizerPlugin,
            MetadataCache metadataCache,
            Time time) {
        if (endpoints.isEmpty()) {
            throw new IllegalArgumentException("HttpRestServer requires at least one endpoint");
        }
        this.endpoints = endpoints;
        this.executorThreads = executorThreads;
        this.basicCredentials = basicCredentials;
        this.brokerConfig = brokerConfig;
        this.replicaManager = replicaManager;
        this.authorizerPlugin = authorizerPlugin;
        this.metadataCache = metadataCache;
        this.time = time;
    }

    public void startup() throws Exception {
        QueuedThreadPool pool = new QueuedThreadPool(executorThreads);
        pool.setName("http-rest");
        jetty = new Server(pool);

        SslContextFactory.Server ssl = endpoints.stream().anyMatch(KafkaConfig.HttpEndpoint::isTls)
                ? SslContextFactories.createServerSideSslContextFactory(brokerConfig)
                : null;

        for (KafkaConfig.HttpEndpoint ep : endpoints) {
            ServerConnector connector = ep.isTls()
                    ? new ServerConnector(jetty, ssl)
                    : new ServerConnector(jetty);
            connector.setHost(ep.host());
            connector.setPort(ep.port());
            jetty.addConnector(connector);
        }

        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        context.addServlet(new ServletHolder(new ServletContainer(
                HttpRouter.build(basicCredentials, replicaManager, authorizerPlugin, metadataCache, time))), "/*");
        jetty.setHandler(context);

        jetty.start();
        log.info("HTTP REST server listening on {}", endpoints);
    }

    public void shutdown() throws Exception {
        if (jetty == null) return;
        try {
            jetty.stop();
            jetty.join();
        } finally {
            jetty.destroy();
            jetty = null;
        }
    }
}
