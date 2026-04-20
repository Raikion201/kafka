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

import org.apache.kafka.common.utils.Time;

import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Embedded HTTP REST server for the Kafka broker. Activated when {@code listeners=}
 * contains one or more {@code HTTP://} or {@code HTTPS://} entries.
 *
 * <p>This class owns only the Jetty lifecycle — binding ports, wiring the SSL
 * context when HTTPS listeners are present, and registering the servlet context.
 * The handler chain (Jersey router, Basic Auth filter, produce resource) is
 * configured in {@link HttpRouter}.
 */
public class HttpRestServer {

    private static final Logger log = LoggerFactory.getLogger(HttpRestServer.class);

    private final List<KafkaConfig.HttpEndpoint> endpoints;
    private final int executorThreads;
    private final Map<String, String> basicCredentials;
    private final KafkaConfig brokerConfig;
    private final Time time;

    private Server jetty;

    public HttpRestServer(
            List<KafkaConfig.HttpEndpoint> endpoints,
            int executorThreads,
            Map<String, String> basicCredentials,
            KafkaConfig brokerConfig,
            Time time) {
        this.endpoints = endpoints;
        this.executorThreads = executorThreads;
        this.basicCredentials = basicCredentials;
        this.brokerConfig = brokerConfig;
        this.time = time;
    }

    public void startup() throws Exception {
        if (endpoints.isEmpty()) {
            log.debug("No HTTP/HTTPS listeners configured; REST server not started");
            return;
        }

        QueuedThreadPool pool = new QueuedThreadPool(executorThreads);
        pool.setName("http-rest");
        jetty = new Server(pool);

        SslContextFactory.Server ssl = null;
        boolean needsTls = endpoints.stream().anyMatch(KafkaConfig.HttpEndpoint::isTls);
        if (needsTls) {
            ssl = HttpSslUtils.createServerSideSslContextFactory(brokerConfig);
            jetty.addBean(ssl);
        }

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
        jetty.setHandler(context);

        // Handler chain (router / auth filter / resources) is installed in a later
        // phase. The context is attached now so Jetty can bind ports cleanly.

        jetty.start();
        log.info("HTTP REST server started on {} endpoint(s): {}", endpoints.size(), endpoints);
    }

    public void shutdown() {
        if (jetty == null) return;
        try {
            jetty.stop();
            jetty.join();
        } catch (Exception e) {
            log.warn("Error shutting down HTTP REST server", e);
        } finally {
            jetty.destroy();
            jetty = null;
        }
    }

    // Accessors used by the router in a later phase so it can reach broker deps
    // without HttpRestServer holding every broker field.

    public Map<String, String> basicCredentials() {
        return basicCredentials;
    }

    public Time time() {
        return time;
    }
}
