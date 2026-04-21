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

import org.apache.kafka.common.network.ConnectionMode;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.server.http.api.BrokerHttpServer;
import org.apache.kafka.server.http.api.BrokerHttpServerContext;
import org.apache.kafka.server.http.api.HttpEndpoint;

import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import org.glassfish.jersey.servlet.ServletContainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Embedded HTTP REST server for the Kafka broker. Activated when {@code listeners=}
 * contains one or more {@code HTTP://} or {@code HTTPS://} entries. Owns the Jetty
 * lifecycle; the routes and filters live in {@link HttpRouter}.
 *
 * <p>TLS goes through Kafka's {@link SslFactory} pipeline (see
 * {@link KafkaSslContextFactory}): the HTTPS listener honours
 * {@code ssl.engine.factory.class}, supports hot keystore rotation via
 * {@code DynamicBrokerConfig}, and picks up {@code listener.name.<name>.ssl.*}
 * per-listener overrides — the same behaviours Kafka's {@code SSL://}
 * listeners have.</p>
 */
public class HttpRestServer implements BrokerHttpServer {

    private static final Logger LOG = LoggerFactory.getLogger(HttpRestServer.class);

    private final BrokerHttpServerContext ctx;

    // Jetty needs a distinct SslContextFactory per HTTPS listener name. All
    // HTTPS endpoints resolve to one listener name ("HTTPS") by design, so
    // they share one KafkaSslContextFactory / SslFactory — cert rotation
    // fires once per listener, not once per bind.
    private final Map<ListenerName, KafkaSslContextFactory> sslFactoriesByListener = new LinkedHashMap<>();

    private volatile Server jetty;

    public HttpRestServer(BrokerHttpServerContext ctx) {
        if (ctx.endpoints().isEmpty()) {
            throw new IllegalArgumentException("HttpRestServer requires at least one endpoint");
        }
        this.ctx = ctx;
    }

    @Override
    public void startup() {
        QueuedThreadPool pool = new QueuedThreadPool(ctx.executorThreads());
        pool.setName("http-rest");
        jetty = new Server(pool);

        for (HttpEndpoint ep : ctx.endpoints()) {
            ServerConnector connector = ep.isTls()
                    ? new ServerConnector(jetty, sslContextFactoryFor(ep))
                    : new ServerConnector(jetty);
            connector.setHost(ep.host());
            connector.setPort(ep.port());
            jetty.addConnector(connector);
        }

        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        context.addServlet(
                new ServletHolder(new ServletContainer(HttpRouter.build(
                        ctx.basicCredentials(),
                        ctx.swaggerUiEnabled(),
                        ctx.requestTimeoutMs(),
                        ctx.appender(),
                        ctx.auth(),
                        ctx.metadataCache(),
                        ctx.time()))),
                "/*");
        jetty.setHandler(context);

        try {
            jetty.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start embedded HTTP server", e);
        }
        LOG.info("HTTP REST server listening on {}", ctx.endpoints());
    }

    /**
     * Build (or reuse) the {@link KafkaSslContextFactory} for the listener name
     * this endpoint resolves to. The broker has already resolved
     * {@code listener.name.<name>.ssl.*} overrides and passed them in via
     * {@link BrokerHttpServerContext#sslConfigsByListener()}, so we just read
     * out the pre-extracted map rather than reaching back into the broker config.
     */
    private SslContextFactory.Server sslContextFactoryFor(HttpEndpoint ep) {
        ListenerName listener = ep.listenerName();
        return sslFactoriesByListener.computeIfAbsent(listener, name -> {
            Map<String, Object> configs = ctx.sslConfigsByListener().get(name);
            if (configs == null) {
                throw new IllegalStateException(
                        "No SSL configs supplied for HTTPS listener " + name +
                        ". BrokerHttpServerContext.sslConfigsByListener() must contain an entry " +
                        "for every HTTPS endpoint's listenerName().");
            }
            SslFactory sslFactory = new SslFactory(
                    ConnectionMode.SERVER,
                    /* clientAuthConfigOverride */ null,
                    // REST clients are external — their certs aren't in the broker's
                    // truststore, so we can't do a mock handshake against it at startup.
                    // SslFactory still validates on every reconfigure via CertificateEntries
                    // + SslEngineValidator; that's the rotation-time safety net.
                    /* keystoreVerifiableUsingTruststore */ false);
            sslFactory.configure(configs);
            KafkaSslContextFactory jettyFactory = new KafkaSslContextFactory(name, sslFactory);
            ctx.reconfigurableRegistry().addReconfigurable(jettyFactory);
            return jettyFactory;
        });
    }

    @Override
    public void shutdown() {
        Server server = jetty;
        for (KafkaSslContextFactory factory : sslFactoriesByListener.values()) {
            try {
                ctx.reconfigurableRegistry().removeReconfigurable(factory);
            } catch (Exception e) {
                LOG.warn("Error removing reconfigurable for {}", factory.listenerName(), e);
            }
        }
        sslFactoriesByListener.clear();

        if (server == null) return;
        try {
            server.stop();    // triggers doStop() on each SslContextFactory, which closes the SslFactory
            server.join();
        } catch (Exception e) {
            LOG.warn("Error stopping embedded HTTP server", e);
        } finally {
            server.destroy();
            jetty = null;
        }
    }
}
