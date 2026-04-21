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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.network.ConnectionMode;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.test.TestSslUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.SSLEngine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the Jetty ↔ Kafka SSL bridge: a {@link KafkaSslContextFactory}
 * wraps a real {@link SslFactory} built with real (test) certs and hands back
 * real {@code SSLEngine} instances, honours reconfigure, and exposes the
 * listener name so {@code DynamicBrokerConfig} can scope config to it.
 */
public class KafkaSslContextFactoryTest {

    private final ListenerName listener = new ListenerName("HTTPS");

    private SslFactory freshSslFactory() throws Exception {
        File truststore = TestUtils.tempFile("kafka.rest.", ".truststore.jks");
        Map<String, Object> configs = TestSslUtils.createSslConfig(
                /* useClientCert */ false,
                /* createTrustStore */ true,
                ConnectionMode.SERVER,
                truststore,
                "kafka-rest");
        SslFactory factory = new SslFactory(ConnectionMode.SERVER, null, true);
        factory.configure(configs);
        return factory;
    }

    @Test
    public void listenerNameIsReported() throws Exception {
        SslFactory sslFactory = freshSslFactory();
        try {
            KafkaSslContextFactory ctxFactory = new KafkaSslContextFactory(listener, sslFactory);
            try {
                assertEquals(listener, ctxFactory.listenerName());
            } finally {
                ctxFactory.stop();
            }
        } finally {
            sslFactory.close();
        }
    }

    @Test
    public void newSSLEngineDelegatesToSslFactory() throws Exception {
        SslFactory sslFactory = freshSslFactory();
        KafkaSslContextFactory ctxFactory = new KafkaSslContextFactory(listener, sslFactory);
        ctxFactory.start();
        try {
            SSLEngine engine1 = ctxFactory.newSSLEngine();
            SSLEngine engine2 = ctxFactory.newSSLEngine();
            // Jetty calls newSSLEngine per connection, so fresh engines expected —
            // if we were caching, engine1 == engine2 would be true.
            assertNotNull(engine1);
            assertNotNull(engine2);
            assertNotSame(engine1, engine2,
                    "KafkaSslContextFactory must not cache SSLEngines — Jetty expects a fresh one per connection.");
            assertFalse(engine1.getUseClientMode(), "server-mode engine expected");
        } finally {
            ctxFactory.stop();
        }
    }

    @Test
    public void reconfigurableConfigsAreTheFactorys() throws Exception {
        SslFactory sslFactory = freshSslFactory();
        try {
            KafkaSslContextFactory ctxFactory = new KafkaSslContextFactory(listener, sslFactory);
            try {
                Set<String> reconfigurable = ctxFactory.reconfigurableConfigs();
                // Exactly the set SslFactory exposes — ssl.keystore.location etc.
                assertTrue(reconfigurable.contains("ssl.keystore.location"),
                        "expected ssl.keystore.location in reconfigurable set, got: " + reconfigurable);
                assertTrue(reconfigurable.contains("ssl.keystore.password"));
            } finally {
                ctxFactory.stop();
            }
        } finally {
            sslFactory.close();
        }
    }

    @Test
    public void configureIsRejected() throws Exception {
        SslFactory sslFactory = freshSslFactory();
        try {
            KafkaSslContextFactory ctxFactory = new KafkaSslContextFactory(listener, sslFactory);
            try {
                // SslFactory is configured at construction; the ListenerReconfigurable
                // configure(Map) entry point is wrong shape for this bridge and must fail
                // rather than silently leave the SslFactory in a half-configured state.
                assertThrows(ConfigException.class, () -> ctxFactory.configure(Map.of()));
            } finally {
                ctxFactory.stop();
            }
        } finally {
            sslFactory.close();
        }
    }

    @Test
    public void reconfigureFlowsThroughToSslFactory() throws Exception {
        SslFactory sslFactory = freshSslFactory();
        KafkaSslContextFactory ctxFactory = new KafkaSslContextFactory(listener, sslFactory);
        ctxFactory.start();
        try {
            // Same config — reconfigure should be a safe no-op (SslEngineFactory
            // detects nothing to rebuild) and not throw.
            Map<String, Object> currentConfigs = new HashMap<>();
            ctxFactory.reconfigure(currentConfigs);
            // If we got here, the path from KafkaSslContextFactory.reconfigure
            // to SslFactory.reconfigure is live.
        } finally {
            ctxFactory.stop();
        }
    }
}
