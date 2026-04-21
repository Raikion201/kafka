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
import org.mockito.Mockito;

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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Verifies the Jetty ↔ Kafka SSL bridge: a {@link KafkaSslContextFactory}
 * wraps a real {@link SslFactory} built with real (test) certs and hands back
 * real {@code SSLEngine} instances, honours reconfigure, and exposes the
 * listener name so {@code DynamicBrokerConfig} can scope config to it.
 *
 * <p>The engine and reconfigure paths use a Mockito {@code spy} on the real
 * {@link SslFactory} so the tests can {@code verify} that the bridge delegates
 * rather than caching or silently dropping calls. A plain integration test
 * with assertions on the produced engine would pass even if
 * {@code KafkaSslContextFactory.reconfigure} were a no-op, because the same
 * config always yields an equivalent engine.</p>
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

    /**
     * Delegation test — verifies via spy that every {@code newSSLEngine} call
     * into the Jetty-facing surface reaches {@link SslFactory#createSslEngine}.
     * The previous "assertNotSame(engine1, engine2)" version would pass even
     * if the bridge stopped delegating and always returned a pre-built engine,
     * as long as it returned two different instances. This one fails.
     */
    @Test
    public void newSSLEngineDelegatesToSslFactory() throws Exception {
        SslFactory real = freshSslFactory();
        SslFactory sslFactory = spy(real);
        KafkaSslContextFactory ctxFactory = new KafkaSslContextFactory(listener, sslFactory);
        ctxFactory.start();
        try {
            SSLEngine engine1 = ctxFactory.newSSLEngine();
            SSLEngine engine2 = ctxFactory.newSSLEngine();
            assertNotNull(engine1);
            assertNotNull(engine2);
            // Jetty needs a fresh engine per connection — proven by spy verify,
            // not by the weaker assertNotSame on what could be any two objects.
            verify(sslFactory, times(2)).createSslEngine(eq(null), eq(0));
            assertNotSame(engine1, engine2,
                    "KafkaSslContextFactory must not cache SSLEngines — Jetty expects a fresh one per connection.");
            assertFalse(engine1.getUseClientMode(), "server-mode engine expected");
        } finally {
            ctxFactory.stop();
            real.close();
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

    /**
     * Delegation test — verifies via spy that the reconfigure entry point on
     * the Jetty-facing surface actually reaches {@link SslFactory#reconfigure}
     * and {@link SslFactory#validateReconfiguration} with the caller's map.
     * The previous version passed an empty map and asserted no-throw, which
     * would pass even if {@code KafkaSslContextFactory.reconfigure} had a
     * void body — the very bug this wrapper exists to avoid.
     */
    @Test
    public void reconfigureFlowsThroughToSslFactory() throws Exception {
        SslFactory real = freshSslFactory();
        SslFactory sslFactory = spy(real);
        KafkaSslContextFactory ctxFactory = new KafkaSslContextFactory(listener, sslFactory);
        ctxFactory.start();
        try {
            Map<String, Object> newConfigs = new HashMap<>();
            newConfigs.put("marker", "v1");

            ctxFactory.validateReconfiguration(newConfigs);
            verify(sslFactory).validateReconfiguration(eq(newConfigs));
            Mockito.reset(sslFactory);

            ctxFactory.reconfigure(newConfigs);
            verify(sslFactory).reconfigure(eq(newConfigs));
        } finally {
            ctxFactory.stop();
            real.close();
        }
    }

    /**
     * When Jetty offers a remote address, the bridge must thread it through
     * to {@link SslFactory#createSslEngine(String, int)} so downstream engine
     * factories (SPIFFE, HSM audit wrappers, etc.) can see the peer. Plain
     * delegation-by-existence tests miss this — we {@code verify} the exact
     * host/port tuple was forwarded.
     */
    @Test
    public void newSSLEngineWithAddressForwardsHostAndPort() throws Exception {
        SslFactory real = freshSslFactory();
        SslFactory sslFactory = spy(real);
        KafkaSslContextFactory ctxFactory = new KafkaSslContextFactory(listener, sslFactory);
        ctxFactory.start();
        try {
            java.net.InetSocketAddress addr = new java.net.InetSocketAddress("192.0.2.1", 4242);
            SSLEngine engine = ctxFactory.newSSLEngine(addr);
            assertNotNull(engine);
            verify(sslFactory).createSslEngine(eq("192.0.2.1"), eq(4242));

            // Jetty occasionally passes null; bridge must not NPE.
            Mockito.reset(sslFactory);
            SSLEngine engineNull = ctxFactory.newSSLEngine((java.net.InetSocketAddress) null);
            assertNotNull(engineNull);
            verify(sslFactory).createSslEngine(eq(null), eq(0));
        } finally {
            ctxFactory.stop();
            real.close();
        }
    }

    /**
     * {@link KafkaSslContextFactory#doStop} must close the wrapped
     * {@link SslFactory} — otherwise key material sits in memory after
     * broker shutdown. Pre-stop state cannot catch this; we check it via
     * a spy rather than by prodding private fields.
     */
    @Test
    public void stopClosesTheWrappedSslFactory() throws Exception {
        SslFactory real = freshSslFactory();
        SslFactory sslFactory = spy(real);
        KafkaSslContextFactory ctxFactory = new KafkaSslContextFactory(listener, sslFactory);
        ctxFactory.start();
        ctxFactory.stop();
        verify(sslFactory).close();
    }
}
