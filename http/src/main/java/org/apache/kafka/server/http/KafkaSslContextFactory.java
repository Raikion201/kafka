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
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.ListenerReconfigurable;
import org.apache.kafka.common.security.ssl.SslFactory;

import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

/**
 * Bridges Jetty's {@link SslContextFactory.Server} to Kafka's {@link SslFactory}.
 *
 * <p>Every {@code SSLEngine} Jetty needs for an incoming TLS connection is
 * produced by {@code SslFactory} — so the HTTPS REST listener goes through
 * exactly the same pipeline as a Kafka {@code SSL://} listener:</p>
 *
 * <ul>
 *   <li>the {@code ssl.engine.factory.class} plugin (default
 *       {@code DefaultSslEngineFactory}) is honoured, so HSM-backed keys or
 *       audit-wrapped engines apply to HTTPS just like they do to SSL://</li>
 *   <li>certificate rotation via
 *       {@code kafka-configs.sh --alter ssl.keystore.location} flows through
 *       {@code SslFactory.reconfigure}, which runs {@code CertificateEntries.ensureCompatible}
 *       and {@code SslEngineValidator.validate} before swapping the engine factory</li>
 *   <li>per-listener overrides keyed on {@code listener.name.<name>.ssl.*} take
 *       effect because we feed {@code SslFactory} configs pulled with
 *       {@code valuesWithPrefixOverride(listenerName.configPrefix())}</li>
 * </ul>
 *
 * <p>Implements {@link ListenerReconfigurable} so {@code DynamicBrokerConfig}
 * invokes {@code validateReconfiguration}/{@code reconfigure} with the correct
 * listener-scoped config snapshot.</p>
 */
public class KafkaSslContextFactory extends SslContextFactory.Server implements ListenerReconfigurable {

    private final ListenerName listener;
    private final SslFactory sslFactory;

    /**
     * @param listener   listener name for this HTTPS endpoint (e.g. {@code HTTPS});
     *                   also the key used to look up per-listener SSL overrides.
     * @param sslFactory an already-configured {@link SslFactory} in
     *                   {@code ConnectionMode.SERVER}. This class takes ownership of
     *                   its lifecycle — callers must not close it themselves.
     */
    // Jetty's SslContextFactory lifecycle validates that an SSLContext is
    // available before the server starts. We never delegate engine creation
    // to it — newSSLEngine below is overridden — but we still need doStart()
    // to succeed, so hand Jetty the JVM-wide default context purely as a
    // precondition satisfier. Calling setSslContext from the constructor is
    // why we suppress the this-escape warning — the call doesn't observe any
    // subclass state, it just stores the context on the superclass.
    @SuppressWarnings("this-escape")
    public KafkaSslContextFactory(ListenerName listener, SslFactory sslFactory) {
        this.listener = listener;
        this.sslFactory = sslFactory;
        try {
            setSslContext(SSLContext.getDefault());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JVM default SSLContext unavailable", e);
        }
    }

    @Override
    public ListenerName listenerName() {
        return listener;
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return sslFactory.reconfigurableConfigs();
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) {
        sslFactory.validateReconfiguration(configs);
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        sslFactory.reconfigure(configs);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        throw new ConfigException(
                "KafkaSslContextFactory is configured indirectly via SslFactory at construction time");
    }

    @Override
    public SSLEngine newSSLEngine() {
        return sslFactory.createSslEngine(null, 0);
    }

    @Override
    public SSLEngine newSSLEngine(InetSocketAddress address) {
        // Jetty occasionally passes null here (request has no remote address).
        String host = address == null ? null : address.getHostString();
        int port = address == null ? 0 : address.getPort();
        return sslFactory.createSslEngine(host, port);
    }

    @Override
    protected void doStop() throws Exception {
        try {
            sslFactory.close();
        } finally {
            super.doStop();
        }
    }
}
