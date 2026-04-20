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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.config.types.Password;

import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.util.List;
import java.util.Map;

/**
 * Builds a Jetty {@link SslContextFactory.Server} from the broker's standard
 * {@code ssl.*} configs so HTTPS REST listeners reuse the same keystore and
 * truststore as the broker's SSL:// Kafka listeners.
 */
public final class HttpSslUtils {

    private HttpSslUtils() {}

    public static SslContextFactory.Server createServerSideSslContextFactory(AbstractConfig config) {
        Map<String, Object> cfg = config.valuesWithPrefixAllOrNothing("");
        SslContextFactory.Server ssl = new SslContextFactory.Server();
        configureKeyStore(ssl, cfg);
        configureTrustStore(ssl, cfg);
        configureAlgorithms(ssl, cfg);
        configureClientAuth(ssl, cfg);
        return ssl;
    }

    private static void configureKeyStore(SslContextFactory ssl, Map<String, Object> cfg) {
        ssl.setKeyStoreType((String) cfg.getOrDefault(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE));
        setIfPresent(cfg, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, v -> ssl.setKeyStorePath((String) v));
        setIfPresent(cfg, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, v -> ssl.setKeyStorePassword(((Password) v).value()));
        setIfPresent(cfg, SslConfigs.SSL_KEY_PASSWORD_CONFIG, v -> ssl.setKeyManagerPassword(((Password) v).value()));
    }

    private static void configureTrustStore(SslContextFactory ssl, Map<String, Object> cfg) {
        ssl.setTrustStoreType((String) cfg.getOrDefault(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE));
        setIfPresent(cfg, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, v -> ssl.setTrustStorePath((String) v));
        setIfPresent(cfg, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, v -> ssl.setTrustStorePassword(((Password) v).value()));
    }

    @SuppressWarnings("unchecked")
    private static void configureAlgorithms(SslContextFactory ssl, Map<String, Object> cfg) {
        List<String> enabledProtocols = (List<String>) cfg.get(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG);
        if (enabledProtocols != null && !enabledProtocols.isEmpty()) {
            ssl.setIncludeProtocols(enabledProtocols.toArray(new String[0]));
        }

        ssl.setProtocol((String) cfg.getOrDefault(SslConfigs.SSL_PROTOCOL_CONFIG, SslConfigs.DEFAULT_SSL_PROTOCOL));
        setIfPresent(cfg, SslConfigs.SSL_PROVIDER_CONFIG, v -> ssl.setProvider((String) v));

        List<String> cipherSuites = (List<String>) cfg.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG);
        if (cipherSuites != null && !cipherSuites.isEmpty()) {
            ssl.setIncludeCipherSuites(cipherSuites.toArray(new String[0]));
        }

        ssl.setKeyManagerFactoryAlgorithm((String) cfg.getOrDefault(
                SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM));
        ssl.setTrustManagerFactoryAlgorithm((String) cfg.getOrDefault(
                SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM));
        setIfPresent(cfg, SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, v -> ssl.setSecureRandomAlgorithm((String) v));
    }

    private static void configureClientAuth(SslContextFactory.Server ssl, Map<String, Object> cfg) {
        String clientAuth = (String) cfg.getOrDefault(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "none");
        switch (clientAuth) {
            case "requested" -> ssl.setWantClientAuth(true);
            case "required" -> ssl.setNeedClientAuth(true);
            default -> { }
        }
    }

    private static void setIfPresent(Map<String, Object> cfg, String key, java.util.function.Consumer<Object> setter) {
        Object value = cfg.get(key);
        if (value != null) setter.accept(value);
    }
}
