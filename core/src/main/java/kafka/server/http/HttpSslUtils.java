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
import java.util.regex.Pattern;

/**
 * Builds a Jetty {@link SslContextFactory.Server} from the broker's standard
 * {@code ssl.*} configs so HTTPS REST listeners reuse the same keystore/truststore
 * as the broker's Kafka SSL listeners. No new SSL config keys are introduced.
 *
 * <p>Adapted from {@code org.apache.kafka.connect.runtime.rest.util.SSLUtils}.
 */
public final class HttpSslUtils {

    private static final Pattern COMMA_WITH_WHITESPACE = Pattern.compile("\\s*,\\s*");

    private HttpSslUtils() {}

    public static SslContextFactory.Server createServerSideSslContextFactory(AbstractConfig config) {
        Map<String, Object> sslConfigValues = config.valuesWithPrefixAllOrNothing("");
        SslContextFactory.Server ssl = new SslContextFactory.Server();
        configureKeyStore(ssl, sslConfigValues);
        configureTrustStore(ssl, sslConfigValues);
        configureAlgorithms(ssl, sslConfigValues);
        configureAuthentication(ssl, sslConfigValues);
        return ssl;
    }

    private static void configureKeyStore(SslContextFactory ssl, Map<String, Object> cfg) {
        ssl.setKeyStoreType((String) getOrDefault(cfg, SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE));
        String keystoreLocation = (String) cfg.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        if (keystoreLocation != null) ssl.setKeyStorePath(keystoreLocation);
        Password keystorePassword = (Password) cfg.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        if (keystorePassword != null) ssl.setKeyStorePassword(keystorePassword.value());
        Password keyPassword = (Password) cfg.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
        if (keyPassword != null) ssl.setKeyManagerPassword(keyPassword.value());
    }

    private static void configureTrustStore(SslContextFactory ssl, Map<String, Object> cfg) {
        ssl.setTrustStoreType((String) getOrDefault(cfg, SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE));
        String truststoreLocation = (String) cfg.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        if (truststoreLocation != null) ssl.setTrustStorePath(truststoreLocation);
        Password truststorePassword = (Password) cfg.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        if (truststorePassword != null) ssl.setTrustStorePassword(truststorePassword.value());
    }

    @SuppressWarnings("unchecked")
    private static void configureAlgorithms(SslContextFactory ssl, Map<String, Object> cfg) {
        List<String> enabledProtocols = (List<String>) getOrDefault(cfg, SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG,
                List.of(COMMA_WITH_WHITESPACE.split(SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS)));
        if (enabledProtocols != null && !enabledProtocols.isEmpty()) {
            ssl.setIncludeProtocols(enabledProtocols.toArray(new String[0]));
        }

        String provider = (String) cfg.get(SslConfigs.SSL_PROVIDER_CONFIG);
        if (provider != null) ssl.setProvider(provider);

        ssl.setProtocol((String) getOrDefault(cfg, SslConfigs.SSL_PROTOCOL_CONFIG, SslConfigs.DEFAULT_SSL_PROTOCOL));

        List<String> cipherSuites = (List<String>) cfg.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG);
        if (cipherSuites != null && !cipherSuites.isEmpty()) {
            ssl.setIncludeCipherSuites(cipherSuites.toArray(new String[0]));
        }

        ssl.setKeyManagerFactoryAlgorithm((String) getOrDefault(cfg, SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG,
                SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM));

        String secureRandomImpl = (String) cfg.get(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG);
        if (secureRandomImpl != null) ssl.setSecureRandomAlgorithm(secureRandomImpl);

        ssl.setTrustManagerFactoryAlgorithm((String) getOrDefault(cfg, SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
                SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM));
    }

    private static void configureAuthentication(SslContextFactory.Server ssl, Map<String, Object> cfg) {
        String clientAuth = (String) getOrDefault(cfg, BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "none");
        switch (clientAuth) {
            case "requested" -> ssl.setWantClientAuth(true);
            case "required" -> ssl.setNeedClientAuth(true);
            default -> {
                ssl.setNeedClientAuth(false);
                ssl.setWantClientAuth(false);
            }
        }
    }

    private static Object getOrDefault(Map<String, Object> cfg, String key, Object defaultValue) {
        return cfg.containsKey(key) ? cfg.get(key) : defaultValue;
    }
}
