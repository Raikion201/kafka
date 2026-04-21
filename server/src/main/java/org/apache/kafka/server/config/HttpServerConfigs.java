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
package org.apache.kafka.server.config;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.PASSWORD;

public class HttpServerConfigs {

    public static final String HTTP_REST_EXECUTOR_THREADS_CONFIG = "http.rest.executor.threads";
    public static final int HTTP_REST_EXECUTOR_THREADS_DEFAULT = 8;
    public static final String HTTP_REST_EXECUTOR_THREADS_DOC =
            "The number of worker threads used by the embedded HTTP REST server to handle HTTP requests.";

    public static final String HTTP_REST_BASIC_CREDENTIALS_CONFIG = "http.rest.basic.credentials";
    public static final String HTTP_REST_BASIC_CREDENTIALS_DEFAULT = "";
    public static final String HTTP_REST_BASIC_CREDENTIALS_DOC =
            "Comma-separated list of <code>user:password</code> pairs used for HTTP Basic Auth on the embedded REST server. " +
            "When empty (the default), the REST server accepts requests without authentication. " +
            "Declared as PASSWORD so the value is redacted in broker logs and in <code>kafka-configs.sh --describe</code>.";

    public static final String HTTP_REST_SWAGGER_UI_ENABLED_CONFIG = "http.rest.swagger-ui.enabled";
    public static final boolean HTTP_REST_SWAGGER_UI_ENABLED_DEFAULT = false;
    public static final String HTTP_REST_SWAGGER_UI_ENABLED_DOC =
            "When <code>true</code>, the embedded REST server exposes <code>/openapi.yaml</code> and " +
            "<code>/swagger</code> (the Swagger UI) without authentication so the API can be explored " +
            "in a browser. Off by default because any client that can reach the REST port would otherwise " +
            "learn the endpoint surface without credentials. Turn on in development or when the REST " +
            "listener is behind a gateway/ACL that already restricts who can reach it.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(HTTP_REST_EXECUTOR_THREADS_CONFIG, INT, HTTP_REST_EXECUTOR_THREADS_DEFAULT,
                    // Jetty itself reports a clear error at startup if the pool is too small
                    // for acceptors + selectors + handlers, so we keep the config floor at 1
                    // and let Jetty be the source of truth for its own minimum.
                    atLeast(1), MEDIUM, HTTP_REST_EXECUTOR_THREADS_DOC)
            .define(HTTP_REST_BASIC_CREDENTIALS_CONFIG, PASSWORD, HTTP_REST_BASIC_CREDENTIALS_DEFAULT,
                    BasicCredentialsValidator.INSTANCE, MEDIUM, HTTP_REST_BASIC_CREDENTIALS_DOC)
            .define(HTTP_REST_SWAGGER_UI_ENABLED_CONFIG, BOOLEAN, HTTP_REST_SWAGGER_UI_ENABLED_DEFAULT,
                    MEDIUM, HTTP_REST_SWAGGER_UI_ENABLED_DOC);

    /**
     * Eagerly validates the {@code user:pass,user:pass,...} shape at broker
     * startup. Without this, a malformed entry would only surface the first
     * time {@code httpBasicCredentials} was read (i.e. when the REST server
     * was about to start) — late-failure config errors are unfriendly.
     */
    private static final class BasicCredentialsValidator implements ConfigDef.Validator {
        static final BasicCredentialsValidator INSTANCE = new BasicCredentialsValidator();

        @Override
        public void ensureValid(String name, Object value) {
            String raw = extractRaw(value);
            if (raw == null || raw.isEmpty()) {
                return; // Empty is valid — Basic Auth simply disabled.
            }
            for (String entry : raw.split(",")) {
                int colon = entry.indexOf(':');
                if (colon <= 0 || colon == entry.length() - 1) {
                    throw new ConfigException(name, "<redacted>",
                            "each entry must be 'user:pass' (non-empty user, non-empty pass)");
                }
            }
        }

        private static String extractRaw(Object value) {
            if (value == null) return null;
            if (value instanceof Password) return ((Password) value).value();
            return value.toString();
        }

        @Override
        public String toString() {
            return "comma-separated user:pass entries";
        }
    }
}
