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

import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.PASSWORD;

public class HttpServerConfigs {

    public static final String HTTP_REST_EXECUTOR_THREADS_CONFIG = "http.rest.executor.threads";
    public static final int HTTP_REST_EXECUTOR_THREADS_DEFAULT = 8;
    public static final String HTTP_REST_EXECUTOR_THREADS_DOC =
            "The number of worker threads used by the embedded HTTP REST server to handle HTTP requests. " +
            "Must be at least 4 — Jetty needs threads for acceptors, selectors, and request handling before it will start.";

    public static final String HTTP_REST_BASIC_CREDENTIALS_CONFIG = "http.rest.basic.credentials";
    public static final String HTTP_REST_BASIC_CREDENTIALS_DEFAULT = "";
    public static final String HTTP_REST_BASIC_CREDENTIALS_DOC =
            "Comma-separated list of <code>user:password</code> pairs used for HTTP Basic Auth on the embedded REST server. " +
            "When empty (the default), the REST server accepts requests without authentication. " +
            "Declared as PASSWORD so the value is redacted in broker logs and in <code>kafka-configs.sh --describe</code>.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(HTTP_REST_EXECUTOR_THREADS_CONFIG, INT, HTTP_REST_EXECUTOR_THREADS_DEFAULT,
                    atLeast(4), MEDIUM, HTTP_REST_EXECUTOR_THREADS_DOC)
            .define(HTTP_REST_BASIC_CREDENTIALS_CONFIG, PASSWORD, HTTP_REST_BASIC_CREDENTIALS_DEFAULT,
                    MEDIUM, HTTP_REST_BASIC_CREDENTIALS_DOC);
}
