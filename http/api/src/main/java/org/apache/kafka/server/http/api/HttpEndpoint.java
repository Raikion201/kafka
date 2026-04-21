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
package org.apache.kafka.server.http.api;

import org.apache.kafka.common.network.ListenerName;

/**
 * An HTTP or HTTPS bind target parsed out of the broker's {@code listeners=} config.
 * Produced by {@code KafkaConfig.httpListeners}; consumed by the {@code :http}
 * implementation to build a Jetty connector per endpoint.
 */
public record HttpEndpoint(String host, int port, boolean isTls) {

    /**
     * Canonical listener name for this endpoint — always {@code "HTTP"} or
     * {@code "HTTPS"}. Multiple endpoints with the same TLS disposition share
     * one listener name (and therefore one set of
     * {@code listener.name.<name>.ssl.*} overrides) by design: the REST proxy
     * treats HTTP and HTTPS as two named listeners rather than N independent
     * ones. If you need per-endpoint SSL configs, bind on one HTTPS port.
     */
    public ListenerName listenerName() {
        return isTls ? HTTPS : HTTP;
    }

    public static final ListenerName HTTP = new ListenerName("HTTP");
    public static final ListenerName HTTPS = new ListenerName("HTTPS");
}
