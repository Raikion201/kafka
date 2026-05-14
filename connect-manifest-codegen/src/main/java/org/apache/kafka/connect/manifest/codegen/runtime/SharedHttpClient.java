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
package org.apache.kafka.connect.manifest.codegen.runtime;

import java.net.http.HttpClient;

/**
 * JVM-wide shared {@link HttpClient} for all generated connector tasks.
 *
 * <p>Running 500+ connectors each with their own {@code HttpClient} creates one
 * thread-pool per client — easily 5000+ threads. Sharing one client reduces this
 * to a single instance while still allowing concurrent requests (HttpClient is
 * fully thread-safe and multiplexes over HTTP/1.1 connections internally).
 *
 * <p>No custom executor is set; the HTTP client uses its own internal selector thread
 * and cached-thread-pool to avoid deadlocks with JDK 26's async DNS resolver under
 * a fixed-size executor.
 */
public final class SharedHttpClient {

    public static final HttpClient INSTANCE = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .followRedirects(HttpClient.Redirect.NORMAL)
        .build();

    private SharedHttpClient() {
    }
}
