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

/**
 * SPI implemented by the {@code :http} module and discovered by the broker via
 * {@link java.util.ServiceLoader}. The broker only depends on this interface
 * plus the rest of {@code :http-api}; if the {@code :http} jar is not on the
 * runtime classpath the broker simply skips starting the REST proxy.
 */
public interface BrokerHttpServerFactory {

    /** Build a {@link BrokerHttpServer} wired to the given broker-supplied context. */
    BrokerHttpServer create(BrokerHttpServerContext context);
}
