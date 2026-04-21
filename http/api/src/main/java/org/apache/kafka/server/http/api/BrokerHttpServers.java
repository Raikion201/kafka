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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * Entry point the broker uses to bring up the embedded HTTP REST proxy
 * without depending on any concrete implementation. Looks up a
 * {@link BrokerHttpServerFactory} via {@link ServiceLoader} and hands it the
 * broker-supplied {@link BrokerHttpServerContext}. If no factory is on the
 * runtime classpath we log a warning and return {@code null}, so a broker
 * distribution that omits the {@code :http} jar boots without the REST
 * proxy instead of failing.
 */
public final class BrokerHttpServers {

    private static final Logger LOG = LoggerFactory.getLogger(BrokerHttpServers.class);

    private BrokerHttpServers() { }

    /**
     * @param context context the caller assembled (adapters, endpoints, credentials, …)
     * @return the constructed {@link BrokerHttpServer} ready for {@code startup()},
     *         or {@code null} if no factory implementation was found on the classpath.
     */
    public static BrokerHttpServer load(BrokerHttpServerContext context) {
        Iterator<BrokerHttpServerFactory> factories = ServiceLoader.load(BrokerHttpServerFactory.class).iterator();
        if (!factories.hasNext()) {
            LOG.warn("listeners= contains HTTP/HTTPS entries but no BrokerHttpServerFactory implementation " +
                    "was found on the classpath. Is the :http module on the runtime classpath? Skipping REST proxy.");
            return null;
        }
        return factories.next().create(context);
    }
}
