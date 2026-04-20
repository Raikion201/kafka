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

import kafka.server.ReplicaManager;

import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.authorizer.Authorizer;

import com.fasterxml.jackson.jakarta.rs.json.JacksonJsonProvider;

import org.glassfish.jersey.server.ResourceConfig;

import java.util.Map;

import scala.Option;

/**
 * Builds the Jersey {@link ResourceConfig} for the embedded HTTP REST server.
 *
 * <p>Registers the JSON provider, the Basic Auth filter (when credentials are
 * configured), and the REST resources.
 */
public final class HttpRouter {

    private HttpRouter() {}

    public static ResourceConfig build(
            Map<String, String> basicCredentials,
            ReplicaManager replicaManager,
            Option<Plugin<Authorizer>> authorizerPlugin,
            MetadataCache metadataCache,
            Time time) {
        ResourceConfig config = new ResourceConfig();
        config.register(JacksonJsonProvider.class);

        if (!basicCredentials.isEmpty()) {
            config.register(new BasicAuthFilter(basicCredentials));
        }

        config.register(new ProduceResource(replicaManager, authorizerPlugin, metadataCache, time));

        return config;
    }
}
