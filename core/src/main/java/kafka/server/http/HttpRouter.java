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

import com.fasterxml.jackson.jakarta.rs.json.JacksonJsonProvider;

import org.glassfish.jersey.server.ResourceConfig;

import java.util.Map;

/**
 * Builds the Jersey {@link ResourceConfig} for the embedded HTTP REST server.
 *
 * <p>Registers the JSON provider, the Basic Auth filter (when credentials are
 * configured), and the REST resources. Resources are added in later phases.
 */
public final class HttpRouter {

    private HttpRouter() {}

    public static ResourceConfig build(Map<String, String> basicCredentials) {
        ResourceConfig config = new ResourceConfig();
        config.register(JacksonJsonProvider.class);

        if (!basicCredentials.isEmpty()) {
            config.register(new BasicAuthFilter(basicCredentials));
        }

        return config;
    }
}
