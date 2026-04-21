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
package org.apache.kafka.server.http;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.http.api.AuthorizationHelper;
import org.apache.kafka.server.http.api.RecordAppender;

import com.fasterxml.jackson.jakarta.rs.json.JacksonJsonProvider;

import org.glassfish.jersey.server.ResourceConfig;

import java.util.Map;

/**
 * The Jersey route registry for the embedded HTTP REST server. One place to
 * look at to see every path the server exposes.
 *
 * <h3>Routes</h3>
 * <pre>
 *   Method  Path                        Resource             Purpose
 *   ------  --------------------------  -------------------  -----------------------
 *   POST    /v1/topics/{name}           ProduceResource      Produce a single record
 *   GET     /openapi.yaml               OpenApiResource      OpenAPI 3 spec
 *   GET     /swagger                    OpenApiResource      Swagger UI (for testing)
 * </pre>
 *
 * <h3>Filters</h3>
 * <pre>
 *   BasicAuthFilter (optional)    Rejects with 401 when http.rest.basic.credentials
 *                                 is set and the request lacks valid credentials.
 *                                 Skips /swagger and /openapi.yaml so the UI loads.
 * </pre>
 *
 * <h3>Providers</h3>
 * <pre>
 *   JacksonJsonProvider           application/json (de)serialization.
 * </pre>
 */
public final class HttpRouter {

    private HttpRouter() { }

    public static ResourceConfig build(Map<String, String> basicCredentials,
                                       RecordAppender appender,
                                       AuthorizationHelper auth,
                                       MetadataCache metadataCache,
                                       Time time) {
        ResourceConfig config = new ResourceConfig();

        // Providers
        config.register(JacksonJsonProvider.class);

        // Filters
        if (!basicCredentials.isEmpty()) {
            config.register(new BasicAuthFilter(basicCredentials));
        }

        // Routes — each resource class declares its own JAX-RS @Path / @Method
        // annotations. The table in the class-level Javadoc is the contract.
        config.register(new ProduceResource(appender, auth, metadataCache, time));
        config.register(new OpenApiResource());

        return config;
    }
}
