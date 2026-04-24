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
 *   Method  Path                                         Resource        Purpose
 *   ------  -------------------------------------------  --------------  -----------------------
 *   POST    /v1/topics/{name}                            ProduceResource Produce a single record
 *   POST    /v1/schemas/subjects/{subject}               SchemaResource  Register a schema
 *   GET     /v1/schemas/subjects/{subject}               SchemaResource  List versions
 *   GET     /v1/schemas/subjects/{subject}/versions/{v}  SchemaResource  Fetch a version
 *   GET     /v1/schemas/{id}                             SchemaResource  Fetch schema by ID
 *   DELETE  /v1/schemas/subjects/{subject}               SchemaResource  Delete a subject
 *   GET     /openapi.yaml                                OpenApiResource OpenAPI 3 spec (opt-in)
 *   GET     /swagger                                     OpenApiResource Swagger UI     (opt-in)
 * </pre>
 *
 * <p>The OpenAPI / Swagger routes are only registered when {@code swaggerUiEnabled}
 * is {@code true}. They are off by default because they're served without Basic
 * Auth (the UI needs to load before the user can authenticate) and reveal the
 * endpoint surface to anyone who can reach the REST port.</p>
 *
 * <h3>Filters</h3>
 * <pre>
 *   BasicAuthFilter (optional)    Rejects with 401 when http.rest.basic.credentials
 *                                 is set and the request lacks valid credentials.
 *                                 Skips /swagger and /openapi.yaml (when those are
 *                                 enabled) so the UI can load.
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
                                       boolean swaggerUiEnabled,
                                       int requestTimeoutMs,
                                       RecordAppender appender,
                                       AuthorizationHelper auth,
                                       MetadataCache metadataCache,
                                       Time time,
                                       SchemaStore schemaStore) {
        ResourceConfig config = new ResourceConfig();

        // Providers
        config.register(JacksonJsonProvider.class);

        // Filters
        if (!basicCredentials.isEmpty()) {
            config.register(new BasicAuthFilter(basicCredentials));
        }

        // Routes — each resource class declares its own JAX-RS @Path / @Method
        // annotations. The table in the class-level Javadoc is the contract.
        config.register(new ProduceResource(appender, auth, metadataCache, time, requestTimeoutMs));
        config.register(new SchemaResource(schemaStore));
        if (swaggerUiEnabled) {
            config.register(new OpenApiResource());
        }

        return config;
    }
}
