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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.Map;

/**
 * Compatibility-config endpoints — matches the Confluent Schema Registry API.
 *
 * <pre>
 *   GET /config/{subject}   Return the compatibility mode for a subject
 *   PUT /config/{subject}   Update the compatibility mode for a subject
 * </pre>
 */
@Path("/config")
@Produces(MediaType.APPLICATION_JSON)
@Consumes({MediaType.APPLICATION_JSON, "application/vnd.schemaregistry.v1+json"})
public class SchemaConfigResource {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final SchemaStore store;

    public SchemaConfigResource(SchemaStore store) {
        this.store = store;
    }

    /**
     * Get the compatibility mode for a subject.
     * Returns {@code BACKWARD} if not explicitly configured.
     *
     * @return 200 {@code {"compatibility":"BACKWARD"}}
     */
    @GET
    @Path("/{subject}")
    public Response getCompatibility(@PathParam("subject") String subject) {
        SchemaCompatibility mode = store.getCompatibility(subject);
        return Response.ok(Map.of("compatibility", mode.name())).build();
    }

    /**
     * Set the compatibility mode for a subject.
     *
     * @param rawBody {@code {"compatibility":"NONE"}} — valid values: BACKWARD, FORWARD, FULL, NONE
     * @return 200 with the updated mode; 400 on invalid input
     */
    @PUT
    @Path("/{subject}")
    public Response setCompatibility(
            @PathParam("subject") String subject,
            String rawBody) {
        if (rawBody == null || rawBody.isBlank()) {
            return SubjectResource.error(Response.Status.BAD_REQUEST, "MISSING_COMPATIBILITY_FIELD");
        }
        JsonNode node;
        try {
            node = MAPPER.readTree(rawBody);
        } catch (Exception e) {
            return SubjectResource.error(Response.Status.BAD_REQUEST, "INVALID_REQUEST_BODY");
        }
        JsonNode compatNode = node.get("compatibility");
        if (compatNode == null || compatNode.asText("").isBlank()) {
            return SubjectResource.error(Response.Status.BAD_REQUEST, "MISSING_COMPATIBILITY_FIELD");
        }
        SchemaCompatibility mode;
        try {
            mode = SchemaCompatibility.fromString(compatNode.asText());
        } catch (IllegalArgumentException e) {
            return SubjectResource.error(
                Response.Status.BAD_REQUEST, "INVALID_COMPATIBILITY_MODE: " + e.getMessage());
        }
        store.setCompatibility(subject, mode);
        return Response.ok(Map.of("compatibility", mode.name())).build();
    }
}
