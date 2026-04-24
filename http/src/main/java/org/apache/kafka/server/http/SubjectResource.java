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
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.List;
import java.util.Map;

/**
 * Subject-scoped endpoints — matches the Confluent Schema Registry API so
 * standard tools like {@code kafka-avro-console-producer} work without
 * any client-side configuration changes.
 *
 * <pre>
 *   POST   /subjects/{subject}/versions         Register a schema
 *   GET    /subjects/{subject}/versions         List version numbers
 *   GET    /subjects/{subject}/versions/latest  Fetch the latest version
 *   GET    /subjects/{subject}/versions/{v}     Fetch a specific version
 *   DELETE /subjects/{subject}                  Delete a subject
 * </pre>
 */
@Path("/subjects")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class SubjectResource {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final SchemaStore store;

    public SubjectResource(SchemaStore store) {
        this.store = store;
    }

    /**
     * Register a schema. Returns the existing ID if the same schema content
     * was already registered. Checks compatibility before accepting a new version.
     *
     * @param subject the subject name (e.g. {@code user-value})
     * @param rawBody {@code {"schema":"..."}} — optionally also {@code "schemaType"}
     * @return 200 {@code {"id":N}}; 400 on bad input; 409 on incompatible schema
     */
    @POST
    @Path("/{subject}/versions")
    public Response registerSchema(
            @PathParam("subject") String subject,
            String rawBody) {
        if (rawBody == null || rawBody.isBlank()) {
            return error(Response.Status.BAD_REQUEST, "SCHEMA_MISSING");
        }
        JsonNode node;
        try {
            node = MAPPER.readTree(rawBody);
        } catch (Exception e) {
            return error(Response.Status.BAD_REQUEST, "INVALID_REQUEST_BODY");
        }
        JsonNode schemaNode = node.get("schema");
        if (schemaNode == null || schemaNode.asText("").isBlank()) {
            return error(Response.Status.BAD_REQUEST, "SCHEMA_MISSING");
        }
        String schema = schemaNode.asText().strip();
        try {
            MAPPER.readTree(schema);
        } catch (Exception e) {
            return error(Response.Status.BAD_REQUEST, "SCHEMA_INVALID_JSON");
        }

        Response compatError = checkCompatibility(subject, schema);
        if (compatError != null) {
            return compatError;
        }

        int id = store.register(subject, schema);
        // Confluent SR returns 200 (not 201) for POST /subjects/{subject}/versions
        return Response.ok(new SchemaRegisteredBody(id)).build();
    }

    /**
     * List all version numbers for a subject.
     *
     * @return 200 {@code [1, 2, ...]}; 404 if subject unknown
     */
    @GET
    @Path("/{subject}/versions")
    public Response getVersions(@PathParam("subject") String subject) {
        List<Integer> versions = store.getVersions(subject);
        if (versions.isEmpty()) {
            return error(Response.Status.NOT_FOUND, "SUBJECT_NOT_FOUND");
        }
        return Response.ok(versions).build();
    }

    /**
     * Fetch the latest registered version of a subject.
     *
     * @return 200 with full schema details; 404 if subject unknown
     */
    @GET
    @Path("/{subject}/versions/latest")
    public Response getLatestVersion(@PathParam("subject") String subject) {
        List<Integer> versions = store.getVersions(subject);
        if (versions.isEmpty()) {
            return error(Response.Status.NOT_FOUND, "SUBJECT_NOT_FOUND");
        }
        int version = versions.size();
        int id = versions.get(version - 1);
        String schema = store.getById(id);
        return Response.ok(new SchemaVersionBody(id, subject, version, schema)).build();
    }

    /**
     * Fetch a specific version of a subject.
     *
     * @param version 1-based version number
     * @return 200 with full schema details; 404 if not found
     */
    @GET
    @Path("/{subject}/versions/{version}")
    public Response getSchemaVersion(
            @PathParam("subject") String subject,
            @PathParam("version") int version) {
        Integer id = store.getVersion(subject, version);
        if (id == null) {
            return error(Response.Status.NOT_FOUND, "VERSION_NOT_FOUND");
        }
        String schema = store.getById(id);
        return Response.ok(new SchemaVersionBody(id, subject, version, schema)).build();
    }

    /**
     * Delete a subject and all its versions.
     * Schema content and global IDs are retained; only the subject mapping is removed.
     *
     * @return 200 {@code [1, 2, ...]} (deleted version numbers); 404 if subject unknown
     */
    @DELETE
    @Path("/{subject}")
    public Response deleteSubject(@PathParam("subject") String subject) {
        List<Integer> deleted = store.deleteSubject(subject);
        if (deleted.isEmpty()) {
            return error(Response.Status.NOT_FOUND, "SUBJECT_NOT_FOUND");
        }
        return Response.ok(deleted).build();
    }

    private Response checkCompatibility(String subject, String schema) {
        String latestSchema = store.getLatestSchema(subject);
        if (latestSchema == null || latestSchema.equals(schema)) {
            return null;
        }
        SchemaCompatibility mode = store.getCompatibility(subject);
        try {
            SchemaCompatibilityChecker.check(latestSchema, schema, mode);
            return null;
        } catch (SchemaCompatibilityException e) {
            return error(Response.Status.CONFLICT, "SCHEMA_INCOMPATIBLE: " + e.getMessage());
        }
    }

    static Response error(Response.Status status, String code) {
        return Response.status(status).entity(Map.of("error", code)).build();
    }
}
