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
 * REST endpoints for the embedded schema registry.
 *
 * <h3>Routes</h3>
 * <pre>
 *   Method  Path                                        Purpose
 *   ------  ------------------------------------------  ----------------------------
 *   POST    /v1/schemas/subjects/{subject}              Register a schema
 *   GET     /v1/schemas/subjects/{subject}              List versions for a subject
 *   GET     /v1/schemas/subjects/{subject}/versions/{v} Fetch a specific version
 *   GET     /v1/schemas/{id}                            Fetch a schema by global ID
 *   DELETE  /v1/schemas/subjects/{subject}              Delete a subject
 * </pre>
 *
 * <p>All operations are synchronous — the underlying {@link SchemaStore} is
 * a pure in-memory structure with no I/O, so async suspension is unnecessary.</p>
 *
 * <p>Registered by {@link HttpRouter} alongside {@link ProduceResource}. Both
 * share the same {@link BasicAuthFilter} when credentials are configured.</p>
 */
@Path("/v1/schemas")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class SchemaResource {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final SchemaStore store;

    public SchemaResource(SchemaStore store) {
        this.store = store;
    }

    /**
     * Register a schema under a subject.
     *
     * <p>If the exact same schema content is already registered (under any
     * subject), the existing ID is returned without creating a new version.</p>
     *
     * @param subject the subject name (e.g. {@code user-value})
     * @param rawBody JSON body: {@code {"schema": "..."}}
     * @return 201 with {@code {"id": N}} on success; 400 on bad input
     */
    @POST
    @Path("/subjects/{subject}")
    public Response registerSchema(@PathParam("subject") String subject, String rawBody) {
        SchemaBody body = parseBody(rawBody);
        if (body == null || body.schema() == null || body.schema().isBlank()) {
            return error(Response.Status.BAD_REQUEST, "SCHEMA_MISSING");
        }
        String schema = body.schema().strip();
        try {
            OBJECT_MAPPER.readTree(schema);
        } catch (Exception e) {
            return error(Response.Status.BAD_REQUEST, "SCHEMA_INVALID_JSON");
        }
        int id = store.register(subject, schema);
        return Response.status(Response.Status.CREATED)
                .entity(new SchemaRegisteredBody(id))
                .build();
    }

    /**
     * List all versions registered under a subject.
     *
     * @param subject the subject name
     * @return 200 with {@code {"versions": [1, 2, ...]}}; 404 if subject unknown
     */
    @GET
    @Path("/subjects/{subject}")
    public Response getVersions(@PathParam("subject") String subject) {
        List<Integer> versions = store.getVersions(subject);
        if (versions.isEmpty()) {
            return error(Response.Status.NOT_FOUND, "SUBJECT_NOT_FOUND");
        }
        return Response.ok(Map.of("versions", versions)).build();
    }

    /**
     * Fetch a specific version of a subject.
     *
     * @param subject the subject name
     * @param version 1-based version number
     * @return 200 with full schema details; 404 if not found
     */
    @GET
    @Path("/subjects/{subject}/versions/{version}")
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
     * Fetch a schema by its global ID.
     *
     * @param id the schema ID returned by a prior register call
     * @return 200 with full schema details; 404 if ID unknown
     */
    @GET
    @Path("/{id}")
    public Response getSchemaById(@PathParam("id") int id) {
        String schema = store.getById(id);
        if (schema == null) {
            return error(Response.Status.NOT_FOUND, "SCHEMA_NOT_FOUND");
        }
        SchemaStore.SubjectVersion sv = store.subjectOf(id);
        // sv is always non-null when schema != null (register() sets both atomically)
        return Response.ok(new SchemaVersionBody(id, sv.subject(), sv.version(), schema)).build();
    }

    /**
     * Delete a subject and all its versions.
     * Schema content and global IDs remain stored — only the subject mapping
     * is removed (same behaviour as Confluent Schema Registry).
     *
     * @param subject the subject name
     * @return 200 with deleted version list; 404 if subject unknown
     */
    @DELETE
    @Path("/subjects/{subject}")
    public Response deleteSubject(@PathParam("subject") String subject) {
        List<Integer> deleted = store.deleteSubject(subject);
        if (deleted.isEmpty()) {
            return error(Response.Status.NOT_FOUND, "SUBJECT_NOT_FOUND");
        }
        return Response.ok(Map.of("subject", subject, "versions", deleted)).build();
    }

    private static SchemaBody parseBody(String rawBody) {
        if (rawBody == null || rawBody.isBlank()) {
            return null;
        }
        try {
            return OBJECT_MAPPER.readValue(rawBody, SchemaBody.class);
        } catch (Exception e) {
            return null;
        }
    }

    private static Response error(Response.Status status, String code) {
        return Response.status(status).entity(Map.of("error", code)).build();
    }
}
