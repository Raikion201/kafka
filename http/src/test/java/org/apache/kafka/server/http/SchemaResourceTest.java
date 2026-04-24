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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import jakarta.ws.rs.core.Response;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link SchemaResource}. Uses a real {@link SchemaStore} —
 * the point is to test the HTTP layer (status codes, response shapes, error
 * handling) not the store itself (covered by {@link SchemaStoreTest}).
 */
public class SchemaResourceTest {

    private static final String SCHEMA_A = "{\"type\":\"string\"}";
    private static final String SCHEMA_B = "{\"type\":\"int\"}";
    private static final String SUBJECT = "user-value";

    private SchemaResource resource;

    @BeforeEach
    void setUp() {
        resource = new SchemaResource(new SchemaStore());
    }

    // ── POST /v1/schemas/subjects/{subject} ──────────────────────────────────

    @Test
    void registerSchema_returns201WithId() {
        Response r = resource.registerSchema(SUBJECT, body(SCHEMA_A));
        assertEquals(201, r.getStatus());
        SchemaRegisteredBody body = assertInstanceOf(SchemaRegisteredBody.class, r.getEntity());
        assertEquals(1, body.id());
    }

    @Test
    void registerSameSchema_returnsSameId() {
        Response r1 = resource.registerSchema(SUBJECT, body(SCHEMA_A));
        Response r2 = resource.registerSchema(SUBJECT, body(SCHEMA_A));
        assertEquals(201, r1.getStatus());
        assertEquals(201, r2.getStatus());
        int id1 = ((SchemaRegisteredBody) r1.getEntity()).id();
        int id2 = ((SchemaRegisteredBody) r2.getEntity()).id();
        assertEquals(id1, id2);
    }

    @Test
    void registerDifferentSchema_returnsNewId() {
        resource.setCompatibility(SUBJECT, "{\"compatibility\":\"NONE\"}");
        Response r1 = resource.registerSchema(SUBJECT, body(SCHEMA_A));
        Response r2 = resource.registerSchema(SUBJECT, body(SCHEMA_B));
        int id1 = ((SchemaRegisteredBody) r1.getEntity()).id();
        int id2 = ((SchemaRegisteredBody) r2.getEntity()).id();
        assertEquals(1, id1);
        assertEquals(2, id2);
    }

    @Test
    void registerSchema_nullBody_returns400() {
        Response r = resource.registerSchema(SUBJECT, null);
        assertEquals(400, r.getStatus());
        assertErrorCode(r, "SCHEMA_MISSING");
    }

    @Test
    void registerSchema_blankBody_returns400() {
        Response r = resource.registerSchema(SUBJECT, "   ");
        assertEquals(400, r.getStatus());
        assertErrorCode(r, "SCHEMA_MISSING");
    }

    @Test
    void registerSchema_malformedJson_returns400() {
        Response r = resource.registerSchema(SUBJECT, "{not valid json}");
        assertEquals(400, r.getStatus());
        assertErrorCode(r, "INVALID_REQUEST_BODY");
    }

    @Test
    void registerSchema_missingSchemaField_returns400() {
        // valid JSON but no "schema" key
        Response r = resource.registerSchema(SUBJECT, "{\"other\":\"field\"}");
        assertEquals(400, r.getStatus());
        assertErrorCode(r, "SCHEMA_MISSING");
    }

    @Test
    void registerSchema_blankSchemaValue_returns400() {
        Response r = resource.registerSchema(SUBJECT, "{\"schema\":\"   \"}");
        assertEquals(400, r.getStatus());
        assertErrorCode(r, "SCHEMA_MISSING");
    }

    @Test
    void registerSchema_invalidJson_returns400() {
        Response r = resource.registerSchema(SUBJECT, "{\"schema\":\"{not valid json}\"}");
        assertEquals(400, r.getStatus());
        assertErrorCode(r, "SCHEMA_INVALID_JSON");
    }

    // ── GET /v1/schemas/subjects/{subject} ───────────────────────────────────

    @Test
    void getVersions_returns200WithVersionList() {
        resource.setCompatibility(SUBJECT, "{\"compatibility\":\"NONE\"}");
        resource.registerSchema(SUBJECT, body(SCHEMA_A));
        resource.registerSchema(SUBJECT, body(SCHEMA_B));

        Response r = resource.getVersions(SUBJECT);
        assertEquals(200, r.getStatus());
        @SuppressWarnings("unchecked")
        Map<String, List<Integer>> entity = (Map<String, List<Integer>>) r.getEntity();
        assertEquals(List.of(1, 2), entity.get("versions"));
    }

    @Test
    void getVersions_unknownSubject_returns404() {
        Response r = resource.getVersions("no-such-subject");
        assertEquals(404, r.getStatus());
        assertErrorCode(r, "SUBJECT_NOT_FOUND");
    }

    // ── GET /v1/schemas/subjects/{subject}/versions/{version} ────────────────

    @Test
    void getSchemaVersion_returns200WithFullDetails() {
        resource.registerSchema(SUBJECT, body(SCHEMA_A));

        Response r = resource.getSchemaVersion(SUBJECT, 1);
        assertEquals(200, r.getStatus());
        SchemaVersionBody body = assertInstanceOf(SchemaVersionBody.class, r.getEntity());
        assertEquals(1, body.id());
        assertEquals(SUBJECT, body.subject());
        assertEquals(1, body.version());
        assertEquals(SCHEMA_A, body.schema());
    }

    @Test
    void getSchemaVersion_unknownSubject_returns404() {
        Response r = resource.getSchemaVersion("no-such-subject", 1);
        assertEquals(404, r.getStatus());
        assertErrorCode(r, "VERSION_NOT_FOUND");
    }

    @Test
    void getSchemaVersion_versionOutOfRange_returns404() {
        resource.registerSchema(SUBJECT, body(SCHEMA_A));
        Response r = resource.getSchemaVersion(SUBJECT, 99);
        assertEquals(404, r.getStatus());
        assertErrorCode(r, "VERSION_NOT_FOUND");
    }

    // ── GET /v1/schemas/{id} ─────────────────────────────────────────────────

    @Test
    void getSchemaById_returns200WithFullDetails() {
        resource.registerSchema(SUBJECT, body(SCHEMA_A));

        Response r = resource.getSchemaById(1);
        assertEquals(200, r.getStatus());
        SchemaVersionBody body = assertInstanceOf(SchemaVersionBody.class, r.getEntity());
        assertEquals(1, body.id());
        assertEquals(SUBJECT, body.subject());
        assertEquals(1, body.version());
        assertEquals(SCHEMA_A, body.schema());
    }

    @Test
    void getSchemaById_unknownId_returns404() {
        Response r = resource.getSchemaById(999);
        assertEquals(404, r.getStatus());
        assertErrorCode(r, "SCHEMA_NOT_FOUND");
    }

    // ── DELETE /v1/schemas/subjects/{subject} ─────────────────────────────────

    @Test
    void deleteSubject_returns200WithDeletedVersionIds() {
        resource.setCompatibility(SUBJECT, "{\"compatibility\":\"NONE\"}");
        resource.registerSchema(SUBJECT, body(SCHEMA_A));
        resource.registerSchema(SUBJECT, body(SCHEMA_B));

        Response r = resource.deleteSubject(SUBJECT);
        assertEquals(200, r.getStatus());
        @SuppressWarnings("unchecked")
        Map<String, Object> entity = (Map<String, Object>) r.getEntity();
        assertEquals(SUBJECT, entity.get("subject"));
        assertEquals(List.of(1, 2), entity.get("versions"));
    }

    @Test
    void deleteSubject_removesSubjectSoGetVersionsReturns404() {
        resource.registerSchema(SUBJECT, body(SCHEMA_A));
        resource.deleteSubject(SUBJECT);

        Response r = resource.getVersions(SUBJECT);
        assertEquals(404, r.getStatus());
    }

    @Test
    void deleteSubject_unknownSubject_returns404() {
        Response r = resource.deleteSubject("no-such-subject");
        assertEquals(404, r.getStatus());
        assertErrorCode(r, "SUBJECT_NOT_FOUND");
    }

    // ── GET/PUT /v1/schemas/subjects/{subject}/config ─────────────────────────

    @Test
    void getCompatibility_defaultIsBackward() {
        Response r = resource.getCompatibility(SUBJECT);
        assertEquals(200, r.getStatus());
        @SuppressWarnings("unchecked")
        Map<String, String> entity = (Map<String, String>) r.getEntity();
        assertEquals("BACKWARD", entity.get("compatibility"));
    }

    @Test
    void setCompatibility_updatesMode() {
        Response r = resource.setCompatibility(SUBJECT, "{\"compatibility\":\"NONE\"}");
        assertEquals(200, r.getStatus());
        @SuppressWarnings("unchecked")
        Map<String, String> entity = (Map<String, String>) r.getEntity();
        assertEquals("NONE", entity.get("compatibility"));

        Response r2 = resource.getCompatibility(SUBJECT);
        @SuppressWarnings("unchecked")
        Map<String, String> entity2 = (Map<String, String>) r2.getEntity();
        assertEquals("NONE", entity2.get("compatibility"));
    }

    @Test
    void setCompatibility_invalidMode_returns400() {
        Response r = resource.setCompatibility(SUBJECT, "{\"compatibility\":\"INVALID\"}");
        assertEquals(400, r.getStatus());
    }

    // ── Compatibility enforcement at register time ────────────────────────────

    private static final String RECORD_V1 =
        "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
        "{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}";

    @Test
    void registerSchema_backwardCompatible_addFieldWithDefault_passes() {
        resource.registerSchema(SUBJECT, body(RECORD_V1));
        String v2 = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}," +
            "{\"name\":\"email\",\"type\":\"string\",\"default\":\"\"}]}";
        Response r = resource.registerSchema(SUBJECT, body(v2));
        assertEquals(201, r.getStatus());
    }

    @Test
    void registerSchema_backwardIncompatible_addFieldWithoutDefault_returns409() {
        resource.registerSchema(SUBJECT, body(RECORD_V1));
        String v2 = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}," +
            "{\"name\":\"email\",\"type\":\"string\"}]}";  // no default
        Response r = resource.registerSchema(SUBJECT, body(v2));
        assertEquals(409, r.getStatus());
        assertErrorCodeStartsWith(r, "SCHEMA_INCOMPATIBLE");
    }

    @Test
    void registerSchema_noneMode_allowsBreakingChange() {
        resource.registerSchema(SUBJECT, body(RECORD_V1));
        resource.setCompatibility(SUBJECT, "{\"compatibility\":\"NONE\"}");
        String v2 = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}," +
            "{\"name\":\"email\",\"type\":\"string\"}]}";  // no default — normally fails BACKWARD
        Response r = resource.registerSchema(SUBJECT, body(v2));
        assertEquals(201, r.getStatus());
    }

    @Test
    void registerSchema_typeChange_returns409() {
        resource.registerSchema(SUBJECT, body("{\"type\":\"string\"}"));
        Response r = resource.registerSchema(SUBJECT, body("{\"type\":\"int\"}"));
        assertEquals(409, r.getStatus());
        assertErrorCodeStartsWith(r, "SCHEMA_INCOMPATIBLE");
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static String body(String schema) {
        return "{\"schema\":\"" + schema.replace("\"", "\\\"") + "\"}";
    }

    @SuppressWarnings("unchecked")
    private static void assertErrorCode(Response r, String expectedCode) {
        Map<String, String> entity = assertInstanceOf(Map.class, r.getEntity());
        assertNotNull(entity.get("error"), "response body should have an 'error' key");
        assertEquals(expectedCode, entity.get("error"));
    }

    @SuppressWarnings("unchecked")
    private static void assertErrorCodeStartsWith(Response r, String prefix) {
        Map<String, String> entity = assertInstanceOf(Map.class, r.getEntity());
        assertNotNull(entity.get("error"), "response body should have an 'error' key");
        assertTrue(entity.get("error").startsWith(prefix),
            "expected error starting with '" + prefix + "' but was: " + entity.get("error"));
    }
}
