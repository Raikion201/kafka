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
 * Unit tests for the Confluent-compatible schema registry HTTP layer.
 * Uses real {@link SchemaStore} — tests status codes, response shapes, and
 * error handling across {@link SubjectResource}, {@link SchemaByIdResource},
 * and {@link SchemaConfigResource}.
 */
public class SchemaResourceTest {

    private static final String SCHEMA_A = "{\"type\":\"string\"}";
    private static final String SCHEMA_B = "{\"type\":\"int\"}";
    private static final String SUBJECT = "user-value";

    private SubjectResource subjects;
    private SchemaByIdResource schemasById;
    private SchemaConfigResource config;

    @BeforeEach
    void setUp() {
        SchemaStore store = new SchemaStore();
        subjects = new SubjectResource(store);
        schemasById = new SchemaByIdResource(store);
        config = new SchemaConfigResource(store);
    }

    // ── POST /subjects/{subject}/versions ────────────────────────────────────

    @Test
    void registerSchema_returns200WithId() {
        Response r = subjects.registerSchema(SUBJECT, body(SCHEMA_A));
        assertEquals(200, r.getStatus());
        SchemaRegisteredBody body = assertInstanceOf(SchemaRegisteredBody.class, r.getEntity());
        assertEquals(1, body.id());
    }

    @Test
    void registerSameSchema_returnsSameId() {
        Response r1 = subjects.registerSchema(SUBJECT, body(SCHEMA_A));
        Response r2 = subjects.registerSchema(SUBJECT, body(SCHEMA_A));
        assertEquals(200, r1.getStatus());
        assertEquals(200, r2.getStatus());
        int id1 = ((SchemaRegisteredBody) r1.getEntity()).id();
        int id2 = ((SchemaRegisteredBody) r2.getEntity()).id();
        assertEquals(id1, id2);
    }

    @Test
    void registerDifferentSchema_returnsNewId() {
        config.setCompatibility(SUBJECT, "{\"compatibility\":\"NONE\"}");
        Response r1 = subjects.registerSchema(SUBJECT, body(SCHEMA_A));
        Response r2 = subjects.registerSchema(SUBJECT, body(SCHEMA_B));
        int id1 = ((SchemaRegisteredBody) r1.getEntity()).id();
        int id2 = ((SchemaRegisteredBody) r2.getEntity()).id();
        assertEquals(1, id1);
        assertEquals(2, id2);
    }

    @Test
    void registerSchema_nullBody_returns400() {
        Response r = subjects.registerSchema(SUBJECT, null);
        assertEquals(400, r.getStatus());
        assertErrorCode(r, "SCHEMA_MISSING");
    }

    @Test
    void registerSchema_blankBody_returns400() {
        Response r = subjects.registerSchema(SUBJECT, "   ");
        assertEquals(400, r.getStatus());
        assertErrorCode(r, "SCHEMA_MISSING");
    }

    @Test
    void registerSchema_malformedJson_returns400() {
        Response r = subjects.registerSchema(SUBJECT, "{not valid json}");
        assertEquals(400, r.getStatus());
        assertErrorCode(r, "INVALID_REQUEST_BODY");
    }

    @Test
    void registerSchema_missingSchemaField_returns400() {
        Response r = subjects.registerSchema(SUBJECT, "{\"other\":\"field\"}");
        assertEquals(400, r.getStatus());
        assertErrorCode(r, "SCHEMA_MISSING");
    }

    @Test
    void registerSchema_blankSchemaValue_returns400() {
        Response r = subjects.registerSchema(SUBJECT, "{\"schema\":\"   \"}");
        assertEquals(400, r.getStatus());
        assertErrorCode(r, "SCHEMA_MISSING");
    }

    @Test
    void registerSchema_invalidJson_returns400() {
        Response r = subjects.registerSchema(SUBJECT, "{\"schema\":\"{not valid json}\"}");
        assertEquals(400, r.getStatus());
        assertErrorCode(r, "SCHEMA_INVALID_JSON");
    }

    // ── GET /subjects/{subject}/versions ─────────────────────────────────────

    @Test
    void getVersions_returns200WithVersionList() {
        config.setCompatibility(SUBJECT, "{\"compatibility\":\"NONE\"}");
        subjects.registerSchema(SUBJECT, body(SCHEMA_A));
        subjects.registerSchema(SUBJECT, body(SCHEMA_B));

        Response r = subjects.getVersions(SUBJECT);
        assertEquals(200, r.getStatus());
        @SuppressWarnings("unchecked")
        List<Integer> versions = (List<Integer>) r.getEntity();
        assertEquals(List.of(1, 2), versions);
    }

    @Test
    void getVersions_unknownSubject_returns404() {
        Response r = subjects.getVersions("no-such-subject");
        assertEquals(404, r.getStatus());
        assertErrorCode(r, "SUBJECT_NOT_FOUND");
    }

    // ── GET /subjects/{subject}/versions/latest ───────────────────────────────

    @Test
    void getLatestVersion_returns200WithFullDetails() {
        config.setCompatibility(SUBJECT, "{\"compatibility\":\"NONE\"}");
        subjects.registerSchema(SUBJECT, body(SCHEMA_A));
        subjects.registerSchema(SUBJECT, body(SCHEMA_B));

        Response r = subjects.getLatestVersion(SUBJECT);
        assertEquals(200, r.getStatus());
        SchemaVersionBody body = assertInstanceOf(SchemaVersionBody.class, r.getEntity());
        assertEquals(2, body.id());
        assertEquals(SUBJECT, body.subject());
        assertEquals(2, body.version());
        assertEquals(SCHEMA_B, body.schema());
    }

    @Test
    void getLatestVersion_unknownSubject_returns404() {
        Response r = subjects.getLatestVersion("no-such-subject");
        assertEquals(404, r.getStatus());
        assertErrorCode(r, "SUBJECT_NOT_FOUND");
    }

    // ── GET /subjects/{subject}/versions/{version} ───────────────────────────

    @Test
    void getSchemaVersion_returns200WithFullDetails() {
        subjects.registerSchema(SUBJECT, body(SCHEMA_A));

        Response r = subjects.getSchemaVersion(SUBJECT, 1);
        assertEquals(200, r.getStatus());
        SchemaVersionBody body = assertInstanceOf(SchemaVersionBody.class, r.getEntity());
        assertEquals(1, body.id());
        assertEquals(SUBJECT, body.subject());
        assertEquals(1, body.version());
        assertEquals(SCHEMA_A, body.schema());
    }

    @Test
    void getSchemaVersion_unknownSubject_returns404() {
        Response r = subjects.getSchemaVersion("no-such-subject", 1);
        assertEquals(404, r.getStatus());
        assertErrorCode(r, "VERSION_NOT_FOUND");
    }

    @Test
    void getSchemaVersion_versionOutOfRange_returns404() {
        subjects.registerSchema(SUBJECT, body(SCHEMA_A));
        Response r = subjects.getSchemaVersion(SUBJECT, 99);
        assertEquals(404, r.getStatus());
        assertErrorCode(r, "VERSION_NOT_FOUND");
    }

    // ── GET /schemas/ids/{id} ─────────────────────────────────────────────────

    @Test
    void getSchemaById_returns200WithSchemaString() {
        subjects.registerSchema(SUBJECT, body(SCHEMA_A));

        Response r = schemasById.getSchemaById(1);
        assertEquals(200, r.getStatus());
        @SuppressWarnings("unchecked")
        Map<String, String> entity = (Map<String, String>) r.getEntity();
        assertEquals(SCHEMA_A, entity.get("schema"));
    }

    @Test
    void getSchemaById_unknownId_returns404() {
        Response r = schemasById.getSchemaById(999);
        assertEquals(404, r.getStatus());
        assertErrorCode(r, "SCHEMA_NOT_FOUND");
    }

    // ── DELETE /subjects/{subject} ────────────────────────────────────────────

    @Test
    void deleteSubject_returns200WithDeletedVersionNumbers() {
        config.setCompatibility(SUBJECT, "{\"compatibility\":\"NONE\"}");
        subjects.registerSchema(SUBJECT, body(SCHEMA_A));
        subjects.registerSchema(SUBJECT, body(SCHEMA_B));

        Response r = subjects.deleteSubject(SUBJECT);
        assertEquals(200, r.getStatus());
        @SuppressWarnings("unchecked")
        List<Integer> deleted = (List<Integer>) r.getEntity();
        assertEquals(List.of(1, 2), deleted);
    }

    @Test
    void deleteSubject_removesSubjectSoGetVersionsReturns404() {
        subjects.registerSchema(SUBJECT, body(SCHEMA_A));
        subjects.deleteSubject(SUBJECT);

        Response r = subjects.getVersions(SUBJECT);
        assertEquals(404, r.getStatus());
    }

    @Test
    void deleteSubject_unknownSubject_returns404() {
        Response r = subjects.deleteSubject("no-such-subject");
        assertEquals(404, r.getStatus());
        assertErrorCode(r, "SUBJECT_NOT_FOUND");
    }

    // ── GET/PUT /config/{subject} ─────────────────────────────────────────────

    @Test
    void getCompatibility_defaultIsBackward() {
        Response r = config.getCompatibility(SUBJECT);
        assertEquals(200, r.getStatus());
        @SuppressWarnings("unchecked")
        Map<String, String> entity = (Map<String, String>) r.getEntity();
        assertEquals("BACKWARD", entity.get("compatibility"));
    }

    @Test
    void setCompatibility_updatesMode() {
        Response r = config.setCompatibility(SUBJECT, "{\"compatibility\":\"NONE\"}");
        assertEquals(200, r.getStatus());
        @SuppressWarnings("unchecked")
        Map<String, String> entity = (Map<String, String>) r.getEntity();
        assertEquals("NONE", entity.get("compatibility"));

        Response r2 = config.getCompatibility(SUBJECT);
        @SuppressWarnings("unchecked")
        Map<String, String> entity2 = (Map<String, String>) r2.getEntity();
        assertEquals("NONE", entity2.get("compatibility"));
    }

    @Test
    void setCompatibility_invalidMode_returns400() {
        Response r = config.setCompatibility(SUBJECT, "{\"compatibility\":\"INVALID\"}");
        assertEquals(400, r.getStatus());
    }

    // ── Compatibility enforcement at register time ────────────────────────────

    private static final String RECORD_V1 =
        "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
        "{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}";

    @Test
    void registerSchema_backwardCompatible_addFieldWithDefault_passes() {
        subjects.registerSchema(SUBJECT, body(RECORD_V1));
        String v2 = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}," +
            "{\"name\":\"email\",\"type\":\"string\",\"default\":\"\"}]}";
        Response r = subjects.registerSchema(SUBJECT, body(v2));
        assertEquals(200, r.getStatus());
    }

    @Test
    void registerSchema_backwardIncompatible_addFieldWithoutDefault_returns409() {
        subjects.registerSchema(SUBJECT, body(RECORD_V1));
        String v2 = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}," +
            "{\"name\":\"email\",\"type\":\"string\"}]}";  // no default
        Response r = subjects.registerSchema(SUBJECT, body(v2));
        assertEquals(409, r.getStatus());
        assertErrorCodeStartsWith(r, "SCHEMA_INCOMPATIBLE");
    }

    @Test
    void registerSchema_noneMode_allowsBreakingChange() {
        subjects.registerSchema(SUBJECT, body(RECORD_V1));
        config.setCompatibility(SUBJECT, "{\"compatibility\":\"NONE\"}");
        String v2 = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}," +
            "{\"name\":\"email\",\"type\":\"string\"}]}";
        Response r = subjects.registerSchema(SUBJECT, body(v2));
        assertEquals(200, r.getStatus());
    }

    @Test
    void registerSchema_typeChange_returns409() {
        subjects.registerSchema(SUBJECT, body("{\"type\":\"string\"}"));
        Response r = subjects.registerSchema(SUBJECT, body("{\"type\":\"int\"}"));
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
