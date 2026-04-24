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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SchemaCompatibilityCheckerTest {

    // ── NONE ─────────────────────────────────────────────────────────────────

    @Test
    void none_anyChange_passes() {
        assertDoesNotThrow(() -> SchemaCompatibilityChecker.check(
            "{\"type\":\"string\"}", "{\"type\":\"int\"}", SchemaCompatibility.NONE));
    }

    // ── Primitive type change ─────────────────────────────────────────────────

    @Test
    void backward_primitiveTypeChange_throws() {
        assertThrows(SchemaCompatibilityException.class, () ->
            SchemaCompatibilityChecker.check(
                "{\"type\":\"string\"}", "{\"type\":\"int\"}", SchemaCompatibility.BACKWARD));
    }

    @Test
    void forward_primitiveTypeChange_throws() {
        assertThrows(SchemaCompatibilityException.class, () ->
            SchemaCompatibilityChecker.check(
                "{\"type\":\"string\"}", "{\"type\":\"int\"}", SchemaCompatibility.FORWARD));
    }

    @Test
    void full_primitiveTypeChange_throws() {
        assertThrows(SchemaCompatibilityException.class, () ->
            SchemaCompatibilityChecker.check(
                "{\"type\":\"string\"}", "{\"type\":\"int\"}", SchemaCompatibility.FULL));
    }

    @Test
    void backward_samePrimitiveType_passes() {
        assertDoesNotThrow(() -> SchemaCompatibilityChecker.check(
            "{\"type\":\"string\"}", "{\"type\":\"string\"}", SchemaCompatibility.BACKWARD));
    }

    // ── BACKWARD: record field rules ──────────────────────────────────────────

    private static final String RECORD_V1 =
        "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
        "{\"name\":\"id\",\"type\":\"int\"}," +
        "{\"name\":\"name\",\"type\":\"string\"}" +
        "]}";

    @Test
    void backward_addFieldWithDefault_passes() {
        String v2 = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"int\"}," +
            "{\"name\":\"name\",\"type\":\"string\"}," +
            "{\"name\":\"email\",\"type\":\"string\",\"default\":\"\"}" +
            "]}";
        assertDoesNotThrow(() ->
            SchemaCompatibilityChecker.check(RECORD_V1, v2, SchemaCompatibility.BACKWARD));
    }

    @Test
    void backward_addFieldWithoutDefault_throws() {
        String v2 = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"int\"}," +
            "{\"name\":\"name\",\"type\":\"string\"}," +
            "{\"name\":\"email\",\"type\":\"string\"}" +  // no default
            "]}";
        assertThrows(SchemaCompatibilityException.class, () ->
            SchemaCompatibilityChecker.check(RECORD_V1, v2, SchemaCompatibility.BACKWARD));
    }

    @Test
    void backward_removeField_passes() {
        String v2 = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"int\"}" +
            "]}";
        assertDoesNotThrow(() ->
            SchemaCompatibilityChecker.check(RECORD_V1, v2, SchemaCompatibility.BACKWARD));
    }

    @Test
    void backward_changeFieldType_throws() {
        String v2 = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"string\"}," +  // was int
            "{\"name\":\"name\",\"type\":\"string\"}" +
            "]}";
        assertThrows(SchemaCompatibilityException.class, () ->
            SchemaCompatibilityChecker.check(RECORD_V1, v2, SchemaCompatibility.BACKWARD));
    }

    // ── FORWARD: record field rules ───────────────────────────────────────────

    private static final String RECORD_WITH_DEFAULT =
        "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
        "{\"name\":\"id\",\"type\":\"int\"}," +
        "{\"name\":\"name\",\"type\":\"string\",\"default\":\"\"}" +
        "]}";

    @Test
    void forward_addFieldNoDefault_passes() {
        String v2 = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"int\"}," +
            "{\"name\":\"name\",\"type\":\"string\",\"default\":\"\"}," +
            "{\"name\":\"email\",\"type\":\"string\"}" +  // no default needed for FORWARD
            "]}";
        assertDoesNotThrow(() ->
            SchemaCompatibilityChecker.check(RECORD_WITH_DEFAULT, v2, SchemaCompatibility.FORWARD));
    }

    @Test
    void forward_removeFieldWithDefault_passes() {
        String v2 = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"int\"}" +
            "]}";
        assertDoesNotThrow(() ->
            SchemaCompatibilityChecker.check(RECORD_WITH_DEFAULT, v2, SchemaCompatibility.FORWARD));
    }

    @Test
    void forward_removeFieldWithoutDefault_throws() {
        // RECORD_V1 has "id" with no default — removing it breaks FORWARD
        String v2 = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
            "{\"name\":\"name\",\"type\":\"string\"}" +
            "]}";
        assertThrows(SchemaCompatibilityException.class, () ->
            SchemaCompatibilityChecker.check(RECORD_V1, v2, SchemaCompatibility.FORWARD));
    }

    // ── FULL ──────────────────────────────────────────────────────────────────

    @Test
    void full_addFieldWithDefault_passes() {
        String v2 = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"int\"}," +
            "{\"name\":\"name\",\"type\":\"string\",\"default\":\"\"}," +
            "{\"name\":\"email\",\"type\":\"string\",\"default\":\"\"}" +
            "]}";
        // RECORD_WITH_DEFAULT has "name" with default — adding "email" with default is FULL safe
        assertDoesNotThrow(() ->
            SchemaCompatibilityChecker.check(RECORD_WITH_DEFAULT, v2, SchemaCompatibility.FULL));
    }

    @Test
    void full_addFieldWithoutDefault_throws() {
        String v2 = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"int\"}," +
            "{\"name\":\"name\",\"type\":\"string\",\"default\":\"\"}," +
            "{\"name\":\"email\",\"type\":\"string\"}" +  // no default — fails BACKWARD
            "]}";
        assertThrows(SchemaCompatibilityException.class, () ->
            SchemaCompatibilityChecker.check(RECORD_WITH_DEFAULT, v2, SchemaCompatibility.FULL));
    }
}
