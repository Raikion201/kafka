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

public class SchemaValidatorTest {

    // ── string ───────────────────────────────────────────────────────────────

    @Test
    void string_validValue_passes() {
        assertDoesNotThrow(() -> SchemaValidator.validate("{\"type\":\"string\"}", "hello"));
    }

    @Test
    void string_nullValue_throws() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaValidator.validate("{\"type\":\"string\"}", null));
    }

    // ── int / long ───────────────────────────────────────────────────────────

    @Test
    void int_validValue_passes() {
        assertDoesNotThrow(() -> SchemaValidator.validate("{\"type\":\"int\"}", "42"));
    }

    @Test
    void long_validValue_passes() {
        assertDoesNotThrow(() -> SchemaValidator.validate("{\"type\":\"long\"}", "9999999999"));
    }

    @Test
    void int_stringValue_throws() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaValidator.validate("{\"type\":\"int\"}", "hello"));
    }

    @Test
    void int_nullValue_throws() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaValidator.validate("{\"type\":\"int\"}", null));
    }

    // ── float / double ───────────────────────────────────────────────────────

    @Test
    void double_validValue_passes() {
        assertDoesNotThrow(() -> SchemaValidator.validate("{\"type\":\"double\"}", "3.14"));
    }

    @Test
    void float_notANumber_throws() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaValidator.validate("{\"type\":\"float\"}", "abc"));
    }

    // ── boolean ──────────────────────────────────────────────────────────────

    @Test
    void boolean_trueValue_passes() {
        assertDoesNotThrow(() -> SchemaValidator.validate("{\"type\":\"boolean\"}", "true"));
    }

    @Test
    void boolean_falseValue_passes() {
        assertDoesNotThrow(() -> SchemaValidator.validate("{\"type\":\"boolean\"}", "false"));
    }

    @Test
    void boolean_invalidValue_throws() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaValidator.validate("{\"type\":\"boolean\"}", "yes"));
    }

    // ── record ───────────────────────────────────────────────────────────────

    private static final String RECORD_SCHEMA =
            "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"int\"}," +
            "{\"name\":\"name\",\"type\":\"string\"}]}";

    @Test
    void record_allFieldsPresent_passes() {
        assertDoesNotThrow(() -> SchemaValidator.validate(RECORD_SCHEMA, "{\"id\":1,\"name\":\"Alice\"}"));
    }

    @Test
    void record_missingField_throws() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaValidator.validate(RECORD_SCHEMA, "{\"id\":1}"));
    }

    @Test
    void record_notJsonObject_throws() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaValidator.validate(RECORD_SCHEMA, "hello"));
    }

    @Test
    void record_invalidJson_throws() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaValidator.validate(RECORD_SCHEMA, "{broken"));
    }

    // ── array ────────────────────────────────────────────────────────────────

    @Test
    void array_validValue_passes() {
        assertDoesNotThrow(() -> SchemaValidator.validate("{\"type\":\"array\"}", "[1,2,3]"));
    }

    @Test
    void array_notArray_throws() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaValidator.validate("{\"type\":\"array\"}", "{\"key\":\"val\"}"));
    }

    // ── unknown type / invalid schema ────────────────────────────────────────

    @Test
    void unknownType_skipsValidation() {
        // should not throw — unknown types are silently skipped
        assertDoesNotThrow(() -> SchemaValidator.validate("{\"type\":\"bytes\"}", "anything"));
    }

    @Test
    void invalidSchema_throws() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaValidator.validate("{not valid json}", "anything"));
    }
}
