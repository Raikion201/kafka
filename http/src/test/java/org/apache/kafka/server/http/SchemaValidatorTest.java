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

    // ── schema-only validation ───────────────────────────────────────────────

    @Test
    void validPrimitiveSchema_passes() {
        assertDoesNotThrow(() -> SchemaValidator.validate("{\"type\":\"string\"}"));
    }

    @Test
    void validRecordSchema_passes() {
        assertDoesNotThrow(() -> SchemaValidator.validate(
                "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"));
    }

    @Test
    void invalidSchema_throws() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaValidator.validate("{not valid json}"));
    }

    @Test
    void unknownType_throws() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaValidator.validate("{\"type\":\"nonexistent\"}"));
    }

    // ── value validation ─────────────────────────────────────────────────────

    @Test
    void string_validValue_passes() {
        assertDoesNotThrow(() -> SchemaValidator.validateValue("{\"type\":\"string\"}", "\"hello\""));
    }

    @Test
    void int_validValue_passes() {
        assertDoesNotThrow(() -> SchemaValidator.validateValue("{\"type\":\"int\"}", "42"));
    }

    @Test
    void long_validValue_passes() {
        assertDoesNotThrow(() -> SchemaValidator.validateValue("{\"type\":\"long\"}", "9999999999"));
    }

    @Test
    void int_stringValue_throws() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaValidator.validateValue("{\"type\":\"int\"}", "\"hello\""));
    }

    @Test
    void double_validValue_passes() {
        assertDoesNotThrow(() -> SchemaValidator.validateValue("{\"type\":\"double\"}", "3.14"));
    }

    @Test
    void float_notANumber_throws() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaValidator.validateValue("{\"type\":\"float\"}", "\"abc\""));
    }

    @Test
    void boolean_trueValue_passes() {
        assertDoesNotThrow(() -> SchemaValidator.validateValue("{\"type\":\"boolean\"}", "true"));
    }

    @Test
    void boolean_falseValue_passes() {
        assertDoesNotThrow(() -> SchemaValidator.validateValue("{\"type\":\"boolean\"}", "false"));
    }

    @Test
    void boolean_invalidValue_throws() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaValidator.validateValue("{\"type\":\"boolean\"}", "\"yes\""));
    }

    // ── record ───────────────────────────────────────────────────────────────

    private static final String RECORD_SCHEMA =
            "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
            "{\"name\":\"id\",\"type\":\"int\"}," +
            "{\"name\":\"name\",\"type\":\"string\"}]}";

    @Test
    void record_allFieldsPresent_passes() {
        assertDoesNotThrow(() -> SchemaValidator.validateValue(RECORD_SCHEMA, "{\"id\":1,\"name\":\"Alice\"}"));
    }

    @Test
    void record_missingField_throws() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaValidator.validateValue(RECORD_SCHEMA, "{\"id\":1}"));
    }

    @Test
    void record_notJsonObject_throws() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaValidator.validateValue(RECORD_SCHEMA, "\"hello\""));
    }

    @Test
    void record_invalidJson_throws() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaValidator.validateValue(RECORD_SCHEMA, "{broken"));
    }
}
