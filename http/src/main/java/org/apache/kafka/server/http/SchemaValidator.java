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

/**
 * Validates a string value against a simple Avro-style schema.
 *
 * <p>Supported types: {@code string}, {@code int}, {@code long},
 * {@code float}, {@code double}, {@code boolean}, {@code record},
 * {@code array}. Unknown types are silently skipped.</p>
 *
 * <p>For {@code record}, every field declared in the schema's
 * {@code "fields"} array must be present as a key in the JSON value.</p>
 */
public final class SchemaValidator {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SchemaValidator() { }

    /**
     * Validate {@code value} against {@code schema}.
     *
     * @throws SchemaValidationException if the value does not match the schema type
     */
    public static void validate(String schema, String value) throws SchemaValidationException {
        JsonNode schemaNode;
        try {
            schemaNode = MAPPER.readTree(schema);
        } catch (Exception e) {
            return; // schema is not valid JSON — nothing to validate against
        }

        String type = schemaNode.path("type").asText("");
        switch (type) {
            case "string"         -> validateString(value);
            case "int", "long"    -> validateInteger(value);
            case "float", "double" -> validateDouble(value);
            case "boolean"        -> validateBoolean(value);
            case "record"         -> validateRecord(value, schemaNode);
            case "array"          -> validateArray(value);
            default               -> { /* unknown type — skip */ }
        }
    }

    private static void validateString(String value) throws SchemaValidationException {
        if (value == null) {
            throw new SchemaValidationException("schema type is 'string' but value is null");
        }
    }

    private static void validateInteger(String value) throws SchemaValidationException {
        if (value == null) {
            throw new SchemaValidationException("schema type is 'int/long' but value is null");
        }
        try {
            Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new SchemaValidationException("schema type is 'int/long' but value is not an integer: " + value);
        }
    }

    private static void validateDouble(String value) throws SchemaValidationException {
        if (value == null) {
            throw new SchemaValidationException("schema type is 'float/double' but value is null");
        }
        try {
            Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new SchemaValidationException("schema type is 'float/double' but value is not a number: " + value);
        }
    }

    private static void validateBoolean(String value) throws SchemaValidationException {
        if (!"true".equals(value) && !"false".equals(value)) {
            throw new SchemaValidationException("schema type is 'boolean' but value is: " + value);
        }
    }

    private static void validateRecord(String value, JsonNode schemaNode) throws SchemaValidationException {
        JsonNode valueNode;
        try {
            valueNode = MAPPER.readTree(value);
        } catch (Exception e) {
            throw new SchemaValidationException("schema type is 'record' but value is not valid JSON");
        }
        if (!valueNode.isObject()) {
            throw new SchemaValidationException("schema type is 'record' but value is not a JSON object");
        }
        JsonNode fields = schemaNode.path("fields");
        if (fields.isArray()) {
            for (JsonNode field : fields) {
                String fieldName = field.path("name").asText();
                if (!valueNode.has(fieldName)) {
                    throw new SchemaValidationException("missing required field: '" + fieldName + "'");
                }
            }
        }
    }

    private static void validateArray(String value) throws SchemaValidationException {
        try {
            JsonNode node = MAPPER.readTree(value);
            if (!node.isArray()) {
                throw new SchemaValidationException("schema type is 'array' but value is not a JSON array");
            }
        } catch (SchemaValidationException e) {
            throw e;
        } catch (Exception e) {
            throw new SchemaValidationException("schema type is 'array' but value is not valid JSON");
        }
    }
}
