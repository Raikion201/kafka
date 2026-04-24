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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Checks whether a new schema version is compatible with the previous version
 * under a given {@link SchemaCompatibility} mode.
 *
 * <h3>Rules per type</h3>
 *
 * <p><b>Primitives</b> ({@code string}, {@code int}, {@code long}, etc.):
 * the {@code type} field must not change under any compatibility mode.</p>
 *
 * <p><b>Records</b>:</p>
 * <ul>
 *   <li><b>BACKWARD</b> (new reads old data):
 *     <ul>
 *       <li>Add field → must have a {@code default} (old data won't have it)</li>
 *       <li>Remove field → always OK (new reader just won't see it)</li>
 *       <li>Change field type → never OK</li>
 *     </ul>
 *   </li>
 *   <li><b>FORWARD</b> (old reads new data):
 *     <ul>
 *       <li>Add field → always OK (old reader ignores unknown fields)</li>
 *       <li>Remove field → must have had a {@code default} in old schema</li>
 *       <li>Change field type → never OK</li>
 *     </ul>
 *   </li>
 *   <li><b>FULL</b>: must satisfy both BACKWARD and FORWARD.</li>
 *   <li><b>NONE</b>: no check performed.</li>
 * </ul>
 */
public final class SchemaCompatibilityChecker {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SchemaCompatibilityChecker() { }

    /**
     * Check that {@code newSchema} is compatible with {@code oldSchema} under
     * the given {@code mode}.
     *
     * @throws SchemaCompatibilityException if the schemas are incompatible
     */
    public static void check(String oldSchema, String newSchema, SchemaCompatibility mode)
            throws SchemaCompatibilityException {
        if (mode == SchemaCompatibility.NONE) {
            return;
        }

        JsonNode oldNode;
        JsonNode newNode;
        try {
            oldNode = MAPPER.readTree(oldSchema);
            newNode = MAPPER.readTree(newSchema);
        } catch (Exception e) {
            throw new SchemaCompatibilityException("Cannot parse schemas for compatibility check: " + e.getMessage());
        }

        String oldType = oldNode.path("type").asText("");
        String newType = newNode.path("type").asText("");

        if (!oldType.equals(newType)) {
            throw new SchemaCompatibilityException(
                "Incompatible type change: '" + oldType + "' → '" + newType + "'. Type cannot change.");
        }

        if ("record".equals(newType)) {
            switch (mode) {
                case BACKWARD -> checkBackward(oldNode, newNode);
                case FORWARD  -> checkForward(oldNode, newNode);
                case FULL     -> {
                    checkBackward(oldNode, newNode);
                    checkForward(oldNode, newNode);
                }
                default       -> { }
            }
        }
        // primitives: type already matched above, nothing more to check
    }

    // new schema must be able to read data written by old schema
    private static void checkBackward(JsonNode oldNode, JsonNode newNode)
            throws SchemaCompatibilityException {
        Map<String, JsonNode> oldFields = fieldsMap(oldNode);
        Map<String, JsonNode> newFields = fieldsMap(newNode);

        // added fields must have a default (old data won't contain them)
        for (Map.Entry<String, JsonNode> entry : newFields.entrySet()) {
            if (!oldFields.containsKey(entry.getKey())) {
                if (!entry.getValue().has("default")) {
                    throw new SchemaCompatibilityException(
                        "BACKWARD incompatible: new field '" + entry.getKey() +
                        "' has no default — old data won't have this field");
                }
            }
        }

        // type changes are never allowed
        checkTypeChanges(oldFields, newFields);
    }

    // old schema must be able to read data written by new schema
    private static void checkForward(JsonNode oldNode, JsonNode newNode)
            throws SchemaCompatibilityException {
        Map<String, JsonNode> oldFields = fieldsMap(oldNode);
        Map<String, JsonNode> newFields = fieldsMap(newNode);

        // removed fields must have had a default in old schema (old reader needs a fallback)
        for (Map.Entry<String, JsonNode> entry : oldFields.entrySet()) {
            if (!newFields.containsKey(entry.getKey())) {
                if (!entry.getValue().has("default")) {
                    throw new SchemaCompatibilityException(
                        "FORWARD incompatible: field '" + entry.getKey() +
                        "' was removed but had no default — old readers cannot handle missing field");
                }
            }
        }

        // type changes are never allowed
        checkTypeChanges(oldFields, newFields);
    }

    private static void checkTypeChanges(Map<String, JsonNode> oldFields, Map<String, JsonNode> newFields)
            throws SchemaCompatibilityException {
        for (Map.Entry<String, JsonNode> entry : newFields.entrySet()) {
            JsonNode oldField = oldFields.get(entry.getKey());
            if (oldField == null) continue; // new field — handled by caller

            String oldType = oldField.path("type").asText("");
            String newType = entry.getValue().path("type").asText("");
            if (!oldType.equals(newType)) {
                throw new SchemaCompatibilityException(
                    "Incompatible type change on field '" + entry.getKey() +
                    "': '" + oldType + "' → '" + newType + "'");
            }
        }
    }

    private static Map<String, JsonNode> fieldsMap(JsonNode schemaNode) {
        Map<String, JsonNode> map = new LinkedHashMap<>();
        JsonNode fields = schemaNode.path("fields");
        if (fields.isArray()) {
            for (JsonNode field : fields) {
                String name = field.path("name").asText();
                if (!name.isEmpty()) {
                    map.put(name, field);
                }
            }
        }
        return map;
    }
}
