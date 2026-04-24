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

import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.apache.avro.SchemaCompatibility.SchemaPairCompatibility;

import static org.apache.avro.SchemaCompatibility.checkReaderWriterCompatibility;

/**
 * Checks whether a new schema version is compatible with the previous version
 * using the official Avro schema compatibility API.
 *
 * <ul>
 *   <li><b>BACKWARD</b>: new schema (reader) can read data written by old schema (writer)</li>
 *   <li><b>FORWARD</b>: old schema (reader) can read data written by new schema (writer)</li>
 *   <li><b>FULL</b>: both BACKWARD and FORWARD</li>
 *   <li><b>NONE</b>: no check</li>
 * </ul>
 */
public final class SchemaCompatibilityChecker {

    private SchemaCompatibilityChecker() { }

    /**
     * Check that {@code newSchemaStr} is compatible with {@code oldSchemaStr} under
     * the given {@code mode}.
     *
     * @throws SchemaCompatibilityException if the schemas are incompatible
     */
    public static void check(String oldSchemaStr, String newSchemaStr, SchemaCompatibility mode)
            throws SchemaCompatibilityException {
        if (mode == SchemaCompatibility.NONE) {
            return;
        }

        Schema oldSchema;
        Schema newSchema;
        try {
            oldSchema = new Schema.Parser().parse(oldSchemaStr);
            newSchema = new Schema.Parser().parse(newSchemaStr);
        } catch (Exception e) {
            throw new SchemaCompatibilityException("Cannot parse schemas for compatibility check: " + e.getMessage());
        }

        switch (mode) {
            case BACKWARD -> assertCompatible(checkReaderWriterCompatibility(newSchema, oldSchema), "BACKWARD");
            case FORWARD  -> assertCompatible(checkReaderWriterCompatibility(oldSchema, newSchema), "FORWARD");
            case FULL     -> {
                assertCompatible(checkReaderWriterCompatibility(newSchema, oldSchema), "FULL (BACKWARD direction)");
                assertCompatible(checkReaderWriterCompatibility(oldSchema, newSchema), "FULL (FORWARD direction)");
            }
            default -> { }
        }
    }

    private static void assertCompatible(SchemaPairCompatibility result, String mode)
            throws SchemaCompatibilityException {
        if (result.getType() != SchemaCompatibilityType.COMPATIBLE) {
            throw new SchemaCompatibilityException(mode + " incompatible: " + result.getDescription());
        }
    }
}
