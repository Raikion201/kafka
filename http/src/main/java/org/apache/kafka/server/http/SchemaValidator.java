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
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

/**
 * Validates that a schema string is a well-formed Avro schema by parsing it
 * with the official Avro {@link Schema.Parser}. Any schema type supported by
 * Avro (record, enum, array, map, union, fixed, and all primitives) is accepted.
 */
public final class SchemaValidator {

    private SchemaValidator() { }

    /**
     * Parse {@code schema} with Avro's schema parser.
     *
     * @throws SchemaValidationException if the string is not a valid Avro schema
     */
    public static void validate(String schema) throws SchemaValidationException {
        try {
            new Schema.Parser().parse(schema);
        } catch (SchemaParseException e) {
            throw new SchemaValidationException("Invalid Avro schema: " + e.getMessage());
        } catch (Exception e) {
            throw new SchemaValidationException("Cannot parse schema: " + e.getMessage());
        }
    }

    /**
     * Validate that {@code jsonValue} conforms to the given Avro {@code schemaStr}
     * by decoding it through Avro's JSON decoder.
     *
     * @throws SchemaValidationException if the schema is invalid or the value does not conform
     */
    public static void validateValue(String schemaStr, String jsonValue) throws SchemaValidationException {
        Schema schema;
        try {
            schema = new Schema.Parser().parse(schemaStr);
        } catch (Exception e) {
            throw new SchemaValidationException("Cannot parse schema: " + e.getMessage());
        }
        try {
            DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, jsonValue);
            reader.read(null, decoder);
        } catch (Exception e) {
            throw new SchemaValidationException("Value does not match schema: " + e.getMessage());
        }
    }
}
