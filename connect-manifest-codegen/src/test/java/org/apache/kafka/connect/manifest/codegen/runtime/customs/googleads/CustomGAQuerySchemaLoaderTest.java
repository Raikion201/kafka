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
package org.apache.kafka.connect.manifest.codegen.runtime.customs.googleads;

import org.apache.kafka.connect.errors.ConnectException;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CustomGAQuerySchemaLoaderTest {

    @Test
    void emitsObjectSchemaWithSelectedFields() {
        CustomGAQuerySchemaLoader loader = new CustomGAQuerySchemaLoader(
            Collections.emptyMap(),
            Map.of("query", "SELECT campaign.id, campaign.name FROM campaign"));

        JsonNode schema = loader.loadSchema("custom_query");
        assertEquals("object", schema.get("type").asText());
        JsonNode props = schema.get("properties");
        assertTrue(props.has("campaign.id"));
        assertTrue(props.has("campaign.name"));
        // Phase-1 fields are typed as nullable string
        JsonNode types = props.get("campaign.id").get("type");
        assertTrue(types.isArray());
        assertEquals("string", types.get(0).asText());
        assertEquals("null", types.get(1).asText());
    }

    @Test
    void appendsCursorFieldWhenProvided() {
        CustomGAQuerySchemaLoader loader = new CustomGAQuerySchemaLoader(
            Collections.emptyMap(),
            Map.of(
                "query", "SELECT campaign.id FROM campaign",
                "cursor_field", "segments.date"));

        JsonNode schema = loader.loadSchema("custom_query");
        JsonNode props = schema.get("properties");
        assertTrue(props.has("segments.date"));
    }

    @Test
    void cursorFieldAlreadySelectedNotDuplicated() {
        CustomGAQuerySchemaLoader loader = new CustomGAQuerySchemaLoader(
            Collections.emptyMap(),
            Map.of(
                "query", "SELECT segments.date, campaign.id FROM campaign",
                "cursor_field", "segments.date"));

        JsonNode props = loader.loadSchema("x").get("properties");
        assertEquals(2, props.size());
    }

    @Test
    void missingQueryThrows() {
        assertThrows(ConnectException.class,
            () -> new CustomGAQuerySchemaLoader(Collections.emptyMap(), Collections.emptyMap()));
    }

    @Test
    void invalidQueryThrows() {
        assertThrows(ConnectException.class,
            () -> new CustomGAQuerySchemaLoader(Collections.emptyMap(), Map.of("query", "no select here")));
    }
}
