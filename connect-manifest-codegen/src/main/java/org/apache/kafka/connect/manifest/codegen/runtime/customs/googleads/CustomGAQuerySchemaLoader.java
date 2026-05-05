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
import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomSchemaLoader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Java port of {@code source_google_ads.components.CustomGAQuerySchemaLoader}
 * (components.py:1009-1116).
 *
 * <p>Phase 1 minimum-viable port: parse the user-supplied GAQL query and emit a
 * draft-07 JSON Schema where every selected field is declared as a nullable string.
 * Python additionally calls into the Google Ads SDK to retrieve per-field metadata
 * (data type, repeated, enum values) — that path requires the gRPC SDK and is
 * deferred to Phase 2 along with the requesters.</p>
 *
 * <p>The Phase 2 work will:</p>
 * <ul>
 *     <li>Replace string-typed properties with {@code GOOGLE_ADS_DATATYPE_MAPPING}-derived types.</li>
 *     <li>Populate {@link #allMessageFields()} for {@link SerializeMessageFieldsTransformation}.</li>
 * </ul>
 */
public final class CustomGAQuerySchemaLoader implements CustomSchemaLoader {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Pattern FROM_PATTERN = Pattern.compile("\\bFROM\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern SELECT_PATTERN = Pattern.compile("\\bSELECT\\b", Pattern.CASE_INSENSITIVE);

    /** Class-level set mirroring Python's {@code _all_message_fields ClassVar[Set[str]]}. */
    private static final Set<String> ALL_MESSAGE_FIELDS = Collections.synchronizedSet(new LinkedHashSet<>());

    private final List<String> fields;

    public CustomGAQuerySchemaLoader(Map<String, String> connectorConfig,
                                     Map<String, Object> componentParams) {
        Object q = componentParams == null ? null : componentParams.get("query");
        if (q == null) {
            throw new ConnectException("CustomGAQuerySchemaLoader requires a 'query' parameter");
        }
        this.fields = parseFields(q.toString());

        Object cursor = componentParams == null ? null : componentParams.get("cursor_field");
        if (cursor != null) {
            String cf = cursor.toString();
            if (!cf.isEmpty() && !this.fields.contains(cf)) {
                this.fields.add(cf);
            }
        }
    }

    /**
     * Mirror of Python {@code _get_list_of_fields} (components.py:1086-1106). Extracts
     * field names from the {@code SELECT ... FROM} portion of the query.
     */
    private static List<String> parseFields(String query) {
        Matcher selectM = SELECT_PATTERN.matcher(query);
        if (!selectM.find()) {
            throw new ConnectException("Could not find a valid SELECT clause in query: " + query);
        }
        Matcher fromM = FROM_PATTERN.matcher(query);
        if (!fromM.find(selectM.end())) {
            throw new ConnectException("Could not find a valid FROM clause in query: " + query);
        }
        String body = query.substring(selectM.end(), fromM.start()).trim();
        List<String> out = new ArrayList<>();
        for (String f : body.split(",")) {
            String trimmed = f.trim();
            if (!trimmed.isEmpty()) {
                out.add(trimmed);
            }
        }
        return out;
    }

    @Override
    public JsonNode loadSchema(String streamName) {
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("$schema", "http://json-schema.org/draft-07/schema#");
        schema.put("type", "object");
        schema.put("additionalProperties", true);

        ObjectNode props = schema.putObject("properties");
        for (String field : fields) {
            ObjectNode prop = props.putObject(field);
            ArrayNode types = prop.putArray("type");
            // Phase 1: every field is a nullable string (Phase 2 wires real Google Ads metadata).
            types.add("string");
            types.add("null");
        }
        return schema;
    }

    /** Visible for tests and {@link SerializeMessageFieldsTransformation}. */
    public static Set<String> allMessageFields() {
        return ALL_MESSAGE_FIELDS;
    }

    /** Visible for tests — drains the shared message-field state. */
    static void resetMessageFieldsForTesting() {
        ALL_MESSAGE_FIELDS.clear();
    }
}
