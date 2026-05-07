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
package org.apache.kafka.connect.manifest.codegen.runtime.customs.generic;

import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomRecordExtractor;
import org.apache.kafka.connect.manifest.codegen.runtime.jinja.JinjaRenderer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Java port of the Airbyte CDK {@code ObjectDpathExtractor} used in
 * {@code source-alpha-vantage}.
 *
 * <p>Navigates into the HTTP response body using {@code field_path} (a list of keys,
 * each potentially a Jinja template rendered against connector config), expects a JSON
 * object at that path, then emits one record per key-value pair. The key is injected
 * into each record under the field named by {@code inject_key_as_field}.</p>
 *
 * <p>Example (alpha-vantage):</p>
 * <pre>
 * field_path: ["Time Series ({{ config['interval'] }})"]
 * inject_key_as_field: timestamp
 * </pre>
 * <p>Response: {@code {"Time Series (1min)": {"2024-01-01 12:00:00": {"1. open": "150.0", ...}}}}</p>
 * <p>Output: one record per timestamp, each containing the OHLCV fields plus
 * {@code "timestamp": "2024-01-01 12:00:00"}.</p>
 *
 * <p>Registered under {@code source_declarative_manifest.components.ObjectDpathExtractor}.</p>
 */
public final class ObjectDpathExtractor implements CustomRecordExtractor {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final List<String> renderedFieldPath;
    private final String injectKeyAsField;

    @SuppressWarnings("unchecked")
    public ObjectDpathExtractor(Map<String, String> connectorConfig,
                                Map<String, Object> componentParams) {
        Map<String, Object> configCtx = new HashMap<>();
        configCtx.put("config", connectorConfig == null ? Map.of() : connectorConfig);

        Object rawPath = componentParams == null ? null : componentParams.get("field_path");
        List<String> rendered = new ArrayList<>();
        if (rawPath instanceof List) {
            for (Object seg : (List<?>) rawPath) {
                if (seg == null) {
                    continue;
                }
                String s = seg.toString();
                rendered.add(JinjaRenderer.hasJinjaSyntax(s)
                    ? JinjaRenderer.renderLenient(s, configCtx)
                    : s);
            }
        }
        this.renderedFieldPath = rendered;

        Object inj = componentParams == null ? null : componentParams.get("inject_key_as_field");
        this.injectKeyAsField = inj == null ? null : inj.toString();
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> extract(JsonNode response) {
        if (response == null || response.isNull()) {
            return Collections.emptyList();
        }
        JsonNode node = navigate(response, renderedFieldPath);
        if (node == null || !node.isObject()) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> records = new ArrayList<>();
        for (Map.Entry<String, JsonNode> entry : node.properties()) {
            Map<String, Object> record = new HashMap<>();
            // Merge the value node's fields into the record
            if (entry.getValue().isObject()) {
                for (Map.Entry<String, JsonNode> vf : entry.getValue().properties()) {
                    record.put(vf.getKey(), jsonNodeToObject(vf.getValue()));
                }
            } else {
                record.put("value", jsonNodeToObject(entry.getValue()));
            }
            // Inject the map key as a named field
            if (injectKeyAsField != null) {
                record.put(injectKeyAsField, entry.getKey());
            }
            records.add(record);
        }
        return records;
    }

    private static JsonNode navigate(JsonNode root, List<String> path) {
        JsonNode cur = root;
        for (String seg : path) {
            if (cur == null || !cur.isObject()) {
                return null;
            }
            cur = cur.get(seg);
        }
        return cur;
    }

    private static Object jsonNodeToObject(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isTextual()) {
            return node.asText();
        }
        if (node.isInt() || node.isLong()) {
            return node.asLong();
        }
        if (node.isNumber()) {
            return node.asDouble();
        }
        if (node.isBoolean()) {
            return node.asBoolean();
        }
        // For objects/arrays, convert via MAPPER to a generic Map/List
        return MAPPER.convertValue(node, Object.class);
    }
}
