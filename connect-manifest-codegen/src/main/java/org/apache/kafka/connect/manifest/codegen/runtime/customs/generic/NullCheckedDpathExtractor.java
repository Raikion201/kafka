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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomRecordExtractor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Java port of Airbyte Pipedrive {@code NullCheckedDpathExtractor}.
 *
 * <p>Extracts records from a response, filtering out entries whose top-level value is null
 * or whose map representation contains only null values. Mirrors the Python implementation
 * which skips records where the checked field is None.</p>
 *
 * <p>Registered under {@code source_declarative_manifest.components.NullCheckedDpathExtractor}.</p>
 */
public final class NullCheckedDpathExtractor implements CustomRecordExtractor {

    public NullCheckedDpathExtractor(Map<String, String> connectorConfig,
                                     Map<String, Object> componentParams) {
    }

    @Override
    public List<Map<String, Object>> extract(JsonNode response) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (response == null || response.isNull()) {
            return result;
        }
        JsonNode data = response.isObject() ? response.get("data") : null;
        JsonNode source = (data != null && !data.isNull()) ? data : response;
        if (source.isArray()) {
            for (JsonNode node : source) {
                if (node.isNull()) continue;
                Map<String, Object> record = toMap(node);
                if (record != null && !record.isEmpty()) {
                    result.add(record);
                }
            }
        } else if (source.isObject()) {
            Map<String, Object> record = toMap(source);
            if (record != null && !record.isEmpty()) {
                result.add(record);
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> toMap(JsonNode node) {
        if (!node.isObject()) return null;
        Map<String, Object> map = new LinkedHashMap<>();
        node.properties().forEach(e -> {
            JsonNode v = e.getValue();
            if (!v.isNull()) {
                map.put(e.getKey(), nodeToObject(v));
            }
        });
        return map;
    }

    private static Object nodeToObject(JsonNode v) {
        if (v.isNull()) return null;
        if (v.isBoolean()) return v.booleanValue();
        if (v.isLong()) return v.longValue();
        if (v.isInt()) return v.intValue();
        if (v.isDouble() || v.isFloat()) return v.doubleValue();
        if (v.isTextual()) return v.textValue();
        if (v.isObject()) {
            Map<String, Object> m = new LinkedHashMap<>();
            v.properties().forEach(e -> m.put(e.getKey(), nodeToObject(e.getValue())));
            return m;
        }
        if (v.isArray()) {
            List<Object> l = new ArrayList<>();
            v.elements().forEachRemaining(e -> l.add(nodeToObject(e)));
            return l;
        }
        return v.asText();
    }
}
