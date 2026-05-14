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
 * Java port of the Jira {@code LabelsRecordExtractor}.
 *
 * <p>The Jira {@code GET /rest/api/3/label} response returns a paginated envelope whose
 * {@code values} array contains plain label strings.  This extractor navigates to that
 * array (via {@code field_path: ["values"]} in the manifest) and wraps each string as a
 * single-field record {@code {"label": "string"}} matching the declared schema.</p>
 *
 * <p>Registered under {@code source_declarative_manifest.components.LabelsRecordExtractor}.</p>
 */
public final class LabelsRecordExtractor implements CustomRecordExtractor {

    public LabelsRecordExtractor(Map<String, String> connectorConfig,
                                 Map<String, Object> componentParams) {
    }

    @Override
    public List<Map<String, Object>> extract(JsonNode response) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (response == null || response.isNull()) {
            return result;
        }
        JsonNode values = response.get("values");
        if (values == null || !values.isArray()) {
            return result;
        }
        for (JsonNode node : values) {
            if (node.isTextual()) {
                Map<String, Object> record = new LinkedHashMap<>();
                record.put("label", node.textValue());
                result.add(record);
            } else if (node.isObject()) {
                Map<String, Object> record = new LinkedHashMap<>();
                node.properties().forEach(e -> record.put(e.getKey(), e.getValue().asText()));
                result.add(record);
            }
        }
        return result;
    }
}
