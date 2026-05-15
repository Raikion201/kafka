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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Port of Google Analytics 4 {@code CombinedExtractor}.
 *
 * <p>GA4 {@code runReport} responses carry a flat {@code dimensionHeaders} / {@code metricHeaders}
 * array at the top level and per-row {@code dimensionValues} / {@code metricValues} arrays inside
 * each {@code rows} entry. This extractor zips headers with values to produce one flat
 * {@code Map<String,Object>} record per row.</p>
 *
 * <p>Registered under
 * {@code source_declarative_manifest.components.CombinedExtractor}.</p>
 */
public final class CombinedExtractor implements CustomRecordExtractor {

    public CombinedExtractor(Map<String, String> connectorConfig,
                              Map<String, Object> componentParams) {
    }

    @Override
    public List<Map<String, Object>> extract(JsonNode response) {
        if (response == null || response.isNull()) {
            return Collections.emptyList();
        }

        List<String> dimNames = headerNames(response.path("dimensionHeaders"));
        List<String> metNames = headerNames(response.path("metricHeaders"));

        JsonNode rows = response.path("rows");
        if (!rows.isArray()) {
            return Collections.emptyList();
        }

        List<Map<String, Object>> result = new ArrayList<>();
        for (JsonNode row : rows) {
            Map<String, Object> record = new LinkedHashMap<>();
            zipValues(record, dimNames, row.path("dimensionValues"));
            zipValues(record, metNames, row.path("metricValues"));
            result.add(record);
        }
        return result;
    }

    private static List<String> headerNames(JsonNode headers) {
        List<String> names = new ArrayList<>();
        if (headers.isArray()) {
            for (JsonNode h : headers) {
                names.add(h.path("name").asText(""));
            }
        }
        return names;
    }

    private static void zipValues(Map<String, Object> record, List<String> names, JsonNode values) {
        if (!values.isArray()) {
            return;
        }
        for (int i = 0; i < names.size() && i < values.size(); i++) {
            record.put(names.get(i), values.get(i).path("value").asText(""));
        }
    }
}
