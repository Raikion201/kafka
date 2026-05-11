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
 * Converts the US Census Bureau API response format (2-D array) into records.
 *
 * <p>The Census API returns {@code [[header1, header2, ...], [val1, val2, ...], ...]}.
 * Row 0 is the header; each subsequent row is a data record zipped with the header.
 */
public final class USCensusRecordExtractor implements CustomRecordExtractor {

    @SuppressWarnings("unused")
    public USCensusRecordExtractor(Map<String, String> connectorConfig, Map<String, Object> params) {
    }

    @Override
    public List<Map<String, Object>> extract(JsonNode response) {
        List<Map<String, Object>> records = new ArrayList<>();
        if (!response.isArray() || response.size() < 2) {
            return records;
        }
        JsonNode headerRow = response.get(0);
        int colCount = headerRow.size();
        String[] headers = new String[colCount];
        for (int i = 0; i < colCount; i++) {
            headers[i] = headerRow.get(i).asText();
        }
        for (int r = 1; r < response.size(); r++) {
            JsonNode dataRow = response.get(r);
            Map<String, Object> record = new LinkedHashMap<>();
            for (int c = 0; c < colCount && c < dataRow.size(); c++) {
                record.put(headers[c], dataRow.get(c).asText());
            }
            records.add(record);
        }
        return records;
    }
}
