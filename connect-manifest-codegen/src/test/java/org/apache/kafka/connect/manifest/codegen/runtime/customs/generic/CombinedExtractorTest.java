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
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CombinedExtractorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final CombinedExtractor extractor = new CombinedExtractor(Map.of(), Map.of());

    @Test
    void extract_twoRows_zipsHeadersAndValues() throws Exception {
        String json = "{"
            + "\"dimensionHeaders\": [{\"name\": \"date\"}, {\"name\": \"country\"}],"
            + "\"metricHeaders\":   [{\"name\": \"sessions\"}, {\"name\": \"users\"}],"
            + "\"rows\": ["
            + "  {\"dimensionValues\": [{\"value\": \"2024-01-01\"}, {\"value\": \"US\"}],"
            + "   \"metricValues\":    [{\"value\": \"100\"}, {\"value\": \"80\"}]},"
            + "  {\"dimensionValues\": [{\"value\": \"2024-01-02\"}, {\"value\": \"GB\"}],"
            + "   \"metricValues\":    [{\"value\": \"200\"}, {\"value\": \"160\"}]}"
            + "]}";
        JsonNode response = MAPPER.readTree(json);

        List<Map<String, Object>> records = extractor.extract(response);

        assertEquals(2, records.size());

        Map<String, Object> row0 = records.get(0);
        assertEquals("2024-01-01", row0.get("date"));
        assertEquals("US", row0.get("country"));
        assertEquals("100", row0.get("sessions"));
        assertEquals("80", row0.get("users"));

        Map<String, Object> row1 = records.get(1);
        assertEquals("2024-01-02", row1.get("date"));
        assertEquals("GB", row1.get("country"));
        assertEquals("200", row1.get("sessions"));
        assertEquals("160", row1.get("users"));
    }

    @Test
    void extract_nullResponse_returnsEmpty() {
        assertTrue(extractor.extract(null).isEmpty());
    }

    @Test
    void extract_missingRows_returnsEmpty() throws Exception {
        JsonNode response = MAPPER.readTree("{\"dimensionHeaders\": [], \"metricHeaders\": []}");
        assertTrue(extractor.extract(response).isEmpty());
    }

    @Test
    void extract_emptyRows_returnsEmpty() throws Exception {
        JsonNode response = MAPPER.readTree(
            "{\"dimensionHeaders\": [], \"metricHeaders\": [], \"rows\": []}");
        assertTrue(extractor.extract(response).isEmpty());
    }

    @Test
    void extract_noHeaders_returnsEmptyMaps() throws Exception {
        String json = "{"
            + "\"dimensionHeaders\": [],"
            + "\"metricHeaders\":   [],"
            + "\"rows\": [{\"dimensionValues\": [], \"metricValues\": []}]"
            + "}";
        List<Map<String, Object>> records = extractor.extract(MAPPER.readTree(json));
        assertEquals(1, records.size());
        assertTrue(records.get(0).isEmpty());
    }

    @Test
    void extract_onlyDimensions_noMetrics() throws Exception {
        String json = "{"
            + "\"dimensionHeaders\": [{\"name\": \"city\"}],"
            + "\"metricHeaders\":   [],"
            + "\"rows\": [{\"dimensionValues\": [{\"value\": \"NYC\"}], \"metricValues\": []}]"
            + "}";
        List<Map<String, Object>> records = extractor.extract(MAPPER.readTree(json));
        assertEquals(1, records.size());
        assertEquals("NYC", records.get(0).get("city"));
        assertEquals(1, records.get(0).size());
    }

    @Test
    void extract_nullNode_returnsEmpty() throws Exception {
        JsonNode nullNode = MAPPER.readTree("null");
        assertEquals(Collections.emptyList(), extractor.extract(nullNode));
    }
}
