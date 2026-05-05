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

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AccessibleAccountsExtractorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final AccessibleAccountsExtractor x =
        new AccessibleAccountsExtractor(Collections.emptyMap(), Collections.emptyMap());

    @Test
    void extractsCustomerIdsFromResourceNames() throws Exception {
        JsonNode body = MAPPER.readTree(
            "{\"resourceNames\":[\"customers/1234567890\",\"customers/9876543210\"]}");

        List<Map<String, Object>> records = x.extract(body);
        assertEquals(2, records.size());
        assertEquals("1234567890", records.get(0).get("accessible_customer_id"));
        assertEquals("9876543210", records.get(1).get("accessible_customer_id"));
    }

    @Test
    void emptyResourceNamesYieldsNoRecords() throws Exception {
        JsonNode body = MAPPER.readTree("{\"resourceNames\":[]}");
        assertTrue(x.extract(body).isEmpty());
    }

    @Test
    void missingResourceNamesYieldsNoRecords() throws Exception {
        JsonNode body = MAPPER.readTree("{}");
        assertTrue(x.extract(body).isEmpty());
    }

    @Test
    void nullResponseYieldsNoRecords() {
        assertTrue(x.extract(null).isEmpty());
    }

    @Test
    void resourceWithoutSlashUsesWholeStringAsId() throws Exception {
        JsonNode body = MAPPER.readTree("{\"resourceNames\":[\"plain\"]}");
        assertEquals("plain", x.extract(body).get(0).get("accessible_customer_id"));
    }
}
