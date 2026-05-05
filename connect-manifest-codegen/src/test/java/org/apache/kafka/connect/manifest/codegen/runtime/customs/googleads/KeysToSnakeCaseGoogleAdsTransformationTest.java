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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeysToSnakeCaseGoogleAdsTransformationTest {

    private final KeysToSnakeCaseGoogleAdsTransformation t =
        new KeysToSnakeCaseGoogleAdsTransformation(Collections.emptyMap(), Collections.emptyMap());

    @Test
    void camelCaseKeyBecomesSnakeCase() {
        assertEquals("client_customer", t.processKey("clientCustomer"));
        assertEquals("ad_group_id", t.processKey("adGroupId"));
    }

    @Test
    void pascalCaseKeyBecomesSnakeCase() {
        assertEquals("ad_group_id", t.processKey("AdGroupId"));
    }

    @Test
    void digitsDoNotInsertUnderscore() {
        // Python "doesn't add underscore before digits": camel transitions split tokens but
        // digits adjacent to letters within a single case-run stay glued to that token.
        assertEquals("name2", t.processKey("name2"));
        // 'campaignV2' → ['campaign', 'V2'] → 'campaign_v2' (no underscore between V and 2).
        assertEquals("campaign_v2", t.processKey("campaignV2"));
    }

    @Test
    void alreadySnakeCasePassesThroughLowercased() {
        assertEquals("snake_case_key", t.processKey("snake_case_key"));
    }

    @Test
    void emptyAndNullKeyReturned() {
        assertEquals("", t.processKey(""));
        assertNull(t.processKey(null));
    }

    @Test
    void recursesIntoNestedDicts() {
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("innerKey", 1);

        Map<String, Object> record = new LinkedHashMap<>();
        record.put("outerKey", nested);

        Map<String, Object> out = t.transform(record);
        assertTrue(out.containsKey("outer_key"));
        @SuppressWarnings("unchecked")
        Map<String, Object> nestedOut = (Map<String, Object>) out.get("outer_key");
        assertTrue(nestedOut.containsKey("inner_key"));
    }

    @Test
    void nullRecordReturnsNull() {
        assertNull(t.transform(null));
    }

    @Test
    void emptyRecordReturnsEmpty() {
        assertEquals(Collections.emptyMap(), t.transform(new LinkedHashMap<>()));
    }
}
