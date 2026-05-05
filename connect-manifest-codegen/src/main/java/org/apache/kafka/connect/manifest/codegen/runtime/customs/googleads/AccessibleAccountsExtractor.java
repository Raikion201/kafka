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

import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomRecordExtractor;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Java port of {@code source_google_ads.components.AccessibleAccountsExtractor}
 * (components.py:104-114).
 *
 * <p>The {@code listAccessibleCustomers} endpoint returns a body shaped like
 * {@code {"resourceNames": ["customers/1234", "customers/5678"]}}. Python yields one
 * record per resource name with the customer id as the last path segment.</p>
 */
public final class AccessibleAccountsExtractor implements CustomRecordExtractor {

    public AccessibleAccountsExtractor(Map<String, String> connectorConfig,
                                       Map<String, Object> componentParams) {
        // No configurable state.
    }

    @Override
    public List<Map<String, Object>> extract(JsonNode response) {
        if (response == null || response.isNull()) {
            return Collections.emptyList();
        }
        JsonNode names = response.get("resourceNames");
        if (names == null || !names.isArray() || names.isEmpty()) {
            return Collections.emptyList();
        }

        List<Map<String, Object>> out = new ArrayList<>(names.size());
        for (JsonNode n : names) {
            if (n == null || n.isNull() || !n.isTextual()) {
                continue;
            }
            String resource = n.asText();
            int slash = resource.lastIndexOf('/');
            String id = slash < 0 ? resource : resource.substring(slash + 1);
            Map<String, Object> rec = new LinkedHashMap<>(1);
            rec.put("accessible_customer_id", id);
            out.add(rec);
        }
        return out;
    }
}
