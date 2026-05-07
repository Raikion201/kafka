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

import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomTransformation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Java port of {@code source-notion/components.py NotionPropertiesTransformation}.
 *
 * <p>Converts the Notion API's flat property dictionary into a normalized list.
 * Input: {@code {"properties": {"Title": {...}, "Status": {...}}}}
 * Output: {@code {"properties": [{"name": "Title", "value": {...}}, {"name": "Status", "value": {...}}]}}
 *
 * <p>Registered under {@code source_declarative_manifest.components.NotionPropertiesTransformation}.
 */
public final class NotionPropertiesTransformation implements CustomTransformation {

    public NotionPropertiesTransformation(Map<String, String> connectorConfig,
                                          Map<String, Object> componentParams) {
        // no parameters
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> transform(Map<String, Object> record) {
        if (record == null) {
            return null;
        }
        Object raw = record.get("properties");
        if (!(raw instanceof Map)) {
            return record;
        }
        Map<String, Object> properties = (Map<String, Object>) raw;
        List<Map<String, Object>> transformed = new ArrayList<>(properties.size());
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            Map<String, Object> item = new LinkedHashMap<>(2);
            item.put("name", entry.getKey());
            item.put("value", entry.getValue());
            transformed.add(item);
        }
        record.put("properties", transformed);
        return record;
    }
}
