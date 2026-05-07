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
 * Java port of {@code source-chargebee/components.py CustomFieldTransformation} (lines 12–48).
 *
 * <p>Chargebee returns custom fields prefixed with {@code "cf_"} as top-level record keys.
 * This transform collects them into {@code record["custom_fields"]} as a list of
 * {@code {"name": key, "value": value}} objects and removes the original keys.</p>
 *
 * <p>Registered under {@code source_declarative_manifest.components.CustomFieldTransformation}.</p>
 */
public final class ChargebeeCustomFieldTransformation implements CustomTransformation {

    @SuppressWarnings("unused")
    public ChargebeeCustomFieldTransformation(Map<String, String> connectorConfig,
                                              Map<String, Object> componentParams) {
    }

    @Override
    public Map<String, Object> transform(Map<String, Object> record) {
        if (record == null) {
            return null;
        }
        List<String> cfKeys = new ArrayList<>();
        for (String key : record.keySet()) {
            if (key.startsWith("cf_")) {
                cfKeys.add(key);
            }
        }
        List<Map<String, Object>> customFields = new ArrayList<>(cfKeys.size());
        for (String key : cfKeys) {
            Object value = record.remove(key);
            Map<String, Object> entry = new LinkedHashMap<>(2);
            entry.put("name", key);
            entry.put("value", value);
            customFields.add(entry);
        }
        record.put("custom_fields", customFields);
        return record;
    }
}
