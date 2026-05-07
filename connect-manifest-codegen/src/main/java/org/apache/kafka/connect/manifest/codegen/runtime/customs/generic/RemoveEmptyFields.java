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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Java port of {@code source-jira/components.py RemoveEmptyFields} (lines 75–107).
 *
 * <p>For each key named in {@code field_pointers}, if the value is a {@link Map}, filters
 * out all entries whose value is {@code null}. Applied by Jira to the {@code fields} and
 * {@code renderedFields} sub-objects, which the API populates sparsely.</p>
 *
 * <p>Registered under {@code source_declarative_manifest.components.RemoveEmptyFields}.</p>
 */
public final class RemoveEmptyFields implements CustomTransformation {

    private final List<String> fieldPointers;

    @SuppressWarnings("unchecked")
    public RemoveEmptyFields(Map<String, String> connectorConfig,
                             Map<String, Object> componentParams) {
        Object raw = componentParams == null ? null : componentParams.get("field_pointers");
        if (raw instanceof List) {
            fieldPointers = (List<String>) raw;
        } else {
            fieldPointers = List.of();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> transform(Map<String, Object> record) {
        if (record == null) {
            return null;
        }
        for (String pointer : fieldPointers) {
            Object nested = record.get(pointer);
            if (!(nested instanceof Map)) {
                continue;
            }
            Map<String, Object> nestedMap = (Map<String, Object>) nested;
            Map<String, Object> filtered = new LinkedHashMap<>(nestedMap.size());
            for (Map.Entry<String, Object> e : nestedMap.entrySet()) {
                if (e.getValue() != null) {
                    filtered.put(e.getKey(), e.getValue());
                }
            }
            record.put(pointer, filtered);
        }
        return record;
    }
}
