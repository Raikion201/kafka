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

import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomTransformation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Java port of {@code source_google_ads.components.FlattenNestedDictsTransformation}
 * (components.py:151-185).
 *
 * <p>Recursively flattens top-level dict-valued fields into dot-joined keys, but does
 * NOT descend into dicts nested inside lists. Example:</p>
 * <pre>
 *   {"a": {"b": 1, "c": {"d": 2}, "e": [{"f": 3}]}, "g": [{"h": 4}]}
 * becomes
 *   {"a.b": 1, "a.c.d": 2, "a.e": [{"f": 3}], "g": [{"h": 4}]}
 * </pre>
 */
public final class FlattenNestedDictsTransformation implements CustomTransformation {

    private final String delimiter;

    public FlattenNestedDictsTransformation(Map<String, String> connectorConfig,
                                            Map<String, Object> componentParams) {
        Object d = componentParams == null ? null : componentParams.get("delimiter");
        this.delimiter = d == null ? "." : d.toString();
    }

    @Override
    public Map<String, Object> transform(Map<String, Object> record) {
        if (record == null) {
            return null;
        }
        Map<String, Object> out = new LinkedHashMap<>(record);

        // Snapshot top-level dict keys (Python: list comprehension over record.items()).
        List<String> dictKeys = new ArrayList<>();
        for (Map.Entry<String, Object> e : out.entrySet()) {
            if (e.getValue() instanceof Map) {
                dictKeys.add(e.getKey());
            }
        }
        for (String k : dictKeys) {
            @SuppressWarnings("unchecked")
            Map<String, Object> nested = (Map<String, Object>) out.remove(k);
            flattenInto(out, k, nested);
        }
        return out;
    }

    private void flattenInto(Map<String, Object> out, String prefix, Map<String, Object> obj) {
        for (Map.Entry<String, Object> e : obj.entrySet()) {
            String newKey = prefix.isEmpty() ? e.getKey() : prefix + delimiter + e.getKey();
            Object value = e.getValue();
            if (value instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> nested = (Map<String, Object>) value;
                flattenInto(out, newKey, nested);
            } else {
                out.put(newKey, value);
            }
        }
    }
}
