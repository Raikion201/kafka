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
package org.apache.kafka.connect.manifest.codegen.runtime.transform;

import org.apache.kafka.connect.manifest.codegen.runtime.jinja.JinjaRenderer;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Java port of Airbyte's {@code keys_replace_transformation.py KeysReplaceTransformation}.
 * Recursively replaces occurrences of {@code old} in every map key with {@code new}.
 * Both {@code old} and {@code new} are Jinja-templated and rendered once per record.
 */
public final class KeysReplaceTransform implements RecordTransformation {

    private final String oldTemplate;
    private final String newTemplate;

    public KeysReplaceTransform(String oldTemplate, String newTemplate) {
        this.oldTemplate = oldTemplate == null ? "" : oldTemplate;
        this.newTemplate = newTemplate == null ? "" : newTemplate;
    }

    @Override
    public void apply(Map<String, Object> record, Map<String, Object> ctx) {
        String oldStr = JinjaRenderer.renderLenient(oldTemplate, ctx);
        String newStr = JinjaRenderer.renderLenient(newTemplate, ctx);
        if (oldStr.isEmpty()) {
            return;
        }
        replaceKeys(record, oldStr, newStr);
    }

    @SuppressWarnings("unchecked")
    private static void replaceKeys(Map<String, Object> map, String oldStr, String newStr) {
        Map<String, Object> replaced = new LinkedHashMap<>(map.size());
        for (Map.Entry<String, Object> e : map.entrySet()) {
            String newKey = e.getKey().replace(oldStr, newStr);
            Object value = e.getValue();
            if (value instanceof Map) {
                replaceKeys((Map<String, Object>) value, oldStr, newStr);
            }
            replaced.put(newKey, value);
        }
        map.clear();
        map.putAll(replaced);
    }
}
