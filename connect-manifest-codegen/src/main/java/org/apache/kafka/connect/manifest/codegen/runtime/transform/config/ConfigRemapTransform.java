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
package org.apache.kafka.connect.manifest.codegen.runtime.transform.config;

import org.apache.kafka.connect.manifest.codegen.runtime.jinja.JinjaRenderer;
import org.apache.kafka.connect.manifest.codegen.runtime.transform.DPath;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Java port of Airbyte's {@code config_transformations/remap_field.py ConfigRemapField}.
 * Navigates to {@code field_path} (Jinja-templated segments), looks up the current
 * value in a static {@code map: Dict[str,str]}, and replaces it if a hit is found.
 * Silently no-ops on missing path or unmapped value.
 */
public final class ConfigRemapTransform implements ConfigTransformation {

    private final List<String> fieldPathTemplates;
    private final Map<String, String> valueMap;

    public ConfigRemapTransform(List<String> fieldPathTemplates, Map<String, String> valueMap) {
        this.fieldPathTemplates = fieldPathTemplates == null ? List.of() : fieldPathTemplates;
        this.valueMap = valueMap == null ? Map.of() : valueMap;
    }

    @Override
    public void apply(Map<String, Object> config) {
        if (fieldPathTemplates.isEmpty() || valueMap.isEmpty()) {
            return;
        }
        Map<String, Object> ctx = Map.of("config", config);

        List<String> resolvedPath = new ArrayList<>(fieldPathTemplates.size());
        for (String seg : fieldPathTemplates) {
            resolvedPath.add(JinjaRenderer.renderLenient(seg, ctx));
        }

        Object current = DPath.navigate(config, resolvedPath);
        if (current == null) {
            return;
        }
        String currentStr = current.toString();
        String mapped = valueMap.get(currentStr);
        if (mapped == null) {
            return;
        }
        DPath.set(config, resolvedPath, mapped);
    }
}
