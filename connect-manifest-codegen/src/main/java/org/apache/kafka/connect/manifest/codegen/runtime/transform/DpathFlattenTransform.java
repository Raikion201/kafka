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

import org.apache.kafka.connect.manifest.codegen.model.KeyTransformationSpec;
import org.apache.kafka.connect.manifest.codegen.runtime.jinja.JinjaRenderer;

import java.util.List;
import java.util.Map;

/**
 * Java port of Airbyte's {@code dpath_flatten_fields.py DpathFlattenFields}.
 * Expands a dpath with optional {@code *} wildcard into concrete paths, then for each match
 * lifts the child map's entries into the parent, with optional prefix/suffix via
 * {@code KeyTransformation}. Flags: {@code delete_origin_value}, {@code replace_record}.
 */
public final class DpathFlattenTransform implements RecordTransformation {

    private final List<String> fieldPath;
    private final boolean deleteOriginValue;
    private final boolean replaceRecord;
    private final KeyTransformationSpec keyTransformation;

    public DpathFlattenTransform(List<String> fieldPath,
                                  boolean deleteOriginValue,
                                  boolean replaceRecord,
                                  KeyTransformationSpec keyTransformation) {
        this.fieldPath = fieldPath == null ? List.of() : fieldPath;
        this.deleteOriginValue = deleteOriginValue;
        this.replaceRecord = replaceRecord;
        this.keyTransformation = keyTransformation;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void apply(Map<String, Object> record, Map<String, Object> ctx) {
        if (fieldPath.isEmpty()) {
            return;
        }
        List<DPath.Match> matches = DPath.expandWildcards(record, fieldPath);
        for (DPath.Match match : matches) {
            Object value = match.value();
            if (!(value instanceof Map)) {
                continue;
            }
            Map<String, Object> nested = (Map<String, Object>) value;
            Map<String, Object> target = replaceRecord ? record : getParent(record, match.path());
            if (target == null) {
                continue;
            }
            for (Map.Entry<String, Object> e : nested.entrySet()) {
                String key = applyKeyTransformation(e.getKey(), ctx);
                target.put(key, e.getValue());
            }
            if (deleteOriginValue) {
                DPath.delete(record, match.path());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getParent(Map<String, Object> record, List<String> path) {
        if (path.size() <= 1) {
            return record;
        }
        Object cur = record;
        for (int i = 0; i < path.size() - 1; i++) {
            if (!(cur instanceof Map)) {
                return null;
            }
            cur = ((Map<String, Object>) cur).get(path.get(i));
        }
        return (cur instanceof Map) ? (Map<String, Object>) cur : null;
    }

    private String applyKeyTransformation(String key, Map<String, Object> ctx) {
        if (keyTransformation == null) {
            return key;
        }
        String prefix = keyTransformation.getPrefix() != null
            ? JinjaRenderer.renderLenient(keyTransformation.getPrefix(), ctx) : "";
        String suffix = keyTransformation.getSuffix() != null
            ? JinjaRenderer.renderLenient(keyTransformation.getSuffix(), ctx) : "";
        return prefix + key + suffix;
    }
}
