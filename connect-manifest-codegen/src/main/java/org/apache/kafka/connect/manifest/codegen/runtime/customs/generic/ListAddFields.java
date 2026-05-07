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
import org.apache.kafka.connect.manifest.codegen.runtime.transform.DPath;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Java port of {@code source-instatus/components.py ListAddFields} (lines 16–64).
 *
 * <p>Extends the standard {@code AddFields} semantic by treating the evaluated {@code value}
 * expression as a list of objects, extracting the {@code "id"} from each, and writing the
 * resulting id-list to {@code path} in the record.</p>
 *
 * <p>The Airbyte CDK evaluates the Jinja {@code value} expression as a Python object (a real
 * list). Java's Jinja renderer produces a string. To avoid lossy string serialization, this
 * implementation detects the common {@code {{ record['field'] }}} / {@code {{ record.field }}}
 * pattern and reads the raw Java object directly from the record map. Other expressions fall
 * back to a no-op for that field.</p>
 *
 * <p>Registered under {@code source_declarative_manifest.components.ListAddFields}.</p>
 */
public final class ListAddFields implements CustomTransformation {

    /**
     * Matches simple {@code {{ record['f'] }}}, {@code {{ record["f"] }}}, or
     * {@code {{ record.f }}} patterns and captures the field name.
     */
    private static final Pattern SIMPLE_RECORD_ACCESS = Pattern.compile(
        "^\\{\\{\\s*record(?:\\['([^']+)'\\]|\\[\"([^\"]+)\"\\]|\\.([a-zA-Z_][a-zA-Z0-9_]*))\\s*\\}\\}$"
    );

    private final List<FieldSpec> fields;

    @SuppressWarnings("unchecked")
    public ListAddFields(Map<String, String> connectorConfig,
                         Map<String, Object> componentParams) {
        Object raw = componentParams == null ? null : componentParams.get("fields");
        List<FieldSpec> parsed = new ArrayList<>();
        if (raw instanceof List) {
            for (Object item : (List<?>) raw) {
                if (!(item instanceof Map)) {
                    continue;
                }
                Map<String, Object> m = (Map<String, Object>) item;
                Object pathObj = m.get("path");
                Object valueObj = m.get("value");
                if (pathObj instanceof List && valueObj instanceof String) {
                    parsed.add(new FieldSpec((List<String>) pathObj, (String) valueObj));
                }
            }
        }
        this.fields = parsed;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> transform(Map<String, Object> record) {
        if (record == null) {
            return null;
        }
        for (FieldSpec f : fields) {
            String sourceField = extractFieldName(f.value);
            if (sourceField == null) {
                continue;
            }
            Object raw = record.get(sourceField);
            if (!(raw instanceof List)) {
                continue;
            }
            List<Object> ids = ((List<?>) raw).stream()
                .filter(item -> item instanceof Map)
                .map(item -> ((Map<?, ?>) item).get("id"))
                .collect(Collectors.toList());
            DPath.set(record, f.path, ids);
        }
        return record;
    }

    private static String extractFieldName(String valueExpr) {
        Matcher m = SIMPLE_RECORD_ACCESS.matcher(valueExpr.trim());
        if (!m.matches()) {
            return null;
        }
        // groups 1, 2, 3 correspond to single-quoted, double-quoted, and dotted access
        for (int i = 1; i <= 3; i++) {
            String g = m.group(i);
            if (g != null) {
                return g;
            }
        }
        return null;
    }

    private static final class FieldSpec {
        final List<String> path;
        final String value;

        FieldSpec(List<String> path, String value) {
            this.path = path;
            this.value = value;
        }
    }
}
