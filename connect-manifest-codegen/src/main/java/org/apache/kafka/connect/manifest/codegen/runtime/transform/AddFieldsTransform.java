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

import org.apache.kafka.connect.manifest.codegen.model.AddedFieldSpec;
import org.apache.kafka.connect.manifest.codegen.runtime.jinja.JinjaRenderer;

import java.util.List;
import java.util.Map;

/**
 * Java port of Airbyte's {@code transformations/add_fields.py AddFields}.
 * For each field definition, renders the Jinja value template in the record context,
 * coerces to the declared {@code value_type}, then writes to the record at the given
 * dpath path (mkdir-p semantics via {@link DPath#set}).
 * An optional outer {@code condition} gates the whole transform.
 */
public final class AddFieldsTransform implements RecordTransformation {

    private final List<AddedFieldSpec> fields;
    private final String condition;

    public AddFieldsTransform(List<AddedFieldSpec> fields, String condition) {
        this.fields = fields == null ? List.of() : fields;
        this.condition = condition;
    }

    @Override
    public void apply(Map<String, Object> record, Map<String, Object> ctx) {
        if (condition != null && !condition.isBlank() && !JinjaTruth.evaluate(condition, ctx)) {
            return;
        }
        for (AddedFieldSpec field : fields) {
            if (field.getPath().isEmpty()) {
                continue;
            }
            String rendered = JinjaRenderer.renderLenient(field.getValue(), ctx);
            Object coerced = coerce(rendered, field.getValueType());
            DPath.set(record, field.getPath(), coerced);
        }
    }

    static Object coerce(String rendered, String valueType) {
        if (valueType == null || valueType.isBlank() || "string".equalsIgnoreCase(valueType)) {
            return rendered;
        }
        try {
            switch (valueType.toLowerCase(java.util.Locale.ROOT)) {
                case "integer": return Long.parseLong(rendered.strip());
                case "number":  return Double.parseDouble(rendered.strip());
                case "boolean": return JinjaTruth.isTruthy(rendered);
                default:        return rendered;
            }
        } catch (NumberFormatException e) {
            return rendered;
        }
    }
}
