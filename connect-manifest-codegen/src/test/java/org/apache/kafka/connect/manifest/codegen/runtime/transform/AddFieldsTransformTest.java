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
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class AddFieldsTransformTest {

    private static AddedFieldSpec field(List<String> path, String value, String valueType) {
        AddedFieldSpec s = new AddedFieldSpec();
        s.setPath(path);
        s.setValue(value);
        s.setValueType(valueType);
        return s;
    }

    private static Map<String, Object> ctx(Map<String, Object> record) {
        Map<String, Object> ctx = new LinkedHashMap<>();
        ctx.put("record", record);
        ctx.put("config", Map.of());
        return ctx;
    }

    @Test
    void apply_addsTopLevelField() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("id", 1);
        AddFieldsTransform t = new AddFieldsTransform(
            List.of(field(List.of("label"), "hello", null)), null);
        t.apply(rec, ctx(rec));
        assertEquals("hello", rec.get("label"));
    }

    @Test
    void apply_jinjaFromRecord() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("id", 42);
        Map<String, Object> ctx = ctx(rec);
        AddFieldsTransform t = new AddFieldsTransform(
            List.of(field(List.of("computed"), "{{ record['id'] }}", "integer")), null);
        t.apply(rec, ctx);
        assertEquals(42L, rec.get("computed"));
    }

    @Test
    void apply_nestedPath() {
        Map<String, Object> rec = new LinkedHashMap<>();
        AddFieldsTransform t = new AddFieldsTransform(
            List.of(field(List.of("a", "b"), "v", null)), null);
        t.apply(rec, ctx(rec));
        @SuppressWarnings("unchecked")
        Map<String, Object> a = (Map<String, Object>) rec.get("a");
        assertEquals("v", a.get("b"));
    }

    @Test
    void apply_conditionFalse_skips() {
        Map<String, Object> rec = new LinkedHashMap<>();
        AddFieldsTransform t = new AddFieldsTransform(
            List.of(field(List.of("x"), "val", null)), "{{ false }}");
        t.apply(rec, ctx(rec));
        assertNull(rec.get("x"));
    }

    @Test
    void apply_conditionTrue_applies() {
        Map<String, Object> rec = new LinkedHashMap<>();
        AddFieldsTransform t = new AddFieldsTransform(
            List.of(field(List.of("x"), "val", null)), "{{ true }}");
        t.apply(rec, ctx(rec));
        assertEquals("val", rec.get("x"));
    }

    @Test
    void coerce_integer() {
        assertEquals(7L, AddFieldsTransform.coerce("7", "integer"));
    }

    @Test
    void coerce_number() {
        assertEquals(3.14, AddFieldsTransform.coerce("3.14", "number"));
    }

    @Test
    void coerce_boolean_truthy() {
        assertEquals(Boolean.TRUE, AddFieldsTransform.coerce("true", "boolean"));
        assertEquals(Boolean.FALSE, AddFieldsTransform.coerce("false", "boolean"));
        assertEquals(Boolean.FALSE, AddFieldsTransform.coerce("0", "boolean"));
    }

    @Test
    void coerce_badNumber_fallsThrough() {
        assertEquals("abc", AddFieldsTransform.coerce("abc", "integer"));
    }

    @Test
    void apply_emptyFieldList_noop() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("k", "v");
        new AddFieldsTransform(List.of(), null).apply(rec, ctx(rec));
        assertEquals(1, rec.size());
    }
}
