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

import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomComponentRegistry;
import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomRecordExtractor;
import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomTransformation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GenericCustomComponentsRegistrarTest {

    private static final String SDM = "source_declarative_manifest.components.";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeEach
    void register() {
        GenericCustomComponentsRegistrar.register();
    }

    // ── registry resolution ─────────────────────────────────────────────────

    @Test
    void allClassNamesResolve() {
        assertTransform(SDM + "CustomFieldTransformation");
        assertTransform(SDM + "TransformEmptyMetrics");
        assertTransform(SDM + "TransformDatetimesToRFC3339");
        assertTransform(SDM + "SanitizeNumericFields");
        assertTransform(SDM + "RemoveEmptyFields");
        assertTransform(SDM + "DateTimeTransformer");
        assertTransform(SDM + "ListAddFields");
        assertExtractor(SDM + "ObjectDpathExtractor");
    }

    @Test
    void registrationIsIdempotent() {
        GenericCustomComponentsRegistrar.register();
        GenericCustomComponentsRegistrar.register();
        assertTransform(SDM + "CustomFieldTransformation");
    }

    // ── CustomFieldTransformation (chargebee) ───────────────────────────────

    @Test
    void chargebeeCustomFieldTransformation_collectsCfKeys() {
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("id", 1);
        record.put("name", "foo");
        record.put("cf_size", "large");
        record.put("cf_color", "blue");

        CustomTransformation t = resolveTransform(SDM + "CustomFieldTransformation");
        Map<String, Object> out = t.transform(record);

        assertNotNull(out);
        assertEquals(1, out.get("id"));
        assertEquals("foo", out.get("name"));
        // cf_ keys must be removed from top level
        assertTrue(!out.containsKey("cf_size"));
        assertTrue(!out.containsKey("cf_color"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> customFields = (List<Map<String, Object>>) out.get("custom_fields");
        assertNotNull(customFields);
        assertEquals(2, customFields.size());
        assertEquals("cf_size", customFields.get(0).get("name"));
        assertEquals("large", customFields.get(0).get("value"));
    }

    @Test
    void chargebeeCustomFieldTransformation_noCustomFields_emptyList() {
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("id", 1);

        CustomTransformation t = resolveTransform(SDM + "CustomFieldTransformation");
        Map<String, Object> out = t.transform(record);

        @SuppressWarnings("unchecked")
        List<?> customFields = (List<?>) out.get("custom_fields");
        assertNotNull(customFields);
        assertTrue(customFields.isEmpty());
    }

    // ── TransformEmptyMetrics ───────────────────────────────────────────────

    @Test
    void transformEmptyMetrics_replacesDashWithNull() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("clicks", "-");
        metrics.put("impressions", "100");
        metrics.put("ctr", "-");
        Map<String, Object> record = new HashMap<>();
        record.put("metrics", metrics);

        CustomTransformation t = resolveTransform(SDM + "TransformEmptyMetrics");
        t.transform(record);

        @SuppressWarnings("unchecked")
        Map<String, Object> m = (Map<String, Object>) record.get("metrics");
        assertEquals(null, m.get("clicks"));
        assertEquals("100", m.get("impressions"));
        assertEquals(null, m.get("ctr"));
    }

    @Test
    void transformEmptyMetrics_noMetricsKey_unchanged() {
        Map<String, Object> record = new HashMap<>();
        record.put("id", 42);

        CustomTransformation t = resolveTransform(SDM + "TransformEmptyMetrics");
        Map<String, Object> out = t.transform(record);

        assertEquals(42, out.get("id"));
    }

    // ── TransformDatetimesToRFC3339 ─────────────────────────────────────────

    @Test
    void transformDatetimesToRFC3339_parsesIsoDatetime() {
        Map<String, Object> record = new HashMap<>();
        record.put("event_time", "2024-06-15 12:30:00");

        CustomTransformation t = resolveTransform(SDM + "TransformDatetimesToRFC3339");
        Map<String, Object> out = t.transform(record);

        String transformed = (String) out.get("event_time");
        assertNotNull(transformed);
        assertTrue(transformed.contains("2024-06-15"), "Should contain date: " + transformed);
    }

    @Test
    void transformDatetimesToRFC3339_nullFieldUntouched() {
        Map<String, Object> record = new HashMap<>();
        record.put("event_time", null);
        record.put("other_field", "keep_me");

        CustomTransformation t = resolveTransform(SDM + "TransformDatetimesToRFC3339");
        Map<String, Object> out = t.transform(record);

        assertEquals(null, out.get("event_time"));
        assertEquals("keep_me", out.get("other_field"));
    }

    // ── SanitizeNumericFields ───────────────────────────────────────────────

    @Test
    void sanitizeNumericFields_coercesStringToNumber() {
        Map<String, Object> record = new HashMap<>();
        record.put("clicks", "42");
        record.put("impressions", "1000");
        record.put("ctr", "0.042");
        record.put("position", "3");

        CustomTransformation t = resolveTransform(SDM + "SanitizeNumericFields");
        Map<String, Object> out = t.transform(record);

        assertEquals(42L, out.get("clicks"));
        assertEquals(1000L, out.get("impressions"));
        assertEquals(0.042, (double) out.get("ctr"), 0.0001);
        assertEquals(3L, out.get("position"));
    }

    @Test
    void sanitizeNumericFields_numberFieldUntouched() {
        Map<String, Object> record = new HashMap<>();
        record.put("clicks", 42L);

        CustomTransformation t = resolveTransform(SDM + "SanitizeNumericFields");
        Map<String, Object> out = t.transform(record);

        assertEquals(42L, out.get("clicks"));
    }

    // ── RemoveEmptyFields ───────────────────────────────────────────────────

    @Test
    void removeEmptyFields_removesNullsFromPointers() {
        Map<String, String> connCfg = Map.of();
        Map<String, Object> params = new HashMap<>();
        params.put("field_pointers", List.of("fields"));

        CustomTransformation t = new RemoveEmptyFields(connCfg, params);

        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("a", "value");
        nested.put("b", null);
        nested.put("c", 42);
        Map<String, Object> record = new HashMap<>();
        record.put("fields", nested);

        Map<String, Object> out = t.transform(record);
        @SuppressWarnings("unchecked")
        Map<String, Object> f = (Map<String, Object>) out.get("fields");

        assertTrue(f.containsKey("a"));
        assertTrue(!f.containsKey("b"), "null key 'b' must be removed");
        assertTrue(f.containsKey("c"));
    }

    @Test
    void removeEmptyFields_multiplePointers() {
        Map<String, Object> params = new HashMap<>();
        params.put("field_pointers", List.of("renderedFields", "fields"));

        CustomTransformation t = new RemoveEmptyFields(Map.of(), params);

        Map<String, Object> rf = new LinkedHashMap<>();
        rf.put("x", null);
        rf.put("y", "ok");
        Map<String, Object> record = new HashMap<>();
        record.put("renderedFields", rf);

        t.transform(record);

        @SuppressWarnings("unchecked")
        Map<String, Object> out = (Map<String, Object>) record.get("renderedFields");
        assertTrue(!out.containsKey("x"));
        assertTrue(out.containsKey("y"));
    }

    // ── DateTimeTransformer ─────────────────────────────────────────────────

    @Test
    void dateTimeTransformer_reformatsDatesInFields() {
        List<Map<String, Object>> fieldSpecs = new ArrayList<>();
        Map<String, Object> spec = new HashMap<>();
        spec.put("path", List.of("date_modified"));
        spec.put("value", "{{ record.date_modified }}");
        fieldSpecs.add(spec);

        Map<String, Object> params = new HashMap<>();
        params.put("fields", fieldSpecs);

        CustomTransformation t = new DateTimeTransformer(Map.of(), params);

        Map<String, Object> record = new HashMap<>();
        record.put("date_modified", "2024-01-15T10:30:00Z");
        record.put("other", "unchanged");

        Map<String, Object> out = t.transform(record);

        String dm = (String) out.get("date_modified");
        assertNotNull(dm);
        assertTrue(dm.startsWith("2024-01-15"), "Date must be preserved: " + dm);
        assertEquals("unchanged", out.get("other"));
    }

    // ── ListAddFields ───────────────────────────────────────────────────────

    @Test
    void listAddFields_extractsIdsFromListField() {
        List<Map<String, Object>> fieldSpecs = new ArrayList<>();
        Map<String, Object> spec = new HashMap<>();
        spec.put("path", List.of("updates_ids"));
        spec.put("value", "{{ record['updates'] }}");
        fieldSpecs.add(spec);

        Map<String, Object> params = new HashMap<>();
        params.put("fields", fieldSpecs);

        CustomTransformation t = new ListAddFields(Map.of(), params);

        List<Map<String, Object>> updates = new ArrayList<>();
        Map<String, Object> u1 = new HashMap<>();
        u1.put("id", "uuid-1");
        u1.put("body", "text1");
        updates.add(u1);
        Map<String, Object> u2 = new HashMap<>();
        u2.put("id", "uuid-2");
        u2.put("body", "text2");
        updates.add(u2);

        Map<String, Object> record = new HashMap<>();
        record.put("updates", updates);
        record.put("name", "incident1");

        Map<String, Object> out = t.transform(record);

        @SuppressWarnings("unchecked")
        List<Object> ids = (List<Object>) out.get("updates_ids");
        assertNotNull(ids);
        assertEquals(2, ids.size());
        assertEquals("uuid-1", ids.get(0));
        assertEquals("uuid-2", ids.get(1));
    }

    @Test
    void listAddFields_dotNotationFieldAccess() {
        List<Map<String, Object>> fieldSpecs = new ArrayList<>();
        Map<String, Object> spec = new HashMap<>();
        spec.put("path", List.of("ids"));
        spec.put("value", "{{ record.items }}");
        fieldSpecs.add(spec);

        Map<String, Object> params = new HashMap<>();
        params.put("fields", fieldSpecs);

        CustomTransformation t = new ListAddFields(Map.of(), params);

        List<Map<String, Object>> items = List.of(
            Map.of("id", 1, "name", "a"),
            Map.of("id", 2, "name", "b")
        );
        Map<String, Object> record = new HashMap<>();
        record.put("items", items);

        t.transform(record);

        @SuppressWarnings("unchecked")
        List<Object> ids = (List<Object>) record.get("ids");
        assertEquals(List.of(1, 2), ids);
    }

    // ── ObjectDpathExtractor ────────────────────────────────────────────────

    @Test
    void objectDpathExtractor_emitsOneRecordPerKey() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("field_path", List.of("Time Series"));
        params.put("inject_key_as_field", "timestamp");

        CustomRecordExtractor extractor = new ObjectDpathExtractor(Map.of(), params);

        String json = "{"
            + "\"Time Series\": {"
            + "  \"2024-01-01 12:00:00\": {\"1. open\": \"150.00\", \"2. high\": \"155.00\"},"
            + "  \"2024-01-01 13:00:00\": {\"1. open\": \"151.00\", \"2. high\": \"156.00\"}"
            + "}}";
        JsonNode response = MAPPER.readTree(json);

        List<Map<String, Object>> records = extractor.extract(response);

        assertEquals(2, records.size());
        for (Map<String, Object> rec : records) {
            assertNotNull(rec.get("timestamp"), "Must have injected timestamp field");
            assertNotNull(rec.get("1. open"));
        }
    }

    @Test
    void objectDpathExtractor_nullResponse_returnsEmpty() {
        Map<String, Object> params = new HashMap<>();
        params.put("field_path", List.of("data"));
        params.put("inject_key_as_field", "key");

        CustomRecordExtractor extractor = new ObjectDpathExtractor(Map.of(), params);
        assertTrue(extractor.extract(null).isEmpty());
    }

    @Test
    void objectDpathExtractor_missingPath_returnsEmpty() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("field_path", List.of("nonexistent"));
        params.put("inject_key_as_field", "key");

        CustomRecordExtractor extractor = new ObjectDpathExtractor(Map.of(), params);
        JsonNode response = MAPPER.readTree("{\"data\": {}}");

        assertTrue(extractor.extract(response).isEmpty());
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private static void assertTransform(String className) {
        CustomTransformation t = resolveTransform(className);
        assertNotNull(t, className + " must resolve to a CustomTransformation");
    }

    private static void assertExtractor(String className) {
        CustomRecordExtractor e = CustomComponentRegistry.create(
            className, CustomRecordExtractor.class, Map.of(), Map.of());
        assertNotNull(e, className + " must resolve to a CustomRecordExtractor");
    }

    private static CustomTransformation resolveTransform(String className) {
        return CustomComponentRegistry.create(
            className, CustomTransformation.class, Map.of(), Map.of());
    }
}
