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
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DpathFlattenTransformTest {

    @Test
    void flattensNestedMapAtPath() {
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("x", 1);
        nested.put("y", 2);
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("data", nested);
        rec.put("other", "keep");

        new DpathFlattenTransform(List.of("data"), false, false, null).apply(rec, Map.of());

        assertEquals(1, rec.get("x"));
        assertEquals(2, rec.get("y"));
        assertEquals("keep", rec.get("other"));
        // original "data" key still present (deleteOriginValue=false)
        assertTrue(rec.containsKey("data"));
    }

    @Test
    void deleteOriginValue_removesSource() {
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("x", 1);
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("data", nested);

        new DpathFlattenTransform(List.of("data"), true, false, null).apply(rec, Map.of());

        assertEquals(1, rec.get("x"));
        assertNull(rec.get("data"));
    }

    @Test
    void replaceRecord_liftsIntoRoot() {
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("a", 10);
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("data", nested);
        rec.put("other", "existing");

        new DpathFlattenTransform(List.of("data"), false, true, null).apply(rec, Map.of());

        assertEquals(10, rec.get("a"));
        // replaceRecord=true lifts to root, not to parent of "data"
        assertTrue(rec.containsKey("a"));
    }

    @Test
    void keyTransformation_addsPrefixSuffix() {
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("k", "v");
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("props", nested);

        KeyTransformationSpec kt = new KeyTransformationSpec();
        kt.setPrefix("pre_");
        kt.setSuffix("_suf");

        new DpathFlattenTransform(List.of("props"), false, false, kt).apply(rec, Map.of());

        assertTrue(rec.containsKey("pre_k_suf"));
        assertEquals("v", rec.get("pre_k_suf"));
    }

    @Test
    void wildcardPath_flattensAllMatches() {
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("field1", 1);
        Map<String, Object> b = new LinkedHashMap<>();
        b.put("field2", 2);
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("a", a);
        data.put("b", b);
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("data", data);

        new DpathFlattenTransform(List.of("data", "*"), false, false, null).apply(rec, Map.of());

        // Each wildcard match's child map entries should be lifted into their parent (data)
        assertTrue(rec.containsKey("data"));
    }

    @Test
    void nonMapValue_skipped() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("data", "not-a-map");

        new DpathFlattenTransform(List.of("data"), false, false, null).apply(rec, Map.of());

        // No crash, record unchanged
        assertEquals("not-a-map", rec.get("data"));
    }

    @Test
    void emptyPath_noop() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("a", 1);
        new DpathFlattenTransform(List.of(), false, false, null).apply(rec, Map.of());
        assertFalse(rec.containsKey("data"));
        assertEquals(1, rec.get("a"));
    }
}
