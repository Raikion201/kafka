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

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RemoveFieldsTransformTest {

    private static Map<String, Object> ctx(Map<String, Object> record) {
        Map<String, Object> ctx = new LinkedHashMap<>();
        ctx.put("record", record);
        ctx.put("config", Map.of());
        return ctx;
    }

    @Test
    void apply_removesTopLevel() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("a", 1);
        rec.put("b", 2);
        new RemoveFieldsTransform(List.of(List.of("a")), null).apply(rec, ctx(rec));
        assertFalse(rec.containsKey("a"));
        assertTrue(rec.containsKey("b"));
    }

    @Test
    void apply_removesNested() {
        Map<String, Object> rec = new LinkedHashMap<>();
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("secret", "x");
        nested.put("keep", "y");
        rec.put("data", nested);
        new RemoveFieldsTransform(List.of(List.of("data", "secret")), null).apply(rec, ctx(rec));
        assertFalse(nested.containsKey("secret"));
        assertTrue(nested.containsKey("keep"));
    }

    @Test
    void apply_missingPath_noThrow() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("a", 1);
        new RemoveFieldsTransform(List.of(List.of("missing")), null).apply(rec, ctx(rec));
        assertEquals(1, rec.get("a"));
    }

    @Test
    void apply_conditionFalse_skips() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("a", 1);
        new RemoveFieldsTransform(List.of(List.of("a")), "{{ false }}").apply(rec, ctx(rec));
        assertTrue(rec.containsKey("a"));
    }

    @Test
    void apply_multiplePointers() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("a", 1);
        rec.put("b", 2);
        rec.put("c", 3);
        new RemoveFieldsTransform(List.of(List.of("a"), List.of("c")), null).apply(rec, ctx(rec));
        assertFalse(rec.containsKey("a"));
        assertTrue(rec.containsKey("b"));
        assertFalse(rec.containsKey("c"));
    }
}
