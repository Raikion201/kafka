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

class FlattenFieldsTransformTest {

    @Test
    void flattensNestedMap() {
        Map<String, Object> rec = new LinkedHashMap<>();
        Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("b", 2);
        rec.put("a", inner);
        rec.put("c", 3);
        new FlattenFieldsTransform(false).apply(rec, Map.of());
        assertFalse(rec.containsKey("a"));
        assertEquals(2, rec.get("a.b"));
        assertEquals(3, rec.get("c"));
    }

    @Test
    void flattenLists_true_indexesListElements() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("arr", List.of("x", "y"));
        new FlattenFieldsTransform(true).apply(rec, Map.of());
        assertFalse(rec.containsKey("arr"));
        assertEquals("x", rec.get("arr.0"));
        assertEquals("y", rec.get("arr.1"));
    }

    @Test
    void flattenLists_false_leavesListsAsIs() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("arr", List.of("x", "y"));
        new FlattenFieldsTransform(false).apply(rec, Map.of());
        assertTrue(rec.containsKey("arr"));
    }

    @Test
    void deeplyNested_dotNotation() {
        Map<String, Object> rec = new LinkedHashMap<>();
        Map<String, Object> b = new LinkedHashMap<>();
        Map<String, Object> c = new LinkedHashMap<>();
        c.put("d", "leaf");
        b.put("c", c);
        rec.put("a", b);
        new FlattenFieldsTransform(false).apply(rec, Map.of());
        assertEquals("leaf", rec.get("a.c.d"));
    }

    @Test
    void emptyMap_noop() {
        Map<String, Object> rec = new LinkedHashMap<>();
        new FlattenFieldsTransform(false).apply(rec, Map.of());
        assertTrue(rec.isEmpty());
    }
}
