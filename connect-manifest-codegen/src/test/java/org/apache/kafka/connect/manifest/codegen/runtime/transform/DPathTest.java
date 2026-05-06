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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DPathTest {

    @Test
    void set_topLevel() {
        Map<String, Object> rec = new LinkedHashMap<>();
        DPath.set(rec, List.of("a"), 1);
        assertEquals(1, rec.get("a"));
    }

    @Test
    void set_nestedCreatesIntermediateMaps() {
        Map<String, Object> rec = new LinkedHashMap<>();
        DPath.set(rec, List.of("a", "b", "c"), "v");
        @SuppressWarnings("unchecked")
        Map<String, Object> a = (Map<String, Object>) rec.get("a");
        @SuppressWarnings("unchecked")
        Map<String, Object> b = (Map<String, Object>) a.get("b");
        assertEquals("v", b.get("c"));
    }

    @Test
    void set_overwritesExisting() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("a", 1);
        DPath.set(rec, List.of("a"), 2);
        assertEquals(2, rec.get("a"));
    }

    @Test
    void set_extendsArrayWithNullsLikePythonDpath() {
        // Docstring example from add_fields.py: setting index 5 on ["value"] →
        // ["value", null, null, null, null, "new_value"].
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("arr", new ArrayList<>(List.of("value")));
        DPath.set(rec, List.of("arr", "5"), "new_value");
        @SuppressWarnings("unchecked")
        List<Object> arr = (List<Object>) rec.get("arr");
        assertEquals(6, arr.size());
        assertEquals("value", arr.get(0));
        for (int i = 1; i <= 4; i++) {
            assertNull(arr.get(i));
        }
        assertEquals("new_value", arr.get(5));
    }

    @Test
    void set_createsArrayWhenNextSegmentIsIndex() {
        Map<String, Object> rec = new LinkedHashMap<>();
        DPath.set(rec, List.of("items", "0", "name"), "first");
        @SuppressWarnings("unchecked")
        List<Object> items = (List<Object>) rec.get("items");
        @SuppressWarnings("unchecked")
        Map<String, Object> first = (Map<String, Object>) items.get(0);
        assertEquals("first", first.get("name"));
    }

    @Test
    void set_emptyPathThrows() {
        Map<String, Object> rec = new LinkedHashMap<>();
        assertThrows(IllegalArgumentException.class, () -> DPath.set(rec, List.of(), "v"));
    }

    @Test
    void delete_topLevel() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("a", 1);
        rec.put("b", 2);
        assertTrue(DPath.delete(rec, List.of("a")));
        assertFalse(rec.containsKey("a"));
        assertEquals(2, rec.get("b"));
    }

    @Test
    void delete_nested() {
        Map<String, Object> rec = new LinkedHashMap<>();
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("secret", "x");
        rec.put("a", nested);
        assertTrue(DPath.delete(rec, List.of("a", "secret")));
        assertFalse(nested.containsKey("secret"));
    }

    @Test
    void delete_missingPathReturnsFalse() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("a", new LinkedHashMap<>());
        assertFalse(DPath.delete(rec, List.of("a", "missing")));
        assertFalse(DPath.delete(rec, List.of("nope")));
    }

    @Test
    void delete_listSlotSetToNullNotRemoved() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("arr", new ArrayList<>(List.of("a", "b", "c")));
        assertTrue(DPath.delete(rec, List.of("arr", "1")));
        @SuppressWarnings("unchecked")
        List<Object> arr = (List<Object>) rec.get("arr");
        assertEquals(Arrays.asList("a", null, "c"), arr);
    }

    @Test
    void delete_emptyPointerReturnsFalse() {
        Map<String, Object> rec = new LinkedHashMap<>();
        assertFalse(DPath.delete(rec, List.of()));
    }

    @Test
    void navigate_returnsValue() {
        Map<String, Object> rec = new LinkedHashMap<>();
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("k", "v");
        rec.put("a", nested);
        assertEquals("v", DPath.navigate(rec, List.of("a", "k")));
    }

    @Test
    void navigate_returnsNullOnMissing() {
        Map<String, Object> rec = new LinkedHashMap<>();
        assertNull(DPath.navigate(rec, List.of("missing")));
        assertNull(DPath.navigate(null, List.of("a")));
    }

    @Test
    void navigate_emptyPathReturnsRoot() {
        Map<String, Object> rec = new LinkedHashMap<>();
        assertSame(rec, DPath.navigate(rec, List.of()));
    }

    @Test
    void expandWildcards_mapStarYieldsAllKeys() {
        Map<String, Object> rec = new LinkedHashMap<>();
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("a", 1);
        data.put("b", 2);
        rec.put("data", data);
        var matches = DPath.expandWildcards(rec, List.of("data", "*"));
        assertEquals(2, matches.size());
        assertEquals(List.of("data", "a"), matches.get(0).path());
        assertEquals(1, matches.get(0).value());
        assertEquals(List.of("data", "b"), matches.get(1).path());
        assertEquals(2, matches.get(1).value());
    }

    @Test
    void expandWildcards_listStarYieldsAllIndices() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("arr", List.of("x", "y", "z"));
        var matches = DPath.expandWildcards(rec, List.of("arr", "*"));
        assertEquals(3, matches.size());
        assertEquals(List.of("arr", "0"), matches.get(0).path());
        assertEquals("x", matches.get(0).value());
    }

    @Test
    void expandWildcards_noMatchReturnsEmpty() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("data", new LinkedHashMap<>());
        var matches = DPath.expandWildcards(rec, List.of("data", "*"));
        assertEquals(0, matches.size());
    }

    @Test
    void expandWildcards_concretePathStillWorks() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("a", "v");
        var matches = DPath.expandWildcards(rec, List.of("a"));
        assertEquals(1, matches.size());
        assertEquals("v", matches.get(0).value());
    }
}
