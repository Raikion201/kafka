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
package org.apache.kafka.connect.manifest.codegen.runtime.customs.googleads;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlattenNestedDictsTransformationTest {

    private final FlattenNestedDictsTransformation t =
        new FlattenNestedDictsTransformation(Collections.emptyMap(), Collections.emptyMap());

    @Test
    void flattensTwoLevelsAndDoesNotDescendIntoLists() {
        Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("d", 2);
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("b", 1);
        a.put("c", inner);
        a.put("e", List.of(Map.of("f", 3)));
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("a", a);
        record.put("g", List.of(Map.of("h", 4)));

        Map<String, Object> out = t.transform(record);

        assertEquals(1, out.get("a.b"));
        assertEquals(2, out.get("a.c.d"));
        assertEquals(List.of(Map.of("f", 3)), out.get("a.e"));
        assertEquals(List.of(Map.of("h", 4)), out.get("g"));
        assertFalse(out.containsKey("a"));
    }

    @Test
    void emptyRecordReturnsEmpty() {
        assertEquals(Collections.emptyMap(), t.transform(new LinkedHashMap<>()));
    }

    @Test
    void nullRecordReturnsNull() {
        assertNull(t.transform(null));
    }

    @Test
    void scalarsAndListsArePassedThrough() {
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("x", 1);
        record.put("y", Arrays.asList(1, 2));
        record.put("z", null);

        Map<String, Object> out = t.transform(record);
        assertEquals(1, out.get("x"));
        assertEquals(Arrays.asList(1, 2), out.get("y"));
        assertTrue(out.containsKey("z"));
    }

    @Test
    void customDelimiterIsHonoured() {
        FlattenNestedDictsTransformation slash = new FlattenNestedDictsTransformation(
            Collections.emptyMap(), Map.of("delimiter", "/"));

        Map<String, Object> record = new LinkedHashMap<>();
        record.put("a", Map.of("b", 1));

        assertEquals(1, slash.transform(record).get("a/b"));
    }
}
