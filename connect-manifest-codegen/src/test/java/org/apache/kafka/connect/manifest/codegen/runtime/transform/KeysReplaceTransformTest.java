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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeysReplaceTransformTest {

    @Test
    void replacesTopLevelKeys() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("foo-bar", 1);
        rec.put("baz-qux", 2);
        new KeysReplaceTransform("-", "_").apply(rec, Map.of());
        assertFalse(rec.containsKey("foo-bar"));
        assertTrue(rec.containsKey("foo_bar"));
        assertTrue(rec.containsKey("baz_qux"));
        assertEquals(1, rec.get("foo_bar"));
    }

    @Test
    void recursesIntoNestedMaps() {
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("inner-key", "v");
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("outer-key", nested);
        new KeysReplaceTransform("-", "_").apply(rec, Map.of());
        assertTrue(rec.containsKey("outer_key"));
        @SuppressWarnings("unchecked")
        Map<String, Object> n = (Map<String, Object>) rec.get("outer_key");
        assertTrue(n.containsKey("inner_key"));
    }

    @Test
    void emptyOld_noop() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("key", 1);
        new KeysReplaceTransform("", "_").apply(rec, Map.of());
        assertTrue(rec.containsKey("key"));
    }

    @Test
    void noMatchingKeys_unchanged() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("abc", 1);
        new KeysReplaceTransform("-", "_").apply(rec, Map.of());
        assertTrue(rec.containsKey("abc"));
    }
}
