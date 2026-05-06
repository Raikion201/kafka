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

class KeysToLowerTransformTest {

    @Test
    void lowercasesAllKeys() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("FooBar", 1);
        rec.put("HELLO", "world");
        rec.put("already_lower", true);
        KeysToLowerTransform.INSTANCE.apply(rec, Map.of());
        assertFalse(rec.containsKey("FooBar"));
        assertFalse(rec.containsKey("HELLO"));
        assertTrue(rec.containsKey("foobar"));
        assertTrue(rec.containsKey("hello"));
        assertTrue(rec.containsKey("already_lower"));
        assertEquals(1, rec.get("foobar"));
        assertEquals("world", rec.get("hello"));
    }

    @Test
    void emptyMap_noop() {
        Map<String, Object> rec = new LinkedHashMap<>();
        KeysToLowerTransform.INSTANCE.apply(rec, Map.of());
        assertTrue(rec.isEmpty());
    }

    @Test
    void doesNotRecurseIntoNestedMaps() {
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("UPPER", "v");
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("OUTER", nested);
        KeysToLowerTransform.INSTANCE.apply(rec, Map.of());
        assertTrue(rec.containsKey("outer"));
        @SuppressWarnings("unchecked")
        Map<String, Object> n = (Map<String, Object>) rec.get("outer");
        // nested map keys are not touched — flat-only transform
        assertTrue(n.containsKey("UPPER"));
    }
}
