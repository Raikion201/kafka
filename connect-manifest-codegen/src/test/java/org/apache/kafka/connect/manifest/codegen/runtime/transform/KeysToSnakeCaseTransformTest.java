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
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeysToSnakeCaseTransformTest {

    @Test
    void camelCase_toSnake() {
        assertEquals("foo_bar", KeysToSnakeCaseTransform.toSnakeCase("FooBar"));
    }

    @Test
    void alreadySnake_unchanged() {
        assertEquals("foo_bar", KeysToSnakeCaseTransform.toSnakeCase("foo_bar"));
    }

    @Test
    void spaceSeparated_toSnake() {
        assertEquals("hello_world", KeysToSnakeCaseTransform.toSnakeCase("Hello World"));
    }

    @Test
    void dashSeparated_toSnake() {
        assertEquals("my_key", KeysToSnakeCaseTransform.toSnakeCase("my-key"));
    }

    @Test
    void leadingDigit_prefixed() {
        // Python: leading-digit token gets "" prepended → joins as "_123abc"
        assertEquals("_123abc", KeysToSnakeCaseTransform.toSnakeCase("123abc"));
    }

    @Test
    void apply_snakifiesKeysAndRecurses() {
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("InnerKey", "v");
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("OuterKey", nested);
        rec.put("AnotherKey", 42);
        KeysToSnakeCaseTransform.INSTANCE.apply(rec, Map.of());
        assertTrue(rec.containsKey("outer_key"));
        assertTrue(rec.containsKey("another_key"));
        @SuppressWarnings("unchecked")
        Map<String, Object> n = (Map<String, Object>) rec.get("outer_key");
        assertTrue(n.containsKey("inner_key"));
        assertEquals(42, rec.get("another_key"));
    }
}
