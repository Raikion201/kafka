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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class SerializeMessageFieldsTransformationTest {

    @Test
    void serialisesNestedDictAtPath() {
        Map<String, Object> blob = new LinkedHashMap<>();
        blob.put("foo", 1);

        Map<String, Object> changeEvent = new LinkedHashMap<>();
        changeEvent.put("old_resource", blob);

        Map<String, Object> record = new LinkedHashMap<>();
        record.put("change_event", changeEvent);

        SerializeMessageFieldsTransformation t = new SerializeMessageFieldsTransformation(
            Collections.emptyMap(),
            Map.of("messageFields", List.of("change_event.old_resource")));

        Map<String, Object> out = t.transform(record);
        @SuppressWarnings("unchecked")
        Map<String, Object> ce = (Map<String, Object>) out.get("change_event");
        assertEquals("{\"foo\":1}", ce.get("old_resource"));
    }

    @Test
    void serialisesEachDictInsideList() {
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("payload", Arrays.asList(Map.of("k", 1), "literal", Map.of("k", 2)));

        SerializeMessageFieldsTransformation t = new SerializeMessageFieldsTransformation(
            Collections.emptyMap(),
            Map.of("messageFields", List.of("payload")));

        Map<String, Object> out = t.transform(record);
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) out.get("payload");
        assertEquals("{\"k\":1}", list.get(0));
        assertEquals("literal", list.get(1));
        assertEquals("{\"k\":2}", list.get(2));
    }

    @Test
    void emptyMessageFieldsReturnsRecordUnchanged() {
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("change_event", Map.of("old_resource", Map.of("foo", 1)));

        SerializeMessageFieldsTransformation t = new SerializeMessageFieldsTransformation(
            Collections.emptyMap(),
            Map.of("messageFields", List.of()));

        // No fields in the override set, and no global state, so passthrough.
        assertSame(record, t.transform(record));
    }

    @Test
    void missingPathIsLeftAlone() {
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("foo", 1);

        SerializeMessageFieldsTransformation t = new SerializeMessageFieldsTransformation(
            Collections.emptyMap(),
            Map.of("messageFields", List.of("change_event.missing")));

        Map<String, Object> out = t.transform(record);
        assertEquals(1, out.get("foo"));
    }

    @Test
    void nullRecordReturnsNull() {
        SerializeMessageFieldsTransformation t = new SerializeMessageFieldsTransformation(
            Collections.emptyMap(),
            Map.of("messageFields", List.of("a")));
        assertNull(t.transform(null));
    }
}
