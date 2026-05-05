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

class DoubleQuotedDictTypeTransformerTest {

    private final DoubleQuotedDictTypeTransformer t =
        new DoubleQuotedDictTypeTransformer(Collections.emptyMap(), Collections.emptyMap());

    @Test
    void rewritesArrayOfDictsToJsonStrings() {
        Map<String, Object> record = new LinkedHashMap<>();
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("key", "campaign");
        a.put("value", "gg_nam_dg_search_brand");
        record.put("labels", List.of(a));

        Map<String, Object> out = t.normalize(record);
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) out.get("labels");
        // Jackson default produces no space after comma; matches Python json.dumps separators
        assertEquals("{\"key\":\"campaign\",\"value\":\"gg_nam_dg_search_brand\"}", list.get(0));
    }

    @Test
    void leavesArrayOfStringsAlone() {
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("tags", Arrays.asList("a", "b"));
        Map<String, Object> out = t.normalize(record);
        assertEquals(Arrays.asList("a", "b"), out.get("tags"));
    }

    @Test
    void leavesScalarsAndNullValuesAlone() {
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("n", 1);
        record.put("z", null);
        Map<String, Object> out = t.normalize(record);
        assertEquals(1, out.get("n"));
        assertNull(out.get("z"));
    }

    @Test
    void emptyArrayPassesThrough() {
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("labels", Collections.emptyList());
        Map<String, Object> out = t.normalize(record);
        assertEquals(Collections.emptyList(), out.get("labels"));
    }

    @Test
    void mixedArrayPassesThroughUnchanged() {
        Map<String, Object> record = new LinkedHashMap<>();
        Object original = Arrays.asList(Map.of("k", 1), "literal");
        record.put("xs", original);
        Map<String, Object> out = t.normalize(record);
        assertSame(original, out.get("xs"));
    }

    @Test
    void nullRecordReturnsNull() {
        assertNull(t.normalize(null));
    }
}
