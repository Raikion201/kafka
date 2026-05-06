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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordFilterTest {

    private static Map<String, Object> ctx(Map<String, Object> record) {
        Map<String, Object> ctx = new LinkedHashMap<>();
        ctx.put("record", record);
        ctx.put("config", Map.of());
        return ctx;
    }

    @Test
    void nullCondition_accepts() {
        Map<String, Object> rec = Map.of("id", 1);
        assertTrue(new RecordFilter(null).accept(rec, ctx(rec)));
    }

    @Test
    void blankCondition_accepts() {
        Map<String, Object> rec = Map.of("id", 1);
        assertTrue(new RecordFilter("  ").accept(rec, ctx(rec)));
    }

    @Test
    void literalTrue_accepts() {
        Map<String, Object> rec = Map.of("id", 1);
        assertTrue(new RecordFilter("{{ true }}").accept(rec, ctx(rec)));
    }

    @Test
    void literalFalse_drops() {
        Map<String, Object> rec = Map.of("id", 1);
        assertFalse(new RecordFilter("{{ false }}").accept(rec, ctx(rec)));
    }

    @Test
    void jinjaCondition_evenId_drops() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("id", 4);
        Map<String, Object> ctx = ctx(rec);
        // Airbyte RecordFilter pattern: keep only odd ids
        assertFalse(new RecordFilter("{{ record['id'] % 2 == 1 }}").accept(rec, ctx));
    }

    @Test
    void jinjaCondition_oddId_accepts() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("id", 3);
        Map<String, Object> ctx = ctx(rec);
        assertTrue(new RecordFilter("{{ record['id'] % 2 == 1 }}").accept(rec, ctx));
    }
}
