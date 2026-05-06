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

import org.apache.kafka.connect.manifest.codegen.model.AddedFieldSpec;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransformationPipelineTest {

    private static Map<String, Object> ctx(Map<String, Object> record) {
        Map<String, Object> ctx = new LinkedHashMap<>();
        ctx.put("record", record);
        ctx.put("config", Map.of());
        return ctx;
    }

    @Test
    void noop_passesThrough() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("a", 1);
        Optional<Map<String, Object>> result = TransformationPipeline.NOOP.process(rec, ctx(rec));
        assertTrue(result.isPresent());
        assertSame(rec, result.get());
    }

    @Test
    void filter_drops() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("id", 2);
        Map<String, Object> ctx = ctx(rec);
        TransformationPipeline pipeline = new TransformationPipeline(
            new RecordFilter("{{ record['id'] % 2 == 1 }}"), List.of());
        Optional<Map<String, Object>> result = pipeline.process(rec, ctx);
        assertFalse(result.isPresent());
    }

    @Test
    void filter_keeps() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("id", 3);
        Map<String, Object> ctx = ctx(rec);
        TransformationPipeline pipeline = new TransformationPipeline(
            new RecordFilter("{{ record['id'] % 2 == 1 }}"), List.of());
        assertTrue(pipeline.process(rec, ctx).isPresent());
    }

    @Test
    void transformationsAppliedInOrder() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("a", "original");

        AddedFieldSpec spec1 = new AddedFieldSpec();
        spec1.setPath(List.of("b"));
        spec1.setValue("first");
        AddedFieldSpec spec2 = new AddedFieldSpec();
        spec2.setPath(List.of("c"));
        spec2.setValue("second");

        TransformationPipeline pipeline = new TransformationPipeline(null, List.of(
            new AddFieldsTransform(List.of(spec1), null),
            new AddFieldsTransform(List.of(spec2), null)
        ));
        Map<String, Object> ctx = ctx(rec);
        pipeline.process(rec, ctx);

        assertEquals("first", rec.get("b"));
        assertEquals("second", rec.get("c"));
    }

    @Test
    void noopSingleton_isNoop() {
        assertTrue(TransformationPipeline.NOOP.isNoop());
    }

    @Test
    void pipelineWithFilter_notNoop() {
        TransformationPipeline p = new TransformationPipeline(
            new RecordFilter("{{ true }}"), List.of());
        assertFalse(p.isNoop());
    }
}
