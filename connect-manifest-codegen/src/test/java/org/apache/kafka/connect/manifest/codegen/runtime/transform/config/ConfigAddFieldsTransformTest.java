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
package org.apache.kafka.connect.manifest.codegen.runtime.transform.config;

import org.apache.kafka.connect.manifest.codegen.model.AddedFieldSpec;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ConfigAddFieldsTransformTest {

    private static AddedFieldSpec field(List<String> path, String value) {
        AddedFieldSpec s = new AddedFieldSpec();
        s.setPath(path);
        s.setValue(value);
        return s;
    }

    @Test
    void addsLiteralField() {
        Map<String, Object> config = new LinkedHashMap<>();
        new ConfigAddFieldsTransform(List.of(field(List.of("env"), "production")), null)
            .apply(config);
        assertEquals("production", config.get("env"));
    }

    @Test
    void jinjaFromConfig_addsComputedField() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("base_url", "https://api.example.com");
        new ConfigAddFieldsTransform(
            List.of(field(List.of("derived"), "{{ config['base_url'] }}/v2")), null)
            .apply(config);
        assertEquals("https://api.example.com/v2", config.get("derived"));
    }

    @Test
    void conditionFalse_skips() {
        Map<String, Object> config = new LinkedHashMap<>();
        new ConfigAddFieldsTransform(
            List.of(field(List.of("x"), "val")), "{{ false }}")
            .apply(config);
        assertNull(config.get("x"));
    }

    @Test
    void multipleFields_appliedInOrder() {
        Map<String, Object> config = new LinkedHashMap<>();
        new ConfigAddFieldsTransform(List.of(
            field(List.of("a"), "1"),
            field(List.of("b"), "2")
        ), null).apply(config);
        assertEquals("1", config.get("a"));
        assertEquals("2", config.get("b"));
    }
}
