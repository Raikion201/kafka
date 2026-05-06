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

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConfigRemapTransformTest {

    @Test
    void remapsMappedValue() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("env", "us");
        Map<String, String> map = Map.of("us", "us-east-1", "eu", "eu-west-1");
        new ConfigRemapTransform(List.of("env"), map).apply(config);
        assertEquals("us-east-1", config.get("env"));
    }

    @Test
    void unmappedValue_unchanged() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("env", "ap");
        Map<String, String> map = Map.of("us", "us-east-1");
        new ConfigRemapTransform(List.of("env"), map).apply(config);
        assertEquals("ap", config.get("env"));
    }

    @Test
    void missingPath_noop() {
        Map<String, Object> config = new LinkedHashMap<>();
        Map<String, String> map = Map.of("us", "us-east-1");
        new ConfigRemapTransform(List.of("missing"), map).apply(config);
        // no exception, no change
        assertEquals(0, config.size());
    }

    @Test
    void nestedPath_resolved() {
        Map<String, Object> config = new LinkedHashMap<>();
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("region", "us");
        config.put("settings", nested);
        Map<String, String> map = Map.of("us", "us-east-1");
        new ConfigRemapTransform(List.of("settings", "region"), map).apply(config);
        @SuppressWarnings("unchecked")
        Map<String, Object> s = (Map<String, Object>) config.get("settings");
        assertEquals("us-east-1", s.get("region"));
    }

    @Test
    void emptyFieldPath_noop() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("x", "v");
        new ConfigRemapTransform(List.of(), Map.of("v", "w")).apply(config);
        assertEquals("v", config.get("x"));
    }

    @Test
    void emptyMap_noop() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("env", "us");
        new ConfigRemapTransform(List.of("env"), Map.of()).apply(config);
        assertEquals("us", config.get("env"));
    }
}
