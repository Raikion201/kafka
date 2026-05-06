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
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigTransformerTest {

    @Test
    void noop_doesNothing() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("k", "v");
        ConfigTransformer.NOOP.apply(config);
        assertEquals("v", config.get("k"));
        assertTrue(ConfigTransformer.NOOP.isNoop());
    }

    @Test
    void chain_appliedInOrder() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("region", "us");

        AddedFieldSpec spec = new AddedFieldSpec();
        spec.setPath(List.of("env"));
        spec.setValue("prod");

        ConfigTransformer transformer = new ConfigTransformer(List.of(
            new ConfigAddFieldsTransform(List.of(spec), null),
            new ConfigRemapTransform(List.of("region"), Map.of("us", "us-east-1"))
        ));
        transformer.apply(config);

        assertEquals("prod", config.get("env"));
        assertEquals("us-east-1", config.get("region"));
    }
}
