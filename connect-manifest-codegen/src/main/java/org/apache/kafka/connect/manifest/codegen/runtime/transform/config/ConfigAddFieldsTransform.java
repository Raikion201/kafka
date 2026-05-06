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
import org.apache.kafka.connect.manifest.codegen.runtime.transform.AddFieldsTransform;

import java.util.List;
import java.util.Map;

/**
 * Java port of Airbyte's {@code config_transformations/add_fields.py ConfigAddFields}.
 * Reuses the per-record {@link AddFieldsTransform} engine pointed at the config map —
 * both the input (config) and the Jinja context use the same config map.
 */
public final class ConfigAddFieldsTransform implements ConfigTransformation {

    private final AddFieldsTransform inner;

    public ConfigAddFieldsTransform(List<AddedFieldSpec> fields, String condition) {
        this.inner = new AddFieldsTransform(fields, condition);
    }

    @Override
    public void apply(Map<String, Object> config) {
        Map<String, Object> ctx = Map.of("config", config);
        inner.apply(config, ctx);
    }
}
