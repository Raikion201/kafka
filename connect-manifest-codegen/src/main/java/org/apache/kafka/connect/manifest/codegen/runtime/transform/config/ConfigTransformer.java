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

import java.util.List;
import java.util.Map;

/**
 * Ordered chain of {@link ConfigTransformation}s applied once at task {@code start()}
 * before any field is read from the config.
 */
public final class ConfigTransformer {

    public static final ConfigTransformer NOOP = new ConfigTransformer(List.of());

    private final List<ConfigTransformation> transformations;

    public ConfigTransformer(List<ConfigTransformation> transformations) {
        this.transformations = transformations == null ? List.of() : transformations;
    }

    public void apply(Map<String, Object> config) {
        for (ConfigTransformation t : transformations) {
            t.apply(config);
        }
    }

    public boolean isNoop() {
        return transformations.isEmpty();
    }
}
