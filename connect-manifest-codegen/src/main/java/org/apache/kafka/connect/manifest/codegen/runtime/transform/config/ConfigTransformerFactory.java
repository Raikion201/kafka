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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.connect.manifest.codegen.model.ConfigTransformationSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Builds a {@link ConfigTransformer} from a JSON-serialized list of
 * {@link ConfigTransformationSpec} objects. Called from generated connector
 * {@code start()} methods at task startup.
 */
public final class ConfigTransformerFactory {

    private static final Logger LOG = Logger.getLogger(ConfigTransformerFactory.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<List<ConfigTransformationSpec>> SPEC_LIST_TYPE =
        new TypeReference<List<ConfigTransformationSpec>>() { };

    private ConfigTransformerFactory() {
    }

    /**
     * Deserializes {@code specsJson} and builds a {@link ConfigTransformer}.
     *
     * @param specsJson JSON string, or {@code null}/{@code "[]"} for no transforms
     * @return ready-to-use transformer; never {@code null}
     */
    public static ConfigTransformer fromJson(String specsJson) {
        if (specsJson == null || specsJson.isBlank() || "[]".equals(specsJson.strip())) {
            return ConfigTransformer.NOOP;
        }
        List<ConfigTransformationSpec> specs;
        try {
            specs = MAPPER.readValue(specsJson, SPEC_LIST_TYPE);
        } catch (Exception e) {
            LOG.warning("ConfigTransformerFactory: failed to parse specs JSON: " + e.getMessage());
            return ConfigTransformer.NOOP;
        }
        return fromSpecs(specs);
    }

    /** Builds a transformer from already-parsed specs. */
    public static ConfigTransformer fromSpecs(List<ConfigTransformationSpec> specs) {
        if (specs == null || specs.isEmpty()) {
            return ConfigTransformer.NOOP;
        }
        List<ConfigTransformation> transforms = new ArrayList<>();
        for (ConfigTransformationSpec spec : specs) {
            ConfigTransformation t = buildTransform(spec);
            if (t != null) {
                transforms.add(t);
            }
        }
        return transforms.isEmpty() ? ConfigTransformer.NOOP : new ConfigTransformer(transforms);
    }

    private static ConfigTransformation buildTransform(ConfigTransformationSpec spec) {
        if (spec == null || spec.getType() == null) {
            return null;
        }
        switch (spec.getType()) {
            case "ConfigAddFields":
                return new ConfigAddFieldsTransform(spec.getFields(), spec.getCondition());
            case "ConfigRemapField":
                return new ConfigRemapTransform(spec.getFieldPath(), spec.getMap());
            default:
                LOG.warning("ConfigTransformerFactory: unknown config transform type '"
                    + spec.getType() + "', skipping");
                return null;
        }
    }

    /** Serializes a list of specs to JSON for embedding in generated code. */
    public static String toJson(List<ConfigTransformationSpec> specs) {
        if (specs == null || specs.isEmpty()) {
            return "[]";
        }
        try {
            return MAPPER.writeValueAsString(specs);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize ConfigTransformationSpec list", e);
        }
    }
}
