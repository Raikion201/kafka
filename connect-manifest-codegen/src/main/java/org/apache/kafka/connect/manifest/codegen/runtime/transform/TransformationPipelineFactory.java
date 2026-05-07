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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.connect.manifest.codegen.model.AddedFieldSpec;
import org.apache.kafka.connect.manifest.codegen.model.KeyTransformationSpec;
import org.apache.kafka.connect.manifest.codegen.model.TransformationSpec;
import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomComponentRegistry;
import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomTransformation;
import org.apache.kafka.connect.manifest.codegen.runtime.customs.asana.AsanaRegistrar;
import org.apache.kafka.connect.manifest.codegen.runtime.customs.generic.GenericCustomComponentsRegistrar;
import org.apache.kafka.connect.manifest.codegen.runtime.customs.jinaai.JinaAiRegistrar;
import org.apache.kafka.connect.manifest.codegen.runtime.customs.mixpanel.MixpanelRegistrar;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Builds a {@link TransformationPipeline} from a JSON-serialized list of
 * {@link TransformationSpec} objects and an optional filter condition string.
 * Called from generated connector {@code start()} methods at task startup.
 */
public final class TransformationPipelineFactory {

    private static final Logger LOG = Logger.getLogger(TransformationPipelineFactory.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<List<TransformationSpec>> SPEC_LIST_TYPE =
        new TypeReference<List<TransformationSpec>>() { };

    static {
        // Eagerly register all connector-specific custom components so that
        // CustomComponentRegistry.create() succeeds for every connector that uses them.
        GenericCustomComponentsRegistrar.register();
        AsanaRegistrar.register();
        JinaAiRegistrar.register();
        MixpanelRegistrar.register();
    }

    private TransformationPipelineFactory() {
    }

    /**
     * Deserializes {@code specsJson} and builds a pipeline with the supplied filter condition.
     *
     * @param specsJson       JSON string, or {@code null}/{@code "[]"} for no transforms
     * @param filterCondition Jinja boolean template for record filtering, or {@code null}
     * @return a ready-to-use pipeline; never {@code null}
     */
    public static TransformationPipeline fromJson(String specsJson, String filterCondition) {
        return fromJson(specsJson, filterCondition, null);
    }

    /**
     * Overload that accepts a connector config for instantiating {@code CustomTransformation}
     * components via {@link CustomComponentRegistry}.
     */
    public static TransformationPipeline fromJson(String specsJson, Map<String, String> connectorConfig) {
        return fromJson(specsJson, null, connectorConfig);
    }

    /**
     * Full form — filter condition + connector config for Custom* lookups.
     */
    public static TransformationPipeline fromJson(
            String specsJson, String filterCondition, Map<String, String> connectorConfig) {
        List<TransformationSpec> specs = parseSpecs(specsJson);
        RecordFilter filter = (filterCondition != null && !filterCondition.isBlank())
            ? new RecordFilter(filterCondition) : null;
        return fromSpecs(specs, filter, connectorConfig);
    }

    /** Builds a pipeline from already-parsed specs without a connector config. */
    public static TransformationPipeline fromSpecs(List<TransformationSpec> specs, RecordFilter filter) {
        return fromSpecs(specs, filter, null);
    }

    /** Builds a pipeline from already-parsed specs, with optional connector config for Custom* components. */
    public static TransformationPipeline fromSpecs(
            List<TransformationSpec> specs, RecordFilter filter, Map<String, String> connectorConfig) {
        if ((specs == null || specs.isEmpty()) && (filter == null || !filter.hasCondition())) {
            return TransformationPipeline.NOOP;
        }
        List<RecordTransformation> transforms = new ArrayList<>();
        if (specs != null) {
            for (TransformationSpec spec : specs) {
                RecordTransformation t = buildTransform(spec, connectorConfig);
                if (t != null) {
                    transforms.add(t);
                }
            }
        }
        return new TransformationPipeline(filter, transforms);
    }

    private static List<TransformationSpec> parseSpecs(String json) {
        if (json == null || json.isBlank() || "[]".equals(json.strip())) {
            return List.of();
        }
        try {
            return MAPPER.readValue(json, SPEC_LIST_TYPE);
        } catch (Exception e) {
            LOG.warning("TransformationPipelineFactory: failed to parse specs JSON: " + e.getMessage());
            return List.of();
        }
    }

    @SuppressWarnings("unchecked")
    private static RecordTransformation buildTransform(TransformationSpec spec, Map<String, String> connectorConfig) {
        if (spec == null || spec.getType() == null) {
            return null;
        }
        switch (spec.getType()) {
            case "AddFields":
                return new AddFieldsTransform(spec.getFields(), spec.getCondition());
            case "RemoveFields":
                return new RemoveFieldsTransform(spec.getFieldPointers(), spec.getCondition());
            case "KeysToLower":
                return KeysToLowerTransform.INSTANCE;
            case "KeysReplace":
                return new KeysReplaceTransform(spec.getOld(), spec.getNewValue());
            case "KeysToSnakeCase":
                return KeysToSnakeCaseTransform.INSTANCE;
            case "FlattenFields":
                return new FlattenFieldsTransform(Boolean.TRUE.equals(spec.getFlattenLists()));
            case "DpathFlattenFields":
            case "KeyTransformation":
                KeyTransformationSpec kt = spec.getKeyTransformation();
                return new DpathFlattenTransform(
                    spec.getFieldPath(),
                    Boolean.TRUE.equals(spec.getDeleteOriginValue()),
                    Boolean.TRUE.equals(spec.getReplaceRecord()),
                    kt);
            case "CustomTransformation":
                String cls = spec.getClassName();
                if (cls == null || cls.isBlank()) {
                    LOG.warning("TransformationPipelineFactory: CustomTransformation missing class_name, skipping");
                    return null;
                }
                CustomTransformation delegate =
                    CustomComponentRegistry.create(cls, CustomTransformation.class, connectorConfig, Map.of());
                return new CustomTransformationAdapter(delegate);
            default:
                LOG.warning("TransformationPipelineFactory: unknown transform type '" + spec.getType() + "', skipping");
                return null;
        }
    }

    /** Wraps a {@link CustomTransformation} as a {@link RecordTransformation}. */
    static final class CustomTransformationAdapter implements RecordTransformation {
        private final CustomTransformation delegate;

        CustomTransformationAdapter(CustomTransformation delegate) {
            this.delegate = delegate;
        }

        @Override
        public void apply(Map<String, Object> record, Map<String, Object> ctx) {
            Map<String, Object> result = delegate.transform(new HashMap<>(record));
            if (result != null) {
                record.clear();
                record.putAll(result);
            }
        }
    }

    /** Serializes a list of specs to JSON for embedding in generated code. */
    public static String toJson(List<TransformationSpec> specs) {
        if (specs == null || specs.isEmpty()) {
            return "[]";
        }
        try {
            return MAPPER.writeValueAsString(specs);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize TransformationSpec list", e);
        }
    }

    /** Builds an AddedFieldSpec from raw values. */
    public static AddedFieldSpec addedField(List<String> path, String value, String valueType) {
        AddedFieldSpec s = new AddedFieldSpec();
        s.setPath(path);
        s.setValue(value);
        s.setValueType(valueType);
        return s;
    }
}
