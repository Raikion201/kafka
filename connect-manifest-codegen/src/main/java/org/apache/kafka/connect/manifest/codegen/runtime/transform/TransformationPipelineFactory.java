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

import java.util.ArrayList;
import java.util.List;
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

    private TransformationPipelineFactory() {
    }

    /**
     * Deserializes {@code specsJson} (Jackson-serialized {@code List<TransformationSpec>})
     * and builds a {@link TransformationPipeline} with the supplied filter condition.
     *
     * @param specsJson       JSON string, or {@code null}/{@code "[]"} for no transforms
     * @param filterCondition Jinja boolean template for record filtering, or {@code null}
     * @return a ready-to-use pipeline; never {@code null}
     */
    public static TransformationPipeline fromJson(String specsJson, String filterCondition) {
        List<TransformationSpec> specs = parseSpecs(specsJson);
        RecordFilter filter = (filterCondition != null && !filterCondition.isBlank())
            ? new RecordFilter(filterCondition) : null;
        return fromSpecs(specs, filter);
    }

    /** Builds a pipeline from already-parsed specs. */
    public static TransformationPipeline fromSpecs(List<TransformationSpec> specs, RecordFilter filter) {
        if ((specs == null || specs.isEmpty()) && (filter == null || !filter.hasCondition())) {
            return TransformationPipeline.NOOP;
        }
        List<RecordTransformation> transforms = new ArrayList<>();
        if (specs != null) {
            for (TransformationSpec spec : specs) {
                RecordTransformation t = buildTransform(spec);
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
    private static RecordTransformation buildTransform(TransformationSpec spec) {
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
            default:
                LOG.warning("TransformationPipelineFactory: unknown transform type '" + spec.getType() + "', skipping");
                return null;
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
