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
package org.apache.kafka.connect.manifest.codegen.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

/**
 * Models one entry in {@code streams[].transformations}. The {@code type} field is the
 * discriminator; the union of fields covers every per-record transformation type the
 * Airbyte declarative manifest supports — {@code AddFields}, {@code RemoveFields},
 * {@code KeysToLower}, {@code KeysReplace}, {@code KeysToSnakeCase}, {@code FlattenFields},
 * {@code DpathFlattenFields}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TransformationSpec {

    private String type;

    /** AddFields. */
    private List<AddedFieldSpec> fields = Collections.emptyList();

    /** RemoveFields. */
    @JsonProperty("field_pointers")
    @com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = FieldPointersDeserializer.class)
    private List<List<String>> fieldPointers = Collections.emptyList();

    /** AddFields / RemoveFields — Jinja boolean gating the whole transform. */
    private String condition;

    /** KeysReplace. */
    private String old;

    @JsonProperty("new")
    private String newValue;

    /** FlattenFields. */
    @JsonProperty("flatten_lists")
    private Boolean flattenLists;

    /** CustomTransformation — fully-qualified Java class_name registered in CustomComponentRegistry. */
    @JsonProperty("class_name")
    private String className;

    /** DpathFlattenFields. */
    @JsonProperty("field_path")
    private List<String> fieldPath = Collections.emptyList();

    @JsonProperty("delete_origin_value")
    private Boolean deleteOriginValue;

    @JsonProperty("replace_record")
    private Boolean replaceRecord;

    @JsonProperty("key_transformation")
    private KeyTransformationSpec keyTransformation;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<AddedFieldSpec> getFields() {
        return fields == null ? Collections.emptyList() : fields;
    }

    public void setFields(List<AddedFieldSpec> fields) {
        this.fields = fields;
    }

    public List<List<String>> getFieldPointers() {
        return fieldPointers == null ? Collections.emptyList() : fieldPointers;
    }

    public void setFieldPointers(List<List<String>> fieldPointers) {
        this.fieldPointers = fieldPointers;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getOld() {
        return old;
    }

    public void setOld(String old) {
        this.old = old;
    }

    public String getNewValue() {
        return newValue;
    }

    public void setNewValue(String newValue) {
        this.newValue = newValue;
    }

    public Boolean getFlattenLists() {
        return flattenLists;
    }

    public void setFlattenLists(Boolean flattenLists) {
        this.flattenLists = flattenLists;
    }

    public List<String> getFieldPath() {
        return fieldPath == null ? Collections.emptyList() : fieldPath;
    }

    public void setFieldPath(List<String> fieldPath) {
        this.fieldPath = fieldPath;
    }

    public Boolean getDeleteOriginValue() {
        return deleteOriginValue;
    }

    public void setDeleteOriginValue(Boolean deleteOriginValue) {
        this.deleteOriginValue = deleteOriginValue;
    }

    public Boolean getReplaceRecord() {
        return replaceRecord;
    }

    public void setReplaceRecord(Boolean replaceRecord) {
        this.replaceRecord = replaceRecord;
    }

    public KeyTransformationSpec getKeyTransformation() {
        return keyTransformation;
    }

    public void setKeyTransformation(KeyTransformationSpec keyTransformation) {
        this.keyTransformation = keyTransformation;
    }

    /**
     * Accepts {@code field_pointers} either as a list-of-lists (Airbyte canonical form,
     * {@code [["a", "b"], ["c"]]}) or as a single flat path ({@code ["a", "b"]}).
     */
    static final class FieldPointersDeserializer
            extends com.fasterxml.jackson.databind.JsonDeserializer<List<List<String>>> {
        @Override
        public List<List<String>> deserialize(com.fasterxml.jackson.core.JsonParser p,
                                              com.fasterxml.jackson.databind.DeserializationContext ctxt)
                throws java.io.IOException {
            com.fasterxml.jackson.databind.JsonNode root = p.readValueAsTree();
            List<List<String>> out = new java.util.ArrayList<>();
            if (root == null || root.isNull()) {
                return out;
            }
            if (root.isArray()) {
                boolean nested = false;
                for (com.fasterxml.jackson.databind.JsonNode child : root) {
                    if (child.isArray()) {
                        nested = true;
                        break;
                    }
                }
                if (nested) {
                    for (com.fasterxml.jackson.databind.JsonNode child : root) {
                        out.add(toStringList(child));
                    }
                } else {
                    out.add(toStringList(root));
                }
            }
            return out;
        }

        private List<String> toStringList(com.fasterxml.jackson.databind.JsonNode n) {
            List<String> result = new java.util.ArrayList<>();
            if (n == null || n.isNull()) {
                return result;
            }
            if (n.isTextual()) {
                result.add(n.asText());
                return result;
            }
            if (n.isArray()) {
                for (com.fasterxml.jackson.databind.JsonNode child : n) {
                    if (child.isTextual()) {
                        result.add(child.asText());
                    } else if (!child.isNull()) {
                        result.add(child.asText());
                    }
                }
            }
            return result;
        }
    }

    /** Convenience: returns {@code true} when {@code keyTransformation} is non-null and has any value. */
    public boolean hasKeyTransformation() {
        return keyTransformation != null
            && (keyTransformation.getPrefix() != null || keyTransformation.getSuffix() != null);
    }
}
