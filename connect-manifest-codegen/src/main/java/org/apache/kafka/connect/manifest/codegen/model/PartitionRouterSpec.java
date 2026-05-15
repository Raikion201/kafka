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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Models a {@code partition_router} entry — either a single SubstreamPartitionRouter
 * or one element of a list of routers on the retriever.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PartitionRouterSpec {

    private String type;

    /** Airbyte CDK Custom* node identifier. Non-null only when type starts with "Custom". */
    @JsonProperty("class_name")
    private String className;

    @JsonProperty("parent_stream_configs")
    private List<ParentStreamConfig> parentStreamConfigs = Collections.emptyList();

    // ── ListPartitionRouter fields ────────────────────────────────────────────

    /**
     * The list of partition values to iterate over.
     * Per Python CDK list_partition_router.py lines 19-56: values may be a literal
     * YAML list OR a single Jinja string that evaluates to a list at runtime.
     * We store whichever form was present; a plain string means config-ref (not yet evaluated).
     */
    @JsonDeserialize(using = StringOrListDeserializer.class)
    private List<String> values = Collections.emptyList();

    /**
     * The key name placed in the partition dict for each slice, e.g. {@code "breakdown"}.
     * Child streams reference it via {@code {{ stream_partition.breakdown }}}.
     * Python CDK: cursor_field (list_partition_router.py line 23).
     */
    @JsonProperty("cursor_field")
    private String cursorField;

    /**
     * Optional injection of the partition value into the HTTP request.
     * Python CDK: request_option (list_partition_router.py line 24).
     */
    @JsonProperty("request_option")
    private RequestOptionSpec requestOption;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public List<ParentStreamConfig> getParentStreamConfigs() {
        return parentStreamConfigs == null ? Collections.emptyList() : parentStreamConfigs;
    }

    public void setParentStreamConfigs(List<ParentStreamConfig> parentStreamConfigs) {
        this.parentStreamConfigs = parentStreamConfigs;
    }

    public boolean isSubstream() {
        return "SubstreamPartitionRouter".equalsIgnoreCase(type);
    }

    public boolean isList() {
        return "ListPartitionRouter".equalsIgnoreCase(type);
    }

    public List<String> getValues() {
        return values == null ? Collections.emptyList() : values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    public String getCursorField() {
        return cursorField;
    }

    public void setCursorField(String cursorField) {
        this.cursorField = cursorField;
    }

    public RequestOptionSpec getRequestOption() {
        return requestOption;
    }

    public void setRequestOption(RequestOptionSpec requestOption) {
        this.requestOption = requestOption;
    }

    /** The Jinja2 variable name injected into the child path, e.g. {@code course}. */
    public String partitionField() {
        return getParentStreamConfigs().isEmpty() ? null : getParentStreamConfigs().get(0).getPartitionField();
    }

    /** The field extracted from each parent record to use as the partition key, e.g. {@code id}. */
    public String parentKey() {
        return getParentStreamConfigs().isEmpty() ? null : getParentStreamConfigs().get(0).getParentKey();
    }

    /** The stream name of the parent stream, e.g. {@code courses}. */
    public String parentStreamName() {
        return getParentStreamConfigs().isEmpty() ? null : getParentStreamConfigs().get(0).getParentStreamName();
    }

    /**
     * The {@code request_option} on the first parent_stream_config, if any.
     * When non-null, the substream router must inject the partition value into the child
     * request via this option (Python CDK substream_partition_router.py lines 162-176)
     * instead of (or in addition to) path substitution.
     */
    public RequestOptionSpec parentRequestOption() {
        return getParentStreamConfigs().isEmpty() ? null : getParentStreamConfigs().get(0).getRequestOption();
    }

    // ── inner classes ─────────────────────────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ParentStreamConfig {

        @JsonProperty("parent_key")
        private String parentKey;

        @JsonProperty("partition_field")
        private String partitionField;

        private StreamRef stream;

        /**
         * How the partition value is injected into the child stream's outgoing request.
         * Python CDK: ParentStreamConfig.request_option (substream_partition_router.py line 79).
         * When set, the substream router calls inject_into_request() per Python lines 162-176.
         */
        @JsonProperty("request_option")
        private RequestOptionSpec requestOption;

        public String getParentKey() {
            return parentKey;
        }

        public void setParentKey(String parentKey) {
            this.parentKey = parentKey;
        }

        public String getPartitionField() {
            return partitionField;
        }

        public void setPartitionField(String partitionField) {
            this.partitionField = partitionField;
        }

        public StreamRef getStream() {
            return stream;
        }

        public void setStream(StreamRef stream) {
            this.stream = stream;
        }

        public RequestOptionSpec getRequestOption() {
            return requestOption;
        }

        public void setRequestOption(RequestOptionSpec requestOption) {
            this.requestOption = requestOption;
        }

        public String getParentStreamName() {
            return stream != null ? stream.refStreamName() : null;
        }
    }

    /**
     * Models {@code request_option} on a ListPartitionRouter — how the current partition
     * value is injected into the HTTP request.
     * Python CDK: RequestOption dataclass (request_option.py).
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RequestOptionSpec {

        @JsonProperty("field_name")
        private String fieldName;

        @JsonProperty("inject_into")
        private String injectInto;

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getInjectInto() {
            return injectInto;
        }

        public void setInjectInto(String injectInto) {
            this.injectInto = injectInto;
        }

        public boolean isRequestParameter() {
            return "request_parameter".equalsIgnoreCase(injectInto);
        }

        public boolean isHeader() {
            return "header".equalsIgnoreCase(injectInto);
        }
    }

    /**
     * Deserializes the {@code values} field which may be either a YAML list of strings
     * or a single Jinja template string (evaluated at runtime by the Python CDK).
     * Python CDK: list_partition_router.py lines 48-56.
     */
    public static class StringOrListDeserializer extends StdDeserializer<List<String>> {

        public StringOrListDeserializer() {
            super(List.class);
        }

        @Override
        public List<String> deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
            JsonNode node = p.getCodec().readTree(p);
            if (node.isArray()) {
                List<String> result = new ArrayList<>(node.size());
                for (JsonNode elem : node) {
                    result.add(elem.asText());
                }
                return result;
            }
            // Scalar string — Jinja template like "{{ config['regions'] }}"; store as single-element
            // list so callers can detect it (starts with "{{") and handle at codegen time.
            return Collections.singletonList(node.asText());
        }
    }

    /**
     * A {@code stream:} block inside parent_stream_configs. May appear as:
     *   stream: { $ref: "#/definitions/streams/courses" }
     * where ManifestParser pre-resolves the ref into the full stream subtree.
     * After resolution, the {@code name} field is populated from the target stream;
     * before resolution (or for raw-ref inspection) {@code ref} carries the pointer.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class StreamRef {

        @JsonProperty("$ref")
        private String ref;

        @JsonProperty("name")
        private String name;

        public String getRef() {
            return ref;
        }

        public void setRef(String ref) {
            this.ref = ref;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        /**
         * Returns the parent stream's name. Prefers the resolved {@code name} field
         * (populated after ManifestParser pre-resolves the {@code $ref}); falls back
         * to deriving the name from the raw ref pointer.
         */
        public String refStreamName() {
            if (name != null && !name.isEmpty()) return name;
            if (ref == null || ref.isEmpty()) return null;
            int last = ref.lastIndexOf('/');
            return last >= 0 ? ref.substring(last + 1) : ref;
        }
    }
}
