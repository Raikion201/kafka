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
 * Models an entry in the top-level {@code dynamic_streams:} list. Airbyte uses these
 * to declare streams whose concrete names and per-stream URL parameters are discovered
 * at runtime by querying a parent endpoint (e.g. for Google Sheets, the spreadsheet
 * metadata endpoint returns the list of sheets).
 *
 * <p>For codegen we extract just enough to drive the generated task:
 * <ul>
 *   <li>{@code stream_template.retriever} — the per-stream URL/auth template.</li>
 *   <li>{@code components_resolver.retriever.requester} — the discovery URL we hit
 *       once at connector start to learn the concrete stream names.</li>
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DynamicStreamSpec {

    @JsonProperty("stream_template")
    private StreamSpec streamTemplate;

    @JsonProperty("components_resolver")
    private ComponentsResolverSpec componentsResolver;

    public StreamSpec getStreamTemplate() {
        return streamTemplate;
    }

    public void setStreamTemplate(StreamSpec v) {
        this.streamTemplate = v;
    }

    public ComponentsResolverSpec getComponentsResolver() {
        return componentsResolver;
    }

    public void setComponentsResolver(ComponentsResolverSpec v) {
        this.componentsResolver = v;
    }

    /** The discovery requester (used at connector start to enumerate streams), or null. */
    public RequesterSpec discoveryRequester() {
        if (componentsResolver == null || componentsResolver.getRetriever() == null) {
            return null;
        }
        return componentsResolver.getRetriever().getRequester();
    }

    /** Returns the components_mapping entries (resolver "fill these fields with the discovered record values"). */
    public List<ComponentMappingDefinition> componentsMapping() {
        if (componentsResolver == null || componentsResolver.getComponentsMapping() == null) {
            return Collections.emptyList();
        }
        return componentsResolver.getComponentsMapping();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ComponentsResolverSpec {

        @JsonProperty("retriever")
        private RetrieverSpec retriever;

        @JsonProperty("components_mapping")
        private List<ComponentMappingDefinition> componentsMapping;

        public RetrieverSpec getRetriever() {
            return retriever;
        }

        public void setRetriever(RetrieverSpec v) {
            this.retriever = v;
        }

        public List<ComponentMappingDefinition> getComponentsMapping() {
            return componentsMapping;
        }

        public void setComponentsMapping(List<ComponentMappingDefinition> v) {
            this.componentsMapping = v;
        }
    }

    /**
     * Models a {@code ComponentMappingDefinition} entry. Each mapping says: at runtime,
     * evaluate {@link #value} as a Jinja template (in the context of {@code config},
     * {@code components_values=record}, {@code stream_slice}) and set the result at
     * {@link #fieldPath} inside a deep-copy of the stream template.
     *
     * <p>Mirrors {@code airbyte_cdk.sources.declarative.resolvers.components_resolver.ComponentMappingDefinition}
     * (python-cdk components_resolver.py lines 17-27).</p>
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ComponentMappingDefinition {

        @JsonProperty("field_path")
        private List<String> fieldPath;

        @JsonProperty("value")
        private String value;

        @JsonProperty("value_type")
        private String valueType;

        @JsonProperty("condition")
        private String condition;

        @JsonProperty("create_or_update")
        private Boolean createOrUpdate;

        public List<String> getFieldPath() {
            return fieldPath == null ? Collections.emptyList() : fieldPath;
        }

        public void setFieldPath(List<String> v) {
            this.fieldPath = v;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String v) {
            this.value = v;
        }

        public String getValueType() {
            return valueType;
        }

        public void setValueType(String v) {
            this.valueType = v;
        }

        public String getCondition() {
            return condition;
        }

        public void setCondition(String v) {
            this.condition = v;
        }

        public Boolean getCreateOrUpdate() {
            return createOrUpdate;
        }

        public void setCreateOrUpdate(Boolean v) {
            this.createOrUpdate = v;
        }
    }
}
