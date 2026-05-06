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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

/**
 * Models a single stream entry inside the {@code streams} list of a manifest.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamSpec {

    private String type;
    private String name;

    /** Holds the raw {@code $ref} value when a stream entry is a reference, e.g. {@code "#/definitions/streams/foo"}. */
    @JsonProperty("$ref")
    private String ref;

    /** Deserializes plain-string stream entries like {@code "- \"#/definitions/my_stream\""} in the streams list. */
    @JsonCreator
    public static StreamSpec fromString(String ref) {
        StreamSpec s = new StreamSpec();
        s.ref = ref;
        return s;
    }

    @JsonProperty("primary_key")
    @com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = PrimaryKeyDeserializer.class)
    private List<String> primaryKey = Collections.emptyList();

    private RetrieverSpec retriever;

    @JsonProperty("incremental_sync")
    private IncrementalSyncSpec incrementalSync;

    private List<TransformationSpec> transformations = Collections.emptyList();

    /**
     * Marker set by ManifestSpec when this stream was synthesised from a top-level
     * {@code dynamic_streams:} block. The codegen task will discover concrete stream
     * names at connector startup rather than treating {@link #name} as authoritative.
     */
    private boolean dynamic;

    public boolean isDynamic() {
        return dynamic;
    }

    public void setDynamic(boolean dynamic) {
        this.dynamic = dynamic;
    }

    /** When isDynamic, the components_resolver requester used for sheet/stream discovery. */
    private RequesterSpec discoveryRequester;

    public RequesterSpec getDiscoveryRequester() {
        return discoveryRequester;
    }

    public void setDiscoveryRequester(RequesterSpec discoveryRequester) {
        this.discoveryRequester = discoveryRequester;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getRef() {
        return ref;
    }

    public void setRef(String ref) {
        this.ref = ref;
    }

    /**
     * Extracts the stream name from a {@code $ref} like {@code "#/definitions/streams/foo"}.
     * Returns {@code null} if this entry is not a ref or the ref path is unrecognised.
     */
    public String refStreamName() {
        if (ref == null) return null;
        // Format: "#/definitions/streams/<name>"
        int idx = ref.lastIndexOf('/');
        return idx >= 0 ? ref.substring(idx + 1) : null;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name == null ? null : name.trim();
    }

    public List<String> getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(List<String> primaryKey) {
        this.primaryKey = primaryKey == null ? Collections.emptyList() : primaryKey;
    }

    public RetrieverSpec getRetriever() {
        return retriever;
    }

    public void setRetriever(RetrieverSpec retriever) {
        this.retriever = retriever;
    }

    public IncrementalSyncSpec getIncrementalSync() {
        return incrementalSync;
    }

    public void setIncrementalSync(IncrementalSyncSpec incrementalSync) {
        this.incrementalSync = incrementalSync;
    }

    public List<TransformationSpec> getTransformations() {
        return transformations == null ? Collections.emptyList() : transformations;
    }

    public void setTransformations(List<TransformationSpec> transformations) {
        this.transformations = transformations == null ? Collections.emptyList() : transformations;
    }

    /**
     * Accepts {@code primary_key} as a string ({@code "id"}), a list of strings
     * ({@code ["id"]}), or a list of lists ({@code [["id"]]}) — Airbyte uses all three forms.
     * Flattens to a single list of column names.
     */
    static final class PrimaryKeyDeserializer extends com.fasterxml.jackson.databind.JsonDeserializer<List<String>> {
        @Override
        public List<String> deserialize(com.fasterxml.jackson.core.JsonParser p,
                                        com.fasterxml.jackson.databind.DeserializationContext ctxt)
                throws java.io.IOException {
            com.fasterxml.jackson.databind.JsonNode node = p.readValueAsTree();
            List<String> out = new java.util.ArrayList<>();
            collect(node, out);
            return out;
        }

        private void collect(com.fasterxml.jackson.databind.JsonNode n, List<String> out) {
            if (n == null || n.isNull()) return;
            if (n.isTextual()) {
                out.add(n.asText());
            } else if (n.isArray()) {
                for (com.fasterxml.jackson.databind.JsonNode child : n) collect(child, out);
            }
        }
    }
}
