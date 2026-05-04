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

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Top-level model for an Airbyte-style connector manifest.yaml.
 *
 * <p>Airbyte manifests use two patterns for streams:
 * <ol>
 *   <li>Inline: {@code streams[].name} and {@code streams[].retriever} defined directly.</li>
 *   <li>Referenced: {@code streams[]} contains only a {@code $ref} pointer; the actual stream
 *       definition lives in {@code definitions.streams.<name>}.</li>
 * </ol>
 * {@link #resolvedStreams()} returns fully populated streams for both patterns.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ManifestSpec {

    private String version;
    private String type;
    private String description;
    private List<StreamSpec> streams = Collections.emptyList();

    @JsonProperty("dynamic_streams")
    private List<DynamicStreamSpec> dynamicStreams = Collections.emptyList();

    private SpecDef spec;
    private DefinitionsDef definitions;
    private String manifestName;

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<StreamSpec> getStreams() {
        return streams;
    }

    public void setStreams(List<StreamSpec> streams) {
        this.streams = streams == null ? Collections.emptyList() : streams;
    }

    public SpecDef getSpec() {
        return spec;
    }

    public void setSpec(SpecDef spec) {
        this.spec = spec;
    }

    public DefinitionsDef getDefinitions() {
        return definitions;
    }

    public void setDefinitions(DefinitionsDef definitions) {
        this.definitions = definitions;
    }

    public String getManifestName() {
        return manifestName;
    }

    public void setManifestName(String manifestName) {
        this.manifestName = manifestName;
    }

    /**
     * Returns the streams with full definitions resolved.
     *
     * <ul>
     *   <li>Inline streams (name + retriever present) are returned as-is.</li>
     *   <li>{@code $ref}-only entries are resolved from {@code definitions.streams}.</li>
     *   <li>If a resolved stream's requester has no url/url_base, the {@code url_base} is
     *       inherited from {@code definitions.base_requester}.</li>
     * </ul>
     */
    public List<DynamicStreamSpec> getDynamicStreams() {
        return dynamicStreams == null ? Collections.emptyList() : dynamicStreams;
    }

    public void setDynamicStreams(List<DynamicStreamSpec> v) {
        this.dynamicStreams = v == null ? Collections.emptyList() : v;
    }

    public List<StreamSpec> resolvedStreams() {
        if (streams.isEmpty()) {
            return synthesiseDynamicStreams();
        }
        Map<String, StreamSpec> defined = definitions != null
            ? definitions.allStreamDefs()
            : Collections.emptyMap();

        RequesterSpec baseRequester = definitions != null ? definitions.getBaseRequester() : null;

        java.util.Set<String> seen = new java.util.LinkedHashSet<>();
        List<StreamSpec> result = new ArrayList<>();
        for (StreamSpec s : streams) {
            if (s.getName() != null && s.getRetriever() != null) {
                // Inline stream — use as-is.
                if (seen.add(s.getName())) {
                    applyBaseRequester(s, baseRequester);
                    result.add(s);
                }
            } else if (s.refStreamName() != null) {
                // $ref entry — look up only the referenced stream by name.
                StreamSpec def = defined.get(s.refStreamName());
                if (def != null && def.getName() != null && seen.add(def.getName())) {
                    applyBaseRequester(def, baseRequester);
                    result.add(def);
                }
            } else {
                // Legacy fallback for manifests where all streams are inlined under definitions
                // and the top-level streams list has no names or refs (e.g. older format).
                for (StreamSpec def : defined.values()) {
                    if (def.getName() != null && seen.add(def.getName())) {
                        applyBaseRequester(def, baseRequester);
                        result.add(def);
                    }
                }
            }
        }
        return result;
    }

    /**
     * Synthesises StreamSpecs from the top-level {@code dynamic_streams:} block, used when
     * a manifest declares no explicit {@code streams:} (e.g. google_sheets). Each synthesised
     * stream carries {@link StreamSpec#isDynamic()} so the codegen knows to emit runtime
     * stream discovery rather than treating the placeholder name as authoritative.
     */
    private List<StreamSpec> synthesiseDynamicStreams() {
        if (getDynamicStreams().isEmpty()) {
            return Collections.emptyList();
        }
        RequesterSpec baseRequester = definitions != null ? definitions.getBaseRequester() : null;
        List<StreamSpec> synth = new ArrayList<>();
        int i = 0;
        for (DynamicStreamSpec ds : getDynamicStreams()) {
            StreamSpec template = ds.getStreamTemplate();
            if (template == null || template.getRetriever() == null) {
                continue;
            }
            template.setDynamic(true);
            template.setDiscoveryRequester(ds.discoveryRequester());
            if (template.getName() == null || template.getName().isBlank()) {
                template.setName("dynamic_stream_" + i);
            }
            applyBaseRequester(template, baseRequester);
            synth.add(template);
            i++;
        }
        return synth;
    }

    private static void applyBaseRequester(StreamSpec stream, RequesterSpec baseRequester) {
        if (baseRequester == null) {
            return;
        }
        if (stream.getRetriever() == null || stream.getRetriever().getRequester() == null) {
            return;
        }
        RequesterSpec req = stream.getRetriever().getRequester();
        if (req.effectiveBaseUrl().isBlank() && !baseRequester.effectiveBaseUrl().isBlank()) {
            req.setUrlBase(baseRequester.effectiveBaseUrl());
        }
        if (req.getAuthenticator() == null && baseRequester.getAuthenticator() != null) {
            req.setAuthenticator(baseRequester.getAuthenticator());
        }
    }

    /** Returns a connector class name derived from the manifest filename (preferred) or first stream name. */
    public String connectorClassName() {
        if (manifestName != null && !manifestName.isEmpty()) {
            return toClassName(manifestName) + "Source";
        }
        List<StreamSpec> resolved = resolvedStreams();
        if (resolved.isEmpty()) {
            return "GeneratedSource";
        }
        return toClassName(resolved.get(0).getName()) + "Source";
    }

    /** Converts snake_case or kebab-case to PascalCase. */
    public static String toClassName(String name) {
        if (name == null || name.isEmpty()) {
            return "Generated";
        }
        StringBuilder sb = new StringBuilder();
        boolean nextUpper = true;
        for (char c : name.toCharArray()) {
            if (c == '_' || c == '-' || c == ' ' || c == '&' || c == '/' || c == '(') {
                nextUpper = true;
            } else if (Character.isLetterOrDigit(c)) {
                if (nextUpper) {
                    sb.append(Character.toUpperCase(c));
                    nextUpper = false;
                } else {
                    sb.append(c);
                }
            }
            // skip any other non-alphanumeric, non-separator characters
        }
        String result = sb.toString();
        // Java identifiers cannot start with a digit — prefix with "S" (for Source)
        if (!result.isEmpty() && Character.isDigit(result.charAt(0))) {
            result = "S" + result;
        }
        return result.isEmpty() ? "Generated" : result;
    }

    /** Models the {@code spec} block — the user-facing configuration schema. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SpecDef {

        @JsonProperty("connection_specification")
        private ConnectionSpec connectionSpecification;

        public ConnectionSpec getConnectionSpecification() {
            return connectionSpecification == null ? new ConnectionSpec() : connectionSpecification;
        }

        public void setConnectionSpecification(ConnectionSpec v) {
            this.connectionSpecification = v;
        }
    }

    /**
     * Models the {@code connection_specification} block — the typed configuration schema
     * with a map of property definitions and a list of required property keys.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ConnectionSpec {

        private Map<String, PropertyDef> properties = Collections.emptyMap();
        private List<String> required = Collections.emptyList();

        public Map<String, PropertyDef> getProperties() {
            return properties == null ? Collections.emptyMap() : properties;
        }

        public void setProperties(Map<String, PropertyDef> properties) {
            this.properties = properties;
        }

        public List<String> getRequired() {
            return required == null ? Collections.emptyList() : required;
        }

        public void setRequired(List<String> required) {
            this.required = required;
        }
    }

    /** Models a single property definition within {@code connection_specification.properties}. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PropertyDef {

        @com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = StringOrArrayDeserializer.class)
        private String type;
        private String description;
        private String title;

        @JsonProperty("default")
        private Object defaultValue;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public Object getDefaultValue() {
            return defaultValue;
        }

        public void setDefaultValue(Object defaultValue) {
            this.defaultValue = defaultValue;
        }

        public boolean hasDefault() {
            return defaultValue != null;
        }

        /** Returns the best available documentation string for this property. */
        public String effectiveDoc() {
            if (description != null && !description.isBlank()) {
                return description;
            }
            return Objects.toString(title, "");
        }

        /**
         * Tolerant deserializer for {@code PropertyDef.type}. JSON Schema allows
         * {@code "type": ["string", "null"]} (an array of allowed types) — pick the first
         * non-null entry as the effective type so the rest of the codegen sees a plain string.
         */
        static final class StringOrArrayDeserializer
                extends com.fasterxml.jackson.databind.JsonDeserializer<String> {
            @Override
            public String deserialize(com.fasterxml.jackson.core.JsonParser p,
                                      com.fasterxml.jackson.databind.DeserializationContext ctxt)
                    throws java.io.IOException {
                com.fasterxml.jackson.databind.JsonNode n = p.readValueAsTree();
                if (n == null || n.isNull()) return null;
                if (n.isTextual()) return n.asText();
                if (n.isArray()) {
                    for (com.fasterxml.jackson.databind.JsonNode child : n) {
                        if (child.isTextual() && !"null".equalsIgnoreCase(child.asText())) {
                            return child.asText();
                        }
                    }
                    return null;
                }
                return n.toString();
            }
        }
    }

    /** Models the {@code definitions} block — reusable stream and requester definitions. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DefinitionsDef {

        private Map<String, StreamSpec> streams;

        @JsonProperty("base_requester")
        private RequesterSpec baseRequester;

        /** Catches top-level definition entries that are stream definitions (not under streams: subkey). */
        private final Map<String, Object> topLevelDefs = new java.util.LinkedHashMap<>();

        private static final ObjectMapper MAPPER = new ObjectMapper();

        public Map<String, StreamSpec> getStreams() {
            return streams == null ? Collections.emptyMap() : streams;
        }

        public void setStreams(Map<String, StreamSpec> streams) {
            this.streams = streams;
        }

        public RequesterSpec getBaseRequester() {
            return baseRequester;
        }

        public void setBaseRequester(RequesterSpec baseRequester) {
            this.baseRequester = baseRequester;
        }

        @JsonAnySetter
        public void setTopLevelDef(String key, Object value) {
            topLevelDefs.put(key, value);
        }

        /** Returns a merged view of streams: definitions.streams + top-level StreamSpec entries. */
        public Map<String, StreamSpec> allStreamDefs() {
            Map<String, StreamSpec> result = new java.util.LinkedHashMap<>(getStreams());
            for (Map.Entry<String, Object> e : topLevelDefs.entrySet()) {
                if (e.getValue() instanceof Map) {
                    try {
                        StreamSpec s = MAPPER.convertValue(e.getValue(), StreamSpec.class);
                        if (s.getName() != null || s.getRetriever() != null) {
                            result.putIfAbsent(e.getKey(), s);
                        }
                    } catch (Exception ignored) { /* not a stream */ }
                }
            }
            return result;
        }
    }
}
