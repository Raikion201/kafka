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
    public List<StreamSpec> resolvedStreams() {
        if (streams.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, StreamSpec> defined = definitions != null && definitions.getStreams() != null
            ? definitions.getStreams()
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
            if (c == '_' || c == '-' || c == ' ') {
                nextUpper = true;
            } else if (nextUpper) {
                sb.append(Character.toUpperCase(c));
                nextUpper = false;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
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

        private String type;
        private String description;
        private String title;

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

        /** Returns the best available documentation string for this property. */
        public String effectiveDoc() {
            if (description != null && !description.isBlank()) {
                return description;
            }
            return Objects.toString(title, "");
        }
    }

    /** Models the {@code definitions} block — reusable stream and requester definitions. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DefinitionsDef {

        private Map<String, StreamSpec> streams;

        @JsonProperty("base_requester")
        private RequesterSpec baseRequester;

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
    }
}
