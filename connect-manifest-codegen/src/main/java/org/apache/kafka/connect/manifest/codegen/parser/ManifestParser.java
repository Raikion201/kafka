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
package org.apache.kafka.connect.manifest.codegen.parser;

import org.apache.kafka.connect.manifest.codegen.model.ManifestSpec;
import org.apache.kafka.connect.manifest.codegen.model.StreamSpec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Parses an Airbyte-style {@code manifest.yaml} into a {@link ManifestSpec}.
 */
public class ManifestParser {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    /**
     * Strings starting with this prefix are treated as JSON Pointer refs and resolved
     * inline. Airbyte manifests put refs directly on object fields and inside map
     * values (e.g. {@code SelectiveAuthenticator.authenticators}) without the canonical
     * {@code {"$ref": "..."}} envelope. URL strings and other free text never start
     * with {@code "#/"}, so this prefix-match is unambiguous in practice.
     */
    private static final String STRING_REF_PREFIX = "#/";

    /** Bound on $ref resolution passes — protects against ref cycles. */
    private static final int MAX_REF_PASSES = 8;

    /**
     * Default component types keyed by {@code <ParentType>.<fieldName>}. Mirrors Python's
     * {@code DEFAULT_MODEL_TYPES} in
     * {@code airbyte_cdk/sources/declarative/parsers/manifest_component_transformer.py:12-70}.
     *
     * <p>When the parser visits a node that lacks an explicit {@code type:} field, it looks
     * up the entry for {@code <parent_type>.<field_name>}. If found, the type is set on the
     * node so that subsequent {@code $parameters} propagation treats it as a real component
     * (and applies inherited parameters as fields). Without this, e.g. mailchimp's typeless
     * {@code requester:} block never picks up the stream's {@code $parameters.path}.
     */
    private static final java.util.Map<String, String> DEFAULT_MODEL_TYPES = defaultModelTypes();

    private static java.util.Map<String, String> defaultModelTypes() {
        java.util.Map<String, String> m = new java.util.HashMap<>();
        m.put("CompositeErrorHandler.error_handlers", "DefaultErrorHandler");
        m.put("CursorPagination.decoder", "JsonDecoder");
        m.put("DatetimeBasedCursor.end_datetime", "MinMaxDatetime");
        m.put("DatetimeBasedCursor.end_time_option", "RequestOption");
        m.put("DatetimeBasedCursor.start_datetime", "MinMaxDatetime");
        m.put("DatetimeBasedCursor.start_time_option", "RequestOption");
        m.put("DeclarativeSource.check", "CheckStream");
        m.put("DeclarativeSource.spec", "Spec");
        m.put("DeclarativeSource.streams", "DeclarativeStream");
        m.put("DeclarativeStream.retriever", "SimpleRetriever");
        m.put("DeclarativeStream.schema_loader", "JsonFileSchemaLoader");
        m.put("DynamicDeclarativeStream.stream_template", "DeclarativeStream");
        m.put("DynamicDeclarativeStream.components_resolver", "ConfigComponentResolver");
        m.put("HttpComponentsResolver.retriever", "SimpleRetriever");
        m.put("HttpComponentsResolver.components_mapping", "ComponentMappingDefinition");
        m.put("ConfigComponentsResolver.stream_config", "StreamConfig");
        m.put("ConfigComponentsResolver.components_mapping", "ComponentMappingDefinition");
        m.put("DefaultErrorHandler.response_filters", "HttpResponseFilter");
        m.put("DefaultPaginator.decoder", "JsonDecoder");
        m.put("DefaultPaginator.page_size_option", "RequestOption");
        m.put("DpathExtractor.decoder", "JsonDecoder");
        m.put("DpathExtractor.record_expander", "RecordExpander");
        m.put("HttpRequester.error_handler", "DefaultErrorHandler");
        m.put("ListPartitionRouter.request_option", "RequestOption");
        m.put("ParentStreamConfig.request_option", "RequestOption");
        m.put("ParentStreamConfig.stream", "DeclarativeStream");
        m.put("RecordSelector.extractor", "DpathExtractor");
        m.put("RecordSelector.record_filter", "RecordFilter");
        m.put("SimpleRetriever.paginator", "NoPagination");
        m.put("SimpleRetriever.record_selector", "RecordSelector");
        m.put("SimpleRetriever.requester", "HttpRequester");
        m.put("SubstreamPartitionRouter.parent_stream_configs", "ParentStreamConfig");
        m.put("AddFields.fields", "AddedFieldDefinition");
        m.put("CustomPartitionRouter.parent_stream_configs", "ParentStreamConfig");
        m.put("DynamicSchemaLoader.retriever", "SimpleRetriever");
        m.put("SchemaTypeIdentifier.types_map", "TypesMap");
        return java.util.Collections.unmodifiableMap(m);
    }

    /**
     * Parse a manifest from a file.
     *
     * @throws ManifestParseException if the file cannot be read or does not conform to the expected structure
     */
    public ManifestSpec parse(File file) throws ManifestParseException {
        try {
            JsonNode root = YAML.readTree(file);
            resolveRefs(root);
            propagateStreamParameters(root);
            return validate(YAML.treeToValue(root, ManifestSpec.class));
        } catch (IOException e) {
            throw new ManifestParseException("Failed to parse manifest file: " + file.getPath(), e);
        }
    }

    /**
     * Parse a manifest from an input stream (e.g. classpath resource in tests).
     *
     * @throws ManifestParseException if parsing fails
     */
    public ManifestSpec parse(InputStream in) throws ManifestParseException {
        try {
            JsonNode root = YAML.readTree(in);
            resolveRefs(root);
            propagateStreamParameters(root);
            return validate(YAML.treeToValue(root, ManifestSpec.class));
        } catch (IOException e) {
            throw new ManifestParseException("Failed to parse manifest from stream", e);
        }
    }

    /**
     * Pre-resolves intra-document {@code $ref} pointers on the parsed YAML tree so that
     * downstream Jackson binding sees fully-inlined definitions.
     *
     * <p>Two ref shapes are handled:
     * <ul>
     *   <li>Object form: {@code {"$ref": "#/path/to/node"}} — replaced by the pointed-to subtree.</li>
     *   <li>String form on allowlisted fields (see {@link #STRING_REF_FIELDS}): the entire
     *       string value (e.g. {@code "#/definitions/authenticator"}) is replaced with the
     *       pointed-to subtree.</li>
     * </ul>
     *
     * <p>Resolution runs iteratively up to {@link #MAX_REF_PASSES} passes — enough for refs
     * that point at definitions which themselves contain refs (common in Airbyte manifests).
     */
    static void resolveRefs(JsonNode root) {
        if (!(root instanceof ObjectNode)) {
            return;
        }
        for (int pass = 0; pass < MAX_REF_PASSES; pass++) {
            boolean changed = resolvePass(root, root);
            if (!changed) {
                return;
            }
        }
    }

    private static boolean resolvePass(JsonNode node, JsonNode root) {
        boolean changed = false;
        if (node instanceof ObjectNode obj) {
            // Iterate over a snapshot of field names so we can mutate the object while walking.
            for (String name : obj.properties().stream().map(java.util.Map.Entry::getKey).toList()) {
                JsonNode child = obj.get(name);
                JsonNode replacement = maybeResolve(child, name, root);
                if (replacement != child) {
                    obj.set(name, replacement);
                    changed = true;
                    // Recurse into the replacement on subsequent passes — don't descend now to
                    // avoid mutating shared subtrees mid-walk.
                } else {
                    if (resolvePass(child, root)) changed = true;
                }
            }
        } else if (node instanceof ArrayNode arr) {
            for (int i = 0; i < arr.size(); i++) {
                JsonNode child = arr.get(i);
                JsonNode replacement = maybeResolve(child, null, root);
                if (replacement != child) {
                    arr.set(i, replacement);
                    changed = true;
                } else {
                    if (resolvePass(child, root)) changed = true;
                }
            }
        }
        return changed;
    }

    /**
     * If {@code node} is a ref (an object containing {@code "$ref": "#/..."} or a plain
     * string starting with {@code "#/"}), returns a deep copy of the pointed-to subtree.
     * Otherwise returns the node unchanged.
     */
    private static JsonNode maybeResolve(JsonNode node, String fieldName, JsonNode root) {
        if (node.isTextual()) {
            return resolveStringRef(node, root);
        }
        if (node instanceof ObjectNode obj && obj.has("$ref") && obj.get("$ref").isTextual()) {
            return resolveObjectRef(obj, root);
        }
        return node;
    }

    private static JsonNode resolveStringRef(JsonNode node, JsonNode root) {
        String text = node.asText();
        if (!text.startsWith(STRING_REF_PREFIX)) {
            return node;
        }
        JsonNode target = pointer(root, text);
        return (target == null || target.isMissingNode()) ? node : target.deepCopy();
    }

    private static JsonNode resolveObjectRef(ObjectNode obj, JsonNode root) {
        JsonNode target = pointer(root, obj.get("$ref").asText());
        if (target == null || target.isMissingNode()) {
            return obj;
        }
        if (obj.size() == 1) {
            return target.deepCopy();
        }
        // Object ref that carries sibling fields (e.g. {$ref: ..., http_method: GET}):
        // start from the ref target, overlay non-$ref siblings.
        if (target instanceof ObjectNode targetObj) {
            ObjectNode merged = targetObj.deepCopy();
            obj.properties().forEach(e -> {
                if (!"$ref".equals(e.getKey())) {
                    merged.set(e.getKey(), e.getValue().deepCopy());
                }
            });
            return merged;
        }
        return obj;
    }

    private static JsonNode pointer(JsonNode root, String ref) {
        // Airbyte refs use "#/foo/bar" — strip the leading "#" to get a JSON Pointer.
        String ptr = ref.startsWith("#") ? ref.substring(1) : ref;
        if (ptr.isEmpty()) return root;
        return root.at(ptr);
    }

    /**
     * Propagates {@code $parameters} down the manifest tree to mirror Airbyte's
     * {@code ManifestComponentTransformer.propagate_types_and_parameters}
     * (airbyte_cdk/sources/declarative/parsers/manifest_component_transformer.py:88-189).
     *
     * <p>Walking rules, in the order they apply at each object node:
     * <ol>
     *   <li>JSON-schema subtrees ({@code type: "object"} or {@code type: [..., "object", ...]})
     *       are skipped entirely — they're record shape descriptions, not declarative components.</li>
     *   <li>The node's local {@code $parameters} overlay the inherited parameters; same-named
     *       keys are taken from the local scope (Python: {@code component_parameters} wins over
     *       {@code parent_parameters} when {@code use_parent_parameters} is False, the default).</li>
     *   <li>For every parameter in the merged scope, if the component lacks that field, the
     *       parameter value is set as a top-level field. Existing fields are never overwritten.</li>
     *   <li>Recurse into each child field/element. When recursing into a child whose key matches
     *       a parameter name, that parameter is temporarily removed from scope to prevent
     *       infinite cycles (Python lines 160 + 168-169).</li>
     * </ol>
     *
     * <p>Notes:
     * <ul>
     *   <li>{@code $parameters} blocks are kept on the node after the walk so codegen can still
     *       interpolate {@code parameters['x']} Jinja templates against them (Python line 188).</li>
     *   <li>This replaces the previous path-only injection (which only set {@code requester.path}
     *       from {@code $parameters.path}). The generalized walk subsumes that case: when a stream
     *       sets {@code $parameters.path}, the value propagates down to {@code retriever.requester}
     *       and is set as {@code path} there.</li>
     * </ul>
     */
    static void propagateStreamParameters(JsonNode root) {
        if (!(root instanceof ObjectNode)) {
            return;
        }
        propagateParams(root, null, java.util.Collections.emptyMap());
    }

    private static void propagateParams(
            JsonNode node, String parentFieldIdentifier, java.util.Map<String, JsonNode> parentParams) {
        if (node instanceof ArrayNode arr) {
            arr.forEach(el -> propagateParams(el, parentFieldIdentifier, parentParams));
            return;
        }
        if (!(node instanceof ObjectNode obj) || isJsonSchemaObject(obj)) {
            return;
        }
        // Default-type lookup: when a node lacks `type` but its position under a typed parent
        // maps to a known default model type, set that type. Mirrors Python's
        // ManifestComponentTransformer (manifest_component_transformer.py:107-117). This is
        // what makes typeless `requester:` blocks behave as `HttpRequester`, typeless
        // `record_selector:` behave as `RecordSelector`, etc. — so `$parameters` set on the
        // enclosing stream (e.g. `path: automations`) propagate down as fields.
        if (!obj.has("type") && parentFieldIdentifier != null) {
            String defaultType = DEFAULT_MODEL_TYPES.get(parentFieldIdentifier);
            if (defaultType != null) {
                obj.put("type", defaultType);
            }
        }
        java.util.Map<String, JsonNode> current = mergeParams(parentParams, obj.get("$parameters"));
        if (!obj.has("type")) {
            // Still typeless after default lookup: not a declarative component (e.g.
            // SelectiveAuthenticator.authenticators map, or the bare definitions container).
            // Do not inject parameters as fields here — doing so corrupts value-maps. Recurse
            // into nested components only (Python: _process_nested_components — lines 198-222).
            recurseIntoNestedComponents(obj, parentFieldIdentifier, current);
            return;
        }
        applyParamsAsFields(obj, current);
        recurseIntoChildren(obj, current);
    }

    /**
     * Recurses into children that are themselves declarative components (objects with a
     * {@code type:} field) or arrays. Mirrors Python {@code _process_nested_components}: the
     * parent field identifier is passed through unchanged so typed grandchildren can still
     * benefit from default-type lookup against the original position.
     */
    private static void recurseIntoNestedComponents(
            ObjectNode obj, String parentFieldIdentifier, java.util.Map<String, JsonNode> scope) {
        java.util.List<String> childFields = new java.util.ArrayList<>();
        obj.fieldNames().forEachRemaining(childFields::add);
        for (String fieldName : childFields) {
            if ("$parameters".equals(fieldName)) {
                continue;
            }
            JsonNode child = obj.get(fieldName);
            if (child instanceof ObjectNode childObj && childObj.has("type")) {
                JsonNode excluded = scope.remove(fieldName);
                try {
                    propagateParams(child, parentFieldIdentifier, scope);
                } finally {
                    if (excluded != null) {
                        scope.put(fieldName, excluded);
                    }
                }
            } else if (child instanceof ArrayNode) {
                propagateParams(child, parentFieldIdentifier, scope);
            } else if (child instanceof ObjectNode childObj) {
                // Typeless nested object (e.g. a map of components like authenticators):
                // descend so we can still reach typed grandchildren.
                propagateParams(childObj, parentFieldIdentifier, scope);
            }
        }
    }

    private static java.util.Map<String, JsonNode> mergeParams(
            java.util.Map<String, JsonNode> parent, JsonNode localParams) {
        java.util.Map<String, JsonNode> merged = new java.util.LinkedHashMap<>(parent);
        if (localParams instanceof ObjectNode localObj) {
            localObj.properties().forEach(e -> merged.put(e.getKey(), e.getValue()));
        }
        return merged;
    }

    /** Sets each in-scope parameter as a top-level field on the component if not already present. */
    private static void applyParamsAsFields(ObjectNode obj, java.util.Map<String, JsonNode> params) {
        for (java.util.Map.Entry<String, JsonNode> e : params.entrySet()) {
            if (!obj.has(e.getKey())) {
                obj.set(e.getKey(), e.getValue().deepCopy());
            }
        }
    }

    /**
     * Recurses into each child field of {@code obj}. When descending into a child whose key
     * matches an in-scope parameter, that parameter is temporarily removed from the scope so
     * that the propagation does not produce {@code requester.requester} cycles or similar.
     *
     * <p>The parent's resolved {@code type} is used to form {@code <type>.<field>} identifiers
     * for each child — that's the key into {@link #DEFAULT_MODEL_TYPES}.
     */
    private static void recurseIntoChildren(ObjectNode obj, java.util.Map<String, JsonNode> scope) {
        String parentType = obj.get("type").asText();
        java.util.List<String> childFields = new java.util.ArrayList<>();
        obj.fieldNames().forEachRemaining(childFields::add);
        for (String fieldName : childFields) {
            if ("$parameters".equals(fieldName)) {
                continue;
            }
            JsonNode excluded = scope.remove(fieldName);
            try {
                propagateParams(obj.get(fieldName), parentType + "." + fieldName, scope);
            } finally {
                if (excluded != null) {
                    scope.put(fieldName, excluded);
                }
            }
        }
    }

    private static boolean isJsonSchemaObject(ObjectNode obj) {
        JsonNode type = obj.get("type");
        if (type == null) {
            return false;
        }
        if (type.isTextual()) {
            return "object".equals(type.asText());
        }
        if (type.isArray()) {
            for (JsonNode t : type) {
                if (t.isTextual() && "object".equals(t.asText())) {
                    return true;
                }
            }
        }
        return false;
    }

    private ManifestSpec validate(ManifestSpec spec) throws ManifestParseException {
        // Filter out malformed streams (missing retriever/requester/url) rather than throwing.
        // Manifests with no usable streams still parse; codegen routes them to a generic stub
        // task so the connector loads in Connect and fails fast on start() with a clear message.
        List<StreamSpec> filtered = new java.util.ArrayList<>();
        for (StreamSpec s : spec.getStreams()) {
            if (s.getName() == null || s.getName().isBlank()) continue;
            if (s.getRetriever() == null) continue;
            if (s.getRetriever().getRequester() == null) continue;
            filtered.add(s);
        }
        spec.setStreams(filtered);
        return spec;
    }
}
