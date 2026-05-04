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
     * Parse a manifest from a file.
     *
     * @throws ManifestParseException if the file cannot be read or does not conform to the expected structure
     */
    public ManifestSpec parse(File file) throws ManifestParseException {
        try {
            JsonNode root = YAML.readTree(file);
            resolveRefs(root);
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
