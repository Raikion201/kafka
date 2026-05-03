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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Handles the {@code response_filters} YAML field, which Airbyte manifests
 * write in three shapes:
 * <ul>
 *   <li>A list of inline filter objects (most manifests).</li>
 *   <li>A single inline filter object.</li>
 *   <li>A single {@code $ref} object pointing into {@code definitions.response_filters.<name>}
 *       (e.g. {@code google_sheets.yaml}).</li>
 * </ul>
 *
 * <p>The first two are normalised to a list. {@code $ref}-only objects are returned as
 * an empty list — we do not currently resolve cross-document {@code $ref}s, and
 * {@link RequesterSpec.ErrorHandlerSpec#getSuccessHttpCodes()} (the only consumer)
 * degrades safely to default HTTP error handling.
 */
public class ResponseFilterListDeserializer extends StdDeserializer<List<RequesterSpec.ResponseFilterSpec>> {

    public ResponseFilterListDeserializer() {
        super(List.class);
    }

    @Override
    public List<RequesterSpec.ResponseFilterSpec> deserialize(JsonParser p, DeserializationContext ctx)
        throws IOException {
        JsonNode node = p.getCodec().readTree(p);
        if (node == null || node.isNull()) {
            return Collections.emptyList();
        }
        if (node instanceof ArrayNode arr) {
            List<RequesterSpec.ResponseFilterSpec> result = new ArrayList<>(arr.size());
            for (JsonNode elem : arr) {
                result.add(ctx.readTreeAsValue(elem, RequesterSpec.ResponseFilterSpec.class));
            }
            return result;
        }
        if (node instanceof ObjectNode obj) {
            // $ref-only object — we don't resolve cross-document refs here.
            if (obj.size() == 1 && obj.has("$ref")) {
                return Collections.emptyList();
            }
            return List.of(ctx.readTreeAsValue(obj, RequesterSpec.ResponseFilterSpec.class));
        }
        return Collections.emptyList();
    }
}
