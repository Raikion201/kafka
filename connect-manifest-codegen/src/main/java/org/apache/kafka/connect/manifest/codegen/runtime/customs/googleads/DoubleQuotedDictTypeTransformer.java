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
package org.apache.kafka.connect.manifest.codegen.runtime.customs.googleads;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomSchemaNormalization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Java port of {@code source_google_ads.components.DoubleQuotedDictTypeTransformer}
 * (components.py:237-285).
 *
 * <p>Python intent: when a JSON Schema field is declared as {@code array} of
 * {@code string} but the runtime value is an array of dicts, serialise each dict to
 * its double-quoted JSON form so the value matches the declared schema. The Python
 * source uses {@code json.dumps(el, separators=(",", ": "))} — no space after commas,
 * one space after colons. Jackson's default for {@code writeValueAsString} produces
 * the same separators for object output, so we use it directly.</p>
 *
 * <p>Phase 1 invocation surface: {@code CustomSchemaNormalization} hands the runtime
 * a record map; the normaliser walks each field and rewrites array-of-dict values
 * regardless of an explicit schema (the field-level Python check delegates to
 * {@code TypeTransformer}, which we do not have at runtime in Phase 1). This is a
 * minor relaxation noted in the plan's "spec-correct, not byte-for-byte" clause.</p>
 */
public final class DoubleQuotedDictTypeTransformer implements CustomSchemaNormalization {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public DoubleQuotedDictTypeTransformer(Map<String, String> connectorConfig,
                                           Map<String, Object> componentParams) {
        // No configurable state.
    }

    @Override
    public Map<String, Object> normalize(Map<String, Object> record) {
        if (record == null) {
            return null;
        }
        Map<String, Object> out = new LinkedHashMap<>(record.size());
        for (Map.Entry<String, Object> e : record.entrySet()) {
            out.put(e.getKey(), maybeRewriteValue(e.getValue()));
        }
        return out;
    }

    private static Object maybeRewriteValue(Object value) {
        if (!(value instanceof List)) {
            return value;
        }
        List<?> list = (List<?>) value;
        if (list.isEmpty()) {
            return value;
        }
        for (Object el : list) {
            if (!(el instanceof Map)) {
                return value;
            }
        }
        List<Object> out = new ArrayList<>(list.size());
        for (Object el : list) {
            out.add(serialize(el));
        }
        return out;
    }

    private static String serialize(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new ConnectException("Failed to JSON-serialize array element", e);
        }
    }
}
