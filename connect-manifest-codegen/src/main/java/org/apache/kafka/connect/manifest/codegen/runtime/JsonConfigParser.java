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
package org.apache.kafka.connect.manifest.codegen.runtime;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parses Kafka Connect config values (all strings) into typed objects for the
 * Jinja context. Manifests expect object-type config fields (e.g. {@code region},
 * {@code credentials}) to be real Maps so templates like
 * {@code config['region']['url_base']} work. Kafka Connect passes every config
 * value as a String; callers that submit a JSON object as the value get the
 * string JSON-parsed back to a Map/List here.
 */
public final class JsonConfigParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonConfigParser() {
    }

    /**
     * Returns a copy of {@code configValues} where every String value that looks
     * like a JSON object ({@code {…}}) or array ({@code […]}) is replaced by the
     * parsed Java type ({@code Map} or {@code List}). Non-JSON strings and
     * non-String values are kept as-is.
     */
    public static Map<String, Object> parseObjectValues(Map<String, Object> configValues) {
        if (configValues == null) {
            return null;
        }
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : configValues.entrySet()) {
            result.put(entry.getKey(), maybeParseJson(entry.getValue()));
        }
        return result;
    }

    private static Object maybeParseJson(Object value) {
        if (!(value instanceof String)) {
            return value;
        }
        String s = ((String) value).trim();
        if (!isJsonStructure(s)) {
            return value;
        }
        try {
            return MAPPER.readValue(s, Object.class);
        } catch (JsonProcessingException e) {
            return value;
        }
    }

    private static boolean isJsonStructure(String s) {
        return (s.startsWith("{") && s.endsWith("}"))
            || (s.startsWith("[") && s.endsWith("]"));
    }
}
