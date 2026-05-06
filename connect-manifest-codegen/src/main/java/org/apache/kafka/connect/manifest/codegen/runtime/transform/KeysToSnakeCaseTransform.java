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
package org.apache.kafka.connect.manifest.codegen.runtime.transform;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Java port of Airbyte's {@code keys_to_snake_transformation.py KeysToSnakeCaseTransformation}.
 * Normalizes each map key to snake_case, recursing into nested maps.
 *
 * <p>Python uses {@code anyascii} for transliteration; Java uses
 * {@link Normalizer#normalize} with NFKD + strip non-ASCII, which covers Latin diacritics
 * but not CJK. This parity gap is acceptable — corpus manifests do not use non-Latin keys.
 */
public final class KeysToSnakeCaseTransform implements RecordTransformation {

    public static final KeysToSnakeCaseTransform INSTANCE = new KeysToSnakeCaseTransform();

    // Splits on any run of non-alphanumeric
    private static final Pattern SPLITTER = Pattern.compile("[^a-zA-Z0-9]+");
    // Camel-case boundary: lowercase/digit followed by uppercase
    private static final Pattern CAMEL_LOWER_UPPER = Pattern.compile("([a-z0-9])([A-Z])");
    // Camel-case boundary: run of uppercase followed by uppercase+lowercase (e.g. "ABCDef" → "ABC_Def")
    private static final Pattern CAMEL_UPPER_RUN = Pattern.compile("([A-Z]+)([A-Z][a-z])");

    @Override
    public void apply(Map<String, Object> record, Map<String, Object> ctx) {
        snakifyKeys(record);
    }

    @SuppressWarnings("unchecked")
    private static void snakifyKeys(Map<String, Object> map) {
        Map<String, Object> result = new LinkedHashMap<>(map.size());
        for (Map.Entry<String, Object> e : map.entrySet()) {
            String newKey = toSnakeCase(e.getKey());
            Object value = e.getValue();
            if (value instanceof Map) {
                snakifyKeys((Map<String, Object>) value);
            }
            result.put(newKey, value);
        }
        map.clear();
        map.putAll(result);
    }

    static String toSnakeCase(String key) {
        if (key == null || key.isEmpty()) {
            return key;
        }
        // Normalize diacritics (NFKD) and strip non-ASCII — Java's anyascii approximation.
        String normalized = Normalizer.normalize(key, Normalizer.Form.NFKD)
            .replaceAll("[^\\x00-\\x7F]", "");

        // Insert _ on camelCase boundaries before splitting
        String withBoundaries = CAMEL_UPPER_RUN.matcher(
            CAMEL_LOWER_UPPER.matcher(normalized).replaceAll("$1_$2"))
            .replaceAll("$1_$2");

        String[] tokens = SPLITTER.split(withBoundaries);
        List<String> parts = new ArrayList<>();
        for (String token : tokens) {
            if (!token.isEmpty()) {
                parts.add(token.toLowerCase(Locale.ROOT));
            }
        }
        if (parts.isEmpty()) {
            return key.toLowerCase(Locale.ROOT);
        }
        // Python special case: leading-digit token gets empty-string prepended (joins as "_token").
        if (Character.isDigit(parts.get(0).charAt(0))) {
            parts.add(0, "");
        }
        return String.join("_", parts);
    }
}
