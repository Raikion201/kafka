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

import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomTransformation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Java port of {@code source_google_ads.components.KeysToSnakeCaseGoogleAdsTransformation}
 * (components.py:445-506).
 *
 * <p>Converts camelCase / PascalCase dict keys into snake_case, recursing into nested
 * dicts. Differs from a naive snake_case in two ways inherited from Python:</p>
 * <ul>
 *     <li>Digits do not cause an underscore to be inserted (lookbehind/look-ahead).</li>
 *     <li>When the tokenised form has 3+ tokens, empty/separator tokens between the
 *         first and last are dropped (matches Python {@code filter_tokens}).</li>
 * </ul>
 *
 * <p>Lists in the record are passed through unchanged because the Python implementation
 * does the same — it only recurses into nested dicts.</p>
 */
public final class KeysToSnakeCaseGoogleAdsTransformation implements CustomTransformation {

    /**
     * Mirrors the Python regex on components.py:452. Three alternations:
     *   1. Optional leading digits, uppercase run, optional lowercase run, optional trailing digits.
     *   2. Optional leading digits, lowercase run, optional trailing digits.
     *   3. Any non-alphanumeric separator(s) — captured as a NoToken group so they can be filtered.
     */
    private static final Pattern TOKEN_PATTERN = Pattern.compile(
        "\\d*[A-Z]+[a-z]*\\d*"
            + "|\\d*[a-z]+\\d*"
            + "|(?<NoToken>[^a-zA-Z\\d]+)");

    @SuppressWarnings("unused")
    public KeysToSnakeCaseGoogleAdsTransformation(Map<String, String> connectorConfig,
                                                  Map<String, Object> componentParams) {
        // No configurable state in Phase 1.
    }

    @Override
    public Map<String, Object> transform(Map<String, Object> record) {
        if (record == null) {
            return null;
        }
        return transformRecord(record);
    }

    private Map<String, Object> transformRecord(Map<String, Object> record) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : record.entrySet()) {
            String newKey = processKey(e.getKey());
            Object value = e.getValue();
            if (value instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> nested = (Map<String, Object>) value;
                value = transformRecord(nested);
            }
            out.put(newKey, value);
        }
        return out;
    }

    /** Visible for tests. */
    String processKey(String key) {
        if (key == null || key.isEmpty()) {
            return key;
        }
        List<String> tokens = tokenize(key);
        tokens = filterTokens(tokens);
        return tokensToSnakeCase(tokens);
    }

    private List<String> tokenize(String key) {
        List<String> tokens = new ArrayList<>();
        Matcher m = TOKEN_PATTERN.matcher(key);
        while (m.find()) {
            // NoToken group means we matched a separator run; emit empty placeholder
            // (matches Python: token = match.group(0) if match.group("NoToken") is None else "")
            String token = m.group("NoToken") == null ? m.group(0) : "";
            tokens.add(token);
        }
        return tokens;
    }

    private List<String> filterTokens(List<String> tokens) {
        if (tokens.size() < 3) {
            return tokens;
        }
        List<String> out = new ArrayList<>();
        out.add(tokens.get(0));
        for (int i = 1; i < tokens.size() - 1; i++) {
            if (!tokens.get(i).isEmpty()) {
                out.add(tokens.get(i));
            }
        }
        out.add(tokens.get(tokens.size() - 1));
        return out;
    }

    private String tokensToSnakeCase(List<String> tokens) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tokens.size(); i++) {
            if (i > 0) {
                sb.append('_');
            }
            sb.append(tokens.get(i).toLowerCase(Locale.ROOT));
        }
        return sb.toString();
    }
}
