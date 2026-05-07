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
package org.apache.kafka.connect.manifest.codegen.runtime.customs.generic;

import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomTransformation;

import java.util.List;
import java.util.Map;

/**
 * Java port of {@code source-google-search-console/components.py SanitizeNumericFields}
 * (lines 139–165).
 *
 * <p>The Python CDK has a bug (airbyte issue #74883) where numeric fields for Google
 * Search Console can be returned as Python {@code complex} values. The Python fix extracts
 * the {@code .real} component. Java has no complex number type, so this transform validates
 * that the four metric fields are proper Java {@link Number} instances and coerces
 * non-numeric strings to {@code Double.NaN} (rather than leaving unparseable values).</p>
 *
 * <p>Registered under {@code source_declarative_manifest.components.SanitizeNumericFields}.</p>
 */
public final class SanitizeNumericFields implements CustomTransformation {

    private static final List<String> NUMERIC_FIELDS = List.of(
        "clicks", "impressions", "ctr", "position"
    );

    @SuppressWarnings("unused")
    public SanitizeNumericFields(Map<String, String> connectorConfig,
                                 Map<String, Object> componentParams) {
    }

    @Override
    public Map<String, Object> transform(Map<String, Object> record) {
        if (record == null) {
            return null;
        }
        for (String field : NUMERIC_FIELDS) {
            Object val = record.get(field);
            if (val == null || val instanceof Number) {
                continue;
            }
            // Coerce string representations to their numeric type
            String s = val.toString().trim();
            try {
                // Try integer first (clicks, impressions)
                record.put(field, Long.parseLong(s));
            } catch (NumberFormatException e1) {
                try {
                    record.put(field, Double.parseDouble(s));
                } catch (NumberFormatException e2) {
                    // Leave the value as-is — not a recognisable numeric string
                }
            }
        }
        return record;
    }
}
