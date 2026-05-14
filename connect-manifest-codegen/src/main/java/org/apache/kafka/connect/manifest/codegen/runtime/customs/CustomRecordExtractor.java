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
package org.apache.kafka.connect.manifest.codegen.runtime.customs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Java equivalent of Airbyte CDK {@code RecordExtractor} (and {@code CustomRecordExtractor}). */
@FunctionalInterface
public interface CustomRecordExtractor extends CustomComponent {

    /** Extract zero or more records from a parsed HTTP response body. */
    List<Map<String, Object>> extract(JsonNode response);

    /**
     * Extract records from the raw HTTP response body string.
     * The default implementation parses the body as JSON and delegates to {@link #extract(JsonNode)}.
     * Override this method to handle non-JSON response bodies (e.g., XML for RSS feeds).
     */
    default List<Map<String, Object>> extractFromRawBody(String body) {
        if (body == null || body.isBlank()) return Collections.emptyList();
        try {
            return extract(new ObjectMapper().readTree(body));
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse response body as JSON in custom extractor", e);
        }
    }
}
