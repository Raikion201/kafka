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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomRecordExtractor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Stub port of Klaviyo's {@code KlaviyoIncludedFieldExtractor}.
 *
 * <p>Klaviyo responses follow JSON:API format with a top-level {@code data} array
 * (main records) and an optional {@code included} array (related resources). This
 * extractor returns the {@code data} records. The {@code included} merge is stubbed —
 * records will be produced without related-resource fields.</p>
 *
 * <p>Registered under
 * {@code source_declarative_manifest.components.KlaviyoIncludedFieldExtractor}.</p>
 */
public final class KlaviyoIncludedFieldExtractor implements CustomRecordExtractor {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE =
        new TypeReference<Map<String, Object>>() { };
    private static final TypeReference<List<Map<String, Object>>> LIST_MAP_TYPE =
        new TypeReference<List<Map<String, Object>>>() { };

    public KlaviyoIncludedFieldExtractor(Map<String, String> connectorConfig,
                                         Map<String, Object> componentParams) {
    }

    @Override
    public List<Map<String, Object>> extract(JsonNode response) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (response == null || response.isNull()) {
            return result;
        }
        JsonNode data = response.get("data");
        if (data == null || data.isNull()) {
            return result;
        }
        if (data.isArray()) {
            return MAPPER.convertValue(data, LIST_MAP_TYPE);
        }
        if (data.isObject()) {
            result.add(MAPPER.convertValue(data, MAP_TYPE));
        }
        return result;
    }
}
