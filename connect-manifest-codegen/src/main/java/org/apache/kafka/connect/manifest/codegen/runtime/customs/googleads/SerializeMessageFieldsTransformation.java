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
import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomTransformation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Java port of {@code source_google_ads.components.SerializeMessageFieldsTransformation}
 * (components.py:189-234).
 *
 * <p>Walks dotted MESSAGE-field paths populated by {@code CustomGAQuerySchemaLoader}
 * and replaces dict / list-of-dict values at those paths with their JSON-serialized
 * string form, so that downstream {@code FlattenNestedDictsTransformation} does not
 * recurse into them and lose the original blob.</p>
 *
 * <p>Phase 1 sources the message-field set from {@link CustomGAQuerySchemaLoader#allMessageFields()}
 * to mirror Python's class-level shared {@code _all_message_fields} set; can also be
 * overridden via the {@code messageFields} component param for unit tests.</p>
 */
public final class SerializeMessageFieldsTransformation implements CustomTransformation {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Set<String> overrideFields;

    public SerializeMessageFieldsTransformation(Map<String, String> connectorConfig,
                                                Map<String, Object> componentParams) {
        Object override = componentParams == null ? null : componentParams.get("messageFields");
        if (override instanceof Collection) {
            Set<String> set = new HashSet<>();
            for (Object o : (Collection<?>) override) {
                set.add(String.valueOf(o));
            }
            this.overrideFields = set;
        } else {
            this.overrideFields = null;
        }
    }

    @Override
    public Map<String, Object> transform(Map<String, Object> record) {
        if (record == null) {
            return null;
        }
        Set<String> fields = overrideFields != null ? overrideFields : CustomGAQuerySchemaLoader.allMessageFields();
        if (fields.isEmpty()) {
            return record;
        }

        for (String fieldName : fields) {
            applyAtPath(record, fieldName);
        }
        return record;
    }

    private void applyAtPath(Map<String, Object> record, String fieldName) {
        String[] parts = fieldName.split("\\.");
        Object current = record;
        for (int i = 0; i < parts.length - 1; i++) {
            if (current instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> m = (Map<String, Object>) current;
                if (m.containsKey(parts[i])) {
                    current = m.get(parts[i]);
                    continue;
                }
            }
            return;
        }
        if (!(current instanceof Map)) {
            return;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> tail = (Map<String, Object>) current;
        String last = parts[parts.length - 1];
        if (!tail.containsKey(last)) {
            return;
        }
        Object value = tail.get(last);
        if (value instanceof Map) {
            tail.put(last, serialize(value));
        } else if (value instanceof List) {
            List<?> in = (List<?>) value;
            List<Object> out = new ArrayList<>(in.size());
            for (Object item : in) {
                out.add(item instanceof Map ? serialize(item) : item);
            }
            tail.put(last, out);
        }
    }

    private static String serialize(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new ConnectException("Failed to serialize MESSAGE field value", e);
        }
    }
}
