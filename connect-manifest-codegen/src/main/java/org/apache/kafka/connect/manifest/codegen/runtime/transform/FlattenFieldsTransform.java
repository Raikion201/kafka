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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Java port of Airbyte's {@code flatten_fields.py FlattenFields}.
 * DFS over the record, producing dot-separated keys for nested maps (and optionally lists).
 * Conflicts (a key that already exists at the top level) are resolved by prepending the
 * parent segment, matching the Python source's conflict-resolution logic.
 */
public final class FlattenFieldsTransform implements RecordTransformation {

    private final boolean flattenLists;

    public FlattenFieldsTransform(boolean flattenLists) {
        this.flattenLists = flattenLists;
    }

    @Override
    public void apply(Map<String, Object> record, Map<String, Object> ctx) {
        Map<String, Object> flat = new LinkedHashMap<>();
        flatten(record, "", flat);
        record.clear();
        record.putAll(flat);
    }

    @SuppressWarnings("unchecked")
    private void flatten(Object node, String prefix, Map<String, Object> out) {
        if (node instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) node;
            if (map.isEmpty() && !prefix.isEmpty()) {
                put(out, prefix, map);
                return;
            }
            for (Map.Entry<String, Object> e : map.entrySet()) {
                String key = prefix.isEmpty() ? e.getKey() : prefix + "." + e.getKey();
                flatten(e.getValue(), key, out);
            }
        } else if (flattenLists && node instanceof List) {
            List<?> list = (List<?>) node;
            for (int i = 0; i < list.size(); i++) {
                String key = prefix + "." + i;
                flatten(list.get(i), key, out);
            }
        } else {
            put(out, prefix, node);
        }
    }

    private static void put(Map<String, Object> out, String key, Object value) {
        if (!out.containsKey(key)) {
            out.put(key, value);
        } else {
            // Conflict: prepend the last parent segment so both entries survive.
            int dot = key.lastIndexOf('.');
            String prefix = dot >= 0 ? key.substring(0, dot) : "";
            String leaf = dot >= 0 ? key.substring(dot + 1) : key;
            String newKey = prefix.isEmpty() ? ("_" + leaf) : (prefix + "._" + leaf);
            out.put(newKey, value);
        }
    }

}
