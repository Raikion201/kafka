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

import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomStateMigration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Java port of {@code source_google_ads.components.GoogleAdsPerPartitionStateMigration}
 * (components.py:288-369).
 *
 * <p>Detects legacy per-customer state of shape
 * {@code {"<customer_id>": {"segments.date": "..."}}} and migrates it to the low-code
 * shape with a {@code states} list and a global {@code state} bookmark. Phase 1 omits
 * the parent-stream join (which Python uses to attach {@code parent_slice} per
 * partition) because it requires a live {@code customer_client} stream — we leave
 * {@code parent_slice} as an empty placeholder map. The Phase 2 retriever upgrade
 * will revisit this by querying the parent stream during start.</p>
 */
public final class GoogleAdsPerPartitionStateMigration implements CustomStateMigration {

    private final String cursorField;

    public GoogleAdsPerPartitionStateMigration(Map<String, String> connectorConfig,
                                               Map<String, Object> componentParams) {
        Object cf = componentParams == null ? null : componentParams.get("cursor_field");
        this.cursorField = cf == null ? "segments.date" : cf.toString();
    }

    @Override
    public boolean shouldMigrate(Map<String, Object> state) {
        // Python: `stream_state and "state" not in stream_state`
        return state != null && !state.isEmpty() && !state.containsKey("state");
    }

    @Override
    public Map<String, Object> migrate(Map<String, Object> state) {
        if (!shouldMigrate(state)) {
            return state;
        }

        // Collect per-customer cursor values that include the cursor field.
        List<Map<String, Object>> partitionsState = new ArrayList<>();
        Map<String, Object> minState = null;
        for (Map.Entry<String, Object> e : state.entrySet()) {
            if (!(e.getValue() instanceof Map)) {
                continue;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> cursor = (Map<String, Object>) e.getValue();
            if (!cursor.containsKey(cursorField)) {
                continue;
            }

            Map<String, Object> entry = new LinkedHashMap<>();
            Map<String, Object> partition = new LinkedHashMap<>();
            partition.put("customer_id", e.getKey());
            // Python uses parent_slice from the parent stream lookup; Phase 1 leaves it empty.
            partition.put("parent_slice", Collections.emptyMap());
            entry.put("partition", partition);
            entry.put("cursor", cursor);
            partitionsState.add(entry);

            if (minState == null || compareCursor(cursor, minState) < 0) {
                minState = cursor;
            }
        }

        if (partitionsState.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("states", partitionsState);
        out.put("state", minState);
        return out;
    }

    private int compareCursor(Map<String, Object> a, Map<String, Object> b) {
        Object av = a.get(cursorField);
        Object bv = b.get(cursorField);
        if (av == null && bv == null) {
            return 0;
        }
        if (av == null) {
            return -1;
        }
        if (bv == null) {
            return 1;
        }
        return av.toString().compareTo(bv.toString());
    }
}
