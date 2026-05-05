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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Java port of {@code source_google_ads.components.GoogleAdsCriterionParentStateMigration}
 * (components.py:806-818).
 *
 * <p>Wraps legacy state with a {@code parent_state.change_status} envelope so the
 * criterion incremental retriever can keep track of the parent stream's cursor. Only
 * runs when state is non-empty and lacks the {@code parent_state} key.</p>
 */
public final class GoogleAdsCriterionParentStateMigration implements CustomStateMigration {

    public GoogleAdsCriterionParentStateMigration(Map<String, String> connectorConfig,
                                                  Map<String, Object> componentParams) {
        // No configurable state.
    }

    @Override
    public boolean shouldMigrate(Map<String, Object> state) {
        return state != null && !state.isEmpty() && !state.containsKey("parent_state");
    }

    @Override
    public Map<String, Object> migrate(Map<String, Object> state) {
        if (!shouldMigrate(state)) {
            return state;
        }
        Map<String, Object> parentState = new LinkedHashMap<>();
        parentState.put("change_status", state);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("parent_state", parentState);
        return out;
    }
}
