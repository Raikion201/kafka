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
 * Java port of {@code source_google_ads.components.GoogleAdsGlobalStateMigration}
 * (components.py:821-831).
 *
 * <p>Adds {@code use_global_cursor: true} to legacy state that pre-dates the new
 * GlobalSubstreamCursor flag. {@link #shouldMigrate} returns true when the state
 * is non-empty and lacks (or has falsy) {@code use_global_cursor}.</p>
 */
public final class GoogleAdsGlobalStateMigration implements CustomStateMigration {

    public GoogleAdsGlobalStateMigration(Map<String, String> connectorConfig,
                                         Map<String, Object> componentParams) {
        // No configurable state.
    }

    @Override
    public boolean shouldMigrate(Map<String, Object> state) {
        if (state == null || state.isEmpty()) {
            return false;
        }
        Object flag = state.get("use_global_cursor");
        return flag == null || (flag instanceof Boolean && !(Boolean) flag);
    }

    @Override
    public Map<String, Object> migrate(Map<String, Object> state) {
        Map<String, Object> out = new LinkedHashMap<>(state);
        out.put("use_global_cursor", true);
        return out;
    }
}
