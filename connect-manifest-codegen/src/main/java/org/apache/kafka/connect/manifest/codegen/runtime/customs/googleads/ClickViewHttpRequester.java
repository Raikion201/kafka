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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Java port of {@code source_google_ads.components.ClickViewHttpRequester}
 * (components.py:425-442).
 *
 * <p>Special-cased one-day window query against the {@code click_view} resource;
 * Google's API enforces single-day pulls for this resource. Window comes from
 * the cursor slice's {@code start_time}.</p>
 */
public final class ClickViewHttpRequester extends BaseGoogleAdsHttpRequester {

    public ClickViewHttpRequester(Map<String, String> connectorConfig,
                                  Map<String, Object> componentParams) {
        super(connectorConfig, componentParams);
    }

    @Override
    public String buildQuery(Map<String, Object> partition, Map<String, Object> state) {
        String start = state == null ? "" : String.valueOf(state.getOrDefault("start_time", ""));
        StringBuilder sb = new StringBuilder("SELECT ");
        boolean first = true;
        for (String f : fieldsParam()) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(f);
            first = false;
        }
        sb.append(" FROM click_view WHERE segments.date = '").append(start).append('\'');
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private List<String> fieldsParam() {
        Object fields = componentParams.get("fields");
        if (fields instanceof List) {
            return (List<String>) fields;
        }
        if (fields instanceof Collection) {
            return ((Collection<?>) fields).stream().map(Object::toString).toList();
        }
        return Collections.emptyList();
    }
}
