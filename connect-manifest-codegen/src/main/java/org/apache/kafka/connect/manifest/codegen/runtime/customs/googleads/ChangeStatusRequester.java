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
 * Java port of {@code source_google_ads.components.ChangeStatusRequester}
 * (components.py:509-535).
 *
 * <p>Builds a windowed GAQL query against the {@code change_status} resource scoped
 * to a single resource type. Window comes from the cursor slice keys
 * {@code start_time}/{@code end_time}.</p>
 */
public final class ChangeStatusRequester extends BaseGoogleAdsHttpRequester {

    static final String CURSOR_FIELD = "change_status.last_change_date_time";
    static final int LIMIT = 10000;

    public ChangeStatusRequester(Map<String, String> connectorConfig,
                                 Map<String, Object> componentParams) {
        super(connectorConfig, componentParams);
    }

    @Override
    public String buildQuery(Map<String, Object> partition, Map<String, Object> state) {
        String name = paramString("name", "change_status");
        String resourceType = paramString("resource_type", "");
        Map<String, Object> slice = state == null ? Collections.emptyMap() : state;

        String start = String.valueOf(slice.getOrDefault("start_time", ""));
        String end = String.valueOf(slice.getOrDefault("end_time", ""));

        StringBuilder sb = new StringBuilder("SELECT ");
        boolean first = true;
        for (String f : fieldsParam()) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(f);
            first = false;
        }
        sb.append(" FROM ").append(name)
            .append(" WHERE ").append(CURSOR_FIELD)
            .append(" BETWEEN '").append(start).append("' AND '").append(end).append('\'')
            .append(" AND change_status.resource_type = ").append(resourceType)
            .append(" ORDER BY ").append(CURSOR_FIELD).append(" ASC")
            .append(" LIMIT ").append(LIMIT);
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
