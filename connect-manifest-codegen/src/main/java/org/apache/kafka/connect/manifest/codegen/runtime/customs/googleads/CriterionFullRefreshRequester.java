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
 * Java port of {@code source_google_ads.components.CriterionFullRefreshRequester}
 * (components.py:789-803).
 *
 * <p>Builds the full-refresh GAQL query for criterion-style streams: every property
 * declared on the schema (via the {@code fields} component param), excluding the
 * cursor field and the synthetic {@code deleted_at}.</p>
 */
public final class CriterionFullRefreshRequester extends BaseGoogleAdsHttpRequester {

    static final String CURSOR_FIELD = "change_status.last_change_date_time";

    public CriterionFullRefreshRequester(Map<String, String> connectorConfig,
                                         Map<String, Object> componentParams) {
        super(connectorConfig, componentParams);
    }

    @Override
    public String buildQuery(Map<String, Object> partition, Map<String, Object> state) {
        List<String> fields = fieldsParam();
        StringBuilder sb = new StringBuilder("SELECT ");
        boolean first = true;
        for (String f : fields) {
            if (CURSOR_FIELD.equals(f) || "deleted_at".equals(f)) {
                continue;
            }
            if (!first) {
                sb.append(", ");
            }
            sb.append(f);
            first = false;
        }
        sb.append(" FROM ").append(paramString("resource_name", ""));
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private List<String> fieldsParam() {
        Object fields = componentParams.get("fields");
        if (fields instanceof List) {
            return (List<String>) fields;
        }
        if (fields instanceof Collection) {
            // narrow defensively
            return ((Collection<?>) fields).stream().map(Object::toString).toList();
        }
        return Collections.emptyList();
    }
}
