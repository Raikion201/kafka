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
 * Java port of {@code source_google_ads.components.CriterionIncrementalRequester}
 * (components.py:751-786).
 *
 * <p>Builds an incremental GAQL query that selects every schema field other than the
 * cursor and the synthetic {@code deleted_at}, restricted to a list of primary-key
 * IDs sourced from the partition.</p>
 */
public final class CriterionIncrementalRequester extends BaseGoogleAdsHttpRequester {

    static final String CURSOR_FIELD = "change_status.last_change_date_time";

    public CriterionIncrementalRequester(Map<String, String> connectorConfig,
                                         Map<String, Object> componentParams) {
        super(connectorConfig, componentParams);
    }

    @Override
    public String buildQuery(Map<String, Object> partition, Map<String, Object> state) {
        String pk = primaryKey();
        List<String> ids = idsFromPartition(partition, pk);
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
        sb.append(" WHERE ").append(pk).append(" IN (");
        boolean firstId = true;
        for (String id : ids) {
            if (!firstId) {
                sb.append(", ");
            }
            sb.append('\'').append(id).append('\'');
            firstId = false;
        }
        sb.append(')');
        return sb.toString();
    }

    private String primaryKey() {
        Object pk = componentParams.get("primary_key");
        if (pk instanceof List && !((List<?>) pk).isEmpty()) {
            return ((List<?>) pk).get(0).toString();
        }
        if (pk instanceof String) {
            return (String) pk;
        }
        return "id";
    }

    @SuppressWarnings("unchecked")
    private List<String> idsFromPartition(Map<String, Object> partition, String pk) {
        if (partition == null) {
            return Collections.emptyList();
        }
        Object ids = partition.get(pk);
        if (ids instanceof List) {
            return ((List<Object>) ids).stream().map(Object::toString).toList();
        }
        if (ids instanceof Collection) {
            return ((Collection<?>) ids).stream().map(Object::toString).toList();
        }
        if (ids != null) {
            return Collections.singletonList(ids.toString());
        }
        return Collections.emptyList();
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
