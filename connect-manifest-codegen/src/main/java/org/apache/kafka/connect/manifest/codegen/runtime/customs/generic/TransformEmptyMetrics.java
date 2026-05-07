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

import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomTransformation;

import java.util.Map;

/**
 * Java port of {@code source-tiktok-marketing/components.py TransformEmptyMetrics} (lines 79–94).
 *
 * <p>TikTok Ads API returns {@code "-"} as a sentinel for "no data" in metric fields.
 * This transform replaces every {@code "-"} value inside {@code record["metrics"]} with
 * {@code null}, which downstream Kafka schemas handle as a proper null.</p>
 *
 * <p>Registered under {@code source_declarative_manifest.components.TransformEmptyMetrics}.</p>
 */
public final class TransformEmptyMetrics implements CustomTransformation {

    private static final String EMPTY_VALUE = "-";

    @SuppressWarnings("unused")
    public TransformEmptyMetrics(Map<String, String> connectorConfig,
                                 Map<String, Object> componentParams) {
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> transform(Map<String, Object> record) {
        if (record == null) {
            return null;
        }
        Object metricsObj = record.get("metrics");
        if (!(metricsObj instanceof Map)) {
            return record;
        }
        Map<String, Object> metrics = (Map<String, Object>) metricsObj;
        for (Map.Entry<String, Object> e : metrics.entrySet()) {
            if (EMPTY_VALUE.equals(e.getValue())) {
                e.setValue(null);
            }
        }
        return record;
    }
}
