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
 * Stub for {@code source-klaviyo/components.py CampaignsDetailedTransformation}.
 *
 * <p>The Python implementation enriches campaign records with two extra API calls per record:
 * {@code GET /campaign-recipient-estimations/{id}} (for {@code estimated_recipient_count})
 * and {@code GET /campaign-messages} via the relationship link (for {@code campaign_messages}).
 * These calls require API key auth, retry policies, and error handling.
 *
 * <p>This stub returns the record unchanged; the connector will start and produce records
 * without the {@code estimated_recipient_count} and {@code campaign_messages} enrichment fields.
 *
 * <p>Registered under {@code source_declarative_manifest.components.CampaignsDetailedTransformation}.
 */
public final class CampaignsDetailedTransformation implements CustomTransformation {

    public CampaignsDetailedTransformation(Map<String, String> connectorConfig,
                                            Map<String, Object> componentParams) {
        // stub — multi-endpoint HTTP enrichment not available in the in-process transform model
    }

    @Override
    public Map<String, Object> transform(Map<String, Object> record) {
        return record;
    }
}
