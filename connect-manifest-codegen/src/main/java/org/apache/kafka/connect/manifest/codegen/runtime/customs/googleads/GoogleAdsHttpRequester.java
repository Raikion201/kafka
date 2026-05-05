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

import java.util.Map;

/**
 * Phase-1 stub for {@code source_google_ads.components.GoogleAdsHttpRequester}
 * (Airbyte components.py:373-422). The real implementation issues a streaming
 * GAQL {@code searchStream} call via {@code com.google.api-ads:google-ads-java};
 * Phase 1 only registers the class so manifest lookup resolves and the connector
 * loads. Actual {@code send(...)} throws via {@link BaseGoogleAdsHttpRequester}.
 *
 * <p>TODO(phase-2): real impl using {@code GoogleAdsServiceClient.searchStream}.</p>
 */
public final class GoogleAdsHttpRequester extends BaseGoogleAdsHttpRequester {

    public GoogleAdsHttpRequester(Map<String, String> connectorConfig,
                                  Map<String, Object> componentParams) {
        super(connectorConfig, componentParams);
    }

    @Override
    public String buildQuery(Map<String, Object> partition, Map<String, Object> state) {
        // Subclasses override; the base class itself uses the manifest-provided GAQL string.
        return paramString("query", "");
    }
}
