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
 * Phase-1 stub for {@code source_google_ads.components.CustomGAQueryHttpRequester}.
 * Used by user-supplied {@code custom_queries_array} streams; query is passed in
 * via {@code componentParams.query}. Real wire call lands in Phase 2.
 */
public final class CustomGAQueryHttpRequester extends BaseGoogleAdsHttpRequester {

    public CustomGAQueryHttpRequester(Map<String, String> connectorConfig,
                                      Map<String, Object> componentParams) {
        super(connectorConfig, componentParams);
    }

    @Override
    public String buildQuery(Map<String, Object> partition, Map<String, Object> state) {
        return paramString("query", "");
    }
}
