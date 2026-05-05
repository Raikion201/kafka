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

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomDecoder;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

/**
 * Phase-1 stub for {@code source_google_ads.components.GoogleAdsStreamingDecoder}.
 * Real impl will consume the {@code ServerStream<SearchGoogleAdsStreamResponse>}
 * from the google-ads-java SDK and yield rows one at a time. Phase 1 throws.
 *
 * <p>TODO(phase-2): wire real impl backed by the SDK's server-streaming response.</p>
 */
public final class GoogleAdsStreamingDecoder implements CustomDecoder {

    @SuppressWarnings("unused")
    private final Map<String, String> connectorConfig;
    @SuppressWarnings("unused")
    private final Map<String, Object> componentParams;

    public GoogleAdsStreamingDecoder(Map<String, String> connectorConfig,
                                     Map<String, Object> componentParams) {
        this.connectorConfig = connectorConfig;
        this.componentParams = componentParams;
    }

    @Override
    public Iterator<JsonNode> decode(InputStream stream) {
        throw new ConnectException(
            "GoogleAdsStreamingDecoder.decode not yet implemented in Phase 1");
    }
}
