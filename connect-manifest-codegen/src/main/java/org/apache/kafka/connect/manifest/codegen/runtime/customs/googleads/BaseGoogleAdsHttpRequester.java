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
import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomRequester;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * Shared abstract base for the four Google Ads requester subclasses ported from
 * {@code source_google_ads.components.GoogleAdsHttpRequester} (components.py:373-422).
 *
 * <p>Phase 1 ships the four subclasses solely so manifest registration resolves; the
 * underlying gRPC streaming call is delivered in Phase 1.5 / Phase 2 using the
 * {@code com.google.api-ads:google-ads-java} SDK. Until then, {@link #send} throws
 * a clear {@link ConnectException}.</p>
 *
 * <p>TODO(phase-2): replace {@link #send} with a real implementation backed by
 * {@code GoogleAdsServiceClient.searchStream}; subclasses will continue to provide
 * the GAQL query string via {@link #buildQuery}.</p>
 */
public abstract class BaseGoogleAdsHttpRequester implements CustomRequester {

    protected final Map<String, String> connectorConfig;
    protected final Map<String, Object> componentParams;

    protected BaseGoogleAdsHttpRequester(Map<String, String> connectorConfig,
                                         Map<String, Object> componentParams) {
        this.connectorConfig = connectorConfig == null ? Collections.emptyMap() : connectorConfig;
        this.componentParams = componentParams == null ? Collections.emptyMap() : componentParams;
    }

    /**
     * Build the GAQL query string for a given partition + state. Phase-2 hook so
     * the four subclasses can be unit-tested independently of the wire layer.
     */
    public abstract String buildQuery(Map<String, Object> partition, Map<String, Object> state);

    @Override
    public Iterator<JsonNode> send(Map<String, Object> partition, Map<String, Object> state) {
        // TODO(phase-2): wire google-ads-java GoogleAdsServiceClient.searchStream
        throw new ConnectException(
            "GoogleAdsHttpRequester.send not yet implemented in Phase 1; query was: "
                + buildQuery(partition, state));
    }

    /** Read a String component param, returning {@code dflt} if absent or null. */
    protected String paramString(String key, String dflt) {
        Object v = componentParams.get(key);
        return v == null ? dflt : v.toString();
    }
}
