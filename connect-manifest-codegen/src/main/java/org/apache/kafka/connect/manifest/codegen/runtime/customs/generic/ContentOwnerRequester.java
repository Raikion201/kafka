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

import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomRequester;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Stub for {@code source-youtube-analytics/components.py ContentOwnerRequester}.
 *
 * <p>The Python implementation extends {@code HttpRequester} and conditionally injects
 * {@code onBehalfOfContentOwner={config['content_owner_id']}} as a query parameter when
 * the config key is present. The base URL is {@code https://youtubereporting.googleapis.com/v1/}
 * and it uses OAuth2 auth (client_id/secret/refresh_token from config).
 *
 * <p>This stub returns an empty iterator because the generated {@code send(empty, empty)} call
 * provides no path or stream context — each stream overrides the path in the manifest YAML but
 * that information is not forwarded to the component at call time. A full implementation requires
 * codegen changes to pass the resolved URL to the component factory.
 *
 * <p>Registered under {@code source_declarative_manifest.components.ContentOwnerRequester}.
 */
public final class ContentOwnerRequester implements CustomRequester {

    private static final Logger LOG = Logger.getLogger(ContentOwnerRequester.class.getName());

    public ContentOwnerRequester(Map<String, String> connectorConfig,
                                  Map<String, Object> componentParams) {
        // stub
    }

    @Override
    public Iterator<JsonNode> send(Map<String, Object> partition, Map<String, Object> state) {
        LOG.warning("ContentOwnerRequester.send() is not yet implemented; returning empty — "
            + "upgrade the codegen to pass path context to the component for full YouTube Analytics support");
        return Collections.emptyIterator();
    }
}
