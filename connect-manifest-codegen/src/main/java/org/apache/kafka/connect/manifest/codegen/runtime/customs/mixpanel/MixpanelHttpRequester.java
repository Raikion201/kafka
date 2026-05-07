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
package org.apache.kafka.connect.manifest.codegen.runtime.customs.mixpanel;

import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomRequester;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Stub for {@code source-mixpanel/source_mixpanel/components.py MixpanelHttpRequester}.
 *
 * <p>The Python implementation extends {@code HttpRequester} and injects {@code project_id}
 * from {@code config['credentials']['project_id']} into request params. The URL base is
 * region-aware: {@code https://mixpanel.com/api/} (US) or
 * {@code https://{region}.mixpanel.com/api/} (EU). Multiple streams share this class,
 * each overriding the path via the manifest {@code path} field.
 *
 * <p>This stub returns an empty iterator because the generated {@code send(empty, empty)} call
 * provides no path or stream context. Registered under
 * {@code source_mixpanel.components.MixpanelHttpRequester}.
 */
public final class MixpanelHttpRequester implements CustomRequester {

    private static final Logger LOG = Logger.getLogger(MixpanelHttpRequester.class.getName());

    public MixpanelHttpRequester(Map<String, String> connectorConfig,
                                  Map<String, Object> componentParams) {
        // stub
    }

    @Override
    public Iterator<JsonNode> send(Map<String, Object> partition, Map<String, Object> state) {
        LOG.warning("MixpanelHttpRequester.send() is not yet implemented; returning empty");
        return Collections.emptyIterator();
    }
}
