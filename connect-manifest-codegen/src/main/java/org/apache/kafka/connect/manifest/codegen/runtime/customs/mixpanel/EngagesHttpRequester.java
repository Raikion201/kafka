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
 * Stub for {@code source-mixpanel/source_mixpanel/components.py EngagesHttpRequester}.
 * Returns empty — full implementation deferred.
 * Registered under {@code source_mixpanel.components.EngagesHttpRequester}.
 */
public final class EngagesHttpRequester implements CustomRequester {

    private static final Logger LOG = Logger.getLogger(EngagesHttpRequester.class.getName());

    public EngagesHttpRequester(Map<String, String> connectorConfig,
                                 Map<String, Object> componentParams) {
        // stub
    }

    @Override
    public Iterator<JsonNode> send(Map<String, Object> partition, Map<String, Object> state) {
        LOG.warning("EngagesHttpRequester.send() is not yet implemented; returning empty");
        return Collections.emptyIterator();
    }
}
