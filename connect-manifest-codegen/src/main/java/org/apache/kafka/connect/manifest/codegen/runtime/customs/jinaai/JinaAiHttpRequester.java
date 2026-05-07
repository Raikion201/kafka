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
package org.apache.kafka.connect.manifest.codegen.runtime.customs.jinaai;

import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomRequester;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Stub for {@code source-jina-ai-reader/source_jina_ai_reader/components.py JinaAiHttpRequester}.
 *
 * <p>The Python implementation extends {@code HttpRequester} and overrides
 * {@code get_request_headers()} to append {@code Authorization: Bearer {config['api_key']}}
 * when {@code api_key} is present in the connector config. It is used for two streams:
 * {@code reader} (URL base: {@code https://r.jina.ai/{config['read_prompt']}}) and
 * {@code search} (URL base: {@code https://s.jina.ai/{config['search_prompt']}}).
 *
 * <p>This stub returns an empty iterator because both streams share the same class name and
 * the generated {@code send(empty, empty)} call does not indicate which stream or URL base
 * should be used. A full implementation requires the codegen to pass the rendered URL base
 * to the factory constructor so the component can call the correct endpoint.
 *
 * <p>Registered under {@code source_jina_ai_reader.components.JinaAiHttpRequester}.
 */
public final class JinaAiHttpRequester implements CustomRequester {

    private static final Logger LOG = Logger.getLogger(JinaAiHttpRequester.class.getName());

    public JinaAiHttpRequester(Map<String, String> connectorConfig,
                                Map<String, Object> componentParams) {
        // stub
    }

    @Override
    public Iterator<JsonNode> send(Map<String, Object> partition, Map<String, Object> state) {
        LOG.warning("JinaAiHttpRequester.send() is not yet implemented; returning empty — "
            + "codegen must forward the rendered url_base to the component factory");
        return Collections.emptyIterator();
    }
}
