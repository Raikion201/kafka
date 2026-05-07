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
package org.apache.kafka.connect.manifest.codegen.runtime.customs.asana;

import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomRequester;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Stub for {@code source-asana/source_asana/components.py AsanaHttpRequester}.
 *
 * <p>The Python implementation extends {@code HttpRequester} and overrides
 * {@code get_request_params()} to append an {@code opt_fields} query parameter built by
 * enumerating the stream's JSON schema fields. Without opt_fields, the Asana REST API only
 * returns three fields ({@code gid}, {@code name}, {@code resource_type}) per object; with it,
 * all schema fields are fetched in a single request.
 *
 * <p>This stub returns an empty iterator because the generated {@code send(empty, empty)} call
 * provides no stream path or schema context — all 20+ Asana streams share the same class name
 * but differ by path ({@code /projects}, {@code /tasks}, etc.) and opt_fields spec.
 * A full implementation requires codegen changes to pass the resolved URL + path to the factory.
 *
 * <p>Registered under {@code source_asana.components.AsanaHttpRequester}.
 */
public final class AsanaHttpRequester implements CustomRequester {

    private static final Logger LOG = Logger.getLogger(AsanaHttpRequester.class.getName());

    public AsanaHttpRequester(Map<String, String> connectorConfig,
                               Map<String, Object> componentParams) {
        // stub
    }

    @Override
    public Iterator<JsonNode> send(Map<String, Object> partition, Map<String, Object> state) {
        LOG.warning("AsanaHttpRequester.send() is not yet implemented; returning empty — "
            + "codegen must pass stream path + schema context to enable opt_fields construction");
        return Collections.emptyIterator();
    }
}
