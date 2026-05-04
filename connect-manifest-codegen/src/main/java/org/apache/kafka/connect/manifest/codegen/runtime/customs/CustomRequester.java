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
package org.apache.kafka.connect.manifest.codegen.runtime.customs;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Iterator;
import java.util.Map;

/**
 * Java equivalent of Airbyte CDK {@code Requester} (and {@code CustomRequester}).
 * Encapsulates the network call shape: given a stream partition and the current cursor
 * state, fetch records as a stream of JSON nodes.
 */
public interface CustomRequester extends CustomComponent {

    /**
     * Send the request(s) for a single partition+state. The returned iterator may stream
     * (e.g. for chunked or gRPC-streamed endpoints); callers must drain it before issuing
     * the next request.
     */
    Iterator<JsonNode> send(Map<String, Object> partition, Map<String, Object> state);
}
