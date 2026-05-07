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

import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomTransformation;

import java.util.Map;

/**
 * Stub for {@code source-hubspot/components.py AddFieldsFromEndpointTransformation}.
 *
 * <p>The Python implementation enriches records by making additional HTTP requests to a
 * specified endpoint (using an internal {@code Requester} + {@code HttpSelector}) and
 * merging the response data into the record. This requires live HTTP access and cannot
 * be implemented as a pure in-process transform.
 *
 * <p>This stub returns the record unchanged; the connector will start and produce base
 * records without the endpoint-enriched fields.
 *
 * <p>Registered under {@code source_declarative_manifest.components.AddFieldsFromEndpointTransformation}.
 */
public final class AddFieldsFromEndpointTransformation implements CustomTransformation {

    public AddFieldsFromEndpointTransformation(Map<String, String> connectorConfig,
                                                Map<String, Object> componentParams) {
        // stub — HTTP requester not available in the in-process transform model
    }

    @Override
    public Map<String, Object> transform(Map<String, Object> record) {
        return record;
    }
}
