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

import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomTransformation;

import java.util.Map;

/**
 * Stub for {@code source-mixpanel/source_mixpanel/components.py EngageTransformation}.
 * Returns the record unchanged — full implementation deferred.
 * Registered under {@code source_mixpanel.components.EngageTransformation}.
 */
public final class EngageTransformation implements CustomTransformation {

    public EngageTransformation(Map<String, String> connectorConfig,
                                 Map<String, Object> componentParams) {
        // stub
    }

    @Override
    public Map<String, Object> transform(Map<String, Object> record) {
        return record;
    }
}
