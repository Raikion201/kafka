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
 * Identity stub for GA4 {@code DimensionFilterConfigTransformation}.
 *
 * <p>In the Python CDK this transformation translates Airbyte filter DSL into a GA4
 * {@code dimensionFilter} API object. In the Java DDS path the generated task body
 * passes dimension filters verbatim from the config; this stub satisfies the registry
 * lookup and passes records through unchanged.</p>
 *
 * <p>Registered under
 * {@code source_declarative_manifest.components.DimensionFilterConfigTransformation}.</p>
 */
public final class DimensionFilterConfigTransformationStub implements CustomTransformation {

    public DimensionFilterConfigTransformationStub(Map<String, String> connectorConfig,
                                                   Map<String, Object> componentParams) {
    }

    @Override
    public Map<String, Object> transform(Map<String, Object> record) {
        return record;
    }
}
