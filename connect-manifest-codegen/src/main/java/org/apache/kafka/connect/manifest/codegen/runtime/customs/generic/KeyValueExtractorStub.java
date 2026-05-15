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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomRecordExtractor;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Stub for GA4 {@code KeyValueExtractor}.
 *
 * <p>In the real Python CDK, {@code KeyValueExtractor} zips a {@code keys_extractor} with a
 * {@code values_extractor} to produce per-row records. In the Java DDS path this logic is
 * inlined into the generated task body; this stub satisfies the registry lookup used when
 * the manifest component tree is traversed by other code paths.</p>
 *
 * <p>Registered under
 * {@code source_declarative_manifest.components.KeyValueExtractor}.</p>
 */
public final class KeyValueExtractorStub implements CustomRecordExtractor {

    public KeyValueExtractorStub(Map<String, String> connectorConfig,
                                 Map<String, Object> componentParams) {
    }

    @Override
    public List<Map<String, Object>> extract(JsonNode response) {
        return Collections.emptyList();
    }
}
