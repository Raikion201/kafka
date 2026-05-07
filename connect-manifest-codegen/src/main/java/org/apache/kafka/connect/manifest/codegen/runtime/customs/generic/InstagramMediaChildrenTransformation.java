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
 * Stub for {@code source-instagram/components.py InstagramMediaChildrenTransformation}.
 *
 * <p>The Python implementation enriches carousel media records by fetching detailed metadata
 * for each child media item via the Instagram Graph API ({@code /media} endpoint), applying
 * {@code InstagramClearUrlTransformation}, and reformatting timestamps to RFC 3339. This
 * requires authenticated HTTP calls to the Instagram API per record.
 *
 * <p>This stub returns the record unchanged; the connector will start and produce records
 * with only the child media IDs in the {@code children} field rather than full child details.
 *
 * <p>Registered under {@code source_declarative_manifest.components.InstagramMediaChildrenTransformation}.
 */
public final class InstagramMediaChildrenTransformation implements CustomTransformation {

    public InstagramMediaChildrenTransformation(Map<String, String> connectorConfig,
                                                 Map<String, Object> componentParams) {
        // stub — HTTP enrichment not available in the in-process transform model
    }

    @Override
    public Map<String, Object> transform(Map<String, Object> record) {
        return record;
    }
}
