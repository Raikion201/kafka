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
package org.apache.kafka.connect.manifest.codegen.runtime.transform;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Chains a {@link RecordFilter} and an ordered list of {@link RecordTransformation}s.
 * Call {@link #process(Map, Map)} per record; an empty result means the record was
 * filtered out and should be dropped before emission.
 */
public final class TransformationPipeline {

    /** No-op singleton for streams that declare no transformations or filter. */
    public static final TransformationPipeline NOOP = new TransformationPipeline(null, List.of());

    private final RecordFilter filter;
    private final List<RecordTransformation> transformations;

    public TransformationPipeline(RecordFilter filter, List<RecordTransformation> transformations) {
        this.filter = filter;
        this.transformations = transformations == null ? List.of() : transformations;
    }

    /**
     * Applies the filter then every transformation in order.
     *
     * @return the (mutated) record to emit, or {@link Optional#empty()} if filtered out
     */
    public Optional<Map<String, Object>> process(Map<String, Object> record, Map<String, Object> ctx) {
        if (filter != null && !filter.accept(record, ctx)) {
            return Optional.empty();
        }
        for (RecordTransformation t : transformations) {
            t.apply(record, ctx);
        }
        return Optional.of(record);
    }

    public boolean isNoop() {
        return (filter == null || !filter.hasCondition()) && transformations.isEmpty();
    }
}
