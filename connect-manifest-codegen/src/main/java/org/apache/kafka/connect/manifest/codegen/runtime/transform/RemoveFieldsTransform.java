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

/**
 * Java port of Airbyte's {@code transformations/remove_fields.py RemoveFields}.
 * Calls {@link DPath#delete} per pointer, swallowing missing-path failures — matching
 * Python {@code dpath.delete}'s tolerance (Airbyte swallows {@code PathNotFound}).
 * An optional outer {@code condition} gates the whole transform.
 */
public final class RemoveFieldsTransform implements RecordTransformation {

    private final List<List<String>> fieldPointers;
    private final String condition;

    public RemoveFieldsTransform(List<List<String>> fieldPointers, String condition) {
        this.fieldPointers = fieldPointers == null ? List.of() : fieldPointers;
        this.condition = condition;
    }

    @Override
    public void apply(Map<String, Object> record, Map<String, Object> ctx) {
        if (condition != null && !condition.isBlank() && !JinjaTruth.evaluate(condition, ctx)) {
            return;
        }
        for (List<String> pointer : fieldPointers) {
            DPath.delete(record, pointer);
        }
    }
}
