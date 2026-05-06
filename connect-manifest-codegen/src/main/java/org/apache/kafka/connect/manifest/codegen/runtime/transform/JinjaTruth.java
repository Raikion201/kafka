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

import org.apache.kafka.connect.manifest.codegen.runtime.jinja.JinjaRenderer;

import java.util.Map;

/**
 * Evaluates a Jinja boolean condition template, matching Python's dpath/Airbyte
 * InterpolatedBoolean semantics: the rendered string is truthy unless it is
 * {@code "false"}, {@code "0"}, {@code ""}, or {@code "none"} (case-insensitive).
 */
final class JinjaTruth {

    private JinjaTruth() {
    }

    static boolean evaluate(String template, Map<String, Object> ctx) {
        if (template == null || template.isBlank()) {
            return true;
        }
        String rendered = JinjaRenderer.renderLenient(template, ctx);
        return isTruthy(rendered);
    }

    static boolean isTruthy(String rendered) {
        if (rendered == null) {
            return false;
        }
        String lower = rendered.strip().toLowerCase(java.util.Locale.ROOT);
        return !lower.isEmpty() && !"false".equals(lower) && !"0".equals(lower) && !"none".equals(lower);
    }
}
