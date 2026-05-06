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
package org.apache.kafka.connect.manifest.codegen.runtime.retry.backoff;

import java.net.http.HttpResponse;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Port of Airbyte's {@code get_numeric_value_from_header} (header_helper.py:12).
 *
 * <p>Reads the named header (case-insensitive), optionally extracts a numeric substring via
 * {@code regex.match()} (Python uses anchored-at-start semantics — Java's
 * {@link Matcher#lookingAt()} matches Python {@code re.match}), and parses to {@code double}.</p>
 */
final class HeaderValueExtractor {

    private HeaderValueExtractor() {
    }

    static Double extract(HttpResponse<String> response, String header, Pattern regex) {
        if (response == null || header == null || header.isEmpty()) {
            return null;
        }
        List<String> values = response.headers().allValues(header);
        if (values.isEmpty()) {
            return null;
        }
        String raw = values.get(0);
        if (raw == null || raw.isEmpty()) {
            return null;
        }
        String value = raw;
        if (regex != null) {
            Matcher m = regex.matcher(raw);
            if (m.lookingAt()) {
                value = m.group();
            }
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }
}
