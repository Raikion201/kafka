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
package org.apache.kafka.connect.manifest.codegen.runtime.retry;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Mirrors Airbyte's {@code DEFAULT_ERROR_MAPPING} table
 * (default_error_mapping.py:15-86) byte-for-byte for the HTTP-status entries.
 *
 * <p>Exception-class entries from the Python table are intentionally skipped — the generated
 * Java task uses {@link java.net.http.HttpClient} which surfaces I/O failures as checked
 * exceptions caught at the retry-loop level rather than mapped through this table.</p>
 */
public final class DefaultErrorMapping {

    private static final Map<Integer, ErrorResolution> CODE_MAP;

    static {
        Map<Integer, ErrorResolution> m = new HashMap<>();
        m.put(400, new ErrorResolution(ResponseAction.FAIL, "system_error",
            "HTTP Status Code: 400. Error: Bad request. Please check your request parameters."));
        m.put(401, new ErrorResolution(ResponseAction.FAIL, "config_error",
            "HTTP Status Code: 401. Error: Unauthorized. Please ensure you are authenticated correctly."));
        m.put(403, new ErrorResolution(ResponseAction.FAIL, "config_error",
            "HTTP Status Code: 403. Error: Forbidden. You don't have permission to access this resource."));
        m.put(404, new ErrorResolution(ResponseAction.FAIL, "system_error",
            "HTTP Status Code: 404. Error: Not found. The requested resource was not found on the server."));
        m.put(405, new ErrorResolution(ResponseAction.FAIL, "system_error",
            "HTTP Status Code: 405. Error: Method not allowed. Please check your request method."));
        m.put(408, new ErrorResolution(ResponseAction.RETRY, "transient_error",
            "HTTP Status Code: 408. Error: Request timeout."));
        m.put(429, new ErrorResolution(ResponseAction.RATE_LIMITED, "transient_error",
            "HTTP Status Code: 429. Error: Too many requests."));
        m.put(500, new ErrorResolution(ResponseAction.RETRY, "transient_error",
            "HTTP Status Code: 500. Error: Internal server error."));
        m.put(502, new ErrorResolution(ResponseAction.RETRY, "transient_error",
            "HTTP Status Code: 502. Error: Bad gateway."));
        m.put(503, new ErrorResolution(ResponseAction.RETRY, "transient_error",
            "HTTP Status Code: 503. Error: Service unavailable."));
        m.put(504, new ErrorResolution(ResponseAction.RETRY, "transient_error",
            "HTTP Status Code: 504. Error: Gateway timeout."));
        CODE_MAP = Collections.unmodifiableMap(m);
    }

    private DefaultErrorMapping() {
    }

    /** Lookup {@code statusCode} in the default table. */
    public static Optional<ErrorResolution> resolve(int statusCode) {
        return Optional.ofNullable(CODE_MAP.get(statusCode));
    }

    /**
     * Fallback for any non-2xx response missing from {@link #resolve(int)} —
     * matches Python {@code create_fallback_error_resolution} (response_models.py:47).
     */
    public static ErrorResolution fallback(int statusCode) {
        return new ErrorResolution(
            ResponseAction.RETRY,
            "system_error",
            "Unexpected HTTP status: " + statusCode);
    }
}
