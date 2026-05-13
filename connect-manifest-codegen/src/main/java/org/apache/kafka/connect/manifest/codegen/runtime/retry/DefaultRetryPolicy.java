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

import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.List;

/**
 * Java port of Airbyte's {@code DefaultErrorHandler} (default_error_handler.py:26).
 *
 * <p>Iteration order matches the Python implementation:
 * <ol>
 *     <li>Walk {@code responseFilters}; first matcher returns its resolution.</li>
 *     <li>If the response is 2xx, return {@link ErrorResolution#SUCCESS}.</li>
 *     <li>Otherwise look up the status in {@link DefaultErrorMapping}; missing entries
 *         fall back to {@link DefaultErrorMapping#fallback(int)} (Python lines 125-132).</li>
 * </ol>
 * </p>
 */
public final class DefaultRetryPolicy implements RetryPolicy {

    private final List<HttpResponseFilter> responseFilters;
    private final int maxRetries;
    private final long maxTimeMillis;

    public DefaultRetryPolicy(
        List<HttpResponseFilter> responseFilters,
        Integer maxRetries,
        Integer maxTimeSeconds
    ) {
        this.responseFilters = responseFilters == null
            ? Collections.emptyList()
            : List.copyOf(responseFilters);
        this.maxRetries = maxRetries == null ? DEFAULT_MAX_RETRIES : maxRetries;
        this.maxTimeMillis = maxTimeSeconds == null
            ? DEFAULT_MAX_TIME_MILLIS
            : maxTimeSeconds * 1000L;
    }

    /**
     * Convenience for generated code that has no manifest-defined error_handler:
     * filters are empty, falls through to {@link DefaultErrorMapping}.
     */
    public static DefaultRetryPolicy fallbackOnly() {
        return new DefaultRetryPolicy(Collections.emptyList(), null, null);
    }

    @Override
    public ErrorResolution interpretResponse(HttpResponse<String> response) {
        for (HttpResponseFilter filter : responseFilters) {
            ErrorResolution match = filter.matches(response);
            if (match != null) {
                return match;
            }
        }

        if (response == null) {
            return DefaultErrorMapping.fallback(0);
        }

        int code = response.statusCode();
        if (code >= 200 && code < 300) {
            return ErrorResolution.SUCCESS;
        }

        ErrorResolution base = DefaultErrorMapping.resolve(code).orElseGet(() -> DefaultErrorMapping.fallback(code));
        String body = response.body();
        if (body != null && !body.isEmpty()) {
            String snippet = body.length() > 300 ? body.substring(0, 300) : body;
            return new ErrorResolution(base.action(), base.failureType(),
                base.errorMessage() + " | Response: " + snippet);
        }
        return base;
    }

    @Override
    public int maxRetries() {
        return maxRetries;
    }

    @Override
    public long maxTimeMillis() {
        return maxTimeMillis;
    }

    public List<HttpResponseFilter> responseFilters() {
        return responseFilters;
    }
}
