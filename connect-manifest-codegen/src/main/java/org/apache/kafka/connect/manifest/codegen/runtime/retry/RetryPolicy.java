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

/**
 * Java analogue of Airbyte's {@code ErrorHandler} (default_error_handler.py:26).
 *
 * <p>Generated source-task code calls {@link #interpretResponse(HttpResponse)} after every HTTP
 * call to decide whether to return the response, retry, ignore, or fail. {@link #maxRetries()}
 * and {@link #maxTimeMillis()} bound the retry loop. Backoff timing comes from a separate
 * {@code BackoffStrategy} (P1.T3); this interface only classifies the response.</p>
 */
public interface RetryPolicy {

    /** Airbyte default: retry up to 5 times. */
    int DEFAULT_MAX_RETRIES = 5;

    /** Airbyte default: 600s elapsed time across all attempts. */
    long DEFAULT_MAX_TIME_MILLIS = 600_000L;

    /**
     * Classify {@code response}.
     *
     * <p>Returns {@link ErrorResolution#SUCCESS} for 2xx responses unless a configured
     * {@code response_filter} matches first; otherwise the resolution carries the
     * {@link ResponseAction} dictated by the manifest, falling back to
     * {@link DefaultErrorMapping} for unhandled status codes.</p>
     */
    ErrorResolution interpretResponse(HttpResponse<String> response);

    default int maxRetries() {
        return DEFAULT_MAX_RETRIES;
    }

    default long maxTimeMillis() {
        return DEFAULT_MAX_TIME_MILLIS;
    }
}
