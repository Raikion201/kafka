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

/**
 * Java analogue of Airbyte's {@code BackoffStrategy} (backoff_strategy.py).
 *
 * <p>Returns the milliseconds to sleep before the next retry, or {@code null} when the strategy
 * declines to handle the current response. {@link BackoffStrategyChain} composes a list of
 * strategies and returns the first non-null value — matching Python's
 * {@code DefaultErrorHandler.backoff_time()} (default_error_handler.py:139-147).</p>
 */
public interface BackoffStrategy {

    /**
     * @param response last HTTP response (may be {@code null} for transport-layer errors)
     * @param attemptCount how many attempts have already been made (1-based at first failure)
     * @return milliseconds to sleep, or {@code null} if this strategy does not apply
     */
    Long backoffMillis(HttpResponse<String> response, int attemptCount);
}
