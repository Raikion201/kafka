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
import java.util.Collections;
import java.util.List;

/**
 * Sequential composition of strategies — first non-null result wins.
 *
 * <p>Mirrors Python's {@code DefaultErrorHandler.backoff_time()} which iterates
 * {@code backoff_strategies} and returns the first non-None value
 * (default_error_handler.py:139-147). When no strategy yields a value, returns
 * {@link ExponentialBackoffStrategy} default ({@code 5 * 2^attempt} seconds) as a safety
 * floor — Airbyte's {@code HttpRequester} does the same when no strategy applies.</p>
 */
public final class BackoffStrategyChain implements BackoffStrategy {

    private static final BackoffStrategy DEFAULT_FALLBACK = new ExponentialBackoffStrategy();

    private final List<BackoffStrategy> strategies;
    private final BackoffStrategy fallback;

    public BackoffStrategyChain(List<BackoffStrategy> strategies) {
        this(strategies, DEFAULT_FALLBACK);
    }

    public BackoffStrategyChain(List<BackoffStrategy> strategies, BackoffStrategy fallback) {
        this.strategies = strategies == null ? Collections.emptyList() : List.copyOf(strategies);
        this.fallback = fallback;
    }

    @Override
    public Long backoffMillis(HttpResponse<String> response, int attemptCount) {
        for (BackoffStrategy strategy : strategies) {
            Long candidate = strategy.backoffMillis(response, attemptCount);
            if (candidate != null) {
                return candidate;
            }
        }
        return fallback == null ? null : fallback.backoffMillis(response, attemptCount);
    }
}
