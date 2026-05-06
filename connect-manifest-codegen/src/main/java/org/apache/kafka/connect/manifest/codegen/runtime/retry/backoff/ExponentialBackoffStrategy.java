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
 * Java port of {@code ExponentialBackoffStrategy} (exponential_backoff_strategy.py:16).
 *
 * <p>Sleep duration is {@code factor * 2^attemptCount} seconds. Default factor is {@code 5}
 * matching Python (line 26).</p>
 */
public final class ExponentialBackoffStrategy implements BackoffStrategy {

    public static final double DEFAULT_FACTOR = 5.0;

    private final double factor;

    public ExponentialBackoffStrategy() {
        this(DEFAULT_FACTOR);
    }

    public ExponentialBackoffStrategy(Double factor) {
        this.factor = factor == null ? DEFAULT_FACTOR : factor;
    }

    @Override
    public Long backoffMillis(HttpResponse<String> response, int attemptCount) {
        double seconds = factor * Math.pow(2, attemptCount);
        return (long) (seconds * 1000.0);
    }

    public double factor() {
        return factor;
    }
}
