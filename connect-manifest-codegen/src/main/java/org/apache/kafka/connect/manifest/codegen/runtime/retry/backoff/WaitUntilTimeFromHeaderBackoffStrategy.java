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
import java.time.Clock;
import java.util.regex.Pattern;

/**
 * Java port of {@code WaitUntilTimeFromHeaderBackoffStrategy} (wait_until_time_from_header_backoff_strategy.py:24).
 *
 * <p>Reads a Unix timestamp (seconds) from the response header and returns
 * {@code waitUntil − now} as the backoff duration. Result is clamped to {@code minWait}
 * when configured (Python lines 65-77).</p>
 *
 * <p>{@link Clock} is injectable so tests can advance time deterministically.</p>
 */
public final class WaitUntilTimeFromHeaderBackoffStrategy implements BackoffStrategy {

    private final String header;
    private final Pattern regex;
    private final Double minWait;
    private final Clock clock;

    public WaitUntilTimeFromHeaderBackoffStrategy(String header, String regex, Double minWait) {
        this(header, regex, minWait, Clock.systemUTC());
    }

    WaitUntilTimeFromHeaderBackoffStrategy(String header, String regex, Double minWait, Clock clock) {
        if (header == null || header.isEmpty()) {
            throw new IllegalArgumentException("WaitUntilTimeFromHeaderBackoffStrategy requires a header name");
        }
        this.header = header;
        this.regex = regex == null || regex.isEmpty() ? null : Pattern.compile(regex);
        this.minWait = minWait;
        this.clock = clock == null ? Clock.systemUTC() : clock;
    }

    @Override
    public Long backoffMillis(HttpResponse<String> response, int attemptCount) {
        Double waitUntil = HeaderValueExtractor.extract(response, header, regex);
        double nowSeconds = clock.millis() / 1000.0;

        if (waitUntil == null || waitUntil == 0.0) {
            return minWait == null ? null : (long) (minWait * 1000.0);
        }

        double waitSeconds = waitUntil - nowSeconds;
        if (minWait != null) {
            return (long) (Math.max(waitSeconds, minWait) * 1000.0);
        }
        if (waitSeconds < 0) {
            return null;
        }
        return (long) (waitSeconds * 1000.0);
    }
}
