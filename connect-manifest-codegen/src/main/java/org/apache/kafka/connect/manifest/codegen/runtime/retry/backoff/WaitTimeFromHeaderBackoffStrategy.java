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

import org.apache.kafka.connect.errors.ConnectException;

import java.net.http.HttpResponse;
import java.util.regex.Pattern;

/**
 * Java port of {@code WaitTimeFromHeaderBackoffStrategy} (wait_time_from_header_backoff_strategy.py:24).
 *
 * <p>Reads the wait duration directly from a response header (typical {@code Retry-After}).
 * If the parsed value exceeds {@code maxWaitingTimeInSeconds}, throws — matching Python's
 * {@code AirbyteTracedException} on lines 65-69.</p>
 */
public final class WaitTimeFromHeaderBackoffStrategy implements BackoffStrategy {

    private final String header;
    private final Pattern regex;
    private final Double maxWaitingTimeInSeconds;

    public WaitTimeFromHeaderBackoffStrategy(String header, String regex, Double maxWaitingTimeInSeconds) {
        if (header == null || header.isEmpty()) {
            throw new IllegalArgumentException("WaitTimeFromHeaderBackoffStrategy requires a header name");
        }
        this.header = header;
        this.regex = regex == null || regex.isEmpty() ? null : Pattern.compile(regex);
        this.maxWaitingTimeInSeconds = maxWaitingTimeInSeconds;
    }

    @Override
    public Long backoffMillis(HttpResponse<String> response, int attemptCount) {
        Double headerValue = HeaderValueExtractor.extract(response, header, regex);
        if (headerValue == null) {
            return null;
        }
        if (maxWaitingTimeInSeconds != null && headerValue >= maxWaitingTimeInSeconds) {
            throw new ConnectException(
                "Rate limit wait time " + headerValue
                    + " is greater than max waiting time of " + maxWaitingTimeInSeconds
                    + " seconds. Stopping the stream.");
        }
        return (long) (headerValue * 1000.0);
    }
}
