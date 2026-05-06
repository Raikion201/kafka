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

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BackoffStrategyTest {

    // ── ExponentialBackoffStrategy ───────────────────────────────────────────

    @Test
    void exponential_defaultFactor_matchesPython() {
        ExponentialBackoffStrategy s = new ExponentialBackoffStrategy();
        assertEquals(5_000L, s.backoffMillis(null, 0));     // 5 * 2^0
        assertEquals(10_000L, s.backoffMillis(null, 1));    // 5 * 2^1
        assertEquals(20_000L, s.backoffMillis(null, 2));    // 5 * 2^2
        assertEquals(40_000L, s.backoffMillis(null, 3));    // 5 * 2^3
    }

    @Test
    void exponential_customFactor() {
        ExponentialBackoffStrategy s = new ExponentialBackoffStrategy(2.0);
        assertEquals(2_000L, s.backoffMillis(null, 0));
        assertEquals(4_000L, s.backoffMillis(null, 1));
        assertEquals(8_000L, s.backoffMillis(null, 2));
    }

    @Test
    void exponential_nullFactor_usesDefault() {
        ExponentialBackoffStrategy s = new ExponentialBackoffStrategy(null);
        assertEquals(ExponentialBackoffStrategy.DEFAULT_FACTOR, s.factor());
    }

    // ── ConstantBackoffStrategy ──────────────────────────────────────────────

    @Test
    void constant_returnsConfiguredValueRegardlessOfAttempt() {
        ConstantBackoffStrategy s = new ConstantBackoffStrategy(30.0);
        assertEquals(30_000L, s.backoffMillis(null, 0));
        assertEquals(30_000L, s.backoffMillis(null, 5));
        assertEquals(30_000L, s.backoffMillis(null, 99));
    }

    @Test
    void constant_subSecondValueRoundsToMillis() {
        ConstantBackoffStrategy s = new ConstantBackoffStrategy(0.5);
        assertEquals(500L, s.backoffMillis(null, 0));
    }

    // ── WaitTimeFromHeaderBackoffStrategy ────────────────────────────────────

    @Test
    void waitTimeFromHeader_extractsNumericFromHeader() {
        WaitTimeFromHeaderBackoffStrategy s = new WaitTimeFromHeaderBackoffStrategy(
            "Retry-After", null, null);
        Long result = s.backoffMillis(stubResponseWithHeader("Retry-After", "60"), 0);
        assertEquals(60_000L, result);
    }

    @Test
    void waitTimeFromHeader_missingHeader_returnsNull() {
        WaitTimeFromHeaderBackoffStrategy s = new WaitTimeFromHeaderBackoffStrategy(
            "Retry-After", null, null);
        assertNull(s.backoffMillis(stubResponseWithHeader("X-Other", "60"), 0));
    }

    @Test
    void waitTimeFromHeader_regexExtractsLeadingDigits() {
        WaitTimeFromHeaderBackoffStrategy s = new WaitTimeFromHeaderBackoffStrategy(
            "X-Backoff", "[0-9]+", null);
        assertEquals(45_000L, s.backoffMillis(stubResponseWithHeader("X-Backoff", "45 seconds"), 0));
    }

    @Test
    void waitTimeFromHeader_throwsWhenExceedsMaxWaitingTime() {
        WaitTimeFromHeaderBackoffStrategy s = new WaitTimeFromHeaderBackoffStrategy(
            "Retry-After", null, 30.0);
        ConnectException ex = assertThrows(ConnectException.class,
            () -> s.backoffMillis(stubResponseWithHeader("Retry-After", "120"), 0));
        assertTrue(ex.getMessage().contains("120"));
        assertTrue(ex.getMessage().contains("30"));
    }

    @Test
    void waitTimeFromHeader_emptyHeaderName_throws() {
        assertThrows(IllegalArgumentException.class,
            () -> new WaitTimeFromHeaderBackoffStrategy("", null, null));
    }

    // ── WaitUntilTimeFromHeaderBackoffStrategy ───────────────────────────────

    @Test
    void waitUntilTimeFromHeader_returnsDifference() {
        Clock fixed = Clock.fixed(Instant.ofEpochSecond(1_000_000), ZoneOffset.UTC);
        WaitUntilTimeFromHeaderBackoffStrategy s = new WaitUntilTimeFromHeaderBackoffStrategy(
            "X-RateLimit-Reset", null, null, fixed);
        Long result = s.backoffMillis(stubResponseWithHeader("X-RateLimit-Reset", "1000060"), 0);
        assertEquals(60_000L, result);
    }

    @Test
    void waitUntilTimeFromHeader_pastTimestamp_returnsNullWithoutMinWait() {
        Clock fixed = Clock.fixed(Instant.ofEpochSecond(1_000_000), ZoneOffset.UTC);
        WaitUntilTimeFromHeaderBackoffStrategy s = new WaitUntilTimeFromHeaderBackoffStrategy(
            "X-Reset", null, null, fixed);
        assertNull(s.backoffMillis(stubResponseWithHeader("X-Reset", "999000"), 0));
    }

    @Test
    void waitUntilTimeFromHeader_minWaitClampsResult() {
        Clock fixed = Clock.fixed(Instant.ofEpochSecond(1_000_000), ZoneOffset.UTC);
        WaitUntilTimeFromHeaderBackoffStrategy s = new WaitUntilTimeFromHeaderBackoffStrategy(
            "X-Reset", null, 100.0, fixed);
        // Header says 30s in the future, but min_wait is 100s → return 100s
        assertEquals(100_000L,
            s.backoffMillis(stubResponseWithHeader("X-Reset", "1000030"), 0));
    }

    @Test
    void waitUntilTimeFromHeader_missingHeader_returnsMinWaitWhenSet() {
        Clock fixed = Clock.fixed(Instant.ofEpochSecond(1_000_000), ZoneOffset.UTC);
        WaitUntilTimeFromHeaderBackoffStrategy s = new WaitUntilTimeFromHeaderBackoffStrategy(
            "X-Reset", null, 5.0, fixed);
        assertEquals(5_000L, s.backoffMillis(stubResponseWithHeader("X-Other", "irrelevant"), 0));
    }

    @Test
    void waitUntilTimeFromHeader_missingHeaderAndNoMinWait_returnsNull() {
        Clock fixed = Clock.fixed(Instant.ofEpochSecond(1_000_000), ZoneOffset.UTC);
        WaitUntilTimeFromHeaderBackoffStrategy s = new WaitUntilTimeFromHeaderBackoffStrategy(
            "X-Reset", null, null, fixed);
        assertNull(s.backoffMillis(stubResponseWithHeader("X-Other", "v"), 0));
    }

    // ── BackoffStrategyChain ─────────────────────────────────────────────────

    @Test
    void chain_firstNonNullWins() {
        BackoffStrategy alwaysNull = (resp, attempt) -> null;
        BackoffStrategy returns42 = (resp, attempt) -> 42L;
        BackoffStrategy returns99 = (resp, attempt) -> 99L;
        BackoffStrategyChain chain = new BackoffStrategyChain(List.of(alwaysNull, returns42, returns99));
        assertEquals(42L, chain.backoffMillis(null, 0));
    }

    @Test
    void chain_emptyList_fallsBackToExponential() {
        BackoffStrategyChain chain = new BackoffStrategyChain(List.of());
        // Default fallback is ExponentialBackoffStrategy with factor 5
        assertEquals(5_000L, chain.backoffMillis(null, 0));
    }

    @Test
    void chain_explicitNullFallback_returnsNullWhenAllNull() {
        BackoffStrategy alwaysNull = (resp, attempt) -> null;
        BackoffStrategyChain chain = new BackoffStrategyChain(List.of(alwaysNull), null);
        assertNull(chain.backoffMillis(null, 0));
    }

    // ── Test plumbing ────────────────────────────────────────────────────────

    private static HttpResponse<String> stubResponseWithHeader(String name, String value) {
        return new StubHttpResponse(name, value);
    }

    private static final class StubHttpResponse implements HttpResponse<String> {
        private final HttpHeaders headers;

        StubHttpResponse(String name, String value) {
            this.headers = HttpHeaders.of(Map.of(name, List.of(value)), (k, v) -> true);
        }

        @Override
        public int statusCode() {
            return 429;
        }

        @Override
        public HttpRequest request() {
            return null;
        }

        @Override
        public Optional<HttpResponse<String>> previousResponse() {
            return Optional.empty();
        }

        @Override
        public HttpHeaders headers() {
            return headers;
        }

        @Override
        public String body() {
            return "";
        }

        @Override
        public Optional<javax.net.ssl.SSLSession> sslSession() {
            return Optional.empty();
        }

        @Override
        public URI uri() {
            return URI.create("https://example.test/");
        }

        @Override
        public HttpClient.Version version() {
            return HttpClient.Version.HTTP_1_1;
        }
    }

    @Test
    void stubExposesHeaders() {
        // sanity check on the test stub itself — keeps assertNotNull import live
        assertNotNull(stubResponseWithHeader("X", "1").headers());
    }
}
