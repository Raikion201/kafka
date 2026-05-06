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

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryPolicyTest {

    // ── DefaultRetryPolicy: fallback table ────────────────────────────────────

    @Test
    void defaultPolicy_2xx_returnsSuccess() {
        DefaultRetryPolicy policy = DefaultRetryPolicy.fallbackOnly();
        ErrorResolution result = policy.interpretResponse(stubResponse(200, "{}"));
        assertSame(ErrorResolution.SUCCESS, result);
    }

    @Test
    void defaultPolicy_500_retriesViaFallbackMapping() {
        DefaultRetryPolicy policy = DefaultRetryPolicy.fallbackOnly();
        ErrorResolution result = policy.interpretResponse(stubResponse(500, ""));
        assertEquals(ResponseAction.RETRY, result.action());
        assertEquals("transient_error", result.failureType());
        assertTrue(result.errorMessage().contains("500"));
    }

    @Test
    void defaultPolicy_401_failsConfigError() {
        DefaultRetryPolicy policy = DefaultRetryPolicy.fallbackOnly();
        ErrorResolution result = policy.interpretResponse(stubResponse(401, ""));
        assertEquals(ResponseAction.FAIL, result.action());
        assertEquals("config_error", result.failureType());
    }

    @Test
    void defaultPolicy_429_isRateLimited() {
        DefaultRetryPolicy policy = DefaultRetryPolicy.fallbackOnly();
        ErrorResolution result = policy.interpretResponse(stubResponse(429, ""));
        assertEquals(ResponseAction.RATE_LIMITED, result.action());
    }

    @Test
    void defaultPolicy_unknownCode_fallsBackToRetryWithSystemError() {
        DefaultRetryPolicy policy = DefaultRetryPolicy.fallbackOnly();
        ErrorResolution result = policy.interpretResponse(stubResponse(599, ""));
        assertEquals(ResponseAction.RETRY, result.action());
        assertEquals("system_error", result.failureType());
    }

    @Test
    void defaultPolicy_defaultMaxRetriesAndMaxTime() {
        DefaultRetryPolicy policy = DefaultRetryPolicy.fallbackOnly();
        assertEquals(5, policy.maxRetries());
        assertEquals(600_000L, policy.maxTimeMillis());
    }

    @Test
    void defaultPolicy_explicitMaxRetriesAndMaxTime() {
        DefaultRetryPolicy policy = new DefaultRetryPolicy(Collections.emptyList(), 10, 30);
        assertEquals(10, policy.maxRetries());
        assertEquals(30_000L, policy.maxTimeMillis());
    }

    // ── DefaultRetryPolicy: response_filters ─────────────────────────────────

    @Test
    void httpCodeFilter_overridesFallback() {
        HttpResponseFilter filter = HttpResponseFilter.from(
            ResponseAction.IGNORE, List.of(401), null, null, "Paid plan required",
            null, Collections.emptyMap());
        DefaultRetryPolicy policy = new DefaultRetryPolicy(List.of(filter), null, null);
        ErrorResolution result = policy.interpretResponse(stubResponse(401, ""));
        assertEquals(ResponseAction.IGNORE, result.action());
        assertEquals("Paid plan required", result.errorMessage());
    }

    @Test
    void filterOrdering_firstMatchWins() {
        HttpResponseFilter ignore404 = HttpResponseFilter.from(
            ResponseAction.IGNORE, List.of(404), null, null, null, null, Collections.emptyMap());
        HttpResponseFilter retry404 = HttpResponseFilter.from(
            ResponseAction.RETRY, List.of(404), null, null, null, null, Collections.emptyMap());
        DefaultRetryPolicy policy = new DefaultRetryPolicy(List.of(ignore404, retry404), null, null);
        assertEquals(ResponseAction.IGNORE, policy.interpretResponse(stubResponse(404, "")).action());
    }

    @Test
    void errorMessageContains_matches() {
        HttpResponseFilter filter = HttpResponseFilter.from(
            ResponseAction.IGNORE, null, null, "rate exceeded", "Rate limit hit",
            null, Collections.emptyMap());
        DefaultRetryPolicy policy = new DefaultRetryPolicy(List.of(filter), null, null);
        ErrorResolution result = policy.interpretResponse(stubResponse(400, "{\"err\":\"rate exceeded today\"}"));
        assertEquals(ResponseAction.IGNORE, result.action());
        assertEquals("Rate limit hit", result.errorMessage());
    }

    @Test
    void predicate_jinjaTrue_matches() {
        HttpResponseFilter filter = HttpResponseFilter.from(
            ResponseAction.FAIL,
            null,
            "{{ response.error == 'bad' }}",
            null,
            "Custom failure: {{ response.error }}",
            "config_error",
            Collections.emptyMap());
        DefaultRetryPolicy policy = new DefaultRetryPolicy(List.of(filter), null, null);
        ErrorResolution result = policy.interpretResponse(stubResponse(403, "{\"error\":\"bad\"}"));
        assertEquals(ResponseAction.FAIL, result.action());
        assertEquals("config_error", result.failureType());
        assertEquals("Custom failure: bad", result.errorMessage());
    }

    @Test
    void predicate_jinjaFalse_doesNotMatch() {
        HttpResponseFilter filter = HttpResponseFilter.from(
            ResponseAction.FAIL,
            null,
            "{{ response.error == 'bad' }}",
            null,
            "X",
            "config_error",
            Collections.emptyMap());
        DefaultRetryPolicy policy = new DefaultRetryPolicy(List.of(filter), null, null);
        ErrorResolution result = policy.interpretResponse(stubResponse(403, "{\"error\":\"good\"}"));
        // Filter did not match → falls through to DEFAULT_ERROR_MAPPING for 403
        assertEquals(ResponseAction.FAIL, result.action());
        assertEquals("config_error", result.failureType());
        assertTrue(result.errorMessage().contains("403"));
    }

    // ── HttpResponseFilter: null response and corner cases ───────────────────

    @Test
    void filter_nullResponse_returnsNull() {
        HttpResponseFilter filter = HttpResponseFilter.from(
            ResponseAction.FAIL, List.of(401), null, null, null, null, Collections.emptyMap());
        assertNull(filter.matches(null));
    }

    @Test
    void filter_from_returnsNullIfActionMissing() {
        assertNull(HttpResponseFilter.from(null, List.of(401), null, null, null, null, null));
    }

    @Test
    void filter_invalidJsonBody_predicateStillEvaluatesAgainstEmptyMap() {
        HttpResponseFilter filter = HttpResponseFilter.from(
            ResponseAction.RETRY,
            null,
            "{{ response == {} }}",
            null,
            null,
            null,
            Collections.emptyMap());
        DefaultRetryPolicy policy = new DefaultRetryPolicy(List.of(filter), null, null);
        ErrorResolution result = policy.interpretResponse(stubResponse(500, "<html>not json</html>"));
        assertEquals(ResponseAction.RETRY, result.action());
    }

    // ── CompositeRetryPolicy ─────────────────────────────────────────────────

    @Test
    void composite_firstChildSuccessWins() {
        HttpResponseFilter ignore401 = HttpResponseFilter.from(
            ResponseAction.IGNORE, List.of(401), null, null, "ignored", null, Collections.emptyMap());
        DefaultRetryPolicy ignoreChild = new DefaultRetryPolicy(List.of(ignore401), null, null);
        DefaultRetryPolicy fallback = DefaultRetryPolicy.fallbackOnly();
        CompositeRetryPolicy composite = new CompositeRetryPolicy(List.of(ignoreChild, fallback));
        ErrorResolution result = composite.interpretResponse(stubResponse(401, ""));
        assertEquals(ResponseAction.IGNORE, result.action());
    }

    @Test
    void composite_emptyChildren_throws() {
        assertThrows(IllegalArgumentException.class,
            () -> new CompositeRetryPolicy(Collections.emptyList()));
    }

    @Test
    void composite_maxRetries_takesFromFirstChild() {
        DefaultRetryPolicy first = new DefaultRetryPolicy(Collections.emptyList(), 7, 30);
        DefaultRetryPolicy second = new DefaultRetryPolicy(Collections.emptyList(), 99, 60);
        CompositeRetryPolicy composite = new CompositeRetryPolicy(List.of(first, second));
        assertEquals(7, composite.maxRetries());
    }

    @Test
    void composite_maxTime_takesMaxAcrossChildren() {
        DefaultRetryPolicy first = new DefaultRetryPolicy(Collections.emptyList(), 7, 30);
        DefaultRetryPolicy second = new DefaultRetryPolicy(Collections.emptyList(), 99, 90);
        CompositeRetryPolicy composite = new CompositeRetryPolicy(List.of(first, second));
        assertEquals(90_000L, composite.maxTimeMillis());
    }

    @Test
    void composite_secondChildWinsWhenFirstReturnsNonTerminal() {
        // No filter on first child + 200 response → SUCCESS, terminal — verify the inverse:
        // first child returns FAIL (not in terminal list), second returns RETRY (terminal) → RETRY wins.
        // Build a filter-only policy that returns FAIL on 500, with no terminal action.
        HttpResponseFilter failFilter = HttpResponseFilter.from(
            ResponseAction.FAIL, List.of(500), null, null, "stop", null, Collections.emptyMap());
        DefaultRetryPolicy failChild = new DefaultRetryPolicy(List.of(failFilter), null, null);
        DefaultRetryPolicy fallback = DefaultRetryPolicy.fallbackOnly();
        CompositeRetryPolicy composite = new CompositeRetryPolicy(List.of(failChild, fallback));
        // First child says FAIL (non-terminal in composite ordering); second child (fallback) says RETRY for 500.
        assertEquals(ResponseAction.RETRY, composite.interpretResponse(stubResponse(500, "")).action());
    }

    // ── Test plumbing ────────────────────────────────────────────────────────

    private static HttpResponse<String> stubResponse(int statusCode, String body) {
        return new StubHttpResponse(statusCode, body);
    }

    /**
     * Minimal {@link HttpResponse} used by tests — enough surface for {@code statusCode()},
     * {@code body()}, and {@code headers()}. JDK HttpResponse is an interface with default
     * methods we don't exercise here.
     */
    private static final class StubHttpResponse implements HttpResponse<String> {
        private final int statusCode;
        private final String body;

        StubHttpResponse(int statusCode, String body) {
            this.statusCode = statusCode;
            this.body = body;
        }

        @Override
        public int statusCode() {
            return statusCode;
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
            return HttpHeaders.of(Map.of("Content-Type", List.of("application/json")), filterAll());
        }

        @Override
        public String body() {
            return body;
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

        private static java.util.function.BiPredicate<String, String> filterAll() {
            return (k, v) -> true;
        }

        @SuppressWarnings("unused")
        private Predicate<Set<String>> dummy() {
            return s -> true;
        }
    }

    @Test
    void stubResponseHasNotNullHeaders() {
        // sanity check on the stub itself — assertNotNull keeps the import live
        assertNotNull(stubResponse(200, "").headers());
    }
}
