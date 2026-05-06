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

import org.apache.kafka.connect.manifest.codegen.runtime.jinja.JinjaRenderer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Java port of Airbyte's {@code HttpResponseFilter} (http_response_filter.py:24).
 *
 * <p>A filter matches a response when any of {@code httpCodes}, {@code predicate}, or
 * {@code errorMessageContains} succeeds. On match, it produces an {@link ErrorResolution}
 * combining the configured action with default {@code failureType} / {@code errorMessage}
 * from {@link DefaultErrorMapping} when not explicitly overridden — matching Python lines
 * 87-109.</p>
 *
 * <p>Predicates and error_message templates are evaluated via {@link JinjaRenderer} with
 * the same {@code response} / {@code headers} context names Airbyte exposes (lines
 * 154-156, 162-166).</p>
 */
public final class HttpResponseFilter {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final ResponseAction action;
    private final Set<Integer> httpCodes;
    private final String predicate;
    private final String errorMessageContains;
    private final String errorMessageTemplate;
    private final String failureType;
    private final Map<String, Object> jinjaContext;

    /**
     * @param action filter action; must be set whenever any matcher is set
     * @param httpCodes matching status codes (empty if not used)
     * @param predicate Jinja boolean expression with {@code response} / {@code headers} bindings
     * @param errorMessageContains substring match against the response body
     * @param errorMessageTemplate Jinja template producing the user-visible error message
     * @param failureType raw {@code failure_type} string from the manifest, or {@code null}
     * @param jinjaContext base context (typically {@code {"config": ...}}) merged into per-call context
     */
    public HttpResponseFilter(
        ResponseAction action,
        Set<Integer> httpCodes,
        String predicate,
        String errorMessageContains,
        String errorMessageTemplate,
        String failureType,
        Map<String, Object> jinjaContext
    ) {
        this.action = action;
        this.httpCodes = httpCodes == null ? Collections.emptySet() : httpCodes;
        this.predicate = predicate;
        this.errorMessageContains = errorMessageContains;
        this.errorMessageTemplate = errorMessageTemplate;
        this.failureType = failureType;
        this.jinjaContext = jinjaContext == null ? Collections.emptyMap() : jinjaContext;
    }

    /** Returns the resolution if this filter matches the response, otherwise {@code null}. */
    public ErrorResolution matches(HttpResponse<String> response) {
        if (response == null || action == null) {
            return null;
        }
        if (!conditionMatches(response)) {
            return null;
        }
        return buildResolution(response);
    }

    private boolean conditionMatches(HttpResponse<String> response) {
        if (httpCodes.contains(response.statusCode())) {
            return true;
        }
        if (predicate != null && !predicate.isEmpty() && evaluatePredicate(response)) {
            return true;
        }
        return errorMessageContains != null
            && !errorMessageContains.isEmpty()
            && response.body() != null
            && response.body().contains(errorMessageContains);
    }

    private boolean evaluatePredicate(HttpResponse<String> response) {
        Map<String, Object> ctx = newContextFor(response);
        String rendered = JinjaRenderer.renderLenient(predicate, ctx);
        if (rendered == null) {
            return false;
        }
        String trimmed = rendered.trim();
        return trimmed.equalsIgnoreCase("true") || trimmed.equals("1");
    }

    private ErrorResolution buildResolution(HttpResponse<String> response) {
        ErrorResolution defaultResolution =
            DefaultErrorMapping.resolve(response.statusCode()).orElse(null);

        String resolvedFailureType = failureType;
        if (resolvedFailureType == null || resolvedFailureType.isEmpty()) {
            resolvedFailureType = defaultResolution != null
                ? defaultResolution.failureType()
                : "system_error";
        }

        String message = renderMessage(response);
        if ((message == null || message.isEmpty()) && defaultResolution != null) {
            message = defaultResolution.errorMessage();
        }

        return new ErrorResolution(action, resolvedFailureType, message);
    }

    private String renderMessage(HttpResponse<String> response) {
        if (errorMessageTemplate == null || errorMessageTemplate.isEmpty()) {
            return null;
        }
        return JinjaRenderer.renderLenient(errorMessageTemplate, newContextFor(response));
    }

    private Map<String, Object> newContextFor(HttpResponse<String> response) {
        Map<String, Object> ctx = new LinkedHashMap<>(jinjaContext);
        ctx.put("response", parseJsonBody(response.body()));
        ctx.put("headers", flattenHeaders(response));
        return ctx;
    }

    private static Object parseJsonBody(String body) {
        if (body == null || body.isEmpty()) {
            return Collections.emptyMap();
        }
        try {
            return JSON.readValue(body, Object.class);
        } catch (Exception ignored) {
            // Mirrors Python _safe_response_json (http_response_filter.py:142): non-JSON => {}.
            return Collections.emptyMap();
        }
    }

    private static Map<String, String> flattenHeaders(HttpResponse<String> response) {
        Map<String, String> out = new HashMap<>();
        response.headers().map().forEach((k, vs) -> {
            if (!vs.isEmpty()) {
                out.put(k, String.join(",", vs));
            }
        });
        return out;
    }

    /**
     * Build a typed filter from a raw map produced by Jackson / manifest parsing.
     * Returns {@code null} when {@code raw} has no action — matching the relaxed
     * behaviour of {@code DefaultErrorHandler.__post_init__} which auto-creates a
     * default filter if none is set.
     */
    public static HttpResponseFilter from(
        ResponseAction action,
        List<Integer> httpCodes,
        String predicate,
        String errorMessageContains,
        String errorMessageTemplate,
        String failureType,
        Map<String, Object> jinjaContext
    ) {
        if (action == null) {
            return null;
        }
        Set<Integer> codeSet = httpCodes == null
            ? Collections.emptySet()
            : Set.copyOf(httpCodes);
        return new HttpResponseFilter(
            action,
            codeSet,
            predicate,
            errorMessageContains,
            errorMessageTemplate,
            failureType,
            Objects.requireNonNullElse(jinjaContext, Collections.emptyMap())
        );
    }
}
