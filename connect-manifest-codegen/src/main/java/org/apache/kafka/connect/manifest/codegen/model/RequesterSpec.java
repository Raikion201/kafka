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
package org.apache.kafka.connect.manifest.codegen.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Models the {@code requester} block — the HTTP request configuration.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RequesterSpec {

    private String type;

    /** Airbyte CDK Custom* node identifier. Non-null only when type starts with "Custom". */
    @JsonProperty("class_name")
    private String className;

    /** Full URL — used when there is no separate url_base + path. */
    private String url;

    @JsonProperty("url_base")
    private String urlBase;

    private String path = "";

    @JsonProperty("http_method")
    private String httpMethod = "GET";

    private AuthenticatorSpec authenticator;

    @JsonProperty("request_parameters")
    @JsonDeserialize(using = StringMapTolerantDeserializer.class)
    private Map<String, String> requestParameters = Collections.emptyMap();

    @JsonProperty("request_headers")
    @JsonDeserialize(using = StringMapTolerantDeserializer.class)
    private Map<String, String> requestHeaders = Collections.emptyMap();

    /**
     * JSON body for POST/PUT requests. Each top-level value may be a Jinja template string
     * or a nested structure (map, list, scalar). Mirrors Airbyte's {@code request_body_json}.
     */
    @JsonProperty("request_body_json")
    private Map<String, Object> requestBodyJson;

    /**
     * Body for POST/PUT requests as a raw string or form-encoded key=value pairs.
     * Mirrors Airbyte's {@code request_body_data} which accepts either a string or a mapping.
     */
    @JsonProperty("request_body_data")
    @JsonDeserialize(using = BodyDataSpecDeserializer.class)
    private BodyDataSpec requestBodyData;

    @JsonProperty("error_handler")
    private ErrorHandlerSpec errorHandler;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUrlBase() {
        return urlBase;
    }

    public void setUrlBase(String urlBase) {
        this.urlBase = urlBase;
    }

    public String getPath() {
        return path == null ? "" : path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getHttpMethod() {
        return httpMethod == null ? "GET" : httpMethod;
    }

    public void setHttpMethod(String httpMethod) {
        this.httpMethod = httpMethod;
    }

    public AuthenticatorSpec getAuthenticator() {
        return authenticator;
    }

    public void setAuthenticator(AuthenticatorSpec authenticator) {
        this.authenticator = authenticator;
    }

    public Map<String, String> getRequestParameters() {
        return requestParameters == null ? Collections.emptyMap() : requestParameters;
    }

    public void setRequestParameters(Map<String, String> requestParameters) {
        this.requestParameters = requestParameters;
    }

    public Map<String, String> getRequestHeaders() {
        return requestHeaders == null ? Collections.emptyMap() : requestHeaders;
    }

    public void setRequestHeaders(Map<String, String> requestHeaders) {
        this.requestHeaders = requestHeaders;
    }

    public Map<String, Object> getRequestBodyJson() {
        return requestBodyJson;
    }

    public void setRequestBodyJson(Map<String, Object> requestBodyJson) {
        this.requestBodyJson = requestBodyJson;
    }

    public BodyDataSpec getRequestBodyData() {
        return requestBodyData;
    }

    public void setRequestBodyData(BodyDataSpec requestBodyData) {
        this.requestBodyData = requestBodyData;
    }

    /** Returns true if this requester has an explicit body payload. */
    public boolean hasRequestBody() {
        return requestBodyJson != null || requestBodyData != null;
    }

    public ErrorHandlerSpec getErrorHandler() {
        return errorHandler;
    }

    public void setErrorHandler(ErrorHandlerSpec errorHandler) {
        this.errorHandler = errorHandler;
    }

    /**
     * Detects the "list-cycle" pattern: returns the config field name used as a
     * comma-separated list iterated by page index, e.g. {@code config['tickers'].split(',')}.
     * Returns {@code null} if this pattern is not present.
     */
    public String listCycleConfigField() {
        Pattern p = Pattern.compile("config\\['(\\w+)'\\]\\.split\\(',");
        if (path != null) {
            Matcher m = p.matcher(path);
            if (m.find()) return m.group(1);
        }
        for (String v : getRequestParameters().values()) {
            Matcher m = p.matcher(v);
            if (m.find()) return m.group(1);
        }
        return null;
    }

    /**
     * For a list-cycle path, extracts the literal path prefix that appears between
     * the closing brace of the {@code if}-tag and the opening of the config-field
     * interpolation, e.g. {@code /v8/finance/chart/}.
     */
    public String listCyclePathPrefix() {
        if (path == null) return "";
        Matcher m = Pattern.compile("%\\}([^{%]+)\\{\\{[^}]*config\\['\\w+'\\]\\.split").matcher(path);
        if (m.find()) return m.group(1).strip();
        return "";
    }

    /** HTTP status codes that the error_handler treats as SUCCESS (e.g. 403 for Yahoo Finance). */
    public Set<Integer> successHttpCodes() {
        return errorHandler != null ? errorHandler.getSuccessHttpCodes() : Collections.emptySet();
    }

    /**
     * Holds the value of {@code request_body_data} which Airbyte allows as either a plain
     * string (raw body) or a mapping of form fields.
     */
    public static final class BodyDataSpec {
        private final String rawBody;
        private final Map<String, String> formFields;

        public BodyDataSpec(String rawBody) {
            this.rawBody = rawBody;
            this.formFields = null;
        }

        public BodyDataSpec(Map<String, String> formFields) {
            this.rawBody = null;
            this.formFields = formFields;
        }

        /** Non-null when the YAML value was a scalar string. */
        public String getRawBody() {
            return rawBody;
        }

        /** Non-null when the YAML value was a mapping of form fields. */
        public Map<String, String> getFormFields() {
            return formFields;
        }

        public boolean isRaw() {
            return rawBody != null;
        }
    }

    /** Deserializes {@code request_body_data} as either a raw string or a form-field map. */
    static final class BodyDataSpecDeserializer
            extends com.fasterxml.jackson.databind.JsonDeserializer<BodyDataSpec> {
        @Override
        public BodyDataSpec deserialize(com.fasterxml.jackson.core.JsonParser p,
                                        com.fasterxml.jackson.databind.DeserializationContext ctxt)
                throws java.io.IOException {
            com.fasterxml.jackson.databind.JsonNode node = p.readValueAsTree();
            if (node == null || node.isNull()) {
                return null;
            }
            if (node.isTextual()) {
                return new BodyDataSpec(node.asText());
            }
            if (node.isObject()) {
                Map<String, String> out = new LinkedHashMap<>();
                for (Map.Entry<String, com.fasterxml.jackson.databind.JsonNode> e : node.properties()) {
                    com.fasterxml.jackson.databind.JsonNode v = e.getValue();
                    if (v == null || v.isNull()) continue;
                    out.put(e.getKey(), v.isTextual() ? v.asText() : v.toString());
                }
                return new BodyDataSpec(out);
            }
            return new BodyDataSpec(node.toString());
        }
    }

    /**
     * Tolerant deserializer for {@code request_parameters} / {@code request_headers}.
     * Airbyte allows non-string values (objects with type/value, arrays of values, ints,
     * booleans). This collapses everything into the {@code Map<String,String>} the codegen
     * expects: scalars become their text form; objects use a {@code value} child if present,
     * else the whole node serialised; arrays are joined with commas; nulls are skipped.
     */
    static final class StringMapTolerantDeserializer
            extends com.fasterxml.jackson.databind.JsonDeserializer<Map<String, String>> {
        @Override
        public Map<String, String> deserialize(com.fasterxml.jackson.core.JsonParser p,
                                               com.fasterxml.jackson.databind.DeserializationContext ctxt)
                throws java.io.IOException {
            com.fasterxml.jackson.databind.JsonNode root = p.readValueAsTree();
            if (root == null || root.isNull() || !root.isObject()) {
                return Collections.emptyMap();
            }
            Map<String, String> out = new java.util.LinkedHashMap<>();
            for (Map.Entry<String, com.fasterxml.jackson.databind.JsonNode> e : root.properties()) {
                com.fasterxml.jackson.databind.JsonNode v = e.getValue();
                if (v == null || v.isNull()) continue;
                if (v.isTextual()) {
                    out.put(e.getKey(), v.asText());
                } else if (v.isNumber() || v.isBoolean()) {
                    out.put(e.getKey(), v.asText());
                } else if (v.isArray()) {
                    StringBuilder sb = new StringBuilder();
                    for (com.fasterxml.jackson.databind.JsonNode child : v) {
                        if (sb.length() > 0) sb.append(',');
                        sb.append(child.isTextual() ? child.asText() : child.toString());
                    }
                    out.put(e.getKey(), sb.toString());
                } else if (v.isObject()) {
                    if (v.hasNonNull("value")) {
                        out.put(e.getKey(), v.get("value").asText());
                    } else {
                        out.put(e.getKey(), v.toString());
                    }
                }
            }
            return out;
        }
    }

    /** Returns the effective base URL, preferring {@code url} over {@code url_base}. */
    public String effectiveBaseUrl() {
        if (url != null && !url.isBlank()) {
            return url;
        }
        return urlBase == null ? "" : urlBase;
    }

    /**
     * Accepts a number or a non-numeric string (Jinja templates such as
     * {@code "{{ config.backoff_factor }}"}) and yields {@code null} for the latter.
     * Phase 1 limitation: templated numeric retry settings fall back to defaults — flagged in
     * the connector docs and tracked for a later phase that wires runtime Jinja into init.
     */
    static final class TolerantDoubleDeserializer
            extends com.fasterxml.jackson.databind.JsonDeserializer<Double> {
        @Override
        public Double deserialize(com.fasterxml.jackson.core.JsonParser p,
                                  com.fasterxml.jackson.databind.DeserializationContext ctxt)
                throws java.io.IOException {
            com.fasterxml.jackson.databind.JsonNode n = p.readValueAsTree();
            if (n == null || n.isNull()) {
                return null;
            }
            if (n.isNumber()) {
                return n.asDouble();
            }
            if (n.isTextual()) {
                String s = n.asText();
                if (s == null || s.isEmpty()) {
                    return null;
                }
                try {
                    return Double.parseDouble(s);
                } catch (NumberFormatException ignored) {
                    return null;
                }
            }
            return null;
        }
    }

    /** Sibling of {@link TolerantDoubleDeserializer} for {@code Integer}-valued fields. */
    static final class TolerantIntegerDeserializer
            extends com.fasterxml.jackson.databind.JsonDeserializer<Integer> {
        @Override
        public Integer deserialize(com.fasterxml.jackson.core.JsonParser p,
                                   com.fasterxml.jackson.databind.DeserializationContext ctxt)
                throws java.io.IOException {
            com.fasterxml.jackson.databind.JsonNode n = p.readValueAsTree();
            if (n == null || n.isNull()) {
                return null;
            }
            if (n.isNumber()) {
                return n.asInt();
            }
            if (n.isTextual()) {
                String s = n.asText();
                if (s == null || s.isEmpty()) {
                    return null;
                }
                try {
                    return Integer.parseInt(s);
                } catch (NumberFormatException ignored) {
                    return null;
                }
            }
            return null;
        }
    }

    /** Models the {@code error_handler} block — handles non-2xx responses. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ErrorHandlerSpec {

        private String type;

        @JsonProperty("class_name")
        private String className;

        @JsonProperty("error_handlers")
        private List<ErrorHandlerSpec> errorHandlers = Collections.emptyList();

        @JsonProperty("response_filters")
        @JsonDeserialize(using = ResponseFilterListDeserializer.class)
        private List<ResponseFilterSpec> responseFilters = Collections.emptyList();

        @JsonProperty("max_retries")
        @JsonDeserialize(using = TolerantIntegerDeserializer.class)
        private Integer maxRetries;

        @JsonProperty("max_time")
        @JsonDeserialize(using = TolerantIntegerDeserializer.class)
        private Integer maxTime;

        @JsonProperty("backoff_strategies")
        private List<BackoffStrategySpec> backoffStrategies = Collections.emptyList();

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getClassName() {
            return className;
        }

        public void setClassName(String className) {
            this.className = className;
        }

        public List<ErrorHandlerSpec> getErrorHandlers() {
            return errorHandlers == null ? Collections.emptyList() : errorHandlers;
        }

        public void setErrorHandlers(List<ErrorHandlerSpec> v) {
            this.errorHandlers = v;
        }

        public List<ResponseFilterSpec> getResponseFilters() {
            return responseFilters == null ? Collections.emptyList() : responseFilters;
        }

        public void setResponseFilters(List<ResponseFilterSpec> v) {
            this.responseFilters = v;
        }

        public Integer getMaxRetries() {
            return maxRetries;
        }

        public void setMaxRetries(Integer v) {
            this.maxRetries = v;
        }

        public Integer getMaxTime() {
            return maxTime;
        }

        public void setMaxTime(Integer v) {
            this.maxTime = v;
        }

        public List<BackoffStrategySpec> getBackoffStrategies() {
            return backoffStrategies == null ? Collections.emptyList() : backoffStrategies;
        }

        public void setBackoffStrategies(List<BackoffStrategySpec> v) {
            this.backoffStrategies = v;
        }

        /** Recursively collects HTTP status codes mapped to SUCCESS action. */
        public Set<Integer> getSuccessHttpCodes() {
            Set<Integer> codes = new HashSet<>();
            for (ResponseFilterSpec f : getResponseFilters()) {
                if ("SUCCESS".equalsIgnoreCase(f.getAction())) {
                    codes.addAll(f.getHttpCodes());
                }
            }
            for (ErrorHandlerSpec nested : getErrorHandlers()) {
                codes.addAll(nested.getSuccessHttpCodes());
            }
            return codes;
        }
    }

    /** Models one entry inside {@code response_filters}. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ResponseFilterSpec {

        private String action;

        @JsonProperty("http_codes")
        private List<Integer> httpCodes = Collections.emptyList();

        @JsonProperty("error_message_contains")
        private String errorMessageContains;

        private String predicate;

        @JsonProperty("error_message")
        private String errorMessage;

        @JsonProperty("failure_type")
        private String failureType;

        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
        }

        public List<Integer> getHttpCodes() {
            return httpCodes == null ? Collections.emptyList() : httpCodes;
        }

        public void setHttpCodes(List<Integer> v) {
            this.httpCodes = v;
        }

        public String getErrorMessageContains() {
            return errorMessageContains;
        }

        public void setErrorMessageContains(String v) {
            this.errorMessageContains = v;
        }

        public String getPredicate() {
            return predicate;
        }

        public void setPredicate(String v) {
            this.predicate = v;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public void setErrorMessage(String v) {
            this.errorMessage = v;
        }

        public String getFailureType() {
            return failureType;
        }

        public void setFailureType(String v) {
            this.failureType = v;
        }
    }

    /**
     * Models one entry inside {@code backoff_strategies}. The {@code type} field
     * selects which subset of attributes is meaningful at runtime. Mirrors:
     * <ul>
     *   <li>ConstantBackoff → backoffTimeInSeconds</li>
     *   <li>ExponentialBackoff → factor (default 5)</li>
     *   <li>WaitTimeFromHeader → header, regex?, maxWaitingTimeInSeconds?</li>
     *   <li>WaitUntilTimeFromHeader → header, minWait?, regex?</li>
     * </ul>
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class BackoffStrategySpec {

        private String type;

        @JsonProperty("backoff_time_in_seconds")
        @JsonDeserialize(using = TolerantDoubleDeserializer.class)
        private Double backoffTimeInSeconds;

        @JsonDeserialize(using = TolerantDoubleDeserializer.class)
        private Double factor;

        private String header;

        private String regex;

        @JsonProperty("max_waiting_time_in_seconds")
        @JsonDeserialize(using = TolerantDoubleDeserializer.class)
        private Double maxWaitingTimeInSeconds;

        @JsonProperty("min_wait")
        @JsonDeserialize(using = TolerantDoubleDeserializer.class)
        private Double minWait;

        public String getType() {
            return type;
        }

        public void setType(String v) {
            this.type = v;
        }

        public Double getBackoffTimeInSeconds() {
            return backoffTimeInSeconds;
        }

        public void setBackoffTimeInSeconds(Double v) {
            this.backoffTimeInSeconds = v;
        }

        public Double getFactor() {
            return factor;
        }

        public void setFactor(Double v) {
            this.factor = v;
        }

        public String getHeader() {
            return header;
        }

        public void setHeader(String v) {
            this.header = v;
        }

        public String getRegex() {
            return regex;
        }

        public void setRegex(String v) {
            this.regex = v;
        }

        public Double getMaxWaitingTimeInSeconds() {
            return maxWaitingTimeInSeconds;
        }

        public void setMaxWaitingTimeInSeconds(Double v) {
            this.maxWaitingTimeInSeconds = v;
        }

        public Double getMinWait() {
            return minWait;
        }

        public void setMinWait(Double v) {
            this.minWait = v;
        }
    }
}
