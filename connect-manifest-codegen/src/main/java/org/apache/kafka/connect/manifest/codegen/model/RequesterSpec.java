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

    @JsonProperty("error_handler")
    private ErrorHandlerSpec errorHandler;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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

    /** Models the {@code error_handler} block — handles non-2xx responses. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ErrorHandlerSpec {

        private String type;

        @JsonProperty("error_handlers")
        private List<ErrorHandlerSpec> errorHandlers = Collections.emptyList();

        @JsonProperty("response_filters")
        @JsonDeserialize(using = ResponseFilterListDeserializer.class)
        private List<ResponseFilterSpec> responseFilters = Collections.emptyList();

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
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
    }
}
