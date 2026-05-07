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

import java.util.List;
import java.util.Map;

/**
 * Models the {@code authenticator} block inside a requester.
 * Supported types: NoAuth, ApiKeyAuthenticator, BearerAuthenticator,
 * BasicHttpAuthenticator, OAuthAuthenticator.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AuthenticatorSpec {

    private String type;

    /** Airbyte CDK Custom* node identifier. Non-null only when type starts with "Custom". */
    @JsonProperty("class_name")
    private String className;

    /** Header name for ApiKeyAuthenticator (legacy field). */
    @JsonProperty("header")
    private String header;

    /** Config template for the API/bearer token. */
    @JsonProperty("api_token")
    private String apiToken;

    /** Where to inject the API key (header or query param). */
    @JsonProperty("inject_into")
    private InjectIntoSpec injectInto;

    // ── BasicHttpAuthenticator ────────────────────────────────────────────────

    @JsonProperty("username")
    private String username;

    @JsonProperty("password")
    private String password;

    // ── OAuthAuthenticator ────────────────────────────────────────────────────

    @JsonProperty("client_id")
    private String clientId;

    @JsonProperty("client_secret")
    private String clientSecret;

    @JsonProperty("refresh_token")
    private String refreshToken;

    @JsonProperty("token_refresh_endpoint")
    private String tokenRefreshEndpoint;

    @JsonProperty("access_token_name")
    private String accessTokenName;

    @JsonProperty("scopes")
    private List<String> scopes;

    /** "refresh_token" (default) or "client_credentials" for OAuthAuthenticator. */
    @JsonProperty("grant_type")
    private String grantType;

    /** Extra POST body fields for client_credentials flow (e.g. box_subject_id). */
    @JsonProperty("refresh_request_body")
    private Map<String, String> refreshRequestBody;

    // ── SessionTokenAuthenticator ─────────────────────────────────────────────

    @JsonProperty("login_requester")
    private LoginRequesterSpec loginRequester;

    @JsonProperty("session_token_path")
    private List<String> sessionTokenPath;

    // ── LegacySessionTokenAuthenticator ──────────────────────────────────────

    @JsonProperty("login_url")
    private String loginUrl;

    @JsonProperty("session_token_response_key")
    private String sessionTokenResponseKey;

    @JsonProperty("validate_session_url")
    private String validateSessionUrl;

    // ── JwtAuthenticator ──────────────────────────────────────────────────────

    @JsonProperty("secret_key")
    private String secretKey;

    @JsonProperty("algorithm")
    private String algorithm;

    @JsonProperty("token_duration")
    private Integer tokenDuration;

    @JsonProperty("header_prefix")
    private String headerPrefix;

    @JsonProperty("jwt_payload")
    private Map<String, String> jwtPayload;

    @JsonProperty("additional_jwt_headers")
    private Map<String, String> additionalJwtHeaders;

    @JsonProperty("additional_jwt_payload")
    private Map<String, String> additionalJwtPayload;

    // ── SelectiveAuthenticator ────────────────────────────────────────────────

    /** Path into the config object that selects which inner authenticator to use, e.g. ["credentials","auth_type"]. */
    @JsonProperty("authenticator_selection_path")
    private List<String> authenticatorSelectionPath;

    /** Map of selection-value → inner AuthenticatorSpec (resolved from $ref by ManifestParser). */
    @JsonProperty("authenticators")
    private Map<String, AuthenticatorSpec> authenticators;

    // ── getters / setters ─────────────────────────────────────────────────────

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

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public String getApiToken() {
        return apiToken;
    }

    public void setApiToken(String apiToken) {
        this.apiToken = apiToken;
    }

    public InjectIntoSpec getInjectInto() {
        return injectInto;
    }

    public void setInjectInto(InjectIntoSpec injectInto) {
        this.injectInto = injectInto;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public void setClientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public void setRefreshToken(String refreshToken) {
        this.refreshToken = refreshToken;
    }

    public String getTokenRefreshEndpoint() {
        return tokenRefreshEndpoint;
    }

    public void setTokenRefreshEndpoint(String tokenRefreshEndpoint) {
        this.tokenRefreshEndpoint = tokenRefreshEndpoint;
    }

    public String getAccessTokenName() {
        return accessTokenName == null ? "access_token" : accessTokenName;
    }

    public void setAccessTokenName(String accessTokenName) {
        this.accessTokenName = accessTokenName;
    }

    public String getGrantType() {
        return grantType;
    }

    public void setGrantType(String grantType) {
        this.grantType = grantType;
    }

    public Map<String, String> getRefreshRequestBody() {
        return refreshRequestBody;
    }

    public void setRefreshRequestBody(Map<String, String> refreshRequestBody) {
        this.refreshRequestBody = refreshRequestBody;
    }

    public LoginRequesterSpec getLoginRequester() {
        return loginRequester;
    }

    public void setLoginRequester(LoginRequesterSpec loginRequester) {
        this.loginRequester = loginRequester;
    }

    public List<String> getSessionTokenPath() {
        return sessionTokenPath;
    }

    public void setSessionTokenPath(List<String> sessionTokenPath) {
        this.sessionTokenPath = sessionTokenPath;
    }

    public String getLoginUrl() {
        return loginUrl == null ? "" : loginUrl;
    }

    public void setLoginUrl(String loginUrl) {
        this.loginUrl = loginUrl;
    }

    public String getSessionTokenResponseKey() {
        return sessionTokenResponseKey == null ? "id" : sessionTokenResponseKey;
    }

    public void setSessionTokenResponseKey(String sessionTokenResponseKey) {
        this.sessionTokenResponseKey = sessionTokenResponseKey;
    }

    public String getValidateSessionUrl() {
        return validateSessionUrl;
    }

    public void setValidateSessionUrl(String validateSessionUrl) {
        this.validateSessionUrl = validateSessionUrl;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getAlgorithm() {
        return algorithm == null ? "RS256" : algorithm;
    }

    public void setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
    }

    public int getTokenDuration() {
        return tokenDuration == null ? 3600 : tokenDuration;
    }

    public void setTokenDuration(Integer tokenDuration) {
        this.tokenDuration = tokenDuration;
    }

    public String getHeaderPrefix() {
        return headerPrefix == null ? "Bearer" : headerPrefix;
    }

    public void setHeaderPrefix(String headerPrefix) {
        this.headerPrefix = headerPrefix;
    }

    public Map<String, String> getJwtPayload() {
        return jwtPayload;
    }

    public void setJwtPayload(Map<String, String> jwtPayload) {
        this.jwtPayload = jwtPayload;
    }

    public Map<String, String> getAdditionalJwtHeaders() {
        return additionalJwtHeaders;
    }

    public void setAdditionalJwtHeaders(Map<String, String> additionalJwtHeaders) {
        this.additionalJwtHeaders = additionalJwtHeaders;
    }

    public Map<String, String> getAdditionalJwtPayload() {
        return additionalJwtPayload;
    }

    public void setAdditionalJwtPayload(Map<String, String> additionalJwtPayload) {
        this.additionalJwtPayload = additionalJwtPayload;
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    public boolean isNoAuth() {
        return type == null || type.equalsIgnoreCase("NoAuth");
    }

    public boolean isApiKey() {
        return "ApiKeyAuthenticator".equalsIgnoreCase(type);
    }

    public boolean isBearer() {
        return "BearerAuthenticator".equalsIgnoreCase(type);
    }

    public boolean isBasicHttp() {
        return "BasicHttpAuthenticator".equalsIgnoreCase(type);
    }

    public boolean isOAuth() {
        return "OAuthAuthenticator".equalsIgnoreCase(type);
    }

    public boolean isClientCredentials() {
        return "client_credentials".equalsIgnoreCase(grantType);
    }

    public boolean isSessionToken() {
        return "SessionTokenAuthenticator".equalsIgnoreCase(type);
    }

    public boolean isLegacySessionToken() {
        return "LegacySessionTokenAuthenticator".equalsIgnoreCase(type);
    }

    public boolean isJwt() {
        return "JwtAuthenticator".equalsIgnoreCase(type);
    }

    public boolean isSelective() {
        return "SelectiveAuthenticator".equalsIgnoreCase(type);
    }

    public List<String> getAuthenticatorSelectionPath() {
        return authenticatorSelectionPath == null ? java.util.Collections.emptyList() : authenticatorSelectionPath;
    }

    public void setAuthenticatorSelectionPath(List<String> v) {
        this.authenticatorSelectionPath = v;
    }

    public Map<String, AuthenticatorSpec> getAuthenticators() {
        return authenticators == null ? java.util.Collections.emptyMap() : authenticators;
    }

    public void setAuthenticators(Map<String, AuthenticatorSpec> v) {
        this.authenticators = v;
    }

    /** First OAuth authenticator inside a SelectiveAuthenticator's branches, or null. */
    public AuthenticatorSpec selectiveOAuth() {
        for (Map.Entry<String, AuthenticatorSpec> e : getAuthenticators().entrySet()) {
            if (e.getValue() != null && e.getValue().isOAuth()) {
                return e.getValue();
            }
        }
        return null;
    }

    /** Selection value (e.g. "Client") that maps to the OAuth branch, or null. */
    public String selectiveOAuthKey() {
        for (Map.Entry<String, AuthenticatorSpec> e : getAuthenticators().entrySet()) {
            if (e.getValue() != null && e.getValue().isOAuth()) {
                return e.getKey();
            }
        }
        return null;
    }

    // ── inner classes ─────────────────────────────────────────────────────────

    /** Describes where to inject a value (header, query param, path, etc.). */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class InjectIntoSpec {

        private String type;

        @JsonProperty("field_name")
        private String fieldName;

        @JsonProperty("inject_into")
        private String injectInto;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getInjectInto() {
            return injectInto;
        }

        public void setInjectInto(String injectInto) {
            this.injectInto = injectInto;
        }

        public boolean isHeader() {
            return "header".equalsIgnoreCase(injectInto);
        }
    }

    /** Models the {@code login_requester} block inside a SessionTokenAuthenticator. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class LoginRequesterSpec {

        @JsonProperty("url_base")
        private String urlBase;

        /** Alternate full-URL field (some manifests use "url" instead of "url_base" + "path"). */
        @JsonProperty("url")
        private String url;

        @JsonProperty("path")
        private String path;

        @JsonProperty("http_method")
        private String httpMethod = "POST";

        @JsonProperty("authenticator")
        private AuthenticatorSpec authenticator;

        @JsonProperty("request_body_json")
        private Map<String, String> requestBodyJson;

        /** Form-encoded POST body (application/x-www-form-urlencoded). */
        @JsonProperty("request_body_data")
        private Map<String, String> requestBodyData;

        /** URL query parameters to append to the login URL. */
        @JsonProperty("request_parameters")
        private Map<String, String> requestParameters;

        /** Extra HTTP request headers for the login request. */
        @JsonProperty("request_headers")
        private Map<String, String> requestHeaders;

        public String getUrlBase() {
            if (urlBase != null) {
                return urlBase;
            }
            return url != null ? url : "";
        }

        public void setUrlBase(String urlBase) {
            this.urlBase = urlBase;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        /** True when a full URL was provided via "url" field (path should be treated as empty). */
        public boolean hasFullUrl() {
            return url != null && urlBase == null;
        }

        public String getPath() {
            return (path == null || hasFullUrl()) ? "" : path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public String getHttpMethod() {
            return httpMethod == null ? "POST" : httpMethod.toUpperCase(java.util.Locale.ROOT);
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

        public Map<String, String> getRequestBodyJson() {
            return requestBodyJson;
        }

        public void setRequestBodyJson(Map<String, String> requestBodyJson) {
            this.requestBodyJson = requestBodyJson;
        }

        public Map<String, String> getRequestBodyData() {
            return requestBodyData;
        }

        public void setRequestBodyData(Map<String, String> requestBodyData) {
            this.requestBodyData = requestBodyData;
        }

        public Map<String, String> getRequestParameters() {
            return requestParameters;
        }

        public void setRequestParameters(Map<String, String> requestParameters) {
            this.requestParameters = requestParameters;
        }

        public Map<String, String> getRequestHeaders() {
            return requestHeaders;
        }

        public void setRequestHeaders(Map<String, String> requestHeaders) {
            this.requestHeaders = requestHeaders;
        }
    }
}
