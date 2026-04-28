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

/**
 * Models the {@code authenticator} block inside a requester.
 * Supported types: NoAuth, ApiKeyAuthenticator, BearerAuthenticator,
 * BasicHttpAuthenticator, OAuthAuthenticator.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AuthenticatorSpec {

    private String type;

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

    // ── getters / setters ─────────────────────────────────────────────────────

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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

    // ── inner class ───────────────────────────────────────────────────────────

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
}
