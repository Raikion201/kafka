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

/**
 * Models the {@code authenticator} block inside a requester.
 * Supported types: NoAuth, ApiKeyAuthenticator, BearerAuthenticator.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AuthenticatorSpec {

    private String type;

    /** Header name for ApiKeyAuthenticator. */
    @JsonProperty("header")
    private String header;

    /** Config key holding the API token for ApiKeyAuthenticator. */
    @JsonProperty("api_token")
    private String apiToken;

    /** Config key holding the bearer token for BearerAuthenticator. */
    @JsonProperty("api_key")
    private String apiKey;

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

    public String getApiKey() {
        return apiKey;
    }

    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

    public boolean isNoAuth() {
        return type == null || type.equalsIgnoreCase("NoAuth");
    }

    public boolean isApiKey() {
        return "ApiKeyAuthenticator".equalsIgnoreCase(type);
    }

    public boolean isBearer() {
        return "BearerAuthenticator".equalsIgnoreCase(type);
    }
}
