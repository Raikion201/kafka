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

import java.util.Collections;
import java.util.Map;

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
    private Map<String, String> requestParameters = Collections.emptyMap();

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

    /** Returns the effective base URL, preferring {@code url} over {@code url_base}. */
    public String effectiveBaseUrl() {
        if (url != null && !url.isBlank()) {
            return url;
        }
        return urlBase == null ? "" : urlBase;
    }
}
