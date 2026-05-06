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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.Optional;

import javax.net.ssl.SSLSession;

/**
 * Builds a synthetic 200 OK / empty-JSON-object {@code HttpResponse<String>} used to short-circuit
 * an {@link ResponseAction#IGNORE} resolution.
 *
 * <p>Airbyte's IGNORE action means "this batch is not an error, but treat it as containing no
 * records and continue." The Java port preserves that semantic by replacing the actual response
 * with a 200 / {@code {}} stand-in so the generated record-extraction path yields zero records
 * without raising.</p>
 */
public final class IgnoredResponses {

    private IgnoredResponses() {
    }

    /** Returns a 200 / {@code "{}"} response inheriting the URI of {@code original}. */
    public static HttpResponse<String> empty(HttpResponse<String> original) {
        URI uri = original == null ? URI.create("about:blank") : original.uri();
        HttpRequest request = original == null ? null : original.request();
        HttpClient.Version version = original == null
            ? HttpClient.Version.HTTP_1_1
            : original.version();
        return new SyntheticResponse(uri, request, version);
    }

    private static final class SyntheticResponse implements HttpResponse<String> {
        private final URI uri;
        private final HttpRequest request;
        private final HttpClient.Version version;
        private final HttpHeaders headers =
            HttpHeaders.of(Collections.emptyMap(), (k, v) -> true);

        SyntheticResponse(URI uri, HttpRequest request, HttpClient.Version version) {
            this.uri = uri;
            this.request = request;
            this.version = version;
        }

        @Override
        public int statusCode() {
            return 200;
        }

        @Override
        public HttpRequest request() {
            return request;
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
            return "{}";
        }

        @Override
        public Optional<SSLSession> sslSession() {
            return Optional.empty();
        }

        @Override
        public URI uri() {
            return uri;
        }

        @Override
        public HttpClient.Version version() {
            return version;
        }
    }
}
