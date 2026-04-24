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
package org.apache.kafka.server.http;

import org.apache.kafka.common.security.auth.KafkaPrincipal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Map;
import java.util.Set;

/**
 * HTTP Basic Auth gate. Rejects requests without valid credentials with 401
 * and, on success, attaches a {@link KafkaPrincipal} to the request context so
 * downstream resources can pass it to the authorizer.
 *
 * <p>Registered by {@link HttpRouter} only when {@code http.rest.basic.credentials}
 * is non-empty. When no credentials are configured the filter is not installed
 * and requests flow through without authentication.</p>
 */
@PreMatching
public class BasicAuthFilter implements ContainerRequestFilter {

    private static final Logger LOG = LoggerFactory.getLogger(BasicAuthFilter.class);

    /** Property name under which authenticated principals are attached to the request context. */
    public static final String PRINCIPAL_PROPERTY = "kafka.rest.principal";

    private static final String BASIC_PREFIX = "Basic ";

    /** Paths that bypass the filter so Swagger UI can load without credentials. */
    private static final Set<String> ALLOW_LISTED_PATHS = Set.of("swagger", "openapi.yaml");

    private final Map<String, String> credentials;

    public BasicAuthFilter(Map<String, String> credentials) {
        this.credentials = credentials;
    }

    @Override
    public void filter(ContainerRequestContext ctx) {
        if (ALLOW_LISTED_PATHS.contains(ctx.getUriInfo().getPath())) {
            return;
        }

        String header = ctx.getHeaderString(HttpHeaders.AUTHORIZATION);
        if (header == null || !header.startsWith(BASIC_PREFIX)) {
            abort(ctx);
            return;
        }

        String[] userPass = decodeCredentials(header);
        if (userPass == null) {
            abort(ctx);
            return;
        }

        String expected = credentials.get(userPass[0]);
        if (expected == null || !constantTimeEquals(expected, userPass[1])) {
            abort(ctx);
            return;
        }

        ctx.setProperty(PRINCIPAL_PROPERTY, new KafkaPrincipal(KafkaPrincipal.USER_TYPE, userPass[0]));
    }

    private String[] decodeCredentials(String header) {
        try {
            String decoded = new String(
                    Base64.getDecoder().decode(header.substring(BASIC_PREFIX.length())),
                    StandardCharsets.UTF_8);
            int colon = decoded.indexOf(':');
            if (colon < 0) {
                return null;
            }
            return new String[]{decoded.substring(0, colon), decoded.substring(colon + 1)};
        } catch (IllegalArgumentException e) {
            LOG.debug("Malformed Base64 in Authorization header: {}", e.getMessage());
            return null;
        }
    }

    private void abort(ContainerRequestContext ctx) {
        ctx.abortWith(Response.status(Response.Status.UNAUTHORIZED)
                .header(HttpHeaders.WWW_AUTHENTICATE, "Basic realm=\"kafka-rest\"")
                .build());
    }

    /**
     * String.equals short-circuits on length mismatch and first difference,
     * leaking password length and common prefix through timing. Compare as
     * UTF-8 bytes via {@link MessageDigest#isEqual}, which runs in constant
     * time relative to the shorter input.
     */
    private static boolean constantTimeEquals(String a, String b) {
        return MessageDigest.isEqual(
                a.getBytes(StandardCharsets.UTF_8),
                b.getBytes(StandardCharsets.UTF_8));
    }
}
