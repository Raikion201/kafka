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
package kafka.server.http;

import org.apache.kafka.common.security.auth.KafkaPrincipal;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

/**
 * HTTP Basic Auth gate. Rejects requests without valid credentials with 401
 * and, on success, attaches a {@link KafkaPrincipal} to the request context
 * so downstream resources can pass it to {@code AuthHelper.authorize}.
 *
 * <p>Registered only when {@code http.rest.basic.credentials} is non-empty.
 * When no credentials are configured the filter is not installed and requests
 * flow through without authentication.
 */
@PreMatching
public class BasicAuthFilter implements ContainerRequestFilter {

    public static final String PRINCIPAL_PROPERTY = "kafka.rest.principal";

    private static final String BASIC_PREFIX = "Basic ";

    private final Map<String, String> credentials;

    public BasicAuthFilter(Map<String, String> credentials) {
        this.credentials = credentials;
    }

    @Override
    public void filter(ContainerRequestContext ctx) {
        String path = ctx.getUriInfo().getPath();
        if (path.equals("swagger") || path.equals("openapi.yaml")) {
            return;
        }

        String header = ctx.getHeaderString(HttpHeaders.AUTHORIZATION);
        if (header == null || !header.startsWith(BASIC_PREFIX)) {
            abort(ctx);
            return;
        }

        String decoded;
        try {
            decoded = new String(Base64.getDecoder().decode(header.substring(BASIC_PREFIX.length())),
                    StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            abort(ctx);
            return;
        }

        int colon = decoded.indexOf(':');
        if (colon < 0) {
            abort(ctx);
            return;
        }

        String user = decoded.substring(0, colon);
        String pass = decoded.substring(colon + 1);
        String expected = credentials.get(user);
        if (expected == null || !expected.equals(pass)) {
            abort(ctx);
            return;
        }

        ctx.setProperty(PRINCIPAL_PROPERTY, new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user));
    }

    private static void abort(ContainerRequestContext ctx) {
        ctx.abortWith(Response.status(Response.Status.UNAUTHORIZED)
                .header(HttpHeaders.WWW_AUTHENTICATE, "Basic realm=\"kafka-rest\"")
                .build());
    }
}
