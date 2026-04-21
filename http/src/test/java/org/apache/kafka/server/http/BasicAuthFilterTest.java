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

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BasicAuthFilterTest {

    private final BasicAuthFilter filter = new BasicAuthFilter(Map.of("alice", "s3cret"));

    private static String basic(String userPass) {
        return "Basic " + Base64.getEncoder().encodeToString(userPass.getBytes(StandardCharsets.UTF_8));
    }

    private static ContainerRequestContext ctxFor(String path, String authHeader) {
        ContainerRequestContext ctx = mock(ContainerRequestContext.class);
        UriInfo uriInfo = mock(UriInfo.class);
        when(uriInfo.getPath()).thenReturn(path);
        when(ctx.getUriInfo()).thenReturn(uriInfo);
        when(ctx.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn(authHeader);
        return ctx;
    }

    @Test
    public void missingHeaderReturns401() {
        ContainerRequestContext ctx = ctxFor("v1/topics/foo", null);
        filter.filter(ctx);
        ArgumentCaptor<Response> captor = ArgumentCaptor.forClass(Response.class);
        verify(ctx).abortWith(captor.capture());
        assertEquals(401, captor.getValue().getStatus());
        assertTrue(captor.getValue().getHeaderString(HttpHeaders.WWW_AUTHENTICATE).startsWith("Basic"));
    }

    @Test
    public void nonBasicSchemeReturns401() {
        ContainerRequestContext ctx = ctxFor("v1/topics/foo", "Bearer some-token");
        filter.filter(ctx);
        verify(ctx).abortWith(any(Response.class));
    }

    @Test
    public void malformedBase64Returns401() {
        ContainerRequestContext ctx = ctxFor("v1/topics/foo", "Basic not==valid==base64===");
        filter.filter(ctx);
        verify(ctx).abortWith(any(Response.class));
    }

    @Test
    public void missingColonReturns401() {
        String encoded = Base64.getEncoder().encodeToString("alicesecret".getBytes(StandardCharsets.UTF_8));
        ContainerRequestContext ctx = ctxFor("v1/topics/foo", "Basic " + encoded);
        filter.filter(ctx);
        verify(ctx).abortWith(any(Response.class));
    }

    @Test
    public void unknownUserReturns401() {
        ContainerRequestContext ctx = ctxFor("v1/topics/foo", basic("bob:s3cret"));
        filter.filter(ctx);
        verify(ctx).abortWith(any(Response.class));
    }

    @Test
    public void wrongPasswordReturns401() {
        ContainerRequestContext ctx = ctxFor("v1/topics/foo", basic("alice:wrong"));
        filter.filter(ctx);
        verify(ctx).abortWith(any(Response.class));
    }

    @Test
    public void correctCredentialsSetPrincipalAndDoNotAbort() {
        ContainerRequestContext ctx = ctxFor("v1/topics/foo", basic("alice:s3cret"));
        filter.filter(ctx);
        verify(ctx, never()).abortWith(any(Response.class));
        ArgumentCaptor<KafkaPrincipal> captor = ArgumentCaptor.forClass(KafkaPrincipal.class);
        verify(ctx).setProperty(eq(BasicAuthFilter.PRINCIPAL_PROPERTY), captor.capture());
        assertEquals("User", captor.getValue().getPrincipalType());
        assertEquals("alice", captor.getValue().getName());
    }

    @Test
    public void passwordWithColonIsAccepted() {
        BasicAuthFilter f = new BasicAuthFilter(Map.of("alice", "pa:ss"));
        ContainerRequestContext ctx = ctxFor("v1/topics/foo", basic("alice:pa:ss"));
        f.filter(ctx);
        verify(ctx, never()).abortWith(any(Response.class));
    }

    @Test
    public void swaggerPathSkipsAuthCheck() {
        ContainerRequestContext ctx = ctxFor("swagger", null);
        filter.filter(ctx);
        verify(ctx, never()).abortWith(any(Response.class));
    }

    @Test
    public void openapiPathSkipsAuthCheck() {
        ContainerRequestContext ctx = ctxFor("openapi.yaml", null);
        filter.filter(ctx);
        verify(ctx, never()).abortWith(any(Response.class));
    }

    /**
     * Both entries in a multi-user credentials map must authenticate
     * independently. Guards against a refactor that collapses the map into a
     * single cached "current user" lookup — a regression that the one-user
     * happy-path test would miss.
     */
    @Test
    public void bothUsersInAMultiUserMapCanAuthenticate() {
        BasicAuthFilter f = new BasicAuthFilter(Map.of("alice", "s3cret", "bob", "hunter2"));

        ContainerRequestContext aliceCtx = ctxFor("v1/topics/foo", basic("alice:s3cret"));
        f.filter(aliceCtx);
        verify(aliceCtx, never()).abortWith(any(Response.class));
        verify(aliceCtx).setProperty(eq(BasicAuthFilter.PRINCIPAL_PROPERTY), any());

        ContainerRequestContext bobCtx = ctxFor("v1/topics/foo", basic("bob:hunter2"));
        f.filter(bobCtx);
        verify(bobCtx, never()).abortWith(any(Response.class));
        verify(bobCtx).setProperty(eq(BasicAuthFilter.PRINCIPAL_PROPERTY), any());

        // Cross-check: alice's password must not authenticate bob.
        ContainerRequestContext swapped = ctxFor("v1/topics/foo", basic("bob:s3cret"));
        f.filter(swapped);
        verify(swapped).abortWith(any(Response.class));
    }

    /**
     * Non-ASCII passwords survive Base64 round-trip and UTF-8 byte compare.
     * A regression to {@code String.equals} on an {@code ISO-8859-1}-decoded
     * header would pass ASCII-only tests; this one catches it.
     */
    @Test
    public void nonAsciiPasswordAuthenticates() {
        String password = "пароль-💥-ñ";
        BasicAuthFilter f = new BasicAuthFilter(Map.of("alice", password));

        ContainerRequestContext ok = ctxFor("v1/topics/foo", basic("alice:" + password));
        f.filter(ok);
        verify(ok, never()).abortWith(any(Response.class));
        verify(ok).setProperty(eq(BasicAuthFilter.PRINCIPAL_PROPERTY), any());

        // Same prefix, different suffix — constant-time compare must reject.
        ContainerRequestContext bad = ctxFor("v1/topics/foo", basic("alice:пароль-💥-X"));
        f.filter(bad);
        verify(bad).abortWith(any(Response.class));
    }
}
