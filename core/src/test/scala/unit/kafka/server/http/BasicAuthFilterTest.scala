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
package kafka.server.http

import java.nio.charset.StandardCharsets
import java.util.Base64

import jakarta.ws.rs.container.ContainerRequestContext
import jakarta.ws.rs.core.{HttpHeaders, Response, UriInfo}

import org.apache.kafka.common.security.auth.KafkaPrincipal

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, eq => eqTo}
import org.mockito.Mockito.{mock, never, verify, when}

class BasicAuthFilterTest {

  private val filter = new BasicAuthFilter(Map("alice" -> "s3cret"))

  private def basic(userPass: String): String =
    "Basic " + Base64.getEncoder.encodeToString(userPass.getBytes(StandardCharsets.UTF_8))

  private def ctxFor(path: String, authHeader: String): ContainerRequestContext = {
    val ctx = mock(classOf[ContainerRequestContext])
    val uriInfo = mock(classOf[UriInfo])
    when(uriInfo.getPath).thenReturn(path)
    when(ctx.getUriInfo).thenReturn(uriInfo)
    when(ctx.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn(authHeader)
    ctx
  }

  @Test
  def missingHeaderReturns401(): Unit = {
    val ctx = ctxFor("v1/topics/foo", null)
    filter.filter(ctx)
    val captor = ArgumentCaptor.forClass(classOf[Response])
    verify(ctx).abortWith(captor.capture())
    assertEquals(401, captor.getValue.getStatus)
    assertTrue(captor.getValue.getHeaderString(HttpHeaders.WWW_AUTHENTICATE).startsWith("Basic"))
  }

  @Test
  def nonBasicSchemeReturns401(): Unit = {
    val ctx = ctxFor("v1/topics/foo", "Bearer some-token")
    filter.filter(ctx)
    verify(ctx).abortWith(any(classOf[Response]))
  }

  @Test
  def malformedBase64Returns401(): Unit = {
    val ctx = ctxFor("v1/topics/foo", "Basic not==valid==base64===")
    filter.filter(ctx)
    verify(ctx).abortWith(any(classOf[Response]))
  }

  @Test
  def missingColonReturns401(): Unit = {
    val encoded = Base64.getEncoder.encodeToString("alicesecret".getBytes(StandardCharsets.UTF_8))
    val ctx = ctxFor("v1/topics/foo", "Basic " + encoded)
    filter.filter(ctx)
    verify(ctx).abortWith(any(classOf[Response]))
  }

  @Test
  def unknownUserReturns401(): Unit = {
    val ctx = ctxFor("v1/topics/foo", basic("bob:s3cret"))
    filter.filter(ctx)
    verify(ctx).abortWith(any(classOf[Response]))
  }

  @Test
  def wrongPasswordReturns401(): Unit = {
    val ctx = ctxFor("v1/topics/foo", basic("alice:wrong"))
    filter.filter(ctx)
    verify(ctx).abortWith(any(classOf[Response]))
  }

  @Test
  def correctCredentialsSetPrincipalAndDoNotAbort(): Unit = {
    val ctx = ctxFor("v1/topics/foo", basic("alice:s3cret"))
    filter.filter(ctx)
    verify(ctx, never()).abortWith(any(classOf[Response]))
    val captor = ArgumentCaptor.forClass(classOf[KafkaPrincipal])
    verify(ctx).setProperty(eqTo(BasicAuthFilter.PrincipalProperty), captor.capture())
    assertEquals("User", captor.getValue.getPrincipalType)
    assertEquals("alice", captor.getValue.getName)
  }

  @Test
  def passwordWithColonIsAccepted(): Unit = {
    val f = new BasicAuthFilter(Map("alice" -> "pa:ss"))
    val ctx = ctxFor("v1/topics/foo", basic("alice:pa:ss"))
    f.filter(ctx)
    verify(ctx, never()).abortWith(any(classOf[Response]))
  }

  @Test
  def swaggerPathSkipsAuthCheck(): Unit = {
    val ctx = ctxFor("swagger", null)
    filter.filter(ctx)
    verify(ctx, never()).abortWith(any(classOf[Response]))
  }

  @Test
  def openapiPathSkipsAuthCheck(): Unit = {
    val ctx = ctxFor("openapi.yaml", null)
    filter.filter(ctx)
    verify(ctx, never()).abortWith(any(classOf[Response]))
  }
}
