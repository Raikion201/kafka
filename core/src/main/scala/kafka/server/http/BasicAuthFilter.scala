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
import java.security.MessageDigest
import java.util.Base64

import jakarta.ws.rs.container.{ContainerRequestContext, ContainerRequestFilter, PreMatching}
import jakarta.ws.rs.core.{HttpHeaders, Response}

import org.apache.kafka.common.security.auth.KafkaPrincipal

object BasicAuthFilter {
  /** Property name under which authenticated principals are attached to the request context. */
  val PrincipalProperty = "kafka.rest.principal"

  private val BasicPrefix = "Basic "

  /** Paths that bypass the filter so Swagger UI can load without credentials. */
  private val AllowListedPaths = Set("swagger", "openapi.yaml")
}

/**
 * HTTP Basic Auth gate. Rejects requests without valid credentials with 401 and,
 * on success, attaches a [[KafkaPrincipal]] to the request context so downstream
 * resources can pass it to `AuthHelper.authorize`.
 *
 * Registered by [[HttpRouter]] only when `http.rest.basic.credentials` is non-empty.
 * When no credentials are configured the filter is not installed and requests flow
 * through without authentication.
 */
@PreMatching
class BasicAuthFilter(credentials: Map[String, String]) extends ContainerRequestFilter {
  import BasicAuthFilter._

  override def filter(ctx: ContainerRequestContext): Unit = {
    if (AllowListedPaths.contains(ctx.getUriInfo.getPath)) return

    val header = ctx.getHeaderString(HttpHeaders.AUTHORIZATION)
    if (header == null || !header.startsWith(BasicPrefix)) {
      abort(ctx)
      return
    }

    decodeCredentials(header) match {
      case Some((user, pass)) =>
        credentials.get(user) match {
          case Some(expected) if constantTimeEquals(expected, pass) =>
            ctx.setProperty(PrincipalProperty, new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user))
          case _ => abort(ctx)
        }
      case None => abort(ctx)
    }
  }

  private def decodeCredentials(header: String): Option[(String, String)] = {
    try {
      val decoded = new String(
        Base64.getDecoder.decode(header.substring(BasicPrefix.length)),
        StandardCharsets.UTF_8)
      val colon = decoded.indexOf(':')
      if (colon < 0) None
      else Some((decoded.substring(0, colon), decoded.substring(colon + 1)))
    } catch {
      case _: IllegalArgumentException => None
    }
  }

  private def abort(ctx: ContainerRequestContext): Unit = {
    ctx.abortWith(Response.status(Response.Status.UNAUTHORIZED)
      .header(HttpHeaders.WWW_AUTHENTICATE, "Basic realm=\"kafka-rest\"")
      .build())
  }

  /**
   * String.equals short-circuits on length mismatch and first difference, leaking
   * password length and common prefix through timing. Compare as UTF-8 bytes via
   * MessageDigest.isEqual, which runs in constant time relative to the shorter input.
   */
  private def constantTimeEquals(a: String, b: String): Boolean =
    MessageDigest.isEqual(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8))
}
