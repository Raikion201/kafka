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

import jakarta.ws.rs.{GET, Path, Produces}
import jakarta.ws.rs.core.Response

/**
 * Serves the OpenAPI spec and a Swagger UI page for in-browser testing.
 *
 * Both paths are allow-listed in [[BasicAuthFilter]] so the spec and UI load
 * without credentials. "Try it out" in the UI will prompt for Basic Auth
 * and send it on to the actual `/v1/topics/...` request.
 */
@Path("/")
class OpenApiResource {

  @GET
  @Path("openapi.yaml")
  @Produces(Array("application/yaml"))
  def spec: Response = streamResource("openapi.yaml", "application/yaml")

  @GET
  @Path("swagger")
  @Produces(Array("text/html"))
  def swaggerUi: Response = streamResource("swagger-ui.html", "text/html")

  private def streamResource(name: String, contentType: String): Response = {
    val stream = getClass.getResourceAsStream(name)
    if (stream == null) {
      Response.status(Response.Status.NOT_FOUND).build()
    } else {
      try {
        Response.ok(stream.readAllBytes()).`type`(contentType).build()
      } catch {
        case e: Exception =>
          Response.serverError().entity(e.getMessage).build()
      } finally {
        stream.close()
      }
    }
  }
}
