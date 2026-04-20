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

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.io.InputStream;

/**
 * Serves the OpenAPI spec and a Swagger UI page for in-browser testing.
 *
 * <p>Both paths are allow-listed in {@link BasicAuthFilter} so the spec
 * and UI load without credentials. "Try it out" in the UI will prompt
 * for Basic Auth and send it on to the actual {@code /v1/topics/...}
 * request.
 */
@Path("/")
public class OpenApiResource {

    @GET
    @Path("openapi.yaml")
    @Produces("application/yaml")
    public Response spec() {
        return streamResource("openapi.yaml", "application/yaml");
    }

    @GET
    @Path("swagger")
    @Produces("text/html")
    public Response swaggerUi() {
        return streamResource("swagger-ui.html", "text/html");
    }

    private Response streamResource(String name, String contentType) {
        InputStream in = getClass().getResourceAsStream(name);
        if (in == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        try (InputStream stream = in) {
            return Response.ok(stream.readAllBytes())
                    .type(contentType)
                    .build();
        } catch (IOException e) {
            return Response.serverError().entity(e.getMessage()).build();
        }
    }
}
