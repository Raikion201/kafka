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

import com.fasterxml.jackson.jakarta.rs.json.JacksonJsonProvider

import org.apache.kafka.common.internals.Plugin
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.server.authorizer.Authorizer

import org.glassfish.jersey.server.ResourceConfig

import kafka.server.ReplicaManager

/**
 * The Jersey route registry for the embedded HTTP REST server. One place to look
 * at to see every path the server exposes.
 *
 * == Routes ==
 * {{{
 *   Method  Path                        Resource             Purpose
 *   ------  --------------------------  -------------------  -----------------------
 *   POST    /v1/topics/{name}           ProduceResource      Produce a single record
 *   GET     /openapi.yaml               OpenApiResource      OpenAPI 3 spec
 *   GET     /swagger                    OpenApiResource      Swagger UI (for testing)
 * }}}
 *
 * == Filters ==
 * {{{
 *   BasicAuthFilter (optional)    Rejects with 401 when http.rest.basic.credentials
 *                                 is set and the request lacks valid credentials.
 *                                 Skips /swagger and /openapi.yaml so the UI loads.
 * }}}
 *
 * == Providers ==
 * {{{
 *   JacksonJsonProvider           application/json (de)serialization.
 * }}}
 */
object HttpRouter {

  def build(
      basicCredentials: Map[String, String],
      replicaManager: ReplicaManager,
      authorizerPlugin: Option[Plugin[Authorizer]],
      metadataCache: MetadataCache,
      time: Time): ResourceConfig = {

    val config = new ResourceConfig

    // Providers
    config.register(classOf[JacksonJsonProvider])

    // Filters
    if (basicCredentials.nonEmpty) {
      config.register(new BasicAuthFilter(basicCredentials))
    }

    // Routes — each resource class declares its own JAX-RS @Path / @Method
    // annotations. The table in the class-level Scaladoc is the contract.
    config.register(new ProduceResource(replicaManager, authorizerPlugin, metadataCache, time))
    config.register(new OpenApiResource)

    config
  }
}
