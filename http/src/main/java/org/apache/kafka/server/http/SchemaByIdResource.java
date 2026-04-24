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

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.Map;

/**
 * Schema lookup by global ID — matches the Confluent Schema Registry API.
 *
 * <pre>
 *   GET /schemas/ids/{id}   Return the schema string for a global ID
 * </pre>
 *
 * Used by {@code KafkaAvroDeserializer} to fetch the writer schema
 * from the 5-byte Confluent wire-format header prepended to every Avro message.
 */
@Path("/schemas")
@Produces(MediaType.APPLICATION_JSON)
public class SchemaByIdResource {

    private final SchemaStore store;

    public SchemaByIdResource(SchemaStore store) {
        this.store = store;
    }

    /**
     * Fetch the schema content for a global schema ID.
     *
     * @param id the schema ID returned by a prior register call
     * @return 200 {@code {"schema":"..."}}; 404 if ID unknown
     */
    @GET
    @Path("/ids/{id}")
    public Response getSchemaById(@PathParam("id") int id) {
        String schema = store.getById(id);
        if (schema == null) {
            return SubjectResource.error(Response.Status.NOT_FOUND, "SCHEMA_NOT_FOUND");
        }
        return Response.ok(Map.of("schema", schema)).build();
    }
}
