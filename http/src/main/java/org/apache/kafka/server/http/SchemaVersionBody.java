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

/**
 * JSON response for schema lookup endpoints:
 * <ul>
 *   <li>{@code GET /v1/schemas/{id}}</li>
 *   <li>{@code GET /v1/schemas/subjects/{subject}/versions/{version}}</li>
 * </ul>
 *
 * <p>{@code version} is 1-based — the first schema registered under a subject
 * is version 1, the second is version 2, and so on.</p>
 */
public record SchemaVersionBody(int id, String subject, int version, String schema) { }
