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
package org.apache.kafka.server.http.api;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.resource.ResourceType;

/**
 * Narrow view of {@code kafka.server.AuthHelper.authorize} exposed to the
 * {@code :http} module. The broker supplies an adapter that delegates to the
 * same {@code Authorizer} plugin that serves binary Kafka requests, so ACL
 * checks are consistent between the REST and binary paths.
 */
@FunctionalInterface
public interface AuthorizationHelper {

    /** @return {@code true} if the principal in {@code ctx} may perform {@code op} on the named resource. */
    boolean authorize(RequestContext ctx, AclOperation op, ResourceType type, String name);
}
