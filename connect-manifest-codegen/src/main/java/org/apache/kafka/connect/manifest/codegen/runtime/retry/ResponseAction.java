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
package org.apache.kafka.connect.manifest.codegen.runtime.retry;

/**
 * Mirrors Airbyte CDK {@code ResponseAction} (response_models.py:14).
 *
 * <p>{@code RATE_LIMITED} is treated as RETRY by the retry loop but distinguished here so
 * generated tasks can apply a separate backoff strategy when wired. {@code REFRESH_TOKEN_THEN_RETRY}
 * is reserved for OAuth flows and is not yet emitted by any policy.</p>
 */
public enum ResponseAction {
    SUCCESS,
    RETRY,
    FAIL,
    IGNORE,
    RESET_PAGINATION,
    RATE_LIMITED,
    REFRESH_TOKEN_THEN_RETRY
}
