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
 * Outcome of evaluating an HTTP response against a {@link RetryPolicy}.
 *
 * <p>Mirrors Airbyte's {@code ErrorResolution} (response_models.py:25). The {@code failureType}
 * string follows Airbyte's {@code FailureType} enum values ({@code config_error},
 * {@code system_error}, {@code transient_error}); kept as a string here to avoid creating a
 * Java enum the generated code does not need.</p>
 */
public final class ErrorResolution {

    public static final ErrorResolution SUCCESS =
        new ErrorResolution(ResponseAction.SUCCESS, null, null);

    private final ResponseAction action;
    private final String failureType;
    private final String errorMessage;

    public ErrorResolution(ResponseAction action, String failureType, String errorMessage) {
        this.action = action;
        this.failureType = failureType;
        this.errorMessage = errorMessage;
    }

    public ResponseAction action() {
        return action;
    }

    public String failureType() {
        return failureType;
    }

    public String errorMessage() {
        return errorMessage;
    }

    @Override
    public String toString() {
        return "ErrorResolution{action=" + action
            + ", failureType=" + failureType
            + ", message=" + errorMessage + '}';
    }
}
