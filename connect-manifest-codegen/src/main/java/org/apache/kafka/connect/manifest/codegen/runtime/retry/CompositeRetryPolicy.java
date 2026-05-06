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

import java.net.http.HttpResponse;
import java.util.List;
import java.util.Objects;

/**
 * Java port of Airbyte's {@code CompositeErrorHandler} (composite_error_handler.py:20).
 *
 * <p>Iterates child policies in declaration order. The first child whose resolution carries
 * one of {@code SUCCESS / RETRY / IGNORE / RESET_PAGINATION} wins (lines 69-75). If no child
 * matches an actionable status, the last child's resolution is returned; if every child
 * returned {@code null}, falls back to a transient retry resolution.</p>
 *
 * <p>{@link #maxRetries()} delegates to the first child (Python line 53); {@link #maxTimeMillis()}
 * is the {@code max} across all children (Python line 57).</p>
 */
public final class CompositeRetryPolicy implements RetryPolicy {

    private static final List<ResponseAction> TERMINAL_ACTIONS = List.of(
        ResponseAction.SUCCESS,
        ResponseAction.RETRY,
        ResponseAction.IGNORE,
        ResponseAction.RESET_PAGINATION
    );

    private final List<RetryPolicy> children;

    public CompositeRetryPolicy(List<RetryPolicy> children) {
        Objects.requireNonNull(children, "children");
        if (children.isEmpty()) {
            throw new IllegalArgumentException(
                "CompositeRetryPolicy expects at least 1 underlying error handler");
        }
        this.children = List.copyOf(children);
    }

    @Override
    public ErrorResolution interpretResponse(HttpResponse<String> response) {
        ErrorResolution last = null;
        for (RetryPolicy child : children) {
            ErrorResolution resolution = child.interpretResponse(response);
            if (resolution == null) {
                continue;
            }
            last = resolution;
            if (TERMINAL_ACTIONS.contains(resolution.action())) {
                return resolution;
            }
        }
        if (last != null) {
            return last;
        }
        return DefaultErrorMapping.fallback(response == null ? 0 : response.statusCode());
    }

    @Override
    public int maxRetries() {
        return children.get(0).maxRetries();
    }

    @Override
    public long maxTimeMillis() {
        long max = 0;
        for (RetryPolicy c : children) {
            max = Math.max(max, c.maxTimeMillis());
        }
        return max;
    }

    public List<RetryPolicy> children() {
        return children;
    }
}
