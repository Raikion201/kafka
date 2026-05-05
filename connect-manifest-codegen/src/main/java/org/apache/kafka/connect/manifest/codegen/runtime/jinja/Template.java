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
package org.apache.kafka.connect.manifest.codegen.runtime.jinja;

import java.util.List;

/**
 * AST nodes for a Jinja template body. A template is a sequence of {@link Node}s,
 * where each node is either raw text, an output expression, a conditional, a loop,
 * or an assignment.
 */
public final class Template {

    private final List<Node> body;

    public Template(List<Node> body) {
        this.body = List.copyOf(body);
    }

    public List<Node> body() {
        return body;
    }

    /** A single template node. */
    public sealed interface Node permits
            Text, Output, IfStmt, ForStmt, SetStmt { }

    /** Literal text segment. */
    public record Text(String value) implements Node { }

    /** {@code {{ expr }}} interpolation. */
    public record Output(Expr expr) implements Node { }

    /**
     * {@code {% if c %}...{% elif c2 %}...{% else %}...{% endif %}}.
     * {@code branches} holds the if + each elif as (cond, body). {@code elseBody}
     * is the else body or empty list.
     */
    public record IfStmt(List<Branch> branches, List<Node> elseBody) implements Node { }

    /** A condition + body pair inside an if/elif chain. */
    public record Branch(Expr cond, List<Node> body) { }

    /**
     * {@code {% for target in iterable [if filter] %}body{% else %}elseBody{% endfor %}}.
     * {@code targets} is a single name for {@code for x in xs}, or multiple names for
     * {@code for k, v in dict.items()}.
     */
    public record ForStmt(
            List<String> targets,
            Expr iterable,
            Expr filter,
            List<Node> body,
            List<Node> elseBody) implements Node { }

    /** {@code {% set name = expr %}}. */
    public record SetStmt(String target, Expr value) implements Node { }
}
