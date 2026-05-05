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
import java.util.Map;

/**
 * AST node for a Jinja expression. Sealed hierarchy of records, one per
 * distinct expression shape we need to represent for evaluation.
 */
public sealed interface Expr permits
        Expr.Literal,
        Expr.Ident,
        Expr.Attr,
        Expr.Index,
        Expr.Slice,
        Expr.Call,
        Expr.Filter,
        Expr.Test,
        Expr.Unary,
        Expr.Binary,
        Expr.Ternary,
        Expr.ListLit,
        Expr.DictLit,
        Expr.TupleLit {

    /** A literal value: integer, float, string, boolean, or null (for {@code None}). */
    record Literal(Object value) implements Expr { }

    /** A bare identifier, e.g. {@code config}, {@code stream_state}. */
    record Ident(String name) implements Expr { }

    /** Attribute access {@code target.attr}. */
    record Attr(Expr target, String name) implements Expr { }

    /** Subscript {@code target[key]}. */
    record Index(Expr target, Expr key) implements Expr { }

    /** Slice {@code target[start:stop:step]}; any of the three may be null for omitted. */
    record Slice(Expr target, Expr start, Expr stop, Expr step) implements Expr { }

    /**
     * Function or method call {@code callee(args, kw=val)}. Kwargs preserve insertion order
     * so for-loops can iterate them deterministically.
     */
    record Call(Expr callee, List<Expr> args, Map<String, Expr> kwargs) implements Expr { }

    /** Filter application {@code target | name(args, kw=val)}. */
    record Filter(Expr target, String name, List<Expr> args, Map<String, Expr> kwargs) implements Expr { }

    /** Test {@code target is name} or {@code target is not name}. */
    record Test(Expr target, String name, boolean negated) implements Expr { }

    /** Unary prefix operator: {@code -x}, {@code +x}, {@code not x}. */
    record Unary(String op, Expr target) implements Expr { }

    /**
     * Binary operator. {@code op} is one of:
     * {@code + - * / %} (arithmetic), {@code ~} (concat),
     * {@code == != &lt; &gt; &lt;= &gt;=} (comparison),
     * {@code and or} (logical), {@code in}, {@code not in} (membership).
     */
    record Binary(String op, Expr left, Expr right) implements Expr { }

    /** Jinja ternary: {@code ifTrue if cond else ifFalse}. */
    record Ternary(Expr cond, Expr ifTrue, Expr ifFalse) implements Expr { }

    /** List literal {@code [a, b, c]}. */
    record ListLit(List<Expr> items) implements Expr { }

    /** Dict literal {@code {a: b, c: d}}. */
    record DictLit(List<Map.Entry<Expr, Expr>> entries) implements Expr { }

    /** Tuple literal {@code (a, b)} — used in {@code for k, v in items}. */
    record TupleLit(List<Expr> items) implements Expr { }
}
