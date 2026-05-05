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

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Evaluates a {@link Template} against a context map and returns the rendered string.
 *
 * <p>This class implements the runtime semantics: Python truthiness for conditionals,
 * numeric tower with promotion to {@code BigInteger}/{@code Double} as needed, string
 * concatenation via {@code ~}, dict and attribute access, list/dict/tuple literal
 * construction, and dispatch into pluggable {@link Functions} / {@link Filters} /
 * {@link Tests} / {@link Methods} registries.
 *
 * <p>Datetime helpers, the full filter/function library, and method bindings are
 * registered by separate classes (see Phase 0.5 / 0.6).
 */
public final class Evaluator {

    private final Functions functions;
    private final Filters filters;
    private final Tests tests;
    private final Methods methods;

    public Evaluator() {
        this(new Functions(), new Filters(), new Tests(), new Methods());
        Builtins.install(this.functions, this.filters, this.tests, this.methods);
    }

    public Evaluator(Functions functions, Filters filters, Tests tests, Methods methods) {
        this.functions = Objects.requireNonNull(functions);
        this.filters = Objects.requireNonNull(filters);
        this.tests = Objects.requireNonNull(tests);
        this.methods = Objects.requireNonNull(methods);
    }

    public Functions functions() {
        return functions;
    }

    public Filters filters() {
        return filters;
    }

    public Tests tests() {
        return tests;
    }

    public Methods methods() {
        return methods;
    }

    /** Render a template against a context, returning the produced string. */
    public String render(Template template, Map<String, Object> context) {
        StringBuilder sb = new StringBuilder();
        Map<String, Object> scope = new HashMap<>(context == null ? Map.of() : context);
        renderNodes(template.body(), scope, sb);
        return sb.toString();
    }

    /** Render a raw template source string against a context. */
    public String render(String src, Map<String, Object> context) {
        return render(TemplateParser.parse(src), context);
    }

    /** Evaluate a single Jinja expression source string, returning the raw Java value. */
    public Object evaluateExpression(String src, Map<String, Object> context) {
        Expr e = Parser.parseExpression(src);
        Map<String, Object> scope = new HashMap<>(context == null ? Map.of() : context);
        return evalExpr(e, scope);
    }

    private void renderNodes(List<Template.Node> nodes, Map<String, Object> scope, StringBuilder sb) {
        for (Template.Node n : nodes) {
            renderNode(n, scope, sb);
        }
    }

    private void renderNode(Template.Node n, Map<String, Object> scope, StringBuilder sb) {
        if (n instanceof Template.Text t) {
            sb.append(t.value());
        } else if (n instanceof Template.Output o) {
            Object v = evalExpr(o.expr(), scope);
            sb.append(stringify(v));
        } else if (n instanceof Template.IfStmt s) {
            renderIf(s, scope, sb);
        } else if (n instanceof Template.ForStmt f) {
            renderFor(f, scope, sb);
        } else if (n instanceof Template.SetStmt set) {
            scope.put(set.target(), evalExpr(set.value(), scope));
        }
    }

    private void renderIf(Template.IfStmt s, Map<String, Object> scope, StringBuilder sb) {
        for (Template.Branch br : s.branches()) {
            if (truthy(evalExpr(br.cond(), scope))) {
                renderNodes(br.body(), scope, sb);
                return;
            }
        }
        renderNodes(s.elseBody(), scope, sb);
    }

    private void renderFor(Template.ForStmt f, Map<String, Object> scope, StringBuilder sb) {
        Object iter = evalExpr(f.iterable(), scope);
        List<Object> items = toIterable(iter);
        if (f.filter() != null) {
            List<Object> filtered = new ArrayList<>();
            Map<String, Object> probe = new HashMap<>(scope);
            for (Object item : items) {
                bindLoopTargets(f.targets(), item, probe);
                if (truthy(evalExpr(f.filter(), probe))) {
                    filtered.add(item);
                }
            }
            items = filtered;
        }
        if (items.isEmpty()) {
            renderNodes(f.elseBody(), scope, sb);
            return;
        }
        Map<String, Object> loopScope = new HashMap<>(scope);
        for (int idx = 0; idx < items.size(); idx++) {
            Object item = items.get(idx);
            bindLoopTargets(f.targets(), item, loopScope);
            loopScope.put("loop", buildLoopVar(idx, items.size()));
            renderNodes(f.body(), loopScope, sb);
        }
    }

    private static void bindLoopTargets(List<String> targets, Object item, Map<String, Object> scope) {
        if (targets.size() == 1) {
            scope.put(targets.get(0), item);
            return;
        }
        List<Object> parts = toIterable(item);
        if (parts.size() != targets.size()) {
            throw new JinjaException(
                "for-loop target arity " + targets.size()
                    + " does not match item arity " + parts.size());
        }
        for (int i = 0; i < targets.size(); i++) {
            scope.put(targets.get(i), parts.get(i));
        }
    }

    private static Map<String, Object> buildLoopVar(int index0, int length) {
        Map<String, Object> loop = new LinkedHashMap<>();
        loop.put("index0", (long) index0);
        loop.put("index", (long) (index0 + 1));
        loop.put("revindex0", (long) (length - index0 - 1));
        loop.put("revindex", (long) (length - index0));
        loop.put("first", index0 == 0);
        loop.put("last", index0 == length - 1);
        loop.put("length", (long) length);
        return loop;
    }

    // ─── expression evaluation ──────────────────────────────────────────────

    private Object evalExpr(Expr e, Map<String, Object> scope) {
        Object simple = evalLeafExpr(e, scope);
        if (simple != UNHANDLED) {
            return simple;
        }
        return evalCompoundExpr(e, scope);
    }

    private static final Object UNHANDLED = new Object();

    private Object evalLeafExpr(Expr e, Map<String, Object> scope) {
        if (e instanceof Expr.Literal lit) {
            return lit.value();
        }
        if (e instanceof Expr.Ident id) {
            return evalIdent(id, scope);
        }
        if (e instanceof Expr.Attr a) {
            return evalAttr(a, scope);
        }
        if (e instanceof Expr.Index idx) {
            return indexInto(evalExpr(idx.target(), scope), evalExpr(idx.key(), scope));
        }
        if (e instanceof Expr.ListLit ll) {
            return evalList(ll.items(), scope);
        }
        if (e instanceof Expr.DictLit dl) {
            return evalDict(dl.entries(), scope);
        }
        if (e instanceof Expr.TupleLit tl) {
            return evalList(tl.items(), scope);
        }
        return UNHANDLED;
    }

    private Object evalCompoundExpr(Expr e, Map<String, Object> scope) {
        if (e instanceof Expr.Call c) {
            return evalCall(c, scope);
        }
        if (e instanceof Expr.Filter f) {
            return evalFilter(f, scope);
        }
        if (e instanceof Expr.Test t) {
            return evalTest(t, scope);
        }
        if (e instanceof Expr.Unary u) {
            return evalUnary(u, scope);
        }
        if (e instanceof Expr.Binary b) {
            return evalBinary(b, scope);
        }
        if (e instanceof Expr.Ternary tn) {
            return truthy(evalExpr(tn.cond(), scope))
                ? evalExpr(tn.ifTrue(), scope) : evalExpr(tn.ifFalse(), scope);
        }
        throw new JinjaException("unhandled expression: " + e.getClass().getName());
    }

    private Object evalIdent(Expr.Ident id, Map<String, Object> scope) {
        if (!scope.containsKey(id.name()) && functions.has(id.name())) {
            return new FunctionRef(id.name());
        }
        return scope.get(id.name());
    }

    private List<Object> evalList(List<Expr> items, Map<String, Object> scope) {
        List<Object> out = new ArrayList<>(items.size());
        for (Expr it : items) {
            out.add(evalExpr(it, scope));
        }
        return out;
    }

    private Map<Object, Object> evalDict(List<Map.Entry<Expr, Expr>> entries, Map<String, Object> scope) {
        Map<Object, Object> out = new LinkedHashMap<>();
        for (Map.Entry<Expr, Expr> entry : entries) {
            out.put(evalExpr(entry.getKey(), scope), evalExpr(entry.getValue(), scope));
        }
        return out;
    }

    private Object evalAttr(Expr.Attr a, Map<String, Object> scope) {
        Object target = evalExpr(a.target(), scope);
        return getAttribute(target, a.name());
    }

    private Object evalCall(Expr.Call c, Map<String, Object> scope) {
        // Method call: target.method(args)
        if (c.callee() instanceof Expr.Attr attr) {
            Object self = evalExpr(attr.target(), scope);
            List<Object> args = evalArgs(c.args(), scope);
            Map<String, Object> kw = evalKwargs(c.kwargs(), scope);
            return methods.invoke(self, attr.name(), args, kw);
        }
        // Bare function call
        if (c.callee() instanceof Expr.Ident id) {
            if (functions.has(id.name())) {
                List<Object> args = evalArgs(c.args(), scope);
                Map<String, Object> kw = evalKwargs(c.kwargs(), scope);
                return functions.invoke(id.name(), args, kw);
            }
            // User-bound callable (rare, but legal if scope holds something callable)
            Object f = scope.get(id.name());
            return invokeCallable(f, c, scope);
        }
        Object f = evalExpr(c.callee(), scope);
        return invokeCallable(f, c, scope);
    }

    private Object invokeCallable(Object callee, Expr.Call c, Map<String, Object> scope) {
        if (callee instanceof FunctionRef ref) {
            return functions.invoke(ref.name, evalArgs(c.args(), scope),
                evalKwargs(c.kwargs(), scope));
        }
        throw new JinjaException("not callable: " + describe(callee));
    }

    private List<Object> evalArgs(List<Expr> args, Map<String, Object> scope) {
        List<Object> out = new ArrayList<>(args.size());
        for (Expr a : args) {
            out.add(evalExpr(a, scope));
        }
        return out;
    }

    private Map<String, Object> evalKwargs(Map<String, Expr> kwargs, Map<String, Object> scope) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, Expr> en : kwargs.entrySet()) {
            out.put(en.getKey(), evalExpr(en.getValue(), scope));
        }
        return out;
    }

    private Object evalFilter(Expr.Filter f, Map<String, Object> scope) {
        Object target = evalExpr(f.target(), scope);
        List<Object> args = evalArgs(f.args(), scope);
        return filters.invoke(f.name(), target, args);
    }

    private Object evalTest(Expr.Test t, Map<String, Object> scope) {
        Object target = evalExpr(t.target(), scope);
        boolean res = tests.invoke(t.name(), target);
        return t.negated() ? !res : res;
    }

    private Object evalUnary(Expr.Unary u, Map<String, Object> scope) {
        Object v = evalExpr(u.target(), scope);
        switch (u.op()) {
            case "not":
                return !truthy(v);
            case "-":
                return negate(v);
            case "+":
                return v;
            default:
                throw new JinjaException("unknown unary op: " + u.op());
        }
    }

    private Object evalBinary(Expr.Binary b, Map<String, Object> scope) {
        switch (b.op()) {
            case "and": {
                Object l = evalExpr(b.left(), scope);
                return truthy(l) ? evalExpr(b.right(), scope) : l;
            }
            case "or": {
                Object l = evalExpr(b.left(), scope);
                return truthy(l) ? l : evalExpr(b.right(), scope);
            }
            default:
                return applyBinaryStrict(b.op(),
                    evalExpr(b.left(), scope), evalExpr(b.right(), scope));
        }
    }

    private static Object applyBinaryStrict(String op, Object l, Object r) {
        Object arith = tryArith(op, l, r);
        if (arith != SENTINEL) {
            return arith;
        }
        Object cmp = tryCompare(op, l, r);
        if (cmp != SENTINEL) {
            return cmp;
        }
        if (op.equals("in")) {
            return containsMembership(r, l);
        }
        if (op.equals("not in")) {
            return !containsMembership(r, l);
        }
        throw new JinjaException("unknown binary op: " + op);
    }

    private static final Object SENTINEL = new Object();

    private static Object tryArith(String op, Object l, Object r) {
        if (op.equals("-")) {
            Object dt = tryTemporalArith(l, r, "-");
            if (dt != SENTINEL) {
                return dt;
            }
        }
        switch (op) {
            case "+": return add(l, r);
            case "-": return arith(l, r, "-");
            case "*": return arith(l, r, "*");
            case "/": return arith(l, r, "/");
            case "%": return arith(l, r, "%");
            case "~": return stringify(l) + stringify(r);
            default: return SENTINEL;
        }
    }

    private static Object tryCompare(String op, Object l, Object r) {
        switch (op) {
            case "==": return pyEquals(l, r);
            case "!=": return !pyEquals(l, r);
            case "<":  return compareNumbersOrStrings(l, r) < 0;
            case ">":  return compareNumbersOrStrings(l, r) > 0;
            case "<=": return compareNumbersOrStrings(l, r) <= 0;
            case ">=": return compareNumbersOrStrings(l, r) >= 0;
            default: return SENTINEL;
        }
    }

    // ─── value helpers ──────────────────────────────────────────────────────

    /** Python-style truthiness: None/0/0.0/""/[]/{}/false → false; else true. */
    public static boolean truthy(Object v) {
        if (v == null) {
            return false;
        }
        if (v instanceof Boolean b) {
            return b;
        }
        if (v instanceof Number n) {
            return numericTruthy(n);
        }
        if (v instanceof CharSequence cs) {
            return cs.length() != 0;
        }
        if (v instanceof Collection<?> c) {
            return !c.isEmpty();
        }
        if (v instanceof Map<?, ?> m) {
            return !m.isEmpty();
        }
        if (v.getClass().isArray()) {
            return java.lang.reflect.Array.getLength(v) != 0;
        }
        return true;
    }

    private static boolean numericTruthy(Number n) {
        if (n instanceof Double d) {
            return d != 0.0d;
        }
        if (n instanceof Float ff) {
            return ff != 0.0f;
        }
        if (n instanceof BigInteger bi) {
            return bi.signum() != 0;
        }
        if (n instanceof BigDecimal bd) {
            return bd.signum() != 0;
        }
        return n.longValue() != 0L;
    }

    public static String stringify(Object v) {
        if (v == null) {
            return "None";
        }
        if (v instanceof Boolean b) {
            return b ? "True" : "False";
        }
        if (v instanceof Double d) {
            if (d == Math.floor(d) && !Double.isInfinite(d)) {
                return Long.toString(d.longValue()) + ".0";
            }
            return d.toString();
        }
        return v.toString();
    }

    @SuppressWarnings("unchecked")
    private static List<Object> toIterable(Object v) {
        if (v == null) {
            return List.of();
        }
        if (v instanceof List<?> l) {
            return (List<Object>) l;
        }
        if (v instanceof Collection<?> c) {
            return new ArrayList<>(c);
        }
        if (v instanceof Map<?, ?> m) {
            return new ArrayList<>(m.keySet());
        }
        if (v instanceof CharSequence cs) {
            return charsAsList(cs);
        }
        if (v.getClass().isArray()) {
            return arrayAsList(v);
        }
        if (v instanceof Iterable<?> it) {
            List<Object> out = new ArrayList<>();
            it.forEach(out::add);
            return out;
        }
        throw new JinjaException("not iterable: " + describe(v));
    }

    private static List<Object> charsAsList(CharSequence cs) {
        List<Object> out = new ArrayList<>(cs.length());
        for (int i = 0; i < cs.length(); i++) {
            out.add(String.valueOf(cs.charAt(i)));
        }
        return out;
    }

    private static List<Object> arrayAsList(Object array) {
        int len = java.lang.reflect.Array.getLength(array);
        List<Object> out = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            out.add(java.lang.reflect.Array.get(array, i));
        }
        return out;
    }

    private static Object indexInto(Object target, Object key) {
        if (target == null) {
            return null;
        }
        if (target instanceof Map<?, ?> m) {
            return m.get(key);
        }
        if (target instanceof List<?> l) {
            return seqIndex(l, l.size(), toInt(key), idx -> l.get(idx));
        }
        if (target instanceof CharSequence cs) {
            return seqIndex(cs, cs.length(), toInt(key),
                idx -> String.valueOf(cs.charAt(idx)));
        }
        if (target.getClass().isArray()) {
            int len = java.lang.reflect.Array.getLength(target);
            return seqIndex(target, len, toInt(key),
                idx -> java.lang.reflect.Array.get(target, idx));
        }
        if (key instanceof CharSequence cs) {
            return getAttribute(target, cs.toString());
        }
        throw new JinjaException("cannot index " + describe(target) + " with " + describe(key));
    }

    private static <T> Object seqIndex(T source, int length, int rawIdx,
                                        java.util.function.IntFunction<Object> get) {
        int idx = rawIdx < 0 ? length + rawIdx : rawIdx;
        if (idx < 0 || idx >= length) {
            return null;
        }
        return get.apply(idx);
    }

    /**
     * Attribute access. Tries map keys first (Jinja semantics for dicts), then
     * Java public fields, then JavaBean-style getters.
     */
    @SuppressWarnings("unchecked")
    public static Object getAttribute(Object target, String name) {
        if (target == null) {
            return null;
        }
        if (target instanceof Map<?, ?> m) {
            Map<Object, Object> mm = (Map<Object, Object>) m;
            if (mm.containsKey(name)) {
                return mm.get(name);
            }
            return null;
        }
        try {
            return target.getClass().getField(name).get(target);
        } catch (NoSuchFieldException | IllegalAccessException ignore) {
            // fall through
        }
        String getter = "get" + Character.toUpperCase(name.charAt(0)) + name.substring(1);
        for (Method m : target.getClass().getMethods()) {
            if ((m.getName().equals(getter) || m.getName().equals(name))
                    && m.getParameterCount() == 0) {
                try {
                    return m.invoke(target);
                } catch (ReflectiveOperationException e) {
                    throw new JinjaException("attribute access failed: " + name, e);
                }
            }
        }
        return null;
    }

    private static int toInt(Object o) {
        if (o instanceof Number n) {
            return n.intValue();
        }
        if (o instanceof CharSequence cs) {
            return Integer.parseInt(cs.toString());
        }
        throw new JinjaException("expected integer, got " + describe(o));
    }

    private static Object negate(Object v) {
        if (v instanceof Long l) {
            return -l;
        }
        if (v instanceof Integer i) {
            return -(long) i;
        }
        if (v instanceof Double d) {
            return -d;
        }
        if (v instanceof BigInteger bi) {
            return bi.negate();
        }
        if (v instanceof BigDecimal bd) {
            return bd.negate();
        }
        if (v instanceof Number n) {
            return -n.doubleValue();
        }
        throw new JinjaException("cannot negate: " + describe(v));
    }

    private static Object add(Object l, Object r) {
        Object dt = tryTemporalArith(l, r, "+");
        if (dt != SENTINEL) {
            return dt;
        }
        if (l instanceof CharSequence || r instanceof CharSequence) {
            return stringify(l) + stringify(r);
        }
        if (l instanceof List<?> ll && r instanceof List<?> rl) {
            List<Object> out = new ArrayList<>(ll.size() + rl.size());
            out.addAll(ll);
            out.addAll(rl);
            return out;
        }
        return arith(l, r, "+");
    }

    private static Object tryTemporalArith(Object l, Object r, String op) {
        if (l instanceof java.time.temporal.Temporal t && r instanceof Datetimes.IsoDuration d) {
            return op.equals("+") ? d.addTo(t) : d.subtractFrom(t);
        }
        if (l instanceof Datetimes.IsoDuration d && r instanceof java.time.temporal.Temporal t
            && op.equals("+")) {
            return d.addTo(t);
        }
        return SENTINEL;
    }

    private static Object arith(Object l, Object r, String op) {
        if (isFloatLike(l) || isFloatLike(r)) {
            double ld = toDouble(l);
            double rd = toDouble(r);
            switch (op) {
                case "+": return ld + rd;
                case "-": return ld - rd;
                case "*": return ld * rd;
                case "/": return ld / rd;
                case "%": return ld % rd;
                default: throw new JinjaException("unknown op " + op);
            }
        }
        long ll = toLong(l);
        long rl = toLong(r);
        switch (op) {
            case "+": return ll + rl;
            case "-": return ll - rl;
            case "*": return ll * rl;
            case "/":
                // Python `/` is true division, returns float.
                return (double) ll / (double) rl;
            case "%": return ll % rl;
            default: throw new JinjaException("unknown op " + op);
        }
    }

    private static boolean isFloatLike(Object v) {
        return v instanceof Double || v instanceof Float || v instanceof BigDecimal;
    }

    private static long toLong(Object v) {
        if (v instanceof Number n) {
            return n.longValue();
        }
        if (v instanceof Boolean b) {
            return b ? 1L : 0L;
        }
        if (v instanceof CharSequence cs) {
            return Long.parseLong(cs.toString());
        }
        throw new JinjaException("not numeric: " + describe(v));
    }

    private static double toDouble(Object v) {
        if (v instanceof Number n) {
            return n.doubleValue();
        }
        if (v instanceof Boolean b) {
            return b ? 1.0 : 0.0;
        }
        if (v instanceof CharSequence cs) {
            return Double.parseDouble(cs.toString());
        }
        throw new JinjaException("not numeric: " + describe(v));
    }

    private static boolean pyEquals(Object l, Object r) {
        if (l == null || r == null) {
            return l == r;
        }
        if (l instanceof Number && r instanceof Number) {
            return Double.compare(((Number) l).doubleValue(), ((Number) r).doubleValue()) == 0;
        }
        return l.equals(r);
    }

    private static int compareNumbersOrStrings(Object l, Object r) {
        if (l == null || r == null) {
            throw new JinjaException("cannot compare null");
        }
        if (l instanceof Number && r instanceof Number) {
            return Double.compare(((Number) l).doubleValue(), ((Number) r).doubleValue());
        }
        if (l instanceof CharSequence && r instanceof CharSequence) {
            return l.toString().compareTo(r.toString());
        }
        if (l instanceof Comparable && l.getClass().isInstance(r)) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            int x = ((Comparable) l).compareTo(r);
            return x;
        }
        throw new JinjaException("cannot compare " + describe(l) + " and " + describe(r));
    }

    private static boolean containsMembership(Object container, Object element) {
        if (container == null) {
            return false;
        }
        if (container instanceof Map<?, ?> m) {
            return m.containsKey(element);
        }
        if (container instanceof Collection<?> c) {
            for (Object o : c) {
                if (pyEquals(o, element)) {
                    return true;
                }
            }
            return false;
        }
        if (container instanceof CharSequence cs) {
            return cs.toString().contains(element == null ? "None" : element.toString());
        }
        if (container.getClass().isArray()) {
            int len = java.lang.reflect.Array.getLength(container);
            for (int i = 0; i < len; i++) {
                if (pyEquals(java.lang.reflect.Array.get(container, i), element)) {
                    return true;
                }
            }
            return false;
        }
        throw new JinjaException("not iterable for membership: " + describe(container));
    }

    private static String describe(Object v) {
        if (v == null) {
            return "None";
        }
        return v.getClass().getSimpleName() + "(" + v + ")";
    }

    /** Internal sentinel for unresolved-but-known function references. */
    private record FunctionRef(String name) { }

    /** Unused but kept to silence javac warnings about unused imports. */
    @SuppressWarnings("unused")
    private static List<Object> array(Object... xs) {
        return Arrays.asList(xs);
    }
}
