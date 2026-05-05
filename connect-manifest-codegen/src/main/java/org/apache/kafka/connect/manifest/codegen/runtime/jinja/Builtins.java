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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Wires the standard Jinja and Airbyte built-in filters / functions onto a set
 * of registries. The datetime helpers ({@code now_utc}, {@code today_utc},
 * {@code duration}, {@code format_datetime}) live in {@link DatetimeBuiltins}.
 */
public final class Builtins {

    private Builtins() { }

    /** Install all built-ins (functions, filters, tests methods, datetime macros). */
    public static void install(Functions fns, Filters filters, Tests tests, Methods methods) {
        installFunctions(fns);
        installCoercionFilters(filters);
        installCollectionFilters(filters);
        installStringFilters(filters);
        Datetimes.install(fns, methods);
    }

    private static void installFunctions(Functions fns) {
        fns.register("max", (a, k) -> reduceCompare(a, true));
        fns.register("min", (a, k) -> reduceCompare(a, false));
        fns.register("len", (a, k) -> length(a.get(0)));
        fns.register("abs", (a, k) -> absoluteValue(a.get(0)));
        fns.register("sum", (a, k) -> sumOf(a.get(0)));
        fns.register("round", Builtins::roundFn);
        fns.register("str", (a, k) -> Evaluator.stringify(a.get(0)));
        fns.register("int", (a, k) -> coerceInt(a.get(0)));
        fns.register("float", (a, k) -> coerceFloat(a.get(0)));
        fns.register("bool", (a, k) -> Evaluator.truthy(a.get(0)));
        fns.register("list", (a, k) -> coerceList(a.get(0)));
        fns.register("range", Builtins::rangeFn);
        fns.register("dict", (a, k) -> new LinkedHashMap<>(k));
    }

    // ─── filter installers ──────────────────────────────────────────────────

    private static void installCoercionFilters(Filters f) {
        f.register("string", (t, a) -> Evaluator.stringify(t));
        f.register("int", (t, a) -> coerceInt(t));
        f.register("float", (t, a) -> coerceFloat(t));
        f.register("bool", (t, a) -> Evaluator.truthy(t));
        f.register("list", (t, a) -> coerceList(t));
        f.register("default", (t, a) -> {
            Object dflt = a.isEmpty() ? "" : a.get(0);
            boolean useFalsy = a.size() > 1 && Evaluator.truthy(a.get(1));
            if (t == null) {
                return dflt;
            }
            return useFalsy && !Evaluator.truthy(t) ? dflt : t;
        });
    }

    private static void installCollectionFilters(Filters f) {
        f.register("length", (t, a) -> length(t));
        f.register("count", (t, a) -> length(t));
        f.register("first", (t, a) -> firstOf(t));
        f.register("last", (t, a) -> lastOf(t));
        f.register("min", (t, a) -> reduceCompare(toList(t), false));
        f.register("max", (t, a) -> reduceCompare(toList(t), true));
        f.register("sum", (t, a) -> sumOf(t));
        f.register("abs", (t, a) -> absoluteValue(t));
        f.register("reverse", (t, a) -> {
            List<Object> l = new ArrayList<>(toList(t));
            Collections.reverse(l);
            return l;
        });
        f.register("sort", (t, a) -> {
            List<Object> l = new ArrayList<>(toList(t));
            l.sort(Builtins::genericCompare);
            return l;
        });
        f.register("unique", (t, a) -> {
            List<Object> out = new ArrayList<>();
            for (Object o : toList(t)) {
                if (!out.contains(o)) {
                    out.add(o);
                }
            }
            return out;
        });
        f.register("join", (t, a) -> {
            String sep = a.isEmpty() ? "" : Evaluator.stringify(a.get(0));
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (Object o : toList(t)) {
                if (!first) {
                    sb.append(sep);
                }
                sb.append(Evaluator.stringify(o));
                first = false;
            }
            return sb.toString();
        });
        f.register("tojson", (t, a) -> JsonEncoder.encode(t));
    }

    private static void installStringFilters(Filters f) {
        f.register("upper", (t, a) -> Evaluator.stringify(t).toUpperCase(Locale.ROOT));
        f.register("lower", (t, a) -> Evaluator.stringify(t).toLowerCase(Locale.ROOT));
        f.register("trim", (t, a) -> Evaluator.stringify(t).strip());
        f.register("strip", (t, a) -> Evaluator.stringify(t).strip());
        f.register("capitalize", (t, a) -> {
            String s = Evaluator.stringify(t);
            return s.isEmpty() ? s : Character.toUpperCase(s.charAt(0))
                + s.substring(1).toLowerCase(Locale.ROOT);
        });
        f.register("title", (t, a) -> titleCase(Evaluator.stringify(t)));
        f.register("replace", (t, a) ->
            Evaluator.stringify(t).replace(
                Evaluator.stringify(a.get(0)), Evaluator.stringify(a.get(1))));
        f.register("truncate", (t, a) -> {
            String s = Evaluator.stringify(t);
            int n = a.isEmpty() ? 255 : ((Number) a.get(0)).intValue();
            return s.length() <= n ? s : s.substring(0, n) + "...";
        });
        f.register("regex_search", (t, a) -> {
            Pattern p = Pattern.compile(Evaluator.stringify(a.get(0)));
            Matcher m = p.matcher(Evaluator.stringify(t));
            return m.find() ? (m.groupCount() > 0 ? m.group(1) : m.group()) : null;
        });
        f.register("regex_replace", (t, a) -> {
            Pattern p = Pattern.compile(Evaluator.stringify(a.get(0)));
            return p.matcher(Evaluator.stringify(t))
                .replaceAll(Evaluator.stringify(a.get(1)));
        });
    }

    // ─── coercion helpers ───────────────────────────────────────────────────

    static Object coerceInt(Object v) {
        if (v == null) {
            return 0L;
        }
        if (v instanceof Boolean b) {
            return b ? 1L : 0L;
        }
        if (v instanceof Number n) {
            return n.longValue();
        }
        return Long.parseLong(v.toString().trim());
    }

    static Object coerceFloat(Object v) {
        if (v == null) {
            return 0.0;
        }
        if (v instanceof Number n) {
            return n.doubleValue();
        }
        return Double.parseDouble(v.toString().trim());
    }

    @SuppressWarnings("unchecked")
    static List<Object> coerceList(Object v) {
        if (v == null) {
            return new ArrayList<>();
        }
        if (v instanceof List<?> l) {
            return new ArrayList<>((List<Object>) l);
        }
        if (v instanceof Collection<?> c) {
            return new ArrayList<>(c);
        }
        if (v instanceof Map<?, ?> m) {
            return new ArrayList<>(m.keySet());
        }
        if (v instanceof CharSequence cs) {
            List<Object> out = new ArrayList<>();
            for (int i = 0; i < cs.length(); i++) {
                out.add(String.valueOf(cs.charAt(i)));
            }
            return out;
        }
        if (v.getClass().isArray()) {
            int len = java.lang.reflect.Array.getLength(v);
            List<Object> out = new ArrayList<>(len);
            for (int i = 0; i < len; i++) {
                out.add(java.lang.reflect.Array.get(v, i));
            }
            return out;
        }
        return new ArrayList<>(List.of(v));
    }

    static List<Object> toList(Object v) {
        return coerceList(v);
    }

    // ─── numeric helpers ────────────────────────────────────────────────────

    static Object length(Object v) {
        if (v == null) {
            return 0L;
        }
        if (v instanceof CharSequence cs) {
            return (long) cs.length();
        }
        if (v instanceof Collection<?> c) {
            return (long) c.size();
        }
        if (v instanceof Map<?, ?> m) {
            return (long) m.size();
        }
        if (v.getClass().isArray()) {
            return (long) java.lang.reflect.Array.getLength(v);
        }
        throw new JinjaException("length() not supported for " + v.getClass().getSimpleName());
    }

    static Object absoluteValue(Object v) {
        if (v instanceof Long l) {
            return Math.abs(l);
        }
        if (v instanceof Integer i) {
            return Math.abs((long) i);
        }
        if (v instanceof Double d) {
            return Math.abs(d);
        }
        if (v instanceof Number n) {
            return Math.abs(n.doubleValue());
        }
        throw new JinjaException("abs(): not numeric");
    }

    static Object sumOf(Object v) {
        double sumD = 0.0;
        long sumL = 0L;
        boolean anyFloat = false;
        for (Object o : coerceList(v)) {
            if (o instanceof Double || o instanceof Float) {
                anyFloat = true;
                sumD += ((Number) o).doubleValue();
            } else if (o instanceof Number n) {
                sumL += n.longValue();
                sumD += n.doubleValue();
            } else if (o == null) {
                continue;
            } else {
                throw new JinjaException("sum(): non-numeric element " + o);
            }
        }
        if (anyFloat) {
            return sumD;
        }
        return sumL;
    }

    private static Object reduceCompare(List<Object> args, boolean wantMax) {
        List<Object> values;
        if (args.size() == 1) {
            values = coerceList(args.get(0));
        } else {
            values = args;
        }
        if (values.isEmpty()) {
            throw new JinjaException((wantMax ? "max" : "min") + "() of empty sequence");
        }
        Object best = values.get(0);
        for (int i = 1; i < values.size(); i++) {
            int cmp = genericCompare(values.get(i), best);
            if (wantMax ? cmp > 0 : cmp < 0) {
                best = values.get(i);
            }
        }
        return best;
    }

    private static int genericCompare(Object a, Object b) {
        if (a instanceof Number na && b instanceof Number nb) {
            return Double.compare(na.doubleValue(), nb.doubleValue());
        }
        return Evaluator.stringify(a).compareTo(Evaluator.stringify(b));
    }

    private static Object firstOf(Object v) {
        List<Object> l = coerceList(v);
        return l.isEmpty() ? null : l.get(0);
    }

    private static Object lastOf(Object v) {
        List<Object> l = coerceList(v);
        return l.isEmpty() ? null : l.get(l.size() - 1);
    }

    private static Object roundFn(List<Object> args, Map<String, Object> kwargs) {
        double v = ((Number) args.get(0)).doubleValue();
        int digits = args.size() > 1 ? ((Number) args.get(1)).intValue() : 0;
        double factor = Math.pow(10, digits);
        double rounded = Math.round(v * factor) / factor;
        if (digits == 0) {
            return (long) rounded;
        }
        return rounded;
    }

    private static Object rangeFn(List<Object> args, Map<String, Object> kwargs) {
        long start;
        long stop;
        long step = 1;
        if (args.size() == 1) {
            start = 0;
            stop = ((Number) args.get(0)).longValue();
        } else {
            start = ((Number) args.get(0)).longValue();
            stop = ((Number) args.get(1)).longValue();
            if (args.size() > 2) {
                step = ((Number) args.get(2)).longValue();
            }
        }
        List<Object> out = new ArrayList<>();
        if (step > 0) {
            for (long i = start; i < stop; i += step) {
                out.add(i);
            }
        } else if (step < 0) {
            for (long i = start; i > stop; i += step) {
                out.add(i);
            }
        } else {
            throw new JinjaException("range(): step cannot be zero");
        }
        return out;
    }

    private static String titleCase(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        boolean wordStart = true;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (Character.isWhitespace(c) || !Character.isLetterOrDigit(c)) {
                wordStart = true;
                sb.append(c);
            } else if (wordStart) {
                sb.append(Character.toUpperCase(c));
                wordStart = false;
            } else {
                sb.append(Character.toLowerCase(c));
            }
        }
        return sb.toString();
    }
}
