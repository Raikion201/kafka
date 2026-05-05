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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Registry for object method dispatch ({@code obj.method(args)}). Looks up by
 * (concrete-target-type → method-name); falls back to a small set of built-in
 * map/list/string method behaviours that the manifest corpus relies on, then
 * to Java reflection on the target object.
 */
public final class Methods {

    @FunctionalInterface
    public interface Method3 {
        Object apply(Object self, List<Object> args, Map<String, Object> kwargs);
    }

    private final Map<String, Method3> table = new HashMap<>();

    public Methods() {
        installDefaults();
    }

    public void register(String key, Method3 m) {
        table.put(key, m);
    }

    public Object invoke(Object self, String name, List<Object> args, Map<String, Object> kwargs) {
        // 1) Exact (type, name) registration.
        if (self != null) {
            Method3 m = table.get(self.getClass().getName() + "#" + name);
            if (m != null) {
                return m.apply(self, args, kwargs);
            }
        }
        // 2) Generic-by-name dispatch handles the bulk of Python dict/list/str semantics.
        Method3 generic = table.get("*#" + name);
        if (generic != null) {
            return generic.apply(self, args, kwargs);
        }
        // 3) Reflective fallback for Java objects whose methods match by name + arity.
        return reflectInvoke(self, name, args);
    }

    private void installDefaults() {
        installMapMethods();
        installStringMethods();
        installListMethods();
    }

    private void installMapMethods() {
        register("*#get", (self, args, kw) -> {
            if (self instanceof Map<?, ?> m) {
                Object key = args.get(0);
                Object dflt = args.size() > 1 ? args.get(1) : null;
                return m.containsKey(key) ? m.get(key) : dflt;
            }
            return Evaluator.getAttribute(self, args.get(0).toString());
        });
        register("*#keys", (self, args, kw) -> requireMap(self).keySet().stream().toList());
        register("*#values", (self, args, kw) -> new ArrayList<>(requireMap(self).values()));
        register("*#items", (self, args, kw) -> {
            Map<?, ?> m = requireMap(self);
            List<Object> out = new ArrayList<>(m.size());
            for (Map.Entry<?, ?> e : m.entrySet()) {
                out.add(List.of(e.getKey(), e.getValue() == null ? "" : e.getValue()));
            }
            return out;
        });
    }

    private void installStringMethods() {
        register("*#startswith", (self, args, kw) ->
            asString(self).startsWith(asString(args.get(0))));
        register("*#endswith", (self, args, kw) ->
            asString(self).endsWith(asString(args.get(0))));
        register("*#lower", (self, args, kw) -> asString(self).toLowerCase(Locale.ROOT));
        register("*#upper", (self, args, kw) -> asString(self).toUpperCase(Locale.ROOT));
        register("*#strip", (self, args, kw) -> asString(self).strip());
        register("*#split", (self, args, kw) -> doSplit(asString(self), args));
        register("*#replace", (self, args, kw) ->
            asString(self).replace(asString(args.get(0)), asString(args.get(1))));
        register("*#format", (self, args, kw) -> doFormat(asString(self), args));
        register("*#join", (self, args, kw) -> doJoin(asString(self), (Collection<?>) args.get(0)));
    }

    private void installListMethods() {
        register("*#append", (self, args, kw) -> {
            @SuppressWarnings("unchecked")
            List<Object> l = (List<Object>) self;
            l.add(args.get(0));
            return null;
        });
    }

    private static Map<?, ?> requireMap(Object self) {
        if (self instanceof Map<?, ?> m) {
            return m;
        }
        throw new JinjaException("expected a mapping, got "
            + (self == null ? "null" : self.getClass().getSimpleName()));
    }

    private static List<String> doSplit(String s, List<Object> args) {
        if (args.isEmpty()) {
            return new ArrayList<>(List.of(s.split("\\s+")));
        }
        String sep = asString(args.get(0));
        int limit = args.size() > 1 ? ((Number) args.get(1)).intValue() + 1 : -1;
        return new ArrayList<>(List.of(s.split(java.util.regex.Pattern.quote(sep), limit)));
    }

    private static String doFormat(String s, List<Object> args) {
        String out = s;
        for (int i = 0; i < args.size(); i++) {
            out = out.replace("{" + i + "}", String.valueOf(args.get(i)));
        }
        return out.replace("{}", args.isEmpty() ? "" : String.valueOf(args.get(0)));
    }

    private static String doJoin(String sep, Collection<?> parts) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Object o : parts) {
            if (!first) {
                sb.append(sep);
            }
            sb.append(o == null ? "" : o.toString());
            first = false;
        }
        return sb.toString();
    }

    private static String asString(Object o) {
        return o == null ? "" : o.toString();
    }

    private static Object reflectInvoke(Object self, String name, List<Object> args) {
        if (self == null) {
            throw new JinjaException("cannot call " + name + "() on None");
        }
        for (Method m : self.getClass().getMethods()) {
            if (m.getName().equals(name) && m.getParameterCount() == args.size()) {
                try {
                    return m.invoke(self, args.toArray());
                } catch (ReflectiveOperationException e) {
                    throw new JinjaException("method invocation failed: " + name, e);
                }
            }
        }
        throw new JinjaException(
            "no method '" + name + "' on " + self.getClass().getSimpleName());
    }

    /** Convenience: build an immutable two-tuple as a List<Object>. */
    @SuppressWarnings("unused")
    private static Map.Entry<Object, Object> pair(Object k, Object v) {
        return new AbstractMap.SimpleImmutableEntry<>(k, v);
    }
}
