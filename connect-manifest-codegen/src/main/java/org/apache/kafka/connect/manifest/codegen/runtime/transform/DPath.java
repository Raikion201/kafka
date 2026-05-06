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
package org.apache.kafka.connect.manifest.codegen.runtime.transform;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Java port of the subset of Python {@code dpath} operations used by Airbyte's declarative
 * transformations. Covers {@code dpath.new} (set), {@code dpath.delete}, simple read-only
 * navigation, and single-segment {@code *} wildcard expansion. Globbing, deep-recursion
 * patterns, and merge are intentionally not implemented — no manifest in our corpus uses
 * those forms.
 *
 * <p>A path segment that parses as a non-negative integer indexes a {@link List} parent;
 * any other segment indexes a {@link Map} parent. Setting through a numeric segment on a
 * list extends the list with {@code null}s up to the requested index, matching the Python
 * dpath docstring example: setting index 5 on {@code ["value"]} produces
 * {@code ["value", null, null, null, null, "new"]}.</p>
 */
public final class DPath {

    private DPath() {
    }

    /** Mkdir-p set: creates intermediate {@link Map}s as needed and grows {@link List}s for numeric indices. */
    public static void set(Map<String, Object> root, List<String> path, Object value) {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("DPath.set requires a non-empty path");
        }
        Object cur = root;
        for (int i = 0; i < path.size() - 1; i++) {
            cur = descend(cur, path.get(i), path.get(i + 1));
        }
        setTerminal(cur, path.get(path.size() - 1), value);
    }

    @SuppressWarnings("unchecked")
    private static Object descend(Object cur, String seg, String nextSeg) {
        boolean nextIsIndex = isIndex(nextSeg);
        if (cur instanceof Map) {
            Map<String, Object> m = (Map<String, Object>) cur;
            Object child = m.get(seg);
            if (needsNewContainer(child, nextIsIndex)) {
                child = nextIsIndex ? new ArrayList<>() : new LinkedHashMap<>();
                m.put(seg, child);
            }
            return child;
        } else if (cur instanceof List) {
            List<Object> l = (List<Object>) cur;
            int idx = parseIndex(seg);
            growTo(l, idx);
            Object child = l.get(idx);
            if (needsNewContainer(child, nextIsIndex)) {
                child = nextIsIndex ? new ArrayList<>() : new LinkedHashMap<>();
                l.set(idx, child);
            }
            return child;
        }
        throw new IllegalStateException("DPath.set: cannot descend into non-container at segment " + seg);
    }

    private static boolean needsNewContainer(Object child, boolean nextIsIndex) {
        return child == null
            || (nextIsIndex && !(child instanceof List))
            || (!nextIsIndex && !(child instanceof Map));
    }

    @SuppressWarnings("unchecked")
    private static void setTerminal(Object cur, String last, Object value) {
        if (cur instanceof Map) {
            ((Map<String, Object>) cur).put(last, value);
        } else if (cur instanceof List) {
            List<Object> l = (List<Object>) cur;
            int idx = parseIndex(last);
            growTo(l, idx);
            l.set(idx, value);
        } else {
            throw new IllegalStateException("DPath.set: terminal segment landed on non-container");
        }
    }

    /**
     * Best-effort delete. Returns {@code false} if any intermediate segment is missing or
     * has the wrong shape, instead of throwing — matches Python {@code dpath.delete}'s
     * tolerance (Airbyte's RemoveFields swallows {@code PathNotFound}). For list-element
     * pointers the slot is set to {@code null} rather than spliced out.
     */
    @SuppressWarnings("unchecked")
    public static boolean delete(Map<String, Object> root, List<String> pointer) {
        if (pointer == null || pointer.isEmpty()) {
            return false;
        }
        Object cur = root;
        for (int i = 0; i < pointer.size() - 1; i++) {
            String seg = pointer.get(i);
            if (cur instanceof Map) {
                cur = ((Map<String, Object>) cur).get(seg);
            } else if (cur instanceof List) {
                int idx = tryIndex(seg);
                if (idx < 0 || idx >= ((List<?>) cur).size()) {
                    return false;
                }
                cur = ((List<?>) cur).get(idx);
            } else {
                return false;
            }
            if (cur == null) {
                return false;
            }
        }
        String last = pointer.get(pointer.size() - 1);
        if (cur instanceof Map) {
            Map<String, Object> m = (Map<String, Object>) cur;
            if (!m.containsKey(last)) {
                return false;
            }
            m.remove(last);
            return true;
        }
        if (cur instanceof List) {
            List<Object> l = (List<Object>) cur;
            int idx = tryIndex(last);
            if (idx < 0 || idx >= l.size()) {
                return false;
            }
            l.set(idx, null);
            return true;
        }
        return false;
    }

    /** Read-only traversal. Returns {@code null} for missing or wrong-shape paths. */
    @SuppressWarnings("unchecked")
    public static Object navigate(Object root, List<String> path) {
        if (path == null || path.isEmpty()) {
            return root;
        }
        Object cur = root;
        for (String seg : path) {
            if (cur == null) {
                return null;
            }
            if (cur instanceof Map) {
                cur = ((Map<String, Object>) cur).get(seg);
            } else if (cur instanceof List) {
                int idx = tryIndex(seg);
                if (idx < 0 || idx >= ((List<?>) cur).size()) {
                    return null;
                }
                cur = ((List<?>) cur).get(idx);
            } else {
                return null;
            }
        }
        return cur;
    }

    /**
     * Expands single-segment {@code *} wildcards into concrete paths reaching every leaf
     * that exists at the matching positions in {@code root}. Map wildcards expand to all
     * keys; list wildcards expand to all valid indices. The result is flat — no recursion
     * past a {@code *} segment — which matches what {@code DpathFlattenFields} needs.
     */
    public static List<Match> expandWildcards(Object root, List<String> pathWithStars) {
        List<Match> out = new ArrayList<>();
        expand(root, pathWithStars, 0, new ArrayList<>(), out);
        return out;
    }

    @SuppressWarnings("unchecked")
    private static void expand(Object cur, List<String> pattern, int idx, List<String> acc, List<Match> out) {
        if (cur == null) {
            return;
        }
        if (idx == pattern.size()) {
            out.add(new Match(new ArrayList<>(acc), cur));
            return;
        }
        String seg = pattern.get(idx);
        if ("*".equals(seg)) {
            if (cur instanceof Map) {
                for (Map.Entry<String, Object> e : ((Map<String, Object>) cur).entrySet()) {
                    acc.add(e.getKey());
                    expand(e.getValue(), pattern, idx + 1, acc, out);
                    acc.remove(acc.size() - 1);
                }
            } else if (cur instanceof List) {
                List<?> l = (List<?>) cur;
                for (int i = 0; i < l.size(); i++) {
                    acc.add(Integer.toString(i));
                    expand(l.get(i), pattern, idx + 1, acc, out);
                    acc.remove(acc.size() - 1);
                }
            }
            return;
        }
        Object next;
        if (cur instanceof Map) {
            next = ((Map<String, Object>) cur).get(seg);
        } else if (cur instanceof List) {
            int i = tryIndex(seg);
            next = (i < 0 || i >= ((List<?>) cur).size()) ? null : ((List<?>) cur).get(i);
        } else {
            return;
        }
        acc.add(seg);
        expand(next, pattern, idx + 1, acc, out);
        acc.remove(acc.size() - 1);
    }

    private static boolean isIndex(String s) {
        return tryIndex(s) >= 0;
    }

    private static int tryIndex(String s) {
        if (s == null || s.isEmpty()) {
            return -1;
        }
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) {
                return -1;
            }
        }
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException nfe) {
            return -1;
        }
    }

    private static int parseIndex(String s) {
        int i = tryIndex(s);
        if (i < 0) {
            throw new IllegalArgumentException("DPath: expected non-negative integer segment, got: " + s);
        }
        return i;
    }

    private static void growTo(List<Object> l, int idx) {
        while (l.size() <= idx) {
            l.add(null);
        }
    }

    /** Result of {@link #expandWildcards(Object, List)} — a concrete path and the value at it. */
    public static final class Match {
        private final List<String> path;
        private final Object value;

        public Match(List<String> path, Object value) {
            this.path = path;
            this.value = value;
        }

        public List<String> path() {
            return path;
        }

        public Object value() {
            return value;
        }
    }
}
