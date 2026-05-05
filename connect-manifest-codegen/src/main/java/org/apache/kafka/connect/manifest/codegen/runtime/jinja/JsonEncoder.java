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

import java.util.Collection;
import java.util.Map;

/**
 * Minimal JSON encoder used by the {@code tojson} filter. Emits compact JSON
 * for the value types produced by the evaluator (null, boolean, Number,
 * CharSequence, Collection, Map, arrays).
 */
final class JsonEncoder {

    private JsonEncoder() { }

    static String encode(Object v) {
        StringBuilder sb = new StringBuilder();
        write(v, sb);
        return sb.toString();
    }

    private static void write(Object v, StringBuilder sb) {
        if (v == null) {
            sb.append("null");
        } else if (v instanceof Boolean b) {
            sb.append(b);
        } else if (v instanceof Number n) {
            writeNumber(n, sb);
        } else if (v instanceof CharSequence cs) {
            writeString(cs.toString(), sb);
        } else if (v instanceof Map<?, ?> m) {
            writeMap(m, sb);
        } else if (v instanceof Collection<?> c) {
            writeCollection(c, sb);
        } else if (v.getClass().isArray()) {
            writeArray(v, sb);
        } else {
            writeString(v.toString(), sb);
        }
    }

    private static void writeNumber(Number n, StringBuilder sb) {
        if (n instanceof Double d) {
            sb.append(d.isNaN() || d.isInfinite() ? "null" : d.toString());
        } else if (n instanceof Float f) {
            sb.append(f.isNaN() || f.isInfinite() ? "null" : f.toString());
        } else {
            sb.append(n);
        }
    }

    private static void writeString(String s, StringBuilder sb) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            appendJsonChar(c, sb);
        }
        sb.append('"');
    }

    private static void appendJsonChar(char c, StringBuilder sb) {
        switch (c) {
            case '"':
                sb.append("\\\"");
                return;
            case '\\':
                sb.append("\\\\");
                return;
            case '\b':
                sb.append("\\b");
                return;
            case '\f':
                sb.append("\\f");
                return;
            case '\n':
                sb.append("\\n");
                return;
            case '\r':
                sb.append("\\r");
                return;
            case '\t':
                sb.append("\\t");
                return;
            default:
                if (c < 0x20) {
                    sb.append(String.format("\\u%04x", (int) c));
                } else {
                    sb.append(c);
                }
        }
    }

    private static void writeMap(Map<?, ?> m, StringBuilder sb) {
        sb.append('{');
        boolean first = true;
        for (Map.Entry<?, ?> e : m.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            writeString(String.valueOf(e.getKey()), sb);
            sb.append(':');
            write(e.getValue(), sb);
            first = false;
        }
        sb.append('}');
    }

    private static void writeCollection(Collection<?> c, StringBuilder sb) {
        sb.append('[');
        boolean first = true;
        for (Object o : c) {
            if (!first) {
                sb.append(',');
            }
            write(o, sb);
            first = false;
        }
        sb.append(']');
    }

    private static void writeArray(Object array, StringBuilder sb) {
        sb.append('[');
        int len = java.lang.reflect.Array.getLength(array);
        for (int i = 0; i < len; i++) {
            if (i > 0) {
                sb.append(',');
            }
            write(java.lang.reflect.Array.get(array, i), sb);
        }
        sb.append(']');
    }
}
