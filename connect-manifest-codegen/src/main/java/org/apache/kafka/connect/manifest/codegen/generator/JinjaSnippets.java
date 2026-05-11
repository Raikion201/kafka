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
package org.apache.kafka.connect.manifest.codegen.generator;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Compile-time helpers for detecting and emitting Jinja templates.
 *
 * <p>Two patterns ({@link #CONFIG_TEMPLATE}, {@link #STREAM_PARTITION_RE}) are kept
 * because the generator still needs to know whether a URL or path contains a config or
 * stream_partition reference in order to choose between a plain string literal and a
 * dynamic concatenation. All actual interpolation goes through
 * {@link org.apache.kafka.connect.manifest.codegen.runtime.jinja.JinjaRenderer} at
 * runtime via {@link #interpolateTemplate(String, Set)}.
 */
public final class JinjaSnippets {

    /** {@code {{ config['key'] }}} or {@code {{ config["key"] }}} (optional {@code or default}). */
    public static final Pattern CONFIG_TEMPLATE = Pattern.compile(
        "\\{\\{\\s*config\\[['\"]([^'\"]+)['\"]\\](?:\\s+or\\s+[^}]+)?\\s*\\}\\}");

    /**
     * {@code {{ stream_partition.fieldName }}} or {@code {{ stream_partition['fieldName'] }}}
     * references in child-stream URL paths. Both dot and bracket notation are matched.
     */
    public static final Pattern STREAM_PARTITION_RE =
        Pattern.compile(
            "\\{\\{\\s*stream_partition(?:\\.(\\w+)|\\[\\s*['\"]([^'\"]+)['\"]\\s*\\])\\s*\\}\\}");

    /** Any {@code {{ ... }}} block — used to enumerate all Jinja expressions in a string. */
    private static final Pattern ANY_EXPR = Pattern.compile("\\{\\{[^}]*\\}\\}");

    private JinjaSnippets() {
    }

    /**
     * Escapes a raw string for use inside a Java double-quoted string literal.
     */
    public static String escapeJavaString(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"")
            .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }

    /**
     * Strips a Jinja template expression and everything after it from the end of {@code url},
     * leaving only the static prefix. Useful for extracting the base-URL prefix from a
     * manifest URL template such as:
     * <pre>
     *   "https://sheets.googleapis.com/v4/spreadsheets/{{ config['spreadsheet_id'] }}/..."
     *   → "https://sheets.googleapis.com/v4/spreadsheets/"
     * </pre>
     */
    public static String stripTemplatedSuffix(String url) {
        if (url == null) return "";
        return url.replaceAll("\\{\\{[^}]*\\}\\}.*$", "");
    }

    /**
     * Returns {@code true} if every {@code {{ ... }}} expression found in any of the given
     * strings is recognised as a config reference or stream_partition reference. The check
     * is intentionally conservative — it only inspects the simplest shapes the dynamic-stream
     * codegen path knows how to handle. Templates containing filters, function calls, or
     * conditionals will return {@code false} and fall back to the stub task body.
     */
    public static boolean allTemplatesRecognized(Iterable<String> templateStrings) {
        for (String s : templateStrings) {
            if (s == null) continue;
            Matcher any = ANY_EXPR.matcher(s);
            while (any.find()) {
                String expr = any.group();
                if (!CONFIG_TEMPLATE.matcher(expr).find()
                        && !STREAM_PARTITION_RE.matcher(expr).find()) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Returns a Java expression that yields the rendered value of {@code template}
     * at runtime. Plain literals (no {@code "{{"} or {@code "{%"}) collapse to a
     * Java string literal; everything else becomes
     * {@code render("template", jinjaCtx())}, deferring Jinja semantics to
     * {@link org.apache.kafka.connect.manifest.codegen.runtime.jinja.JinjaRenderer}
     * so manifests get exact Airbyte CDK behaviour (filters, functions, conditionals)
     * rather than the legacy regex subset.
     *
     * @param template  the Jinja template string from the manifest
     * @param specKeys  declared config property keys (currently unused; kept for API
     *                  compatibility while the stream_partition path still calls in)
     */
    public static String interpolateTemplate(String template, Set<String> specKeys) {
        if (template == null || template.isEmpty()) return "\"\"";
        if (!template.contains("{{") && !template.contains("{%")) {
            return "\"" + escapeJavaString(template) + "\"";
        }
        return "render(\"" + escapeJavaString(template) + "\", jinjaCtx())";
    }
}
