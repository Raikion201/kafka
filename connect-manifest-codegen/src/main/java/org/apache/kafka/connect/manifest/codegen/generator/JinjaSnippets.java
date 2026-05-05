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

import org.apache.kafka.connect.manifest.codegen.model.ManifestSpec;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pattern-matching registry for the Jinja2 snippet vocabulary used by Airbyte manifests.
 *
 * <p>Rather than parsing or translating Jinja2 in full, we recognise a fixed set of
 * expression shapes that appear in practice across the Airbyte connector catalog and
 * convert each to the equivalent Java expression at codegen time (Option-3 approach).
 *
 * <p>Both the static-stream path ({@link TaskGenerator}) and the dynamic-stream path
 * ({@link DynamicStreamTaskBody}) share these constants and helpers so that new snippet
 * shapes need to be added in exactly one place.
 */
public final class JinjaSnippets {

    // -------------------------------------------------------------------------
    // Recognised Jinja2 expression patterns
    // -------------------------------------------------------------------------

    /** {@code {{ config['key'] }}} or {@code {{ config["key"] }}} (optional {@code or default}). */
    public static final Pattern CONFIG_TEMPLATE = Pattern.compile(
        "\\{\\{\\s*config\\[['\"]([^'\"]+)['\"]\\](?:\\s+or\\s+[^}]+)?\\s*\\}\\}");

    /** {@code config['key']} or {@code config["key"]} bare inside a larger Jinja expression. */
    public static final Pattern CONFIG_KEY_IN_EXPR = Pattern.compile("config\\[['\"]([^'\"]+)['\"]\\]");

    /** {@code {{ config.key }}} dot-notation (optional {@code or default}); excludes method calls. */
    public static final Pattern CONFIG_DOT_TEMPLATE = Pattern.compile(
        "\\{\\{\\s*config\\.([a-zA-Z_]\\w*)(?!\\()(?:\\s+or\\s+[^}]+)?\\s*\\}\\}");

    /** {@code config.key} dot-notation bare inside a larger expression; excludes method calls. */
    public static final Pattern CONFIG_DOT_KEY_IN_EXPR = Pattern.compile("config\\.([a-zA-Z_]\\w*)(?!\\()");

    /** {@code config.get('key', default)} — Python-dict accessor used in many Airbyte manifests. */
    public static final Pattern CONFIG_GET_CALL =
        Pattern.compile("config\\.get\\(\\s*['\"]([^'\"]+)['\"]");

    /** Either bracket or dot form; used for multi-token interpolation in mixed URL templates. */
    public static final Pattern CONFIG_ANY_INTERPOLATION = Pattern.compile(
        "\\{\\{\\s*config(?:\\[['\"]([^'\"]+)['\"]\\]|\\.([a-zA-Z_]\\w*))(?:\\s+or\\s+[^}]+)?\\s*\\}\\}");

    /** {@code {{ stream_partition.fieldName }}} references in child-stream URL paths. */
    public static final Pattern STREAM_PARTITION_RE =
        Pattern.compile("\\{\\{\\s*stream_partition\\.(\\w+)\\s*\\}\\}");

    /** Any {@code {{ ... }}} block — used to enumerate all Jinja expressions in a string. */
    private static final Pattern ANY_EXPR = Pattern.compile("\\{\\{[^}]*\\}\\}");

    /** Sentinel returned when no config key can be extracted from a template string. */
    public static final String UNRESOLVED_GETTER = "__unresolved__";

    private JinjaSnippets() {
    }

    // -------------------------------------------------------------------------
    // Stateless helpers
    // -------------------------------------------------------------------------

    /**
     * Returns the first config key referenced in {@code template} via bracket or dot notation,
     * or {@code null} if the template contains no recognisable config reference.
     */
    public static String extractConfigKey(String template) {
        if (template == null) return null;
        Matcher m = CONFIG_TEMPLATE.matcher(template);
        if (m.find()) return m.group(1);
        Matcher d = CONFIG_DOT_TEMPLATE.matcher(template);
        if (d.find()) return d.group(1);
        return null;
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
     * strings is a recognised snippet (a config reference, a stream_partition reference, or
     * a config.get call). Returns {@code false} when at least one expression is unrecognised,
     * meaning the manifest needs a new matcher before it can be fully generated.
     *
     * <p>This is intended as a soft gate: callers can use it to determine whether a manifest's
     * templates all fall within the known vocabulary before committing to generating a full task.
     */
    public static boolean allTemplatesRecognized(Iterable<String> templateStrings) {
        for (String s : templateStrings) {
            if (s == null) continue;
            Matcher any = ANY_EXPR.matcher(s);
            while (any.find()) {
                String expr = any.group();
                if (!CONFIG_TEMPLATE.matcher(expr).find()
                        && !CONFIG_DOT_TEMPLATE.matcher(expr).find()
                        && !CONFIG_GET_CALL.matcher(expr).find()
                        && !STREAM_PARTITION_RE.matcher(expr).find()) {
                    return false;
                }
            }
        }
        return true;
    }

    // -------------------------------------------------------------------------
    // spec-aware helpers (require the set of declared config property keys)
    // -------------------------------------------------------------------------

    /**
     * Resolves a Jinja template to a Java getter name like {@code "getApiKey"}, or
     * {@link #UNRESOLVED_GETTER} when the key cannot be extracted or is not in {@code specKeys}.
     */
    public static String resolveConfigGetter(String template, Set<String> specKeys) {
        if (template == null) return UNRESOLVED_GETTER;
        String key = null;
        Matcher m = CONFIG_TEMPLATE.matcher(template.trim());
        if (m.matches()) key = m.group(1);
        if (key == null) {
            Matcher md = CONFIG_DOT_TEMPLATE.matcher(template.trim());
            if (md.matches()) key = md.group(1);
        }
        if (key == null) {
            Matcher partial = CONFIG_TEMPLATE.matcher(template);
            if (partial.find()) key = partial.group(1);
        }
        if (key == null) {
            Matcher partialDot = CONFIG_DOT_TEMPLATE.matcher(template);
            if (partialDot.find()) key = partialDot.group(1);
        }
        if (key == null) {
            Matcher getCall = CONFIG_GET_CALL.matcher(template);
            if (getCall.find()) key = getCall.group(1);
        }
        if (key == null) return UNRESOLVED_GETTER;
        if (!specKeys.contains(key)) return UNRESOLVED_GETTER;
        return "get" + ManifestSpec.toClassName(key);
    }

    /**
     * Like {@link #resolveConfigGetter} but also matches bare {@code config['key']} or
     * {@code config.key} inside complex Jinja expressions such as
     * {@code format_datetime(config['since'], ...)}.
     */
    public static String resolveConfigGetterLoose(String expr, Set<String> specKeys) {
        if (expr == null) return UNRESOLVED_GETTER;
        String getter = resolveConfigGetter(expr, specKeys);
        if (!UNRESOLVED_GETTER.equals(getter)) return getter;
        String key = null;
        Matcher m = CONFIG_KEY_IN_EXPR.matcher(expr);
        if (m.find()) key = m.group(1);
        if (key == null) {
            Matcher d = CONFIG_DOT_KEY_IN_EXPR.matcher(expr);
            if (d.find()) key = d.group(1);
        }
        if (key == null) {
            Matcher gc = CONFIG_GET_CALL.matcher(expr);
            if (gc.find()) key = gc.group(1);
        }
        if (key == null) return UNRESOLVED_GETTER;
        if (!specKeys.contains(key)) return UNRESOLVED_GETTER;
        return "get" + ManifestSpec.toClassName(key);
    }

    /**
     * Returns a Java expression for a credential value (username or password template).
     * Emits a plain string literal for non-Jinja templates and a runtime
     * {@code render(...)} call for anything containing Jinja syntax. The
     * generated SourceTask supplies {@code render} as a static import and
     * {@code jinjaCtx()} as a no-arg method building the context map.
     */
    public static String resolveCredentialExpr(String template, Set<String> specKeys) {
        return interpolateTemplate(template, specKeys);
    }

    /**
     * Returns a Java expression that yields the rendered value of {@code template}
     * at runtime. Plain literals (no {@code "{{"} or {@code "{%"}) collapse to a
     * Java string literal; everything else becomes
     * {@code render("template", jinjaCtx())}, deferring Jinja semantics to
     * {@code JinjaRenderer} so manifests get exact Airbyte CDK behaviour
     * (filters, functions, conditionals) rather than the legacy regex subset.
     */
    public static String interpolateTemplate(String template, Set<String> specKeys) {
        if (template == null || template.isEmpty()) return "\"\"";
        if (!template.contains("{{") && !template.contains("{%")) {
            return "\"" + escapeJavaString(template) + "\"";
        }
        return "render(\"" + escapeJavaString(template) + "\", jinjaCtx())";
    }
}
