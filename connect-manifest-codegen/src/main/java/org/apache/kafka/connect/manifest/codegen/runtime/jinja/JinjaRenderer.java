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

import org.apache.kafka.connect.errors.ConnectException;

import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.JinjavaConfig;
import com.hubspot.jinjava.interpret.RenderResult;
import com.hubspot.jinjava.interpret.TemplateError;

import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Thin wrapper over {@link com.hubspot.jinjava.Jinjava} that exposes a single
 * {@link #render(String, Map)} entry point and registers Airbyte's CDK
 * filter / function set on the global context.
 *
 * <p>This is the only place generated source-task code (and codegen helpers)
 * touch a Jinja engine. Manifests use Python Jinja2 semantics; jinjava is
 * Apache-2.0 Java implementation of that surface and is the runtime engine
 * after the swap from the hand-rolled interpreter.</p>
 *
 * <p>Two render paths:</p>
 * <ul>
 *   <li>{@link #render(String, Map)} — strict; throws {@link ConnectException}
 *       if jinjava reports any error. Use this for credential / URL templates
 *       where a silent fallback would mask broken configuration.</li>
 *   <li>{@link #renderLenient(String, Map)} — best-effort; returns whatever
 *       jinjava produces even if errors were collected. Mirrors the legacy
 *       {@code JinjaSnippets.interpolateTemplate} behaviour where unresolvable
 *       references silently render as empty strings.</li>
 * </ul>
 *
 * <p>Templates with no Jinja syntax (no {@code "{{"} or {@code "{%"}) are
 * returned verbatim — zero-cost short-circuit for plain literals.</p>
 */
public final class JinjaRenderer {

    private static final Jinjava JINJAVA = build();

    /**
     * Rewrites Python-style {@code 'sep'.join(expr)} to Jinja2 {@code (expr)|join('sep')}.
     *
     * <p>Python allows {@code str.join(iterable)} as an instance method.  Jinja2 does not —
     * it uses the {@code join} filter: {@code iterable|join('sep')}.  Several Airbyte manifests
     * (gnews, news-api, newsdata) use the Python form.  jinjava resolves the call as a static
     * {@code String.join(delimiter, elements)} invocation, but argument resolution fails → NPE.</p>
     *
     * <p>The rewrite is safe: {@code 'x'.join(y)} has no valid Jinja2 meaning other than this
     * Python idiom, so there is no risk of false positives.</p>
     */
    static String rewritePythonJoin(String template) {
        // Match: single- or double-quoted separator literal followed by .join(expr)
        // Group 1: quoted separator, Group 2: join argument (up to closing paren, non-nested)
        Pattern p = Pattern.compile("(['\"][^'\"]*['\"])\\.join\\(([^)]+)\\)");
        return p.matcher(template).replaceAll("($2)|join($1)");
    }

    private JinjaRenderer() {
    }

    /**
     * Render {@code template} against {@code context}. Throws if jinjava
     * collected any error during rendering.
     */
    public static String render(String template, Map<String, ?> context) {
        if (template == null) {
            return "";
        }
        if (!hasJinjaSyntax(template)) {
            return template;
        }
        if (template.contains(".join(")) {
            template = rewritePythonJoin(template);
        }
        RenderResult result = JINJAVA.renderForResult(template, asObjectMap(context));
        if (!result.getErrors().isEmpty()) {
            TemplateError first = result.getErrors().get(0);
            throw new ConnectException(
                "Jinja render failed for template '" + template + "': " + first.getMessage());
        }
        return result.getOutput();
    }

    /**
     * Render {@code template} but never throw — returns whatever jinjava
     * produced even if errors were collected. Use for best-effort rendering
     * where empty strings on missing references is acceptable behaviour.
     */
    public static String renderLenient(String template, Map<String, ?> context) {
        if (template == null) {
            return "";
        }
        if (!hasJinjaSyntax(template)) {
            return template;
        }
        if (template.contains(".join(")) {
            template = rewritePythonJoin(template);
        }
        return JINJAVA.renderForResult(template, asObjectMap(context)).getOutput();
    }

    /**
     * Returns {@code true} when {@code s} contains Jinja interpolation
     * ({@code "{{ ... }}"}) or a statement block ({@code "{% ... %}"}).
     * Used by callers that want to skip Jinja overhead for plain literals.
     */
    public static boolean hasJinjaSyntax(String s) {
        return s != null && (s.indexOf("{{") >= 0 || s.indexOf("{%") >= 0);
    }

    /** Exposed for filter / function registration tests. */
    public static Jinjava jinjava() {
        return JINJAVA;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asObjectMap(Map<String, ?> context) {
        if (context == null) {
            return Collections.emptyMap();
        }
        return (Map<String, Object>) context;
    }

    private static Jinjava build() {
        // jinjava defaults match Python Jinja2 closely enough for Airbyte
        // manifests: undefined variables render as empty strings (not errors),
        // autoescape is off, and trim/lstrip block defaults are unset. Nested
        // interpretation is enabled so a few manifests that emit templates
        // from inside templates (rare) keep working.
        JinjavaConfig cfg = JinjavaConfig.newBuilder()
            .withFailOnUnknownTokens(false)
            .withNestedInterpretationEnabled(true)
            .build();
        Jinjava j = new Jinjava(cfg);
        AirbyteJinjaFunctions.registerAll(j);
        AirbyteJinjaFilters.registerAll(j);
        return j;
    }
}
