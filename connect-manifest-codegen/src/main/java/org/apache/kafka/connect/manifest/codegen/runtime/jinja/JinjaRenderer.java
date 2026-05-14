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
import java.util.regex.Matcher;
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
        if (!template.contains(".join(")) {
            return template;
        }
        // Match: single- or double-quoted separator literal followed by .join(
        // then use balanced-paren scanner to find the matching ')' so nested calls
        // like ' '.join(day_delta(-7).split('.')[0].split('T')) are handled correctly.
        Pattern p = Pattern.compile("(['\"][^'\"]*['\"])\\.join\\(");
        Matcher m = p.matcher(template);
        StringBuilder sb = new StringBuilder();
        int last = 0;
        while (m.find(last)) {
            sb.append(template, last, m.start());
            String sep = m.group(1);
            int argsStart = m.end(); // index just past the opening '('
            int argsEnd = findMatchingClose(template, argsStart); // index just past the matching ')'
            String joinArg = template.substring(argsStart, argsEnd - 1).trim();
            sb.append("(").append(joinArg).append(")|join(").append(sep).append(")");
            last = argsEnd;
        }
        sb.append(template, last, template.length());
        return sb.toString();
    }

    /**
     * Rewrites {@code now_utc() - duration(X)} to {@code now_utc().minus(duration(X))}.
     *
     * <p>jinjava does not support the binary {@code -} operator between arbitrary Java objects
     * (it only handles numeric subtraction).  {@code now_utc()} returns an
     * {@link AirbyteDateTime} whose {@code minus(TemporalAmount)} method performs the
     * subtraction.  Converting the infix {@code -} to a method call lets jinjava resolve it
     * via normal reflection.</p>
     *
     * <p>The pattern is safe: {@code now_utc() - duration(X)} has no other meaning in Jinja2.</p>
     */
    static String rewriteNowUtcArithmetic(String template) {
        if (!template.contains("now_utc()")) {
            return template;
        }
        Pattern p = Pattern.compile("now_utc\\(\\)\\s*-\\s*(duration\\([^)]+\\))");
        return p.matcher(template).replaceAll("now_utc().minus($1)");
    }

    /**
     * Rewrites {@code format_datetime(dt, fmt[, inputFmt])} to filter form:
     * {@code (dt) | format_datetime_filter(fmt[, inputFmt])}.
     *
     * <p>jinjava/JUEL cannot invoke registered EL functions when the first
     * argument is a {@link String} produced by a method-chain expression
     * (e.g. {@code now_utc().strftime(...)}).  Converting to a Jinja filter avoids
     * JUEL's function-call invocation path: the pipe operator evaluates the left-hand
     * expression as a standalone expression, then jinjava passes its result to the
     * filter via direct Java invocation, bypassing JUEL entirely.</p>
     *
     * <p>The marker {@code "format_datetime("} never matches
     * {@code "format_datetime_filter("} because the marker ends with {@code '('}
     * and the filter name inserts {@code "_filter"} before the {@code '('}.</p>
     */
    static String rewriteFormatDatetime(String template) {
        String marker = "format_datetime(";
        if (!template.contains(marker)) {
            return template;
        }
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (i < template.length()) {
            int idx = template.indexOf(marker, i);
            if (idx < 0) {
                out.append(template, i, template.length());
                break;
            }
            out.append(template, i, idx);
            int argsStart = idx + marker.length();
            int argsEnd = findMatchingClose(template, argsStart);
            // Split at top-level commas: args[0] = dt, args[1..] = format args
            java.util.List<String> args = splitTopLevelArgs(template, argsStart, argsEnd - 1);
            out.append("(").append(args.get(0)).append(") | format_datetime_filter(");
            for (int a = 1; a < args.size(); a++) {
                if (a > 1) {
                    out.append(", ");
                }
                out.append(args.get(a));
            }
            out.append(")");
            i = argsEnd;
        }
        return out.toString();
    }

    /**
     * Splits {@code s[start..end]} at top-level commas (depth 1),
     * respecting nested parens/brackets and single/double-quoted strings.
     */
    private static java.util.List<String> splitTopLevelArgs(String s, int start, int end) {
        java.util.List<String> parts = new java.util.ArrayList<>();
        int depth = 0;
        boolean inSQ = false;
        boolean inDQ = false;
        int segStart = start;
        for (int i = start; i < end; i++) {
            char c = s.charAt(i);
            if (inSQ) {
                if (c == '\'') {
                    inSQ = false;
                }
            } else if (inDQ) {
                if (c == '"') {
                    inDQ = false;
                }
            } else if (c == '\'') {
                inSQ = true;
            } else if (c == '"') {
                inDQ = true;
            } else if (c == '(' || c == '[') {
                depth++;
            } else if (c == ')' || c == ']') {
                depth--;
            } else if (c == ',' && depth == 0) {
                parts.add(s.substring(segStart, i).trim());
                segStart = i + 1;
            }
        }
        parts.add(s.substring(segStart, end).trim());
        return parts;
    }

    /**
     * Rewrites Python-style {@code expr.split('sep')} to jinjava filter form
     * {@code expr|split('sep')}.
     *
     * <p>jinjava dispatches {@code str.split("sep")} as a Java method call
     * {@code String.split(String)} which treats the separator as a regex, causing
     * incorrect splits for characters like {@code '.'} that are regex metacharacters.
     * jinjava's built-in {@code split} filter does literal splitting instead.</p>
     *
     * <p>Processes left-to-right: for each {@code .split('sep')} occurrence, scans
     * backwards in the already-built output to identify the full LHS expression
     * (balanced-paren/bracket-aware, pipe-aware), then replaces with
     * {@code (lhs)|split('sep')} so the result is a jinjava filter chain.</p>
     */
    static String rewriteStrSplit(String template) {
        String marker = ".split(";
        if (!template.contains(marker)) {
            return template;
        }
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (i < template.length()) {
            int idx = template.indexOf(marker, i);
            if (idx < 0) {
                out.append(template, i, template.length());
                break;
            }
            int qPos = idx + marker.length();
            if (qPos >= template.length()) {
                out.append(template, i, template.length());
                break;
            }
            char q = template.charAt(qPos);
            if (q != '\'' && q != '"') {
                // Not a quoted string separator — pass through
                out.append(template, i, idx + 1);
                i = idx + 1;
                continue;
            }
            int closeQ = template.indexOf(q, qPos + 1);
            if (closeQ < 0 || closeQ + 1 >= template.length() || template.charAt(closeQ + 1) != ')') {
                out.append(template, i, idx + 1);
                i = idx + 1;
                continue;
            }
            // Append template up to (not including) the '.' of '.split('
            out.append(template, i, idx);
            // Scan backward in 'out' to find the start of the LHS expression
            int lhsEnd = out.length();
            int lhsStart = scanLhsExprEnd(out, lhsEnd);
            String lhs = out.substring(lhsStart, lhsEnd);
            String sep = template.substring(qPos, closeQ + 1); // includes quotes
            out.setLength(lhsStart);
            i = appendSplitRewrite(out, lhs, sep, template, closeQ + 2);
        }
        return out.toString();
    }

    /**
     * Appends {@code lhs|split(sep)} (or {@code lhs|split(sep)|first} if {@code [0]}
     * follows at {@code nextPos} in the template) to {@code out}, and returns the
     * updated template cursor position.
     *
     * <p>Handles two forms of {@code [0]} that jinjava cannot parse directly:
     * <ul>
     *   <li>Case A — {@code [0]} is already at the end of {@code lhs} (from a prior
     *       iteration that did not consume it).  Replaces it with {@code |first}.</li>
     *   <li>Case B — {@code [0]} follows the closing {@code )} of {@code .split('sep')}
     *       in the template.  Emits {@code |first} and skips past {@code [0]}.</li>
     * </ul>
     */
    private static int appendSplitRewrite(StringBuilder out, String lhs, String sep,
                                          String template, int nextPos) {
        if (lhs.endsWith("[0]")) {
            lhs = lhs.substring(0, lhs.length() - 3) + "|first";
        }
        if (nextPos + 2 < template.length()
                && template.charAt(nextPos) == '['
                && template.charAt(nextPos + 1) == '0'
                && template.charAt(nextPos + 2) == ']') {
            out.append(lhs).append("|split(").append(sep).append(")|first");
            return nextPos + 3;
        }
        out.append(lhs).append("|split(").append(sep).append(")");
        return nextPos;
    }

    /**
     * Scans backward from {@code end} in {@code s} to find the start of the
     * expression that ends at {@code end}. Handles balanced {@code ()}, {@code []},
     * identifier chars, periods, and pipe-filter chains. Stops at unmatched
     * {@code (} or any other non-expression delimiter.
     */
    private static int scanLhsExprEnd(CharSequence s, int end) {
        int i = end - 1;
        while (i >= 0) {
            char c = s.charAt(i);
            if (c == ']') {
                i = scanBackPair(s, i, ']', '[');
            } else if (c == ')') {
                i = scanBackPair(s, i, ')', '(');
            } else if (Character.isLetterOrDigit(c) || c == '_' || c == '.' || c == '|') {
                i--;
            } else {
                break;
            }
        }
        return i + 1;
    }

    /** Scans backward from {@code i} through a matching pair (e.g. {@code ']'} to {@code '['}). */
    private static int scanBackPair(CharSequence s, int i, char close, char open) {
        int depth = 1;
        i--;
        while (i >= 0 && depth > 0) {
            char cc = s.charAt(i);
            if (cc == close) depth++;
            else if (cc == open) depth--;
            i--;
        }
        return i;
    }

    /** Return the index just past the matching ')' for the '(' that opened at {@code start - 1}. */
    private static int findMatchingClose(String s, int start) {
        int depth = 1;
        boolean inSQ = false;
        boolean inDQ = false;
        int i = start;
        while (i < s.length() && depth > 0) {
            char c = s.charAt(i);
            if (inSQ) {
                if (c == '\'') {
                    inSQ = false;
                }
            } else if (inDQ) {
                if (c == '"') {
                    inDQ = false;
                }
            } else if (c == '\'') {
                inSQ = true;
            } else if (c == '"') {
                inDQ = true;
            } else if (c == '(' || c == '[') {
                depth++;
            } else if (c == ')' || c == ']') {
                depth--;
            }
            i++;
        }
        return i;
    }

    /**
     * Rewrites Python-style {@code dict.get('key', default)} to valid Jinja2.
     *
     * <p>Python dicts have a 2-arg {@code get(key, default)} method. jinjava
     * resolves it against Java's {@link java.util.Map#get(Object)} which is
     * single-argument, causing "Cannot find method get with 2 parameters".
     * Rewrite to {@code (dict['key'] if 'key' in dict else default)}.</p>
     *
     * <p>Handles both simple identifiers ({@code ident.get('k', d)}) and
     * complex LHS expressions ending in {@code ]} or {@code )} such as
     * {@code (expr).get('k', d)} — uses backward scanning via
     * {@link #scanLhsExprEnd} in both cases.</p>
     */
    static String rewriteDictGet(String template) {
        if (!template.contains(".get(")) {
            return template;
        }
        StringBuilder sb = new StringBuilder();
        int pos = 0;
        while (pos < template.length()) {
            int dotIdx = template.indexOf(".get(", pos);
            if (dotIdx < 0) {
                sb.append(template, pos, template.length());
                break;
            }
            int[] rewrite = tryRewriteDictGetAt(template, dotIdx, pos);
            if (rewrite == null) {
                sb.append(template, pos, dotIdx + 1);
                pos = dotIdx + 1;
            } else {
                int objStart = rewrite[0];
                int getEnd   = rewrite[1];
                String obj  = template.substring(objStart, dotIdx);
                String key  = template.substring(rewrite[2], rewrite[3]);
                String dflt = template.substring(rewrite[4], rewrite[5]).trim();
                sb.append(template, pos, objStart);
                sb.append("((").append(obj).append("[").append(key).append("]) if ")
                  .append(key).append(" in ").append(obj)
                  .append(" else (").append(dflt).append("))");
                pos = getEnd;
            }
        }
        return sb.toString();
    }

    /**
     * Returns {@code [objStart, getEnd, keyFrom, keyTo, dfltFrom, dfltTo]} describing
     * a {@code obj.get('key', default)} call at {@code dotIdx}, or {@code null} if
     * this is not a 2-arg dict-get pattern.
     */
    private static int[] tryRewriteDictGetAt(String template, int dotIdx, int minObjStart) {
        if (dotIdx <= minObjStart) {
            return null;
        }
        char prev = template.charAt(dotIdx - 1);
        if (!Character.isLetterOrDigit(prev) && prev != '_' && prev != ']' && prev != ')') {
            return null;
        }
        int objStart = scanLhsExprEnd(template, dotIdx);
        if (objStart < minObjStart) {
            return null;
        }
        return parseDictGetArgs(template, dotIdx, objStart);
    }

    private static int[] parseDictGetArgs(String template, int dotIdx, int objStart) {
        int kStart = skipSpaces(template, dotIdx + 5); // skip ".get(" then spaces
        if (kStart >= template.length()) {
            return null;
        }
        char q = template.charAt(kStart);
        if (q != '\'' && q != '"') {
            return null;
        }
        int kEnd = template.indexOf(q, kStart + 1);
        if (kEnd < 0) {
            return null;
        }
        // Require comma — 2-arg form only
        int afterComma = skipSpaces(template, kEnd + 1);
        if (afterComma >= template.length() || template.charAt(afterComma) != ',') {
            return null;
        }
        int dfltStart = skipSpaces(template, afterComma + 1);
        int getEnd = findMatchingClose(template, dfltStart);
        return new int[]{objStart, getEnd, kStart, kEnd + 1, dfltStart, getEnd - 1};
    }

    private static int skipSpaces(String s, int i) {
        while (i < s.length() && s.charAt(i) == ' ') {
            i++;
        }
        return i;
    }

    /**
     * Rewrites Python-style chained comparisons {@code a <= b <= c} to
     * valid Jinja2 {@code a <= b and b <= c}.
     *
     * <p>Python supports chained comparisons; Jinja2 / jinjava do not —
     * the parser raises a syntax error after the second operator.</p>
     */
    static String rewriteChainedComparisons(String template) {
        if (!template.contains("<=") && !template.contains(">=")) {
            return template;
        }
        // Match: number <=|>=|<|> expr <=|>=|<|> number (within {% %} or {{ }})
        Pattern p = Pattern.compile(
            "(-?\\d[\\d.]*(?:[eE][+-]?\\d+)?)" // left numeric literal
            + "\\s*(<=|>=|<|>)\\s*"              // first operator
            + "([^<>={}%]+?)"                    // middle expression (no braces/operators)
            + "\\s*(<=|>=|<|>)\\s*"              // second operator
            + "(-?\\d[\\d.]*(?:[eE][+-]?\\d+)?)"); // right numeric literal
        Matcher m = p.matcher(template);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String left  = m.group(1);
            String op1   = m.group(2);
            String mid   = m.group(3).trim();
            String op2   = m.group(4);
            String right = m.group(5);
            m.appendReplacement(sb, Matcher.quoteReplacement(
                left + " " + op1 + " " + mid + " and " + mid + " " + op2 + " " + right));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    /**
     * Rewrites {@code day_delta(n, format='...')} keyword arg to positional
     * {@code day_delta(n, '...')} since jinjava does not support named args
     * for ELFunctionDefinition varargs methods.
     */
    static String rewriteDayDeltaKeyword(String template) {
        if (!template.contains("day_delta(") || !template.contains("format=")) {
            return template;
        }
        Pattern p = Pattern.compile("day_delta\\(([^,)]+),\\s*format=(['\"][^'\"]+['\"])\\)");
        return p.matcher(template).replaceAll("day_delta($1, $2)");
    }

    /**
     * Wraps bare function calls after {@code else} in ternary expressions in parentheses.
     *
     * <p>jinjava misparses {@code X if COND else func(args)} — it treats the preceding
     * identifier (e.g. "config") as a namespace prefix for the function name.
     * Wrapping the else clause: {@code X if COND else (func(args))} disambiguates.</p>
     */
    static String rewriteElseFunctionCall(String template) {
        if (!template.contains("else ")) {
            return template;
        }
        // Match: "else <word>(" but not inside a nested "else if"
        Pattern p = Pattern.compile("\\belse\\s+((?!if\\b)[a-zA-Z_][a-zA-Z0-9_]*)\\s*\\(");
        Matcher m = p.matcher(template);
        StringBuilder sb = new StringBuilder();
        int lastEnd = 0;
        while (m.find(lastEnd)) {
            sb.append(template, lastEnd, m.start());
            // Find the matching closing ')' using a depth counter
            int depth = 1;
            int i = m.end();
            while (i < template.length() && depth > 0) {
                char c = template.charAt(i);
                if (c == '(') depth++;
                else if (c == ')') depth--;
                i++;
            }
            // Wrap: "else funcName(args)" → "else (funcName(args))"
            sb.append("else (").append(m.group(1)).append("(");
            sb.append(template, m.end(), i); // args + closing ')'
            sb.append(")");
            lastEnd = i;
        }
        sb.append(template, lastEnd, template.length());
        return sb.toString();
    }

    /**
     * Rewrites Python-style string slices to {@code | pyslice(start[, end])}.
     *
     * <p>Handles the two forms used by manifests:
     * <ul>
     *   <li>{@code expr[:-N]} → {@code expr|pyslice(0,-N)} (all but last N chars)</li>
     *   <li>{@code expr[-N:]} → {@code expr|pyslice(-N)} (last N chars)</li>
     * </ul>
     * Both forms require the LHS to be a dotted-word expression (e.g.
     * {@code stream_slice.start_time}); complex subscripted LHS is not handled
     * here — those are covered by individual connector fixes.</p>
     */
    static String rewritePythonSlice(String template) {
        if (!template.contains("[:-") && !template.contains("[-")) {
            return template;
        }
        // [:-N] form — all but last N chars
        template = Pattern.compile("([\\w.]+)\\[:-([0-9]+)\\]")
            .matcher(template)
            .replaceAll(mr -> mr.group(1) + "|pyslice(0,-" + mr.group(2) + ")");
        // [-N:] form — last N chars
        template = Pattern.compile("([\\w.]+)\\[-([0-9]+):\\]")
            .matcher(template)
            .replaceAll(mr -> mr.group(1) + "|pyslice(-" + mr.group(2) + ")");
        return template;
    }

    private JinjaRenderer() {
    }

    /**
     * Mirrors the Airbyte Python CDK's {@code InterpolatedString} behaviour: after Jinja
     * rendering, if the entire result string is wrapped in matching single or double quotes,
     * strip the outer quote characters.
     *
     * <p>Airbyte manifests use this idiom to protect special characters (brackets, commas)
     * from YAML/Jinja interpretation. For example:
     * {@code "'[{{stream_slice['start_time']}}, {{stream_slice['end_time']}}]'"}
     * renders to {@code '[1704067200, 1706745600]'} and the Python CDK (via
     * {@code ast.literal_eval}) strips the outer single quotes to produce
     * {@code [1704067200, 1706745600]} as the actual query-parameter value.
     */
    static String stripOuterQuotes(String s) {
        if (s == null || s.length() < 2) return s;
        char first = s.charAt(0);
        char last = s.charAt(s.length() - 1);
        if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
            return s.substring(1, s.length() - 1);
        }
        return s;
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
        template = preprocess(template);
        RenderResult result = JINJAVA.renderForResult(template, asObjectMap(context));
        if (!result.getErrors().isEmpty()) {
            TemplateError first = result.getErrors().get(0);
            Exception cause = first.getException();
            String detail = first.getMessage();
            if (cause != null) {
                detail += " | cause: " + cause.getClass().getSimpleName() + ": " + cause.getMessage();
                if (cause.getCause() != null) {
                    detail += " | root: " + cause.getCause().getClass().getSimpleName() + ": " + cause.getCause().getMessage();
                }
            }
            throw new ConnectException(
                "Jinja render failed for template '" + template + "': " + detail);
        }
        return stripOuterQuotes(result.getOutput());
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
        template = preprocess(template);
        return stripOuterQuotes(JINJAVA.renderForResult(template, asObjectMap(context)).getOutput());
    }

    // Each rewrite function handles its own "nothing to do" early-return, so
    // preprocess can call them unconditionally — no NPath explosion from guards.
    static String preprocess(String template) {
        template = rewritePythonJoin(template);
        template = rewriteStrSplit(template);
        template = rewritePythonSlice(template);
        template = rewriteNowUtcArithmetic(template);
        template = rewriteFormatDatetime(template);
        template = rewriteDictGet(template);
        template = rewriteChainedComparisons(template);
        template = rewriteDayDeltaKeyword(template);
        template = rewriteElseFunctionCall(template);
        return template;
    }

    /**
     * Returns {@code true} when {@code s} contains Jinja interpolation
     * ({@code "{{ ... }}"}) or a statement block ({@code "{% ... %}"}).
     * Used by callers that want to skip Jinja overhead for plain literals.
     */
    public static boolean hasJinjaSyntax(String s) {
        return s != null && (s.indexOf("{{") >= 0 || s.indexOf("{%") >= 0);
    }

    /**
     * Collapses double-slashes in the path portion of a URL that may arise when a
     * Jinja expression evaluates to a value that already begins with {@code '/'} but
     * the template string also has a preceding separator slash.
     *
     * <p>Only the portion after {@code "://"} is normalised, so the scheme separator
     * (e.g. {@code "https://"}) is never touched.</p>
     */
    public static String normalizeUrl(String url) {
        if (url == null) return url;
        int sep = url.indexOf("://");
        if (sep < 0) return url;
        String path = url.substring(sep + 3);
        if (!path.contains("//")) return url;
        return url.substring(0, sep + 3) + path.replace("//", "/");
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
