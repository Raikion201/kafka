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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Smoke test: every Jinja expression that appears between {{ … }} or {% … %}
 * inside the manifest corpus must at least parse without error. Some
 * evaluations are skipped because they reference undefined variables —
 * parseability is the contract here, not full evaluation.
 */
class CorpusJinjaParseTest {

    private static final Path[] CORPUS_ROOTS = {
        Paths.get("src/test/resources/manifests"),
        Paths.get("src/main/manifests")
    };

    // Unused now (we walk the source by hand to track {{…}} with nested braces),
    // but kept for documentation of the rough shape we're extracting.
    @SuppressWarnings("unused")
    private static final Pattern EXPR = Pattern.compile("\\{\\{(.+?)\\}\\}|\\{%(.+?)%\\}", Pattern.DOTALL);

    @Test
    void everyCorpusExpressionParses() throws IOException {
        Set<String> exprs = new LinkedHashSet<>();
        for (Path root : CORPUS_ROOTS) {
            if (!Files.isDirectory(root)) {
                continue;
            }
            try (Stream<Path> files = Files.walk(root)) {
                files.filter(p -> p.toString().endsWith(".yaml"))
                    .forEach(p -> extractExpressions(p, exprs));
            }
        }
        assertTrue(exprs.size() > 100,
            "expected >100 unique Jinja expressions in the corpus, found " + exprs.size());

        List<String> failures = new java.util.ArrayList<>();
        int total = 0;
        for (String e : exprs) {
            total++;
            String trimmed = e.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                continue;
            }
            // Statement bodies (`if`, `for`, …) are header lines, not expressions —
            // they require a full template parse with paired {% endX %}, which we
            // exercise separately in TemplateParserTest. Here we focus on {{ … }}.
            if (looksLikeStatement(trimmed)) {
                continue;
            }
            if (KNOWN_MALFORMED.contains(trimmed)) {
                continue;
            }
            // Whitespace-control markers ({%- … -%}) leave residue like "- foo -"
            // that is not syntactically an expression. Strip before parsing.
            String parseable = stripWsMarkers(trimmed);
            try {
                Parser.parseExpression(parseable);
            } catch (RuntimeException ex) {
                failures.add(trimmed + "  ⇒ " + ex.getMessage());
            }
        }
        if (!failures.isEmpty()) {
            int show = Math.min(failures.size(), 25);
            StringBuilder msg = new StringBuilder();
            msg.append(failures.size()).append('/').append(total)
               .append(" corpus expressions failed to parse. First ")
               .append(show).append(":\n");
            for (int i = 0; i < show; i++) {
                msg.append("  ").append(failures.get(i)).append('\n');
            }
            fail(msg.toString());
        }
    }

    private static final Set<String> STATEMENT_KEYWORDS = Set.of(
        "else", "endif", "endfor", "raw", "endraw");

    private static final List<String> STATEMENT_PREFIXES = List.of(
        "if ", "elif ", "for ", "set ");

    /**
     * Corpus expressions known to be malformed in the upstream manifest. We
     * keep them in the corpus (so the test still walks them) but allowlist
     * them by exact body so the parse-rate gate stays at 100%.
     */
    private static final Set<String> KNOWN_MALFORMED = Set.of(
        // src/test/resources/manifests/source-ticktick.yaml — stray ')' before }}
        "config.authorization.client_secret)"
    );

    private static boolean looksLikeStatement(String s) {
        // Strip whitespace-control markers ({%- … -%}) before classifying.
        String stripped = stripWsMarkers(s);
        if (STATEMENT_KEYWORDS.contains(stripped)) {
            return true;
        }
        for (String p : STATEMENT_PREFIXES) {
            if (stripped.startsWith(p)) {
                return true;
            }
        }
        return false;
    }

    private static String stripWsMarkers(String s) {
        String t = s;
        if (t.startsWith("- ")) {
            t = t.substring(2);
        } else if (t.startsWith("-")) {
            t = t.substring(1);
        }
        if (t.endsWith(" -")) {
            t = t.substring(0, t.length() - 2);
        } else if (t.endsWith("-")) {
            t = t.substring(0, t.length() - 1);
        }
        return t.trim();
    }

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    private static void extractExpressions(Path p, Set<String> out) {
        try {
            JsonNode root = YAML.readTree(p.toFile());
            walk(root, out);
        } catch (IOException ignored) {
            // some fixtures aren't valid YAML; skip
        }
    }

    private static void walk(JsonNode node, Set<String> out) {
        if (node == null) {
            return;
        }
        if (node.isTextual()) {
            collectFromSource(node.textValue(), out);
            return;
        }
        if (node.isArray()) {
            for (JsonNode child : node) {
                walk(child, out);
            }
            return;
        }
        if (node.isObject()) {
            Iterator<String> names = node.fieldNames();
            while (names.hasNext()) {
                walk(node.get(names.next()), out);
            }
        }
    }

    private static void collectFromSource(String src, Set<String> out) {
        int i = 0;
        while (i < src.length() - 1) {
            char c = src.charAt(i);
            char n = src.charAt(i + 1);
            if (c == '{' && (n == '{' || n == '%')) {
                String close = n == '{' ? "}}" : "%}";
                int end = findClose(src, i + 2, close);
                if (end < 0) {
                    return;
                }
                String body = src.substring(i + 2, end);
                out.add(body);
                // Recurse: bodies may contain nested {{ … }} (e.g.
                // `{{ '{{ inner }}' }}`) — collect those inner expressions too.
                collectFromSource(body, out);
                i = end + 2;
            } else {
                i++;
            }
        }
    }

    /**
     * Walk forward to the matching closing delimiter while ignoring quoted
     * strings (so {@code "}}"} inside a quoted literal does not terminate the
     * block) and tracking inner brace depth (so Python dict literals nested
     * inside {@code {{ … }}} do not confuse a lazy match).
     */
    private static int findClose(String src, int start, String close) {
        int depth = 0;
        char quote = 0;
        for (int i = start; i < src.length() - 1; i++) {
            char c = src.charAt(i);
            if (quote != 0) {
                if (c == '\\' && i + 1 < src.length()) {
                    i++;
                } else if (c == quote) {
                    quote = 0;
                }
                continue;
            }
            if (c == '\'' || c == '"') {
                quote = c;
                continue;
            }
            if (c == '{') {
                depth++;
            } else if (c == '}' && depth > 0) {
                depth--;
            } else if (depth == 0 && src.startsWith(close, i)) {
                return i;
            }
        }
        return -1;
    }

}
