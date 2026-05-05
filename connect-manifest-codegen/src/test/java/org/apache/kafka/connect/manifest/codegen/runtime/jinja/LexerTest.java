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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LexerTest {

    private static List<Token.Type> types(String src) {
        return Lexer.tokenize(src).stream().map(t -> t.type).toList();
    }

    private static String text(String src, int idx) {
        return Lexer.tokenize(src).get(idx).text;
    }

    @Test
    void emptyAndPlainText() {
        assertEquals(List.of(Token.Type.EOF), types(""));
        assertEquals(List.of(Token.Type.RAW_TEXT, Token.Type.EOF), types("hello"));
        assertEquals("hello", text("hello", 0));
    }

    @Test
    void simpleConfigInterp() {
        List<Token> t = Lexer.tokenize("{{ config['api_key'] }}");
        assertEquals(List.of(
            Token.Type.LSTACHE,
            Token.Type.IDENT,    // config
            Token.Type.LBRACK,
            Token.Type.STRING,   // api_key
            Token.Type.RBRACK,
            Token.Type.RSTACHE,
            Token.Type.EOF
        ), t.stream().map(x -> x.type).toList());
        assertEquals("api_key", t.get(3).text);
    }

    @Test
    void doubleQuotedAndDotAccess() {
        List<Token.Type> t = types("{{ config[\"start_date\"] or config.end_date }}");
        assertEquals(List.of(
            Token.Type.LSTACHE,
            Token.Type.IDENT, Token.Type.LBRACK, Token.Type.STRING, Token.Type.RBRACK,
            Token.Type.OR,
            Token.Type.IDENT, Token.Type.DOT, Token.Type.IDENT,
            Token.Type.RSTACHE, Token.Type.EOF
        ), t);
    }

    @Test
    void allOperators() {
        List<Token.Type> t = types("{{ a == b != c <= d >= e < f > g + h - i * j / k % l ~ m }}");
        assertTrue(t.contains(Token.Type.EQ));
        assertTrue(t.contains(Token.Type.NEQ));
        assertTrue(t.contains(Token.Type.LE));
        assertTrue(t.contains(Token.Type.GE));
        assertTrue(t.contains(Token.Type.LT));
        assertTrue(t.contains(Token.Type.GT));
        assertTrue(t.contains(Token.Type.PLUS));
        assertTrue(t.contains(Token.Type.MINUS));
        assertTrue(t.contains(Token.Type.STAR));
        assertTrue(t.contains(Token.Type.SLASH));
        assertTrue(t.contains(Token.Type.PERCENT));
        assertTrue(t.contains(Token.Type.TILDE));
    }

    @Test
    void stringEscapes() {
        List<Token> t = Lexer.tokenize("{{ 'a\\'b' }}");
        assertEquals("a'b", t.get(1).text);
        t = Lexer.tokenize("{{ \"line\\nbreak\" }}");
        assertEquals("line\nbreak", t.get(1).text);
    }

    @Test
    void integersAndFloats() {
        List<Token> t = Lexer.tokenize("{{ 42 + 3.14 }}");
        assertEquals(Token.Type.INTEGER, t.get(1).type);
        assertEquals("42", t.get(1).text);
        assertEquals(Token.Type.FLOAT, t.get(3).type);
        assertEquals("3.14", t.get(3).text);
    }

    @Test
    void scientificFloat() {
        List<Token> t = Lexer.tokenize("{{ 1e3 + 2.5e-2 }}");
        assertEquals(Token.Type.FLOAT, t.get(1).type);
        assertEquals(Token.Type.FLOAT, t.get(3).type);
    }

    @Test
    void keywordsAndLiterals() {
        List<Token.Type> t = types("{{ true and false or none }}");
        assertEquals(List.of(
            Token.Type.LSTACHE,
            Token.Type.TRUE, Token.Type.AND, Token.Type.FALSE, Token.Type.OR, Token.Type.NONE,
            Token.Type.RSTACHE, Token.Type.EOF
        ), t);
        // capitalised forms
        assertEquals(Token.Type.TRUE, Lexer.tokenize("{{ True }}").get(1).type);
        assertEquals(Token.Type.FALSE, Lexer.tokenize("{{ False }}").get(1).type);
        assertEquals(Token.Type.NONE, Lexer.tokenize("{{ None }}").get(1).type);
    }

    @Test
    void isAndInTests() {
        List<Token.Type> t = types("{{ x is not none and y in z }}");
        assertTrue(t.contains(Token.Type.IS));
        assertTrue(t.contains(Token.Type.NOT));
        assertTrue(t.contains(Token.Type.NONE));
        assertTrue(t.contains(Token.Type.AND));
        assertTrue(t.contains(Token.Type.IN));
    }

    @Test
    void filterPipeAndCall() {
        List<Token.Type> t = types("{{ v | replace('%', '') | float }}");
        long pipes = t.stream().filter(x -> x == Token.Type.PIPE).count();
        assertEquals(2, pipes);
        assertTrue(t.contains(Token.Type.LPAREN));
        assertTrue(t.contains(Token.Type.STRING));
    }

    @Test
    void statementIfElseEndif() {
        List<Token.Type> t = types("{% if a %}x{% else %}y{% endif %}");
        assertEquals(List.of(
            Token.Type.LSTMT, Token.Type.IF, Token.Type.IDENT, Token.Type.RSTMT,
            Token.Type.RAW_TEXT,
            Token.Type.LSTMT, Token.Type.ELSE, Token.Type.RSTMT,
            Token.Type.RAW_TEXT,
            Token.Type.LSTMT, Token.Type.ENDIF, Token.Type.RSTMT,
            Token.Type.EOF
        ), t);
    }

    @Test
    void statementForEndfor() {
        List<Token.Type> t = types("{% for i in xs %}{{ i }}{% endfor %}");
        assertEquals(List.of(
            Token.Type.LSTMT, Token.Type.FOR, Token.Type.IDENT, Token.Type.IN, Token.Type.IDENT, Token.Type.RSTMT,
            Token.Type.LSTACHE, Token.Type.IDENT, Token.Type.RSTACHE,
            Token.Type.LSTMT, Token.Type.ENDFOR, Token.Type.RSTMT,
            Token.Type.EOF
        ), t);
    }

    @Test
    void statementSet() {
        List<Token.Type> t = types("{% set x = 1 %}");
        assertEquals(List.of(
            Token.Type.LSTMT, Token.Type.SET, Token.Type.IDENT, Token.Type.ASSIGN, Token.Type.INTEGER,
            Token.Type.RSTMT, Token.Type.EOF
        ), t);
    }

    @Test
    void rawBlockEmitsBodyVerbatim() {
        List<Token> t = Lexer.tokenize("a{% raw %}{{ not interpolated }}{% endraw %}b");
        // expected: RAW_TEXT(a), LSTMT, RAW(raw), RSTMT, RAW_TEXT({{ not interpolated }}), LSTMT, ENDRAW, RSTMT, RAW_TEXT(b)
        assertEquals(Token.Type.RAW_TEXT, t.get(0).type);
        assertEquals("a", t.get(0).text);
        assertEquals(Token.Type.RAW, t.get(2).type);
        assertEquals(Token.Type.RAW_TEXT, t.get(4).type);
        assertEquals("{{ not interpolated }}", t.get(4).text);
        assertEquals(Token.Type.ENDRAW, t.get(6).type);
        assertEquals("b", t.get(8).text);
    }

    @Test
    void unterminatedRawBlockThrows() {
        assertThrows(JinjaException.class, () -> Lexer.tokenize("{% raw %}forever"));
    }

    @Test
    void unterminatedExpressionThrows() {
        assertThrows(JinjaException.class, () -> Lexer.tokenize("{{ never closed"));
    }

    @Test
    void whitespaceControlOpenAndClose() {
        // {{- expr -}} trims surrounding whitespace
        List<Token> t = Lexer.tokenize("a   {{- expr -}}   b");
        // RAW_TEXT("a") -- LSTACHE -- IDENT -- RSTACHE -- RAW_TEXT(rest after trim)
        assertEquals(Token.Type.RAW_TEXT, t.get(0).type);
        assertEquals("a", t.get(0).text);
        assertEquals(Token.Type.LSTACHE, t.get(1).type);
        assertEquals(Token.Type.IDENT, t.get(2).type);
        assertEquals("expr", t.get(2).text);
        assertEquals(Token.Type.RSTACHE, t.get(3).type);
        assertEquals(Token.Type.RAW_TEXT, t.get(4).type);
        assertEquals("b", t.get(4).text);
    }

    @Test
    void unicodeIdentifiers() {
        // Jinja allows non-ASCII identifiers; we follow Java's isLetter
        List<Token> t = Lexer.tokenize("{{ café }}");
        assertEquals(Token.Type.IDENT, t.get(1).type);
        assertEquals("café", t.get(1).text);
    }

    @Test
    void corpusComplexExpressionsTokenizeWithoutError() {
        // Sample of the most complex expressions seen in the manifest corpus.
        String[] samples = {
            "{{ now_utc().strftime('%Y-%m-%dT%H:%M:%SZ') }}",
            "{{ stream_slice.cursor_slice.end_time }}",
            "{{ next_page_token['next_page_token'] or '0' }}",
            "{{ v | replace('%', '') | float if v is not none and v != \"\" else None }}",
            "{{ format_datetime( max(config.get('replication_start_date',"
                + "(now_utc() - duration('P730D')).strftime('%Y-%m-%dT%H:%M:%SZ') ),"
                + "(now_utc() - duration('P730D')).strftime('%Y-%m-%dT%H:%M:%SZ')),"
                + "'%Y-%m-%dT%H:%M:%SZ') }}",
            "{% if config[\"spreadsheet_id\"] | regex_search(\"^(https://.*)\") %}x{% endif %}",
            "{% for key, value in cohort.items() if key != 'enabled' %}x{% endfor %}",
            "{{ (now_utc() - duration('P' ~ ((now_utc().weekday() + 1) % 7 + 7) ~ 'D'))"
                + ".strftime('%Y-%m-%dT00:00:00Z') }}",
            "{{ headers['link']['next']['url'] }}",
            "{{ 'next' not in headers['link'] }}"
        };
        for (String s : samples) {
            List<Token> t = Lexer.tokenize(s);
            // last token is EOF
            assertEquals(Token.Type.EOF, t.get(t.size() - 1).type, "EOF missing for: " + s);
            // first non-text token after any prefix RAW_TEXT is LSTACHE or LSTMT
            int j = 0;
            while (j < t.size() && t.get(j).type == Token.Type.RAW_TEXT) j++;
            Token.Type k = t.get(j).type;
            assertTrue(k == Token.Type.LSTACHE || k == Token.Type.LSTMT,
                "expected interp/stmt opener for: " + s);
        }
    }
}
