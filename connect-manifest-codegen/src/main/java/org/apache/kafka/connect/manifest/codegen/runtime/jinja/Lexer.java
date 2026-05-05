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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tokenises a Jinja2 template string.
 *
 * <p>The lexer operates in two modes:
 * <ul>
 *   <li><b>TEXT</b>: emits a {@link Token.Type#RAW_TEXT} run, switching to
 *       EXPR mode at {@code {{} or STMT mode at {@code {%}.</li>
 *   <li><b>EXPR/STMT</b>: emits structured tokens for identifiers, numbers,
 *       strings, operators and keywords until the matching {@code }}} or
 *       {@code %}} closer.</li>
 * </ul>
 *
 * <p>Whitespace control variants ({@code {{- }}, {%- %}, -%}}) trim adjacent
 * raw text and are reported as the regular open/close tokens.
 *
 * <p>{@code {% raw %} ... {% endraw %}} blocks are handled at lex time:
 * the body between the two tags is emitted as a single {@code RAW_TEXT}
 * token without further interpolation.
 */
public final class Lexer {

    private static final Map<String, Token.Type> KEYWORDS = buildKeywords();
    private static final Map<Character, Token.Type> ONE_CHAR_OPS = buildOneCharOps();
    private static final Map<String, Token.Type> TWO_CHAR_OPS = buildTwoCharOps();

    private static Map<Character, Token.Type> buildOneCharOps() {
        Map<Character, Token.Type> m = new HashMap<>();
        m.put('.', Token.Type.DOT);
        m.put(',', Token.Type.COMMA);
        m.put(':', Token.Type.COLON);
        m.put('|', Token.Type.PIPE);
        m.put('~', Token.Type.TILDE);
        m.put('=', Token.Type.ASSIGN);
        m.put('(', Token.Type.LPAREN);
        m.put(')', Token.Type.RPAREN);
        m.put('[', Token.Type.LBRACK);
        m.put(']', Token.Type.RBRACK);
        m.put('{', Token.Type.LBRACE);
        m.put('}', Token.Type.RBRACE);
        m.put('+', Token.Type.PLUS);
        m.put('-', Token.Type.MINUS);
        m.put('*', Token.Type.STAR);
        m.put('/', Token.Type.SLASH);
        m.put('%', Token.Type.PERCENT);
        m.put('<', Token.Type.LT);
        m.put('>', Token.Type.GT);
        return m;
    }

    private static Map<String, Token.Type> buildTwoCharOps() {
        Map<String, Token.Type> m = new HashMap<>();
        m.put("==", Token.Type.EQ);
        m.put("!=", Token.Type.NEQ);
        m.put("<=", Token.Type.LE);
        m.put(">=", Token.Type.GE);
        return m;
    }

    private static Map<String, Token.Type> buildKeywords() {
        Map<String, Token.Type> m = new HashMap<>();
        m.put("and", Token.Type.AND);
        m.put("or", Token.Type.OR);
        m.put("not", Token.Type.NOT);
        m.put("in", Token.Type.IN);
        m.put("is", Token.Type.IS);
        m.put("if", Token.Type.IF);
        m.put("elif", Token.Type.ELIF);
        m.put("else", Token.Type.ELSE);
        m.put("endif", Token.Type.ENDIF);
        m.put("for", Token.Type.FOR);
        m.put("endfor", Token.Type.ENDFOR);
        m.put("set", Token.Type.SET);
        m.put("raw", Token.Type.RAW);
        m.put("endraw", Token.Type.ENDRAW);
        m.put("true", Token.Type.TRUE);
        m.put("True", Token.Type.TRUE);
        m.put("false", Token.Type.FALSE);
        m.put("False", Token.Type.FALSE);
        m.put("none", Token.Type.NONE);
        m.put("None", Token.Type.NONE);
        m.put("null", Token.Type.NONE);
        return m;
    }

    private final String src;
    private int i;
    private int braceDepth;
    private final List<Token> out = new ArrayList<>();

    public Lexer(String src) {
        this.src = src == null ? "" : src;
    }

    public static List<Token> tokenize(String src) {
        return new Lexer(src).run();
    }

    public List<Token> run() {
        while (i < src.length()) {
            int start = i;
            int next = findNextOpener(i);
            if (next < 0) {
                emitText(src.substring(start), start);
                i = src.length();
                break;
            }
            if (next > i) {
                emitText(src.substring(start, next), start);
                i = next;
            }
            char c2 = src.charAt(i + 1);
            if (c2 == '{') {
                lexExpression();
            } else {
                lexStatement();
            }
        }
        out.add(new Token(Token.Type.EOF, "", i));
        return out;
    }

    /** Returns the index of the next {@code {{} or {@code {%}, or -1. */
    private int findNextOpener(int from) {
        for (int j = from; j < src.length() - 1; j++) {
            if (src.charAt(j) != '{') {
                continue;
            }
            char nc = src.charAt(j + 1);
            if (nc == '{' || nc == '%') {
                return j;
            }
        }
        return -1;
    }

    private void emitText(String text, int pos) {
        if (text.isEmpty()) {
            return;
        }
        out.add(new Token(Token.Type.RAW_TEXT, text, pos));
    }

    private void lexExpression() {
        int start = i;
        i += 2;
        if (consumeOptDash()) {
            trimPrecedingText();
        }
        out.add(new Token(Token.Type.LSTACHE, "{{", start));
        lexInner(false);
    }

    private void lexStatement() {
        int start = i;
        i += 2;
        if (consumeOptDash()) {
            trimPrecedingText();
        }
        out.add(new Token(Token.Type.LSTMT, "{%", start));
        int save = i;
        skipWs();
        if (matchKeywordRaw()) {
            out.add(new Token(Token.Type.RAW, "raw", i - 3));
            skipWs();
            consumeStmtClose();
            captureRawBlockBody();
            return;
        }
        i = save;
        lexInner(true);
    }

    private boolean consumeOptDash() {
        if (i < src.length() && src.charAt(i) == '-') {
            i++;
            return true;
        }
        return false;
    }

    private boolean matchKeywordRaw() {
        if (i + 3 > src.length()) {
            return false;
        }
        if (src.charAt(i) != 'r' || src.charAt(i + 1) != 'a' || src.charAt(i + 2) != 'w') {
            return false;
        }
        if (i + 3 < src.length() && isIdentPart(src.charAt(i + 3))) {
            return false;
        }
        i += 3;
        return true;
    }

    private void consumeStmtClose() {
        skipWs();
        boolean trimRight = consumeOptDash();
        if (trimRight) {
            skipWs();
        }
        if (i + 2 > src.length() || src.charAt(i) != '%' || src.charAt(i + 1) != '}') {
            throw new JinjaException("expected '%}' at position " + i);
        }
        out.add(new Token(Token.Type.RSTMT, "%}", i));
        i += 2;
        if (trimRight) {
            trimFollowingText();
        }
    }

    private void captureRawBlockBody() {
        int bodyStart = i;
        while (i + 1 < src.length()) {
            if (src.charAt(i) == '{' && src.charAt(i + 1) == '%' && peekEndraw(i + 2)) {
                emitRawBlockClose(bodyStart);
                return;
            }
            i++;
        }
        throw new JinjaException("unterminated {% raw %} block starting at " + (bodyStart - 2));
    }

    private boolean peekEndraw(int from) {
        int j = from;
        if (j < src.length() && src.charAt(j) == '-') {
            j++;
        }
        while (j < src.length() && Character.isWhitespace(src.charAt(j))) {
            j++;
        }
        if (j + 6 > src.length() || !src.startsWith("endraw", j)) {
            return false;
        }
        return j + 6 == src.length() || !isIdentPart(src.charAt(j + 6));
    }

    private void emitRawBlockClose(int bodyStart) {
        out.add(new Token(Token.Type.RAW_TEXT, src.substring(bodyStart, i), bodyStart));
        int stmtStart = i;
        i += 2;
        boolean trimLeft = consumeOptDash();
        out.add(new Token(Token.Type.LSTMT, "{%", stmtStart));
        if (trimLeft) {
            int last = out.size() - 2;
            Token t = out.get(last);
            if (t.type == Token.Type.RAW_TEXT) {
                out.set(last, new Token(Token.Type.RAW_TEXT, rtrim(t.text), t.pos));
            }
        }
        skipWs();
        int idStart = i;
        while (i < src.length() && isIdentPart(src.charAt(i))) {
            i++;
        }
        out.add(new Token(Token.Type.ENDRAW, src.substring(idStart, i), idStart));
        consumeStmtClose();
    }

    /** Lex tokens inside a {@code {{ ... }}} or {@code {% ... %}} block. */
    private void lexInner(boolean stmtMode) {
        while (i < src.length()) {
            skipWs();
            if (i >= src.length()) {
                break;
            }
            if (tryCloser(stmtMode)) {
                return;
            }
            lexOneToken();
        }
        throw new JinjaException("unterminated " + (stmtMode ? "{% %}" : "{{ }}") + " block");
    }

    /** Returns true if the lexer consumed the matching closer (including whitespace control). */
    private boolean tryCloser(boolean stmtMode) {
        char closer = stmtMode ? '%' : '}';
        Token.Type closerType = stmtMode ? Token.Type.RSTMT : Token.Type.RSTACHE;
        String closerText = stmtMode ? "%}" : "}}";
        char c = src.charAt(i);
        if (c == '-' && i + 2 < src.length()
                && src.charAt(i + 1) == closer && src.charAt(i + 2) == '}') {
            emitClose(closerType, closerText, 3);
            trimFollowingText();
            return true;
        }
        if (c == closer && i + 1 < src.length() && src.charAt(i + 1) == '}') {
            // When inside an inner dict literal, the first `}` of `}}` belongs
            // to the dict, not to the Jinja closer. Emit RBRACE for it and let
            // a subsequent iteration close the block.
            if (closer == '}' && braceDepth > 0) {
                out.add(new Token(Token.Type.RBRACE, "}", i));
                braceDepth--;
                i++;
                return false;
            }
            emitClose(closerType, closerText, 2);
            return true;
        }
        return false;
    }

    private void emitClose(Token.Type type, String text, int width) {
        out.add(new Token(type, text, i + (width - text.length())));
        i += width;
    }

    private void lexOneToken() {
        char c = src.charAt(i);
        if (c == '\'' || c == '"') {
            lexString(c);
            return;
        }
        if (Character.isDigit(c)) {
            lexNumber();
            return;
        }
        if (isIdentStart(c)) {
            lexIdentifierOrKeyword();
            return;
        }
        if (lexOperator()) {
            return;
        }
        throw new JinjaException("unexpected character '" + c + "' at position " + i);
    }

    private boolean lexOperator() {
        int p = i;
        if (i + 1 < src.length()) {
            String two = src.substring(i, i + 2);
            Token.Type t2 = TWO_CHAR_OPS.get(two);
            if (t2 != null) {
                out.add(new Token(t2, two, p));
                i += 2;
                return true;
            }
        }
        Token.Type t1 = ONE_CHAR_OPS.get(src.charAt(i));
        if (t1 != null) {
            char ch = src.charAt(i);
            if (ch == '{') {
                braceDepth++;
            } else if (ch == '}' && braceDepth > 0) {
                braceDepth--;
            }
            out.add(new Token(t1, String.valueOf(ch), p));
            i++;
            return true;
        }
        return false;
    }

    private void lexString(char quote) {
        int start = i;
        i++;
        StringBuilder sb = new StringBuilder();
        while (i < src.length()) {
            char c = src.charAt(i);
            if (c == '\\' && i + 1 < src.length()) {
                sb.append(unescape(src.charAt(i + 1)));
                i += 2;
                continue;
            }
            if (c == quote) {
                i++;
                out.add(new Token(Token.Type.STRING, sb.toString(), start));
                return;
            }
            sb.append(c);
            i++;
        }
        throw new JinjaException("unterminated string literal starting at " + start);
    }

    private static char unescape(char n) {
        switch (n) {
            case 'n': return '\n';
            case 't': return '\t';
            case 'r': return '\r';
            case '\\': return '\\';
            case '\'': return '\'';
            case '"': return '"';
            default: return n;
        }
    }

    private void lexNumber() {
        int start = i;
        boolean isFloat = false;
        consumeDigits();
        if (i < src.length() && src.charAt(i) == '.'
                && i + 1 < src.length() && Character.isDigit(src.charAt(i + 1))) {
            isFloat = true;
            i++;
            consumeDigits();
        }
        if (i < src.length() && (src.charAt(i) == 'e' || src.charAt(i) == 'E')) {
            isFloat = true;
            i++;
            if (i < src.length() && (src.charAt(i) == '+' || src.charAt(i) == '-')) {
                i++;
            }
            consumeDigits();
        }
        String text = src.substring(start, i);
        out.add(new Token(isFloat ? Token.Type.FLOAT : Token.Type.INTEGER, text, start));
    }

    private void consumeDigits() {
        while (i < src.length() && Character.isDigit(src.charAt(i))) {
            i++;
        }
    }

    private void lexIdentifierOrKeyword() {
        int start = i;
        while (i < src.length() && isIdentPart(src.charAt(i))) {
            i++;
        }
        String text = src.substring(start, i);
        Token.Type kw = KEYWORDS.get(text);
        out.add(new Token(kw != null ? kw : Token.Type.IDENT, text, start));
    }

    private static boolean isIdentStart(char c) {
        return Character.isLetter(c) || c == '_';
    }

    private static boolean isIdentPart(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }

    private void skipWs() {
        while (i < src.length() && Character.isWhitespace(src.charAt(i))) {
            i++;
        }
    }

    private void trimPrecedingText() {
        if (out.isEmpty()) {
            return;
        }
        int j = out.size() - 1;
        Token t = out.get(j);
        if (t.type == Token.Type.RAW_TEXT) {
            out.set(j, new Token(Token.Type.RAW_TEXT, rtrim(t.text), t.pos));
        }
    }

    private void trimFollowingText() {
        while (i < src.length() && Character.isWhitespace(src.charAt(i))) {
            i++;
        }
    }

    private static String rtrim(String s) {
        int end = s.length();
        while (end > 0 && Character.isWhitespace(s.charAt(end - 1))) {
            end--;
        }
        return s.substring(0, end);
    }
}
