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

import java.math.BigInteger;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Recursive-descent parser for Jinja expressions, used by the template parser
 * and directly when an expression is needed standalone (e.g. evaluating a single
 * {@code {{ ... }}} interpolation).
 *
 * <p>Precedence (low → high):
 * <ol>
 *   <li>ternary {@code A if B else C}</li>
 *   <li>{@code or}</li>
 *   <li>{@code and}</li>
 *   <li>{@code not} (unary prefix)</li>
 *   <li>{@code in} / {@code not in}</li>
 *   <li>{@code is} / {@code is not} (test)</li>
 *   <li>comparison {@code == != < > <= >=}</li>
 *   <li>filter {@code |}</li>
 *   <li>concat {@code ~}</li>
 *   <li>add/sub {@code + -}</li>
 *   <li>mul/div/mod {@code * / %}</li>
 *   <li>unary {@code - +}</li>
 *   <li>postfix {@code . [ ] ( )}</li>
 *   <li>atom (literal, ident, list/dict/tuple)</li>
 * </ol>
 */
public final class Parser {

    private final List<Token> tokens;
    private int p;

    public Parser(List<Token> tokens) {
        this.tokens = tokens;
        this.p = 0;
    }

    /** Parse a single expression from a Jinja expression source string. */
    public static Expr parseExpression(String source) {
        // The lexer starts in TEXT mode and only enters expression mode after {{ or {%.
        // For convenience the public surface accepts bare expressions like
        // `config["foo"]`; wrap them so the lexer hands back expression tokens.
        String t = source.stripLeading();
        String wrapped = t.startsWith("{{") || t.startsWith("{%")
            ? source : "{{ " + source + " }}";
        List<Token> all = Lexer.tokenize(wrapped);
        // If the source is a bare expression (no {{ }} wrapper), all tokens are at top-level.
        // Strip a leading LSTACHE / trailing RSTACHE if present so we can also parse
        // wrapped expressions like "{{ x }}".
        int from = 0;
        int to = all.size() - 1; // exclude EOF
        if (!all.isEmpty() && all.get(0).type == Token.Type.LSTACHE) {
            from = 1;
        }
        if (to >= 0 && all.get(to).type == Token.Type.EOF) {
            to--;
        }
        if (to >= 0 && all.get(to).type == Token.Type.RSTACHE) {
            to--;
        }
        List<Token> slice = new ArrayList<>(all.subList(from, to + 1));
        slice.add(new Token(Token.Type.EOF, "", 0));
        Parser parser = new Parser(slice);
        Expr e = parser.parseExpr();
        parser.expect(Token.Type.EOF);
        return e;
    }

    /** Parse an expression from an existing token stream, advancing the cursor. */
    public Expr parseExpr() {
        return parseTernary();
    }

    /**
     * Parse an expression without consuming a trailing inline ternary. Used by
     * {@link TemplateParser} for {@code for x in iterable [if filter]} where the
     * {@code if} is a loop filter, not a ternary.
     */
    public Expr parseExprNoTernary() {
        return parseOr();
    }

    private Expr parseTernary() {
        Expr value = parseOr();
        if (peek().type == Token.Type.IF) {
            advance();
            Expr cond = parseOr();
            // Jinja allows {{ value if cond }} (implicit None else).
            Expr alt;
            if (peek().type == Token.Type.ELSE) {
                advance();
                alt = parseTernary();
            } else {
                alt = new Expr.Literal(null);
            }
            return new Expr.Ternary(cond, value, alt);
        }
        return value;
    }

    private Expr parseOr() {
        Expr left = parseAnd();
        while (peek().type == Token.Type.OR) {
            advance();
            Expr right = parseAnd();
            left = new Expr.Binary("or", left, right);
        }
        return left;
    }

    private Expr parseAnd() {
        Expr left = parseNot();
        while (peek().type == Token.Type.AND) {
            advance();
            Expr right = parseNot();
            left = new Expr.Binary("and", left, right);
        }
        return left;
    }

    private Expr parseNot() {
        if (peek().type == Token.Type.NOT) {
            advance();
            return new Expr.Unary("not", parseNot());
        }
        return parseInOrIs();
    }

    private Expr parseInOrIs() {
        Expr left = parseComparison();
        while (true) {
            Token t = peek();
            if (t.type == Token.Type.IN) {
                advance();
                left = new Expr.Binary("in", left, parseComparison());
            } else if (t.type == Token.Type.NOT && peekAhead(1).type == Token.Type.IN) {
                advance();
                advance();
                left = new Expr.Binary("not in", left, parseComparison());
            } else if (t.type == Token.Type.IS) {
                advance();
                boolean negated = false;
                if (peek().type == Token.Type.NOT) {
                    advance();
                    negated = true;
                }
                String name = consumeTestName();
                left = new Expr.Test(left, name, negated);
            } else {
                return left;
            }
        }
    }

    private String consumeTestName() {
        Token t = peek();
        if (t.type == Token.Type.IDENT
                || t.type == Token.Type.NONE
                || t.type == Token.Type.TRUE
                || t.type == Token.Type.FALSE) {
            advance();
            return t.text;
        }
        throw new JinjaException("expected test name after 'is' at position " + t.pos);
    }

    private Expr parseComparison() {
        Expr left = parseConcat();
        while (true) {
            Token t = peek();
            String op = comparisonOp(t.type);
            if (op == null) {
                return left;
            }
            advance();
            Expr right = parseConcat();
            left = new Expr.Binary(op, left, right);
        }
    }

    private static String comparisonOp(Token.Type t) {
        switch (t) {
            case EQ: return "==";
            case NEQ: return "!=";
            case LT: return "<";
            case GT: return ">";
            case LE: return "<=";
            case GE: return ">=";
            default: return null;
        }
    }

    private Expr parseFilter() {
        Expr left = parsePostfix();
        while (peek().type == Token.Type.PIPE) {
            advance();
            Token name = expect(Token.Type.IDENT);
            List<Expr> args = new ArrayList<>();
            Map<String, Expr> kwargs = new LinkedHashMap<>();
            if (peek().type == Token.Type.LPAREN) {
                advance();
                parseCallArgs(args, kwargs);
                expect(Token.Type.RPAREN);
            }
            left = new Expr.Filter(left, name.text, args, kwargs);
        }
        return left;
    }

    private Expr parseConcat() {
        Expr left = parseAddSub();
        while (peek().type == Token.Type.TILDE) {
            advance();
            left = new Expr.Binary("~", left, parseAddSub());
        }
        return left;
    }

    private Expr parseAddSub() {
        Expr left = parseMulDivMod();
        while (peek().type == Token.Type.PLUS || peek().type == Token.Type.MINUS) {
            String op = peek().type == Token.Type.PLUS ? "+" : "-";
            advance();
            left = new Expr.Binary(op, left, parseMulDivMod());
        }
        return left;
    }

    private Expr parseMulDivMod() {
        Expr left = parseUnary();
        while (true) {
            Token.Type t = peek().type;
            String op;
            if (t == Token.Type.STAR) {
                op = "*";
            } else if (t == Token.Type.SLASH) {
                op = "/";
            } else if (t == Token.Type.PERCENT) {
                op = "%";
            } else {
                return left;
            }
            advance();
            left = new Expr.Binary(op, left, parseUnary());
        }
    }

    private Expr parseUnary() {
        Token.Type t = peek().type;
        if (t == Token.Type.MINUS) {
            advance();
            return new Expr.Unary("-", parseUnary());
        }
        if (t == Token.Type.PLUS) {
            advance();
            return new Expr.Unary("+", parseUnary());
        }
        return parseFilter();
    }

    private Expr parsePostfix() {
        Expr base = parseAtom();
        while (true) {
            Token.Type t = peek().type;
            if (t == Token.Type.DOT) {
                advance();
                Token name = expectIdentLike();
                base = new Expr.Attr(base, name.text);
            } else if (t == Token.Type.LBRACK) {
                advance();
                base = parseSubscript(base);
            } else if (t == Token.Type.LPAREN) {
                advance();
                List<Expr> args = new ArrayList<>();
                Map<String, Expr> kw = new LinkedHashMap<>();
                parseCallArgs(args, kw);
                expect(Token.Type.RPAREN);
                base = new Expr.Call(base, args, kw);
            } else {
                return base;
            }
        }
    }

    /**
     * Parse the inside of {@code base[ ... ]}, after the opening {@code [}.
     * Supports {@code [k]} indices and {@code [a:b]}, {@code [:b]}, {@code [a:]},
     * {@code [:]}, {@code [a:b:c]} slices. Negative bounds work via the unary minus.
     */
    private Expr parseSubscript(Expr base) {
        Expr first = null;
        if (peek().type != Token.Type.COLON) {
            first = parseExpr();
        }
        if (peek().type == Token.Type.COLON) {
            advance();
            Expr stop = null;
            if (peek().type != Token.Type.COLON && peek().type != Token.Type.RBRACK) {
                stop = parseExpr();
            }
            Expr step = null;
            if (peek().type == Token.Type.COLON) {
                advance();
                if (peek().type != Token.Type.RBRACK) {
                    step = parseExpr();
                }
            }
            expect(Token.Type.RBRACK);
            return new Expr.Slice(base, first, stop, step);
        }
        expect(Token.Type.RBRACK);
        return new Expr.Index(base, first);
    }

    private static final Set<Token.Type> IDENT_LIKE = EnumSet.of(
        Token.Type.IDENT, Token.Type.IN, Token.Type.IS, Token.Type.NOT,
        Token.Type.AND, Token.Type.OR, Token.Type.IF, Token.Type.ELSE,
        Token.Type.FOR, Token.Type.SET, Token.Type.TRUE, Token.Type.FALSE,
        Token.Type.NONE);

    /** Identifier-or-keyword usable as an attribute or test name. */
    private Token expectIdentLike() {
        Token t = peek();
        if (IDENT_LIKE.contains(t.type)) {
            advance();
            return t;
        }
        throw new JinjaException(
            "expected identifier after '.', got " + t.type + " at " + t.pos);
    }

    private void parseCallArgs(List<Expr> args, Map<String, Expr> kwargs) {
        if (peek().type == Token.Type.RPAREN) {
            return;
        }
        parseSingleCallArg(args, kwargs);
        while (peek().type == Token.Type.COMMA) {
            advance();
            if (peek().type == Token.Type.RPAREN) {
                return;
            }
            parseSingleCallArg(args, kwargs);
        }
    }

    private void parseSingleCallArg(List<Expr> args, Map<String, Expr> kwargs) {
        if (kwargs != null
                && peek().type == Token.Type.IDENT
                && peekAhead(1).type == Token.Type.ASSIGN) {
            String name = advance().text;
            advance();
            kwargs.put(name, parseExpr());
            return;
        }
        args.add(parseExpr());
    }

    private Expr parseAtom() {
        Token t = peek();
        switch (t.type) {
            case INTEGER:
                advance();
                return new Expr.Literal(parseInteger(t.text));
            case FLOAT:
                advance();
                return new Expr.Literal(Double.parseDouble(t.text));
            case STRING:
                advance();
                return new Expr.Literal(t.text);
            case TRUE:
                advance();
                return new Expr.Literal(Boolean.TRUE);
            case FALSE:
                advance();
                return new Expr.Literal(Boolean.FALSE);
            case NONE:
                advance();
                return new Expr.Literal(null);
            case IDENT:
                advance();
                return new Expr.Ident(t.text);
            case LPAREN:
                return parseParenOrTuple();
            case LBRACK:
                return parseListLit();
            case LBRACE:
                return parseDictLit();
            default:
                throw new JinjaException(
                    "unexpected token " + t.type + " '" + t.text + "' at " + t.pos);
        }
    }

    private static Object parseInteger(String text) {
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException nfe) {
            return new BigInteger(text);
        }
    }

    private Expr parseParenOrTuple() {
        advance();
        if (peek().type == Token.Type.RPAREN) {
            advance();
            return new Expr.TupleLit(List.of());
        }
        Expr first = parseExpr();
        if (peek().type == Token.Type.COMMA) {
            List<Expr> items = new ArrayList<>();
            items.add(first);
            while (peek().type == Token.Type.COMMA) {
                advance();
                if (peek().type == Token.Type.RPAREN) {
                    break;
                }
                items.add(parseExpr());
            }
            expect(Token.Type.RPAREN);
            return new Expr.TupleLit(items);
        }
        expect(Token.Type.RPAREN);
        return first;
    }

    private Expr parseListLit() {
        advance();
        List<Expr> items = new ArrayList<>();
        if (peek().type == Token.Type.RBRACK) {
            advance();
            return new Expr.ListLit(items);
        }
        items.add(parseExpr());
        while (peek().type == Token.Type.COMMA) {
            advance();
            if (peek().type == Token.Type.RBRACK) {
                break;
            }
            items.add(parseExpr());
        }
        expect(Token.Type.RBRACK);
        return new Expr.ListLit(items);
    }

    private Expr parseDictLit() {
        advance();
        List<Map.Entry<Expr, Expr>> entries = new ArrayList<>();
        if (peek().type == Token.Type.RBRACE) {
            advance();
            return new Expr.DictLit(entries);
        }
        entries.add(parseDictEntry());
        while (peek().type == Token.Type.COMMA) {
            advance();
            if (peek().type == Token.Type.RBRACE) {
                break;
            }
            entries.add(parseDictEntry());
        }
        expect(Token.Type.RBRACE);
        return new Expr.DictLit(entries);
    }

    private Map.Entry<Expr, Expr> parseDictEntry() {
        Expr k = parseExpr();
        expect(Token.Type.COLON);
        Expr v = parseExpr();
        return new AbstractMap.SimpleImmutableEntry<>(k, v);
    }

    // ─── token cursor helpers ─────────────────────────────────────────────────

    /** Public view of the next token without advancing. */
    public Token peekToken() {
        return peek();
    }

    /** Advance the cursor by one and return the consumed token. */
    public Token consume() {
        return advance();
    }

    /** Advance past a token of the given type, throwing if it does not match. */
    public Token expectType(Token.Type type) {
        return expect(type);
    }

    Token peek() {
        return tokens.get(p);
    }

    Token peekAhead(int offset) {
        int idx = p + offset;
        return idx < tokens.size() ? tokens.get(idx) : tokens.get(tokens.size() - 1);
    }

    Token advance() {
        Token t = tokens.get(p);
        if (p < tokens.size() - 1) {
            p++;
        }
        return t;
    }

    Token expect(Token.Type type) {
        Token t = peek();
        if (t.type != type) {
            throw new JinjaException(
                "expected " + type + " but got " + t.type + " '" + t.text + "' at " + t.pos);
        }
        return advance();
    }

    int position() {
        return p;
    }

    void seek(int newPos) {
        this.p = newPos;
    }
}
