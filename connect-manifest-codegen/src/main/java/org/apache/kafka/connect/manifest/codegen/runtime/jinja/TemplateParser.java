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
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Parses a complete Jinja template into a {@link Template} AST. Handles raw text,
 * {@code {{ expr }}} interpolations, and the {@code if / for / set / raw}
 * statement tags found in the manifest corpus.
 */
public final class TemplateParser {

    private static final Set<Token.Type> IF_TERMINATORS = EnumSet.of(
        Token.Type.ELIF, Token.Type.ELSE, Token.Type.ENDIF);
    private static final Set<Token.Type> FOR_TERMINATORS = EnumSet.of(
        Token.Type.ELSE, Token.Type.ENDFOR);

    private final Parser exprParser;

    public TemplateParser(List<Token> tokens) {
        this.exprParser = new Parser(new ArrayList<>(tokens));
    }

    /** Parse a full template source string. */
    public static Template parse(String src) {
        return new TemplateParser(Lexer.tokenize(src)).parseTemplate();
    }

    /** Parse the entire token stream into a Template. */
    public Template parseTemplate() {
        List<Template.Node> body = parseNodesUntil(Set.of());
        if (peek().type != Token.Type.EOF) {
            throw new JinjaException(
                "unexpected trailing token " + peek().type + " at " + peek().pos);
        }
        return new Template(body);
    }

    /**
     * Parse nodes until we hit EOF or a statement keyword in {@code stoppers}.
     * The terminator token (e.g. {@code endif}) is left unconsumed for the caller.
     */
    private List<Template.Node> parseNodesUntil(Set<Token.Type> stoppers) {
        List<Template.Node> body = new ArrayList<>();
        while (peek().type != Token.Type.EOF) {
            Token t = peek();
            if (t.type == Token.Type.RAW_TEXT) {
                consume();
                if (!t.text.isEmpty()) {
                    body.add(new Template.Text(t.text));
                }
            } else if (t.type == Token.Type.LSTACHE) {
                body.add(parseOutput());
            } else if (t.type == Token.Type.LSTMT) {
                if (peekStatementKeyword(stoppers)) {
                    return body;
                }
                Template.Node node = parseStatement();
                if (node != null) {
                    body.add(node);
                }
            } else {
                throw new JinjaException(
                    "unexpected token " + t.type + " at " + t.pos);
            }
        }
        return body;
    }

    /** True if the current LSTMT introduces a statement keyword in {@code stoppers}. */
    private boolean peekStatementKeyword(Set<Token.Type> stoppers) {
        if (stoppers.isEmpty()) {
            return false;
        }
        Token next = exprParser.peekAhead(1);
        return stoppers.contains(next.type);
    }

    private Template.Output parseOutput() {
        expect(Token.Type.LSTACHE);
        Expr e = exprParser.parseExpr();
        expect(Token.Type.RSTACHE);
        return new Template.Output(e);
    }

    /** Dispatch on the keyword that opens this {@code {% ... %}} statement. */
    private Template.Node parseStatement() {
        expect(Token.Type.LSTMT);
        Token kw = peek();
        switch (kw.type) {
            case IF:
                return parseIf();
            case FOR:
                return parseFor();
            case SET:
                return parseSet();
            case RAW:
                consume();
                expect(Token.Type.RSTMT);
                return null;
            case ENDRAW:
                consume();
                expect(Token.Type.RSTMT);
                return null;
            default:
                throw new JinjaException(
                    "unexpected statement keyword " + kw.type + " '" + kw.text
                        + "' at " + kw.pos);
        }
    }

    private Template.IfStmt parseIf() {
        expect(Token.Type.IF);
        List<Template.Branch> branches = new ArrayList<>();
        Expr cond = exprParser.parseExpr();
        expect(Token.Type.RSTMT);
        List<Template.Node> body = parseNodesUntil(IF_TERMINATORS);
        branches.add(new Template.Branch(cond, body));

        while (peek().type == Token.Type.LSTMT
                && exprParser.peekAhead(1).type == Token.Type.ELIF) {
            expect(Token.Type.LSTMT);
            expect(Token.Type.ELIF);
            Expr ec = exprParser.parseExpr();
            expect(Token.Type.RSTMT);
            List<Template.Node> eb = parseNodesUntil(IF_TERMINATORS);
            branches.add(new Template.Branch(ec, eb));
        }

        List<Template.Node> elseBody = List.of();
        if (peek().type == Token.Type.LSTMT
                && exprParser.peekAhead(1).type == Token.Type.ELSE) {
            expect(Token.Type.LSTMT);
            expect(Token.Type.ELSE);
            expect(Token.Type.RSTMT);
            elseBody = parseNodesUntil(IF_TERMINATORS);
        }

        expect(Token.Type.LSTMT);
        expect(Token.Type.ENDIF);
        expect(Token.Type.RSTMT);
        return new Template.IfStmt(branches, elseBody);
    }

    private Template.ForStmt parseFor() {
        expect(Token.Type.FOR);
        List<String> targets = new ArrayList<>();
        targets.add(expect(Token.Type.IDENT).text);
        while (peek().type == Token.Type.COMMA) {
            consume();
            targets.add(expect(Token.Type.IDENT).text);
        }
        expect(Token.Type.IN);
        Expr iterable = exprParser.parseExprNoTernary();
        Expr filter = null;
        if (peek().type == Token.Type.IF) {
            consume();
            filter = exprParser.parseExpr();
        }
        expect(Token.Type.RSTMT);

        List<Template.Node> body = parseNodesUntil(FOR_TERMINATORS);
        List<Template.Node> elseBody = List.of();
        if (peek().type == Token.Type.LSTMT
                && exprParser.peekAhead(1).type == Token.Type.ELSE) {
            expect(Token.Type.LSTMT);
            expect(Token.Type.ELSE);
            expect(Token.Type.RSTMT);
            elseBody = parseNodesUntil(FOR_TERMINATORS);
        }
        expect(Token.Type.LSTMT);
        expect(Token.Type.ENDFOR);
        expect(Token.Type.RSTMT);
        return new Template.ForStmt(targets, iterable, filter, body, elseBody);
    }

    private Template.SetStmt parseSet() {
        expect(Token.Type.SET);
        Token name = expect(Token.Type.IDENT);
        expect(Token.Type.ASSIGN);
        Expr value = exprParser.parseExpr();
        expect(Token.Type.RSTMT);
        return new Template.SetStmt(name.text, value);
    }

    private Token peek() {
        return exprParser.peekToken();
    }

    private Token consume() {
        return exprParser.consume();
    }

    private Token expect(Token.Type type) {
        return exprParser.expectType(type);
    }
}
