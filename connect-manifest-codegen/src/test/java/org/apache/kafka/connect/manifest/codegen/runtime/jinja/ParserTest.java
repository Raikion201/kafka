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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParserTest {

    private static Expr parse(String src) {
        return Parser.parseExpression(src);
    }

    @Test
    void literals() {
        assertEquals(42L, ((Expr.Literal) parse("{{ 42 }}")).value());
        assertEquals(3.14, ((Expr.Literal) parse("{{ 3.14 }}")).value());
        assertEquals("hi", ((Expr.Literal) parse("{{ 'hi' }}")).value());
        assertEquals(Boolean.TRUE, ((Expr.Literal) parse("{{ true }}")).value());
        assertEquals(Boolean.FALSE, ((Expr.Literal) parse("{{ false }}")).value());
        assertNull(((Expr.Literal) parse("{{ none }}")).value());
    }

    @Test
    void bareIdentifier() {
        Expr e = parse("{{ config }}");
        assertEquals("config", ((Expr.Ident) e).name());
    }

    @Test
    void attributeAccess() {
        Expr e = parse("{{ config.api_key }}");
        Expr.Attr a = (Expr.Attr) e;
        assertEquals("api_key", a.name());
        assertEquals("config", ((Expr.Ident) a.target()).name());
    }

    @Test
    void chainedAttributeAccess() {
        Expr e = parse("{{ stream_slice.cursor_slice.end_time }}");
        Expr.Attr outer = (Expr.Attr) e;
        assertEquals("end_time", outer.name());
        Expr.Attr mid = (Expr.Attr) outer.target();
        assertEquals("cursor_slice", mid.name());
        assertEquals("stream_slice", ((Expr.Ident) mid.target()).name());
    }

    @Test
    void indexAccess() {
        Expr e = parse("{{ config['api_key'] }}");
        Expr.Index idx = (Expr.Index) e;
        assertEquals("config", ((Expr.Ident) idx.target()).name());
        assertEquals("api_key", ((Expr.Literal) idx.key()).value());
    }

    @Test
    void chainedIndexAccess() {
        Expr e = parse("{{ headers['link']['next']['url'] }}");
        Expr.Index outer = (Expr.Index) e;
        assertEquals("url", ((Expr.Literal) outer.key()).value());
        Expr.Index mid = (Expr.Index) outer.target();
        assertEquals("next", ((Expr.Literal) mid.key()).value());
        Expr.Index inner = (Expr.Index) mid.target();
        assertEquals("link", ((Expr.Literal) inner.key()).value());
        assertEquals("headers", ((Expr.Ident) inner.target()).name());
    }

    @Test
    void simpleCall() {
        Expr e = parse("{{ now_utc() }}");
        Expr.Call c = (Expr.Call) e;
        assertEquals("now_utc", ((Expr.Ident) c.callee()).name());
        assertTrue(c.args().isEmpty());
        assertTrue(c.kwargs().isEmpty());
    }

    @Test
    void methodCall() {
        Expr e = parse("{{ now_utc().strftime('%Y') }}");
        Expr.Call outer = (Expr.Call) e;
        Expr.Attr method = (Expr.Attr) outer.callee();
        assertEquals("strftime", method.name());
        Expr.Call inner = (Expr.Call) method.target();
        assertEquals("now_utc", ((Expr.Ident) inner.callee()).name());
        assertEquals(1, outer.args().size());
        assertEquals("%Y", ((Expr.Literal) outer.args().get(0)).value());
    }

    @Test
    void callWithKwargs() {
        Expr e = parse("{{ f(1, name='x', flag=true) }}");
        Expr.Call c = (Expr.Call) e;
        assertEquals(1, c.args().size());
        assertEquals(2, c.kwargs().size());
        assertEquals("x", ((Expr.Literal) c.kwargs().get("name")).value());
        assertEquals(Boolean.TRUE, ((Expr.Literal) c.kwargs().get("flag")).value());
    }

    @Test
    void filterApplication() {
        Expr e = parse("{{ v | float }}");
        Expr.Filter f = (Expr.Filter) e;
        assertEquals("float", f.name());
        assertTrue(f.args().isEmpty());
        assertEquals("v", ((Expr.Ident) f.target()).name());
    }

    @Test
    void filterChainedWithArgs() {
        Expr e = parse("{{ v | replace('%', '') | float }}");
        Expr.Filter outer = (Expr.Filter) e;
        assertEquals("float", outer.name());
        Expr.Filter inner = (Expr.Filter) outer.target();
        assertEquals("replace", inner.name());
        assertEquals(2, inner.args().size());
    }

    @Test
    void unaryNot() {
        Expr e = parse("{{ not flag }}");
        Expr.Unary u = (Expr.Unary) e;
        assertEquals("not", u.op());
        assertEquals("flag", ((Expr.Ident) u.target()).name());
    }

    @Test
    void unaryMinus() {
        Expr e = parse("{{ -1 }}");
        Expr.Unary u = (Expr.Unary) e;
        assertEquals("-", u.op());
        assertEquals(1L, ((Expr.Literal) u.target()).value());
    }

    @Test
    void arithmeticPrecedence() {
        Expr e = parse("{{ 1 + 2 * 3 }}");
        Expr.Binary b = (Expr.Binary) e;
        assertEquals("+", b.op());
        assertEquals(1L, ((Expr.Literal) b.left()).value());
        Expr.Binary mul = (Expr.Binary) b.right();
        assertEquals("*", mul.op());
    }

    @Test
    void modAndConcat() {
        Expr e = parse("{{ 'a' ~ b ~ 'c' }}");
        Expr.Binary outer = (Expr.Binary) e;
        assertEquals("~", outer.op());
        Expr.Binary lhs = (Expr.Binary) outer.left();
        assertEquals("~", lhs.op());
    }

    @Test
    void comparisonOperators() {
        for (String op : new String[]{"==", "!=", "<", ">", "<=", ">="}) {
            Expr e = parse("{{ a " + op + " b }}");
            Expr.Binary b = (Expr.Binary) e;
            assertEquals(op, b.op(), "for op " + op);
        }
    }

    @Test
    void andOrPrecedence() {
        Expr e = parse("{{ a or b and c }}");
        Expr.Binary or = (Expr.Binary) e;
        assertEquals("or", or.op());
        Expr.Binary and = (Expr.Binary) or.right();
        assertEquals("and", and.op());
    }

    @Test
    void inMembership() {
        Expr e = parse("{{ x in xs }}");
        Expr.Binary b = (Expr.Binary) e;
        assertEquals("in", b.op());
    }

    @Test
    void notInMembership() {
        Expr e = parse("{{ 'k' not in headers }}");
        Expr.Binary b = (Expr.Binary) e;
        assertEquals("not in", b.op());
    }

    @Test
    void isTest() {
        Expr e = parse("{{ x is none }}");
        Expr.Test t = (Expr.Test) e;
        assertEquals("none", t.name());
        assertEquals(false, t.negated());
    }

    @Test
    void isNotTest() {
        Expr e = parse("{{ x is not none }}");
        Expr.Test t = (Expr.Test) e;
        assertEquals("none", t.name());
        assertEquals(true, t.negated());
    }

    @Test
    void ternary() {
        Expr e = parse("{{ a if cond else b }}");
        Expr.Ternary t = (Expr.Ternary) e;
        assertEquals("cond", ((Expr.Ident) t.cond()).name());
        assertEquals("a", ((Expr.Ident) t.ifTrue()).name());
        assertEquals("b", ((Expr.Ident) t.ifFalse()).name());
    }

    @Test
    void ternaryNested() {
        Expr e = parse("{{ v | float if v is not none and v != \"\" else None }}");
        assertInstanceOf(Expr.Ternary.class, e);
        Expr.Ternary t = (Expr.Ternary) e;
        assertInstanceOf(Expr.Filter.class, t.ifTrue());
        assertInstanceOf(Expr.Binary.class, t.cond());
        assertInstanceOf(Expr.Literal.class, t.ifFalse());
        assertNull(((Expr.Literal) t.ifFalse()).value());
    }

    @Test
    void listLiteral() {
        Expr e = parse("{{ [1, 2, 3] }}");
        Expr.ListLit l = (Expr.ListLit) e;
        assertEquals(3, l.items().size());
    }

    @Test
    void emptyListLiteral() {
        Expr e = parse("{{ [] }}");
        assertTrue(((Expr.ListLit) e).items().isEmpty());
    }

    @Test
    void dictLiteral() {
        Expr e = parse("{{ {'a': 1, 'b': 2} }}");
        Expr.DictLit d = (Expr.DictLit) e;
        assertEquals(2, d.entries().size());
        assertEquals("a", ((Expr.Literal) d.entries().get(0).getKey()).value());
        assertEquals(1L, ((Expr.Literal) d.entries().get(0).getValue()).value());
    }

    @Test
    void emptyDictLiteral() {
        Expr e = parse("{{ {} }}");
        assertTrue(((Expr.DictLit) e).entries().isEmpty());
    }

    @Test
    void parenthesizedGrouping() {
        Expr e = parse("{{ (1 + 2) * 3 }}");
        Expr.Binary b = (Expr.Binary) e;
        assertEquals("*", b.op());
        assertEquals("+", ((Expr.Binary) b.left()).op());
    }

    @Test
    void tupleLiteral() {
        Expr e = parse("{{ (1, 2) }}");
        Expr.TupleLit t = (Expr.TupleLit) e;
        assertEquals(2, t.items().size());
    }

    @Test
    void emptyTupleLiteral() {
        Expr e = parse("{{ () }}");
        assertTrue(((Expr.TupleLit) e).items().isEmpty());
    }

    @Test
    void attributeNameCanBeKeyword() {
        // Jinja allows obj.is, obj.in, etc — Python parses these as attribute accesses.
        Expr e = parse("{{ obj.is }}");
        Expr.Attr a = (Expr.Attr) e;
        assertEquals("is", a.name());
    }

    @Test
    void corpusComplexExpression1() {
        Expr e = parse("{{ next_page_token['next_page_token'] or '0' }}");
        Expr.Binary or = (Expr.Binary) e;
        assertEquals("or", or.op());
        assertInstanceOf(Expr.Index.class, or.left());
        assertEquals("0", ((Expr.Literal) or.right()).value());
    }

    @Test
    void corpusComplexExpression2() {
        // duration call inside subtraction inside attribute method call
        Expr e = parse("{{ (now_utc() - duration('P730D')).strftime('%Y-%m-%dT%H:%M:%SZ') }}");
        Expr.Call outer = (Expr.Call) e;
        Expr.Attr method = (Expr.Attr) outer.callee();
        assertEquals("strftime", method.name());
        Expr.Binary diff = (Expr.Binary) method.target();
        assertEquals("-", diff.op());
        assertInstanceOf(Expr.Call.class, diff.left());
        assertInstanceOf(Expr.Call.class, diff.right());
    }

    @Test
    void corpusComplexExpression3() {
        Expr e = parse(
            "{{ (now_utc() - duration('P' ~ ((now_utc().weekday() + 1) % 7 + 7) ~ 'D'))"
                + ".strftime('%Y-%m-%dT00:00:00Z') }}");
        assertInstanceOf(Expr.Call.class, e);
    }

    @Test
    void corpusFilterRegexSearch() {
        Expr e = parse("{{ config[\"spreadsheet_id\"] | regex_search(\"^(https://.*)\") }}");
        Expr.Filter f = (Expr.Filter) e;
        assertEquals("regex_search", f.name());
        assertEquals(1, f.args().size());
    }

    @Test
    void corpusOrCallChain() {
        Expr e = parse("{{ config.get('replication_start_date', 'fallback') }}");
        Expr.Call c = (Expr.Call) e;
        Expr.Attr a = (Expr.Attr) c.callee();
        assertEquals("get", a.name());
        assertEquals(2, c.args().size());
    }

    @Test
    void unterminatedExpressionThrows() {
        assertThrows(JinjaException.class, () -> parse("{{ a + }}"));
    }

    @Test
    void mismatchedParenThrows() {
        assertThrows(JinjaException.class, () -> parse("{{ (a + b }}"));
    }

    @Test
    void rightAssociativeTernary() {
        // a if c1 else b if c2 else c  →  a if c1 else (b if c2 else c)
        Expr e = parse("{{ a if c1 else b if c2 else c }}");
        Expr.Ternary outer = (Expr.Ternary) e;
        assertEquals("a", ((Expr.Ident) outer.ifTrue()).name());
        assertEquals("c1", ((Expr.Ident) outer.cond()).name());
        Expr.Ternary inner = (Expr.Ternary) outer.ifFalse();
        assertEquals("b", ((Expr.Ident) inner.ifTrue()).name());
        assertEquals("c", ((Expr.Ident) inner.ifFalse()).name());
    }

    @Test
    void corpusSamplesAllParse() {
        // Every complex jinja expression we found in the manifest corpus must parse.
        String[] samples = {
            "{{ now_utc().strftime('%Y-%m-%dT%H:%M:%SZ') }}",
            "{{ stream_slice.cursor_slice.end_time }}",
            "{{ next_page_token['next_page_token'] or '0' }}",
            "{{ v | replace('%', '') | float if v is not none and v != \"\" else None }}",
            "{{ format_datetime( max(config.get('replication_start_date',"
                + "(now_utc() - duration('P730D')).strftime('%Y-%m-%dT%H:%M:%SZ') ),"
                + "(now_utc() - duration('P730D')).strftime('%Y-%m-%dT%H:%M:%SZ')),"
                + "'%Y-%m-%dT%H:%M:%SZ') }}",
            "{{ (now_utc() - duration('P' ~ ((now_utc().weekday() + 1) % 7 + 7) ~ 'D'))"
                + ".strftime('%Y-%m-%dT00:00:00Z') }}",
            "{{ headers['link']['next']['url'] }}",
            "{{ 'next' not in headers['link'] }}",
            "{{ config['start_date'] or config.end_date }}",
            "{{ config['spreadsheet_id'] | regex_search('^(https://.*)') }}"
        };
        for (String s : samples) {
            Expr e = parse(s);
            // smoke check: parsing returned something non-null
            assertTrue(e != null, "null parse for: " + s);
        }
    }

}
