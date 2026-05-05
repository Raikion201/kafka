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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EvaluatorTest {

    private final Evaluator ev = new Evaluator();

    private Object eval(String expr, Map<String, Object> ctx) {
        return ev.evaluateExpression(expr, ctx);
    }

    private String render(String src, Map<String, Object> ctx) {
        return ev.render(src, ctx);
    }

    @Test
    void renderPlainText() {
        assertEquals("hello", render("hello", Map.of()));
    }

    @Test
    void interpolateString() {
        assertEquals("hi alice", render("hi {{ name }}", Map.of("name", "alice")));
    }

    @Test
    void interpolateMissingVarBecomesNoneText() {
        assertEquals("v=None", render("v={{ x }}", Map.of()));
    }

    @Test
    void arithmeticIntegers() {
        assertEquals(5L, eval("{{ 2 + 3 }}", Map.of()));
        assertEquals(6L, eval("{{ 2 * 3 }}", Map.of()));
        assertEquals(1L, eval("{{ 5 % 4 }}", Map.of()));
    }

    @Test
    void divisionAlwaysFloat() {
        // Python's `/` is true division.
        assertEquals(2.5, eval("{{ 5 / 2 }}", Map.of()));
    }

    @Test
    void mixedNumericPromotesToDouble() {
        assertEquals(3.5, eval("{{ 1 + 2.5 }}", Map.of()));
    }

    @Test
    void stringConcatTilde() {
        assertEquals("ab", eval("{{ 'a' ~ 'b' }}", Map.of()));
        assertEquals("x=42", eval("{{ 'x=' ~ n }}", Map.of("n", 42L)));
    }

    @Test
    void plusOperatorOnStringsAlsoConcats() {
        // We treat + on strings as string concat (Python does too).
        assertEquals("ab", eval("{{ 'a' + 'b' }}", Map.of()));
    }

    @Test
    void truthinessRules() {
        assertFalse(Evaluator.truthy(null));
        assertFalse(Evaluator.truthy(0L));
        assertFalse(Evaluator.truthy(0.0));
        assertFalse(Evaluator.truthy(""));
        assertFalse(Evaluator.truthy(List.of()));
        assertFalse(Evaluator.truthy(Map.of()));
        assertFalse(Evaluator.truthy(false));
        assertTrue(Evaluator.truthy(1L));
        assertTrue(Evaluator.truthy("x"));
        assertTrue(Evaluator.truthy(List.of(1)));
    }

    @Test
    void ifElseBranches() {
        Map<String, Object> ctx = new LinkedHashMap<>();
        ctx.put("flag", true);
        assertEquals("yes", render("{% if flag %}yes{% else %}no{% endif %}", ctx));
        ctx.put("flag", false);
        assertEquals("no", render("{% if flag %}yes{% else %}no{% endif %}", ctx));
    }

    @Test
    void elifChain() {
        String src = "{% if x == 1 %}a{% elif x == 2 %}b{% elif x == 3 %}c{% else %}d{% endif %}";
        assertEquals("a", render(src, Map.of("x", 1L)));
        assertEquals("b", render(src, Map.of("x", 2L)));
        assertEquals("c", render(src, Map.of("x", 3L)));
        assertEquals("d", render(src, Map.of("x", 99L)));
    }

    @Test
    void forLoopOverList() {
        String src = "{% for i in xs %}{{ i }},{% endfor %}";
        assertEquals("1,2,3,", render(src, Map.of("xs", List.of(1L, 2L, 3L))));
    }

    @Test
    void forLoopElseWhenEmpty() {
        String src = "{% for i in xs %}x{% else %}empty{% endfor %}";
        assertEquals("empty", render(src, Map.of("xs", List.of())));
    }

    @Test
    void forLoopVar() {
        String src = "{% for i in xs %}{{ loop.index }}={{ i }};{% endfor %}";
        assertEquals("1=a;2=b;3=c;", render(src, Map.of("xs", List.of("a", "b", "c"))));
    }

    @Test
    void forLoopFilter() {
        String src = "{% for i in xs if i > 2 %}{{ i }};{% endfor %}";
        assertEquals("3;4;", render(src, Map.of("xs", List.of(1L, 2L, 3L, 4L))));
    }

    @Test
    void forLoopTupleUnpack() {
        Map<String, Object> ctx = new LinkedHashMap<>();
        Map<String, Object> dict = new LinkedHashMap<>();
        dict.put("a", 1L);
        dict.put("b", 2L);
        ctx.put("d", dict);
        String src = "{% for k, v in d.items() %}{{ k }}={{ v }};{% endfor %}";
        assertEquals("a=1;b=2;", render(src, ctx));
    }

    @Test
    void setStatement() {
        String src = "{% set y = x + 1 %}{{ y }}";
        assertEquals("11", render(src, Map.of("x", 10L)));
    }

    @Test
    void dictAccessByIndexAndAttr() {
        Map<String, Object> cfg = new LinkedHashMap<>();
        cfg.put("api_key", "K");
        Map<String, Object> ctx = Map.of("config", cfg);
        assertEquals("K", eval("{{ config['api_key'] }}", ctx));
        assertEquals("K", eval("{{ config.api_key }}", ctx));
    }

    @Test
    void chainedDictAccess() {
        Map<String, Object> link = Map.of("next", Map.of("url", "u"));
        Map<String, Object> headers = Map.of("link", link);
        assertEquals("u", eval("{{ headers['link']['next']['url'] }}", Map.of("headers", headers)));
    }

    @Test
    void ternaryExpression() {
        assertEquals("yes", eval("{{ 'yes' if x else 'no' }}", Map.of("x", true)));
        assertEquals("no", eval("{{ 'yes' if x else 'no' }}", Map.of("x", false)));
        assertEquals("no", eval("{{ 'yes' if x else 'no' }}", Map.of("x", 0L)));
    }

    @Test
    void andOrShortCircuit() {
        // 'or' returns the first truthy (or last) operand
        assertEquals("a", eval("{{ 'a' or 'b' }}", Map.of()));
        assertEquals("b", eval("{{ '' or 'b' }}", Map.of()));
        // 'and' returns the first falsey (or last) operand
        assertEquals("b", eval("{{ 'a' and 'b' }}", Map.of()));
        assertEquals("", eval("{{ '' and 'b' }}", Map.of()));
    }

    @Test
    void unaryNot() {
        assertEquals(true, eval("{{ not 0 }}", Map.of()));
        assertEquals(false, eval("{{ not 'x' }}", Map.of()));
    }

    @Test
    void unaryMinus() {
        assertEquals(-3L, eval("{{ -3 }}", Map.of()));
        assertEquals(-2.5, eval("{{ -x }}", Map.of("x", 2.5)));
    }

    @Test
    void comparisonOperators() {
        assertEquals(true, eval("{{ 1 < 2 }}", Map.of()));
        assertEquals(false, eval("{{ 1 > 2 }}", Map.of()));
        assertEquals(true, eval("{{ 'abc' == 'abc' }}", Map.of()));
        assertEquals(true, eval("{{ 'abc' != 'def' }}", Map.of()));
        assertEquals(true, eval("{{ 1 == 1.0 }}", Map.of())); // numeric equality across types
    }

    @Test
    void inMembership() {
        assertEquals(true, eval("{{ 'a' in xs }}", Map.of("xs", List.of("a", "b"))));
        assertEquals(false, eval("{{ 'z' in xs }}", Map.of("xs", List.of("a", "b"))));
        assertEquals(true, eval("{{ 'k' in d }}", Map.of("d", Map.of("k", 1L))));
        assertEquals(true, eval("{{ 'foo' in 'hello foo bar' }}", Map.of()));
    }

    @Test
    void notInMembership() {
        assertEquals(true, eval("{{ 'next' not in headers['link'] }}",
            Map.of("headers", Map.of("link", Map.of("prev", "p")))));
    }

    @Test
    void isNoneTest() {
        assertEquals(true, eval("{{ x is none }}", new LinkedHashMap<>()));
        assertEquals(false, eval("{{ x is none }}", Map.of("x", 1L)));
        assertEquals(true, eval("{{ x is not none }}", Map.of("x", 1L)));
    }

    @Test
    void listLiteralRoundTrip() {
        Object v = eval("{{ [1, 2, 3] }}", Map.of());
        assertEquals(List.of(1L, 2L, 3L), v);
    }

    @Test
    void dictLiteralRoundTrip() {
        Object v = eval("{{ {'a': 1, 'b': 2} }}", Map.of());
        assertEquals(Map.of("a", 1L, "b", 2L), v);
    }

    @Test
    void rawBlockEmitsBodyVerbatim() {
        assertEquals("{{ x }}", render("{% raw %}{{ x }}{% endraw %}", Map.of("x", 1L)));
    }

    @Test
    void stringifyNoneAndBoolean() {
        assertEquals("None", Evaluator.stringify(null));
        assertEquals("True", Evaluator.stringify(true));
        assertEquals("False", Evaluator.stringify(false));
        assertEquals("1.0", Evaluator.stringify(1.0));
        assertEquals("hello", Evaluator.stringify("hello"));
    }

    @Test
    void mapMethodGetWithDefault() {
        Map<String, Object> cfg = new LinkedHashMap<>();
        cfg.put("a", 1L);
        Object v = eval("{{ config.get('missing', 'default') }}", Map.of("config", cfg));
        assertEquals("default", v);
        assertEquals(1L, eval("{{ config.get('a', 'd') }}", Map.of("config", cfg)));
    }

    @Test
    void stringMethodStartswith() {
        assertEquals(true, eval("{{ s.startswith('he') }}", Map.of("s", "hello")));
        assertEquals(false, eval("{{ s.startswith('zz') }}", Map.of("s", "hello")));
    }

    @Test
    void stringMethodSplitJoin() {
        Object parts = eval("{{ s.split(',') }}", Map.of("s", "a,b,c"));
        assertEquals(List.of("a", "b", "c"), parts);
    }

    @Test
    void nullDictLookupReturnsNull() {
        assertNull(eval("{{ d['missing'] }}", Map.of("d", Map.of())));
    }

    @Test
    void nestedConditionalInLoopFromCorpus() {
        String src = "{% for i in xs %}{% if i > 1 %}>{{ i }}{% else %}={{ i }}{% endif %};{% endfor %}";
        assertEquals("=1;>2;>3;", render(src, Map.of("xs", List.of(1L, 2L, 3L))));
    }

    @Test
    void parenGroupingForcesPrecedence() {
        assertEquals(9L, eval("{{ (1 + 2) * 3 }}", Map.of()));
    }
}
