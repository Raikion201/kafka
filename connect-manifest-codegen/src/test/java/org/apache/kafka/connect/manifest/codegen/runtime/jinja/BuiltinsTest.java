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
import static org.junit.jupiter.api.Assertions.assertNull;

class BuiltinsTest {

    private final Evaluator ev = new Evaluator();

    private Object eval(String expr) {
        return ev.evaluateExpression(expr, Map.of());
    }

    private Object eval(String expr, Map<String, Object> ctx) {
        return ev.evaluateExpression(expr, ctx);
    }

    // ── functions ──────────────────────────────────────────────────────────

    @Test
    void maxAndMinFunctions() {
        assertEquals(3L, eval("{{ max(1, 2, 3) }}"));
        assertEquals(1L, eval("{{ min(1, 2, 3) }}"));
        assertEquals(3L, eval("{{ max([1, 2, 3]) }}"));
    }

    @Test
    void lenFunction() {
        assertEquals(3L, eval("{{ len([1, 2, 3]) }}"));
        assertEquals(5L, eval("{{ len('hello') }}"));
        assertEquals(2L, eval("{{ len(d) }}", Map.of("d", Map.of("a", 1, "b", 2))));
    }

    @Test
    void absSumRound() {
        assertEquals(5L, eval("{{ abs(-5) }}"));
        assertEquals(6L, eval("{{ sum([1, 2, 3]) }}"));
        assertEquals(3L, eval("{{ round(2.7) }}"));
        assertEquals(2.5, eval("{{ round(2.456, 1) }}"));
    }

    @Test
    void rangeFunction() {
        assertEquals(List.of(0L, 1L, 2L), eval("{{ range(3) }}"));
        assertEquals(List.of(2L, 3L, 4L), eval("{{ range(2, 5) }}"));
        assertEquals(List.of(0L, 2L, 4L), eval("{{ range(0, 5, 2) }}"));
    }

    @Test
    void coercionFunctions() {
        assertEquals(42L, eval("{{ int('42') }}"));
        assertEquals(3.14, eval("{{ float('3.14') }}"));
        assertEquals("42", eval("{{ str(42) }}"));
        assertEquals(true, eval("{{ bool(1) }}"));
        assertEquals(false, eval("{{ bool(0) }}"));
    }

    // ── filters ────────────────────────────────────────────────────────────

    @Test
    void coercionFilters() {
        assertEquals(42L, eval("{{ '42' | int }}"));
        assertEquals(3.14, eval("{{ '3.14' | float }}"));
        assertEquals("42", eval("{{ 42 | string }}"));
        assertEquals(true, eval("{{ 'x' | bool }}"));
    }

    @Test
    void defaultFilter() {
        Map<String, Object> ctx = new LinkedHashMap<>();
        assertEquals("fallback", eval("{{ x | default('fallback') }}", ctx));
        ctx.put("x", "value");
        assertEquals("value", eval("{{ x | default('fallback') }}", ctx));
        // truthy variant: replace falsey too
        ctx.put("x", "");
        assertEquals("fallback", eval("{{ x | default('fallback', true) }}", ctx));
    }

    @Test
    void lengthFilter() {
        assertEquals(3L, eval("{{ [1, 2, 3] | length }}"));
        assertEquals(5L, eval("{{ 'hello' | length }}"));
    }

    @Test
    void firstLastFilters() {
        assertEquals(1L, eval("{{ [1, 2, 3] | first }}"));
        assertEquals(3L, eval("{{ [1, 2, 3] | last }}"));
        assertNull(eval("{{ [] | first }}"));
    }

    @Test
    void minMaxFilters() {
        assertEquals(1L, eval("{{ [3, 1, 2] | min }}"));
        assertEquals(3L, eval("{{ [3, 1, 2] | max }}"));
    }

    @Test
    void joinFilter() {
        assertEquals("a,b,c", eval("{{ ['a', 'b', 'c'] | join(',') }}"));
        assertEquals("abc", eval("{{ ['a', 'b', 'c'] | join }}"));
    }

    @Test
    void reverseSortUnique() {
        assertEquals(List.of(3L, 2L, 1L), eval("{{ [1, 2, 3] | reverse }}"));
        assertEquals(List.of(1L, 2L, 3L), eval("{{ [3, 1, 2] | sort }}"));
        assertEquals(List.of(1L, 2L, 3L), eval("{{ [1, 2, 2, 3, 1] | unique }}"));
    }

    @Test
    void stringTransformFilters() {
        assertEquals("HELLO", eval("{{ 'hello' | upper }}"));
        assertEquals("hello", eval("{{ 'HELLO' | lower }}"));
        assertEquals("hello", eval("{{ '  hello  ' | trim }}"));
        assertEquals("Hello", eval("{{ 'hello' | capitalize }}"));
        assertEquals("Hello World", eval("{{ 'hello world' | title }}"));
    }

    @Test
    void replaceAndTruncate() {
        assertEquals("haxa", eval("{{ 'haba' | replace('b', 'x') }}"));
        assertEquals("hello...", eval("{{ 'hello world' | truncate(5) }}"));
        assertEquals("short", eval("{{ 'short' | truncate(50) }}"));
    }

    @Test
    void regexSearchFilter() {
        // Without group: returns full match
        assertEquals("abc123", eval("{{ 'xyz abc123 def' | regex_search('[a-z]+\\\\d+') }}"));
        // With group: returns first capture
        assertEquals("123", eval("{{ 'abc123' | regex_search('([0-9]+)') }}"));
        // No match: null
        assertNull(eval("{{ 'abc' | regex_search('\\\\d+') }}"));
    }

    @Test
    void regexReplaceFilter() {
        assertEquals("a-b-c", eval("{{ 'a b c' | regex_replace(' ', '-') }}"));
        assertEquals("X X X", eval("{{ 'a b c' | regex_replace('[a-z]', 'X') }}"));
    }

    @Test
    void tojsonFilter() {
        assertEquals("{\"a\":1,\"b\":\"x\"}",
            eval("{{ d | tojson }}", Map.of("d", new LinkedHashMap<>(Map.of("a", 1L, "b", "x")))));
        assertEquals("[1,2,3]", eval("{{ [1, 2, 3] | tojson }}"));
        assertEquals("\"hi\"", eval("{{ 'hi' | tojson }}"));
        assertEquals("null", eval("{{ x | tojson }}", Map.of()));
    }

    @Test
    void corpusReplacePipeline() {
        // "{{ v | replace('%', '') | float if v is not none and v != "" else None }}"
        assertEquals(40.5,
            ev.evaluateExpression(
                "{{ v | replace('%', '') | float if v is not none and v != \"\" else None }}",
                Map.of("v", "40.5%")));
    }
}
