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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TemplateParserTest {

    private static Template parse(String src) {
        return TemplateParser.parse(src);
    }

    @Test
    void emptyTemplateHasEmptyBody() {
        assertTrue(parse("").body().isEmpty());
    }

    @Test
    void plainTextOnly() {
        Template t = parse("hello world");
        assertEquals(1, t.body().size());
        assertEquals("hello world", ((Template.Text) t.body().get(0)).value());
    }

    @Test
    void singleInterpolation() {
        Template t = parse("{{ x }}");
        assertEquals(1, t.body().size());
        Template.Output o = (Template.Output) t.body().get(0);
        assertEquals("x", ((Expr.Ident) o.expr()).name());
    }

    @Test
    void textBracketInterpolation() {
        Template t = parse("a={{ b }}!");
        assertEquals(3, t.body().size());
        assertInstanceOf(Template.Text.class, t.body().get(0));
        assertInstanceOf(Template.Output.class, t.body().get(1));
        assertInstanceOf(Template.Text.class, t.body().get(2));
        assertEquals("a=", ((Template.Text) t.body().get(0)).value());
        assertEquals("!", ((Template.Text) t.body().get(2)).value());
    }

    @Test
    void simpleIf() {
        Template t = parse("{% if a %}x{% endif %}");
        Template.IfStmt s = (Template.IfStmt) t.body().get(0);
        assertEquals(1, s.branches().size());
        assertEquals("a", ((Expr.Ident) s.branches().get(0).cond()).name());
        Template.Text body = (Template.Text) s.branches().get(0).body().get(0);
        assertEquals("x", body.value());
        assertTrue(s.elseBody().isEmpty());
    }

    @Test
    void ifElse() {
        Template t = parse("{% if a %}x{% else %}y{% endif %}");
        Template.IfStmt s = (Template.IfStmt) t.body().get(0);
        assertEquals(1, s.branches().size());
        assertEquals("y", ((Template.Text) s.elseBody().get(0)).value());
    }

    @Test
    void ifElifElse() {
        Template t = parse("{% if a %}1{% elif b %}2{% elif c %}3{% else %}4{% endif %}");
        Template.IfStmt s = (Template.IfStmt) t.body().get(0);
        assertEquals(3, s.branches().size());
        assertEquals("a", ((Expr.Ident) s.branches().get(0).cond()).name());
        assertEquals("b", ((Expr.Ident) s.branches().get(1).cond()).name());
        assertEquals("c", ((Expr.Ident) s.branches().get(2).cond()).name());
        assertEquals("4", ((Template.Text) s.elseBody().get(0)).value());
    }

    @Test
    void simpleFor() {
        Template t = parse("{% for i in xs %}{{ i }}{% endfor %}");
        Template.ForStmt f = (Template.ForStmt) t.body().get(0);
        assertEquals(List.of("i"), f.targets());
        assertEquals("xs", ((Expr.Ident) f.iterable()).name());
        assertNull(f.filter());
        assertEquals(1, f.body().size());
    }

    @Test
    void forWithTupleUnpack() {
        Template t = parse("{% for k, v in cohort.items() %}{{ k }}{% endfor %}");
        Template.ForStmt f = (Template.ForStmt) t.body().get(0);
        assertEquals(List.of("k", "v"), f.targets());
        assertInstanceOf(Expr.Call.class, f.iterable());
    }

    @Test
    void forWithFilter() {
        Template t = parse("{% for k, v in cohort.items() if k != 'enabled' %}x{% endfor %}");
        Template.ForStmt f = (Template.ForStmt) t.body().get(0);
        assertNotNull(f.filter());
        Expr.Binary b = (Expr.Binary) f.filter();
        assertEquals("!=", b.op());
    }

    @Test
    void forWithElse() {
        Template t = parse("{% for x in xs %}A{% else %}B{% endfor %}");
        Template.ForStmt f = (Template.ForStmt) t.body().get(0);
        assertEquals(1, f.body().size());
        assertEquals("B", ((Template.Text) f.elseBody().get(0)).value());
    }

    @Test
    void setStatement() {
        Template t = parse("{% set x = 1 + 2 %}");
        Template.SetStmt s = (Template.SetStmt) t.body().get(0);
        assertEquals("x", s.target());
        assertInstanceOf(Expr.Binary.class, s.value());
    }

    @Test
    void rawBlockEmitsBodyVerbatim() {
        Template t = parse("a{% raw %}{{ verbatim }}{% endraw %}b");
        // Three text nodes: "a", "{{ verbatim }}", "b" — raw/endraw are skipped.
        long texts = t.body().stream().filter(n -> n instanceof Template.Text).count();
        assertEquals(3, texts);
        assertEquals("{{ verbatim }}", ((Template.Text) t.body().get(1)).value());
    }

    @Test
    void nestedIfAndFor() {
        Template t = parse(
            "{% for x in xs %}{% if x %}{{ x }}{% else %}-{% endif %}{% endfor %}");
        Template.ForStmt f = (Template.ForStmt) t.body().get(0);
        assertInstanceOf(Template.IfStmt.class, f.body().get(0));
    }

    @Test
    void complexInterpolationFromCorpus() {
        Template t = parse(
            "{{ format_datetime( max(config.get('replication_start_date',"
                + "(now_utc() - duration('P730D')).strftime('%Y-%m-%dT%H:%M:%SZ') ),"
                + "(now_utc() - duration('P730D')).strftime('%Y-%m-%dT%H:%M:%SZ')),"
                + "'%Y-%m-%dT%H:%M:%SZ') }}");
        Template.Output o = (Template.Output) t.body().get(0);
        assertInstanceOf(Expr.Call.class, o.expr());
    }

    @Test
    void whitespaceControlTrimsAround() {
        Template t = parse("a   {{- x -}}   b");
        // RawText "a", Output, RawText "b" — surrounding whitespace trimmed.
        assertEquals("a", ((Template.Text) t.body().get(0)).value());
        assertInstanceOf(Template.Output.class, t.body().get(1));
        assertEquals("b", ((Template.Text) t.body().get(2)).value());
    }

    @Test
    void unterminatedIfThrows() {
        assertThrows(JinjaException.class, () -> parse("{% if a %}x"));
    }

    @Test
    void unterminatedForThrows() {
        assertThrows(JinjaException.class, () -> parse("{% for x in xs %}x"));
    }

    @Test
    void unknownStatementKeywordThrows() {
        assertThrows(JinjaException.class, () -> parse("{% banana %}x{% endbanana %}"));
    }

    @Test
    void parsesAllCorpusSamplesWithoutError() {
        String[] samples = {
            "{{ now_utc().strftime('%Y-%m-%dT%H:%M:%SZ') }}",
            "{{ stream_slice.cursor_slice.end_time }}",
            "{{ next_page_token['next_page_token'] or '0' }}",
            "{{ v | replace('%', '') | float if v is not none and v != \"\" else None }}",
            "{% if config[\"spreadsheet_id\"] | regex_search(\"^(https://.*)\") %}x{% endif %}",
            "{% for key, value in cohort.items() if key != 'enabled' %}x{% endfor %}",
            "{% set state_threshold = '2024-01-01' %}{{ state_threshold }}",
            "before {% if a %}A{% elif b %}B{% else %}C{% endif %} after"
        };
        for (String s : samples) {
            Template t = parse(s);
            assertNotNull(t);
        }
    }
}
