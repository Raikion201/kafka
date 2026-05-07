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

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JinjaRendererTest {

    private static Map<String, Object> ctx() {
        return new LinkedHashMap<>();
    }

    // ── plain literal short-circuit ───────────────────────────────────────────

    @Test
    void rendersPlainLiteralVerbatim() {
        assertEquals("https://api.example.com/v1", JinjaRenderer.render("https://api.example.com/v1", ctx()));
    }

    @Test
    void nullTemplateReturnsEmpty() {
        assertEquals("", JinjaRenderer.render(null, ctx()));
        assertEquals("", JinjaRenderer.renderLenient(null, ctx()));
    }

    @Test
    void hasJinjaSyntaxRecognisesBraces() {
        assertTrue(JinjaRenderer.hasJinjaSyntax("hello {{ name }}"));
        assertTrue(JinjaRenderer.hasJinjaSyntax("{% if x %}y{% endif %}"));
        assertTrue(!JinjaRenderer.hasJinjaSyntax("plain text"));
        assertTrue(!JinjaRenderer.hasJinjaSyntax(""));
        assertTrue(!JinjaRenderer.hasJinjaSyntax(null));
    }

    // ── variable interpolation ────────────────────────────────────────────────

    @Test
    void interpolatesContextVariable() {
        Map<String, Object> c = ctx();
        Map<String, Object> config = new HashMap<>();
        config.put("api_key", "secret123");
        c.put("config", config);
        assertEquals("Bearer secret123", JinjaRenderer.render("Bearer {{ config.api_key }}", c));
    }

    @Test
    void unknownVariableRendersEmptyByDefault() {
        // jinjava with failOnUnknownTokens(false) treats unknowns as empty.
        assertEquals("x=", JinjaRenderer.render("x={{ missing }}", ctx()));
    }

    // ── strict vs lenient ─────────────────────────────────────────────────────

    @Test
    void strictRenderThrowsOnSyntaxError() {
        assertThrows(ConnectException.class,
            () -> JinjaRenderer.render("{{ unclosed", ctx()));
    }

    @Test
    void lenientRenderSwallowsErrors() {
        // Should not throw even if jinjava records an error.
        String out = JinjaRenderer.renderLenient("{{ unclosed", ctx());
        assertNotNull(out);
    }

    // ── Airbyte functions ─────────────────────────────────────────────────────

    @Test
    void nowUtcIsIso8601() {
        String out = JinjaRenderer.render("{{ now_utc() }}", ctx());
        assertNotNull(OffsetDateTime.parse(out));
    }

    @Test
    void nowUtcStrftime_formatsWithPythonPattern() {
        String out = JinjaRenderer.render("{{ now_utc().strftime('%Y-%m-%d') }}", ctx());
        assertNotNull(out);
        // result must be a valid date string (4-digit year, month, day)
        assertTrue(out.matches("\\d{4}-\\d{2}-\\d{2}"), "strftime must produce a date string: " + out);
    }

    @Test
    void nowUtcMinusDuration_strftime() {
        // (now_utc() - duration('PT23H')).strftime(fmt) — tests arithmetic + method call
        String out = JinjaRenderer.render(
            "{{ (now_utc() - duration('PT23H')).strftime('%Y-%m-%dT%H:%M:%S') }}", ctx());
        assertNotNull(out);
        assertTrue(out.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}"),
            "arithmetic + strftime must produce a datetime string: " + out);
    }

    @Test
    void formatDatetimeAcceptsAirbyteDateTime() {
        // format_datetime(now_utc() - duration('P1D'), fmt) — AirbyteDateTime as first arg
        String out = JinjaRenderer.render(
            "{{ format_datetime(now_utc().minus(duration('P1D')), '%Y-%m-%d') }}", ctx());
        assertNotNull(out);
        assertTrue(out.matches("\\d{4}-\\d{2}-\\d{2}"),
            "format_datetime must accept AirbyteDateTime: " + out);
    }

    @Test
    void rewriteNowUtcArithmetic_rewritesMinusToMethodCall() {
        String original = "{{ (now_utc() - duration('PT23H')).strftime('%Y-%m-%d') }}";
        String rewritten = JinjaRenderer.rewriteNowUtcArithmetic(original);
        assertEquals("{{ (now_utc().minus(duration('PT23H'))).strftime('%Y-%m-%d') }}", rewritten);
    }

    @Test
    void todayUtcIsoDate() {
        String out = JinjaRenderer.render("{{ today_utc() }}", ctx());
        assertNotNull(LocalDate.parse(out));
    }

    @Test
    void todayWithTimezoneRendersDate() {
        String out = JinjaRenderer.render("{{ today_with_timezone('America/Los_Angeles') }}", ctx());
        assertNotNull(LocalDate.parse(out));
    }

    @Test
    void timestampOfFixedDate() {
        // 2024-01-01T00:00:00Z = 1704067200
        String out = JinjaRenderer.render("{{ timestamp('2024-01-01T00:00:00Z') }}", ctx());
        assertEquals("1704067200", out);
    }

    @Test
    void timestampOfNumberPassesThrough() {
        String out = JinjaRenderer.render("{{ timestamp(42) }}", ctx());
        assertEquals("42", out);
    }

    @Test
    void strToDatetimeRoundTrip() {
        String out = JinjaRenderer.render(
            "{{ format_datetime(str_to_datetime('2024-06-15T12:34:56Z'), '%Y-%m-%d') }}", ctx());
        assertEquals("2024-06-15", out);
    }

    @Test
    void formatDatetimeWithInputFormat() {
        // Airbyte: format_datetime(dt, output_fmt, input_fmt) — input_fmt parses dt.
        String out = JinjaRenderer.render(
            "{{ format_datetime('15-06-2024', '%Y/%m/%d', '%d-%m-%Y') }}", ctx());
        assertEquals("2024/06/15", out);
    }

    @Test
    void dayDeltaPositive() {
        // Default Airbyte format is %Y-%m-%dT%H:%M:%S.%f%z (note %z = +0000, no colon).
        LocalDate expected = LocalDate.now(ZoneOffset.UTC).plusDays(7);
        String out = JinjaRenderer.render("{{ day_delta(7) }}", ctx());
        assertTrue(out.startsWith(expected.toString()), "expected prefix " + expected + " in " + out);
        assertTrue(out.endsWith("+0000"), "expected +0000 suffix in " + out);
    }

    @Test
    void dayDeltaCustomFormat() {
        String out = JinjaRenderer.render("{{ day_delta(0, '%Y-%m-%d') }}", ctx());
        assertEquals(LocalDate.now(ZoneOffset.UTC).toString(), out);
    }

    @Test
    void durationParsesIsoString() {
        // Airbyte's duration() returns a TemporalAmount usable in Java composition.
        // EL doesn't natively add ZonedDateTime + TemporalAmount, so verify the
        // function itself rather than composition.
        java.time.temporal.TemporalAmount d = AirbyteJinjaFunctions.duration("P1D");
        assertNotNull(d);
        assertEquals(java.time.Period.ofDays(1), d);
    }

    @Test
    void maxOfPicksLarger() {
        assertEquals("9", JinjaRenderer.render("{{ max(3, 9) }}", ctx()));
        assertEquals("b", JinjaRenderer.render("{{ max('a', 'b') }}", ctx()));
    }

    @Test
    void minOfPicksSmaller() {
        assertEquals("3", JinjaRenderer.render("{{ min(3, 9) }}", ctx()));
    }

    @Test
    void sanitizeUrlPercentEncodes() {
        String out = JinjaRenderer.render("{{ sanitize_url('hello world & friends') }}", ctx());
        assertEquals("hello+world+%26+friends", out);
    }

    @Test
    void camelToSnakeCase() {
        assertEquals("camel_case_string",
            JinjaRenderer.render("{{ camel_case_to_snake_case('CamelCaseString') }}", ctx()));
    }

    @Test
    void generateUuidProducesValidUuid() {
        String out = JinjaRenderer.render("{{ generate_uuid() }}", ctx());
        java.util.UUID.fromString(out);
    }

    // ── Airbyte filters ───────────────────────────────────────────────────────

    @Test
    void hashFilterMd5Default() {
        // md5("hello") = 5d41402abc4b2a76b9719d911017c592
        String out = JinjaRenderer.render("{{ 'hello' | hash }}", ctx());
        assertEquals("5d41402abc4b2a76b9719d911017c592", out);
    }

    @Test
    void hashFilterSha256() {
        // sha256("hello") = 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
        String out = JinjaRenderer.render("{{ 'hello' | hash('sha256') }}", ctx());
        assertEquals("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", out);
    }

    @Test
    void hashFilterWithSalt() {
        String unsalted = JinjaRenderer.render("{{ 'hello' | hash('md5') }}", ctx());
        String salted = JinjaRenderer.render("{{ 'hello' | hash('md5', 'pepper') }}", ctx());
        assertTrue(!unsalted.equals(salted));
    }

    @Test
    void hmacFilterSha256() {
        // hmac-sha256("hello", "key") via Python: hmac.new(b"key", b"hello", sha256).hexdigest()
        String out = JinjaRenderer.render("{{ 'hello' | hmac('key') }}", ctx());
        assertEquals("9307b3b915efb5171ff14d8cb55fbcc798c6c0ef1456d66ded1a6aa723a58b7b", out);
    }

    @Test
    void regexSearchReturnsFirstGroup() {
        String out = JinjaRenderer.render(
            "{{ 'order-12345-shipped' | regex_search('order-(\\\\d+)') }}", ctx());
        assertEquals("12345", out);
    }

    @Test
    void regexSearchNoMatchReturnsEmpty() {
        String out = JinjaRenderer.render(
            "{{ 'no numbers' | regex_search('(\\\\d+)') }}", ctx());
        assertEquals("", out);
    }

    @Test
    void regexSearchNoCaptureReturnsMatch() {
        String out = JinjaRenderer.render(
            "{{ 'foo123bar' | regex_search('\\\\d+') }}", ctx());
        assertEquals("123", out);
    }

    @Test
    void base64EncodeDecodeRoundTrip() {
        String enc = JinjaRenderer.render("{{ 'hello world' | base64encode }}", ctx());
        assertEquals("aGVsbG8gd29ybGQ=", enc);
        Map<String, Object> c = ctx();
        c.put("v", enc);
        assertEquals("hello world", JinjaRenderer.render("{{ v | base64decode }}", c));
    }

    @Test
    void base64BinasciiDecodePreservesBytes() {
        // Encode bytes 0xff 0x00 0x7f as base64, then binascii_decode should round-trip via Latin-1.
        byte[] raw = new byte[] {(byte) 0xff, 0x00, 0x7f};
        String b64 = java.util.Base64.getEncoder().encodeToString(raw);
        Map<String, Object> c = ctx();
        c.put("v", b64);
        String out = JinjaRenderer.render("{{ v | base64binascii_decode }}", c);
        assertEquals(3, out.length());
        assertEquals(0xff, out.charAt(0) & 0xff);
        assertEquals(0x00, out.charAt(1) & 0xff);
        assertEquals(0x7f, out.charAt(2) & 0xff);
    }

    @Test
    void stringFilterCoercesToString() {
        assertEquals("42", JinjaRenderer.render("{{ 42 | string }}", ctx()));
    }

    @Test
    void regexReplaceUsesJinjavaBuiltin() {
        // jinjava ships regex_replace; just ensure it works through our renderer.
        String out = JinjaRenderer.render(
            "{{ 'foo123bar' | regex_replace('\\\\d+', 'X') }}", ctx());
        assertEquals("fooXbar", out);
    }

    // ── compound use ──────────────────────────────────────────────────────────

    @Test
    void chainedFiltersAndFunctions() {
        Map<String, Object> c = ctx();
        c.put("now", "2024-06-15T00:00:00Z");
        String out = JinjaRenderer.render(
            "{{ format_datetime(str_to_datetime(now), '%Y/%m/%d') }}", c);
        assertEquals("2024/06/15", out);
    }

    @Test
    void formatDatetimeOnIsoStringWithoutInputFormat() {
        ZonedDateTime ref = ZonedDateTime.of(LocalDateTime.of(2024, 1, 2, 3, 4, 5), ZoneOffset.UTC);
        String iso = ref.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        Map<String, Object> c = ctx();
        c.put("ts", iso);
        String out = JinjaRenderer.render("{{ format_datetime(ts, '%Y-%m-%dT%H:%M:%S') }}", c);
        assertEquals("2024-01-02T03:04:05", out);
    }

    // ── |float and |int filters ───────────────────────────────────────────────

    @Test
    void floatFilter_fromString() {
        Map<String, Object> c = ctx();
        c.put("lat", "37.7749");
        String out = JinjaRenderer.render("{{ lat|float }}", c);
        assertEquals("37.7749", out);
    }

    @Test
    void floatFilter_comparison() {
        Map<String, Object> c = ctx();
        c.put("lat", "45.0");
        String out = JinjaRenderer.render("{% if lat|float <= 90.0 %}yes{% else %}no{% endif %}", c);
        assertEquals("yes", out);
    }

    @Test
    void intFilter_fromString() {
        Map<String, Object> c = ctx();
        c.put("page", "42");
        String out = JinjaRenderer.render("{{ page|int }}", c);
        assertEquals("42", out);
    }

    @Test
    void intFilter_fromFloat() {
        Map<String, Object> c = ctx();
        c.put("val", 3.9);
        String out = JinjaRenderer.render("{{ val|int }}", c);
        assertEquals("3", out);
    }

    // ── Python-style str.join() pre-processor ─────────────────────────────────

    @Test
    void rewritePythonJoin_singleQuoteSeparator() {
        assertEquals("(tags)|join(',')", JinjaRenderer.rewritePythonJoin("','.join(tags)"));
    }

    @Test
    void rewritePythonJoin_doubleQuoteSeparator() {
        assertEquals("(tags)|join(\",\")", JinjaRenderer.rewritePythonJoin("\",\".join(tags)"));
    }

    @Test
    void rewritePythonJoin_insideJinjaBlock() {
        Map<String, Object> c = ctx();
        c.put("tags", java.util.List.of("a", "b", "c"));
        String out = JinjaRenderer.render("{{ ','.join(tags) }}", c);
        assertEquals("a,b,c", out);
    }

    @Test
    void rewritePythonJoin_spaceSeparatorInsideTemplate() {
        Map<String, Object> c = ctx();
        c.put("words", java.util.List.of("hello", "world"));
        String out = JinjaRenderer.render("{{ ' '.join(words) }}", c);
        assertEquals("hello world", out);
    }

    @Test
    void rewritePythonJoin_noJoin_unchanged() {
        assertEquals("{{ config['key'] }}", JinjaRenderer.rewritePythonJoin("{{ config['key'] }}"));
    }

    @Test
    void nowUtcMinusStrftime() {
        // Verify the sub-expression works in isolation
        String tpl = "{{ (now_utc().minus(duration('P730D'))).strftime('%Y-%m-%dT%H:%M:%SZ') }}";
        String out = JinjaRenderer.render(tpl, Map.of());
        assertNotNull(out);
        assertTrue(!out.isEmpty());
    }

    @Test
    void formatDatetimeWithMaxTwoStrings() {
        String tpl = "{{ format_datetime(max('2022-01-01T00:00:00Z', '2020-01-01T00:00:00Z'), '%Y-%m-%dT%H:%M:%SZ') }}";
        String out = JinjaRenderer.render(tpl, Map.of());
        assertEquals("2022-01-01T00:00:00Z", out);
    }

    @Test
    void maxOfStringAndNowUtcStrftime() {
        String tpl = "{{ max('2022-01-01T00:00:00Z', (now_utc().minus(duration('P730D'))).strftime('%Y-%m-%dT%H:%M:%SZ')) }}";
        String out = JinjaRenderer.render(tpl, Map.of());
        assertNotNull(out);
        assertTrue(!out.isEmpty());
    }

    @Test
    void formatDatetimeOfNowUtcStrftime() {
        // format_datetime with a single method-chain arg (not nested in max)
        String tpl = "{{ format_datetime(now_utc().strftime('%Y-%m-%dT%H:%M:%SZ'), '%Y-%m-%dT%H:%M:%SZ') }}";
        String out = JinjaRenderer.render(tpl, Map.of());
        assertNotNull(out);
        assertTrue(!out.isEmpty());
    }

    @Test
    void formatDatetimeWithMaxStringAndNowUtc() {
        String tpl = "{{ format_datetime(max('2022-01-01T00:00:00Z', (now_utc().minus(duration('P730D'))).strftime('%Y-%m-%dT%H:%M:%SZ')), '%Y-%m-%dT%H:%M:%SZ') }}";
        String out = JinjaRenderer.render(tpl, Map.of());
        assertNotNull(out);
        assertTrue(!out.isEmpty());
    }

    @Test
    void formatDatetimeWithMaxNowUtcComplexTemplate() {
        String tpl = "{{ format_datetime( max(((config['replication_start_date']) if 'replication_start_date' in config else ((now_utc()).minus(duration('P730D'))).strftime('%Y-%m-%dT%H:%M:%SZ') ), (now_utc().minus(duration('P730D'))).strftime('%Y-%m-%dT%H:%M:%SZ')), '%Y-%m-%dT%H:%M:%SZ') }}";
        Map<String, Object> c = ctx();
        c.put("replication_start_date", "2022-01-01T00:00:00Z");
        String out = JinjaRenderer.render(tpl, c);
        assertNotNull(out);
        assertTrue(!out.isEmpty());
    }

    // ── rewriteFormatDatetime ─────────────────────────────────────────────────

    @Test
    void rewriteFormatDatetime_twoArgs() {
        assertEquals("{{ (ts) | format_datetime_filter('%Y-%m-%d') }}",
            JinjaRenderer.rewriteFormatDatetime("{{ format_datetime(ts, '%Y-%m-%d') }}"));
    }

    @Test
    void rewriteFormatDatetime_threeArgs() {
        assertEquals("{{ ('15-06-2024') | format_datetime_filter('%Y/%m/%d', '%d-%m-%Y') }}",
            JinjaRenderer.rewriteFormatDatetime("{{ format_datetime('15-06-2024', '%Y/%m/%d', '%d-%m-%Y') }}"));
    }

    @Test
    void rewriteFormatDatetime_nestedParens() {
        assertEquals("{{ (max('a', 'b')) | format_datetime_filter('%Y') }}",
            JinjaRenderer.rewriteFormatDatetime("{{ format_datetime(max('a', 'b'), '%Y') }}"));
    }

    @Test
    void rewriteFormatDatetime_doesNotTouchFilterForm() {
        // format_datetime_filter( does not match the marker format_datetime( so it's left alone
        String in = "{{ (ts) | format_datetime_filter('%Y') }}";
        assertEquals(in, JinjaRenderer.rewriteFormatDatetime(in));
    }

    // ── rewriteElseFunctionCall ───────────────────────────────────────────────

    @Test
    void rewriteElseFunctionCall_wrapsElseClause() {
        String in = "{{ X if cond else day_delta(-7, '%Y-%m-%d') }}";
        String out = JinjaRenderer.rewriteElseFunctionCall(in);
        assertEquals("{{ X if cond else (day_delta(-7, '%Y-%m-%d')) }}", out);
    }

    @Test
    void rewriteElseFunctionCall_noMatchOnElseIf() {
        String in = "{% if a %}x{% elif b %}y{% else %}z{% endif %}";
        assertEquals(in, JinjaRenderer.rewriteElseFunctionCall(in));
    }

    // ── or operator with strftime ─────────────────────────────────────────────

    @Test
    void orOperator_emptyFallsThrough() {
        Map<String, Object> c = ctx();
        c.put("start_date", "");
        String out = JinjaRenderer.render("{{ start_date or now_utc().strftime('%Y-%m-%d') }}", c);
        assertTrue(out.matches("\\d{4}-\\d{2}-\\d{2}"), "or fallthrough should produce date: " + out);
    }

    @Test
    void orOperator_missingKeyFallsThrough() {
        String out = JinjaRenderer.render("{{ config['start_date'] or now_utc().strftime('%Y-%m-%d') }}", ctx());
        assertTrue(out.matches("\\d{4}-\\d{2}-\\d{2}") || out.isEmpty(),
            "or with missing key: " + out);
    }

    @Test
    void orOperator_nestedConfigMapNullValue() {
        Map<String, Object> c = ctx();
        Map<String, Object> config = new HashMap<>();
        config.put("start_date", null); // null value in map
        c.put("config", config);
        String out = JinjaRenderer.render("{{ config['start_date'] or now_utc().strftime('%Y-%m-%d') }}", c);
        assertTrue(out.matches("\\d{4}-\\d{2}-\\d{2}"), "null config value should fall through: " + out);
    }

    @Test
    void orOperator_nestedConfigMapMissingKey() {
        Map<String, Object> c = ctx();
        c.put("config", new HashMap<>());
        String out = JinjaRenderer.render("{{ config['start_date'] or now_utc().strftime('%Y-%m-%d') }}", c);
        assertTrue(out.matches("\\d{4}-\\d{2}-\\d{2}"), "missing config key should fall through: " + out);
    }

    // ── String.split() support ────────────────────────────────────────────────

    @Test
    void stringSplitViaFilter() {
        Map<String, Object> c = ctx();
        c.put("s", "hello.world.foo");
        String out = JinjaRenderer.render("{{ s | split('.') | first }}", c);
        assertEquals("hello", out);
    }

    @Test
    void gnewsStyleDatetimeSplit() {
        // Replicates gnews pattern: ' '.join(day_delta(-7).split('.')[0].split('T'))
        // day_delta returns "YYYY-MM-DDTHH:MM:SS.ffffff+0000"; result should be "YYYY-MM-DD HH:MM:SS"
        String out = JinjaRenderer.render("{{ ' '.join(day_delta(-7).split('.')[0].split('T')) }}", ctx());
        assertNotNull(out);
        assertTrue(out.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"),
            "gnews split pattern must produce 'YYYY-MM-DD HH:MM:SS': " + out);
    }

    @Test
    void ouraStyleSplitWithTrailingIndexZero() {
        // Replicates oura pattern: config['end_datetime'].split('T')[0]
        // [0] comes immediately after .split('T') — must become |split('T')|first
        Map<String, Object> c = ctx();
        c.put("config", Map.of("end_datetime", "2024-03-15T12:00:00Z"));
        String out = JinjaRenderer.render("{{ config['end_datetime'].split('T')[0] }}", c);
        assertEquals("2024-03-15", out);
    }

    @Test
    void tiktokStyleComplexLhsDictGet() {
        // Replicates tiktok-marketing: (conditional_expr).get('auth_type', "")
        // credentials not in config → conditional evaluates to {} → .get returns ""
        Map<String, Object> c = ctx();
        c.put("config", new HashMap<>());  // no 'credentials' key
        String out = JinjaRenderer.render(
            "{{ ((config['credentials']) if 'credentials' in config else ({})).get('auth_type', 'default_type') }}",
            c);
        assertEquals("default_type", out);
    }

    @Test
    void tiktokStyleDictGetAfterIfKeyword() {
        // Regression: scanLhsExprEnd must NOT scan through spaces, so "sandbox-ads" if config.get(...)
        // rewrites config.get() correctly — not (" if config).get()"
        Map<String, Object> c = ctx();
        c.put("config", new HashMap<>());  // no 'credentials' key → sandbox-api resolves to "business-api"
        String out = JinjaRenderer.render(
            "{{ 'sandbox-ads' if config.get('credentials', {}).get('auth_type', '') == 'sandbox_access_token' else 'business-api' }}",
            c);
        assertEquals("business-api", out);
    }

    @Test
    void dictGetSimpleIdentifier() {
        // Original single-ident case still works after rewrite
        Map<String, Object> c = ctx();
        Map<String, Object> params = new HashMap<>();
        params.put("foo", "bar");
        c.put("params", params);
        String out = JinjaRenderer.render("{{ params.get('foo', 'missing') }}", c);
        assertEquals("bar", out);
        String outMissing = JinjaRenderer.render("{{ params.get('baz', 'missing') }}", c);
        assertEquals("missing", outMissing);
    }

    @Test
    void quickbooksStylePythonSlice() {
        // Replicates quickbooks pattern: ts[:-2] + ":" + ts[-2:]
        // Converts "+0000" (compact offset) to "+00:00" (colon-separated offset)
        Map<String, Object> c = ctx();
        Map<String, Object> slice = new HashMap<>();
        slice.put("start_time", "2024-01-15T10:30:45+0000");
        c.put("stream_slice", slice);
        String out = JinjaRenderer.render(
            "{{ stream_slice.start_time[:-2] + ':' + stream_slice.start_time[-2:] }}", c);
        assertEquals("2024-01-15T10:30:45+00:00", out);
    }
}
