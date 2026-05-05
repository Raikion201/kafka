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

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatetimesTest {

    private final Evaluator ev = new Evaluator();

    private Object eval(String expr) {
        return ev.evaluateExpression(expr, Map.of());
    }

    private Object eval(String expr, Map<String, Object> ctx) {
        return ev.evaluateExpression(expr, ctx);
    }

    @Test
    void nowAndTodayUtcReturnTemporals() {
        Object now = eval("{{ now_utc() }}");
        assertTrue(now instanceof ZonedDateTime, "now_utc returns ZonedDateTime");
        Object today = eval("{{ today_utc() }}");
        assertTrue(today instanceof LocalDate, "today_utc returns LocalDate");
    }

    @Test
    void todayWithTimezone() {
        Object d = eval("{{ today_with_timezone('Pacific/Tarawa') }}");
        assertTrue(d instanceof LocalDate);
    }

    @Test
    void durationParsesIso8601() {
        Object d = eval("{{ duration('P1D') }}");
        assertTrue(d instanceof Datetimes.IsoDuration);
        Datetimes.IsoDuration iso = (Datetimes.IsoDuration) eval("{{ duration('P1Y2M3DT4H5M6S') }}");
        assertEquals(1, iso.period().getYears());
        assertEquals(2, iso.period().getMonths());
        assertEquals(3, iso.period().getDays());
        assertEquals(4 * 3600 + 5 * 60 + 6, iso.duration().getSeconds());
    }

    @Test
    void durationArithmeticOnTemporals() {
        ZonedDateTime base = ZonedDateTime.of(2024, 6, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        Object plus = eval("{{ d + duration('P1D') }}", Map.of("d", base));
        assertEquals(base.plusDays(1), plus);
        Object minus = eval("{{ d - duration('P30D') }}", Map.of("d", base));
        assertEquals(base.minusDays(30), minus);
    }

    @Test
    void formatDatetimeFromString() {
        assertEquals("2024-06-01",
            eval("{{ format_datetime('2024-06-01T10:00:00Z', '%Y-%m-%d') }}"));
        assertEquals("20240601",
            eval("{{ format_datetime('2024-06-01T10:00:00Z', '%Y%m%d') }}"));
    }

    @Test
    void formatDatetimeFromEpoch() {
        // 1717200000 = 2024-06-01T00:00:00Z
        Object out = eval("{{ format_datetime(1717200000, '%Y-%m-%d') }}");
        assertEquals("2024-06-01", out);
    }

    @Test
    void formatDatetimeEpochToken() {
        Object out = eval("{{ format_datetime('2024-01-01T00:00:00Z', '%s') }}");
        assertEquals(Long.toString(1704067200L), out);
    }

    @Test
    void formatDatetimeEpochMicroseconds() {
        Object out = eval("{{ format_datetime('2024-01-01T00:00:00Z', '%epoch_microseconds') }}");
        assertEquals(Long.toString(1704067200_000_000L), out);
    }

    @Test
    void strftimeMethodOnDatetime() {
        ZonedDateTime z = ZonedDateTime.of(2024, 6, 1, 12, 30, 45, 123456000, ZoneOffset.UTC);
        assertEquals("2024-06-01", eval("{{ d.strftime('%Y-%m-%d') }}", Map.of("d", z)));
        assertEquals("12:30:45", eval("{{ d.strftime('%H:%M:%S') }}", Map.of("d", z)));
        assertEquals("123456", eval("{{ d.strftime('%f') }}", Map.of("d", z)));
        assertEquals("+0000", eval("{{ d.strftime('%z') }}", Map.of("d", z)));
    }

    @Test
    void timestampFromString() {
        assertEquals(1704067200L, eval("{{ timestamp('2024-01-01T00:00:00Z') }}"));
    }

    @Test
    void strToDatetime() {
        Object o = eval("{{ str_to_datetime('2024-06-01') }}");
        assertTrue(o instanceof ZonedDateTime);
        assertEquals(2024, ((ZonedDateTime) o).getYear());
    }

    @Test
    void dayDelta() {
        Object o = eval("{{ day_delta(0, '%Y-%m-%d') }}");
        assertNotNull(o);
        assertEquals(10, o.toString().length());
    }

    @Test
    void sanitizeUrl() {
        assertEquals("a%2Fb%26c", eval("{{ sanitize_url('a/b&c') }}"));
    }

    @Test
    void camelCaseToSnakeCase() {
        assertEquals("my_camel_case", eval("{{ camel_case_to_snake_case('MyCamelCase') }}"));
        assertEquals("simple", eval("{{ camel_case_to_snake_case('Simple') }}"));
    }

    @Test
    void generateUuid() {
        Object u = eval("{{ generate_uuid() }}");
        assertTrue(u instanceof String);
        assertEquals(36, u.toString().length());
    }

    @Test
    void corpusStartDateMaxPattern() {
        // Mirrors a manifest pattern: format_datetime(max(config.start_date, (now_utc() - duration('P30D'))), '%Y-%m-%d')
        Object out = ev.evaluateExpression(
            "{{ format_datetime(max(config.start_date, now_utc() - duration('P30D')), '%Y-%m-%d') }}",
            Map.of("config", Map.of("start_date", "2024-06-01T00:00:00Z")));
        assertNotNull(out);
        assertEquals(10, out.toString().length());
    }

    @Test
    void corpusStrftimePattern() {
        // {{ now_utc().strftime('%Y-%m-%d') }} – common manifest pattern
        Object out = eval("{{ now_utc().strftime('%Y-%m-%d') }}");
        assertNotNull(out);
        assertEquals(10, out.toString().length());
    }
}
