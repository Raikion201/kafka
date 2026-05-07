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
package org.apache.kafka.connect.manifest.codegen.runtime;

import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatetimeWindowHelperTest {

    private static final String ISO_FMT = "%Y-%m-%dT%H:%M:%S";
    private static final String DATE_FMT = "%Y-%m-%d";
    private static final String EPOCH_FMT = "%s";

    // ── computeWindowEnd ─────────────────────────────────────────────────────

    @Test
    void computeWindowEnd_dayStep_addsOneDay() {
        String start = "2022-01-01T00:00:00";
        String end = DatetimeWindowHelper.computeWindowEnd(start, ISO_FMT, "P1D");
        assertEquals("2022-01-02T00:00:00", end);
    }

    @Test
    void computeWindowEnd_monthStep() {
        String end = DatetimeWindowHelper.computeWindowEnd("2022-01-01T00:00:00", ISO_FMT, "P1M");
        assertEquals("2022-02-01T00:00:00", end);
    }

    @Test
    void computeWindowEnd_yearStep() {
        String end = DatetimeWindowHelper.computeWindowEnd("2022-01-01T00:00:00", ISO_FMT, "P1Y");
        assertEquals("2023-01-01T00:00:00", end);
    }

    @Test
    void computeWindowEnd_weekStep() {
        String end = DatetimeWindowHelper.computeWindowEnd("2022-01-01T00:00:00", ISO_FMT, "P1W");
        assertEquals("2022-01-08T00:00:00", end);
    }

    @Test
    void computeWindowEnd_capsAtNow() {
        // Start is yesterday — window of 1 year would exceed now, so result should be <= now
        ZonedDateTime yesterday = ZonedDateTime.now(ZoneOffset.UTC).minusDays(1);
        String start = yesterday.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
        String end = DatetimeWindowHelper.computeWindowEnd(start, ISO_FMT, "P1Y");
        assertNotNull(end);
        ZonedDateTime parsedEnd = DatetimeWindowHelper.parseDate(end, ISO_FMT);
        assertTrue(parsedEnd.isBefore(ZonedDateTime.now(ZoneOffset.UTC).plusSeconds(2)));
    }

    @Test
    void computeWindowEnd_futureStart_returnsNull() {
        String futureStart = "2099-01-01T00:00:00";
        assertNull(DatetimeWindowHelper.computeWindowEnd(futureStart, ISO_FMT, "P1D"));
    }

    @Test
    void computeWindowEnd_nullStep_returnsNull() {
        assertNull(DatetimeWindowHelper.computeWindowEnd("2022-01-01T00:00:00", ISO_FMT, null));
    }

    @Test
    void computeWindowEnd_epochFormat_dayStep() {
        // 2022-01-01T00:00:00 UTC = epoch 1640995200
        String end = DatetimeWindowHelper.computeWindowEnd("1640995200", EPOCH_FMT, "P1D");
        assertNotNull(end);
        long endEpoch = Long.parseLong(end);
        assertEquals(1640995200L + 86400L, endEpoch);
    }

    @Test
    void computeWindowEnd_dateFormat_monthStep() {
        String end = DatetimeWindowHelper.computeWindowEnd("2022-01-01", DATE_FMT, "P1M");
        assertEquals("2022-02-01", end);
    }

    // ── advanceCursor ────────────────────────────────────────────────────────

    @Test
    void advanceCursor_addsMicrosecond() {
        // PT0.000001S = 1 microsecond = 1000 nanoseconds
        String advanced = DatetimeWindowHelper.advanceCursor("2022-01-02T00:00:00", ISO_FMT, "PT0.000001S");
        // One microsecond added — should still round to same second in this format
        assertNotNull(advanced);
        assertEquals("2022-01-02T00:00:00", advanced); // sub-second, truncated by format
    }

    @Test
    void advanceCursor_addsOneSecond() {
        String advanced = DatetimeWindowHelper.advanceCursor("2022-01-02T00:00:00", ISO_FMT, "PT1S");
        assertEquals("2022-01-02T00:00:01", advanced);
    }

    @Test
    void advanceCursor_nullGranularity_unchanged() {
        String advanced = DatetimeWindowHelper.advanceCursor("2022-01-02T00:00:00", ISO_FMT, null);
        assertEquals("2022-01-02T00:00:00", advanced);
    }

    @Test
    void advanceCursor_emptyGranularity_unchanged() {
        String advanced = DatetimeWindowHelper.advanceCursor("2022-01-02T00:00:00", ISO_FMT, "");
        assertEquals("2022-01-02T00:00:00", advanced);
    }

    // ── parseDuration ────────────────────────────────────────────────────────

    @Test
    void parseDuration_period_P30D() {
        ZonedDateTime base = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        ZonedDateTime result = base.plus(DatetimeWindowHelper.parseDuration("P30D"));
        assertEquals(ZonedDateTime.of(2022, 1, 31, 0, 0, 0, 0, ZoneOffset.UTC), result);
    }

    @Test
    void parseDuration_period_P1Y() {
        ZonedDateTime base = ZonedDateTime.of(2022, 3, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        ZonedDateTime result = base.plus(DatetimeWindowHelper.parseDuration("P1Y"));
        assertEquals(2023, result.getYear());
        assertEquals(3, result.getMonthValue());
    }

    @Test
    void parseDuration_duration_PT1H() {
        ZonedDateTime base = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        ZonedDateTime result = base.plus(DatetimeWindowHelper.parseDuration("PT1H"));
        assertEquals(1, result.getHour());
    }

    @Test
    void parseDuration_microSecond() {
        ZonedDateTime base = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        ZonedDateTime result = base.plus(DatetimeWindowHelper.parseDuration("PT0.000001S"));
        assertEquals(1000, result.getNano()); // 1 microsecond = 1000 nanoseconds
    }

    @Test
    void parseDuration_combined_P1DT6H() {
        ZonedDateTime base = ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        ZonedDateTime result = base.plus(DatetimeWindowHelper.parseDuration("P1DT6H"));
        assertEquals(ZonedDateTime.of(2022, 1, 2, 6, 0, 0, 0, ZoneOffset.UTC), result);
    }

    // ── toJavaFormatter / round-trip ──────────────────────────────────────────

    @Test
    void roundTrip_isoWithMillis() {
        String input = "2022-01-15T08:30:45";
        ZonedDateTime parsed = DatetimeWindowHelper.parseDate(input, ISO_FMT);
        String formatted = DatetimeWindowHelper.formatDate(parsed, ISO_FMT);
        assertEquals(input, formatted);
    }

    @Test
    void roundTrip_dateOnly() {
        String input = "2022-06-20";
        ZonedDateTime parsed = DatetimeWindowHelper.parseDate(input, DATE_FMT);
        String formatted = DatetimeWindowHelper.formatDate(parsed, DATE_FMT);
        assertEquals(input, formatted);
    }

    @Test
    void roundTrip_epochSeconds() {
        String input = "1640995200";
        ZonedDateTime parsed = DatetimeWindowHelper.parseDate(input, EPOCH_FMT);
        String formatted = DatetimeWindowHelper.formatDate(parsed, EPOCH_FMT);
        assertEquals(input, formatted);
    }

    @Test
    void parseDate_epochFormat_dateStringFallback() {
        // Manifests like delighted/intercom use cursor_datetime_formats: ["%s"] but the
        // initial config value is a human-readable date.  parseDate must not throw.
        ZonedDateTime result = DatetimeWindowHelper.parseDate("2020-01-01", EPOCH_FMT);
        assertNotNull(result);
        assertEquals(2020, result.getYear());
        assertEquals(1, result.getMonthValue());
        assertEquals(1, result.getDayOfMonth());
    }
}
