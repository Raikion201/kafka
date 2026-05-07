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

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAmount;

/**
 * Runtime helper for DatetimeBasedCursor window-slicing arithmetic.
 *
 * <p>Airbyte's {@code DatetimeBasedCursor} with a {@code step} field divides the sync
 * range into fixed-size windows (ISO 8601 durations). Each poll fetches one window and
 * advances the cursor to the window end, so the next poll starts the next window.
 *
 * <p>Python source reference:
 * airbyte_cdk/sources/declarative/incremental/datetime_based_cursor.py
 * (DatetimeBasedCursor.stream_slices, DatetimeBasedCursor._partition_daterange)
 */
public final class DatetimeWindowHelper {

    private DatetimeWindowHelper() {
    }

    /**
     * Compute the end of the window that starts at {@code windowStart}.
     *
     * <p>Returns {@code windowStart + stepDuration}, but capped at the current UTC time so
     * we do not request future data. If {@code windowStart} is already at or beyond now,
     * returns {@code null} (caller should skip the HTTP request).
     *
     * @param windowStart  cursor string (in {@code pythonFmt} format)
     * @param pythonFmt    Python strftime format (e.g. {@code %Y-%m-%dT%H:%M:%S})
     * @param step         ISO 8601 duration string (e.g. {@code P1M}, {@code P30D}, {@code PT1H})
     * @return formatted window-end string, or {@code null} when already caught up to now
     */
    public static String computeWindowEnd(String windowStart, String pythonFmt, String step) {
        if (windowStart == null || windowStart.isEmpty() || step == null || step.isEmpty()) {
            return null;
        }
        ZonedDateTime start = parseDate(windowStart, pythonFmt);
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        if (!start.isBefore(now)) {
            return null;
        }
        ZonedDateTime end = start.plus(parseDuration(step));
        if (end.isAfter(now)) {
            end = now;
        }
        return formatDate(end, pythonFmt);
    }

    /**
     * Advance the cursor past a completed window to avoid re-fetching the last instant.
     *
     * <p>Returns {@code windowEnd + granularity}. If {@code granularity} is null or empty,
     * returns {@code windowEnd} unchanged.
     *
     * @param windowEnd   the window-end string (in {@code pythonFmt} format)
     * @param pythonFmt   Python strftime format
     * @param granularity ISO 8601 duration (e.g. {@code PT0.000001S}, {@code PT1S}); may be null
     * @return advanced cursor string
     */
    public static String advanceCursor(String windowEnd, String pythonFmt, String granularity) {
        if (windowEnd == null || windowEnd.isEmpty()) {
            return windowEnd;
        }
        if (granularity == null || granularity.isEmpty()) {
            return windowEnd;
        }
        ZonedDateTime end = parseDate(windowEnd, pythonFmt);
        ZonedDateTime advanced = end.plus(parseDuration(granularity));
        return formatDate(advanced, pythonFmt);
    }

    // ── public helpers (called by generated connector tasks) ──────────────────

    /** Epoch used when start_datetime config key is absent/empty (mirrors Airbyte CDK default). */
    private static final ZonedDateTime EPOCH_FALLBACK =
        ZonedDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

    public static ZonedDateTime parseDate(String value, String pythonFmt) {
        if (value == null || value.isBlank()) {
            return EPOCH_FALLBACK;
        }
        if ("%s".equals(pythonFmt)) {
            return parseEpochSeconds(value.trim());
        }
        if ("%ms".equals(pythonFmt)) {
            return parseEpochMillis(value.trim());
        }
        return tryParseWithFormatter(value, toJavaFormatter(pythonFmt));
    }

    private static ZonedDateTime parseEpochSeconds(String value) {
        try {
            return Instant.ofEpochSecond(Long.parseLong(value)).atZone(ZoneOffset.UTC);
        } catch (NumberFormatException e) {
            return parseIsoFallback(value);
        }
    }

    private static ZonedDateTime parseEpochMillis(String value) {
        try {
            return Instant.ofEpochMilli(Long.parseLong(value)).atZone(ZoneOffset.UTC);
        } catch (NumberFormatException e) {
            return parseIsoFallback(value);
        }
    }

    // Tries ZonedDateTime → LocalDateTime → LocalDate → ISO fallback.
    private static ZonedDateTime tryParseWithFormatter(String value, DateTimeFormatter fmt) {
        try {
            return ZonedDateTime.parse(value, fmt);
        } catch (Exception e1) {
            try {
                return LocalDateTime.parse(value, fmt).atZone(ZoneOffset.UTC);
            } catch (Exception e2) {
                try {
                    return LocalDate.parse(value, fmt).atStartOfDay(ZoneOffset.UTC);
                } catch (Exception e3) {
                    return parseIsoFallback(value.trim());
                }
            }
        }
    }

    public static String formatDate(ZonedDateTime dt, String pythonFmt) {
        if (pythonFmt != null && pythonFmt.equals("%s")) {
            return String.valueOf(dt.toEpochSecond());
        }
        if (pythonFmt != null && pythonFmt.equals("%ms")) {
            return String.valueOf(dt.toInstant().toEpochMilli());
        }
        return dt.format(toJavaFormatter(pythonFmt));
    }

    /**
     * Parses an ISO 8601 duration string. Tries {@link Period} for pure date durations
     * ({@code PnYnMnD}) and {@link Duration} for pure time durations ({@code PTnHnMnS}).
     * Combined forms (e.g. {@code P1DT6H}) are parsed as {@link Duration} by converting
     * days to hours.
     */
    public static TemporalAmount parseDuration(String iso8601) {
        if (iso8601 == null || iso8601.isEmpty()) {
            throw new IllegalArgumentException("Duration string must not be empty");
        }
        // Pure time duration (no date component)
        if (iso8601.startsWith("PT") || iso8601.matches("P\\d+\\.\\d+S")) {
            return parseMicroDuration(iso8601);
        }
        // Pure date duration — no T component
        if (!iso8601.contains("T")) {
            return Period.parse(iso8601);
        }
        // Combined (P1DT6H etc) — decompose
        int tIdx = iso8601.indexOf('T');
        Period datePart = Period.parse("P" + iso8601.substring(1, tIdx));
        Duration timePart = Duration.parse("PT" + iso8601.substring(tIdx + 1));
        // Convert Period to an approximate Duration (months = 30d, years = 365d) for addition
        long extraSeconds = (long) datePart.getYears() * 365L * 86400L
            + (long) datePart.getMonths() * 30L * 86400L
            + (long) datePart.getDays() * 86400L;
        return timePart.plusSeconds(extraSeconds);
    }

    /**
     * Converts a Python strftime format string to a Java {@link DateTimeFormatter}.
     *
     * <p>Covers the subset used by Airbyte manifests in the corpus:
     * {@code %Y %m %d %H %M %S %f %z %Z}.
     *
     * <p>The returned formatter is for <em>formatting</em> only — it strictly follows the
     * pattern. For lenient parsing (tolerating missing timezone), use {@link #parseDate} which
     * falls back through ZonedDateTime → LocalDateTime → LocalDate.
     */
    static DateTimeFormatter toJavaFormatter(String pythonFmt) {
        if (pythonFmt == null || pythonFmt.isEmpty()) {
            return DateTimeFormatter.ISO_OFFSET_DATE_TIME;
        }
        // Substitute Python strftime codes → Java DateTimeFormatter pattern tokens.
        // Replace %z first (two chars) before single-char replacements to avoid double-replace.
        String java = pythonFmt
            .replace("%Y", "yyyy")
            .replace("%m", "MM")
            .replace("%d", "dd")
            .replace("%H", "HH")
            .replace("%M", "mm")
            .replace("%S", "ss")
            .replace("%z", "XXX")      // +00:00 / Z form (ISO 8601 offset)
            .replace("%Z", "z")        // timezone name
            .replace("%f", "SSSSSS");  // microseconds (6 digits)
        // T is a reserved letter in Java DTF patterns — quote it as a literal.
        java = java.replace("T", "'T'");
        // Strip any fractional-seconds suffix from the Python format that may appear as
        // ".%f+00:00" — Java DateTimeFormatter needs these sections handled separately.
        // Split around XXX so we can make the offset optional for formatting.
        boolean hasOffset = java.contains("XXX") || java.contains("VV") || java.contains("z");
        if (hasOffset) {
            // When the format explicitly includes an offset specifier, use it as-is.
            return new DateTimeFormatterBuilder()
                .appendPattern(java)
                .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                .parseDefaulting(ChronoField.NANO_OF_SECOND, 0)
                .toFormatter()
                .withZone(ZoneOffset.UTC);
        }
        // No offset in format — use a plain formatter and let parseDate handle zone defaulting.
        return new DateTimeFormatterBuilder()
            .appendPattern(java)
            .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
            .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
            .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
            .parseDefaulting(ChronoField.NANO_OF_SECOND, 0)
            .toFormatter();
    }

    // ── private ────────────────────────────────────────────────────────────────

    /** Parses a human-readable date/datetime string using common ISO formats. */
    private static ZonedDateTime parseIsoFallback(String value) {
        try {
            return ZonedDateTime.parse(value);
        } catch (Exception e1) {
            try {
                return LocalDateTime.parse(value).atZone(ZoneOffset.UTC);
            } catch (Exception e2) {
                return LocalDate.parse(value).atStartOfDay(ZoneOffset.UTC);
            }
        }
    }

    /** Handles sub-second durations like {@code PT0.000001S} that {@link Duration#parse} rejects. */
    private static TemporalAmount parseMicroDuration(String iso8601) {
        // PT0.000001S → extract numeric seconds with fractional part
        java.util.regex.Matcher m = java.util.regex.Pattern
            .compile("PT(\\d+(?:\\.\\d+)?)S")
            .matcher(iso8601);
        if (m.matches()) {
            double secs = Double.parseDouble(m.group(1));
            long nanos = Math.round(secs * 1_000_000_000L);
            return Duration.ofNanos(nanos);
        }
        return Duration.parse(iso8601);
    }
}
