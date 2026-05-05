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

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * Datetime macros + ISO 8601 duration arithmetic mirrored on
 * {@code airbyte_cdk.sources.declarative.interpolation.macros}.
 *
 * <p>Registered functions:
 * {@code now_utc, today_utc, today_with_timezone, timestamp, str_to_datetime,
 * day_delta, duration, format_datetime, sanitize_url, camel_case_to_snake_case,
 * generate_uuid}. Wildcard {@code strftime} method on temporal objects so the
 * Python idiom {@code now_utc().strftime('%Y-%m-%d')} parses straight through.
 *
 * <p>{@link IsoDuration} is the pair (period, duration) returned from
 * {@link #parseIsoDuration(String)} so we can model {@code P1Y2M3DT4H} which
 * neither {@link Period} nor {@link Duration} represents alone.
 */
public final class Datetimes {

    private Datetimes() { }

    /** ISO-8601 duration that combines a calendar Period and a clock Duration. */
    public record IsoDuration(Period period, Duration duration) {
        public Temporal addTo(Temporal t) {
            Temporal a = period.isZero() ? t : period.addTo(t);
            return duration.isZero() ? a : duration.addTo(a);
        }

        public Temporal subtractFrom(Temporal t) {
            Temporal a = duration.isZero() ? t : duration.subtractFrom(t);
            return period.isZero() ? a : period.subtractFrom(a);
        }

        public IsoDuration negated() {
            return new IsoDuration(period.negated(), duration.negated());
        }
    }

    public static void install(Functions fns, Methods methods) {
        fns.register("now_utc", (a, k) -> ZonedDateTime.now(ZoneOffset.UTC));
        fns.register("today_utc", (a, k) -> LocalDate.now(ZoneOffset.UTC));
        fns.register("today_with_timezone", (a, k) ->
            LocalDate.now(ZoneId.of(asString(a.get(0)))));
        fns.register("timestamp", Datetimes::timestampFn);
        fns.register("str_to_datetime", (a, k) -> parseDatetimeFlexible(asString(a.get(0))));
        fns.register("day_delta", Datetimes::dayDeltaFn);
        fns.register("duration", (a, k) -> parseIsoDuration(asString(a.get(0))));
        fns.register("format_datetime", Datetimes::formatDatetimeFn);
        fns.register("sanitize_url", (a, k) ->
            java.net.URLEncoder.encode(asString(a.get(0)), java.nio.charset.StandardCharsets.UTF_8));
        fns.register("camel_case_to_snake_case", (a, k) -> camelToSnake(asString(a.get(0))));
        fns.register("generate_uuid", (a, k) -> UUID.randomUUID().toString());

        methods.register("*#strftime", (self, args, kw) ->
            pyStrftime(asTemporal(self), asString(args.get(0))));
        methods.register("*#isoformat", (self, args, kw) -> asTemporal(self).toString());
    }

    // ── functions ──────────────────────────────────────────────────────────

    private static Object timestampFn(List<Object> a, Map<String, Object> k) {
        Object v = a.get(0);
        if (v instanceof Number n) {
            return n.longValue();
        }
        ZonedDateTime z = parseDatetimeFlexible(asString(v));
        return z.toEpochSecond();
    }

    private static Object dayDeltaFn(List<Object> a, Map<String, Object> k) {
        long days = ((Number) a.get(0)).longValue();
        String fmt = a.size() > 1 ? asString(a.get(1)) : "%Y-%m-%dT%H:%M:%S.%f%z";
        ZonedDateTime z = ZonedDateTime.now(ZoneOffset.UTC).plusDays(days);
        return pyStrftime(z, fmt);
    }

    private static Object formatDatetimeFn(List<Object> a, Map<String, Object> k) {
        Object v = a.get(0);
        String fmt = asString(a.get(1));
        String inputFmt = a.size() > 2 ? asString(a.get(2)) : null;
        TemporalAccessor t = parseToTemporal(v, inputFmt);
        return pyStrftime(t, fmt);
    }

    private static TemporalAccessor parseToTemporal(Object v, String inputFmt) {
        if (v instanceof TemporalAccessor t) {
            return t;
        }
        if (v instanceof Number n) {
            return Instant.ofEpochSecond(n.longValue()).atZone(ZoneOffset.UTC);
        }
        String s = asString(v);
        if (inputFmt != null && !inputFmt.isEmpty()) {
            return parseWithPyFormat(s, inputFmt);
        }
        return parseDatetimeFlexible(s);
    }

    // ── parsing ────────────────────────────────────────────────────────────

    static ZonedDateTime parseDatetimeFlexible(String s) {
        String trimmed = s.trim();
        try {
            return ZonedDateTime.parse(trimmed);
        } catch (DateTimeParseException ignore) {
            // continue
        }
        try {
            return OffsetDateTime.parse(trimmed).toZonedDateTime();
        } catch (DateTimeParseException ignore) {
            // continue
        }
        try {
            return LocalDateTime.parse(trimmed).atZone(ZoneOffset.UTC);
        } catch (DateTimeParseException ignore) {
            // continue
        }
        try {
            return LocalDate.parse(trimmed).atStartOfDay(ZoneOffset.UTC);
        } catch (DateTimeParseException e) {
            throw new JinjaException("could not parse datetime: " + s);
        }
    }

    private static ZonedDateTime parseWithPyFormat(String s, String pyFmt) {
        DateTimeFormatter f = DateTimeFormatter.ofPattern(pyToJavaPattern(pyFmt), Locale.ROOT);
        try {
            return ZonedDateTime.parse(s, f);
        } catch (DateTimeParseException e) {
            try {
                return LocalDateTime.parse(s, f).atZone(ZoneOffset.UTC);
            } catch (DateTimeParseException e2) {
                return LocalDate.parse(s, f).atStartOfDay(ZoneOffset.UTC);
            }
        }
    }

    public static IsoDuration parseIsoDuration(String s) {
        String upper = s.trim().toUpperCase(Locale.ROOT);
        if (upper.isEmpty() || upper.charAt(0) != 'P') {
            throw new JinjaException("invalid ISO 8601 duration: " + s);
        }
        int tIdx = upper.indexOf('T');
        String dPart = tIdx < 0 ? upper : upper.substring(0, tIdx);
        String tPart = tIdx < 0 ? "" : "P" + upper.substring(tIdx);
        Period period = dPart.equals("P") ? Period.ZERO : Period.parse(dPart);
        Duration duration = tPart.isEmpty() ? Duration.ZERO : Duration.parse(tPart);
        return new IsoDuration(period, duration);
    }

    // ── strftime ───────────────────────────────────────────────────────────

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity",
                       "checkstyle:JavaNCSS"})
    static String pyStrftime(TemporalAccessor t, String fmt) {
        if (fmt.contains("%epoch_microseconds")) {
            long micros = epochMicros(t);
            fmt = fmt.replace("%epoch_microseconds", Long.toString(micros));
        }
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < fmt.length(); i++) {
            char c = fmt.charAt(i);
            if (c != '%' || i + 1 >= fmt.length()) {
                out.append(c);
                continue;
            }
            char tok = fmt.charAt(++i);
            appendStrftimeToken(out, tok, t);
        }
        return out.toString();
    }

    private static void appendStrftimeToken(StringBuilder out, char tok, TemporalAccessor t) {
        out.append(strftimeToken(tok, t));
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private static String strftimeToken(char tok, TemporalAccessor t) {
        switch (tok) {
            case 'Y':
                return pad(t.get(ChronoField.YEAR), 4);
            case 'y':
                return pad(t.get(ChronoField.YEAR) % 100, 2);
            case 'm':
                return pad(t.get(ChronoField.MONTH_OF_YEAR), 2);
            case 'd':
                return pad(t.get(ChronoField.DAY_OF_MONTH), 2);
            case 'H':
                return pad(getOr(t, ChronoField.HOUR_OF_DAY, 0), 2);
            case 'I':
                int h12 = getOr(t, ChronoField.HOUR_OF_DAY, 0) % 12;
                return pad(h12 == 0 ? 12 : h12, 2);
            case 'M':
                return pad(getOr(t, ChronoField.MINUTE_OF_HOUR, 0), 2);
            case 'S':
                return pad(getOr(t, ChronoField.SECOND_OF_MINUTE, 0), 2);
            case 'f':
                return pad(getOr(t, ChronoField.MICRO_OF_SECOND, 0), 6);
            case 'p':
                return getOr(t, ChronoField.HOUR_OF_DAY, 0) < 12 ? "AM" : "PM";
            case 'j':
                return pad(t.get(ChronoField.DAY_OF_YEAR), 3);
            case 'B':
                return monthName(t.get(ChronoField.MONTH_OF_YEAR), false);
            case 'b':
                return monthName(t.get(ChronoField.MONTH_OF_YEAR), true);
            case 'A':
                return dayName(t.get(ChronoField.DAY_OF_WEEK), false);
            case 'a':
                return dayName(t.get(ChronoField.DAY_OF_WEEK), true);
            case 'w':
                return Integer.toString(t.get(ChronoField.DAY_OF_WEEK) % 7);
            case 'z':
                return formatOffset(t);
            case 's':
                return Long.toString(epochSeconds(t));
            case '%':
                return "%";
            default:
                return "%" + tok;
        }
    }

    private static String pad(long v, int width) {
        String s = Long.toString(v);
        if (s.length() >= width) {
            return s;
        }
        StringBuilder sb = new StringBuilder(width);
        for (int i = s.length(); i < width; i++) {
            sb.append('0');
        }
        return sb.append(s).toString();
    }

    private static int getOr(TemporalAccessor t, ChronoField f, int dflt) {
        return t.isSupported(f) ? t.get(f) : dflt;
    }

    private static String monthName(int month, boolean abbreviated) {
        String full = Month.of(month).getDisplayName(
            abbreviated ? java.time.format.TextStyle.SHORT : java.time.format.TextStyle.FULL,
            Locale.ENGLISH);
        return full;
    }

    private static String dayName(int dayOfWeek, boolean abbreviated) {
        return DayOfWeek.of(dayOfWeek).getDisplayName(
            abbreviated ? java.time.format.TextStyle.SHORT : java.time.format.TextStyle.FULL,
            Locale.ENGLISH);
    }

    private static String formatOffset(TemporalAccessor t) {
        if (!t.isSupported(ChronoField.OFFSET_SECONDS)) {
            return "";
        }
        int secs = t.get(ChronoField.OFFSET_SECONDS);
        char sign = secs < 0 ? '-' : '+';
        int abs = Math.abs(secs);
        int hh = abs / 3600;
        int mm = (abs % 3600) / 60;
        return String.format(Locale.ROOT, "%c%02d%02d", sign, hh, mm);
    }

    private static long epochSeconds(TemporalAccessor t) {
        if (t instanceof Instant ins) {
            return ins.getEpochSecond();
        }
        if (t instanceof ZonedDateTime z) {
            return z.toEpochSecond();
        }
        if (t instanceof OffsetDateTime o) {
            return o.toEpochSecond();
        }
        if (t instanceof LocalDateTime ldt) {
            return ldt.toEpochSecond(ZoneOffset.UTC);
        }
        if (t instanceof LocalDate ld) {
            return ld.atStartOfDay(ZoneOffset.UTC).toEpochSecond();
        }
        return Instant.from(t).getEpochSecond();
    }

    private static long epochMicros(TemporalAccessor t) {
        Instant i;
        if (t instanceof ZonedDateTime z) {
            i = z.toInstant();
        } else if (t instanceof OffsetDateTime o) {
            i = o.toInstant();
        } else if (t instanceof LocalDateTime ldt) {
            i = ldt.toInstant(ZoneOffset.UTC);
        } else if (t instanceof LocalDate ld) {
            i = ld.atStartOfDay(ZoneOffset.UTC).toInstant();
        } else if (t instanceof Instant ins) {
            i = ins;
        } else {
            i = Instant.from(t);
        }
        return i.getEpochSecond() * 1_000_000L + i.getNano() / 1_000L;
    }

    // ── format-pattern conversion ───────────────────────────────────────────

    private static String pyToJavaPattern(String py) {
        StringBuilder out = new StringBuilder(py.length());
        for (int i = 0; i < py.length(); i++) {
            char c = py.charAt(i);
            if (c != '%' || i + 1 >= py.length()) {
                if (Character.isLetter(c)) {
                    out.append('\'').append(c).append('\'');
                } else {
                    out.append(c);
                }
                continue;
            }
            appendJavaPatternToken(out, py.charAt(++i));
        }
        return out.toString();
    }

    private static void appendJavaPatternToken(StringBuilder out, char tok) {
        switch (tok) {
            case 'Y':
                out.append("yyyy");
                return;
            case 'y':
                out.append("yy");
                return;
            case 'm':
                out.append("MM");
                return;
            case 'd':
                out.append("dd");
                return;
            case 'H':
                out.append("HH");
                return;
            case 'M':
                out.append("mm");
                return;
            case 'S':
                out.append("ss");
                return;
            case 'f':
                out.append("SSSSSS");
                return;
            case 'z':
                out.append("Z");
                return;
            case '%':
                out.append('%');
                return;
            default:
                out.append('\'').append('%').append(tok).append('\'');
        }
    }

    // ── helpers ────────────────────────────────────────────────────────────

    private static String camelToSnake(String s) {
        StringBuilder out = new StringBuilder(s.length() + 4);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (Character.isUpperCase(c) && i > 0) {
                out.append('_');
            }
            out.append(Character.toLowerCase(c));
        }
        return out.toString();
    }

    private static TemporalAccessor asTemporal(Object v) {
        if (v instanceof TemporalAccessor t) {
            return t;
        }
        if (v instanceof CharSequence cs) {
            return parseDatetimeFlexible(cs.toString());
        }
        throw new JinjaException("not a datetime: " + (v == null ? "null" : v.getClass().getSimpleName()));
    }

    private static String asString(Object o) {
        return o == null ? "" : o.toString();
    }
}
