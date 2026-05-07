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

import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.lib.fn.ELFunctionDefinition;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Static implementations of every Airbyte CDK Jinja macro / function. Each
 * method is registered as an {@link ELFunctionDefinition} on the global
 * jinjava context by {@link #registerAll(Jinjava)} so manifests can call
 * them with the same names and semantics as the Python CDK.
 *
 * <p>Mirrors {@code airbyte_cdk.sources.declarative.interpolation.macros}.
 * Method names use camelCase to satisfy Java conventions; the manifest-facing
 * names (set in the {@code ELFunctionDefinition}) keep Airbyte's
 * {@code snake_case}.</p>
 */
public final class AirbyteJinjaFunctions {

    /** ISO-8601 datetime formatter that always emits a numeric offset (e.g. +00:00). */
    private static final DateTimeFormatter ISO_OUT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX");

    private AirbyteJinjaFunctions() {
    }

    /** Wires every function below onto {@code j}'s global context. */
    public static void registerAll(Jinjava j) {
        register(j, "now_utc", "nowUtc");
        register(j, "today_utc", "todayUtc");
        register(j, "today_with_timezone", "todayWithTimezone", Object.class);
        register(j, "timestamp", "timestamp", Object.class);
        register(j, "str_to_datetime", "strToDatetime", Object.class);
        register(j, "max", "maxOf", Object.class, Object.class);
        register(j, "min", "minOf", Object.class, Object.class);
        register(j, "day_delta", "dayDelta", Object[].class);
        register(j, "duration", "duration", Object.class);
        register(j, "format_datetime", "formatDatetime", Object[].class);
        register(j, "sanitize_url", "sanitizeUrl", Object.class);
        register(j, "camel_case_to_snake_case", "camelCaseToSnakeCase", Object.class);
        register(j, "generate_uuid", "generateUuid");
    }

    private static void register(Jinjava j, String manifestName, String javaMethod, Class<?>... params) {
        j.getGlobalContext().registerFunction(
            new ELFunctionDefinition("", manifestName, AirbyteJinjaFunctions.class, javaMethod, params));
    }

    // ─────────────────────────────────────────────────────────────────────
    //  Datetime functions
    // ─────────────────────────────────────────────────────────────────────

    public static AirbyteDateTime nowUtc() {
        return new AirbyteDateTime(ZonedDateTime.now(ZoneOffset.UTC));
    }

    public static String todayUtc() {
        return LocalDate.now(ZoneOffset.UTC).toString();
    }

    public static String todayWithTimezone(Object timezone) {
        if (timezone == null) {
            return todayUtc();
        }
        return LocalDate.now(ZoneId.of(timezone.toString())).toString();
    }

    /** Unix epoch seconds for a numeric or ISO-string datetime. */
    public static long timestamp(Object dt) {
        if (dt == null) {
            return 0L;
        }
        if (dt instanceof Number n) {
            return n.longValue();
        }
        ZonedDateTime z = parseToZdt(dt.toString());
        return z.toEpochSecond();
    }

    /** Parse ISO-8601 string to a ZonedDateTime (assumes UTC if no offset). */
    public static ZonedDateTime strToDatetime(Object value) {
        if (value == null) {
            return null;
        }
        return parseToZdt(value.toString());
    }

    /** Returns max of two values; numeric if both numeric, else string compare. */
    public static Object maxOf(Object a, Object b) {
        return compare(a, b) >= 0 ? a : b;
    }

    /** Returns min of two values; numeric if both numeric, else string compare. */
    public static Object minOf(Object a, Object b) {
        return compare(a, b) <= 0 ? a : b;
    }

    /**
     * Returns now(UTC) shifted by {@code numDays} formatted with the supplied
     * Python-style {@code format} string.
     */
    public static String dayDelta(Object... args) {
        if (args == null || args.length < 1 || args[0] == null) {
            return "";
        }
        long days = ((Number) args[0]).longValue();
        Object format = args.length >= 2 ? args[1] : null;
        ZonedDateTime when = ZonedDateTime.now(ZoneOffset.UTC).plusDays(days);
        return when.format(toJavaFormat(format == null ? "%Y-%m-%dT%H:%M:%S.%f%z" : format.toString()));
    }

    /**
     * Parse an ISO-8601 duration string ({@code P1D}, {@code PT2H}, …) to a
     * {@link TemporalAmount}. Used inside expressions like
     * {@code now_utc() - duration("P1D")}.
     */
    public static TemporalAmount duration(Object iso) {
        if (iso == null) {
            return Duration.ZERO;
        }
        String s = iso.toString();
        // Period covers years/months/days; Duration covers time. Try Period first
        // when there's no 'T'; otherwise Duration.
        try {
            return s.contains("T") ? Duration.parse(s) : Period.parse(s);
        } catch (DateTimeParseException e) {
            // Some manifests use "P0DT0H0M0S" — try Duration as fallback.
            return Duration.parse(s);
        }
    }

    /**
     * {@code format_datetime(dt, format[, input_format])}. {@code dt} may be a
     * string, an {@link Instant}, or a {@link ZonedDateTime}. Registered with
     * {@code Object[].class} so jinjava can invoke either the 2-arg or 3-arg
     * form from manifests.
     */
    public static String formatDatetime(Object... args) {
        if (args == null || args.length < 2) {
            return "";
        }
        Object dt = args[0];
        Object format = args[1];
        Object inputFormat = args.length >= 3 ? args[2] : null;
        if (dt == null || format == null) {
            return "";
        }
        ZonedDateTime z;
        if (dt instanceof AirbyteDateTime ad) {
            z = ad.toZonedDateTime();
        } else if (dt instanceof ZonedDateTime zd) {
            z = zd;
        } else if (dt instanceof Instant i) {
            z = i.atZone(ZoneOffset.UTC);
        } else if (inputFormat != null) {
            DateTimeFormatter f = toJavaFormat(inputFormat.toString());
            String s = dt.toString();
            try {
                z = LocalDateTime.parse(s, f).atZone(ZoneOffset.UTC);
            } catch (DateTimeParseException e) {
                z = LocalDate.parse(s, f).atStartOfDay(ZoneOffset.UTC);
            }
        } else {
            z = parseToZdt(dt.toString());
        }
        return z.format(toJavaFormat(format.toString()));
    }

    // ─────────────────────────────────────────────────────────────────────
    //  String functions
    // ─────────────────────────────────────────────────────────────────────

    public static String sanitizeUrl(Object value) {
        if (value == null) {
            return "";
        }
        return URLEncoder.encode(value.toString(), StandardCharsets.UTF_8);
    }

    private static final Pattern CAMEL_BOUNDARY =
        Pattern.compile("([a-z0-9])([A-Z])|([A-Z]+)([A-Z][a-z])");

    public static String camelCaseToSnakeCase(Object value) {
        if (value == null) {
            return "";
        }
        String s = value.toString();
        Matcher m = CAMEL_BOUNDARY.matcher(s);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            String first = m.group(1) != null ? m.group(1) : m.group(3);
            String second = m.group(2) != null ? m.group(2) : m.group(4);
            m.appendReplacement(out, Matcher.quoteReplacement(first + "_" + second));
        }
        m.appendTail(out);
        return out.toString().toLowerCase(java.util.Locale.ROOT);
    }

    public static String generateUuid() {
        return UUID.randomUUID().toString();
    }

    // ─────────────────────────────────────────────────────────────────────
    //  Helpers
    // ─────────────────────────────────────────────────────────────────────

    /**
     * Convert a Python {@code strftime} pattern to a Java
     * {@link DateTimeFormatter} pattern. Supports the Airbyte CDK subset:
     * {@code %Y %m %d %H %M %S %f %z %Z %j %y %B %b %A %a %p}.
     */
    static DateTimeFormatter toJavaFormat(String pythonFormat) {
        return DateTimeFormatter.ofPattern(pythonStrftimeToJavaPattern(pythonFormat));
    }

    static String pythonStrftimeToJavaPattern(String s) {
        StringBuilder out = new StringBuilder();
        int i = 0;
        boolean inLiteral = false;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == '%' && i + 1 < s.length()) {
                if (inLiteral) {
                    out.append('\'');
                    inLiteral = false;
                }
                out.append(strftimeDirective(s.charAt(i + 1)));
                i += 2;
            } else {
                // Letters need quoting for Java DateTimeFormatter.
                if (Character.isLetter(c)) {
                    if (!inLiteral) {
                        out.append('\'');
                        inLiteral = true;
                    }
                    out.append(c);
                } else if (c == '\'') {
                    if (inLiteral) {
                        out.append('\'');
                        inLiteral = false;
                    }
                    out.append("''");
                } else {
                    if (inLiteral) {
                        out.append('\'');
                        inLiteral = false;
                    }
                    out.append(c);
                }
                i++;
            }
        }
        if (inLiteral) {
            out.append('\'');
        }
        return out.toString();
    }

    @SuppressWarnings("CyclomaticComplexity")
    private static String strftimeDirective(char d) {
        switch (d) {
            case 'Y': return "yyyy";
            case 'y': return "yy";
            case 'm': return "MM";
            case 'd': return "dd";
            case 'H': return "HH";
            case 'I': return "hh";
            case 'M': return "mm";
            case 'S': return "ss";
            case 'f': return "SSSSSS";
            case 'z': return "Z";
            case 'Z': return "zzz";
            case 'j': return "DDD";
            case 'B': return "MMMM";
            case 'b': return "MMM";
            case 'A': return "EEEE";
            case 'a': return "EEE";
            case 'p': return "a";
            case '%': return "'%'";
            default:  return "'%" + d + "'";
        }
    }

    /**
     * Parse a string into a {@link ZonedDateTime}, accepting:
     * full ISO-8601 with offset, ISO without offset (assumed UTC), and
     * date-only (assumed UTC midnight).
     */
    static ZonedDateTime parseToZdt(String s) {
        String trimmed = s.trim();
        try {
            return ZonedDateTime.parse(trimmed);
        } catch (DateTimeParseException ignored) {
            // try offset
        }
        try {
            return OffsetDateTime.parse(trimmed).toZonedDateTime();
        } catch (DateTimeParseException ignored) {
            // try local
        }
        try {
            return LocalDateTime.parse(trimmed).atZone(ZoneOffset.UTC);
        } catch (DateTimeParseException ignored) {
            // try date-only
        }
        return LocalDate.parse(trimmed).atStartOfDay(ZoneOffset.UTC);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static int compare(Object a, Object b) {
        if (a == null && b == null) {
            return 0;
        }
        if (a == null) {
            return -1;
        }
        if (b == null) {
            return 1;
        }
        if (a instanceof Number na && b instanceof Number nb) {
            return Double.compare(na.doubleValue(), nb.doubleValue());
        }
        if (a instanceof Comparable && a.getClass() == b.getClass()) {
            return ((Comparable) a).compareTo(b);
        }
        return a.toString().compareTo(b.toString());
    }

    // Suppresses unused-import warning for ChronoUnit while keeping the import
    // available for future enhancements (e.g. truncating to seconds in formatDatetime).
    @SuppressWarnings("unused")
    private static final ChronoUnit RESERVED_FOR_FUTURE_USE = ChronoUnit.SECONDS;
}
