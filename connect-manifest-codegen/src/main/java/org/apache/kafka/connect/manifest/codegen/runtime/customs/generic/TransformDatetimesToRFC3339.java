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
package org.apache.kafka.connect.manifest.codegen.runtime.customs.generic;

import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomTransformation;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;

/**
 * Java port of {@code source-amplitude/components.py TransformDatetimesToRFC3339} (lines 59–93).
 *
 * <p>Amplitude returns seven datetime fields in a variety of non-standard formats
 * (space-separated, with or without microseconds, with or without UTC suffix).
 * This transform parses each with a multi-format strategy and re-emits as ISO 8601
 * {@code yyyy-MM-dd'T'HH:mm:ssXXX} (RFC 3339 subset).</p>
 *
 * <p>Registered under {@code source_declarative_manifest.components.TransformDatetimesToRFC3339}.</p>
 */
public final class TransformDatetimesToRFC3339 implements CustomTransformation {

    private static final List<String> DATE_TIME_FIELDS = List.of(
        "event_time", "server_upload_time", "processed_time",
        "server_received_time", "user_creation_time",
        "client_upload_time", "client_event_time"
    );

    private static final List<DateTimeFormatter> PARSE_FMTS = List.of(
        DateTimeFormatter.ISO_OFFSET_DATE_TIME,
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
        DateTimeFormatter.ISO_LOCAL_DATE_TIME,
        DateTimeFormatter.ISO_LOCAL_DATE
    );

    private static final DateTimeFormatter OUT_FMT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX");

    @SuppressWarnings("unused")
    public TransformDatetimesToRFC3339(Map<String, String> connectorConfig,
                                       Map<String, Object> componentParams) {
    }

    @Override
    public Map<String, Object> transform(Map<String, Object> record) {
        if (record == null) {
            return null;
        }
        for (String field : DATE_TIME_FIELDS) {
            Object val = record.get(field);
            if (val == null) {
                continue;
            }
            String s = val.toString().trim();
            if (s.isEmpty()) {
                continue;
            }
            ZonedDateTime zdt = tryParse(s);
            if (zdt != null) {
                record.put(field, zdt.withZoneSameInstant(ZoneOffset.UTC).format(OUT_FMT));
            }
        }
        return record;
    }

    private static ZonedDateTime tryParse(String s) {
        for (DateTimeFormatter fmt : PARSE_FMTS) {
            try {
                return ZonedDateTime.parse(s, fmt);
            } catch (DateTimeParseException ignored) {
                // fall through to next format
            }
            try {
                return OffsetDateTime.parse(s, fmt).toZonedDateTime();
            } catch (DateTimeParseException ignored) {
                // fall through to next format
            }
            try {
                return LocalDateTime.parse(s, fmt).atZone(ZoneOffset.UTC);
            } catch (DateTimeParseException ignored) {
                // fall through to next format
            }
            try {
                return LocalDate.parse(s, fmt).atStartOfDay(ZoneOffset.UTC);
            } catch (DateTimeParseException ignored) {
                // fall through
            }
        }
        return null;
    }
}
