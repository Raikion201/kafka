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
import org.apache.kafka.connect.manifest.codegen.runtime.jinja.JinjaRenderer;
import org.apache.kafka.connect.manifest.codegen.runtime.transform.DPath;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Java port of {@code source-bigcommerce components.py DateTimeTransformer}.
 *
 * <p>For each field spec ({@code path}, {@code value}) in the {@code fields} parameter,
 * evaluates the Jinja {@code value} template against the record, parses the result as an
 * ISO 8601 datetime (best-effort multi-format), and writes the RFC 3339 string back to
 * {@code path} in the record. Fields whose value cannot be parsed as a datetime are left
 * unchanged.</p>
 *
 * <p>Registered under {@code source_declarative_manifest.components.DateTimeTransformer}.</p>
 */
public final class DateTimeTransformer implements CustomTransformation {

    private static final DateTimeFormatter OUT_FMT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX");

    private static final List<DateTimeFormatter> PARSE_FMTS = List.of(
        DateTimeFormatter.ISO_OFFSET_DATE_TIME,
        DateTimeFormatter.ISO_LOCAL_DATE_TIME,
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
        DateTimeFormatter.ISO_LOCAL_DATE
    );

    private final List<FieldSpec> fields;

    @SuppressWarnings("unchecked")
    public DateTimeTransformer(Map<String, String> connectorConfig,
                               Map<String, Object> componentParams) {
        Object raw = componentParams == null ? null : componentParams.get("fields");
        List<FieldSpec> parsed = new ArrayList<>();
        if (raw instanceof List) {
            for (Object item : (List<?>) raw) {
                if (!(item instanceof Map)) {
                    continue;
                }
                Map<String, Object> m = (Map<String, Object>) item;
                Object pathObj = m.get("path");
                Object valueObj = m.get("value");
                if (pathObj instanceof List && valueObj instanceof String) {
                    parsed.add(new FieldSpec((List<String>) pathObj, (String) valueObj));
                }
            }
        }
        this.fields = parsed;
    }

    @Override
    public Map<String, Object> transform(Map<String, Object> record) {
        if (record == null) {
            return null;
        }
        Map<String, Object> ctx = new HashMap<>();
        ctx.put("record", record);
        for (FieldSpec f : fields) {
            String rendered = JinjaRenderer.renderLenient(f.value, ctx);
            if (rendered == null || rendered.isBlank()) {
                continue;
            }
            ZonedDateTime zdt = tryParse(rendered.trim());
            if (zdt != null) {
                DPath.set(record, f.path, zdt.withZoneSameInstant(ZoneOffset.UTC).format(OUT_FMT));
            }
        }
        return record;
    }

    private static ZonedDateTime tryParse(String s) {
        for (DateTimeFormatter fmt : PARSE_FMTS) {
            try {
                return ZonedDateTime.parse(s, fmt);
            } catch (DateTimeParseException ignored) {
                // try next
            }
            try {
                return OffsetDateTime.parse(s, fmt).toZonedDateTime();
            } catch (DateTimeParseException ignored) {
                // try next
            }
            try {
                return LocalDateTime.parse(s, fmt).atZone(ZoneOffset.UTC);
            } catch (DateTimeParseException ignored) {
                // try next
            }
            try {
                return LocalDate.parse(s, fmt).atStartOfDay(ZoneOffset.UTC);
            } catch (DateTimeParseException ignored) {
                // try next
            }
        }
        return null;
    }

    private static final class FieldSpec {
        final List<String> path;
        final String value;

        FieldSpec(List<String> path, String value) {
            this.path = path;
            this.value = value;
        }
    }
}
