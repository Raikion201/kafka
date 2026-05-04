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
package org.apache.kafka.connect.manifest.codegen.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Models the {@code incremental_sync} block on a stream (DatetimeBasedCursor).
 * Injects date-range query parameters derived from config into each request.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IncrementalSyncSpec {

    private String type;

    @JsonProperty("cursor_field")
    private String cursorField;

    @JsonProperty("start_datetime")
    private DatetimeSpec startDatetime;

    @JsonProperty("end_datetime")
    private DatetimeSpec endDatetime;

    @JsonProperty("start_time_option")
    private TimeOptionSpec startTimeOption;

    @JsonProperty("end_time_option")
    private TimeOptionSpec endTimeOption;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getCursorField() {
        return cursorField;
    }

    public void setCursorField(String cursorField) {
        this.cursorField = cursorField;
    }

    public DatetimeSpec getStartDatetime() {
        return startDatetime;
    }

    public void setStartDatetime(DatetimeSpec startDatetime) {
        this.startDatetime = startDatetime;
    }

    public DatetimeSpec getEndDatetime() {
        return endDatetime;
    }

    public void setEndDatetime(DatetimeSpec endDatetime) {
        this.endDatetime = endDatetime;
    }

    public TimeOptionSpec getStartTimeOption() {
        return startTimeOption;
    }

    public void setStartTimeOption(TimeOptionSpec startTimeOption) {
        this.startTimeOption = startTimeOption;
    }

    public TimeOptionSpec getEndTimeOption() {
        return endTimeOption;
    }

    public void setEndTimeOption(TimeOptionSpec endTimeOption) {
        this.endTimeOption = endTimeOption;
    }

    public boolean isDatetimeBased() {
        return "DatetimeBasedCursor".equalsIgnoreCase(type);
    }

    // ── inner classes ─────────────────────────────────────────────────────────

    /**
     * Models {@code start_datetime} / {@code end_datetime}. Airbyte allows two YAML forms:
     * an object ({@code {type: MinMaxDatetime, datetime: "{{...}}", datetime_format: "..."}}),
     * or a bare Jinja string shorthand ({@code "{{ config.get('end_date') }}"}). The custom
     * deserializer accepts both and normalises onto this bean.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = DatetimeSpec.Deserializer.class)
    public static class DatetimeSpec {

        @JsonProperty("type")
        private String type;

        @JsonProperty("datetime")
        private String datetime;

        @JsonProperty("datetime_format")
        private String datetimeFormat;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getDatetime() {
            return datetime;
        }

        public void setDatetime(String datetime) {
            this.datetime = datetime;
        }

        public String getDatetimeFormat() {
            return datetimeFormat;
        }

        public void setDatetimeFormat(String datetimeFormat) {
            this.datetimeFormat = datetimeFormat;
        }

        static final class Deserializer extends com.fasterxml.jackson.databind.JsonDeserializer<DatetimeSpec> {
            @Override
            public DatetimeSpec deserialize(com.fasterxml.jackson.core.JsonParser p,
                                            com.fasterxml.jackson.databind.DeserializationContext ctxt)
                    throws java.io.IOException {
                com.fasterxml.jackson.databind.JsonNode node = p.readValueAsTree();
                if (node == null || node.isNull()) {
                    return null;
                }
                DatetimeSpec out = new DatetimeSpec();
                if (node.isTextual()) {
                    out.datetime = node.asText();
                    return out;
                }
                if (node.isObject()) {
                    if (node.hasNonNull("type")) out.type = node.get("type").asText();
                    if (node.hasNonNull("datetime")) out.datetime = node.get("datetime").asText();
                    if (node.hasNonNull("datetime_format")) out.datetimeFormat = node.get("datetime_format").asText();
                    return out;
                }
                return null;
            }
        }
    }

    /** Models {@code start_time_option} / {@code end_time_option} (RequestOption). */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TimeOptionSpec {

        @JsonProperty("field_name")
        private String fieldName;

        @JsonProperty("inject_into")
        private String injectInto;

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getInjectInto() {
            return injectInto;
        }

        public void setInjectInto(String injectInto) {
            this.injectInto = injectInto;
        }

        public boolean isRequestParameter() {
            return "request_parameter".equalsIgnoreCase(injectInto);
        }
    }
}
