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

    /** Models {@code start_datetime} / {@code end_datetime} (MinMaxDatetime). */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DatetimeSpec {

        @JsonProperty("datetime")
        private String datetime;

        public String getDatetime() {
            return datetime;
        }

        public void setDatetime(String datetime) {
            this.datetime = datetime;
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
