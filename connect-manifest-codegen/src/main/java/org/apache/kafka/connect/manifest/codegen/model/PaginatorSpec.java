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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Models the {@code paginator} block inside a retriever.
 *
 * <p>Airbyte paginators use a two-level structure:
 * <ul>
 *   <li>Outer: a {@code DefaultPaginator} that names the page-token and page-size query parameters.</li>
 *   <li>Inner: a {@code pagination_strategy} that describes the specific strategy
 *       (CursorPagination, PageIncrement, OffsetIncrement, NoPagination).</li>
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PaginatorSpec {

    private String type;

    @JsonProperty("page_token_option")
    private OptionSpec pageTokenOption;

    @JsonProperty("page_size_option")
    private OptionSpec pageSizeOption;

    @JsonProperty("pagination_strategy")
    private StrategySpec paginationStrategy;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public OptionSpec getPageTokenOption() {
        return pageTokenOption;
    }

    public void setPageTokenOption(OptionSpec pageTokenOption) {
        this.pageTokenOption = pageTokenOption;
    }

    public OptionSpec getPageSizeOption() {
        return pageSizeOption;
    }

    public void setPageSizeOption(OptionSpec pageSizeOption) {
        this.pageSizeOption = pageSizeOption;
    }

    public StrategySpec getPaginationStrategy() {
        return paginationStrategy;
    }

    public void setPaginationStrategy(StrategySpec paginationStrategy) {
        this.paginationStrategy = paginationStrategy;
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    public boolean isCursor() {
        return paginationStrategy != null && "CursorPagination".equalsIgnoreCase(paginationStrategy.getType());
    }

    public boolean isPageIncrement() {
        return paginationStrategy != null && "PageIncrement".equalsIgnoreCase(paginationStrategy.getType());
    }

    public boolean isOffsetIncrement() {
        return paginationStrategy != null && "OffsetIncrement".equalsIgnoreCase(paginationStrategy.getType());
    }

    public boolean hasNoPagination() {
        return paginationStrategy == null || "NoPagination".equalsIgnoreCase(paginationStrategy.getType());
    }

    /** Field name for the page/cursor/offset query parameter, defaulting to "page". */
    public String pageParamName() {
        return pageTokenOption != null && pageTokenOption.getFieldName() != null
            ? pageTokenOption.getFieldName()
            : "page";
    }

    /** Field name for the page-size/limit query parameter, defaulting to "per_page". */
    public String sizeParamName() {
        return pageSizeOption != null && pageSizeOption.getFieldName() != null
            ? pageSizeOption.getFieldName()
            : "per_page";
    }

    /** Page/batch size from the strategy, defaulting to 100. */
    public int pageSize() {
        return paginationStrategy != null ? paginationStrategy.getPageSize() : 100;
    }

    // ── inner classes ─────────────────────────────────────────────────────────

    /** Models a {@code page_token_option} or {@code page_size_option} entry. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OptionSpec {

        private String type;

        @JsonProperty("field_name")
        private String fieldName;

        @JsonProperty("inject_into")
        private String injectInto;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

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

        public boolean isRequestPath() {
            return "RequestPath".equalsIgnoreCase(type);
        }
    }

    /** Models the nested {@code pagination_strategy} block. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class StrategySpec {

        private String type;

        @JsonProperty("page_size")
        @com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = IntOrTemplateDeserializer.class)
        private int pageSize;

        @JsonProperty("cursor_value")
        private String cursorValue;

        @JsonProperty("start_from_page")
        private int startFromPage;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public int getPageSize() {
            return pageSize;
        }

        public void setPageSize(int pageSize) {
            this.pageSize = pageSize;
        }

        public String getCursorValue() {
            return cursorValue;
        }

        public void setCursorValue(String cursorValue) {
            this.cursorValue = cursorValue;
        }

        /**
         * Extracts the JSON field path from a Jinja2 {@code cursor_value} expression.
         *
         * <p>Handles patterns like:
         * <ul>
         *   <li>{@code {{ response.get('page_details', {}).get('next_url') }}} → {@code ["page_details", "next_url"]}</li>
         *   <li>{@code {{ response['next_stream_position'] }}} → {@code ["next_stream_position"]}</li>
         * </ul>
         * Double-quote bracket access ({@code response["key"]}) is intentionally not matched here
         * because those expressions often embed arithmetic (e.g., {@code response["num"]+1}).
         */
        public List<String> parseCursorJsonPath() {
            if (cursorValue == null || cursorValue.isBlank()) {
                return Collections.emptyList();
            }
            // Match .get('key') or ['key'] (single-quote only to avoid arithmetic expressions)
            Pattern p = Pattern.compile("(?:\\.get\\('([^']+)'|\\['([^']+)'\\])");
            Matcher m = p.matcher(cursorValue);
            List<String> path = new ArrayList<>();
            while (m.find()) {
                path.add(m.group(1) != null ? m.group(1) : m.group(2));
            }
            return path;
        }

        public int getStartFromPage() {
            return startFromPage;
        }

        public void setStartFromPage(int startFromPage) {
            this.startFromPage = startFromPage;
        }

        /**
         * Tolerant int deserializer used for {@code page_size}: accepts a plain int OR a
         * Jinja-templated string ({@code "{{ config.get('page_size', 100) }}"}). When the
         * value is a string, parses any embedded numeric literal — typically the second
         * argument of {@code .get()} — as the runtime default; otherwise falls back to 100.
         */
        static final class IntOrTemplateDeserializer extends com.fasterxml.jackson.databind.JsonDeserializer<Integer> {
            @Override
            public Integer deserialize(com.fasterxml.jackson.core.JsonParser p,
                                       com.fasterxml.jackson.databind.DeserializationContext ctxt)
                    throws java.io.IOException {
                com.fasterxml.jackson.databind.JsonNode n = p.readValueAsTree();
                if (n == null || n.isNull()) return 0;
                if (n.isInt() || n.isLong()) return n.asInt();
                if (n.isTextual()) {
                    String s = n.asText();
                    Matcher m = Pattern.compile("\\b(\\d+)\\b").matcher(s);
                    int last = 0;
                    boolean found = false;
                    while (m.find()) {
                        last = Integer.parseInt(m.group(1));
                        found = true;
                    }
                    return found ? last : 100;
                }
                return 0;
            }
        }
    }
}
