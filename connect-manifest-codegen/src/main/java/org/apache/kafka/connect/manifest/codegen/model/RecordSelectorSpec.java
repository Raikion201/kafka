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

import java.util.Collections;
import java.util.List;

/**
 * Models the {@code record_selector} block — how records are extracted from a response.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RecordSelectorSpec {

    private String type;
    private ExtractorSpec extractor;

    @JsonProperty("record_filter")
    private RecordFilterSpec recordFilter;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public ExtractorSpec getExtractor() {
        return extractor;
    }

    public void setExtractor(ExtractorSpec extractor) {
        this.extractor = extractor;
    }

    public RecordFilterSpec getRecordFilter() {
        return recordFilter;
    }

    public void setRecordFilter(RecordFilterSpec recordFilter) {
        this.recordFilter = recordFilter;
    }

    /** Models the {@code extractor} inside a record_selector — the JSON path to the records array. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ExtractorSpec {

        private String type;

        @JsonProperty("class_name")
        private String className;

        @JsonProperty("field_path")
        private List<String> fieldPath = Collections.emptyList();

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getClassName() {
            return className;
        }

        public void setClassName(String className) {
            this.className = className;
        }

        public List<String> getFieldPath() {
            return fieldPath == null ? Collections.emptyList() : fieldPath;
        }

        public void setFieldPath(List<String> fieldPath) {
            this.fieldPath = fieldPath;
        }
    }
}
