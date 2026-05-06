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
import java.util.Map;

/**
 * Models one entry in the top-level {@code config_transformations} list. The {@code type}
 * field is the discriminator; the union of fields covers {@code ConfigAddFields} and
 * {@code ConfigRemapField}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConfigTransformationSpec {

    private String type;

    /** ConfigAddFields. */
    private List<AddedFieldSpec> fields = Collections.emptyList();
    private String condition;

    /** ConfigRemapField. */
    @JsonProperty("field_path")
    private List<String> fieldPath = Collections.emptyList();

    private Map<String, String> map = Collections.emptyMap();

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<AddedFieldSpec> getFields() {
        return fields == null ? Collections.emptyList() : fields;
    }

    public void setFields(List<AddedFieldSpec> fields) {
        this.fields = fields;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public List<String> getFieldPath() {
        return fieldPath == null ? Collections.emptyList() : fieldPath;
    }

    public void setFieldPath(List<String> fieldPath) {
        this.fieldPath = fieldPath;
    }

    public Map<String, String> getMap() {
        return map == null ? Collections.emptyMap() : map;
    }

    public void setMap(Map<String, String> map) {
        this.map = map;
    }
}
