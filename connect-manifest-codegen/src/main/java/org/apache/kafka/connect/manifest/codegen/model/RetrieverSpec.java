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
 * Models the {@code retriever} block of a stream — how data is fetched.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RetrieverSpec {

    private String type;
    private RequesterSpec requester;

    @JsonProperty("record_selector")
    private RecordSelectorSpec recordSelector;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public RequesterSpec getRequester() {
        return requester;
    }

    public void setRequester(RequesterSpec requester) {
        this.requester = requester;
    }

    public RecordSelectorSpec getRecordSelector() {
        return recordSelector;
    }

    public void setRecordSelector(RecordSelectorSpec recordSelector) {
        this.recordSelector = recordSelector;
    }
}
