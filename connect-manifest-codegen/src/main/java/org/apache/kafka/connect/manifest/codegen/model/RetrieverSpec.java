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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.Collections;
import java.util.List;

/**
 * Models the {@code retriever} block of a stream — how data is fetched.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RetrieverSpec {

    private String type;

    /** Airbyte CDK Custom* node identifier. Non-null only when type starts with "Custom". */
    @JsonProperty("class_name")
    private String className;

    private RequesterSpec requester;

    @JsonProperty("record_selector")
    private RecordSelectorSpec recordSelector;

    private PaginatorSpec paginator;

    @JsonProperty("partition_router")
    @JsonDeserialize(using = PartitionRouterListDeserializer.class)
    private List<PartitionRouterSpec> partitionRouter = Collections.emptyList();

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

    public PaginatorSpec getPaginator() {
        return paginator;
    }

    public void setPaginator(PaginatorSpec paginator) {
        this.paginator = paginator;
    }

    public List<PartitionRouterSpec> getPartitionRouter() {
        return partitionRouter == null ? Collections.emptyList() : partitionRouter;
    }

    public void setPartitionRouter(List<PartitionRouterSpec> partitionRouter) {
        this.partitionRouter = partitionRouter;
    }

    public boolean hasSubstreamPartition() {
        return getPartitionRouter().stream().anyMatch(PartitionRouterSpec::isSubstream);
    }

    /** Returns the single SubstreamPartitionRouter, or null if none / more than one. */
    public PartitionRouterSpec getSubstreamRouter() {
        List<PartitionRouterSpec> subs = getPartitionRouter().stream()
            .filter(PartitionRouterSpec::isSubstream)
            .toList();
        return subs.size() == 1 ? subs.get(0) : null;
    }
}
