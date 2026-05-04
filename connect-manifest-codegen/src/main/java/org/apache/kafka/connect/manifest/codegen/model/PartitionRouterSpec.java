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
 * Models a {@code partition_router} entry — either a single SubstreamPartitionRouter
 * or one element of a list of routers on the retriever.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PartitionRouterSpec {

    private String type;

    /** Airbyte CDK Custom* node identifier. Non-null only when type starts with "Custom". */
    @JsonProperty("class_name")
    private String className;

    @JsonProperty("parent_stream_configs")
    private List<ParentStreamConfig> parentStreamConfigs = Collections.emptyList();

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

    public List<ParentStreamConfig> getParentStreamConfigs() {
        return parentStreamConfigs == null ? Collections.emptyList() : parentStreamConfigs;
    }

    public void setParentStreamConfigs(List<ParentStreamConfig> parentStreamConfigs) {
        this.parentStreamConfigs = parentStreamConfigs;
    }

    public boolean isSubstream() {
        return "SubstreamPartitionRouter".equalsIgnoreCase(type);
    }

    /** The Jinja2 variable name injected into the child path, e.g. {@code course}. */
    public String partitionField() {
        return getParentStreamConfigs().isEmpty() ? null : getParentStreamConfigs().get(0).getPartitionField();
    }

    /** The field extracted from each parent record to use as the partition key, e.g. {@code id}. */
    public String parentKey() {
        return getParentStreamConfigs().isEmpty() ? null : getParentStreamConfigs().get(0).getParentKey();
    }

    /** The stream name of the parent stream, e.g. {@code courses}. */
    public String parentStreamName() {
        return getParentStreamConfigs().isEmpty() ? null : getParentStreamConfigs().get(0).getParentStreamName();
    }

    // ── inner classes ─────────────────────────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ParentStreamConfig {

        @JsonProperty("parent_key")
        private String parentKey;

        @JsonProperty("partition_field")
        private String partitionField;

        private StreamRef stream;

        public String getParentKey() {
            return parentKey;
        }

        public void setParentKey(String parentKey) {
            this.parentKey = parentKey;
        }

        public String getPartitionField() {
            return partitionField;
        }

        public void setPartitionField(String partitionField) {
            this.partitionField = partitionField;
        }

        public StreamRef getStream() {
            return stream;
        }

        public void setStream(StreamRef stream) {
            this.stream = stream;
        }

        public String getParentStreamName() {
            return stream != null ? stream.refStreamName() : null;
        }
    }

    /** A {@code stream: {$ref: "#/definitions/streams/courses"}} block inside parent_stream_configs. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class StreamRef {

        @JsonProperty("$ref")
        private String ref;

        public String getRef() {
            return ref;
        }

        public void setRef(String ref) {
            this.ref = ref;
        }

        /** Extracts the stream name from a ref like {@code "#/definitions/streams/courses"}. */
        public String refStreamName() {
            if (ref == null || ref.isEmpty()) return null;
            int last = ref.lastIndexOf('/');
            return last >= 0 ? ref.substring(last + 1) : ref;
        }
    }
}
