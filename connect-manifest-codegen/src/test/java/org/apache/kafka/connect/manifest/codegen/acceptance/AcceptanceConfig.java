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
package org.apache.kafka.connect.manifest.codegen.acceptance;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Acceptance-test config, mirroring Airbyte's {@code acceptance-test-config.yaml}.
 * One file per connector under {@code src/test/resources/acceptance-tests/}; the
 * harness reads it, resolves env-var placeholders, and drives the generated SourceTask
 * through {@code read} + {@code state-resume} phases.
 *
 * <p>Secrets must NEVER be committed literally — every config value that resolves to
 * a secret should be written as {@code ${ENV_VAR_NAME}}; missing env vars cause the
 * test to be skipped, not failed.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class AcceptanceConfig {

    /** Generated SourceTask class name prefix, e.g. {@code GoogleClassroom} (yields {@code GoogleClassroomSourceTask}). */
    @JsonProperty("connector")
    private String connector;

    /** Connector config map; values may contain {@code ${ENV}} placeholders. */
    @JsonProperty("config")
    private Map<String, String> config = new LinkedHashMap<>();

    /** Per-stream expectations driving {@code read}/{@code state-resume} assertions. */
    @JsonProperty("streams")
    private List<StreamSpec> streams = Collections.emptyList();

    /** Cap on poll iterations per phase to keep tests bounded. Default 20. */
    @JsonProperty("max_polls")
    private int maxPolls = 20;

    /** Number of polls to issue before snapshotting offsets in the state-resume phase. Default 2. */
    @JsonProperty("resume_after_polls")
    private int resumeAfterPolls = 2;

    public String getConnector() {
        return connector;
    }

    public void setConnector(String connector) {
        this.connector = connector;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    public List<StreamSpec> getStreams() {
        return streams;
    }

    public void setStreams(List<StreamSpec> streams) {
        this.streams = streams;
    }

    public int getMaxPolls() {
        return maxPolls;
    }

    public void setMaxPolls(int maxPolls) {
        this.maxPolls = maxPolls;
    }

    public int getResumeAfterPolls() {
        return resumeAfterPolls;
    }

    public void setResumeAfterPolls(int resumeAfterPolls) {
        this.resumeAfterPolls = resumeAfterPolls;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class StreamSpec {

        /** Stream name as it appears in the manifest and on the SourceRecord topic. */
        @JsonProperty("name")
        private String name;

        /** Minimum number of records that must arrive across all polls in {@code read}. 0 = no lower bound. */
        @JsonProperty("min_records")
        private int minRecords = 0;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getMinRecords() {
            return minRecords;
        }

        public void setMinRecords(int minRecords) {
            this.minRecords = minRecords;
        }
    }
}
