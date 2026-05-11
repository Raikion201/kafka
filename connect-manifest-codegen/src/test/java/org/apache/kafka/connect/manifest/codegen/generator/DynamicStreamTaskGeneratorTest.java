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
package org.apache.kafka.connect.manifest.codegen.generator;

import org.apache.kafka.connect.manifest.codegen.model.ManifestSpec;
import org.apache.kafka.connect.manifest.codegen.parser.ManifestParser;

import com.squareup.javapoet.JavaFile;

import org.junit.jupiter.api.Test;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the output shape of {@link DynamicStreamTaskGenerator} so that regressions in the
 * generated Google Sheets task are caught immediately. Also verifies that manifests with
 * unrecognised dynamic-stream shapes produce the stub (fail-fast) task rather than broken code.
 */
public class DynamicStreamTaskGeneratorTest {

    private static final String PKG = ConfigGenerator.BASE_PACKAGE;
    private final TaskGenerator generator = new TaskGenerator();
    private final ManifestParser parser = new ManifestParser();

    private InputStream resource(String name) {
        return getClass().getClassLoader().getResourceAsStream("manifests/" + name);
    }

    private ManifestSpec load(String name) throws Exception {
        return parser.parse(resource(name));
    }

    // ── Google Sheets — full task ─────────────────────────────────────────────

    @Test
    void googleSheets_generatesFullTaskClass() throws Exception {
        JavaFile file = generator.generate(load("google_sheets.yaml"), PKG);
        String src = file.toString();
        assertTrue(src.contains("discoverSheets"),    "Must contain discoverSheets");
        assertTrue(src.contains("getAccessToken"),    "Must contain getAccessToken");
        assertTrue(src.contains("fetchSheetRows"),    "Must contain fetchSheetRows");
        assertTrue(src.contains("sendWithRetry"),     "Must contain sendWithRetry");
        assertTrue(src.contains("extractSpreadsheetId"), "Must contain extractSpreadsheetId");
    }

    @Test
    void googleSheets_containsOAuthEndpoint() throws Exception {
        JavaFile file = generator.generate(load("google_sheets.yaml"), PKG);
        String src = file.toString();
        assertTrue(src.contains("googleapis.com"), "Must contain a googleapis.com OAuth token endpoint");
    }

    @Test
    void googleSheets_containsSheetsApiUrl() throws Exception {
        JavaFile file = generator.generate(load("google_sheets.yaml"), PKG);
        String src = file.toString();
        assertTrue(src.contains("sheets.googleapis.com"), "Must contain Sheets API base URL");
    }

    @Test
    void googleSheets_hasMandatorySourceTaskMethods() throws Exception {
        JavaFile file = generator.generate(load("google_sheets.yaml"), PKG);
        boolean hasVersion = file.typeSpec.methodSpecs.stream().anyMatch(m -> m.name.equals("version"));
        boolean hasStart   = file.typeSpec.methodSpecs.stream().anyMatch(m -> m.name.equals("start"));
        boolean hasPoll    = file.typeSpec.methodSpecs.stream().anyMatch(m -> m.name.equals("poll"));
        boolean hasStop    = file.typeSpec.methodSpecs.stream().anyMatch(m -> m.name.equals("stop"));
        assertTrue(hasVersion && hasStart && hasPoll && hasStop,
            "Generated task must have version/start/poll/stop");
    }

    @Test
    void googleSheets_extendsSourceTask() throws Exception {
        JavaFile file = generator.generate(load("google_sheets.yaml"), PKG);
        assertTrue(file.toString().contains("extends SourceTask"), "Must extend SourceTask");
    }

    // ── Unknown dynamic-stream shape — stub ───────────────────────────────────

    @Test
    void unknownDynamicStream_generatesStubWithConnectException() throws Exception {
        // LinkedIn Ads uses dynamic_streams but is not a Google-Sheets-shaped manifest.
        JavaFile file = generator.generate(load("source-linkedin-ads.yaml"), PKG);
        String src = file.toString();
        assertTrue(src.contains("ConnectException"), "Stub must throw ConnectException on start");
        assertFalse(src.contains("discoverSheets"),  "Stub must not contain full Sheets logic");
    }

    @Test
    void unknownDynamicStream_stillExtendsSourceTask() throws Exception {
        JavaFile file = generator.generate(load("source-linkedin-ads.yaml"), PKG);
        assertTrue(file.toString().contains("extends SourceTask"),
            "Stub must still extend SourceTask so the connector loads in Connect");
    }

    // ── JinjaSnippets.allTemplatesRecognized ──────────────────────────────────

    @Test
    void allTemplatesRecognized_simpleConfigRef() {
        assertTrue(JinjaSnippets.allTemplatesRecognized(
            java.util.List.of("https://api.example.com/{{ config['api_key'] }}/data")));
    }

    @Test
    void allTemplatesRecognized_streamPartitionRef() {
        assertTrue(JinjaSnippets.allTemplatesRecognized(
            java.util.List.of("?since={{ stream_partition.cursor_value }}")));
    }

    @Test
    void allTemplatesRecognized_streamPartitionBracketNotation() {
        // Gmail and others use bracket notation: stream_partition['label_id']
        assertTrue(JinjaSnippets.allTemplatesRecognized(
            java.util.List.of("labels/{{ stream_partition['label_id'] }}")));
        assertTrue(JinjaSnippets.allTemplatesRecognized(
            java.util.List.of("messages/{{ stream_partition[\"message_id\"] }}")));
    }

    @Test
    void allTemplatesRecognized_unknownFilter_returnsFalse() {
        // | upper is not a recognised snippet, so this should return false.
        // (Nested-brace patterns like regex_search cannot be detected by the simple
        // ANY_EXPR scanner, so we use a flat-brace unrecognised expression here.)
        assertFalse(JinjaSnippets.allTemplatesRecognized(
            java.util.List.of("{{ config['id'] | upper }}")));
    }

    @Test
    void allTemplatesRecognized_emptyList() {
        assertTrue(JinjaSnippets.allTemplatesRecognized(java.util.List.of()));
    }

    // ── JinjaSnippets.stripTemplatedSuffix ────────────────────────────────────

    @Test
    void stripTemplatedSuffix_removesTemplateAndRemainder() {
        String url = "https://sheets.googleapis.com/v4/spreadsheets/{{ config['spreadsheet_id'] }}/values:batchGet";
        String prefix = JinjaSnippets.stripTemplatedSuffix(url);
        assertTrue(prefix.equals("https://sheets.googleapis.com/v4/spreadsheets/"),
            "Got: " + prefix);
    }

    @Test
    void stripTemplatedSuffix_noTemplate_returnsUnchanged() {
        String url = "https://api.example.com/v1/data";
        assertTrue(JinjaSnippets.stripTemplatedSuffix(url).equals(url));
    }

    @Test
    void stripTemplatedSuffix_null_returnsEmpty() {
        assertTrue(JinjaSnippets.stripTemplatedSuffix(null).isEmpty());
    }
}
