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
package org.apache.kafka.connect.manifest.codegen.parser;

import org.apache.kafka.connect.manifest.codegen.model.ManifestSpec;

import org.junit.jupiter.api.Test;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ManifestParserTest {

    private final ManifestParser parser = new ManifestParser();

    private InputStream resource(String name) {
        return getClass().getClassLoader().getResourceAsStream("manifests/" + name);
    }

    // ── defillama ─────────────────────────────────────────────────────────────

    @Test
    void defillama_parsesStreamName() throws Exception {
        ManifestSpec spec = parser.parse(resource("defillama.yaml"));
        assertEquals(1, spec.getStreams().size());
        assertEquals("Defillama", spec.getStreams().get(0).getName());
    }

    @Test
    void defillama_parsesBaseUrl() throws Exception {
        ManifestSpec spec = parser.parse(resource("defillama.yaml"));
        String base = spec.getStreams().get(0).getRetriever().getRequester().effectiveBaseUrl();
        assertEquals("https://api.llama.fi/protocol/curve-finance", base);
    }

    @Test
    void defillama_parsesFieldPath() throws Exception {
        ManifestSpec spec = parser.parse(resource("defillama.yaml"));
        var fieldPath = spec.getStreams().get(0).getRetriever().getRecordSelector().getExtractor().getFieldPath();
        assertEquals(2, fieldPath.size());
        assertEquals("chainTvls", fieldPath.get(0));
        assertEquals("Plume Mainnet", fieldPath.get(1));
    }

    @Test
    void defillama_emptySpec() throws Exception {
        ManifestSpec spec = parser.parse(resource("defillama.yaml"));
        assertNotNull(spec.getSpec());
        // defillama has an empty properties map — ConnectionSpec is typed so no raw cast needed
        assertTrue(spec.getSpec().getConnectionSpecification().getProperties().isEmpty());
    }

    // ── xkcd ─────────────────────────────────────────────────────────────────

    @Test
    void xkcd_parsesStreamName() throws Exception {
        ManifestSpec spec = parser.parse(resource("xkcd.yaml"));
        // xkcd uses $ref in streams list — resolved via definitions
        assertEquals("xkcd", spec.resolvedStreams().get(0).getName());
    }

    @Test
    void xkcd_parsesUrlBase() throws Exception {
        ManifestSpec spec = parser.parse(resource("xkcd.yaml"));
        String base = spec.resolvedStreams().get(0).getRetriever().getRequester().effectiveBaseUrl();
        assertEquals("https://xkcd.com", base);
    }

    @Test
    void xkcd_parsesConfigField() throws Exception {
        ManifestSpec spec = parser.parse(resource("xkcd.yaml"));
        assertNotNull(spec.getSpec());
        var props = spec.getSpec().getConnectionSpecification().getProperties();
        assertNotNull(props);
        assertTrue(props.containsKey("comic_number"));
    }

    @Test
    void xkcd_primaryKey() throws Exception {
        ManifestSpec spec = parser.parse(resource("xkcd.yaml"));
        var pk = spec.resolvedStreams().get(0).getPrimaryKey();
        assertEquals(1, pk.size());
        assertEquals("num", pk.get(0));
    }

    // ── zapier ────────────────────────────────────────────────────────────────

    @Test
    void zapier_parsesStreamName() throws Exception {
        ManifestSpec spec = parser.parse(resource("zapier.yaml"));
        assertEquals("zapier_supported_storage", spec.getStreams().get(0).getName());
    }

    @Test
    void zapier_parsesUrlBase() throws Exception {
        ManifestSpec spec = parser.parse(resource("zapier.yaml"));
        String base = spec.getStreams().get(0).getRetriever().getRequester().effectiveBaseUrl();
        assertEquals("https://store.zapier.com/api", base);
    }

    @Test
    void zapier_parsesPath() throws Exception {
        ManifestSpec spec = parser.parse(resource("zapier.yaml"));
        String path = spec.getStreams().get(0).getRetriever().getRequester().getPath();
        assertEquals("/records", path);
    }

    @Test
    void zapier_parsesRequiredConfigField() throws Exception {
        ManifestSpec spec = parser.parse(resource("zapier.yaml"));
        var props = spec.getSpec().getConnectionSpecification().getProperties();
        assertTrue(props.containsKey("secret"));
    }

    @Test
    void zapier_emptyFieldPath() throws Exception {
        ManifestSpec spec = parser.parse(resource("zapier.yaml"));
        var fieldPath = spec.getStreams().get(0).getRetriever().getRecordSelector().getExtractor().getFieldPath();
        assertTrue(fieldPath.isEmpty());
    }

    // ── typed ConnectionSpec ──────────────────────────────────────────────────

    @Test
    void zapier_parsesRequiredList() throws Exception {
        ManifestSpec spec = parser.parse(resource("zapier.yaml"));
        var required = spec.getSpec().getConnectionSpecification().getRequired();
        assertTrue(required.contains("secret"), "Expected 'secret' in required list");
    }

    @Test
    void xkcd_propertyDefHasDescription() throws Exception {
        ManifestSpec spec = parser.parse(resource("xkcd.yaml"));
        ManifestSpec.PropertyDef def = spec.getSpec().getConnectionSpecification()
            .getProperties().get("comic_number");
        assertNotNull(def);
        assertTrue(def.effectiveDoc().contains("comic"), "effectiveDoc must return the description");
    }

    @Test
    void malformedProperties_throwsParseException() {
        // With typed POJOs, Jackson rejects a non-map value for 'properties' at parse time
        String yaml = "version: 1.0\ntype: DeclarativeSource\n"
            + "streams:\n  - name: foo\n    retriever:\n      type: SimpleRetriever\n"
            + "      requester:\n        type: HttpRequester\n        url_base: https://x.com\n"
            + "      record_selector:\n        type: RecordSelector\n        extractor:\n"
            + "          type: DpathExtractor\n          field_path: []\n"
            + "spec:\n  connection_specification:\n    properties: not-a-map\n";
        InputStream in = new java.io.ByteArrayInputStream(yaml.getBytes());
        assertThrows(ManifestParseException.class, () -> parser.parse(in));
    }

    // ── validation ────────────────────────────────────────────────────────────

    @Test
    void missingStreams_parsesEmptyAndDelegatesToCodegenStub() throws Exception {
        InputStream empty = new java.io.ByteArrayInputStream("version: 1.0\ntype: DeclarativeSource\n".getBytes());
        ManifestSpec spec = parser.parse(empty);
        assertTrue(spec.resolvedStreams().isEmpty());
    }

    @Test
    void missingRetriever_filtersStream() throws Exception {
        String yaml = "version: 1.0\ntype: DeclarativeSource\nstreams:\n  - name: foo\n";
        InputStream in = new java.io.ByteArrayInputStream(yaml.getBytes());
        ManifestSpec spec = parser.parse(in);
        assertTrue(spec.resolvedStreams().isEmpty());
    }

    @Test
    void missingUrl_keepsStreamForCodegenToHandle() throws Exception {
        String yaml = "version: 1.0\ntype: DeclarativeSource\nstreams:\n  - name: foo\n    retriever:\n      type: SimpleRetriever\n      requester:\n        type: HttpRequester\n";
        InputStream in = new java.io.ByteArrayInputStream(yaml.getBytes());
        // No throw; codegen handles blank url.
        parser.parse(in);
    }

    // ── class name helper ─────────────────────────────────────────────────────

    @Test
    void toClassName_snakeCase() {
        assertEquals("ZapierSupportedStorage", ManifestSpec.toClassName("zapier_supported_storage"));
    }

    @Test
    void toClassName_simple() {
        assertEquals("Xkcd", ManifestSpec.toClassName("xkcd"));
    }

    @Test
    void connectorClassName_usesFirstStream() throws Exception {
        ManifestSpec spec = parser.parse(resource("xkcd.yaml"));
        assertEquals("XkcdSource", spec.connectorClassName());
    }

    // ── ListPartitionRouter ───────────────────────────────────────────────────

    @Test
    void listPartitionRouter_parsesValues() throws Exception {
        ManifestSpec spec = parser.parse(resource("list_partition_router_test.yaml"));
        var routers = spec.resolvedStreams().get(0).getRetriever().getListRouters();
        assertEquals(1, routers.size());
        var lr = routers.get(0);
        assertTrue(lr.isList(), "PartitionRouterSpec.isList() must return true for ListPartitionRouter");
        assertEquals(java.util.List.of("electronics", "clothing", "books"), lr.getValues());
    }

    @Test
    void listPartitionRouter_parsesCursorField() throws Exception {
        ManifestSpec spec = parser.parse(resource("list_partition_router_test.yaml"));
        var lr = spec.resolvedStreams().get(0).getRetriever().getListRouters().get(0);
        assertEquals("category", lr.getCursorField());
    }

    @Test
    void listPartitionRouter_parsesRequestOption() throws Exception {
        ManifestSpec spec = parser.parse(resource("list_partition_router_test.yaml"));
        var lr = spec.resolvedStreams().get(0).getRetriever().getListRouters().get(0);
        assertNotNull(lr.getRequestOption());
        assertEquals("category", lr.getRequestOption().getFieldName());
        assertTrue(lr.getRequestOption().isRequestParameter());
    }

    @Test
    void listPartitionRouter_retrieverExposesListRouters() throws Exception {
        ManifestSpec spec = parser.parse(resource("list_partition_router_test.yaml"));
        var retriever = spec.resolvedStreams().get(0).getRetriever();
        assertEquals(1, retriever.getListRouters().size());
        assertEquals(0, retriever.getSubstreamRouter() == null ? 0 : 1);
        assertTrue(!retriever.hasSubstreamPartition());
    }

    // ── DefaultErrorHandler / backoff / response_filter ───────────────────────

    @Test
    void airtable_parsesDefaultErrorHandlerMaxRetriesAndBackoffStrategies() throws Exception {
        ManifestSpec spec = parser.parse(resource("source-airtable.yaml"));
        var requester = spec.resolvedStreams().get(0).getRetriever().getRequester();
        var eh = requester.getErrorHandler();
        assertNotNull(eh, "airtable bases stream must inherit error_handler from base_requester via $ref");
        assertEquals("DefaultErrorHandler", eh.getType());
        assertEquals(Integer.valueOf(10), eh.getMaxRetries());
        assertEquals(1, eh.getBackoffStrategies().size());
        var backoff = eh.getBackoffStrategies().get(0);
        assertEquals("ConstantBackoffStrategy", backoff.getType());
        assertEquals(Double.valueOf(30.0), backoff.getBackoffTimeInSeconds());
    }

    @Test
    void airtable_parsesResponseFilterPredicateAndFailureType() throws Exception {
        ManifestSpec spec = parser.parse(resource("source-airtable.yaml"));
        var eh = spec.resolvedStreams().get(0).getRetriever().getRequester().getErrorHandler();
        var filters = eh.getResponseFilters();
        assertEquals(3, filters.size(), "airtable defines 3 response_filters (predicate + 2 http_codes)");

        var predicateFilter = filters.get(0);
        assertNotNull(predicateFilter.getPredicate());
        assertTrue(predicateFilter.getPredicate().contains("INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND"));
        assertEquals("FAIL", predicateFilter.getAction());
        assertEquals("config_error", predicateFilter.getFailureType());
        assertNotNull(predicateFilter.getErrorMessage());
        assertTrue(predicateFilter.getErrorMessage().contains("Personal Access Token"));

        var codeFilter = filters.get(1);
        assertEquals(java.util.List.of(403, 422), codeFilter.getHttpCodes());
        assertEquals("FAIL", codeFilter.getAction());
        assertEquals("config_error", codeFilter.getFailureType());
        assertEquals("Permission denied or entity is unprocessable.", codeFilter.getErrorMessage());
    }

    @Test
    void assemblyai_parsesCompositeErrorHandlerWithIgnoreFilter() throws Exception {
        ManifestSpec spec = parser.parse(resource("assemblyai.yaml"));
        var lemur = spec.resolvedStreams().stream()
            .filter(s -> "lemur_response".equals(s.getName()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("lemur_response stream missing"));
        var eh = lemur.getRetriever().getRequester().getErrorHandler();
        assertNotNull(eh);
        assertEquals("CompositeErrorHandler", eh.getType());
        assertEquals(1, eh.getErrorHandlers().size());

        var nested = eh.getErrorHandlers().get(0);
        assertEquals("DefaultErrorHandler", nested.getType());
        assertEquals(1, nested.getResponseFilters().size());

        var filter = nested.getResponseFilters().get(0);
        assertEquals("IGNORE", filter.getAction());
        assertEquals(java.util.List.of(401), filter.getHttpCodes());
        assertEquals("Paid plan required", filter.getErrorMessage());
    }
}
