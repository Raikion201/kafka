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
        assertTrue(spec.getSpec().getConnectionSpecification().isEmpty()
            || spec.getSpec().getConnectionSpecification().get("properties") != null);
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
        var props = (java.util.Map<?, ?>) spec.getSpec().getConnectionSpecification().get("properties");
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
        var props = (java.util.Map<?, ?>) spec.getSpec().getConnectionSpecification().get("properties");
        assertTrue(props.containsKey("secret"));
    }

    @Test
    void zapier_emptyFieldPath() throws Exception {
        ManifestSpec spec = parser.parse(resource("zapier.yaml"));
        var fieldPath = spec.getStreams().get(0).getRetriever().getRecordSelector().getExtractor().getFieldPath();
        assertTrue(fieldPath.isEmpty());
    }

    // ── validation ────────────────────────────────────────────────────────────

    @Test
    void missingStreams_throws() {
        InputStream empty = new java.io.ByteArrayInputStream("version: 1.0\ntype: DeclarativeSource\n".getBytes());
        assertThrows(ManifestParseException.class, () -> parser.parse(empty));
    }

    @Test
    void missingRetriever_throws() {
        String yaml = "version: 1.0\ntype: DeclarativeSource\nstreams:\n  - name: foo\n";
        InputStream in = new java.io.ByteArrayInputStream(yaml.getBytes());
        assertThrows(ManifestParseException.class, () -> parser.parse(in));
    }

    @Test
    void missingUrl_throws() {
        String yaml = "version: 1.0\ntype: DeclarativeSource\nstreams:\n  - name: foo\n    retriever:\n      type: SimpleRetriever\n      requester:\n        type: HttpRequester\n";
        InputStream in = new java.io.ByteArrayInputStream(yaml.getBytes());
        assertThrows(ManifestParseException.class, () -> parser.parse(in));
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
}
