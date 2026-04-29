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
package org.apache.kafka.connect.manifest.codegen;

import org.apache.kafka.connect.manifest.codegen.generator.CodegenException;
import org.apache.kafka.connect.manifest.codegen.generator.ConfigGenerator;
import org.apache.kafka.connect.manifest.codegen.generator.ConnectorGenerator;
import org.apache.kafka.connect.manifest.codegen.generator.TaskGenerator;
import org.apache.kafka.connect.manifest.codegen.model.ManifestSpec;
import org.apache.kafka.connect.manifest.codegen.model.RetrieverSpec;
import org.apache.kafka.connect.manifest.codegen.model.StreamSpec;
import org.apache.kafka.connect.manifest.codegen.parser.ManifestParseException;
import org.apache.kafka.connect.manifest.codegen.parser.ManifestParser;

import com.squareup.javapoet.JavaFile;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Full-pipeline integration tests for the code generator.
 *
 * <p>Tests run all three generators — Config, Connector, Task — against real Airbyte manifests
 * and verify that all three files compile together, reference each other correctly, and contain
 * the expected auth headers and pagination patterns.
 */
public class CodegenIntegrationTest {

    private static final String PKG = ConfigGenerator.BASE_PACKAGE;

    private final ManifestParser parser = new ManifestParser();
    private final ConfigGenerator configGen = new ConfigGenerator();
    private final ConnectorGenerator connectorGen = new ConnectorGenerator();
    private final TaskGenerator taskGen = new TaskGenerator();

    private InputStream resource(String name) {
        return getClass().getClassLoader().getResourceAsStream("manifests/" + name);
    }

    private ManifestSpec load(String name) throws Exception {
        return parser.parse(resource(name));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // PARAMETRIZED: all manifests compile
    // ══════════════════════════════════════════════════════════════════════════

    static Stream<Arguments> allManifests() {
        return Stream.of(
            Arguments.of("defillama.yaml"),
            Arguments.of("xkcd.yaml"),
            Arguments.of("zapier.yaml"),
            Arguments.of("gmail.yaml"),
            // ApiKey header — free tier at newsapi.org/register
            Arguments.of("newsapi.yaml"),
            // BasicHttp — free forever at track.toggl.com
            Arguments.of("toggl.yaml"),
            Arguments.of("illumina_basespace.yaml"),
            Arguments.of("box.yaml"),
            Arguments.of("assemblyai.yaml"),
            Arguments.of("akeneo.yaml"),
            Arguments.of("google_analytics_jwt.yaml"),
            // JWT RS256 via GCS IAM — no GA4 UI needed
            Arguments.of("google_cloud_storage_jwt.yaml"),
            Arguments.of("us_census.yaml"),
            Arguments.of("metabase.yaml"),
            Arguments.of("acuity_scheduling.yaml"),
            // Public-API manifests covering each pagination type
            Arguments.of("rickandmorty.yaml"),
            Arguments.of("pokeapi.yaml"),
            Arguments.of("jsonplaceholder.yaml")
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allManifests")
    void allThreeFilesCompileTogether(String manifest, @TempDir Path tmpDir) throws Exception {
        compileTriple(generate(manifest), tmpDir);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // HAPPY PATHS — existing manifests
    // ══════════════════════════════════════════════════════════════════════════

    // ── defillama: no config properties, nested field_path ────────────────────

    @Test
    void defillama_allThreeFilesGenerated() throws Exception {
        GeneratedTriple g = generate("defillama.yaml");
        assertEquals("DefillamaConnectorConfig",   g.config.typeSpec.name);
        assertEquals("DefillamaSourceConnector",   g.connector.typeSpec.name);
        assertEquals("DefillamaSourceTask",        g.task.typeSpec.name);
    }

    @Test
    void defillama_connectorReferencesCorrectTaskAndConfig() throws Exception {
        GeneratedTriple g = generate("defillama.yaml");
        String connectorSrc = g.connector.toString();
        assertTrue(connectorSrc.contains("DefillamaSourceTask.class"),
            "Connector must reference DefillamaSourceTask.class");
        assertTrue(connectorSrc.contains("DefillamaConnectorConfig.config()"),
            "Connector must delegate config() to DefillamaConnectorConfig");
    }

    @Test
    void defillama_taskEmbedsFieldPath() throws Exception {
        GeneratedTriple g = generate("defillama.yaml");
        String taskSrc = g.task.toString();
        assertTrue(taskSrc.contains("chainTvls"),    "Task must navigate field_path[0] = 'chainTvls'");
        assertTrue(taskSrc.contains("Plume Mainnet"), "Task must navigate field_path[1] = 'Plume Mainnet'");
    }

    // ── xkcd: optional config field, cursor pagination ────────────────────────

    @Test
    void xkcd_allThreeFilesGenerated() throws Exception {
        GeneratedTriple g = generate("xkcd.yaml");
        assertEquals("XkcdConnectorConfig",  g.config.typeSpec.name);
        assertEquals("XkcdSourceConnector",  g.connector.typeSpec.name);
        assertEquals("XkcdSourceTask",       g.task.typeSpec.name);
    }

    @Test
    void xkcd_configHasComicNumberConstant() throws Exception {
        GeneratedTriple g = generate("xkcd.yaml");
        boolean found = g.config.typeSpec.fieldSpecs.stream()
            .anyMatch(f -> f.name.equals("COMIC_NUMBER_CONFIG"));
        assertTrue(found, "Config must declare COMIC_NUMBER_CONFIG constant");
    }

    @Test
    void xkcd_generatedTask_containsCursorPagination() throws Exception {
        String taskSrc = generate("xkcd.yaml").task.toString();
        assertTrue(taskSrc.contains("nextCursor"),
            "xkcd uses CursorPagination — task must maintain nextCursor variable");
        assertTrue(taskSrc.contains("\"cursor\""),
            "xkcd cursor must be stored in offset map under key 'cursor'");
    }

    // ── zapier: required field, request_parameters from config ────────────────

    @Test
    void zapier_allThreeFilesGenerated() throws Exception {
        GeneratedTriple g = generate("zapier.yaml");
        assertEquals("ZapierConnectorConfig",  g.config.typeSpec.name);
        assertEquals("ZapierSourceConnector",  g.connector.typeSpec.name);
        assertEquals("ZapierSourceTask",       g.task.typeSpec.name);
    }

    @Test
    void zapier_taskInjectsSecretFromConfig() throws Exception {
        GeneratedTriple g = generate("zapier.yaml");
        String taskSrc = g.task.toString();
        assertTrue(taskSrc.contains("getSecret()"),
            "Task must call config.getSecret() for request_parameters");
        assertTrue(taskSrc.contains("secret="),
            "Task must include 'secret=' query parameter in URL");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // AUTH TYPE ASSERTIONS
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void gmail_generatedTask_containsOAuthRefreshMethod() throws Exception {
        String taskSrc = generate("gmail.yaml").task.toString();
        assertTrue(taskSrc.contains("refreshAccessToken"),
            "OAuth task must contain refreshAccessToken() method");
    }

    @Test
    void gmail_generatedTask_containsBearerHeader() throws Exception {
        String taskSrc = generate("gmail.yaml").task.toString();
        assertTrue(taskSrc.contains("\"Bearer \""),
            "OAuth task must set Authorization: Bearer header");
    }

    @Test
    void illumina_basespace_generatedTask_containsBearerHeader() throws Exception {
        String taskSrc = generate("illumina_basespace.yaml").task.toString();
        assertTrue(taskSrc.contains("\"Bearer \""),
            "Illumina Basespace BearerAuthenticator task must set Authorization: Bearer header");
    }

    @Test
    void newsapi_generatedTask_containsApiKeyHeader() throws Exception {
        String taskSrc = generate("newsapi.yaml").task.toString();
        assertTrue(taskSrc.contains("X-Api-Key"),
            "NewsAPI ApiKeyAuthenticator task must inject X-Api-Key header");
    }

    @Test
    void toggl_generatedTask_containsBasicAuthHeader() throws Exception {
        String taskSrc = generate("toggl.yaml").task.toString();
        assertTrue(taskSrc.contains("\"Basic \""),
            "Toggl BasicHttpAuthenticator task must set Authorization: Basic header");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // PAGINATION ASSERTIONS
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void newsapi_generatedTask_containsPageIncrementPagination() throws Exception {
        String taskSrc = generate("newsapi.yaml").task.toString();
        assertTrue(taskSrc.contains("nextPage"),
            "NewsAPI PageIncrement task must compute nextPage for next poll() call");
        assertTrue(taskSrc.contains("page + 1"),
            "NewsAPI PageIncrement task must advance page by 1");
        assertTrue(taskSrc.contains("page="),
            "NewsAPI PageIncrement task must include page query param");
    }

    @Test
    void illumina_basespace_generatedTask_containsOffsetIncrementPagination() throws Exception {
        String taskSrc = generate("illumina_basespace.yaml").task.toString();
        assertTrue(taskSrc.contains("nextOffset"),
            "Illumina Basespace OffsetIncrement task must compute nextOffset for next poll() call");
        assertTrue(taskSrc.contains("offset + pageLimit"),
            "Illumina Basespace OffsetIncrement task must advance offset by pageLimit");
        assertTrue(taskSrc.contains("offset=") || taskSrc.contains("Offset="),
            "Illumina Basespace OffsetIncrement task must include offset query param");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // NEW AUTH TYPES
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void box_generatedTask_containsClientCredentials() throws Exception {
        String taskSrc = generate("box.yaml").task.toString();
        assertTrue(taskSrc.contains("client_credentials"),
            "Box OAuthAuthenticator client_credentials task must use grant_type=client_credentials");
    }

    @Test
    void akeneo_generatedTask_containsLoginMethod() throws Exception {
        String taskSrc = generate("akeneo.yaml").task.toString();
        assertTrue(taskSrc.contains("loginAndCacheSessionToken"),
            "Akeneo SessionTokenAuthenticator task must contain loginAndCacheSessionToken method");
    }

    @Test
    void akeneo_generatedTask_containsBearerHeader() throws Exception {
        String taskSrc = generate("akeneo.yaml").task.toString();
        assertTrue(taskSrc.contains("\"Bearer \""),
            "Akeneo SessionTokenAuthenticator task must set Authorization: Bearer header");
    }

    @Test
    void google_sheets_generatedTask_containsJwtMethod() throws Exception {
        String taskSrc = generate("google_analytics_jwt.yaml").task.toString();
        assertTrue(taskSrc.contains("buildJwt"),
            "Google Sheets JwtAuthenticator task must contain buildJwt method");
    }

    @Test
    void google_sheets_generatedTask_containsRsaSigning() throws Exception {
        String taskSrc = generate("google_analytics_jwt.yaml").task.toString();
        assertTrue(taskSrc.contains("SHA256withRSA"),
            "Google Sheets JwtAuthenticator task must sign with SHA256withRSA");
    }

    @Test
    void gcs_generatedTask_containsJwtMethod() throws Exception {
        String taskSrc = generate("google_cloud_storage_jwt.yaml").task.toString();
        assertTrue(taskSrc.contains("buildJwt"),
            "GCS JwtAuthenticator task must contain buildJwt method");
    }

    @Test
    void gcs_generatedTask_containsDevstorageScopeInPayload() throws Exception {
        String taskSrc = generate("google_cloud_storage_jwt.yaml").task.toString();
        assertTrue(taskSrc.contains("devstorage.read_only"),
            "GCS task JWT payload must include devstorage.read_only scope");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // NEW PAGINATION TYPES
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void assemblyai_generatedTask_usesRequestPathCursor() throws Exception {
        String taskSrc = generate("assemblyai.yaml").task.toString();
        assertTrue(taskSrc.contains("nextCursor"),
            "AssemblyAI RequestPath cursor task must maintain nextCursor variable");
        // cursor must NOT be appended as a query param — it replaces the whole URL
        assertTrue(!taskSrc.contains("urlBuilder.append") || taskSrc.contains("url ="),
            "AssemblyAI RequestPath cursor task must use cursor as full URL, not query param");
    }

    @Test
    void usCensus_generatedTask_appendsApiKeyAsQueryParam() throws Exception {
        String taskSrc = generate("us_census.yaml").task.toString();
        assertTrue(taskSrc.contains("key="),
            "US Census task must append api key as query param (?key=...)");
        // must NOT set an Authorization or X-API-Key header
        assertTrue(!taskSrc.contains("header(\"X-API-Key") && !taskSrc.contains("header(\"Authorization"),
            "US Census task must not add an auth header when inject_into is request_parameter");
    }

    @Test
    void metabase_generatedTask_containsLegacyLoginMethod() throws Exception {
        String taskSrc = generate("metabase.yaml").task.toString();
        assertTrue(taskSrc.contains("loginAndCacheLegacyToken"),
            "Metabase task must contain loginAndCacheLegacyToken() helper");
    }

    @Test
    void metabase_generatedTask_usesCustomSessionHeader() throws Exception {
        String taskSrc = generate("metabase.yaml").task.toString();
        assertTrue(taskSrc.contains("X-Metabase-Session"),
            "Metabase task must inject session token via X-Metabase-Session header");
        assertTrue(taskSrc.contains("cachedLegacyToken"),
            "Metabase task must use cachedLegacyToken field");
    }

    @Test
    void acuityScheduling_generatedTask_containsDateRangeParams() throws Exception {
        String taskSrc = generate("acuity_scheduling.yaml").task.toString();
        assertTrue(taskSrc.contains("minDate"),
            "Acuity Scheduling task must inject start date as minDate query param");
        assertTrue(taskSrc.contains("maxDate"),
            "Acuity Scheduling task must inject end date as maxDate query param");
    }

    @Test
    void acuityScheduling_generatedTask_containsBasicAuth() throws Exception {
        String taskSrc = generate("acuity_scheduling.yaml").task.toString();
        assertTrue(taskSrc.contains("Basic"),
            "Acuity Scheduling task must use Basic auth header");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // PUBLIC-API MANIFESTS: real-world pagination types
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void rickandmorty_generatedTask_usesRequestPathCursor() throws Exception {
        String taskSrc = generate("rickandmorty.yaml").task.toString();
        assertTrue(taskSrc.contains("nextCursor"),
            "Rick & Morty RequestPath cursor task must maintain nextCursor variable");
        assertTrue(taskSrc.contains("\"cursor\""),
            "Rick & Morty cursor must be stored in offset map under key 'cursor'");
    }

    @Test
    void rickandmorty_generatedTask_navigatesInfoNext() throws Exception {
        String taskSrc = generate("rickandmorty.yaml").task.toString();
        assertTrue(taskSrc.contains("\"info\""),
            "Rick & Morty task must navigate response[\"info\"] to find next page URL");
        assertTrue(taskSrc.contains("\"next\""),
            "Rick & Morty task must extract \"next\" field from info object");
    }

    @Test
    void pokeapi_generatedTask_containsOffsetIncrementPagination() throws Exception {
        String taskSrc = generate("pokeapi.yaml").task.toString();
        assertTrue(taskSrc.contains("nextOffset"),
            "PokéAPI OffsetIncrement task must compute nextOffset for next poll() call");
        assertTrue(taskSrc.contains("offset + pageLimit"),
            "PokéAPI OffsetIncrement task must advance offset by pageLimit");
        assertTrue(taskSrc.contains("offset="),
            "PokéAPI OffsetIncrement task must include offset query param");
        assertTrue(taskSrc.contains("limit="),
            "PokéAPI OffsetIncrement task must include limit query param");
    }

    @Test
    void jsonplaceholder_generatedTask_containsPageIncrementPagination() throws Exception {
        String taskSrc = generate("jsonplaceholder.yaml").task.toString();
        assertTrue(taskSrc.contains("nextPage"),
            "JSONPlaceholder PageIncrement task must compute nextPage for next poll() call");
        assertTrue(taskSrc.contains("page + 1"),
            "JSONPlaceholder PageIncrement task must advance page by 1");
        assertTrue(taskSrc.contains("_page="),
            "JSONPlaceholder PageIncrement task must include _page query param");
        assertTrue(taskSrc.contains("_limit="),
            "JSONPlaceholder PageIncrement task must include _limit query param");
    }

    @Test
    void jsonplaceholder_generatedTask_resetsOnLastPage() throws Exception {
        String taskSrc = generate("jsonplaceholder.yaml").task.toString();
        assertTrue(taskSrc.contains("records.size() < pageLimit"),
            "JSONPlaceholder PageIncrement task must detect last page via incomplete batch");
        assertTrue(taskSrc.contains("startPage"),
            "JSONPlaceholder PageIncrement task must reset to startPage after last page");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // BAD PATHS
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void badYaml_failsAtParseLayer() {
        InputStream bad = streamOf("not: valid: yaml: [[[");
        assertThrows(ManifestParseException.class, () -> parser.parse(bad),
            "Malformed YAML must throw ManifestParseException");
    }

    @Test
    void noStreams_failsAtParseLayer() {
        InputStream in = streamOf("version: 1.0\ntype: DeclarativeSource\n");
        assertThrows(ManifestParseException.class, () -> parser.parse(in),
            "Manifest with no streams must throw ManifestParseException");
    }

    @Test
    void streamWithNoUrl_failsAtParseLayer() {
        String yaml = "version: 1.0\ntype: DeclarativeSource\nstreams:\n"
            + "  - name: foo\n    retriever:\n      type: SimpleRetriever\n"
            + "      requester:\n        type: HttpRequester\n";
        assertThrows(ManifestParseException.class, () -> parser.parse(streamOf(yaml)),
            "Stream with no url/url_base must throw ManifestParseException");
    }

    @Test
    void malformedProperties_failsAtParseLayer() {
        String yaml = "version: 1.0\ntype: DeclarativeSource\nstreams:\n"
            + "  - name: foo\n    retriever:\n      type: SimpleRetriever\n"
            + "      requester:\n        type: HttpRequester\n        url_base: https://x.com\n"
            + "      record_selector:\n        type: RecordSelector\n        extractor:\n"
            + "          type: DpathExtractor\n          field_path: []\n"
            + "spec:\n  connection_specification:\n    properties: not-a-map\n";
        assertThrows(ManifestParseException.class, () -> parser.parse(streamOf(yaml)),
            "Scalar 'properties' must throw ManifestParseException");
    }

    @Test
    void streamWithNoRetriever_failsAtCodegenLayer() {
        ManifestSpec spec = specWithDefinedStreamMissingRetriever();
        assertThrows(CodegenException.class, () -> taskGen.generate(spec, PKG),
            "Stream with no retriever must throw CodegenException from TaskGenerator");
    }

    @Test
    void streamWithNoRequester_failsAtCodegenLayer() {
        ManifestSpec spec = specWithDefinedStreamMissingRequester();
        assertThrows(CodegenException.class, () -> taskGen.generate(spec, PKG),
            "Stream with no requester must throw CodegenException from TaskGenerator");
    }

    @Test
    void emptyStreams_configGeneratorReturnsEmptyFields() throws Exception {
        ManifestSpec spec = load("defillama.yaml");
        JavaFile config = configGen.generate(spec, PKG);
        boolean hasConfigField = config.typeSpec.fieldSpecs.stream()
            .anyMatch(f -> f.name.endsWith("_CONFIG"));
        assertTrue(!hasConfigField,
            "Manifest with no spec properties must generate a config with no _CONFIG fields");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // HELPERS
    // ══════════════════════════════════════════════════════════════════════════

    private GeneratedTriple generate(String manifestName) throws Exception {
        ManifestSpec spec = load(manifestName);
        spec.setManifestName(manifestName.replaceFirst("\\.[^.]+$", ""));
        JavaFile config    = configGen.generate(spec, PKG);
        JavaFile connector = connectorGen.generate(spec, PKG);
        JavaFile task      = taskGen.generate(spec, PKG);
        return new GeneratedTriple(config, connector, task);
    }

    private void compileTriple(GeneratedTriple g, Path tmpDir) throws Exception {
        g.config.writeTo(tmpDir);
        g.connector.writeTo(tmpDir);
        g.task.writeTo(tmpDir);

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull(compiler, "JDK required — javax.tools.JavaCompiler not available");

        DiagnosticCollector<JavaFileObject> diags = new DiagnosticCollector<>();
        String classpath = System.getProperty("java.class.path");
        String pkgDir = PKG.replace('.', File.separatorChar) + File.separator;

        List<File> sources = List.of(
            tmpDir.resolve(pkgDir + g.config.typeSpec.name    + ".java").toFile(),
            tmpDir.resolve(pkgDir + g.connector.typeSpec.name + ".java").toFile(),
            tmpDir.resolve(pkgDir + g.task.typeSpec.name      + ".java").toFile()
        );

        try (var fm = compiler.getStandardFileManager(diags, null, null)) {
            var units = fm.getJavaFileObjectsFromFiles(sources);
            boolean ok = compiler.getTask(
                null, fm, diags, Arrays.asList("-classpath", classpath), null, units
            ).call();
            if (!ok) {
                StringBuilder sb = new StringBuilder("Compilation of generated triple failed:\n");
                diags.getDiagnostics().forEach(d -> sb.append(d).append('\n'));
                throw new AssertionError(sb.toString());
            }
        }
    }

    private static InputStream streamOf(String yaml) {
        return new java.io.ByteArrayInputStream(yaml.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private ManifestSpec specWithDefinedStreamMissingRetriever() {
        StreamSpec bad = new StreamSpec();
        bad.setName("bad_stream");

        java.util.Map<String, StreamSpec> defsMap = new java.util.HashMap<>();
        defsMap.put("bad_stream", bad);

        ManifestSpec.DefinitionsDef defs = new ManifestSpec.DefinitionsDef();
        defs.setStreams(defsMap);

        StreamSpec refEntry = new StreamSpec();

        ManifestSpec spec = new ManifestSpec();
        spec.setVersion("1.0");
        spec.setType("DeclarativeSource");
        spec.setStreams(List.of(refEntry));
        spec.setDefinitions(defs);
        return spec;
    }

    private ManifestSpec specWithDefinedStreamMissingRequester() {
        RetrieverSpec retriever = new RetrieverSpec();

        StreamSpec bad = new StreamSpec();
        bad.setName("bad_stream");
        bad.setRetriever(retriever);

        java.util.Map<String, StreamSpec> defsMap = new java.util.HashMap<>();
        defsMap.put("bad_stream", bad);

        ManifestSpec.DefinitionsDef defs = new ManifestSpec.DefinitionsDef();
        defs.setStreams(defsMap);

        StreamSpec refEntry = new StreamSpec();

        ManifestSpec spec = new ManifestSpec();
        spec.setVersion("1.0");
        spec.setType("DeclarativeSource");
        spec.setStreams(List.of(refEntry));
        spec.setDefinitions(defs);
        return spec;
    }

    private static final class GeneratedTriple {
        final JavaFile config;
        final JavaFile connector;
        final JavaFile task;

        GeneratedTriple(JavaFile config, JavaFile connector, JavaFile task) {
            this.config    = config;
            this.connector = connector;
            this.task      = task;
        }
    }
}
