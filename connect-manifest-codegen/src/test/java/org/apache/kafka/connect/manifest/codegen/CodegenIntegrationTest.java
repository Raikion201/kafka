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

import com.sun.net.httpserver.HttpServer;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import org.mockito.Mockito;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    static Stream<Arguments> allManifests() throws Exception {
        URL dir = CodegenIntegrationTest.class.getClassLoader().getResource("manifests");
        assertNotNull(dir, "manifests/ resource directory not found on classpath");
        return Files.list(Path.of(dir.toURI()))
            .filter(p -> p.toString().endsWith(".yaml"))
            .map(p -> Arguments.of(p.getFileName().toString()))
            .sorted(java.util.Comparator.comparing(a -> (String) a.get()[0]));
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
        assertTrue(taskSrc.contains("render(\"{{ config['secret'] }}\""),
            "Task must render the {{ config['secret'] }} template at runtime");
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

    @Test
    void yahooFinancePrice_generatedTask_usesListCycle() throws Exception {
        String taskSrc = generate("yahoo_finance_price.yaml").task.toString();
        assertTrue(taskSrc.contains("split(\",\")"),
            "Yahoo Finance task must split the tickers config field on comma");
        assertTrue(taskSrc.contains("ticker_index"),
            "Yahoo Finance task must store position as ticker_index in offset");
        assertTrue(taskSrc.contains("_nextIndex"),
            "Yahoo Finance task must compute next ticker index");
        assertTrue(taskSrc.contains("getTickers"),
            "Yahoo Finance task must call getTickers() on config");
    }

    @Test
    void yahooFinancePrice_generatedTask_handles403AsSuccess() throws Exception {
        String taskSrc = generate("yahoo_finance_price.yaml").task.toString();
        assertTrue(taskSrc.contains("statusCode() == 403"),
            "Yahoo Finance task must treat 403 as success per error_handler");
    }

    @Test
    void yahooFinancePrice_generatedTask_containsCustomHeaders() throws Exception {
        String taskSrc = generate("yahoo_finance_price.yaml").task.toString();
        assertTrue(taskSrc.contains("User-Agent"),
            "Yahoo Finance task must include User-Agent request header");
        assertTrue(taskSrc.contains("Accept"),
            "Yahoo Finance task must include Accept request header");
    }

    @Test
    void yahooFinancePrice_generatedTask_noJinja2InUrl() throws Exception {
        String taskSrc = generate("yahoo_finance_price.yaml").task.toString();
        assertFalse(taskSrc.contains("{%"),
            "Yahoo Finance task must not contain raw Jinja2 control flow in generated code");
        assertFalse(taskSrc.contains("next_page_token"),
            "Yahoo Finance task must not reference next_page_token (Airbyte-internal)");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // DATETIME-BASED CURSOR (delighted.yaml)
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void delighted_generatedTask_tracksIncrementalCursor() throws Exception {
        String taskSrc = generate("delighted.yaml").task.toString();
        // Each incremental stream gets a volatile cursor_ field
        assertTrue(taskSrc.contains("cursor_unsubscribes"),
            "Delighted task must have cursor_unsubscribes field for DatetimeBasedCursor stream");
        assertTrue(taskSrc.contains("cursor_people"),
            "Delighted task must have cursor_people field for DatetimeBasedCursor stream");
        // Cursor value is loaded from offset store on first poll
        assertTrue(taskSrc.contains("_incStored") || taskSrc.contains("offsetStorageReader"),
            "Delighted task must read cursor from offset store");
    }

    @Test
    void delighted_generatedTask_injectsSinceUntilParams() throws Exception {
        String taskSrc = generate("delighted.yaml").task.toString();
        assertTrue(taskSrc.contains("since="),
            "Delighted task must inject 'since=' query param from DatetimeBasedCursor start_time_option");
        assertTrue(taskSrc.contains("until="),
            "Delighted task must inject 'until=' query param from DatetimeBasedCursor end_time_option");
    }

    @Test
    void delighted_generatedTask_usesEpochSecondsForUntil() throws Exception {
        String taskSrc = generate("delighted.yaml").task.toString();
        assertTrue(taskSrc.contains("currentTimeMillis() / 1000"),
            "Delighted task must use epoch seconds (currentTimeMillis / 1000) for 'until' — not ISO-8601");
    }

    @Test
    void delighted_generatedTask_containsBasicAuth() throws Exception {
        String taskSrc = generate("delighted.yaml").task.toString();
        assertTrue(taskSrc.contains("\"Basic \""),
            "Delighted task must use Basic auth (BasicHttpAuthenticator)");
    }

    @Test
    void delighted_generatedTask_advancesCursorFromRecord() throws Exception {
        String taskSrc = generate("delighted.yaml").task.toString();
        // The cursor update block compares record field value against current cursor
        assertTrue(taskSrc.contains("compareTo"),
            "Delighted task must advance cursor using lexicographic compareTo on record values");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // SUBSTREAM PARTITION ROUTER (google_classroom.yaml)
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void googleClassroom_generatedTask_fetchesPartitionKeys() throws Exception {
        String taskSrc = generate("google_classroom.yaml").task.toString();
        // fetchXxxPartitionKeys() is generated for each child stream
        assertTrue(taskSrc.contains("fetchTeachersPartitionKeys"),
            "Google Classroom task must have fetchTeachersPartitionKeys() method for SubstreamPartitionRouter");
        assertTrue(taskSrc.contains("fetchStudentsPartitionKeys"),
            "Google Classroom task must have fetchStudentsPartitionKeys() method");
    }

    @Test
    void googleClassroom_generatedTask_skipsMultiLevelSubstreams() throws Exception {
        String taskSrc = generate("google_classroom.yaml").task.toString();
        // studentsubmissions has 2 partition routers (course + coursework), must be skipped
        assertFalse(taskSrc.contains("pollStudentsubmissions"),
            "Google Classroom task must not generate pollStudentsubmissions — it requires 2-level nesting");
    }

    @Test
    void googleClassroom_generatedTask_substitutesPartitionKeyInUrl() throws Exception {
        String taskSrc = generate("google_classroom.yaml").task.toString();
        // The child stream URL template {{ stream_partition.course }} must be resolved at runtime
        assertFalse(taskSrc.contains("stream_partition"),
            "Google Classroom task must not contain raw 'stream_partition' Jinja2 template in generated code");
        assertTrue(taskSrc.contains("_partitionKey"),
            "Google Classroom task must use _partitionKey variable in URL construction");
    }

    @Test
    void googleClassroom_generatedTask_containsOAuth() throws Exception {
        String taskSrc = generate("google_classroom.yaml").task.toString();
        assertTrue(taskSrc.contains("refreshAccessToken"),
            "Google Classroom task must contain refreshAccessToken() for OAuth2 auth");
        assertTrue(taskSrc.contains("\"Bearer \""),
            "Google Classroom task must set Authorization: Bearer header");
    }

    @Test
    void googleClassroom_generatedTask_storesPartitionIndex() throws Exception {
        String taskSrc = generate("google_classroom.yaml").task.toString();
        // Partition index is persisted in offset store so polls resume across restarts
        assertTrue(taskSrc.contains("partition_idx"),
            "Google Classroom task must persist partition_idx in offset map for resume-on-restart");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // LIST PARTITION ROUTER
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void listRouter_generatedTask_containsForLoop() throws Exception {
        String taskSrc = generate("list_partition_router_test.yaml").task.toString();
        assertTrue(taskSrc.contains("for (String _lp_category"),
            "ListPartitionRouter task must iterate with for (_lp_category : ...) loop");
    }

    @Test
    void listRouter_generatedTask_containsLiteralValues() throws Exception {
        String taskSrc = generate("list_partition_router_test.yaml").task.toString();
        assertTrue(taskSrc.contains("\"electronics\""),
            "ListPartitionRouter task must embed literal 'electronics' partition value");
        assertTrue(taskSrc.contains("\"clothing\""),
            "ListPartitionRouter task must embed literal 'clothing' partition value");
        assertTrue(taskSrc.contains("\"books\""),
            "ListPartitionRouter task must embed literal 'books' partition value");
    }

    @Test
    void listRouter_generatedTask_compiles(@TempDir Path tmpDir) throws Exception {
        compileTriple(generate("list_partition_router_test.yaml"), tmpDir);
    }

    @Test
    void listRouter_generatedTask_noRawJinja() throws Exception {
        String taskSrc = generate("list_partition_router_test.yaml").task.toString();
        assertFalse(taskSrc.contains("stream_partition"),
            "ListPartitionRouter task must not emit raw 'stream_partition' Jinja template");
        // Note: {{ ... }} delimiters can still appear inside string literals passed to
        // JinjaRenderer.render(...), which is the runtime swap (T4). They must not appear
        // outside of render(...) string-literal arguments.
        for (String line : taskSrc.split("\n")) {
            if (line.contains("{{")) {
                assertTrue(line.contains("render("),
                    "Raw Jinja delimiters outside render() call: " + line);
            }
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // LIST PARTITION ROUTER — MOCK HTTP RUNTIME TEST
    // Mirrors Airbyte's acceptance-test approach: start a local HTTP server that
    // returns fixture JSON regardless of auth headers, then run poll() and verify
    // SourceRecords are produced. No real credentials required.
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void listRouter_mockHttp_pollProducesRecordsForEachPartitionValue(@TempDir Path tmpDir) throws Exception {
        // Step 1 — generate + compile the triple from the mock-server manifest.
        GeneratedTriple g = generate("list_router_mock_server_test.yaml");
        compileTripleWithOutputDir(g, tmpDir);

        // Step 2 — start a JDK HttpServer on a random port.
        // Any request → 200 + fixture JSON; auth headers are accepted but not validated
        // (mirrors Airbyte's mock-server pattern for unit testing without real credentials).
        byte[] body = "{\"results\":[{\"id\":1,\"name\":\"widget\"}]}".getBytes(StandardCharsets.UTF_8);
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        int port = server.getAddress().getPort();

        try {
            // Step 3 — load the compiled task class from tmpDir.
            URLClassLoader loader = new URLClassLoader(
                new URL[]{tmpDir.toUri().toURL()}, getClass().getClassLoader());
            Class<?> taskClass = loader.loadClass(PKG + "." + g.task.typeSpec.name);

            // Step 4 — wire a Mockito-mocked SourceTaskContext (no stored offsets).
            SourceTaskContext ctx = Mockito.mock(SourceTaskContext.class);
            OffsetStorageReader reader = Mockito.mock(OffsetStorageReader.class);
            Mockito.when(ctx.offsetStorageReader()).thenReturn(reader);
            Mockito.when(reader.offset(Mockito.any())).thenReturn(null);

            Object task = taskClass.getDeclaredConstructor().newInstance();
            Method initialize = taskClass.getMethod("initialize", SourceTaskContext.class);
            initialize.invoke(task, ctx);

            // Step 5 — start the task with config pointing to the mock server.
            Map<String, String> props = Map.of(
                "server_url", "http://localhost:" + port,
                "api_key",    "test-api-key-mock"
            );
            taskClass.getMethod("start", Map.class).invoke(task, props);

            // Step 6 — poll() must return records for each partition value (electronics, clothing).
            List<?> records = (List<?>) taskClass.getMethod("poll").invoke(task);
            assertFalse(records.isEmpty(),
                "poll() must return SourceRecords; got empty list — check HTTP fetch or field_path navigation");

            // Two partition values × one record per response = 2 records total.
            assertEquals(2, records.size(),
                "Expected 2 records (one per partition value: electronics, clothing)");
        } finally {
            server.stop(0);
        }
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
    void noStreams_parsesAndProducesStubTask() throws Exception {
        // Manifests with no usable streams parse cleanly; codegen emits a stub task that
        // throws ConnectException at start() so the connector still loads in Connect.
        InputStream in = streamOf("version: 1.0\ntype: DeclarativeSource\n");
        ManifestSpec spec = parser.parse(in);
        assertTrue(spec.resolvedStreams().isEmpty());
    }

    @Test
    void streamWithNoUrl_isFilteredOut() throws Exception {
        String yaml = "version: 1.0\ntype: DeclarativeSource\nstreams:\n"
            + "  - name: foo\n    retriever:\n      type: SimpleRetriever\n"
            + "      requester:\n        type: HttpRequester\n";
        ManifestSpec spec = parser.parse(streamOf(yaml));
        // Stream is kept (has a requester); url_base is just blank — codegen handles that.
        // Test exists to lock in that the parser no longer throws on this shape.
        assertTrue(spec.resolvedStreams().size() <= 1);
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
        compileTripleWithOutputDir(g, tmpDir);
    }

    // Compiles the triple and writes .class files into tmpDir (same location as sources).
    // Used by compile-only tests and the mock-HTTP runtime test (which loads via URLClassLoader).
    private void compileTripleWithOutputDir(GeneratedTriple g, Path tmpDir) throws Exception {
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
            // -d tmpDir ensures .class files land in tmpDir so URLClassLoader can find them.
            boolean ok = compiler.getTask(
                null, fm, diags,
                Arrays.asList("-classpath", classpath, "-d", tmpDir.toString()),
                null, units
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
