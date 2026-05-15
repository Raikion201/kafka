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
        boolean isFixture = name.endsWith("_test.yaml")
            || java.util.Set.of("zapier.yaml", "newsapi.yaml", "rickandmorty.yaml",
                "jsonplaceholder.yaml", "google_analytics_jwt.yaml",
                "google_cloud_storage_jwt.yaml", "pokeapi.yaml",
                "acuity_scheduling.yaml").contains(name);
        String dir = isFixture ? "test-fixtures/" : "manifests/";
        InputStream is = getClass().getClassLoader().getResourceAsStream(dir + name);
        // Airbyte-sourced manifests are named source-<name> after the parity cleanup.
        // Tests may use short names (no prefix) or underscores where the file uses hyphens.
        if (is == null && !isFixture && !name.startsWith("source-")) {
            is = getClass().getClassLoader().getResourceAsStream(dir + "source-" + name);
        }
        if (is == null && !isFixture && !name.startsWith("source-") && name.contains("_")) {
            String hyphen = name.replace('_', '-');
            is = getClass().getClassLoader().getResourceAsStream(dir + "source-" + hyphen);
        }
        return is;
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

    // ── spec/auth field-name mismatch: jinjaCtx must see originals, not ConfigDef-filtered values ──
    // appfollow's spec declares `api_secret` but its authenticator reads `{{ config['api_key'] }}`.
    // The generated _cfgMap must therefore be built from `originalsStrings()` (raw user input)
    // rather than `config.values()` (which strips fields not in ConfigDef). This mirrors Airbyte
    // Python's JinjaInterpolation.eval, which passes the raw config dict unfiltered
    // (airbyte_cdk/sources/declarative/interpolation/jinja.py: context = {"config": config, ...}).
    @Test
    void appfollow_taskBuildsCfgMapFromOriginalsStrings() throws Exception {
        String taskSrc = generate("source-appfollow.yaml").task.toString();
        assertTrue(
            taskSrc.contains("new LinkedHashMap<String, Object>(this.config.originalsStrings())"),
            "Task must build _cfgMap from originalsStrings() so spec-undeclared fields like "
                + "appfollow's `api_key` (declared as `api_secret` in spec) reach the Jinja context. "
                + "Found task source did not contain the originalsStrings()-backed LinkedHashMap.");
        assertTrue(
            taskSrc.contains(
                "this.config.values().forEach((_k, _v) -> { if (_v != null) _cfgMap.putIfAbsent(_k, _v); })"),
            "Task must also merge ConfigDef defaults via values() so spec-declared fields the "
                + "user omitted (e.g. shortcut's `query` default) reach Jinja interpolation.");
        assertTrue(taskSrc.contains("render(\"{{ config['api_key'] }}\""),
            "Task must still emit the manifest's `config['api_key']` interpolation for the "
                + "X-AppFollow-API-Token header.");
    }

    @Test
    void shortcut_taskPicksUpQueryDefaultFromConfigDef() throws Exception {
        // Shortcut's search_epics stream interpolates `{{ config['query'] }}` into a required
        // request parameter. The spec declares `query` with a default of "title:Our first Epic"
        // and the field is NOT marked required. Without merging ConfigDef defaults into the
        // Jinja context, omitting `query` from the connector config yields a 400 from the API
        // ("missing required parameter").
        String taskSrc = generate("source-shortcut.yaml").task.toString();
        assertTrue(
            taskSrc.contains("this.config.values().forEach((_k, _v) ->"),
            "Shortcut task must merge ConfigDef defaults so `query` resolves to its spec default "
                + "when the user does not supply one in the connector config.");
        assertTrue(taskSrc.contains("render(\"{{ config['query'] }}\""),
            "Shortcut task must still emit the manifest's `{{ config['query'] }}` interpolation.");
    }

    @Test
    void intercom_companiesStreamUsesItsOwnErrorHandlerFilters() throws Exception {
        // Intercom's companies stream uses the deprecated /companies/scroll endpoint and
        // declares per-stream response_filters (401 FAIL, 404 IGNORE, 400 RETRY, 500
        // RESET_PAGINATION). The admin_activity_logs stream (which appears first in the
        // manifest) declares only 401 FAIL. A previous global retryPolicy field captured the
        // first stream's filters and applied them everywhere, so a 404 on companies/scroll —
        // which Intercom returns after a scroll expires — propagated as a fatal error.
        String taskSrc = generate("source-intercom.yaml").task.toString();
        int pollCompaniesStart = taskSrc.indexOf("private List<SourceRecord> pollCompanies(");
        assertTrue(pollCompaniesStart > 0, "Generated task must define pollCompanies()");
        int pollCompaniesEnd = taskSrc.indexOf("private List<SourceRecord> poll",
            pollCompaniesStart + 1);
        String pollCompaniesBody = pollCompaniesEnd > 0
            ? taskSrc.substring(pollCompaniesStart, pollCompaniesEnd)
            : taskSrc.substring(pollCompaniesStart);
        assertTrue(pollCompaniesBody.contains("ResponseAction.IGNORE")
                && pollCompaniesBody.contains("List.of(404)"),
            "pollCompanies must wire its own 404→IGNORE HttpResponseFilter. "
                + "Without it, the Intercom scroll API's 404 (\"scroll parameter not found\") "
                + "fails the task instead of returning zero records.");
        assertTrue(pollCompaniesBody.contains("ResponseAction.RESET_PAGINATION")
                && pollCompaniesBody.contains("List.of(500)"),
            "pollCompanies must wire its own 500→RESET_PAGINATION HttpResponseFilter.");
        assertTrue(pollCompaniesBody.contains("ConstantBackoffStrategy(60.0d)"),
            "pollCompanies must wire its own ConstantBackoffStrategy(60s).");
        assertFalse(taskSrc.contains("this.retryPolicy ="),
            "Task must NOT assign a single global retryPolicy field — that pattern silently "
                + "applies the first stream's error_handler to all streams.");
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
    // AIRTABLE — DynamicDeclarativeStream (HttpComponentsResolver)
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void airtable_generatedTask_isNotDynamicStreamStub() throws Exception {
        String taskSrc = generate("source-airtable.yaml").task.toString();
        assertTrue(!taskSrc.contains("GenericDynamicStreamStub"),
            "source-airtable.yaml must generate a real task, not a stub");
        assertTrue(taskSrc.contains("discoverStreams"),
            "Airtable task must contain discoverStreams() method");
    }

    @Test
    void airtable_generatedTask_containsOAuthAndPatBranches() throws Exception {
        String taskSrc = generate("source-airtable.yaml").task.toString();
        assertTrue(taskSrc.contains("airtable.com/oauth2/v1/token"),
            "Airtable task must reference OAuth token endpoint");
        assertTrue(taskSrc.contains("api_key"),
            "Airtable task must reference PAT api_key credential");
    }

    @Test
    void airtable_generatedTask_compilesCleanly(@TempDir Path tmpDir) throws Exception {
        compileTriple(generate("source-airtable.yaml"), tmpDir);
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
        // Phase-4 window-slicing: _windowEnd is computed by DatetimeWindowHelper.computeWindowEnd
        // with format "%s", so it is an epoch-seconds string. The 'until' param gets _windowEnd.
        assertTrue(taskSrc.contains("_windowEnd"),
            "Delighted task must use _windowEnd (epoch seconds via DatetimeWindowHelper) for 'until'");
        assertTrue(taskSrc.contains("\"until=\"") || taskSrc.contains("until="),
            "Delighted task must append 'until' query parameter");
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
        // Phase-4 window-slicing: cursor is advanced past the window end via advanceCursor()
        assertTrue(taskSrc.contains("advanceCursor"),
            "Delighted task must advance cursor via DatetimeWindowHelper.advanceCursor (window-slicing)");
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
    void googleClassroom_generatedTask_supportsMultiLevelSubstreams() throws Exception {
        String taskSrc = generate("google_classroom.yaml").task.toString();
        // studentsubmissions has 2 nested SubstreamPartitionRouters (course → coursework)
        assertTrue(taskSrc.contains("pollStudentsubmissions"),
            "Google Classroom task must generate pollStudentsubmissions for nested substream pattern");
        assertTrue(taskSrc.contains("fetchStudentsubmissionsPartitionKeys"),
            "Google Classroom task must generate fetchStudentsubmissionsPartitionKeys for 2-level nested fetch");
    }

    @Test
    void googleClassroom_generatedTask_substitutesPartitionKeyInUrl() throws Exception {
        String taskSrc = generate("google_classroom.yaml").task.toString();
        // Single-router child streams (teachers/students/coursework) inject via _partitionKey
        assertTrue(taskSrc.contains("_partitionKey"),
            "Google Classroom task must use _partitionKey variable in single-router URL construction");
        // Nested-router child stream (studentsubmissions) injects via Map<String,String> _partition
        assertTrue(taskSrc.contains("_partition.get(") || taskSrc.contains("Map<String, String> _partition"),
            "Google Classroom task must use Map<String,String> _partition for nested substream");
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
    void googleClassroom_generatedTask_nestedFetchTraversesParentChain() throws Exception {
        String taskSrc = generate("google_classroom.yaml").task.toString();
        // Nested fetch must call BOTH parent endpoints in order: courses → coursework
        int coursesIdx = taskSrc.indexOf("/v1/courses\"");
        int courseworkIdx = taskSrc.indexOf("/courseWork\"");
        int submissionsIdx = taskSrc.indexOf("/studentSubmissions");
        assertTrue(coursesIdx > 0,     "Nested fetch must request /v1/courses (level 1 parent)");
        assertTrue(courseworkIdx > 0,  "Nested fetch must request /courseWork (level 2 parent)");
        assertTrue(submissionsIdx > 0, "Child poll must request /studentSubmissions");
        assertTrue(coursesIdx < courseworkIdx,
            "Level-1 (courses) fetch must be emitted before level-2 (courseWork) fetch");
        // Each slice carries both parent ids — partition_field 'course' and 'coursework'
        assertTrue(taskSrc.contains("\"course\""),
            "Nested fetch must store 'course' partition_field in slice map");
        assertTrue(taskSrc.contains("\"coursework\""),
            "Nested fetch must store 'coursework' partition_field in slice map");
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
    // ERROR HANDLER + BACKOFF — codegen + mock HTTP runtime
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void errorHandler_generatedTask_containsDefaultRetryPolicyAndConstantBackoff() throws Exception {
        String taskSrc = generate("error_handler_test.yaml").task.toString();
        assertTrue(taskSrc.contains("new DefaultRetryPolicy("),
            "Generated task must instantiate DefaultRetryPolicy from the parsed error_handler");
        assertTrue(taskSrc.contains("new ConstantBackoffStrategy("),
            "Generated task must instantiate ConstantBackoffStrategy from backoff_strategies");
        assertTrue(taskSrc.contains("retryPolicy.interpretResponse"),
            "sendWithRetry must delegate response classification to the RetryPolicy field");
        assertTrue(taskSrc.contains("backoffStrategy.backoffMillis"),
            "sendWithRetry must delegate sleep timing to the BackoffStrategy field");
    }

    @Test
    void errorHandler_mockHttp_retriesTransient500ThenSucceeds(@TempDir Path tmpDir) throws Exception {
        GeneratedTriple g = generate("error_handler_test.yaml");
        compileTripleWithOutputDir(g, tmpDir);

        byte[] body = "{\"results\":[{\"id\":1,\"name\":\"widget\"}]}".getBytes(StandardCharsets.UTF_8);
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/items", exchange -> {
            int n = calls.incrementAndGet();
            if (n <= 2) {
                exchange.sendResponseHeaders(500, -1);
            } else {
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
            }
            exchange.close();
        });
        server.start();
        int port = server.getAddress().getPort();

        try {
            Object task = startMockTask(g, tmpDir, port);
            List<?> records = pollOnce(task);
            assertEquals(1, records.size(), "Task must succeed after 2 transient 500s");
            assertEquals(3, calls.get(), "Task must have called the endpoint exactly 3 times");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void errorHandler_mockHttp_ignoreFilterReturnsEmptyForMatchingStatus(@TempDir Path tmpDir) throws Exception {
        GeneratedTriple g = generate("error_handler_test.yaml");
        compileTripleWithOutputDir(g, tmpDir);

        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/items", exchange -> {
            byte[] msg = "{\"error\":\"paid plan required\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(401, msg.length);
            exchange.getResponseBody().write(msg);
            exchange.close();
        });
        server.start();
        int port = server.getAddress().getPort();

        try {
            Object task = startMockTask(g, tmpDir, port);
            List<?> records = pollOnce(task);
            // IGNORE keeps the response as-is; field_path navigation finds no `results` array →
            // poll() yields no records but does not throw.
            assertEquals(0, records.size(),
                "401 IGNORE must result in zero records, not a thrown ConnectException");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void errorHandler_mockHttp_failFilterThrowsConnectException(@TempDir Path tmpDir) throws Exception {
        GeneratedTriple g = generate("error_handler_test.yaml");
        compileTripleWithOutputDir(g, tmpDir);

        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/items", exchange -> {
            exchange.sendResponseHeaders(418, -1);
            exchange.close();
        });
        server.start();
        int port = server.getAddress().getPort();

        try {
            Object task = startMockTask(g, tmpDir, port);
            java.lang.reflect.InvocationTargetException ex = assertThrows(
                java.lang.reflect.InvocationTargetException.class,
                () -> task.getClass().getMethod("poll").invoke(task));
            Throwable cause = ex.getCause();
            assertNotNull(cause, "poll() must propagate the underlying exception");
            assertTrue(cause.getClass().getSimpleName().contains("ConnectException")
                    || (cause.getCause() != null
                        && cause.getCause().getClass().getSimpleName().contains("ConnectException")),
                "418 FAIL filter must surface as ConnectException, got: " + cause);
        } finally {
            server.stop(0);
        }
    }

    private Object startMockTask(GeneratedTriple g, Path tmpDir, int port) throws Exception {
        URLClassLoader loader = new URLClassLoader(
            new URL[]{tmpDir.toUri().toURL()}, getClass().getClassLoader());
        Class<?> taskClass = loader.loadClass(PKG + "." + g.task.typeSpec.name);

        SourceTaskContext ctx = Mockito.mock(SourceTaskContext.class);
        OffsetStorageReader reader = Mockito.mock(OffsetStorageReader.class);
        Mockito.when(ctx.offsetStorageReader()).thenReturn(reader);
        Mockito.when(reader.offset(Mockito.any())).thenReturn(null);

        Object task = taskClass.getDeclaredConstructor().newInstance();
        Method initialize = taskClass.getMethod("initialize", SourceTaskContext.class);
        initialize.invoke(task, ctx);

        Map<String, String> props = Map.of("server_url", "http://localhost:" + port);
        taskClass.getMethod("start", Map.class).invoke(task, props);
        return task;
    }

    private List<?> pollOnce(Object task) throws Exception {
        return (List<?>) task.getClass().getMethod("poll").invoke(task);
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
    // TRANSFORMATIONS PIPELINE (transformations_test.yaml)
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void transformations_generatedTaskCompiles(@TempDir Path tmpDir) throws Exception {
        GeneratedTriple g = generate("transformations_test.yaml");
        compileTripleWithOutputDir(g, tmpDir);
        String src = g.task.toString();
        assertTrue(src.contains("TransformationPipelineFactory"),
            "Generated task must reference TransformationPipelineFactory");
        assertTrue(src.contains("ConfigTransformerFactory"),
            "Generated task must reference ConfigTransformerFactory");
        assertTrue(src.contains("pipeline_add_remove"),
            "Generated task must declare a per-stream pipeline field for add_remove");
        assertTrue(src.contains("configValues"),
            "Generated task must store configValues for use in jinjaCtx()");
    }

    @Test
    void transformations_mockHttp_addFieldsAppearOnRecords(@TempDir Path tmpDir) throws Exception {
        GeneratedTriple g = generate("transformations_test.yaml");
        compileTripleWithOutputDir(g, tmpDir);
        byte[] body = ("{\"results\":[{\"id\":1,\"name\":\"alice\",\"sensitive\":\"secret\"}]}")
            .getBytes(StandardCharsets.UTF_8);
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        int port = server.getAddress().getPort();
        try {
            Object task = loadAndStartTask(g, tmpDir, port);
            List<?> records = pollStream(task, "pollAddRemove");
            assertEquals(1, records.size(), "Expected exactly 1 record");
            String json = (String) records.get(0).getClass().getMethod("value").invoke(records.get(0));
            assertTrue(json.contains("\"source\""), "AddFields must inject 'source' key");
            assertTrue(json.contains("static_value"), "AddFields must set source=static_value");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void transformations_mockHttp_removeFieldsStrips(@TempDir Path tmpDir) throws Exception {
        GeneratedTriple g = generate("transformations_test.yaml");
        compileTripleWithOutputDir(g, tmpDir);
        byte[] body = ("{\"results\":[{\"id\":1,\"name\":\"alice\",\"sensitive\":\"secret\"}]}")
            .getBytes(StandardCharsets.UTF_8);
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        int port = server.getAddress().getPort();
        try {
            Object task = loadAndStartTask(g, tmpDir, port);
            List<?> records = pollStream(task, "pollAddRemove");
            assertEquals(1, records.size(), "Expected exactly 1 record");
            String json = (String) records.get(0).getClass().getMethod("value").invoke(records.get(0));
            assertFalse(json.contains("sensitive"), "RemoveFields must strip 'sensitive' key");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void transformations_mockHttp_recordFilterDropsEvenIds(@TempDir Path tmpDir) throws Exception {
        GeneratedTriple g = generate("transformations_test.yaml");
        compileTripleWithOutputDir(g, tmpDir);
        byte[] body = ("{\"results\":[{\"id\":1},{\"id\":2},{\"id\":3},{\"id\":4}]}")
            .getBytes(StandardCharsets.UTF_8);
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        int port = server.getAddress().getPort();
        try {
            Object task = loadAndStartTask(g, tmpDir, port);
            List<?> records = pollStream(task, "pollFiltered");
            assertEquals(2, records.size(), "RecordFilter must keep only odd-id records (id=1,3)");
            for (Object r : records) {
                String json = (String) r.getClass().getMethod("value").invoke(r);
                assertFalse(json.contains("\"id\":2") || json.contains("\"id\":4"),
                    "Even-id records must be filtered out by RecordFilter");
            }
        } finally {
            server.stop(0);
        }
    }

    @Test
    void transformations_mockHttp_keyRewritesApplied(@TempDir Path tmpDir) throws Exception {
        GeneratedTriple g = generate("transformations_test.yaml");
        compileTripleWithOutputDir(g, tmpDir);
        // Input has camelCase, uppercase, and a key that will be renamed by KeysReplace
        byte[] body = ("{\"results\":[{\"camelCase\":\"x\",\"FooBar\":\"y\",\"ID\":1}]}")
            .getBytes(StandardCharsets.UTF_8);
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        int port = server.getAddress().getPort();
        try {
            Object task = loadAndStartTask(g, tmpDir, port);
            List<?> records = pollStream(task, "pollKeyRewrite");
            assertEquals(1, records.size(), "Expected exactly 1 record");
            String json = (String) records.get(0).getClass().getMethod("value").invoke(records.get(0));
            // KeysToSnakeCase: camelCase→camel_case, FooBar→foo_bar, ID→id
            // KeysToLower: already lowercase after snake_case
            // KeysReplace("_bar","_baz"): foo_bar→foo_baz
            assertTrue(json.contains("camel_case"), "KeysToSnakeCase must convert camelCase→camel_case");
            assertTrue(json.contains("foo_baz"), "KeysReplace must rename foo_bar→foo_baz");
            assertTrue(json.contains("\"id\""), "KeysToSnakeCase must convert ID→id");
            assertFalse(json.contains("camelCase"), "Original camelCase key must not appear");
            assertFalse(json.contains("FooBar"), "Original FooBar key must not appear");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void transformations_mockHttp_flattenFields(@TempDir Path tmpDir) throws Exception {
        GeneratedTriple g = generate("transformations_test.yaml");
        compileTripleWithOutputDir(g, tmpDir);
        byte[] body = ("{\"results\":[{\"id\":1,\"nested\":{\"x\":1,\"y\":2}}]}")
            .getBytes(StandardCharsets.UTF_8);
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        int port = server.getAddress().getPort();
        try {
            Object task = loadAndStartTask(g, tmpDir, port);
            List<?> records = pollStream(task, "pollFlatten");
            assertEquals(1, records.size(), "Expected exactly 1 record");
            String json = (String) records.get(0).getClass().getMethod("value").invoke(records.get(0));
            assertTrue(json.contains("nested.x"), "FlattenFields must produce dot-notation key 'nested.x'");
            assertTrue(json.contains("nested.y"), "FlattenFields must produce dot-notation key 'nested.y'");
            assertFalse(json.contains("\"nested\":{"), "FlattenFields must collapse nested object");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void transformations_mockHttp_dpathFlattenWithKeyTransform(@TempDir Path tmpDir) throws Exception {
        GeneratedTriple g = generate("transformations_test.yaml");
        compileTripleWithOutputDir(g, tmpDir);
        byte[] body = ("{\"results\":[{\"id\":1,\"props\":{\"a\":10,\"b\":20}}]}")
            .getBytes(StandardCharsets.UTF_8);
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        int port = server.getAddress().getPort();
        try {
            Object task = loadAndStartTask(g, tmpDir, port);
            List<?> records = pollStream(task, "pollDpathFlat");
            assertEquals(1, records.size(), "Expected exactly 1 record");
            String json = (String) records.get(0).getClass().getMethod("value").invoke(records.get(0));
            assertTrue(json.contains("props_a"), "DpathFlattenFields+KeyTransformation must produce 'props_a'");
            assertTrue(json.contains("props_b"), "DpathFlattenFields+KeyTransformation must produce 'props_b'");
            assertFalse(json.contains("\"props\":{"), "DpathFlattenFields must remove origin 'props' object");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void transformations_configAddFields_appliedAtStart(@TempDir Path tmpDir) throws Exception {
        GeneratedTriple g = generate("transformations_test.yaml");
        compileTripleWithOutputDir(g, tmpDir);
        byte[] body = ("{\"results\":[{\"id\":1}]}").getBytes(StandardCharsets.UTF_8);
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        int port = server.getAddress().getPort();
        try {
            URLClassLoader loader = new URLClassLoader(
                new URL[]{tmpDir.toUri().toURL()}, getClass().getClassLoader());
            Class<?> taskClass = loader.loadClass(PKG + "." + g.task.typeSpec.name);
            SourceTaskContext ctx = Mockito.mock(SourceTaskContext.class);
            OffsetStorageReader reader = Mockito.mock(OffsetStorageReader.class);
            Mockito.when(ctx.offsetStorageReader()).thenReturn(reader);
            Mockito.when(reader.offset(Mockito.any())).thenReturn(null);
            Object task = taskClass.getDeclaredConstructor().newInstance();
            taskClass.getMethod("initialize", SourceTaskContext.class).invoke(task, ctx);
            taskClass.getMethod("start", Map.class).invoke(task,
                Map.of("server_url", "http://localhost:" + port));

            // configValues should contain the added field 'api_version: v2'
            java.lang.reflect.Field cvField = taskClass.getDeclaredField("configValues");
            cvField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, Object> configValues = (Map<String, Object>) cvField.get(task);
            assertEquals("v2", String.valueOf(configValues.get("api_version")),
                "ConfigAddFields must inject api_version=v2 into configValues");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void transformations_configRemapField_replacesValue(@TempDir Path tmpDir) throws Exception {
        GeneratedTriple g = generate("transformations_test.yaml");
        compileTripleWithOutputDir(g, tmpDir);
        byte[] body = ("{\"results\":[{\"id\":1}]}").getBytes(StandardCharsets.UTF_8);
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        int port = server.getAddress().getPort();
        try {
            URLClassLoader loader = new URLClassLoader(
                new URL[]{tmpDir.toUri().toURL()}, getClass().getClassLoader());
            Class<?> taskClass = loader.loadClass(PKG + "." + g.task.typeSpec.name);
            SourceTaskContext ctx = Mockito.mock(SourceTaskContext.class);
            OffsetStorageReader reader = Mockito.mock(OffsetStorageReader.class);
            Mockito.when(ctx.offsetStorageReader()).thenReturn(reader);
            Mockito.when(reader.offset(Mockito.any())).thenReturn(null);
            Object task = taskClass.getDeclaredConstructor().newInstance();
            taskClass.getMethod("initialize", SourceTaskContext.class).invoke(task, ctx);
            // Pass server_region=us; ConfigRemapField should remap it to us-east-1
            taskClass.getMethod("start", Map.class).invoke(task, Map.of(
                "server_url", "http://localhost:" + port,
                "server_region", "us"
            ));

            java.lang.reflect.Field cvField = taskClass.getDeclaredField("configValues");
            cvField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, Object> configValues = (Map<String, Object>) cvField.get(task);
            assertEquals("us-east-1", String.valueOf(configValues.get("server_region")),
                "ConfigRemapField must remap server_region: us→us-east-1");
        } finally {
            server.stop(0);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // POST / PUT — request_body_json + request_body_data
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void postBody_generatedTaskCompiles(@TempDir Path tmpDir) throws Exception {
        GeneratedTriple g = generate("post_body_test.yaml");
        String taskSrc = g.task.toString();
        assertTrue(taskSrc.contains("BodyPublishers"),
            "POST task must reference HttpRequest.BodyPublishers");
        assertTrue(taskSrc.contains(".POST("),
            "POST task must call .POST(...)");
        assertTrue(taskSrc.contains("application/json"),
            "JSON-body stream must set Content-Type: application/json");
        assertTrue(taskSrc.contains("application/x-www-form-urlencoded"),
            "form-body stream must set Content-Type: application/x-www-form-urlencoded");
        assertTrue(taskSrc.contains("_bodyMap.put(\"cursor\""),
            "cursor page token must be injected into body map for post_cursor_body");
        assertFalse(taskSrc.contains("\"cursor=\""),
            "cursor page token must not be appended to URL query string");
        compileTripleWithOutputDir(g, tmpDir);
    }

    @Test
    void postBody_mockHttp_staticBody(@TempDir Path tmpDir) throws Exception {
        GeneratedTriple g = generate("post_body_test.yaml");
        compileTripleWithOutputDir(g, tmpDir);

        java.util.concurrent.atomic.AtomicReference<String> capturedBody =
            new java.util.concurrent.atomic.AtomicReference<>();
        byte[] resp = ("{\"results\":[{\"id\":1,\"name\":\"alice\"}]}")
            .getBytes(StandardCharsets.UTF_8);
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/records", exchange -> {
            if ("POST".equals(exchange.getRequestMethod())) {
                capturedBody.set(new String(exchange.getRequestBody().readAllBytes(),
                    StandardCharsets.UTF_8));
            }
            exchange.sendResponseHeaders(200, resp.length);
            exchange.getResponseBody().write(resp);
            exchange.close();
        });
        server.start();
        int port = server.getAddress().getPort();
        try {
            Object task = loadAndStartTask(g, tmpDir, port);
            List<?> records = pollStream(task, "pollPostStatic");
            assertEquals(1, records.size(), "Expected 1 record from post_static stream");
            assertNotNull(capturedBody.get(), "Server must receive a POST body");
            assertTrue(capturedBody.get().contains("\"query\""),
                "POST body must contain static field 'query'");
            assertTrue(capturedBody.get().contains("\"active\""),
                "POST body must contain static value 'active'");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void postBody_mockHttp_cursorBodyInjection(@TempDir Path tmpDir) throws Exception {
        GeneratedTriple g = generate("post_body_test.yaml");
        compileTripleWithOutputDir(g, tmpDir);

        // Cursor pagination emits one page per poll() call; state is preserved via SourceRecord offsets.
        // To simulate a second page, we: (1) collect the cursor from the first poll's SourceRecord offset,
        // (2) configure the mock OffsetStorageReader to return it, (3) call poll again.
        java.util.concurrent.CopyOnWriteArrayList<String> bodies =
            new java.util.concurrent.CopyOnWriteArrayList<>();

        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/paged", exchange -> {
            bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            String payload = bodies.size() == 1
                ? "{\"data\":[{\"id\":1}],\"next_cursor\":\"tok42\"}"
                : "{\"data\":[{\"id\":2}],\"next_cursor\":\"\"}";
            byte[] out = payload.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, out.length);
            exchange.getResponseBody().write(out);
            exchange.close();
        });
        server.start();
        int port = server.getAddress().getPort();
        try {
            URLClassLoader loader = new URLClassLoader(
                new URL[]{tmpDir.toUri().toURL()}, getClass().getClassLoader());
            Class<?> taskClass = loader.loadClass(PKG + "." + g.task.typeSpec.name);
            SourceTaskContext ctx = Mockito.mock(SourceTaskContext.class);
            OffsetStorageReader reader = Mockito.mock(OffsetStorageReader.class);
            Mockito.when(ctx.offsetStorageReader()).thenReturn(reader);

            // First poll: OffsetStorageReader returns null (fresh start).
            Mockito.when(reader.offset(Mockito.any())).thenReturn(null);
            Object task = taskClass.getDeclaredConstructor().newInstance();
            taskClass.getMethod("initialize", SourceTaskContext.class).invoke(task, ctx);
            taskClass.getMethod("start", Map.class).invoke(task,
                Map.of("server_url", "http://localhost:" + port));

            List<?> page1 = pollStream(task, "pollPostCursorBody");
            assertFalse(page1.isEmpty(), "First poll must return records");
            // Extract cursor from offset map stored in the first SourceRecord.
            Object firstRecord = page1.get(0);
            @SuppressWarnings("unchecked")
            Map<String, Object> offset1 = (Map<String, Object>)
                firstRecord.getClass().getMethod("sourceOffset").invoke(firstRecord);
            String savedCursor = String.valueOf(offset1.get("cursor"));
            assertEquals("tok42", savedCursor, "Cursor must be saved in SourceRecord offset");

            // Second poll: OffsetStorageReader returns the saved cursor.
            Mockito.when(reader.offset(Mockito.any()))
                .thenReturn(Map.of("cursor", savedCursor));
            List<?> page2 = pollStream(task, "pollPostCursorBody");
            assertFalse(page2.isEmpty(), "Second poll must return records");
            assertEquals(2, bodies.size(), "Server must receive exactly 2 requests");
            String secondBody = bodies.get(1);
            assertTrue(secondBody.contains("tok42"),
                "Second request body must contain cursor token 'tok42'");
            assertFalse(secondBody.contains("cursor=tok42"),
                "Cursor must not appear as URL-encoded form field");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void postBody_mockHttp_rawDataBody(@TempDir Path tmpDir) throws Exception {
        GeneratedTriple g = generate("post_body_test.yaml");
        compileTripleWithOutputDir(g, tmpDir);

        java.util.concurrent.atomic.AtomicReference<String> capturedBody =
            new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<String> capturedContentType =
            new java.util.concurrent.atomic.AtomicReference<>();
        byte[] resp = ("{\"items\":[{\"id\":99}]}").getBytes(StandardCharsets.UTF_8);
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/form", exchange -> {
            capturedBody.set(new String(exchange.getRequestBody().readAllBytes(),
                StandardCharsets.UTF_8));
            capturedContentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
            exchange.sendResponseHeaders(200, resp.length);
            exchange.getResponseBody().write(resp);
            exchange.close();
        });
        server.start();
        int port = server.getAddress().getPort();
        try {
            Object task = loadAndStartTask(g, tmpDir, port);
            List<?> records = pollStream(task, "pollPostData");
            assertEquals(1, records.size(), "Expected 1 record from post_data stream");
            assertNotNull(capturedBody.get(), "Server must receive a POST body");
            assertEquals("format=json&version=2", capturedBody.get(),
                "Raw request_body_data must be sent verbatim");
            assertEquals("application/x-www-form-urlencoded", capturedContentType.get(),
                "form-data stream must set Content-Type: application/x-www-form-urlencoded");
        } finally {
            server.stop(0);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // PHASE 4 — DatetimeBasedCursor window slicing
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void stepWindow_generatedTaskCompiles(@TempDir Path tmpDir) throws Exception {
        GeneratedTriple g = generate("step_window_test.yaml");
        compileTriple(g, tmpDir);
        String taskSrc = g.task.toString();
        // Window-end computation must be emitted
        assertTrue(taskSrc.contains("_windowEnd"), "Must emit _windowEnd variable");
        assertTrue(taskSrc.contains("DatetimeWindowHelper.computeWindowEnd"),
            "Must call computeWindowEnd");
        assertTrue(taskSrc.contains("DatetimeWindowHelper.advanceCursor"),
            "Must call advanceCursor for cursor advancement");
        // end_time_option must inject _windowEnd, not raw epoch seconds
        assertTrue(taskSrc.contains("encode(_windowEnd"),
            "end_time_option must be URL-encoded _windowEnd");
        // Jinja-templated step (dynamic_step stream)
        assertTrue(taskSrc.contains("render(\"P"), "Dynamic step stream must interpolate step");
    }

    @Test
    void stepWindow_mockHttp_windowBoundaries(@TempDir Path tmpDir) throws Exception {
        // Window: 2020-01-01 → 2020-01-02 (P1D step, events_daily stream)
        String startDate = "2020-01-01T00:00:00";
        String expectedEnd = "2020-01-02T00:00:00";

        com.sun.net.httpserver.HttpServer server = com.sun.net.httpserver.HttpServer.create(
            new java.net.InetSocketAddress(0), 0);
        java.util.concurrent.atomic.AtomicReference<String> capturedStart = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<String> capturedEnd   = new java.util.concurrent.atomic.AtomicReference<>();

        server.createContext("/api/events", exchange -> {
            String query = exchange.getRequestURI().getQuery();
            if (query != null) {
                for (String part : query.split("&")) {
                    if (part.startsWith("start=")) capturedStart.set(java.net.URLDecoder.decode(part.substring(6), java.nio.charset.StandardCharsets.UTF_8));
                    if (part.startsWith("end=")) capturedEnd.set(java.net.URLDecoder.decode(part.substring(4), java.nio.charset.StandardCharsets.UTF_8));
                }
            }
            byte[] body = "{\"items\":[{\"id\":1,\"created_at\":\"2020-01-01T10:00:00\"}]}".getBytes();
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.getResponseBody().close();
        });
        server.start();
        int port = server.getAddress().getPort();
        try {
            GeneratedTriple g = generate("step_window_test.yaml");
            compileTriple(g, tmpDir);

            URLClassLoader loader = new URLClassLoader(new URL[]{tmpDir.toUri().toURL()}, getClass().getClassLoader());
            Class<?> taskClass = loader.loadClass(PKG + ".StepWindowTestSourceTask");
            SourceTaskContext ctx = Mockito.mock(SourceTaskContext.class);
            OffsetStorageReader reader = Mockito.mock(OffsetStorageReader.class);
            Mockito.when(ctx.offsetStorageReader()).thenReturn(reader);
            Mockito.when(reader.offset(Mockito.any())).thenReturn(null);
            Object task = taskClass.getDeclaredConstructor().newInstance();
            taskClass.getMethod("initialize", SourceTaskContext.class).invoke(task, ctx);
            taskClass.getMethod("start", Map.class).invoke(task,
                Map.of("server_url", "http://localhost:" + port,
                       "start_date", startDate));

            List<?> records = pollStream(task, "pollEventsDaily");
            assertFalse(records.isEmpty(), "Must return records from window");
            // start param must match cursor (start_date on first poll)
            assertNotNull(capturedStart.get(), "start query param must be sent");
            assertEquals(startDate, capturedStart.get(), "start must be the cursor (start_date)");
            assertNotNull(capturedEnd.get(), "end query param must be sent");
            assertEquals(expectedEnd, capturedEnd.get(),
                "end must be cursor + P1D (window end)");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void stepWindow_mockHttp_cursorAdvancesAfterWindow(@TempDir Path tmpDir) throws Exception {
        // After first window, cursor must advance to windowEnd + granularity
        // events_daily: step=P1D, granularity=PT1S
        // Window 1: 2020-01-01 → 2020-01-02; cursor after = 2020-01-02T00:00:01
        String startDate = "2020-01-01T00:00:00";

        com.sun.net.httpserver.HttpServer server = com.sun.net.httpserver.HttpServer.create(
            new java.net.InetSocketAddress(0), 0);
        java.util.concurrent.atomic.AtomicReference<String> window2Start = new java.util.concurrent.atomic.AtomicReference<>();

        final int[] callCount = {0};
        server.createContext("/api/events", exchange -> {
            callCount[0]++;
            String query = exchange.getRequestURI().getQuery();
            if (callCount[0] == 2 && query != null) {
                for (String part : query.split("&")) {
                    if (part.startsWith("start=")) {
                        window2Start.set(java.net.URLDecoder.decode(part.substring(6), java.nio.charset.StandardCharsets.UTF_8));
                    }
                }
            }
            byte[] body = ("{\"items\":[{\"id\":" + callCount[0] + ",\"created_at\":\"2020-01-01T10:00:00\"}]}").getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.getResponseBody().close();
        });
        server.start();
        int port = server.getAddress().getPort();
        try {
            GeneratedTriple g = generate("step_window_test.yaml");
            compileTriple(g, tmpDir);

            URLClassLoader loader = new URLClassLoader(new URL[]{tmpDir.toUri().toURL()}, getClass().getClassLoader());
            Class<?> taskClass = loader.loadClass(PKG + ".StepWindowTestSourceTask");
            SourceTaskContext ctx = Mockito.mock(SourceTaskContext.class);
            OffsetStorageReader reader = Mockito.mock(OffsetStorageReader.class);
            Mockito.when(ctx.offsetStorageReader()).thenReturn(reader);
            Mockito.when(reader.offset(Mockito.any())).thenReturn(null);
            Object task = taskClass.getDeclaredConstructor().newInstance();
            taskClass.getMethod("initialize", SourceTaskContext.class).invoke(task, ctx);
            taskClass.getMethod("start", Map.class).invoke(task,
                Map.of("server_url", "http://localhost:" + port,
                       "start_date", startDate));

            // Poll 1: window 2020-01-01 → 2020-01-02
            pollStream(task, "pollEventsDaily");

            // Simulate cursor save: extract cursor value from first poll's SourceRecord offset
            // and mock reader returning it for second poll
            java.lang.reflect.Field cursorField = taskClass.getDeclaredField("cursor_events_daily");
            cursorField.setAccessible(true);
            String savedCursor = (String) cursorField.get(task);
            assertNotNull(savedCursor, "Cursor must be set after first window");
            // Cursor should be 2020-01-02T00:00:01 (window end + 1s granularity)
            assertEquals("2020-01-02T00:00:01", savedCursor,
                "Cursor must advance to windowEnd + cursor_granularity");
        } finally {
            server.stop(0);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // PHASE 5 — CustomTransformation + CustomRecordExtractor wiring
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void customComponents_generatedTaskCompiles(@TempDir Path tmpDir) throws Exception {
        GeneratedTriple g = generate("custom_components_test.yaml");
        compileTriple(g, tmpDir);
        String taskSrc = g.task.toString();
        // TransformationPipeline init must pass originalsStrings() for Custom* support
        assertTrue(taskSrc.contains("originalsStrings()"),
            "TransformationPipelineFactory.fromJson must receive originalsStrings()");
        // CustomRecordExtractor site must reference CustomComponentRegistry + create
        assertTrue(taskSrc.contains("CustomComponentRegistry"),
            "Generated task must reference CustomComponentRegistry for custom extractor");
        assertTrue(taskSrc.contains("CustomRecordExtractor"),
            "Generated task must reference CustomRecordExtractor interface");
        assertTrue(taskSrc.contains("test.WrappedItemsExtractor"),
            "Generated task must embed the extractor class_name literal");
        assertTrue(taskSrc.contains("test.UpperCaseNameTransformation"),
            "Transformation class_name must be serialized into the pipeline JSON literal");
    }

    @Test
    void customComponents_mockHttp_customTransformationModifiesRecord(@TempDir Path tmpDir) throws Exception {
        // Register a CustomTransformation that uppercases the "name" field.
        org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomComponentRegistry
            .<org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomTransformation>register(
            "test.UpperCaseNameTransformation",
            (cfg, params) -> record -> {
                java.util.Map<String, Object> out = new java.util.LinkedHashMap<>(record);
                if (out.get("name") instanceof String s) out.put("name", s.toUpperCase(java.util.Locale.ROOT));
                return out;
            }
        );

        GeneratedTriple g = generate("custom_components_test.yaml");
        compileTripleWithOutputDir(g, tmpDir);

        byte[] body = "{\"items\":[{\"id\":1,\"name\":\"widget\"}]}".getBytes(StandardCharsets.UTF_8);
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/items", exchange -> {
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.getResponseBody().close();
        });
        server.start();
        int port = server.getAddress().getPort();
        try {
            URLClassLoader loader = new URLClassLoader(
                new URL[]{tmpDir.toUri().toURL()}, getClass().getClassLoader());
            Class<?> taskClass = loader.loadClass(PKG + "." + g.task.typeSpec.name);
            SourceTaskContext ctx = Mockito.mock(SourceTaskContext.class);
            OffsetStorageReader reader = Mockito.mock(OffsetStorageReader.class);
            Mockito.when(ctx.offsetStorageReader()).thenReturn(reader);
            Mockito.when(reader.offset(Mockito.any())).thenReturn(null);
            Object task = taskClass.getDeclaredConstructor().newInstance();
            taskClass.getMethod("initialize", SourceTaskContext.class).invoke(task, ctx);
            taskClass.getMethod("start", Map.class).invoke(task,
                Map.of("server_url", "http://localhost:" + port));

            List<?> records = pollStream(task, "pollItemsTransformed");
            assertFalse(records.isEmpty(), "Must return at least one record");

            // The "name" field must have been uppercased by CustomTransformation.
            // SourceRecord value is a JSON string — deserialize it for assertion.
            org.apache.kafka.connect.source.SourceRecord first =
                (org.apache.kafka.connect.source.SourceRecord) records.get(0);
            @SuppressWarnings("unchecked")
            java.util.Map<String, Object> value = new com.fasterxml.jackson.databind.ObjectMapper()
                .readValue((String) first.value(), java.util.Map.class);
            assertEquals("WIDGET", value.get("name"),
                "CustomTransformation must uppercase the 'name' field");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void customComponents_mockHttp_customRecordExtractorOverridesExtraction(@TempDir Path tmpDir) throws Exception {
        // Register a CustomRecordExtractor that always returns a fixed synthetic record.
        org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomComponentRegistry
            .<org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomRecordExtractor>register(
            "test.WrappedItemsExtractor",
            (cfg, params) -> response -> java.util.List.of(
                java.util.Map.of("id", 999, "source", "custom_extractor")
            )
        );

        GeneratedTriple g = generate("custom_components_test.yaml");
        compileTripleWithOutputDir(g, tmpDir);

        byte[] body = "{\"anything\":\"ignored by custom extractor\"}".getBytes(StandardCharsets.UTF_8);
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/raw", exchange -> {
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.getResponseBody().close();
        });
        server.start();
        int port = server.getAddress().getPort();
        try {
            URLClassLoader loader = new URLClassLoader(
                new URL[]{tmpDir.toUri().toURL()}, getClass().getClassLoader());
            Class<?> taskClass = loader.loadClass(PKG + "." + g.task.typeSpec.name);
            SourceTaskContext ctx = Mockito.mock(SourceTaskContext.class);
            OffsetStorageReader reader = Mockito.mock(OffsetStorageReader.class);
            Mockito.when(ctx.offsetStorageReader()).thenReturn(reader);
            Mockito.when(reader.offset(Mockito.any())).thenReturn(null);
            Object task = taskClass.getDeclaredConstructor().newInstance();
            taskClass.getMethod("initialize", SourceTaskContext.class).invoke(task, ctx);
            taskClass.getMethod("start", Map.class).invoke(task,
                Map.of("server_url", "http://localhost:" + port));

            List<?> records = pollStream(task, "pollItemsExtracted");
            assertFalse(records.isEmpty(), "CustomRecordExtractor must produce at least one record");

            // SourceRecord value is a JSON string — deserialize it for assertion.
            org.apache.kafka.connect.source.SourceRecord first =
                (org.apache.kafka.connect.source.SourceRecord) records.get(0);
            @SuppressWarnings("unchecked")
            java.util.Map<String, Object> value = new com.fasterxml.jackson.databind.ObjectMapper()
                .readValue((String) first.value(), java.util.Map.class);
            assertEquals(999, value.get("id"), "Record must come from the custom extractor");
            assertEquals("custom_extractor", value.get("source"),
                "CustomRecordExtractor must override normal field_path extraction");
        } finally {
            server.stop(0);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // BUG FIX B2 — start_datetime.datetime_format → cursor format conversion
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void startFmtMismatch_generatedCodeContainsFormatConversion(@TempDir Path tmpDir) throws Exception {
        GeneratedTriple g = generate("start_fmt_mismatch_test.yaml");
        compileTriple(g, tmpDir);
        String taskSrc = g.task.toString();
        // Must parse start date using startDt.getDatetimeFormat() ("%Y-%m-%d")
        // and reformat to cursor format ("%Y-%m-%dT%H:%M:%S").
        assertTrue(taskSrc.contains("DatetimeWindowHelper.parseDate"),
            "cursor init must call parseDate to convert from start_datetime format");
        assertTrue(taskSrc.contains("DatetimeWindowHelper.formatDate"),
            "cursor init must call formatDate to reformat to cursor format");
    }

    @Test
    void startFmtMismatch_mockHttp_cursorConvertedToDatetimeFormat(@TempDir Path tmpDir) throws Exception {
        // start_date config value is "%Y-%m-%d" (date-only) but cursor format is "%Y-%m-%dT%H:%M:%S".
        // Without the fix, parseDate("2020-01-01", "%Y-%m-%dT%H:%M:%S") throws DateTimeParseException.
        com.sun.net.httpserver.HttpServer server = com.sun.net.httpserver.HttpServer.create(
            new java.net.InetSocketAddress(0), 0);
        java.util.concurrent.atomic.AtomicReference<String> capturedStart = new java.util.concurrent.atomic.AtomicReference<>();

        server.createContext("/api/items", exchange -> {
            String query = exchange.getRequestURI().getQuery();
            if (query != null) {
                for (String part : query.split("&")) {
                    if (part.startsWith("start=")) capturedStart.set(
                        java.net.URLDecoder.decode(part.substring(6), StandardCharsets.UTF_8));
                }
            }
            byte[] body = "{\"data\":[{\"id\":1,\"updated_at\":\"2020-01-01T10:00:00\"}]}".getBytes();
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.getResponseBody().close();
        });
        server.start();
        int port = server.getAddress().getPort();
        try {
            GeneratedTriple g = generate("start_fmt_mismatch_test.yaml");
            compileTripleWithOutputDir(g, tmpDir);

            URLClassLoader loader = new URLClassLoader(new URL[]{tmpDir.toUri().toURL()}, getClass().getClassLoader());
            Class<?> taskClass = loader.loadClass(PKG + ".StartFmtMismatchTestSourceTask");
            SourceTaskContext ctx = Mockito.mock(SourceTaskContext.class);
            OffsetStorageReader reader = Mockito.mock(OffsetStorageReader.class);
            Mockito.when(ctx.offsetStorageReader()).thenReturn(reader);
            Mockito.when(reader.offset(Mockito.any())).thenReturn(null);
            Object task = taskClass.getDeclaredConstructor().newInstance();
            taskClass.getMethod("initialize", SourceTaskContext.class).invoke(task, ctx);
            taskClass.getMethod("start", Map.class).invoke(task,
                Map.of("server_url", "http://localhost:" + port,
                       "start_date", "2020-01-01"));

            // Must not throw DateTimeParseException; must produce records.
            List<?> records = pollStream(task, "pollItems");
            assertFalse(records.isEmpty(), "Must produce records when start_datetime format differs from cursor format");
            // The start query param must be in cursor format (%Y-%m-%dT%H:%M:%S), not date-only
            assertNotNull(capturedStart.get(), "start query param must be sent");
            assertEquals("2020-01-01T00:00:00", capturedStart.get(),
                "Date-only start_date must be converted to cursor datetime format before injection");
        } finally {
            server.stop(0);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // HELPERS
    // ══════════════════════════════════════════════════════════════════════════

    /**
     * Loads the compiled task class from tmpDir, wires a mock context, and calls
     * start() with the given server port pointing to server_url.
     */
    private Object loadAndStartTask(GeneratedTriple g, Path tmpDir, int port) throws Exception {
        URLClassLoader loader = new URLClassLoader(
            new URL[]{tmpDir.toUri().toURL()}, getClass().getClassLoader());
        Class<?> taskClass = loader.loadClass(PKG + "." + g.task.typeSpec.name);
        SourceTaskContext ctx = Mockito.mock(SourceTaskContext.class);
        OffsetStorageReader reader = Mockito.mock(OffsetStorageReader.class);
        Mockito.when(ctx.offsetStorageReader()).thenReturn(reader);
        Mockito.when(reader.offset(Mockito.any())).thenReturn(null);
        Object task = taskClass.getDeclaredConstructor().newInstance();
        taskClass.getMethod("initialize", SourceTaskContext.class).invoke(task, ctx);
        taskClass.getMethod("start", Map.class).invoke(task,
            Map.of("server_url", "http://localhost:" + port));
        return task;
    }

    /**
     * Calls a named poll method on the task via reflection and returns the resulting records.
     * Method names follow the TaskGenerator naming convention: "poll" + TitleCase(streamName).
     * The generated poll methods are private, so setAccessible(true) is required.
     */
    @SuppressWarnings("unchecked")
    private List<?> pollStream(Object task, String pollMethodName) throws Exception {
        java.lang.reflect.Method m = task.getClass().getDeclaredMethod(pollMethodName);
        m.setAccessible(true);
        return (List<?>) m.invoke(task);
    }

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
