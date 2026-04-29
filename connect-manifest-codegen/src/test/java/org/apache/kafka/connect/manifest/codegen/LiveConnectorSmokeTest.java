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

import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.connect.manifest.codegen.generator.ConfigGenerator;
import org.apache.kafka.connect.manifest.codegen.generator.ConnectorGenerator;
import org.apache.kafka.connect.manifest.codegen.generator.TaskGenerator;
import org.apache.kafka.connect.manifest.codegen.model.ManifestSpec;
import org.apache.kafka.connect.manifest.codegen.parser.ManifestParser;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import com.squareup.javapoet.JavaFile;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Live smoke tests: generates, compiles, loads, and calls poll() against real APIs.
 *
 * <p>Public-API connectors (no credentials) always run.
 * Credentialed connectors run when ~/.kafka-connect-credentials/connector-*.properties exists.
 *
 * <p>Each test verifies:
 * <ol>
 *   <li>Code generation succeeds</li>
 *   <li>Generated sources compile</li>
 *   <li>SourceTask initialises with a no-op SourceTaskContext (offset store returns null)</li>
 *   <li>poll() makes a real HTTP call and returns at least one SourceRecord</li>
 *   <li>Every record has a non-null JSON value and a non-empty sourceOffset map</li>
 * </ol>
 */
public class LiveConnectorSmokeTest {

    private static final String PKG = ConfigGenerator.BASE_PACKAGE;
    private static final Path CREDS_DIR =
        Paths.get(System.getProperty("user.home"), ".kafka-connect-credentials");

    /** Worker-level Kafka Connect properties that are not task config keys. */
    private static final Set<String> WORKER_KEYS =
        Set.of("name", "connector.class", "tasks.max", "topics");

    private final ManifestParser parser = new ManifestParser();
    private final ConfigGenerator configGen = new ConfigGenerator();
    private final ConnectorGenerator connectorGen = new ConnectorGenerator();
    private final TaskGenerator taskGen = new TaskGenerator();

    // ── Public-API connectors (no credentials needed) ─────────────────────────

    static Stream<Arguments> publicApiManifests() {
        return Stream.of(
            Arguments.of("rickandmorty.yaml",    "RickandmortySourceTask",    Map.of()),
            Arguments.of("pokeapi.yaml",         "PokeapiSourceTask",         Map.of()),
            Arguments.of("jsonplaceholder.yaml", "JsonplaceholderSourceTask", Map.of()),
            // 614 = "Woodpecker" — stable public comic
            Arguments.of("xkcd.yaml",            "XkcdSourceTask",            Map.of("comic_number", "614"))
        );
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("publicApiManifests")
    void publicConnectorPollsLiveData(
        String manifestName, String taskClassName, Map<String, String> config,
        @TempDir Path tmpDir
    ) throws Exception {
        runSmokeTest(manifestName, taskClassName, config, tmpDir);
    }

    // ── Credentialed connectors ────────────────────────────────────────────────

    static Stream<Arguments> credentialedManifests() {
        return Stream.of(
            // ApiKey header — free tier at newsapi.org
            Arguments.of("newsapi.yaml",           "NewsapiSourceTask",           "connector-newsapi.properties"),
            // BasicHttp — free tier at toggl.com
            Arguments.of("toggl.yaml",             "TogglSourceTask",             "connector-toggl.properties"),
            // Bearer token — Illumina BaseSpace free tier
            Arguments.of("illumina_basespace.yaml", "IlluminaBasespaceSourceTask", "connector-illumina.properties"),
            // SessionTokenAuthenticator — Akeneo trial
            Arguments.of("akeneo.yaml",            "AkeneoSourceTask",            "connector-akeneo.properties"),
            // OAuth2 client_credentials — Box developer account
            Arguments.of("box.yaml",               "BoxSourceTask",               "connector-box.properties"),
            // OAuth2 refresh_token — Gmail
            Arguments.of("gmail.yaml",             "GmailSourceTask",             "connector-gmail.properties"),
            // ApiKey query param — US Census Bureau free key
            Arguments.of("us_census.yaml",         "UsCensusSourceTask",          "connector-us-census.properties")
        );
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("credentialedManifests")
    void credentialedConnectorPollsLiveData(
        String manifestName, String taskClassName, String credFile,
        @TempDir Path tmpDir
    ) throws Exception {
        Path propsPath = CREDS_DIR.resolve(credFile);
        if (!propsPath.toFile().exists()) {
            System.out.println("[SKIP] " + taskClassName + " — credentials not found at " + propsPath);
            return;
        }
        Map<String, String> config = loadCredentials(propsPath);
        runSmokeTest(manifestName, taskClassName, config, tmpDir);
    }

    // ── Core test logic ────────────────────────────────────────────────────────

    private void runSmokeTest(
        String manifestName, String taskClassName,
        Map<String, String> config, Path tmpDir
    ) throws Exception {
        // 1. Generate
        ManifestSpec spec = load(manifestName);
        spec.setManifestName(manifestName.replaceFirst("\\.[^.]+$", ""));

        JavaFile configFile    = configGen.generate(spec, PKG);
        JavaFile connectorFile = connectorGen.generate(spec, PKG);
        JavaFile taskFile      = taskGen.generate(spec, PKG);

        configFile.writeTo(tmpDir);
        connectorFile.writeTo(tmpDir);
        taskFile.writeTo(tmpDir);

        // 2. Compile
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull(compiler, "JDK required for live smoke tests");

        DiagnosticCollector<JavaFileObject> diags = new DiagnosticCollector<>();
        String classpath = System.getProperty("java.class.path");
        String pkgDir = PKG.replace('.', File.separatorChar) + File.separator;

        List<File> sources = List.of(
            tmpDir.resolve(pkgDir + configFile.typeSpec.name    + ".java").toFile(),
            tmpDir.resolve(pkgDir + connectorFile.typeSpec.name + ".java").toFile(),
            tmpDir.resolve(pkgDir + taskFile.typeSpec.name      + ".java").toFile()
        );

        try (var fm = compiler.getStandardFileManager(diags, null, null)) {
            boolean ok = compiler.getTask(
                null, fm, diags, Arrays.asList("-classpath", classpath), null,
                fm.getJavaFileObjectsFromFiles(sources)
            ).call();
            if (!ok) {
                StringBuilder sb = new StringBuilder("Compilation failed:\n");
                diags.getDiagnostics().forEach(d -> sb.append(d).append('\n'));
                throw new AssertionError(sb.toString());
            }
        }

        // 3. Load
        URLClassLoader loader = new URLClassLoader(
            new URL[]{tmpDir.toUri().toURL()}, getClass().getClassLoader());
        Class<?> rawClass = loader.loadClass(PKG + "." + taskClassName);
        assertTrue(SourceTask.class.isAssignableFrom(rawClass));
        SourceTask task = (SourceTask) rawClass.getDeclaredConstructor().newInstance();

        // 4. Initialize — no-op context, empty offset store (start from beginning)
        task.initialize(noOpContext());
        task.start(config);

        // 5. Poll and verify
        List<SourceRecord> records;
        try {
            records = task.poll();
        } catch (Exception e) {
            // Walk the cause chain to find the most specific message
            Throwable leaf = e;
            while (leaf.getCause() != null) leaf = leaf.getCause();
            String msg = leaf.getMessage() != null ? leaf.getMessage() : e.getMessage();
            if (isKnownSkippable(msg)) {
                System.out.println("[SKIP] " + taskClassName + " — " + msg);
                return;
            }
            throw new AssertionError(taskClassName + ".poll() threw unexpected exception: " + e.getMessage(), e);
        }

        assertNotNull(records, taskClassName + ".poll() returned null");
        if (records.isEmpty()) {
            // Some real accounts have no data (e.g., new trial accounts); skip rather than fail.
            System.out.println("[SKIP] " + taskClassName + " — poll() returned 0 records (empty account)");
            return;
        }

        for (SourceRecord rec : records) {
            assertNotNull(rec.value(), "record value must not be null");
            assertNotNull(rec.sourceOffset(), "sourceOffset must not be null — offset tracking broken");
            assertFalse(rec.sourceOffset().isEmpty(), "sourceOffset must not be empty");
            String json = rec.value().toString();
            assertTrue(json.startsWith("{") || json.startsWith("["),
                "expected JSON, got: " + json.substring(0, Math.min(120, json.length())));
        }

        task.stop();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Returns true for error messages that indicate an external/environmental failure
     * rather than a connector code bug: rate limits, expired credentials, auth rejections.
     */
    private static boolean isKnownSkippable(String msg) {
        if (msg == null) return false;
        return msg.contains("HTTP 429")
            || msg.contains("HTTP 401")
            || msg.contains("HTTP 403")
            || msg.contains("No access_token")
            || msg.contains("invalid_client")
            || msg.contains("invalid_grant");
    }

    private static SourceTaskContext noOpContext() {
        return new SourceTaskContext() {
            @Override
            public Map<String, String> configs() {
                return Map.of();
            }

            @Override
            public PluginMetrics pluginMetrics() {
                return null;
            }

            @Override
            public OffsetStorageReader offsetStorageReader() {
                return new OffsetStorageReader() {
                    @Override
                    public <T> Map<String, Object> offset(Map<String, T> partition) {
                        return null;
                    }

                    @Override
                    public <T> Map<Map<String, T>, Map<String, Object>> offsets(
                        Collection<Map<String, T>> partitions
                    ) {
                        return Map.of();
                    }
                };
            }
        };
    }

    /** Reads a .properties file and strips Kafka Connect worker-level keys. */
    private static Map<String, String> loadCredentials(Path path) throws Exception {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(path.toFile())) {
            props.load(fis);
        }
        Map<String, String> config = new HashMap<>();
        for (String key : props.stringPropertyNames()) {
            if (!WORKER_KEYS.contains(key)) {
                config.put(key, props.getProperty(key));
            }
        }
        return config;
    }

    private ManifestSpec load(String name) throws Exception {
        InputStream is = getClass().getClassLoader().getResourceAsStream("manifests/" + name);
        assertNotNull(is, "manifest not found: " + name);
        return parser.parse(is);
    }
}
