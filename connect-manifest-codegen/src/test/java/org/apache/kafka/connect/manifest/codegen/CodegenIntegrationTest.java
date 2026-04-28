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

import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Full-pipeline integration tests for the code generator.
 *
 * <p>Each test runs all three generators together — Config, Connector, Task —
 * against a real Airbyte manifest and verifies:
 * <ul>
 *   <li>All three files are produced</li>
 *   <li>The files correctly reference each other (connector → task, connector → config, task → config)</li>
 *   <li>All three files compile together as a unit (they cannot be compiled in isolation)</li>
 *   <li>Bad manifests fail at the right layer with the right exception</li>
 * </ul>
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
    // HAPPY PATHS
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
    void defillama_taskReferencesCorrectConfig() throws Exception {
        GeneratedTriple g = generate("defillama.yaml");
        String taskSrc = g.task.toString();
        assertTrue(taskSrc.contains("DefillamaConnectorConfig"),
            "Task must instantiate DefillamaConnectorConfig in start()");
    }

    @Test
    void defillama_taskEmbedsFieldPath() throws Exception {
        GeneratedTriple g = generate("defillama.yaml");
        String taskSrc = g.task.toString();
        assertTrue(taskSrc.contains("chainTvls"),    "Task must navigate field_path[0] = 'chainTvls'");
        assertTrue(taskSrc.contains("Plume Mainnet"), "Task must navigate field_path[1] = 'Plume Mainnet'");
    }

    @Test
    void defillama_configHasNoPropertyConstants() throws Exception {
        GeneratedTriple g = generate("defillama.yaml");
        boolean hasConfigConstant = g.config.typeSpec.fieldSpecs.stream()
            .anyMatch(f -> f.name.endsWith("_CONFIG"));
        assertTrue(!hasConfigConstant,
            "defillama has no spec properties, so no _CONFIG constants expected");
    }

    @Test
    void defillama_allThreeFilesCompileTogether(@TempDir Path tmpDir) throws Exception {
        compileTriple(generate("defillama.yaml"), tmpDir);
    }

    // ── xkcd: optional config field, empty field_path, $ref stream ───────────

    @Test
    void xkcd_allThreeFilesGenerated() throws Exception {
        GeneratedTriple g = generate("xkcd.yaml");
        assertEquals("XkcdConnectorConfig",  g.config.typeSpec.name);
        assertEquals("XkcdSourceConnector",  g.connector.typeSpec.name);
        assertEquals("XkcdSourceTask",       g.task.typeSpec.name);
    }

    @Test
    void xkcd_connectorReferencesCorrectTaskAndConfig() throws Exception {
        GeneratedTriple g = generate("xkcd.yaml");
        String connectorSrc = g.connector.toString();
        assertTrue(connectorSrc.contains("XkcdSourceTask.class"),
            "Connector must reference XkcdSourceTask.class");
        assertTrue(connectorSrc.contains("XkcdConnectorConfig.config()"),
            "Connector must delegate config() to XkcdConnectorConfig");
    }

    @Test
    void xkcd_configHasComicNumberConstant() throws Exception {
        GeneratedTriple g = generate("xkcd.yaml");
        boolean found = g.config.typeSpec.fieldSpecs.stream()
            .anyMatch(f -> f.name.equals("COMIC_NUMBER_CONFIG"));
        assertTrue(found, "Config must declare COMIC_NUMBER_CONFIG constant");
    }

    @Test
    void xkcd_configComicNumberIsOptional_mediumImportance() throws Exception {
        GeneratedTriple g = generate("xkcd.yaml");
        // comic_number is not in the required list → MEDIUM importance
        String configSrc = g.config.toString();
        assertTrue(configSrc.contains("Importance.MEDIUM"),
            "Optional field comic_number must have MEDIUM importance");
    }

    @Test
    void xkcd_taskEmbedsBaseUrl() throws Exception {
        GeneratedTriple g = generate("xkcd.yaml");
        assertTrue(g.task.toString().contains("https://xkcd.com"),
            "Task must embed xkcd base URL");
    }

    @Test
    void xkcd_taskHasNoFieldPathNavigation() throws Exception {
        GeneratedTriple g = generate("xkcd.yaml");
        // xkcd field_path is empty — task must not call .get("...") for path traversal
        String taskSrc = g.task.toString();
        // The only .get() calls should be from Map.of("stream",...) and Map.of("position",...)
        // not field_path navigation
        assertTrue(!taskSrc.contains("instanceof Map"),
            "xkcd has empty field_path, no instanceof Map navigation expected");
    }

    @Test
    void xkcd_allThreeFilesCompileTogether(@TempDir Path tmpDir) throws Exception {
        compileTriple(generate("xkcd.yaml"), tmpDir);
    }

    // ── zapier: required field, request_parameters from config ────────────────

    @Test
    void zapier_allThreeFilesGenerated() throws Exception {
        GeneratedTriple g = generate("zapier.yaml");
        assertEquals("ZapierSupportedStorageConnectorConfig",  g.config.typeSpec.name);
        assertEquals("ZapierSupportedStorageSourceConnector",  g.connector.typeSpec.name);
        assertEquals("ZapierSupportedStorageSourceTask",       g.task.typeSpec.name);
    }

    @Test
    void zapier_connectorReferencesCorrectTaskAndConfig() throws Exception {
        GeneratedTriple g = generate("zapier.yaml");
        String connectorSrc = g.connector.toString();
        assertTrue(connectorSrc.contains("ZapierSupportedStorageSourceTask.class"),
            "Connector must reference ZapierSupportedStorageSourceTask.class");
        assertTrue(connectorSrc.contains("ZapierSupportedStorageConnectorConfig.config()"),
            "Connector must delegate config() to ZapierSupportedStorageConnectorConfig");
    }

    @Test
    void zapier_configSecretIsRequired_highImportance() throws Exception {
        GeneratedTriple g = generate("zapier.yaml");
        String configSrc = g.config.toString();
        assertTrue(configSrc.contains("SECRET_CONFIG"),     "Config must declare SECRET_CONFIG");
        assertTrue(configSrc.contains("Importance.HIGH"),   "Required field secret must be HIGH importance");
    }

    @Test
    void zapier_taskInjectsSecretFromConfig() throws Exception {
        GeneratedTriple g = generate("zapier.yaml");
        String taskSrc = g.task.toString();
        // The request_parameters secret="{{ config['secret'] }}" must become getSecret() call
        assertTrue(taskSrc.contains("getSecret()"),
            "Task must call config.getSecret() for request_parameters");
        assertTrue(taskSrc.contains("secret="),
            "Task must include 'secret=' query parameter in URL");
    }

    @Test
    void zapier_taskReferencesCorrectConfig() throws Exception {
        GeneratedTriple g = generate("zapier.yaml");
        assertTrue(g.task.toString().contains("ZapierSupportedStorageConnectorConfig"),
            "Task must instantiate ZapierSupportedStorageConnectorConfig in start()");
    }

    @Test
    void zapier_allThreeFilesCompileTogether(@TempDir Path tmpDir) throws Exception {
        compileTriple(generate("zapier.yaml"), tmpDir);
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
        // 'properties' must be a map — Jackson rejects a scalar at parse time
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
        // Construct a ManifestSpec that passes the parser but has a stream with no retriever
        // (resolved via definitions so resolvedStreams() includes it)
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
        // A manifest whose spec has no properties → config generates class with no _CONFIG fields
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

    /** Runs all three generators against one manifest and returns the triple. */
    private GeneratedTriple generate(String manifestName) throws Exception {
        ManifestSpec spec = load(manifestName);
        JavaFile config    = configGen.generate(spec, PKG);
        JavaFile connector = connectorGen.generate(spec, PKG);
        JavaFile task      = taskGen.generate(spec, PKG);
        return new GeneratedTriple(config, connector, task);
    }

    /**
     * Writes all three files to disk and compiles them together with javac.
     * This is the strongest integration check: the files reference each other,
     * so a class-name mismatch or missing import causes a compile failure here.
     */
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
        // retriever intentionally null

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
        // requester intentionally null

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

    /** Holds the three generated files for one manifest. */
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
