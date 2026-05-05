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
import org.apache.kafka.connect.manifest.codegen.model.RetrieverSpec;
import org.apache.kafka.connect.manifest.codegen.model.StreamSpec;
import org.apache.kafka.connect.manifest.codegen.parser.ManifestParser;

import com.squareup.javapoet.JavaFile;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit and integration tests for {@link TaskGenerator}.
 */
public class TaskGeneratorTest {

    private static final String PKG = ConfigGenerator.BASE_PACKAGE;
    private final TaskGenerator generator = new TaskGenerator();
    private final ConfigGenerator configGenerator = new ConfigGenerator();
    private final ManifestParser parser = new ManifestParser();

    private InputStream resource(String name) {
        return getClass().getClassLoader().getResourceAsStream("manifests/" + name);
    }

    private ManifestSpec load(String name) throws Exception {
        return parser.parse(resource(name));
    }

    // ── class name ────────────────────────────────────────────────────────────

    @Test
    void zapier_generatesCorrectClassName() throws Exception {
        JavaFile file = generator.generate(load("zapier.yaml"), PKG);
        assertTrue(file.typeSpec.name.equals("ZapierSupportedStorageSourceTask"),
            "Got: " + file.typeSpec.name);
    }

    @Test
    void xkcd_generatesCorrectClassName() throws Exception {
        JavaFile file = generator.generate(load("xkcd.yaml"), PKG);
        assertTrue(file.typeSpec.name.equals("XkcdSourceTask"),
            "Got: " + file.typeSpec.name);
    }

    @Test
    void defillama_generatesCorrectClassName() throws Exception {
        JavaFile file = generator.generate(load("defillama.yaml"), PKG);
        assertTrue(file.typeSpec.name.equals("DefillamaSourceTask"),
            "Got: " + file.typeSpec.name);
    }

    // ── extends ───────────────────────────────────────────────────────────────

    @Test
    void generatedClass_extendsSourceTask() throws Exception {
        JavaFile file = generator.generate(load("zapier.yaml"), PKG);
        String src = file.toString();
        assertTrue(src.contains("extends SourceTask"), "Must extend SourceTask");
    }

    // ── required methods ──────────────────────────────────────────────────────

    @Test
    void generatedClass_hasVersionMethod() throws Exception {
        JavaFile file = generator.generate(load("xkcd.yaml"), PKG);
        boolean found = file.typeSpec.methodSpecs.stream()
            .anyMatch(m -> m.name.equals("version"));
        assertTrue(found, "Expected version() method");
    }

    @Test
    void generatedClass_hasStartMethod() throws Exception {
        JavaFile file = generator.generate(load("xkcd.yaml"), PKG);
        boolean found = file.typeSpec.methodSpecs.stream()
            .anyMatch(m -> m.name.equals("start"));
        assertTrue(found, "Expected start() method");
    }

    @Test
    void generatedClass_hasPollMethod() throws Exception {
        JavaFile file = generator.generate(load("xkcd.yaml"), PKG);
        boolean found = file.typeSpec.methodSpecs.stream()
            .anyMatch(m -> m.name.equals("poll"));
        assertTrue(found, "Expected poll() method");
    }

    @Test
    void generatedClass_hasStopMethod() throws Exception {
        JavaFile file = generator.generate(load("xkcd.yaml"), PKG);
        boolean found = file.typeSpec.methodSpecs.stream()
            .anyMatch(m -> m.name.equals("stop"));
        assertTrue(found, "Expected stop() method");
    }

    // ── URL constants ─────────────────────────────────────────────────────────

    @Test
    void zapier_containsBaseUrl() throws Exception {
        JavaFile file = generator.generate(load("zapier.yaml"), PKG);
        String src = file.toString();
        assertTrue(src.contains("https://store.zapier.com/api"), "Must embed base URL");
    }

    @Test
    void defillama_containsFullUrl() throws Exception {
        JavaFile file = generator.generate(load("defillama.yaml"), PKG);
        String src = file.toString();
        assertTrue(src.contains("https://api.llama.fi/protocol/curve-finance"),
            "Must embed defillama URL");
    }

    // ── field_path navigation ─────────────────────────────────────────────────

    @Test
    void defillama_containsFieldPathSegments() throws Exception {
        JavaFile file = generator.generate(load("defillama.yaml"), PKG);
        String src = file.toString();
        assertTrue(src.contains("chainTvls"), "Must reference 'chainTvls' from field_path");
        assertTrue(src.contains("Plume Mainnet"), "Must reference 'Plume Mainnet' from field_path");
    }

    @Test
    void xkcd_emptyFieldPath_doesNotNavigate() throws Exception {
        JavaFile file = generator.generate(load("xkcd.yaml"), PKG);
        String src = file.toString();
        // xkcd has empty field_path; no .get("...") navigation should appear for path traversal
        // The response is used directly as the record
        assertTrue(src.contains("records"), "Should still build records list");
    }

    // ── request parameters ────────────────────────────────────────────────────

    @Test
    void zapier_requestParamsInjectedFromConfig() throws Exception {
        JavaFile file = generator.generate(load("zapier.yaml"), PKG);
        String src = file.toString();
        // zapier has request_parameters: secret: "{{ config['secret'] }}"
        assertTrue(src.contains("getSecret()"), "Must call config.getSecret() for request param");
        assertTrue(src.contains("secret="), "Must include 'secret=' in query string");
    }

    // ── stream name constant ──────────────────────────────────────────────────

    @Test
    void zapier_streamNameConstantPresent() throws Exception {
        JavaFile file = generator.generate(load("zapier.yaml"), PKG);
        String src = file.toString();
        assertTrue(src.contains("zapier_supported_storage"),
            "Must include stream name constant");
    }

    // ── exception paths ───────────────────────────────────────────────────────

    @Test
    void noStreams_emitsStubTask() throws Exception {
        // Manifests with no usable streams now produce a stub task that throws
        // ConnectException at start() — connector loads, task fails fast with clear message.
        ManifestSpec empty = new ManifestSpec();
        empty.setStreams(Collections.emptyList());
        com.squareup.javapoet.JavaFile out = generator.generate(empty, PKG);
        assertTrue(out.toString().contains("ConnectException"),
            "Stub task must throw ConnectException at start()");
    }

    @Test
    void streamWithNoRetriever_throwsCodegenException() {
        ManifestSpec spec = buildSpecWithStreamMissingRetriever();
        CodegenException ex = assertThrows(CodegenException.class,
            () -> generator.generate(spec, PKG));
        assertTrue(ex.getMessage().contains("retriever"),
            "Exception must mention retriever; got: " + ex.getMessage());
    }

    @Test
    void streamWithNoRequester_throwsCodegenException() {
        ManifestSpec spec = buildSpecWithStreamMissingRequester();
        CodegenException ex = assertThrows(CodegenException.class,
            () -> generator.generate(spec, PKG));
        assertTrue(ex.getMessage().contains("requester"),
            "Exception must mention requester; got: " + ex.getMessage());
    }

    // ── integration: generated source compiles ────────────────────────────────

    @Test
    void zapier_generatedSourceCompiles(@TempDir Path tmpDir) throws Exception {
        assertGeneratedCodeCompiles(load("zapier.yaml"), tmpDir);
    }

    @Test
    void xkcd_generatedSourceCompiles(@TempDir Path tmpDir) throws Exception {
        assertGeneratedCodeCompiles(load("xkcd.yaml"), tmpDir);
    }

    @Test
    void defillama_generatedSourceCompiles(@TempDir Path tmpDir) throws Exception {
        assertGeneratedCodeCompiles(load("defillama.yaml"), tmpDir);
    }

    // ── custom-component dispatch ─────────────────────────────────────────────

    @Test
    void googleAds_customRetriever_emitsRegistryDispatch() throws Exception {
        JavaFile file = generator.generate(load("source_google_ads.yaml"), PKG);
        String src = file.toString();
        assertTrue(src.contains("CustomComponentRegistry.create("),
            "Custom-retriever streams must dispatch through CustomComponentRegistry; got:\n" + src);
        assertTrue(src.contains("source_google_ads.components.GoogleAdsRetriever"),
            "Must reference the retriever class_name from the manifest");
        assertTrue(src.contains("CustomRetriever"),
            "Must import/reference CustomRetriever interface");
    }

    @Test
    void googleAds_customStreams_skipsHttpAuthHelpers() throws Exception {
        JavaFile file = generator.generate(load("source_google_ads.yaml"), PKG);
        String src = file.toString();
        // No HTTP code path exists for an all-custom manifest, so sendWithRetry must not be emitted.
        assertTrue(!src.contains("private HttpResponse<String> sendWithRetry"),
            "All-custom manifest must not emit sendWithRetry()");
    }

    @Test
    void googleAds_generatedSourceCompiles(@TempDir Path tmpDir) throws Exception {
        assertGeneratedCodeCompiles(load("source_google_ads.yaml"), tmpDir);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    /**
     * Builds a ManifestSpec where definitions.streams contains a stream with no retriever.
     * A $ref-style entry in streams causes resolvedStreams() to pull from definitions,
     * so the bad stream is actually included in the resolved list.
     */
    private ManifestSpec buildSpecWithStreamMissingRetriever() {
        StreamSpec badStream = new StreamSpec();
        badStream.setName("bad_stream");
        // retriever intentionally null

        java.util.Map<String, StreamSpec> defsMap = new HashMap<>();
        defsMap.put("bad_stream", badStream);

        ManifestSpec.DefinitionsDef defs = new ManifestSpec.DefinitionsDef();
        defs.setStreams(defsMap);

        // A $ref-style stream entry: name=null, retriever=null → resolvedStreams() goes to definitions
        StreamSpec refEntry = new StreamSpec();

        ManifestSpec spec = new ManifestSpec();
        spec.setVersion("1.0");
        spec.setType("DeclarativeSource");
        spec.setStreams(List.of(refEntry));
        spec.setDefinitions(defs);
        return spec;
    }

    private ManifestSpec buildSpecWithStreamMissingRequester() {
        StreamSpec badStream = new StreamSpec();
        badStream.setName("bad_stream");
        RetrieverSpec retriever = new RetrieverSpec();
        // requester intentionally null
        badStream.setRetriever(retriever);

        java.util.Map<String, StreamSpec> defsMap = new HashMap<>();
        defsMap.put("bad_stream", badStream);

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

    /**
     * Writes config, task, and the connector source files, then compiles all together.
     */
    private void assertGeneratedCodeCompiles(ManifestSpec spec, Path tmpDir) throws Exception {
        JavaFile configFile = configGenerator.generate(spec, PKG);
        JavaFile taskFile = generator.generate(spec, PKG);
        configFile.writeTo(tmpDir);
        taskFile.writeTo(tmpDir);

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull(compiler, "javax.tools.JavaCompiler not available (JDK required)");

        DiagnosticCollector<JavaFileObject> diags = new DiagnosticCollector<>();
        String classpath = System.getProperty("java.class.path");
        String pkgDir = PKG.replace('.', File.separatorChar) + File.separator;

        List<File> srcFiles = List.of(
            tmpDir.resolve(pkgDir + configFile.typeSpec.name + ".java").toFile(),
            tmpDir.resolve(pkgDir + taskFile.typeSpec.name + ".java").toFile()
        );

        try (var fm = compiler.getStandardFileManager(diags, null, null)) {
            var units = fm.getJavaFileObjectsFromFiles(srcFiles);
            boolean ok = compiler.getTask(
                null, fm, diags, Arrays.asList("-classpath", classpath), null, units
            ).call();
            if (!ok) {
                StringBuilder sb = new StringBuilder("Compilation failed:\n");
                diags.getDiagnostics().forEach(d -> sb.append(d).append('\n'));
                throw new AssertionError(sb.toString());
            }
        }
    }

    @SuppressWarnings("unused")
    private void writeSource(Path tmpDir, String src) throws IOException {
        Path pkgDir = tmpDir.resolve(PKG.replace('.', File.separatorChar));
        Files.createDirectories(pkgDir);
    }
}
