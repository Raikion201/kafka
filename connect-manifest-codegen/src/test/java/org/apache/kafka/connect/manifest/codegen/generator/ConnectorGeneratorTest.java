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
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import javax.lang.model.element.Modifier;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit and integration tests for {@link ConnectorGenerator}.
 */
public class ConnectorGeneratorTest {

    private static final String PKG = ConfigGenerator.BASE_PACKAGE;
    private final ConnectorGenerator generator = new ConnectorGenerator();
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
        assertEquals("ZapierSupportedStorageSourceConnector", file.typeSpec.name);
    }

    @Test
    void xkcd_generatesCorrectClassName() throws Exception {
        JavaFile file = generator.generate(load("xkcd.yaml"), PKG);
        assertEquals("XkcdSourceConnector", file.typeSpec.name);
    }

    @Test
    void defillama_generatesCorrectClassName() throws Exception {
        JavaFile file = generator.generate(load("defillama.yaml"), PKG);
        assertEquals("DefillamaSourceConnector", file.typeSpec.name);
    }

    // ── extends ───────────────────────────────────────────────────────────────

    @Test
    void generatedClass_extendsSourceConnector() throws Exception {
        JavaFile file = generator.generate(load("zapier.yaml"), PKG);
        String src = file.toString();
        assertTrue(src.contains("extends SourceConnector"), "Must extend SourceConnector");
    }

    // ── required methods ──────────────────────────────────────────────────────

    @Test
    void generatedClass_hasVersionMethod() throws Exception {
        JavaFile file = generator.generate(load("zapier.yaml"), PKG);
        boolean found = file.typeSpec.methodSpecs.stream()
            .anyMatch(m -> m.name.equals("version") && !m.modifiers.contains(Modifier.STATIC));
        assertTrue(found, "Expected version() method");
    }

    @Test
    void generatedClass_hasStartMethod() throws Exception {
        JavaFile file = generator.generate(load("zapier.yaml"), PKG);
        boolean found = file.typeSpec.methodSpecs.stream()
            .anyMatch(m -> m.name.equals("start"));
        assertTrue(found, "Expected start() method");
    }

    @Test
    void generatedClass_hasStopMethod() throws Exception {
        JavaFile file = generator.generate(load("zapier.yaml"), PKG);
        boolean found = file.typeSpec.methodSpecs.stream()
            .anyMatch(m -> m.name.equals("stop"));
        assertTrue(found, "Expected stop() method");
    }

    @Test
    void generatedClass_hasTaskClassMethod() throws Exception {
        JavaFile file = generator.generate(load("zapier.yaml"), PKG);
        boolean found = file.typeSpec.methodSpecs.stream()
            .anyMatch(m -> m.name.equals("taskClass"));
        assertTrue(found, "Expected taskClass() method");
    }

    @Test
    void generatedClass_hasTaskConfigsMethod() throws Exception {
        JavaFile file = generator.generate(load("zapier.yaml"), PKG);
        boolean found = file.typeSpec.methodSpecs.stream()
            .anyMatch(m -> m.name.equals("taskConfigs"));
        assertTrue(found, "Expected taskConfigs() method");
    }

    @Test
    void generatedClass_hasConfigMethod() throws Exception {
        JavaFile file = generator.generate(load("zapier.yaml"), PKG);
        boolean found = file.typeSpec.methodSpecs.stream()
            .anyMatch(m -> m.name.equals("config"));
        assertTrue(found, "Expected config() method");
    }

    // ── references correct task class ─────────────────────────────────────────

    @Test
    void zapier_taskClassReferencesCorrectTask() throws Exception {
        JavaFile file = generator.generate(load("zapier.yaml"), PKG);
        String src = file.toString();
        assertTrue(src.contains("ZapierSupportedStorageSourceTask.class"),
            "Must reference ZapierSupportedStorageSourceTask.class");
    }

    @Test
    void xkcd_taskClassReferencesCorrectTask() throws Exception {
        JavaFile file = generator.generate(load("xkcd.yaml"), PKG);
        String src = file.toString();
        assertTrue(src.contains("XkcdSourceTask.class"), "Must reference XkcdSourceTask.class");
    }

    // ── references correct config class ──────────────────────────────────────

    @Test
    void zapier_configDelegatestoConfigClass() throws Exception {
        JavaFile file = generator.generate(load("zapier.yaml"), PKG);
        String src = file.toString();
        assertTrue(src.contains("ZapierSupportedStorageConnectorConfig.config()"),
            "config() must delegate to XxxConnectorConfig.config()");
    }

    // ── integration: generated source compiles ────────────────────────────────

    @Test
    void zapier_generatedSourceCompiles(@TempDir Path tmpDir) throws Exception {
        ManifestSpec spec = load("zapier.yaml");
        assertGeneratedCodeCompiles(spec, tmpDir);
    }

    @Test
    void xkcd_generatedSourceCompiles(@TempDir Path tmpDir) throws Exception {
        ManifestSpec spec = load("xkcd.yaml");
        assertGeneratedCodeCompiles(spec, tmpDir);
    }

    @Test
    void defillama_generatedSourceCompiles(@TempDir Path tmpDir) throws Exception {
        ManifestSpec spec = load("defillama.yaml");
        assertGeneratedCodeCompiles(spec, tmpDir);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    /**
     * Writes config, a stub task, and the connector class, then compiles all three together.
     * The connector references both the config class and the task class, so all must be present.
     */
    private void assertGeneratedCodeCompiles(ManifestSpec spec, Path tmpDir) throws Exception {
        JavaFile configFile = configGenerator.generate(spec, PKG);
        JavaFile connectorFile = generator.generate(spec, PKG);
        configFile.writeTo(tmpDir);
        connectorFile.writeTo(tmpDir);

        // Write a minimal stub task so the connector's taskClass() reference compiles.
        String taskClassName = connectorFile.typeSpec.name.replace("Connector", "Task");
        writeStubTaskSource(tmpDir, taskClassName);

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull(compiler, "javax.tools.JavaCompiler not available (JDK required)");

        DiagnosticCollector<JavaFileObject> diags = new DiagnosticCollector<>();
        String classpath = System.getProperty("java.class.path");
        String pkgDir = PKG.replace('.', File.separatorChar) + File.separator;

        List<File> srcFiles = List.of(
            tmpDir.resolve(pkgDir + configFile.typeSpec.name + ".java").toFile(),
            tmpDir.resolve(pkgDir + taskClassName + ".java").toFile(),
            tmpDir.resolve(pkgDir + connectorFile.typeSpec.name + ".java").toFile()
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

    private void writeStubTaskSource(Path tmpDir, String taskClassName) throws IOException {
        String src = "package " + PKG + ";\n"
            + "import org.apache.kafka.connect.source.SourceTask;\n"
            + "import org.apache.kafka.connect.source.SourceRecord;\n"
            + "import java.util.List;\n"
            + "import java.util.Map;\n"
            + "public class " + taskClassName + " extends SourceTask {\n"
            + "    public String version() { return \"1.0.0\"; }\n"
            + "    public void start(Map<String, String> props) { }\n"
            + "    public List<SourceRecord> poll() { return List.of(); }\n"
            + "    public void stop() { }\n"
            + "}\n";
        Path pkgDir = tmpDir.resolve(PKG.replace('.', File.separatorChar));
        Files.createDirectories(pkgDir);
        Files.writeString(pkgDir.resolve(taskClassName + ".java"), src);
    }
}
