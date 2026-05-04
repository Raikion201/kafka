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
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
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
 * Unit and integration tests for {@link ConfigGenerator}.
 */
public class ConfigGeneratorTest {

    private static final String PKG = ConfigGenerator.BASE_PACKAGE;
    private final ConfigGenerator generator = new ConfigGenerator();
    private final ManifestParser parser = new ManifestParser();

    // ── helpers ───────────────────────────────────────────────────────────────

    private InputStream resource(String name) {
        return getClass().getClassLoader().getResourceAsStream("manifests/" + name);
    }

    private ManifestSpec load(String name) throws Exception {
        return parser.parse(resource(name));
    }

    // ── unit tests: zapier (has required "secret" field) ─────────────────────

    @Test
    void zapier_generatesClassWithCorrectName() throws Exception {
        JavaFile file = generator.generate(load("zapier.yaml"), PKG);
        assertEquals("ZapierSupportedStorageConnectorConfig", file.typeSpec.name);
    }

    @Test
    void zapier_generatesConstantForSecret() throws Exception {
        JavaFile file = generator.generate(load("zapier.yaml"), PKG);
        boolean found = file.typeSpec.fieldSpecs.stream()
            .anyMatch(f -> f.name.equals("SECRET_CONFIG"));
        assertTrue(found, "Expected SECRET_CONFIG constant");
    }

    @Test
    void zapier_requiredField_hasHighImportance() throws Exception {
        ManifestSpec spec = load("zapier.yaml");
        JavaFile file = generator.generate(spec, PKG);
        // "secret" is required → HIGH importance; verify via generated source string
        String src = file.toString();
        assertTrue(src.contains("Importance.HIGH"), "Required field must use HIGH importance");
    }

    @Test
    void zapier_generatesAccessorForSecret() throws Exception {
        JavaFile file = generator.generate(load("zapier.yaml"), PKG);
        boolean found = file.typeSpec.methodSpecs.stream()
            .anyMatch(m -> m.name.equals("getSecret"));
        assertTrue(found, "Expected getSecret() accessor");
    }

    // ── unit tests: xkcd (has optional "comic_number" field) ─────────────────

    @Test
    void xkcd_generatesConstantForComicNumber() throws Exception {
        JavaFile file = generator.generate(load("xkcd.yaml"), PKG);
        boolean found = file.typeSpec.fieldSpecs.stream()
            .anyMatch(f -> f.name.equals("COMIC_NUMBER_CONFIG"));
        assertTrue(found, "Expected COMIC_NUMBER_CONFIG constant");
    }

    @Test
    void xkcd_optionalField_hasMediumImportance() throws Exception {
        JavaFile file = generator.generate(load("xkcd.yaml"), PKG);
        String src = file.toString();
        assertTrue(src.contains("Importance.MEDIUM"), "Optional field must use MEDIUM importance");
    }

    @Test
    void xkcd_generatesAccessorForComicNumber() throws Exception {
        JavaFile file = generator.generate(load("xkcd.yaml"), PKG);
        boolean found = file.typeSpec.methodSpecs.stream()
            .anyMatch(m -> m.name.equals("getComicNumber"));
        assertTrue(found, "Expected getComicNumber() accessor");
    }

    // ── unit tests: defillama (no spec properties) ───────────────────────────

    @Test
    void defillama_noProperties_generatesEmptyConfig() throws Exception {
        JavaFile file = generator.generate(load("defillama.yaml"), PKG);
        // No key constants expected (fieldSpecs only has type constants if any)
        boolean hasConfigConstants = file.typeSpec.fieldSpecs.stream()
            .anyMatch(f -> f.name.endsWith("_CONFIG"));
        // defillama has no spec properties — no _CONFIG fields expected
        assertTrue(!hasConfigConstants || file.typeSpec.fieldSpecs.isEmpty(),
            "Defillama has no spec properties; no _CONFIG constants expected");
    }

    @Test
    void defillama_generatesValidClassName() throws Exception {
        JavaFile file = generator.generate(load("defillama.yaml"), PKG);
        // connectorClassName() = "DefillamaSource" → strip "Source" → "Defillama" + "ConnectorConfig"
        assertEquals("DefillamaConnectorConfig", file.typeSpec.name);
    }

    // ── unit tests: generated source structure ────────────────────────────────

    @Test
    void generatedClass_extendsAbstractConfig() throws Exception {
        JavaFile file = generator.generate(load("zapier.yaml"), PKG);
        String src = file.toString();
        assertTrue(src.contains("extends AbstractConfig"), "Generated class must extend AbstractConfig");
    }

    @Test
    void generatedClass_hasStaticConfigMethod() throws Exception {
        JavaFile file = generator.generate(load("zapier.yaml"), PKG);
        boolean hasConfigMethod = file.typeSpec.methodSpecs.stream()
            .anyMatch(m -> m.name.equals("config") && m.modifiers.contains(Modifier.STATIC));
        assertTrue(hasConfigMethod, "Expected static config() factory method");
    }

    @Test
    void generatedClass_hasConstructorTakingMap() throws Exception {
        JavaFile file = generator.generate(load("zapier.yaml"), PKG);
        boolean hasConstructor = file.typeSpec.methodSpecs.stream()
            .anyMatch(m -> m.isConstructor());
        assertTrue(hasConstructor, "Expected a constructor");
    }

    // ── unit tests: ConfigField internal logic ────────────────────────────────

    @Test
    void configField_constantName_snakeCase() {
        ConfigGenerator.ConfigField f = new ConfigGenerator.ConfigField("api_key", "doc", "string", true, null);
        assertEquals("API_KEY_CONFIG", f.constantName());
    }

    @Test
    void configField_constantName_kebabCase() {
        ConfigGenerator.ConfigField f = new ConfigGenerator.ConfigField("comic-number", "doc", "string", false, null);
        assertEquals("COMIC_NUMBER_CONFIG", f.constantName());
    }

    @Test
    void configField_importance_required() {
        ConfigGenerator.ConfigField f = new ConfigGenerator.ConfigField("k", "d", "string", true, null);
        assertEquals("HIGH", f.importance());
    }

    @Test
    void configField_importance_optional() {
        ConfigGenerator.ConfigField f = new ConfigGenerator.ConfigField("k", "d", "string", false, null);
        assertEquals("MEDIUM", f.importance());
    }

    @Test
    void configField_configDefType_string() {
        ConfigGenerator.ConfigField f = new ConfigGenerator.ConfigField("k", "d", "string", false, null);
        assertEquals("STRING", f.configDefType());
    }

    @Test
    void configField_configDefType_boolean() {
        ConfigGenerator.ConfigField f = new ConfigGenerator.ConfigField("k", "d", "boolean", false, null);
        assertEquals("BOOLEAN", f.configDefType());
        assertTrue(f.isBoolean());
    }

    @Test
    void configField_configDefType_integer() {
        ConfigGenerator.ConfigField f = new ConfigGenerator.ConfigField("k", "d", "integer", false, null);
        assertEquals("LONG", f.configDefType());
    }

    @Test
    void configField_configDefType_nullDefaultsToString() {
        ConfigGenerator.ConfigField f = new ConfigGenerator.ConfigField("k", "d", null, false, null);
        assertEquals("STRING", f.configDefType());
    }

    // ── integration test: generated code compiles ─────────────────────────────

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

    // ── integration test: generated class loads and config() returns non-null ──

    @Test
    void zapier_compiledClass_configMethodReturnsNonNull(@TempDir Path tmpDir) throws Exception {
        Class<?> cls = compileAndLoad(load("zapier.yaml"), tmpDir);
        Object configDef = cls.getMethod("config").invoke(null);
        assertNotNull(configDef, "config() must return a non-null ConfigDef");
    }

    @Test
    void xkcd_compiledClass_configMethodReturnsNonNull(@TempDir Path tmpDir) throws Exception {
        Class<?> cls = compileAndLoad(load("xkcd.yaml"), tmpDir);
        Object configDef = cls.getMethod("config").invoke(null);
        assertNotNull(configDef, "config() must return a non-null ConfigDef");
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private void assertGeneratedCodeCompiles(ManifestSpec spec, Path tmpDir) throws Exception {
        JavaFile file = generator.generate(spec, PKG);
        file.writeTo(tmpDir);

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull(compiler, "javax.tools.JavaCompiler not available (JDK required)");

        DiagnosticCollector<JavaFileObject> diags = new DiagnosticCollector<>();
        String classpath = System.getProperty("java.class.path");

        File srcFile = tmpDir.resolve(
            PKG.replace('.', File.separatorChar) + File.separator + file.typeSpec.name + ".java"
        ).toFile();

        try (var fm = compiler.getStandardFileManager(diags, null, null)) {
            var units = fm.getJavaFileObjectsFromFiles(List.of(srcFile));
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

    private Class<?> compileAndLoad(ManifestSpec spec, Path tmpDir) throws Exception {
        JavaFile file = generator.generate(spec, PKG);
        assertGeneratedCodeCompiles(spec, tmpDir);

        URL[] urls = {tmpDir.toUri().toURL()};
        try (URLClassLoader cl = new URLClassLoader(urls, getClass().getClassLoader())) {
            return cl.loadClass(PKG + "." + file.typeSpec.name);
        }
    }
}
