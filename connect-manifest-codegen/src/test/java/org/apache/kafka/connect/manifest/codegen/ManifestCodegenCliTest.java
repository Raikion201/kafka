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

import org.apache.kafka.connect.manifest.codegen.generator.ConfigGenerator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ManifestCodegenCli}.
 *
 * <p>We call {@link ManifestCodegenCli#run} directly so that {@link System#exit} is never invoked.
 */
public class ManifestCodegenCliTest {

    private File resource(String name) throws Exception {
        URL url = getClass().getClassLoader().getResource("manifests/" + name);
        if (url == null) {
            throw new IllegalArgumentException("Resource not found: manifests/" + name);
        }
        return new File(url.toURI());
    }

    // ── bad-args exit code ────────────────────────────────────────────────────

    @Test
    void noArgs_returnsBadArgs() {
        assertEquals(1, ManifestCodegenCli.run(new String[0]));
    }

    @Test
    void oneArg_returnsBadArgs() {
        assertEquals(1, ManifestCodegenCli.run(new String[]{"only-one"}));
    }

    @Test
    void fourArgs_returnsBadArgs() {
        assertEquals(1, ManifestCodegenCli.run(new String[]{"a", "b", "c", "d"}));
    }

    @Test
    void missingManifestFile_returnsBadArgs(@TempDir Path tmpDir) {
        int code = ManifestCodegenCli.run(new String[]{"nonexistent.yaml", tmpDir.toString()});
        assertEquals(1, code);
    }

    // ── parse error exit code ─────────────────────────────────────────────────

    @Test
    void emptyManifest_returnsParseError(@TempDir Path tmpDir) throws Exception {
        Path bad = tmpDir.resolve("bad.yaml");
        Files.writeString(bad, "version: 1.0\ntype: DeclarativeSource\n");
        int code = ManifestCodegenCli.run(new String[]{bad.toString(), tmpDir.toString()});
        assertEquals(2, code);
    }

    @Test
    void manifestWithNoStreams_returnsParseError(@TempDir Path tmpDir) throws Exception {
        Path bad = tmpDir.resolve("nostreams.yaml");
        Files.writeString(bad, "version: 1.0\ntype: DeclarativeSource\nstreams: []\n");
        int code = ManifestCodegenCli.run(new String[]{bad.toString(), tmpDir.toString()});
        assertEquals(2, code);
    }

    // ── success: three files generated ────────────────────────────────────────

    @Test
    void zapier_generatesThreeFiles(@TempDir Path tmpDir) throws Exception {
        int code = ManifestCodegenCli.run(new String[]{
            resource("zapier.yaml").toString(),
            tmpDir.toString()
        });
        assertEquals(0, code, "Expected exit code 0 (success)");

        String pkg = ConfigGenerator.BASE_PACKAGE.replace('.', '/');
        assertTrue(Files.exists(tmpDir.resolve(pkg + "/ZapierConnectorConfig.java")),
            "Config file not generated");
        assertTrue(Files.exists(tmpDir.resolve(pkg + "/ZapierSourceConnector.java")),
            "Connector file not generated");
        assertTrue(Files.exists(tmpDir.resolve(pkg + "/ZapierSourceTask.java")),
            "Task file not generated");
    }

    @Test
    void xkcd_generatesThreeFiles(@TempDir Path tmpDir) throws Exception {
        int code = ManifestCodegenCli.run(new String[]{
            resource("xkcd.yaml").toString(),
            tmpDir.toString()
        });
        assertEquals(0, code, "Expected exit code 0 (success)");

        String pkg = ConfigGenerator.BASE_PACKAGE.replace('.', '/');
        assertTrue(Files.exists(tmpDir.resolve(pkg + "/XkcdConnectorConfig.java")));
        assertTrue(Files.exists(tmpDir.resolve(pkg + "/XkcdSourceConnector.java")));
        assertTrue(Files.exists(tmpDir.resolve(pkg + "/XkcdSourceTask.java")));
    }

    @Test
    void defillama_generatesThreeFiles(@TempDir Path tmpDir) throws Exception {
        int code = ManifestCodegenCli.run(new String[]{
            resource("defillama.yaml").toString(),
            tmpDir.toString()
        });
        assertEquals(0, code, "Expected exit code 0 (success)");

        String pkg = ConfigGenerator.BASE_PACKAGE.replace('.', '/');
        assertTrue(Files.exists(tmpDir.resolve(pkg + "/DefillamaConnectorConfig.java")));
        assertTrue(Files.exists(tmpDir.resolve(pkg + "/DefillamaSourceConnector.java")));
        assertTrue(Files.exists(tmpDir.resolve(pkg + "/DefillamaSourceTask.java")));
    }

    // ── custom package ────────────────────────────────────────────────────────

    @Test
    void customPackage_generatesFilesInCorrectPackage(@TempDir Path tmpDir) throws Exception {
        String customPkg = "com.example.connectors";
        int code = ManifestCodegenCli.run(new String[]{
            resource("xkcd.yaml").toString(),
            tmpDir.toString(),
            customPkg
        });
        assertEquals(0, code);

        String pkgPath = "com/example/connectors";
        assertTrue(Files.exists(tmpDir.resolve(pkgPath + "/XkcdConnectorConfig.java")));
        assertTrue(Files.exists(tmpDir.resolve(pkgPath + "/XkcdSourceConnector.java")));
        assertTrue(Files.exists(tmpDir.resolve(pkgPath + "/XkcdSourceTask.java")));
    }

    // ── generated file content sanity checks ──────────────────────────────────

    @Test
    void zapier_configFileContainsSecretConstant(@TempDir Path tmpDir) throws Exception {
        ManifestCodegenCli.run(new String[]{
            resource("zapier.yaml").toString(),
            tmpDir.toString()
        });
        String pkg = ConfigGenerator.BASE_PACKAGE.replace('.', '/');
        String content = Files.readString(
            tmpDir.resolve(pkg + "/ZapierConnectorConfig.java"));
        assertTrue(content.contains("SECRET_CONFIG"), "Config must declare SECRET_CONFIG constant");
    }

    @Test
    void xkcd_taskFileContainsFieldPath(@TempDir Path tmpDir) throws Exception {
        ManifestCodegenCli.run(new String[]{
            resource("xkcd.yaml").toString(),
            tmpDir.toString()
        });
        // xkcd field_path is empty; BASE_URL must still be present
        String pkg = ConfigGenerator.BASE_PACKAGE.replace('.', '/');
        String content = Files.readString(
            tmpDir.resolve(pkg + "/XkcdSourceTask.java"));
        assertTrue(content.contains("https://xkcd.com"), "Task must embed base URL");
    }
}
