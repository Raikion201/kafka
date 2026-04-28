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

import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.connect.manifest.codegen.generator.CodegenException;
import org.apache.kafka.connect.manifest.codegen.generator.ConfigGenerator;
import org.apache.kafka.connect.manifest.codegen.generator.ConnectorGenerator;
import org.apache.kafka.connect.manifest.codegen.generator.TaskGenerator;
import org.apache.kafka.connect.manifest.codegen.model.ManifestSpec;
import org.apache.kafka.connect.manifest.codegen.parser.ManifestParseException;
import org.apache.kafka.connect.manifest.codegen.parser.ManifestParser;

import com.squareup.javapoet.JavaFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Command-line entry point for the Airbyte-manifest-to-Kafka-Connect code generator.
 *
 * <p>Usage:
 * <pre>
 *   java -jar connect-manifest-codegen.jar &lt;manifest.yaml&gt; &lt;output-dir&gt; [package]
 * </pre>
 *
 * <ul>
 *   <li>{@code manifest.yaml} — path to the Airbyte connector manifest</li>
 *   <li>{@code output-dir} — directory where generated {@code .java} files are written</li>
 *   <li>{@code package} — (optional) Java package; defaults to
 *       {@link ConfigGenerator#BASE_PACKAGE}</li>
 * </ul>
 *
 * <p>Three files are generated:
 * <ol>
 *   <li>{@code XxxConnectorConfig.java}</li>
 *   <li>{@code XxxSourceConnector.java}</li>
 *   <li>{@code XxxSourceTask.java}</li>
 * </ol>
 *
 * <p>Exit codes:
 * <ul>
 *   <li>0 — success</li>
 *   <li>1 — bad arguments</li>
 *   <li>2 — manifest parse error</li>
 *   <li>3 — code generation error</li>
 *   <li>4 — I/O error writing output files</li>
 * </ul>
 */
public class ManifestCodegenCli {

    private static final Logger LOG = LoggerFactory.getLogger(ManifestCodegenCli.class);

    private static final int EXIT_BAD_ARGS = 1;
    private static final int EXIT_PARSE_ERROR = 2;
    private static final int EXIT_CODEGEN_ERROR = 3;
    private static final int EXIT_IO_ERROR = 4;

    public static void main(String[] args) {
        Exit.exit(run(args));
    }

    /**
     * Core logic, separated from {@link #main} so it can be called from tests without
     * triggering {@link System#exit}.
     *
     * @return exit code
     */
    static int run(String[] args) {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: ManifestCodegenCli <manifest.yaml> <output-dir> [package]");
            return EXIT_BAD_ARGS;
        }

        File manifestFile = new File(args[0]);
        Path outputDir = Path.of(args[1]);
        String pkgName = args.length == 3 ? args[2] : ConfigGenerator.BASE_PACKAGE;

        if (!manifestFile.isFile()) {
            System.err.println("Manifest file not found: " + manifestFile.getAbsolutePath());
            return EXIT_BAD_ARGS;
        }

        ManifestSpec spec;
        try {
            spec = new ManifestParser().parse(manifestFile);
        } catch (ManifestParseException e) {
            LOG.error("Failed to parse manifest {}: {}", manifestFile, e.getMessage(), e);
            System.err.println("Parse error: " + e.getMessage());
            return EXIT_PARSE_ERROR;
        }

        try {
            JavaFile configFile = new ConfigGenerator().generate(spec, pkgName);
            JavaFile connectorFile = new ConnectorGenerator().generate(spec, pkgName);
            JavaFile taskFile = new TaskGenerator().generate(spec, pkgName);

            configFile.writeTo(outputDir);
            connectorFile.writeTo(outputDir);
            taskFile.writeTo(outputDir);

            LOG.info("Generated {} in {}", configFile.typeSpec.name, outputDir);
            LOG.info("Generated {} in {}", connectorFile.typeSpec.name, outputDir);
            LOG.info("Generated {} in {}", taskFile.typeSpec.name, outputDir);

            System.out.println("Generated:");
            System.out.println("  " + configFile.typeSpec.name + ".java");
            System.out.println("  " + connectorFile.typeSpec.name + ".java");
            System.out.println("  " + taskFile.typeSpec.name + ".java");
        } catch (CodegenException e) {
            LOG.error("Code generation failed: {}", e.getMessage(), e);
            System.err.println("Codegen error: " + e.getMessage());
            return EXIT_CODEGEN_ERROR;
        } catch (IOException e) {
            LOG.error("Failed to write output files to {}: {}", outputDir, e.getMessage(), e);
            System.err.println("I/O error: " + e.getMessage());
            return EXIT_IO_ERROR;
        }

        return 0;
    }
}
