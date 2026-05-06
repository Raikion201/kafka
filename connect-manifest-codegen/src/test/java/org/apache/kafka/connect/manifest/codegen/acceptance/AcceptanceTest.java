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
package org.apache.kafka.connect.manifest.codegen.acceptance;

import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Discovers every YAML under {@code src/test/resources/acceptance-tests/} and
 * generates two dynamic tests per file: {@code read} and {@code state-resume}.
 *
 * <p>When env vars referenced by the YAML are unset the test is <b>skipped</b>
 * (Airbyte's CAT semantics) so CI on a clean checkout stays green; a separate
 * credentialed CI job sets the env vars and the same tests then assert real
 * behaviour against the live API.
 */
final class AcceptanceTest {

    @TestFactory
    Stream<DynamicNode> acceptanceTests() throws Exception {
        Path dir = acceptanceTestsDir();
        if (dir == null || !Files.isDirectory(dir)) return Stream.empty();

        List<DynamicNode> nodes = new ArrayList<>();
        try (var paths = Files.list(dir)) {
            List<Path> yamls = paths
                .filter(p -> p.toString().endsWith(".yaml") || p.toString().endsWith(".yml"))
                .sorted()
                .toList();
            for (Path yaml : yamls) {
                AcceptanceConfig cfg = AcceptanceTestHarness.load(yaml);
                String name = yaml.getFileName().toString();
                nodes.add(DynamicTest.dynamicTest(name + " :: read", () -> runRead(cfg)));
                nodes.add(DynamicTest.dynamicTest(name + " :: state-resume", () -> runStateResume(cfg)));
            }
        }
        return nodes.stream();
    }

    private static void runRead(AcceptanceConfig cfg) throws Exception {
        AcceptanceTestHarness.Result r = AcceptanceTestHarness.runRead(cfg, System.getenv());
        assumeFalse(r.skipped, () -> "skipped — " + r.skipReason);
        assertTrue(r.ok(), r::summary);
    }

    private static void runStateResume(AcceptanceConfig cfg) throws Exception {
        AcceptanceTestHarness.Result r = AcceptanceTestHarness.runStateResume(cfg, System.getenv());
        assumeFalse(r.skipped, () -> "skipped — " + r.skipReason);
        assertTrue(r.ok(), r::summary);
    }

    private static Path acceptanceTestsDir() throws URISyntaxException {
        URL url = AcceptanceTest.class.getClassLoader().getResource("acceptance-tests");
        return url == null ? null : Path.of(url.toURI());
    }

    @SuppressWarnings("unused") // keep classpath stable when no YAMLs exist
    private static final List<String> EMPTY = Collections.emptyList();
}
