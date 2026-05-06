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
import org.apache.kafka.connect.manifest.codegen.generator.TaskGenerator;
import org.apache.kafka.connect.manifest.codegen.model.ManifestSpec;
import org.apache.kafka.connect.manifest.codegen.parser.ManifestParser;
import com.squareup.javapoet.JavaFile;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Grades each manifest by whether the generated connector would actually function
 * end-to-end against the real API, not just whether the Java compiles.
 *
 * <p>A manifest is BROKEN if it uses any feature we do not implement at runtime —
 * even if codegen happily emits a class for it.
 */
public class ManifestCoverageReport {

    /** Features whose presence makes the generated connector unable to pull real data. */
    private static final String[] HARD_BLOCKERS = {
        "type: AsyncRetriever",                // long-running report queries
        "type: GraphqlRequester",              // we only do REST
        "type: GraphQLHttpRequester",
        "type: ConditionalStreams",            // conditional stream gating
        "type: FileUploader",                  // file uploads
        "type: DynamicDeclarativeStream",      // alt dynamic-stream form
        "type: ParametrizedComponentsResolver", // parametrized component resolution
        "type: ConfigComponentsResolver",      // config-driven component resolution
        "type: PropertiesFromEndpoint",        // properties fetched from endpoint
        "type: QueryProperties",               // dynamic field-list expansion (Hubspot)
        "type: IncrementingCountCursor",       // integer cursor (non-datetime)
        "type: GroupingPartitionRouter",       // batched parent IDs
        "type: DynamicSchemaLoader",           // schema discovered at runtime
        "type: StateDelegatingStream",         // full→incremental switchover
        "type: LegacyToPerPartitionStateMigration",
        "type: ConfigMigration",
        "type: ConfigNormalizationRules",
        "type: HttpRequestRegexMatcher",
        "type: GroupByKeyMergeStrategy",
        "type: RecordExpander",
        "type: HTTPAPIBudget",                 // rate limiting (would 429-storm)
        "type: MovingWindowCallRatePolicy",
        "type: FixedWindowCallRatePolicy",
        "type: ZipfileDecoder",
        "type: XmlDecoder",                    // not implemented (only JSON)
        "type: GzipDecoder",
        "type: JsonlDecoder",
        "type: IterableDecoder",
        "type: CsvDecoder",
        "type: PaginationReset",
        "type: DpathValidator",
        "type: ValidateAdheresToSchema",
        "type: SchemaTypeIdentifier",
        "type: TypesMap",
        "type: PaginationResetLimits",
        "type: CustomRetriever",
        "type: CustomRequester",
        "type: CustomAuthenticator",
        "type: CustomDecoder",
        "type: CustomErrorHandler",
        "type: CustomBackoffStrategy",
        "type: CustomPaginationStrategy",
        "type: CustomPartitionRouter",
        "type: CustomRecordExtractor",
        "type: CustomRecordFilter",
        "type: CustomTransformation",
        "type: CustomSchemaLoader",
        "type: CustomSchemaNormalization",
        "type: CustomStateMigration",
        "type: CustomIncrementalSync",
        "type: CustomValidationStrategy",
        "type: CustomConfigTransformation",
    };

    /** Features that silently change record shape if we do not implement them. */
    private static final String[] SOFT_BLOCKERS = {
        "type: AddFields",         // computed fields appended per record
        "type: RemoveFields",      // fields removed per record
        "type: RecordFilter",      // filter records by predicate
        "type: KeysToLower",
        "type: KeysReplace",
        "type: KeysToSnakeCase",
        "type: KeyTransformation",
        "type: DpathFlattenFields",
        "type: ConfigAddFields",
        "type: ConfigRemapField",
        "type: FlattenFields",
    };

    /**
     * Reliability features whose absence means the task dies on first transient error or hits rate limits.
     *
     * <p>Phase 1 (DefaultErrorHandler / CompositeErrorHandler / ExponentialBackoffStrategy /
     * ConstantBackoffStrategy / WaitTimeFromHeader / WaitUntilTimeFromHeader) is now wired
     * through the generated tasks via {@code RetryPolicy} + {@code BackoffStrategy} fields.
     * Only HTTPAPIBudget remains as a reliability gap.</p>
     */
    private static final String[] RELIABILITY_GAPS = {
        "type: HTTPAPIBudget",
    };

    @Test
    void gradeEndToEndCoverage() throws Exception {
        URL dir = getClass().getClassLoader().getResource("manifests");
        Path manifestsDir = Path.of(dir.toURI());
        ManifestParser parser = new ManifestParser();
        TaskGenerator taskGen = new TaskGenerator();

        int total = 0;
        int worksFully = 0;
        int worksRecordShape = 0;
        int worksReliability = 0;
        int hardBroken = 0;
        int stubOnly = 0;

        Map<String, Integer> blockerCounts = new LinkedHashMap<>();
        List<String> hardBrokenList = new ArrayList<>();
        List<String> stubOnlyList = new ArrayList<>();

        try (var dirStream = Files.list(manifestsDir)) {
            List<Path> all = dirStream.filter(p -> p.toString().endsWith(".yaml")).sorted().toList();
            for (Path p : all) {
                total++;
                String name = p.getFileName().toString();
                String body = Files.readString(p);

                // Classify codegen outcome first.
                boolean isStub;
                try (var in = Files.newInputStream(p)) {
                    ManifestSpec spec = parser.parse(in);
                    JavaFile taskFile = taskGen.generate(spec, ConfigGenerator.BASE_PACKAGE);
                    String src = taskFile.toString();
                    isStub = src.contains("Dynamic-stream codegen not yet supported")
                        || spec.resolvedStreams().isEmpty();
                } catch (Exception e) {
                    hardBroken++;
                    hardBrokenList.add(name + " -> codegen-fail: " + e.getClass().getSimpleName());
                    continue;
                }

                if (isStub) {
                    stubOnly++;
                    stubOnlyList.add(name);
                    continue;
                }

                Set<String> hits = featureHits(body, HARD_BLOCKERS);
                Set<String> soft = featureHits(body, SOFT_BLOCKERS);
                Set<String> rely = featureHits(body, RELIABILITY_GAPS);
                // POST/PUT requesters — codegen emits GET only.
                boolean nonGet = body.contains("http_method: POST") || body.contains("http_method: PUT")
                    || body.contains("http_method: \"POST\"") || body.contains("http_method: \"PUT\"");
                if (nonGet) {
                    hits.add("http_method: POST/PUT");
                }
                // DatetimeBasedCursor with step → window slicing not implemented.
                boolean slicedCursor = body.contains("type: DatetimeBasedCursor")
                    && body.matches("(?s).*\\bstep:\\s*P[A-Z0-9]+.*");
                if (slicedCursor) {
                    hits.add("DatetimeBasedCursor.step (slicing)");
                }

                hits.forEach(f -> blockerCounts.merge(f, 1, Integer::sum));
                soft.forEach(f -> blockerCounts.merge(f + " [soft]", 1, Integer::sum));
                rely.forEach(f -> blockerCounts.merge(f + " [reliability]", 1, Integer::sum));

                if (!hits.isEmpty()) {
                    hardBroken++;
                    hardBrokenList.add(name + " -> " + String.join(", ", hits));
                } else if (!soft.isEmpty()) {
                    worksRecordShape++;     // records arrive with wrong shape
                } else if (!rely.isEmpty()) {
                    worksReliability++;     // first transient failure kills the task
                } else {
                    worksFully++;
                }
            }
        }

        System.out.println("\n========== END-TO-END COVERAGE ==========");
        System.out.println("Total manifests        : " + total);
        System.out.println("WORKS (no missing feat): " + worksFully);
        System.out.println("WORKS but wrong shape  : " + worksRecordShape + "  (uses transformations we ignore)");
        System.out.println("WORKS until first error: " + worksReliability + "  (no retry/backoff)");
        System.out.println("BROKEN (missing feature): " + hardBroken);
        System.out.println("STUB (already non-fn)  : " + stubOnly);
        System.out.println("=========================================\n");

        System.out.println("---- BLOCKER FREQUENCY (manifests using each feature) ----");
        blockerCounts.entrySet().stream()
            .sorted((a, b) -> Integer.compare(b.getValue(), a.getValue()))
            .forEach(e -> System.out.printf("  %4d  %s%n", e.getValue(), e.getKey()));

        System.out.println("\n---- BROKEN MANIFESTS (full list, " + hardBroken + ") ----");
        hardBrokenList.forEach(System.out::println);

        System.out.println("\n---- STUB MANIFESTS (" + stubOnly + ") ----");
        stubOnlyList.forEach(System.out::println);
    }

    private Set<String> featureHits(String body, String[] features) {
        Set<String> hits = new LinkedHashSet<>();
        for (String f : features) {
            if (body.contains(f)) {
                hits.add(f.replace("type: ", ""));
            }
        }
        return hits;
    }
}
