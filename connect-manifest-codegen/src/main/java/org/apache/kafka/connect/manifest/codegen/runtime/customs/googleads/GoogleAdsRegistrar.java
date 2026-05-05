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
package org.apache.kafka.connect.manifest.codegen.runtime.customs.googleads;

import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomComponentRegistry;

/**
 * Wires every Phase 1 Google Ads {@code class_name} from {@code source_google_ads.yaml}
 * to its hand-written Java implementation.
 *
 * <p>Generated source-task code calls {@link #register()} once at task start. A static
 * initialiser also calls it so plain classloading is sufficient when the generated code
 * holds a reference to any class in this package.</p>
 *
 * <p>Note: {@code DoubleQuotedDictTypeTransformer} is referenced 5 times in the manifest
 * (once per stream that needs schema normalisation). It is registered exactly once here
 * — the registry is keyed by class_name, so all references resolve to the same factory.</p>
 *
 * <p>Phase 1 only registers the 14 trivial classes. The remaining four —
 * {@code GoogleAdsHttpRequester}, {@code GoogleAdsStreamingDecoder},
 * {@code GoogleAdsRetriever}, {@code CriterionRetriever}, {@code CustomGAQueryHttpRequester} —
 * are wired in later phases (1.5 + 2).</p>
 */
public final class GoogleAdsRegistrar {

    private static final String PREFIX = "source_google_ads.components.";

    static {
        register();
    }

    private GoogleAdsRegistrar() {
    }

    /** Idempotent registration of every Phase 1 Google Ads class_name. */
    public static void register() {
        // Transformations
        CustomComponentRegistry.register(
            PREFIX + "KeysToSnakeCaseGoogleAdsTransformation",
            KeysToSnakeCaseGoogleAdsTransformation::new);
        CustomComponentRegistry.register(
            PREFIX + "FlattenNestedDictsTransformation",
            FlattenNestedDictsTransformation::new);
        CustomComponentRegistry.register(
            PREFIX + "SerializeMessageFieldsTransformation",
            SerializeMessageFieldsTransformation::new);

        // Schema normalization (one impl, manifest references it 5x — registered once).
        CustomComponentRegistry.register(
            PREFIX + "DoubleQuotedDictTypeTransformer",
            DoubleQuotedDictTypeTransformer::new);

        // Record extractor / filter
        CustomComponentRegistry.register(
            PREFIX + "AccessibleAccountsExtractor",
            AccessibleAccountsExtractor::new);
        CustomComponentRegistry.register(
            PREFIX + "CustomerClientFilter",
            CustomerClientFilter::new);

        // Schema loader
        CustomComponentRegistry.register(
            PREFIX + "CustomGAQuerySchemaLoader",
            CustomGAQuerySchemaLoader::new);

        // State migrations
        CustomComponentRegistry.register(
            PREFIX + "GoogleAdsPerPartitionStateMigration",
            GoogleAdsPerPartitionStateMigration::new);
        CustomComponentRegistry.register(
            PREFIX + "GoogleAdsCriterionParentStateMigration",
            GoogleAdsCriterionParentStateMigration::new);
        CustomComponentRegistry.register(
            PREFIX + "GoogleAdsGlobalStateMigration",
            GoogleAdsGlobalStateMigration::new);

        // Requester subclasses (Phase 1 stubs that throw on send())
        CustomComponentRegistry.register(
            PREFIX + "CriterionFullRefreshRequester",
            CriterionFullRefreshRequester::new);
        CustomComponentRegistry.register(
            PREFIX + "CriterionIncrementalRequester",
            CriterionIncrementalRequester::new);
        CustomComponentRegistry.register(
            PREFIX + "ChangeStatusRequester",
            ChangeStatusRequester::new);
        CustomComponentRegistry.register(
            PREFIX + "ClickViewHttpRequester",
            ClickViewHttpRequester::new);
    }
}
