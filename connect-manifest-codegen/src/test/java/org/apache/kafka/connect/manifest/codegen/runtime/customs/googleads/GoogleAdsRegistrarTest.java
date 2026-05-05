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

import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomComponent;
import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomComponentRegistry;
import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomRecordExtractor;
import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomRecordFilter;
import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomRequester;
import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomSchemaLoader;
import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomSchemaNormalization;
import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomStateMigration;
import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomTransformation;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GoogleAdsRegistrarTest {

    private static final String P = "source_google_ads.components.";

    @Test
    void everyPhaseOneClassNameResolves() {
        GoogleAdsRegistrar.register();

        assertResolves(P + "KeysToSnakeCaseGoogleAdsTransformation",
            CustomTransformation.class, Collections.emptyMap());
        assertResolves(P + "FlattenNestedDictsTransformation",
            CustomTransformation.class, Collections.emptyMap());
        assertResolves(P + "SerializeMessageFieldsTransformation",
            CustomTransformation.class, Collections.emptyMap());

        assertResolves(P + "DoubleQuotedDictTypeTransformer",
            CustomSchemaNormalization.class, Collections.emptyMap());

        assertResolves(P + "AccessibleAccountsExtractor",
            CustomRecordExtractor.class, Collections.emptyMap());
        assertResolves(P + "CustomerClientFilter",
            CustomRecordFilter.class, Collections.emptyMap());

        assertResolves(P + "CustomGAQuerySchemaLoader",
            CustomSchemaLoader.class, Map.of("query", "SELECT campaign.id FROM campaign"));

        assertResolves(P + "GoogleAdsPerPartitionStateMigration",
            CustomStateMigration.class, Collections.emptyMap());
        assertResolves(P + "GoogleAdsCriterionParentStateMigration",
            CustomStateMigration.class, Collections.emptyMap());
        assertResolves(P + "GoogleAdsGlobalStateMigration",
            CustomStateMigration.class, Collections.emptyMap());

        assertResolves(P + "CriterionFullRefreshRequester",
            CustomRequester.class, Collections.emptyMap());
        assertResolves(P + "CriterionIncrementalRequester",
            CustomRequester.class, Collections.emptyMap());
        assertResolves(P + "ChangeStatusRequester",
            CustomRequester.class, Collections.emptyMap());
        assertResolves(P + "ClickViewHttpRequester",
            CustomRequester.class, Collections.emptyMap());
    }

    @Test
    void registerIsIdempotent() {
        GoogleAdsRegistrar.register();
        GoogleAdsRegistrar.register();
        // No throw, and registry still resolves.
        assertTrue(CustomComponentRegistry.isRegistered(P + "FlattenNestedDictsTransformation"));
    }

    private static <T extends CustomComponent> void assertResolves(
            String className,
            Class<T> expected,
            Map<String, Object> params) {
        assertTrue(CustomComponentRegistry.isRegistered(className),
            "expected " + className + " to be registered");
        T instance = CustomComponentRegistry.create(
            className, expected, Collections.emptyMap(), params);
        assertNotNull(instance, className + " factory returned null");
    }
}
