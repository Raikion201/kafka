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
package org.apache.kafka.connect.manifest.codegen.runtime.customs.generic;

import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomComponentRegistry;

/**
 * Registers all {@code source_declarative_manifest.components.*} custom component
 * implementations that are shared across multiple Airbyte connectors.
 *
 * <p>Covered components (by connector of origin):</p>
 * <ul>
 *   <li>{@code CustomFieldTransformation} — Chargebee (collects {@code cf_*} keys)</li>
 *   <li>{@code TransformEmptyMetrics} — TikTok Marketing (replaces {@code "-"} with null)</li>
 *   <li>{@code TransformDatetimesToRFC3339} — Amplitude (normalizes 7 datetime fields)</li>
 *   <li>{@code SanitizeNumericFields} — Google Search Console (coerces metric fields)</li>
 *   <li>{@code RemoveEmptyFields} — Jira (removes null values from nested dicts)</li>
 *   <li>{@code DateTimeTransformer} — BigCommerce (reformats datetime fields to ISO 8601)</li>
 *   <li>{@code ListAddFields} — Instatus (extracts IDs from a list field)</li>
 *   <li>{@code ObjectDpathExtractor} — Alpha Vantage (emits each map key as a record)</li>
 * </ul>
 */
public final class GenericCustomComponentsRegistrar {

    private static final String SDM = "source_declarative_manifest.components.";

    static {
        register();
    }

    private GenericCustomComponentsRegistrar() {
    }

    private static final String RSS = "source_rss.components.";

    /** Idempotent registration. Called from the static initializer and by generated task code. */
    public static void register() {
        CustomComponentRegistry.register(
            RSS + "CustomExtractor",
            RssCustomExtractor::new);

        CustomComponentRegistry.register(
            SDM + "CustomFieldTransformation",
            ChargebeeCustomFieldTransformation::new);

        CustomComponentRegistry.register(
            SDM + "TransformEmptyMetrics",
            TransformEmptyMetrics::new);

        CustomComponentRegistry.register(
            SDM + "TransformDatetimesToRFC3339",
            TransformDatetimesToRFC3339::new);

        CustomComponentRegistry.register(
            SDM + "SanitizeNumericFields",
            SanitizeNumericFields::new);

        CustomComponentRegistry.register(
            SDM + "RemoveEmptyFields",
            RemoveEmptyFields::new);

        CustomComponentRegistry.register(
            SDM + "DateTimeTransformer",
            DateTimeTransformer::new);

        CustomComponentRegistry.register(
            SDM + "ListAddFields",
            ListAddFields::new);

        CustomComponentRegistry.register(
            SDM + "ObjectDpathExtractor",
            ObjectDpathExtractor::new);

        CustomComponentRegistry.register(
            SDM + "NotionPropertiesTransformation",
            NotionPropertiesTransformation::new);

        CustomComponentRegistry.register(
            SDM + "BingAdsCampaignsRecordTransformer",
            BingAdsCampaignsRecordTransformer::new);

        CustomComponentRegistry.register(
            SDM + "AddFieldsFromEndpointTransformation",
            AddFieldsFromEndpointTransformation::new);

        CustomComponentRegistry.register(
            SDM + "InstagramMediaChildrenTransformation",
            InstagramMediaChildrenTransformation::new);

        CustomComponentRegistry.register(
            SDM + "CampaignsDetailedTransformation",
            CampaignsDetailedTransformation::new);

        CustomComponentRegistry.register(
            SDM + "ContentOwnerRequester",
            ContentOwnerRequester::new);

        CustomComponentRegistry.register(
            SDM + "JobRequester",
            JobRequester::new);

        CustomComponentRegistry.register(
            SDM + "USCensusRecordExtractor",
            USCensusRecordExtractor::new);

        CustomComponentRegistry.register(
            SDM + "NullCheckedDpathExtractor",
            NullCheckedDpathExtractor::new);

        CustomComponentRegistry.register(
            SDM + "LabelsRecordExtractor",
            LabelsRecordExtractor::new);

        CustomComponentRegistry.register(
            SDM + "KlaviyoIncludedFieldExtractor",
            KlaviyoIncludedFieldExtractor::new);

        CustomComponentRegistry.register(
            SDM + "CombinedExtractor",
            CombinedExtractor::new);

        CustomComponentRegistry.register(
            SDM + "KeyValueExtractor",
            KeyValueExtractorStub::new);

        CustomComponentRegistry.register(
            SDM + "DimensionFilterConfigTransformation",
            DimensionFilterConfigTransformationStub::new);
    }
}
