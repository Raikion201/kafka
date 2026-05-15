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

import org.apache.kafka.connect.manifest.codegen.model.AuthenticatorSpec;
import org.apache.kafka.connect.manifest.codegen.model.ManifestSpec;
import org.apache.kafka.connect.manifest.codegen.model.RequesterSpec;
import org.apache.kafka.connect.manifest.codegen.model.StreamSpec;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.TypeSpec;

/**
 * Specialised codegen path for manifests whose primary stream is a {@code dynamic_streams}
 * entry (Airbyte runtime stream discovery).
 *
 * <p>Currently supports three shapes:
 * <ul>
 *   <li>Google Sheets — {@code sheets.googleapis.com} discovery + {@code spreadsheet_id} config</li>
 *   <li>Airtable — {@code api.airtable.com} base URL, HttpComponentsResolver</li>
 *   <li>Google Analytics Data API — {@code analyticsdata.googleapis.com} base URL,
 *       ConfigComponentsResolver with 57 predefined reports</li>
 * </ul>
 * All other manifests fall back to {@link GenericDynamicStreamStub} which throws on
 * {@code start()} with a clear "not yet supported" message.
 */
public class DynamicStreamTaskGenerator {

    JavaFile generate(ManifestSpec spec, String pkgName, StreamSpec stream) throws CodegenException {
        String baseName = ManifestSpec.toClassName(spec.connectorClassName().replace("Source", ""));
        String taskClassName = baseName + "SourceTask";
        String configClassName = baseName + "ConnectorConfig";
        ClassName configClass = ClassName.get(pkgName, configClassName);

        if (canGenerateDynamicStream(spec, stream)) {
            AuthenticatorSpec auth = stream.getRetriever().getRequester().getAuthenticator();
            AuthenticatorSpec oauth = auth.selectiveOAuth();
            String tokenEndpoint = oauth.getTokenRefreshEndpoint();
            if (tokenEndpoint == null || tokenEndpoint.isBlank()) {
                throw new CodegenException("OAuth branch is missing token_refresh_endpoint");
            }
            String selectionKey = auth.selectiveOAuthKey();
            String selectionPath = String.join(",", auth.getAuthenticatorSelectionPath());

            RequesterSpec discoveryReq = stream.getDiscoveryRequester();
            String discoveryUrl = discoveryReq != null ? discoveryReq.effectiveBaseUrl() : "";
            if (discoveryUrl.isBlank()) {
                discoveryUrl = stream.getRetriever().getRequester().effectiveBaseUrl();
            }

            String baseUrlTemplate = stream.getRetriever().getRequester().effectiveBaseUrl();
            String baseUrlPrefix = JinjaSnippets.stripTemplatedSuffix(baseUrlTemplate);

            TypeSpec type = DynamicStreamTaskBody.build(taskClassName, configClass,
                tokenEndpoint, selectionKey, selectionPath, discoveryUrl, baseUrlPrefix);
            return JavaFile.builder(pkgName, type).skipJavaLangImports(true).build();
        }

        if (canGenerateAirtable(stream)) {
            TypeSpec type = AirtableTaskBody.build(taskClassName, configClass);
            return JavaFile.builder(pkgName, type).skipJavaLangImports(true).build();
        }

        if (canGenerateGoogleAnalytics(stream)) {
            TypeSpec type = GoogleAnalyticsDataApiTaskBody.build(taskClassName, configClass);
            return JavaFile.builder(pkgName, type).skipJavaLangImports(true).build();
        }

        TypeSpec stub = GenericDynamicStreamStub.build(taskClassName, configClass,
            spec.connectorClassName());
        return JavaFile.builder(pkgName, stub).skipJavaLangImports(true).build();
    }

    /**
     * Returns true when the manifest's dynamic stream is shaped like Google Sheets.
     */
    private static boolean canGenerateDynamicStream(ManifestSpec spec, StreamSpec stream) {
        RequesterSpec discoveryReq = stream.getDiscoveryRequester();
        String discoveryUrl = discoveryReq != null ? discoveryReq.effectiveBaseUrl() : "";
        boolean isSheetsUrl = discoveryUrl.contains("sheets.googleapis.com");
        boolean hasSpreadsheetId = spec.getSpec() != null
            && spec.getSpec().getConnectionSpecification() != null
            && spec.getSpec().getConnectionSpecification().getProperties() != null
            && spec.getSpec().getConnectionSpecification().getProperties().containsKey("spreadsheet_id");
        AuthenticatorSpec auth = stream.getRetriever().getRequester().getAuthenticator();
        boolean hasSelectiveOAuth = auth != null && auth.isSelective() && auth.selectiveOAuth() != null;
        return isSheetsUrl && hasSpreadsheetId && hasSelectiveOAuth;
    }

    /** Returns true when the manifest's dynamic stream template polls api.airtable.com. */
    private static boolean canGenerateAirtable(StreamSpec stream) {
        String baseUrl = stream.getRetriever().getRequester().effectiveBaseUrl();
        return baseUrl != null && baseUrl.contains("api.airtable.com");
    }

    /** Returns true when the manifest's dynamic stream polls analyticsdata.googleapis.com. */
    private static boolean canGenerateGoogleAnalytics(StreamSpec stream) {
        String baseUrl = stream.getRetriever().getRequester().effectiveBaseUrl();
        return baseUrl != null && baseUrl.contains("analyticsdata.googleapis.com");
    }
}
